import asyncio
import hashlib
import logging
import re
import ssl
import time
import uuid
from typing import Callable, Awaitable

import aiomqtt
from aiomqtt import Message
from meshtastic.protobuf import portnums_pb2

from src import config as cfg
from src import __version__

logger = logging.getLogger("multiplextashtic.mqtt")

MQTT_TOPIC_RE = re.compile(r"^(msh|#|\$share)/.*$")
MAX_PAYLOAD = 512

# Only publish packets that arrived via LoRa radio.
# transport_mechanism values: 0=internal, 1=lora, 2-4=lora_alt, 5=mqtt, 6=udp, 7=api
_LORA_TRANSPORTS = frozenset({1, 2, 3, 4})


def _sanitize_client_id(raw: str, max_len: int = 32) -> str:
    cleaned = re.sub(r"[^\x21-\x7E]", "", raw)
    return cleaned[:max_len] or "multiplextashtic"


def _node_id_to_client_suffix(node_num: int) -> str:
    return f"{node_num:08x}"


class MqttLeg:
    """One MQTT broker connection (reusable for primary + secondary legs).

    Owns connection lifecycle only: client, connected event, reconnect loop,
    publish + per-leg counters. Fan-out policy (which topics go where,
    per-leg raw_uplink/raw_scope, overlap guard) and downlink processing
    stay in MqttBridge.
    """

    def __init__(
        self,
        name: str,
        endpoint_fn: Callable[[], dict],
        id_fn: Callable[[], str],
        subscribe: bool = False,
    ):
        self.name = name
        self._endpoint_fn = endpoint_fn
        self._id_fn = id_fn
        self._subscribe = subscribe
        self._client: aiomqtt.Client | None = None
        self._connected = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._running = False
        self._published = 0
        self._dropped = 0

    def client_id(self) -> str:
        return self._id_fn()

    @property
    def published(self) -> int:
        return self._published

    @property
    def dropped(self) -> int:
        return self._dropped

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def note_drop(self) -> None:
        self._dropped += 1

    async def publish(
        self, topic: str, payload: bytes, qos: int = 0, retain: bool = False,
        err_prefix: str = "MqttBridge: publish failed",
    ) -> bool:
        if self._client is None or not self._connected.is_set():
            self._dropped += 1
            logger.debug(f"MqttBridge: {self.name} not connected, dropping publish")
            return False
        try:
            await self._client.publish(topic, payload=payload, qos=qos, retain=retain)
            self._published += 1
            return True
        except Exception as e:
            self._dropped += 1
            logger.error(f"{err_prefix}: {e}")
            return False

    async def start(
        self,
        *,
        is_running_fn: Callable[[], bool],
        active_fn: Callable[[], bool],
        topics_fn: Callable[[], list] | None = None,
        on_message: Callable[[Message], Awaitable[None]] | None = None,
    ) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(
            self._loop(is_running_fn, active_fn, topics_fn, on_message)
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._client:
            try:
                await self._client.__aexit__()
            except Exception as e:
                logger.warning(f"MqttBridge: {self.name} disconnect error: {e}")
            self._client = None
        self._connected.clear()

    async def _loop(
        self,
        is_running_fn: Callable[[], bool],
        active_fn: Callable[[], bool],
        topics_fn: Callable[[], list] | None,
        on_message: Callable[[Message], Awaitable[None]] | None,
    ) -> None:
        """Shared connection loop: connect with backoff, optionally subscribe.

        Idles while inactive so an absent/disabled leg costs nothing.
        Uplink-only legs never subscribe: nothing from that broker ever
        reaches the mesh. Credentials never logged.
        """
        label = "" if self.name == "primary" else f"{self.name} "
        backoff = 1
        while is_running_fn():
            if not active_fn():
                await asyncio.sleep(5)
                continue
            ep = self._endpoint_fn()
            try:
                kwargs: dict = dict(
                    hostname=ep["host"],
                    port=ep["port"],
                    username=ep["username"] or None,
                    password=ep["password"] or None,
                    identifier=self._id_fn(),
                    keepalive=ep.get("keepalive", 60),
                )
                if ep.get("tls"):
                    kwargs["tls_context"] = ssl.create_default_context()
                async with aiomqtt.Client(**kwargs) as client:
                    self._client = client
                    self._connected.set()
                    backoff = 1
                    logger.info(f"MqttBridge: {label}connected as {self._id_fn()}")
                    if self._subscribe and on_message is not None and topics_fn is not None:
                        topics = topics_fn()
                        for t in topics:
                            await client.subscribe(t, qos=0)
                        logger.info(f"MqttBridge: subscribed to {len(topics)} downlink topics")
                    if self._subscribe:
                        async for msg in client.messages:
                            await on_message(msg)
                    else:
                        while is_running_fn() and self._connected.is_set():
                            await asyncio.sleep(30)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"MqttBridge: {label}connection lost ({e}); reconnecting in {backoff}s")
                self._connected.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)


class MqttBridge:
    """Orchestrator over two MqttLegs: policy, envelopes, dedupe, downlink."""

    def __init__(self, app_config: cfg.AppConfig, node_num: int = 0):
        self._config = app_config.mqtt_bridge
        self._node_num = node_num
        self._id_uuid: str | None = None
        self._primary = MqttLeg(
            "primary",
            endpoint_fn=self._primary_endpoint,
            id_fn=self._client_id,
            subscribe=True,
        )
        self._secondary = MqttLeg(
            "secondary",
            endpoint_fn=self._secondary_endpoint,
            id_fn=self._client_id2,
            subscribe=False,
        )
        self._stats_task: asyncio.Task | None = None
        self._running = False
        self._downlink_cb: Callable[[str, bytes], Awaitable[None]] | None = None
        self._dropped_uplink = 0
        self._dropped_downlink_disabled = 0
        self._loop_guard_drops = 0
        self._cache = None
        self._ok_to_mqtt_missing_logged = False
        self._recent_publishes: dict[str, float] = {}
        self._echo_suppressed = 0
        self._retained_skipped = 0
        self._recent_gateway_ids: dict[int, float] = {}
        self._recent_proxy_ids: dict[int, float] = {}
        self._node_mqtt: dict | None = None
        self._observed_region: str | None = None
        self._gateway_published = 0
        self._standard_mirrored = 0

    # -- leg compatibility shims (tests poke these directly) --
    @property
    def _client(self):
        return self._primary._client

    @_client.setter
    def _client(self, value) -> None:
        self._primary._client = value

    @property
    def _connected(self) -> asyncio.Event:
        return self._primary._connected

    @property
    def _task(self):
        return self._primary._task

    @_task.setter
    def _task(self, value) -> None:
        self._primary._task = value

    @property
    def _client2(self):
        return self._secondary._client

    @_client2.setter
    def _client2(self, value) -> None:
        self._secondary._client = value

    @property
    def _connected2(self) -> asyncio.Event:
        return self._secondary._connected

    @property
    def _task2(self):
        return self._secondary._task

    @_task2.setter
    def _task2(self, value) -> None:
        self._secondary._task = value

    # -- leg endpoints (read live so post-init config tweaks apply) --
    def _primary_endpoint(self) -> dict:
        c = self._config
        return {
            "host": c.broker,
            "port": c.port,
            "username": c.username,
            "password": c.password,
            "tls": c.tls,
            "keepalive": c.keepalive,
        }

    def _secondary_endpoint(self) -> dict:
        cfg2 = self._config.broker2
        return {
            "host": cfg2.broker,
            "port": cfg2.port,
            "username": cfg2.username,
            "password": cfg2.password,
            "tls": cfg2.tls,
            "keepalive": cfg2.keepalive,
        }

    def _client_id(self) -> str:
        # Stable when mqtt_username is set (mux-v{ver}-{mqtt_username}-{nodehex});
        # otherwise a per-boot uuid fragment keeps two muxes on the same node
        # from kick-fighting over one client-id. mqtt_username is budgeted so
        # the nodehex (and uuid) never gets truncated off the 32-char cap.
        ver = __version__
        node = _node_id_to_client_suffix(self._node_num)
        if self._config.mqtt_username:
            prefix = f"mux-v{ver}-"
            budget = max(0, 32 - len(prefix) - 1 - len(node))
            raw = f"{prefix}{self._config.mqtt_username[:budget]}-{node}"
        else:
            if self._id_uuid is None:
                self._id_uuid = uuid.uuid4().hex[:12]
            raw = f"mux-v{ver}-{node}-{self._id_uuid}"
        return _sanitize_client_id(raw)

    def _client_id2(self) -> str:
        """Client id for the secondary broker: always the primary id.

        Both legs share one id because they never point at the same broker
        (brokers_overlap() disables the secondary leg on a match). There is
        deliberately no override: the mux reports as mux everywhere.
        """
        return self._client_id()

    def brokers_overlap(self) -> bool:
        """True when broker2 points at the same host:port as primary.

        Orchestrator guard: both legs must never target the same broker
        (duplicate publishes, echo/session clashes). Normalizes scheme,
        case, trailing path and port suffix via _normalize_broker_host.
        """
        cfg2 = self._config.broker2
        if cfg2 is None or not cfg2.broker:
            return False
        return (
            self._normalize_broker_host(cfg2.broker)
            == self._normalize_broker_host(self._config.broker)
            and (cfg2.port or 0) == (self._config.port or 0)
        )

    def _broker2_active(self) -> bool:
        """True when a secondary broker is configured and enabled."""
        cfg2 = self._config.broker2
        return bool(
            self._config.enabled
            and cfg2 is not None
            and cfg2.enabled
            and cfg2.broker
            and not self.brokers_overlap()
        )

    def _topic_for_proxy(self, topic: str) -> str:
        root = self._config.root_topic.rstrip("/")
        if topic.startswith(root):
            return topic
        return f"{root}/{topic.lstrip('/')}"

    def _is_valid_downlink_topic(self, topic: str) -> bool:
        root = self._config.root_topic
        raw_root = self._config.raw_root_topic
        if not (topic == root or topic.startswith(f"{root}/")
                or topic == raw_root or topic.startswith(f"{raw_root}/")):
            return False
        return bool(
            MQTT_TOPIC_RE.match(topic)
            or topic.startswith(f"{root}/")
            or topic.startswith(f"{raw_root}/")
        )

    def resolve_region(self) -> str:
        """Resolve the region topic segment (explicit > observed > default)."""
        if self._config.region:
            return self._config.region
        if self._observed_region:
            return self._observed_region
        return "EU_868"

    def observe_proxy_topic(self, topic: str) -> None:
        """Learn the region from node-emitted proxy topics (self-updating)."""
        parts = topic.split("/")
        if len(parts) >= 2 and parts[0] == self._config.root_topic and parts[1]:
            if parts[1] != self._observed_region:
                self._observed_region = parts[1]
                logger.info(f"MqttBridge: learned region {parts[1]} from proxy topic")

    def _channel_name(self, channel_index: int) -> str | None:
        if self._cache is None:
            return None
        for ch in self._cache.channels:
            if ch.get("index") == channel_index:
                return ch.get("settings", {}).get("name") or None
        return None

    def _channel_psk(self, channel_index: int) -> bytes | None:
        if self._cache is None:
            return None
        for ch in self._cache.channels:
            if ch.get("index") == channel_index:
                psk = ch.get("settings", {}).get("psk")
                return bytes(psk) if psk else None
        return None

    def resolve_portnum_label(self, packet) -> str:
        """Portnum routing cascade for the raw topic label (label-only).

        Order: decoded present -> real name; pki_encrypted flag -> PKI;
        cached channel PSK trial decrypt -> real name; otherwise ENCRYPTED.
        Never raises; decrypted bytes never leave this function.
        """
        try:
            if packet.HasField("decoded") and packet.decoded.portnum:
                entry = portnums_pb2._PORTNUM.values_by_number.get(packet.decoded.portnum)
                return entry.name if entry else f"UNKNOWN_{packet.decoded.portnum}"
            if getattr(packet, "pki_encrypted", False):
                return "PKI"
            if packet.HasField("encrypted") and packet.encrypted:
                from src.mqtt_crypto import decrypt_data_to_portnum
                psk = self._channel_psk(packet.channel)
                if psk:
                    portnum = decrypt_data_to_portnum(
                        bytes(packet.encrypted),
                        getattr(packet, "from", 0),
                        packet.id,
                        psk,
                    )
                    if portnum:
                        entry = portnums_pb2._PORTNUM.values_by_number.get(portnum)
                        return entry.name if entry else f"UNKNOWN_{portnum}"
            return "ENCRYPTED"
        except Exception:
            return "ENCRYPTED"

    def raw_topic_for_packet(self, packet) -> str:
        """Build raw/{region}/!{senderhex} (never under msh/)."""
        sender = getattr(packet, "from", 0) or 0
        return f"{self._config.raw_root_topic}/{self.resolve_region()}/!{sender:08x}"

    @staticmethod
    def _normalize_broker_host(address: str) -> str:
        """Bare lowercase host for broker comparison (empty = public default)."""
        host = (address or "").strip().lower()
        for prefix in ("mqtt://", "mqtts://", "tcp://", "ssl://"):
            if host.startswith(prefix):
                host = host[len(prefix):]
        host = host.split("/")[0].split(":")[0]
        if not host:
            return "mqtt.meshtastic.org"
        return host

    def standard_mirror_active(self) -> bool:
        """Auto rule: mirror uplink channels to msh/ iff brokers differ."""
        mode = self._config.standard_mirror
        if mode == "on":
            return True
        if mode == "off":
            return False
        snap = getattr(self, "_node_mqtt", None)
        node_host = self._normalize_broker_host(
            (snap or {}).get("address", "") if isinstance(snap, dict) else ""
        )
        bridge_host = self._normalize_broker_host(self._config.broker)
        differ = node_host != bridge_host
        if differ:
            logger.debug(
                f"MqttBridge: standard mirror on (brokers differ: {node_host} vs {bridge_host})"
            )
        return differ

    def standard_topic_for_packet(self, packet) -> str | None:
        """msh/{region}/2/e/{channelName}/!{gateway} for known channels, else None."""
        name = self._channel_name(packet.channel)
        if not name:
            return None
        root = self._config.root_topic.rstrip("/")
        return f"{root}/{self.resolve_region()}/2/e/{name}/!{self._node_num:08x}"

    async def publish_standard(self, packet) -> bool:
        """Standard-tree mirror copy (known channels only, proxy-id deduped).

        Gated solely by standard_mirror (off disables); proxy relay is
        independent of this flag."""
        if not self._config.enabled:
            return False
        if not self.standard_mirror_active():
            return False
        if getattr(packet, "via_mqtt", False):
            return False
        if getattr(packet, "transport_mechanism", 0) not in _LORA_TRANSPORTS:
            logger.debug(f"MqttBridge: standard mirror skipping non-LoRa packet (transport={packet.transport_mechanism})")
            return False
        topic = self.standard_topic_for_packet(packet)
        if topic is None:
            return False
        if self.is_proxy_published(packet.id):
            logger.debug(f"MqttBridge: standard mirror skipping id={packet.id} (proxy covered)")
            return False
        try:
            from meshtastic.protobuf import mqtt_pb2
            env = mqtt_pb2.ServiceEnvelope()
            env.packet.CopyFrom(packet)
            env.gateway_id = f"!{self._node_num:08x}"
            payload = env.SerializeToString()
        except Exception as e:
            logger.warning(f"MqttBridge: standard envelope build failed: {e}")
            return False
        if len(payload) > MAX_PAYLOAD:
            logger.warning(f"MqttBridge: standard payload {len(payload)} > {MAX_PAYLOAD}, dropping")
            return False
        if self._client is None or not self._connected.is_set():
            logger.debug("MqttBridge: standard mirror not connected, dropping packet")
            return False
        ok = await self._primary.publish(
            topic, payload=payload, qos=0, retain=False,
            err_prefix="MqttBridge: standard mirror publish failed",
        )
        if not ok:
            return False
        self._standard_mirrored += 1
        self._remember_publish(topic, payload)
        logger.info(f"MqttBridge: standard mirror published {topic} ({len(payload)}B)")
        await self._publish_secondary(topic, payload)
        return True

    def is_proxy_covered(self, channel_index: int) -> bool:
        """True when the proxy path owns this channel (uplink flag set)."""
        if self._cache is None:
            return False
        for ch in self._cache.channels:
            if ch.get("index") == channel_index:
                return bool(ch.get("settings", {}).get("uplink_enabled", False))
        return False

    def set_node_mqtt_snapshot(self, snapshot: dict | None) -> None:
        """Node MQTT state from provisioning: {enabled, proxy, has_net, address}."""
        self._node_mqtt = snapshot

    def is_mqtt_covered(self, channel_index: int) -> bool:
        """True when something other than gateway already carries this channel.

        Covered = uplink flag AND (proxy frames active OR node reporting direct
        [enabled + proxy off + net-capable]). Unknown snapshot preserves the
        previous proxy-assumed behavior.
        """
        if self._cache is None:
            return False
        uplink = False
        for ch in self._cache.channels:
            if ch.get("index") == channel_index:
                uplink = bool(ch.get("settings", {}).get("uplink_enabled", False))
                break
        if not uplink:
            return False
        snap = getattr(self, "_node_mqtt", None)
        if snap is None:
            return True
        if snap.get("proxy"):
            return True
        return bool(snap.get("enabled") and not snap.get("proxy") and snap.get("has_net"))

    def _note_gateway_id(self, packet_id: int) -> bool:
        """Packet-id safety dedupe: True if already published recently."""
        now = time.monotonic()
        ts = self._recent_gateway_ids.get(packet_id)
        if ts is not None and now - ts < 60.0:
            return True
        self._recent_gateway_ids[packet_id] = now
        if len(self._recent_gateway_ids) > 500:
            cutoff = now - 60.0
            for k in [k for k, v in self._recent_gateway_ids.items() if v < cutoff]:
                del self._recent_gateway_ids[k]
        return False

    def note_proxy_published(self, packet_id: int) -> None:
        self._recent_proxy_ids[packet_id] = time.monotonic()
        if len(self._recent_proxy_ids) > 500:
            cutoff = time.monotonic() - 60.0
            for k in [k for k, v in self._recent_proxy_ids.items() if v < cutoff]:
                del self._recent_proxy_ids[k]

    def _recent_id(self, store: dict, packet_id: int) -> bool:
        ts = store.get(packet_id)
        if ts is None:
            return False
        if time.monotonic() - ts > 60.0:
            del store[packet_id]
            return False
        return True

    def is_proxy_published(self, packet_id: int) -> bool:
        return self._recent_id(self._recent_proxy_ids, packet_id)

    def is_gateway_published(self, packet_id: int) -> bool:
        return self._recent_id(self._recent_gateway_ids, packet_id)

    async def _publish_secondary(self, topic: str, payload: bytes) -> bool:
        """Best-effort mirror of one publish to broker2.

        Callers enforce scope: msh-tree paths always fan out, raw-tree
        gateway only when that leg's raw_uplink is true. Never raises;
        offline broker just bumps the drop counter.
        """
        if not self._broker2_active():
            return False
        if len(payload) > MAX_PAYLOAD:
            self._secondary.note_drop()
            return False
        if self._client2 is None or not self._connected2.is_set():
            self._secondary.note_drop()
            logger.debug("MqttBridge: secondary not connected, dropping mirror")
            return False
        return await self._secondary.publish(
            topic, payload=payload, qos=0, retain=False,
            err_prefix="MqttBridge: secondary publish failed",
        )

    def _matches_raw_scope(self, scope: str, packet_id: int) -> bool:
        """Scope filter for one raw leg: all = everything, else uncovered only."""
        if scope == "all":
            return True
        return not self.is_proxy_published(packet_id)

    def raw_dispatch_wanted(self) -> bool:
        """Cheap pre-gate for the router: True when any leg has raw on."""
        cfg2 = self._config.broker2
        return bool(
            self._config.enabled
            and (
                self._config.raw_uplink
                or (self._broker2_active() and cfg2 is not None and cfg2.raw_uplink)
            )
        )

    async def publish_packet(self, packet) -> bool:
        """Gateway uplink: publish one serial packet envelope to the raw tree.

        Each leg decides independently via its own raw_uplink + raw_scope.
        Returns True when at least one leg published."""
        if not self._config.enabled:
            return False
        if getattr(packet, "via_mqtt", False):
            logger.debug("MqttBridge: gateway skipping via_mqtt packet")
            return False
        if getattr(packet, "transport_mechanism", 0) not in _LORA_TRANSPORTS:
            logger.debug(f"MqttBridge: gateway skipping non-LoRa packet (transport={packet.transport_mechanism})")
            return False
        cfg2 = self._config.broker2
        primary_want = self._config.raw_uplink and self._matches_raw_scope(
            self._config.raw_scope, packet.id)
        secondary_want = (
            self._broker2_active()
            and cfg2 is not None
            and cfg2.raw_uplink
            and self._matches_raw_scope(cfg2.raw_scope, packet.id)
        )
        if not (primary_want or secondary_want):
            logger.debug(f"MqttBridge: gateway skipping id={packet.id} (no leg wants raw)")
            return False
        if self._note_gateway_id(packet.id):
            logger.debug(f"MqttBridge: gateway skipping duplicate packet id={packet.id}")
            return False
        try:
            from meshtastic.protobuf import mqtt_pb2
            env = mqtt_pb2.ServiceEnvelope()
            env.packet.CopyFrom(packet)
            env.gateway_id = f"!{self._node_num:08x}"
            payload = env.SerializeToString()
        except Exception as e:
            logger.warning(f"MqttBridge: gateway envelope build failed: {e}")
            return False
        if len(payload) > MAX_PAYLOAD:
            logger.warning(f"MqttBridge: gateway payload {len(payload)} > {MAX_PAYLOAD}, dropping")
            return False
        topic = self.raw_topic_for_packet(packet)
        published = False
        if primary_want:
            if self._client is None or not self._connected.is_set():
                logger.debug("MqttBridge: gateway not connected, dropping packet")
            elif await self._primary.publish(
                topic, payload=payload, qos=0, retain=False,
                err_prefix="MqttBridge: gateway publish failed",
            ):
                self._gateway_published += 1
                published = True
        if secondary_want:
            if await self._publish_secondary(topic, payload):
                published = True
        if published:
            self._remember_publish(topic, payload)
            logger.debug(f"MqttBridge: gateway published {topic} ({len(payload)}B)")
        return published

    async def start(self, downlink_callback: Callable[[str, bytes], Awaitable[None]] | None = None) -> None:
        if not self._config.enabled:
            logger.info("MqttBridge: not enabled, skipping start")
            return
        self._downlink_cb = downlink_callback
        self._running = True
        await self._primary.start(
            is_running_fn=lambda: self._running,
            active_fn=lambda: bool(self._config.enabled),
            topics_fn=self._downlink_topics,
            on_message=self._on_message,
        )
        await self._secondary.start(
            is_running_fn=lambda: self._running,
            active_fn=self._broker2_active,
        )
        self._stats_task = asyncio.create_task(self._stats_loop())
        logger.info(f"MqttBridge: starting client_id={self._client_id()} {self._config.broker}:{self._config.port}")
        if self._broker2_active():
            cfg2 = self._config.broker2
            # Never log credentials: host/port/client-id only.
            logger.info(f"MqttBridge: secondary starting client_id={self._client_id2()} {cfg2.broker}:{cfg2.port}")
        elif self.brokers_overlap():
            logger.warning(
                "MqttBridge: broker2 duplicates primary "
                f"({self._config.broker}:{self._config.port}); secondary disabled"
            )

    async def _stats_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(300)
                if self._running:
                    s = self.stats
                    logger.info(
                        "MqttBridge stats: connected=%s published=%d gateway=%d standard=%d dropped_uplink=%d "
                        "dropped_downlink_disabled=%d loop_guard_drops=%d echo_suppressed=%d "
                        "retained_skipped=%d published2=%d dropped2=%d",
                        self.connected, s["published"], s["gateway_published"], s["standard_mirrored"], s["dropped_uplink"],
                        s["dropped_downlink_disabled"], s["loop_guard_drops"],
                        s["echo_suppressed"], s["retained_skipped"],
                        s["published2"], s["dropped2"],
                    )
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        self._running = False
        await self._primary.stop()
        await self._secondary.stop()
        stats_task = getattr(self, "_stats_task", None)
        if stats_task:
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass
            self._stats_task = None
        logger.info("MqttBridge: stopped")

    def _downlink_topics(self) -> list[str]:
        return [
            f"{self._config.root_topic}/#",
            f"{self._config.raw_root_topic}/#",
        ]

    @staticmethod
    def _echo_key(topic: str, payload: bytes) -> str:
        return f"{topic}:{hashlib.sha256(payload).hexdigest()[:16]}"

    def _remember_publish(self, topic: str, payload: bytes) -> None:
        now = time.monotonic()
        self._recent_publishes[self._echo_key(topic, payload)] = now
        if len(self._recent_publishes) > 200:
            cutoff = now - 30.0
            for k in [k for k, ts in self._recent_publishes.items() if ts < cutoff]:
                del self._recent_publishes[k]

    def _is_recent_echo(self, topic: str, payload: bytes) -> bool:
        key = self._echo_key(topic, payload)
        ts = self._recent_publishes.get(key)
        if ts is None:
            return False
        if time.monotonic() - ts > 30.0:
            del self._recent_publishes[key]
            return False
        return True

    async def _on_message(self, msg: Message) -> None:
        try:
            topic = str(msg.topic)
            payload = bytes(msg.payload) if msg.payload else b""
            if not self._is_valid_downlink_topic(topic):
                logger.debug(f"MqttBridge: ignoring non-root downlink topic {topic}")
                return
            if not self._config.downlink_enabled:
                self._dropped_downlink_disabled += 1
                logger.debug(f"MqttBridge: downlink disabled; dropped message on {topic}")
                return
            if self._is_recent_echo(topic, payload):
                self._echo_suppressed += 1
                logger.debug(f"MqttBridge: echo suppression dropped own uplink {topic}")
                return
            if getattr(msg, "retain", False):
                self._retained_skipped += 1
                logger.info(f"MqttBridge: skipping retained downlink {topic} (stale replay guard)")
                return
            if self._node_num != 0:
                try:
                    from meshtastic.protobuf import mqtt_pb2
                    env = mqtt_pb2.ServiceEnvelope()
                    env.ParseFromString(payload)
                    if env.gateway_id and env.gateway_id == f"!{self._node_num:08x}":
                        self._loop_guard_drops += 1
                        logger.debug("MqttBridge: loop guard dropped own envelope")
                        return
                except Exception:
                    pass
            await self._downlink_cb(topic, payload)
        except Exception as e:
            logger.error(f"MqttBridge: downlink processing error: {e}")

    async def publish_proxy(self, topic: str, payload: bytes, retain: bool = False) -> None:
        if not self._config.enabled or not self._config.uplink_enabled:
            self._dropped_uplink += 1
            logger.debug("MqttBridge: uplink disabled; dropped proxy message")
            return
        if not self._config.ignore_ok_to_mqtt and not self._check_uplink_allowed():
            self._dropped_uplink += 1
            logger.debug("MqttBridge: ok_to_mqtt / channel uplink check blocked publish")
            return
        final_topic = self._topic_for_proxy(topic)
        self.observe_proxy_topic(final_topic)
        if len(payload) > MAX_PAYLOAD:
            logger.warning(f"MqttBridge: payload {len(payload)} > {MAX_PAYLOAD}, dropping")
            return
        if self._client is None or not self._connected.is_set():
            logger.debug("MqttBridge: not connected, dropping proxy publish")
            self._dropped_uplink += 1
            return
        # Leg already logs failures (default err_prefix matches legacy text).
        ok = await self._primary.publish(final_topic, payload=payload, qos=0, retain=retain)
        if not ok:
            return
        self._remember_publish(final_topic, payload)
        logger.debug(f"MqttBridge: published proxy to {final_topic} ({len(payload)}B)")
        await self._publish_secondary(final_topic, payload)

    async def _ensure_connected(self) -> None:
        if self._client is None or not self._connected.is_set():
            raise RuntimeError("MqttBridge not connected")

    def set_cache(self, cache) -> None:
        self._cache = cache

    def _check_uplink_allowed(self) -> bool:
        if self._config.ignore_ok_to_mqtt:
            return True
        if self._cache is None:
            return True
        pkt_channel = getattr(self, "_last_channel", None)
        if pkt_channel is not None and self._cache.channels:
            for ch in self._cache.channels:
                if ch.get("index") == pkt_channel:
                    allowed = ch.get("settings", {}).get("uplink_enabled", True)
                    if not allowed and not self._ok_to_mqtt_missing_logged:
                        logger.warning("MqttBridge: channel uplink_enabled=false; set ignore_ok_to_mqtt=true to bypass")
                        self._ok_to_mqtt_missing_logged = True
                    return allowed
        return True

    def mark_channel(self, channel_index: int) -> None:
        self._last_channel = channel_index

    @property
    def connected(self) -> bool:
        return self._primary.is_connected()

    @property
    def connected2(self) -> bool:
        return self._secondary.is_connected()

    @property
    def stats(self) -> dict:
        return {
            "published": self._primary.published,
            "gateway_published": self._gateway_published,
            "standard_mirrored": self._standard_mirrored,
            "dropped_uplink": self._dropped_uplink,
            "dropped_downlink_disabled": self._dropped_downlink_disabled,
            "loop_guard_drops": self._loop_guard_drops,
            "echo_suppressed": self._echo_suppressed,
            "retained_skipped": self._retained_skipped,
            "published2": self._secondary.published,
            "dropped2": self._secondary.dropped,
        }
