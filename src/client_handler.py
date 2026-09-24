import asyncio
import logging
import time

from meshtastic.protobuf import mesh_pb2

from src import config as cfg
from src.protocol import encode_frame, find_frame

logger = logging.getLogger("multiplextashtic.client")

CONFIG_COOLDOWN_SECONDS = 5


class ClientHandler:
    def __init__(
        self,
        client_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        config: cfg.ServerConfig,
        phys_mgr,
        app_config: cfg.AppConfig,
        cache=None,
        config_capture=None,
        msg_queue=None,
        broadcast_cb=None,
        mqtt_bridge=None,
    ):
        self._client_id = client_id
        self._reader = reader
        self._writer = writer
        self._config = config
        self._phys_mgr = phys_mgr
        self._app_config = app_config
        self._cache = cache
        self._config_capture = config_capture
        self._msg_queue = msg_queue
        self._broadcast_cb = broadcast_cb or (lambda x: None)
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._peer_name = writer.get_extra_info("peername", ("unknown", 0))
        self._config_sent = False
        self._last_config_sent_at = 0.0
        self._last_config_id = 0
        self.last_activity: float = time.time()
        self.last_activity_monotonic: float = time.monotonic()
        self._mqtt_bridge = mqtt_bridge

    def set_mqtt_bridge(self, mqtt_bridge) -> None:
        self._mqtt_bridge = mqtt_bridge

    @property
    def peer_name(self) -> tuple:
        return self._peer_name

    @property
    def queue(self) -> asyncio.Queue:
        return self._queue

    async def handle(self) -> None:
        logger.info(f"Client {self._client_id}: connected from {self._peer_name}")
        try:
            read_task = asyncio.create_task(self._read_loop(), name=f"read-{self._client_id}")
            write_task = asyncio.create_task(self._write_loop(), name=f"write-{self._client_id}")

            done, _ = await asyncio.wait(
                [read_task, write_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for t in done:
                try:
                    t.result()
                except (asyncio.IncompleteReadError, ConnectionResetError, ConnectionAbortedError):
                    logger.info(f"Client {self._client_id}: disconnected")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Client {self._client_id}: error: {e}")

            read_task.cancel()
            write_task.cancel()
            await asyncio.gather(read_task, write_task, return_exceptions=True)

        except asyncio.IncompleteReadError:
            logger.info(f"Client {self._client_id}: disconnected")
        except ConnectionResetError:
            logger.info(f"Client {self._client_id}: connection reset")
        except Exception as e:
            logger.error(f"Client {self._client_id}: unhandled error: {e}")
        finally:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            logger.info(f"Client {self._client_id}: cleaned up")

    async def _read_loop(self) -> None:
        buffer = b""
        while True:
            chunk = await self._reader.read(4096)
            if not chunk:
                break
            self.last_activity = time.time()
            self.last_activity_monotonic = time.monotonic()
            buffer += chunk

            while True:
                payload, buffer = find_frame(buffer)
                if payload is None:
                    break
                await self._handle_to_radio(payload)

    async def _write_loop(self) -> None:
        while True:
            data = await self._queue.get()
            try:
                frame = encode_frame(data)
                self._writer.write(frame)
                await self._writer.drain()
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                logger.info(f"Client {self._client_id}: write error ({e}), stopping write loop")
                raise

    async def _send_raw(self, data: bytes) -> None:
        frame = encode_frame(data)
        self._writer.write(frame)
        await self._writer.drain()

    async def _handle_to_radio(self, payload: bytes) -> None:
        try:
            to_radio = mesh_pb2.ToRadio()
            to_radio.ParseFromString(payload)
        except Exception as e:
            logger.warning(f"Failed to parse ToRadio: {e}")
            return

        # wantConfigId - init flow with rate limiting
        if to_radio.want_config_id:
            now = time.time()
            elapsed = now - self._last_config_sent_at
            if self._config_sent and to_radio.want_config_id == self._last_config_id and elapsed < CONFIG_COOLDOWN_SECONDS:
                logger.info(f"Client {self._client_id}: throttled duplicate config request ({elapsed:.1f}s ago)")
                return
            self._last_config_id = to_radio.want_config_id
            self._last_config_sent_at = now
            logger.info(f"Client {self._client_id}: requested config (id={to_radio.want_config_id})")
            await self._send_initial_config()
            self._config_sent = True
            return

        # Heartbeat - respond with QueueStatus locally (before config_sent check)
        if to_radio.HasField("heartbeat"):
            logger.debug(f"Client {self._client_id}: heartbeat, sending QueueStatus")
            await self._send_queue_status()
            return

        # Must have completed config before processing messages
        if not self._config_sent:
            logger.debug(f"Client {self._client_id}: ignoring message before config complete")
            return

        # Disconnect
        if to_radio.disconnect:
            logger.info(f"Client {self._client_id}: requested disconnect")
            return

        # Route proxy messages via MQTT bridge through MessageRouter
        if to_radio.HasField("mqttClientProxyMessage"):
            from src.message_router import MessageRouter
            router = MessageRouter(self._phys_mgr, self._app_config, self._mqtt_bridge)
            client_ip = self._peer_name[0] if self._peer_name else "unknown"
            allowed, raw_to_forward, client_response = await router.route_from_client(payload, client_ip)
            if client_response:
                await self._send_raw(client_response)
            return

        # Route packet messages
        if to_radio.HasField("packet"):
            from src.message_router import MessageRouter
            router = MessageRouter(self._phys_mgr, self._app_config, self._mqtt_bridge)
            client_ip = self._peer_name[0] if self._peer_name else "unknown"
            allowed, raw_to_forward, client_response = await router.route_from_client(payload, client_ip)
            if client_response:
                await self._send_raw(client_response)
            if allowed and raw_to_forward and self._msg_queue:
                # Phase C10: process locally so it appears in web UI
                await self._process_locally(to_radio)
                await self._msg_queue.enqueue_raw(raw_to_forward)
            return

        logger.debug(f"Client {self._client_id}: unhandled ToRadio type, ignoring")

    async def _process_locally(self, to_radio: mesh_pb2.ToRadio) -> None:
        """Broadcast a client's outgoing message to ALL clients for web UI visibility."""
        if not to_radio.HasField("packet"):
            return
        p = to_radio.packet
        # Yeraze Issue #626: populate from=0 with local nodeNum for correct attribution
        override_from = self._phys_mgr.my_node_num if getattr(p, 'from', 0) == 0 else None
        try:
            fr = mesh_pb2.FromRadio()
            fr.id = p.id
            fr.packet.CopyFrom(p)
            if override_from is not None:
                setattr(fr.packet, "from", override_from)
            raw = fr.SerializeToString()
            await self._broadcast_cb(raw)
        except Exception as e:
            logger.debug(f"Local processing: {e}")

    async def _send_queue_status(self) -> None:
        try:
            fr = mesh_pb2.FromRadio()
            qs = mesh_pb2.QueueStatus()
            qs.res = 0
            qs.free = 32
            qs.maxlen = 32
            qs.mesh_packet_id = 0
            fr.queueStatus.CopyFrom(qs)
            data = fr.SerializeToString()
            await self._send_raw(data)
        except Exception as e:
            logger.debug(f"Failed to send QueueStatus: {e}")

    async def _send_initial_config(self) -> None:
        from src.config_capture import ConfigReplayBuilder
        builder = ConfigReplayBuilder(self._config_capture, self._phys_mgr, self._app_config, self._cache)
        await builder.replay(self._writer, self._last_config_id)

    async def refresh_config(self) -> None:
        self._config_sent = False
        self._last_config_id = 0
        self._last_config_sent_at = 0.0
        await self._send_initial_config()
        self._config_sent = True
