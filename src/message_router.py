import asyncio
import ipaddress
import logging

from meshtastic.protobuf import mesh_pb2, portnums_pb2

from src import config as cfg

logger = logging.getLogger("multiplextashtic.router")

BLOCKED_PORTNUMS = {
    portnums_pb2.ADMIN_APP,
}

LOCALHOST_IPS = {"127.0.0.1", "::1", "localhost"}


def _ip_matches_any(client_ip: str, allowed_entries: list[str]) -> bool:
    for entry in allowed_entries:
        try:
            if "/" in entry:
                network = ipaddress.ip_network(entry, strict=False)
                if ipaddress.ip_address(client_ip) in network:
                    return True
            else:
                if ipaddress.ip_address(client_ip) == ipaddress.ip_address(entry):
                    return True
        except ValueError:
            logger.warning(f"Invalid IP/CIDR in allow_admin_from_ips: {entry}")
    return False


class MessageRouter:
    def __init__(self, phys_mgr, app_config: cfg.AppConfig, mqtt_bridge=None):
        self._phys_mgr = phys_mgr
        self._app_config = app_config
        self._mqtt_bridge = mqtt_bridge

    def set_mqtt_bridge(self, mqtt_bridge) -> None:
        self._mqtt_bridge = mqtt_bridge

    async def route_from_client(self, payload: bytes, client_ip: str = "unknown") -> tuple[bool, bytes | None, bytes | None]:
        """Route a ToRadio payload from a client.

        Returns (allowed, raw_to_forward, client_response):
          allowed=True, raw_to_forward=bytes -> forward to physical node via queue
          allowed=False, raw_to_forward=None -> blocked/handled locally
          client_response=bytes -> send directly back to the requesting client
          client_response=None -> no response to send back
        """
        try:
            to_radio = mesh_pb2.ToRadio()
            to_radio.ParseFromString(payload)
        except Exception as e:
            logger.warning(f"Failed to parse ToRadio: {e}")
            return False, None, None

        if to_radio.disconnect:
            logger.info("Client requested disconnect")
            return False, None, None

        if to_radio.HasField("heartbeat"):
            return False, None, None

        if to_radio.want_config_id:
            logger.info(f"Client requested config (id={to_radio.want_config_id})")
            return False, None, None

        if to_radio.HasField("mqttClientProxyMessage"):
            logger.debug("Client sent mqttClientProxyMessage; publishing via MQTT bridge")
            if self._mqtt_bridge is not None:
                msg = to_radio.mqttClientProxyMessage
                payload = bytes(msg.data) if msg.data else (msg.text.encode() if msg.text else b"")
                await self._mqtt_bridge.publish_proxy(msg.topic, payload, retain=msg.retained)
            return False, None, None

        if to_radio.HasField("packet"):
            return await self._route_packet(to_radio, payload, client_ip)

        logger.debug("Unhandled ToRadio type, ignoring")
        return False, None, None

    async def _route_packet(self, to_radio: mesh_pb2.ToRadio, raw_payload: bytes, client_ip: str) -> tuple[bool, bytes | None, bytes | None]:
        packet = to_radio.packet
        portnum = packet.decoded.portnum if packet.decoded.portnum else None

        # Admin command IP whitelist check
        if portnum == portnums_pb2.ADMIN_APP:
            # Self-addressed bypass: safe read-only queries (getConfig, getChannel, etc.)
            pkt_from = getattr(packet, "from", 0)
            if pkt_from != 0 and pkt_from == packet.to:
                logger.debug(f"Allowing self-addressed ADMIN_APP (from==to=0x{pkt_from:08x})")
            else:
                allowed_ips = self._app_config.security.allow_admin_from_ips
                is_localhost = client_ip in LOCALHOST_IPS
                
                if is_localhost:
                    logger.debug(f"Allowing ADMIN_APP from localhost ({client_ip})")
                elif allowed_ips:
                    if _ip_matches_any(client_ip, allowed_ips):
                        logger.debug(f"Allowing ADMIN_APP from whitelisted IP ({client_ip})")
                    else:
                        logger.warning(f"Blocked ADMIN_APP from {client_ip} (not in allowlist: {allowed_ips})")
                        return False, None, None
                else:
                    # No allowlist configured - block all non-localhost
                    logger.warning(f"Blocked ADMIN_APP from {client_ip} (no allowlist configured)")
                    return False, None, None

        # Blocked portnums: ADMIN_APP is handled by the whitelist above, so
        # any other entry in BLOCKED_PORTNUMS is dropped here.
        if portnum is not None and portnum in BLOCKED_PORTNUMS and portnum != portnums_pb2.ADMIN_APP:
            logger.warning(f"Blocked portnum {portnum} from client, silently discarding")
            return False, None, None

        # Admin command interception (Phase C11)
        if portnum == portnums_pb2.ADMIN_APP:
            admin_payload = packet.decoded.payload if packet.decoded else b""
            if admin_payload:
                try:
                    from meshtastic.protobuf import admin_pb2
                    admin_msg = admin_pb2.AdminMessage()
                    admin_msg.ParseFromString(admin_payload)
                    if admin_msg.HasField("remove_by_nodenum"):
                        logger.warning(f"Intercepted removeByNodenum for node {admin_msg.remove_by_nodenum}, fabricating ACK")
                        fake_ack = self._build_fake_routing_ack(packet.id, packet)
                        return False, None, fake_ack
                    if admin_msg.HasField("add_contact"):
                        logger.warning("Blocked addContact admin message (PKI keystore protection)")
                        return False, None, None
                except Exception:
                    pass

        portnum_name = portnums_pb2._PORTNUM.values_by_number.get(portnum)
        name_str = portnum_name.name if portnum_name else f"UNKNOWN[{portnum}]"
        logger.debug(f"Forwarding {name_str}[{portnum}] to physical node")

        # Strip PKI encryption from from=0 packets (Yeraze §5.7)
        # Android clients send PKI-encrypted packets with from=0 which the
        # physical node can't validate (no public key for node 0).
        pkt_from = getattr(packet, "from", 0)
        if pkt_from == 0 and packet.pki_encrypted:
            logger.debug(f"Stripping PKI encryption (from=0, pki_encrypted=True)")
            try:
                packet.pki_encrypted = False
                packet.ClearField("public_key")
                new_tr = mesh_pb2.ToRadio()
                new_tr.packet.CopyFrom(packet)
                raw_payload = new_tr.SerializeToString()
            except Exception as e:
                logger.warning(f"PKI stripping failed, forwarding original: {e}")

        return True, raw_payload, None

    def _build_fake_routing_ack(self, request_id: int, packet: mesh_pb2.MeshPacket) -> bytes:
        """Fabricate a ROUTING_APP ACK with errorReason=NONE for removeByNodenum.
        Prevents client UI from hanging on delete gesture (Yeraze §5.4)."""
        requester_node = getattr(packet, "from", 0) or self._phys_mgr.my_node_num
        acker_node = self._phys_mgr.my_node_num

        data = mesh_pb2.Data()
        data.portnum = portnums_pb2.ROUTING_APP
        routing = mesh_pb2.Routing()
        routing.request_id = request_id
        routing.error_reason = mesh_pb2.Routing.NONE
        data.payload = routing.SerializeToString()

        mp = mesh_pb2.MeshPacket()
        mp.id = request_id
        setattr(mp, "from", acker_node)
        mp.to = requester_node
        mp.decoded.CopyFrom(data)

        fr = mesh_pb2.FromRadio()
        fr.packet.CopyFrom(mp)
        return fr.SerializeToString()

    def route_from_physical(self, from_radio_bytes: bytes) -> bytes | None:
        """Route a FromRadio payload from physical node to clients.
        Returns the bytes to broadcast, or None to skip/divert."""
        try:
            from_radio = mesh_pb2.FromRadio()
            from_radio.ParseFromString(from_radio_bytes)
        except Exception:
            return from_radio_bytes

        if from_radio.config_complete_id:
            logger.debug(f"Router: forwarding config_complete id={from_radio.config_complete_id} to clients")

        if from_radio.HasField("mqttClientProxyMessage"):
            logger.debug("Router: diverting mqttClientProxyMessage to MQTT bridge")
            if self._mqtt_bridge is not None:
                asyncio.create_task(self._publish_proxy_to_bridge(from_radio))
            return None

        if from_radio.HasField("packet") and self._mqtt_bridge is not None:
            bridge_cfg = self._app_config.mqtt_bridge
            if bridge_cfg.enabled and self._mqtt_bridge.raw_dispatch_wanted():
                pkt = from_radio.packet
                logger.debug(
                    f"Router: dispatching raw id={pkt.id} ch={pkt.channel} "
                    "(per-leg scope decided by bridge)"
                )
                asyncio.create_task(self._mqtt_bridge.publish_packet(pkt))
            if self._mqtt_bridge.standard_mirror_active():
                asyncio.create_task(self._mqtt_bridge.publish_standard(pkt))

            return from_radio_bytes

    async def _publish_proxy_to_bridge(self, from_radio: mesh_pb2.FromRadio) -> None:
        try:
            msg = from_radio.mqttClientProxyMessage
            payload = bytes(msg.data) if msg.data else (msg.text.encode() if msg.text else b"")
            if payload:
                try:
                    from meshtastic.protobuf import mqtt_pb2
                    env = mqtt_pb2.ServiceEnvelope()
                    env.ParseFromString(payload)
                    if env.packet.id:
                        self._mqtt_bridge.note_proxy_published(env.packet.id)
                except Exception:
                    pass
            await self._mqtt_bridge.publish_proxy(msg.topic, payload, retain=msg.retained)
        except Exception as e:
            logger.error(f"Router: failed to publish proxy to MQTT bridge: {e}")
