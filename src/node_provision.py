"""Automatic node MQTT provisioning (local physical node only).

When the MQTT bridge is enabled, the mux ensures the directly-attached node
has the two flags proxy-relay needs: moduleConfig.mqtt.enabled and
moduleConfig.mqtt.proxy_to_client_enabled. Only false->true flips, only those
two booleans, only the local node, attempted once per boot. Everything else
(address, root, credentials, channels) is never touched.

Flow: query live moduleConfig -> compare -> begin_edit -> set_module_config
-> commit_edit -> re-query to verify. Any failure degrades silently to
today's behavior (bridge runs, possibly idle).
"""

import asyncio
import logging
import random

from meshtastic.protobuf import admin_pb2, mesh_pb2, module_config_pb2, portnums_pb2

logger = logging.getLogger("multiplextashtic.provision")

MQTT_CONFIG_TYPE = 0  # AdminMessage.ModuleConfigType.MQTT_CONFIG
QUERY_TIMEOUT = 8.0
STEP_DELAY = 0.5

# NOTE: Routing ACK correlation was dropped: this protobuf's Routing message
# has no request_id field, so admin sends are fire-and-forget with a settle
# delay; the re-query read-back is the real verification.


def _new_packet_id() -> int:
    return random.randint(1, 0xFFFFFFFF)


def build_admin_toradio(
    admin_msg: admin_pb2.AdminMessage, node_num: int, packet_id: int
) -> bytes:
    """Wrap an AdminMessage in a ToRadio packet addressed to the local node."""
    mp = mesh_pb2.MeshPacket()
    mp.id = packet_id
    setattr(mp, "from", node_num)
    mp.to = node_num
    mp.channel = 0
    mp.want_ack = True
    data = mesh_pb2.Data()
    data.portnum = portnums_pb2.ADMIN_APP
    data.payload = admin_msg.SerializeToString()
    data.want_response = True
    mp.decoded.CopyFrom(data)
    tr = mesh_pb2.ToRadio()
    tr.packet.CopyFrom(mp)
    return tr.SerializeToString()


def build_begin_edit(node_num: int, packet_id: int) -> bytes:
    m = admin_pb2.AdminMessage()
    m.begin_edit_settings = True
    return build_admin_toradio(m, node_num, packet_id)


def build_commit_edit(node_num: int, packet_id: int) -> bytes:
    m = admin_pb2.AdminMessage()
    m.commit_edit_settings = True
    return build_admin_toradio(m, node_num, packet_id)


def build_get_mqtt_config(node_num: int, packet_id: int) -> bytes:
    m = admin_pb2.AdminMessage()
    m.get_module_config_request = MQTT_CONFIG_TYPE
    return build_admin_toradio(m, node_num, packet_id)


def build_set_mqtt(
    node_num: int, packet_id: int, current_mqtt, ensure_proxy: bool = True
) -> bytes:
    """Build set_module_config flipping ONLY enabled (+proxy) false->true.

    All other mqtt fields are copied verbatim from the live config.
    With ensure_proxy=False only mqtt.enabled is managed; the proxy flag is
    never touched (used for network-capable nodes that report direct).
    Returns b"" when nothing needs changing.
    """
    need_mqtt = not current_mqtt.enabled
    need_proxy = ensure_proxy and not current_mqtt.proxy_to_client_enabled
    if not need_mqtt and not need_proxy:
        return b""
    m = admin_pb2.AdminMessage()
    sub = module_config_pb2.ModuleConfig()
    sub.mqtt.CopyFrom(current_mqtt)
    if need_mqtt:
        sub.mqtt.enabled = True
    if need_proxy:
        sub.mqtt.proxy_to_client_enabled = True
    m.set_module_config.CopyFrom(sub)
    return build_admin_toradio(m, node_num, packet_id)


def _subscribe_waiter(phys_mgr, predicate):
    """Subscribe first (avoids send/subscribe race); returns awaitable."""
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    async def _cb(from_radio_bytes: bytes) -> None:
        if fut.done():
            return
        try:
            fr = mesh_pb2.FromRadio()
            fr.ParseFromString(from_radio_bytes)
        except Exception:
            return
        try:
            if predicate(fr):
                fut.set_result(fr)
        except Exception:
            pass

    phys_mgr.subscribe(_cb)

    async def _wait(timeout: float):
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None

    return _wait


async def query_mqtt_config(phys_mgr, node_num: int):
    """Read the live moduleConfig.mqtt from the node. Returns proto or None."""
    pid = _new_packet_id()

    def _is_mqtt(fr: mesh_pb2.FromRadio) -> bool:
        return fr.HasField("moduleConfig") and fr.moduleConfig.HasField("mqtt")

    # Subscribe BEFORE sending: answers can arrive immediately.
    wait = _subscribe_waiter(phys_mgr, _is_mqtt)
    await phys_mgr.send_raw_to_radio(build_get_mqtt_config(node_num, pid))
    fr = await wait(QUERY_TIMEOUT)
    if fr is None:
        logger.warning("Provision: timed out waiting for mqtt moduleConfig")
        return None
    return fr.moduleConfig.mqtt


async def ensure_mqtt_proxy(phys_mgr, node_num: int, has_net=None) -> dict:
    """Ensure node MQTT for bridging. Returns outcome dict.

    Profiles: has_net True -> manage mqtt.enabled only, never touch the proxy
    flag (network nodes report direct; forcing proxy would hijack them).
    has_net False/None -> manage both flags (previous behavior).
    Outcome: {"checked", "changed", "verified", "reason", "profile", "snapshot"}.
    Never disables anything; only false->true flips. The re-query read-back
    is the real verification.
    """
    want_proxy = has_net is not True
    profile = "full" if want_proxy else "mqtt-only"
    current = await query_mqtt_config(phys_mgr, node_num)
    if current is None:
        return {"checked": False, "changed": False, "verified": False,
                "reason": "no moduleConfig response from node",
                "profile": profile, "snapshot": None}
    snapshot = _snapshot(current, has_net)
    if current.enabled and (current.proxy_to_client_enabled or not want_proxy):
        logger.info(f"Provision [{profile}]: node MQTT already sufficient, nothing to do")
        return {"checked": True, "changed": False, "verified": True,
                "reason": "already enabled", "profile": profile,
                "snapshot": snapshot}

    logger.info(
        f"Provision [{profile}]: enabling node MQTT "
        f"(enabled={current.enabled} proxy_to_client={current.proxy_to_client_enabled})"
    )
    pid_begin = _new_packet_id()
    await phys_mgr.send_raw_to_radio(build_begin_edit(node_num, pid_begin))
    await asyncio.sleep(STEP_DELAY)

    pid_set = _new_packet_id()
    raw = build_set_mqtt(node_num, pid_set, current, ensure_proxy=want_proxy)
    if not raw:
        return {"checked": True, "changed": False, "verified": True,
                "reason": "already enabled", "profile": profile,
                "snapshot": snapshot}
    await phys_mgr.send_raw_to_radio(raw)
    await asyncio.sleep(STEP_DELAY)

    pid_commit = _new_packet_id()
    await phys_mgr.send_raw_to_radio(build_commit_edit(node_num, pid_commit))
    await asyncio.sleep(STEP_DELAY)

    # The node may briefly stall while saving (commit); retry the read-back.
    for attempt in range(3):
        recheck = await query_mqtt_config(phys_mgr, node_num)
        if recheck is not None and _sufficient(recheck, want_proxy):
            logger.info("Provision: node MQTT enabled and verified")
            return {"checked": True, "changed": True, "verified": True,
                    "reason": "ok", "profile": profile,
                    "snapshot": _snapshot(recheck, has_net)}
        if attempt < 2:
            await asyncio.sleep(STEP_DELAY)
    return {"checked": True, "changed": True, "verified": False,
            "reason": "verify read-back mismatch", "profile": profile,
            "snapshot": snapshot}


def _sufficient(mqtt_proto, want_proxy: bool) -> bool:
    if not mqtt_proto.enabled:
        return False
    return mqtt_proto.proxy_to_client_enabled or not want_proxy


def _snapshot(mqtt_proto, has_net) -> dict:
    return {
        "enabled": bool(mqtt_proto.enabled),
        "proxy": bool(mqtt_proto.proxy_to_client_enabled),
        "has_net": has_net,
        "address": mqtt_proto.address or "",
    }
