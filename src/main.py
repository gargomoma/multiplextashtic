import argparse
import asyncio
import logging
import logging.handlers
import sys
from pathlib import Path

from src import config as cfg
from meshtastic.protobuf import mesh_pb2, portnums_pb2
from src.state_cache import StateCache
from src.config_capture import ConfigCapture

_PORTNUM_LOOKUP = portnums_pb2._PORTNUM.values_by_number


def setup_logging(log_cfg: cfg.LoggingConfig) -> logging.Logger:
    logger = logging.getLogger("multiplextashtic")
    logger.setLevel(getattr(logging, log_cfg.level))
    # Avoid duplicate handlers across reloads / test invocations.
    for h in list(logger.handlers):
        logger.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if log_cfg.console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.stream = open(
            sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", closefd=False
        )
        logger.addHandler(console_handler)

    if log_cfg.file:
        log_path = Path(log_cfg.file)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                log_path, maxBytes=5 * 1024 * 1024, backupCount=3
            )
        except OSError as e:
            # Never let logging setup kill startup (read-only FS, bad path):
            # fall back to console-only and say so on stderr.
            print(
                f"multiplextashtic: cannot set up log file {log_path}: {e}; "
                "continuing console-only",
                file=sys.stderr,
            )
        else:
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multiplextashtic - Node Multiplexer")
    parser.add_argument(
        "-c", "--config",
        default="configs/config.yaml",
        help="Path to configuration YAML file (default: configs/config.yaml)",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use mock physical node instead of real hardware",
    )
    return parser.parse_args(args)


def print_banner(logger: logging.Logger) -> None:
    from src import __version__
    banner = [
        "",
        "___  ___      _ _   _       _           _            _     _   _",
        r"|  \/  |     | | | (_)     | |         | |          | |   | | (_)",
        r"| .  . |_   _| | |_ _ _ __ | | _____  _| |_ __ _ ___| |__ | |_ _  ___",
        r"| |\/| | | | | | __| | '_ \| |/ _ \ \/ / __/ _` / __| '_ \| __| |/ __|",
        r"| |  | | |_| | | |_| | |_) | |  __/>  <| || (_| \__ \ | | | |_| | (__",
        r"\_|  |_/\__,_|_|\__|_| .__/|_|\___/_/\_\\__\__,_|___/_| |_|\__|_|\___|",
        "                     | |",
        "                     |_|",
        f"  v{__version__}",
        "",
    ]
    for line in banner:
        logger.info(line)


async def main() -> None:
    if sys.version_info < (3, 10):
        raise RuntimeError(f"Multiplextashtic requires Python 3.10+, got {sys.version}")
    args = parse_args()
    app_config = cfg.AppConfig.from_yaml(args.config)
    logger = setup_logging(app_config.logging)

    print_banner(logger)
    from src import __version__
    logger.info(f"Starting Multiplextashtic v{__version__}")
    logger.info(f"Config loaded from: {args.config}")
    logger.info(f"Mock mode: {args.mock}")
    logger.info(f"Server port: {app_config.server.port}")
    logger.info(f"Physical node: {app_config.physical_node.connection_type}")

    if args.mock:
        logger.info("Using mock physical node (no real hardware required)")
        from src.mock_physical_node import MockPhysicalNodeManager
        phys_mgr = MockPhysicalNodeManager()
    else:
        from src.physical_node import PhysicalNodeManager
        phys_mgr = PhysicalNodeManager(app_config.physical_node)

    cache = StateCache()
    phys_mgr.subscribe(lambda pkt: _update_cache(pkt, cache, logger))

    config_capture = ConfigCapture()

    async def _on_physical_reconnect():
        if server is None:
            logger.warning("Reconnect handler: server not ready, skipping refresh")
            return
        logger.info("Physical node reconnected, re-capturing config")
        try:
            ok = await phys_mgr.wait_for_my_info(timeout=15.0)
            if ok and phys_mgr.my_info:
                cache.update_my_info({
                    "my_node_num": phys_mgr.my_info.my_node_num,
                    "reboot_count": phys_mgr.my_info.reboot_count,
                    "min_app_version": phys_mgr.my_info.min_app_version,
                    "device_id": phys_mgr.my_info.device_id,
                    "firmware_edition": phys_mgr.my_info.firmware_edition,
                    "nodedb_count": phys_mgr.my_info.nodedb_count,
                })
            await phys_mgr.wait_for_metadata(timeout=5.0)
            fw_version = phys_mgr.metadata.firmware_version if phys_mgr.metadata else None
            config_capture.capture_defaults(phys_mgr.my_node_num, synthetic_nodes=80, firmware_version=fw_version)
            logger.info(f"Config re-captured: {config_capture.message_count} messages")
            await server.refresh_all_clients()
        except Exception as e:
            logger.error(f"Reconnect handler failed: {e}")

    phys_mgr.set_on_reconnect(_on_physical_reconnect)

    incoming_task = None
    read_loop_task = None
    server = None
    mqtt_bridge = None
    try:
        await phys_mgr.connect()

        if args.mock:
            cache.update_my_info(phys_mgr.my_info or {"my_node_num": phys_mgr.my_node_num})
            config_capture.capture_defaults(phys_mgr.my_node_num)
        else:
            ok = await phys_mgr.wait_for_my_info(timeout=15.0)
            if ok and phys_mgr.my_info:
                cache.update_my_info({
                    "my_node_num": phys_mgr.my_info.my_node_num,
                    "reboot_count": phys_mgr.my_info.reboot_count,
                    "min_app_version": phys_mgr.my_info.min_app_version,
                    "device_id": phys_mgr.my_info.device_id,
                    "firmware_edition": phys_mgr.my_info.firmware_edition,
                    "nodedb_count": phys_mgr.my_info.nodedb_count,
                })
            await phys_mgr.wait_for_metadata(timeout=5.0)
            fw_version = phys_mgr.metadata.firmware_version if phys_mgr.metadata else None
            config_capture.capture_defaults(phys_mgr.my_node_num, synthetic_nodes=80, firmware_version=fw_version)
            logger.info(f"Config capture: {config_capture.message_count} messages (defaults)")

        node_num = phys_mgr.my_node_num or 0x12345678
        node_id = f"!{node_num:08x}"
        short_name = (
            cache.node_info.get("user", {}).get("short_name")
            if cache.node_info else app_config.virtual_node.short_name
        ) or app_config.virtual_node.short_name

        from src.message_router import MessageRouter
        router = MessageRouter(phys_mgr, app_config, mqtt_bridge)

        from src.tcp_server import TCPServer
        server = TCPServer(app_config.server, phys_mgr, app_config, cache, config_capture, mqtt_bridge)
        phys_mgr.subscribe(lambda pkt: _route_broadcast(pkt, server, cache, logger, router))

        incoming_task = asyncio.create_task(phys_mgr.process_incoming())

        # Once per boot: auto-provision node MQTT proxy when bridged (local node only).
        provision_snapshot = None
        if app_config.mqtt_bridge.enabled and not args.mock:
            from src.node_provision import ensure_mqtt_proxy
            try:
                metadata = getattr(phys_mgr, "metadata", None)
                has_net = None
                if metadata is not None:
                    has_net = bool(
                        getattr(metadata, "hasWifi", False)
                        or getattr(metadata, "hasEthernet", False)
                    )
                result = await ensure_mqtt_proxy(phys_mgr, node_num, has_net=has_net)
                logger.info(f"Node provision result: {result}")
                provision_snapshot = result.get("snapshot")
            except Exception as e:
                logger.warning(f"Node provision failed, continuing degraded: {e}")

        if app_config.mqtt_bridge.enabled:
            from src.mqtt_bridge import MqttBridge
            mqtt_bridge = MqttBridge(app_config, node_num=node_num)
            mqtt_bridge.set_cache(cache)
            async def _mqtt_downlink(topic: str, payload: bytes) -> None:
                if not phys_mgr._connected or not phys_mgr.my_info:
                    logger.debug("MqttBridge: downlink deferred until handshake complete")
                    return
                from meshtastic.protobuf import mesh_pb2
                to_radio = mesh_pb2.ToRadio()
                mqtt_msg = mesh_pb2.MqttClientProxyMessage()
                mqtt_msg.topic = topic
                # Raw bytes always go in `data` (typically a serialized
                # ServiceEnvelope). Decoding binary into `text` would corrupt it.
                mqtt_msg.data = payload
                to_radio.mqttClientProxyMessage.CopyFrom(mqtt_msg)
                await phys_mgr.send_raw_to_radio(to_radio.SerializeToString())
                logger.debug(f"MQTT bridge injected downlink to mesh from {topic}")
            await mqtt_bridge.start(downlink_callback=_mqtt_downlink)
            logger.info("MQTT bridge started")
            router.set_mqtt_bridge(mqtt_bridge)
            server.set_mqtt_bridge(mqtt_bridge)
            cache.mqtt_bridge = mqtt_bridge
            if provision_snapshot is not None:
                mqtt_bridge.set_node_mqtt_snapshot(provision_snapshot)
                logger.info(
                    "MQTT coexistence: node reports to "
                    f"{provision_snapshot.get('address') or '(default)'} / "
                    f"bridge reports to {app_config.mqtt_bridge.broker}"
                )

        from src.mdns_service import MDNSService
        mdns = MDNSService(
            app_config,
            short_name=short_name,
            node_id=node_id,
            node_id_provider=lambda: f"!{(phys_mgr.my_node_num or 0):08x}",
            is_connected=lambda: bool(getattr(phys_mgr, "_connected", False)),
        )

        await server.start()

        node_short = (
            cache.node_info.get("user", {}).get("short_name", short_name)
            if cache.node_info else short_name
        )
        mdns.set_short_name(f"V_{node_short}")
        mdns_task = asyncio.create_task(mdns.start())

        shutdown_event = asyncio.Event()
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down...")
        if mqtt_bridge is not None:
            await mqtt_bridge.stop()
        if incoming_task:
            incoming_task.cancel()
        mdns_task_obj = locals().get("mdns_task")
        if mdns_task_obj is not None and not mdns_task_obj.done():
            mdns_task_obj.cancel()
            try:
                await mdns_task_obj
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        mdns_obj = locals().get("mdns")
        if mdns_obj is not None:
            try:
                await mdns_obj.stop()
            except Exception:
                pass
        if server is not None:
            try:
                await server.stop()
            except Exception:
                pass
        await phys_mgr.disconnect()
        logger.info("Shutdown complete")


def _make_node_info_for_cache(num: int, user) -> dict:
    role_val = user.role if hasattr(user, 'role') and user.role else None
    return {
        "num": num,
        "user": {
            "id": user.id or f"!{num:08x}",
            "long_name": user.long_name or "Unknown",
            "short_name": user.short_name or "????",
            "hw_model": user.hw_model,
            "role": role_val,
            "public_key": user.public_key if user.public_key else None,
        },
        "last_heard": 0,
    }


async def _update_cache(from_radio_bytes: bytes, cache: StateCache, logger: logging.Logger) -> None:
    try:
        fr = mesh_pb2.FromRadio()
        fr.ParseFromString(from_radio_bytes)
    except Exception:
        return

    if fr.HasField("node_info"):
        ni = fr.node_info
        num = ni.num
        node_info = _make_node_info_for_cache(num, ni.user)
        node_info["last_heard"] = ni.last_heard
        node_info["snr"] = ni.snr
        node_info["hops_away"] = ni.hops_away
        node_info["via_mqtt"] = ni.via_mqtt
        node_info["is_favorite"] = ni.is_favorite
        node_info["is_ignored"] = ni.is_ignored
        node_info["device_metrics"] = {
            "battery_level": ni.device_metrics.battery_level,
            "voltage": ni.device_metrics.voltage,
            "channel_utilization": ni.device_metrics.channel_utilization,
            "air_util_tx": ni.device_metrics.air_util_tx,
        }
        node_info["position"] = {
            "latitude_i": ni.position.latitude_i,
            "longitude_i": ni.position.longitude_i,
            "altitude": ni.position.altitude,
            "time": ni.position.time,
        }
        cache.update_known_node(node_info)
        if num == cache.my_node_num:
            logger.debug(f"Cache: own NodeInfo captured via from_radio.node_info: {ni.user.long_name}")
            cache.update_node_info(node_info)
        return

    if fr.HasField("channel"):
        ch = fr.channel
        channel_data = {
            "index": ch.index,
            "role": ch.role,
            "settings": {
                "name": ch.settings.name,
                "psk": ch.settings.psk,
                "uplink_enabled": ch.settings.uplink_enabled,
                "downlink_enabled": ch.settings.downlink_enabled,
            },
        }
        cache.update_channel(channel_data)
        bridge = getattr(cache, "mqtt_bridge", None)
        if bridge is not None:
            bridge.mark_channel(ch.index)
        logger.debug(f"Cache: channel {ch.index} captured: {ch.settings.name}")
        return

    if not fr.HasField("packet"):
        return

    pkt = fr.packet
    decoded = pkt.decoded

    if decoded.portnum == portnums_pb2.NODEINFO_APP:
        try:
            user = mesh_pb2.User()
            user.ParseFromString(decoded.payload)
        except Exception:
            return
        if not user.id:
            return
        num = getattr(pkt, "from", 0)
        role_val = user.role if hasattr(user, 'role') and user.role else None
        node_info = {
            "num": num,
            "user": {
                "id": user.id or f"!{num:08x}",
                "long_name": user.long_name or "Unknown",
                "short_name": user.short_name or "????",
                "hw_model": user.hw_model,
                "role": role_val,
                "public_key": user.public_key if user.public_key else None,
            },
            "snr": pkt.rx_snr,
            "hops_away": max(0, 3 - pkt.hop_limit),
            "via_mqtt": pkt.via_mqtt,
            "last_heard": pkt.rx_time,
        }
        cache.update_known_node(node_info)
        if num == cache.my_node_num:
            logger.debug(f"Cache: own NodeInfo captured via NODEINFO_APP: {user.long_name}")
            cache.update_node_info(node_info)


async def _broadcast_callback(from_radio_bytes: bytes, server: "TCPServer", cache: StateCache, logger: logging.Logger) -> None:
    try:
        fr = mesh_pb2.FromRadio()
        fr.ParseFromString(from_radio_bytes)
    except Exception:
        return

    if fr.HasField("packet"):
        pkt = fr.packet
        decoded = pkt.decoded
        portnum = decoded.portnum
        pn_entry = _PORTNUM_LOOKUP.get(portnum)
        portnum_str = pn_entry.name if pn_entry else f"UNKNOWN[{portnum}]"
        logger.debug(
            f"ch={pkt.channel} id={pkt.id} type={portnum_str} "
            f"from={hex(getattr(pkt, 'from', 0))} to={hex(pkt.to)}"
        )
    elif fr.HasField("my_info"):
        logger.debug(f"MyNodeInfo: node_num=0x{fr.my_info.my_node_num:08x}")
    elif fr.HasField("node_info"):
        logger.debug(f"NodeInfo: num=0x{fr.node_info.num:08x}")
    elif fr.HasField("channel"):
        logger.debug(f"Channel: idx={fr.channel.index} name={fr.channel.settings.name}")
    elif fr.config_complete_id != 0:
        logger.debug(f"ConfigComplete: id={fr.config_complete_id}")

    await server.broadcast_from_radio(from_radio_bytes)


async def _route_broadcast(from_radio_bytes: bytes, server: "TCPServer", cache: StateCache, logger: logging.Logger, router: "MessageRouter") -> None:
    result = router.route_from_physical(from_radio_bytes)
    if result is not None:
        await _broadcast_callback(result, server, cache, logger)


if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
