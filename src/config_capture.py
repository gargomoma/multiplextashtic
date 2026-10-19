"""Config capture and replay system.

Reads the physical node's configuration state from the Meshtastic firmware library
interface after connection, builds the init sequence, and replays it to
virtual node clients following the firmware's PhoneAPI state machine order.

Capture order (matches firmware PhoneAPI.cpp):
  1. MyNodeInfo   2. OwnNodeInfo   3. Metadata
  4. Channels (0-7)   5. Configs   6. ModuleConfigs
  7. Other NodeInfos   8. ConfigComplete
"""

import asyncio
import logging

from meshtastic.protobuf import mesh_pb2, config_pb2, channel_pb2, module_config_pb2

from src import config as cfg
from src import __version__
from src.protocol import encode_frame

logger = logging.getLogger("multiplextashtic.capture")

MODEM_PRESET_CHANNEL_NAMES: dict[int, str] = {
    0: "LongFast",
    1: "LongSlow",
    2: "VeryLongSlow",
    3: "MediumSlow",
    4: "MediumFast",
    5: "ShortSlow",
    6: "ShortFast",
    7: "LongModerate",
    8: "ShortTurbo",
}


class ConfigCapture:
    """Captures physical node configuration state from the library interface."""

    def __init__(self):
        self._captured: list[dict] = []
        self._complete = False

    @property
    def is_complete(self) -> bool:
        return self._complete

    @property
    def message_count(self) -> int:
        return len(self._captured)

    def capture_from_interface(self, interface) -> None:
        """Read config state from the library interface after connection."""
        self._captured = []
        self._complete = False

        has_data = False

        # MyNodeInfo
        if interface.myInfo:
            self._add("myInfo", self._capture_my_info(interface.myInfo))
            has_data = True

        # Own NodeInfo
        own = self._capture_own_node_info(interface)
        if own:
            self._add("ownNodeInfo", own)
            has_data = True

        # Metadata - use interface.metadata directly (set during config stream)
        if interface.metadata:
            md = self._capture_metadata_from_proto(interface.metadata)
            self._add("metadata", md)
            has_data = True
        else:
            md = self._capture_metadata(interface)
            self._add("metadata", md)
            has_data = True

        # Channels - use private _localChannels attribute
        channels = getattr(interface, "_localChannels", None)
        if channels:
            for ch in channels:
                serialized = ch.SerializeToString()
                fr = mesh_pb2.FromRadio()
                fr.channel.CopyFrom(ch)
                self._add("channel", fr.SerializeToString())
            has_data = True
            logger.info(f"Captured {len(channels)} channels from physical node")
        else:
            logger.warning("No _localChannels found on interface, using defaults")
            for index in range(8):
                fr = mesh_pb2.FromRadio()
                ch = channel_pb2.Channel()
                ch.index = index
                if index == 0:
                    ch.role = channel_pb2.Channel.Role.Value("PRIMARY")
                    ch.settings.name = MODEM_PRESET_CHANNEL_NAMES.get(4, "MediumFast")
                    ch.settings.psk = b"\x01" + b"\x00" * 31
                    ch.settings.uplink_enabled = True
                    ch.settings.downlink_enabled = True
                else:
                    ch.role = channel_pb2.Channel.Role.Value("DISABLED")
                fr.channel.CopyFrom(ch)
                self._add("channel", fr.SerializeToString())

        # Configs (each sub-config as a separate FromRadio)
        # Access via interface.localNode.localConfig
        local_node = getattr(interface, "localNode", None)
        local_config = getattr(local_node, "localConfig", None) if local_node else None
        if local_config:
            self._add_configs(local_config)
            has_data = True
            logger.info("Captured localConfig from physical node")
        else:
            logger.warning("No localConfig found on interface.localNode, using defaults")
            self._add_default_configs()

        # ModuleConfigs
        module_config = getattr(local_node, "moduleConfig", None) if local_node else None
        if module_config:
            self._add_module_configs(module_config)
            has_data = True
            logger.info("Captured moduleConfig from physical node")
        else:
            logger.warning("No moduleConfig found on interface.localNode, using defaults")
            self._add_default_module_configs()

        if has_data:
            self._complete = True
            logger.info(f"Config capture complete ({len(self._captured)} messages)")
        else:
            logger.warning("Config capture: no data available from interface")

    def _add(self, msg_type: str, data: bytes):
        self._captured.append({"type": msg_type, "data": data})

    def _add_configs(self, local_config):
        """Send each sub-config as a separate FromRadio (matching firmware behavior).
        
        local_config can be either config_pb2.Config or localonly_pb2.LocalConfig.
        Both have the same sub-fields (lora, device, display, etc.)."""
        c = local_config
        pairs = [
            "lora",
            "device",
            "display",
            "power",
            "network",
            "bluetooth",
        ]
        for name in pairs:
            fr = mesh_pb2.FromRadio()
            sub = config_pb2.Config()
            if c.HasField(name):
                # Copy the specific sub-field from local_config to sub
                getattr(sub, name).CopyFrom(getattr(c, name))
            fr.config.CopyFrom(sub)
            self._add(f"config_{name}", fr.SerializeToString())

    def _add_module_configs(self, module_config):
        """Send each sub-module-config as a separate FromRadio.
        
        module_config can be either module_config_pb2.ModuleConfig or localonly_pb2.LocalModuleConfig.
        Both have the same sub-fields (mqtt, telemetry)."""
        mc = module_config
        pairs = ["mqtt", "telemetry"]
        for name in pairs:
            fr = mesh_pb2.FromRadio()
            sub = module_config_pb2.ModuleConfig()
            if mc.HasField(name):
                getattr(sub, name).CopyFrom(getattr(mc, name))
            fr.moduleConfig.CopyFrom(sub)
            self._add(f"moduleConfig_{name}", fr.SerializeToString())

    def _add_default_configs(self):
        defaults = {
            "lora": lambda c: setattr(c.lora, "hop_limit", 3) or setattr(c.lora, "tx_power", 20) or
                              setattr(c.lora, "region", config_pb2.Config.LoRaConfig.RegionCode.Value("UNSET")) or
                              setattr(c.lora, "modem_preset", config_pb2.Config.LoRaConfig.ModemPreset.Value("MEDIUM_FAST")) or
                              setattr(c.lora, "use_preset", True),
            "device": lambda c: setattr(c.device, "role", config_pb2.Config.DeviceConfig.Role.Value("CLIENT")) or
                                setattr(c.device, "serial_enabled", False),
            "display": lambda c: setattr(c.display, "screen_on_secs", 0) or
                                 setattr(c.display, "units", config_pb2.Config.DisplayConfig.DisplayUnits.Value("METRIC")),
            "power": lambda c: None,
            "network": lambda c: setattr(c.network, "wifi_enabled", False) or
                                 setattr(c.network, "eth_enabled", False),
            "bluetooth": lambda c: None,
        }
        for name, setup in defaults.items():
            fr = mesh_pb2.FromRadio()
            c = config_pb2.Config()
            setup(c)
            fr.config.CopyFrom(c)
            self._add(f"config_{name}", fr.SerializeToString())

    def _add_default_module_configs(self):
        fr = mesh_pb2.FromRadio()
        mc = module_config_pb2.ModuleConfig()
        mc.mqtt.enabled = False
        fr.moduleConfig.CopyFrom(mc)
        self._add("moduleConfig_mqtt", fr.SerializeToString())

        fr = mesh_pb2.FromRadio()
        mc = module_config_pb2.ModuleConfig()
        mc.telemetry.device_update_interval = 120
        mc.telemetry.environment_update_interval = 120
        mc.telemetry.air_quality_interval = 120
        fr.moduleConfig.CopyFrom(mc)
        self._add("moduleConfig_telemetry", fr.SerializeToString())

    def _capture_my_info(self, my_info_proto) -> bytes:
        fr = mesh_pb2.FromRadio()
        fr.my_info.CopyFrom(my_info_proto)
        return fr.SerializeToString()

    def _capture_own_node_info(self, interface) -> bytes | None:
        node_num = getattr(interface, "myInfo", None) and interface.myInfo.my_node_num
        if not node_num:
            return None
        own_node = None
        nodes_by_num = getattr(interface, "nodesByNum", {})
        if node_num in nodes_by_num:
            own_node = nodes_by_num[node_num]
        fr = mesh_pb2.FromRadio()
        ni = mesh_pb2.NodeInfo()
        ni.num = node_num
        if own_node:
            ni.user.id = own_node.get("user", {}).get("id", f"!{node_num:08x}")
            ni.user.long_name = own_node.get("user", {}).get("longName", "Virtual Mux")
            ni.user.short_name = own_node.get("user", {}).get("shortName", "VMUX")
            ni.user.hw_model = own_node.get("user", {}).get("hwModel", 0)
            ni.last_heard = own_node.get("lastHeard", 0)
        else:
            ni.user.id = f"!{node_num:08x}"
            ni.user.long_name = "Virtual Multiplexer"
            ni.user.short_name = "VMUX"
            ni.user.hw_model = mesh_pb2.HardwareModel.Value("UNSET")
            ni.last_heard = 0
        fr.node_info.CopyFrom(ni)
        return fr.SerializeToString()

    def _capture_metadata(self, interface=None) -> bytes:
        fr = mesh_pb2.FromRadio()
        md = mesh_pb2.DeviceMetadata()
        if interface and hasattr(interface, "getMyNodeInfo"):
            my_info = interface.getMyNodeInfo()
            if my_info and hasattr(my_info, "firmware_version"):
                md.firmware_version = my_info.firmware_version or "2.6.0-virtual-mux"
            else:
                md.firmware_version = "2.6.0-virtual-mux"
            if my_info and hasattr(my_info, "hw_model"):
                md.hw_model = my_info.hw_model
        else:
            md.firmware_version = "2.6.0-virtual-mux"
        md.device_state_version = 0
        md.canShutdown = False
        md.hasBluetooth = False
        md.hasWifi = False
        md.hasEthernet = False
        md.role = config_pb2.Config.DeviceConfig.Role.Value("CLIENT")
        md.position_flags = 0
        fr.metadata.CopyFrom(md)
        return fr.SerializeToString()

    def _capture_metadata_from_proto(self, metadata_proto) -> bytes:
        """Capture metadata directly from a DeviceMetadata protobuf."""
        fr = mesh_pb2.FromRadio()
        md = mesh_pb2.DeviceMetadata()
        md.CopyFrom(metadata_proto)
        fr.metadata.CopyFrom(md)
        return fr.SerializeToString()

    def get_config_messages(self) -> list[dict]:
        return list(self._captured)

    def capture_defaults(self, node_num: int, synthetic_nodes: int = 200, firmware_version: str | None = None) -> None:
        """Generate default config with synthetic nodes mimicking a real mesh.

        Args:
            node_num: Virtual node number
            synthetic_nodes: Number of fake NodeInfo entries to generate
                             (helps match the ~240 messages Yeraze produces).
            firmware_version: Real firmware version from physical node, or None for fallback.
        """
        self._captured = []
        self._complete = False

        mi_fr = mesh_pb2.FromRadio()
        mi = mesh_pb2.MyNodeInfo()
        mi.my_node_num = node_num
        mi.reboot_count = 0
        mi.min_app_version = 20200
        mi.device_id = f"virtual-{node_num:08x}".encode("utf-8")
        mi.firmware_edition = mesh_pb2.FirmwareEdition.Value("VANILLA")
        mi.nodedb_count = synthetic_nodes
        mi_fr.my_info.CopyFrom(mi)
        self._add("myInfo", mi_fr.SerializeToString())

        ni_fr = mesh_pb2.FromRadio()
        ni = mesh_pb2.NodeInfo()
        ni.num = node_num
        ni.user.id = f"!{node_num:08x}"
        ni.user.long_name = "Virtual Multiplexer"
        ni.user.short_name = "VMUX"
        ni.user.hw_model = mesh_pb2.HardwareModel.Value("UNSET")
        ni.last_heard = 0
        ni.snr = 0.0
        ni.hops_away = 0
        ni.via_mqtt = False
        ni.is_favorite = False
        ni.is_ignored = False
        ni.device_metrics.battery_level = 100
        ni.device_metrics.voltage = 4.2
        ni.device_metrics.channel_utilization = 0.0
        ni.device_metrics.air_util_tx = 0.0
        ni.position.latitude_i = 0
        ni.position.longitude_i = 0
        ni.position.altitude = 0
        ni_fr.node_info.CopyFrom(ni)
        self._add("ownNodeInfo", ni_fr.SerializeToString())

        md_fr = mesh_pb2.FromRadio()
        md = mesh_pb2.DeviceMetadata()
        md.firmware_version = firmware_version or "2.6.0-virtual-mux"
        md.device_state_version = 0
        md.canShutdown = False
        md.hasBluetooth = False
        md.hasWifi = False
        md.hasEthernet = False
        md.role = config_pb2.Config.DeviceConfig.Role.Value("CLIENT")
        md.position_flags = 0
        md_fr.metadata.CopyFrom(md)
        self._add("metadata", md_fr.SerializeToString())

        for index in range(8):
            ch_fr = mesh_pb2.FromRadio()
            ch = channel_pb2.Channel()
            ch.index = index
            if index == 0:
                ch.role = channel_pb2.Channel.Role.Value("PRIMARY")
                ch.settings.name = MODEM_PRESET_CHANNEL_NAMES.get(4, "MediumFast")
                ch.settings.psk = b"\x01" + b"\x00" * 31
                ch.settings.uplink_enabled = True
                ch.settings.downlink_enabled = True
            else:
                ch.role = channel_pb2.Channel.Role.Value("DISABLED")
            ch_fr.channel.CopyFrom(ch)
            self._add("channel", ch_fr.SerializeToString())

        self._add_default_configs()
        self._add_default_module_configs()

        # Synthetic NodeInfos to match Yeraze ~240 message count
        names = [
            ("Node Alpha", "ALPH"), ("Node Beta", "BETA"), ("Node Gamma", "GMMA"),
            ("Node Delta", "DLTA"), ("Node Echo", "ECHO"), ("Node Foxtrot", "FOX"),
            ("Node Golf", "GOLF"), ("Node Hotel", "HTL"), ("Node India", "IND"),
            ("Node Juliet", "JLT"), ("Node Kilo", "KILO"), ("Node Lima", "LIMA"),
            ("Node Mike", "MIKE"), ("Node November", "NOV"), ("Node Oscar", "OSCR"),
            ("Node Papa", "PAPA"), ("Node Quebec", "QUE"), ("Node Romeo", "ROM"),
            ("Node Sierra", "SIR"), ("Node Tango", "TNG"), ("Node Uniform", "UNI"),
            ("Node Victor", "VIC"), ("Node Whiskey", "WHIS"), ("Node Xray", "XRAY"),
            ("Node Yankee", "YANK"), ("Node Zulu", "ZULU"),
        ]
        for i in range(synthetic_nodes):
            seed = 0xABCD0000 + i
            name, short = names[i % len(names)]
            if i >= len(names):
                name = f"Node {seed:04x}"
                short = f"N{i:04d}"[:4]
            sn_fr = mesh_pb2.FromRadio()
            sni = mesh_pb2.NodeInfo()
            sni.num = seed
            sni.user.id = f"!{seed:08x}"
            sni.user.long_name = name
            sni.user.short_name = short
            sni.user.hw_model = i % 10
            sni.last_heard = i * 3600
            sni.snr = float(i % 20 - 10)
            sni.hops_away = i % 4
            sni.via_mqtt = False
            sni.is_favorite = False
            sni.is_ignored = False
            sni.device_metrics.battery_level = 50 + (i % 50)
            sni.device_metrics.voltage = 3.7 + (i % 10) * 0.1
            sni.device_metrics.channel_utilization = float(i % 100) / 100.0
            sni.device_metrics.air_util_tx = float(i % 50) / 100.0
            sni.position.latitude_i = 373000000 + (i * 1000)
            sni.position.longitude_i = -1220000000 - (i * 1000)
            sni.position.altitude = 10 + (i % 100)
            sn_fr.node_info.CopyFrom(sni)
            self._add("nodeInfo", sn_fr.SerializeToString())

        self._complete = True
        logger.info(f"Config defaults captured ({len(self._captured)} messages, {synthetic_nodes} synthetic nodes)")


class ConfigReplayBuilder:
    """Replays captured config to a VN client, rebuilding dynamic data from state_cache."""

    def __init__(self, capture: ConfigCapture, phys_mgr, app_config: cfg.AppConfig, state_cache):
        self._capture = capture
        self._phys_mgr = phys_mgr
        self._app_config = app_config
        self._state_cache = state_cache

    async def replay(self, writer: asyncio.StreamWriter, config_id: int) -> int:
        vn = self._app_config.virtual_node
        node_num = self._phys_mgr.my_node_num or 0x12345678
        next_id = 1
        sent = 0
        sync_identity = self._app_config.virtual_node.sync_physical_identity

        NONCE_ONLY_CONFIG = 69420
        NONCE_ONLY_DB = 69421
        is_db_only = config_id == NONCE_ONLY_DB
        is_config_only = config_id == NONCE_ONLY_CONFIG

        def alloc_id():
            nonlocal next_id
            i = next_id
            next_id += 1
            return i

        async def send_fr(fr: mesh_pb2.FromRadio):
            nonlocal sent
            fr.id = alloc_id()
            data = fr.SerializeToString()
            frame = encode_frame(data)
            writer.write(frame)
            await writer.drain()
            sent += 1

        async def send_captured(data: bytes):
            nonlocal sent
            try:
                fr = mesh_pb2.FromRadio()
                fr.ParseFromString(data)
                fr.id = alloc_id()
                data2 = fr.SerializeToString()
                frame = encode_frame(data2)
                writer.write(frame)
                await writer.drain()
                sent += 1
            except Exception as e:
                logger.warning(f"ConfigReplay: failed to send captured message: {e}")

        async def send_own_node_info():
            fr = mesh_pb2.FromRadio()
            ni = mesh_pb2.NodeInfo()
            ni.num = node_num
            if sync_identity and self._state_cache and self._state_cache.node_info:
                cached = self._state_cache.node_info
                ni.user.id = cached.get("user", {}).get("id", f"!{node_num:08x}")
                ni.user.long_name = cached.get("user", {}).get("long_name", vn.long_name)
                ni.user.short_name = cached.get("user", {}).get("short_name", vn.short_name)
                ni.user.hw_model = cached.get("user", {}).get("hw_model", mesh_pb2.HardwareModel.Value("UNSET"))
                role_val = cached.get("user", {}).get("role")
                if role_val is not None:
                    ni.user.role = role_val
                pk = cached.get("user", {}).get("public_key")
                if pk:
                    ni.user.public_key = pk
                ni.last_heard = cached.get("last_heard", 0)
                ni.snr = cached.get("snr", 0.0)
                ni.hops_away = cached.get("hops_away", 0)
                ni.via_mqtt = cached.get("via_mqtt", False)
                ni.is_favorite = cached.get("is_favorite", False)
                ni.is_ignored = cached.get("is_ignored", False)
                ni.device_metrics.battery_level = cached.get("device_metrics", {}).get("battery_level", 100)
                ni.device_metrics.voltage = cached.get("device_metrics", {}).get("voltage", 4.2)
                ni.device_metrics.channel_utilization = cached.get("device_metrics", {}).get("channel_utilization", 0.0)
                ni.device_metrics.air_util_tx = cached.get("device_metrics", {}).get("air_util_tx", 0.0)
                pos = cached.get("position", {})
                ni.position.latitude_i = pos.get("latitude_i", 0)
                ni.position.longitude_i = pos.get("longitude_i", 0)
                ni.position.altitude = pos.get("altitude", 0)
                ni.position.time = pos.get("time", 0)
            else:
                ni.user.id = f"!{node_num:08x}"
                ni.user.long_name = vn.long_name
                ni.user.short_name = vn.short_name
                ni.user.hw_model = mesh_pb2.HardwareModel.Value("UNSET")
                ni.last_heard = 0
                ni.snr = 0.0
                ni.hops_away = 0
                ni.via_mqtt = False
                ni.is_favorite = False
                ni.is_ignored = False
                ni.device_metrics.battery_level = 100
                ni.device_metrics.voltage = 4.2
                ni.device_metrics.channel_utilization = 0.0
                ni.device_metrics.air_util_tx = 0.0
                ni.position.latitude_i = 0
                ni.position.longitude_i = 0
                ni.position.altitude = 0
            fr.node_info.CopyFrom(ni)
            await send_fr(fr)

        async def send_my_node_info():
            fr = mesh_pb2.FromRadio()
            mi = mesh_pb2.MyNodeInfo()
            if sync_identity and self._state_cache and self._state_cache.my_info:
                cached = self._state_cache.my_info
                mi.my_node_num = cached.get("my_node_num", node_num)
                mi.reboot_count = cached.get("reboot_count", 0)
                mi.min_app_version = cached.get("min_app_version", 20200)
                mi.device_id = cached.get("device_id", f"virtual-{node_num:08x}".encode("utf-8"))
                mi.firmware_edition = cached.get("firmware_edition", mesh_pb2.FirmwareEdition.Value("VANILLA"))
                mi.nodedb_count = cached.get("nodedb_count", 0)
            else:
                mi.my_node_num = node_num
                mi.reboot_count = 0
                mi.min_app_version = 20200
                mi.device_id = f"virtual-{node_num:08x}".encode("utf-8")
                mi.firmware_edition = mesh_pb2.FirmwareEdition.Value("VANILLA")
                mi.nodedb_count = 0
            fr.my_info.CopyFrom(mi)
            await send_fr(fr)

        async def send_metadata():
            nonlocal sent
            metadata_entry = None
            for entry in self._capture.get_config_messages():
                if entry["type"] == "metadata":
                    metadata_entry = entry
                    break
            real_metadata = getattr(self._phys_mgr, "metadata", None)
            if real_metadata is not None or metadata_entry:
                fr = mesh_pb2.FromRadio()
                if real_metadata is not None:
                    fr.metadata.CopyFrom(real_metadata)
                else:
                    fr.ParseFromString(metadata_entry["data"])
                if fr.metadata.device_state_version < 0:
                    fr.metadata.device_state_version = 0
                if fr.metadata.position_flags < 0:
                    fr.metadata.position_flags = 0
                if fr.metadata.firmware_version:
                    fr.metadata.firmware_version = f"{fr.metadata.firmware_version}-Mx{__version__}"
                fr.id = alloc_id()
                data = fr.SerializeToString()
                frame = encode_frame(data)
                writer.write(frame)
                await writer.drain()
                sent += 1
            else:
                fr = mesh_pb2.FromRadio()
                md = mesh_pb2.DeviceMetadata()
                md.firmware_version = f"2.6.0-virtual-mux-Mx{__version__}"
                md.device_state_version = 0
                md.canShutdown = False
                md.hasBluetooth = False
                md.hasWifi = False
                md.hasEthernet = False
                md.role = config_pb2.Config.DeviceConfig.Role.Value("CLIENT")
                md.position_flags = 0
                fr.metadata.CopyFrom(md)
                await send_fr(fr)

        async def send_channels():
            channels_sent = 0
            if self._state_cache and self._state_cache.channels:
                for ch_data in self._state_cache.channels:
                    fr = mesh_pb2.FromRadio()
                    ch = channel_pb2.Channel()
                    ch.index = ch_data.get("index", 0)
                    ch.role = ch_data.get("role", 0)
                    settings = ch_data.get("settings", {})
                    ch.settings.name = settings.get("name", "")
                    ch.settings.psk = settings.get("psk", b"")
                    ch.settings.uplink_enabled = settings.get("uplink_enabled", False)
                    ch.settings.downlink_enabled = settings.get("downlink_enabled", False)
                    fr.channel.CopyFrom(ch)
                    await send_fr(fr)
                    channels_sent += 1
            if channels_sent == 0:
                for entry in self._capture.get_config_messages():
                    if entry["type"] == "channel":
                        await send_captured(entry["data"])
                        channels_sent += 1
            logger.info(f"ConfigReplay: sent {channels_sent} channels")

        async def send_configs_and_module_configs():
            # Prefer the node's real config dump so settings screens show actual values
            real_frames = list(getattr(self._phys_mgr, "config_frames", {}).values())
            real_module_frames = list(getattr(self._phys_mgr, "module_config_frames", {}).values())
            if real_frames:
                for data in real_frames + real_module_frames:
                    await send_captured(data)
                logger.info(f"ConfigReplay: sent {len(real_frames)} configs and "
                            f"{len(real_module_frames)} module configs from physical node")
                return
            skip_types = {"myInfo", "ownNodeInfo", "metadata", "configComplete", "queueStatus", "fromRadio", "nodeInfo", "channel"}
            for entry in self._capture.get_config_messages():
                etype = entry["type"]
                if etype in skip_types:
                    continue
                await send_captured(entry["data"])
            logger.info(f"ConfigReplay: sent configs and module configs from capture")

        async def send_known_nodes():
            known_node_count = 0
            if self._state_cache and self._state_cache.known_nodes:
                for num, node in sorted(self._state_cache.known_nodes.items()):
                    if num == 0xFFFFFFFF:
                        continue
                    fr = mesh_pb2.FromRadio()
                    ni = mesh_pb2.NodeInfo()
                    ni.num = num
                    ni.user.id = node.get("user", {}).get("id", f"!{num:08x}")
                    ni.user.long_name = node.get("user", {}).get("long_name", "Unknown")
                    ni.user.short_name = node.get("user", {}).get("short_name", "????")
                    ni.user.hw_model = node.get("user", {}).get("hw_model", 0)
                    role_val = node.get("user", {}).get("role")
                    if role_val is not None:
                        ni.user.role = role_val
                    pk = node.get("user", {}).get("public_key")
                    if pk:
                        ni.user.public_key = pk
                    ni.last_heard = node.get("last_heard", 0)
                    ni.snr = node.get("snr", 0.0)
                    ni.hops_away = node.get("hops_away", 0)
                    ni.via_mqtt = node.get("via_mqtt", False)
                    ni.is_favorite = node.get("is_favorite", False)
                    ni.is_ignored = node.get("is_ignored", False)
                    ni.device_metrics.battery_level = node.get("device_metrics", {}).get("battery_level", 0)
                    ni.device_metrics.voltage = node.get("device_metrics", {}).get("voltage", 0.0)
                    ni.device_metrics.channel_utilization = node.get("device_metrics", {}).get("channel_utilization", 0.0)
                    ni.device_metrics.air_util_tx = node.get("device_metrics", {}).get("air_util_tx", 0.0)
                    pos = node.get("position", {})
                    ni.position.latitude_i = pos.get("latitude_i", 0)
                    ni.position.longitude_i = pos.get("longitude_i", 0)
                    ni.position.altitude = pos.get("altitude", 0)
                    ni.position.time = pos.get("time", 0)
                    fr.node_info.CopyFrom(ni)
                    await send_fr(fr)
                    known_node_count += 1
            if known_node_count == 0:
                for entry in self._capture.get_config_messages():
                    if entry["type"] == "nodeInfo":
                        await send_captured(entry["data"])
                        known_node_count += 1
                logger.info(f"ConfigReplay: sent {known_node_count} synthetic nodes from capture (fallback)")

        async def send_config_complete():
            fr = mesh_pb2.FromRadio()
            fr.config_complete_id = config_id
            await send_fr(fr)

        mode = "db_only" if is_db_only else "config_only" if is_config_only else "full"
        logger.info(f"ConfigReplay: start (node_num=0x{node_num:08x}, config_id={config_id}, "
                    f"sync_identity={sync_identity}, mode={mode})")

        if is_db_only:
            await send_own_node_info()
            await send_known_nodes()
            await send_config_complete()
            logger.info(f"ConfigReplay: DB-only done ({sent} messages, config_id={config_id})")
            return sent

        await send_my_node_info()
        await send_own_node_info()
        await send_metadata()
        await send_channels()
        await send_configs_and_module_configs()

        if not is_config_only:
            await send_known_nodes()

        await send_config_complete()

        logger.info(f"ConfigReplay: done ({sent} messages, config_id={config_id}, mode={mode})")
        return sent
