import yaml
from pathlib import Path
from typing import Literal, Optional, List
from pydantic import BaseModel, Field, field_validator, model_validator


class ReconnectConfig(BaseModel):
    enabled: bool = True
    initial_delay: int = 1
    max_delay: int = 60
    multiplier: int = 2


class PhysicalNodeConfig(BaseModel):
    connection_type: Literal["serial", "tcp"] = "serial"
    serial_port: str = "/dev/ttyUSB0"
    tcp_host: str = "192.168.1.100"
    tcp_port: int = 4403
    baud_rate: int = 115200
    tcp_idle_timeout: int = 30
    # Serial idle watchdog: a USB CDC re-enumeration leaves the old fd silent
    # with no EOF/error, so without these the serial read loop hangs forever.
    # poke: after this many seconds of silence, actively send want_config.
    # idle: after this many seconds of silence, declare the link dead and
    # reconnect (re-opens the port fresh). Kept above the ~60s telemetry period
    # to avoid false positives during quiet mesh periods.
    serial_poke_timeout: int = 45
    serial_idle_timeout: int = 90
    reconnect: ReconnectConfig = ReconnectConfig()


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 4404
    max_clients: int = 20
    max_payload_size: int = 10240


class VirtualNodeConfig(BaseModel):
    long_name: str = "Virtual Multiplexer"
    short_name: str = "VMUX"
    sync_physical_identity: bool = True


class SecurityConfig(BaseModel):
    blocked_portnums: List[str] = Field(default_factory=lambda: ["ADMIN_APP"])
    allow_admin_from_ips: List[str] = Field(default_factory=list)


class DiscoveryConfig(BaseModel):
    enabled: bool = True


class SecondaryMqttConfig(BaseModel):
    enabled: bool = True
    broker: str = ""
    port: int = 8883
    tls: bool = True
    username: str = ""
    password: str = ""
    keepalive: int = 60
    raw_uplink: bool = False  # raw-tree gateway packets go here when true
    raw_scope: Literal["uncovered", "all"] = "uncovered"  # all = include proxy-covered packets too

    @model_validator(mode="before")
    @classmethod
    def _reject_renamed_keys(cls, data):
        if isinstance(data, dict) and "msh_only" in data:
            raise ValueError("broker2.msh_only was replaced by broker2.raw_uplink (msh_only=true means raw_uplink=false)")
        if isinstance(data, dict) and "send_raw" in data:
            raise ValueError("broker2.send_raw was renamed to broker2.raw_uplink (same meaning)")
        if isinstance(data, dict) and "client_id" in data:
            raise ValueError("broker2.client_id was removed: the secondary leg always shares the primary mux-v id")
        return data


class MqttBridgeConfig(BaseModel):
    enabled: bool = False
    broker: str = "mqtt.meshtastic.org"
    port: int = 8883
    tls: bool = True
    username: str = ""
    password: str = ""
    uplink_enabled: bool = True
    downlink_enabled: bool = False
    ignore_ok_to_mqtt: bool = False
    mqtt_username: str = ""
    root_topic: str = "msh"
    keepalive: int = 60
    raw_uplink: bool = False  # raw-tree gateway packets go to the primary broker when true
    raw_scope: Literal["uncovered", "all"] = "uncovered"  # all = include proxy-covered packets too
    raw_root_topic: str = "raw"
    region: str = ""
    standard_mirror: Literal["auto", "on", "off"] = "auto"
    broker2: Optional[SecondaryMqttConfig] = None

    @model_validator(mode="before")
    @classmethod
    def _reject_renamed_keys(cls, data):
        if isinstance(data, dict) and "gateway_enabled" in data:
            raise ValueError("gateway_enabled was replaced by raw_uplink (raw to primary) + standard_mirror (msh mirror stands alone)")
        if isinstance(data, dict) and "raw_mirror_all" in data:
            raise ValueError("raw_mirror_all was replaced by raw_scope (uncovered|all, per leg)")
        return data


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    file: Optional[str] = "logs/multiplexer.log"
    console: bool = True


class AppConfig(BaseModel):
    physical_node: PhysicalNodeConfig = PhysicalNodeConfig()
    server: ServerConfig = ServerConfig()
    virtual_node: VirtualNodeConfig = VirtualNodeConfig()
    security: SecurityConfig = SecurityConfig()
    discovery: DiscoveryConfig = DiscoveryConfig()
    mqtt_bridge: MqttBridgeConfig = MqttBridgeConfig()
    logging: LoggingConfig = LoggingConfig()

    @classmethod
    def from_yaml(cls, path: str = "config.yaml") -> "AppConfig":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if data is None:
            raise ValueError(f"Config file is empty: {path}")
        return cls(**data)

    @field_validator("server")
    @classmethod
    def validate_server_port(cls, v: ServerConfig) -> ServerConfig:
        if v.port == 4403:
            raise ValueError("Server port 4403 conflicts with physical node default. Use 4404 or higher.")
        return v
