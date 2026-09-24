# Multiplextashtic

A Python asyncio multiplexer that allows **multiple Meshtastic-compatible mobile/desktop apps** to connect simultaneously to a **single physical node** running Meshtastic firmware.

> **⚠️ This project has been entirely vibecoded.** Contributions are welcome — feel free to open a pull request with any improvement.

```
App (Client 1) ──┐
App (Client 2) ──┤── TCP:4404 ──► Multiplexer ── serial/TCP:4403 ──► Physical Node
App (Client N) ──┘                (Python asyncio)
```

## Credits

This project is **an inspiration** of [Yeraze's MeshMonitor Virtual Node Server](https://github.com/Yeraze/meshmonitor).

Key differences from Yeraze:
- **No database layer** — everything runs in-memory
- **Configurable IP allowlist** for admin commands instead of a binary flag
- **Raw byte transport** (matches Yeraze's TCP transport, no pubsub library)
- **mDNS service discovery** added for LAN auto-discovery

## Features

- **Multi-client support** — up to `server.max_clients` (default 20) simultaneous TCP clients on port 4404
- **Config replay** — auto-captures physical node state and replays it to new clients (MyNodeInfo, OwnNodeInfo, Metadata, channels, configs, live NodeInfos; 80 synthetic NodeInfos at runtime as fallback before the mesh is heard)
- **Raw byte transport** — connects via raw serial or TCP, reads framed `FromRadio` bytes directly (no pubsub library), matching Yeraze's architecture
- **mDNS service discovery** — announces on the LAN so apps find it automatically
- **Security filtering** — `ADMIN_APP` from client→physical requires localhost, `security.allow_admin_from_ips` (IP/CIDR), or self-addressed `from==to`; everything else forwards
- **Admin command interception** — `removeByNodenum` gets a fake ACK (prevents UI hang), `addContact` blocked from all IPs
- **Self-addressed admin bypass** — `from==to` queries (safe read-only ops) bypass the IP whitelist
- **PKI encryption stripping** — strips PKI encryption from `from=0` packets before forwarding
- **Heartbeat → QueueStatus** — responds to client heartbeats to keep iOS/Android apps alive
- **Client lifecycle management** — inactivity timeout (5 min), cleanup loop (1 min), config request rate limiting (5 s)
- **Bounded message queue** — max 100 messages with 10 ms send delay
- **TCP idle timeout** — detects silent disconnects (no data for 30s), triggers exponential backoff reconnection
- **Serial watchdog** — pokes the link after 45 s silence, reconnects after 90 s (handles USB re-enumeration with no EOF/error)
- **Config re-capture on reconnect** — re-captures config and refreshes all connected clients
- **Node auto-provisioning** — once per boot, enables node MQTT when the MQTT bridge is on (false→true only, verified by read-back; network-capable nodes get `mqtt.enabled` only, others get both flags; never touches address/credentials)
- **MQTT bridge** — proxy-relay uplink plus optional raw gateway (`raw/...`) and standard mirror (`msh/...`), with optional secondary broker (`broker2`); see `docs/dual-mqtt-easy.md` and `docs/dual-mqtt-technical.md`
- **Mock mode** — run without real hardware for development/testing
- **Docker support** — multi-stage build, non-root user, health check, `.dockerignore`

## Requirements

- Python 3.10+ (enforced at startup; `X | Y` type syntax is used throughout)
- A node running Meshtastic firmware connected via USB (serial) or TCP
- `meshtastic>=2.5`
- `protobuf>=4.0`
- `pyyaml>=6.0`
- `pydantic>=2.0`
- `pyserial-asyncio>=0.6`
- `zeroconf>=0.132.0` (for mDNS service discovery)
- `aiomqtt==2.5.1` (MQTT bridge)
- `pycryptodome>=3.20.0` (gateway channel-label decrypt, label-only)

## Installation

```bash
git clone https://github.com/gargomoma/multiplextashtic.git
cd multiplextashtic
python -m venv .venv
# Windows: .venv\Scripts\activate | Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Copy the example config and customize:

```bash
cp configs/config.example.yaml configs/config.yaml
```

### Key configuration sections

```yaml
physical_node:
  connection_type: "serial"        # "serial" or "tcp"
  serial_port: "COM55"             # Windows
  # serial_port: "/dev/ttyUSB0"    # Linux
  baud_rate: 115200

  # TCP settings (used when connection_type = "tcp")
  tcp_host: "192.168.1.100"
  tcp_port: 4403
  tcp_idle_timeout: 30           # seconds without data before assuming TCP disconnect
  serial_poke_timeout: 45        # serial: poke link with want_config after Ns silence
  serial_idle_timeout: 90        # serial: reconnect after Ns silence (USB re-enumeration)
  reconnect:
    enabled: true
    initial_delay: 1               # seconds
    max_delay: 60
    multiplier: 2                  # exponential backoff

server:
  host: "0.0.0.0"
  port: 4404                       # multiplexer listen port (4403 is rejected: physical node default)
  max_clients: 20
  max_payload_size: 10240          # reserved, currently unused; wire cap is 512 bytes (see Protocol)

virtual_node:
  long_name: "Virtual Multiplexer"
  short_name: "VMUX"
  sync_physical_identity: true

security:
  blocked_portnums:
    - "ADMIN_APP"
  allow_admin_from_ips: []         # empty = block all remote admin

discovery:
  enabled: true

mqtt_bridge:
  enabled: false              # enable MQTT proxy-relay to mqtt.meshtastic.org:8883
  broker: "mqtt.meshtastic.org"
  port: 8883
  tls: true
  username: ""                # broker AUTH user (never in client-id)
  password: ""                # plaintext in config.yaml (chmod 600)
  uplink_enabled: true        # always uplink when enabled
  downlink_enabled: false     # allow MQTT->mesh injection when true
  ignore_ok_to_mqtt: false    # true = upload any message ignoring channel flags
  mqtt_username: ""           # client-id: mux-v{ver}-{mqtt_username}-{nodehex}; empty = mux-v{ver}-{nodehex}-{uuid12-per-boot}
  root_topic: "msh"
  raw_root_topic: "raw"       # raw gateway tree root (outside msh/ so apps never see it)
  region: ""                  # explicit region (empty = auto: proxy topics > LoRa enum > EU_868)
  raw_uplink: false           # raw-tree gateway (raw/...) to the primary broker; proxy relay (msh/# verbatim) runs independently of this flag
  raw_scope: "uncovered"      # uncovered = skip proxy-covered packets, all = mirror everything
  standard_mirror: "auto"     # auto = mirror known channels to msh/ iff node/bridge brokers differ; on/off force (stands alone, independent from raw_uplink)
  keepalive: 60
  # broker2:                  # optional secondary broker (dual-MQTT); see docs/dual-mqtt-technical.md
  #   enabled: true
  #   broker: "<secondary-broker>"
  #   port: 1883
  #   tls: false
  #   username: ""
  #   password: ""            # gitignored config only, never logged
  #   keepalive: 60
  #   raw_uplink: false       # raw-tree gateway (raw/...) to this broker
  #   raw_scope: "uncovered"  # uncovered = skip proxy-covered packets, all = mirror everything

  # Gateway mode
  #
  # Each leg decides raw separately (`raw_uplink` on/off + `raw_scope`
  # uncovered/all): every LoRa packet seen on serial is evaluated per leg by
  # `publish_packet` to `raw/{region}/!{senderHex}`
  # (e.g. `raw/EU_868/!<node_hex>`), including channels the node
  # doesn't have configured, and `publish_standard` to `msh/{region}/2/e/{chan}/!{gatewayHex}`
  # (gateway = mux node id, `gateway_id=!<node>`) for known channels when
  # `standard_mirror` is active (independent from raw).
  # The proxy relay path (`msh/#` verbatim from `mqttClientProxyMessage`
  # frames) is independent of both flags. Only LoRa-transport
  # packets are processed (`via_mqtt` and non-LoRa transports are skipped).
  # Forwarded bytes are always the original opaque envelope
  # (`resolve_portnum_label()` exists as a helper but is currently unused).
  # On Windows run
  # with `PYTHONUTF8=1` (aiomqtt path needs the selector event loop -- handled in `main.py`).

logging:
  level: "INFO"                    # DEBUG, INFO, WARNING, ERROR
  file: "logs/multiplexer.log"
  console: true
```

> Renamed keys fail fast at startup: `gateway_enabled` → `raw_uplink` + `standard_mirror`; `raw_mirror_all` → `raw_scope`; `broker2.msh_only` → `broker2.raw_uplink` (inverted); `broker2.send_raw` → `broker2.raw_uplink`; `broker2.client_id` removed (secondary always shares the primary mux-v id).

## Usage

### Quick start (mock mode — no hardware needed)

```bash
python -m src.main --mock
```

### With a real ESP32 connected via serial

```bash
python -m src.main --config configs/config.yaml
```

### With a real ESP32 connected via TCP

Set `connection_type: "tcp"`, `tcp_host`, and `tcp_port` in `configs/config.yaml`, then:

```bash
python -m src.main --config configs/config.yaml
```

### Command-line arguments

| Argument | Description |
|----------|-------------|
| `-c, --config PATH` | Path to YAML config (default: `configs/config.yaml`) |
| `--mock` | Use simulated physical node (no real hardware) |

## Connecting clients

Once the multiplexer is running, point any compatible app to `IP:4404`:

- **Android app:** Settings → Radio Configuration → Network → "TCP Client" → enter IP and port 4404
- **iOS app:** Settings → Connect → TCP → enter IP and port 4404
- **meshtastic CLI:** `meshtastic --host IP --port 4404 --info`
- **meshtastic CLI (mock):** `meshtastic --host 127.0.0.1 --port 4404 --info`

### mDNS auto-discovery

If `discovery.enabled: true` (default), the multiplexer announces itself as `Multiplextashtic._meshtastic._tcp.local.` on the LAN. Compatible mobile apps should discover it automatically without manual IP entry.

> **Note on Windows:** The built-in Windows mDNS responder holds port 5353, which can prevent the Python zeroconf library from responding to mDNS queries. The service registers (visible in logs) but may not appear in network scans. On Linux/Docker this works reliably.

## Docker

```bash
# Build and run
docker compose up -d

# Or build manually
docker build -t multiplextashtic .
docker run -d --name multiplextashtic -p 4404:4404 \
  -v ./configs/config.yaml:/app/configs/config.yaml:ro \
  -v ./logs:/app/logs \
  multiplextashtic
```

> **mDNS on Docker:** `docker-compose.yml` already uses `network_mode: host` so the container can send multicast packets. Bridge mode blocks multicast, so don't switch back to it if you need discovery.

The container runs as a non-root `mux` user with a health check on port 4404.

## Project structure

```
├── src/
│   ├── __init__.py             # version (mux-v client-id prefix)
│   ├── main.py                 # Entry point, logging, wiring, callbacks
│   ├── config.py               # Pydantic config with YAML loading
│   ├── protocol.py             # Frame format: 0x94 0xC3 + 2-byte BE length
│   ├── physical_node.py        # Serial/TCP connection to real node with reconnect
│   ├── mock_physical_node.py   # Simulated node for dev/testing
│   ├── tcp_server.py           # Async TCP server listening on port 4404
│   ├── client_handler.py       # Per-client lifecycle (read/write loops, config replay)
│   ├── message_router.py       # Bidirectional routing with security filtering
│   ├── config_capture.py       # Reads node config state, builds init sequence
│   ├── mdns_service.py         # mDNS service discovery (_meshtastic._tcp)
│   ├── message_queue.py        # Bounded async queue (max 100, 10ms delay)
│   ├── state_cache.py          # In-memory cache of node state
│   ├── mqtt_bridge.py          # MQTT: MqttLeg x2 (shared connection) + MqttBridge orchestrator (aiomqtt)
│   ├── mqtt_crypto.py          # Channel-label decrypt (label-only, never forwarded)
│   └── node_provision.py       # Once-per-boot node MQTT provisioning (mqtt-only vs full profiles)
├── configs/
│   ├── config.yaml             # Your configuration (gitignored, not in repo)
│   └── config.example.yaml     # Reference configuration (incl. mqtt_bridge + broker2)
├── tools/                      # local-only, gitignored, not shipped (e.g. MeshView helper)
├── docs/
│   ├── dual-mqtt-easy.md        # Dual-MQTT plain-language overview
│   ├── dual-mqtt-technical.md   # Dual-MQTT technical reference (fan-out, identity, rollback)
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Testing

```bash
# Smoke-test without hardware
python -m src.main --mock
```

> The `tests/` suite is gitignored (not shipped in the public repo, local dev only). To run it locally: `python -m pytest tests -q` — requires Python 3.10+.

## Security model

| Direction | Portnums blocked | Notes |
|-----------|-----------------|-------|
| Client → Physical | ADMIN_APP (unless whitelisted, see below) | Other `security.blocked_portnums` entries are reserved; only ADMIN_APP is enforced |
| Physical → Client | None | All packets forwarded (only `config_complete_id` filtered to match Yeraze) |

**Admin command exceptions:**
- Localhost (`127.0.0.1`, `::1`) — always allowed
- IP/CIDR allowlist (`security.allow_admin_from_ips`) — supports individual IPs and CIDR subnets (e.g. `"192.168.1.0/24"` for whole LAN)
- Self-addressed queries (`from==to`) — always allowed (safe read-only ops like `--get-config`)
- `removeByNodenum` — intercepted, fake ACK sent back, command dropped
- `addContact` — blocked from ALL IPs including localhost

## Init sequence (PhoneAPI order)

When a client connects, the multiplexer replays the physical node's state. The sequence depends on the `want_config_id` nonce:

- **Full (random nonce):** MyNodeInfo → OwnNodeInfo → Metadata → Channels → Configs → ModuleConfigs → NodeInfos → ConfigComplete
- **69420 (config-only):** Same without NodeInfos (client only needs config refresh)
- **69421 (DB-only):** OwnNodeInfo + NodeInfos + ConfigComplete (client only needs node list refresh)

Message order matches the PhoneAPI.cpp state machine: MyNodeInfo, OwnNodeInfo, Metadata, Channels, Configs, ModuleConfigs, NodeInfos, ConfigComplete.

## Protocol

The wire format matches the Meshtastic-compatible PhoneAPI TCP framing:

- **Start bytes:** `0x94 0xC3`
- **Length:** 2-byte big-endian (max payload: 512 bytes)
- **Payload:** Serialized protobuf
- **Client → Server:** `ToRadio` protobuf
- **Server → Client:** `FromRadio` protobuf

## Documentation

- `configs/config.example.yaml` — annotated reference config (all sections)
- `docs/dual-mqtt-easy.md` — dual-MQTT plain-language overview
- `docs/dual-mqtt-technical.md` — dual-MQTT technical reference (fan-out rules, `broker2`, identity/ban-avoidance, verification, rollback)
- `tools/` (local-only, gitignored, not shipped) — e.g. `meshview.py` helper (MeshView `packets_seen` API via Anubis PoW, stdlib-only)

## Known limitations

- **mDNS on Windows:** Python zeroconf competes with the built-in Windows mDNS responder for port 5353. Works reliably on Linux/Docker.
- **No database layer:** Node state is held in memory (lost on restart). Config is re-captured from device defaults on reconnect.
- **MQTT bridge:** Proxy-relay to `mqtt.meshtastic.org:8883` with `ignore_ok_to_mqtt`, `downlink_enabled`, and `uplink_enabled` toggles; per-leg raw gateway uplink (`raw_uplink`, `raw_scope`) plus standard mirror (`standard_mirror`) and optional secondary broker (`broker2`, see `docs/dual-mqtt-technical.md`). Requires `proxy_to_client_enabled` on the node for the proxy path.
- **Credentials stored plaintext in `configs/config.yaml`** — restrict file permissions (`chmod 600`). Passwords are never logged.
- **On Windows run with `PYTHONUTF8=1`** so the MQTT path keeps the selector event loop (handled in `main.py`).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `RuntimeError: requires Python 3.10+` | old interpreter (e.g. 3.9) | use the Docker image or a 3.10+ venv |
| `Server port 4403 conflicts` | `server.port` set to the physical-node default | use 4404+ (validated in `src/config.py`) |
| `Config file not found` | running from the wrong cwd or missing copy | `cp configs/config.example.yaml configs/config.yaml`, run from repo root |
| No data for 30 s (TCP) / 90 s (serial), reconnecting | dead link or USB re-enumeration | expected watchdog behavior; check cable, `serial_port`, `tcp_host` |
| mDNS registered but invisible (Windows) | built-in responder holds port 5353 | expected; works reliably on Linux/Docker with `network_mode: host` |
| `secondary connection lost (Not authorized)` | broker2 creds/id rejected — ban risk | set `broker2.enabled: false` and restart; see `docs/dual-mqtt-technical.md` §8 |

## License

MIT
