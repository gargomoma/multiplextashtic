# Dual-MQTT Reporting — Technical Reference

The mux (`multiplextashtic`) publishes mesh traffic to **two MQTT brokers
simultaneously** from a single process: a primary (usually local) broker and
a secondary (usually community) broker.

> Notation: `<primary-broker>`, `<secondary-broker>`, `<node-id>` (e.g.
> `!a1b2c3d4`), `<node-ip>` are placeholders for site values, which live only
> in the gitignored local config files. No real hosts, ids, or credentials
> appear in this document.

## 1. Architecture

```
LoRa mesh ◄──► physical node (<node-ip>, <node-id>)
                        │ PhoneAPI/TCP (serial packets + proxy frames)
                        ▼
              ┌─────────────────────┐
              │ mux process         │  python -m src.main --config configs/<site>.yaml
              │  MqttBridge (orchestrator)                              │
              │   primary leg (MqttLeg) ──► <primary-broker>    (mux-style id)
              │   secondary leg (MqttLeg) ─► <secondary-broker> (mux-style id + uuid, §4)
              └─────────────────────┘
Both legs share one connection class (`MqttLeg`: connect/reconnect loop,
publish, per-leg counters); both legs share the derived id (§4) and differ
only in the subscribe flag and topic scope. Fan-out policy lives in
`MqttBridge`.
Node direct leg: may be configured but silent — all observed secondary-broker
traffic is mux-relayed (same topics/ids, gateway_id=<node-id>).
```

## 2. Per-packet fan-out rules (`src/message_router.py`, `src/mqtt_bridge.py`)

| Path | Trigger | Primary | Secondary |
|---|---|---|---|
| Proxy forward (`publish_proxy`) | `mqttClientProxyMessage` frame from node/app (independent of all raw/standard flags) | topic + payload verbatim | same, verbatim |
| Standard mirror (`publish_standard`) | known channel + `standard_mirror_active()` (`auto`: node broker ≠ mux broker; `on` forces; `off` disables) + LoRa transport only | `msh/{region}/2/e/{chan}/!{gateway}` with `gateway_id=!<node>` | same |
| Gateway (`publish_packet`) | per-leg `raw_uplink: true` + per-leg scope (`uncovered` skips proxy-covered ids, `all` takes everything) + LoRa transport only | `raw/{region}/!{sender}` when primary wants it | same rule, evaluated independently |
| Downlink (broker → mesh) | broker message | only when `downlink_enabled: true` (retained msgs + own-gateway echoes dropped) | never built (no subscriptions) |

Shared gates: `via_mqtt` packets and non-LoRa transports (`transport_mechanism`
not in `{1,2,3,4}`) are skipped on both serial-packet paths; proxy relay is
independent of the raw/standard flags. Proxy publishes additionally require
`uplink_enabled: true` and, unless `ignore_ok_to_mqtt: true`, the channel's
`uplink_enabled` flag (`_check_uplink_allowed`). Region resolves as explicit
`region` > observed proxy topic (`observe_proxy_topic`) > `EU_868` default.

Coverage partition: `is_mqtt_covered(ch)` = uplink flag AND (proxy frames active
OR node-direct snapshot). Each leg answers raw for itself (`raw_uplink` on/off
+ `raw_scope` uncovered/all, matched per packet id via `_matches_raw_scope`);
the proxy path runs independently of all raw flags. The router only pre-gates
dispatch (`raw_dispatch_wanted()` = any leg has raw on); the bridge is the
authority per packet.

## 3. Configuration (site config file, models in `src/config.py`)

```yaml
mqtt_bridge:
  broker: "<primary-broker>"  # primary — unchanged behavior
  port: 1883
  tls: false
  username: "<username>"  # broker AUTH user (never in client-id)
  password: "<password>"  # gitignored file only; never logged
  uplink_enabled: true    # kill switch for proxy uplink
  downlink_enabled: false  # uplink-only in effect: primary still subscribes to msh/#/raw/#, inbound msgs are dropped
  ignore_ok_to_mqtt: false # true = publish ignoring channel uplink flags
  mqtt_username: ""       # primary client-id: mux-v{ver}-{mqtt_username}-{nodehex};
                          # empty = mux-v{ver}-{nodehex}-{uuid12-per-boot}
  root_topic: "msh"
  raw_root_topic: "raw"   # raw gateway tree root (outside msh/)
  region: ""              # explicit region (empty = auto: proxy topics > EU_868)
  raw_uplink: false       # raw-tree gateway (raw/...) to the primary broker
  raw_scope: "uncovered"  # uncovered = skip proxy-covered packet ids, all = mirror everything
  standard_mirror: "auto" # auto = mirror known channels iff node/bridge brokers differ;
                          # on forces, off disables (stands alone, no master switch)
  keepalive: 60
  broker2:                 # SecondaryMqttConfig; absent/disabled = single-broker
    enabled: true
    broker: "<secondary-broker>"
    port: 1883             # use whichever port the broker actually listens on;
                          # verify with a TCP probe (a filtered port just times out)
    tls: false
    username: "<username>" # copied from node's live moduleConfig.mqtt
    password: "<password>" # gitignored file only; never logged (tested)
    keepalive: 60          # per-leg keepalive (independent from primary)
    raw_uplink: false     # raw-tree gateway (raw/...) to this broker
    raw_scope: "uncovered" # uncovered = skip proxy-covered packet ids, all = mirror everything
```

Site config files (`configs/config.yaml`, per-site variants) are gitignored —
credentials never enter git.

## 4. Identity

Both legs share one derived id per `MqttBridge` instance
(`_client_id2()` delegates to `_client_id()`; no override exists):

```
with mqtt_username:    mux-v{ver}-{mqtt_username}-{nodehex}   (stable)
without (per boot):    mux-v{ver}-{nodehex}-{uuid12}
e.g. mux-v1.0.0-d86cbf84-55a0ef813976
```

Sharing one id is safe because the legs never point at the same broker
(`brokers_overlap()` guard, below). Per-boot UUID ⇒ no clash with the node
itself or phone apps; `_sanitize_client_id` caps derived ids at 32 chars.
The mux reports as mux everywhere by design — there is deliberately no way
to assume an Android-proxy identity.
* Secondary leg never subscribes ⇒ no broker→mesh injection, no loop surface.
* ⚠️ Ban risk: some community brokers have returned `CONNACK 0x87 Not
  authorized` for computer sessions under a plain mux id. If the secondary
  leg starts flapping with `Not authorized`, disable the leg (§8) — do not
  retry blindly. A stale `broker2.client_id` key in a site config fails fast
  at startup instead of being silently ignored.
* Same-broker guard: `MqttBridge.brokers_overlap()` compares normalized
  `host:port` of both legs (scheme/case/port-suffix agnostic). On a match the
  secondary leg is disabled (`_broker2_active()` false, warning at start), so
  a copy-paste config can never double-publish or fight itself on one broker.

## 5. Firmware single-reporter rule (why "only one MQTT at a time")

Upstream `meshtastic/firmware`, `src/mqtt/MQTT.cpp`, `MQTT::reconnect()`:

```cpp
if (moduleConfig.mqtt.proxy_to_client_enabled) {
    LOG_INFO("MQTT connect via client proxy instead");
    return; // Don't try to connect directly to the server
}
```

`wantsLink() = enabled && (map_report || anyMqttChannel) && (proxy || net)`.
Consequence: with proxy ON the node never dials its broker (mux is sole
reporter); with proxy OFF + live network it should dial direct. If the live
snapshot predicts direct (`enabled, proxy=false, net`) yet the broker feed
stays silent, the root cause is node-side (node network/broker session/
firmware state — invisible from mux logs) and needs node-side diagnostics.

## 6. Code map

* `src/config.py` — `SecondaryMqttConfig`, `MqttBridgeConfig.broker2`.
* `src/mqtt_bridge.py` — `MqttLeg` (one reusable connection: client,
  connected event, reconnect loop, publish + per-leg counters; primary
  subscribes, secondary never does), `MqttBridge` orchestrator (fan-out
  policy, envelopes, dedupe, downlink), shared derived id (`_client_id()`,
  `_client_id2()` delegates), `brokers_overlap()` guard + `_broker2_active()`,
  `_publish_secondary()` (best-effort, drop counters), fan-out calls in
  `publish_proxy` / `publish_standard`, per-leg raw fan-out in
  `publish_packet` (`raw_uplink` + `_matches_raw_scope`, `raw_dispatch_wanted()`
  pre-gate for the router), `resolve_region()` (explicit > observed > `EU_868`),
  LoRa-transport + `via_mqtt` skips, `uplink_enabled` / `ignore_ok_to_mqtt` gates,
  `stats` gains `published2`/`dropped2` (surfaced in the periodic stats line).
  Note: `resolve_portnum_label()` is currently unused (no callers) — the raw
  topic carries no portnum segment; it is reserved for future routing use.
* `src/node_provision.py` — `query_mqtt_config()` reads live
  `moduleConfig.mqtt` (address/user/pass/root); `_snapshot()` keeps boot
  state; provisioning only flips `enabled`/`proxy` false→true, never address
  or credentials. Two profiles: network-capable nodes (`has_net=True`) get
  `mqtt-only` (proxy flag untouched); others get `full` (both flags managed).
* `src/message_router.py` — client/node proxy diversion,
  gateway-vs-proxy partition, mirror dispatch.

## 7. Verification (how "REPORTING ×2" was proven)

1. `MqttBridge stats` line: `published` (→primary) and `published2`
   (→secondary) climbing together, `dropped2` flat, no
   `secondary connection lost`.
2. External proof via MeshView (local-only `tools/meshview.py`, gitignored, not shipped; one shared client):
   fresh `from=<node-id>` packet ids from the mux log return
   `msh/<region>/…/!<node-id>` rows with `rx_snr 0.0 / rx_rssi 0`
   (MQTT-ingress). Negative control `packets_seen(1)` → `seen=[]`.
3. Secondary session line: `secondary connected as mux-v…` (same id as primary by design).

## 8. Failure modes and rollback

| Symptom | Meaning | Action |
|---|---|---|
| `secondary connection lost ([code:135] Not authorized)` repeating | creds/id rejected — ban risk | set `broker2.enabled: false`, restart via the site launcher; do not retry blindly |
| connect/disconnect flap on secondary | client-id clash (node session revived) | same instant rollback; then decide: disable node MQTT (config write, explicit consent) or drop the secondary leg |
| `dropped2` climbing, primary fine | secondary unreachable, local net OK | investigate LAN→WAN path; primary leg unaffected by design |
| `secondary not connected, dropping mirror` at boot | transient (connect races first publishes) | normal; resolves once `secondary connected` logs |

Rollback is one flag flip + restart; primary leg never depends on secondary
state (separate clients, events, counters).

## 9. Tests

`tests/test_mqtt_bridge.py` (local-only suite, gitignored, not shipped): shared-ID rule (secondary == primary,
stable per bridge, fresh per bridge), override/renamed-key rejection,
per-instance uniqueness, proxy fan-out to both, offline secondary drops
without touching primary. `tests/test_mqtt_gateway.py`: per-leg raw
scope matrix (`uncovered`/`all`), `raw_dispatch_wanted()` pre-gate,
secondary-only fan-out end to end, standard-mirror matrix. Suites:
`test_mqtt_bridge/gateway/router/node_provision/config` — all passing.
