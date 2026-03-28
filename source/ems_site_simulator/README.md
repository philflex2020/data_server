# ems_site_simulator

EMS/Longbow MQTT site simulator with physics-based state machine and WebSocket runtime control.

Publishes realistic battery telemetry on the standard Longbow topic hierarchy at configurable rates,
from a single dinky unit (175 msg/s) up to a full 46-unit site (81,420 msg/s).

---

## Topic format

```
ems/site/{site_id}/unit/{unit_id}/{device}/{instance}/{point_name}/{dtype}
```

Payload: `{"ts":"2024-11-15T21:27:52.775Z","value":3.7}`

Matches the 4 depth variants handled by `real_writer` `topic_patterns`:

| Depth | Pattern | Example |
|---|---|---|
| 6 | `ems/site/+/root/+/+` | `ems/site/0/root/SOC/float` |
| 7 | `ems/site/+/+/+/+/+` | `ems/site/0/meter/meter_1/kW/float` |
| 8 | `ems/site/+/unit/+/root/+/+` | `ems/site/0/unit/0/root/IP_address/string` |
| 9 | `ems/site/+/unit/+/+/+/+/+` | `ems/site/0/unit/0/bms/bms_0/voltage/float` |

---

## Build

```bash
make                        # builds ems_site_simulator
make clean
```

**Dependencies:** `libmosquitto-dev`, `libsimdjson-dev`

```bash
# Ubuntu/Debian
sudo apt install libmosquitto-dev libsimdjson-dev
```

---

## Usage

```bash
./ems_site_simulator [options]
```

| Option | Default | Description |
|---|---|---|
| `--host <host>` | localhost | MQTT broker |
| `--port <port>` | 1883 | MQTT port |
| `--template <path>` | auto-detect | `ems_topic_template.json` |
| `--units <n>` | 4 | synthetic unit count |
| `--unit-id <id>` | — | explicit unit ID (repeatable) |
| `--site-id <id>` | 0 | site identifier |
| `--rate <n>` | 0 | target msg/s (0 = unlimited) |
| `--qos <0\|1>` | 0 | MQTT QoS |
| `--id <str>` | ems-stress | MQTT client base ID |
| `--ws-port <port>` | 8769 | WebSocket control port |
| `--soc <pct>` | 50.0 | initial SOC for all units |
| `--prefix <str>` | — | topic prefix (replaces `ems/site/{site_id}`) |

---

## Quick start

```bash
# Single unit, dinky rate (demo)
./ems_site_simulator --units 1 --rate 175

# Full 46-unit Evelyn site
./ems_site_simulator --units 46 --rate 81420

# 4 slices for gx10 multi-writer setup
./ems_site_simulator --prefix A --units 12 --rate 21240 --ws-port 8769 &
./ems_site_simulator --prefix B --units 12 --rate 21240 --ws-port 8779 &
./ems_site_simulator --prefix C --units 11 --rate 19470 --ws-port 8789 &
./ems_site_simulator --prefix D --units 11 --rate 19470 --ws-port 8799 &
```

---

## WebSocket control

Connect to `ws://host:8769` (or configured `--ws-port`). Send JSON text frames:

```json
{"type":"set_mode",      "units":["ALL"], "mode":"charge", "current_a":200}
{"type":"set_contactor", "units":["ALL"], "closed":true}
{"type":"inject_fault",  "units":["ALL"], "fault":"overtemp", "rack":0, "value":65.0}
{"type":"clear_faults",  "units":["ALL"]}
{"type":"set_noise",     "units":["ALL"], "amplitude":2.5}
{"type":"set_rate",      "rate":81420}
{"type":"get_status"}
```

Faults: `overtemp`, `low_soc`, `undervolt`, `overvolt`, `overcurrent`, `soc_imbalance`

Modes: `standby`, `charge`, `discharge`, `offline`

Server broadcasts a status JSON to all connected clients every 1 s.

---

## UI control panel

Open `http://192.168.86.46:8080/writer/ems_simulator_control.html`

Connect to host + ws-port to control units interactively.

---

## Signal template

`ems_topic_template.json` defines the per-unit signal list (device, instance, point, dtype).
The simulator loads this at startup; if not found in the current directory it auto-detects
from parent directories.

---

## Relationship to stress_runner

`ems_site_simulator` supersedes `stress_real_pub` from `source/stress_runner/`.
The original `stress_real_pub.cpp` is retained there for reference.
