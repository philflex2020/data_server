# data_server — system context for Claude

## Machines

| Name | WiFi IP | Wired IP | Role |
|---|---|---|---|
| phil-dev | 192.168.86.46 | — | Dev machine — stress runner, parquet writer, push agent, FlashMQ :1883 |
| lp3 (LattePanda) | 192.168.86.24 | 192.168.0.20 | Edge host — stress runner, writer.cpp, push agent, FlashMQ :1883 |
| fractal-phil | 192.168.86.51 | 192.168.0.51 | AWS-sim — FlashMQ :1884, Telegraf, InfluxDB2, subscriber_api (systemd), manager |
| phil-256g (Supermicro) | 192.168.86.34 | — | Torture test server — 2× Xeon E5-2690 v4 (56T), 256 GB RAM, 1.8 TB, Docker 26 |
| gx10-d94c | 192.168.86.48 | — | EMS real-data test — 20 cores, 119 GB RAM; 4× ems_site_simulator + 4× parquet_writer |

> **Note:** Always use wired IPs (192.168.0.x) for bridge and rsync — higher throughput, avoids WiFi contention at ~80k msgs/sec.
> **Important:** 192.168.0.x is only reachable from lp3. phil-dev (192.168.86.46) is WiFi-only and cannot reach this subnet — bridge/rsync configs using 192.168.0.51 must be run from lp3, not phil-dev.

## Architecture

```
stress_runner (:8769)
      │ MQTT QoS-0
      ▼
FlashMQ host :1883  (phil-dev, systemd)
      │  FlashMQ bridge
      ▼
FlashMQ aws-sim :1884  (fractal-phil)
      │                        │
      │  OLD PATH              │  EDGE PATH (parquet)
      ▼                        ▼
  Telegraf              writer.cpp  →  /srv/data/parquet/
      │                        │  push_agent (:8770) rsync → fractal-phil
      ▼                        ▼
  InfluxDB2 :8086    /srv/data/parquet-aws-sim/  (fractal-phil)
                               │
                               ▼
                       subscriber_api :8767/:8768  (fractal-phil, DuckDB)
```

## What runs where

### phil-dev (192.168.86.46) — start from repo root

```bash
# HTML server (serves html/ dir)
cd html && python3 -m http.server 8080 --bind 0.0.0.0 &

# ptac HTML server (serves /home/phil/work/gen-ai/ptac/html/ — systemd, auto-start)
# sudo systemctl start ptac-html   (already enabled, starts on boot)

# Stress runner
.venv/bin/python source/stress_runner/stress_runner.py --config source/stress_runner/config.yaml > ~/logs/stress_runner.log 2>&1 &

# Parquet writer (C++ binary, pre-built)
cd source/parquet_writer_cpp && ./parquet_writer --config writer_config.yaml > ~/logs/writer-host.log 2>&1 &

# Push agent (must run from its own dir)
cd source/rsync_push && /home/phil/work/gen-ai/data_server/.venv/bin/python push_agent.py > ~/logs/push_agent.log 2>&1 &
```

### gx10-d94c (192.168.86.48) — git repo at ~/work/gen-ai/data_server

**External drive:** `/dev/sda` → `/data` (1.8TB ext4, fstab entry present) — sudo password: `spark1`

**Logs:** `/data/logs/YYYY/MM/DD/` — **Parquet:** `/data/parquet-evelyn/{a,b,c,d}/`

#### Mode 1 — single simulator (baseline / WAL testing)

```bash
bash scripts/gx10-evelyn-start.sh    # 1 sim + 1 writer, 81,420 msg/s, topic: unit/#
bash scripts/gx10-evelyn-stop.sh
curl http://localhost:8771/health
```

#### Mode 2 — 4 simulators + 4 writers (scenario testing)

46 Evelyn units split across 4 prefixed namespaces:

| Slice | Prefix | Units | msg/s | WS port | Health port | Parquet path |
|---|---|---|---|---|---|---|
| A | `A/unit/#` | 0–11  (12) | 21,240 | 8769 | 8771 | `/data/parquet-evelyn/a/` |
| B | `B/unit/#` | 12–23 (12) | 21,240 | 8779 | 8772 | `/data/parquet-evelyn/b/` |
| C | `C/unit/#` | 24–34 (11) | 19,470 | 8789 | 8773 | `/data/parquet-evelyn/c/` |
| D | `D/unit/#` | 35–45 (11) | 19,470 | 8799 | 8774 | `/data/parquet-evelyn/d/` |

```bash
bash scripts/gx10-evelyn-4sim-start.sh   # launch all 4 sim+writer pairs
bash scripts/gx10-evelyn-4sim-stop.sh
```

#### Mode 3 — writers only (real hardware or external publisher)

```bash
bash scripts/gx10-evelyn-writers-only-start.sh   # 4 writers, no simulators
bash scripts/gx10-evelyn-writers-only-stop.sh
```

#### Scenario generator (run from phil-dev or gx10, requires PyYAML)

```bash
# Run against gx10 from phil-dev:
python3 scripts/scenario_generator.py \
    --scenario scripts/scenarios/overtemp_cascade.yaml \
    --host 192.168.86.48

# WebSocket commands: set_mode, set_contactor, inject_fault,
#                     clear_faults, set_noise, set_rate, get_status
# Scenario YAML: scripts/scenarios/overtemp_cascade.yaml (example)
```

#### EMS simulator WebSocket control UI

Open `http://192.168.86.46:8080/writer/ems_simulator_control.html`
— connect to `192.168.86.48` port `8769` (sim A), `8779` (B), `8789` (C), `8799` (D)

#### Build on gx10

```bash
cd source/parquet_writer_cpp && make real_writer
cd source/stress_runner      && make ems_site_simulator
```

### lp3 (192.168.86.20) — git repo at ~/work/gen-ai/data_server

```bash
# Build writer (Arrow/Parquet dev headers now installed)
cd ~/work/gen-ai/data_server/source/parquet_writer_cpp && make parquet_writer

# Start writer
./parquet_writer --config writer_config.yaml > ~/logs/writer-host.log 2>&1 &
```

- Git remote uses HTTPS (no GitHub SSH key on lp3): `https://github.com/philflex2020/data_server.git`
- SSH auth: password `phil` (use sshpass from phil-dev)
- ufw allows: 22, 1883, 8769, 8770, 8771

### fractal-phil (192.168.86.51) — git repo at ~/data_server

```bash
# subscriber_api is under systemd — check/restart with:
sudo systemctl status subscriber-api
sudo systemctl restart subscriber-api

# Full stack (from repo root on fractal-phil)
bash start_aws_sim.sh

# Or via manager
python manager/manager.py --config manager/config.fractal.yaml
```

## Port map

| Port | Component | Machine |
|---|---|---|
| 1883 | FlashMQ host (MQTT) | phil-dev / lp3 |
| 1884 | FlashMQ aws-sim (MQTT) | fractal-phil |
| 8080 | HTML server (data_server) | phil-dev |
| 8090 | HTML server (ptac) | phil-dev |
| 8086 | InfluxDB2 | fractal-phil |
| 8761 | Manager WebSocket | fractal-phil |
| 8765 | Generator WebSocket | phil-dev |
| 8767 | subscriber_api WebSocket | fractal-phil |
| 8768 | subscriber_api flux HTTP | fractal-phil |
| 8769 | stress_runner / sim-A WebSocket | phil-dev / lp3 / gx10 |
| 8770 | push_agent HTTP API | phil-dev / lp3 |
| 8771 | parquet_writer health / writer-A health | phil-dev / lp3 / gx10 |
| 8772 | parquet_writer health (test) / writer-B health | lp3 / gx10 |
| 8773 | writer-C health | gx10 |
| 8774 | writer-D health | gx10 |
| 8779 | sim-B WebSocket | gx10 |
| 8789 | sim-C WebSocket | gx10 |
| 8799 | sim-D WebSocket | gx10 |
| 8096 | InfluxDB compare-baseline (native) | gx10 |
| 8097 | InfluxDB compare-filtered (native) | gx10 |
| 11888 | FlashMQ compare-baseline (native) | gx10 |
| 11889 | FlashMQ compare-filtered (native) | gx10 |

## Key paths

| Path | Machine | Contents |
|---|---|---|
| `/srv/data/parquet/` | phil-dev / lp3 | writer.cpp output (partitioned parquet) |
| `/srv/data/parquet-test/` | lp3 | test writer output (config_test.yaml) |
| `/srv/data/parquet-aws-sim/` | fractal-phil | rsynced copy (DuckDB source) |
| `/data/parquet-evelyn/` | gx10 | single-sim real_writer output (external drive) |
| `/data/parquet-evelyn/{a,b,c,d}/` | gx10 | 4-sim writer outputs, one dir per slice |
| `/data/logs/YYYY/MM/DD/` | gx10 | real_writer + ems_site_simulator logs (dated, external drive) |
| `/etc/flashmq/bridge-conf.d/` | phil-dev / lp3 | FlashMQ bridge config (generated) |

## Python environment

- Single `.venv` at **repo root** — used by all Python components
- Binary: `.venv/bin/python`
- Always run from repo root or pass absolute venv path when cd-ing into a subdir

## Parquet monitor

- URL: `http://192.168.86.46:8080/parquet_monitor.html`
- Host field should be `192.168.86.46`
- Host Parquet Store panel feeds from push_agent `:8770/parquet_stats`
- Writer status is inferred from stress_runner WebSocket mps > 0

## Evelyn cost-comparison stack

Demonstrates Filter A+B savings from `html/writer/evelyn_cost_analysis.html`.
Live results at: `http://192.168.86.46:8080/writer/evelyn_live_compare.html`

Two parallel ingest paths fed from identical EMS generators:

| Path | Broker | Telegraf | InfluxDB | Filtering |
|---|---|---|---|---|
| Baseline | :11888 | all signals | :8096 `ems-baseline` | none — mirrors Longbow today |
| Filtered | :11889 | Filter A+B | :8097 `ems-filtered` | A: drop strings, B: dedup int/bool |

Measured savings: **~27% fewer rows** (matches Conservative scenario ~26.8%).

### Run natively on gx10 (192.168.86.48) — preferred

```bash
# Start all: FlashMQ×2, Telegraf×2, InfluxDB×2, generators
bash scripts/evelyn-compare-native-start.sh               # 1 unit, 175 msg/s (default)
bash scripts/evelyn-compare-native-start.sh --rate 81420 --units 46  # full Evelyn load

# Stop all
bash scripts/evelyn-compare-native-stop.sh
# Wipe data:  rm -rf /data/influx-base/* /data/influx-filt/*
```

Requires on gx10: `/tmp/FlashMQ/build/flashmq`, `/usr/local/bin/telegraf`, `/usr/local/bin/influxd`
(binaries extracted from Docker images — see session notes if they need to be re-extracted)

### Access (native)

- **Live compare UI**: `http://192.168.86.46:8080/writer/evelyn_live_compare.html` (host=.48, ports 8096/8097)
- **InfluxDB baseline**: `http://192.168.86.48:8096`
- **InfluxDB filtered**: `http://192.168.86.48:8097`
- **Token**: `ems-token-secret`  **Org**: `ems-org`

### Run on Docker host (phil-256g .34) — alternative

```bash
# Start Docker stack (FlashMQ×2, Telegraf×2, InfluxDB×2, Grafana)
bash scripts/evelyn-compare-docker-start.sh

# Stop stack (keep volumes)
bash scripts/evelyn-compare-docker-stop.sh
# Wipe data:  cd docker/evelyn-compare && docker compose down -v

# Run generators separately (Docker paths use ports 11883/11884, InfluxDB :8086/:8087)
bash scripts/evelyn-compare-generators.sh --docker-host 192.168.86.34
bash scripts/evelyn-compare-generators.sh --stop
```

### Filter details

| Filter | What | Implementation | Reduction |
|---|---|---|---|
| A | Drop string dtype rows | `tagdrop = {dtype = ["string"]}` on InfluxDB output | ~4% |
| B | Int/bool change-on-value | `[[processors.dedup]] dedup_interval=60s` with tagpass on integer/boolean_integer | ~23% |
| A+B combined | Conservative scenario | Both above | **~26.8%** |
| A+B+float COV | Moderate | + Telegraf dedup on floats | ~40.6% |
| A+B+p95 deadband | Aggressive | + per-signal threshold | ~85.5% |

Config files written by start script to `/tmp/telegraf-native-{base,filt}.conf`

## Diagnostic scripts

```bash
scripts/test/test_host  192.168.86.20              # lp3 health (mqtt/stress/writer/push_agent)
scripts/test/test_aws   192.168.86.51              # fractal-phil health (mqtt_aws/influxdb/subscriber)
scripts/test/config_host 192.168.86.20 -p phil     # show lp3 configs via SSH
scripts/test/config_aws  192.168.86.51             # show fractal-phil configs via SSH
```

Run with python3 directly (not bash). All scripts support `--help` and optional section args.

## Stress runner topology

- Config: `source/stress_runner/config.yaml`
- 2 sites × 12 racks × 8 modules × 52 cells = 9,984 cells
- Mode: `per_cell_item` → ~79,872 msgs/sec
- paho QoS-0 drop guard: skip publish when `_out_packet > 20000`
- Max sustainable rate before drops: ~100k msgs/sec (localhost FlashMQ ceiling)
