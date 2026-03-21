# data_server — system context for Claude

## Machines

| Name | WiFi IP | Wired IP | Role |
|---|---|---|---|
| phil-dev | 192.168.86.46 | — | Dev machine (retired as edge host) |
| lp3 (LattePanda) | 192.168.86.20 | 192.168.0.20 | Host / edge — stress runner, writer.cpp, push agent, FlashMQ :1883 |
| fractal-phil | 192.168.86.51 | 192.168.0.51 | AWS-sim — FlashMQ :1884, Telegraf, InfluxDB2, subscriber_api, manager |

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

# Stress runner
.venv/bin/python source/stress_runner/stress_runner.py --config source/stress_runner/config.yaml > /tmp/stress_runner.log 2>&1 &

# Parquet writer (C++ binary, pre-built)
cd source/parquet_writer_cpp && ./parquet_writer --config config.yaml > /tmp/writer-host.log 2>&1 &

# Push agent (must run from its own dir)
cd source/rsync_push && /home/phil/work/gen-ai/data_server/.venv/bin/python push_agent.py > /tmp/push_agent.log 2>&1 &
```

### fractal-phil (192.168.86.51) — managed by manager or start_aws_sim.sh

```bash
# Full stack (from repo root on fractal-phil)
bash start_aws_sim.sh

# Or via manager
python manager/manager.py --config manager/config.fractal.yaml
```

## Port map

| Port | Component | Machine |
|---|---|---|
| 1883 | FlashMQ host (MQTT) | phil-dev |
| 1884 | FlashMQ aws-sim (MQTT) | fractal-phil |
| 8080 | HTML server | phil-dev |
| 8086 | InfluxDB2 | fractal-phil |
| 8761 | Manager WebSocket | fractal-phil |
| 8765 | Generator WebSocket | phil-dev |
| 8767 | subscriber_api WebSocket | fractal-phil |
| 8768 | subscriber_api flux HTTP | fractal-phil |
| 8769 | stress_runner WebSocket | phil-dev |
| 8770 | push_agent HTTP API | phil-dev |

## Key paths

| Path | Machine | Contents |
|---|---|---|
| `/srv/data/parquet/` | phil-dev | writer.cpp output (partitioned parquet) |
| `/srv/data/parquet-aws-sim/` | fractal-phil | rsynced copy (DuckDB source) |
| `/etc/flashmq/bridge-conf.d/` | phil-dev | FlashMQ bridge config (generated) |

## Python environment

- Single `.venv` at **repo root** — used by all Python components
- Binary: `.venv/bin/python`
- Always run from repo root or pass absolute venv path when cd-ing into a subdir

## Parquet monitor

- URL: `http://192.168.86.46:8080/parquet_monitor.html`
- Host field should be `192.168.86.46`
- Host Parquet Store panel feeds from push_agent `:8770/parquet_stats`
- Writer status is inferred from stress_runner WebSocket mps > 0

## Stress runner topology

- Config: `source/stress_runner/config.yaml`
- 2 sites × 12 racks × 8 modules × 52 cells = 9,984 cells
- Mode: `per_cell_item` → ~79,872 msgs/sec
- paho QoS-0 drop guard: skip publish when `_out_packet > 20000`
- Max sustainable rate before drops: ~100k msgs/sec (localhost FlashMQ ceiling)
