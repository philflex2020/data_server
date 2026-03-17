# Edge Parquet Architecture

## Overview

Move parquet file generation to the site controller (edge) rather than the cloud.
Each site runs `writer.cpp` locally, generates compressed parquet files, and rsyncs
them directly to S3. AWS becomes a thin query layer.

> **Implementation update:** the original design used NATS JetStream + bridge.py +
> writer.py.  These have been replaced by `writer.cpp` (C++17, Arrow, simdjson) which
> subscribes directly to FlashMQ and writes parquet locally.  The NATS Leaf Node
> forwarding role is now handled by the FlashMQ bridge.  Cost figures and resilience
> analysis below remain valid — the implementation is simpler (fewer processes).

---

## Architecture Diagram

```
SITE CONTROLLER (on-premise / edge)
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  FlashMQ :1883 (MQTT broker, QoS-1, persistent session) │
│       │                          │                      │
│       ▼                          │ FlashMQ bridge       │
│  writer.cpp                      │ (real-time forward)  │
│  (C++17 · Arrow · simdjson)      │                      │
│       │                          │                      │
│       ▼                          ▼                      │
│  /srv/data/parquet/          AWS FlashMQ                │
│  rsync → S3 (every 15 min)                              │
│                                                         │
└─────────────────────────────────────────────────────────┘
                                   │
                         ┌─────────┘
                         │ FlashMQ bridge (TLS, single TCP conn)
                         │ selected topic groups forwarded live
                         ▼
AWS
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  FlashMQ (AWS) :1883                                    │
│       │                                                 │
│       ▼                                                 │
│  writer.cpp (AWS, current_state mode)                   │
│       │                                                 │
│       ▼                                                 │
│  current_state.parquet  (local · no rsync lag)          │
│                                                         │
│  S3  ◄── rsync from site every 15 min                   │
│   │                                                     │
│   └──► DuckDB (S3 history + current_state.parquet)      │
│                          │                              │
│                          ▼                              │
│  Subscriber API / Monitor / Viewer (browser)            │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## How It Works

### Edge (site controller)

Each site runs the writer.cpp pipeline locally:

- **FlashMQ :1883** — receives MQTT from battery/solar/wind/grid devices (QoS-1,
  persistent session — queues messages during writer downtime)
- **writer.cpp** — subscribes to FlashMQ, parses JSON, buffers per (source, site),
  flushes to local parquet every 15 min; also writes `current_state.parquet` if configured
- **FlashMQ bridge** — forwards selected topic groups to AWS FlashMQ over a single
  outbound TLS connection (replaces NATS Leaf Node)
- **rsync cron** — rsyncs `/srv/data/parquet/` to S3 every 15 min (can be extended
  further once AWS writer is live and `current_state.parquet` covers the gap)

The site controller continues operating fully offline if the WAN link drops.
Parquet files accumulate locally and rsync when connectivity resumes — no data loss.

### Cloud (AWS)

- **FlashMQ (AWS) :1883** — receives bridge from site FlashMQ
- **writer.cpp (AWS, current_state mode)** — subscribes to AWS FlashMQ, writes
  `current_state.parquet` locally (one row per sensor, always latest reading).
  Covers the gap between S3 rsync cycles with no rsync lag of its own
- **S3** — receives rsynced parquet from the site every 15 min
- **DuckDB** — queries S3 (historical) + local `current_state.parquet` (current state)
- **Subscriber API** — serves history and current-state queries

---

## FlashMQ Bridge (replaces NATS Leaf Node)

FlashMQ supports outbound MQTT bridges. The site FlashMQ connects outbound to the
AWS FlashMQ over a single TLS connection and forwards selected topic groups.

Configured in `source/bridge/bridge.yaml` — regenerate and reload live:

```bash
python3 source/bridge/gen_bridge_conf.py bridge.yaml \
        --output /etc/flashmq/bridge-conf.d/bridge.conf
flashmq --reload-config
```

No inbound firewall rules needed on the site — the bridge dials out.

---

## Data Flow Comparison

### Current architecture (cloud pipeline — being replaced)

```
Site MQTT → [WAN] → AWS IoT Core → Lambda → InfluxDB
                  8,000 msgs/s × ~1 KB = 8 MB/s raw
                  = ~700 GB/day transfer per site
```

### Edge parquet architecture (current implementation)

```
Site MQTT → FlashMQ → writer.cpp → [rsync, 15 min] → S3
                      15 min flush × ~12 MB = ~17 GB/day
                      (same data, 40× less transfer)

Site FlashMQ → [bridge, single TCP connection] → AWS FlashMQ
                      live messages only → writer.cpp (AWS) → current_state.parquet
```

---

## Cost Comparison

### Per site, stress load (1,152 cells, ~8,000 msgs/s)

| Component | Current NATS (cloud) | Edge Parquet | InfluxDB (current AWS) |
|-----------|---------------------|--------------|----------------------|
| EC2 | $37 (`t3.large`) | $10 (`t3.small`) | $182 (`r5.xlarge`) |
| EBS JetStream | $2.50 | $0 | $25 |
| S3 storage | $10 | $10 | — |
| Data transfer | $14 | $0.35 | $3,600* |
| IoT Core | $0 | $0 | $3,000* |
| Lambda | $0 | $0 | $2,000* |
| **Total** | **~$64** | **~$21** | **~$76,200*** |

*Figures from actual AWS bill across 80 sites.

### At 80 sites

| | Edge Parquet | InfluxDB (actual) | Saving |
|--|--|--|--|
| EC2 (shared subscriber) | $20 | $36,000 | $35,980 |
| EBS/EFS | $0 | $10,000 | $10,000 |
| S3 (80 sites) | $800 | $3,000 | $2,200 |
| Data transfer | $28 | $3,600 | $3,572 |
| IoT Core | $0 | $3,000 | $3,000 |
| Lambda | $0 | $2,000 | $2,000 |
| CloudWatch | $10 | $2,600 | $2,590 |
| **Total** | **~$860/month** | **~$76,200/month** | **~$75,340/month** |

> Note: The Subscriber API on AWS is stateless and shared across all sites.
> EC2 cost does not scale with site count — one `t3.small` serves all 80 sites.

---

## Site Controller Requirements

The existing spark-22b6 stack is the reference implementation. Minimum hardware
for a production site controller:

| Resource | Minimum | Notes |
|----------|---------|-------|
| CPU | 2 cores | NATS + bridge + writer comfortably fit |
| RAM | 4 GB | NATS buffer + parquet writer in-memory batch |
| Disk | 20 GB | Local parquet queue (handles 24h offline) |
| Network | 1 Mbit/s | ~17 GB/day upload ≈ 1.6 Mbit/s average |

Suitable hardware: Raspberry Pi 5, industrial mini-PC, existing site SCADA server.

---

## Resilience: Dual-Path Data Protection

Every message travels two independent paths to S3 simultaneously:

```
Device
  │
  ▼
FlashMQ (QoS 1 — device retransmits if broker doesn't ack)
  │
  ▼
Site NATS JetStream (WAL on disk — survives process restart)
  │
  ├── PATH 1: Site Parquet Writer ──► S3  (primary, every 60s)
  │           only acks to NATS after successful upload
  │
  └── PATH 2: Leaf Node ──► AWS NATS ──► Gap-fill Writer ──► S3
              real-time forward          writes only if site hasn't
                                         uploaded that window
```

**Path 1 failure (power loss mid-buffer):**
- Messages already forwarded to AWS via Path 2 — not lost
- On restart, site NATS replays unacknowledged messages
- Site writer uploads the missing window to S3 on first flush
- Gap-fill writer may have already covered the window — S3 key check prevents duplicates

**Path 2 failure (WAN link down):**
- Site writer continues uploading to S3 as normal
- Leaf Node reconnects automatically when WAN recovers
- AWS NATS buffer refills; no gap in historical data (S3 already has it)

**Both paths fail simultaneously (power loss + WAN down):**
- Site NATS WAL preserves messages on local disk
- On power recovery, site writer replays and uploads the missing window
- Gap-fill writer covers the window from AWS NATS once Leaf Node reconnects

| Scenario | Current cloud pipeline | Edge parquet |
|----------|----------------------|--------------|
| Site power loss (mid-buffer) | **Data loss** | **Covered by Path 2 (Leaf Node)** |
| WAN link drops | **Data loss** — IoT Core drops messages | **No loss** — parquet queues locally |
| Site power loss + WAN down | **Total loss** | **Covered by site NATS WAL replay** |
| AWS outage | **Total failure** | Site continues, uploads resume on recovery |
| Site controller restart (clean) | Replay from NATS | Replay from local NATS JetStream |

This makes the edge architecture **more resilient than the current cloud pipeline**
for every failure mode.

---

## Migration Path

The edge architecture is a pure extension of the current proof-of-concept:

1. **Phase 1** — Deploy current spark-22b6 stack to 3–4 troubled sites (cloud pipeline,
   no site controller changes). Validates NATS→S3 in production. ~$22k/month saving.

2. **Phase 2** — Add local NATS + parquet writer to site controllers. Switch S3 upload
   to originate from site rather than AWS. Add NATS Leaf Node config. No AWS code changes.
   Additional ~$14/site/month saving on data transfer + EC2 reduction.

3. **Phase 3** — Full fleet rollout. AWS reduced to S3 + single Subscriber API instance.

---

## Key Files

| File | Purpose |
|------|---------|
| `source/parquet_writer_cpp/writer.cpp` | C++ parquet writer — runs on edge and AWS |
| `source/parquet_writer_cpp/Makefile` | Build with `make` |
| `source/parquet_writer_cpp/config.yaml` | Writer config — set `flush_interval_seconds`, `current_state_path` |
| `source/bridge/bridge.yaml` | FlashMQ bridge config — topic group selection |
| `source/bridge/gen_bridge_conf.py` | Generates FlashMQ bridge conf from bridge.yaml |
| `subscriber/api/server.py` | Subscriber API — runs on AWS, queries S3 + current_state.parquet |
