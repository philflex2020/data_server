# Data Server — Battery Cell Telemetry Pipeline

A development and demonstration environment for a battery cell telemetry pipeline,
running two complete architectures side by side:

- **orig_aws** — the customer's current production setup (reference implementation)
- **New NATS stack** — the proposed replacement at ~95% lower AWS cost

---

## Quick Start

```bash
./start.sh
```

Then open the HTML pages in a browser (see [Browser UI](#browser-ui) below).

---

## Architecture Overview

### orig_aws — Current Production (Reference)

```
IoT Battery Sites
      │  MQTT
      ▼
  FlashMQ          ← high-performance MQTT broker, port 1883
      │
  Telegraf         ← subscribes to batteries/#, parses JSON
      │
  InfluxDB2        ← time-series DB, port 8086
      │
  Grafana          ← dashboards (replaced here by html/orig_aws.html)
```

**AWS monthly cost: ~$76,200/month**

| Service | Cost | Driver |
|---|---|---|
| EC2 (InfluxDB2 + FlashMQ) | $36,000 | Large memory instances |
| EFS (InfluxDB2 data) | $10,000 | 30TB+ at $0.30/GB-month |
| IoT Core | $3,000 | MQTT rules engine |
| CloudWatch | $2,600 | High-resolution metrics |
| Lambda | $2,000 | Rules engine triggers |
| S3 | $3,000 | Backups/exports |
| Data Transfer | $3,600 | Egress to clients |

---

### New NATS Stack — Proposed Replacement

```
IoT Battery Sites
      │  MQTT
      ▼
  FlashMQ / Mosquitto    ← MQTT broker, port 1884 (dev), 1883 (prod)
      │
  NATS Bridge            ← subscribes to batteries/#, forwards to NATS
      │
  NATS JetStream         ← durable message stream, port 4222
      │
  Parquet Writer         ← buffers + writes Parquet files to S3/MinIO
      │
  S3 / MinIO             ← columnar storage, port 9000
      │
  Subscriber API         ← /live (NATS) + /history (DuckDB over Parquet)
```

**Target AWS cost: ~$500–2,000/month  (~$74,000/month saving)**

Key differences from orig_aws:

| Aspect | orig_aws | New stack |
|---|---|---|
| Storage | InfluxDB2 on EFS ($10k/mo) | Parquet on S3 (~$100/mo) |
| Data loss risk | Telegraf buffer loss | Zero (ack-after-write) |
| Historical queries | Flux over InfluxDB | DuckDB over S3 Parquet |
| Schema flexibility | Fixed measurement+tags | Any columnar schema |
| Retention | Configured in InfluxDB | S3 lifecycle policies |

---

## Running the System

### Prerequisites

```bash
# Create Python venv and install deps (once)
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# orig_aws systemd services (installed via orig_aws/setup_aws.sh)
# FlashMQ, InfluxDB2, Telegraf run as systemd services on boot
```

### Start Everything

```bash
./start.sh
```

This will:
1. Ensure FlashMQ, InfluxDB2, Telegraf systemd services are running
2. Start the **orig_aws manager** on port 8760 (controls the generator + streams service logs)
3. Start the **NATS stack manager** on port 8761 (controls all new stack components)

### Start Individually

**orig_aws manager** (generator + log tails for the 3 systemd services):
```bash
.venv/bin/python manager/manager.py --config orig_aws/manager_config.yaml
# → dashboard.html on port 8760
```

**New NATS stack manager** (all new components):
```bash
.venv/bin/python manager/manager.py --config manager/config.yaml
# → dashboard.html on port 8761
```

---

## Browser UI

Open these HTML files directly in a browser — they auto-connect and remember the host across pages.

| Page | Purpose | Connects to |
|---|---|---|
| `html/dashboard.html` | Process manager — start/stop components, live logs | port 8760 (orig_aws) or 8761 (new stack) |
| `html/orig_aws.html` | InfluxDB2 battery monitor — SVG heatmap + time-series charts | InfluxDB2 port 8086 |
| `html/generator.html` | Live generator data — cell measurements, msg/sec | Generator WebSocket port 8765 |
| `html/aws.html` | S3/MinIO Parquet store — file browser, sample data | AWS Store server port 8766 |
| `html/subscriber.html` | Query interface — live stream + historical DuckDB queries | Subscriber API port 8767 |
| `html/costs.html` | AWS cost comparison — orig vs new | (static) |
| `html/architecture.html` | Live pipeline diagrams — both stacks with status + flow | ports 8760 + 8761 |

---

## Port Map

| Port | Service | Stack |
|---|---|---|
| 1883 | FlashMQ MQTT broker | orig_aws |
| 1884 | Mosquitto MQTT broker | New NATS |
| 4222 | NATS JetStream | New NATS |
| 8086 | InfluxDB2 API + UI | orig_aws |
| 8760 | orig_aws process manager WebSocket | orig_aws |
| 8761 | New NATS process manager WebSocket | New NATS |
| 8765 | Generator WebSocket (live control) | New NATS |
| 8766 | AWS Store server WebSocket | New NATS |
| 9000 | MinIO S3-compatible API | New NATS |
| 9011 | MinIO console UI | New NATS |

---

## Directory Structure

```
data_server/
├── start.sh                    ← start everything
├── requirements.txt            ← Python deps for new stack
├── .venv/                      ← Python virtual environment
│
├── orig_aws/                   ← customer's current setup (reference — do not change)
│   ├── README.md               ← orig_aws architecture + cost breakdown
│   ├── docker-compose.yaml     ← optional Docker dev environment
│   ├── setup_aws.sh            ← native EC2 install script
│   ├── manager_config.yaml     ← manager config (port 8760)
│   ├── flashmq/flashmq.conf    ← FlashMQ broker config
│   ├── telegraf/telegraf.conf  ← Telegraf MQTT→InfluxDB2 config
│   ├── influxdb/               ← InfluxDB2 setup notes + Flux queries
│   └── source/generator.py    ← MQTT battery data simulator
│
├── manager/                    ← new stack process manager
│   ├── manager.py              ← WebSocket process manager
│   ├── config.yaml             ← component definitions (port 8761)
│   └── nats-server.conf        ← NATS JetStream config
│
├── source/                     ← new stack data sources
│   ├── generator/              ← MQTT generator + WebSocket server (port 8765)
│   ├── nats_bridge/            ← MQTT → NATS bridge
│   └── parquet_writer/         ← NATS → S3 Parquet writer
│
├── aws/data_store/             ← S3/MinIO server (port 8766)
├── subscriber/api/             ← DuckDB query API
│
├── html/                       ← browser UI pages
│   ├── dashboard.html
│   ├── orig_aws.html
│   ├── generator.html
│   ├── aws.html
│   ├── subscriber.html
│   └── costs.html
│
└── docs/                       ← architecture proposal + billing context
```

---

## ⚠️ KNOWN ISSUE: InfluxDB2 CORS Blocks Browser Status Probe

**Problem:** The browser pages (`monitor.html`, `orig_aws.html`) probe InfluxDB2 at `:8086/ping`
to show the online/offline dot. Because the pages are served from `:8080`, this is a
cross-origin request. InfluxDB2 returns **no `Access-Control-Allow-Origin` header** on `/ping`,
so Chrome/Firefox block it — the dot shows red even when InfluxDB2 is running fine.

The actual data queries (Flux API at `/api/v2/query`) work because InfluxDB2 does include
CORS headers on that endpoint. Only the ping probe is broken.

**Workarounds to try (in order of preference):**

1. **Enable CORS in InfluxDB2 config** — add to `/etc/influxdb/config.toml`:
   ```toml
   http-cors-enabled = true
   http-cors-allowed-origins = ["http://phil-dev:8080", "http://localhost:8080"]
   ```
   Then `sudo systemctl restart influxdb`. This is the cleanest fix.

2. **Nginx reverse proxy** — serve both `:8080` (static files) and `:8086` (InfluxDB2)
   from the same origin via a proxy path (e.g. `/influx/` → `http://localhost:8086/`).
   Eliminates all cross-origin issues.

3. **Ignore the dot** — the dot is cosmetic. If you can run a Flux query (Connect button
   works), InfluxDB2 is up. The ping probe failure does not affect data fetching.

**Root cause:** InfluxDB2 OSS does not send `Access-Control-Allow-Origin` on `/ping` or `/health`
endpoints. `mode: 'no-cors'` fetch also fails in Chrome when the server explicitly returns no
CORS headers (Chrome rejects the opaque response as `ERR_FAILED`).

---

## InfluxDB2 Credentials (orig_aws)

| Setting | Value |
|---|---|
| URL | http://localhost:8086 |
| Username | admin |
| Password | adminpassword123 |
| Token | battery-token-secret |
| Org | battery-org |
| Bucket | battery-data |

---

## Data Schema

Battery cells publish to MQTT topic:
```
batteries/site={S}/rack={R}/module={M}/cell={C}
```

JSON payload:
```json
{
  "timestamp": 1700000000,
  "site_id": "0", "rack_id": "1", "module_id": "2", "cell_id": "3",
  "voltage": 3.72,
  "current": -1.5,
  "temperature": 28.4,
  "soc": 72.3,
  "soh": 98.1,
  "resistance": 0.0023,
  "capacity": 98.5,
  "power": -5.58
}
```

Default hierarchy: 3 sites × 4 racks × 8 modules × 12 cells = **1,152 cells** at 1s resolution = ~1,152 msg/sec.
