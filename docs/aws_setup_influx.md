# AWS Setup — FlashMQ → Telegraf → InfluxDB2 Path

**Stack:** FlashMQ (MQTT broker) → Telegraf (bridge) → InfluxDB2 (time-series DB) → Grafana (dashboard)

**Files:**
- `orig_aws/setup_aws.sh` — native install script (Ubuntu 22.04/24.04)
- `orig_aws/docker-compose.yaml` — local dev / Docker version
- `orig_aws/flashmq/flashmq.conf` — FlashMQ config
- `orig_aws/telegraf/telegraf.conf` — Telegraf config
- `orig_aws/influxdb/README.md` — InfluxDB2 credentials and Flux query reference

---

## Architecture

```
[Generator / Stress Runner]
    | MQTT QoS-1  :1883
    v
[FlashMQ]  — MQTT broker
    | subscribe  batteries/#
    v
[Telegraf]  — MQTT consumer plugin
    | json_v2 parser → measurement: battery_cell
    v
[InfluxDB2]  — TSM time-series store
    | Flux API  :8086
    v
[Grafana]  — dashboards  :3000
```

---

## Option A — Native Install (Recommended)

Use `orig_aws/setup_aws.sh`. Run as root on Ubuntu 22.04 or 24.04.

```bash
git clone <repo> data_server
cd data_server/orig_aws
sudo ./setup_aws.sh
```

The script:
1. Installs FlashMQ from `flashmq.org` apt repo, enables systemd service on `:1883`
2. Installs InfluxDB2 + CLI from `repos.influxdata.com`, runs `influx setup` to initialise org/bucket/token
3. Installs Telegraf, patches `telegraf.conf` to use `localhost` instead of Docker hostnames
4. Enables all three as systemd services

**EFS mount (required before running script in production):**

Mount EFS at `/var/lib/influxdb2` before first run. If the mount is absent, InfluxDB2
data lands on the local EBS volume — fine for a test but data is lost if the instance
is terminated.

```
# /etc/fstab entry
fs-XXXXXXXX.efs.us-east-1.amazonaws.com:/ /var/lib/influxdb2 efs defaults,_netdev,tls 0 0
```

Then mount:
```bash
sudo mount -a
```

---

## Option B — Docker Compose (local dev only)

```bash
cd orig_aws
docker compose up -d
```

Starts FlashMQ, InfluxDB2, Telegraf, and Grafana. All inter-service hostnames resolve
via Docker DNS (`flashmq:1883`, `influxdb:8086`). No config changes needed.

**Ports exposed to host:**

| Service | Port |
|---------|------|
| FlashMQ (MQTT) | `1883` |
| InfluxDB2 UI + API | `8086` |
| Grafana | `3000` |

---

## Component Configuration

### FlashMQ (`orig_aws/flashmq/flashmq.conf`)

```conf
listen {
    port 1883
    protocol mqtt
}
allow_anonymous true
client_initial_buffer_size 65536
max_packet_size 1048576
max_qos_msg_pending_per_client 1000
```

Key points:
- Accepts anonymous connections — add TLS + auth for production
- 64 KB per-client buffer, 1 MB max message size
- No persistence (`persistence false` is the default) — all in-memory

### Telegraf (`orig_aws/telegraf/telegraf.conf`)

```toml
[agent]
  interval            = "1s"
  metric_batch_size   = 1000
  metric_buffer_limit = 100000
  flush_interval      = "10s"

[[inputs.mqtt_consumer]]
  servers  = ["tcp://localhost:1883"]   # "flashmq:1883" in Docker
  topics   = ["batteries/#"]
  qos      = 1
  data_format = "json_v2"

  [[inputs.mqtt_consumer.json_v2]]
    measurement_name = "battery_cell"
    [[inputs.mqtt_consumer.json_v2.tag]]
      path = "site_id"
    [[inputs.mqtt_consumer.json_v2.tag]]
      path = "rack_id"
    # ... module_id, cell_id
    [[inputs.mqtt_consumer.json_v2.field]]
      path = "voltage"
      type = "float"
    # ... current, temperature, soc, soh, resistance, capacity, power

[[outputs.influxdb_v2]]
  urls         = ["http://localhost:8086"]   # "http://influxdb:8086" in Docker
  token        = "battery-token-secret"
  organization = "battery-org"
  bucket       = "battery-data"
  timeout      = "5s"
```

Key points:
- `flush_interval = "10s"` — data lands in InfluxDB within 10 seconds of arrival
- `metric_buffer_limit = 100000` — Telegraf buffers 100k metrics in memory if InfluxDB is slow
- `qos = 1` — Telegraf requests at-least-once delivery from FlashMQ
- `json_v2` parser maps flat JSON fields; the generator's nested `{value, unit}` payloads are **not** handled here — the generator must publish flat payloads for this path, or the Parquet writer's flattening is not available

### InfluxDB2 (`orig_aws/influxdb/README.md`)

| Setting | Value |
|---------|-------|
| URL | `http://HOST:8086` |
| Organisation | `battery-org` |
| Bucket | `battery-data` |
| Retention | 30 days (rolling) |
| Admin token | `battery-token-secret` |
| Admin password | `adminpassword123` |

**Change credentials before production deployment.**

---

## EC2 Security Group Rules

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 1883 | TCP | on-prem only | FlashMQ MQTT |
| 8086 | TCP | 0.0.0.0/0 | InfluxDB2 API + UI |
| 3000 | TCP | 0.0.0.0/0 | Grafana |
| 22 | TCP | your IP | SSH |

For production, restrict `:8086` to internal IPs only. The admin token provides no
IP-based access control.

---

## Instance Sizing

| Load | Recommended instance | Notes |
|------|---------------------|-------|
| 144 msgs/s (dev) | `t3.medium` (2 vCPU, 4 GB) | Comfortable |
| 1,152 msgs/s | `t3.large` (2 vCPU, 8 GB) or `c6g.xlarge` | InfluxDB TSM is write-heavy |
| 3,000+ msgs/s | `c6g.2xlarge` or dedicated InfluxDB node | Separate Telegraf + InfluxDB2 hosts |

InfluxDB2 is the bottleneck at high rates. Telegraf is lightweight.

---

## Grafana Setup

Grafana provisions the InfluxDB2 datasource automatically via
`orig_aws/grafana/provisioning/datasources/influxdb.yaml`:

```yaml
- name: InfluxDB
  type: influxdb
  url: http://influxdb:8086
  jsonData:
    version: Flux
    organization: battery-org
    defaultBucket: battery-data
  secureJsonData:
    token: battery-token-secret
```

For native (non-Docker) Grafana, update `url` to `http://localhost:8086`.

---

## Data Retention

Default retention is 30 days. Older data is automatically deleted by InfluxDB2.

To change:
```bash
influx bucket update --name battery-data --retention 90d
```

To add a downsampled long-term bucket:
```bash
influx bucket create --name battery-data-1h --retention 365d
```

---

## Verification Checklist

- [ ] FlashMQ running: `systemctl status flashmq` — `active (running)` on port 1883
- [ ] InfluxDB2 running: `curl http://localhost:8086/health` → `{"status":"pass"}`
- [ ] Telegraf running: `systemctl status telegraf` — no error lines in journal
- [ ] Data flowing: InfluxDB2 UI → Data Explorer → `battery-data` bucket → `battery_cell` measurement shows rows
- [ ] Generator subscribed: `journalctl -u telegraf -f` → no connection errors
- [ ] Grafana reachable: `http://HOST:3000` (admin / admin)
- [ ] (Production) EFS mounted at `/var/lib/influxdb2` before starting InfluxDB2

---

## Monitoring the InfluxDB Path

### InfluxDB2 health

```bash
# Liveness
curl http://localhost:8086/ping        # → 204
curl http://localhost:8086/health      # → {"status":"pass"}

# Write queue (via CLI)
influx query 'from(bucket:"_monitoring") |> range(start:-5m) |> filter(fn:(r)=>r._measurement=="influxdb_write")'
```

### Telegraf internal metrics

Telegraf exposes internal metrics via `[[inputs.internal]]` (already in the config).
They appear in InfluxDB2 under the `telegraf` measurement. Useful fields:

| Metric | Field | Meaning |
|--------|-------|---------|
| `internal_agent` | `metrics_gathered` | Total metrics consumed from inputs |
| `internal_agent` | `metrics_written` | Total metrics written to InfluxDB |
| `internal_agent` | `metrics_dropped` | Metrics dropped (buffer overflow) |
| `internal_write` | `buffer_size` | Current in-memory metric buffer |

```flux
from(bucket: "battery-data")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "internal_agent")
  |> filter(fn: (r) => r._field == "metrics_dropped")
```

If `metrics_dropped > 0`, Telegraf's 100k buffer is full — InfluxDB2 cannot keep up with
the ingest rate. Increase instance size or reduce `metric_buffer_limit` to fail early.

### FlashMQ `$SYS/#` stats

FlashMQ publishes broker stats to `$SYS/#` every 10 seconds. Subscribe to see client
count and message rates:

```bash
mosquitto_sub -h localhost -p 1883 -t '$SYS/#' -v
```

Useful topics:

| Topic | Meaning |
|-------|---------|
| `$SYS/broker/clients/connected` | Current connected clients |
| `$SYS/broker/messages/received` | Total messages received |
| `$SYS/broker/messages/sent` | Total messages sent |
| `$SYS/broker/load/messages/received/1min` | Messages/sec (1-min moving average) |

---

## Known Issues with This Path

See `docs/system_faults.md` for a full list. Key InfluxDB path issues:

1. **30-day rolling delete** — data older than 30 days is gone. No long-term archive.
2. **EFS cost** — 30–40 TB at 1s resolution for 30 days costs ~$10k/month. EBS `gp3` is 10× cheaper if single-AZ is acceptable.
3. **No data-completeness check** — InfluxDB2 has no built-in "which cells are missing?" query. Write a Flux task or use the Parquet path completeness checks as a cross-reference.
4. **Flat payload requirement** — the `json_v2` parser expects flat top-level JSON fields. If the generator publishes nested `{value, unit}` objects, Telegraf will not parse them correctly and data will be silently dropped.
5. **Telegraf buffer overflow** — at 1,152 msgs/s, if InfluxDB2 is slow, the 100k metric buffer fills in ~87 seconds. Metrics are then dropped silently. Monitor `metrics_dropped`.
