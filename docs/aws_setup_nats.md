# AWS Setup — NATS Path

**Stack:** FlashMQ (MQTT) → NATS Bridge → NATS JetStream → Parquet Writer → S3 → Subscriber API

**Files:**
- `docs/aws_deploy.md` — EC2 + S3 setup (Parquet Writer + Subscriber API only)
- `source/nats_bridge/config.yaml` / `config.spark.yaml` — Bridge config
- `source/parquet_writer/config.yaml` — Writer config
- `subscriber/api/config.yaml` — Subscriber API config
- `manager/nats-server.conf` — NATS server config

---

## Architecture

```
[Generator / Stress Runner]
    | MQTT QoS-1  :1883/:1884
    v
[FlashMQ]  — MQTT broker  (on-prem, spark-22b6 or AWS EC2)
    | subscribe  batteries/#
    v
[NATS Bridge]  (source/nats_bridge/bridge.py)
    | NATS JetStream publish  batteries.*.*.*.*
    v
[NATS JetStream  :4222]  — stream: BATTERY_DATA
    | 48-hour retention, 8 GB max, file storage
    |
    +── pull consumer (durable: parquet-writer) ──> [Parquet Writer]
    |                                                      |
    |                                               Snappy Parquet → [S3 / MinIO]
    |                                               project=*/site=*/{year}/{month}/{day}/
    |
    +── push consumer (DeliverPolicy.NEW) ──> [Subscriber API  :8767 / :8768]
                                                   |
                                           WebSocket live stream
                                           DuckDB historical query (reads S3)
                                           Flux-compat HTTP  :8768
```

---

## Deployment Topology Options

### Option A — All components on-prem (dev/test)

All services run on `spark-22b6` managed by the process manager. No AWS required.
MinIO provides local S3-compatible storage. This is the current working configuration.

| Component | Host | Port |
|-----------|------|------|
| FlashMQ | spark-22b6 | 1883 |
| NATS server | spark-22b6 | 4222, 8222 |
| NATS bridge | spark-22b6 | — |
| Parquet writer | spark-22b6 | — |
| MinIO | spark-22b6 | 9000 |
| Subscriber API | spark-22b6 | 8767, 8768 |

### Option B — Parquet Writer + Subscriber API on EC2, rest on-prem

The two Python services that write/read S3 run on EC2. Everything else stays on-prem.
NATS must be reachable from EC2 — use a VPN (WireGuard recommended).

| Component | Host | Port |
|-----------|------|------|
| FlashMQ | spark-22b6 | 1883 |
| NATS server | spark-22b6 | 4222, 8222 |
| NATS bridge | spark-22b6 | — |
| **Parquet writer** | **EC2** | — |
| **Subscriber API** | **EC2** | 8767, 8768 |
| S3 | AWS S3 | — |

### Option C — Full AWS (NATS on EC2)

Run NATS server on EC2. On-prem only runs FlashMQ and the NATS bridge (which connects
to the EC2 NATS instance). Useful when on-prem storage is limited or when multiple
on-prem sites should funnel to one central NATS.

---

## Step-by-Step: Option B (Parquet Writer + Subscriber API on EC2)

### 1. EC2 instance

- OS: Ubuntu 24.04 LTS (arm64 or amd64)
- Minimum: `t3.medium` (2 vCPU, 4 GB)
- Recommended for DuckDB queries: `t3.large` or `c6g.xlarge`

**Security group inbound rules:**

| Port | Source | Purpose |
|------|--------|---------|
| 22 | your IP | SSH |
| 8767 | 0.0.0.0/0 | Subscriber WebSocket |
| 8768 | 0.0.0.0/0 | Flux-compat HTTP API |
| 4222 | on-prem NATS bridge IP | NATS (if running NATS on EC2) |

### 2. S3 bucket

```bash
aws s3 mb s3://battery-data-prod --region us-east-1
```

IAM role policy (attach to EC2 instance role — no credentials in config):

```json
{
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject","s3:GetObject","s3:ListBucket","s3:HeadBucket"],
    "Resource": [
      "arn:aws:s3:::battery-data-prod",
      "arn:aws:s3:::battery-data-prod/*"
    ]
  }]
}
```

### 3. Software install on EC2

```bash
git clone <repo> data_server
cd data_server
python3 -m venv .venv
.venv/bin/pip install -r requirements.spark.txt
```

### 4. Config changes

Two files need editing — the only change is removing the MinIO `endpoint_url`.

**`source/parquet_writer/config.yaml`:**
```yaml
nats:
  url: "nats://SPARK_PRIVATE_IP:4222"   # or VPN address

s3:
  # endpoint_url: ""    ← remove this line for real AWS S3
  bucket: battery-data-prod
  region: us-east-1
  access_key: ""    # leave blank when using EC2 instance role
  secret_key: ""
  project_id: 0
  partitions:
    - "project={project_id}"
    - "site={site_id}"
    - "{year}"
    - "{month}"
    - "{day}"

buffer:
  flush_interval_seconds: 60
  max_messages: 10000
  fetch_batch: 500

parquet:
  compression: snappy
```

**`subscriber/api/config.yaml`:**
```yaml
nats:
  url: "nats://SPARK_PRIVATE_IP:4222"

s3:
  # endpoint_url: ""    ← remove this line
  bucket: battery-data-prod
  region: us-east-1
  access_key: ""
  secret_key: ""

websocket:
  host: 0.0.0.0
  port: 8767

flux_http:
  port: 8768

history:
  default_limit: 1000
  max_limit: 10000
```

### 5. NATS connectivity

The Parquet writer and Subscriber API both connect to NATS on spark-22b6.

**Simplest option — WireGuard VPN:**
```bash
# On spark-22b6
wg-quick up wg0

# On EC2
wg-quick up wg0
ping 10.0.0.1   # verify VPN to spark-22b6
```

Then set `nats.url: "nats://10.0.0.1:4222"` in both config files.

**Alternative — NATS server on EC2:**
Run a second NATS server on EC2 and bridge the two with a NATS leafnode or gateway.
Adds complexity; only worth it if on-prem NATS should be hidden from EC2.

### 6. Systemd services on EC2

**`/etc/systemd/system/parquet-writer.service`:**
```ini
[Unit]
Description=Parquet Writer (NATS → S3)
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/data_server
ExecStart=/home/ubuntu/data_server/.venv/bin/python source/parquet_writer/writer.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=parquet-writer
```

**`/etc/systemd/system/subscriber-api.service`:**
```ini
[Unit]
Description=Subscriber API (DuckDB + WebSocket)
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/data_server
ExecStart=/home/ubuntu/data_server/.venv/bin/python subscriber/api/server.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=subscriber-api
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable parquet-writer subscriber-api
sudo systemctl start  parquet-writer subscriber-api
sudo systemctl status parquet-writer subscriber-api
```

---

## Component Configuration Reference

### NATS Server (`manager/nats-server.conf`)

```
port: 4222
http_port: 8222

jetstream {
  store_dir: "/tmp/nats-jetstream"
  max_memory_store: 536870912   # 512 MB
  max_file_store:   10737418240  # 10 GB
}
```

The NATS bridge creates the `BATTERY_DATA` stream on first start. Stream config (from bridge):

| Parameter | Value | Notes |
|-----------|-------|-------|
| Subjects | `batteries.>` | All cell topics |
| Storage | FILE | Persists to disk |
| Retention | LIMITS | Age + size |
| Max age | 48 hours | Writer recovery window |
| Max bytes | 8 GB | At ~285 KB/s ingest → ~7.8 hrs |

### NATS Bridge (`source/nats_bridge/config.yaml`)

```yaml
mqtt:
  host: localhost
  port: 1883         # FlashMQ
  topic: batteries/#

nats:
  url: nats://localhost:4222
  stream: BATTERY_DATA
  subjects:
    - batteries.>
  max_age_hours: 48
```

For spark-22b6 (`config.spark.yaml`): same structure, different host values.

**Topic conversion:** `/` → `.`
```
MQTT:  batteries/site=0/rack=1/module=2/cell=3
NATS:  batteries.site=0.rack=1.module=2.cell=3
```

### Parquet Writer — key config parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `flush_interval_seconds` | 60 | S3 flush interval (the "60-second gap") |
| `max_messages` | 10,000 | Flush early if buffer hits this count |
| `fetch_batch` | 500 | NATS pull batch size per iteration |
| `compression` | snappy | Parquet file compression codec |

---

## S3 Partition Layout

```
s3://battery-data-prod/
  project=0/
    site=0/
      2026/03/15/
        20260315T120000Z.parquet
        20260315T120100Z.parquet
      ...
    site=1/
      ...
```

DuckDB reads with `hive_partitioning=true`. Query pattern:

```sql
SELECT * FROM read_parquet(
  's3://battery-data-prod/project=*/*/*/*/*/*.parquet',
  hive_partitioning=true
)
WHERE timestamp > epoch(NOW()) - 3600;
```

---

## Verification Checklist

- [ ] NATS reachable from EC2: `nats-cli sub --server=nats://NATS_IP:4222 "batteries.>"`
- [ ] Parquet writer connected: `journalctl -u parquet-writer -f` → no `NATS connection` errors
- [ ] Files appearing in S3: `aws s3 ls s3://battery-data-prod/ --recursive | tail -5`
- [ ] Subscriber API up: `curl http://EC2_IP:8768/diag | jq .`
- [ ] Live subscribe works: connect to `ws://EC2_IP:8767`, send `{"type":"subscribe","subject":"batteries.>"}`
- [ ] History query works: send `query_history` with `from_ts` 5 minutes ago
- [ ] Browser localStorage: set `ds_host` to EC2 hostname in `monitor.html` / `subscriber.html`

---

## NATS Monitoring Endpoints

All available from NATS HTTP port `:8222`:

| Endpoint | Returns |
|----------|---------|
| `GET :8222/healthz` | `{"status":"ok"}` if healthy |
| `GET :8222/varz` | Server overview: uptime, connections, msg rates, memory |
| `GET :8222/jsz?streams=true&consumers=true` | JetStream stream + consumer stats |
| `GET :8222/connz` | Active connections (bridge, writer, subscriber) |
| `GET :8222/subsz` | Subject subscriptions |

See `html/nats.html` for an interactive explorer of these endpoints.

---

## Known Issues with This Path

See `docs/system_faults.md` for the full list. Key NATS path issues:

1. **60-second gap** — data in the last `flush_interval_seconds` is only in NATS, not S3.
   `query_history` returns zero rows for this window. Combine live subscribe + historical
   query for complete coverage.
2. **NATS 8 GB cap** — at 1,152 msgs/s the stream hits its size limit in ~7.8 hours,
   purging old messages. If the Parquet writer is behind by more than this, data is lost.
3. **NATS bridge crash (fixed)** — historically the bridge subscribed to the wrong MQTT
   topic, causing it to crash on start. Fixed by reverting to `batteries/#`. Monitor the
   bridge process and alert on unexpected restarts.
4. **Hive partitioning schema mismatch (fixed)** — DuckDB failed when the S3 glob
   matched Parquet files with different schemas (mixed-format files). Fixed by targeting
   only the `project=*` glob prefix, excluding top-level non-partition files.
5. **No FlashMQ HTTP API** — FlashMQ does not expose an HTTP health endpoint. Monitor
   via `$SYS/#` MQTT topics or by checking the systemd service status.
6. **Nested payload flattening** — the generator publishes nested `{value, unit}` objects.
   The Parquet writer flattens these. If a field is missing or malformed, the row is
   written with a `null` value — silently, with no alert.
