# Deploying to AWS (EC2 + S3)

## Current deployment — writer.cpp (AWS writer mode)

The current AWS deployment runs `writer.cpp` in current_state mode alongside the
Subscriber API.  NATS and MinIO are no longer needed.

**Components on AWS EC2:**

- **`writer.cpp`** — subscribes to the local AWS FlashMQ (which receives the bridge
  from the host), writes `current_state.parquet` (one row per sensor, always latest
  reading) to local disk. No NATS, no S3 writes from this process.
- **`subscriber/api/server.py`** — queries S3 (historical parquet rsynced from host)
  and reads `current_state.parquet` (local, no rsync lag) for current-state queries.

**Data flow:**
```
Host writer.cpp → /srv/data/parquet/ ──rsync──► S3 (every 15 min)
                                                      │
Host FlashMQ ──bridge──► AWS FlashMQ                 │
                              │                       │
                         writer.cpp (AWS)             │
                              │                       │
                         current_state.parquet         │
                         (local, no rsync lag)        │
                              │                       │
                         DuckDB ◄─────────────────────┘
```

### AWS writer config (`config.yaml`)

```yaml
mqtt:
  host: localhost       # AWS FlashMQ receiving the bridge
  port: 1883
  client_id: parquet-writer-aws
  topic: "#"
  qos: 1

output:
  base_path: /data/parquet-aws
  current_state_path: /data/parquet-aws/current_state.parquet
  hot_file_path: ""                   # not needed — current_state covers the gap
  flush_interval_seconds: 60
```

### Build and install on EC2

```bash
# Install Arrow + dependencies (Ubuntu 24.04)
sudo apt install -y libmosquitto-dev libyaml-cpp-dev libsimdjson-dev \
    libparquet-dev libarrow-dev pkg-config g++

git clone git@github.com:philflex2020/data_server.git
cd data_server/source/parquet_writer_cpp
make

mkdir -p /data/parquet-aws
./parquet_writer --config writer_config.yaml
```

### Systemd service

```ini
[Unit]
Description=Parquet Writer (AWS current_state mode)
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/data_server/source/parquet_writer_cpp
ExecStart=/home/ubuntu/data_server/source/parquet_writer_cpp/parquet_writer --config writer_config.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=parquet-writer

[Install]
WantedBy=multi-user.target
```

### Security group rules (AWS)

| Port | Protocol | Source       | Purpose                    |
|------|----------|--------------|----------------------------|
| 8767 | TCP      | 0.0.0.0/0    | Subscriber WebSocket       |
| 8768 | TCP      | 0.0.0.0/0    | Flux-compat HTTP API       |
| 1883 | TCP      | host only    | FlashMQ (bridge inbound)   |
| 22   | TCP      | your IP      | SSH                        |

### Checklist

- [ ] EC2 Ubuntu 24.04 launched
- [ ] Arrow + dependencies installed (`make deps` in parquet_writer_cpp/)
- [ ] writer.cpp built (`make`)
- [ ] AWS FlashMQ running and bridge configured from host
- [ ] `config.yaml` updated: `current_state_path` set, `hot_file_path` empty
- [ ] writer.cpp systemd service enabled and started
- [ ] S3 bucket accessible from EC2 (instance role or IAM keys)
- [ ] Subscriber API config updated to read `current_state.parquet` locally

---

## Legacy deployment — Python writer.py + NATS (superseded)

> **Superseded.** The following describes the old `NATS JetStream → writer.py → MinIO`
> deployment.  Retained for reference only.  New deployments should use writer.cpp above.

The two Python programs that need AWS infrastructure are:

- **`source/parquet_writer/writer.py`** — consumes from NATS JetStream, writes Parquet to S3
- **`subscriber/api/server.py`** — queries S3 via DuckDB, serves WebSocket + Flux HTTP API

Everything else (NATS, FlashMQ, NATS bridge, stress runner) stays on-prem generating
and routing data. The parquet writer and subscriber API are the only components that
talk to S3.

---

## 1. EC2 Instance

- **OS**: Ubuntu 24.04 LTS (arm64 or amd64)
- **Size**: `t3.medium` minimum — DuckDB history queries are memory-hungry; `t3.large`
  recommended for production
- **Inbound security group rules**:

| Port | Protocol | Source       | Purpose                        |
|------|----------|--------------|--------------------------------|
| 8767 | TCP      | 0.0.0.0/0    | Subscriber WebSocket           |
| 8768 | TCP      | 0.0.0.0/0    | Flux-compat HTTP API           |
| 22   | TCP      | your IP      | SSH                            |
| 4222 | TCP      | on-prem only | NATS (if running NATS on EC2)  |

---

## 2. S3 Bucket

Create a bucket, e.g. `battery-data-prod`:

```bash
aws s3 mb s3://battery-data-prod --region us-east-1
```

Optional lifecycle rule to expire old data (edit `--days` as needed):

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket battery-data-prod \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "expire-old-data",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "Expiration": {"Days": 90}
    }]
  }'
```

---

## 3. IAM — EC2 Instance Role (recommended)

Create an IAM role with the following inline policy and attach it to the EC2 instance.
This avoids storing credentials in config files.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:HeadBucket",
      "s3:CreateBucket"
    ],
    "Resource": [
      "arn:aws:s3:::battery-data-prod",
      "arn:aws:s3:::battery-data-prod/*"
    ]
  }]
}
```

If using IAM keys instead, set `access_key` and `secret_key` in the config files below.

---

## 4. Software Install on EC2

```bash
git clone git@github.com:philflex2020/data_server.git
cd data_server
python3 -m venv .venv
.venv/bin/pip install -r requirements.spark.txt
```

---

## 5. Config Changes

Only two files need editing. The sole change is removing the MinIO `endpoint_url` —
boto3 then routes to real AWS S3 automatically.

**`source/parquet_writer/config.yaml`**:
```yaml
nats:
  url: "nats://spark-22b6:4222"   # or VPN/private IP of your NATS server

s3:
  # endpoint_url: removed — omitting this uses real AWS S3
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

**`subscriber/api/config.yaml`**:
```yaml
nats:
  url: "nats://spark-22b6:4222"

s3:
  # endpoint_url: removed — omitting this uses real AWS S3
  bucket: battery-data-prod
  region: us-east-1
  access_key: ""
  secret_key: ""
  project_id: 0

websocket:
  host: 0.0.0.0
  port: 8767

flux_http:
  port: 8768

history:
  default_limit: 1000
  max_limit: 10000
```

---

## 6. NATS Connectivity

The parquet writer and subscriber API both connect to NATS. Options:

| Option | Setup | Notes |
|--------|-------|-------|
| **VPN** | WireGuard or AWS VPN between EC2 and on-prem | Simplest; NATS stays on spark |
| **NATS on EC2** | Run nats-server on EC2, run a second NATS bridge on spark | Two bridges, two streams |
| **NATS cluster** | spark + EC2 as NATS cluster peers | Messages replicate automatically |

**Simplest starting point**: WireGuard VPN so `nats://spark-22b6:4222` is reachable
from EC2 at a private IP.

---

## 7. Systemd Services on EC2

Create `/etc/systemd/system/parquet-writer.service`:

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

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/subscriber-api.service`:

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

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable parquet-writer subscriber-api
sudo systemctl start parquet-writer subscriber-api
sudo systemctl status parquet-writer subscriber-api
```

---

## 8. Browser Config

In the dashboard or any HTML page, set `aws_host` (localStorage) to the EC2 public
hostname or IP. The monitor, subscriber, and aws pages all use `aws_host` for the
spark-22b6 / AWS stack buttons.

---

## Checklist

- [ ] EC2 Ubuntu 24.04 launched with instance role attached
- [ ] S3 bucket created, region matches config
- [ ] Repo cloned, venv created, `requirements.spark.txt` installed
- [ ] `endpoint_url` removed from `parquet_writer/config.yaml` and `subscriber/api/config.yaml`
- [ ] Bucket name and region set correctly in both configs
- [ ] NATS reachable from EC2 (VPN recommended)
- [ ] Both systemd services enabled and started
- [ ] `aws_host` in browser localStorage set to EC2 hostname
