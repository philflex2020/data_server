# Evelyn Demo Stack — Deployment Guide

**Stack:** FlashMQ broker · parquet-writer · bridge_api management API · HTML dashboard
**Purpose:** Ingest EMS MQTT data → Parquet files with live management UI
**Data source:** real EMS MQTT broker (via FlashMQ bridge) or local `stress_real_pub` for testing

---

## Architecture

```
[EMS / remote MQTT]──bridge──┐
                              ↓
[stress_real_pub]──:11883──→ [FlashMQ broker] ──ems/#──→ [parquet-writer] ──→ /parquet/
                                                                                 (disk)
[bridge_api :8772] ─── manages broker config, storage path, start/stop
[Dashboard  :8080] ─── health :8771 · mgmt :8772
```

Two deployment modes with identical configs and dashboard:

| Component | Docker mode | Native mode |
|---|---|---|
| FlashMQ | `evelyn34_broker` container | `flashmq` systemd service |
| parquet-writer | `evelyn34_writer` container | `parquet-writer` systemd service |
| bridge_api | `bridge_api.py` (native, manages Docker) | `bridge_api_native.py` (native, manages systemd) |
| Configs | `docker/evelyn-34/configs/` | `docker/evelyn-native/configs/` |
| Dashboard | `html/writer/evelyn_docker_34.html` | same file, change host field |

---

## Prerequisites

### Both modes
- Python 3.7+
- `mosquitto-clients` (`mosquitto_pub` — needed for FlashMQ health check)

### Docker mode
- Docker Engine 20+ and `docker compose` v2 plugin

### Native mode
- `libmosquitto1` (runtime dep for FlashMQ and stress_real_pub)
- Arrow/Parquet runtime libs — install from `.deb` files in `docker/torture/debs/`
  or copy `libarrow.so.2300` / `libparquet.so.2300` to `/usr/local/lib/`
- systemd (for service management)
- `g++`, `libmosquitto-dev`, `libsimdjson-dev` — only if building from source

---

## A: Docker Deployment

### 1. Pack (run on a built machine, e.g. phil-256g .34)

```bash
mkdir -p evelyn-stack/images evelyn-stack/stack evelyn-stack/html

# Save Docker images
docker save flashmq:torture       | gzip > evelyn-stack/images/flashmq-torture.tar.gz
docker save parquet-writer:latest | gzip > evelyn-stack/images/parquet-writer-latest.tar.gz

# Copy stack files
cp -r docker/evelyn-34/.           evelyn-stack/stack/
cp html/writer/evelyn_docker_34.html evelyn-stack/html/

tar czf evelyn-stack-docker.tar.gz evelyn-stack/
# ~350 MB; images are x86-64
```

### 2. Deploy on target

```bash
tar xzf evelyn-stack-docker.tar.gz
cd evelyn-stack

# Load images
docker load < images/flashmq-torture.tar.gz
docker load < images/parquet-writer-latest.tar.gz

# Create parquet output dir (adjust path to suit target disk)
sudo mkdir -p /data/parquet-evelyn && sudo chown $USER /data/parquet-evelyn

# Set storage path (edit stack/.env or use dashboard after start)
echo "PARQUET_PATH=/data/parquet-evelyn" > stack/.env

# Start broker + writer
cd stack && docker compose up -d

# Start management API  (keep running; add to cron/@reboot or systemd if permanent)
nohup python3 bridge_api.py > /tmp/bridge_api.log 2>&1 &
```

### 3. Connect data source

Use the dashboard bridge panel to enter the EMS broker IP/port, or edit
`stack/configs/flashmq.conf` and uncomment the bridge block manually:

```conf
bridge {
    address <EMS_IP>
    port 1883
    topic ems/#  in  0
    clientid evelyn-bridge
}
```

Then `docker restart evelyn34_broker` (or use the dashboard).

### 4. Verify

```bash
curl http://localhost:8771/health   # writer health JSON
curl http://localhost:8772/status   # full stack status JSON
```

---

## B: Native (No-Docker) Deployment

### 1. Pack (run on a built machine)

```bash
mkdir -p evelyn-stack-native/{bin,lib,configs,systemd,html}

# Binaries (x86-64 or rebuild for target arch)
cp source/parquet_writer_cpp/real_writer   evelyn-stack-native/bin/parquet-writer
cp source/stress_runner/stress_real_pub    evelyn-stack-native/bin/   # optional
cp source/stress_runner/ems_topic_template.json evelyn-stack-native/

# FlashMQ binary (extract from Docker image or build from source)
docker run --rm flashmq:torture cat /usr/sbin/flashmq > evelyn-stack-native/bin/flashmq
chmod +x evelyn-stack-native/bin/flashmq

# Arrow/Parquet runtime libs (if not system-installed)
cp docker/torture/debs/*.deb evelyn-stack-native/lib/

# Stack files
cp docker/evelyn-native/configs/flashmq.conf           evelyn-stack-native/configs/
cp docker/evelyn-native/configs/writer-native.yaml     evelyn-stack-native/configs/
cp docker/evelyn-native/bridge_api_native.py           evelyn-stack-native/
cp docker/evelyn-native/systemd/*.service               evelyn-stack-native/systemd/
cp docker/evelyn-native/install.sh                      evelyn-stack-native/
cp html/writer/evelyn_docker_34.html                    evelyn-stack-native/html/

tar czf evelyn-stack-native.tar.gz evelyn-stack-native/
# ~50 MB; binaries are arch-specific
```

### 2. Install on target

```bash
tar xzf evelyn-stack-native.tar.gz
cd evelyn-stack-native
bash install.sh
```

`install.sh` does:
- Installs Arrow/Parquet `.deb` files
- Copies binaries to `/usr/local/bin/`
- Copies configs to `/etc/evelyn/`
- Installs and enables systemd units
- Creates default parquet output dir

### 3. Manage

```bash
systemctl start  flashmq parquet-writer
systemctl stop   flashmq parquet-writer
systemctl status flashmq parquet-writer

# Management API (same HTTP surface as Docker version)
nohup python3 /usr/local/bin/bridge_api_native.py > /tmp/bridge_api.log 2>&1 &
```

### 4. Bridge to EMS

Edit `/etc/evelyn/flashmq.conf`, add:

```conf
bridge {
    address <EMS_IP>
    port 1883
    topic ems/#  in  0
    clientid evelyn-bridge
}
```

Then `systemctl restart flashmq` — or use the dashboard bridge panel.

---

## Config Reference

### `flashmq.conf`

Identical in both modes. Bridge block is commented out by default; added by bridge_api when configured via dashboard.

### `writer-evelyn.yaml` / `writer-native.yaml`

| Key | Docker value | Native value |
|---|---|---|
| `mqtt.host` | `broker` (service name) | `localhost` |
| `mqtt.port` | `1883` | `1883` |
| `mqtt.topic` | `ems/#` | `ems/#` |
| `output.base_path` | `/data/parquet` | `/data/parquet-evelyn` |
| `compact.enabled` | `true` | `true` |
| `compact.interval_seconds` | `600` | `600` |

### bridge_api endpoints (same in both modes)

| Endpoint | Action |
|---|---|
| `GET /status` | broker/writer/sim state + storage path + bridge config |
| `POST /start` | start broker + writer |
| `POST /stop` | stop broker + writer + kill sim |
| `GET /storage` | current parquet output path |
| `POST /storage` | `{"path":"..."}` — set path, mkdir, restart writer |
| `GET /bridge` | current FlashMQ bridge config |
| `POST /bridge` | `{"address","port","topic"}` — set bridge, restart broker |
| `POST /bridge/clear` | remove bridge, restart broker |

---

## Dashboard

Open `html/writer/evelyn_docker_34.html` in a browser (served from any HTTP server).

Set **Host** to the target machine IP. The dashboard connects to:
- `:8771` — parquet-writer health (live metrics, sparkline)
- `:8772` — bridge_api (stack control, storage, bridge config)

---

## Files in Repo

```
docker/evelyn-34/           Docker mode stack
  docker-compose.yml
  bridge_api.py             manages Docker containers
  start-evelyn.sh
  stop-evelyn.sh
  configs/
    flashmq.conf
    writer-evelyn.yaml

docker/evelyn-native/       Native mode stack
  bridge_api_native.py      manages systemd services
  install.sh
  configs/
    flashmq.conf            (identical to Docker version)
    writer-native.yaml      (mqtt.host: localhost)
  systemd/
    flashmq.service
    parquet-writer.service
    bridge-api.service

html/writer/
  evelyn_docker_34.html     dashboard (works for both modes)

docs/
  evelyn_deploy.md          this file
```

---

## Ports

| Port | Service | Notes |
|---|---|---|
| 1883 | FlashMQ (internal / native) | MQTT; Docker exposes as 11883 on host |
| 11883 | FlashMQ host port | Docker mode only — stress_real_pub connects here |
| 8771 | parquet-writer health | `GET /health` → JSON metrics |
| 8772 | bridge_api | stack management HTTP API |
