# Porting the AWS Data Store to spark-22b6

## Architecture

```
phil-dev (source + pipeline)          spark-22b6 (storage)
─────────────────────────────         ────────────────────
Mosquitto :1884                       MinIO :9000  :9011
NATS JetStream :4222                  AWS Store Server :8766
NATS Bridge
Parquet Writer  ──────────────────→  MinIO :9000
Subscriber API  ──────────────────→  MinIO :9000 (DuckDB history)
Manager :8761
HTML pages      ──────────────────→  AWS Store Server :8766
                                          (aws.html connects here)
```

---

## One-time Setup on spark-22b6

### 1. Clone the repo

```bash
git clone <repo-url> ~/data_server
cd ~/data_server
```

### 2. Create Python venv

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.spark.txt
```

### 3. Download MinIO binary

```bash
cd aws/data_store
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
cd ../..
```

### 4. Open firewall ports (if needed)

```bash
sudo ufw allow 9000/tcp   # MinIO S3 API  (parquet writer + subscriber API)
sudo ufw allow 9011/tcp   # MinIO console (optional — web UI)
sudo ufw allow 8766/tcp   # AWS Store Server WebSocket (aws.html)
sudo ufw allow 8762/tcp   # Manager WebSocket (dashboard.html — optional)
```

---

## Running on spark-22b6

### Option A — via manager (recommended, gives dashboard.html)

```bash
.venv/bin/python manager/manager.py --config manager/config.spark.yaml
```

Then open `html/dashboard.html` in a browser, set host to `spark-22b6` and port to `8762`.

### Option B — manually (two terminals)

```bash
# Terminal 1 — MinIO
cd aws/data_store
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio server ./minio-data --console-address :9011

# Terminal 2 — AWS Store Server
.venv/bin/python aws/data_store/server.py --config aws/data_store/config.yaml
```

---

## phil-dev — What Changed

| File | Change |
|---|---|
| `source/parquet_writer/config.yaml` | `endpoint_url` → `http://spark-22b6:9000` |
| `subscriber/api/config.yaml` | `endpoint_url` → `http://spark-22b6:9000` |
| `manager/config.yaml` | removed `minio` and `aws_server` components |

### Start phil-dev as normal

```bash
./start.sh
# or
.venv/bin/python manager/manager.py --config manager/config.yaml
```

### Connect aws.html to spark-22b6

Open `html/aws.html` — set the host field to `spark-22b6` and port to `8766`. This is saved in localStorage so only needs doing once.

---

## Verify the Pipeline

1. Start MinIO + aws_server on spark-22b6
2. Start the pipeline on phil-dev (`./start.sh`)
3. Open `html/generator.html` → click Start
4. Open `html/aws.html` (host: `spark-22b6`, port: `8766`) — Parquet files should appear
5. Open `html/subscriber.html` → run a history query — should return data from spark-22b6

---

## Resetting MinIO Data

```bash
# On spark-22b6
rm -rf aws/data_store/minio-data
```

MinIO will recreate the directory on next start. The bucket (`battery-data`) is created automatically by the aws_server on first connection.
