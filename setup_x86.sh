#!/usr/bin/env bash
# setup_x86.sh — provision a fresh x86 Ubuntu machine and run the NATS pipeline test
#
# Usage: bash setup_x86.sh <host> <user> [sudo_pass]
#        (key-based SSH auth — run ssh-copy-id first if needed)
#        sudo_pass is only needed for apt install; omit if packages are pre-installed
#
# What it does:
#   1. Installs apt packages: python3, git, mosquitto, FlashMQ, Telegraf, InfluxDB 2
#   2. Clones / updates the data_server repo
#   3. Creates Python venv and installs requirements
#   4. Downloads nats-server binary if not already present
#   5. Starts MinIO, NATS, generator, bridge, parquet writer
#   6. Sends WebSocket start command to the generator
#   7. Waits for data to flow, then runs nats_test.py

set -euo pipefail

HOST="${1:?Usage: $0 <host> <user> [sudo_pass]}"
USER="${2:?Usage: $0 <host> <user> [sudo_pass]}"
SUDO_PASS="${3:-}"

REPO="https://github.com/philflex2020/data_server.git"
REPO_DIR="/home/${USER}/data_server"
NATS_VERSION="v2.12.5"
NATS_URL="https://github.com/nats-io/nats-server/releases/download/${NATS_VERSION}/nats-server-${NATS_VERSION}-linux-amd64.tar.gz"

# ── Helpers ──────────────────────────────────────────────────────────────────

ssh_run() {
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${USER}@${HOST}" "$@"
}

echo "==> Connecting to ${USER}@${HOST} ..."
ssh_run "uname -m && lsb_release -sd 2>/dev/null || grep PRETTY /etc/os-release | cut -d= -f2 | tr -d '\"'"

# ── 1. System packages ────────────────────────────────────────────────────────

echo ""
echo "==> Installing system packages ..."
if [ -n "$SUDO_PASS" ]; then
    # base64-encode to avoid all special-char / history-expansion issues
    B64_PASS=$(printf '%s' "$SUDO_PASS" | base64)
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${USER}@${HOST}" "
set -e
SP=\$(echo ${B64_PASS} | base64 -d)
sudop() { echo \"\$SP\" | sudo -S \"\$@\"; }
export DEBIAN_FRONTEND=noninteractive

echo '-- apt update --'
sudop apt-get update -qq

echo '-- base packages --'
sudop apt-get install -y -qq \
    python3 python3-venv python3-pip git \
    mosquitto unzip curl wget gnupg lsb-release ca-certificates

# InfluxDB 2 + Telegraf
if ! command -v influx &>/dev/null || ! command -v telegraf &>/dev/null; then
    echo '-- InfluxData apt repo --'
    curl -fsSL https://repos.influxdata.com/influxdata-archive.key \
        | gpg --dearmor > /tmp/influxdb.gpg
    sudop cp /tmp/influxdb.gpg /usr/share/keyrings/influxdb-archive-keyring.gpg; rm /tmp/influxdb.gpg
    echo 'deb [signed-by=/usr/share/keyrings/influxdb-archive-keyring.gpg] https://repos.influxdata.com/ubuntu stable main' \
        > /tmp/influxdb.list
    sudop cp /tmp/influxdb.list /etc/apt/sources.list.d/influxdb.list; rm /tmp/influxdb.list
    sudop apt-get update -qq
    sudop apt-get install -y -qq influxdb2 telegraf
fi
echo \"InfluxDB: \$(influxd version 2>&1 | head -1)\"
echo \"Telegraf: \$(telegraf --version 2>&1 | head -1)\"
echo '-- all packages OK --'
"
else
    echo "    (no sudo_pass supplied — skipping apt)"
    ssh_run "for cmd in mosquitto influx telegraf; do command -v \$cmd &>/dev/null && echo \"\$cmd: OK\" || echo \"WARNING: \$cmd not found\"; done"
fi

# ── 2. Clone / update repo ────────────────────────────────────────────────────

echo ""
echo "==> Setting up repo at ${REPO_DIR} ..."
ssh_run "
if [ -d '${REPO_DIR}/.git' ]; then
    echo 'Repo exists — pulling latest ...'
    cd '${REPO_DIR}' && git pull --ff-only
else
    echo 'Cloning repo ...'
    git clone '${REPO}' '${REPO_DIR}'
fi
"

# ── 3. Python venv ────────────────────────────────────────────────────────────

echo ""
echo "==> Setting up Python venv ..."
ssh_run "
cd '${REPO_DIR}'
if [ ! -d .venv ]; then
    python3 -m venv .venv
    echo 'venv created'
fi
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements.txt
echo 'pip install done'
"

# ── 4. nats-server binary ─────────────────────────────────────────────────────

echo ""
echo "==> Checking nats-server ..."
if [ -n "$SUDO_PASS" ]; then
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${USER}@${HOST}" "
SP=\$(echo ${B64_PASS} | base64 -d)
if command -v nats-server &>/dev/null; then
    echo \"nats-server already installed: \$(nats-server --version)\"
else
    echo 'Downloading nats-server ${NATS_VERSION} ...'
    cd /tmp
    curl -sSL '${NATS_URL}' -o nats-server.tar.gz
    tar -xzf nats-server.tar.gz
    echo \"\$SP\" | sudo -S mv nats-server-${NATS_VERSION}-linux-amd64/nats-server /usr/local/bin/
    rm -rf nats-server.tar.gz nats-server-${NATS_VERSION}-linux-amd64
    echo \"nats-server installed: \$(nats-server --version)\"
fi
"
else
    ssh_run "nats-server --version 2>/dev/null || echo 'WARNING: nats-server not found — re-run with sudo_pass'"
fi

# ── 5. MinIO binary ───────────────────────────────────────────────────────────

echo ""
echo "==> Checking MinIO ..."
ssh_run "
cd '${REPO_DIR}'
if [ ! -x aws/data_store/minio ]; then
    echo 'MinIO binary missing — downloading ...'
    curl -sSL 'https://dl.min.io/server/minio/release/linux-amd64/minio' \
        -o aws/data_store/minio
    chmod +x aws/data_store/minio
fi
echo \"MinIO: \$(aws/data_store/minio --version 2>&1 | head -1)\"
"

# ── 6. Start MinIO + create bucket ───────────────────────────────────────────

echo ""
echo "==> Starting MinIO ..."
ssh_run "
cd '${REPO_DIR}/aws/data_store'
pkill -f 'minio server' 2>/dev/null || true
sleep 1
mkdir -p minio-data
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    nohup ./minio server ./minio-data --console-address :9011 \
    &>/tmp/minio.log &
echo \"MinIO PID: \$!\"
sleep 3
curl -sf http://localhost:9000/minio/health/live && echo 'MinIO health: OK' || echo 'MinIO health: FAILED'
"

ssh_run "
cd '${REPO_DIR}'
.venv/bin/python3 -c \"
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin',
    region_name='us-east-1')
try:
    s3.create_bucket(Bucket='battery-data')
    print('Bucket created: battery-data')
except Exception as e:
    print('Bucket:', e)
\"
"

# ── 7. NATS server ────────────────────────────────────────────────────────────

echo ""
echo "==> Starting NATS server ..."
ssh_run "
pkill -f 'nats-server' 2>/dev/null || true; sleep 1
cd '${REPO_DIR}/manager'
nohup nats-server -c nats-server.conf &>/tmp/nats-server.log &
echo \"NATS PID: \$!\"
sleep 2
curl -sf http://localhost:8222/varz | python3 -c \"import sys,json; d=json.load(sys.stdin); print('NATS', d['version'], 'OK')\"
"

# ── 8. Mosquitto (MQTT broker for test) ──────────────────────────────────────

echo ""
echo "==> Starting Mosquitto (port 1884) ..."
ssh_run "
pkill -f 'mosquitto -p 1884' 2>/dev/null || true; sleep 1
nohup /usr/sbin/mosquitto -p 1884 &>/tmp/mosquitto.log &
echo \"Mosquitto PID: \$!\"
sleep 1
echo 'Mosquitto started on port 1884'
"

# ── 9. InfluxDB 2 ─────────────────────────────────────────────────────────────

echo ""
echo "==> Starting InfluxDB 2 ..."
ssh_run "
pkill -f 'influxd' 2>/dev/null || true; sleep 1
nohup influxd &>/tmp/influxd.log &
echo \"InfluxDB PID: \$!\"
sleep 5
curl -sf http://localhost:8086/health | python3 -c \"import sys,json; d=json.load(sys.stdin); print('InfluxDB', d.get('version','?'), d.get('status','?'))\" || echo 'InfluxDB health check failed'
"

# ── 10. Parquet writer config patch (localhost MinIO) ────────────────────────

echo ""
echo "==> Patching parquet writer config for localhost MinIO ..."
ssh_run "
cd '${REPO_DIR}'
sed -i 's|endpoint_url:.*|endpoint_url: \"http://localhost:9000\"  # local MinIO|' \
    source/parquet_writer/config.yaml
grep 'endpoint_url' source/parquet_writer/config.yaml
"

# ── 11. Generator ─────────────────────────────────────────────────────────────

echo ""
echo "==> Starting generator ..."
ssh_run "
pkill -f 'generator.py' 2>/dev/null || true; sleep 1
cd '${REPO_DIR}/source/generator'
nohup /home/${USER}/data_server/.venv/bin/python generator.py --config config.yaml \
    &>/tmp/generator.log &
echo \"Generator PID: \$!\"
sleep 3
"

# Send start command to generator WebSocket
ssh_run "
cd '${REPO_DIR}'
.venv/bin/python3 -c \"
import asyncio, websockets, json
async def start():
    try:
        async with websockets.connect('ws://localhost:8765') as ws:
            await ws.recv()  # initial status
            await ws.send(json.dumps({'type': 'start'}))
            resp = await asyncio.wait_for(ws.recv(), timeout=3)
            d = json.loads(resp)
            print(f'Generator running: {d.get(\\\"running\\\")}, assets: {d.get(\\\"cell_count\\\")}')
    except Exception as e:
        print(f'WS error: {e}')
asyncio.run(start())
\"
"

# ── 12. NATS bridge ───────────────────────────────────────────────────────────

echo ""
echo "==> Starting NATS bridge ..."
ssh_run "
pkill -f 'bridge.py' 2>/dev/null || true; sleep 1
cd '${REPO_DIR}/source/nats_bridge'
nohup /home/${USER}/data_server/.venv/bin/python bridge.py --config config.yaml \
    &>/tmp/bridge.log &
echo \"Bridge PID: \$!\"
sleep 3
grep -a 'INFO\|ERROR' /tmp/bridge.log | tail -3
"

# ── 13. Parquet writer ────────────────────────────────────────────────────────

echo ""
echo "==> Starting parquet writer ..."
ssh_run "
pkill -f 'writer.py' 2>/dev/null || true; sleep 1
cd '${REPO_DIR}/source/parquet_writer'
nohup /home/${USER}/data_server/.venv/bin/python writer.py --config config.yaml \
    &>/tmp/writer.log &
echo \"Writer PID: \$!\"
sleep 3
grep -a 'INFO\|ERROR' /tmp/writer.log | tail -3
"

# ── 14. Stress runner ────────────────────────────────────────────────────────

echo ""
echo "==> Starting stress runner ..."
ssh_run "
pkill -f 'stress_runner.py' 2>/dev/null || true; sleep 1
cd '${REPO_DIR}/source/stress_runner'
nohup /home/${USER}/data_server/.venv/bin/python stress_runner.py --config config.yaml \
    &>/tmp/stress_runner.log &
echo \"Stress runner PID: \$!\"
sleep 3
grep -a 'INFO\|ERROR\|started\|Listening' /tmp/stress_runner.log | tail -3
"

# ── 15. Verify generators via test_generators.py ─────────────────────────────

echo ""
echo "==> Running test_generators.py ..."
ssh_run "
cd '${REPO_DIR}'
.venv/bin/python scripts/test_generators.py \
    --mqtt-host localhost \
    --mqtt-port 1884 \
    --gen-ws    ws://localhost:8765 \
    --stress-ws ws://localhost:8769 \
    --sample-secs 10
"

echo ""
echo "==> Verifying pipeline (10s sample) ..."
ssh_run "
sleep 5
IN1=\$(curl -s http://localhost:8222/varz | python3 -c \"import sys,json; print(json.load(sys.stdin)['in_msgs'])\")
sleep 10
IN2=\$(curl -s http://localhost:8222/varz | python3 -c \"import sys,json; print(json.load(sys.stdin)['in_msgs'])\")
RATE=\$(python3 -c \"print(round((\$IN2-\$IN1)/10, 1))\")
echo \"NATS throughput: \${RATE} msg/sec\"
"

# ── 15. Run nats_test.py ──────────────────────────────────────────────────────

echo ""
echo "==> Running nats_test.py (30s sample) ..."
ssh_run "
cd '${REPO_DIR}'
.venv/bin/python scripts/nats_test.py \
    --nats-mon http://localhost:8222 \
    --s3-endpoint http://localhost:9000 \
    --sample-secs 30
"

echo ""
echo "==> Setup complete on ${HOST}"
echo "    Logs: /tmp/{minio,nats-server,mosquitto,influxd,generator,stress_runner,bridge,writer}.log"
echo "    InfluxDB UI: http://${HOST}:8086"
echo "    Results: ${REPO_DIR}/docs/nats_details.md"
