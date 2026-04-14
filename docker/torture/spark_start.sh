#!/bin/bash
# spark_start.sh — Start the native torture test stack on spark1 (192.168.86.48)
#
# Usage:
#   bash spark_start.sh            # start everything including spark_api
#   bash spark_start.sh --no-api   # start stack only (called from spark_api /stack/up)
#   bash spark_start.sh --stop     # stop all processes and exit
#
# After a successful run:
#   FlashMQ broker      :1883  (20 threads, 4MB client buffer)
#   parquet writers × 4 :8771-8774  →  /mnt/spark-data/parquet-{a,b,c,d}
#   C++ stress pubs × 4 → batteries_{a,b,c,d}/#  (12 racks × 8 modules × 52 cells)
#   spark_api           :8780  (monitor/control API)

set -euo pipefail

REPO=~/work/gen-ai/data_server
WRITER_BIN=$REPO/source/parquet_writer_cpp/parquet_writer
FLASHMQ_SRC=/tmp/FlashMQ
FLASHMQ_BIN=$FLASHMQ_SRC/build/flashmq
STRESS_SRC_BRANCH=origin/03202026_run_with_real_data
STRESS_PUB=/tmp/stress_pub
DATA_BASE=/mnt/spark-data
LOG_DIR=/tmp
API_SCRIPT=$REPO/docker/torture/spark_api.py
START_API=true

for arg in "$@"; do
    case "$arg" in
        --no-api)  START_API=false ;;
        --stop)
            echo "[spark_start] stopping all stack processes..."
            pkill -f flashmq       2>/dev/null || true
            pkill -f parquet_writer 2>/dev/null || true
            pkill -f stress_pub    2>/dev/null || true
            pkill -f spark_api     2>/dev/null || true
            echo "[spark_start] stopped."
            exit 0
            ;;
    esac
done

# ── 1. Stop any existing stack processes ─────────────────────────────────────
echo "[spark_start] stopping existing processes..."
pkill -f flashmq        2>/dev/null || true
pkill -f parquet_writer 2>/dev/null || true
pkill -f stress_pub     2>/dev/null || true
sleep 1

# ── 2. Build FlashMQ if not present ──────────────────────────────────────────
if [[ ! -x "$FLASHMQ_BIN" ]]; then
    echo "[spark_start] building FlashMQ from source..."
    if [[ ! -d "$FLASHMQ_SRC" ]]; then
        git clone --depth 1 --branch v1.26.0 \
            https://github.com/halfgaar/FlashMQ.git "$FLASHMQ_SRC"
    fi
    cmake -B "$FLASHMQ_SRC/build" -DCMAKE_BUILD_TYPE=Release "$FLASHMQ_SRC" -q
    cmake --build "$FLASHMQ_SRC/build" -j$(nproc) --quiet
    echo "[spark_start] FlashMQ built OK"
fi

# ── 3. Build stress_pub if not present ───────────────────────────────────────
if [[ ! -x "$STRESS_PUB" ]]; then
    echo "[spark_start] building stress_pub..."
    cd "$REPO"
    git fetch origin --quiet
    git show "$STRESS_SRC_BRANCH:source/stress_runner/stress_pub.cpp" > /tmp/stress_pub.cpp
    g++ -O2 -std=c++17 -o "$STRESS_PUB" /tmp/stress_pub.cpp -lmosquitto
    echo "[spark_start] stress_pub built OK"
fi

# ── 4. Build parquet_writer if not present ───────────────────────────────────
if [[ ! -x "$WRITER_BIN" ]]; then
    echo "[spark_start] building parquet_writer..."
    make -C "$REPO/source/parquet_writer_cpp" parquet_writer
    echo "[spark_start] parquet_writer built OK"
fi

# ── 5. Ensure data directories exist ─────────────────────────────────────────
for x in a b c d; do
    mkdir -p "$DATA_BASE/parquet-$x"
done

# ── 6. Write writer configs ───────────────────────────────────────────────────
for x in a b c d; do
    port=877$(echo "$x" | tr abcd 1234)
    cat > "/tmp/writer-spark-$x.yaml" << EOF
mqtt:
  client_id: parquet-writer-$x
  host: localhost
  port: 1883
  topic: "batteries_$x/#"
  qos: 0
  reconnect_min_seconds: 1
  reconnect_max_seconds: 5
output:
  base_path: $DATA_BASE/parquet-$x
  compression: snappy
  flush_interval_seconds: 10
  max_messages_per_part: 50000
  max_total_buffer_rows: 2000000
  project_id: 0
  partitions:
    - "project={project_id}"
    - "site={site_id}"
    - "{year}"
    - "{month}"
    - "{day}"
health:
  enabled: true
  port: $port
wal:
  enabled: false
  path: /tmp/spark-wal-$x
guard:
  min_free_gb: 1.0
EOF
done

# ── 7. Write FlashMQ config ───────────────────────────────────────────────────
mkdir -p /tmp/flashmq-data
cat > /tmp/flashmq.conf << 'EOF'
# thread_count not set = auto-detect = 20 on spark1 (10 big + 10 little cores)
client_initial_buffer_size 4194304
allow_anonymous true
storage_dir /tmp/flashmq-data
EOF

# ── 8. Start FlashMQ ─────────────────────────────────────────────────────────
echo "[spark_start] starting FlashMQ (20 threads, 4MB client buffer)..."
nohup "$FLASHMQ_BIN" --config-file /tmp/flashmq.conf \
    > "$LOG_DIR/flashmq.log" 2>&1 &
sleep 2
if mosquitto_pub -h localhost -p 1883 -t test -m ping 2>/dev/null; then
    echo "[spark_start] FlashMQ OK"
else
    echo "[spark_start] ERROR: FlashMQ failed to start — check $LOG_DIR/flashmq.log"
    exit 1
fi

# ── 9. Start 4 parquet writers ────────────────────────────────────────────────
echo "[spark_start] starting 4 writers..."
for x in a b c d; do
    nohup "$WRITER_BIN" --config "/tmp/writer-spark-$x.yaml" \
        > "$LOG_DIR/writer-spark-$x.log" 2>&1 &
done
sleep 2
echo "[spark_start] writers started"

# ── 10. Start 4 stress publishers ─────────────────────────────────────────────
echo "[spark_start] starting 4 stress publishers..."
for x in a b c d; do
    nohup "$STRESS_PUB" \
        --host localhost --port 1883 \
        --prefix "batteries_$x" \
        --racks 12 --modules 8 --cells 52 \
        --conns 1 --id "stress-$x" \
        > "$LOG_DIR/stress-spark-$x.log" 2>&1 &
done
echo "[spark_start] stress publishers started"

# ── 11. Start spark_api ───────────────────────────────────────────────────────
if [[ "$START_API" == "true" ]]; then
    pkill -f spark_api 2>/dev/null || true
    sleep 1
    echo "[spark_start] starting spark_api on :8780..."
    nohup python3 "$API_SCRIPT" > "$LOG_DIR/spark_api.log" 2>&1 &
    sleep 2
fi

echo ""
echo "[spark_start] stack ready."
echo "  Monitor: http://192.168.86.48:8780/health"
echo "  Writers: http://192.168.86.48:8771/health (a) .. :8774/health (d)"
