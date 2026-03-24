#!/usr/bin/env bash
# gx10-evelyn-start.sh — start real_writer + stress_real_pub on gx10-d94c (.48)
#                         simulating the Evelyn site (81,420 topics, site 0215D1D8)
#
# Run from repo root on gx10:
#   bash scripts/gx10-evelyn-start.sh
#
# Logs:   /data/logs/YYYY/MM/DD/real_writer.log
# Health: curl http://localhost:8771/health

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="/data/logs/$(date +%Y/%m/%d)"
mkdir -p "$LOGS"

WRITER_BIN="$REPO/source/parquet_writer_cpp/real_writer"
WRITER_CFG="$REPO/source/parquet_writer_cpp/config.gx10-evelyn.yaml"
STRESS_BIN="$REPO/source/stress_runner/stress_real_pub"
STRESS_TPL="$REPO/source/stress_runner/ems_topic_template.json"

# ── sanity checks ─────────────────────────────────────────────────────────
for f in "$WRITER_BIN" "$WRITER_CFG" "$STRESS_BIN" "$STRESS_TPL"; do
    [[ -f "$f" ]] || { echo "ERROR: missing $f — run make first"; exit 1; }
done

# ── stop any existing instances ────────────────────────────────────────────
pkill -f "real_writer.*config.gx10-evelyn"   2>/dev/null && echo "stopped old real_writer"   || true
pkill -f "stress_real_pub.*8769"             2>/dev/null && echo "stopped old stress_real_pub" || true
sleep 1

# ── start real_writer ──────────────────────────────────────────────────────
nohup "$WRITER_BIN" --config "$WRITER_CFG" \
    > "$LOGS/real_writer.log" 2>&1 &
WRITER_PID=$!
echo "real_writer  pid=$WRITER_PID  log=$LOGS/real_writer.log"

# ── start stress_real_pub ──────────────────────────────────────────────────
# rate=81420 = 1 full sweep/sec across all 46 Evelyn units
nohup "$STRESS_BIN" \
    --host localhost \
    --template "$STRESS_TPL" \
    --rate 81420 \
    --ws-port 8769 \
    > "$LOGS/stress_real_pub.log" 2>&1 &
STRESS_PID=$!
echo "stress_real_pub  pid=$STRESS_PID  log=$LOGS/stress_real_pub.log"

# ── wait for health endpoint ────────────────────────────────────────────────
echo -n "waiting for health endpoint"
for i in $(seq 1 20); do
    sleep 1
    if curl -sf http://localhost:8771/health > /dev/null 2>&1; then
        echo " ok"
        curl -s http://localhost:8771/health | python3 -m json.tool 2>/dev/null \
            || curl -s http://localhost:8771/health
        exit 0
    fi
    echo -n "."
done
echo " timeout — check $LOGS/real_writer.log"
exit 1
