#!/usr/bin/env bash
# start-evelyn.sh — Evelyn demo stack on phil-256g (.34)
# Starts broker + writer containers, then launches stress_real_pub natively.
#
# Usage:
#   bash start-evelyn.sh [--units N] [--rate R]
#
# Defaults: 1 unit, 175 msg/s (dinky Evelyn)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SIM_BIN="$REPO_ROOT/source/stress_runner/stress_real_pub"
TEMPLATE="$REPO_ROOT/source/stress_runner/ems_topic_template.json"
LOG_DIR="/data/logs/$(date +%Y/%m/%d)"

UNITS=1
RATE=175

while [[ $# -gt 0 ]]; do
  case $1 in
    --units) UNITS="$2"; shift 2 ;;
    --rate)  RATE="$2";  shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo "=== Evelyn demo stack — .34 ==="
echo "    units=$UNITS  rate=$RATE msg/s"

# ── Parquet output dir ───────────────────────────────────────────────────────
mkdir -p /mnt/tort-sdf/evelyn
echo "    parquet → /mnt/tort-sdf/evelyn"

# ── Log dir ─────────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"

# ── Docker: broker + writer ──────────────────────────────────────────────────
echo ""
echo "Starting broker + writer containers..."
cd "$SCRIPT_DIR"
docker compose up -d

echo -n "Waiting for broker health"
for i in $(seq 1 30); do
  if docker inspect evelyn34_broker --format '{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; then
    echo " OK"
    break
  fi
  echo -n "."
  sleep 1
done

# ── stress_real_pub — native on host ─────────────────────────────────────────
if [[ ! -x "$SIM_BIN" ]]; then
  echo "Building stress_real_pub..."
  make -C "$REPO_ROOT/source/stress_runner" stress_real_pub
fi

LOG_FILE="$LOG_DIR/stress_real_pub_$(date +%H%M%S).log"
echo ""
echo "Starting stress_real_pub → localhost:11883  ($UNITS unit(s), $RATE msg/s)"
echo "    log → $LOG_FILE"
"$SIM_BIN" \
  --host localhost \
  --port 11883 \
  --units "$UNITS" \
  --rate "$RATE" \
  --template "$TEMPLATE" \
  > "$LOG_FILE" 2>&1 &

SIM_PID=$!
echo "    PID=$SIM_PID"
echo "$SIM_PID" > /tmp/evelyn34_sim.pid

# ── bridge_api — broker config API ───────────────────────────────────────────
BRIDGE_LOG="$LOG_DIR/bridge_api_$(date +%H%M%S).log"
echo ""
echo "Starting bridge_api on :8772"
echo "    log → $BRIDGE_LOG"
python3 "$SCRIPT_DIR/bridge_api.py" > "$BRIDGE_LOG" 2>&1 &
echo $! > /tmp/evelyn34_bridge_api.pid

echo ""
echo "Stack running."
echo "  Writer health:  http://$(hostname -I | awk '{print $1}'):8771/health"
echo "  Bridge API:     http://$(hostname -I | awk '{print $1}'):8772/bridge"
echo "  Parquet data:   /mnt/tort-sdf/evelyn/"
echo "  Stop with:      bash $SCRIPT_DIR/stop-evelyn.sh"
