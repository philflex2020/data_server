#!/usr/bin/env bash
# stop-evelyn.sh — stop Evelyn demo stack on phil-256g (.34)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Stopping Evelyn demo stack ==="

# Kill ems_site_simulator
if [[ -f /tmp/evelyn34_sim.pid ]]; then
  PID=$(cat /tmp/evelyn34_sim.pid)
  if kill -0 "$PID" 2>/dev/null; then
    echo "  Stopping ems_site_simulator (PID $PID)..."
    kill "$PID"
  else
    echo "  ems_site_simulator not running (PID $PID already gone)"
  fi
  rm -f /tmp/evelyn34_sim.pid
else
  echo "  No PID file — killing any ems_site_simulator processes..."
  pkill -x ems_site_simulator 2>/dev/null || true
fi

# Kill bridge_api
if [[ -f /tmp/evelyn34_bridge_api.pid ]]; then
  PID=$(cat /tmp/evelyn34_bridge_api.pid)
  if kill -0 "$PID" 2>/dev/null; then
    echo "  Stopping bridge_api (PID $PID)..."
    kill "$PID"
  fi
  rm -f /tmp/evelyn34_bridge_api.pid
else
  pkill -f bridge_api.py 2>/dev/null || true
fi

# Stop containers
echo "  Stopping broker + writer containers..."
cd "$SCRIPT_DIR"
docker compose down

echo "Done."
