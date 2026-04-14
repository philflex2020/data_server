#!/usr/bin/env bash
# start_demo_server.sh — start the demo control server on :8788
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="$HOME/logs"
mkdir -p "$LOGS"

pkill -f "demo_server.py" 2>/dev/null && echo "stopped old demo_server" || true
sleep 0.5

nohup "$REPO/.venv/bin/python" "$REPO/source/demo_server/demo_server.py" \
    > "$LOGS/demo_server.log" 2>&1 &
echo "demo_server pid=$!  port=8788  log=$LOGS/demo_server.log"
sleep 1
curl -sf http://localhost:8788/health && echo " health ok" || echo " (not responding yet)"
