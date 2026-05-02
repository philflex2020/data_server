#!/bin/bash
# start_flux_duck.sh — run flux_duck on .34 against writer-b rg3 data
#
# Starts on port 8087 (InfluxDB already holds 8096).
# Sends logs to /pool4/fractal/data/influx-compare/flux_duck.log
#
# Usage:
#   bash start_flux_duck.sh
#   # health check:
#   curl http://localhost:8087/health

LOG=/pool4/fractal/data/influx-compare/flux_duck.log
PYTHONPATH=/home/phil/work/data_storage/python

if pgrep -f "port 8087" > /dev/null || ss -tlnp 2>/dev/null | grep -q ':8087 '; then
    echo "flux_duck already running on :8087"
    exit 0
fi

echo "Starting flux_duck on :8087 → /pool4/fractal/data/writer-b/rg3"
echo "Logs: $LOG"

FLUX_HOT_BASE=/pool4/fractal/data/writer-b/rg3 \
FLUX_SITE_DIRS=false \
FLUX_HOT_DAYS=30 \
FLUX_INFLUX_COMPAT=true \
FLUX_PORT=8087 \
PYTHONPATH=$PYTHONPATH \
nohup python3 -m uvicorn flux_duck.server:app \
    --host 0.0.0.0 --port 8087 --log-level info \
    > "$LOG" 2>&1 &

PID=$!
echo "PID $PID — waiting for health..."
for i in $(seq 1 20); do
    sleep 1
    if curl -s --max-time 1 http://localhost:8087/health > /dev/null 2>&1; then
        echo "flux_duck up ($(curl -s http://localhost:8087/health))"
        exit 0
    fi
done
echo "ERROR: flux_duck did not come up — check $LOG"
exit 1
