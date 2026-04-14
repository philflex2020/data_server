#!/usr/bin/env bash
# evelyn-compare-docker-start.sh — start the Evelyn cost-comparison Docker stack
#
# Run on the machine that has Docker (phil-256g .34, or gx10 .48 if Docker is installed).
# Generators run separately via evelyn-compare-generators.sh.
#
# Services started:
#   flashmq-baseline  :11883   flashmq-filtered  :11884
#   influxdb-baseline :8086    influxdb-filtered  :8087
#   telegraf-baseline           telegraf-filtered
#   grafana           :3000
#
# Grafana: http://<this-host>:3000   admin/admin
# InfluxDB baseline:  http://<this-host>:8086   admin/adminpassword123
# InfluxDB filtered:  http://<this-host>:8087   admin/adminpassword123

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="$REPO/docker/evelyn-compare"

cd "$COMPOSE_DIR"

echo "Starting Evelyn compare stack..."
docker compose up -d

echo ""
echo "Waiting for services to be healthy..."
for i in $(seq 1 60); do
    sleep 2
    HEALTHY=$(docker compose ps --format json 2>/dev/null \
        | python3 -c "
import sys, json
lines = sys.stdin.read().strip()
# docker compose ps --format json outputs one JSON object per line
services = [json.loads(l) for l in lines.splitlines() if l.strip()]
total = len(services)
up = sum(1 for s in services if s.get('Health','') in ('healthy','') and s.get('State','') == 'running')
print(f'{up}/{total}')
" 2>/dev/null || echo "?/?")
    echo -n "  services up: $HEALTHY  "
    if curl -sf http://localhost:8086/ping > /dev/null 2>&1 && \
       curl -sf http://localhost:8087/ping > /dev/null 2>&1; then
        echo "— InfluxDB ready"
        break
    fi
    echo ""
done

echo ""
echo "Stack ready."
echo ""
echo "  Grafana:            http://$(hostname -I | awk '{print $1}'):3000  (admin/admin)"
echo "  InfluxDB baseline:  http://$(hostname -I | awk '{print $1}'):8086  (ems-baseline bucket)"
echo "  InfluxDB filtered:  http://$(hostname -I | awk '{print $1}'):8087  (ems-filtered bucket)"
echo "  FlashMQ baseline:   $(hostname -I | awk '{print $1}'):11883"
echo "  FlashMQ filtered:   $(hostname -I | awk '{print $1}'):11884"
echo ""
echo "Start generators (on gx10 or this machine):"
MYIP=$(hostname -I | awk '{print $1}')
echo "  bash scripts/evelyn-compare-generators.sh --docker-host $MYIP"
