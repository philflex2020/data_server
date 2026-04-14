#!/usr/bin/env bash
# evelyn-compare-docker-stop.sh — stop the Evelyn cost-comparison Docker stack

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO/docker/evelyn-compare"

docker compose down
echo "Evelyn compare stack stopped."
echo ""
echo "To also remove volumes (wipe InfluxDB data):"
echo "  docker compose down -v"
