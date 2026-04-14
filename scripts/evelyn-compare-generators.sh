#!/usr/bin/env bash
# evelyn-compare-generators.sh — start two EMS stress generators for the compare stack
#
# Generator 1 (baseline) → docker-host:11883  (no filtering in broker)
# Generator 2 (filtered) → docker-host:11884  (filtering happens in Telegraf)
#
# Both generators publish the same full Evelyn data (46 units, 81,420 msg/s).
# The difference in row count comes entirely from Telegraf-side filtering.
#
# Usage:
#   bash scripts/evelyn-compare-generators.sh --docker-host 192.168.86.34
#   bash scripts/evelyn-compare-generators.sh --docker-host localhost
#
# Stop generators:
#   bash scripts/evelyn-compare-generators.sh --stop

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="/data/logs/$(date +%Y/%m/%d)"
mkdir -p "$LOGS" 2>/dev/null || { mkdir -p "$HOME/logs"; LOGS="$HOME/logs"; }

STRESS_BIN="$REPO/source/ems_site_simulator/ems_site_simulator"
STRESS_TPL="$REPO/source/ems_site_simulator/ems_topic_template.json"
DOCKER_HOST="localhost"

# ── argument parsing ────────────────────────────────────────────────────────
STOP=0
for arg in "$@"; do
    case $arg in
        --docker-host) shift; DOCKER_HOST="$1" ;;
        --docker-host=*) DOCKER_HOST="${arg#*=}" ;;
        --stop) STOP=1 ;;
    esac
    shift 2>/dev/null || true
done

if [[ $STOP == 1 ]]; then
    pkill -f "ems_site_simulator.*11883" 2>/dev/null && echo "stopped generator-baseline" || echo "generator-baseline not running"
    pkill -f "ems_site_simulator.*11884" 2>/dev/null && echo "stopped generator-filtered" || echo "generator-filtered not running"
    exit 0
fi

# ── sanity checks ───────────────────────────────────────────────────────────
[[ -f "$STRESS_BIN" ]] || { echo "ERROR: missing $STRESS_BIN — run: cd source/stress_runner && make ems_site_simulator"; exit 1; }
[[ -f "$STRESS_TPL" ]] || { echo "ERROR: missing $STRESS_TPL"; exit 1; }

# Stop any existing compare generators
pkill -f "ems_site_simulator.*11883" 2>/dev/null && echo "stopped old generator-baseline" || true
pkill -f "ems_site_simulator.*11884" 2>/dev/null && echo "stopped old generator-filtered" || true
sleep 1

echo "Target Docker host: $DOCKER_HOST"
echo ""

# All 46 real Evelyn unit IDs
UNIT_IDS=(
    0215F562 0215F5A0 0215F5BF 0215F5CF 0215F5D6 0215F5DD 0215F5DF 0215F5E3
    0215F5E7 0215F5F5 0215F5FE 0215F60B 0215F60D 0215F625 0215F63C 0215F662
    0215F6B1 0215F6BF 0215F6FD 0215F708 0215F70F 0215F734 0215F849 0215F87A
    0227C829 0227C82A 0227C82B 0227C838 0227C842 0227C84E 0227C86B 0227C86E
    02281632 02281634 02281636 02281646 02281656 0228165D 0228165E 02281660
    02281663 02281683 02281684 0228168A 0228168E 02285B20
)
UNIT_ID_ARGS=()
for uid in "${UNIT_IDS[@]}"; do UNIT_ID_ARGS+=(--unit-id "$uid"); done

# ── Generator 1: baseline (→ port 11883) ────────────────────────────────────
# Full Evelyn data — all 46 units, all signal types, 81,420 msg/s
nohup "$STRESS_BIN" \
    --host "$DOCKER_HOST" \
    --port 11883 \
    --template "$STRESS_TPL" \
    --rate 81420 \
    --ws-port 8769 \
    --site-id "0215D1D8" \
    "${UNIT_ID_ARGS[@]}" \
    > "$LOGS/compare_gen_baseline.log" 2>&1 &
echo "generator-baseline  pid=$!  → $DOCKER_HOST:11883  rate=81420  units=46  ws=:8769"
echo "  log: $LOGS/compare_gen_baseline.log"

# ── Generator 2: filtered (→ port 11884) ─────────────────────────────────────
# Identical data — filtering is Telegraf-side, not generator-side.
nohup "$STRESS_BIN" \
    --host "$DOCKER_HOST" \
    --port 11884 \
    --template "$STRESS_TPL" \
    --rate 81420 \
    --ws-port 8779 \
    --site-id "0215D1D8" \
    "${UNIT_ID_ARGS[@]}" \
    > "$LOGS/compare_gen_filtered.log" 2>&1 &
echo "generator-filtered  pid=$!  → $DOCKER_HOST:11884  rate=81420  units=46  ws=:8779"
echo "  log: $LOGS/compare_gen_filtered.log"

echo ""
echo "Both generators running. Grafana: http://$DOCKER_HOST:3000"
echo ""
echo "WebSocket control:"
echo "  Baseline sim: ws://<this-host>:8769"
echo "  Filtered sim: ws://<this-host>:8779"
echo "  UI: http://192.168.86.46:8080/writer/ems_simulator_control.html"
echo ""
echo "Stop generators: bash scripts/evelyn-compare-generators.sh --stop"
