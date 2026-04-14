#!/usr/bin/env bash
# gx10-evelyn-writers-only-start.sh — start 4 real_writers without simulators
#
# Use when real EMS hardware (or an external publisher) is already sending
# data to the broker on topics A/unit/#, B/unit/#, C/unit/#, D/unit/#.
#
# Writers only — no ems_site_simulator launched.
#
# Health ports:  8771 (A)  8772 (B)  8773 (C)  8774 (D)
# Parquet data:  /data/parquet-evelyn/{a,b,c,d}/
# Logs:          /data/logs/YYYY/MM/DD/real_writer_{a,b,c,d}.log

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="/data/logs/$(date +%Y/%m/%d)"
mkdir -p "$LOGS"

WRITER_BIN="$REPO/source/parquet_writer_cpp/real_writer"

# ── sanity checks ─────────────────────────────────────────────────────────
[[ -f "$WRITER_BIN" ]] || { echo "ERROR: missing $WRITER_BIN — run make first"; exit 1; }
for s in a b c d; do
    cfg="$REPO/source/parquet_writer_cpp/config.gx10-evelyn-${s}.yaml"
    [[ -f "$cfg" ]] || { echo "ERROR: missing $cfg"; exit 1; }
done

# ── stop any existing writer instances ────────────────────────────────────
pkill -f "real_writer.*config.gx10-evelyn" 2>/dev/null && echo "stopped old real_writer(s)" || true
sleep 1

# ── start 4 writers ───────────────────────────────────────────────────────
for SIM in a b c d; do
    CFG="$REPO/source/parquet_writer_cpp/config.gx10-evelyn-${SIM}.yaml"
    nohup "$WRITER_BIN" --config "$CFG" \
        > "$LOGS/real_writer_${SIM}.log" 2>&1 &
    echo "real_writer-${SIM}  pid=$!  log=$LOGS/real_writer_${SIM}.log"
done

# ── wait for all health endpoints ─────────────────────────────────────────
echo ""
echo -n "waiting for health endpoints"
for i in $(seq 1 20); do
    sleep 1
    ALL_OK=1
    for P in 8771 8772 8773 8774; do
        curl -sf "http://localhost:${P}/health" > /dev/null 2>&1 || ALL_OK=0
    done
    if [[ $ALL_OK == 1 ]]; then
        echo " ok"
        for P in 8771 8772 8773 8774; do
            echo "--- :${P} ---"
            curl -s "http://localhost:${P}/health" | python3 -m json.tool 2>/dev/null \
                || curl -s "http://localhost:${P}/health"
        done
        exit 0
    fi
    echo -n "."
done
echo " timeout — check $LOGS/"
exit 1
