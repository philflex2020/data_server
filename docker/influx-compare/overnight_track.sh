#!/bin/bash
# overnight_track.sh — sample writer-b-p4 vs influx every 30 min
# Output: /tmp/overnight_compare.log
# Run:  nohup bash overnight_track.sh > /pool4/fractal/data/influx-compare/overnight_track_stderr.log 2>&1 &

LOG=/pool4/fractal/data/influx-compare/overnight_compare.log
INTERVAL=1800   # 30 minutes

INFLUX_URL="http://localhost:8096"
INFLUX_TOKEN="ems-token-secret"
PARQUET_HEALTH="http://localhost:8785"
RG3_DIR=/pool4/fractal/data/writer-b/rg3
INFLUX_DATA=/pool4/fractal/data/influx-compare/influxdb-data

# ── header (written once on first run) ──────────────────────────────────────
if [[ ! -f "$LOG" ]]; then
    printf "%-22s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s\n" \
        "timestamp" \
        "wb_cpu%" "wb_rss_mb" \
        "influx_cpu%" "influx_rss_mb" \
        "tg_cpu%" "tg_rss_mb" \
        "wb_flush_ms" "wb_sort_ms" "wb_tbl_ms" \
        "rg3_mb" "influx_mb" "influx_bpp" "tsm_files" \
        >> "$LOG"
fi

snapshot() {
    TS=$(date '+%Y-%m-%dT%H:%M:%S')

    # ── writer-b-p4 process ───────────────────────────────────────────────────
    WB_PID=$(pgrep -f "writer-b-p4.yaml" | head -1)
    WB_CPU=$(ps -o %cpu= -p "$WB_PID" 2>/dev/null | awk '{printf "%.1f", $1}')
    WB_RSS=$(ps -o rss=  -p "$WB_PID" 2>/dev/null | awk '{printf "%.0f", $1/1024}')

    # ── writer-b health ───────────────────────────────────────────────────────
    HEALTH=$(curl -s --max-time 3 "$PARQUET_HEALTH/health" 2>/dev/null)
    WB_FLUSH=$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('last_flush_ms',0))" 2>/dev/null)
    WB_SORT=$(echo  "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('last_rg3_sort_us',0)//1000)" 2>/dev/null)
    WB_TBL=$(echo   "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('last_table_build_us',0)//1000)" 2>/dev/null)

    # ── influxdb container ────────────────────────────────────────────────────
    INFLUX_PID=$(docker inspect --format '{{.State.Pid}}' influxdb-compare 2>/dev/null)
    INFLUX_CPU=$(ps -o %cpu= -p "$INFLUX_PID" 2>/dev/null | awk '{printf "%.1f", $1}')
    INFLUX_RSS=$(ps -o rss=  -p "$INFLUX_PID" 2>/dev/null | awk '{printf "%.0f", $1/1024}')

    # ── telegraf container ────────────────────────────────────────────────────
    TG_PID=$(docker inspect --format '{{.State.Pid}}' telegraf-compare 2>/dev/null)
    TG_CPU=$(ps -o %cpu= -p "$TG_PID" 2>/dev/null | awk '{printf "%.1f", $1}')
    TG_RSS=$(ps -o rss=  -p "$TG_PID" 2>/dev/null | awk '{printf "%.0f", $1/1024}')

    # ── parquet rg3 disk (writer-b) ───────────────────────────────────────────
    RG3_KB=$(du -sk "$RG3_DIR" 2>/dev/null | awk '{print $1}')
    RG3_MB=$(awk "BEGIN{printf \"%.0f\", $RG3_KB/1024}")

    # ── InfluxDB disk ─────────────────────────────────────────────────────────
    INFLUX_KB=$(du -sk "$INFLUX_DATA/engine" 2>/dev/null | awk '{print $1}')
    INFLUX_MB=$(awk "BEGIN{printf \"%.0f\", $INFLUX_KB/1024}")

    # ── InfluxDB bytes/point from Prometheus ──────────────────────────────────
    PROM=$(curl -s --max-time 3 "$INFLUX_URL/metrics" 2>/dev/null)
    TSM_BYTES=$(echo "$PROM"  | grep '^storage_tsm_files_disk_bytes{'  | awk -F' ' '{print $2}' | head -1)
    WAL_BYTES=$(echo "$PROM"  | grep '^storage_wal_size{'               | awk -F' ' '{print $2}' | head -1)
    POINTS=$(echo   "$PROM"  | grep '^storage_shard_write_count{'       | awk -F' ' '{print $2}' | head -1)
    TSM_FILES=$(echo "$PROM" | grep '^storage_tsm_files_total{'         | awk -F' ' '{print $2}' | head -1)
    INFLUX_BPP=$(python3 -c "
tsm=${TSM_BYTES:-0}; wal=${WAL_BYTES:-0}; pts=${POINTS:-1}
print(f'{(tsm+wal)/max(pts,1):.2f}')
" 2>/dev/null)

    printf "%-22s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s  %8s\n" \
        "$TS" \
        "${WB_CPU:--}" "${WB_RSS:--}" \
        "${INFLUX_CPU:--}" "${INFLUX_RSS:--}" \
        "${TG_CPU:--}" "${TG_RSS:--}" \
        "${WB_FLUSH:--}" "${WB_SORT:--}" "${WB_TBL:--}" \
        "${RG3_MB:--}" "${INFLUX_MB:--}" "${INFLUX_BPP:--}" "${TSM_FILES:--}" \
        >> "$LOG"

    echo "[$TS] sampled — rg3=${RG3_MB}MB influx=${INFLUX_MB}MB bpp=${INFLUX_BPP} tsm_files=${TSM_FILES}"
}

echo "overnight_track.sh started — logging to $LOG every ${INTERVAL}s"
snapshot   # immediate first sample

while true; do
    sleep "$INTERVAL"
    snapshot
done
