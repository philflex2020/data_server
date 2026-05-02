#!/bin/bash
# Snapshot CPU/RSS/disk for influxdb-compare + telegraf-compare
# and compare to parquet writers on the same host.
# Run from .34 after ~30 min warm-up.

INFLUX_TOKEN="ems-token-secret"
INFLUX_URL="http://localhost:8096"

echo "════════════════════════════════════════"
echo "  influx-compare  snapshot  $(date -Iseconds)"
echo "════════════════════════════════════════"

# ── Container PIDs ────────────────────────────────────────────────────────────
INFLUX_PID=$(docker inspect --format '{{.State.Pid}}' influxdb-compare 2>/dev/null)
TG_PID=$(docker inspect --format '{{.State.Pid}}' telegraf-compare 2>/dev/null)

if [[ -z "$INFLUX_PID" || -z "$TG_PID" ]]; then
  echo "ERROR: containers not running — docker-compose up -d first"
  exit 1
fi

echo ""
echo "── RSS / CPU (ps) ───────────────────────────────────────────"
printf "%-22s %8s %8s\n" "Process" "RSS_MB" "%CPU"
for entry in "influxdb-compare:$INFLUX_PID" "telegraf-compare:$TG_PID"; do
  name="${entry%%:*}"; pid="${entry##*:}"
  rss_kb=$(ps -o rss= -p "$pid" 2>/dev/null || echo 0)
  cpu=$(ps -o %cpu= -p "$pid" 2>/dev/null || echo 0)
  printf "%-22s %8.1f %8s\n" "$name" "$(echo "$rss_kb / 1024" | bc -l)" "$cpu"
done

echo ""
echo "── parquet_writer RSS / CPU ─────────────────────────────────"
printf "%-22s %8s %8s\n" "Process" "RSS_MB" "%CPU"
for pw_pid in $(pgrep -f parquet_writer); do
  cfg=$(cat /proc/"$pw_pid"/cmdline 2>/dev/null | tr '\0' ' ' | grep -oP '(?<=--config )\S+' | xargs basename 2>/dev/null)
  rss_kb=$(ps -o rss= -p "$pw_pid" 2>/dev/null || echo 0)
  cpu=$(ps -o %cpu= -p "$pw_pid" 2>/dev/null || echo 0)
  printf "%-22s %8.1f %8s\n" "${cfg:-writer-?}" "$(echo "$rss_kb / 1024" | bc -l)" "$cpu"
done

echo ""
INFLUX_DATA=/pool4/fractal/data/influx-compare/influxdb-data
echo "── InfluxDB disk usage ($INFLUX_DATA) ──────────────────────"
du -sh "$INFLUX_DATA/engine/data" 2>/dev/null || echo "  engine/data: (not yet)"
du -sh "$INFLUX_DATA/engine/wal"  2>/dev/null || echo "  engine/wal:  (not yet)"
du -sh "$INFLUX_DATA"             2>/dev/null

echo ""
echo "── parquet_writer rg3 disk usage ───────────────────────────"
for d in /pool4/fractal/data/writer-b/rg3 /pool4/fractal/data/writer-c/rg3; do
  [[ -d "$d" ]] && printf "%-44s %s\n" "$d" "$(du -sh "$d" 2>/dev/null | cut -f1)"
done

echo ""
echo "── Telegraf internal metrics (Prometheus :9273) ─────────────"
curl -s http://localhost:9273/metrics 2>/dev/null \
  | grep -E '^(internal_agent_gather_metrics_gathered|internal_write_metrics_written|internal_write_buffer_size|internal_write_metrics_dropped)' \
  | grep -v '^#' \
  | sort

echo ""
echo "── InfluxDB write point count (last 5 min) ──────────────────"
curl -s -H "Authorization: Token $INFLUX_TOKEN" \
  -H "Content-Type: application/vnd.flux" \
  -H "Accept: application/csv" \
  "$INFLUX_URL/api/v2/query?org=ems-org" \
  --data 'from(bucket:"ems-data") |> range(start: -5m) |> count() |> sum()' 2>/dev/null \
  | grep -v "^#" | awk -F',' 'NR>1 && NF>0 {print "  " $NF " points"}' | tail -1 \
  || echo "  (query failed or no data yet)"

echo ""
echo "── parquet_writer health endpoints ─────────────────────────"
for port in 8781 8784 8785 8787; do
  result=$(curl -s --max-time 2 "http://localhost:$port/health" 2>/dev/null \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
p     = d.get('health_port', $port)
mps   = d.get('msgs_per_sec', '?')
flush = d.get('last_flush_ms', '?')
rg3   = d.get('last_rg3_write_us', 0) // 1000
sort  = d.get('last_rg3_sort_us', 0) // 1000
tbl   = d.get('last_table_build_us', 0) // 1000
rss   = d.get('rss_mb', '?')
print(f'  :{p}  msgs/s={mps:.0f}  flush={flush}ms  rg3={rg3}ms  sort={sort}ms  tbl={tbl}ms  rss={rss}MB')
" 2>/dev/null)
  [[ -n "$result" ]] && echo "$result"
done
echo ""
