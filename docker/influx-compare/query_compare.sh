#!/bin/bash
# query_compare.sh — side-by-side Flux query timing: flux_duck vs InfluxDB
#
# Prerequisites:
#   - InfluxDB running on :8096
#   - flux_duck running on :8087  (bash start_flux_duck.sh)
#
# Usage:
#   bash query_compare.sh [--unit 0215F6BD] [--point AvgCellT]

INFLUX_BASE="http://localhost:8096"
INFLUX_TOKEN="ems-token-secret"
INFLUX_ORG="ems-org"
INFLUX_BUCKET="ems-data"
INFLUX_URL="${INFLUX_BASE}/api/v2/query?org=${INFLUX_ORG}"

FLUXDUCK_URL="http://localhost:8087/api/v2/query"

UNIT="${1:-0215F6BD}"
POINT="${2:-AvgCellT}"

# override with named args
while [[ $# -gt 0 ]]; do
    case $1 in
        --unit)  UNIT="$2";  shift 2 ;;
        --point) POINT="$2"; shift 2 ;;
        *) shift ;;
    esac
done

echo "════════════════════════════════════════════════════════════"
echo "  query_compare  $(date -Iseconds)"
echo "  unit=$UNIT  point=$POINT"
echo "════════════════════════════════════════════════════════════"

# ── helper: time a Flux query against an endpoint ────────────────────────────
# usage: run_query LABEL FULL_URL FLUX_BODY [extra curl args...]
run_query() {
    local label="$1" url="$2" flux="$3"
    shift 3
    local extra_headers=("$@")

    local t0 t1 elapsed rows

    t0=$(date +%s%3N)
    local result
    result=$(curl -s --max-time 30 \
        -X POST "$url" \
        -H "Content-Type: application/vnd.flux" \
        "${extra_headers[@]}" \
        --data "$flux" 2>/dev/null)
    t1=$(date +%s%3N)
    elapsed=$(( t1 - t0 ))

    rows=$(echo "$result" | grep -v '^#' | grep -v '^,$' | grep -c ',')
    printf "  %-20s  %6d ms  %6d rows\n" "$label" "$elapsed" "$rows"
}

influx_headers=(
    -H "Authorization: Token $INFLUX_TOKEN"
    -H "Accept: application/csv"
)

echo ""
echo "── Q1: single series, last 1h ───────────────────────────────"

FD_Q1="from(bucket:\"ems\")
  |> range(start: -1h)
  |> filter(fn: (r) => r[\"unit_id\"] == \"$UNIT\")
  |> filter(fn: (r) => r[\"point_name\"] == \"$POINT\")"

# InfluxDB: unit_controller_id tag, _field filter for float
IN_Q1="from(bucket:\"$INFLUX_BUCKET\")
  |> range(start: -1h)
  |> filter(fn: (r) => r[\"unit_controller_id\"] == \"$UNIT\")
  |> filter(fn: (r) => r[\"point_name\"] == \"$POINT\")
  |> filter(fn: (r) => r[\"_field\"] == \"float\")"

run_query "flux_duck"  "$FLUXDUCK_URL" "$FD_Q1"
run_query "influxdb"   "$INFLUX_URL"   "$IN_Q1" "${influx_headers[@]}"

echo ""
echo "── Q2: single series, 5m averages, last 1h ─────────────────"

FD_Q2="$FD_Q1
  |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)"

IN_Q2="$IN_Q1
  |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)"

run_query "flux_duck"  "$FLUXDUCK_URL" "$FD_Q2"
run_query "influxdb"   "$INFLUX_URL"   "$IN_Q2" "${influx_headers[@]}"

echo ""
echo "── Q3: all float series for unit, last 1h ───────────────────"

FD_Q3="from(bucket:\"ems\")
  |> range(start: -1h)
  |> filter(fn: (r) => r[\"unit_id\"] == \"$UNIT\")"

IN_Q3="from(bucket:\"$INFLUX_BUCKET\")
  |> range(start: -1h)
  |> filter(fn: (r) => r[\"unit_controller_id\"] == \"$UNIT\")
  |> filter(fn: (r) => r[\"_field\"] == \"float\")"

run_query "flux_duck"  "$FLUXDUCK_URL" "$FD_Q3"
run_query "influxdb"   "$INFLUX_URL"   "$IN_Q3" "${influx_headers[@]}"

echo ""
echo "── Q4: all float series for unit, hourly mean, last 6h ──────"

FD_Q4="from(bucket:\"ems\")
  |> range(start: -6h)
  |> filter(fn: (r) => r[\"unit_id\"] == \"$UNIT\")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)"

IN_Q4="from(bucket:\"$INFLUX_BUCKET\")
  |> range(start: -6h)
  |> filter(fn: (r) => r[\"unit_controller_id\"] == \"$UNIT\")
  |> filter(fn: (r) => r[\"_field\"] == \"float\")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)"

run_query "flux_duck"  "$FLUXDUCK_URL" "$FD_Q4"
run_query "influxdb"   "$INFLUX_URL"   "$IN_Q4" "${influx_headers[@]}"

echo ""
echo "── Q5: site-wide fan-out, 1h count ─────────────────────────"

FD_Q5="from(bucket:\"ems\")
  |> range(start: -1h)
  |> count()"

IN_Q5="from(bucket:\"$INFLUX_BUCKET\")
  |> range(start: -1h)
  |> filter(fn: (r) => r[\"_field\"] == \"float\")
  |> count()"

run_query "flux_duck"  "$FLUXDUCK_URL" "$FD_Q5"
run_query "influxdb"   "$INFLUX_URL"   "$IN_Q5" "${influx_headers[@]}"

echo ""
