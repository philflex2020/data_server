#!/usr/bin/env bash
# evelyn-compare-native-start.sh — start the Evelyn cost-comparison stack natively (no Docker)
#
# Run on gx10 (192.168.86.48).  Requires:
#   - /tmp/FlashMQ/build/flashmq   (built from source: cmake/make in /tmp/FlashMQ)
#   - /usr/local/bin/telegraf       (extracted from Docker image or installed)
#   - /usr/local/bin/influxd        (extracted from Docker image or installed)
#   - /usr/local/bin/influx         (extracted from Docker image or installed)
#   - ems_site_simulator already built (make ems_site_simulator)
#
# Two parallel ingest paths — identical generators, filtering is Telegraf-side:
#
#   Baseline  FlashMQ :11888  →  Telegraf (all signals)      →  InfluxDB :8096  ems-baseline
#   Filtered  FlashMQ :11889  →  Telegraf (Filter A+B)       →  InfluxDB :8097  ems-filtered
#
# Filter A: drop string dtype (tagdrop on output)
# Filter B: dedup integer/boolean_integer for 60s (processors.dedup with tagpass)
#
# Generators: 1 unit (0215F562), 175 msg/s — enough to show Filter B dedup in action.
# Increase --rate and add more --unit-id args to scale up.
#
# Usage:
#   bash scripts/evelyn-compare-native-start.sh
#   bash scripts/evelyn-compare-native-start.sh --rate 1770 --units 1   (default)
#   bash scripts/evelyn-compare-native-start.sh --rate 81420 --units 46 (full Evelyn load)

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="/data/logs/$(date +%Y/%m/%d)"
mkdir -p "$LOGS" 2>/dev/null || { mkdir -p "$HOME/logs"; LOGS="$HOME/logs"; }

FLASHMQ_BIN="/tmp/FlashMQ/build/flashmq"
TELEGRAF_BIN="/usr/local/bin/telegraf"
INFLUXD_BIN="/usr/local/bin/influxd"
INFLUX_BIN="/usr/local/bin/influx"
STRESS_BIN="$REPO/source/ems_site_simulator/ems_site_simulator"
STRESS_TPL="$REPO/source/ems_site_simulator/ems_topic_template.json"

RATE=175
UNITS=1   # number of unit IDs to use (from UNIT_IDS array below)

# ── argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --rate)    RATE="$2";  shift 2 ;;
        --rate=*)  RATE="${1#*=}"; shift ;;
        --units)   UNITS="$2"; shift 2 ;;
        --units=*) UNITS="${1#*=}"; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ── sanity checks ─────────────────────────────────────────────────────────────
for bin in "$FLASHMQ_BIN" "$TELEGRAF_BIN" "$INFLUXD_BIN" "$STRESS_BIN" "$STRESS_TPL"; do
    [[ -f "$bin" ]] || { echo "ERROR: missing $bin"; exit 1; }
done

# ── stop any existing compare processes ───────────────────────────────────────
echo "Stopping any existing compare processes..."
pkill -f "flashmq.*fmq.conf"          2>/dev/null && echo "  stopped flashmq"           || true
pkill -f "telegraf.*telegraf-native-base" 2>/dev/null && echo "  stopped telegraf-base" || true
pkill -f "telegraf.*telegraf-native-filt" 2>/dev/null && echo "  stopped telegraf-filt" || true
pkill -f "influxd.*influx-base"        2>/dev/null && echo "  stopped influxd-base"     || true
pkill -f "influxd.*influx-filt"        2>/dev/null && echo "  stopped influxd-filt"     || true
pkill -f "ems_site_simulator"             2>/dev/null && echo "  stopped generator"        || true
sleep 1

# ── write config files to /tmp ────────────────────────────────────────────────
cat > /tmp/fmq.conf <<'EOF'
listen {
    port 11888
    protocol mqtt
}
allow_anonymous true
max_packet_size 65536
client_max_write_buffer_size 67108864
EOF

cat > /tmp/telegraf-native-base.conf <<'EOF'
[agent]
  interval              = "1s"
  round_interval        = true
  metric_batch_size     = 5000
  metric_buffer_limit   = 500000
  flush_interval        = "5s"
  flush_jitter          = "1s"
  precision             = "1ms"
  omit_hostname         = true

[[inputs.mqtt_consumer]]
  servers            = ["tcp://localhost:11888"]
  topics             = ["ems/#"]
  qos                = 0
  client_id          = "telegraf-compare-base"
  connection_timeout = "30s"
  data_format        = "json_v2"

  [[inputs.mqtt_consumer.json_v2]]
    timestamp_path     = "ts"
    timestamp_format   = "2006-01-02T15:04:05.000Z"
    timestamp_timezone = "UTC"

    [[inputs.mqtt_consumer.json_v2.field]]
      path = "value"

    # 9-seg: ems/site/{site}/unit/{uid}/{device}/{instance}/{point}/{dtype}
    [[inputs.mqtt_consumer.topic_parsing]]
      topic       = "ems/site/+/unit/+/+/+/+/+"
      tags        = "_/_/site_id/_/unit_id/device/instance/point_name/data_type"
      measurement = "_/_/_/measurement/_/_/_/_/_"

    # 7-seg: ems/site/{site}/{meas}/{dev}/{point}/{dtype}  (meter/rtac)
    [[inputs.mqtt_consumer.topic_parsing]]
      topic       = "ems/site/+/+/+/+/+"
      tags        = "_/_/site_id/_/device/point_name/data_type"
      measurement = "_/_/_/measurement/_/_/_"

    # 6-seg: ems/site/{site}/root/{point}/{dtype}
    [[inputs.mqtt_consumer.topic_parsing]]
      topic       = "ems/site/+/root/+/+"
      tags        = "_/_/site_id/_/point_name/data_type"
      measurement = "_/measurement/_/_/_/_"

[[inputs.internal]]
  collect_memstats = false

# Pivot data_type tag → typed fields (float, integer, boolean_integer, string)
[[processors.pivot]]
  order     = 1
  tag_key   = "data_type"
  value_key = "value"

[[outputs.influxdb_v2]]
  urls         = ["http://localhost:8096"]
  token        = "ems-token-secret"
  organization = "ems-org"
  bucket       = "ems-baseline"
  timeout      = "10s"
  namedrop     = ["internal_*"]
EOF

cat > /tmp/telegraf-native-filt.conf <<'EOF'
# Filter A: drop string data_type at input (~4% reduction)
# Filter B: dedup integer/boolean_integer for 60s (~23% reduction)
# Combined: ~26.8% Conservative scenario
# Schema aligned with Longbow production telegraf-conf-v2-cloud.conf

[agent]
  interval              = "1s"
  round_interval        = true
  metric_batch_size     = 5000
  metric_buffer_limit   = 500000
  flush_interval        = "5s"
  flush_jitter          = "1s"
  precision             = "1ms"
  omit_hostname         = true

[[inputs.mqtt_consumer]]
  servers            = ["tcp://localhost:11888"]
  topics             = ["ems/#"]
  qos                = 0
  client_id          = "telegraf-compare-filt"
  connection_timeout = "30s"
  data_format        = "json_v2"

  # Filter A: drop string dtype before any processing
  [inputs.mqtt_consumer.tagdrop]
    data_type = ["string"]

  [[inputs.mqtt_consumer.json_v2]]
    timestamp_path     = "ts"
    timestamp_format   = "2006-01-02T15:04:05.000Z"
    timestamp_timezone = "UTC"

    [[inputs.mqtt_consumer.json_v2.field]]
      path = "value"

    # 9-seg: ems/site/{site}/unit/{uid}/{device}/{instance}/{point}/{dtype}
    [[inputs.mqtt_consumer.topic_parsing]]
      topic       = "ems/site/+/unit/+/+/+/+/+"
      tags        = "_/_/site_id/_/unit_id/device/instance/point_name/data_type"
      measurement = "_/_/_/measurement/_/_/_/_/_"

    # 7-seg: ems/site/{site}/{meas}/{dev}/{point}/{dtype}  (meter/rtac)
    [[inputs.mqtt_consumer.topic_parsing]]
      topic       = "ems/site/+/+/+/+/+"
      tags        = "_/_/site_id/_/device/point_name/data_type"
      measurement = "_/_/_/measurement/_/_/_"

    # 6-seg: ems/site/{site}/root/{point}/{dtype}
    [[inputs.mqtt_consumer.topic_parsing]]
      topic       = "ems/site/+/root/+/+"
      tags        = "_/_/site_id/_/point_name/data_type"
      measurement = "_/measurement/_/_/_/_"

[[inputs.internal]]
  collect_memstats = false

# Filter B: dedup integer + boolean signals (pass-through others unmodified)
[[processors.dedup]]
  order          = 0
  dedup_interval = "60s"
  [processors.dedup.tagpass]
    data_type = ["integer", "boolean_integer"]

# Pivot data_type tag → typed fields — runs after dedup
[[processors.pivot]]
  order     = 1
  tag_key   = "data_type"
  value_key = "value"

[[outputs.influxdb_v2]]
  urls         = ["http://localhost:8097"]
  token        = "ems-token-secret"
  organization = "ems-org"
  bucket       = "ems-filtered"
  timeout      = "10s"
  namedrop     = ["internal_*"]
EOF

echo "Config files written to /tmp."

# ── ensure InfluxDB data dirs exist ───────────────────────────────────────────
mkdir -p /data/influx-base /data/influx-filt

# ── start FlashMQ ─────────────────────────────────────────────────────────────
nohup "$FLASHMQ_BIN" --config-file /tmp/fmq.conf \
    > "$LOGS/compare_flashmq.log" 2>&1 &
echo "flashmq        pid=$!  port=11888  log=$LOGS/compare_flashmq.log"

sleep 1

# ── start InfluxDB ────────────────────────────────────────────────────────────
nohup "$INFLUXD_BIN" \
    --bolt-path  /data/influx-base/influxd.bolt \
    --engine-path /data/influx-base/engine \
    --http-bind-address :8096 \
    > "$LOGS/compare_influxd_base.log" 2>&1 &
echo "influxd-base   pid=$!  port=8096   log=$LOGS/compare_influxd_base.log"

nohup "$INFLUXD_BIN" \
    --bolt-path  /data/influx-filt/influxd.bolt \
    --engine-path /data/influx-filt/engine \
    --http-bind-address :8097 \
    > "$LOGS/compare_influxd_filt.log" 2>&1 &
echo "influxd-filt   pid=$!  port=8097   log=$LOGS/compare_influxd_filt.log"

echo ""
echo "Waiting for InfluxDB to be ready..."
for port in 8096 8097; do
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:$port/ping" > /dev/null 2>&1; then
            echo "  influxdb :$port ready"
            break
        fi
        sleep 1
    done
done

# ── setup InfluxDB (idempotent — skips if already set up) ─────────────────────
"$INFLUX_BIN" setup \
    --host http://localhost:8096 \
    --username admin --password adminpassword123 \
    --org ems-org --bucket ems-baseline \
    --token ems-token-secret --force \
    -n base 2>/dev/null && echo "influxdb-base setup done" || echo "influxdb-base already set up (ok)"

"$INFLUX_BIN" setup \
    --host http://localhost:8097 \
    --username admin --password adminpassword123 \
    --org ems-org --bucket ems-filtered \
    --token ems-token-secret --force \
    -n filt 2>/dev/null && echo "influxdb-filt setup done" || echo "influxdb-filt already set up (ok)"

# ── start Telegraf ────────────────────────────────────────────────────────────
nohup "$TELEGRAF_BIN" --config /tmp/telegraf-native-base.conf \
    > "$LOGS/compare_telegraf_base.log" 2>&1 &
echo "telegraf-base  pid=$!  log=$LOGS/compare_telegraf_base.log"

nohup "$TELEGRAF_BIN" --config /tmp/telegraf-native-filt.conf \
    > "$LOGS/compare_telegraf_filt.log" 2>&1 &
echo "telegraf-filt  pid=$!  log=$LOGS/compare_telegraf_filt.log"

sleep 2

# ── start generators ──────────────────────────────────────────────────────────
# All 46 real Evelyn unit IDs
ALL_UNIT_IDS=(
    0215F562 0215F5A0 0215F5BF 0215F5CF 0215F5D6 0215F5DD 0215F5DF 0215F5E3
    0215F5E7 0215F5F5 0215F5FE 0215F60B 0215F60D 0215F625 0215F63C 0215F662
    0215F6B1 0215F6BF 0215F6FD 0215F708 0215F70F 0215F734 0215F849 0215F87A
    0227C829 0227C82A 0227C82B 0227C838 0227C842 0227C84E 0227C86B 0227C86E
    02281632 02281634 02281636 02281646 02281656 0228165D 0228165E 02281660
    02281663 02281683 02281684 0228168A 0228168E 02285B20
)

# Slice to requested number of units
UNIT_IDS=("${ALL_UNIT_IDS[@]:0:$UNITS}")
UNIT_ID_ARGS=()
for uid in "${UNIT_IDS[@]}"; do UNIT_ID_ARGS+=(--unit-id "$uid"); done

echo ""
echo "Starting generators: rate=$RATE  units=$UNITS"

nohup "$STRESS_BIN" \
    --host localhost --port 11888 \
    --ws-port 8769 \
    --template "$STRESS_TPL" \
    --rate "$RATE" \
    --site-id "0215D1D8" \
    "${UNIT_ID_ARGS[@]}" \
    > "$LOGS/compare_gen.log" 2>&1 &
echo "generator      pid=$!  port=11888  ws=8769  log=$LOGS/compare_gen.log"

echo ""
echo "All processes started."
echo ""
echo "  InfluxDB baseline:  http://$(hostname -I | awk '{print $1}'):8096  bucket=ems-baseline"
echo "  InfluxDB filtered:  http://$(hostname -I | awk '{print $1}'):8097  bucket=ems-filtered"
echo "  FlashMQ baseline:   localhost:11888"
echo "  FlashMQ filtered:   localhost:11889"
echo ""
echo "  Live compare UI:    http://192.168.86.46:8080/writer/evelyn_live_compare.html"
echo "  (set host to $(hostname -I | awk '{print $1}'), ports 8096 / 8097)"
echo ""
echo "Stop: bash scripts/evelyn-compare-native-stop.sh"
