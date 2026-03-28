#!/usr/bin/env bash
# start_46_influx.sh — start InfluxDB + Telegraf + generator on phil-dev (.46)
#
# Uses the existing FlashMQ systemd service on :1883.
# Starts ONE influxd instance (port 8096, bucket ems-baseline) for config validation.
# Add --filtered flag to also start the filtered path (port 8097, bucket ems-filtered).
#
# Binaries:  /usr/bin/influxd  /usr/bin/telegraf  /usr/bin/influx
# Data:      ~/data/influx-46/{base,filt}/
# Logs:      ~/logs/
#
# Usage:
#   bash scripts/start_46_influx.sh              # baseline only
#   bash scripts/start_46_influx.sh --filtered   # baseline + filtered
#   bash scripts/start_46_influx.sh --no-gen     # no generator (Telegraf only)

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="$HOME/logs"
mkdir -p "$LOGS"

INFLUXD_BIN="/usr/bin/influxd"
INFLUX_BIN="/usr/bin/influx"
TELEGRAF_BIN="/usr/bin/telegraf"
STRESS_BIN="$REPO/source/ems_site_simulator/ems_site_simulator"
STRESS_TPL="$REPO/source/ems_site_simulator/ems_topic_template.json"

FILTERED=false
START_GEN=true
RATE=175
UNITS=1

ALL_UNIT_IDS=(
  0215F562 0215F5A0 0215F5BF 0215F5CF 0215F5D6 0215F5DD 0215F5DF 0215F5E3
  0215F5E7 0215F5F5 0215F5FE 0215F60B 0215F60D 0215F625 0215F63C 0215F662
  0215F6B1 0215F6BF 0215F6FD 0215F708 0215F70F 0215F734 0215F849 0215F87A
  0227C829 0227C82A 0227C82B 0227C838 0227C842 0227C84E 0227C86B 0227C86E
  02281632 02281634 02281636 02281646 02281656 0228165D 0228165E 02281660
  02281663 02281683 02281684 0228168A 0228168E 02285B20
)

while [[ $# -gt 0 ]]; do
  case $1 in
    --filtered)  FILTERED=true;        shift ;;
    --no-gen)    START_GEN=false;      shift ;;
    --rate)      RATE="$2";            shift 2 ;;
    --rate=*)    RATE="${1#*=}";       shift ;;
    --units)     UNITS="$2";           shift 2 ;;
    --units=*)   UNITS="${1#*=}";      shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# ── sanity checks ────────────────────────────────────────────────────────────
for bin in "$INFLUXD_BIN" "$TELEGRAF_BIN" "$INFLUX_BIN"; do
  [[ -f "$bin" ]] || { echo "ERROR: missing $bin"; exit 1; }
done
if $START_GEN; then
  for f in "$STRESS_BIN" "$STRESS_TPL"; do
    [[ -f "$f" ]] || { echo "ERROR: missing $f"; exit 1; }
  done
fi
systemctl is-active --quiet flashmq || { echo "ERROR: flashmq not running — sudo systemctl start flashmq"; exit 1; }

# ── stop any existing 46-influx processes ────────────────────────────────────
echo "Stopping any existing 46-influx processes..."
pkill -f "influxd.*influx-46-base"  2>/dev/null && echo "  stopped influxd-base"  || true
pkill -f "influxd.*influx-46-filt"  2>/dev/null && echo "  stopped influxd-filt"  || true
pkill -f "telegraf.*telegraf-46"    2>/dev/null && echo "  stopped telegraf"       || true
pkill -f "ems_site_simulator.*1883"    2>/dev/null && echo "  stopped generator"      || true
sleep 1

# ── data dirs ────────────────────────────────────────────────────────────────
mkdir -p "$HOME/data/influx-46/base"
$FILTERED && mkdir -p "$HOME/data/influx-46/filt"

# ── write Telegraf configs ────────────────────────────────────────────────────

cat > /tmp/telegraf-46-base.conf <<'EOF'
# Baseline: all signals, no filtering
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
  servers            = ["tcp://localhost:1883"]
  topics             = ["ems/#"]
  qos                = 0
  client_id          = "telegraf-46-base"
  connection_timeout = "30s"
  keepalive          = 120
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

    # 7-seg: ems/site/{site}/{meas}/{dev}/{point}/{dtype}
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

# Pivot data_type tag → typed fields (float, integer, boolean_integer)
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

if $FILTERED; then
cat > /tmp/telegraf-46-filt.conf <<EOF
# Filtered: float_dedup execd processor + pivot
#
# float_dedup handles ALL dtype filtering in one pass (order=1):
#   Float   — deadband COV: pass if |new-last| >= epsilon OR heartbeat elapsed.
#             Per-signal epsilon rules in float_dedup.toml tuned from real data.
#   Integer — exact-match dedup (same as processors.dedup).
#   Boolean — exact-match dedup.
#   String  — exact-match dedup: only forward on value change, no heartbeat.
#             Strings (status enums, firmware versions, mode labels) change
#             rarely; suppressing repeats saves TSM writes without data loss.
#             No need to drop strings at input any more — dedup handles them.
#   Binary: $REPO/source/telegraf_plugins/float_dedup/float_dedup
#   Config: $REPO/source/telegraf_plugins/float_dedup/float_dedup.toml

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
  servers            = ["tcp://localhost:1883"]
  topics             = ["ems/#"]
  qos                = 0
  client_id          = "telegraf-46-filt"
  connection_timeout = "30s"
  keepalive          = 120
  data_format        = "json_v2"

  # NOTE: strings are no longer dropped at input — float_dedup handles them
  # with exact-match dedup, so only value-change events reach InfluxDB.
  # OLD Filter A (tagdrop) kept here for reference:
  #   [inputs.mqtt_consumer.tagdrop]
  #     data_type = ["string"]

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

    # 7-seg: ems/site/{site}/{meas}/{dev}/{point}/{dtype}
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

# ── float_dedup execd processor (order=1) ────────────────────────────────────
# Replaces starlark float×100 + processors.dedup + Filter A tagdrop.
# Handles floats (deadband), integers (exact), booleans (exact), strings (exact).
#
# OLD APPROACH (kept for reference):
#   Starlark multiplied floats by 100 and retagged as float_i so that
#   processors.dedup could do exact-match comparison.
#   This only suppressed floats with change < 0.01 in reported units.
#   Analysis of real Evelyn data: only ~17% of float rows benefited because
#   high-frequency signals (HS-CONTROL-*, kW, kVAR, voltages) have typical
#   step sizes of 5–125, making a fixed 0.01 epsilon nearly useless.
#   Strings were dropped entirely at input (Filter A tagdrop) — this also
#   meant string change events were lost rather than being stored on change.
#
#   [[processors.starlark]]
#     order = 0
#     source = '''
#   def apply(metric):
#       if metric.tags.get("data_type") != "float":
#           return metric
#       v = metric.fields.get("value")
#       if v == None:
#           return metric
#       metric.fields["value"] = int(float(v) * 100)
#       metric.tags["data_type"] = "float_i"
#       return metric
#   '''
#
#   [[processors.dedup]]
#     order          = 1
#     dedup_interval = "60s"
#     [processors.dedup.tagpass]
#       data_type = ["integer", "boolean_integer", "float_i"]
#
# NEW APPROACH: float_dedup execd
#   Per-signal epsilon rules tuned from real Evelyn data distribution.
#   Each point_name is matched against glob patterns in float_dedup.toml.
#   e.g. HS-CONTROL-AGC → epsilon=10.0  (median diff ~125, suppress sub-10 jitter)
#        kW             → epsilon=5.0   (median diff ~31 kW)
#        Max Cell Voltage → epsilon=0.005 (quantised to 0.001 V, 5mV threshold)
[[processors.execd]]
  command       = ["$REPO/source/telegraf_plugins/float_dedup/float_dedup",
                   "--config", "$REPO/source/telegraf_plugins/float_dedup/float_dedup.toml"]
  restart_delay = "5s"
  data_format   = "influx"
  order         = 1

# Pivot data_type tag → typed fields — runs after float_dedup (order=2).
# Converts the data_type tag value into a field name so InfluxDB stores
# typed columns: integer, boolean_integer, float (native, not scaled).
# Note: with float_dedup, floats are passed through unchanged (no ÷100 needed
# on read — values in InfluxDB are the original engineering-unit values).
[[processors.pivot]]
  order     = 2
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
fi

echo "Telegraf config(s) written."

# ── start InfluxDB ────────────────────────────────────────────────────────────
nohup "$INFLUXD_BIN" \
    --bolt-path  "$HOME/data/influx-46/base/influxd.bolt" \
    --engine-path "$HOME/data/influx-46/base/engine" \
    --http-bind-address :8096 \
    > "$LOGS/influxd-46-base.log" 2>&1 &
echo "influxd-base   pid=$!  port=8096  log=$LOGS/influxd-46-base.log"

if $FILTERED; then
  nohup "$INFLUXD_BIN" \
      --bolt-path  "$HOME/data/influx-46/filt/influxd.bolt" \
      --engine-path "$HOME/data/influx-46/filt/engine" \
      --http-bind-address :8097 \
      > "$LOGS/influxd-46-filt.log" 2>&1 &
  echo "influxd-filt   pid=$!  port=8097  log=$LOGS/influxd-46-filt.log"
fi

echo ""
echo "Waiting for InfluxDB to be ready..."
for port in 8096 $( $FILTERED && echo 8097 || true ); do
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:$port/ping" > /dev/null 2>&1; then
      echo "  influxdb :$port ready"
      break
    fi
    sleep 1
  done
done

# ── setup InfluxDB (idempotent) ───────────────────────────────────────────────
"$INFLUX_BIN" setup \
    --host http://localhost:8096 \
    --username admin --password adminpassword123 \
    --org ems-org --bucket ems-baseline \
    --token ems-token-secret --force \
    -n base 2>/dev/null && echo "influxdb-base setup done" || echo "influxdb-base already set up (ok)"

if $FILTERED; then
  "$INFLUX_BIN" setup \
      --host http://localhost:8097 \
      --username admin --password adminpassword123 \
      --org ems-org --bucket ems-filtered \
      --token ems-token-secret --force \
      -n filt 2>/dev/null && echo "influxdb-filt setup done" || echo "influxdb-filt already set up (ok)"
fi

# ── start Telegraf ────────────────────────────────────────────────────────────
nohup "$TELEGRAF_BIN" --config /tmp/telegraf-46-base.conf \
    > "$LOGS/telegraf-46-base.log" 2>&1 &
echo "telegraf-base  pid=$!  log=$LOGS/telegraf-46-base.log"

if $FILTERED; then
  nohup "$TELEGRAF_BIN" --config /tmp/telegraf-46-filt.conf \
      > "$LOGS/telegraf-46-filt.log" 2>&1 &
  echo "telegraf-filt  pid=$!  log=$LOGS/telegraf-46-filt.log"
fi

sleep 2

# ── start generator ───────────────────────────────────────────────────────────
if $START_GEN; then
  UNIT_ID_ARGS=()
  for uid in "${ALL_UNIT_IDS[@]:0:$UNITS}"; do UNIT_ID_ARGS+=(--unit-id "$uid"); done

  nohup "$STRESS_BIN" \
      --host localhost --port 1883 \
      --id ems-46-stress \
      --template "$STRESS_TPL" \
      --rate "$RATE" \
      "${UNIT_ID_ARGS[@]}" \
      > "$LOGS/stress-46.log" 2>&1 &
  echo "generator      pid=$!  port=1883  rate=$RATE  units=$UNITS  log=$LOGS/stress-46.log"
fi

echo ""
echo "Stack up on .46:"
echo "  InfluxDB baseline:  http://localhost:8096  (http://192.168.86.46:8096)"
echo "  InfluxDB filtered:  http://localhost:8097  (only with --filtered)"
echo "  Telegraf log:       $LOGS/telegraf-46-base.log"
echo "  Generator log:      $LOGS/stress-46.log"
echo ""
echo "  Viewer:  http://192.168.86.46:8080/writer/influx_viewer.html"
echo "  (host=192.168.86.46  port=8096  token=ems-token-secret)"
echo ""
echo "Stop: bash scripts/stop_46_influx.sh"
