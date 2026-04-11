#!/usr/bin/env bash
# writer_compare_test.sh — set up and run the schema comparison benchmark
# Portable: works on phil-dev (.46) and phil-256g (.34)
#
# Usage:
#   bash scripts/writer_compare_test.sh [--sweeps N] [--cov] [--outdir /path]
#
# On .34: bash scripts/writer_compare_test.sh --outdir /mnt/tort-sdi/bench
# On .46: bash scripts/writer_compare_test.sh  (uses /tmp/bench defaults)

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
SWEEPS=600
COV_FLAG=""
OUTDIR=""
SITE="SITE_A"
CFGDIR="/tmp/writer-compare-cfg"
LOGDIR=""
LOOP=0
INTERVAL=30   # seconds between loop iterations
CSV_FLAG=""
COMPACT_INTERVAL=600   # compactor firing interval (seconds)

# ── arg parse ─────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --sweeps)    SWEEPS="$2";    shift 2 ;;
    --cov)       COV_FLAG="--cov"; shift ;;
    --outdir)    OUTDIR="$2";    shift 2 ;;
    --site)      SITE="$2";      shift 2 ;;
    --logdir)    LOGDIR="$2";    shift 2 ;;
    --loop)      LOOP=1;         shift ;;
    --interval)  INTERVAL="$2";  shift 2 ;;
    --csv)              CSV_FLAG="--csv $2"; shift 2 ;;
    --compact-interval) COMPACT_INTERVAL="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# ── locate repo root (works from any dir) ─────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WRITER="$REPO_ROOT/source/parquet_writer/parquet_writer"

# ── resolve output / log dirs ─────────────────────────────────────────────────
if [[ -z "$OUTDIR" ]]; then
  # auto-detect: use mounted torture drive on .34, /tmp on others
  if mountpoint -q /mnt/tort-sdi 2>/dev/null; then
    OUTDIR="/mnt/tort-sdi/bench"
  elif mountpoint -q /data 2>/dev/null; then
    OUTDIR="/data/bench"
  else
    OUTDIR="/tmp/bench-compare"
  fi
fi

if [[ -z "$LOGDIR" ]]; then
  if [[ -d /data/logs ]]; then
    LOGDIR="/data/logs/$(date +%Y/%m/%d)"
  else
    LOGDIR="$OUTDIR/logs"
  fi
fi

RESULTS_FILE="$OUTDIR/results.json"
LOG_FILE="$LOGDIR/writer_compare_test.log"

mkdir -p "$CFGDIR" "$OUTDIR" "$LOGDIR"

echo "=================================================="
echo "  writer_compare_test"
echo "  repo    : $REPO_ROOT"
echo "  writer  : $WRITER"
echo "  outdir  : $OUTDIR"
echo "  cfgdir  : $CFGDIR"
echo "  results : $RESULTS_FILE"
echo "  log     : $LOG_FILE"
echo "  sweeps  : $SWEEPS  cov: ${COV_FLAG:-off}  compact: ${COMPACT_INTERVAL}s"
echo "=================================================="

# ── 1. git pull ───────────────────────────────────────────────────────────────
echo ""
echo "[1] git pull..."
cd "$REPO_ROOT"
git pull --ff-only || echo "  WARNING: git pull failed (continuing with current code)"

# ── 2. check / build writer ───────────────────────────────────────────────────
echo ""
echo "[2] Checking writer binary..."
if [[ ! -f "$WRITER" ]]; then
  echo "  Binary not found — building..."
  cd "$REPO_ROOT/source/parquet_writer"
  make parquet_writer
  cd "$REPO_ROOT"
else
  SRC="$REPO_ROOT/source/parquet_writer/parquet_writer.cpp"
  if [[ "$SRC" -nt "$WRITER" ]]; then
    echo "  Source newer than binary — rebuilding..."
    cd "$REPO_ROOT/source/parquet_writer"
    make parquet_writer
    cd "$REPO_ROOT"
  else
    echo "  Binary up-to-date: $WRITER"
  fi
fi

# ── 3. python deps ────────────────────────────────────────────────────────────
echo ""
echo "[3] Checking Python deps..."
VENV="$REPO_ROOT/.venv"
if [[ ! -d "$VENV" ]]; then
  echo "  Creating venv..."
  python3 -m venv "$VENV"
fi
PY="$VENV/bin/python3"
PIP="$VENV/bin/pip"

for pkg in paho-mqtt pyarrow pyyaml psutil; do
  if ! "$PY" -c "import ${pkg//-/_}" 2>/dev/null; then
    echo "  Installing $pkg..."
    "$PIP" install --quiet "$pkg"
  else
    echo "  $pkg ok"
  fi
done

# ── 4. ensure MQTT broker running (FlashMQ or mosquitto) ─────────────────────
echo ""
echo "[4] Checking MQTT broker..."
if systemctl is-active --quiet flashmq 2>/dev/null; then
  echo "  FlashMQ active (systemd)"
elif pgrep -x flashmq >/dev/null 2>&1; then
  echo "  FlashMQ running (process)"
elif systemctl is-active --quiet mosquitto 2>/dev/null; then
  echo "  mosquitto active (systemd)"
elif pgrep -x mosquitto >/dev/null 2>&1; then
  echo "  mosquitto running (process)"
else
  echo "  Starting broker..."
  if command -v flashmq >/dev/null 2>&1; then
    flashmq --config /etc/flashmq/flashmq.conf --daemon 2>/dev/null || \
    flashmq --daemon 2>/dev/null || \
    { flashmq &
      sleep 1
      echo "  FlashMQ started (background)" ; }
  elif command -v mosquitto >/dev/null 2>&1; then
    echo phil | sudo -S systemctl start mosquitto 2>/dev/null || \
    { mosquitto -d
      sleep 1
      echo "  mosquitto started (background)" ; }
  else
    echo "  ERROR: no MQTT broker found — install FlashMQ or mosquitto"
    exit 1
  fi
fi
sleep 0.5

# ── 5. copy / generate configs ───────────────────────────────────────────────
echo ""
echo "[5] Writing configs to $CFGDIR..."

cat > "$CFGDIR/config_normalized_long_bench.yaml" << YAML
# Normalized long — one row per signal per sweep
mqtt:
  host: localhost
  port: 1883
  client_id: pw-bench-norm-long
  topic: "bench/#"
  qos: 0
  topic_parser: positional
  topic_segments:
    - "_"
    - unit_id
    - device_type
    - device
    - point_name
    - dtype_hint
  partition_field: site_id
  drop_point_names:
    - BMS_SysHB
    - SysHB
    - Counter
    - ItemDeletionTime
    - TS
output:
  base_path: $OUTDIR/norm-long
  site_id: "$SITE"
  partitions: ["site={site_id}", "{year}", "{month}", "{day}"]
  compression: snappy
  flush_interval_seconds: 60
  max_messages_per_part: 6000
  time_window_ms: 250
  wide_point_name: true
  store_mqtt_topic: false
  store_project_id: false
  store_sample_count: false
compact:
  enabled: true
  interval_seconds: ${COMPACT_INTERVAL}
  min_files: 3
  log_path: $OUTDIR/compact.log
health:
  enabled: false
wal:
  enabled: false
guard:
  min_free_gb: 0.0
YAML

cat > "$CFGDIR/config_long_compound.yaml" << YAML
# Long + compound_point_name — unit_id/device/instance/signal in one column
mqtt:
  host: localhost
  port: 1883
  client_id: pw-bench-long-cmp
  topic: "bench/#"
  qos: 0
  topic_parser: positional
  topic_segments:
    - "_"
    - unit_id
    - device
    - instance
    - point_name
    - dtype_hint
  compound_point_name: [unit_id, device, instance, point_name]
  partition_field: site_id
  drop_point_names:
    - BMS_SysHB
    - SysHB
    - Counter
    - ItemDeletionTime
    - TS
output:
  base_path: $OUTDIR/long-cmp
  site_id: "$SITE"
  partitions: ["site={site_id}", "{year}", "{month}", "{day}"]
  compression: snappy
  flush_interval_seconds: 60
  max_messages_per_part: 6000
  time_window_ms: 250
  wide_point_name: true
  store_mqtt_topic: false
  store_project_id: false
  store_sample_count: false
compact:
  enabled: true
  interval_seconds: ${COMPACT_INTERVAL}
  min_files: 3
  log_path: $OUTDIR/compact.log
health:
  enabled: false
wal:
  enabled: false
guard:
  min_free_gb: 0.0
YAML

cat > "$CFGDIR/config_wide_pivot.yaml" << YAML
# Wide pivot — one column per signal, one row per sweep (coalesced)
mqtt:
  host: localhost
  port: 1883
  client_id: pw-bench-wide
  topic: "bench/#"
  qos: 0
  topic_parser: positional
  topic_segments:
    - "_"
    - unit_id
    - device
    - instance
    - point_name
    - dtype_hint
  compound_field_name: [device, instance, point_name]
  partition_field: unit_id
  drop_point_names:
    - BMS_SysHB
    - SysHB
    - Counter
    - ItemDeletionTime
    - TS
output:
  base_path: $OUTDIR/wide
  site_id: "$SITE"
  partitions: ["{year}", "{month}", "{day}"]
  compression: zstd
  flush_interval_seconds: 60
  max_messages_per_part: 6000
  partition_as_filename_prefix: true
  time_window_ms: 250
  wide_point_name: true
  null_fill_unchanged: true
  store_mqtt_topic: false
  store_project_id: false
  store_sample_count: false
compact:
  enabled: true
  interval_seconds: ${COMPACT_INTERVAL}
  min_files: 3
  log_path: $OUTDIR/compact.log
health:
  enabled: false
wal:
  enabled: false
guard:
  min_free_gb: 0.0
YAML

echo "  configs written"

# ── 6. run benchmark (once or loop) ──────────────────────────────────────────
RUN=0
while true; do
  RUN=$(( RUN + 1 ))
  [[ $LOOP -eq 1 ]] && echo "" && echo "========== RUN $RUN (loop mode, interval=${INTERVAL}s) =========="

  # rotate log per run when looping
  if [[ $LOOP -eq 1 ]]; then
    RUN_LOG="${LOG_FILE%.log}_run${RUN}.log"
  else
    RUN_LOG="$LOG_FILE"
  fi

  cat > "$OUTDIR/status.json" << JSON
{"status":"running","sweeps":$SWEEPS,"run":$RUN,"ts":"$(date -u +%Y-%m-%dT%H:%M:%SZ)"}
JSON

  echo ""
  echo "[6] Running benchmark (sweeps=$SWEEPS, run=$RUN)..."
  echo "    Log: $RUN_LOG"
  echo ""

  "$PY" "$REPO_ROOT/source/parquet_writer/compare_run.py" \
    --cfgdir   "$CFGDIR" \
    --outdir   "$OUTDIR" \
    --results  "$RESULTS_FILE" \
    --sweeps   "$SWEEPS" \
    --site     "$SITE" \
    $COV_FLAG \
    $CSV_FLAG \
    2>&1 | tee "$RUN_LOG"

  EXIT=${PIPESTATUS[0]}

  echo ""
  echo "=================================================="
  if [[ $EXIT -eq 0 ]]; then
    echo "  DONE run=$RUN — results at $RESULTS_FILE"
    cat > "$OUTDIR/status.json" << JSON
{"status":"complete","results":"$RESULTS_FILE","run":$RUN,"ts":"$(date -u +%Y-%m-%dT%H:%M:%SZ)"}
JSON
  else
    echo "  FAILED run=$RUN (exit $EXIT) — see $RUN_LOG"
    cat > "$OUTDIR/status.json" << JSON
{"status":"failed","exit":$EXIT,"run":$RUN,"log":"$RUN_LOG","ts":"$(date -u +%Y-%m-%dT%H:%M:%SZ)"}
JSON
  fi
  echo "=================================================="

  [[ $LOOP -eq 0 ]] && exit $EXIT

  echo "  Next run in ${INTERVAL}s — Ctrl-C to stop"
  sleep "$INTERVAL"
done
