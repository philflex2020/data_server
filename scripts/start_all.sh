#!/usr/bin/env bash
# start_all.sh — start the full data stack on spark-22b6
# Usage: ./scripts/start_all.sh [--verbose|-v] spark_nats | spark_influx
#
#   -v / --verbose  print every command run and tail logs on failure

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

VERBOSE=0
STACK=""
for arg in "$@"; do
    case "$arg" in
        -v|--verbose) VERBOSE=1 ;;
        *) STACK="$arg" ;;
    esac
done

if [[ -z "$STACK" ]]; then
    echo "Usage: $0 [-v] spark_nats | spark_influx"
    exit 1
fi

VENV="$(pwd)/.venv/bin/python"
LOG_DIR="/tmp/ds_logs"
mkdir -p "$LOG_DIR"

# ── Colour helpers ────────────────────────────────────────────────────────
G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; B='\033[0;34m'; N='\033[0m'
ok()   { echo -e "${G}  [ok]${N}   $*"; }
info() { echo -e "${B}  -->  ${N}  $*"; }
warn() { echo -e "${Y}  [warn]${N} $*"; }
fail() { echo -e "${R}  [FAIL]${N} $*"; }

vlog() {
    # print command in verbose mode
    [[ $VERBOSE -eq 1 ]] && echo -e "${Y}  [cmd]${N}  $*" || true
}

# ── Helpers ───────────────────────────────────────────────────────────────
kill_proc() {
    local pattern="$1"
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
        warn "killing existing '$pattern' (PID $pids)"
        kill $pids 2>/dev/null || true
        sleep 0.5
        # SIGKILL anything still alive
        for pid in $pids; do
            kill -0 "$pid" 2>/dev/null && {
                warn "PID $pid still alive — SIGKILL"
                kill -9 "$pid" 2>/dev/null || true
            } || true
        done
    else
        [[ $VERBOSE -eq 1 ]] && echo "         (no existing '$pattern')"
    fi
}

launch() {
    # launch <label> <logfile> <cmd...>
    local label="$1" logfile="$2"
    shift 2
    vlog "$@  >> $logfile"
    nohup "$@" >> "$logfile" 2>&1 &
    local pid=$!
    echo "         PID $pid  log: $logfile"
    echo "$pid"  # return pid
}

wait_port() {
    local port="$1" label="$2" logfile="${3:-}"
    info "waiting for $label on :$port ..."
    for i in $(seq 1 30); do
        ss -tlnp "sport = :${port}" 2>/dev/null | grep -q ":${port}" && {
            ok "$label up on :$port"
            return 0
        }
        sleep 0.5
    done
    fail "$label did not come up on :$port after 15s"
    if [[ -n "$logfile" && -f "$logfile" ]]; then
        echo "--- last 20 lines of $logfile ---"
        tail -20 "$logfile"
        echo "---------------------------------"
    fi
    return 1
}

# ── spark_nats ─────────────────────────────────────────────────────────────
if [[ "$STACK" == "spark_nats" ]]; then
    echo -e "\n${B}==> Starting spark_nats stack${N}"

    echo "--- Stopping any stale processes ---"
    kill_proc "stress_runner.py"
    kill_proc "subscriber/api/server.py"
    kill_proc "aws/data_store/server.py"
    kill_proc "source/parquet_writer/writer.py"
    kill_proc "source/nats_bridge/bridge.py"
    kill_proc "nats-server"
    kill_proc "minio server"
    sleep 1

    echo ""
    echo "--- Starting services ---"

    info "MinIO (S3)"
    vlog "aws/data_store/minio server aws/data_store/minio-data --console-address :9011"
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
        nohup aws/data_store/minio server aws/data_store/minio-data \
        --console-address :9011 \
        >> "$LOG_DIR/minio.log" 2>&1 &
    echo "         PID $!  log: $LOG_DIR/minio.log"
    wait_port 9000 "MinIO" "$LOG_DIR/minio.log"

    info "NATS JetStream"
    vlog "nats-server -c manager/nats-server.conf"
    nohup nats-server -c manager/nats-server.conf \
        >> "$LOG_DIR/nats.log" 2>&1 &
    echo "         PID $!  log: $LOG_DIR/nats.log"
    wait_port 4222 "NATS" "$LOG_DIR/nats.log"

    info "NATS Bridge"
    vlog "$VENV source/nats_bridge/bridge.py --config source/nats_bridge/config.spark.yaml"
    nohup "$VENV" source/nats_bridge/bridge.py \
        --config source/nats_bridge/config.spark.yaml \
        >> "$LOG_DIR/bridge.log" 2>&1 &
    echo "         PID $!  log: $LOG_DIR/bridge.log"
    sleep 1
    if [[ $VERBOSE -eq 1 ]]; then
        echo "         --- bridge startup log ---"
        tail -5 "$LOG_DIR/bridge.log" | sed 's/^/         /'
    fi

    info "Parquet Writer"
    vlog "$VENV source/parquet_writer/writer.py --config source/parquet_writer/config.yaml"
    nohup "$VENV" source/parquet_writer/writer.py \
        --config source/parquet_writer/config.yaml \
        >> "$LOG_DIR/writer.log" 2>&1 &
    echo "         PID $!  log: $LOG_DIR/writer.log"
    sleep 1
    if [[ $VERBOSE -eq 1 ]]; then
        echo "         --- writer startup log ---"
        tail -5 "$LOG_DIR/writer.log" | sed 's/^/         /'
    fi

    info "AWS Data Store server"
    vlog "$VENV aws/data_store/server.py --config aws/data_store/config.yaml"
    nohup "$VENV" aws/data_store/server.py \
        --config aws/data_store/config.yaml \
        >> "$LOG_DIR/aws_server.log" 2>&1 &
    echo "         PID $!  log: $LOG_DIR/aws_server.log"
    wait_port 8766 "AWS server" "$LOG_DIR/aws_server.log"

    info "Subscriber API"
    vlog "(cd subscriber/api && $VENV server.py --config config.yaml)"
    (cd subscriber/api && nohup "$VENV" server.py \
        --config config.yaml \
        >> "$LOG_DIR/subscriber.log" 2>&1 &)
    echo "         PID $!  log: $LOG_DIR/subscriber.log"
    wait_port 8767 "Subscriber API" "$LOG_DIR/subscriber.log"

    info "Stress Runner"
    vlog "$VENV source/stress_runner/stress_runner.py --config source/stress_runner/config.spark.yaml"
    nohup "$VENV" source/stress_runner/stress_runner.py \
        --config source/stress_runner/config.spark.yaml \
        >> "$LOG_DIR/stress_runner.log" 2>&1 &
    echo "         PID $!  log: $LOG_DIR/stress_runner.log"
    wait_port 8769 "Stress Runner" "$LOG_DIR/stress_runner.log"

    echo ""
    echo -e "${G}==> spark_nats stack running${N}"
    echo "    Logs: $LOG_DIR/"
    if [[ $VERBOSE -eq 1 ]]; then
        echo ""
        echo "    Running processes:"
        ps aux | grep -E "minio|nats-server|bridge\.py|writer\.py|aws/data_store/server|subscriber/api/server|stress_runner" \
               | grep -v grep \
               | awk '{printf "    PID %-7s %s %s %s\n", $2, $3"%", $11, $12}'
    fi

# ── spark_influx ────────────────────────────────────────────────────────────
elif [[ "$STACK" == "spark_influx" ]]; then
    echo -e "\n${B}==> Starting spark_influx stack${N}"

    for svc in flashmq influxdb telegraf; do
        if systemctl is-active --quiet "$svc"; then
            ok "$svc already running"
        else
            info "starting $svc"
            vlog "sudo systemctl start $svc"
            sudo systemctl start "$svc"
            ok "$svc started"
        fi
    done

    echo ""
    echo -e "${G}==> spark_influx stack running${N}"

else
    echo "Unknown stack: $STACK"
    echo "Usage: $0 [-v] spark_nats | spark_influx"
    exit 1
fi
