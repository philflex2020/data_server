#!/usr/bin/env bash
# stop_all.sh — stop all data stack processes on spark-22b6
# Usage: ./scripts/stop_all.sh [-v] [spark_nats | spark_influx | all]
#   Defaults to spark_nats.
#
#   -v / --verbose  show each PID found and confirm it's gone

set -euo pipefail

VERBOSE=0
STACK=""
for arg in "$@"; do
    case "$arg" in
        -v|--verbose) VERBOSE=1 ;;
        *) STACK="$arg" ;;
    esac
done
STACK="${STACK:-spark_nats}"

G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; B='\033[0;34m'; N='\033[0m'
ok()   { echo -e "${G}  [ok]${N}   $*"; }
info() { echo -e "${B}  -->  ${N}  $*"; }
warn() { echo -e "${Y}  [warn]${N} $*"; }

kill_proc() {
    local pattern="$1"
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [[ -z "$pids" ]]; then
        [[ $VERBOSE -eq 1 ]] && echo "         (not running: $pattern)"
        return 0
    fi
    for pid in $pids; do
        local cmd
        cmd=$(ps -p "$pid" -o cmd= 2>/dev/null || echo "?")
        warn "stopping PID $pid  ($cmd)"
        kill "$pid" 2>/dev/null || true
    done
    # wait up to 3s for clean exit
    sleep 1
    local remaining=""
    for pid in $pids; do
        kill -0 "$pid" 2>/dev/null && remaining="$remaining $pid" || \
            { [[ $VERBOSE -eq 1 ]] && ok "PID $pid exited cleanly"; true; }
    done
    if [[ -n "$remaining" ]]; then
        warn "PIDs$remaining still alive after SIGTERM — sending SIGKILL"
        kill -9 $remaining 2>/dev/null || true
        sleep 0.3
        for pid in $remaining; do
            kill -0 "$pid" 2>/dev/null && \
                warn "PID $pid would not die" || \
                { [[ $VERBOSE -eq 1 ]] && ok "PID $pid killed"; true; }
        done
    fi
}

if [[ "$STACK" == "spark_nats" || "$STACK" == "all" ]]; then
    echo -e "\n${B}==> Stopping spark_nats stack${N}"
    kill_proc "stress_runner.py"
    kill_proc "subscriber/api/server.py"
    kill_proc "aws/data_store/server.py"
    kill_proc "source/parquet_writer/writer.py"
    kill_proc "source/nats_bridge/bridge.py"
    kill_proc "nats-server"
    kill_proc "minio server"

    if [[ $VERBOSE -eq 1 ]]; then
        echo ""
        echo "    Checking for survivors..."
        SURVIVORS=$(pgrep -f "minio|nats-server|bridge\.py|writer\.py|aws/data_store/server|subscriber/api/server|stress_runner" 2>/dev/null || true)
        if [[ -z "$SURVIVORS" ]]; then
            ok "all spark_nats processes gone"
        else
            for pid in $SURVIVORS; do
                warn "still running: PID $pid  $(ps -p $pid -o cmd= 2>/dev/null || echo '?')"
            done
        fi
    fi
    echo -e "${G}==> spark_nats stopped${N}"
fi

if [[ "$STACK" == "spark_influx" || "$STACK" == "all" ]]; then
    echo -e "\n${B}==> Stopping spark_influx stack${N}"
    for svc in telegraf influxdb flashmq; do
        if systemctl is-active --quiet "$svc" 2>/dev/null; then
            info "stopping $svc"
            sudo systemctl stop "$svc"
            ok "$svc stopped"
        else
            [[ $VERBOSE -eq 1 ]] && echo "         (not running: $svc)"
        fi
    done
    echo -e "${G}==> spark_influx stopped${N}"
fi
