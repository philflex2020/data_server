#!/usr/bin/env bash
# fault_inject.sh — torture test fault injector
# Runs inside the 'fault' container (has docker CLI + host docker socket)
#
# Env vars:
#   SCENARIO      broker_kill | disk_fill | chmod_flip | overflow | kill9 | all
#   CYCLE_SECONDS interval between fault injections (default 60)
#   TARGET        target writer container name (default torture_writer_d)

set -euo pipefail

SCENARIO="${SCENARIO:-all}"
CYCLE="${CYCLE_SECONDS:-60}"
TARGET="${TARGET:-torture_writer_d}"
LOG=/tmp/fault_inject.log

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

# ── S1: broker_kill ──────────────────────────────────────────────────────────
# Stops the broker for 30s to force WAL accumulation, then restarts.
# Writers should reconnect and replay WAL to the original date partition.
broker_kill() {
    log "S1 broker_kill: stopping torture_broker for 30s"
    docker stop torture_broker
    sleep 30
    docker start torture_broker
    sleep 10
    log "S1 broker_kill: broker restarted — writers should reconnect and replay WAL"
}

# ── S2: disk_fill ────────────────────────────────────────────────────────────
# Fills the writer-c data volume to trigger the disk guard, then removes the file.
disk_fill() {
    log "S2 disk_fill: filling /data-c to trigger guard"
    # Find available space and leave only ~500 MB
    AVAIL_KB=$(df /data-c | awk 'NR==2 {print $4}')
    FILL_KB=$(( AVAIL_KB - 512000 ))
    if [ "$FILL_KB" -gt 0 ]; then
        fallocate -l "${FILL_KB}k" /data-c/.disk_fill_blocker 2>/dev/null || \
            dd if=/dev/zero of=/data-c/.disk_fill_blocker bs=1M count=$(( FILL_KB / 1024 )) status=none
        log "S2 disk_fill: created ${FILL_KB}k blocker file — writer-c should log LOW SPACE"
        sleep 15
        rm -f /data-c/.disk_fill_blocker
        log "S2 disk_fill: removed blocker — writer-c should resume"
    else
        log "S2 disk_fill: volume already nearly full (${AVAIL_KB}k avail) — skipping fill"
    fi
}

# ── S3: chmod_flip ───────────────────────────────────────────────────────────
# Makes writer-c output dir temporarily unwritable during a flush window.
# NC2 (temp+rename) and NC3 (no-throw remove) should prevent corrupt files.
chmod_flip() {
    log "S3 chmod_flip: revoking write on /data-c for 500ms"
    chmod 555 /data-c
    sleep 0.5
    chmod 755 /data-c
    log "S3 chmod_flip: restored — checking for .tmp orphans"
    ORPHANS=$(find /data-c -name '*.tmp' 2>/dev/null | wc -l)
    log "S3 chmod_flip: orphan .tmp files = $ORPHANS (expect 0)"
}

# ── S4: overflow ─────────────────────────────────────────────────────────────
# Pauses the target writer container briefly so MQTT messages pile up.
# NC4 pipe-wake should flush within 1s after resume, not wait the full interval.
overflow() {
    log "S4 overflow: pausing $TARGET for 5s to build buffer overflow"
    docker pause "$TARGET" || { log "S4 overflow: pause failed (container not running?)"; return; }
    sleep 5
    docker unpause "$TARGET"
    log "S4 overflow: unpaused — watching health endpoint for rapid flush"
    # Poll health endpoint until flush_count increases
    local before after port
    case "$TARGET" in
        *_a) port=8771 ;; *_b) port=8772 ;; *_c) port=8773 ;; *) port=8774 ;;
    esac
    before=$(curl -s "http://localhost:${port}/health" 2>/dev/null | grep -o '"flush_count":[0-9]*' | cut -d: -f2 || echo 0)
    sleep 3
    after=$(curl -s "http://localhost:${port}/health" 2>/dev/null | grep -o '"flush_count":[0-9]*' | cut -d: -f2 || echo 0)
    log "S4 overflow: flush_count before=$before after=$after (expect after > before within 3s)"
}

# ── S5: kill9 ────────────────────────────────────────────────────────────────
# Sends SIGKILL to the writer process inside the target container.
# Docker restart policy brings it back; WAL should replay without duplicates.
kill9() {
    log "S5 kill9: sending SIGKILL to parquet_writer in $TARGET"
    docker exec "$TARGET" sh -c 'kill -9 $(pgrep parquet_writer)' 2>/dev/null || true
    sleep 5
    log "S5 kill9: container should have restarted — checking health"
    case "$TARGET" in
        *_a) port=8771 ;; *_b) port=8772 ;; *_c) port=8773 ;; *) port=8774 ;;
    esac
    curl -s "http://localhost:${port}/health" 2>/dev/null | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  msgs_received={d[\"msgs_received\"]} wal_replay_rows={d[\"wal_replay_rows\"]}')" \
        2>/dev/null || log "S5 kill9: health endpoint not yet up (normal — still restarting)"
}

# ── dispatch ─────────────────────────────────────────────────────────────────
run_one() {
    case "$1" in
        broker_kill) broker_kill ;;
        disk_fill)   disk_fill   ;;
        chmod_flip)  chmod_flip  ;;
        overflow)    overflow    ;;
        kill9)       kill9       ;;
        *) log "Unknown scenario: $1" ;;
    esac
}

log "fault_inject starting: SCENARIO=$SCENARIO CYCLE=${CYCLE}s TARGET=$TARGET"

if [ "$SCENARIO" = "all" ]; then
    SCENARIOS=(broker_kill disk_fill chmod_flip overflow kill9)
    i=0
    while true; do
        s="${SCENARIOS[$((i % ${#SCENARIOS[@]}))]}"
        log "── running $s ──"
        run_one "$s"
        log "── $s done — sleeping ${CYCLE}s ──"
        sleep "$CYCLE"
        i=$((i + 1))
    done
else
    while true; do
        log "── running $SCENARIO ──"
        run_one "$SCENARIO"
        log "── $SCENARIO done — sleeping ${CYCLE}s ──"
        sleep "$CYCLE"
    done
fi
