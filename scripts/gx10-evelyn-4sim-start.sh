#!/usr/bin/env bash
# gx10-evelyn-4sim-start.sh — 4 simulator + 4 writer pairs on gx10-d94c
#
# 46 Evelyn units split across 4 prefixed namespaces:
#   A: units  0-11 (12 units, 21,240 msg/s)  ws=8769  health=8771
#   B: units 12-23 (12 units, 21,240 msg/s)  ws=8779  health=8772
#   C: units 24-34 (11 units, 19,470 msg/s)  ws=8789  health=8773
#   D: units 35-45 (11 units, 19,470 msg/s)  ws=8799  health=8774
#
# Logs:   /data/logs/YYYY/MM/DD/
# Health: curl http://localhost:8771/health  (repeat for 8772/8773/8774)
# Scenario: python3 scripts/scenario_generator.py --scenario scripts/scenarios/overtemp_cascade.yaml

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="/data/logs/$(date +%Y/%m/%d)"
mkdir -p "$LOGS"

WRITER_BIN="$REPO/source/parquet_writer_cpp/real_writer"
STRESS_BIN="$REPO/source/ems_site_simulator/ems_site_simulator"
STRESS_TPL="$REPO/source/ems_site_simulator/ems_topic_template.json"
FLASHMQ_BIN="/tmp/FlashMQ/build/flashmq"

# ── sanity checks ─────────────────────────────────────────────────────────
for f in "$WRITER_BIN" "$STRESS_BIN" "$STRESS_TPL"; do
    [[ -f "$f" ]] || { echo "ERROR: missing $f — run make first"; exit 1; }
done
for s in a b c d; do
    cfg="$REPO/source/parquet_writer_cpp/config.gx10-evelyn-${s}.yaml"
    [[ -f "$cfg" ]] || { echo "ERROR: missing $cfg"; exit 1; }
done
[[ -x "$FLASHMQ_BIN" ]] || { echo "ERROR: FlashMQ binary not found at $FLASHMQ_BIN"; exit 1; }

# ── stop any existing instances ────────────────────────────────────────────
pkill -f "real_writer.*config.gx10-evelyn"   2>/dev/null && echo "stopped old real_writer(s)"   || true
pkill -f "ems_site_simulator.*topic-prefix"     2>/dev/null && echo "stopped old ems_site_simulator(s)" || true
pkill -f "ems_site_simulator.*8769\|ems_site_simulator.*8779\|ems_site_simulator.*8789\|ems_site_simulator.*8799" 2>/dev/null || true
pkill -f "flashmq.*gx10-evelyn"              2>/dev/null && echo "stopped old FlashMQ"            || true
sleep 1

# ── start FlashMQ broker ───────────────────────────────────────────────────
# client_initial_buffer_size 4MB: prevents QoS-0 drops at ~81k msg/s
# (system mosquitto has a fixed ~64KB socket buffer and drops under load)
cat > /tmp/flashmq-gx10-evelyn.conf << 'EOF'
allow_anonymous true
storage_dir /tmp/flashmq-evelyn-data
client_initial_buffer_size 4194304
EOF
mkdir -p /tmp/flashmq-evelyn-data
nohup "$FLASHMQ_BIN" --config-file /tmp/flashmq-gx10-evelyn.conf \
    > "$LOGS/flashmq.log" 2>&1 &
echo "FlashMQ  pid=$!  config=/tmp/flashmq-gx10-evelyn.conf  log=$LOGS/flashmq.log"
sleep 2
mosquitto_pub -h localhost -p 1883 -t _test -m ping 2>/dev/null \
    && echo "FlashMQ OK" \
    || { echo "ERROR: FlashMQ failed to start — check $LOGS/flashmq.log"; exit 1; }

# ── unit ID slices from ems_topic_template.json ────────────────────────────
# 46 units: A=0..11, B=12..23, C=24..34, D=35..45
UNITS_A=(0215F562 0215F5A0 0215F5BF 0215F5CF 0215F5D6 0215F5DD 0215F5DF 0215F5E3 0215F5E7 0215F5F5 0215F5FE 0215F60B)
UNITS_B=(0215F60D 0215F625 0215F63C 0215F662 0215F6B1 0215F6BF 0215F6FD 0215F708 0215F70F 0215F734 0215F849 0215F87A)
UNITS_C=(0227C829 0227C82A 0227C82B 0227C838 0227C842 0227C84E 0227C86B 0227C86E 02281632 02281634 02281636)
UNITS_D=(02281646 02281656 0228165D 0228165E 02281660 02281663 02281683 02281684 0228168A 0228168E 02285B20)

unit_flags() { local arr=("$@"); for id in "${arr[@]}"; do printf -- '--unit-id %s ' "$id"; done; }

# ── start 4 simulator + writer pairs ──────────────────────────────────────
for SIM in a b c d; do
    case $SIM in
        a) PREFIX=A; WS=8769; RATE=21240; UNITS=("${UNITS_A[@]}") ;;
        b) PREFIX=B; WS=8779; RATE=21240; UNITS=("${UNITS_B[@]}") ;;
        c) PREFIX=C; WS=8789; RATE=19470; UNITS=("${UNITS_C[@]}") ;;
        d) PREFIX=D; WS=8799; RATE=19470; UNITS=("${UNITS_D[@]}") ;;
    esac

    CFG="$REPO/source/parquet_writer_cpp/config.gx10-evelyn-${SIM}.yaml"

    # writer
    nohup "$WRITER_BIN" --config "$CFG" \
        > "$LOGS/real_writer_${SIM}.log" 2>&1 &
    echo "real_writer-${SIM}  pid=$!  log=$LOGS/real_writer_${SIM}.log"

    # simulator
    # shellcheck disable=SC2046
    nohup "$STRESS_BIN" \
        --host localhost \
        --template "$STRESS_TPL" \
        --topic-prefix "$PREFIX" \
        --id "ems-stress-${SIM}" \
        $(unit_flags "${UNITS[@]}") \
        --rate "$RATE" \
        --ws-port "$WS" \
        --site-id "0215D1D8" \
        > "$LOGS/stress_${SIM}.log" 2>&1 &
    echo "stress-${SIM}  pid=$!  prefix=${PREFIX}  units=${#UNITS[@]}  rate=${RATE}  ws=:${WS}"
done

# ── wait for all health endpoints ─────────────────────────────────────────
echo ""
echo -n "waiting for health endpoints"
for i in $(seq 1 30); do
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
