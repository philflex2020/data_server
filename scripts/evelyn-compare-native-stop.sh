#!/usr/bin/env bash
# evelyn-compare-native-stop.sh — stop the native Evelyn cost-comparison stack
#
# Stops: generators, Telegraf instances, FlashMQ instances, InfluxDB instances.
# Data in /data/influx-base/ and /data/influx-filt/ is preserved.
#
# To wipe data and start fresh:
#   rm -rf /data/influx-base/* /data/influx-filt/*

set -euo pipefail

stop_proc() {
    local label="$1"; local pattern="$2"
    if pkill -f "$pattern" 2>/dev/null; then
        echo "stopped $label"
    else
        echo "$label not running"
    fi
}

stop_proc "flashmq"         "flashmq.*fmq.conf"
stop_proc "generator"       "stress_real_pub"
stop_proc "telegraf-base"   "telegraf.*telegraf-native-base"
stop_proc "telegraf-filt"   "telegraf.*telegraf-native-filt"
stop_proc "influxd-base"    "influxd.*influx-base"
stop_proc "influxd-filt"    "influxd.*influx-filt"

echo ""
echo "All compare processes stopped."
echo "Data preserved in /data/influx-base/ and /data/influx-filt/"
