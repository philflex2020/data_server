#!/usr/bin/env bash
# gx10-evelyn-4sim-stop.sh — stop all 4 sim + writer pairs on gx10-d94c

pkill -f "real_writer.*config.gx10-evelyn-[abcd]" 2>/dev/null && echo "stopped real_writer(s)"    || echo "real_writer(s) not running"
pkill -f "ems_site_simulator.*topic-prefix"           2>/dev/null && echo "stopped ems_site_simulator(s)" || echo "ems_site_simulator(s) not running"
pkill -f "flashmq.*gx10-evelyn"                    2>/dev/null && echo "stopped FlashMQ"            || echo "FlashMQ not running"
