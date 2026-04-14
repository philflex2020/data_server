#!/usr/bin/env bash
# gx10-evelyn-stop.sh — stop real_writer + ems_site_simulator on gx10-d94c

pkill -f "real_writer.*config.gx10-evelyn"   2>/dev/null && echo "stopped real_writer"    || echo "real_writer not running"
pkill -f "ems_site_simulator.*8769"             2>/dev/null && echo "stopped ems_site_simulator" || echo "ems_site_simulator not running"
