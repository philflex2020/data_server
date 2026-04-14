#!/usr/bin/env bash
# gx10-evelyn-writers-only-stop.sh — stop the 4 real_writers (leave simulators running)

pkill -f "real_writer.*config.gx10-evelyn" 2>/dev/null && echo "stopped real_writer(s)" || echo "real_writer(s) not running"
