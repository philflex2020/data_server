# Branch: 03192026_dual_host — Dual-Host Path (phil-dev → fractal-phil)

Goal: make the full pipeline work with phil-dev as the generator/writer host
and fractal-phil (192.168.86.51) simulating AWS (FlashMQ :1884, Telegraf, InfluxDB2,
subscriber-api).

---

## Changes

### source/bridge/bridge.yaml
- `aws_live` bridge: changed host from placeholder `flashmq.aws.example.com:1883`
  to `192.168.86.51:1884` (fractal-phil FlashMQ aws-sim)
- Changed `qos: 1` → `qos: 0` (matches publisher QoS; QoS-1 bridge on QoS-0
  publish makes no difference and misleads)
- Added `clientid_prefix: phil-dev-bridge` to identify the bridge connection

### subscriber/api/flux_compat.py
- `/compare` Flux count query: added `|> filter(fn: (r) => r._field == "value")`
  to exclude the `timestamp` _field row telegraf stores per per_cell_item message.
  Without this, InfluxDB count was 2× DuckDB count (two _field entries per message).

### subscriber/api/flux_compat.py (proc_stats)
- New `/proc_stats` endpoint: reads `/proc/{pid}/stat` and `/proc/{pid}/status`
  to return `cpu_pct` and `rss_mb` for influxd, telegraf, and flashmq (aws-sim
  instance selected by cmdline fragment).  No psutil dependency.

### html/parquet_monitor.html — WS reconnect fix
- `onclose = onerror = sameHandler` caused two retries per failed connection.
  Each retry also doubled, eventually thrashing the loop dead (requiring page reload).
- Fix: `onerror` is now a no-op (onclose always follows); `_scheduleGenReconnect`
  and `_scheduleSubReconnect` use a pending-flag so at most one retry is in flight.

### html/parquet_monitor.html — proc stats display
- Added CPU% and RAM rows to FlashMQ :1884 box (`fmq-cpu`, `fmq-mem`)
- Added Telegraf CPU%/RAM and InfluxDB CPU%/RAM rows to the Telegraf→InfluxDB2 box
- `checkProcStats(aws)` called every 2 s via pollTimer

### html/parquet_monitor.html — curl example window
- Capped `<pre id="flux-curl-cmd">` to `max-height:4.5em; overflow-y:auto`
- Clipboard copy reads `textContent` so captures full command regardless of scroll

### html/parquet_monitor.html — AWS cost estimate panel
- Added collapsible **AWS Cost Estimate** section above the status bar
- Three-button mode switch: **InfluxDB** / **DuckDB** / **Both**
  - InfluxDB path: S3 (raw) + EC2 (m5/r5 class) + OpenSearch nodes
  - DuckDB path: S3 with configurable Parquet compression ratio + small writer EC2
  - Both: side-by-side tables + green savings row ($/mo, $/yr, % reduction)
- Shared inputs: msg/s, bytes/msg, retention months; "↺ Live rate" syncs from generator
- All instance dropdowns have a custom $/hr fallback

### html/parquet_monitor.html — QoS and bridge topology labels
- `box-fmq-host` subtitle: `QoS-1 · bridges all traffic` → `QoS-0 · sim: stress_runner publishes direct to :1884`
- Bridge arrow label: `MQTT · QoS-1` → `MQTT · QoS-0`

### subscriber/api/server.py (earlier session — pre-branch)
- Always subscribe to data topic in `mqtt_connect_loop` for accurate sync drop counting
- Only broadcast live messages when `g_live_subject` is set
- Added `mqtt_connected` and `ws_clients` to all three stats broadcast sites

### source/telegraf/telegraf-aws-sim.conf (earlier session — pre-branch)
- Added `topic_parsing` for 7-segment per_cell_item topics
- Made all JSON identity tags `optional = true` (fixes telegraf crash on per_cell_item)
- Added `value` field for per_cell_item readings
- Changed `qos = 1` → `qos = 0`

---

## Pending / In-Progress

- [ ] stress_runner/config.yaml: change port 1884 → 1883 (publish to host FlashMQ)
- [ ] Generate FlashMQ bridge config on phil-dev:
      `mkdir -p /etc/flashmq/bridge-conf.d`
      `python3 source/bridge/gen_bridge_conf.py source/bridge/bridge.yaml \`
      `        --output /etc/flashmq/bridge-conf.d/bridge.conf`
      `sudo flashmq --reload-config`
- [ ] Add `include_dir /etc/flashmq/bridge-conf.d` to `/etc/flashmq/flashmq.conf`
- [ ] Update config.fractal.yaml rsync.src to pull from phil-dev over SSH:
      `src: phil@192.168.86.46:/srv/data/parquet/`
- [ ] SSH key: fractal-phil needs passwordless SSH access to phil-dev for rsync pull
- [ ] End-to-end test: verify data flows phil-dev:1883 → bridge → fractal-phil:1884
      → telegraf → InfluxDB and subscriber-api → parquet
