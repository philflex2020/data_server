# phil-dev Component Install & Startup
## Dual-host path: phil-dev (192.168.86.46) → fractal-phil (192.168.86.51)

---

## Topology

```
phil-dev (192.168.86.46)
────────────────────────────────────────────────────
stress_runner  →  FlashMQ :1883  →  writer.cpp → /srv/data/parquet/
                       │
                  FlashMQ bridge ──────────────────────────────────────►
                                                       fractal-phil :1884
                                               ┌───────────────────────┐
                                               │ Telegraf → InfluxDB2  │
                                               │ subscriber-api :8767  │
                                               │ flux_compat   :8768   │
                                               │ /srv/data/parquet-    │
                                               │   aws-sim/ ← rsync    │
                                               └───────────────────────┘
```

---

## Prerequisites (one-time)

```bash
# System packages
sudo apt-get install -y \
  flashmq \
  libmosquitto-dev \
  libyaml-cpp-dev \
  libsimdjson-dev \
  libarrow-dev \
  libparquet-dev \
  mosquitto-clients   # for smoke-testing

# Python venv (already created)
# /home/phil/work/gen-ai/data_server/.venv
# Packages: aiomqtt, paho-mqtt, websockets, pyyaml, duckdb, pyarrow, boto3
```

---

## Component 1 — FlashMQ :1883

**Status:** installed, running via systemd.

### Config: `/etc/flashmq/flashmq.conf`

```
listen {
    port 1883
    protocol mqtt
}
log_file /var/log/flashmq/flashmq.log
log_level info
allow_anonymous true
client_initial_buffer_size 65536
max_packet_size 1048576
max_qos_msg_pending_per_client 1000
max_incoming_topic_alias_value 16

# Bridge to fractal-phil (add this line — see Component 2)
include_dir /etc/flashmq/bridge-conf.d
```

### Start / stop

```bash
sudo systemctl start  flashmq
sudo systemctl stop   flashmq
sudo systemctl status flashmq
sudo journalctl -u flashmq -f
```

### Smoke test

```bash
mosquitto_sub -h localhost -p 1883 -t 'test/#' &
mosquitto_pub -h localhost -p 1883 -t 'test/1' -m 'hello'
kill %1
```

---

## Component 2 — FlashMQ Bridge → fractal-phil :1884

Bridges `batteries/#` from :1883 on phil-dev to :1884 on fractal-phil.

### Step 1 — add `include_dir` to flashmq.conf (if not already there)

```bash
grep -q 'bridge-conf.d' /etc/flashmq/flashmq.conf || \
  echo "include_dir /etc/flashmq/bridge-conf.d" | sudo tee -a /etc/flashmq/flashmq.conf
```

### Step 2 — generate bridge config

```bash
cd /home/phil/work/gen-ai/data_server
sudo mkdir -p /etc/flashmq/bridge-conf.d
python3 source/bridge/gen_bridge_conf.py source/bridge/bridge.yaml \
        --output /tmp/bridge.conf
sudo cp /tmp/bridge.conf /etc/flashmq/bridge-conf.d/bridge.conf
```

Generated file (`/etc/flashmq/bridge-conf.d/bridge.conf`):

```
# Bridge: aws_live
# Forward live sensor data to fractal-phil FlashMQ aws-sim (192.168.86.51:1884)
bridge {
    address 192.168.86.51
    port 1884
    protocol_version mqtt3.1.1
    clientid_prefix phil-dev-bridge

    # Topics (1 from 1 group(s))
    publish batteries/# 0
}
```

### Step 3 — reload FlashMQ

```bash
sudo systemctl reload flashmq   # or: sudo flashmq --reload-config
```

### Verify bridge is working

```bash
# On fractal-phil — subscribe to :1884
ssh phil@192.168.86.51 "mosquitto_sub -h localhost -p 1884 -t 'batteries/test' &"

# On phil-dev — publish to :1883
mosquitto_pub -h localhost -p 1883 -t 'batteries/test' -m 'bridge-test'

# Should see "bridge-test" printed on fractal-phil
# Kill subscriber on fractal-phil when done
ssh phil@192.168.86.51 "pkill -f 'mosquitto_sub.*1884'"
```

---

## Component 3 — stress_runner

Publishes simulated battery telemetry to :1883.

### Config: `source/stress_runner/config.yaml`

**Change needed:** port 1884 → 1883 (target host FlashMQ, not fractal-phil direct).

```yaml
mqtt:
  host: localhost
  port: 1883          # ← was 1884; now publishes to host FlashMQ
  client_id: stress-runner
  username: ""
  password: ""

websocket:
  host: 0.0.0.0
  port: 8769

topic_prefix: batteries
sample_interval_seconds: 1      # 1 s per cell sweep

projects:
  - project_id: 1
    sites:
      - site_id: 10
        racks: 4
        modules_per_rack: 8
        cells_per_module: 12    # 384 cells/s per site
      - site_id: 11
        racks: 4
        modules_per_rack: 8
        cells_per_module: 12    # 768 msg/s total (per_cell mode)
```

### Start

```bash
cd /home/phil/work/gen-ai/data_server/source/stress_runner

# Foreground
/home/phil/work/gen-ai/data_server/.venv/bin/python3 stress_runner.py --config config.yaml

# Background
nohup /home/phil/work/gen-ai/data_server/.venv/bin/python3 stress_runner.py \
      --config config.yaml > /tmp/stress_runner.log 2>&1 &
echo "stress_runner pid=$!"
```

### Stop

```bash
pkill -f stress_runner.py
```

### WebSocket control (from parquet_monitor or curl)

```
ws://192.168.86.46:8769
```

Messages: `{"type":"stats"}` → rate/total; `{"type":"set_rate","interval":0.5}` → 2× speed.

---

## Component 4 — writer.cpp (parquet_writer_cpp)

Subscribes to :1883, writes Parquet files to `/srv/data/parquet/`.

### Build

```bash
cd /home/phil/work/gen-ai/data_server/source/parquet_writer_cpp

# Install C++ deps if missing
make deps    # runs: sudo apt-get install -y libmosquitto-dev libyaml-cpp-dev libsimdjson-dev

# Build (binary already present, rebuild if source changed)
make

# Verify
./parquet_writer --help
```

### Data directory

```bash
sudo mkdir -p /srv/data/parquet
sudo chown phil:phil /srv/data/parquet
```

### Config: `source/parquet_writer_cpp/config.yaml`

```yaml
mqtt:
  host: localhost
  port: 1883          # host FlashMQ
  client_id: parquet-writer
  topic: "batteries/#"
  qos: 0              # match publisher QoS

output:
  base_path: /srv/data/parquet
  project_id: 0
  partitions:
    - "project={project_id}"
    - "site={site_id}"
    - "{year}/{month}/{day}"
  compression: snappy
  flush_interval_seconds: 60
  max_messages_per_part: 50000
  current_state_path: ""
  hot_file_path: ""
```

### Start

```bash
cd /home/phil/work/gen-ai/data_server/source/parquet_writer_cpp

# Foreground
./parquet_writer

# Background
nohup ./parquet_writer > /tmp/parquet_writer.log 2>&1 &
echo "parquet_writer pid=$!"
```

### Stop

```bash
pkill -f parquet_writer
```

### Verify data is being written

```bash
watch -n2 'find /srv/data/parquet -name "*.parquet" | wc -l'
ls -lhR /srv/data/parquet/ | tail -20
```

---

## Component 5 — rsync pull (fractal-phil side)

The subscriber-api on fractal-phil pulls Parquet files from phil-dev via SSH rsync.

### SSH key setup (one-time, run on fractal-phil)

```bash
# On fractal-phil:
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""   # if no key exists
ssh-copy-id phil@192.168.86.46                       # authorise on phil-dev

# Test
ssh phil@192.168.86.46 "ls /srv/data/parquet/ | head -5"
```

### config.fractal.yaml rsync section (on fractal-phil)

```yaml
rsync:
  src:      phil@192.168.86.46:/srv/data/parquet/   # ← pull from phil-dev
  dst:      /srv/data/parquet-aws-sim/
  interval: 5
```

No code change needed — the subscriber-api rsync loop passes src/dst directly
to `rsync -a --delete`, which handles SSH syntax transparently.

### Trigger via monitor

Click **▶ Start** in the rsync section of parquet_monitor.html, or:

```bash
curl -X POST http://192.168.86.51:8768/rsync/start
curl -s          http://192.168.86.51:8768/rsync/status
```

---

## Complete Startup Sequence

```bash
# ── phil-dev ──────────────────────────────────────────
# 1. FlashMQ (should already be running)
sudo systemctl start flashmq

# 2. Install bridge config (first time only)
sudo mkdir -p /etc/flashmq/bridge-conf.d
cd /home/phil/work/gen-ai/data_server
python3 source/bridge/gen_bridge_conf.py source/bridge/bridge.yaml \
        --output /tmp/bridge.conf && sudo cp /tmp/bridge.conf /etc/flashmq/bridge-conf.d/bridge.conf
sudo systemctl reload flashmq

# 3. Parquet writer
cd source/parquet_writer_cpp
nohup ./parquet_writer > /tmp/parquet_writer.log 2>&1 &

# 4. Stress runner
cd /home/phil/work/gen-ai/data_server/source/stress_runner
nohup /home/phil/work/gen-ai/data_server/.venv/bin/python3 stress_runner.py \
      --config config.yaml > /tmp/stress_runner.log 2>&1 &

# ── fractal-phil (192.168.86.51) ──────────────────────
# 5. subscriber-api (update config first — see rsync section above)
ssh phil@192.168.86.51 "
  pkill -f server.py 2>/dev/null; sleep 1
  nohup /home/phil/data_server/.venv/bin/python3 \
    /home/phil/data_server/subscriber/api/server.py \
    --config /home/phil/data_server/subscriber/api/config.fractal.yaml \
    > /tmp/subscriber_api.log 2>&1 &
  echo started
"

# 6. Start rsync pull via monitor or:
curl -X POST http://192.168.86.51:8768/rsync/start
```

---

## Health Checks

```bash
# FlashMQ listening on :1883
ss -tlnp | grep 1883

# Bridge connected to fractal-phil
grep -i bridge /var/log/flashmq/flashmq.log | tail -5

# Parquet files growing
ls /srv/data/parquet/project=0/ | head -5
find /srv/data/parquet -name '*.parquet' | wc -l

# stress_runner rate
curl -s http://localhost:8769  # WebSocket — use wscat or parquet_monitor

# subscriber-api on fractal-phil
curl -s http://192.168.86.51:8768/ping

# Compare InfluxDB vs DuckDB (after rsync catches up)
curl -s 'http://192.168.86.51:8768/compare?window=60&offset=30' | python3 -m json.tool

# Process stats on fractal-phil
curl -s http://192.168.86.51:8768/proc_stats | python3 -m json.tool
```

---

## Log Files

| Component | Log |
|-----------|-----|
| FlashMQ | `/var/log/flashmq/flashmq.log` |
| parquet_writer | `/tmp/parquet_writer.log` |
| stress_runner | `/tmp/stress_runner.log` |
| subscriber-api (fractal-phil) | `/tmp/subscriber_api.log` |
