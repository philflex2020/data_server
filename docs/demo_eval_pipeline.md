# Pipeline Evaluation — Generator × Filter × Storage

## Goal

Compare three filter strategies across two storage backends to quantify the
cost/benefit of each filtering approach in terms of CPU, memory, and storage.
This evaluation feeds directly into the Evelyn cost-reduction business case.

---

## Item 1 — Generator modes

The EMS simulator (`ems_site_simulator`) produces two distinct output patterns
controlled via the `set_smooth_burst` WebSocket command (or the checkbox in
Sim Control):

| Mode | `g_slow_interval_ms` | Behaviour | Use for |
|---|---|---|---|
| **Smooth** | 1000 ms | Every signal published every second — constant flat rate | Baseline measurement, predictable load |
| **Burst** (unsmoothed) | 20000 ms | Slow signals publish every 20s in a burst; fast signals every second — mirrors real Evelyn cadence | Realistic dedup evaluation |

**Burst mode is the correct mode for dedup evaluation** — smooth mode inflates
the apparent benefit of dedup because slow-changing signals appear to change
every second rather than every 20s.

Both modes use the same 46-unit Evelyn dataset (`ems_topic_template.json`)
with the real unit IDs.

---

## Item 2 — InfluxDB pipeline comparison

Three Telegraf configurations subscribe to the same FlashMQ broker (:11888)
and write to three separate InfluxDB instances.

```
ems_site_simulator
      │ MQTT QoS-0
      ▼
FlashMQ :11888
      ├─── Telegraf-A  (unfiltered)   ──────────────────► InfluxDB :8096  bucket=ems-baseline
      ├─── Telegraf-B  (regular dedup)─── dedup 60s ─────► InfluxDB :8097  bucket=ems-dedup
      └─── Telegraf-C  (float_dedup)  ─── execd ──────────► InfluxDB :8098  bucket=ems-floatdedup
```

### Config A — Unfiltered (baseline)
- `processors.pivot` only
- All signals, all types, every message written
- Represents current Longbow production cost

### Config B — Regular dedup
- `processors.dedup dedup_interval=60s` with `tagpass data_type=[integer, boolean_integer]`
- Only integer/boolean signals are deduplicated; floats and strings pass through every message
- Previous "Filter B" approach

### Config C — float_dedup execd
- `[[processors.execd]]` calling `float_dedup` binary
- Handles ALL field types: float by ε-threshold, int/bool/string by exact-match change
- 60s heartbeat guarantees at least one row per 60s per series
- Per-signal epsilon rules in `float_dedup.toml`
- Most aggressive filter; expected to show largest storage reduction

### Metrics to capture (per Telegraf instance)

| Metric | Source | Method |
|---|---|---|
| CPU % | `/proc/[pid]/stat` or `top` | Stats script or Prometheus output |
| Memory (RSS MB) | `/proc/[pid]/status` VmRSS | Same |
| Rows/s into InfluxDB | InfluxDB Flux count query | Demo InfluxDB Viewer |
| InfluxDB storage (MB) | `du -sm /data/influx-{a,b,c}/engine/` | SSH script |
| Telegraf internal metrics | `[[inputs.internal]]` → InfluxDB | Already in configs |

### InfluxDB ports (proposed)

| Instance | Port | Bucket | Filter |
|---|---|---|---|
| influxd-A | 8096 | ems-baseline | none |
| influxd-B | 8097 | ems-dedup | regular dedup |
| influxd-C | 8098 | ems-floatdedup | float_dedup |

> **Note:** Current native start script only runs two instances (:8096/:8097).
> Adding :8098 requires extending `evelyn-compare-native-start.sh`.

---

## Item 3 — Parquet writer comparison

Three parquet writer instances receive the same three streams, bypassing
Telegraf entirely. The parquet writer is a C++ binary that subscribes directly
to MQTT and writes Arrow/Parquet files.

```
ems_site_simulator
      │ MQTT QoS-0
      ▼
FlashMQ :11888
      ├─── parquet_writer-A  (unfiltered)   ──────────────► /data/parquet-eval/a/
      ├─── parquet_writer-B  (regular dedup)─── ???  ──────► /data/parquet-eval/b/
      └─── parquet_writer-C  (float_dedup)  ─── ???  ──────► /data/parquet-eval/c/
```

The parquet writer currently ingests raw MQTT messages without filtering.
Filtering must be applied upstream or inline.

### Filtering options for parquet paths

**Option 1 — MQTT bridge with float_dedup**
```
FlashMQ :11888  →  float_dedup (stdin/stdout line protocol)  →  re-publish MQTT :11889
                                                                      │
                                                              parquet_writer-C
```
Requires a small bridge process that:
1. Subscribes to FlashMQ :11888
2. Pipes each message through float_dedup
3. Re-publishes filtered messages to a second FlashMQ (or the same broker on a different topic prefix)

**Option 2 — Telegraf mqtt→mqtt**
Use Telegraf as a filter-and-republish bridge:
- `[[inputs.mqtt_consumer]]` → float_dedup execd → `[[outputs.mqtt]]`
- Telegraf has an `[[outputs.mqtt]]` plugin that publishes back to a broker
- parquet_writer subscribes to the filtered topic

**Option 3 — Inline filter in parquet_writer**
Add float_dedup logic directly into the parquet_writer C++ binary as an
optional compile-time or runtime filter. Cleanest long-term but requires
modifying the writer.

**Recommended for evaluation: Option 2 (Telegraf mqtt→mqtt)**
Telegraf's `outputs.mqtt` plugin already exists and is battle-tested.
This keeps the parquet_writer unmodified for the evaluation phase.

### Metrics to capture (per parquet_writer instance)

| Metric | Source | Method |
|---|---|---|
| CPU % | `/proc/[pid]/status` | writer health endpoint (not yet) or top |
| Memory (RSS MB) | `/proc/[pid]/status` VmRSS | Same |
| Rows written | `total_rows_written` | Health endpoint GET /health |
| Last flush rows | `last_flush_rows` | Health endpoint |
| Parquet file size (MB) | `du -sm /data/parquet-eval/*/` | SSH script or health endpoint |
| Disk free (GB) | `disk_free_gb` | Health endpoint (already present) |

### parquet_writer health endpoint (current fields)
```json
{
  "status": "ok",
  "site_id": "...",
  "msgs_received": 12345,
  "buffer_rows": 100,
  "overflow_drops": 0,
  "sync_drops": 0,
  "flush_count": 42,
  "total_rows_written": 99000,
  "last_flush_rows": 2400,
  "last_flush_ms": 312,
  "wal_replay_rows": 0,
  "disk_free_gb": 1240.5
}
```

CPU and RSS are **not yet** in the health endpoint — they need to be added
for automated collection. Alternatively, a sidecar stats script can poll
`/proc/[pid]/stat` and serve the result.

---

## Evaluation matrix

| | Unfiltered (A) | Reg dedup (B) | float_dedup (C) |
|---|---|---|---|
| **InfluxDB — CPU** | baseline | expected lower | expected lowest |
| **InfluxDB — Mem** | baseline | similar | similar |
| **InfluxDB — Storage** | baseline | −20–30% | −40–60% (est.) |
| **InfluxDB — Rows/s** | baseline | −23% int/bool | −40–60% all types |
| **Parquet — CPU** | baseline | TBD | TBD |
| **Parquet — Mem** | baseline | TBD | TBD |
| **Parquet — File size** | baseline | TBD | TBD |
| **Parquet — Rows/s** | baseline | TBD | TBD |

---

## What needs to be built

### Ready now
- [x] `ems_site_simulator` with smooth/burst toggle
- [x] Telegraf-A config (unfiltered, :8096)
- [x] float_dedup binary (`source/telegraf_plugins/float_dedup/`)
- [x] parquet_writer (real_writer binary, gx10)
- [x] Demo InfluxDB Viewer page

### Needs building
- [ ] Telegraf-B config (regular dedup → :8097) — update start script to add as separate instance
- [ ] Telegraf-C config (float_dedup execd → :8098) — new third instance
- [ ] InfluxDB :8098 instance — extend start script
- [ ] MQTT bridge for parquet filtered paths (Option 2: Telegraf mqtt→mqtt, or Option 1: Go bridge)
- [ ] 3× parquet_writer configs for eval paths (`/data/parquet-eval/{a,b,c}/`)
- [ ] CPU/RSS fields in parquet_writer health endpoint (or sidecar stats script)
- [ ] Eval dashboard HTML page (`html/writer/demo_eval.html`)
- [ ] Stats collection script on gx10 (polls all 6 process metrics, serves JSON on a port)

### Open questions
1. **Parquet format for filtered streams**: does float_dedup output change the
   schema expected by the parquet writer? float_dedup preserves the original
   Influx line protocol fields/tags, so the schema should be unchanged.
2. **Three-broker vs one-broker**: can all six consumers (3 Telegraf + 3 parquet)
   subscribe to the same FlashMQ :11888? Yes — FlashMQ handles high fan-out well.
3. **Regular dedup for parquet**: the `processors.dedup` plugin only works inside
   Telegraf. For parquet path B, the MQTT bridge approach is needed.
4. **Evaluation duration**: how long to run each test before collecting metrics?
   Suggest 10 minutes with burst mode to get representative dedup statistics.

---

## Proposed gx10 script layout

```
scripts/
  eval-start.sh          # start all 6 pipelines + generator
  eval-stop.sh           # stop all
  eval-stats.sh          # snapshot CPU/mem/storage for all 6 processes → JSON
  eval-reset.sh          # wipe all influx + parquet eval data, restart clean

configs/
  telegraf-eval-a.conf   # unfiltered → :8096 ems-baseline
  telegraf-eval-b.conf   # reg dedup  → :8097 ems-dedup
  telegraf-eval-c.conf   # float_dedup→ :8098 ems-floatdedup
  telegraf-bridge-b.conf # MQTT→dedup→MQTT republish for parquet-B
  telegraf-bridge-c.conf # MQTT→float_dedup→MQTT republish for parquet-C
  writer-eval-a.yaml     # parquet writer, /data/parquet-eval/a/, health :8881
  writer-eval-b.yaml     # parquet writer, /data/parquet-eval/b/, health :8882
  writer-eval-c.yaml     # parquet writer, /data/parquet-eval/c/, health :8883
```
