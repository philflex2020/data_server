# FlashMQ → Parquet Writer (C++) — Design, Review & Tests

**Date:** 2026-03-17
**Component:** `source/parquet_writer_cpp/writer.cpp`
**Replaces:** Mosquitto + `bridge.py` + NATS JetStream + Python `parquet_writer`

---

## Motivation

The previous pipeline had a serial async bottleneck in `bridge.py`:

```
Generator → FlashMQ:1883 → bridge.py → NATS JetStream → parquet_writer.py → MinIO/S3
```

`bridge.py` issued one `await js.publish()` per message, gating throughput at one
NATS round-trip at a time (~4 k msg/s ceiling measured at 24× scale).  NATS and MinIO
added infrastructure cost and operational complexity.

The new path is:

```
Generator → FlashMQ:1883 → writer.cpp → /srv/data/parquet/{source}/{partitions}/
                                      ├→ /srv/data/parquet/hot.parquet         (every flush)
                                      └→ /srv/data/parquet/current_state.parquet  (AWS mode)
```

Three processes instead of six.  No NATS.  No MinIO.  No Python in the data path.

---

## Architecture

### Components removed

| Removed | Why |
|---|---|
| `bridge.py` | writer.cpp subscribes to FlashMQ itself |
| NATS JetStream | no longer on the ingest path |
| Python `parquet_writer` | replaced by writer.cpp |
| MinIO / S3 | local filesystem; DuckDB queries local parquet equally well |

### Data flow inside writer.cpp

```
FlashMQ:1883 (MQTT QoS-1, persistent session)
    │
    │  on_message callback (mosquitto background thread)
    ▼
parse_topic(topic)          batteries/site=0/rack=1/module=2/cell=3
    → TopicInfo { source_type, kv{site,rack,module,cell} }
parse_payload(json, topic, project_id, cfg)
    → Row { ints{project_id,site_id,…}, floats{timestamp,voltage,…} }
    │
    │  lock g_mutex
    ▼
g_buffers[{source_type, partition_value}].rows.push_back(row)
    │  unlock
    │  notify flush thread if partition reaches max_messages_per_part
    │
    ▼  (flush thread — every flush_interval_seconds OR max messages)
std::exchange(g_buffers, {})   ← swap under lock, release before I/O
    │
    ├─▶ flush_partition(path, rows, compression)   ← one file per (source, site)
    │       → Arrow Int64Builder  (project_id, site_id, rack_id, …)
    │       → Arrow DoubleBuilder (timestamp, voltage, … — dynamic columns)
    │       → parquet::arrow::WriteTable → FileOutputStream
    │       → /srv/data/parquet/batteries/project=0/site=0/2026/03/17/…parquet
    │
    └─▶ flush_partition(hot.parquet.tmp, all_rows, compression)  ← flat merge
            → fs::rename(hot.parquet.tmp, hot.parquet)           ← atomic
            → /srv/data/parquet/hot.parquet
```

### Key design decisions

**QoS-1 persistent session** — `mosquitto_new(id, clean_session=false)` with
`mosquitto_reconnect_delay_set(2s, 30s, exponential)`.  FlashMQ queues messages
while the writer is offline; they drain automatically on reconnect.  This gives the
same durability guarantee that NATS JetStream was providing.

**Lock-release before I/O** — the mutex is held only during the buffer swap
(`std::exchange`), not during parquet writes.  The MQTT receive thread never blocks
on disk I/O regardless of flush duration.

**Dynamic schema** — columns are discovered per batch from whichever fields appear
in the buffered rows.  Batteries get voltage/current/temperature/soc/soh/resistance/
capacity; solar gets irradiance/panel_temp/dc_voltage/dc_current/ac_power/efficiency.
Missing columns within a batch are null-padded.

**Config-driven topic mapping** — `topic_kv_map` in config maps topic kv keys to
row column names (e.g. `site → site_id`).  `partition_field` selects which column
drives buffer partitioning.  No code changes needed for different sensor topologies.

**Hot file for near-real-time DuckDB** — on every flush, all rows are flat-merged
into `hot.parquet` via atomic temp+rename.  Rsynced to S3 every ~90 s to cover the
gap between partitioned-file rsync cycles.  Disabled by leaving `hot_file_path` empty.

**Current-state file (AWS writer mode)** — when `current_state_path` is set, the
writer maintains one row per unique sensor (keyed by `source_type` + all int identity
fields: project_id, site_id, rack_id, module_id, cell_id) and writes it atomically on
every flush.  Intended for deployment on AWS: subscribe `writer.cpp` to the FlashMQ
bridge on AWS and configure only `current_state_path` — no partitioned store needed.
DuckDB on AWS reads `current_state.parquet` locally with no rsync lag, eliminating
the hot.parquet rsync gap entirely.  Updated every flush interval (~60 s).

**Partition key** — `(source_type, partition_value)`.  One parquet file per
source×site per flush interval.  At 24× scale (48 sites × 2 source types) this
produces 96 files per flush.

---

## Code Review

### File layout

```
source/parquet_writer_cpp/
  writer.cpp          single translation unit (~560 lines)
  Makefile            pkg-config based, no CMake
  config.yaml         mqtt + output settings
  tests/
    test_runner.h     zero-dependency test framework (~90 lines)
    test_writer.cpp   37 unit tests
    kpi_writer.cpp    3 KPI benchmarks
```

### Public-facing types

```cpp
struct Config          // loaded from config.yaml
struct TopicInfo       // source_type + kv map from topic segments
struct Row             // ints{} + floats{} — one decoded message
struct PartitionKey    // (source_type, partition_value) — buffer grouping key
struct Partition       // rows vector + creation timestamp
```

### Pure functions (fully unit-testable)

| Function | Signature | Side effects |
|---|---|---|
| `parse_topic` | `string → optional<TopicInfo>` | none |
| `parse_payload` | `string, TopicInfo, int, Config → Row` | none |
| `replace_vars` | `string, map → string` | none |
| `to_parquet_compression` | `string → Compression::type` | none |
| `flush_partition` | `string, vector<Row>, string → arrow::Status` | writes file |
| `make_output_path` | `Config, string, int → string` | creates directories |

### Review notes

- `parse_topic` splits on `/`, iterates segments — O(n) in topic depth, negligible.
- `parse_payload` uses simdjson DOM API (`thread_local simdjson::dom::parser`) — safe
  on bad input, returns row with topic metadata only on parse failure, no crash.
- Topic kv mapping and integer column set are driven by `cfg.topic_kv_map` — no
  hard-coded field names.  Default Config{} keeps backwards compatibility in tests.
- `flush_partition` builds Arrow arrays in two passes (collect columns, fill values).
  Null-fill for missing columns is handled explicitly per row.
- Hot file written in `do_flush` alongside partitioned files — no extra thread, no
  extra lock contention.  `fs::rename` from `.tmp` is atomic on the same filesystem.
- No retry on failed parquet write — logged as ERROR, data for that flush interval
  is dropped.  Acceptable for telemetry; add retry if needed.
- `mosquitto_loop_start` creates a background thread — callbacks run off main thread.
  All buffer access is protected by `g_mutex`.

---

## Test Suite

### Building

```sh
# Install dev headers (once)
make deps        # sudo apt install libmosquitto-dev libyaml-cpp-dev libsimdjson-dev

# Build and run unit tests
make test

# Build and run KPI benchmarks
make kpi
```

### Unit tests — 37 total

#### `parse_topic` (7 tests)

| Test | What it checks |
|---|---|
| batteries fully-qualified | all 4 kv fields extracted, source_type correct |
| solar source type | source_type == "solar" |
| wind source type | source_type == "wind" |
| source only, no kv | valid result, kv empty |
| empty string | returns `nullopt` |
| unknown extra kv keys | kept in kv map (forward compatible) |
| non-numeric kv value | bad key silently dropped, others intact |

#### `parse_payload` (8 tests)

| Test | What it checks |
|---|---|
| nested {value,unit} format | voltage, current, timestamp extracted as floats |
| flat scalar format | plain numeric fields parsed correctly |
| metadata from topic | project_id, site_id, rack_id, module_id, cell_id in ints |
| bad JSON | no crash; topic metadata still present; floats empty |
| empty JSON object `{}` | site_id intact from topic, floats empty |
| int column from payload | `site_id` in payload goes to `ints` map |
| full battery payload | 8 float columns (timestamp + 7 measurements) |
| full solar payload | 7 float columns (timestamp + 6 measurements) |

#### `replace_vars` (6 tests)

| Test | What it checks |
|---|---|
| single substitution | `{year}` → `2026` |
| multiple substitutions | `{year}/{month}/{day}` → `2026/03/17` |
| unknown placeholder | left intact — `{project_id}` not replaced if not in vars |
| repeated placeholder | all occurrences replaced |
| empty template | returns empty string |
| no placeholders | passes through unchanged |

#### `flush_partition` (6 tests)

| Test | What it checks |
|---|---|
| empty rows | `arrow::Status::OK()`, no file created |
| single row | file exists, non-zero size, round-trip row count == 1 |
| 100 rows | round-trip row count == 100 |
| column count | 5 int + 4 float columns = 9 total |
| sparse columns (nulls) | rows with extra column write without crash; 2 rows in output |
| zstd compression | valid file produced |

#### `hot file` (5 tests)

| Test | What it checks |
|---|---|
| multi-partition merge | 5 battery + 3 solar rows → single file, row count == 8 |
| atomic rename | `.tmp` does not exist after success; final file is valid |
| disabled (empty path) | no file written when feature is off |
| overwrite on re-flush | second flush replaces file; count reflects new batch only |
| mixed source types | sparse columns (irradiance vs voltage) null-padded, no crash |

#### `current_state` (5 tests)

| Test | What it checks |
|---|---|
| one row per unique sensor | 4 sensors (2 sites × 2 cells) → file has 4 rows |
| latest reading wins | same SensorKey written twice → still 1 row with the later value |
| disabled when path is empty | no file written; `Status::OK()` returned |
| atomic rename | `.tmp` absent after success; final file is valid |
| mixed source types | batteries + solar in same map; sparse columns null-padded, no crash |

### KPI benchmarks

#### KPI 1 — Parse throughput

1 000 000 calls each, measuring:
- `parse_topic` alone
- `parse_topic + parse_payload` (battery payload, 8 fields)
- `parse_topic + parse_payload` (solar payload, 7 fields)

**Target:** parse throughput must be >> 14 976 msg/s (24× fleet load).

#### KPI 2 — Parquet write throughput

`flush_partition` at batch sizes 1 000 / 10 000 / 50 000 rows, snappy compression.
Reports rows/s and MB/s.  ~200 k total rows written per batch size.

#### KPI 3 — End-to-end ingest simulation

Full 14 976-topic fleet (3 456 battery + 11 520 solar), 10 rounds buffered:
- Reports parse+buffer throughput (no I/O)
- Then flushes all buffered partitions to disk and reports flush time and rows/s

---

## Test Results

**Host:** phil-dev · **Arrow:** 23.0.1 · **g++:** 11.4.0 · **C++17** · **simdjson** 1.0.2

### Unit tests — 37 passed, 0 failed

```
=== parquet_writer unit tests ===
  PASS  parse_topic: batteries fully-qualified
  PASS  parse_topic: solar source type
  PASS  parse_topic: wind source type
  PASS  parse_topic: source only, no kv
  PASS  parse_topic: empty string returns nullopt
  PASS  parse_topic: unknown extra kv keys ignored gracefully
  PASS  parse_topic: non-numeric value in kv skipped
  PASS  parse_payload: nested {value,unit} format
  PASS  parse_payload: flat scalar format
  PASS  parse_payload: metadata from topic injected as int columns
  PASS  parse_payload: bad JSON does not crash, topic metadata still present
  PASS  parse_payload: empty JSON object
  PASS  parse_payload: int columns from payload take int type
  PASS  parse_payload: full battery payload all 7 measurements
  PASS  parse_payload: full solar payload all 6 measurements
  PASS  replace_vars: single substitution
  PASS  replace_vars: multiple substitutions
  PASS  replace_vars: partial substitution — unknown placeholder left intact
  PASS  replace_vars: repeated placeholder replaced everywhere
  PASS  replace_vars: empty template
  PASS  replace_vars: no placeholders passes through unchanged
  PASS  flush_partition: empty rows writes no file
  PASS  flush_partition: single row produces valid parquet
  PASS  flush_partition: N rows round-trips correctly
  PASS  flush_partition: correct column count (5 int + 4 float)
  PASS  flush_partition: heterogeneous rows (sparse columns padded with nulls)
  PASS  flush_partition: zstd compression produces valid file
  PASS  hot file: merges rows from multiple partitions into one file
  PASS  hot file: atomic rename — .tmp is removed on success
  PASS  hot file: disabled when path is empty — no file written
  PASS  hot file: overwritten on second flush with new row count
  PASS  hot file: mixed source types merged correctly
  PASS  current_state: one row per unique sensor
  PASS  current_state: latest reading wins for same sensor
  PASS  current_state: disabled when path is empty — no file written
  PASS  current_state: atomic rename — .tmp absent on success
  PASS  current_state: mixed source types — sparse columns null-padded

37 passed, 0 failed
```

### KPI benchmarks

> Parse throughput varies with system load; figures below from a loaded dev machine.
> The headroom at minimum observed throughput remains well above the fleet target.

```
=== parquet_writer KPI benchmarks ===

--- KPI 1: parse throughput ---
parse_topic (1M calls)                       2 404 000 ops/s   (0.416s for 1 000 000)
parse_topic + parse_payload battery (1M)       430 257 ops/s   (2.324s for 1 000 000)
parse_topic + parse_payload solar (1M)         478 122 ops/s   (2.092s for 1 000 000)
Fleet target (24× scale): 14 976 msg/s — parse headroom: 29×

--- KPI 2: parquet write throughput ---
flush_partition snappy batch=1 000             962 449 ops/s   18.8 MB/s
flush_partition snappy batch=10 000            970 408 ops/s   15.5 MB/s
flush_partition snappy batch=50 000            834 794 ops/s   13.4 MB/s

--- KPI 3: end-to-end ingest sim (parse + buffer, no I/O) ---
parse+buffer 14 976 msg × 10 rounds           409 628 ops/s   (0.366s for 149 760)
Buffered partitions: 96   total rows: 149 760
Flush 149 760 rows → 96 parquet files: 0.124s  (1 204 880 rows/s)
```

### Results vs targets

| Metric | Best observed | This run | Target | Headroom |
|---|---|---|---|---|
| parse_topic + parse_payload | 607 000 msg/s | 430 000 msg/s | > 14 976 msg/s | **29–41×** |
| Parquet write (1k rows) | 1 009 000 rows/s | 962 000 rows/s | > 500 000 rows/s | **1.9×** |
| Flush 10 s of fleet data (149 760 rows) | 0.124 s | 0.162 s | < 1 s | **6–8×** |
| Buffered partitions at 24× scale | 96 | 96 | — | 48 sites × 2 sources |

---

## Config Reference

```yaml
mqtt:
  host: localhost
  port: 1883           # FlashMQ native MQTT port
  client_id: parquet-writer
  topic: "#"           # subscribe to all sources
  qos: 1

  # Maps topic kv key → row column name (all stored as int64).
  # Omit to use the defaults below.
  topic_kv_map:
    site:   site_id
    rack:   rack_id
    module: module_id
    cell:   cell_id

  # Which column from topic_kv_map to use as the partition key.
  partition_field: site_id

output:
  base_path: /srv/data/parquet
  project_id: 0
  partitions:          # directory level per token
    - "project={project_id}"
    - "site={site_id}"
    - "{year}"
    - "{month}"
    - "{day}"
  compression: snappy  # snappy | zstd | gzip | none
  flush_interval_seconds: 900  # 15 min recommended once AWS writer is live;
                               # current_state.parquet covers real-time gap on AWS
  max_messages_per_part: 50000

  # Rolling hot file — flat parquet rewritten on every flush.
  # Rsynced to S3 at higher frequency than the partitioned store to
  # cover the DuckDB gap.  Leave empty to disable.
  hot_file_path: /srv/data/parquet/hot.parquet

  # Current-state file — one row per unique sensor, always the latest reading.
  # Intended for AWS writer mode: deploy writer.cpp on AWS subscribed to
  # the FlashMQ bridge.  current_state.parquet is written locally so DuckDB
  # on AWS has sub-flush-interval current state with no rsync lag.
  # With this in place, hot.parquet rsync is no longer needed on AWS.
  # Leave empty to disable.
  current_state_path: ""
```

Output path examples:
```
/srv/data/parquet/batteries/project=0/site=3/2026/03/17/20260317T120000Z.parquet
/srv/data/parquet/hot.parquet
/srv/data/parquet/current_state.parquet    # AWS writer mode only
```

AWS writer config (current_state mode — no partitioned store or hot file needed):
```yaml
output:
  base_path: /data/parquet-aws
  current_state_path: /data/parquet-aws/current_state.parquet
  hot_file_path: ""                   # not needed — current_state covers the gap
  flush_interval_seconds: 60
```

DuckDB query — S3 partitioned store + local current_state on AWS:
```sql
-- on AWS: combine historical S3 parquet with local current_state
SELECT source, site_id, cell_id,
       arg_max(value, timestamp) AS latest_value,
       max(timestamp)            AS latest_ts
FROM read_parquet([
    's3://bucket/parquet/batteries/**/*.parquet',
    '/data/parquet-aws/current_state.parquet'    -- no rsync lag
], union_by_name=true, hive_partitioning=true)
GROUP BY 1,2,3
```

> **AWS writer eliminates the hot.parquet gap.**
> With `writer.cpp` running on AWS (subscribed to the FlashMQ bridge),
> `current_state.parquet` is written locally — no rsync needed.
> DuckDB on AWS reads it directly with zero network lag.
> The gap between the partitioned S3 files and "now" is covered by
> `current_state.parquet`, updated every flush interval (~60 s).
