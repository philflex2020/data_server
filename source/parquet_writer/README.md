# parquet_writer — MQTT → Parquet ingest daemon

Subscribes to an MQTT broker and writes time-series rows into partitioned Parquet files.
Designed for 80 k+ msg/s: a lock-free SPSC ring buffer (`mqtt_ring.h`) keeps the
libmosquitto network thread free — `on_message` does only a `memcpy`.

---

## Quick start

```bash
make parquet_writer
./parquet_writer --config writer_config.yaml
```

Health check:

```bash
curl http://localhost:8771/health
```

---

## Topic parsing

The writer extracts structured metadata from the MQTT topic string and stores each
segment as a column in the Parquet row.  There are three modes, selected by
`mqtt.topic_parser` in the config.

---

### Mode 1 — `positional` (most common)

Topic segments are mapped positionally to column names via `topic_segments`.

**Special segment names:**

| Name | Effect |
|---|---|
| `_` | Skip — segment is consumed but not stored |
| `dtype_hint` | Consumed for value casting (`float` → FLOAT64, `integer`/`boolean_integer` → INT64); not stored as a column |
| `field_name` | Renames the `value` column to this segment's value (wide-sparse schema); not stored as a separate column |
| anything else | Stored as a UTF-8 string column with that name (narrow schema) |

**How `source_type` is derived:**
`source_type` is always `parts[0]` — the first segment of the topic.  It feeds into the
output directory path: `{base_path}/{source_type}/{partitions…}/{timestamp}.parquet`.

#### Example — single-prefix topic

```
topic:  unit/0215F562/bms/bms_1/Batt1_SBMU1_Current/float
parts:  [0]unit  [1]0215F562  [2]bms  [3]bms_1  [4]Batt1_SBMU1_Current  [5]float
```

Config:

```yaml
mqtt:
  topic: "unit/#"
  topic_parser: positional
  topic_segments:
    - "_"          # 0: "unit" — literal, skip
    - unit_id      # 1: stored as string column
    - device       # 2: stored as string column
    - instance     # 3: stored as string column
    - point_name   # 4: stored as string column
    - dtype_hint   # 5: drives INT64 vs FLOAT64 cast, not stored
```

Resulting Parquet columns: `ts`, `unit_id`, `device`, `instance`, `point_name`, `value` (FLOAT64),
`site_id` (from `output.site_id`), `mqtt_topic` (full topic string).

#### Example — namespaced/sliced topic (multi-writer deployment)

When multiple writers share a broker namespace, each slice gets a prefix letter.

```
topic:  A/unit/0215F562/bms/bms_1/Batt1_SBMU1_Current/float
parts:  [0]A  [1]unit  [2]0215F562  [3]bms  [4]bms_1  [5]Batt1_SBMU1_Current  [6]float
```

Config:

```yaml
mqtt:
  topic: "A/unit/#"
  topic_parser: positional
  topic_segments:
    - "_"          # 0: "A" — slice prefix, skip
    - "_"          # 1: "unit" — literal, skip
    - unit_id      # 2
    - device       # 3
    - instance     # 4
    - point_name   # 5
    - dtype_hint   # 6
```

The output path is `{base_path}/A/{partitions…}/{timestamp}.parquet` because
`source_type = parts[0] = "A"`.

---

### Mode 2 — `kv` (key=value topics)

Segments in the form `key=value` are parsed into integer columns.  Bare segments
(no `=`) set `field_name`.

```
topic:  sensors/site=3/rack=7/temperature
```

```yaml
mqtt:
  topic: "sensors/#"
  topic_parser: kv
```

Resulting columns: `ts`, `site` (INT64 = 3), `rack` (INT64 = 7), `value` (column named
`temperature` in wide-sparse mode).

---

### Mode 3 — `topic_patterns` (multi-pattern / Telegraf-style)

For brokers that publish mixed topic structures, define multiple patterns.  The first
matching pattern wins.  Use `+` to match any single segment, `#` to match the remainder.

```yaml
mqtt:
  topic: "#"
  topic_patterns:
    - match: "ems/+/+/+/+"
      segments:
        - "_"        # 0: "ems"
        - site_id    # 1
        - device     # 2
        - point_name # 3
        - dtype_hint # 4

    - match: "legacy/+/+"
      segments:
        - "_"        # 0: "legacy"
        - unit_id    # 1
        - point_name # 2

    - match: "debug/#"
      segments:
        - "_"        # 0: "debug"
        - subsystem  # 1
```

The `reverse: true` flag maps `segs[0]` to the **last** topic segment, working
right-to-left.  Useful when the meaningful part is at the end of variable-depth topics.

```yaml
    - match: "telemetry/#"
      reverse: true
      segments:
        - dtype_hint   # last segment
        - point_name   # second-to-last
```

Unmatched topics are written with empty tags and a warning logged once per unique
depth+prefix combination (for discovery during bringup).

---

## Output path

```
{base_path}/{source_type}/{partition_1}/{partition_2}/…/{timestamp}.parquet
```

Partitions are rendered by substituting `{var}` tokens:

| Token | Value |
|---|---|
| `{site_id}` | `output.site_id` from config |
| `{year}` | UTC year of flush time |
| `{month}` | UTC month |
| `{day}` | UTC day |
| `{<field>}` | Any string column parsed from the topic |

Default partition layout: `site={site_id}/{year}/{month}/{day}`.

---

## Filename prefix — avoiding collisions across writers

When multiple writer instances write to the **same `base_path`** (or when files are
later merged into a shared directory), the default timestamp filename
(`1711234567890.parquet`) can collide.

Use `output.filename_prefix` to prepend a fixed string:

```yaml
output:
  base_path: /data/parquet-evelyn
  filename_prefix: "slice-a"   # → slice-a_1711234567890.parquet
```

Alternatively, give each writer a distinct `base_path` or a distinct `source_type` via
the topic prefix (the `A/`, `B/`, … pattern above is the preferred approach on gx10):

```
/data/parquet-evelyn/A/site=0215D1D8/2026/03/30/1711234567890.parquet
/data/parquet-evelyn/B/site=0215D1D8/2026/03/30/1711234567890.parquet
```

DuckDB can query both slices in a single glob:

```sql
SELECT * FROM read_parquet('/data/parquet-evelyn/*/site=0215D1D8/*/*/*.parquet')
WHERE point_name = 'Batt1_SBMU1_Current';
```

---

## MQTT topic options

### Single-writer, flat namespace

```
unit/{unit_id}/{device}/{instance}/{point_name}/{dtype_hint}
```

Subscribe: `unit/#`

### Multi-writer, prefixed namespace (gx10 style)

```
A/unit/{unit_id}/…
B/unit/{unit_id}/…
```

Subscribe per writer: `A/unit/#`, `B/unit/#`.  The prefix becomes `source_type` in the
output path automatically — no extra config needed beyond adding one extra `"_"` skip in
`topic_segments`.

### Site-qualified namespace (multi-site broker)

```
{site_id}/unit/{unit_id}/…
```

Subscribe: `{site_id}/unit/#` or `+/unit/#` with a pattern rule.  Use `site_id` as the
first non-skipped segment name to override the config `site_id` per-message.

### Mixed-type broker (use `topic_patterns`)

When a single broker carries EMS, HVAC, metering, etc.:

```
ems/{site}/{device}/{point}/{dtype}
hvac/{zone}/{sensor}
meter/{site}/{channel}
```

Subscribe: `#`
Define one `topic_patterns` entry per structure.  Each can have different segment names
and independent column schemas.  Rows from different patterns coexist in the same
Parquet files as long as they land in the same partition; DuckDB handles sparse columns
gracefully.

---

## Full config reference

```yaml
mqtt:
  host: localhost
  port: 1883
  client_id: parquet-writer
  topic: "unit/#"          # MQTT subscription filter
  qos: 0

  # Parser mode: positional | kv
  topic_parser: positional

  # Positional map (segment index → column name)
  topic_segments:
    - "_"
    - unit_id
    - device
    - instance
    - point_name
    - dtype_hint

  # OR: multi-pattern (mutually exclusive with topic_segments)
  # topic_patterns:
  #   - match: "unit/+/+/+/+/+"
  #     segments: ["_", unit_id, device, instance, point_name, dtype_hint]
  #   - match: "status/+"
  #     segments: ["_", unit_id]
  #     reverse: false

  partition_field:  site_id
  timestamp_field:  ts
  timestamp_format: iso8601

  # MQTT tuning
  keepalive_seconds:     60
  reconnect_min_seconds: 2
  reconnect_max_seconds: 30
  connect_retry_seconds: 5

output:
  base_path: /srv/data/parquet-ems
  site_id: "0215D1D8"
  filename_prefix: ""          # optional: prepended to timestamp filename
  partitions:
    - "site={site_id}"
    - "{year}"
    - "{month}"
    - "{day}"
  compression: snappy          # snappy | zstd | lz4 | none
  flush_interval_seconds: 60
  max_messages_per_part: 5000000
  max_total_buffer_rows: 2000000
  flush_max_retries: 3
  flush_retry_base_seconds: 1

wal:
  enabled: true
  # path: ""   # default: {base_path}/.wal/

health:
  enabled: true
  port: 8771

guard:
  min_free_gb: 1.0

compact:
  enabled: false
  interval_seconds: 600
  min_files: 3
  min_age_seconds: 0
```

---

## Build

```bash
make              # builds parquet_writer (C++20, requires g++ >= 10)
make test         # 32 unit tests
make kpi          # throughput benchmarks
make check-deps   # diagnose missing libraries
make deps         # install apt packages (requires sudo)
```

Dependencies: `libarrow-dev`, `libparquet-dev`, `libmosquitto-dev`,
`libyaml-cpp-dev`, `libsimdjson-dev`.
