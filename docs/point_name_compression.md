# EMS Telemetry Wire Optimisation — Topic Structure

## Approach

The EMS topic structure already uses compact hex serial numbers for site and unit identifiers,
assigned by hardware and looked up in a registry when human-readable names are needed:

```
Original (full EMS topic):
  ems/site/0215D1D8/unit/02281660/bms/bms_1/Batt1_SBMU1_Current/float
           ^^^^^^^^       ^^^^^^^^
           4-byte hex     4-byte hex    ← already encoded identifiers
```

The optimisation extends the same existing pattern in two ways:

### 1. Drop site_id from the topic

The site serial (`0215D1D8`) is repeated on every message solely for routing, but the
partition directory already carries it (`site=0215D1D8/`). Removing it from the topic saves
18 bytes per message with no information loss — the writer injects it from config.

### 2. Use dtype as a parse-time hint, not a stored column

The dtype segment (`integer` / `float` / `boolean_integer`) tells the writer how to store the
value. It is consumed at parse time and not written as a parquet column. Parquet's built-in
dictionary encoding handles repeated `point_name` strings automatically — no external registry
is needed.

### Result

```
Optimised topic:
  unit/02281660/bms/bms_1/Batt1_SBMU1_Current/float
```

| Metric | Before | After |
|---|---|---|
| Avg topic length | 102 B | 84 B |
| Avg MQTT frame | 159 B | 141 B |
| Bandwidth @ 81k msgs/sec | 13.0 MB/s | 11.5 MB/s |
| Parquet compression ratio | 16.6× | 14.7× |
| External registry required | no | no |
| Parquet output human-readable | yes | yes |

## What was considered and rejected

A point name registry (4-hex-char IDs replacing full point name strings) was evaluated.
It was rejected because:

- Parquet dictionary encoding already stores repeated strings once per column chunk — the
  storage problem it would solve does not exist.
- The complexity of distributing and versioning a registry outweighs the wire savings (~54 B/msg
  further reduction beyond what the topic restructure already achieves).
- The existing encoding pattern applies to hardware-assigned identifiers; point names are
  firmware-defined strings where human readability in the topic has operational value.

## Parquet schema

```
project_id  INT64
ts          FLOAT64   Unix epoch seconds (parsed from ISO 8601 payload "ts" field)
site_id     VARCHAR   Injected from writer config — matches partition directory name
unit_id     VARCHAR   From topic segment 1
device      VARCHAR   "bms" | "pcs" | "rack"
instance    VARCHAR   "bms_1" | "bms_2" | "pcs_1" | etc.
point_name  VARCHAR   Full human-readable name — dictionary-encoded by Parquet
value_i     INT64     Populated for integer / boolean_integer dtype points
value_f     FLOAT64   Populated for float dtype points
```

## DuckDB query pattern

```sql
-- Time-series for one point
SELECT ts, COALESCE(value_f, CAST(value_i AS DOUBLE)) AS value
FROM read_parquet('/srv/data/parquet-ems/site=0215D1D8/**/*.parquet')
WHERE unit_id = '02281660'
  AND point_name = 'RackSOC'
  AND ts BETWEEN 1731700000 AND 1731710000
ORDER BY ts;

-- All active fault flags across the fleet
SELECT unit_id, point_name, count(*) AS triggers
FROM read_parquet('/srv/data/parquet-ems/site=0215D1D8/**/*.parquet')
WHERE device = 'bms' AND value_i != 0
GROUP BY unit_id, point_name
ORDER BY triggers DESC;
```

## Implementation files

| File | Role |
|---|---|
| `source/parquet_writer_cpp/real_writer.cpp` | Positional parser, dtype hint, site_id injection |
| `source/parquet_writer_cpp/real_writer_config.yaml` | topic `unit/#`, site_id, topic_segments |
| `source/stress_runner/real_stress_runner.py` | Publishes `unit/{uid}/...` topics |
| `source/stress_runner/real_stress_runner_config.yaml` | Runner config |
| `source/bridge/bridge.yaml` | `ems_all` group forwards `unit/#` |
