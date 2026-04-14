# Float dedup / COV filtering — design note

## Background

`real_writer.cpp` currently writes every MQTT message as one parquet row.
The `sample_count` column (INT64, always 1 today) is a placeholder for a
future change-on-value (COV) dedup stage that would collapse runs of
identical readings into a single row and record how many samples it
represents.

## Why float dedup is non-trivial

For integer and boolean signals, equality is exact — dedup is
straightforward. For float signals, raw sensor output typically has
low-order noise (ADC quantisation, cable pickup, rounding in firmware),
so two "identical" readings may differ by ±1 LSB even when the physical
quantity has not changed.

A bitwise equality check would therefore almost never trigger for floats,
making the dedup useless on real hardware.

## Proposed approach: per-signal epsilon band

Each signal would have a configured dead-band:

```
dedup:
  enabled: true
  default_epsilon: 0.0          # exact match (safe default for ints)
  max_hold_seconds: 60          # flush even if unchanged, at least every N s
  per_signal:
    voltage:  0.01              # V  — ignore changes < 10 mV
    current:  0.05              # A
    soc:      0.1               # %
    temp:     0.1               # °C
```

A new value is written only when `|new - last| > epsilon` **or**
`now - last_written_ts > max_hold_seconds`.

`sample_count` is incremented for every suppressed message, and reset to 1
when a new row is written.

## State required per signal

```
struct DedupState {
    double   last_value;          // last value written to parquet
    int64_t  last_int_value;      // for integer signals
    int64_t  sample_count;        // accumulator
    int64_t  last_written_ts_ms;  // epoch ms of last written row
};
// keyed by: site_id + unit_id + device + instance + point_name
std::unordered_map<std::string, DedupState> g_dedup;
```

## Flush considerations

- On flush (periodic or shutdown) all held `DedupState` entries must emit
  their accumulated row so no data is silently lost.
- The emitted row carries the **last seen value** and the accumulated
  `sample_count`, with `ts` set to the timestamp of the last suppressed
  message (not the first).
- This preserves the ability to reconstruct "value was X from T1 to T2"
  by joining on `ts` and `sample_count`.

## Schema impact

`sample_count` is already present in every row written by `real_writer.cpp`
(value = 1). No schema migration is needed when COV is enabled — DuckDB
and the compactor handle the existing column transparently.

## Open questions

1. Should epsilon be absolute or relative (% of range)?
2. Should `max_hold_seconds` be per-signal or global?
3. Do we want a `dedup_ratio` health metric (suppressed / total msgs)?
