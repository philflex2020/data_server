# Tesla Edge Storage — Telemetry Architecture

Reference notes on how Tesla handles high-frequency vehicle telemetry, and how
that maps to the Evelyn edge ingest problem.

## On-vehicle (edge)

- Logs to a local **SQLite** database on the vehicle's Linux filesystem
- Also writes binary **`.clog` / `.bz2`** compressed log files per subsystem
  (CAN bus, motor controller, thermal, etc.)
- A dedicated "diag" partition (~few GB) is reserved on the eMMC for this
- Sampling rates vary by signal:
  - High-frequency (motor current, torque): ~100 Hz
  - Slow signals (SOC, temperatures): ~1 Hz

## Upload to Tesla servers

- Opportunistic — triggered when parked on WiFi (home, Supercharger)
- Batched, not real-time
- Estimated ~1–2 GB/day per vehicle of raw telemetry (varies with drive activity)
- At fleet scale (~6M vehicles) this is potentially petabytes/day ingested
- Destination: Tesla backend (mix of AWS and own DCs)

## Compression and reduction techniques

### Delta encoding
For slowly-changing signals (SOC, temperature) store the first value then only
the difference. A signal holding at 72.3 °C for 30 seconds becomes one stored
value plus a run of near-zero deltas.

### Run-length encoding (RLE)
For boolean/discrete signals (contactor state, fault flags) that hold a value
for extended periods, store value + duration rather than repeating samples.

### Adaptive downsampling on quiescence
High-frequency signals are logged at full rate only during transients. When a
signal is "boring" (low variance over a sliding window) the log rate
automatically drops to ~1 Hz. The interesting data is preserved; the flat
sections are compressed away.

### Deadband threshold (change-on-value with tolerance)
For analog signals a deadband is applied: a new sample is only written if the
value has moved by more than X% from the last logged value. This is applied at
the **logger level** — data that hasn't changed meaningfully never gets written
at all, which is more efficient than filtering downstream.

### Binary serialisation — Protobuf
Wire format is binary (not JSON), schema-versioned via Protobuf. Compact and
CPU-efficient to encode/decode.

## Mapping to Evelyn edge ingest

| Tesla technique | Evelyn / our filter equivalent |
|---|---|
| Deadband on analog signals | Filter B (dedup) — time-window today, value-delta is the stretch goal |
| Drop quiescent boolean signals | Filter B `tagpass` on `boolean_integer` |
| Drop string / enum signals | Filter A |
| Protobuf binary on-wire | Parquet columnar binary at rest |
| SQLite / clog on eMMC | Parquet on `/data` (external drive) |
| CAN bus → local writer | MQTT → `real_writer` |
| WiFi batch upload | `push_agent` rsync |
| Tesla backend TSDB | InfluxDB / DuckDB |

## Key gap in current Evelyn approach

Our Filter B uses `dedup_interval = 60s` — **time-based**: one sample passes per
minute regardless of whether the value changed. Tesla's deadband is
**value-based**: only store on meaningful change.

The time-based approach is the Conservative scenario (~27% reduction). Moving to
value-based deadband is the Aggressive scenario (~85% reduction) from the cost
analysis. The Telegraf native `processors.dedup` does not support deadband —
a `starlark` processor would be needed to implement per-signal thresholds.
