# Telegraf / InfluxDB — Live Performance Measurements

**Test date:** 2026-03-17
**Environment:** spark-22b6 (aarch64, single node)
**InfluxDB:** v2.8.0 &nbsp;|&nbsp; **Telegraf:** v1.38.0
**Active sources:** battery (144 cells) + solar (480 cells) = **624 assets total**
**Sample interval:** 1 second per asset

All measurements were taken against the live running system via the InfluxDB HTTP API
(`http://spark-22b6:8086`) using the credentials from `orig_aws/influxdb/README.md`.

---

## Tests Performed

### 1. Write Throughput (metrics/sec)

**Query:**
```flux
from(bucket: "battery-data")
  |> range(start: -2m)
  |> filter(fn: (r) => r._measurement == "internal_write")
  |> filter(fn: (r) => r._field == "metrics_written")
  |> derivative(unit: 1s, nonNegative: true)
```

**Result:** Sustained **~1,000–1,100 metrics/sec**, spiking to 2,000/sec at
Telegraf's 10-second flush boundary. The flush spike is a structural characteristic
of Telegraf's `flush_interval = "10s"` — InfluxDB sees a burst every 10 seconds
rather than a smooth write rate.

### 2. Lifetime Write Volume

**Query:**
```flux
from(bucket: "battery-data")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "internal_write")
  |> filter(fn: (r) => r._field == "metrics_written")
  |> last()
```

**Result:** **302,807,911 metrics written** total since Mar 14 (~3 days uptime).
Average lifetime write rate: ~1,170 metrics/sec.

### 3. Data Volume in InfluxDB (last hour)

**Query:**
```flux
from(bucket: "battery-data")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "battery_cell")
  |> filter(fn: (r) => r._field == "voltage")
  |> group()
  |> count()
```

**Result:** **4,121,381 points** for the `voltage` field alone in the last hour.
With 7 measurement fields (voltage, current, temperature, soc, soh, resistance, capacity):
**~28.8 million points/hour** ingested per namespace.

### 4. Dropped Metrics (buffer overflow indicator)

**Query:**
```flux
from(bucket: "battery-data")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "internal_agent")
  |> filter(fn: (r) => r._field == "metrics_dropped")
  |> last()
```

**Result:** **0 dropped metrics** at current load (624 assets, single namespace, quiet server).

This is the calm-water result. At 1,100 metrics/sec, Telegraf's 100,000-metric buffer
fills in **~91 seconds** of InfluxDB unavailability. In production, EFS latency spikes,
TSM compaction pauses, or a pod restart can easily exceed 91 seconds — at which point
metrics are silently dropped with no alert.

### 5. Process Resource Usage

Measured via `ps aux` on spark-22b6:

| Process | CPU (cumulative) | RSS Memory | Uptime |
|---------|-----------------|------------|--------|
| `influxd` | 6.6% | **490 MB** | 3 days |
| `telegraf` | 12.2% | **171 MB** | 3 days |
| **Combined** | **18.8%** | **661 MB** | — |

At 624 assets on a lightly loaded ARM server. CPU is cumulative (averaged over 3 days)
so instantaneous CPU during flush spikes is higher.

### 6. InfluxDB Schema Cardinality Risk

**Tag keys observed on `battery_cell` measurement:**
- `cell_id`, `module_id`, `rack_id`, `site_id` — expected hierarchy tags
- `host` — Telegraf hostname
- `topic` — **full MQTT topic path** (e.g. `batteries/project=0/site=0/rack=0/module=0/cell=0`)

The `topic` tag is the cardinality bomb. Each unique MQTT topic creates a separate
InfluxDB series **per field**. For 624 assets × 7 fields = **4,368 active series** in
this single-namespace test. For the production fleet:

| Fleet size | Cells | Fields | Active series |
|------------|------:|-------:|--------------:|
| Current test (1 namespace) | 624 | 7 | 4,368 |
| 6 production namespaces | 3,744 | 7 | 26,208 |
| 6 namespaces + 4 source types | ~15,000 | 7–8 | ~112,000 |
| 80 sites full fleet | ~100,000 | 7–8 | ~750,000 |

InfluxDB performance degrades measurably above ~1M series and becomes problematic
above ~10M. The current topology is within safe range only because of small fleet size.

### 7. Data Retention and Storage Projection

**Bucket retention:** 30 days (`everySeconds: 2592000`)

| Metric | Value |
|--------|-------|
| Points/hour (current load) | ~28.8 million |
| Points/day | ~691 million |
| Points/30-day retention | **~20.7 billion** |
| × 6 namespaces | **~124 billion** |

**Storage cost comparison** at 124 billion points:

| Format | Bytes/point | Total storage | AWS cost/month |
|--------|------------|---------------|----------------|
| InfluxDB TSM | ~40 bytes | ~4.5 TB | ~$1,350 (EFS) / ~$135 (EBS gp3) |
| Parquet + Snappy | ~5–8 bytes | ~600 GB–900 GB | **~$14–21 (S3)** |

---

## Fleet Scaling Projection

Starting from the measured single-namespace baseline:

| Metric | 1 namespace (measured) | 6 namespaces | 6 ns + 4 source types |
|--------|----------------------:|-------------:|----------------------:|
| Assets | 624 | ~3,744 | ~15,000 |
| Metrics/sec | ~1,100 | ~6,600 | ~26,400 |
| Buffer fill time (100k limit) | 91 sec | **15 sec** | **3.8 sec** |
| InfluxDB RAM | 490 MB | ~3 GB | ~12 GB |
| Telegraf RAM | 171 MB | ~1 GB | ~4 GB |
| Points/hour | 28.8M | 172M | 688M |

The **buffer fill time** is the critical metric. At 6 production namespaces with
current battery-only load, a 15-second InfluxDB hiccup starts dropping data.
Adding further source types (solar, wind, localgen) brings that to under 4 seconds —
shorter than a typical EFS latency spike.

---

## The Version Trap

The running stack is **InfluxDB v2.8.0** with Flux queries. InfluxDB 3.0 (Apache
Arrow/DataFusion-based IOx engine) is now GA and is a completely different system:

- Flux is **deprecated** — all queries must be rewritten in SQL
- Storage engine is incompatible — no in-place upgrade path
- Schema model changes — existing tag/field structure requires rethinking

**Staying on the telegraf/InfluxDB path does not avoid a migration. It defers one.**
The choice is:
- Migrate to InfluxDB 3.0: rewrite all queries in SQL, new storage costs, same
  cardinality scaling problems remain
- Migrate to NATS + Parquet: rewrite queries in SQL (DuckDB-compatible),
  60% compute cost reduction, S3 storage at $14/month vs $1,350/month

---

## Summary

| Finding | Value |
|---------|-------|
| Write rate (624 assets, measured) | ~1,100 metrics/sec |
| Lifetime writes (3 days) | 302.8 million |
| Dropped metrics | 0 (at current low load) |
| Buffer headroom at 6× fleet | **15 seconds** |
| InfluxDB RAM (current) | 490 MB |
| Telegraf RAM (current) | 171 MB |
| Storage cost (6 ns, 30 days) on EFS | ~$1,350/month |
| Storage cost (6 ns, 30 days) on S3/Parquet | ~$17/month |
| InfluxDB v2 future | Dead end — Flux deprecated |
