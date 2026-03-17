# Telegraf / InfluxDB — Live Performance Measurements (x86_64)

**Test date:** 2026-03-17 02:57 UTC
**Environment:** phil-dev (phil-dev) (x86_64), single node
**InfluxDB:** 2.8.0 | **Telegraf:** 1.38.0
**Active assets:** 1,297 battery series
**Sample interval:** 1 second per asset

> See also: `telegraf_influx_details.md` for the peer architecture results.

All measurements taken against the live running system via the InfluxDB HTTP API.

---

## Test 0 — Flux API

**Result:** Flux v2 API returned data. All further tests also run via InfluxQL for consistency.

---

## Test 1 — Write Throughput

```sql
-- messages_received delta over 30s observation window
SELECT messages_received FROM internal_mqtt_consumer ORDER BY time DESC LIMIT 1
```

**Result:** **—** records (messages), **—/sec** field writes (9 fields × —).

---

## Test 2 — Lifetime Write Volume

```sql
SELECT last(metrics_written) FROM internal_write
```

**Result:** **108,552 metrics written** (lifetime total).

---

## Test 3 — Data Points in InfluxDB (last hour)

```sql
SELECT count(voltage) FROM battery_cell WHERE time > now()-1h
```

**Result:** **600,549 voltage points** in the last hour.
With 9 fields: **~5,404,941 total field values/hour**.

---

## Test 4 — Dropped Metrics

```sql
SELECT last(metrics_dropped) FROM internal_write
```

**Result:** **0 dropped metrics**.

---

## Test 5 — Process Resource Usage

Measured via `ps aux`:

| Process | CPU% | RSS Memory |
|---------|-----:|----------:|
| `influxd` | 4.2% | 1861 MB |
| `telegraf` | 0.9% | 150 MB |

---

## Test 6 — Series Cardinality

```sql
SHOW SERIES FROM battery_cell
SHOW TAG VALUES FROM battery_cell WITH KEY = site_id  -- (repeated per tag)
```

**Result:** **1,297 unique series**.

| Tag | Unique values |
|-----|:---:|
| site_id | 3 |
| rack_id | 4 |
| module_id | 8 |
| cell_id | 12 |

---

## Test 7 — Known Failure Mode Signatures

Scans journalctl (telegraf) and FlashMQ log for known silent failure patterns.

| Failure mode | Detected |
|---|:---:|
| Nested payload parse errors | ⚠ YES |
| FlashMQ QoS drop warnings | ⚠ YES |
| Consumer stall (counter frozen during sample) | ⚠ YES |

---

## Summary

| Finding | Value |
|---------|-------|
| Write rate (records/sec) | — |
| Field writes/sec | — |
| Lifetime writes | 108,552 |
| Dropped metrics | 0 |
| InfluxDB RAM | 1861 MB |
| Telegraf RAM | 150 MB |
| Buffer headroom | — |
| Series count | 1,297 |
| Flux v2 API | Working |
| Nested payload parse errors | DETECTED |
| QoS drop / consumer stall | DETECTED |
