# NATS JetStream + Parquet Writer — Live Performance Measurements

**Test date:** 2026-03-17 04:03 UTC
**Environment:** phil-dev (x86_64)
**NATS Server:** 2.12.5
**S3/MinIO reachable:** yes

---

## Test 0 — NATS Server Health

**Result:** Server up and responding.

- Connections: 4
- Server RSS: 104 MB

---

## Test 1 — JetStream Streams

### Stream: `BATTERY_DATA`

- Subjects: `?`
- Messages stored: **21,538,864**
- Bytes stored: **8.59 GB**
- Retention: **0.0h**

| Consumer | Pending (lag) | Ack floor |
|----------|:---:|:---:|
| `4zAEfCoF` | 0 | 27,745,824 |
| `BR0VHyrE` | 0 | 27,745,824 |
| `parquet-writer` | 10,856 | 0 |

---

## Test 2 — Message Throughput

**Result:** **913.6/sec** messages ingested to JetStream.

---

## Test 3 — Consumer Lag

**Result:** 4zAEfCoF: 0 pending, BR0VHyrE: 0 pending, parquet-writer: 29575 pending

(Lag = messages in stream not yet acknowledged by a consumer. Zero lag means the consumer is keeping up in real time.)

---

## Test 4 — Parquet Storage (S3/MinIO)

- Files written (last 2h): **72**
- Bytes written (last 2h): **9.6 MB**
- Total parquet files: **590**
- Total parquet storage: **24.1 MB**

**Sample file schema** (`20260317T040407Z.parquet`):

Columns (21): `ac_power`, `capacity`, `cell_id`, `current`, `dc_current`, `dc_voltage`, `efficiency`, `internal_resistance`, `irradiance`, `module_id`, `panel_temp`, `project_id`, `rack_id`, `resistance`, `site_id`, `soc`, `soh`, `source_id`, `temperature`, `timestamp`, `voltage`

Rows in file: **4,256**

**Extra (non-standard) columns found:** `ac_power`, `dc_current`, `dc_voltage`, `efficiency`, `irradiance`, `panel_temp`

These extra columns were automatically preserved without any config change.

---

## Test 5 — Schema Flexibility

Analysis of writer.py behaviour with payload schema changes:

| Scenario | Telegraf behaviour | NATS + Parquet behaviour |
|---|---|---|
| **Extra field in payload** | Silently ignored (not in config) | Automatically stored as new Parquet column |
| **Missing field** | Error → entire message DROPPED | NULL in that column, record preserved |
| **Nested `{value, unit}` payload** | Error → entire message DROPPED | `_flatten()` handles both nested and flat |
| **New source type (solar, wind)** | Not subscribed → zero data | Subject `>` wildcard catches everything |
| **Schema change** | Edit config file + restart required | Zero config, zero restart |
| **Parse error recovery** | Manual restart, no replay | JetStream replay from last consumer position |

**Dynamic schema collection:** ✓ (`all_keys = {k for r in flat for k in r}` — every row's keys included)

**Nested payload handling:** ✓ (flattens `{"value": X}` objects transparently)

**Null-safe missing fields:** ✓ (`r.get(k)` → None → NULL column, record not dropped)

---

## Test 6 — Resilience Properties

- JetStream file storage: ✓
- Sync to disk before ACK: ✓
- Max file store: 50GB
- Retention window: **?h** — if parquet writer is down for ≤?h, zero data loss on restart

**Recovery scenario (vs Telegraf):**

| Event | Telegraf | NATS + Parquet |
|---|---|---|
| Writer/consumer down 1h | Data lost (MQTT publish drops) | Replayed from JetStream on restart |
| Payload format changes | Silent data loss (parse error suppressed) | Raw JSON preserved in stream |
| Add new sensor field | Lost permanently | In JetStream, appears in Parquet after schema update |
| S3 write fails | N/A | Retries with exponential backoff, message stays un-ACK'd |

---

## Test 7 — Process Resources

| Process | RSS Memory |
|---------|----------:|
| `nats-server` | 99 MB |
| `parquet writer` | 209 MB |
| `nats bridge` | 19 MB |

---

## Test 8 — AWS Cost Estimates

Extrapolated from measured throughput (913.6/sec) and storage rate (0.12 GB/day parquet).

### Fleet Scaling — Monthly Cost Breakdown

| Service | 1× (current) | 6× | 24× |
|---------|:---:|:---:|:---:|
| **Messages/sec** | 913.6 | 5,481.6 | 21,926.4 |
| **Parquet GB/month** | 3.5 | 20.8 | 83.2 |
| S3 storage | $0.08 | $0.48 | $1.91 |
| S3 PUT requests | $0.13 | $0.78 | $3.11 |
| S3 GET requests | $0.00 | $0.00 | $0.01 |
| EC2 NATS server | $15.18 (t3.small) | $15.18 (t3.small) | $30.37 (t3.medium) |
| EC2 Parquet writer | $7.59 (t3.micro) | $15.18 (t3.small) | $15.18 (t3.small) |
| **Total / month** | **$22.98** | **$31.62** | **$50.58** |

### Alternative: AWS IoT Core for MQTT Ingest

| Scale | IoT Core cost/month | Notes |
|-------|:---:|---|
| 1× | $2368.05 | Replaces FlashMQ + NATS bridge + EC2 for MQTT layer |
| 6× | $14208.31 | Replaces FlashMQ + NATS bridge + EC2 for MQTT layer |
| 24× | $56833.23 | Replaces FlashMQ + NATS bridge + EC2 for MQTT layer |

### Notes

- EC2 = on-demand us-east-1 Linux. Reserved 1-year saves ~40%.
- S3 Standard. Intelligent-Tiering would reduce cost for cold partitions.
- IoT Core alternative shown for MQTT ingest only — replaces FlashMQ + NATS bridge EC2.
- Data transfer out not included — queries assumed within same region via Athena.
- NATS server handles >> current load; no vertical scaling needed before 6x.

---

## Summary

| Finding | Value |
|---------|-------|
| Message throughput | 913.6/sec |
| Stream storage | 8.59 GB |
| Consumer lag | 4zAEfCoF: 0 pending, BR0VHyrE: 0 pending, parquet-writer: 29575 pending |
| Parquet files (last 2h) | 72 |
| Parquet storage (total) | 24.1 MB |
| Retention window | ?h |
| Schema: extra fields stored | ✓ yes |
| Schema: missing fields → NULL | ✓ yes (record preserved) |
| Schema: nested payload handled | ✓ yes |
| nats-server RSS | 99 MB |
| Est. AWS cost (1× scale) | $22.98/month |
