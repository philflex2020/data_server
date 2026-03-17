# NATS JetStream + Parquet Writer — Live Performance Measurements

**Test date:** 2026-03-17 04:29 UTC
**Environment:** phil-dev (x86_64)
**NATS Server:** 2.12.5
**S3/MinIO reachable:** yes

---

## Test 0 — NATS Server Health

**Result:** Server up and responding.

- Connections: 3
- Server RSS: 136 MB

---

## Test 1 — JetStream Streams

### Stream: `BATTERY_DATA`

- Subjects: `?`
- Messages stored: **21,415,378**
- Bytes stored: **8.59 GB**
- Retention: **0.0h**

| Consumer | Pending (lag) | Ack floor |
|----------|:---:|:---:|
| `4zAEfCoF` | 0 | 30,629,187 |
| `BR0VHyrE` | 0 | 30,629,187 |
| `parquet-writer` | 0 | 0 |

---

## Test 2 — Message Throughput

**Result:** **1,306.0/sec** messages ingested to JetStream.

---

## Test 3 — Consumer Lag

**Result:** 4zAEfCoF: 0 pending, BR0VHyrE: 0 pending, parquet-writer: 0 pending

(Lag = messages in stream not yet acknowledged by a consumer. Zero lag means the consumer is keeping up in real time.)

---

## Test 4 — Parquet Storage (S3/MinIO)

- Files written (last 2h): **151**
- Bytes written (last 2h): **22.3 MB**
- Total parquet files: **669**
- Total parquet storage: **36.7 MB**

**Sample file schema** (`20260317T043017Z.parquet`):

Columns (20): `ac_power`, `capacity`, `cell_id`, `current`, `dc_current`, `dc_voltage`, `efficiency`, `irradiance`, `module_id`, `panel_temp`, `project_id`, `rack_id`, `resistance`, `site_id`, `soc`, `soh`, `source_id`, `temperature`, `timestamp`, `voltage`

Rows in file: **25,079**

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
| `nats-server` | 166 MB |
| `parquet writer` | 2 MB |
| `nats bridge` | 19 MB |

---

## Test 8 — AWS Cost Estimates

Extrapolated from measured throughput (1,306.0/sec) and storage rate (0.27 GB/day parquet).

### Fleet Scaling — Monthly Cost Breakdown

| Service | 1× (current) | 6× | 24× |
|---------|:---:|:---:|:---:|
| **Messages/sec** | 1,306.0 | 7,836.0 | 31,344.0 |
| **Parquet GB/month** | 8.0 | 48.1 | 192.4 |
| S3 storage | $0.18 | $1.11 | $4.43 |
| S3 PUT requests | $0.27 | $1.63 | $6.52 |
| S3 GET requests | $0.00 | $0.01 | $0.03 |
| EC2 NATS server | $15.18 (t3.small) | $15.18 (t3.small) | $30.37 (t3.medium) |
| EC2 Parquet writer | $7.59 (t3.micro) | $15.18 (t3.small) | $15.18 (t3.small) |
| **Total / month** | **$23.22** | **$33.11** | **$56.53** |

### Alternative: AWS IoT Core for MQTT Ingest

| Scale | IoT Core cost/month | Notes |
|-------|:---:|---|
| 1× | $3385.15 | Replaces FlashMQ + NATS bridge + EC2 for MQTT layer |
| 6× | $20310.91 | Replaces FlashMQ + NATS bridge + EC2 for MQTT layer |
| 24× | $81243.65 | Replaces FlashMQ + NATS bridge + EC2 for MQTT layer |

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
| Message throughput | 1,306.0/sec |
| Stream storage | 8.59 GB |
| Consumer lag | 4zAEfCoF: 0 pending, BR0VHyrE: 0 pending, parquet-writer: 0 pending |
| Parquet files (last 2h) | 151 |
| Parquet storage (total) | 36.7 MB |
| Retention window | ?h |
| Schema: extra fields stored | ✓ yes |
| Schema: missing fields → NULL | ✓ yes (record preserved) |
| Schema: nested payload handled | ✓ yes |
| nats-server RSS | 166 MB |
| Est. AWS cost (1× scale) | $23.22/month |
