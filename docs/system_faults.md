# System Faults & Known Problems

Summary of data loss risks, recall failures, known bugs, and operational gaps in the
current pipeline. Cost issues are excluded — see `docs/aws_billing_context.md`.

---

## Data Loss / Dropping

### 1. NATS stream size cap (active risk)

**Symptom:** Parquet writer falls behind, older NATS messages are purged before being written to S3.

**Detail:** The `BATTERY_DATA` stream is capped at 8 GB with a `LIMITS` retention policy.
At full stress-runner load (1,152 msgs/s, ~285 KB/s), the cap is reached in ~7.8 hours.
If the Parquet writer is stopped, crashed, or slow for longer than 7.8 hours, messages
at the front of the stream are purged. Those messages are permanently lost — not written
to S3, not recoverable from NATS.

**Affected path:** NATS → Parquet writer → S3

**Risk level:** High at full load. Low at 144 msgs/s (48-hour time window is the binding
constraint, size cap takes ~63 hours to fill).

**Mitigation options:**
- Increase `max_bytes` in the stream config (costs more disk on the NATS host)
- Add an alerting rule: if `nats.pending > 10,000` for more than 60 seconds, alert
- Monitor `nats.ack_pending` via `/diag` — if growing, writer is stalled

---

### 2. 60-second gap in historical recall (design limitation)

**Symptom:** `query_history` returns zero rows for data written in the last 60 seconds,
even though the data is clearly in the live stream.

**Detail:** The Parquet writer buffers messages and flushes to S3 every 60 seconds or
10,000 messages. Until a flush occurs, the data is only in NATS JetStream. The
`query_history` API reads S3 only. There is no API path to query the NATS stream
directly for historical data.

At 1,152 msgs/s the 10,000-message cap fires in ~9 seconds, so the gap is 9–10 seconds.
At 144 msgs/s the gap is the full 60 seconds.

**Affected path:** Subscriber API `query_history` command

**Risk level:** Medium — any dashboard or tool that queries recent history will silently
miss the most recent data window.

**Mitigation:**
- Combine live subscribe + `query_history` in the dashboard (current `monitor.html` approach)
- Add a `query_recent` command that reads directly from NATS JetStream for the last N seconds

---

### 3. Telegraf buffer overflow — InfluxDB path (active risk at high rate)

**Symptom:** Telegraf drops metrics silently when InfluxDB2 cannot keep up with ingest.

**Detail:** Telegraf's `metric_buffer_limit = 100000`. At 1,152 msgs/s, the buffer fills
in ~87 seconds if InfluxDB2 is slow or unreachable. Once full, new metrics are dropped
with no client-visible error. The `metrics_dropped` internal metric (InfluxDB2 measurement
`internal_agent`) records the count.

**Affected path:** FlashMQ → Telegraf → InfluxDB2

**Risk level:** High at 1,152 msgs/s if InfluxDB2 is undersized or temporarily unavailable.

**Mitigation:**
- Monitor `metrics_dropped` via Flux query
- Use a larger instance for InfluxDB2 at high rates
- The NATS path does not have this problem — durable JetStream ensures no drops

---

### 4. FlashMQ no-persistence setting (design choice)

**Symptom:** If FlashMQ restarts while the NATS bridge or Telegraf is disconnected,
queued MQTT messages are lost.

**Detail:** FlashMQ runs without persistence (`max_qos_msg_pending_per_client 1000` is
the in-memory queue only). Messages published during a subscriber disconnect are held in
memory up to 1,000 per client. If FlashMQ itself restarts, all in-memory state is lost.

**Affected paths:** Both InfluxDB and NATS paths.

**Risk level:** Low in normal operation (restarts are rare). High if combined with
a simultaneous broker restart and subscriber crash.

**Mitigation:** NATS JetStream provides the durability guarantee downstream — once a
message reaches NATS, it is persisted to disk regardless of FlashMQ state.

---

## Recall Problems

### 5. NATS bridge crash — wrong MQTT topic (fixed, git: b261119)

**Symptom:** NATS bridge crashed on startup.

**Detail:** The bridge was subscribing to an incorrect MQTT topic. Fixed by reverting the
subscription to `batteries/#`. The fix is in production.

**Status:** Fixed. However, the bridge has no process supervisor beyond the manager's
restart logic. If it crashes due to a new topic mismatch or NATS connection error, the
manager will restart it with a 2-second backoff. Data published during the crash window
is lost (not in NATS, not in S3).

**Monitor:** Watch the process manager dashboard for unexpected bridge restarts.

---

### 6. Hive partitioning schema mismatch (fixed, git: 6dae1ba + 3b5e0c0)

**Symptom:** DuckDB queries failed with schema errors when reading S3.

**Detail:** `hive_partitioning=true` caused DuckDB to fail when the S3 glob matched
Parquet files with different schemas — for example, a top-level `_SUCCESS` marker file
or files from a different project that had different columns.

Fixed by targeting the glob at `project=*` prefix only, excluding non-partition files.
Later restored fully by using `project=*/` to ensure DuckDB only sees hive-partitioned
directories.

**Status:** Fixed. Risk remains if new file types (e.g. JSON metadata files) are added
to the S3 bucket, as they would break the glob again.

---

### 7. Flux compat shim — regex-based parser (fragile)

**Symptom:** Flux queries that use unsupported syntax return empty results or errors
with no clear diagnostic.

**Detail:** `subscriber/api/flux_compat.py` implements a minimal Flux parser using
regex, not a real Flux AST parser. It supports only three query patterns:
- `aggregateWindow` → timeseries groupBy
- `last()` → heatmap (ROW_NUMBER window function)
- Neither → write rate (COUNT)

Any Flux query that uses other functions (`derivative()`, `pivot()`, `join()`, etc.) will
silently fall through to the write-rate path and return the wrong data.

**Affected path:** Subscriber API Flux HTTP endpoint `:8768`

**Risk level:** Medium for users who copy Flux queries from Grafana dashboards.

**Mitigation:** Document the three supported patterns. Add an `unsupported` error
response for unrecognised query types rather than silently returning wrong data.

---

### 8. InfluxDB 30-day rolling delete (data loss by design)

**Symptom:** Historical data older than 30 days is inaccessible from Grafana/InfluxDB.

**Detail:** The InfluxDB2 bucket `battery-data` was created with `--retention 30d`.
Data older than 30 days is automatically compacted and deleted by the TSM engine.
There is no archive or long-term storage in the InfluxDB path.

**Affected path:** InfluxDB2 path only. The NATS/Parquet path has no automatic deletion
(indefinite retention on S3 — but no lifecycle policy is configured either).

**Risk level:** High for any use case requiring trend analysis beyond 30 days (warranty
claims, regulatory reporting, seasonal comparisons).

**Mitigation:**
- Add a Flux downsampling task to write 1-minute averages to a `battery-data-1h` bucket
  with 365-day retention before the 30-day window expires
- Or switch to the NATS/Parquet path for long-term storage

---

## Monitoring Gaps

### 9. No data completeness verification (active gap)

**Symptom:** A cell stops publishing data. No alert fires. The silence is invisible.

**Detail:** Neither the InfluxDB nor NATS path has an automated check that verifies all
expected cells are present in recent data. If a generator restarts with fewer cells, or
a rack goes offline, the pipeline continues writing without error — it just writes fewer
rows.

**Affected paths:** Both.

**Mitigation:** The three DuckDB completeness check queries in `docs/study_aws_monitoring.md`
provide a manual approach. These need to be automated as a cron job or Prometheus exporter.

---

### 10. No FlashMQ HTTP health endpoint

**Symptom:** No way to check FlashMQ health via a simple HTTP call.

**Detail:** FlashMQ does not expose an HTTP API. Monitoring requires subscribing to
`$SYS/#` MQTT topics, checking `systemctl status flashmq`, or inferring health from NATS
message rate (if the rate drops to zero, FlashMQ or the bridge has likely failed).

**Affected paths:** Both.

**Mitigation:** Add a lightweight MQTT subscription to `$SYS/broker/clients/connected`
as a sidecar health check, or use the NATS `in_msgs_rate` from `:8222/varz` as a proxy.

---

### 11. Nested payload not handled by Telegraf path

**Symptom:** Voltage, current, etc. fields appear as `null` in InfluxDB when the generator
publishes nested `{value, unit}` payloads.

**Detail:** The generator's `per_cell` mode optionally publishes nested JSON:
```json
{"voltage": {"value": 3.714, "unit": "V"}, ...}
```
Telegraf's `json_v2` parser in `telegraf.conf` maps `voltage` as a top-level float.
Nested objects are silently ignored — the field is written as null or omitted entirely.

The NATS/Parquet path handles this correctly — `writer.py` explicitly flattens nested
`{value, unit}` objects before writing Parquet.

**Affected path:** InfluxDB path only (if generator is in nested mode).

**Mitigation:** Ensure the generator config uses flat payloads when targeting the InfluxDB
path, or update the `telegraf.conf` `json_v2` block to use `path = "voltage.value"`.

---

## Summary Table

| # | Problem | Path affected | Status | Risk |
|---|---------|---------------|--------|------|
| 1 | NATS stream size cap — data purged if writer stalls > 7.8 hr | NATS | Active | High |
| 2 | 60-second query gap — recent data not in S3 | NATS | By design | Medium |
| 3 | Telegraf buffer overflow at high rate | InfluxDB | Active | High |
| 4 | FlashMQ no persistence on restart | Both | By design | Low |
| 5 | NATS bridge crash — wrong topic | NATS | Fixed | — |
| 6 | Hive partitioning schema mismatch | NATS | Fixed | — |
| 7 | Flux shim regex parser — unsupported queries silently wrong | NATS Flux | Active | Medium |
| 8 | InfluxDB 30-day rolling delete | InfluxDB | By design | High |
| 9 | No completeness verification — silent cell dropout | Both | Active gap | High |
| 10 | No FlashMQ HTTP health endpoint | Both | Active gap | Medium |
| 11 | Nested payload not parsed by Telegraf | InfluxDB | Active | Medium |
