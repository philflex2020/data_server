# Study 3 — AWS Monitoring & Performance

**HTML version:** [`html/study_aws_monitoring.html`](../html/study_aws_monitoring.html)

---

## Overview and Deliverable

This study covers how the current systems pull data from AWS, and how to verify and
benchmark that data once it is flowing. It addresses four questions:

1. **Who pulls what?** — identify every monitor, dashboard, and query client and the API it uses.
2. **Is the stack healthy?** — infrastructure and data-flow health checks.
3. **How fast is recall?** — latency specifications for real-time through 30-day queries.
4. **Is the data complete?** — checking that every expected cell has written data recently.

**Your deliverables:**

1. Monitor audit: list every tool currently querying AWS data, the endpoint it uses, and the
   query pattern. Note which will break if the architecture changes.
2. Run the `/diag` endpoint on a live stack and document actual values for each field.
3. Execute recall benchmarks for the five query windows in the spec table below.
4. Write and test the three completeness check queries against a running dataset.
5. Propose alert thresholds for the completeness checks (acceptable staleness, minimum
   cell count, maximum write interval).

---

## Diagnostic Endpoint

All three monitoring dimensions are accessible from one HTTP call:

```
GET http://HOST:8768/diag
```

### Response structure

```json
{
  "nats": {
    "connected": true,
    "stream": "BATTERY_DATA",
    "subjects": ["batteries.>"],
    "messages": 41472000,
    "bytes": 11878825984,
    "consumer_name": "parquet-writer",
    "pending": 0,
    "ack_pending": 0
  },
  "s3": {
    "connected": true,
    "bucket": "battery-data",
    "file_count": 4153,
    "total_bytes": 2197815296
  },
  "subscriber": {
    "nats_connected": true,
    "subscriptions": 2,
    "messages_received": 41472000,
    "messages_forwarded": 41468300
  }
}
```

### Field definitions

| Field | What it tells you |
|-------|------------------|
| `nats.messages` | Total messages stored in the JetStream stream |
| `nats.pending` | Messages not yet consumed by the Parquet writer |
| `nats.ack_pending` | Messages consumed but not yet ACKed (write in progress) |
| `s3.file_count` | Number of Parquet files in the bucket |
| `s3.total_bytes` | Total compressed size on disk |
| `subscriber.messages_received` | Messages received from NATS by the Subscriber API |
| `subscriber.messages_forwarded` | Messages forwarded to WebSocket subscribers |

### Health thresholds (proposed)

| Field | Green | Amber | Red |
|-------|-------|-------|-----|
| `nats.connected` | true | — | false |
| `nats.pending` | < 5,000 | 5,000 – 50,000 | > 50,000 |
| `nats.ack_pending` | < 500 | 500 – 5,000 | > 5,000 |
| `s3.connected` | true | — | false |

---

## Monitoring Categories

### 1. Infrastructure Health

Checks that the core services are running and reachable.

| Check | How | Pass condition |
|-------|-----|---------------|
| NATS JetStream reachable | `GET :8222/healthz` → `{"status":"ok"}` | HTTP 200 |
| Parquet writer connected | `nats.ack_pending < 500` in `/diag` | Pending stays low |
| S3 / MinIO reachable | `s3.connected == true` in `/diag` | No connection errors |
| Subscriber API up | `GET :8768/diag` returns 200 | Endpoint responds |

### 2. Data Flow Health

Checks that messages are moving through the pipeline without stalling.

| Check | How | Pass condition |
|-------|-----|---------------|
| NATS ingest rate | `GET :8222/varz` → `in_msgs_rate` | > 0 msgs/s when generating |
| Parquet writer consuming | `nats.pending` stable or decreasing | Doesn't grow unbounded |
| S3 file count growing | Poll `/diag` → `s3.file_count` over time | Increases each flush (~60s) |
| Bridge not stalled | `GET :8222/connz` → connection from bridge IP | Bridge connection present |

### 3. Data Completeness

Checks that every expected cell is represented in recent data.

See the three DuckDB queries in the [Completeness Checks](#completeness-checks) section.

---

## Recall Time Specifications

### Real-time data (< 1 second latency)

Source: NATS JetStream push consumer → Subscriber API WebSocket.

```json
{"type": "subscribe", "subject": "batteries.>"}
```

Data arrives as `"type": "live"` events within 100ms of publication. No query is needed —
messages are pushed as they arrive.

### Retained data (S3 Parquet via DuckDB)

The Parquet writer flushes every 60 seconds or 10,000 messages. Data older than this
window is queryable via `query_history`.

```json
{
  "type":     "query_history",
  "query_id": "bench-5min",
  "proj_id":  "0",
  "site_id":  "0",
  "from_ts":  1741234267.0,
  "to_ts":    1741234567.0,
  "limit":    5000
}
```

### Benchmark specification

At **1,152 msgs/s** (spark-22b6 stress runner, 3 sites):

| Query window | Expected rows | Expected files | Target response |
|-------------|--------------|----------------|-----------------|
| Last 30s | ~34,560 | ~3–4 | < 500 ms |
| Last 5 min | ~345,600 | ~35 | < 2,000 ms |
| Last 1 hr | ~4,147,200 | ~415 | < 10,000 ms |
| Last 24 hr | ~99,532,800 | ~9,953 | < 120,000 ms |
| Last 30 days | ~2,985,984,000 | ~298,598 | benchmark required |

At **144 msgs/s** (phil-dev generator, 1 site):

| Query window | Expected rows | Expected files | Target response |
|-------------|--------------|----------------|-----------------|
| Last 5 min | ~43,200 | ~5 | < 500 ms |
| Last 1 hr | ~518,400 | ~52 | < 3,000 ms |
| Last 24 hr | ~12,441,600 | ~1,244 | < 30,000 ms |

Parquet files per hour per site: `ceil(rate_per_s × 3600 / 10000)`.

### The 60-second gap

Data in the last `flush_interval_seconds` (60s, or ~9s at 1,152 msgs/s) is only in NATS
JetStream, not S3. `query_history` will return zero rows for this window. Use the live
subscribe path for sub-60s recall.

| Scenario | Correct path |
|---------|-------------|
| Need data from last 9s (full stress load) | Live subscribe `:8767` |
| Need data from last 60s (144 msgs/s) | Live subscribe `:8767` |
| Need data from 2 minutes ago | `query_history` `:8767` |

---

## Completeness Checks

These DuckDB SQL queries run against the S3 Parquet store. Run them from the AWS host
where DuckDB can reach the MinIO endpoint (or via the Subscriber API `/diag` tooling).

### Check 1 — Cell count

Verifies the correct number of distinct cells are present in recent data.

```sql
-- Expected: 1152 cells for spark-22b6 (1 project × 3 sites × 4r × 8m × 12c)
-- Expected: 144 cells for phil-dev (1 project × 1 site × 2r × 3m × 4c × 6 values)
SELECT
  COUNT(DISTINCT CONCAT(project_id,'.',site_id,'.',rack_id,'.',module_id,'.',cell_id))
    AS distinct_cells,
  MIN(timestamp) AS oldest_ts,
  MAX(timestamp) AS newest_ts,
  NOW() - epoch_ms(CAST(MAX(timestamp)*1000 AS BIGINT)) AS data_age
FROM read_parquet(
  's3://battery-data/project=*/*/*/*/*/*.parquet',
  hive_partitioning=true
)
WHERE timestamp > epoch(NOW()) - 300;  -- last 5 minutes
```

**Pass:** `distinct_cells` equals expected count. `data_age` < 120 seconds.

### Check 2 — Per-cell last-seen age

Finds any cells that have not written data recently (stale or missing).

```sql
SELECT
  project_id, site_id, rack_id, module_id, cell_id,
  MAX(timestamp) AS last_seen,
  NOW() - epoch_ms(CAST(MAX(timestamp)*1000 AS BIGINT)) AS age
FROM read_parquet(
  's3://battery-data/project=*/*/*/*/*/*.parquet',
  hive_partitioning=true
)
WHERE timestamp > epoch(NOW()) - 3600   -- look back 1 hour
GROUP BY 1,2,3,4,5
HAVING age > INTERVAL '120 seconds'     -- alert threshold: 2 minutes stale
ORDER BY age DESC;
```

**Pass:** zero rows returned. Each row is a cell that needs investigation.

### Check 3 — Write interval health

Measures actual flush intervals. The Parquet writer should flush every 60s or 10,000
messages. Significantly longer intervals indicate backpressure or writer stalls.

```sql
WITH file_times AS (
  SELECT
    project_id,
    site_id,
    MIN(timestamp) AS file_start,
    MAX(timestamp) AS file_end,
    COUNT(*) AS row_count
  FROM read_parquet(
    's3://battery-data/project=*/*/*/*/*/*.parquet',
    hive_partitioning=true,
    filename=true
  )
  WHERE timestamp > epoch(NOW()) - 3600
  GROUP BY project_id, site_id, filename
)
SELECT
  project_id,
  site_id,
  COUNT(*) AS file_count,
  AVG(file_end - file_start) AS avg_span_s,
  MAX(file_end - file_start) AS max_span_s,
  AVG(row_count) AS avg_rows_per_file
FROM file_times
GROUP BY 1, 2
ORDER BY 1, 2;
```

**Pass:** `avg_span_s` < 70 seconds (60s + tolerance). `max_span_s` < 180 seconds.
`avg_rows_per_file` close to `min(rate × 60, 10000)`.

---

## Fleet Monitoring Scale

### Single-site deployment

Each site runs its own independent stack:

| Component | Count | Notes |
|-----------|-------|-------|
| NATS server | 1 per site | JetStream, `:4222` / `:8222` |
| Parquet writer | 1 per site | Writes to site-partitioned S3 path |
| Subscriber API | 1 per site | WS `:8767`, HTTP `:8768` |
| MQTT broker | 1 per site | FlashMQ (spark) / Mosquitto (dev) |
| NATS bridge | 1 per site | MQTT→NATS relay |

### Multi-site / fleet monitoring

For a fleet of N sites, one central monitor host reads across all sites:

| Component | Count | Notes |
|-----------|-------|-------|
| Fleet monitor process | 1 central | Polls `/diag` on each site |
| DuckDB completeness checks | 1 central | Cross-partition S3 glob |
| Alert dispatcher | 1 central | Sends alerts based on thresholds |

### Resource budget (per site, at 1,152 msgs/s)

| Resource | Value |
|---------|-------|
| NATS memory | < 512 MB (configured cap) |
| NATS file store | ~285 KB/s → ~1 GB/hr |
| Parquet writer memory | < 200 MB buffer |
| S3 ingest | ~20–40 MB/hr compressed (snappy) |
| Subscriber API memory | < 100 MB |

---

## How to Run Benchmarks

### Prerequisites

- Stack running on spark-22b6 with stress runner active
- At least 15 minutes of data in S3 before timing multi-file queries
- WebSocket client (browser console, `wscat`, or `html/subscriber.html`)

### Procedure

1. **Confirm stack health:**
   ```
   curl http://spark-22b6:8768/diag | jq .
   ```
   Verify `nats.connected`, `s3.connected`, and `nats.pending < 5000`.

2. **Run recall benchmarks:**
   Connect to `ws://spark-22b6:8767` and send each `query_history` request.
   Record `elapsed_ms` and `total` from the response. Repeat 3×, take median.

3. **Run completeness checks:**
   Connect DuckDB to the MinIO endpoint and execute each SQL query above.
   Record row counts and timing.

4. **Document results** in the format:

   | Test | Expected | Actual | Pass? |
   |------|----------|--------|-------|
   | Cell count (5 min) | 1,152 | ? | ? |
   | Stale cells (>120s) | 0 | ? | ? |
   | Avg flush span | < 70s | ? | ? |
   | 5-min query latency | < 2,000 ms | ? | ? |
   | 1-hr query latency | < 10,000 ms | ? | ? |

---

## Open Questions and Research Tasks

1. **Run the benchmark table on a live stack** and fill in actual `elapsed_ms` values.
   Determine whether the targets above are achievable or need adjustment.

2. **Automate the completeness checks.** The SQL queries above are written for manual
   execution. Decide whether they should run as a cron job, a Prometheus exporter, or
   an alert integrated into the Subscriber API `/diag` response.

3. **Set concrete alert thresholds.** The proposed thresholds (stale > 120s, pending >
   50,000, avg span > 70s) are starting points. Validate them against actual data before
   treating them as production SLOs.

4. **Test at the NATS 8 GB cap.** At 1,152 msgs/s the cap is reached in ~7.8 hours.
   Verify that data already flushed to S3 remains queryable after NATS purges it. This
   confirms the two-tier independence guarantee.

5. **Measure cross-site query cost.** Run the cell-count completeness check against one
   site vs all three sites. If `elapsed_ms` scales linearly, partition pruning is working.
   If not, investigate the DuckDB glob pattern and MinIO ListObjects cost.

6. **Design the fleet monitor.** For N sites, specify the polling interval, the payload
   format for the central monitor's HTTP calls, and the latency budget for a full fleet
   health sweep to complete.

7. **Define retention policy.** No lifecycle rule is currently set on the S3 bucket.
   Decide the retention period per asset type and estimate storage cost at full fleet
   scale (see `aws_billing_context.md` for current cost baseline).
