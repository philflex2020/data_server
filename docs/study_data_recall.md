# Study 2 — Data Recall & Query Patterns

**HTML version:** [`html/study_data_recall.html`](../html/study_data_recall.html)

---

## Overview and Deliverable

The pipeline stores data in two places: NATS JetStream (short-term stream) and MinIO/S3 as
Parquet files (long-term). The Subscriber API exposes both via a single WebSocket interface
on port 8767 and a Flux-compatible HTTP interface on port 8768.

**Your deliverable:**

1. Define the three recall tiers precisely: latency, storage layer, API call, and retention window.
2. Benchmark query response times for the long-term path (DuckDB over S3) across windows of 30s, 5min, 1hr, and 24hr.
3. Document the 60-second gap and specify which API path applies for sub-60s recall.
4. Confirm partition pruning is functioning correctly (queries scan only relevant files).
5. Extend the recall spec to cover Solar, Wind, and Grid once Study 1 is complete.

---

## Three-Tier Recall Architecture

### Tier 1 — Real-time (latency: < 1 second)

Data flows: Generator → FlashMQ (MQTT :1883) → NATS Bridge → NATS JetStream :4222 →
Subscriber API → WebSocket client.

The Subscriber API maintains a NATS push consumer on the JetStream stream. When a client
subscribes, new messages are forwarded immediately as `live` events. End-to-end latency
from publish to browser is under 100ms under normal load.

**API call (WebSocket :8767):**
```json
{"type": "subscribe", "subject": "batteries.>"}
```

**Response events:**
```json
{
  "type": "live",
  "subject": "batteries.site=0.rack=1.module=2.cell=3",
  "payload": {
    "timestamp": 1741234567.891,
    "site_id": 0, "rack_id": 1, "module_id": 2, "cell_id": 3,
    "voltage": 3.714, "current": -0.42, "temperature": 28.3,
    "soc": 71.2, "soh": 97.8, "resistance": 0.0118, "capacity": 99.4,
    "power": -1.560
  }
}
```

**Use cases:** live operator displays, real-time alerting, streaming dashboards.

---

### Tier 2 — Medium-term (window: 1 second – 48 hours)

Data is retained in NATS JetStream for 48 hours (rolling) up to 8 GB. This tier covers
replay of recent history that has not yet been flushed to S3, and overlaps with Tier 3
for data older than 60 seconds.

**Critical:** the Subscriber API `query_history` call reads from S3 Parquet only. For data
in the last ~60 seconds (or ~9 seconds at 1,152 msgs/s), NATS is the only source. There is
no `query_history` equivalent that reads directly from the NATS stream through the current
API — use the live subscribe path or implement a direct NATS replay consumer.

**Retention configuration (NATS bridge config):**
- `max_age`: 48 hours
- `max_bytes`: 8 GB
- At limit: oldest messages are purged first

**At 1,152 msgs/s** (~285 KB/s ingest): 8 GB ÷ 285 KB/s ≈ 28,070 seconds ≈ **7.8 hours**
before the size cap is hit. The 48-hour time window is not the binding constraint at this rate.

**At 144 msgs/s** (~35 KB/s ingest): 8 GB ÷ 35 KB/s ≈ 228,571 seconds ≈ 63 hours before
size cap. The 48-hour time window is the binding constraint.

---

### Tier 3 — Long-term (window: > ~60 seconds)

The Parquet writer buffers messages and flushes to MinIO/S3 every 60 seconds or every
10,000 messages, whichever comes first. Once flushed, data is queryable via DuckDB through
the `query_history` WebSocket command.

**S3 partition layout:**
```
s3://battery-data/project={p}/site={s}/{year}/{month}/{day}/{YYYYMMDDTHHMMSSz}.parquet
```

Compression: Snappy. Files are not split further below the daily directory.

**API call (WebSocket :8767):**
```json
{
  "type":     "query_history",
  "query_id": "q1",
  "proj_id":  "0",
  "site_id":  "0",
  "from_ts":  1741230967.0,
  "to_ts":    1741234567.0,
  "limit":    1000
}
```

**Response:**
```json
{
  "type":       "history",
  "query_id":   "q1",
  "columns":    ["timestamp","site_id","rack_id","module_id","cell_id",
                 "voltage","current","temperature","soc","soh",
                 "resistance","capacity","power","project_id"],
  "rows":       [[1741230967.0, 0, 1, 2, 3, 3.714, -0.42, 28.3, 71.2, 97.8, 0.0118, 99.4, -1.560, 0],
                 ...],
  "total":      345600,
  "elapsed_ms": 1840
}
```

**Retention:** indefinite — no lifecycle policy is configured. Data accumulates until a
policy is added or the bucket is manually cleared.

**Use cases:** historical trend analysis, anomaly investigation, regulatory reporting.

---

## The 60-Second Gap

### What it is

The Parquet writer holds an in-memory buffer of unpublished messages. Every 60 seconds (or
when the buffer reaches 10,000 messages) it flushes to S3. Until a flush occurs, the most
recent data is not queryable via `query_history`.

This creates a gap: data from the last `N` seconds may be visible on the live subscribe
path but will return zero rows from `query_history`.

### Effective gap duration

At **1,152 msgs/s** the 10,000-message cap fires in:
```
10,000 messages ÷ 1,152 msgs/s ≈ 8.7 seconds
```
The effective gap is therefore approximately **9–10 seconds**.

At **144 msgs/s** the time-based flush fires first:
```
10,000 messages ÷ 144 msgs/s ≈ 69 seconds → 60-second limit fires first
```
The effective gap is the full **60 seconds**.

### Implications

| Scenario | Action |
|---------|--------|
| Need data from last 10 seconds (at full load) | Use live subscribe |
| Need data from last 60 seconds (at 144 msgs/s) | Use live subscribe |
| Need data from 2 minutes ago | Use query_history (data is in S3) |
| Building a dashboard showing last 5 min | Combine live subscribe + initial query_history |

### Research tasks

- Run a query at T+5s, T+15s, T+30s, T+70s after stack start. Record first query that returns data.
- Verify the flush trigger (time vs message count) by monitoring MinIO for new file creation.
- Consider whether the gap is acceptable for operational use cases, or whether a NATS replay
  path should be added to `query_history`.

---

## Data Retention Specifications

| Tier | Storage layer | Retention window | Max size | At limit |
|------|--------------|------------------|----------|----------|
| 1 — Real-time | NATS JetStream (push delivery) | No window — push only | — | Messages missed if consumer disconnected |
| 2 — Medium-term | NATS JetStream (stream replay) | 48 hours rolling | 8 GB | Oldest messages purged first |
| 3 — Long-term | MinIO / S3 Parquet | Indefinite | Unconfigured | No automatic deletion |

---

## Query Response Benchmarks

### What to measure

For each query window, record:
- `elapsed_ms` from the API response
- Row count (`total` field)
- Number of Parquet files scanned (via DuckDB `EXPLAIN` or MinIO file count)
- Whether partition pruning is effective (single site vs all sites)

### Expected results (at 1,152 msgs/s)

| Query window | Expected rows | Parquet files | Expected elapsed_ms | Notes |
|-------------|--------------|---------------|---------------------|-------|
| Last 30s | ~34,560 | ~3–4 | < 500 | May return 0 — 60s gap |
| Last 5min | ~345,600 | ~35 | < 2,000 | Single day partition |
| Last 1hr | ~4,147,200 | ~415 | 2,000–10,000 | Benchmark DuckDB thread count |
| Last 24hr | ~99,532,800 | ~9,953 | 30,000–120,000 | Crosses midnight |
| 1hr, all 3 sites | ~12,441,600 | ~1,244 | 5,000–30,000 | Three site partitions |

Parquet files per hour calculation: `ceil(1152 × 3600 / 10000) = 415 files/hr/site`.

### How to measure

1. Connect to WS :8767 on spark-22b6 (stress runner running).
2. Send `query_history` with the target window.
3. Record `elapsed_ms` and `total` from the response.
4. Repeat three times; take the median.
5. Compare single-site vs cross-site queries to confirm partition pruning.

---

## Sample API Interactions

### Full request/response: subscribe to live stream

**Request (send after WebSocket `onopen`):**
```json
{"type": "subscribe", "subject": "batteries.>"}
```

**Response events (one per published message):**
```json
{"type": "live", "subject": "batteries.site=0.rack=1.module=2.cell=3",
 "payload": {"timestamp": 1741234567.891, "site_id": 0, "rack_id": 1,
             "module_id": 2, "cell_id": 3, "voltage": 3.714,
             "current": -0.42, "temperature": 28.3, "soc": 71.2,
             "soh": 97.8, "resistance": 0.0118, "capacity": 99.4,
             "power": -1.560}}
```

---

### Full request/response: query last 5 minutes

**Request:**
```json
{
  "type":     "query_history",
  "query_id": "q5min",
  "proj_id":  "0",
  "site_id":  "0",
  "from_ts":  1741234267.0,
  "to_ts":    1741234567.0,
  "limit":    1000
}
```

**Response:**
```json
{
  "type":       "history",
  "query_id":   "q5min",
  "columns":    ["timestamp","site_id","rack_id","module_id","cell_id",
                 "voltage","current","temperature","soc","soh",
                 "resistance","capacity","power","project_id"],
  "rows":       [
    [1741234267.0, 0, 0, 0, 0, 3.714, -0.42, 28.3, 71.2, 97.8, 0.0118, 99.4, -1.560, 0],
    [1741234267.0, 0, 0, 0, 1, 3.721, -0.38, 27.9, 70.8, 97.6, 0.0121, 99.1, -1.414, 0],
    "..."
  ],
  "total":      345600,
  "elapsed_ms": 1840
}
```

Note: `limit: 1000` caps the returned rows to 1,000 even though `total` is 345,600. Adjust
`limit` to retrieve more rows, or paginate using `from_ts` offsets.

---

### Full request/response: get status

**Request:**
```json
{"type": "get_status"}
```

**Response:**
```json
{
  "type":           "status",
  "nats_connected": true,
  "s3_connected":   true,
  "subscribed":     true,
  "subject":        "batteries.>"
}
```

---

### HTTP diagnostic endpoint

```
GET http://HOST:8768/diag
```

Returns JSON with NATS, S3, and subscriber stats. Use to confirm the stack is healthy
before running benchmarks.

---

### Flux-compatible HTTP query

```
POST http://HOST:8768/api/v2/query
Content-Type: application/vnd.flux

from(bucket: "battery-data")
  |> range(start: -1h)
  |> filter(fn: (r) => r.site_id == "0")
```

Returns InfluxDB2 annotated CSV. Useful for tools that speak the Flux HTTP API.

---

## Open Questions and Research Tasks

1. **Confirm the effective gap duration** at both deployment configurations (144 msgs/s
   on phil-dev; 1,152 msgs/s on spark-22b6). Document the first query window that reliably
   returns data.

2. **Benchmark DuckDB query times** for each window size listed in the table above.
   Record on spark-22b6 (full stress load) and on phil-dev (lower rate). Note whether
   the difference is proportional to file count.

3. **Verify partition pruning.** Run a 1-hour single-site query vs a cross-site query.
   If `elapsed_ms` scales linearly with site count, pruning is working. If it does not,
   investigate DuckDB glob pattern and S3 ListObjects cost.

4. **Test behaviour at the NATS 8 GB cap.** At full stress load, the cap is reached in
   ~7.8 hours. After the cap is hit, old messages are purged from NATS. Confirm that
   those messages are still queryable from S3, verifying the two tiers are independent.

5. **Evaluate whether a NATS replay path is needed.** The current API has no way to
   query the NATS stream directly — only S3. For the 60-second gap, the only option is
   the live subscribe path. Determine whether this is acceptable for all use cases, or
   whether a `query_recent` command should be added that reads from JetStream.

6. **Extend to new asset types.** After Study 1 implements Solar, Wind, and Grid, confirm
   that `query_history` can retrieve their data. Check whether the API requires `proj_id`
   and `site_id` to be specified, or whether wildcards are supported. Document the S3
   partition path for each new type.

7. **Specify retention requirements.** No lifecycle policy is currently configured on
   MinIO/S3. Determine whether one is needed, what the retention period should be per
   asset type, and what the storage cost impact would be (see aws_billing_context.md).
