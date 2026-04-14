# Parquet Writer

Consumes battery telemetry from NATS JetStream, buffers messages, and flushes compressed Parquet files to S3 (or MinIO).  Designed for **zero data loss** — messages are acknowledged only after a successful upload.

---

## What it does

1. Creates a **durable pull consumer** named `parquet-writer` on the `BATTERY_DATA` JetStream stream.
2. Fetches messages in batches (up to `fetch_batch` per iteration, default 500).
3. Accumulates messages in an in-memory buffer.
4. When either `flush_interval_seconds` or `max_messages` is reached, converts the buffer to Parquet (Snappy-compressed by default) and uploads to S3.
5. Sends NATS ACKs **only after** the S3 upload succeeds.
6. On shutdown (SIGINT/SIGTERM) performs a final flush before draining.

---

## Zero Data Loss Guarantee

The writer never acknowledges a NATS message until the corresponding data has been durably stored in S3:

```
buffer.append((payload, nats_msg))
...
# --- upload succeeds or raises after 5 retries ---
await upload_with_retry(s3, bucket, key, data)
# --- only reached after successful upload ---
for msg in msgs_to_ack:
    await msg.ack()
```

If the writer crashes, loses network, or S3 is unreachable, all buffered messages remain **unacknowledged** in NATS.  On restart the durable consumer resumes from the last acknowledged position — no gap in S3.

S3 uploads are retried up to 5 times with exponential back-off (2, 4, 8, 16, 32 seconds).  If all retries fail, the flush is aborted and retried on the next interval — messages remain in the buffer.

The only scenario that can lose data is if the NATS stream's 48-hour retention window expires before the writer recovers from an extended outage.

---

## Quick Start

```bash
cd source/parquet_writer
pip install nats-py pyarrow boto3 pyyaml
python writer.py
# or
python writer.py --config config.yaml
```

Requires:
- NATS server running with JetStream enabled.
- `BATTERY_DATA` stream to exist (created by `nats_bridge` on its first start).
- S3/MinIO accessible at `endpoint_url`.

The writer retries all connections on startup — it can be started before NATS or MinIO are ready.

---

## Config Reference

File: `source/parquet_writer/config.yaml`

```yaml
nats:
  url: "nats://localhost:4222"
  stream_name: BATTERY_DATA
  consumer_name: parquet-writer    # durable — survives restarts
  subject_filter: "batteries.>"

buffer:
  flush_interval_seconds: 60       # flush after this many seconds
  max_messages: 10000              # or after this many messages
  fetch_batch: 500                 # messages to pull per NATS fetch call

s3:
  endpoint_url: "http://spark-22b6:9000"  # omit for real AWS S3
  bucket: battery-data
  region: us-east-1
  access_key: minioadmin
  secret_key: minioadmin
  prefix: ""                       # optional key prefix within the bucket

parquet:
  compression: snappy              # snappy | zstd | gzip
```

| Key | Type | Default | Notes |
|-----|------|---------|-------|
| `nats.consumer_name` | string | `parquet-writer` | Durable consumer name; changing this creates a new consumer from the beginning |
| `nats.subject_filter` | string | `batteries.>` | NATS subject filter for the consumer |
| `buffer.flush_interval_seconds` | int | `60` | Maximum seconds between flushes |
| `buffer.max_messages` | int | `10000` | Flush early if buffer reaches this size |
| `buffer.fetch_batch` | int | `500` | Messages fetched per NATS pull call |
| `s3.endpoint_url` | string | — | MinIO URL; omit for AWS S3 |
| `s3.bucket` | string | `battery-data` | Target bucket (created if missing) |
| `parquet.compression` | string | `snappy` | `snappy`, `zstd`, or `gzip` |

---

## S3 Partition Layout

Files are written with Hive-compatible partition keys so that DuckDB (used by the Subscriber API) can prune partitions efficiently:

```
s3://battery-data/
  site=0/
    date=2026-03-14/
      hour=13/
        20260314T130009Z.parquet
        20260314T133335Z.parquet
      hour=14/
        20260314T140059Z.parquet
  site=1/
    date=2026-03-14/
      hour=13/
        ...
```

Each flush produces **one file per unique `site_id`** in the buffer at flush time.  The filename is the UTC timestamp of the flush (`YYYYMMDDTHHMMSSz.parquet`).

**Parquet schema:**

| Column | Type |
|--------|------|
| `timestamp` | float64 (Unix epoch seconds) |
| `site_id` | numeric |
| `rack_id` | numeric |
| `module_id` | numeric |
| `cell_id` | numeric |
| `voltage` | float64 |
| `current` | float64 |
| `temperature` | float64 |
| `soc` | float64 |
| `internal_resistance` | float64 |

Note: The generator emits measurements as `{"value": 3.71, "unit": "V"}` objects.  The writer **flattens** these to plain floats before writing Parquet.
