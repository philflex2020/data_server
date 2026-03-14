"""
Parquet Writer — consumes battery data from NATS JetStream (durable consumer),
buffers messages, and flushes partitioned Parquet files to S3.

Zero data loss guarantee:
  Messages are acknowledged to NATS ONLY after a successful S3 upload.
  If the writer restarts, NATS replays all unacknowledged messages from
  the last committed position — no gaps in S3.

Partition layout:
  s3://{bucket}/{prefix}site={site_id}/date={YYYY-MM-DD}/hour={HH}/{ts}.parquet

Usage:
  pip install nats-py pyarrow boto3 pyyaml
  python writer.py
  python writer.py --config config.yaml
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

import boto3
import nats
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from botocore.exceptions import ClientError
from nats.js.api import ConsumerConfig, DeliverPolicy

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def build_s3_client(cfg: dict):
    s3_cfg = cfg["s3"]
    kwargs = dict(
        region_name           = s3_cfg.get("region", "us-east-1"),
        aws_access_key_id     = s3_cfg.get("access_key"),
        aws_secret_access_key = s3_cfg.get("secret_key"),
    )
    if s3_cfg.get("endpoint_url"):
        kwargs["endpoint_url"] = s3_cfg["endpoint_url"]
    return boto3.client("s3", **kwargs)


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
            log.info("Created bucket: %s", bucket)
        else:
            raise


def _flatten(row: dict) -> dict:
    """Flatten generator's {value, unit} measurement objects to plain floats."""
    out = {}
    for k, v in row.items():
        if isinstance(v, dict) and "value" in v:
            out[k] = v["value"]
        else:
            out[k] = v
    return out


def rows_to_parquet(rows: list[dict], compression: str) -> bytes:
    flat = [_flatten(r) for r in rows]
    all_keys = list({k for r in flat for k in r})
    cols = {k: [r.get(k) for r in flat] for k in all_keys}
    buf = BytesIO()
    pq.write_table(pa.table(cols), buf, compression=compression)
    return buf.getvalue()


_DEFAULT_PARTITIONS = [
    "project={project_id}",
    "site={site_id}",
    "{year}",
    "{month}",
    "{day}",
]

def s3_key(cfg: dict, site_id, now: datetime) -> str:
    """Build an S3 key from the partitions template in cfg["s3"]["partitions"]."""
    s3_cfg = cfg["s3"]
    partitions = s3_cfg.get("partitions", _DEFAULT_PARTITIONS)
    prefix     = s3_cfg.get("prefix", "").strip("/")
    vals = {
        "project_id": s3_cfg.get("project_id", 0),
        "site_id":    site_id,
        "year":       now.strftime("%Y"),
        "month":      now.strftime("%m"),
        "day":        now.strftime("%d"),
        "hour":       now.strftime("%H"),
    }
    parts = [p for p in [prefix] + [p.format(**vals) for p in partitions]
             + [now.strftime("%Y%m%dT%H%M%SZ") + ".parquet"] if p]
    return "/".join(parts)


async def upload_with_retry(s3, bucket: str, key: str, data: bytes, retries: int = 5) -> None:
    loop = asyncio.get_event_loop()
    for attempt in range(1, retries + 1):
        try:
            await loop.run_in_executor(None, lambda: s3.put_object(Bucket=bucket, Key=key, Body=data))
            return
        except Exception as exc:
            wait = 2 ** attempt
            log.warning("S3 upload attempt %d/%d failed: %s — retrying in %ds", attempt, retries, exc, wait)
            await asyncio.sleep(wait)
    raise RuntimeError(f"S3 upload failed after {retries} attempts: {key}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(cfg: dict) -> None:
    nats_url       = cfg["nats"]["url"]
    stream_name    = cfg["nats"]["stream_name"]
    consumer_name  = cfg["nats"].get("consumer_name", "parquet-writer")
    subject_filter = cfg["nats"].get("subject_filter", "batteries.>")
    flush_interval = cfg["buffer"]["flush_interval_seconds"]
    max_msgs       = cfg["buffer"]["max_messages"]
    fetch_batch    = cfg["buffer"].get("fetch_batch", 500)
    compression    = cfg["parquet"].get("compression", "snappy")
    bucket         = cfg["s3"]["bucket"]

    s3 = build_s3_client(cfg)

    # Retry until MinIO/S3 is reachable (may start after us)
    while True:
        try:
            await asyncio.get_event_loop().run_in_executor(None, ensure_bucket, s3, bucket)
            break
        except Exception as exc:
            log.warning("S3 not ready yet (%s) — retrying in 5s ...", exc.__class__.__name__)
            await asyncio.sleep(5)

    # Retry NATS connect until server is up
    while True:
        try:
            nc = await nats.connect(nats_url)
            break
        except Exception as exc:
            log.warning("NATS not ready yet (%s) — retrying in 5s ...", exc.__class__.__name__)
            await asyncio.sleep(5)

    js = nc.jetstream()
    log.info("Connected to NATS at %s", nats_url)

    # Retry until the stream exists (created by nats_bridge on its startup)
    while True:
        try:
            sub = await js.pull_subscribe(
                subject_filter,
                durable = consumer_name,
                stream  = stream_name,
                config  = ConsumerConfig(
                    deliver_policy = DeliverPolicy.ALL,
                    ack_wait       = 60,
                    max_deliver    = 10,
                ),
            )
            break
        except Exception as exc:
            log.warning("Stream '%s' not ready yet (%s) — retrying in 5s ...", stream_name, exc.__class__.__name__)
            await asyncio.sleep(5)

    log.info("Durable consumer '%s' on stream '%s' subject '%s'",
             consumer_name, stream_name, subject_filter)
    log.info("Flushing every %ds or every %d messages", flush_interval, max_msgs)

    # Buffer: list of (decoded_dict, nats_msg) tuples
    buffer: list[tuple[dict, object]] = []
    last_flush = time.monotonic()

    async def flush() -> None:
        nonlocal buffer, last_flush
        if not buffer:
            return

        rows_to_write, msgs_to_ack = zip(*buffer)
        buffer = []
        last_flush = time.monotonic()

        # Group by site_id
        by_site: dict[str, list] = defaultdict(list)
        for row in rows_to_write:
            by_site[str(row.get("site_id", "unknown"))].append(row)

        now = datetime.now(timezone.utc)
        for site_id, site_rows in by_site.items():
            key  = s3_key(cfg, site_id, now)
            data = rows_to_parquet(list(site_rows), compression)
            await upload_with_retry(s3, bucket, key, data)
            log.info("Uploaded %d rows → s3://%s/%s (%d bytes)",
                     len(site_rows), bucket, key, len(data))

        # Ack all messages only after successful upload
        for msg in msgs_to_ack:
            await msg.ack()
        log.info("Acknowledged %d messages", len(msgs_to_ack))

    stop = asyncio.Event()

    def shutdown():
        log.info("Shutdown signal received")
        stop.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT,  shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    while not stop.is_set():
        try:
            msgs = await sub.fetch(fetch_batch, timeout=1.0)
            for msg in msgs:
                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    await msg.ack()   # discard unparseable messages
                    continue
                buffer.append((payload, msg))
        except nats.errors.TimeoutError:
            pass  # normal — no messages available right now
        except Exception as exc:
            log.error("Fetch error: %s", exc)
            await asyncio.sleep(2)
            continue

        # Flush if interval elapsed or buffer is full
        elapsed = time.monotonic() - last_flush
        if buffer and (elapsed >= flush_interval or len(buffer) >= max_msgs):
            log.info("Flushing %d messages (%s) ...",
                     len(buffer), "size limit" if len(buffer) >= max_msgs else "interval")
            try:
                await flush()
            except Exception as exc:
                log.error("Flush failed: %s — will retry next interval", exc)

    # Final flush on shutdown
    if buffer:
        log.info("Final flush: %d messages", len(buffer))
        try:
            await flush()
        except Exception as exc:
            log.error("Final flush failed: %s", exc)

    await nc.drain()
    log.info("Shutdown complete")


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NATS JetStream → Parquet → S3 writer")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    asyncio.run(run(load_config(args.config)))
