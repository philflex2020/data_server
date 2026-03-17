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
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

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


_INT_COLS  = {"project_id", "site_id", "rack_id", "module_id", "cell_id"}
_FLOAT_COLS = {"timestamp", "voltage", "current", "temperature", "soc", "soh",
               "internal_resistance", "resistance", "capacity", "power"}


def _flatten(row: dict) -> dict:
    """Flatten {value, unit} measurement objects and normalise column types."""
    out = {}
    for k, v in row.items():
        if isinstance(v, dict) and "value" in v:
            v = v["value"]
        if k in _INT_COLS:
            try:
                v = int(v)
            except (TypeError, ValueError):
                v = None
        elif k in _FLOAT_COLS:
            try:
                v = float(v)
            except (TypeError, ValueError):
                v = None
        out[k] = v
    # Ensure project_id always present so rows from different generators mix cleanly
    if "project_id" not in out:
        out["project_id"] = 0
    return out


_PA_INT   = pa.int64()
_PA_FLOAT = pa.float64()

def rows_to_parquet(rows: list[dict], compression: str) -> bytes:
    flat = [_flatten(r) for r in rows]
    all_keys = list({k for r in flat for k in r})
    arrays = {}
    for k in all_keys:
        vals = [r.get(k) for r in flat]
        if k in _INT_COLS:
            arrays[k] = pa.array(vals, type=_PA_INT)
        elif k in _FLOAT_COLS:
            arrays[k] = pa.array(vals, type=_PA_FLOAT)
        else:
            arrays[k] = pa.array(vals)
    buf = BytesIO()
    pq.write_table(pa.table(arrays), buf, compression=compression)
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
                    deliver_policy  = DeliverPolicy.ALL,
                    ack_policy      = AckPolicy.ALL,
                    ack_wait        = 60,
                    max_deliver     = 10,
                    max_ack_pending = 100_000,  # allow full flush-batch in flight
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

        # Group by (project_id, site_id) — project_id read from payload if present,
        # falling back to cfg["s3"]["project_id"] so single-project deployments
        # need no changes.
        default_proj = cfg["s3"].get("project_id", 0)
        by_partition: dict[tuple, list] = defaultdict(list)
        for row in rows_to_write:
            proj = row.get("project_id", default_proj)
            site = str(row.get("site_id", "unknown"))
            by_partition[(str(proj), site)].append(row)

        now = datetime.now(timezone.utc)

        async def _upload(proj_id, site_id, part_rows):
            part_cfg = {**cfg, "s3": {**cfg["s3"], "project_id": proj_id}}
            key  = s3_key(part_cfg, site_id, now)
            data = rows_to_parquet(list(part_rows), compression)
            await upload_with_retry(s3, bucket, key, data)
            log.info("Uploaded %d rows → s3://%s/%s (%d bytes)",
                     len(part_rows), bucket, key, len(data))

        # Upload all partitions in parallel, then ack once (AckPolicy.ALL)
        await asyncio.gather(*[
            _upload(proj_id, site_id, part_rows)
            for (proj_id, site_id), part_rows in by_partition.items()
        ])
        await msgs_to_ack[-1].ack()
        log.info("Acknowledged %d messages (batch)", len(msgs_to_ack))

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
