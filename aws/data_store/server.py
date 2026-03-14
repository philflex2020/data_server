"""
AWS data store server — monitors MinIO for incoming Parquet files, can generate
test Parquet files directly into MinIO, and exposes a WebSocket interface for
the browser-based AWS dashboard.

WebSocket API (ws://localhost:8766):
  Client → Server:
    {"type": "get_status"}
    {"type": "set_interval", "seconds": 5}
    {"type": "generate",
       "sites":           2,      # number of sites
       "racks_per_site":  3,
       "modules_per_rack":4,
       "cells_per_module":6,
       "rows_per_file":   500,    # measurements per Parquet file
       "num_files":       1,      # files to generate per site
       "hours_ago":       1}      # timestamp base (data appears N hours in past

  Server → Client:
    {"type": "status",   "s3_connected": bool, "bucket": str, "endpoint": str}
    {"type": "stats",    "file_count": int, "total_rows": int, "total_bytes": int}
    {"type": "file",     "key": str, "size": int, "rows": int, "columns": [...], "sample": {...}, "arrived_at": str}
    {"type": "generate_progress", "done": int, "total": int, "key": str}
    {"type": "generate_done",     "files_written": int, "total_rows": int}

Usage:
  pip install boto3 pyarrow websockets pyyaml
  python server.py
  python server.py --config config.yaml
"""

import argparse
import asyncio
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Set

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import websockets
import yaml
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

g_config: dict = {}
g_s3 = None
g_s3_connected: bool = False
g_connected: Set = set()
g_seen: set = set()
g_stats = {"file_count": 0, "total_rows": 0, "total_bytes": 0}
g_poll_interval: int = 10


# ---------------------------------------------------------------------------
# S3 helpers
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


def check_s3_connection(s3, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            # bucket missing — try to create it
            try:
                s3.create_bucket(Bucket=bucket)
                log.info("Created bucket: %s", bucket)
                return True
            except Exception:
                pass
        return False
    except Exception:
        return False


def list_objects(s3, bucket: str, prefix: str) -> list[dict]:
    try:
        items = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                items.append({
                    "key":           obj["Key"],
                    "size":          obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })
        return items
    except Exception:
        return []


def peek_parquet(s3, bucket: str, key: str, max_rows: int = 5) -> dict:
    try:
        resp  = s3.get_object(Bucket=bucket, Key=key)
        data  = resp["Body"].read()
        table = pq.read_table(BytesIO(data))
        # Convert sample to JSON-serialisable form (handle non-serialisable types)
        raw_sample = table.slice(0, max_rows).to_pydict()
        sample = {
            k: [str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v for v in vals]
            for k, vals in raw_sample.items()
        }
        return {
            "rows":    table.num_rows,
            "columns": table.schema.names,
            "sample":  sample,
        }
    except Exception as exc:
        return {"rows": 0, "columns": [], "sample": {}, "error": str(exc)}


# ---------------------------------------------------------------------------
# Parquet generator
# ---------------------------------------------------------------------------

MEASUREMENTS = {
    "voltage":           (3.0,  4.2,  3.7),
    "current":           (-50.0, 50.0, 0.0),
    "temperature":       (20.0, 45.0, 25.0),
    "soc":               (0.0,  100.0, 80.0),
    "internal_resistance": (0.001, 0.05, 0.01),
}


def _make_parquet_bytes(rows: list[dict], compression: str = "snappy") -> bytes:
    if not rows:
        return b""
    all_keys = list({k for r in rows for k in r})
    cols = {k: [r.get(k) for r in rows] for k in all_keys}
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

def _s3_key(cfg: dict, site_id: int, ts: datetime) -> str:
    """Build an S3 key from the partitions template in cfg["s3"]["partitions"]."""
    s3_cfg = cfg["s3"]
    partitions = s3_cfg.get("partitions", _DEFAULT_PARTITIONS)
    prefix     = s3_cfg.get("prefix", "").strip("/")
    vals = {
        "project_id": s3_cfg.get("project_id", 0),
        "site_id":    site_id,
        "year":       ts.strftime("%Y"),
        "month":      ts.strftime("%m"),
        "day":        ts.strftime("%d"),
        "hour":       ts.strftime("%H"),
    }
    parts = [p for p in [prefix] + [p.format(**vals) for p in partitions]
             + [ts.strftime("%Y%m%dT%H%M%SZ") + ".parquet"] if p]
    return "/".join(parts)


def generate_parquet_files(s3, cfg: dict, gen_cfg: dict, progress_cb=None) -> dict:
    """
    Synchronous — run in executor.
    Generates Parquet files directly into S3/MinIO.
    Returns {"files_written": N, "total_rows": N}
    """
    bucket         = cfg["s3"]["bucket"]
    sites          = int(gen_cfg.get("sites", 2))
    racks          = int(gen_cfg.get("racks_per_site", 3))
    modules        = int(gen_cfg.get("modules_per_rack", 4))
    cells_pm       = int(gen_cfg.get("cells_per_module", 6))
    rows_per_file  = int(gen_cfg.get("rows_per_file", 500))
    num_files      = int(gen_cfg.get("num_files", 1))
    hours_ago      = float(gen_cfg.get("hours_ago", 1))

    total_files    = sites * num_files
    files_written  = 0
    total_rows     = 0

    base_ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)

    for site_id in range(sites):
        # Current value state per (rack, module, cell, measurement)
        state: dict[tuple, float] = {}
        for rack in range(racks):
            for mod in range(modules):
                for cell in range(cells_pm):
                    for mname, (mn, mx, nom) in MEASUREMENTS.items():
                        state[(rack, mod, cell, mname)] = nom + random.uniform(-0.05, 0.05) * (mx - mn)

        for file_idx in range(num_files):
            file_ts = base_ts + timedelta(minutes=file_idx * (rows_per_file / 60))
            rows = []
            interval = 1.0 / max(1, racks * modules * cells_pm)  # spread samples across 1s per cell

            for row_i in range(rows_per_file):
                rack   = row_i % racks
                mod    = (row_i // racks) % modules
                cell   = (row_i // (racks * modules)) % cells_pm
                ts     = file_ts + timedelta(seconds=row_i * interval)
                row    = {
                    "timestamp": ts.timestamp(),
                    "site_id":   site_id,
                    "rack_id":   rack,
                    "module_id": mod,
                    "cell_id":   cell,
                }
                for mname, (mn, mx, _) in MEASUREMENTS.items():
                    v = state[(rack, mod, cell, mname)]
                    v += random.uniform(-0.01, 0.01) * (mx - mn)
                    v  = max(mn, min(mx, v))
                    state[(rack, mod, cell, mname)] = v
                    row[mname] = round(v, 4)
                rows.append(row)

            data = _make_parquet_bytes(rows)
            key  = _s3_key(cfg, site_id, file_ts)

            s3.put_object(Bucket=bucket, Key=key, Body=data)
            files_written += 1
            total_rows    += len(rows)
            log.info("Generated %s  (%d bytes, %d rows)", key, len(data), len(rows))

            if progress_cb:
                progress_cb(files_written, total_files, key)

    return {"files_written": files_written, "total_rows": total_rows}


# ---------------------------------------------------------------------------
# WebSocket helpers
# ---------------------------------------------------------------------------

async def broadcast(message: dict) -> None:
    if not g_connected:
        return
    data = json.dumps(message, default=str)
    await asyncio.gather(
        *[ws.send(data) for ws in list(g_connected)],
        return_exceptions=True,
    )


async def send_status(ws=None) -> None:
    msg = {
        "type":         "status",
        "s3_connected": g_s3_connected,
        "bucket":       g_config["s3"]["bucket"],
        "endpoint":     g_config["s3"].get("endpoint_url", "AWS S3"),
        "poll_interval": g_poll_interval,
    }
    if ws:
        await ws.send(json.dumps(msg, default=str))
    else:
        await broadcast(msg)


async def send_stats(ws=None) -> None:
    msg = {"type": "stats", **g_stats}
    if ws:
        await ws.send(json.dumps(msg, default=str))
    else:
        await broadcast(msg)


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

async def ws_handler(websocket) -> None:
    global g_poll_interval

    g_connected.add(websocket)
    log.info("Client connected (%d total)", len(g_connected))

    await send_status(websocket)
    await send_stats(websocket)

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = msg.get("type")
            if t == "get_status":
                await send_status(websocket)
                await send_stats(websocket)
            elif t == "set_interval":
                g_poll_interval = max(1, int(msg.get("seconds", 10)))
                log.info("Poll interval set to %ds", g_poll_interval)
                await send_status(websocket)
            elif t == "generate":
                gen_cfg = {k: msg[k] for k in msg if k != "type"}
                total_files = int(gen_cfg.get("sites", 2)) * int(gen_cfg.get("num_files", 1))
                log.info("Generating %d Parquet file(s) into s3://%s ...", total_files, g_config["s3"]["bucket"])

                def progress(done, total, key):
                    asyncio.run_coroutine_threadsafe(
                        broadcast({"type": "generate_progress", "done": done, "total": total, "key": key}),
                        asyncio.get_event_loop(),
                    )

                try:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, generate_parquet_files, g_s3, g_config, gen_cfg, progress
                    )
                    await broadcast({"type": "generate_done", **result})
                except Exception as exc:
                    log.error("Generate failed: %s", exc)
                    await broadcast({"type": "generate_done", "error": str(exc), "files_written": 0, "total_rows": 0})

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        g_connected.discard(websocket)
        log.info("Client disconnected (%d remaining)", len(g_connected))


# ---------------------------------------------------------------------------
# Poll loop
# ---------------------------------------------------------------------------

async def poll_loop() -> None:
    global g_s3_connected, g_stats

    bucket = g_config["s3"]["bucket"]
    prefix = g_config["s3"].get("prefix", "")

    while True:
        # Check / re-check S3 connection
        was_connected = g_s3_connected
        g_s3_connected = await asyncio.get_event_loop().run_in_executor(
            None, check_s3_connection, g_s3, bucket
        )

        if g_s3_connected != was_connected:
            await send_status()

        if g_s3_connected:
            objects = await asyncio.get_event_loop().run_in_executor(
                None, list_objects, g_s3, bucket, prefix
            )

            new_objects = [o for o in objects if o["key"] not in g_seen]

            for obj in new_objects:
                g_seen.add(obj["key"])
                meta = await asyncio.get_event_loop().run_in_executor(
                    None, peek_parquet, g_s3, bucket, obj["key"]
                )

                rows  = meta.get("rows", 0)
                g_stats["file_count"]  += 1
                g_stats["total_rows"]  += rows
                g_stats["total_bytes"] += obj["size"]

                log.info(
                    "New file: %s  (%d bytes, %d rows)",
                    obj["key"], obj["size"], rows,
                )

                await broadcast({
                    "type":       "file",
                    "key":        obj["key"],
                    "size":       obj["size"],
                    "rows":       rows,
                    "columns":    meta.get("columns", []),
                    "sample":     meta.get("sample", {}),
                    "arrived_at": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                })
                await send_stats()

        await asyncio.sleep(g_poll_interval)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main_async(cfg: dict) -> None:
    global g_config, g_s3, g_poll_interval

    g_config       = cfg
    g_s3           = build_s3_client(cfg)
    g_poll_interval = cfg.get("poll_interval_seconds", 10)

    ws_cfg  = cfg.get("websocket", {})
    ws_host = ws_cfg.get("host", "0.0.0.0")
    ws_port = ws_cfg.get("port", 8766)

    log.info("WebSocket server on ws://%s:%d", ws_host, ws_port)
    log.info("Monitoring s3://%s  endpoint=%s", cfg["s3"]["bucket"], cfg["s3"].get("endpoint_url", "AWS"))

    async with websockets.serve(ws_handler, ws_host, ws_port):
        await poll_loop()


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AWS data store WebSocket server")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    try:
        asyncio.run(main_async(load_config(args.config)))
    except KeyboardInterrupt:
        log.info("Shutting down")
