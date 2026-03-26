"""
Subscriber API — WebSocket server for battery telemetry.

Real-time: FlashMQ MQTT subscription (paho-mqtt) broadcasts incoming
           messages to all connected WebSocket clients.
History:   client sends a query_history request; DuckDB reads date-
           partitioned Parquet files from S3/local and returns results.

Wire protocol (all JSON):

  Client → Server
    {"type":"query_history","query_id":"1",
     "site":"evelyn","instance":"unit-3",
     "from_ts":1234567890.0,"to_ts":1234567891.0,"limit":2000}

  Server → Client
    {"type":"live",    "topic":"unit/3/kw", "payload":{flat record}}
    {"type":"history", "query_id":"1","columns":[...],"rows":[...],
                       "total":N,"elapsed_ms":N,"backend":"duckdb"}
    {"type":"error",   "query_id":"1","message":"..."}

Path layout:
  s3://{bucket}/{prefix}/{site}/{instance}/{yyyy}/{mm}/{dd}/*.parquet
  e.g. s3://ems-archive/parquet/evelyn/unit-3/2025/06/01/data.parquet

Usage:
  pip install duckdb websockets pyyaml paho-mqtt
  python subscriber.py
  python subscriber.py --config config.yaml
"""

import argparse
import asyncio
import json
import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import os

import duckdb
import paho.mqtt.client as mqtt
import websockets
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_clients: set = set()
_executor = ThreadPoolExecutor(max_workers=4)
_loop: asyncio.AbstractEventLoop | None = None   # set in run()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten(row: dict) -> dict:
    """Flatten {value, unit} measurement objects to plain scalars."""
    return {
        k: (v["value"] if isinstance(v, dict) and "value" in v else v)
        for k, v in row.items()
    }


def _day_partitions(from_ts: float, to_ts: float):
    """Yield (yyyy, mm, dd) strings for every UTC day in [from_ts, to_ts]."""
    dt = datetime.fromtimestamp(from_ts, tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = datetime.fromtimestamp(to_ts, tz=timezone.utc)
    while dt <= end:
        yield dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
        dt += timedelta(days=1)


def _build_globs(cfg: dict, site: str, instance: str,
                 from_ts: float, to_ts: float) -> list:
    """Return glob paths covering [from_ts, to_ts] for this site/instance.

    Uses local_path if configured (fractal-phil / tests), otherwise S3.
    """
    local = cfg["s3"].get("local_path")
    if local:
        base = os.path.join(local.rstrip("/"), site, instance)
        return [
            os.path.join(base, yyyy, mm, dd, "*.parquet")
            for yyyy, mm, dd in _day_partitions(from_ts, to_ts)
        ]

    bucket = cfg["s3"]["bucket"]
    prefix = cfg["s3"].get("prefix", "").strip("/")
    base = "s3://{}/{}{}".format(
        bucket,
        prefix + "/" if prefix else "",
        f"{site}/{instance}",
    )
    return [
        f"{base}/{yyyy}/{mm}/{dd}/*.parquet"
        for yyyy, mm, dd in _day_partitions(from_ts, to_ts)
    ]


# ---------------------------------------------------------------------------
# DuckDB / S3
# ---------------------------------------------------------------------------

def _make_conn(cfg: dict) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    if cfg["s3"].get("local_path"):
        return conn   # local filesystem — no httpfs needed
    s3 = cfg["s3"]
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    if s3.get("endpoint_url"):
        netloc = urlparse(s3["endpoint_url"]).netloc
        conn.execute(f"SET s3_endpoint='{netloc}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
    conn.execute(f"SET s3_region='{s3.get('region', 'us-east-1')}';")
    if s3.get("access_key"):
        conn.execute(f"SET s3_access_key_id='{s3['access_key']}';")
        conn.execute(f"SET s3_secret_access_key='{s3['secret_key']}';")
    return conn


def _query_history(cfg: dict, site: str, instance: str,
                   from_ts: float, to_ts: float, limit: int):
    """Query parquet files for a site/instance/time-range. Returns (columns, rows)."""
    conn = _make_conn(cfg)
    globs = _build_globs(cfg, site, instance, from_ts, to_ts)
    sources = ", ".join(f"'{g}'" for g in globs)

    try:
        result = conn.execute(f"""
            SELECT * FROM read_parquet([{sources}], union_by_name=true)
            WHERE timestamp >= {from_ts} AND timestamp <= {to_ts}
            ORDER BY timestamp
            LIMIT {limit}
        """)
        cols = [d[0] for d in result.description]
        rows = result.fetchall()
    except Exception as exc:
        # Some day partitions may not exist — fall back to full-instance glob
        log.warning("Partition query failed (%s) — falling back to instance glob", exc)
        local = cfg["s3"].get("local_path")
        if local:
            base = os.path.join(local.rstrip("/"), site, instance)
        else:
            bucket = cfg["s3"]["bucket"]
            prefix = cfg["s3"].get("prefix", "").strip("/")
            base = "s3://{}/{}{}".format(
                bucket,
                prefix + "/" if prefix else "",
                f"{site}/{instance}",
            )
        try:
            result = conn.execute(f"""
                SELECT * FROM read_parquet('{base}/*/*/*/*.parquet', union_by_name=true)
                WHERE timestamp >= {from_ts} AND timestamp <= {to_ts}
                ORDER BY timestamp
                LIMIT {limit}
            """)
            cols = [d[0] for d in result.description]
            rows = result.fetchall()
        except Exception as fallback_exc:
            conn.close()
            raise RuntimeError(str(fallback_exc)) from fallback_exc

    conn.close()
    return cols, rows


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

async def _broadcast(msg: dict) -> None:
    if not _clients:
        return
    text = json.dumps(msg, default=str)
    await asyncio.gather(
        *(c.send(text) for c in list(_clients)),
        return_exceptions=True,
    )


async def _ws_handler(websocket, cfg: dict) -> None:
    _clients.add(websocket)
    log.info("Client connected %s  (total: %d)", websocket.remote_address, len(_clients))
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if msg.get("type") != "query_history":
                continue

            query_id = str(msg.get("query_id", ""))
            site     = str(msg.get("site",     "unknown"))
            instance = str(msg.get("instance", "unknown"))
            from_ts  = float(msg.get("from_ts", time.time() - 300))
            to_ts    = float(msg.get("to_ts",   time.time()))
            limit    = int(msg.get("limit",   2000))

            t0 = time.monotonic()
            try:
                loop = asyncio.get_running_loop()
                cols, rows = await loop.run_in_executor(
                    _executor, _query_history, cfg, site, instance, from_ts, to_ts, limit
                )
                elapsed = int((time.monotonic() - t0) * 1000)
                await websocket.send(json.dumps({
                    "type":       "history",
                    "query_id":   query_id,
                    "columns":    cols,
                    "rows":       [list(r) for r in rows],
                    "total":      len(rows),
                    "elapsed_ms": elapsed,
                    "backend":    "duckdb",
                }, default=str))
                log.info("History  site=%s  instance=%s  rows=%d  %dms",
                         site, instance, len(rows), elapsed)
            except Exception as exc:
                await websocket.send(json.dumps({
                    "type":     "error",
                    "query_id": query_id,
                    "message":  str(exc),
                }))
                log.error("Query failed: %s", exc)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        _clients.discard(websocket)
        log.info("Client disconnected (total: %d)", len(_clients))


# ---------------------------------------------------------------------------
# MQTT live subscription — paho runs in its own thread, bridges to asyncio
# ---------------------------------------------------------------------------

def _start_mqtt(cfg: dict, loop: asyncio.AbstractEventLoop) -> None:
    """Start paho-mqtt client in a daemon thread, posting to asyncio event loop."""
    mqtt_cfg = cfg["live"]["mqtt"]
    url      = mqtt_cfg["url"]          # tcp://host:port
    topics   = mqtt_cfg.get("topics", ["unit/#"])

    parsed = urlparse(url)
    host   = parsed.hostname
    port   = parsed.port or 1883

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            for topic in topics:
                client.subscribe(topic)
            log.info("MQTT connected %s:%d  topics=%s", host, port, topics)
        else:
            log.warning("MQTT connect failed rc=%d", rc)

    def on_message(client, userdata, msg):
        try:
            payload = _flatten(json.loads(msg.payload))
            broadcast = {"type": "live", "topic": msg.topic, "payload": payload}
            asyncio.run_coroutine_threadsafe(_broadcast(broadcast), loop)
        except Exception:
            pass

    def on_disconnect(client, userdata, rc):
        log.warning("MQTT disconnected rc=%d — paho will reconnect", rc)

    client = mqtt.Client()
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=10)

    client.connect_async(host, port)
    client.loop_forever()   # blocks this thread; auto-reconnects on drop


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run(cfg: dict) -> None:
    global _loop
    _loop = asyncio.get_running_loop()

    host = cfg["server"].get("host", "0.0.0.0")
    port = cfg["server"].get("port", 8767)

    mqtt_thread = threading.Thread(
        target=_start_mqtt, args=(cfg, _loop), daemon=True, name="mqtt-live"
    )
    mqtt_thread.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT,  stop.set)
    loop.add_signal_handler(signal.SIGTERM, stop.set)

    async def handler(websocket):
        await _ws_handler(websocket, cfg)

    async with websockets.serve(handler, host, port):
        log.info("Subscriber API  ws://%s:%d", host, port)
        await stop.wait()

    log.info("Shutdown complete")


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Battery Subscriber API")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    asyncio.run(run(load_config(args.config)))
