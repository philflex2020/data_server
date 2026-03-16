"""
Subscriber API server — single WebSocket interface for both real-time and
historical battery data.

Real-time:  consumes live messages from NATS JetStream (push consumer, new
            messages only) and forwards them to connected WebSocket clients.
Historical: queries S3 Parquet files via embedded DuckDB.

WebSocket API (ws://localhost:8767):

  Client → Server:
    {"type": "get_status"}
    {"type": "subscribe",   "subject": "batteries.>"}   # start live stream
    {"type": "unsubscribe"}                              # stop live stream
    {"type": "query_history",
       "query_id": "q1",
       "proj_id":  "0",          # omit for all projects
       "site_id":  "0",          # omit for all sites
       "from_ts":  1700000000.0, # unix timestamp, omit for last 1 hour
       "to_ts":    1700003600.0, # unix timestamp, omit for now
       "limit":    1000}

  Server → Client:
    {"type": "status",  "nats_connected": bool, "s3_connected": bool, "subscribed": bool, "subject": str}
    {"type": "live",    "subject": str, "payload": {...}}
    {"type": "stats",   "live_total": int, "live_per_sec": int, "queries_run": int}
    {"type": "history", "query_id": str, "columns": [...], "rows": [...], "total": int, "elapsed_ms": float}
    {"type": "error",   "query_id": str, "message": str}

Usage:
  pip install nats-py duckdb websockets boto3 pyyaml
  python server.py
  python server.py --config config.yaml
"""

import argparse
import asyncio
import collections
import json
import logging
import time
from typing import Optional, Set

import duckdb
import nats
import websockets
import yaml
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from flux_compat import serve_flux_api, update_server_state

LOG_BUFFER: collections.deque = collections.deque(maxlen=200)

class _BufferHandler(logging.Handler):
    def emit(self, record):
        LOG_BUFFER.append(self.format(record))

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_buf_handler = _BufferHandler()
_buf_handler.setFormatter(_fmt)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
logging.getLogger().addHandler(_buf_handler)

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

g_config: dict = {}
g_connected: Set = set()
g_nc: Optional[object] = None        # NATS connection
g_nats_connected: bool = False
g_s3_connected: bool = False
g_live_sub: Optional[object] = None  # NATS push subscription
g_live_subject: str = ""
g_stats = {"live_total": 0, "live_per_sec": 0, "queries_run": 0}
g_live_window: list = []
g_start_time = time.time()
g_duckdb: Optional[duckdb.DuckDBPyConnection] = None
g_stream_first_ts: float = 0.0   # unix ts of oldest retained message in NATS stream

# In-memory ring buffer of live messages for fast recent-history queries.
# At ~3000 msgs/s (3 sites), 1_080_000 entries covers ~6 minutes. Each entry is a flat dict.
BUFFER_MAXLEN = 1_080_000
g_live_buffer: collections.deque = collections.deque(maxlen=BUFFER_MAXLEN)


# ---------------------------------------------------------------------------
# DuckDB / S3
# ---------------------------------------------------------------------------

def build_duckdb(cfg: dict) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    s3 = cfg["s3"]
    if s3.get("endpoint_url"):
        endpoint = s3["endpoint_url"].replace("http://", "").replace("https://", "")
        ssl = "true" if s3["endpoint_url"].startswith("https") else "false"
        conn.execute(f"""
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{s3.get("access_key", "")}';
            SET s3_secret_access_key='{s3.get("secret_key", "")}';
            SET s3_use_ssl={ssl};
            SET s3_url_style='path';
            SET s3_region='{s3.get("region", "us-east-1")}';
        """)
    else:
        conn.execute(f"""
            SET s3_access_key_id='{s3.get("access_key", "")}';
            SET s3_secret_access_key='{s3.get("secret_key", "")}';
            SET s3_region='{s3.get("region", "us-east-1")}';
        """)
    return conn


def check_s3(cfg: dict) -> bool:
    try:
        import boto3
        s3_cfg = cfg["s3"]
        kwargs = dict(
            region_name           = s3_cfg.get("region", "us-east-1"),
            aws_access_key_id     = s3_cfg.get("access_key"),
            aws_secret_access_key = s3_cfg.get("secret_key"),
        )
        if s3_cfg.get("endpoint_url"):
            kwargs["endpoint_url"] = s3_cfg["endpoint_url"]
        boto3.client("s3", **kwargs).head_bucket(Bucket=s3_cfg["bucket"])
        return True
    except Exception:
        return False


def _date_paths(base: str, proj_id: str, site_id: str, from_ts: float, to_ts: float) -> list[str]:
    """Return targeted S3 glob paths covering only the date dirs spanned by [from_ts, to_ts].
    Writer lays out: project={p}/site={s}/{YYYY}/{MM}/{DD}/*.parquet
    """
    from datetime import datetime, timezone, timedelta
    proj_glob = f"project={proj_id}" if proj_id else "project=*"
    site_glob = f"site={site_id}"    if site_id else "site=*"
    start = datetime.fromtimestamp(from_ts, tz=timezone.utc).date()
    end   = datetime.fromtimestamp(to_ts,   tz=timezone.utc).date()
    paths, day = [], start
    while day <= end:
        paths.append(
            f"{base}{proj_glob}/{site_glob}/"
            f"{day.year}/{day.month:02d}/{day.day:02d}/*.parquet"
        )
        day += timedelta(days=1)
    return paths


def run_history_query(cfg: dict, proj_id: str, site_id: str, from_ts: float, to_ts: float, limit: int) -> dict:
    bucket = cfg["s3"]["bucket"]
    prefix = cfg["s3"].get("prefix", "").strip("/")
    base = f"s3://{bucket}/{prefix + '/' if prefix else ''}"

    paths = _date_paths(base, proj_id, site_id, from_ts, to_ts)
    # DuckDB accepts a list of globs; hive_partitioning extracts project= and site= columns.
    path_list = "[" + ", ".join(f"'{p}'" for p in paths) + "]"

    where = [f"timestamp >= {from_ts}", f"timestamp <= {to_ts}"]
    sql = f"""
        SELECT *
        FROM read_parquet({path_list}, hive_partitioning=true, union_by_name=true)
        WHERE {' AND '.join(where)}
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    t0       = time.monotonic()
    cursor   = g_duckdb.execute(sql)
    columns  = [d[0] for d in cursor.description]
    raw_rows = cursor.fetchall()
    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
    rows = [
        [float(v) if isinstance(v, (int, float))
         else v.decode("utf-8", errors="replace") if isinstance(v, bytes)
         else str(v) if v is not None else None
         for v in row]
        for row in raw_rows
    ]
    return {"columns": columns, "rows": rows, "total": len(rows), "elapsed_ms": elapsed_ms}


# ---------------------------------------------------------------------------
# NATS live subscription
# ---------------------------------------------------------------------------

async def start_live_sub(subject: str) -> None:
    global g_live_sub, g_live_subject
    await stop_live_sub()

    js = g_nc.jetstream()
    stream_name = g_config["nats"]["stream_name"]

    async def message_handler(msg):
        global g_stats, g_live_window
        await msg.ack()
        now = time.monotonic()
        g_live_window = [t for t in g_live_window if now - t < 1.0]
        g_live_window.append(now)
        g_stats["live_total"]   += 1
        g_stats["live_per_sec"]  = len(g_live_window)
        try:
            raw = json.loads(msg.data)
            # Flatten {value, unit} measurement objects to plain scalars
            payload = {k: (v["value"] if isinstance(v, dict) and "value" in v else v)
                       for k, v in raw.items()}
        except Exception:
            payload = {"raw": msg.data.decode(errors="replace")}
        g_live_buffer.append(payload)
        await broadcast({"type": "live", "subject": msg.subject, "payload": payload})

    # Push consumer — deliver only NEW messages (not historical backlog)
    g_live_sub = await js.subscribe(
        subject,
        stream  = stream_name,
        config  = ConsumerConfig(
            deliver_policy = DeliverPolicy.NEW,
            ack_policy     = AckPolicy.EXPLICIT,
        ),
        cb = message_handler,
    )
    g_live_subject = subject
    log.info("Live subscription started: subject=%s stream=%s", subject, stream_name)


async def stop_live_sub() -> None:
    global g_live_sub, g_live_subject
    if g_live_sub:
        try:
            await g_live_sub.unsubscribe()
        except Exception:
            pass
        g_live_sub     = None
        g_live_subject = ""
        log.info("Live subscription stopped")


# ---------------------------------------------------------------------------
# WebSocket helpers
# ---------------------------------------------------------------------------

async def broadcast(msg: dict) -> None:
    if not g_connected:
        return
    data = json.dumps(msg, default=str)
    await asyncio.gather(*[ws.send(data) for ws in list(g_connected)], return_exceptions=True)


async def broadcast_status() -> None:
    await broadcast({
        "type":           "status",
        "nats_connected": g_nats_connected,
        "s3_connected":   g_s3_connected,
        "subscribed":     g_live_sub is not None,
        "subject":        g_live_subject,
    })


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

async def ws_handler(websocket) -> None:
    g_connected.add(websocket)
    log.info("Client connected (%d total)", len(g_connected))

    await websocket.send(json.dumps({
        "type":           "status",
        "nats_connected": g_nats_connected,
        "s3_connected":   g_s3_connected,
        "subscribed":     g_live_sub is not None,
        "subject":        g_live_subject,
    }))
    await websocket.send(json.dumps({"type": "stats", **g_stats}))

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = msg.get("type")

            if t == "get_status":
                await websocket.send(json.dumps({
                    "type":           "status",
                    "nats_connected": g_nats_connected,
                    "s3_connected":   g_s3_connected,
                    "subscribed":     g_live_sub is not None,
                    "subject":        g_live_subject,
                }))
                await websocket.send(json.dumps({"type": "stats", **g_stats}))

            elif t == "subscribe":
                subject = msg.get("subject", g_config["nats"].get("default_subject", "batteries.>"))
                if g_nats_connected:
                    try:
                        await start_live_sub(subject)
                        await broadcast_status()
                    except Exception as exc:
                        log.error("Subscribe failed: %s", exc)
                        await websocket.send(json.dumps({
                            "type": "error", "query_id": "", "message": str(exc)
                        }))
                else:
                    await websocket.send(json.dumps({
                        "type": "error", "query_id": "", "message": "NATS not connected"
                    }))

            elif t == "unsubscribe":
                await stop_live_sub()
                await broadcast_status()

            elif t == "get_logs":
                lines = int(msg.get("lines", 100))
                await websocket.send(json.dumps({
                    "type": "logs",
                    "lines": list(LOG_BUFFER)[-lines:]
                }))

            elif t == "query_history":
                query_id = msg.get("query_id", "q")
                proj_id  = msg.get("proj_id",  "")
                site_id  = msg.get("site_id",  "")
                now      = time.time()
                from_ts  = float(msg.get("from_ts", now - 3600))
                to_ts    = float(msg.get("to_ts",   now))
                limit    = min(int(msg.get("limit",  g_config["history"]["default_limit"])),
                               g_config["history"]["max_limit"])
                g_stats["queries_run"] += 1
                try:
                    buf_oldest = g_live_buffer[0].get("timestamp", 0) if g_live_buffer else 0
                    buf_newest = g_live_buffer[-1].get("timestamp", 0) if g_live_buffer else 0
                    use_buffer = bool(g_live_buffer) and to_ts >= buf_oldest and from_ts <= buf_newest
                    if use_buffer:
                        result = run_buffer_query(from_ts, to_ts, proj_id, site_id, limit)
                    else:
                        result = await asyncio.get_event_loop().run_in_executor(
                            None, run_history_query, g_config, proj_id, site_id, from_ts, to_ts, limit
                        )
                    await websocket.send(json.dumps({"type": "history", "query_id": query_id, **result}, default=str))
                except Exception as exc:
                    log.error("History query failed: %s", exc)
                    await websocket.send(json.dumps({"type": "error", "query_id": query_id, "message": str(exc)}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        g_connected.discard(websocket)
        log.info("Client disconnected (%d remaining)", len(g_connected))
        if not g_connected:
            await stop_live_sub()


# ---------------------------------------------------------------------------
# Background loops
# ---------------------------------------------------------------------------

async def stats_loop() -> None:
    while True:
        update_server_state({
            "nats_connected": g_nats_connected,
            "s3_connected":   g_s3_connected,
            "live_per_sec":   g_stats["live_per_sec"],
            "live_total":     g_stats["live_total"],
            "queries_run":    g_stats["queries_run"],
            "ws_clients":     len(g_connected),
            "live_subject":   g_live_subject,
            "uptime_sec":     round(time.time() - g_start_time),
        })
        await broadcast({"type": "stats", **g_stats})
        await asyncio.sleep(1)


async def nats_connect_loop() -> None:
    global g_nc, g_nats_connected
    nats_url = g_config["nats"]["url"]

    while True:
        try:
            if g_nc is None or g_nc.is_closed:
                g_nc = await nats.connect(
                    nats_url,
                    disconnected_cb = on_nats_disconnect,
                    reconnected_cb  = on_nats_reconnect,
                    error_cb        = on_nats_error,
                )
                g_nats_connected = True
                log.info("Connected to NATS at %s", nats_url)
                await broadcast_status()
        except Exception as exc:
            g_nats_connected = False
            log.warning("NATS connection failed: %s — retrying in 5s", exc)
            await broadcast_status()
        await asyncio.sleep(5)


async def on_nats_disconnect() -> None:
    global g_nats_connected
    g_nats_connected = False
    log.warning("NATS disconnected")
    await broadcast_status()


async def on_nats_reconnect() -> None:
    global g_nats_connected, g_live_sub
    g_nats_connected = True
    log.info("NATS reconnected")
    # Re-establish live subscription if one was active
    if g_live_subject:
        g_live_sub = None  # old sub is dead
        try:
            await start_live_sub(g_live_subject)
            log.info("Live subscription restored after reconnect: %s", g_live_subject)
        except Exception as exc:
            log.warning("Could not restore live subscription: %s", exc)
    await broadcast_status()


async def on_nats_error(e) -> None:
    log.warning("NATS error: %s", e)


def run_buffer_query(from_ts: float, to_ts: float, proj_id: str, site_id: str, limit: int) -> dict:
    """Serve a time-range query from the in-memory live buffer.
    No NATS consumers, no network — just a deque scan. O(n) but fast.
    """
    t0       = time.monotonic()
    all_rows = []
    # Deque is ordered oldest→newest; iterate in reverse for newest-first.
    for msg in reversed(g_live_buffer):
        ts = float(msg.get("timestamp", 0))
        if ts > to_ts:
            continue
        if ts < from_ts:
            break   # time-ordered: everything older can be skipped
        if site_id and str(msg.get("site_id", "")) != site_id:
            continue
        if proj_id and str(msg.get("project_id", msg.get("project", ""))) != proj_id:
            continue
        all_rows.append(msg)
        if len(all_rows) >= limit:
            break

    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
    if not all_rows:
        return {"columns": [], "rows": [], "total": 0, "elapsed_ms": elapsed_ms}

    col_set = set()
    for row in all_rows:
        col_set.update(row.keys())
    columns = ["timestamp"] + sorted(col_set - {"timestamp"})
    rows = [
        [float(v) if isinstance(v, (int, float)) else str(v) if v is not None else None
         for v in (row.get(c) for c in columns)]
        for row in all_rows
    ]
    log.info("Buffer query: %d rows in %.0fms (%.0f–%.0f, site=%s)",
             len(rows), elapsed_ms, from_ts, to_ts, site_id or "*")
    return {"columns": columns, "rows": rows, "total": len(rows), "elapsed_ms": elapsed_ms}


async def stream_first_ts_loop() -> None:
    """Periodically refresh g_stream_first_ts from the NATS monitoring endpoint."""
    global g_stream_first_ts
    import urllib.request
    from datetime import datetime, timezone
    stream_name = g_config["nats"]["stream_name"]
    nats_mon    = g_config["nats"].get("monitor_url", "http://localhost:8222")

    while True:
        try:
            def _fetch():
                url = f"{nats_mon}/jsz?streams=1"
                with urllib.request.urlopen(url, timeout=3) as r:
                    return json.load(r)
            data = await asyncio.get_event_loop().run_in_executor(None, _fetch)
            for acc in data.get("account_details", []):
                for s in acc.get("stream_detail", []):
                    if s["name"] == stream_name:
                        ts_str = s["state"].get("first_ts", "")
                        if ts_str:
                            g_stream_first_ts = datetime.fromisoformat(
                                ts_str.replace("Z", "+00:00")
                            ).timestamp()
        except Exception as exc:
            log.debug("stream_first_ts refresh failed: %s", exc)
        await asyncio.sleep(30)


async def s3_check_loop() -> None:
    global g_s3_connected
    while True:
        was = g_s3_connected
        g_s3_connected = await asyncio.get_event_loop().run_in_executor(None, check_s3, g_config)
        if g_s3_connected != was:
            await broadcast_status()
        await asyncio.sleep(15)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main_async(cfg: dict) -> None:
    global g_config, g_duckdb

    g_config = cfg
    g_duckdb = build_duckdb(cfg)

    ws_cfg  = cfg.get("websocket", {})
    ws_host = ws_cfg.get("host", "0.0.0.0")
    ws_port = ws_cfg.get("port", 8767)
    log.info("WebSocket server on ws://%s:%d", ws_host, ws_port)

    async with websockets.serve(ws_handler, ws_host, ws_port):
        await asyncio.gather(
            stats_loop(),
            nats_connect_loop(),
            s3_check_loop(),
            serve_flux_api(cfg, g_duckdb),
        )


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Subscriber API server")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    try:
        asyncio.run(main_async(load_config(args.config)))
    except KeyboardInterrupt:
        log.info("Shutting down")
        if g_nc:
            asyncio.run(g_nc.drain())
