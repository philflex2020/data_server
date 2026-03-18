"""
Subscriber API server — single WebSocket interface for both real-time and
historical battery data.

Real-time:  subscribes to an MQTT broker (FlashMQ / Mosquitto) and forwards
            incoming messages to all connected WebSocket clients.
Historical: queries Parquet files via embedded DuckDB (S3/MinIO or local path).

WebSocket API (ws://localhost:8767):

  Client → Server:
    {"type": "get_status"}
    {"type": "subscribe",   "subject": "batteries/#"}   # start live stream (MQTT topic)
    {"type": "unsubscribe"}                              # stop live stream
    {"type": "query_history",
       "query_id": "q1",
       "proj_id":  "0",          # omit for all projects
       "site_id":  "0",          # omit for all sites
       "from_ts":  1700000000.0, # unix timestamp, omit for last 1 hour
       "to_ts":    1700003600.0, # unix timestamp, omit for now
       "limit":    1000}

  Server → Client:
    {"type": "status",  "mqtt_connected": bool, "s3_connected": bool, "subscribed": bool, "subject": str}
    {"type": "live",    "subject": str, "payload": {...}}
    {"type": "stats",   "live_total": int, "live_per_sec": int, "queries_run": int}
    {"type": "history", "query_id": str, "columns": [...], "rows": [...], "total": int, "elapsed_ms": float}
    {"type": "error",   "query_id": str, "message": str}

Usage:
  pip install aiomqtt duckdb websockets boto3 pyyaml
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

import aiomqtt
import duckdb
import websockets
import yaml

from flux_compat import serve_flux_api, update_server_state, flux_stats, _gap_fill_exists_flux as _gap_fill_exists

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
g_mqtt_client: Optional[aiomqtt.Client] = None   # active MQTT client
g_mqtt_connected: bool = False
g_s3_connected: bool = False
g_live_subject: str = ""                          # active MQTT subscription topic
g_stats = {"live_total": 0, "live_per_sec": 0, "queries_run": 0,
           "last_query_ms": 0.0, "avg_query_ms": 0.0}
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

def _local_path(cfg: dict) -> str | None:
    """Return configured local Parquet directory, or None if using S3."""
    return (cfg.get("local") or {}).get("path") or None


def build_duckdb(cfg: dict) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    if _local_path(cfg):
        return conn   # local filesystem — no httpfs or S3 credentials needed
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
    if _local_path(cfg):
        import os
        return os.path.isdir(_local_path(cfg))
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
    local = _local_path(cfg)
    if local:
        base      = local.rstrip("/") + "/"
        gap_base  = ""
    else:
        bucket     = cfg["s3"]["bucket"]
        prefix     = cfg["s3"].get("prefix", "").strip("/")
        gap_prefix = cfg["s3"].get("gap_fill_prefix", "").strip("/")
        base     = f"s3://{bucket}/{prefix + '/' if prefix else ''}"
        gap_base = f"s3://{bucket}/{gap_prefix + '/' if gap_prefix else ''}" if gap_prefix else ""

    primary_paths = _date_paths(base, proj_id, site_id, from_ts, to_ts)

    # Include current_state.parquet (gap writer output) if present — covers the
    # most recent flush-interval window not yet in the date-partitioned files.
    if local:
        import os
        cs = local.rstrip("/") + "/current_state.parquet"
        if os.path.exists(cs):
            primary_paths.append(cs)

    primary_list = "[" + ", ".join(f"'{p}'" for p in primary_paths) + "]"
    where        = f"timestamp >= {from_ts} AND timestamp <= {to_ts}"

    if not local and gap_base and _gap_fill_exists(cfg):
        gap_paths = _date_paths(gap_base, proj_id, site_id, from_ts, to_ts)
        gap_list  = "[" + ", ".join(f"'{p}'" for p in gap_paths) + "]"
        # UNION primary + gap-fill, deduplicate keeping primary (src=0) over gap-fill (src=1).
        sql = f"""
            WITH combined AS (
                SELECT *, 0 AS _src
                FROM read_parquet({primary_list}, hive_partitioning=true, union_by_name=true)
                WHERE {where}
                UNION ALL
                SELECT *, 1 AS _src
                FROM read_parquet({gap_list}, hive_partitioning=true, union_by_name=true)
                WHERE {where}
            ),
            deduped AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY timestamp, site_id, rack_id, module_id, cell_id
                    ORDER BY _src
                ) AS _rn
                FROM combined
            )
            SELECT * EXCLUDE (_src, _rn) FROM deduped
            WHERE _rn = 1
            ORDER BY timestamp DESC
            LIMIT {limit}
        """
        log.debug("History query: gap-fill UNION active")
    else:
        sql = f"""
            SELECT *
            FROM read_parquet({primary_list}, hive_partitioning=true, union_by_name=true)
            WHERE {where}
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
# MQTT live subscription helpers
# ---------------------------------------------------------------------------

async def start_live_sub(topic: str) -> None:
    """Subscribe to an MQTT topic. Replaces any active subscription."""
    global g_live_subject
    if g_mqtt_client is None:
        raise RuntimeError("MQTT not connected")
    if g_live_subject:
        try:
            await g_mqtt_client.unsubscribe(g_live_subject)
        except Exception:
            pass
    await g_mqtt_client.subscribe(topic)
    g_live_subject = topic
    log.info("MQTT subscribed: %s", topic)


async def stop_live_sub() -> None:
    global g_live_subject
    if g_mqtt_client and g_live_subject:
        try:
            await g_mqtt_client.unsubscribe(g_live_subject)
        except Exception:
            pass
    g_live_subject = ""
    log.info("MQTT unsubscribed")


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
        "mqtt_connected": g_mqtt_connected,
        "s3_connected":   g_s3_connected,
        "subscribed":     bool(g_live_subject),
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
        "mqtt_connected": g_mqtt_connected,
        "s3_connected":   g_s3_connected,
        "subscribed":     bool(g_live_subject),
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
                    "mqtt_connected": g_mqtt_connected,
                    "s3_connected":   g_s3_connected,
                    "subscribed":     bool(g_live_subject),
                    "subject":        g_live_subject,
                }))
                await websocket.send(json.dumps({"type": "stats", **g_stats}))

            elif t == "subscribe":
                topic = msg.get("subject", g_config.get("mqtt", {}).get("default_topic", "batteries/#"))
                if g_mqtt_connected:
                    try:
                        await start_live_sub(topic)
                        await broadcast_status()
                    except Exception as exc:
                        log.error("Subscribe failed: %s", exc)
                        await websocket.send(json.dumps({
                            "type": "error", "query_id": "", "message": str(exc)
                        }))
                else:
                    await websocket.send(json.dumps({
                        "type": "error", "query_id": "", "message": "MQTT not connected"
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
                    ms = result.get("elapsed_ms", 0.0)
                    g_stats["last_query_ms"] = ms
                    n = g_stats["queries_run"]
                    g_stats["avg_query_ms"] = round(
                        g_stats["avg_query_ms"] * (n - 1) / n + ms / n, 1)
                    log.info("History query: %d rows in %.0fms (id=%s)", result.get("total", 0), ms, query_id)
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
            "mqtt_connected": g_mqtt_connected,
            "s3_connected":   g_s3_connected,
            "live_per_sec":   g_stats["live_per_sec"],
            "live_total":     g_stats["live_total"],
            "queries_run":    g_stats["queries_run"],
            "ws_clients":     len(g_connected),
            "live_subject":   g_live_subject,
            "uptime_sec":     round(time.time() - g_start_time),
        })
        await broadcast({"type": "stats", **g_stats, **flux_stats})
        await asyncio.sleep(1)


async def mqtt_connect_loop() -> None:
    """Maintain a persistent MQTT connection; re-subscribe on reconnect."""
    global g_mqtt_client, g_mqtt_connected
    mqtt_cfg  = g_config.get("mqtt", {})
    host      = mqtt_cfg.get("host", "localhost")
    port      = int(mqtt_cfg.get("port", 1883))
    client_id = mqtt_cfg.get("client_id", "subscriber-api")

    while True:
        try:
            async with aiomqtt.Client(
                host, port=port, identifier=client_id
            ) as client:
                g_mqtt_client    = client
                g_mqtt_connected = True
                log.info("MQTT connected: %s:%d", host, port)
                await broadcast_status()

                # Restore active subscription after reconnect
                if g_live_subject:
                    await client.subscribe(g_live_subject)
                    log.info("MQTT subscription restored: %s", g_live_subject)

                async for message in client.messages:
                    await _handle_mqtt_message(
                        str(message.topic), bytes(message.payload)
                    )

        except aiomqtt.MqttError as exc:
            g_mqtt_client    = None
            g_mqtt_connected = False
            log.warning("MQTT error: %s — retrying in 5s", exc)
            await broadcast_status()
            await asyncio.sleep(5)
        except Exception as exc:
            g_mqtt_client    = None
            g_mqtt_connected = False
            log.warning("MQTT unexpected error: %s — retrying in 5s", exc)
            await broadcast_status()
            await asyncio.sleep(5)


async def _handle_mqtt_message(topic: str, payload: bytes) -> None:
    global g_stats, g_live_window
    now = time.monotonic()
    g_live_window = [t for t in g_live_window if now - t < 1.0]
    g_live_window.append(now)
    g_stats["live_total"]  += 1
    g_stats["live_per_sec"] = len(g_live_window)
    try:
        raw = json.loads(payload)
        flat = {k: (v["value"] if isinstance(v, dict) and "value" in v else v)
                for k, v in raw.items()}
    except Exception:
        flat = {"raw": payload.decode(errors="replace")}
    g_live_buffer.append(flat)
    await broadcast({"type": "live", "subject": topic, "payload": flat})


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
            mqtt_connect_loop(),
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
