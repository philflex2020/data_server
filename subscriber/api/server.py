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

try:
    import numpy as np
    import pandas as pd
    _NUMPY_OK = True
    _PANDAS_OK = True
except ImportError:
    _NUMPY_OK = False
    _PANDAS_OK = False

import aiomqtt
import duckdb
import websockets
import yaml

from flux_compat import serve_flux_api, update_server_state, flux_stats, get_rsync_stats, get_s3sync_stats, _router

LOG_BUFFER: collections.deque = collections.deque(maxlen=200)

class _BufferHandler(logging.Handler):
    def emit(self, record):
        LOG_BUFFER.append(self.format(record))

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_buf_handler = _BufferHandler()
_buf_handler.setFormatter(_fmt)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Numpy ring buffer
# ---------------------------------------------------------------------------

# Default schema: matches DEFAULT_CELL_DATA + cell identity fields.
# timestamp is float64 (unix epoch needs full precision).
# IDs are int32. Measurements are float32 (4 bytes, plenty of precision).
# Memory: 1×8 + 5×4 + 8×4 = 60 bytes/entry  vs  ~2 KB for a Python dict.
_BUFFER_SCHEMA_DEFAULT: dict = {
    "timestamp":   "float64",
    "project_id":  "int32",
    "site_id":     "int32",
    "rack_id":     "int32",
    "module_id":   "int32",
    "cell_id":     "int32",
    "voltage":     "float32",
    "current":     "float32",
    "temperature": "float32",
    "soc":         "float32",
    "soh":         "float32",
    "resistance":  "float32",
    "capacity":    "float32",
    "power":       "float32",
}


class _NumpyRingBuffer:
    """Pre-allocated numpy ring buffer for fixed-schema telemetry.

    ~30x more memory-efficient than a deque of Python dicts.
    Fields not in the schema are silently ignored on append; they still
    appear in live WebSocket broadcasts (which use the original flat dict).
    """

    def __init__(self, maxlen: int, schema: dict):
        self._maxlen = maxlen
        self._schema = schema
        self._ptr    = 0      # next write slot
        self._count  = 0      # valid entries (< maxlen until buffer fills)
        self._cols   = {name: np.zeros(maxlen, dtype=np.dtype(dt))
                        for name, dt in schema.items()}

    def __len__(self)  -> int:  return self._count
    def __bool__(self) -> bool: return self._count > 0

    def append(self, record: dict) -> None:
        i = self._ptr
        for name, arr in self._cols.items():
            v = record.get(name)
            try:
                arr[i] = v if v is not None else 0
            except (TypeError, ValueError):
                arr[i] = 0
        self._ptr = (self._ptr + 1) % self._maxlen
        if self._count < self._maxlen:
            self._count += 1

    def _ordered_idx(self) -> "np.ndarray":
        """Index array into _cols in chronological order (oldest → newest)."""
        if self._count < self._maxlen:
            return np.arange(self._count)
        # Buffer is full — oldest slot is at _ptr.
        return (np.arange(self._maxlen) + self._ptr) % self._maxlen

    def to_dataframe(self, from_ts: float, to_ts: float,
                     site_id: str = "", proj_id: str = "") -> "pd.DataFrame":
        """Return a pandas DataFrame of rows in [from_ts, to_ts], oldest-first.
        Filtering is fully vectorized — no Python loop."""
        if self._count == 0:
            return pd.DataFrame(columns=list(self._schema))
        idx  = self._ordered_idx()
        ts   = self._cols["timestamp"][idx]
        mask = (ts >= from_ts) & (ts <= to_ts)
        sel  = idx[mask]
        if not len(sel):
            return pd.DataFrame(columns=list(self._schema))
        df = pd.DataFrame({name: arr[sel] for name, arr in self._cols.items()})
        if site_id:
            df = df[df["site_id"].astype(str) == site_id]
        if proj_id:
            df = df[df["project_id"].astype(str) == proj_id]
        return df

    def oldest_ts(self) -> float:
        if self._count == 0:
            return 0.0
        if self._count < self._maxlen:
            return float(self._cols["timestamp"][0])
        return float(self._cols["timestamp"][self._ptr])

    def newest_ts(self) -> float:
        if self._count == 0:
            return 0.0
        last = (self._ptr - 1) % self._maxlen
        return float(self._cols["timestamp"][last])
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
# Chain-of-custody: updated when _sync messages arrive from the publisher
g_sync_stats = {
    "session_id":      "",
    "sync_seq":        0,
    "last_sync_ts":    0.0,
    "received_since":  0,    # data messages received since last _sync
    "drops_detected":  0,    # cumulative drops detected vs expected
    "total_received":  0,    # total data messages received on this MQTT connection
}
g_start_time = time.time()
# Per-second rate counter — two ints, no list allocation per message.
_rate_count: int = 0
_rate_window_start: float = 0.0
g_duckdb: Optional[duckdb.DuckDBPyConnection] = None
g_stream_first_ts: float = 0.0   # unix ts of oldest retained message in NATS stream

# In-memory ring buffer of live messages for fast recent-history queries.
# At ~3000 msgs/s (3 sites), 1_080_000 entries covers ~6 minutes. Each entry is a flat dict.
BUFFER_MAXLEN = 1_080_000
g_live_buffer: collections.deque = collections.deque(maxlen=BUFFER_MAXLEN)
# Subsample counter — incremented per live message, wraps at _broadcast_stride.
_broadcast_n: int = 0

# Live-state gap fill — optional (config: live_state.enabled).
# When enabled, buffer rows within the query window are merged into parquet
# results, covering the flush-interval gap (~60 s) with zero extra processes.
g_live_state_enabled: bool = False


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
        base = local.rstrip("/") + "/"
    else:
        bucket = cfg["s3"]["bucket"]
        prefix = cfg["s3"].get("prefix", "").strip("/")
        base   = f"s3://{bucket}/{prefix + '/' if prefix else ''}"

    primary_paths = _date_paths(base, proj_id, site_id, from_ts, to_ts)
    primary_list  = "[" + ", ".join(f"'{p}'" for p in primary_paths) + "]"
    where         = f"timestamp >= {from_ts} AND timestamp <= {to_ts}"

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
        "type":                "status",
        "mqtt_connected":      g_mqtt_connected,
        "s3_connected":        g_s3_connected,
        "subscribed":          bool(g_live_subject),
        "subject":             g_live_subject,
        "live_state_enabled":  g_live_state_enabled,
        "buffer_size":         len(g_live_buffer),
    })


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

async def ws_handler(websocket) -> None:
    global g_live_state_enabled
    g_connected.add(websocket)
    log.info("Client connected (%d total)", len(g_connected))

    await websocket.send(json.dumps({
        "type":           "status",
        "mqtt_connected": g_mqtt_connected,
        "s3_connected":   g_s3_connected,
        "subscribed":     bool(g_live_subject),
        "subject":        g_live_subject,
    }))
    await websocket.send(json.dumps({"type": "stats", **g_stats, **flux_stats,
                                     "mqtt_connected": g_mqtt_connected,
                                     "ws_clients":     len(g_connected),
                                     "live_state_enabled": g_live_state_enabled,
                                     "buffer_size":        len(g_live_buffer),
                                     "rsync": get_rsync_stats(), "s3sync": get_s3sync_stats(),
                                     "router": dict(_router), "sync": dict(g_sync_stats)}))

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
                await websocket.send(json.dumps({"type": "stats", **g_stats, **flux_stats,
                                                 "mqtt_connected": g_mqtt_connected,
                                                 "ws_clients":     len(g_connected),
                                                 "rsync": get_rsync_stats(), "s3sync": get_s3sync_stats(),
                                                 "router": dict(_router), "sync": dict(g_sync_stats)}))

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

            elif t == "set_live_state":
                g_live_state_enabled = bool(msg.get("enabled", False))
                log.info("Live-state gap fill %s via API", "ENABLED" if g_live_state_enabled else "DISABLED")
                await websocket.send(json.dumps({
                    "type": "live_state_ack", "enabled": g_live_state_enabled
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
                    buf_oldest = g_live_buffer.oldest_ts()
                    buf_newest = g_live_buffer.newest_ts()
                    has_buf    = g_live_state_enabled and bool(g_live_buffer)

                    if has_buf and from_ts >= buf_oldest:
                        # Query entirely within buffer — serve from memory
                        log.debug("Query route: buffer-only (%.0f–%.0f in buf %.0f–%.0f)",
                                  from_ts, to_ts, buf_oldest, buf_newest)
                        result = run_buffer_query(from_ts, to_ts, proj_id, site_id, limit)
                    elif has_buf and to_ts >= buf_oldest:
                        # Query spans parquet + buffer — hybrid UNION
                        log.debug("Query route: hybrid parquet+buffer (buf_oldest=%.0f)", buf_oldest)
                        result = await asyncio.get_event_loop().run_in_executor(
                            None, run_hybrid_query, g_config, proj_id, site_id,
                            from_ts, to_ts, buf_oldest, limit
                        )
                    else:
                        # Query entirely in parquet range (or buffer disabled)
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
        await broadcast({"type": "stats", **g_stats, **flux_stats,
                          "mqtt_connected":     g_mqtt_connected,
                          "ws_clients":         len(g_connected),
                          "live_state_enabled": g_live_state_enabled,
                          "buffer_size":        len(g_live_buffer),
                          "rsync": get_rsync_stats(), "s3sync": get_s3sync_stats(),
                          "router": dict(_router), "sync": dict(g_sync_stats)})
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

                # Always subscribe to sync heartbeats for chain-of-custody tracking
                await client.subscribe("_sync")
                # Always subscribe to data topic for accurate sync drop counting —
                # even when no live stream is active, messages must be counted.
                data_topic = g_config.get("mqtt", {}).get("default_topic", "batteries/#")
                await client.subscribe(data_topic)
                log.info("MQTT background subscription: %s", data_topic)
                # Restore active live-data subscription after reconnect
                if g_live_subject and g_live_subject != data_topic:
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
    global g_stats, g_sync_stats, _rate_count, _rate_window_start, _broadcast_n

    # Chain-of-custody sync message — count received vs expected, detect drops
    if topic == "_sync":
        try:
            msg = json.loads(payload)
            session = msg.get("session_id", "")
            seq     = msg.get("seq", 0)
            expect  = msg.get("interval_published", 0)

            if session != g_sync_stats["session_id"]:
                # New session (restart) — reset per-session counters
                if g_sync_stats["session_id"]:
                    log.info("Sync: new session %s (was %s) — counters reset",
                             session, g_sync_stats["session_id"])
                g_sync_stats["session_id"]     = session
                g_sync_stats["received_since"] = 0
                g_sync_stats["sync_seq"]       = seq
                g_sync_stats["last_sync_ts"]   = msg.get("timestamp", 0.0)
                return  # skip comparison for first sync of new session

            received = g_sync_stats["received_since"]
            dropped  = max(0, expect - received)
            g_sync_stats["drops_detected"] += dropped
            g_sync_stats["received_since"]  = 0
            g_sync_stats["sync_seq"]        = seq
            g_sync_stats["last_sync_ts"]    = msg.get("timestamp", 0.0)

            if dropped:
                log.warning("Sync #%d DROPS: expected=%d received=%d dropped=%d cumulative=%d",
                            seq, expect, received, dropped, g_sync_stats["drops_detected"])
            else:
                log.info("Sync #%d OK: %d/%d received", seq, received, expect)
        except Exception as e:
            log.warning("Sync parse error: %s", e)
        return

    # Regular data message — count for sync tracking (regardless of live subscription state)
    g_sync_stats["received_since"] += 1
    g_sync_stats["total_received"] += 1

    now = time.monotonic()
    _rate_count += 1
    g_stats["live_total"] += 1
    if now - _rate_window_start >= 1.0:
        g_stats["live_per_sec"] = _rate_count
        _rate_count = 0
        _rate_window_start = now
    try:
        raw = json.loads(payload)
        flat = {k: (v["value"] if isinstance(v, dict) and "value" in v else v)
                for k, v in raw.items()}
    except Exception:
        flat = {"raw": payload.decode(errors="replace")}
    g_live_buffer.append(flat)
    # Only push live messages to WebSocket clients when someone has explicitly subscribed —
    # the background subscription for sync counting should not generate unsolicited traffic.
    # Subsample to ~200 msg/s so the event loop isn't saturated at high ingest rates.
    if g_live_subject:
        rate = g_stats["live_per_sec"]
        stride = max(1, rate // 200)
        _broadcast_n = (_broadcast_n + 1) % stride
        if _broadcast_n == 0:
            await broadcast({"type": "live", "subject": topic, "payload": flat})


def run_buffer_query(from_ts: float, to_ts: float, proj_id: str, site_id: str, limit: int) -> dict:
    """Serve a time-range query from the numpy ring buffer. Fully vectorized."""
    t0 = time.monotonic()
    df = g_live_buffer.to_dataframe(from_ts, to_ts, site_id=site_id, proj_id=proj_id)
    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
    if df.empty:
        return {"columns": [], "rows": [], "total": 0, "elapsed_ms": elapsed_ms}
    df = df.sort_values("timestamp", ascending=False).head(limit)
    columns = list(df.columns)
    rows = [
        [float(v) if isinstance(v, (float, int, np.floating, np.integer)) else
         str(v) if v is not None else None
         for v in row]
        for row in df.itertuples(index=False, name=None)
    ]
    log.info("Buffer query: %d rows in %.0fms (%.0f–%.0f, site=%s)",
             len(rows), elapsed_ms, from_ts, to_ts, site_id or "*")
    return {"columns": columns, "rows": rows, "total": len(rows), "elapsed_ms": elapsed_ms}



def run_hybrid_query(cfg: dict, proj_id: str, site_id: str,
                     from_ts: float, to_ts: float,
                     buf_oldest: float, limit: int) -> dict:
    """Parquet (older portion) UNION live buffer (recent portion), parquet wins on dedup.

    Used when the query window spans both parquet history and the in-memory buffer.
    Requires pandas (pip install pandas).
    """
    if not _PANDAS_OK:
        log.warning("Hybrid query fallback: pandas not installed — using parquet only")
        return run_history_query(cfg, proj_id, site_id, from_ts, to_ts, limit)

    buf_df = g_live_buffer.to_dataframe(buf_oldest, to_ts, site_id=site_id, proj_id=proj_id)
    if buf_df.empty:
        return run_history_query(cfg, proj_id, site_id, from_ts, to_ts, limit)
    # Ensure dedup key columns exist so PARTITION BY never fails
    for col in ("timestamp", "site_id", "rack_id", "module_id", "cell_id"):
        if col not in buf_df.columns:
            buf_df[col] = None

    g_duckdb.register("_live_buffer_view", buf_df)

    local = _local_path(cfg)
    if local:
        base = local.rstrip("/") + "/"
    else:
        bucket = cfg["s3"]["bucket"]
        prefix = cfg["s3"].get("prefix", "").strip("/")
        base   = f"s3://{bucket}/{prefix + '/' if prefix else ''}"

    primary_paths = _date_paths(base, proj_id, site_id, from_ts, to_ts)
    primary_list  = "[" + ", ".join(f"'{p}'" for p in primary_paths) + "]"
    where        = f"timestamp >= {from_ts} AND timestamp <= {to_ts}"

    sql = f"""
        WITH combined AS (
            SELECT *, 0 AS _src
            FROM read_parquet({primary_list}, hive_partitioning=true, union_by_name=true)
            WHERE {where}
            UNION ALL BY NAME
            SELECT *, 1 AS _src
            FROM _live_buffer_view
            WHERE timestamp >= {from_ts} AND timestamp <= {to_ts}
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
    log.info("Hybrid query: %d rows in %.0fms (parquet+buffer, ts=%.0f–%.0f)",
             len(rows), elapsed_ms, from_ts, to_ts)
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
    global g_config, g_duckdb, g_live_state_enabled, g_live_buffer

    g_config = cfg
    g_duckdb = build_duckdb(cfg)

    buf_cfg    = cfg.get("buffer", {})
    buf_maxlen = int(buf_cfg.get("maxlen", BUFFER_MAXLEN))
    buf_schema = buf_cfg.get("schema", _BUFFER_SCHEMA_DEFAULT)
    if _NUMPY_OK:
        g_live_buffer = _NumpyRingBuffer(buf_maxlen, buf_schema)
        nbytes = buf_maxlen * sum(np.dtype(dt).itemsize for dt in buf_schema.values())
        log.info("Live buffer: numpy ring, maxlen=%d, pre-alloc=%.0f MB",
                 buf_maxlen, nbytes / 1e6)
    else:
        g_live_buffer = collections.deque(maxlen=buf_maxlen)
        log.warning("numpy not available — using dict deque buffer (higher memory)")

    ls_cfg = cfg.get("live_state", {})
    g_live_state_enabled = bool(ls_cfg.get("enabled", False))
    if g_live_state_enabled:
        log.info("Live-state gap fill ENABLED (config)")

    default_route = cfg.get("flux_http", {}).get("default_route", "influx")
    from flux_compat import set_route_mode
    set_route_mode(default_route)

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
