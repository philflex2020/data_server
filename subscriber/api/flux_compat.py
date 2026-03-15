"""
Minimal InfluxDB2 Flux-compatible HTTP shim for the Subscriber API.

Exposes POST /api/v2/query on a configurable port (default 8768) so that
orig_aws.html can point at the Subscriber API instead of InfluxDB2 with no
browser-side code changes — just swap the port.

Supports the three Flux patterns issued by orig_aws.html:

  1. queryTimeSeries  — range + tag filters + aggregateWindow(fn:mean)
     Returns one CSV section per field (_field, _value, _time).

  2. queryHeatmap     — range + tag filters + _field=="voltage" + last()
     Returns latest voltage per (module_id, cell_id).

  3. queryWriteRate   — range + _field=="voltage" + count() + sum()
     Returns a single _value = row count.

Also serves:
  GET  /ping            → 204  (connection probe)
  GET  /health          → 204
  OPTIONS *             → CORS preflight
"""

import asyncio
import json as _json
import logging
import re
import time
import urllib.request
from datetime import datetime, timezone

log = logging.getLogger(__name__)

FIELD_COLS = ["voltage", "temperature", "soc", "current", "internal_resistance"]

# Shared state injected by server.py for /diag
_server_state: dict = {}

def update_server_state(d: dict) -> None:
    _server_state.update(d)


# ---------------------------------------------------------------------------
# Flux parser helpers
# ---------------------------------------------------------------------------

def _ts_to_rfc3339(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_range(flux: str):
    m = re.search(r"range\s*\(\s*start\s*:\s*(-\d+)([smhd])", flux)
    if not m:
        return time.time() - 3600, time.time()
    n = int(m.group(1))          # negative integer
    secs = {"s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]
    now = time.time()
    return now + n * secs, now


def _parse_filters(flux: str) -> dict:
    out = {}
    for m in re.finditer(r'r\.(\w+)\s*==\s*"([^"]*)"', flux):
        out[m.group(1)] = m.group(2)
    return out


def _parse_agg_seconds(flux: str) -> int:
    m = re.search(r"every\s*:\s*(\d+)([smh])", flux)
    if not m:
        return 10
    n, unit = int(m.group(1)), m.group(2)
    return n * {"s": 1, "m": 60, "h": 3600}[unit]


def _query_type(flux: str) -> str:
    if "aggregateWindow" in flux:
        return "timeseries"
    if "last()" in flux:
        return "heatmap"
    return "writerate"


# ---------------------------------------------------------------------------
# SQL builder
# ---------------------------------------------------------------------------

def flux_to_sql(flux: str, cfg: dict):
    """
    Parse a Flux query and return (qtype, sql, from_ts, to_ts, fields).
    fields is the list of FIELD_COLS to include in the response.
    """
    from_ts, to_ts = _parse_range(flux)
    filters = _parse_filters(flux)
    qtype = _query_type(flux)

    bucket = cfg["s3"]["bucket"]
    prefix = cfg["s3"].get("prefix", "").strip("/")
    path = "s3://{}/{}**/*.parquet".format(bucket, prefix + "/" if prefix else "")
    read = "read_parquet('{}', hive_partitioning=false, union_by_name=true)".format(path)

    where = ["timestamp >= {}".format(from_ts), "timestamp <= {}".format(to_ts)]
    for col in ("site_id", "rack_id", "module_id", "cell_id"):
        if col in filters:
            where.append("CAST({} AS VARCHAR) = '{}'".format(col, filters[col]))
    where_sql = " AND ".join(where)

    if qtype == "timeseries":
        bucket_secs = _parse_agg_seconds(flux)
        agg_cols = ", ".join(
            "AVG({f}) AS {f}".format(f=f) for f in FIELD_COLS
        )
        sql = """
            SELECT
                CAST(timestamp / {b} AS BIGINT) * {b} + {b} / 2.0 AS bucket_ts,
                {cols}
            FROM {read}
            WHERE {where}
            GROUP BY CAST(timestamp / {b} AS BIGINT)
            ORDER BY bucket_ts
        """.format(b=bucket_secs, cols=agg_cols, read=read, where=where_sql)
        return qtype, sql, from_ts, to_ts, FIELD_COLS

    elif qtype == "heatmap":
        sql = """
            SELECT timestamp, module_id, cell_id, voltage
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY module_id, cell_id ORDER BY timestamp DESC
                ) AS rn
                FROM {read}
                WHERE {where}
            ) sub
            WHERE rn = 1
        """.format(read=read, where=where_sql)
        return qtype, sql, from_ts, to_ts, ["voltage"]

    else:  # writerate
        sql = "SELECT COUNT(*) AS cnt FROM {} WHERE {}".format(read, where_sql)
        return qtype, sql, from_ts, to_ts, []


# ---------------------------------------------------------------------------
# InfluxDB2 annotated CSV formatter
# ---------------------------------------------------------------------------

def to_influx_csv(qtype: str, col_names, rows, fields) -> str:
    lines = []

    if qtype == "timeseries":
        for table_idx, field in enumerate(fields):
            lines += [
                "#datatype,string,long,dateTime:RFC3339,double,string",
                "#group,false,false,false,false,true",
                "#default,_result,,,,",
                ",result,table,_time,_value,_field",
            ]
            col_map = {c: i for i, c in enumerate(col_names)}
            for row in rows:
                ts = row[col_map["bucket_ts"]]
                val = row[col_map.get(field, -1)] if field in col_map else None
                if val is None:
                    continue
                t_str = _ts_to_rfc3339(float(ts))
                lines.append(",,{},{},{},{}".format(table_idx, t_str, round(float(val), 6), field))
            lines.append("")  # blank line between tables

    elif qtype == "heatmap":
        lines += [
            "#datatype,string,long,dateTime:RFC3339,double,string,string",
            "#group,false,false,false,false,true,true",
            "#default,_result,,,,,",
            ",result,table,_time,_value,module_id,cell_id",
        ]
        col_map = {c: i for i, c in enumerate(col_names)}
        for row in rows:
            ts   = float(row[col_map["timestamp"]])
            val  = row[col_map["voltage"]]
            mod  = str(int(float(row[col_map["module_id"]])))
            cell = str(int(float(row[col_map["cell_id"]])))
            t_str = _ts_to_rfc3339(ts)
            lines.append(",,0,{},{},{},{}".format(t_str, round(float(val), 6), mod, cell))
        lines.append("")

    else:  # writerate
        lines += [
            "#datatype,string,long,long",
            "#group,false,false,false",
            "#default,_result,,",
            ",result,table,_value",
        ]
        cnt = rows[0][0] if rows else 0
        lines.append(",,0,{}".format(int(cnt)))
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Synchronous query runner (called in thread executor)
# ---------------------------------------------------------------------------

def run_flux_query(flux: str, cfg: dict, duckdb_conn) -> str:
    qtype, sql, from_ts, to_ts, fields = flux_to_sql(flux, cfg)
    t0 = time.monotonic()
    cur = duckdb_conn.execute(sql)
    col_names = [d[0] for d in cur.description]
    rows = cur.fetchall()
    elapsed = round((time.monotonic() - t0) * 1000, 1)
    log.info("Flux %s  rows=%d  %.1fms", qtype, len(rows), elapsed)
    return to_influx_csv(qtype, col_names, rows, fields)


# ---------------------------------------------------------------------------
# Diagnostic helpers (sync — run in executor)
# ---------------------------------------------------------------------------

def _nats_diag(mon_url: str) -> dict:
    """Fetch NATS JetStream info from the monitoring port. Sync — run in executor."""
    out: dict = {}
    try:
        url = mon_url.rstrip("/") + "/jsz?streams=true&consumers=true&config=true&state=true"
        with urllib.request.urlopen(url, timeout=3) as resp:
            data = _json.loads(resp.read())
        streams = data.get("streams") or []
        if streams:
            s = streams[0]
            state  = s.get("state",  {})
            config = s.get("config", {})
            consumers = s.get("consumer_detail") or []
            pending = sum(c.get("num_pending", 0) for c in consumers)
            max_bytes = config.get("max_bytes", -1)
            cur_bytes = state.get("bytes", 0)
            out = {
                "stream":           s.get("name"),
                "messages":         state.get("messages", 0),
                "bytes":            cur_bytes,
                "consumer_pending": pending,
                "num_consumers":    len(consumers),
                "max_age_hours":    round(config.get("max_age", 0) / 1e9 / 3600, 1),
                "max_bytes":        max_bytes,
                "fill_pct":         round(cur_bytes / max_bytes * 100, 1) if max_bytes > 0 else None,
            }
        else:
            out["error"] = "no streams found"
    except Exception as exc:
        out["error"] = str(exc)
    return out


def _s3_diag(cfg: dict) -> dict:
    """List recent S3/MinIO objects for diagnostics. Sync — run in executor."""
    import boto3
    s3_cfg = cfg["s3"]
    bucket = s3_cfg["bucket"]
    prefix = s3_cfg.get("prefix", "").strip("/")
    kwargs = dict(
        region_name           = s3_cfg.get("region", "us-east-1"),
        aws_access_key_id     = s3_cfg.get("access_key"),
        aws_secret_access_key = s3_cfg.get("secret_key"),
    )
    if s3_cfg.get("endpoint_url"):
        kwargs["endpoint_url"] = s3_cfg["endpoint_url"]

    out: dict = {"bucket": bucket, "last_file": None, "last_modified": None,
                 "total_files": 0, "total_bytes": 0, "write_interval_sec": None}
    try:
        client    = boto3.client("s3", **kwargs)
        paginator = client.get_paginator("list_objects_v2")
        objects: list = []
        for page in paginator.paginate(Bucket=bucket,
                                       Prefix=prefix + "/" if prefix else "",
                                       PaginationConfig={"MaxItems": 2000}):
            objects.extend(page.get("Contents", []))
        if objects:
            objects.sort(key=lambda o: o["LastModified"])
            last = objects[-1]
            out["last_file"]    = last["Key"]
            out["last_modified"] = last["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")
            out["total_files"]  = len(objects)
            out["total_bytes"]  = sum(o["Size"] for o in objects)
            recent = objects[-10:]
            spans = []
            for i in range(1, len(recent)):
                dt = (recent[i]["LastModified"] - recent[i-1]["LastModified"]).total_seconds()
                if dt > 0:
                    spans.append(dt)
            if spans:
                out["write_interval_sec"] = round(sum(spans) / len(spans), 1)
    except Exception as exc:
        out["error"] = str(exc)
    return out


# ---------------------------------------------------------------------------
# Asyncio HTTP server
# ---------------------------------------------------------------------------

def _http_response(writer, status: int, extra_headers: list, body: bytes):
    reason = {200: "OK", 204: "No Content", 404: "Not Found",
              405: "Method Not Allowed"}.get(status, "OK")
    lines = ["HTTP/1.1 {} {}\r\n".format(status, reason)]
    for k, v in extra_headers:
        lines.append("{}: {}\r\n".format(k, v))
    lines.append("Content-Length: {}\r\n".format(len(body)))
    lines.append("\r\n")
    writer.write("".join(lines).encode())
    if body:
        writer.write(body)


async def _handle(reader, writer, cfg, duckdb_conn):
    try:
        req_line = (await reader.readline()).decode(errors="replace").strip()
        if not req_line:
            return
        parts = req_line.split()
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]

        headers = {}
        while True:
            line = (await reader.readline()).decode(errors="replace").strip()
            if not line:
                break
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        body = b""
        if "content-length" in headers:
            body = await reader.read(int(headers["content-length"]))

        origin = headers.get("origin", "*")
        cors = [
            ("Access-Control-Allow-Origin", origin),
            ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
            ("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept"),
        ]

        if method == "OPTIONS":
            _http_response(writer, 204, cors, b"")
            return

        if path.startswith("/diag"):
            loop = asyncio.get_running_loop()
            mon_url = cfg.get("nats", {}).get("monitoring_url", "http://localhost:8222")
            nats_info, s3_info = await asyncio.gather(
                loop.run_in_executor(None, _nats_diag, mon_url),
                loop.run_in_executor(None, _s3_diag, cfg),
            )
            diag = {
                "timestamp": time.time(),
                "nats":      nats_info,
                "s3":        s3_info,
                "subscriber": dict(_server_state),
            }
            body = _json.dumps(diag, default=str).encode()
            _http_response(writer, 200,
                           cors + [("Content-Type", "application/json")],
                           body)
            return

        if path.startswith("/ping") or path.startswith("/health"):
            _http_response(writer, 204, cors, b"")
            return

        if method == "POST" and "/api/v2/query" in path:
            flux = body.decode(errors="replace")
            try:
                loop = asyncio.get_running_loop()
                csv_text = await loop.run_in_executor(
                    None, run_flux_query, flux, cfg, duckdb_conn
                )
                _http_response(writer, 200,
                               cors + [("Content-Type", "application/csv; charset=utf-8")],
                               csv_text.encode())
            except Exception as exc:
                log.error("Flux query error: %s", exc)
                _http_response(writer, 500, cors, str(exc).encode())
            return

        _http_response(writer, 404, cors, b"Not Found")

    except Exception as exc:
        log.warning("HTTP handler error: %s", exc)
    finally:
        try:
            await writer.drain()
            writer.close()
        except Exception:
            pass


async def serve_flux_api(cfg: dict, duckdb_conn) -> None:
    host = cfg.get("websocket", {}).get("host", "0.0.0.0")
    port = cfg.get("flux_http", {}).get("port", 8768)

    async def handler(reader, writer):
        await _handle(reader, writer, cfg, duckdb_conn)

    server = await asyncio.start_server(handler, host, port)
    log.info("Flux-compat HTTP API on http://%s:%d  (point orig_aws.html here)", host, port)
    async with server:
        await server.serve_forever()
