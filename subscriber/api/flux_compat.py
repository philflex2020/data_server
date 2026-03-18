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

# Flux query stats — read by server.py stats_loop
flux_stats: dict = {"flux_queries_run": 0, "flux_last_ms": 0.0, "flux_avg_ms": 0.0}


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

def _flux_source(cfg: dict, gap_exists: bool) -> str:
    """Build the FROM clause: plain read_parquet, or a UNION + dedup CTE if gap-fill files exist."""
    bucket     = cfg["s3"]["bucket"]
    prefix     = cfg["s3"].get("prefix", "").strip("/")
    gap_prefix = cfg["s3"].get("gap_fill_prefix", "").strip("/")
    base     = "s3://{}/{}".format(bucket, prefix + "/" if prefix else "")
    gap_base = "s3://{}/{}".format(bucket, gap_prefix + "/" if gap_prefix else "")

    primary_path = "{}project=*/**/*.parquet".format(base)
    primary_read = "read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(primary_path)

    if gap_prefix and gap_exists:
        gap_path = "{}project=*/**/*.parquet".format(gap_base)
        gap_read = "read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(gap_path)
        # Return a subquery alias that callers can use as a table reference.
        return (
            "(SELECT * EXCLUDE (_src, _rn) FROM ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY timestamp, site_id, rack_id, module_id, cell_id ORDER BY _src"
            "  ) AS _rn FROM ("
            "    SELECT *, 0 AS _src FROM {primary} UNION ALL"
            "    SELECT *, 1 AS _src FROM {gap}"
            "  ) _combined"
            ") _deduped WHERE _rn = 1) _src_data"
        ).format(primary=primary_read, gap=gap_read)

    return "({}) _src_data".format(primary_read)


def flux_to_sql(flux: str, cfg: dict):
    """
    Parse a Flux query and return (qtype, sql, from_ts, to_ts, fields).
    fields is the list of FIELD_COLS to include in the response.
    """
    from_ts, to_ts = _parse_range(flux)
    filters = _parse_filters(flux)
    qtype = _query_type(flux)

    gap_prefix = cfg["s3"].get("gap_fill_prefix", "").strip("/")
    gap_exists = bool(gap_prefix) and _gap_fill_exists_flux(cfg)
    source = _flux_source(cfg, gap_exists)

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
            FROM {source}
            WHERE {where}
            GROUP BY CAST(timestamp / {b} AS BIGINT)
            ORDER BY bucket_ts
        """.format(b=bucket_secs, cols=agg_cols, source=source, where=where_sql)
        return qtype, sql, from_ts, to_ts, FIELD_COLS

    elif qtype == "heatmap":
        sql = """
            SELECT timestamp, module_id, cell_id, voltage
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY module_id, cell_id ORDER BY timestamp DESC
                ) AS rn
                FROM {source}
                WHERE {where}
            ) sub
            WHERE rn = 1
        """.format(source=source, where=where_sql)
        return qtype, sql, from_ts, to_ts, ["voltage"]

    else:  # writerate
        sql = "SELECT COUNT(*) AS cnt FROM {} WHERE {}".format(source, where_sql)
        return qtype, sql, from_ts, to_ts, []


def _gap_fill_exists_flux(cfg: dict) -> bool:
    """Check whether gap-fill data exists (local dir or S3 prefix)."""
    local_gap = (cfg.get("local") or {}).get("gap_fill_path") or None
    if local_gap:
        import os
        return os.path.isdir(local_gap)
    gap_prefix = cfg["s3"].get("gap_fill_prefix", "").strip("/")
    if not gap_prefix:
        return False
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
        resp = boto3.client("s3", **kwargs).list_objects_v2(
            Bucket=s3_cfg["bucket"], Prefix=gap_prefix + "/", MaxKeys=1
        )
        return bool(resp.get("Contents"))
    except Exception:
        return False


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
    flux_stats["flux_queries_run"] += 1
    n = flux_stats["flux_queries_run"]
    flux_stats["flux_last_ms"] = elapsed
    flux_stats["flux_avg_ms"] = round(flux_stats["flux_avg_ms"] * (n - 1) / n + elapsed / n, 1)
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

        if path.startswith("/ping") or path == "/health":
            _http_response(writer, 204, cors, b"")
            return

        if path.startswith("/influx_health"):
            influx_port = cfg.get("influx", {}).get("port", 8086)
            influx_host = cfg.get("influx", {}).get("host", "localhost")
            loop = asyncio.get_running_loop()
            def _check():
                try:
                    with urllib.request.urlopen(
                        f"http://{influx_host}:{influx_port}/health", timeout=3
                    ) as resp:
                        return resp.status, resp.read()
                except Exception as exc:
                    return 503, str(exc).encode()
            status, body = await loop.run_in_executor(None, _check)
            _http_response(writer, status,
                           cors + [("Content-Type", "application/json")],
                           body)
            return

        if method == "POST" and "/api/v2/query" in path:
            flux = body.decode(errors="replace")
            influx_cfg  = cfg.get("influx", {})
            passthrough = influx_cfg.get("passthrough", False)
            t0 = time.monotonic()
            try:
                loop = asyncio.get_running_loop()
                if passthrough:
                    # Phase 1: proxy straight to InfluxDB, track stats
                    influx_host = influx_cfg.get("host", "localhost")
                    influx_port = influx_cfg.get("port", 8086)
                    qs = "?" + path.split("?", 1)[1] if "?" in path else ""
                    def _proxy():
                        import urllib.request as _ur
                        req = _ur.Request(
                            f"http://{influx_host}:{influx_port}/api/v2/query{qs}",
                            data=flux.encode(),
                            headers={k: v for k, v in headers.items()
                                     if k.lower() in ("authorization","content-type","accept")},
                            method="POST",
                        )
                        with _ur.urlopen(req, timeout=10) as resp:
                            return resp.status, resp.read()
                    status, resp_body = await loop.run_in_executor(None, _proxy)
                    elapsed = round((time.monotonic() - t0) * 1000, 1)
                    log.info("Flux proxy → InfluxDB  %.1fms", elapsed)
                    _http_response(writer, status,
                                   cors + [("Content-Type", "application/csv; charset=utf-8")],
                                   resp_body)
                else:
                    # Phase 3+: translate Flux → DuckDB SQL
                    csv_text = await loop.run_in_executor(
                        None, run_flux_query, flux, cfg, duckdb_conn
                    )
                    elapsed = round((time.monotonic() - t0) * 1000, 1)
                    _http_response(writer, 200,
                                   cors + [("Content-Type", "application/csv; charset=utf-8")],
                                   csv_text.encode())
                flux_stats["flux_queries_run"] += 1
                n = flux_stats["flux_queries_run"]
                flux_stats["flux_last_ms"] = elapsed
                flux_stats["flux_avg_ms"]  = round(
                    flux_stats["flux_avg_ms"] * (n - 1) / n + elapsed / n, 1)
            except Exception as exc:
                log.error("Flux query error: %s", exc)
                _http_response(writer, 500, cors, str(exc).encode())
            return

        if path.startswith("/parquet_stats"):
            parquet_path = cfg.get("local", {}).get("path", "/srv/data/parquet")
            def _scan():
                import os
                total_files = 0
                total_bytes = 0
                latest_mtime = 0.0
                latest_file  = ""
                try:
                    for dirpath, _, filenames in os.walk(parquet_path):
                        for fn in filenames:
                            if not fn.endswith(".parquet"):
                                continue
                            fp = os.path.join(dirpath, fn)
                            try:
                                st = os.stat(fp)
                                total_files += 1
                                total_bytes += st.st_size
                                if st.st_mtime > latest_mtime:
                                    latest_mtime = st.st_mtime
                                    latest_file  = fp
                            except OSError:
                                pass
                except OSError:
                    pass
                return {
                    "path":         parquet_path,
                    "file_count":   total_files,
                    "total_mb":     round(total_bytes / 1_048_576, 1),
                    "latest_file":  latest_file.replace(parquet_path, "").lstrip("/"),
                    "latest_age_s": round(time.time() - latest_mtime, 0) if latest_mtime else None,
                }
            loop = asyncio.get_running_loop()
            info = await loop.run_in_executor(None, _scan)
            body = _json.dumps(info).encode()
            _http_response(writer, 200,
                           cors + [("Content-Type", "application/json")],
                           body)
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
