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
import os
import re
import subprocess
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
# Query router state  (mode: influx | duckdb | both)
# ---------------------------------------------------------------------------
_router: dict = {
    "mode":           "influx",   # current routing mode
    "influx_count":   0,
    "influx_last_ms": 0.0,
    "influx_avg_ms":  0.0,
    "duckdb_count":   0,
    "duckdb_last_ms": 0.0,
    "duckdb_avg_ms":  0.0,
    "gap_s":          None,       # newest influx ts − newest duckdb ts
    "gap_updated":    None,       # unix ts of last gap measurement
}

def set_route_mode(mode: str) -> dict:
    if mode not in ("influx", "duckdb", "both"):
        return {"ok": False, "msg": f"unknown mode: {mode}"}
    _router["mode"] = mode
    log.info("Query route mode → %s", mode)
    return {"ok": True, "mode": mode}

def _update_path_stats(path: str, elapsed_ms: float) -> None:
    _router[f"{path}_count"] += 1
    n = _router[f"{path}_count"]
    _router[f"{path}_last_ms"] = elapsed_ms
    _router[f"{path}_avg_ms"]  = round(
        _router[f"{path}_avg_ms"] * (n - 1) / n + elapsed_ms / n, 1)

def _flux_count_from_csv(csv_text: str) -> int:
    """Extract the integer count from a Flux count()|sum() CSV response.

    The annotated CSV looks like:
      #group,false,false,true
      #datatype,string,long,long
      #default,_result,,
      ,result,table,_value
      ,_result,0,92160         ← we want the last column of the last data row
    """
    last_val = None
    for line in csv_text.splitlines():
        if not line or line.startswith("#") or line.startswith(",result"):
            continue
        parts = line.split(",")
        # Data rows start with an empty first field; take the last column
        if parts and parts[0] == "":
            try:
                last_val = int(parts[-1].strip())
            except (ValueError, IndexError):
                pass
    return last_val if last_val is not None else 0


def _latest_ts_from_csv(csv_text: str) -> float:
    """Extract the newest _time value from an InfluxDB CSV response."""
    from datetime import datetime, timezone as _tz
    _fmts = ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ")
    latest = 0.0
    for line in csv_text.splitlines():
        if not line or line.startswith("#") or line.startswith(",result"):
            continue
        for val in line.split(",")[3:7]:
            val = val.strip()
            if "T" not in val or not val.endswith("Z"):
                continue
            for fmt in _fmts:
                try:
                    ts = datetime.strptime(val, fmt).replace(tzinfo=_tz.utc).timestamp()
                    if ts > latest:
                        latest = ts
                    break
                except ValueError:
                    pass
    return latest

# ---------------------------------------------------------------------------
# rsync control state
# ---------------------------------------------------------------------------
_rsync_state: dict = {
    "running":      False,
    "start_time":   None,   # unix ts when loop started
    "elapsed_s":    0,
    "run_count":    0,      # number of rsync invocations completed
    "last_ms":      0.0,    # duration of last rsync run
    "file_count":   0,      # files in dest after last run
    "total_mb":     0.0,    # size of dest after last run
    "interval_s":   5,      # configured sweep interval (set by start_rsync)
}
_rsync_task: asyncio.Task = None  # background task handle

def get_rsync_stats() -> dict:
    s = dict(_rsync_state)
    if s["running"] and s["start_time"]:
        s["elapsed_s"] = round(time.time() - s["start_time"])
    return s

async def _rsync_loop(src: str, dst: str, interval: int) -> None:
    log.info("rsync loop started: %s → %s every %ds", src, dst, interval)
    while True:
        t0 = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                "rsync", "-a", "--delete", src, dst,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            await proc.wait()
        except Exception as exc:
            log.error("rsync error: %s", exc)
        _rsync_state["last_ms"] = round((time.monotonic() - t0) * 1000, 0)
        _rsync_state["run_count"] += 1
        # count files + size in dest
        total_files, total_bytes = 0, 0
        for dirpath, _, filenames in os.walk(dst):
            for fn in filenames:
                if fn.endswith(".parquet"):
                    try:
                        st = os.stat(os.path.join(dirpath, fn))
                        total_files += 1
                        total_bytes += st.st_size
                    except OSError:
                        pass
        _rsync_state["file_count"] = total_files
        _rsync_state["total_mb"]   = round(total_bytes / 1_048_576, 1)
        await asyncio.sleep(interval)

def start_rsync(src: str, dst: str, interval: int = 5) -> dict:
    global _rsync_task
    if _rsync_state["running"]:
        return {"ok": False, "msg": "already running"}
    _rsync_state.update({"running": True, "start_time": time.time(),
                          "elapsed_s": 0, "run_count": 0, "interval_s": interval})
    _rsync_task = asyncio.get_event_loop().create_task(_rsync_loop(src, dst, interval))
    log.info("rsync started")
    return {"ok": True, "msg": "started"}

def stop_rsync() -> dict:
    global _rsync_task
    if not _rsync_state["running"]:
        return {"ok": False, "msg": "not running"}
    if _rsync_task:
        _rsync_task.cancel()
        _rsync_task = None
    _rsync_state["running"] = False
    log.info("rsync stopped after %d runs", _rsync_state["run_count"])
    return {"ok": True, "msg": "stopped"}


# ---------------------------------------------------------------------------
# S3 sync state (boto3 upload to MinIO / real S3)
# ---------------------------------------------------------------------------
_s3sync_state: dict = {
    "running":        False,
    "start_time":     None,
    "elapsed_s":      0,
    "run_count":      0,
    "files_uploaded": 0,
    "bytes_uploaded": 0,
    "last_ms":        0.0,
    "last_error":     None,
}
_s3sync_task: asyncio.Task = None

def get_s3sync_stats() -> dict:
    s = dict(_s3sync_state)
    if s["running"] and s["start_time"]:
        s["elapsed_s"] = round(time.time() - s["start_time"])
    return s

def _s3_client(s3_cfg: dict):
    import boto3 as _boto3
    kwargs = dict(
        region_name           = s3_cfg.get("region", "us-east-1"),
        aws_access_key_id     = s3_cfg.get("access_key", "minioadmin"),
        aws_secret_access_key = s3_cfg.get("secret_key", "minioadmin"),
    )
    if s3_cfg.get("endpoint_url"):
        kwargs["endpoint_url"] = s3_cfg["endpoint_url"]
    return _boto3.client("s3", **kwargs)

async def _s3sync_loop(src: str, s3_cfg: dict, interval: int) -> None:
    bucket  = s3_cfg["bucket"]
    prefix  = s3_cfg.get("prefix", "").strip("/")
    log.info("S3 sync loop started: %s → s3://%s/%s every %ds", src, bucket, prefix, interval)
    while True:
        t0 = time.monotonic()
        uploaded = 0
        uploaded_bytes = 0
        error = None
        try:
            client = _s3_client(s3_cfg)
            # Build set of existing S3 keys → etag for change detection
            existing = {}
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
                for obj in page.get("Contents", []):
                    existing[obj["Key"]] = obj["ETag"].strip('"')
            # Walk local src, upload new or changed files
            for dirpath, _, filenames in os.walk(src):
                for fn in filenames:
                    if not fn.endswith(".parquet"):
                        continue
                    local_fp = os.path.join(dirpath, fn)
                    rel      = os.path.relpath(local_fp, src)
                    s3_key   = "{}/{}".format(prefix, rel).lstrip("/") if prefix else rel
                    try:
                        st = os.stat(local_fp)
                        if s3_key not in existing:
                            client.upload_file(local_fp, bucket, s3_key)
                            uploaded += 1
                            uploaded_bytes += st.st_size
                    except OSError:
                        pass
        except Exception as exc:
            error = str(exc)
            log.error("S3 sync error: %s", exc)
        elapsed = round((time.monotonic() - t0) * 1000, 0)
        _s3sync_state["run_count"]      += 1
        _s3sync_state["files_uploaded"] += uploaded
        _s3sync_state["bytes_uploaded"] += uploaded_bytes
        _s3sync_state["last_ms"]         = elapsed
        _s3sync_state["last_error"]      = error
        if uploaded:
            log.info("S3 sync: +%d files (+%.1f MB) in %.0fms",
                     uploaded, uploaded_bytes / 1_048_576, elapsed)
        await asyncio.sleep(interval)

def start_s3sync(src: str, s3_cfg: dict, interval: int = 10) -> dict:
    global _s3sync_task
    if _s3sync_state["running"]:
        return {"ok": False, "msg": "already running"}
    _s3sync_state.update({"running": True, "start_time": time.time(),
                           "elapsed_s": 0, "run_count": 0,
                           "files_uploaded": 0, "bytes_uploaded": 0})
    _s3sync_task = asyncio.get_event_loop().create_task(_s3sync_loop(src, s3_cfg, interval))
    log.info("S3 sync started")
    return {"ok": True, "msg": "started"}

def stop_s3sync() -> dict:
    global _s3sync_task
    if not _s3sync_state["running"]:
        return {"ok": False, "msg": "not running"}
    if _s3sync_task:
        _s3sync_task.cancel()
        _s3sync_task = None
    _s3sync_state["running"] = False
    log.info("S3 sync stopped after %d runs, %d files uploaded",
             _s3sync_state["run_count"], _s3sync_state["files_uploaded"])
    return {"ok": True, "msg": "stopped"}


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
    """Build the FROM clause: local path or S3, with optional gap-fill UNION.

    TODO performance: scope the parquet glob to the query date range instead of
    reading all files. flux_to_sql() already computes from_ts/to_ts from range().
    Pass those in and build date-scoped paths like:
      YYYY/MM/DD/*.parquet  for each date in [from_ts, to_ts]
    This avoids a full table scan on large stores and is critical for production
    where months of data exist. At current scale (~1k files, 1 GB) the glob is
    fast enough, but at >10k files query latency will grow linearly without this.
    """
    local_cfg = cfg.get("local") or {}
    s3_cfg    = cfg.get("s3")    or {}

    if local_cfg.get("path"):
        # Local filesystem mode (fractal-phil / parquet-aws-sim)
        base         = local_cfg["path"].rstrip("/") + "/"
        primary_path = "{}project=*/**/*.parquet".format(base)
        primary_read = "read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(primary_path)
        if local_cfg.get("gap_fill_path") and gap_exists:
            gap_base = local_cfg["gap_fill_path"].rstrip("/") + "/"
            gap_path = "{}project=*/**/*.parquet".format(gap_base)
            gap_read = "read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(gap_path)
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
        # No outer parens — DuckDB rejects (read_parquet(...)) alias without SELECT
        return "{} _src_data".format(primary_read)
    else:
        # S3 / MinIO mode
        bucket     = s3_cfg["bucket"]
        prefix     = s3_cfg.get("prefix", "").strip("/")
        gap_prefix = s3_cfg.get("gap_fill_prefix", "").strip("/")
        base     = "s3://{}/{}".format(bucket, prefix + "/" if prefix else "")
        gap_base = "s3://{}/{}".format(bucket, gap_prefix + "/" if gap_prefix else "")
        primary_path = "{}project=*/**/*.parquet".format(base)
        primary_read = "read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(primary_path)
        if gap_prefix and gap_exists:
            gap_path = "{}project=*/**/*.parquet".format(gap_base)
            gap_read = "read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(gap_path)
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
        return "{} _src_data".format(primary_read)


def flux_to_sql(flux: str, cfg: dict):
    """
    Parse a Flux query and return (qtype, sql, from_ts, to_ts, fields).
    fields is the list of FIELD_COLS to include in the response.
    """
    from_ts, to_ts = _parse_range(flux)
    filters = _parse_filters(flux)
    qtype = _query_type(flux)

    gap_prefix = (cfg.get("s3") or {}).get("gap_fill_prefix", "").strip("/")
    gap_exists = (bool(gap_prefix) or bool((cfg.get("local") or {}).get("gap_fill_path"))) \
                 and _gap_fill_exists_flux(cfg)
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
    gap_prefix = (cfg.get("s3") or {}).get("gap_fill_prefix", "").strip("/")
    if not gap_prefix:
        return False
    try:
        import boto3
        s3_cfg = cfg.get("s3") or {}
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
# Telegraf control helpers (sync — run in executor)
# ---------------------------------------------------------------------------

def _telegraf_stats(influx_cfg: dict, tg_cfg: dict) -> dict:
    """Query InfluxDB internal_write metrics to get telegraf buffer stats."""
    host  = influx_cfg.get("host", "localhost")
    port  = influx_cfg.get("port", 8086)
    org   = tg_cfg.get("org", "battery-org")
    token = tg_cfg.get("token", "battery-token-secret")
    flux = (
        'from(bucket:"battery-data")'
        ' |> range(start: -2m)'
        ' |> filter(fn: (r) => r._measurement == "internal_write" and r.output == "influxdb_v2")'
        ' |> filter(fn: (r) => r._field == "buffer_limit" or r._field == "buffer_size"'
        '   or r._field == "metrics_dropped" or r._field == "metrics_written")'
        ' |> last()'
    )
    req = urllib.request.Request(
        f"http://{host}:{port}/api/v2/query?org={org}",
        data=flux.encode(),
        headers={"Authorization": f"Token {token}",
                 "Content-Type": "application/vnd.flux"},
        method="POST",
    )
    out: dict = {"buffer_limit": None, "buffer_size": None,
                 "metrics_dropped": None, "metrics_written": None}
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            for line in resp.read().decode().splitlines():
                if not line or line.startswith("#") or ",result," in line:
                    continue
                parts = line.split(",")
                if len(parts) < 8:
                    continue
                # CSV cols: ,result,table,_start,_stop,_time,_value,_field,...
                value = parts[6]
                field = parts[7]
                if field in out:
                    try:
                        out[field] = int(float(value))
                    except ValueError:
                        pass
    except Exception as exc:
        out["error"] = str(exc)
    # compute fill_pct
    if out["buffer_limit"] and out["buffer_size"] is not None:
        out["fill_pct"] = round(out["buffer_size"] / out["buffer_limit"] * 100, 1)
    else:
        out["fill_pct"] = None
    return out


def _telegraf_set_buffer(conf_path: str, binary: str, new_limit: int) -> dict:
    """Update metric_buffer_limit in telegraf conf and restart telegraf."""
    import re
    # Read + patch config
    with open(conf_path) as f:
        content = f.read()
    content = re.sub(
        r'(metric_buffer_limit\s*=\s*)\d+[^\n]*',
        f'\\g<1>{new_limit}    # set via parquet_monitor',
        content,
    )
    with open(conf_path, "w") as f:
        f.write(content)
    # Kill existing instance
    try:
        subprocess.run(["pkill", "-f", "telegraf.*telegraf-aws-sim"], timeout=5)
    except Exception:
        pass
    time.sleep(1.5)
    # Restart
    proc = subprocess.Popen(
        [binary, "--config", conf_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    log.info("telegraf restarted (pid=%d) with buffer_limit=%d", proc.pid, new_limit)
    return {"ok": True, "limit": new_limit, "telegraf_pid": proc.pid}


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
            flux       = body.decode(errors="replace")
            influx_cfg = cfg.get("influx", {})
            mode       = _router["mode"]
            qs         = "?" + path.split("?", 1)[1] if "?" in path else ""
            fwd_hdrs   = {k: v for k, v in headers.items()
                          if k.lower() in ("authorization", "content-type", "accept")}
            loop       = asyncio.get_running_loop()

            def _influx_query():
                influx_host = influx_cfg.get("host", "localhost")
                influx_port = influx_cfg.get("port", 8086)
                req = urllib.request.Request(
                    f"http://{influx_host}:{influx_port}/api/v2/query{qs}",
                    data=flux.encode(), headers=fwd_hdrs, method="POST")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return resp.status, resp.read().decode(errors="replace")

            def _duckdb_query():
                return 200, run_flux_query(flux, cfg, duckdb_conn)

            try:
                if mode == "influx":
                    t0 = time.monotonic()
                    status, csv = await loop.run_in_executor(None, _influx_query)
                    _update_path_stats("influx", round((time.monotonic() - t0) * 1000, 1))
                    _http_response(writer, status,
                                   cors + [("Content-Type", "application/csv; charset=utf-8")],
                                   csv.encode())

                elif mode == "duckdb":
                    t0 = time.monotonic()
                    _, csv = await loop.run_in_executor(None, _duckdb_query)
                    _update_path_stats("duckdb", round((time.monotonic() - t0) * 1000, 1))
                    _http_response(writer, 200,
                                   cors + [("Content-Type", "application/csv; charset=utf-8")],
                                   csv.encode())

                elif mode == "both":
                    # Run influx query + DuckDB MAX(timestamp) in parallel.
                    # gap_s = time.time() - duckdb_max_ts  (data age, always ≥ 0)
                    # Using wall-clock rather than InfluxDB _stop avoids negative gaps
                    # caused by the race between InfluxDB's _stop timestamp and new
                    # parquet files arriving mid-query.
                    local_path = (cfg.get("local") or {}).get("path", "")
                    def _duckdb_max_ts():
                        if not local_path:
                            return None
                        pq = "{}/project=*/**/*.parquet".format(local_path.rstrip("/"))
                        try:
                            cur = duckdb_conn.execute(
                                "SELECT MAX(timestamp) FROM read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(pq)
                            )
                            row = cur.fetchone()
                            return float(row[0]) if row and row[0] else None
                        except Exception:
                            return None
                    t_query = time.time()
                    t0 = time.monotonic()
                    (i_status, i_csv), d_ts = await asyncio.gather(
                        loop.run_in_executor(None, _influx_query),
                        loop.run_in_executor(None, _duckdb_max_ts),
                    )
                    elapsed = round((time.monotonic() - t0) * 1000, 1)
                    _update_path_stats("influx", elapsed)
                    _update_path_stats("duckdb", elapsed)
                    if d_ts:
                        _router["gap_s"]      = round(t_query - d_ts, 1)
                        _router["gap_updated"] = time.time()
                        log.info("Gap: %.1fs  (now %.0f  duckdb %.0f)", _router["gap_s"], t_query, d_ts)
                    _http_response(writer, i_status,
                                   cors + [("Content-Type", "application/csv; charset=utf-8")],
                                   i_csv.encode())

                flux_stats["flux_queries_run"] += 1
                n = flux_stats["flux_queries_run"]
                last_ms = _router.get("influx_last_ms") or _router.get("duckdb_last_ms") or 0
                flux_stats["flux_last_ms"] = last_ms
                flux_stats["flux_avg_ms"]  = round(
                    flux_stats["flux_avg_ms"] * (n - 1) / n + last_ms / n, 1)
            except Exception as exc:
                log.error("Flux query error: %s", exc)
                _http_response(writer, 500, cors, str(exc).encode())
            return

        if path.startswith("/latest_ts"):
            # Fast gap probe: MAX(timestamp) from DuckDB parquet store
            local_path = (cfg.get("local") or {}).get("path", "")
            def _max_ts():
                if not local_path:
                    return None
                pq = "{}/project=*/**/*.parquet".format(local_path.rstrip("/"))
                try:
                    cur = duckdb_conn.execute(
                        "SELECT MAX(timestamp) FROM read_parquet('{}', hive_partitioning=true, union_by_name=true)".format(pq)
                    )
                    row = cur.fetchone()
                    return float(row[0]) if row and row[0] else None
                except Exception as exc:
                    log.warning("latest_ts query failed: %s", exc)
                    return None
            loop = asyncio.get_running_loop()
            max_ts = await loop.run_in_executor(None, _max_ts)
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps({"max_ts": max_ts, "now": time.time()}).encode())
            return

        if path.startswith("/route/"):
            mode = path.split("/route/", 1)[1].split("?")[0].strip()
            result = set_route_mode(mode)
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(result).encode())
            return

        if path.startswith("/route"):
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps({"mode": _router["mode"], **_router}).encode())
            return

        if path.startswith("/s3sync/start"):
            src     = cfg.get("rsync", {}).get("src", "/srv/data/parquet/")
            s3_cfg  = cfg.get("s3") or {}
            ivl     = int(cfg.get("s3", {}).get("sync_interval", 10))
            if not s3_cfg.get("bucket"):
                _http_response(writer, 400, cors,
                               b'{"ok":false,"msg":"no s3 config in config.yaml"}')
                return
            result = start_s3sync(src, s3_cfg, ivl)
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(result).encode())
            return

        if path.startswith("/s3sync/stop"):
            result = stop_s3sync()
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(result).encode())
            return

        if path.startswith("/s3sync/status"):
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(get_s3sync_stats()).encode())
            return

        if path.startswith("/rsync/start"):
            src = cfg.get("rsync", {}).get("src", "/srv/data/parquet/")
            dst = cfg.get("rsync", {}).get("dst", "/srv/data/parquet-aws-sim/")
            ivl = int(cfg.get("rsync", {}).get("interval", 5))
            result = start_rsync(src, dst, ivl)
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(result).encode())
            return

        if path.startswith("/rsync/stop"):
            result = stop_rsync()
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(result).encode())
            return

        if path.startswith("/rsync/status"):
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(get_rsync_stats()).encode())
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

        if path.startswith("/telegraf/stats"):
            influx_cfg = cfg.get("influx", {})
            tg_cfg     = cfg.get("telegraf", {})
            loop = asyncio.get_running_loop()
            stats = await loop.run_in_executor(None, _telegraf_stats, influx_cfg, tg_cfg)
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(stats).encode())
            return

        if path.startswith("/telegraf/set_buffer"):
            tg_cfg    = cfg.get("telegraf", {})
            conf_path = tg_cfg.get("conf_path", "")
            binary    = tg_cfg.get("binary", "/usr/bin/telegraf")
            try:
                body_json = _json.loads(body or b"{}")
                new_limit = int(body_json.get("limit", 0))
                if new_limit < 100 or new_limit > 1_000_000:
                    raise ValueError("limit out of range")
            except Exception as exc:
                _http_response(writer, 400, cors, str(exc).encode())
                return
            loop   = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None, _telegraf_set_buffer, conf_path, binary, new_limit)
            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps(result).encode())
            return

        if path.startswith("/compare"):
            # Run an identical time-range COUNT query against both InfluxDB and DuckDB,
            # then return the comparison as JSON so the monitor can highlight mismatches.
            #
            # Query params:
            #   window  — window length in seconds (default 120)
            #   offset  — how far in the past the window *ends*, in seconds (default 90)
            #             Must be > flush_interval (60 s) for Parquet rows to be on disk.
            # Example: /compare?window=120&offset=90
            #          counts rows in [now-210s, now-90s]
            from urllib.parse import parse_qs as _parse_qs
            qs_raw   = path.split("?", 1)[1] if "?" in path else ""
            qp       = _parse_qs(qs_raw)
            window_s = int(qp.get("window", ["120"])[0])
            offset_s = int(qp.get("offset", ["90"])[0])

            now     = time.time()
            t_end   = now - offset_s
            t_start = t_end - window_s

            influx_cfg = cfg.get("influx", {})
            local_path = (cfg.get("local") or {}).get("path", "")

            def _influx_count():
                host   = influx_cfg.get("host", "localhost")
                port   = influx_cfg.get("port", 8086)
                token  = influx_cfg.get("token", "")
                org    = influx_cfg.get("org",   "battery-org")
                bucket = influx_cfg.get("bucket", "battery-data")
                start_rfc = datetime.fromtimestamp(t_start, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                stop_rfc  = datetime.fromtimestamp(t_end,   tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                flux_q = (
                    f'from(bucket: "{bucket}")\n'
                    f'  |> range(start: {start_rfc}, stop: {stop_rfc})\n'
                    f'  |> filter(fn: (r) => r._measurement == "battery_cell")\n'
                    f'  |> filter(fn: (r) => r._field == "value")\n'
                    f'  |> count()\n'
                    f'  |> group()\n'
                    f'  |> sum(column: "_value")\n'
                )
                try:
                    hdrs = {"Authorization": f"Token {token}",
                            "Content-Type": "application/vnd.flux"}
                    req = urllib.request.Request(
                        f"http://{host}:{port}/api/v2/query?org={org}",
                        data=flux_q.encode(), headers=hdrs, method="POST")
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        csv_text = resp.read().decode(errors="replace")
                    return {"ok": True, "count": _flux_count_from_csv(csv_text), "error": None}
                except Exception as exc:
                    return {"ok": False, "count": None, "error": str(exc)}

            def _duckdb_count():
                if not local_path:
                    return {"ok": False, "count": None, "error": "no local.path in config"}
                pq = "{}/project=*/**/*.parquet".format(local_path.rstrip("/"))
                try:
                    cur = duckdb_conn.execute(
                        "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=true, "
                        "union_by_name=true) WHERE timestamp >= ? AND timestamp < ?",
                        [pq, t_start, t_end])
                    row = cur.fetchone()
                    return {"ok": True, "count": int(row[0]) if row else 0, "error": None}
                except Exception as exc:
                    return {"ok": False, "count": None, "error": str(exc)}

            loop = asyncio.get_running_loop()
            t0 = time.monotonic()
            i_res, d_res = await asyncio.gather(
                loop.run_in_executor(None, _influx_count),
                loop.run_in_executor(None, _duckdb_count),
            )
            elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

            i_count = i_res.get("count")
            d_count = d_res.get("count")
            delta   = (d_count - i_count) if (i_count is not None and d_count is not None) else None
            match   = delta == 0

            _http_response(writer, 200, cors + [("Content-Type", "application/json")],
                           _json.dumps({
                               "t_start":    t_start,
                               "t_end":      t_end,
                               "window_s":   window_s,
                               "offset_s":   offset_s,
                               "influx":     i_res,
                               "duckdb":     d_res,
                               "match":      match,
                               "delta":      delta,   # +ve = DuckDB has more (parquet captured what influx dropped)
                               "elapsed_ms": elapsed_ms,
                               "queried_at": now,
                           }).encode())
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
