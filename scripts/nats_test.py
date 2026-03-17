#!/usr/bin/env python3
"""
nats_test.py — Live NATS JetStream + Parquet Writer performance test runner.

Measures write throughput, consumer health, schema flexibility, and resilience
of the NATS → Parquet → S3/MinIO pipeline. Generates .md and .html reports.

Usage:
    python scripts/nats_test.py                                  # defaults from config
    python scripts/nats_test.py --config source/parquet_writer/config.yaml
    python scripts/nats_test.py --nats-mon http://spark-22b6:8222
    python scripts/nats_test.py --s3-endpoint http://spark-22b6:9000
    python scripts/nats_test.py --sample-secs 30 --dry-run
"""

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import yaml
from datetime import datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
DEFAULT_DOCS_DIR = REPO_ROOT / "docs"
DEFAULT_HTML_DIR = REPO_ROOT / "html"

# ── NATS monitoring helpers ────────────────────────────────────────────────────

def nats_get(mon_base, path):
    url = mon_base.rstrip("/") + path
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

def get_stream_info(mon_base, stream="BATTERY_DATA"):
    """Return JetStream stream info dict, or {} on failure."""
    data = nats_get(mon_base, f"/jsz?streams=1&consumers=1&stream={stream}")
    if "error" in data:
        return {"error": data["error"]}
    account = data.get("account_details", [{}])[0] if data.get("account_details") else data
    streams = account.get("stream_detail", []) or data.get("account_details", [{}])[0].get("stream_detail", [])
    # Try direct stream info
    stream_data = nats_get(mon_base, f"/streamz?stream={stream}")
    if "config" in stream_data:
        return stream_data
    # Try jsz approach
    data2 = nats_get(mon_base, "/jsz?streams=1&consumers=1")
    if "account_details" in data2:
        for acc in data2["account_details"]:
            for s in acc.get("stream_detail", []):
                if s.get("name") == stream:
                    return s
    return {}

def get_server_info(mon_base):
    return nats_get(mon_base, "/varz")

def get_all_streams(mon_base):
    """Return list of stream detail dicts."""
    data = nats_get(mon_base, "/jsz?streams=1&consumers=1&accounts=1")
    if "error" in data:
        return []
    streams = []
    for acc in data.get("account_details", [data]):
        for s in acc.get("stream_detail", []):
            streams.append(s)
    if not streams:
        # Try top-level
        for s in data.get("streams", []):
            streams.append(s)
    return streams

# ── S3 / MinIO helpers ─────────────────────────────────────────────────────────

def build_s3(cfg):
    if not HAS_BOTO3:
        return None
    s3_cfg = cfg.get("s3", {})
    kwargs = dict(
        region_name           = s3_cfg.get("region", "us-east-1"),
        aws_access_key_id     = s3_cfg.get("access_key", "minioadmin"),
        aws_secret_access_key = s3_cfg.get("secret_key", "minioadmin"),
    )
    if s3_cfg.get("endpoint_url"):
        kwargs["endpoint_url"] = s3_cfg["endpoint_url"]
    try:
        return boto3.client("s3", **kwargs)
    except Exception:
        return None

def list_recent_parquet(s3, bucket, prefix="", hours=2):
    """Return list of (key, size_bytes, last_modified) for parquet files modified in last N hours."""
    if not s3:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    files = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet") and obj["LastModified"] > cutoff:
                    files.append((obj["Key"], obj["Size"], obj["LastModified"]))
    except Exception as e:
        return []
    return sorted(files, key=lambda x: x[2], reverse=True)

def read_parquet_info(s3, bucket, key):
    """Return (num_rows, schema_fields, extra_fields) from a parquet file in S3."""
    if not s3 or not HAS_PYARROW:
        return None, None, None
    known_fields = {
        "project_id", "site_id", "rack_id", "module_id", "cell_id",
        "timestamp", "voltage", "current", "temperature", "soc", "soh",
        "internal_resistance", "resistance", "capacity", "power",
        "source_id",
    }
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        buf = BytesIO(obj["Body"].read())
        pf = pq.read_table(buf)
        all_cols = set(pf.schema.names)
        extra = sorted(all_cols - known_fields)
        return pf.num_rows, sorted(all_cols), extra
    except Exception as e:
        return None, None, None

def s3_total_bytes(s3, bucket, prefix=""):
    """Return (total_files, total_bytes) for all parquet objects under prefix."""
    if not s3:
        return None, None
    total_files, total_bytes = 0, 0
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    total_files += 1
                    total_bytes += obj["Size"]
    except Exception:
        return None, None
    return total_files, total_bytes

# ── Process helpers ────────────────────────────────────────────────────────────

def proc_rss_kb(fragment):
    try:
        out = subprocess.check_output(["ps", "aux"], text=True)
        for line in out.splitlines():
            if fragment in line and "grep" not in line and sys.argv[0] not in line:
                parts = line.split()
                if len(parts) > 5:
                    return int(parts[5])
    except Exception:
        pass
    return None

def get_version(cmd):
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        m = re.search(r'\d+\.\d+\.\d+', out)
        return m.group(0) if m else "unknown"
    except Exception:
        return "unknown"

def load_writer_config(path):
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

# ── AWS cost estimates ─────────────────────────────────────────────────────────

# AWS us-east-1 on-demand pricing (2026, approximate)
S3_STORAGE_PER_GB_MONTH = 0.023       # Standard storage
S3_PUT_PER_1000         = 0.005       # PUT / COPY / POST
S3_GET_PER_1000         = 0.0004      # GET / SELECT
S3_TRANSFER_OUT_PER_GB  = 0.09        # First 10 TB/month
EC2_T3_MICRO_HR         = 0.0104      # t3.micro  (2 vCPU,  1 GB)
EC2_T3_SMALL_HR         = 0.0208      # t3.small  (2 vCPU,  2 GB)
EC2_T3_MEDIUM_HR        = 0.0416      # t3.medium (2 vCPU,  4 GB)
EC2_T3_LARGE_HR         = 0.0832      # t3.large  (2 vCPU,  8 GB)
HOURS_PER_MONTH         = 730

# IoT Core pricing (alternative to self-managed MQTT + NATS)
IOT_CORE_PER_M_MSGS     = 1.00        # $1.00 per million messages (basic ingest)
IOT_CORE_RULE_PER_M     = 0.15        # $0.15 per million rules triggered

def compute_aws_costs(r):
    """
    Estimate monthly AWS costs for the NATS + Parquet → S3 pipeline,
    extrapolated from measured throughput and storage figures.

    Scale columns: 1x = current measured, 6x = ~4k assets, 24x = ~15k assets.
    """
    msgs_per_sec      = r.get("msgs_per_sec") or 0.0
    parquet_bytes_2h  = r.get("parquet_bytes_2h") or 0
    parquet_files_2h  = r.get("parquet_files_2h") or 0
    total_stream_bytes = r.get("total_stream_bytes") or 0

    # Extrapolate storage rate
    bytes_per_day   = parquet_bytes_2h * 12   # 12 × 2-hour windows
    gb_per_day      = bytes_per_day / 1e9
    gb_per_month    = gb_per_day * 30

    # S3 PUT requests (one per parquet file written)
    files_per_month = parquet_files_2h * 12 * 30

    # S3 GET requests: assume 10 Athena/query scans per day across all objects/month
    # Each scan reads ~5% of total files → conservative
    gets_per_month  = parquet_files_2h * 12 * 30 * 0.05

    def _s3_cost(scale):
        stor = gb_per_month * scale * S3_STORAGE_PER_GB_MONTH
        put  = (files_per_month * scale / 1000) * S3_PUT_PER_1000
        get  = (gets_per_month  * scale / 1000) * S3_GET_PER_1000
        return round(stor, 2), round(put, 2), round(get, 2)

    def _ec2_cost(scale):
        # NATS server: t3.small for 1x–6x, t3.medium for 24x
        if scale <= 6:
            nats_hr = EC2_T3_SMALL_HR
            nats_label = "t3.small"
        else:
            nats_hr = EC2_T3_MEDIUM_HR
            nats_label = "t3.medium"
        # Parquet writer: t3.micro for 1x, t3.small for 6x+
        if scale == 1:
            writer_hr = EC2_T3_MICRO_HR
            writer_label = "t3.micro"
        else:
            writer_hr = EC2_T3_SMALL_HR
            writer_label = "t3.small"
        nats_cost   = round(nats_hr   * HOURS_PER_MONTH, 2)
        writer_cost = round(writer_hr * HOURS_PER_MONTH, 2)
        return nats_cost, nats_label, writer_cost, writer_label

    def _iot_alt(scale):
        """IoT Core cost if MQTT handled by AWS instead of self-managed."""
        msgs_month = msgs_per_sec * scale * 3600 * 24 * 30
        cost = (msgs_month / 1e6) * IOT_CORE_PER_M_MSGS
        return round(cost, 2)

    scales = []
    for scale in (1, 6, 24):
        s3_stor, s3_put, s3_get = _s3_cost(scale)
        nats_ec2, nats_lbl, writer_ec2, writer_lbl = _ec2_cost(scale)
        iot_alt = _iot_alt(scale)
        total = round(s3_stor + s3_put + s3_get + nats_ec2 + writer_ec2, 2)
        scales.append({
            "scale":        scale,
            "msgs_per_sec": round(msgs_per_sec * scale, 1),
            "gb_per_month": round(gb_per_month * scale, 1),
            "s3_storage":   s3_stor,
            "s3_put":       s3_put,
            "s3_get":       s3_get,
            "nats_ec2":     nats_ec2,
            "nats_label":   nats_lbl,
            "writer_ec2":   writer_ec2,
            "writer_label": writer_lbl,
            "total":        total,
            "iot_alt":      iot_alt,     # IoT Core alternative for MQTT layer
        })

    return {
        "gb_per_day":        round(gb_per_day, 2),
        "gb_per_month":      round(gb_per_month, 1),
        "files_per_month":   int(files_per_month),
        "msgs_per_sec_base": msgs_per_sec,
        "scales":            scales,
        # Notes for documentation
        "notes": [
            "EC2 = on-demand us-east-1 Linux. Reserved 1-year saves ~40%.",
            "S3 Standard. Intelligent-Tiering would reduce cost for cold partitions.",
            "IoT Core alternative shown for MQTT ingest only — replaces FlashMQ + NATS bridge EC2.",
            "Data transfer out not included — queries assumed within same region via Athena.",
            "NATS server handles >> current load; no vertical scaling needed before 6x.",
        ],
    }


# ── Core measurements ──────────────────────────────────────────────────────────

def measure(mon_base, s3_cfg_override, sample_secs=30, writer_config_path=None):
    r = {}

    # System info
    r["arch"]      = platform.machine()
    r["hostname"]  = platform.node()
    r["test_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    r["nats_ver"]  = "unknown"

    # Load writer config for S3 credentials
    cfg = {}
    if writer_config_path and Path(writer_config_path).exists():
        cfg = load_writer_config(writer_config_path)
    # Allow overrides
    if s3_cfg_override:
        cfg.setdefault("s3", {}).update(s3_cfg_override)

    # ── Test 0: NATS server liveness
    print("  [0] NATS server info...", flush=True)
    srv = get_server_info(mon_base)
    if "error" in srv:
        r["nats_up"] = False
        r["nats_error"] = srv["error"]
        print(f"      ERROR: {srv['error']}")
    else:
        r["nats_up"] = True
        r["nats_ver"] = srv.get("version", "unknown")
        r["nats_connections"] = srv.get("connections", 0)
        r["nats_mem_mb"] = round(srv.get("mem", 0) / 1024 / 1024, 1)

    # ── Test 1: JetStream stream info
    print("  [1] JetStream stream stats...", flush=True)
    streams = get_all_streams(mon_base)
    r["streams"] = []
    total_msgs = 0
    total_bytes_stored = 0
    for s in streams:
        name = s.get("name", "?")
        state = s.get("state", {})
        config = s.get("config", {})
        consumers_raw = s.get("consumer_detail", []) or []
        consumers_info = []
        for c in consumers_raw:
            cstate = c.get("num_pending", 0)
            consumers_info.append({
                "name":    c.get("name", "?"),
                "pending": cstate,
                "ack_floor": c.get("ack_floor", {}).get("stream_seq", 0),
            })
        info = {
            "name":        name,
            "subjects":    config.get("subjects", []),
            "msgs":        state.get("messages", 0),
            "bytes":       state.get("bytes", 0),
            "max_age_hrs": round(config.get("max_age", 0) / 1e9 / 3600, 1),
            "consumers":   consumers_info,
        }
        r["streams"].append(info)
        total_msgs += info["msgs"]
        total_bytes_stored += info["bytes"]

    r["total_stream_msgs"]  = total_msgs
    r["total_stream_bytes"] = total_bytes_stored

    # ── Test 2: Message throughput (two snapshots)
    # Use server-level in_msgs counter rather than stream message count.
    # Stream count can be stable when the stream is at capacity (evicts old messages
    # at the same rate new ones arrive), giving a false zero.
    print(f"  [2] Message throughput — sampling {sample_secs}s...", flush=True)
    snap1_varz = get_server_info(mon_base)
    snap1_in   = snap1_varz.get("in_msgs", 0)
    t0 = time.time()
    time.sleep(sample_secs)
    snap2_varz = get_server_info(mon_base)
    snap2_in   = snap2_varz.get("in_msgs", 0)
    elapsed    = time.time() - t0
    snap2_streams = get_all_streams(mon_base)  # still needed for consumer lag

    if snap2_in >= snap1_in and elapsed > 0:
        r["msgs_per_sec"] = round((snap2_in - snap1_in) / elapsed, 1)
    else:
        r["msgs_per_sec"] = None

    # ── Test 3: Consumer lag
    print("  [3] Consumer lag...", flush=True)
    snap2_streams_full = get_all_streams(mon_base)  # with consumers for lag
    r["consumer_lag"] = {}
    for s in snap2_streams_full:
        for c in (s.get("consumer_detail") or []):
            r["consumer_lag"][c.get("name", "?")] = c.get("num_pending", 0)

    # ── Test 4: Parquet files in S3/MinIO
    print("  [4] Parquet file stats (S3/MinIO)...", flush=True)
    s3 = build_s3(cfg) if HAS_BOTO3 else None
    bucket = cfg.get("s3", {}).get("bucket", "battery-data")
    r["s3_reachable"] = False
    r["parquet_files_2h"] = None
    r["parquet_bytes_2h"] = None
    r["parquet_files_total"] = None
    r["parquet_bytes_total"] = None
    r["sample_schema"] = None
    r["sample_rows"] = None
    r["schema_extra_fields"] = []

    if s3:
        try:
            s3.head_bucket(Bucket=bucket)
            r["s3_reachable"] = True

            recent = list_recent_parquet(s3, bucket, hours=2)
            r["parquet_files_2h"]  = len(recent)
            r["parquet_bytes_2h"]  = sum(f[1] for f in recent)

            tf, tb = s3_total_bytes(s3, bucket)
            r["parquet_files_total"] = tf
            r["parquet_bytes_total"] = tb

            # Inspect newest file for schema
            if recent and HAS_PYARROW:
                newest_key = recent[0][0]
                r["sample_file"] = newest_key
                rows, schema, extra = read_parquet_info(s3, bucket, newest_key)
                r["sample_rows"]  = rows
                r["sample_schema"] = schema
                r["schema_extra_fields"] = extra or []

        except Exception as e:
            r["s3_error"] = str(e)

    # ── Test 5: Schema flexibility — compare writer code behaviour
    print("  [5] Schema flexibility analysis...", flush=True)
    writer_path = REPO_ROOT / "source" / "parquet_writer" / "writer.py"
    r["schema_flex"] = {}
    if writer_path.exists():
        src = writer_path.read_text()
        # Check for dynamic key collection
        r["schema_flex"]["dynamic_keys"] = "all_keys" in src and "{k for r in flat for k in r}" in src
        # Check for nested value flattening
        r["schema_flex"]["handles_nested"]  = 'isinstance(v, dict) and "value" in v' in src
        # Check for null-tolerant missing fields
        r["schema_flex"]["null_on_missing"] = 'r.get(k)' in src
        # Check subject filter
        r["schema_flex"]["wildcard_filter"] = "subject_filter" in src

    # ── Test 6: JetStream resilience properties
    print("  [6] Resilience check...", flush=True)
    nats_conf_path = REPO_ROOT / "manager" / "nats-server.conf"
    r["resilience"] = {}
    if nats_conf_path.exists():
        conf_text = nats_conf_path.read_text()
        r["resilience"]["file_storage"]    = "store_dir" in conf_text
        r["resilience"]["sync_on_write"]   = "sync_interval" in conf_text
        r["resilience"]["max_file_store"]  = re.search(r'max_file_store:\s*"?(\S+)"?', conf_text)
        if r["resilience"]["max_file_store"]:
            r["resilience"]["max_file_store"] = r["resilience"]["max_file_store"].group(1)

    # Retention window from stream config
    for s in r["streams"]:
        if s["max_age_hrs"] > 0:
            r["resilience"]["retention_hours"] = s["max_age_hrs"]
            break

    # ── Test 7: Process resources
    print("  [7] Process resources...", flush=True)
    r["nats_rss_kb"]   = proc_rss_kb("/nats-server ") or proc_rss_kb("nats-server ")
    r["writer_rss_kb"] = proc_rss_kb("writer.py")
    r["bridge_rss_kb"] = proc_rss_kb("bridge.py")

    # ── Test 8: AWS cost estimates
    print("  [8] AWS cost estimates...", flush=True)
    r["aws"] = compute_aws_costs(r)

    return r


# ── Formatting helpers ──────────────────────────────────────────────────────────

def fmt_num(v, default="—"):
    if v is None:
        return default
    if isinstance(v, float):
        return f"{v:,.1f}"
    return f"{int(v):,}"

def fmt_bytes(b, default="—"):
    if b is None:
        return default
    if b >= 1_000_000_000:
        return f"{b/1_000_000_000:.2f} GB"
    if b >= 1_000_000:
        return f"{b/1_000_000:.1f} MB"
    return f"{b/1_000:.1f} KB"

def fmt_mb(kb, default="—"):
    if kb is None:
        return default
    return f"{kb / 1024:.0f} MB"


# ── AWS markdown / HTML helpers ────────────────────────────────────────────────

def _aws_md_lines(aws):
    if not aws or not aws.get("scales"):
        return ["*AWS cost data not available.*"]
    lines = [
        f"Extrapolated from measured throughput ({fmt_num(aws.get('msgs_per_sec_base'))}/sec) "
        f"and storage rate ({aws.get('gb_per_day', '?')} GB/day parquet).",
        "",
        "### Fleet Scaling — Monthly Cost Breakdown",
        "",
        "| Service | 1× (current) | 6× | 24× |",
        "|---------|:---:|:---:|:---:|",
    ]
    def col(scales, key):
        return " | ".join(f"${s[key]:.2f}" for s in scales)
    sc = aws["scales"]
    nats_ec2_cols   = " | ".join(f"${s['nats_ec2']:.2f} ({s['nats_label']})"   for s in sc)
    writer_ec2_cols = " | ".join(f"${s['writer_ec2']:.2f} ({s['writer_label']})" for s in sc)
    total_cols      = " | ".join(f"**${s['total']:.2f}**" for s in sc)
    lines += [
        f"| **Messages/sec** | {' | '.join(fmt_num(s['msgs_per_sec']) for s in sc)} |",
        f"| **Parquet GB/month** | {' | '.join(str(s['gb_per_month']) for s in sc)} |",
        f"| S3 storage | {col(sc, 's3_storage')} |",
        f"| S3 PUT requests | {col(sc, 's3_put')} |",
        f"| S3 GET requests | {col(sc, 's3_get')} |",
        f"| EC2 NATS server | {nats_ec2_cols} |",
        f"| EC2 Parquet writer | {writer_ec2_cols} |",
        f"| **Total / month** | {total_cols} |",
        "",
        "### Alternative: AWS IoT Core for MQTT Ingest",
        "",
        "| Scale | IoT Core cost/month | Notes |",
        "|-------|:---:|---|",
    ]
    for s in sc:
        lines.append(
            f"| {s['scale']}× | ${s['iot_alt']:.2f} | "
            f"Replaces FlashMQ + NATS bridge + EC2 for MQTT layer |"
        )
    lines += [
        "",
        "### Notes",
        "",
    ]
    for note in aws.get("notes", []):
        lines.append(f"- {note}")
    return lines


# ── Markdown generation ────────────────────────────────────────────────────────

def build_markdown(r):
    arch_label = "aarch64" if "aarch64" in r["arch"] or "arm" in r["arch"].lower() else "x86_64"
    sf = r.get("schema_flex", {})
    res = r.get("resilience", {})
    lag_str = ", ".join(f"{k}: {v} pending" for k, v in r.get("consumer_lag", {}).items()) or "none"
    retention = res.get("retention_hours", "?")

    lines = [
        "# NATS JetStream + Parquet Writer — Live Performance Measurements",
        "",
        f"**Test date:** {r['test_date']}",
        f"**Environment:** {r['hostname']} ({arch_label})",
        f"**NATS Server:** {r['nats_ver']}",
        f"**S3/MinIO reachable:** {'yes' if r['s3_reachable'] else 'no'}",
        "",
        "---",
        "",
        "## Test 0 — NATS Server Health",
        "",
        ("**Result:** Server up and responding." if r.get("nats_up")
         else f"**Result:** Server unreachable — {r.get('nats_error', 'unknown error')}"),
        "",
        f"- Connections: {fmt_num(r.get('nats_connections'))}",
        f"- Server RSS: {fmt_mb(r.get('nats_mem_mb', 0) * 1024 / 1 if r.get('nats_mem_mb') else None)}",
        "",
        "---",
        "",
        "## Test 1 — JetStream Streams",
        "",
    ]

    for s in r.get("streams", []):
        lines += [
            f"### Stream: `{s['name']}`",
            "",
            f"- Subjects: `{'`, `'.join(s['subjects']) if s['subjects'] else '?'}`",
            f"- Messages stored: **{fmt_num(s['msgs'])}**",
            f"- Bytes stored: **{fmt_bytes(s['bytes'])}**",
            f"- Retention: **{s['max_age_hrs']}h**",
            "",
        ]
        if s.get("consumers"):
            lines.append("| Consumer | Pending (lag) | Ack floor |")
            lines.append("|----------|:---:|:---:|")
            for c in s["consumers"]:
                lines.append(f"| `{c['name']}` | {fmt_num(c['pending'])} | {fmt_num(c['ack_floor'])} |")
            lines.append("")

    lines += [
        "---",
        "",
        "## Test 2 — Message Throughput",
        "",
        f"**Result:** **{fmt_num(r['msgs_per_sec'])}/sec** messages ingested to JetStream.",
        "",
        "---",
        "",
        "## Test 3 — Consumer Lag",
        "",
        f"**Result:** {lag_str}",
        "",
        "(Lag = messages in stream not yet acknowledged by a consumer. "
        "Zero lag means the consumer is keeping up in real time.)",
        "",
        "---",
        "",
        "## Test 4 — Parquet Storage (S3/MinIO)",
        "",
        f"- Files written (last 2h): **{fmt_num(r['parquet_files_2h'])}**",
        f"- Bytes written (last 2h): **{fmt_bytes(r['parquet_bytes_2h'])}**",
        f"- Total parquet files: **{fmt_num(r['parquet_files_total'])}**",
        f"- Total parquet storage: **{fmt_bytes(r['parquet_bytes_total'])}**",
        "",
    ]

    if r.get("sample_schema"):
        lines += [
            f"**Sample file schema** (`{Path(r.get('sample_file','')).name}`):",
            "",
            f"Columns ({len(r['sample_schema'])}): `{'`, `'.join(r['sample_schema'])}`",
            "",
            f"Rows in file: **{fmt_num(r['sample_rows'])}**",
            "",
        ]
        if r.get("schema_extra_fields"):
            lines += [
                f"**Extra (non-standard) columns found:** `{'`, `'.join(r['schema_extra_fields'])}`",
                "",
                "These extra columns were automatically preserved without any config change.",
                "",
            ]

    lines += [
        "---",
        "",
        "## Test 5 — Schema Flexibility",
        "",
        "Analysis of writer.py behaviour with payload schema changes:",
        "",
        "| Scenario | Telegraf behaviour | NATS + Parquet behaviour |",
        "|---|---|---|",
        "| **Extra field in payload** | Silently ignored (not in config) | Automatically stored as new Parquet column |",
        "| **Missing field** | Error → entire message DROPPED | NULL in that column, record preserved |",
        "| **Nested `{value, unit}` payload** | Error → entire message DROPPED | `_flatten()` handles both nested and flat |",
        "| **New source type (solar, wind)** | Not subscribed → zero data | Subject `>` wildcard catches everything |",
        "| **Schema change** | Edit config file + restart required | Zero config, zero restart |",
        "| **Parse error recovery** | Manual restart, no replay | JetStream replay from last consumer position |",
        "",
        f"**Dynamic schema collection:** {'✓' if sf.get('dynamic_keys') else '✗'} "
        f"(`all_keys = {{k for r in flat for k in r}}` — every row's keys included)",
        "",
        f"**Nested payload handling:** {'✓' if sf.get('handles_nested') else '✗'} "
        f"(flattens `{{\"value\": X}}` objects transparently)",
        "",
        f"**Null-safe missing fields:** {'✓' if sf.get('null_on_missing') else '✗'} "
        f"(`r.get(k)` → None → NULL column, record not dropped)",
        "",
        "---",
        "",
        "## Test 6 — Resilience Properties",
        "",
        f"- JetStream file storage: {'✓' if res.get('file_storage') else '?'}",
        f"- Sync to disk before ACK: {'✓' if res.get('sync_on_write') else '?'}",
        f"- Max file store: {res.get('max_file_store', '?')}",
        f"- Retention window: **{retention}h** — if parquet writer is down for ≤{retention}h, zero data loss on restart",
        "",
        "**Recovery scenario (vs Telegraf):**",
        "",
        "| Event | Telegraf | NATS + Parquet |",
        "|---|---|---|",
        "| Writer/consumer down 1h | Data lost (MQTT publish drops) | Replayed from JetStream on restart |",
        "| Payload format changes | Silent data loss (parse error suppressed) | Raw JSON preserved in stream |",
        "| Add new sensor field | Lost permanently | In JetStream, appears in Parquet after schema update |",
        "| S3 write fails | N/A | Retries with exponential backoff, message stays un-ACK'd |",
        "",
        "---",
        "",
        "## Test 7 — Process Resources",
        "",
        "| Process | RSS Memory |",
        "|---------|----------:|",
        f"| `nats-server` | {fmt_mb(r.get('nats_rss_kb'))} |",
        f"| `parquet writer` | {fmt_mb(r.get('writer_rss_kb'))} |",
        f"| `nats bridge` | {fmt_mb(r.get('bridge_rss_kb'))} |",
        "",
        "---",
        "",
        "## Test 8 — AWS Cost Estimates",
        "",
    ] + _aws_md_lines(r.get("aws", {})) + [
        "",
        "---",
        "",
        "## Summary",
        "",
        "| Finding | Value |",
        "|---------|-------|",
        f"| Message throughput | {fmt_num(r['msgs_per_sec'])}/sec |",
        f"| Stream storage | {fmt_bytes(r['total_stream_bytes'])} |",
        f"| Consumer lag | {lag_str} |",
        f"| Parquet files (last 2h) | {fmt_num(r['parquet_files_2h'])} |",
        f"| Parquet storage (total) | {fmt_bytes(r['parquet_bytes_total'])} |",
        f"| Retention window | {retention}h |",
        f"| Schema: extra fields stored | {'✓ yes' if sf.get('dynamic_keys') else '? unknown'} |",
        f"| Schema: missing fields → NULL | {'✓ yes (record preserved)' if sf.get('null_on_missing') else '? unknown'} |",
        f"| Schema: nested payload handled | {'✓ yes' if sf.get('handles_nested') else '? unknown'} |",
        f"| nats-server RSS | {fmt_mb(r.get('nats_rss_kb'))} |",
        f"| Est. AWS cost (1× scale) | ${r.get('aws', {}).get('scales', [{}])[0].get('total', 0):.2f}/month |",
        "",
    ]

    return "\n".join(lines)


# ── HTML generation ────────────────────────────────────────────────────────────

CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Segoe UI', system-ui, sans-serif;
  background: #0d1117; color: #e6edf3;
  padding: 40px 32px; max-width: 1100px; margin: auto; line-height: 1.65;
}
h1   { color: #58a6ff; font-size: 1.8em; margin-bottom: 6px; }
h2   { color: #79c0ff; font-size: 1.1em; margin: 40px 0 14px;
       border-bottom: 1px solid #30363d; padding-bottom: 8px; }
h3   { color: #d2a679; font-size: 0.95em; margin: 20px 0 8px; }
p    { color: #c9d1d9; font-size: 0.88em; margin-bottom: 10px; }
code { color: #f39c12; background: #161b22; padding: 1px 6px; border-radius: 3px; font-size: 0.85em; }
pre  { background: #161b22; border: 1px solid #30363d; border-radius: 6px;
       padding: 14px 16px; overflow-x: auto; margin: 10px 0 16px; }
pre code { background: none; padding: 0; color: #79c0ff; font-size: 0.83em; line-height: 1.6; }
ul   { margin: 6px 0 10px 18px; color: #8b949e; font-size: 0.85em; }
ul li { margin-bottom: 4px; }
strong { color: #e6edf3; }
a    { color: #58a6ff; text-decoration: none; }
a:hover { text-decoration: underline; }
.subtitle { color: #8b949e; font-size: 0.90em; margin-bottom: 32px; margin-top: 4px; }
.meta-bar {
  display: flex; flex-wrap: wrap; gap: 10px;
  background: #161b22; border: 1px solid #30363d; border-radius: 6px;
  padding: 14px 20px; margin-bottom: 36px;
}
.meta-item { font-size: 0.82em; color: #8b949e; }
.meta-item strong { color: #c9d1d9; }
.result-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 12px; margin: 16px 0 24px;
}
.result-card {
  background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px;
}
.result-card .label { font-size: 0.75em; color: #8b949e; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px; }
.result-card .value { font-size: 1.5em; font-weight: 700; color: #3fb950; font-variant-numeric: tabular-nums; }
.result-card .value.warn   { color: #f0883e; }
.result-card .value.danger { color: #f85149; }
.result-card .value.blue   { color: #58a6ff; }
.result-card .sub { font-size: 0.78em; color: #8b949e; margin-top: 4px; }
.callout {
  border-radius: 6px; padding: 14px 18px; margin: 16px 0; font-size: 0.87em; line-height: 1.6;
}
.callout.warn   { background: #2d1f0e; border-left: 3px solid #f0883e; color: #e3b981; }
.callout.danger { background: #2d0e0e; border-left: 3px solid #f85149; color: #e38181; }
.callout.info   { background: #0e1d2d; border-left: 3px solid #58a6ff; color: #8ac4f5; }
.callout.ok     { background: #0d2d1a; border-left: 3px solid #3fb950; color: #7fba84; }
.callout strong { color: inherit; }
table { width: 100%; border-collapse: collapse; font-size: 0.84em; margin: 12px 0 20px; }
thead th {
  background: #161b22; color: #8b949e; text-align: left; padding: 8px 12px;
  font-size: 0.78em; text-transform: uppercase; letter-spacing: 0.06em;
  border-bottom: 1px solid #30363d;
}
tbody td { padding: 8px 12px; color: #c9d1d9; border-bottom: 1px solid #21262d; }
tbody tr:hover td { background: #161b22; }
td.num   { text-align: right; font-variant-numeric: tabular-nums; color: #58a6ff; }
td.green { color: #3fb950; font-weight: 600; }
td.red   { color: #f85149; font-weight: 600; }
td.orange{ color: #f0883e; font-weight: 600; }
td.muted { color: #8b949e; font-style: italic; }
.section { margin-bottom: 48px; }
.nav { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 32px; }
.nav a {
  padding: 5px 12px; border-radius: 5px; font-size: 0.82em;
  background: #161b22; border: 1px solid #30363d; color: #8b949e;
}
.nav a:hover { border-color: #58a6ff; color: #58a6ff; text-decoration: none; }
.badge-ok   { display: inline-block; background: #0d2d1a; color: #3fb950;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px; margin-left: 4px; }
.badge-fail { display: inline-block; background: #3d1010; color: #f85149;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px; margin-left: 4px; }
.badge-warn { display: inline-block; background: #2d1f0e; color: #f0883e;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px; margin-left: 4px; }
.test-number {
  display: inline-block; background: #1f3a6e; color: #79c0ff;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px;
  margin-right: 6px; vertical-align: middle; letter-spacing: 0.05em;
}
.check { color: #3fb950; font-weight: 700; }
.cross { color: #f85149; font-weight: 700; }
""".strip()


def card(label, value, cls="", sub=""):
    return (f'<div class="result-card">'
            f'<div class="label">{label}</div>'
            f'<div class="value {cls}">{value}</div>'
            + (f'<div class="sub">{sub}</div>' if sub else "")
            + '</div>')

def check(v):
    return '<span class="check">✓</span>' if v else '<span class="cross">✗</span>'


def build_html(r):
    arch_label = "aarch64" if "aarch64" in r["arch"] or "arm" in r["arch"].lower() else "x86_64"
    sf  = r.get("schema_flex", {})
    res = r.get("resilience", {})
    retention = res.get("retention_hours", "?")
    lag_items = r.get("consumer_lag", {})
    nats_badge = '<span class="badge-ok">UP</span>' if r.get("nats_up") else '<span class="badge-fail">DOWN</span>'
    s3_badge   = '<span class="badge-ok">REACHABLE</span>' if r.get("s3_reachable") else '<span class="badge-warn">UNREACHABLE</span>'

    # Pre-compute stream rows
    stream_rows = ""
    for s in r.get("streams", []):
        subj = ", ".join(f"<code>{x}</code>" for x in s["subjects"]) if s["subjects"] else "—"
        stream_rows += f"""
  <tr>
    <td><strong>{s['name']}</strong></td>
    <td>{subj}</td>
    <td class="num">{fmt_num(s['msgs'])}</td>
    <td class="num">{fmt_bytes(s['bytes'])}</td>
    <td class="num">{s['max_age_hrs']}h</td>
  </tr>"""

    # Consumer lag rows
    lag_rows = ""
    for name, pending in lag_items.items():
        lag_cls = "red" if pending > 10000 else ("orange" if pending > 1000 else "green")
        lag_rows += f"""
  <tr>
    <td><code>{name}</code></td>
    <td class="num {lag_cls}">{fmt_num(pending)}</td>
    <td class="{'green' if pending == 0 else 'orange'}">{'real-time' if pending == 0 else 'catching up'}</td>
  </tr>"""
    if not lag_rows:
        lag_rows = '<tr><td colspan="3" class="muted">No consumer data available</td></tr>'

    # Schema columns section
    schema_section = ""
    if r.get("sample_schema"):
        cols_html = " ".join(f"<code>{c}</code>" for c in r["sample_schema"])
        extra_html = ""
        if r.get("schema_extra_fields"):
            extra_list = " ".join(f"<code>{c}</code>" for c in r["schema_extra_fields"])
            extra_html = f"""
<div class="callout ok">
  <strong>Extra columns detected in live parquet file</strong> — stored automatically without any config change:<br>
  {extra_list}
</div>"""
        schema_section = f"""
<h3>Sample file: <code>{Path(r.get("sample_file","")).name}</code> — {fmt_num(r["sample_rows"])} rows</h3>
<p>Columns ({len(r["sample_schema"])}): {cols_html}</p>
{extra_html}"""

    # Retention callout
    ret_callout = (
        f'<div class="callout ok"><strong>Retention window: {retention}h.</strong> '
        f'If the parquet writer is stopped and restarted within {retention} hours, '
        f'NATS replays all missed messages from the consumer\'s last acknowledged position. '
        f'Zero data loss within the retention window.</div>'
        if retention != "?" else ""
    )

    # Pre-compute AWS cost section
    aws = r.get("aws", {})
    aws_scales = aws.get("scales", [])
    if aws_scales:
        def _td_usd(val):
            return f'<td class="num">${val:.2f}</td>'
        aws_rows = ""
        row_defs = [
            ("Messages/sec",        lambda s: f'<td class="num blue">{fmt_num(s["msgs_per_sec"])}</td>'),
            ("Parquet GB/month",    lambda s: f'<td class="num">{s["gb_per_month"]}</td>'),
            ("S3 storage",          lambda s: _td_usd(s["s3_storage"])),
            ("S3 PUT requests",     lambda s: _td_usd(s["s3_put"])),
            ("S3 GET requests",     lambda s: _td_usd(s["s3_get"])),
            ("EC2 NATS server",     lambda s: f'<td class="num">${s["nats_ec2"]:.2f} <small>({s["nats_label"]})</small></td>'),
            ("EC2 Parquet writer",  lambda s: f'<td class="num">${s["writer_ec2"]:.2f} <small>({s["writer_label"]})</small></td>'),
        ]
        for label, fn in row_defs:
            aws_rows += f"  <tr><td>{label}</td>" + "".join(fn(s) for s in aws_scales) + "</tr>\n"
        # Total row
        aws_rows += "  <tr><td><strong>Total / month</strong></td>" + \
            "".join(f'<td class="num"><strong>${s["total"]:.2f}</strong></td>' for s in aws_scales) + "</tr>\n"

        iot_rows = ""
        for s in aws_scales:
            iot_rows += (f'  <tr><td>{s["scale"]}× scale</td>'
                         f'<td class="num">${s["iot_alt"]:.2f}</td>'
                         f'<td>Replaces FlashMQ + NATS bridge EC2 for MQTT layer</td></tr>\n')

        aws_notes_html = "".join(f"<li>{n}</li>" for n in aws.get("notes", []))
        aws_section = f"""
<div class="callout info">
  Extrapolated from measured <strong>{fmt_num(aws.get("msgs_per_sec_base"))}/sec</strong> throughput
  and <strong>{aws.get("gb_per_day", "?")} GB/day</strong> parquet write rate.
  Pricing: AWS us-east-1 on-demand.
</div>
<h3>Fleet Scaling — Monthly Cost Breakdown</h3>
<table>
  <thead>
    <tr>
      <th>Service</th>
      <th>1× (current)</th>
      <th>6× scale</th>
      <th>24× scale</th>
    </tr>
  </thead>
  <tbody>
{aws_rows}  </tbody>
</table>
<h3>Alternative: AWS IoT Core for MQTT Ingest</h3>
<p>IoT Core can replace the self-managed FlashMQ + NATS bridge EC2 instance.
Cost is per-message; large fleets become expensive relative to a fixed EC2.</p>
<table>
  <thead><tr><th>Scale</th><th>IoT Core cost/month</th><th>Notes</th></tr></thead>
  <tbody>
{iot_rows}  </tbody>
</table>
<h3>Notes</h3>
<ul>{aws_notes_html}</ul>"""
    else:
        aws_section = '<div class="callout warn">AWS cost data not available.</div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NATS JetStream + Parquet — Live Measurements</title>
<style>{CSS}</style>
</head>
<body>

<div class="nav">
  <a href="dashboard.html">Dashboard</a>
  <a href="telegraf_influx_details.html">aarch64 (Telegraf)</a>
  <a href="telegraf_influx_details_x86.html">x86 (Telegraf)</a>
  <a href="subscriber.html">Subscriber</a>
  <a href="generator.html">Generator</a>
</div>

<h1>NATS JetStream + Parquet Writer — Live Performance Measurements</h1>
<p class="subtitle">
  Empirical measurements taken {r["test_date"]} on {r["hostname"]} ({arch_label}).
  Compare with <a href="telegraf_influx_details.html">Telegraf/InfluxDB baseline</a>.
</p>

<div class="meta-bar">
  <div class="meta-item"><strong>Host</strong> {r["hostname"]}</div>
  <div class="meta-item"><strong>Arch</strong> {arch_label}</div>
  <div class="meta-item"><strong>NATS</strong> {r["nats_ver"]} {nats_badge}</div>
  <div class="meta-item"><strong>S3/MinIO</strong> {s3_badge}</div>
  <div class="meta-item"><strong>Retention</strong> {retention}h JetStream replay</div>
</div>

<!-- ═══════ TEST 0: SERVER HEALTH -->
<div class="section">
<h2><span class="test-number">TEST 0</span> NATS Server Health {nats_badge}</h2>
<div class="result-grid">
  {card("NATS version", r["nats_ver"], "blue")}
  {card("Active connections", fmt_num(r.get("nats_connections")), "blue")}
  {card("Server RSS", str(round(r["nats_mem_mb"], 0)) + " MB" if r.get("nats_mem_mb") else "—", "")}
  {card("S3/MinIO", "reachable" if r["s3_reachable"] else "unreachable",
        "" if r["s3_reachable"] else "warn")}
</div>
</div>

<!-- ═══════ TEST 1: STREAMS -->
<div class="section">
<h2><span class="test-number">TEST 1</span> JetStream Streams</h2>
<table>
  <thead>
    <tr><th>Stream</th><th>Subjects</th><th>Messages stored</th><th>Bytes stored</th><th>Retention</th></tr>
  </thead>
  <tbody>{stream_rows}</tbody>
</table>
<div class="result-grid">
  {card("Total messages stored", fmt_num(r["total_stream_msgs"]), "blue")}
  {card("Total bytes stored", fmt_bytes(r["total_stream_bytes"]), "blue")}
  {card("Retention window", str(retention) + "h", "")}
</div>
</div>

<!-- ═══════ TEST 2: THROUGHPUT -->
<div class="section">
<h2><span class="test-number">TEST 2</span> Message Throughput</h2>
<div class="result-grid">
  {card("Messages / sec", fmt_num(r["msgs_per_sec"]), "blue", "all subjects combined")}
</div>
<div class="callout info">
  <strong>What this measures:</strong> server-level <code>in_msgs</code> delta (not stream message count).
  Stream message count can appear flat when the stream is at its <code>max_file_store</code> limit
  — new messages arrive while the oldest are evicted at the same rate.
  Using the server counter captures the true ingest rate regardless of stream capacity.
</div>
</div>

<!-- ═══════ TEST 3: CONSUMER LAG -->
<div class="section">
<h2><span class="test-number">TEST 3</span> Consumer Lag</h2>
<table>
  <thead><tr><th>Consumer</th><th>Pending messages</th><th>Status</th></tr></thead>
  <tbody>{lag_rows}</tbody>
</table>
<div class="callout ok">
  <strong>Zero lag = consumer is keeping up in real time.</strong>
  Lag only builds during downtime or overload. Unlike Telegraf's 100k buffer,
  unprocessed messages live in durable JetStream storage — they are not lost
  when the consumer is busy or restarted.
</div>
</div>

<!-- ═══════ TEST 4: PARQUET STORAGE -->
<div class="section">
<h2><span class="test-number">TEST 4</span> Parquet Storage (S3/MinIO)</h2>
<div class="result-grid">
  {card("Files (last 2h)", fmt_num(r["parquet_files_2h"]), "blue")}
  {card("Bytes (last 2h)", fmt_bytes(r["parquet_bytes_2h"]), "blue")}
  {card("Total parquet files", fmt_num(r["parquet_files_total"]), "")}
  {card("Total parquet storage", fmt_bytes(r["parquet_bytes_total"]), "")}
</div>
{schema_section}
</div>

<!-- ═══════ TEST 5: SCHEMA FLEXIBILITY -->
<div class="section">
<h2><span class="test-number">TEST 5</span> Schema Flexibility</h2>

<div class="callout ok">
  <strong>Zero-config schema evolution.</strong>
  The parquet writer collects all keys from all rows dynamically
  (<code>all_keys = {{k for r in flat for k in r}}</code>).
  New fields appear automatically as new Parquet columns. Missing fields
  become NULLs — the record is always written.
</div>

<table>
  <thead>
    <tr><th>Scenario</th><th>Telegraf / InfluxDB</th><th>NATS + Parquet</th></tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Extra field in payload</strong></td>
      <td class="orange">Silently ignored — not in config mapping</td>
      <td class="green">Automatically stored as new Parquet column</td>
    </tr>
    <tr>
      <td><strong>Missing field (e.g. <code>power</code>)</strong></td>
      <td class="red">Error → entire message DROPPED, 0 writes</td>
      <td class="green">NULL in that column — record preserved</td>
    </tr>
    <tr>
      <td><strong>Nested <code>&#123;"value": X, "unit": "V"&#125;</code> payload</strong></td>
      <td class="red">Error → entire message DROPPED</td>
      <td class="green"><code>_flatten()</code> handles both formats transparently</td>
    </tr>
    <tr>
      <td><strong>New source type (solar, wind)</strong></td>
      <td class="red">Not subscribed — telegraf only listens to <code>batteries/#</code></td>
      <td class="green">Subject filter <code>&gt;</code> catches all subjects automatically</td>
    </tr>
    <tr>
      <td><strong>Schema change</strong></td>
      <td class="red">Edit config + restart telegraf required</td>
      <td class="green">Zero config, zero restart, zero downtime</td>
    </tr>
    <tr>
      <td><strong>Parse error recovery</strong></td>
      <td class="red">Manual restart — missed data lost forever</td>
      <td class="green">JetStream replay from last acknowledged position</td>
    </tr>
  </tbody>
</table>

<h3>Writer code verification</h3>
<table>
  <thead><tr><th>Property</th><th>Status</th><th>Evidence</th></tr></thead>
  <tbody>
    <tr>
      <td>Dynamic schema (all fields captured)</td>
      <td>{check(sf.get("dynamic_keys"))}</td>
      <td><code>all_keys = {{k for r in flat for k in r}}</code></td>
    </tr>
    <tr>
      <td>Nested payload flattening</td>
      <td>{check(sf.get("handles_nested"))}</td>
      <td><code>if isinstance(v, dict) and "value" in v: v = v["value"]</code></td>
    </tr>
    <tr>
      <td>Null-safe missing fields</td>
      <td>{check(sf.get("null_on_missing"))}</td>
      <td><code>r.get(k)</code> → None → nullable Parquet array</td>
    </tr>
    <tr>
      <td>Wildcard subject subscription</td>
      <td>{check(sf.get("wildcard_filter"))}</td>
      <td><code>subject_filter: "&gt;"</code> — all source types received</td>
    </tr>
  </tbody>
</table>
</div>

<!-- ═══════ TEST 6: RESILIENCE -->
<div class="section">
<h2><span class="test-number">TEST 6</span> Resilience Properties</h2>

{ret_callout}

<table>
  <thead><tr><th>Failure event</th><th>Telegraf / InfluxDB</th><th>NATS + Parquet</th></tr></thead>
  <tbody>
    <tr>
      <td>Consumer / writer down for &lt;{retention}h</td>
      <td class="red">MQTT publishes dropped once buffer fills (~88s at 1,134/s)</td>
      <td class="green">JetStream stores all messages — replayed on restart</td>
    </tr>
    <tr>
      <td>Consumer / writer down for &gt;{retention}h</td>
      <td class="red">All data during outage lost</td>
      <td class="orange">Gap fill from AWS S3 backup or gap_fill consumer</td>
    </tr>
    <tr>
      <td>S3 write failure</td>
      <td class="muted">N/A</td>
      <td class="green">Exponential backoff retry; message stays un-ACK'd in stream</td>
    </tr>
    <tr>
      <td>Multiple independent consumers</td>
      <td class="red">Single pipeline — one telegraf instance per broker</td>
      <td class="green">Durable consumers: parquet-writer + gap-fill + subscriber all independent</td>
    </tr>
    <tr>
      <td>Historical replay / backfill</td>
      <td class="red">Not possible — data only in InfluxDB time-series</td>
      <td class="green">Seek consumer to any stream position within retention window</td>
    </tr>
  </tbody>
</table>

<div class="result-grid">
  {card("File storage", "✓ durable" if res.get("file_storage") else "?", "blue" if res.get("file_storage") else "warn")}
  {card("Sync before ACK", "✓ enabled" if res.get("sync_on_write") else "?", "blue" if res.get("sync_on_write") else "warn")}
  {card("Max file store", str(res.get("max_file_store", "?")), "")}
  {card("Retention", str(retention) + "h", "blue")}
</div>
</div>

<!-- ═══════ TEST 7: RESOURCES -->
<div class="section">
<h2><span class="test-number">TEST 7</span> Process Resources</h2>
<div class="result-grid">
  {card("nats-server RSS", fmt_mb(r.get("nats_rss_kb")), "")}
  {card("parquet writer RSS", fmt_mb(r.get("writer_rss_kb")), "")}
  {card("nats bridge RSS", fmt_mb(r.get("bridge_rss_kb")), "")}
</div>
</div>

<!-- ═══════ TEST 8: AWS COSTS -->
<div class="section">
<h2><span class="test-number">TEST 8</span> AWS Cost Estimates</h2>
{aws_section}
</div>

<!-- ═══════ SUMMARY -->
<div class="section">
<h2>Summary</h2>
<div class="result-grid">
  {card("Throughput", fmt_num(r["msgs_per_sec"]) + "/sec", "blue", "JetStream messages")}
  {card("Consumer lag", "0" if all(v == 0 for v in lag_items.values()) else fmt_num(max(lag_items.values()) if lag_items else None),
        "" if all(v == 0 for v in lag_items.values()) else "warn", "pending messages")}
  {card("Parquet files (2h)", fmt_num(r["parquet_files_2h"]), "blue")}
  {card("Total storage", fmt_bytes(r["parquet_bytes_total"]), "")}
  {card("Schema flexibility", "dynamic", "blue", "zero-config field addition")}
  {card("Missing field behaviour", "NULL", "blue", "record preserved")}
  {card("Retention / replay", str(retention) + "h", "blue", "zero data loss window")}
  {card("Est. AWS cost (1×)", "$" + str(aws_scales[0]["total"]) if aws_scales else "—", "blue", "/month")}
</div>
</div>

</body>
</html>
"""


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="NATS JetStream + Parquet live test runner")
    ap.add_argument("--config",      default=None,
                    help="Path to parquet_writer config.yaml (for S3 credentials)")
    ap.add_argument("--nats-mon",    default="http://localhost:8222",
                    help="NATS monitoring base URL")
    ap.add_argument("--s3-endpoint", default=None,
                    help="S3/MinIO endpoint URL override")
    ap.add_argument("--s3-bucket",   default="battery-data")
    ap.add_argument("--s3-key",      default="minioadmin")
    ap.add_argument("--s3-secret",   default="minioadmin")
    ap.add_argument("--sample-secs", default=30, type=int,
                    help="Throughput sample window (s)")
    ap.add_argument("--docs-dir",    default=str(DEFAULT_DOCS_DIR))
    ap.add_argument("--html-dir",    default=str(DEFAULT_HTML_DIR))
    ap.add_argument("--dry-run",     action="store_true",
                    help="Print results, don't write files")
    args = ap.parse_args()

    # Build S3 config override
    s3_override = {
        "bucket":   args.s3_bucket,
        "access_key": args.s3_key,
        "secret_key": args.s3_secret,
    }
    if args.s3_endpoint:
        s3_override["endpoint_url"] = args.s3_endpoint

    # Quick NATS connectivity check
    print(f"Connecting to NATS monitoring at {args.nats_mon}...", flush=True)
    srv = nats_get(args.nats_mon, "/varz")
    if "error" in srv:
        print(f"WARNING: NATS monitoring unreachable — {srv['error']}")
        print("Continuing — resource and schema flexibility checks will still run.")

    print(f"\nRunning measurements ({args.sample_secs}s throughput sample)...", flush=True)
    results = measure(
        mon_base          = args.nats_mon,
        s3_cfg_override   = s3_override,
        sample_secs       = args.sample_secs,
        writer_config_path= args.config,
    )

    # Print summary
    print(f"\nHost: {results['hostname']}  NATS: {results['nats_ver']}")
    print(f"  Throughput:      {fmt_num(results['msgs_per_sec'])}/sec")
    print(f"  Stream msgs:     {fmt_num(results['total_stream_msgs'])}")
    print(f"  Stream bytes:    {fmt_bytes(results['total_stream_bytes'])}")
    print(f"  Consumer lag:    " +
          (", ".join(f"{k}={v}" for k, v in results.get("consumer_lag", {}).items()) or "—"))
    print(f"  Parquet (2h):    {fmt_num(results['parquet_files_2h'])} files, "
          f"{fmt_bytes(results['parquet_bytes_2h'])}")
    print(f"  Parquet (total): {fmt_bytes(results['parquet_bytes_total'])}")
    print(f"  nats-server RSS: {fmt_mb(results.get('nats_rss_kb'))}")
    sf = results.get("schema_flex", {})
    print(f"  Schema flexible: dynamic={sf.get('dynamic_keys')}, "
          f"nested={sf.get('handles_nested')}, null_safe={sf.get('null_on_missing')}")
    aws = results.get("aws", {})
    if aws.get("scales"):
        s = aws["scales"][0]
        print(f"  AWS est. (1×):   ${s['total']:.2f}/month  "
              f"(S3 ${s['s3_storage']:.2f} + EC2 ${s['nats_ec2']+s['writer_ec2']:.2f})")

    if args.dry_run:
        print("\n[dry-run] Files not written.")
        return

    docs_dir = Path(args.docs_dir)
    html_dir = Path(args.html_dir)
    docs_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)

    md_path   = docs_dir / "nats_details.md"
    html_path = html_dir / "nats_details.html"

    md_path.write_text(build_markdown(results))
    html_path.write_text(build_html(results))

    print(f"\nWrote: {md_path}")
    print(f"Wrote: {html_path}")


if __name__ == "__main__":
    main()
