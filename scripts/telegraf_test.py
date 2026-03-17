#!/usr/bin/env python3
"""
telegraf_test.py — Live Telegraf/InfluxDB performance test runner.

Queries the live running stack, collects metrics over a short observation window,
and regenerates docs/telegraf_influx_details[_x86].md and the matching HTML page.

Usage:
    python scripts/telegraf_test.py                     # auto-detect arch
    python scripts/telegraf_test.py --arch aarch64      # force label
    python scripts/telegraf_test.py --host spark-22b6   # remote InfluxDB
    python scripts/telegraf_test.py --sample-secs 60    # longer rate sample
    python scripts/telegraf_test.py --out-dir /tmp      # custom output path
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
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
DEFAULT_DOCS_DIR = REPO_ROOT / "docs"
DEFAULT_HTML_DIR = REPO_ROOT / "html"

# ── InfluxDB client (stdlib only) ──────────────────────────────────────────────

class InfluxClient:
    def __init__(self, host, port, token, org, bucket):
        self.base   = f"http://{host}:{port}"
        self.token  = token
        self.org    = org
        self.bucket = bucket

    def _get(self, url, extra_headers=None):
        headers = {"Authorization": f"Token {self.token}"}
        if extra_headers:
            headers.update(extra_headers)
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            return {"error": f"HTTP {e.code}: {e.read().decode()[:200]}"}
        except Exception as e:
            return {"error": str(e)}

    def influxql(self, query):
        """Run InfluxQL query; return first series as {columns, values, name}."""
        db  = self.bucket
        url = (f"{self.base}/query"
               f"?db={urllib.parse.quote(db)}"
               f"&q={urllib.parse.quote(query)}")
        data = self._get(url)
        if "results" in data and data["results"]:
            r = data["results"][0]
            if "series" in r and r["series"]:
                s = r["series"][0]
                return {
                    "ok":      True,
                    "name":    s.get("name", ""),
                    "columns": s.get("columns", []),
                    "values":  s.get("values", []),
                }
            err = r.get("error", "")
            return {"ok": False, "name": "", "columns": [], "values": [], "error": err}
        return {"ok": False, "name": "", "columns": [], "values": [],
                "error": data.get("error", "empty result")}

    def scalar(self, query, col=1):
        """Return single scalar value from first row of first series."""
        r = self.influxql(query)
        if r["values"]:
            v = r["values"][0][col]
            return v
        return None

    def flux(self, query):
        """Try a Flux query; return (success:bool, rows:int|error_str)."""
        url  = f"{self.base}/api/v2/query?org={urllib.parse.quote(self.org)}"
        body = json.dumps({"query": query, "type": "flux"}).encode()
        req  = urllib.request.Request(url, data=body, headers={
            "Authorization":  f"Token {self.token}",
            "Content-Type":   "application/json",
            "Accept":         "application/csv",
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                lines = [l for l in r.read().decode().strip().splitlines()
                         if l and not l.startswith("#")]
                return True, max(0, len(lines) - 1)
        except urllib.error.HTTPError as e:
            return False, f"HTTP {e.code}"
        except Exception as e:
            return False, str(e)


# ── System helpers ─────────────────────────────────────────────────────────────

def proc_rss_kb(fragment):
    """RSS in KB for first process whose command line contains fragment."""
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

def proc_cpu_pct(fragment):
    """Cumulative %CPU for first matching process."""
    try:
        out = subprocess.check_output(["ps", "aux"], text=True)
        for line in out.splitlines():
            if fragment in line and "grep" not in line and sys.argv[0] not in line:
                parts = line.split()
                if len(parts) > 3:
                    return float(parts[2])
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

def journalctl_has(unit, pattern, since="72 hours ago"):
    try:
        out = subprocess.check_output(
            ["journalctl", "-u", unit, "--no-pager",
             "--since", since, "--grep", pattern],
            text=True, stderr=subprocess.DEVNULL)
        lines = [l for l in out.splitlines()
                 if l.strip() and not l.startswith("--")]
        return len(lines)
    except Exception:
        return 0

def flashmq_log_has(pattern, log="/var/log/flashmq/flashmq.log"):
    try:
        out = subprocess.check_output(["grep", "-c", pattern, log],
                                      text=True, stderr=subprocess.DEVNULL)
        return int(out.strip())
    except Exception:
        return 0


# ── Core measurements ──────────────────────────────────────────────────────────

def measure(client, sample_secs=30):
    """Run all tests and return a results dict."""
    c = client
    r = {}

    # ── System info
    r["bucket"]       = client.bucket
    r["arch"]         = platform.machine()
    r["hostname"]     = platform.node()
    r["test_date"]    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    r["influxd_ver"]  = get_version(["influxd", "version"])
    r["telegraf_ver"] = get_version(["telegraf", "--version"])

    # ── Test 0: Flux API
    print("  [0] Flux API check...", flush=True)
    flux_query = (f'from(bucket: "{client.bucket}") '
                  '|> range(start: -1h) |> first() |> limit(n: 1)')
    flux_http_ok, flux_detail = c.flux(flux_query)
    # Flux is only "working" if HTTP succeeded AND returned at least one data row
    r["flux_ok"]     = flux_http_ok and isinstance(flux_detail, int) and flux_detail > 0
    r["flux_detail"] = flux_detail

    # ── Test 1: Write rate (two snapshots)
    print(f"  [1] Write rate — sampling {sample_secs}s...", flush=True)
    snap1 = c.scalar("SELECT messages_received FROM internal_mqtt_consumer "
                     "ORDER BY time DESC LIMIT 1")
    t0 = time.time()
    time.sleep(sample_secs)
    snap2 = c.scalar("SELECT messages_received FROM internal_mqtt_consumer "
                     "ORDER BY time DESC LIMIT 1")
    elapsed = time.time() - t0

    r["mqtt_total"] = snap2
    if snap1 is not None and snap2 is not None and snap2 > snap1:
        r["msg_per_sec"] = round((snap2 - snap1) / elapsed, 1)
    else:
        # Fallback: count(voltage) last 1 min
        v = c.scalar("SELECT count(voltage) FROM battery_cell WHERE time > now()-1m")
        r["msg_per_sec"] = round(v / 60, 1) if v else None

    # Field count → field writes/sec
    fk = c.influxql("SHOW FIELD KEYS FROM battery_cell")
    r["field_count"] = len(fk["values"]) if fk["values"] else None
    if r["msg_per_sec"] and r["field_count"]:
        r["field_writes_per_sec"] = round(r["msg_per_sec"] * r["field_count"])
    else:
        r["field_writes_per_sec"] = None

    # ── Test 2: Lifetime writes
    print("  [2] Lifetime writes...", flush=True)
    r["metrics_written"] = c.scalar("SELECT last(metrics_written) FROM internal_write")

    # ── Test 3: Hourly point count (voltage as proxy for record count)
    print("  [3] Hourly point count...", flush=True)
    v = c.scalar("SELECT count(voltage) FROM battery_cell WHERE time > now()-1h")
    r["voltage_points_1h"] = v
    if v and r["field_count"]:
        r["all_fields_1h"] = v * r["field_count"]
    else:
        r["all_fields_1h"] = None

    # ── Test 4: Dropped metrics
    print("  [4] Dropped metrics...", flush=True)
    d = c.scalar("SELECT last(metrics_dropped) FROM internal_write")
    r["metrics_dropped"] = d if d is not None else 0

    # ── Test 5: Process resources
    print("  [5] Process resources...", flush=True)
    # Use path-anchored fragments to avoid matching journalctl/grep lines
    r["influxd_rss_kb"]  = proc_rss_kb("/influxd")  or proc_rss_kb("influxd ")
    r["telegraf_rss_kb"] = proc_rss_kb("/telegraf ") or proc_rss_kb("telegraf ")
    r["influxd_cpu_pct"] = proc_cpu_pct("/influxd")  or proc_cpu_pct("influxd ")
    r["telegraf_cpu_pct"]= proc_cpu_pct("/telegraf ") or proc_cpu_pct("telegraf ")

    # ── Test 6: Series cardinality
    print("  [6] Series cardinality...", flush=True)
    series = c.influxql("SHOW SERIES FROM battery_cell")
    r["series_count"] = len(series["values"]) if series["values"] else None
    for tag in ("site_id", "rack_id", "module_id", "cell_id"):
        tv = c.influxql(f"SHOW TAG VALUES FROM battery_cell WITH KEY = {tag}")
        r[f"tag_{tag}"] = [v[1] for v in tv["values"]] if tv["values"] else []
        r[f"tag_{tag}_count"] = len(r[f"tag_{tag}"])

    # ── Test 7: Known failure mode signatures
    print("  [7] Error pattern scan...", flush=True)
    r["err_nested_payload"]  = journalctl_has("telegraf", "doesn't exist")
    r["err_qos_drop"]        = flashmq_log_has("Dropping QoS message")
    r["err_consumer_stall"]  = (
        r["err_qos_drop"] > 0 and
        r["mqtt_total"] is not None and snap1 is not None and
        snap2 == snap1  # counter didn't move during sample
    )

    # Asset count: number of active series ≈ series_count (fallback: computed)
    r["asset_count"] = r["series_count"]

    # Buffer fill time (seconds at current rate)
    BUFFER_LIMIT = 100_000
    if r["field_writes_per_sec"] and r["field_writes_per_sec"] > 0:
        r["buffer_fill_sec"] = round(BUFFER_LIMIT / r["field_writes_per_sec"])
    else:
        r["buffer_fill_sec"] = None

    return r


# ── Formatting helpers ──────────────────────────────────────────────────────────

def fmt_num(v, default="—"):
    if v is None:
        return default
    if isinstance(v, float):
        return f"{v:,.1f}"
    return f"{v:,}"

def fmt_mb(kb, default="—"):
    if kb is None:
        return default
    return f"{kb / 1024:.0f} MB"

def fmt_rate(v, default="—"):
    if v is None:
        return default
    return f"~{v:,.0f}/sec"


# ── Markdown generation ────────────────────────────────────────────────────────

def build_markdown(r):
    arch_label = "aarch64" if "aarch64" in r["arch"] or "arm" in r["arch"].lower() else "x86_64"
    arch_note  = "(spark-22b6)" if arch_label == "aarch64" else "(phil-dev)"
    peer_doc   = "telegraf_influx_details_x86.md" if arch_label == "aarch64" else "telegraf_influx_details.md"

    lines = [
        f"# Telegraf / InfluxDB — Live Performance Measurements ({arch_label})",
        "",
        f"**Test date:** {r['test_date']}",
        f"**Environment:** {r['hostname']} {arch_note} ({arch_label}), single node",
        f"**InfluxDB:** {r['influxd_ver']} | **Telegraf:** {r['telegraf_ver']}",
        f"**Active assets:** {fmt_num(r['asset_count'])} battery series",
        f"**Sample interval:** 1 second per asset",
        "",
        f"> See also: `{peer_doc}` for the peer architecture results.",
        "",
        "All measurements taken against the live running system via the InfluxDB HTTP API.",
        "",
        "---",
        "",
        "## Test 0 — Flux API",
        "",
    ]

    if r["flux_ok"]:
        lines += [
            "**Result:** Flux v2 API returned data. All further tests also run via InfluxQL for consistency.",
            "",
        ]
    else:
        lines += [
            "**Result:** Flux v2 API returned empty results or an error.",
            "",
            f"```\nFlux test query returned: {r['flux_detail']}\n```",
            "",
            "**Impact:** All remaining tests conducted via v1 InfluxQL API (`/query?db=...`).",
            "If dashboards or alerting depend on Flux queries, they may be silently empty.",
            "",
        ]

    lines += [
        "---",
        "",
        "## Test 1 — Write Throughput",
        "",
        "```sql",
        "-- messages_received delta over 30s observation window",
        "SELECT messages_received FROM internal_mqtt_consumer ORDER BY time DESC LIMIT 1",
        "```",
        "",
        f"**Result:** **{fmt_rate(r['msg_per_sec'])}** records (messages), "
        f"**{fmt_num(r['field_writes_per_sec'])}/sec** field writes "
        f"({r['field_count'] or '?'} fields × {fmt_rate(r['msg_per_sec'])}).",
        "",
        "---",
        "",
        "## Test 2 — Lifetime Write Volume",
        "",
        "```sql",
        "SELECT last(metrics_written) FROM internal_write",
        "```",
        "",
        f"**Result:** **{fmt_num(r['metrics_written'])} metrics written** (lifetime total).",
        "",
        "---",
        "",
        "## Test 3 — Data Points in InfluxDB (last hour)",
        "",
        "```sql",
        "SELECT count(voltage) FROM battery_cell WHERE time > now()-1h",
        "```",
        "",
        f"**Result:** **{fmt_num(r['voltage_points_1h'])} voltage points** in the last hour.",
        f"With {r['field_count'] or '?'} fields: "
        f"**~{fmt_num(r['all_fields_1h'])} total field values/hour**.",
        "",
        "---",
        "",
        "## Test 4 — Dropped Metrics",
        "",
        "```sql",
        "SELECT last(metrics_dropped) FROM internal_write",
        "```",
        "",
        f"**Result:** **{fmt_num(r['metrics_dropped'])} dropped metrics**.",
        "",
    ]

    if r["buffer_fill_sec"] is not None:
        lines += [
            f"Buffer fill time at current rate: **{r['buffer_fill_sec']} seconds** "
            "(100k metric buffer ÷ current field write rate).",
            "",
        ]

    lines += [
        "---",
        "",
        "## Test 5 — Process Resource Usage",
        "",
        "Measured via `ps aux`:",
        "",
        "| Process | CPU% | RSS Memory |",
        "|---------|-----:|----------:|",
        f"| `influxd` | {r['influxd_cpu_pct'] or '—'}% | {fmt_mb(r['influxd_rss_kb'])} |",
        f"| `telegraf` | {r['telegraf_cpu_pct'] or '—'}% | {fmt_mb(r['telegraf_rss_kb'])} |",
        "",
        "---",
        "",
        "## Test 6 — Series Cardinality",
        "",
        "```sql",
        "SHOW SERIES FROM battery_cell",
        "SHOW TAG VALUES FROM battery_cell WITH KEY = site_id  -- (repeated per tag)",
        "```",
        "",
        f"**Result:** **{fmt_num(r['series_count'])} unique series**.",
        "",
        "| Tag | Unique values |",
        "|-----|:---:|",
        f"| site_id | {r['tag_site_id_count']} |",
        f"| rack_id | {r['tag_rack_id_count']} |",
        f"| module_id | {r['tag_module_id_count']} |",
        f"| cell_id | {r['tag_cell_id_count']} |",
        "",
        "---",
        "",
        "## Test 7 — Known Failure Mode Signatures",
        "",
        "Scans journalctl (telegraf) and FlashMQ log for known silent failure patterns.",
        "",
        "| Failure mode | Detected |",
        "|---|:---:|",
        f"| Nested payload parse errors | {'⚠ YES' if r['err_nested_payload'] else '✓ none'} |",
        f"| FlashMQ QoS drop warnings | {'⚠ YES' if r['err_qos_drop'] else '✓ none'} |",
        f"| Consumer stall (counter frozen during sample) | {'⚠ YES' if r['err_consumer_stall'] else '✓ none'} |",
        "",
        "---",
        "",
        "## Summary",
        "",
        "| Finding | Value |",
        "|---------|-------|",
        f"| Write rate (records/sec) | {fmt_rate(r['msg_per_sec'])} |",
        f"| Field writes/sec | {fmt_num(r['field_writes_per_sec'])} |",
        f"| Lifetime writes | {fmt_num(r['metrics_written'])} |",
        f"| Dropped metrics | {fmt_num(r['metrics_dropped'])} |",
        f"| InfluxDB RAM | {fmt_mb(r['influxd_rss_kb'])} |",
        f"| Telegraf RAM | {fmt_mb(r['telegraf_rss_kb'])} |",
        f"| Buffer headroom | {(str(r['buffer_fill_sec']) + ' sec') if r['buffer_fill_sec'] else '—'} |",
        f"| Series count | {fmt_num(r['series_count'])} |",
        f"| Flux v2 API | {'Working' if r['flux_ok'] else 'Non-functional — silent empty results'} |",
        f"| Nested payload parse errors | {'DETECTED' if r['err_nested_payload'] else 'none detected'} |",
        f"| QoS drop / consumer stall | {'DETECTED' if r['err_qos_drop'] else 'none detected'} |",
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
  display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
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
td.num    { text-align: right; font-variant-numeric: tabular-nums; color: #58a6ff; }
td.red    { color: #f85149; font-weight: 700; }
td.orange { color: #f0883e; font-weight: 700; }
td.green  { color: #3fb950; }
td.hi     { color: #e6edf3; font-weight: 600; }
td.muted  { color: #8b949e; font-style: italic; }
.section  { margin-bottom: 48px; }
.nav { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 32px; }
.nav a {
  padding: 5px 12px; border-radius: 5px; font-size: 0.82em;
  background: #161b22; border: 1px solid #30363d; color: #8b949e;
}
.nav a:hover { border-color: #58a6ff; color: #58a6ff; text-decoration: none; }
.badge-fail { display: inline-block; background: #3d1010; color: #f85149;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px; margin-left: 6px; }
.badge-pass { display: inline-block; background: #0d2d1a; color: #3fb950;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px; margin-left: 6px; }
.badge-warn { display: inline-block; background: #2d1f0e; color: #f0883e;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px; margin-left: 6px; }
.test-number {
  display: inline-block; background: #1f3a6e; color: #79c0ff;
  font-size: 0.72em; font-weight: 700; padding: 2px 8px; border-radius: 4px;
  margin-right: 6px; vertical-align: middle; letter-spacing: 0.05em;
}
""".strip()


def card(label, value, cls="", sub=""):
    return (f'<div class="result-card">'
            f'<div class="label">{label}</div>'
            f'<div class="value {cls}">{value}</div>'
            + (f'<div class="sub">{sub}</div>' if sub else "") +
            f'</div>')


def build_html(r):  # noqa: C901
    arch_label  = "aarch64" if "aarch64" in r["arch"] or "arm" in r["arch"].lower() else "x86_64"
    peer_html   = ("telegraf_influx_details_x86.html" if arch_label == "aarch64"
                   else "telegraf_influx_details.html")
    peer_label  = "x86_64 results" if arch_label == "aarch64" else "aarch64 results"

    flux_badge  = ('<span class="badge-pass">WORKING</span>' if r["flux_ok"]
                   else '<span class="badge-fail">NON-FUNCTIONAL</span>')

    # ── Fleet projection (6× and 6×4× the measured asset count) ──────────────
    rate  = r["field_writes_per_sec"] or 0
    rss   = (r["influxd_rss_kb"] or 0) / 1024  # MB
    assets= r["asset_count"] or 1
    buf   = r["buffer_fill_sec"]

    def scale(factor):
        scaled_rate = rate * factor
        scaled_rss  = rss * factor
        fill        = round(100_000 / scaled_rate) if scaled_rate > 0 else None
        fill_str    = f"{fill} sec" if fill else "—"
        fill_cls    = "red" if (fill and fill < 30) else ("orange" if (fill and fill < 120) else "")
        return scaled_rate, scaled_rss, fill_str, fill_cls

    rate6, rss6, fill6, fill6_cls   = scale(6)
    rate24, rss24, fill24, fill24_cls = scale(24)

    # ── Error pattern callouts ─────────────────────────────────────────────────
    err_sections = ""

    if r["err_nested_payload"]:
        err_sections += f"""
<h3><span class="badge-fail">FAILURE MODE 1</span> &nbsp;Nested Payload — Error Suppression</h3>
<p>Telegraf detected JSON paths that don't exist (e.g. <code>soh</code> as a nested object
instead of a flat scalar). The error was logged {r["err_nested_payload"]} time(s) then suppressed.
All subsequent messages fail silently — 0 writes, 0 <code>metrics_dropped</code> increment.</p>
<div class="callout danger">
  <strong>Silent data loss.</strong> Once suppressed, no log entry, no metric, no Grafana gap indicator.
  Fix: ensure generator uses <code>payload_format: flat</code>, then restart telegraf.
</div>
"""

    if r["err_qos_drop"]:
        err_sections += f"""
<h3><span class="badge-fail">FAILURE MODE 2</span> &nbsp;QoS Queue Overflow → Consumer Stall</h3>
<p>FlashMQ logged {r["err_qos_drop"]} QoS drop warning(s) for <code>telegraf-battery-consumer</code>.
After this event, the Telegraf MQTT consumer may stall — the <code>messages_received</code> counter
freezes while Telegraf appears healthy (connected, writing internal metrics).</p>
<div class="callout danger">
  <strong>Self-healing: No.</strong> Only <code>sudo systemctl restart telegraf</code> recovers the consumer.
  Observable only by watching <code>messages_received</code> in internal metrics — not visible in Grafana
  without a custom alert.
</div>
"""

    if not r["err_nested_payload"] and not r["err_qos_drop"] and not r["err_consumer_stall"]:
        err_sections = """
<div class="callout ok">
  <strong>No known failure mode signatures detected</strong> in the current log window (last 72h).
  Nested payload errors, QoS drops, and consumer stalls were all absent.
</div>
"""

    # ── Pre-compute all conditional blocks (avoid nested f-strings) ───────────
    flux_section = (
        "<div class='callout ok'><strong>Flux API is working.</strong> "
        "Data returned from the v2 query endpoint.</div>"
        if r["flux_ok"] else
        "<div class='callout danger'>"
        "<strong>Flux API returned no data or error: "
        + str(r["flux_detail"]) +
        "</strong><br>All tests below use the v1 InfluxQL API. "
        "If Grafana dashboards use Flux queries on this host, "
        "they are silently returning empty results.</div>"
    )

    buf_warning = (
        "<div class='callout warn'><strong>Buffer headroom is thin.</strong> "
        "At current write rate the 100k Telegraf buffer fills in "
        + str(buf) + " seconds if InfluxDB becomes unavailable.</div>"
        if buf and buf < 120 else ""
    )

    buf_str     = (str(buf) + " sec") if buf else "—"
    buf_cls     = "danger" if (buf and buf < 30) else ("warn" if (buf and buf < 120) else "")
    buf_row_cls = "orange" if (buf and buf < 120) else ""
    rss6_cls    = "red" if rss6 > 4000 else ("orange" if rss6 > 2000 else "")

    parse_err_td = (
        '<td class="red">⚠ ' + str(r["err_nested_payload"]) + ' occurrences</td>'
        if r["err_nested_payload"] else '<td class="green">✓ clean</td>'
    )
    qos_err_td = (
        '<td class="red">⚠ ' + str(r["err_qos_drop"]) + ' occurrences</td>'
        if r["err_qos_drop"] else '<td class="green">✓ clean</td>'
    )
    stall_td = (
        '<td class="red">⚠ STALLED</td>'
        if r["err_consumer_stall"] else '<td class="green">✓ active</td>'
    )

    assets6_str  = fmt_num(r["asset_count"] * 6  if r["asset_count"] else None)
    assets24_str = fmt_num(r["asset_count"] * 24 if r["asset_count"] else None)
    points_day   = fmt_num(round((r["all_fields_1h"] or 0) * 24))
    points_day6  = fmt_num(round((r["all_fields_1h"] or 0) * 24 * 6))
    points_day24 = fmt_num(round((r["all_fields_1h"] or 0) * 24 * 24))

    flux_label = "WORKING" if r["flux_ok"] else "BROKEN"
    flux_cls   = "" if r["flux_ok"] else "danger"
    parse_label= "DETECTED" if r["err_nested_payload"] else "none"
    parse_cls  = "danger" if r["err_nested_payload"] else ""
    qos_label  = "DETECTED" if r["err_qos_drop"] else "none"
    qos_cls    = "danger" if r["err_qos_drop"] else ""
    drop_cls   = "danger" if (r["metrics_dropped"] or 0) > 0 else ""
    rss_cls    = "warn" if (r["influxd_rss_kb"] or 0) > 1_000_000 else ""
    fc_str     = str(r["field_count"] or "?")
    bucket_str = r["bucket"]

    # ── Assemble HTML ──────────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Telegraf / InfluxDB — Live Measurements ({arch_label})</title>
<style>{CSS}</style>
</head>
<body>

<div class="nav">
  <a href="dashboard.html">Dashboard</a>
  <a href="{peer_html}">{peer_label}</a>
  <a href="subscriber.html">Subscriber</a>
  <a href="generator.html">Generator</a>
</div>

<h1>Telegraf / InfluxDB — Live Performance Measurements
  <small style="font-size:0.55em;color:#8b949e">({arch_label})</small></h1>
<p class="subtitle">
  Empirical measurements taken {r["test_date"]} on {r["hostname"]} ({arch_label}).
  Compare with <a href="{peer_html}">{peer_label}</a>.
</p>

<div class="meta-bar">
  <div class="meta-item"><strong>Host</strong> {r["hostname"]}</div>
  <div class="meta-item"><strong>Arch</strong> {arch_label}</div>
  <div class="meta-item"><strong>InfluxDB</strong> {r["influxd_ver"]}</div>
  <div class="meta-item"><strong>Telegraf</strong> {r["telegraf_ver"]}</div>
  <div class="meta-item"><strong>Active series</strong> {fmt_num(r["asset_count"])}</div>
  <div class="meta-item"><strong>Flux v2 API</strong> {flux_badge}</div>
</div>

<!-- ═══════════ TEST 0 — FLUX API -->
<div class="section">
<h2><span class="test-number">TEST 0</span> Flux v2 API {flux_badge}</h2>
<pre><code>from(bucket: "{bucket_str}")
  |> range(start: -1h) |> first() |> limit(n: 1)</code></pre>
{flux_section}
</div>

<!-- ═══════════ TEST RESULTS -->
<div class="section">
<h2>Test Results</h2>

<h3><span class="test-number">TEST 1</span> Write Throughput</h3>
<div class="result-grid">
  {card("Records / sec", fmt_rate(r["msg_per_sec"]).lstrip("~"), "blue")}
  {card("Field writes / sec", fmt_num(r["field_writes_per_sec"]), "blue", fc_str + " fields per record")}
  {card("Active assets", fmt_num(r["asset_count"]), "")}
</div>

<h3><span class="test-number">TEST 2</span> Lifetime Writes</h3>
<div class="result-grid">
  {card("Total metrics written", fmt_num(r["metrics_written"]), "blue", "since last Telegraf start")}
</div>

<h3><span class="test-number">TEST 3</span> Hourly Point Count</h3>
<div class="result-grid">
  {card("Voltage points (1h)", fmt_num(r["voltage_points_1h"]), "", "1 field proxy")}
  {card("All fields (1h)", fmt_num(r["all_fields_1h"]), "warn", fc_str + " fields × voltage count")}
</div>

<h3><span class="test-number">TEST 4</span> Dropped Metrics</h3>
<div class="result-grid">
  {card("Dropped metrics", fmt_num(r["metrics_dropped"]), drop_cls)}
  {card("Buffer fill time", buf_str, buf_cls, "at current rate (100k buffer)")}
</div>
{buf_warning}

<h3><span class="test-number">TEST 5</span> Process Resources</h3>
<div class="result-grid">
  {card("influxd RSS", fmt_mb(r["influxd_rss_kb"]), rss_cls)}
  {card("telegraf RSS", fmt_mb(r["telegraf_rss_kb"]), "")}
  {card("influxd CPU%", str(r["influxd_cpu_pct"] or "—") + "%", "")}
  {card("telegraf CPU%", str(r["telegraf_cpu_pct"] or "—") + "%", "")}
</div>

<h3><span class="test-number">TEST 6</span> Series Cardinality</h3>
<div class="result-grid">
  {card("Unique series", fmt_num(r["series_count"]), "blue")}
  {card("Sites", str(r["tag_site_id_count"]), "blue")}
  {card("Racks", str(r["tag_rack_id_count"]), "blue")}
  {card("Modules", str(r["tag_module_id_count"]), "blue")}
  {card("Cells/module", str(r["tag_cell_id_count"]), "blue")}
</div>
</div>

<!-- ═══════════ FAILURE MODES -->
<div class="section">
<h2>Test 7 — Known Silent Failure Mode Signatures</h2>
<p>Scans <code>journalctl -u telegraf</code> and <code>/var/log/flashmq/flashmq.log</code>
for patterns associated with known data-loss events (last 72h).</p>

<table>
  <thead><tr><th>Failure mode</th><th>Pattern checked</th><th>Status</th></tr></thead>
  <tbody>
    <tr>
      <td>Nested payload / parse error suppression</td>
      <td><code>doesn't exist</code> in telegraf log</td>
      {parse_err_td}
    </tr>
    <tr>
      <td>QoS queue overflow</td>
      <td><code>Dropping QoS message</code> in FlashMQ log</td>
      {qos_err_td}
    </tr>
    <tr>
      <td>Consumer stall (counter frozen during sample)</td>
      <td><code>messages_received</code> delta = 0 over sample window</td>
      {stall_td}
    </tr>
  </tbody>
</table>

{err_sections}
</div>

<!-- ═══════════ FLEET PROJECTION -->
<div class="section">
<h2>Fleet Scaling Projection</h2>
<p>Extrapolated from {fmt_num(r["asset_count"])} measured assets
({fmt_rate(r["field_writes_per_sec"])} field writes/sec,
{fmt_mb(r["influxd_rss_kb"])} influxd RAM).</p>

<table>
  <thead>
    <tr>
      <th>Metric</th>
      <th>{fmt_num(r["asset_count"])} assets<br><small>measured</small></th>
      <th>{assets6_str} assets<br><small>6× scale</small></th>
      <th>{assets24_str} assets<br><small>24× scale</small></th>
    </tr>
  </thead>
  <tbody>
    <tr><td>Field writes/sec</td>
        <td class="num">{fmt_num(rate)}</td>
        <td class="num">{fmt_num(round(rate6))}</td>
        <td class="num">{fmt_num(round(rate24))}</td></tr>
    <tr><td>Buffer fill time</td>
        <td class="num {buf_row_cls}">{buf} sec</td>
        <td class="num {fill6_cls}">{fill6}</td>
        <td class="num {fill24_cls}">{fill24}</td></tr>
    <tr><td>InfluxDB RAM</td>
        <td class="num">{fmt_mb(r["influxd_rss_kb"])}</td>
        <td class="num {rss6_cls}">{rss6:.0f} MB</td>
        <td class="num red">{rss24:.0f} MB</td></tr>
    <tr><td>Points/day (all fields)</td>
        <td class="num">{points_day}</td>
        <td class="num">{points_day6}</td>
        <td class="num">{points_day24}</td></tr>
  </tbody>
</table>
</div>

<!-- ═══════════ SUMMARY -->
<div class="section">
<h2>Summary</h2>
<div class="result-grid">
  {card("Write rate", fmt_rate(r["msg_per_sec"]), "blue", "records/sec")}
  {card("Field writes/sec", fmt_num(r["field_writes_per_sec"]), "blue")}
  {card("InfluxDB RAM", fmt_mb(r["influxd_rss_kb"]), rss_cls)}
  {card("Dropped metrics", fmt_num(r["metrics_dropped"]), drop_cls)}
  {card("Buffer headroom", buf_str, buf_cls)}
  {card("Flux v2 API", flux_label, flux_cls)}
  {card("Parse errors", parse_label, parse_cls)}
  {card("QoS drops", qos_label, qos_cls)}
</div>
</div>

</body>
</html>
"""


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Telegraf/InfluxDB live test runner")
    ap.add_argument("--host",        default="localhost",        help="InfluxDB host")
    ap.add_argument("--port",        default=8086,  type=int,   help="InfluxDB port")
    ap.add_argument("--token",       default="battery-token-secret")
    ap.add_argument("--org",         default="battery-org")
    ap.add_argument("--bucket",      default="battery-data")
    ap.add_argument("--sample-secs", default=30,   type=int,   help="Write rate sample window (s)")
    ap.add_argument("--docs-dir",    default=str(DEFAULT_DOCS_DIR), help="Output dir for .md file")
    ap.add_argument("--html-dir",    default=str(DEFAULT_HTML_DIR), help="Output dir for .html file")
    ap.add_argument("--arch",        default=None,               help="Override arch label (aarch64|x86_64)")
    ap.add_argument("--dry-run",     action="store_true",        help="Print results, don't write files")
    args = ap.parse_args()

    client = InfluxClient(args.host, args.port, args.token, args.org, args.bucket)

    # Quick connectivity check
    print(f"Connecting to InfluxDB at {args.host}:{args.port}...", flush=True)
    check = client.influxql("SHOW DATABASES")
    if not check["ok"]:
        print(f"ERROR: Cannot reach InfluxDB — {check.get('error', 'unknown')}", file=sys.stderr)
        sys.exit(1)
    print("Connected.\n", flush=True)

    print(f"Running measurements ({args.sample_secs}s rate sample)...")
    results = measure(client, sample_secs=args.sample_secs)

    # Allow arch override
    if args.arch:
        results["arch"] = args.arch

    arch_label = ("aarch64" if "aarch64" in results["arch"] or "arm" in results["arch"].lower()
                  else "x86_64")
    suffix = "" if arch_label == "aarch64" else "_x86"
    md_name   = f"telegraf_influx_details{suffix}.md"
    html_name = f"telegraf_influx_details{suffix}.html"

    print(f"\nArch: {arch_label}  →  {md_name} / {html_name}")
    print(f"  Write rate:     {fmt_rate(results['msg_per_sec'])}")
    print(f"  Field writes:   {fmt_num(results['field_writes_per_sec'])}/sec")
    print(f"  Lifetime total: {fmt_num(results['metrics_written'])}")
    print(f"  Dropped:        {fmt_num(results['metrics_dropped'])}")
    print(f"  influxd RSS:    {fmt_mb(results['influxd_rss_kb'])}")
    print(f"  telegraf RSS:   {fmt_mb(results['telegraf_rss_kb'])}")
    print(f"  Flux API:       {'working' if results['flux_ok'] else 'NON-FUNCTIONAL'}")
    print(f"  Parse errors:   {results['err_nested_payload'] or 'none'}")
    print(f"  QoS drops:      {results['err_qos_drop'] or 'none'}")

    if args.dry_run:
        print("\n[dry-run] Files not written.")
        return

    docs_dir = Path(args.docs_dir)
    html_dir = Path(args.html_dir)
    docs_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)

    md_path   = docs_dir / md_name
    html_path = html_dir / html_name

    md_path.write_text(build_markdown(results))
    html_path.write_text(build_html(results))

    print(f"\nWrote: {md_path}")
    print(f"Wrote: {html_path}")


if __name__ == "__main__":
    main()
