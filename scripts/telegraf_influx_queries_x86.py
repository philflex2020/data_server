#!/usr/bin/env python3
"""
telegraf_influx_queries_x86.py — Simulated web-client query loop against InfluxDB2.

Runs a representative dashboard query set every 10 seconds for a configurable
duration, measuring response time and result freshness.  Generates an MD + HTML
report showing each query, timings, and sample data.

Usage:
    python scripts/telegraf_influx_queries_x86.py
    python scripts/telegraf_influx_queries_x86.py --duration 120 --interval 10
    python scripts/telegraf_influx_queries_x86.py --host 192.168.86.51
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT   = SCRIPT_DIR.parent
DOCS_DIR    = REPO_ROOT / "docs"
HTML_DIR    = REPO_ROOT / "html"

# ── InfluxDB query helper ──────────────────────────────────────────────────────

class InfluxClient:
    def __init__(self, host, port, token, org, bucket):
        self.base   = f"http://{host}:{port}"
        self.token  = token
        self.org    = org
        self.bucket = bucket

    def influxql(self, query, epoch="s"):
        url = (f"{self.base}/query"
               f"?db={urllib.parse.quote(self.bucket)}"
               f"&epoch={epoch}"
               f"&q={urllib.parse.quote(query)}")
        req = urllib.request.Request(url, headers={"Authorization": f"Token {self.token}"})
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                body = r.read()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                data = json.loads(body)
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return {"ok": False, "error": str(e), "elapsed_ms": elapsed_ms,
                    "columns": [], "values": [], "row_count": 0}

        result = data.get("results", [{}])[0]
        if "error" in result:
            return {"ok": False, "error": result["error"], "elapsed_ms": elapsed_ms,
                    "columns": [], "values": [], "row_count": 0}

        series = result.get("series", [])
        if not series:
            return {"ok": True, "elapsed_ms": elapsed_ms,
                    "columns": [], "values": [], "row_count": 0}

        # Flatten multi-series results
        all_values = []
        columns    = series[0].get("columns", [])
        for s in series:
            tags = s.get("tags", {})
            for row in s.get("values", []):
                all_values.append((tags, row))

        return {
            "ok":         True,
            "elapsed_ms": elapsed_ms,
            "columns":    columns,
            "values":     all_values,       # list of (tags_dict, row_list)
            "row_count":  len(all_values),
        }


# ── Query definitions ──────────────────────────────────────────────────────────

QUERIES = [
    {
        "id":    "Q1",
        "title": "Latest snapshot — all battery cells, site 0 (last 5 min)",
        "desc":  "Web dashboard: 'current status' panel — last reading per cell.",
        "sql":   ("SELECT last(voltage), last(current), last(temperature), last(soc), last(soh) "
                  "FROM battery_cell "
                  "WHERE time > now()-5m AND site_id='0' AND source_id='battery' "
                  "GROUP BY site_id, rack_id, module_id, cell_id"),
    },
    {
        "id":    "Q2",
        "title": "Time series — single cell history (last 5 min)",
        "desc":  "Web dashboard: 'cell detail' chart — voltage/temp trend.",
        "sql":   ("SELECT voltage, current, temperature, soc "
                  "FROM battery_cell "
                  "WHERE time > now()-5m "
                  "  AND site_id='0' AND rack_id='0' AND module_id='0' AND cell_id='0' "
                  "ORDER BY time DESC LIMIT 300"),
    },
    {
        "id":    "Q3",
        "title": "Rack aggregates — mean voltage & temperature per rack (last 5 min, 1-min buckets)",
        "desc":  "Web dashboard: 'rack health' heatmap — averaged per 1-minute window.",
        "sql":   ("SELECT mean(voltage), mean(temperature), mean(soc) "
                  "FROM battery_cell "
                  "WHERE time > now()-5m AND source_id='battery' "
                  "GROUP BY site_id, rack_id, time(1m) "
                  "FILL(none)"),
    },
    {
        "id":    "Q4",
        "title": "Alert scan — cells with voltage outside 3.2–4.1 V (last 5 min)",
        "desc":  "Web dashboard: alert panel — any cell outside safe voltage window.",
        "sql":   ("SELECT last(voltage) "
                  "FROM battery_cell "
                  "WHERE time > now()-5m AND source_id='battery' "
                  "GROUP BY site_id, rack_id, module_id, cell_id"),
        "post_filter": lambda vals: [
            (tags, row) for tags, row in vals
            if row and row[-1] is not None and (row[-1] < 3.2 or row[-1] > 4.1)
        ],
        "post_filter_label": "filtered to voltage < 3.2 V or > 4.1 V",
    },
    {
        "id":    "Q5",
        "title": "Solar overview — last irradiance & AC power, all panels (last 5 min)",
        "desc":  "Web dashboard: 'solar fleet' overview panel.",
        "sql":   ("SELECT last(irradiance), last(ac_power), last(efficiency) "
                  "FROM battery_cell "
                  "WHERE time > now()-5m AND source_id='solar' "
                  "GROUP BY site_id, rack_id, module_id, cell_id"),
    },
    {
        "id":    "Q6",
        "title": "Fleet health — active cell count & data freshness (last 1 min)",
        "desc":  "Web dashboard: 'system health' badge — confirm data is flowing.",
        "sql":   ("SELECT count(voltage) FROM battery_cell "
                  "WHERE time > now()-1m AND source_id='battery' "
                  "GROUP BY site_id"),
    },
]


# ── Query runner ───────────────────────────────────────────────────────────────

def run_cycle(client):
    """Execute all queries once.  Returns list of result dicts."""
    results = []
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    for qdef in QUERIES:
        res = client.influxql(qdef["sql"])
        post = qdef.get("post_filter")
        filtered_count = None
        if post and res["ok"]:
            filtered_count = len(post(res["values"]))
        results.append({
            "id":             qdef["id"],
            "title":          qdef["title"],
            "desc":           qdef["desc"],
            "sql":            qdef["sql"],
            "ok":             res["ok"],
            "elapsed_ms":     res["elapsed_ms"],
            "row_count":      res["row_count"],
            "filtered_count": filtered_count,
            "post_label":     qdef.get("post_filter_label", ""),
            "columns":        res["columns"],
            "sample_rows":    res["values"][:5],   # first 5 for display
            "error":          res.get("error", ""),
            "ts":             ts,
        })
    return results


def run_loop(client, duration_secs, interval_secs):
    """Run query cycles until duration_secs elapsed.  Returns list of cycle results."""
    all_cycles = []
    t_end = time.time() + duration_secs
    cycle  = 0
    while time.time() < t_end:
        cycle += 1
        remaining = max(0, t_end - time.time())
        print(f"  Cycle {cycle} ({remaining:.0f}s remaining)...", flush=True)
        cycle_results = run_cycle(client)
        all_cycles.append(cycle_results)
        for r in cycle_results:
            status = f"OK  {r['elapsed_ms']:6.1f}ms  {r['row_count']:4d} rows"
            if r["filtered_count"] is not None:
                status += f"  ({r['filtered_count']} after filter)"
            if not r["ok"]:
                status = f"ERR {r['error'][:60]}"
            print(f"    {r['id']}: {status}", flush=True)
        # Wait for next interval (minus elapsed time this cycle)
        elapsed = sum(r["elapsed_ms"] for r in cycle_results) / 1000
        sleep_t = max(0, interval_secs - elapsed)
        if sleep_t > 0 and time.time() < t_end:
            time.sleep(sleep_t)
    return all_cycles


# ── Stats aggregation ──────────────────────────────────────────────────────────

def aggregate(all_cycles):
    """Per-query stats across all cycles."""
    stats = {}
    for qdef in QUERIES:
        qid = qdef["id"]
        times  = [c_res["elapsed_ms"] for cycle in all_cycles
                  for c_res in cycle if c_res["id"] == qid and c_res["ok"]]
        counts = [c_res["row_count"] for cycle in all_cycles
                  for c_res in cycle if c_res["id"] == qid and c_res["ok"]]
        errors = [c_res["error"] for cycle in all_cycles
                  for c_res in cycle if c_res["id"] == qid and not c_res["ok"]]
        stats[qid] = {
            "n":          len(times),
            "avg_ms":     round(sum(times) / len(times), 1) if times else None,
            "min_ms":     round(min(times), 1) if times else None,
            "max_ms":     round(max(times), 1) if times else None,
            "avg_rows":   round(sum(counts) / len(counts), 1) if counts else 0,
            "errors":     len(errors),
        }
    # Last cycle sample rows for display
    last_cycle = all_cycles[-1] if all_cycles else []
    return stats, last_cycle


def compute_query_costs(stats, interval_secs, n_clients=1, scale_factor=1):
    """
    Estimate AWS query costs from measured row counts and latencies.

    Assumptions:
      - Each returned row ≈ 5 fields × 8 bytes = 40 bytes returned
      - Timestream scans ~20 bytes per point in magnetic store, typically
        scans 10–20× more data than returned (GROUP BY + time range)
      - InfluxDB Cloud charges ~$0.008/MB of line-protocol data *read* (rough)
      - Timestream: $0.01/GB scanned
      - One cycle = all 6 queries fired once
    """
    # Estimated bytes scanned per cycle (returned rows × scan_multiplier)
    SCAN_MULTIPLIER = 15        # GROUP BY queries scan ~15× the returned bytes
    BYTES_PER_ROW   = 40        # ~5 fields × 8 bytes

    total_rows_per_cycle = sum(s["avg_rows"] for s in stats.values()) * scale_factor
    bytes_scanned_cycle  = total_rows_per_cycle * BYTES_PER_ROW * SCAN_MULTIPLIER
    mb_scanned_cycle     = bytes_scanned_cycle / (1024 * 1024)

    # Number of cycles per month
    cycles_per_day   = (86_400 // interval_secs)
    cycles_per_month = cycles_per_day * 30 * n_clients

    gb_scanned_month = (mb_scanned_cycle / 1024) * cycles_per_month

    # Timestream query cost: $0.01/GB scanned
    timestream_query_cost = gb_scanned_month * 0.01

    # InfluxDB Cloud: rough equivalent — $0.008/MB read
    influx_cloud_query_cost = (mb_scanned_cycle * cycles_per_month) * 0.008 / 1024  # $/GB equivalent

    # EC2 CPU impact: Q3+Q5 are the heavy queries (GROUP BY aggregations)
    heavy_ms = (stats.get("Q3", {}).get("avg_ms") or 0) + (stats.get("Q5", {}).get("avg_ms") or 0)
    heavy_pct_cpu = round((heavy_ms * cycles_per_day / n_clients) / (86_400 * 10) * 100, 1)

    return {
        "rows_per_cycle":         round(total_rows_per_cycle),
        "mb_scanned_cycle":       round(mb_scanned_cycle, 2),
        "gb_scanned_month":       round(gb_scanned_month, 1),
        "cycles_per_month":       cycles_per_month,
        "timestream_query_cost":  round(timestream_query_cost, 2),
        "influx_cloud_query_cost":round(influx_cloud_query_cost, 2),
        "heavy_ms":               round(heavy_ms, 1),
        "heavy_cpu_pct":          heavy_pct_cpu,
        "n_clients":              n_clients,
        "scale_factor":           scale_factor,
    }


# ── Formatting helpers ─────────────────────────────────────────────────────────

def fmt_ms(v):
    return f"{v:.1f} ms" if v is not None else "—"

def fmt_row(tags, cols, row):
    """Format a single result row with tag context."""
    tag_str = " ".join(f"{k}={v}" for k, v in sorted(tags.items())) if tags else ""
    col_vals = []
    for c, v in zip(cols, row):
        if c == "time":
            col_vals.append(f"t={v}")
        elif isinstance(v, float):
            col_vals.append(f"{c}={v:.4f}")
        else:
            col_vals.append(f"{c}={v}")
    return (f"[{tag_str}] " if tag_str else "") + "  ".join(col_vals)


# ── Markdown generation ────────────────────────────────────────────────────────

def _cost_table_md(costs_1x, costs_6x, costs_24x, q3ms=0, q5ms=0):
    c1, c6, c24 = costs_1x, costs_6x, costs_24x
    return "\n".join([
        "## Query Cost Impact on AWS",
        "",
        f"Based on ~{c1['mb_scanned_cycle']:.1f} MB scanned per cycle "
        f"({c1['rows_per_cycle']} rows × 15× scan multiplier).  "
        f"One web client querying every {c1['n_clients'] * 10}s.",
        "",
        f"| Metric | 1× test load | 6× scale | 24× (full fleet) |",
        f"|--------|-------------:|---------:|------------------:|",
        f"| Rows returned / cycle | {c1['rows_per_cycle']:,} | {c6['rows_per_cycle']:,} | {c24['rows_per_cycle']:,} |",
        f"| MB scanned / cycle | {c1['mb_scanned_cycle']:.1f} | {c6['mb_scanned_cycle']:.1f} | {c24['mb_scanned_cycle']:.1f} |",
        f"| GB scanned / month (1 client) | {c1['gb_scanned_month']:.0f} | {c6['gb_scanned_month']:.0f} | {c24['gb_scanned_month']:.0f} |",
        f"| AWS Timestream query cost/mo | **${c1['timestream_query_cost']:.2f}** | **${c6['timestream_query_cost']:.2f}** | **${c24['timestream_query_cost']:.2f}** |",
        f"| InfluxDB Cloud query cost/mo | **${c1['influx_cloud_query_cost']:.2f}** | **${c6['influx_cloud_query_cost']:.2f}** | **${c24['influx_cloud_query_cost']:.2f}** |",
        f"| Heavy query CPU (Q3+Q5) | {c1['heavy_ms']:.0f} ms | — | — |",
        "",
        "> **Per additional web client:** multiply query costs by client count.",
        "> At 80 sites (24×) with 5 concurrent clients, Timestream query costs reach "
        f"**~${c24['timestream_query_cost'] * 5:,.0f}/month** on top of the write charges.",
        f"> Q3 (rack aggregates, {q3ms:.0f} ms) and Q5 (solar overview, {q5ms:.0f} ms) dominate — "
        "GROUP BY over large time windows. Use pre-aggregated continuous queries to reduce scan cost.",
        "",
    ])


def build_markdown(stats, last_cycle, meta):
    lines = [
        "# InfluxDB Query Performance — Simulated Web Client (x86_64)",
        "",
        f"**Test date:** {meta['test_date']}",
        f"**Host:** {meta['host']}  |  **Bucket:** {meta['bucket']}",
        f"**Duration:** {meta['duration']}s  |  "
        f"**Interval:** {meta['interval']}s  |  "
        f"**Cycles:** {meta['cycles']}",
        "",
        "Queries simulate a web dashboard making requests to InfluxDB every 10 seconds.",
        "All queries target the last 5 minutes of data (1-minute aggregates where noted).",
        "",
        "---",
        "",
        "## Query Performance Summary",
        "",
        "| Query | Title | Avg latency | Min | Max | Avg rows | Errors |",
        "|-------|-------|------------:|----:|----:|---------:|-------:|",
    ]
    for qdef in QUERIES:
        qid = qdef["id"]
        s   = stats[qid]
        err_flag = " ⚠" if s["errors"] > 0 else ""
        lines.append(
            f"| {qid} | {qdef['title'][:55]}… | "
            f"{fmt_ms(s['avg_ms'])} | {fmt_ms(s['min_ms'])} | {fmt_ms(s['max_ms'])} | "
            f"{s['avg_rows']} | {s['errors']}{err_flag} |"
        )
    lines += ["", "---", ""]

    # Cost impact section
    c1  = compute_query_costs(stats, meta["interval"], n_clients=1, scale_factor=1)
    c6  = compute_query_costs(stats, meta["interval"], n_clients=1, scale_factor=6)
    c24 = compute_query_costs(stats, meta["interval"], n_clients=1, scale_factor=24)
    q3ms = stats.get("Q3", {}).get("avg_ms") or 0
    q5ms = stats.get("Q5", {}).get("avg_ms") or 0
    lines += [_cost_table_md(c1, c6, c24, q3ms, q5ms), "---", ""]

    for qdef in QUERIES:
        qid  = qdef["id"]
        s    = stats[qid]
        last = next((r for r in last_cycle if r["id"] == qid), None)
        lines += [
            f"## {qid} — {qdef['title']}",
            "",
            f"*{qdef['desc']}*",
            "",
            "```sql",
            qdef["sql"],
            "```",
            "",
            f"**Avg latency:** {fmt_ms(s['avg_ms'])}  |  "
            f"**Rows returned:** ~{s['avg_rows']}  |  "
            f"**Cycles run:** {s['n']}",
            "",
        ]
        if last and last["ok"] and last["sample_rows"]:
            lines += ["**Sample output (last cycle, first 5 rows):**", ""]
            lines += ["```"]
            for tags, row in last["sample_rows"]:
                lines.append(fmt_row(tags, last["columns"], row))
            lines += ["```", ""]
            if last.get("filtered_count") is not None:
                lines.append(f"> After filter ({last['post_label']}): "
                              f"**{last['filtered_count']} rows**")
                lines.append("")
        elif last and not last["ok"]:
            lines += [f"> ⚠ Error: `{last['error']}`", ""]
        else:
            lines += ["> No rows returned.", ""]
        lines += ["---", ""]

    return "\n".join(lines)


# ── HTML generation ────────────────────────────────────────────────────────────

CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Segoe UI', system-ui, sans-serif;
  background: #0d1117; color: #e6edf3;
  padding: 40px 32px; max-width: 1200px; margin: auto; line-height: 1.65;
}
h1   { color: #58a6ff; font-size: 1.8em; margin-bottom: 6px; }
h2   { color: #79c0ff; font-size: 1.1em; margin: 40px 0 14px;
       border-bottom: 1px solid #30363d; padding-bottom: 8px; }
h3   { color: #d2a679; font-size: 0.95em; margin: 20px 0 8px; }
p    { color: #c9d1d9; font-size: 0.88em; margin-bottom: 10px; }
code { color: #f39c12; background: #161b22; padding: 1px 6px; border-radius: 3px; font-size: 0.85em; }
pre  { background: #161b22; border: 1px solid #30363d; border-radius: 6px;
       padding: 14px 16px; overflow-x: auto; margin: 10px 0 16px; }
pre code { background: none; padding: 0; color: #79c0ff; font-size: 0.82em; line-height: 1.6; }
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
table { width: 100%; border-collapse: collapse; font-size: 0.84em; margin: 12px 0 20px; }
thead th {
  background: #161b22; color: #8b949e; text-align: left; padding: 8px 12px;
  font-size: 0.78em; text-transform: uppercase; letter-spacing: 0.06em;
  border-bottom: 1px solid #30363d;
}
tbody td { padding: 8px 12px; color: #c9d1d9; border-bottom: 1px solid #21262d; }
tbody tr:hover td { background: #161b22; }
td.num  { text-align: right; font-variant-numeric: tabular-nums; color: #58a6ff; }
td.fast { color: #3fb950; }
td.slow { color: #f0883e; font-weight: 700; }
td.err  { color: #f85149; font-weight: 700; }
td.hi   { color: #e6edf3; font-weight: 600; }
td.muted { color: #8b949e; font-style: italic; }
.q-card {
  background: #0d1a2a; border: 1px solid #1d3a5c; border-radius: 8px;
  padding: 20px 24px; margin-bottom: 28px;
}
.q-id {
  display: inline-block; background: #1f3a6e; color: #79c0ff;
  font-size: 0.72em; font-weight: 700; padding: 2px 10px; border-radius: 4px;
  margin-right: 8px; vertical-align: middle; letter-spacing: 0.05em;
}
.q-title { color: #e6edf3; font-size: 1.0em; font-weight: 600; vertical-align: middle; }
.q-desc  { color: #8b949e; font-size: 0.84em; margin: 8px 0 14px; }
.stat-row { display: flex; gap: 20px; flex-wrap: wrap; margin: 12px 0; }
.stat { font-size: 0.82em; color: #8b949e; }
.stat strong { color: #58a6ff; font-size: 1.1em; }
.callout {
  border-radius: 6px; padding: 12px 16px; margin: 10px 0; font-size: 0.85em;
}
.callout.ok   { background: #0d2d1a; border-left: 3px solid #3fb950; color: #7fba84; }
.callout.warn { background: #2d1f0e; border-left: 3px solid #f0883e; color: #e3b981; }
.callout.err  { background: #2d0e0e; border-left: 3px solid #f85149; color: #e38181; }
.nav { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 32px; }
.nav a {
  padding: 5px 12px; border-radius: 5px; font-size: 0.82em;
  background: #161b22; border: 1px solid #30363d; color: #8b949e;
}
.nav a:hover { border-color: #58a6ff; color: #58a6ff; text-decoration: none; }
.section { margin-bottom: 48px; }
""".strip()


def _latency_cls(ms):
    if ms is None:    return ""
    if ms < 20:       return "fast"
    if ms > 200:      return "slow"
    return ""


def _cost_section_html(stats, interval_secs):
    c1  = compute_query_costs(stats, interval_secs, n_clients=1, scale_factor=1)
    c6  = compute_query_costs(stats, interval_secs, n_clients=1, scale_factor=6)
    c24 = compute_query_costs(stats, interval_secs, n_clients=1, scale_factor=24)
    q3ms = stats.get("Q3", {}).get("avg_ms") or 0
    q5ms = stats.get("Q5", {}).get("avg_ms") or 0
    return f"""
<div class="section">
<h2>Query Cost Impact on AWS</h2>
<p>Based on ~{c1["mb_scanned_cycle"]:.1f} MB scanned per cycle
({c1["rows_per_cycle"]:,} rows × 15× scan multiplier for GROUP BY queries).
One web client querying every {interval_secs}s.</p>

<table>
  <thead>
    <tr>
      <th>Metric</th>
      <th class="num">1× test load</th>
      <th class="num">6× scale</th>
      <th class="num">24× (full fleet)</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>Rows returned / cycle</td>
        <td class="num">{c1["rows_per_cycle"]:,}</td>
        <td class="num">{c6["rows_per_cycle"]:,}</td>
        <td class="num">{c24["rows_per_cycle"]:,}</td></tr>
    <tr><td>MB scanned / cycle</td>
        <td class="num">{c1["mb_scanned_cycle"]:.1f} MB</td>
        <td class="num">{c6["mb_scanned_cycle"]:.1f} MB</td>
        <td class="num">{c24["mb_scanned_cycle"]:.1f} MB</td></tr>
    <tr><td>GB scanned / month (1 client)</td>
        <td class="num">{c1["gb_scanned_month"]:.0f} GB</td>
        <td class="num">{c6["gb_scanned_month"]:.0f} GB</td>
        <td class="num">{c24["gb_scanned_month"]:.0f} GB</td></tr>
    <tr><td>AWS Timestream query cost / month</td>
        <td class="num hi">${c1["timestream_query_cost"]:.2f}</td>
        <td class="num hi">${c6["timestream_query_cost"]:.2f}</td>
        <td class="num hi">${c24["timestream_query_cost"]:.2f}</td></tr>
    <tr><td>InfluxDB Cloud query cost / month</td>
        <td class="num hi">${c1["influx_cloud_query_cost"]:.2f}</td>
        <td class="num hi">${c6["influx_cloud_query_cost"]:.2f}</td>
        <td class="num hi">${c24["influx_cloud_query_cost"]:.2f}</td></tr>
  </tbody>
</table>

<div class="callout warn">
  <strong>Heavy queries: Q3 ({q3ms:.0f} ms) + Q5 ({q5ms:.0f} ms)</strong> —
  GROUP BY aggregations over all series in a time window. These dominate scan cost.
  At 80 sites with 5 concurrent web clients, Timestream query charges reach
  <strong>~${c24["timestream_query_cost"] * 5:,.0f}/month</strong>
  on top of write charges — on top of an already large bill.
  Pre-aggregated continuous queries or materialized views can reduce scan cost 10–50×.
</div>
<div class="callout ok">
  <strong>Self-hosted Telegraf + InfluxDB:</strong> query load is CPU-only — no per-GB charge.
  Q3+Q5 together take {q3ms + q5ms:.0f} ms per cycle, well within single-node capacity.
</div>
</div>
"""


def build_html(stats, last_cycle, meta):
    # ── Summary table rows
    summary_rows = ""
    for qdef in QUERIES:
        qid = qdef["id"]
        s   = stats[qid]
        lat_cls = _latency_cls(s["avg_ms"])
        err_td  = (f'<td class="num err">{s["errors"]}</td>' if s["errors"]
                   else f'<td class="num fast">0</td>')
        summary_rows += f"""
    <tr>
      <td><span class="q-id">{qid}</span></td>
      <td>{qdef["title"]}</td>
      <td class="num {lat_cls}">{fmt_ms(s["avg_ms"])}</td>
      <td class="num">{fmt_ms(s["min_ms"])}</td>
      <td class="num">{fmt_ms(s["max_ms"])}</td>
      <td class="num">{s["avg_rows"]}</td>
      {err_td}
    </tr>"""

    # ── Per-query detail cards
    cards = ""
    for qdef in QUERIES:
        qid  = qdef["id"]
        s    = stats[qid]
        last = next((r for r in last_cycle if r["id"] == qid), None)
        lat_cls = _latency_cls(s["avg_ms"])

        if last and last["ok"] and last["sample_rows"]:
            sample_lines = "\n".join(
                fmt_row(tags, last["columns"], row)
                for tags, row in last["sample_rows"]
            )
            sample_html = f"<pre><code>{sample_lines}</code></pre>"
            if last.get("filtered_count") is not None:
                sample_html += (
                    f'<div class="callout ok">After filter '
                    f'(<em>{last["post_label"]}</em>): '
                    f'<strong>{last["filtered_count"]} rows matched</strong></div>'
                )
        elif last and not last["ok"]:
            sample_html = (f'<div class="callout err">'
                           f'Error: <code>{last["error"]}</code></div>')
        else:
            sample_html = '<div class="callout warn">No rows returned.</div>'

        cards += f"""
<div class="q-card">
  <div>
    <span class="q-id">{qid}</span>
    <span class="q-title">{qdef["title"]}</span>
  </div>
  <p class="q-desc">{qdef["desc"]}</p>
  <pre><code>{qdef["sql"]}</code></pre>
  <div class="stat-row">
    <div class="stat">Avg latency <strong class="{lat_cls}">{fmt_ms(s["avg_ms"])}</strong></div>
    <div class="stat">Min <strong>{fmt_ms(s["min_ms"])}</strong></div>
    <div class="stat">Max <strong>{fmt_ms(s["max_ms"])}</strong></div>
    <div class="stat">Avg rows <strong>{s["avg_rows"]}</strong></div>
    <div class="stat">Cycles <strong>{s["n"]}</strong></div>
    <div class="stat">Errors <strong class="{'err' if s['errors'] else 'fast'}">{s["errors"]}</strong></div>
  </div>
  <h3>Sample output — last cycle, first 5 rows</h3>
  {sample_html}
</div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>InfluxDB Query Performance — Web Client (x86_64)</title>
<style>{CSS}</style>
</head>
<body>

<div class="nav">
  <a href="dashboard.html">Dashboard</a>
  <a href="telegraf_influx_details_x86.html">Telegraf details</a>
  <a href="subscriber.html">Subscriber</a>
</div>

<h1>InfluxDB — Simulated Web Client Queries</h1>
<p class="subtitle">
  Query loop against live InfluxDB2 instance — {meta["cycles"]} cycles,
  every {meta["interval"]}s, over {meta["duration"]}s.
  All queries target the <strong>last 5 minutes</strong> of data.
</p>

<div class="meta-bar">
  <div class="meta-item"><strong>Date</strong> {meta["test_date"]}</div>
  <div class="meta-item"><strong>Host</strong> {meta["host"]}</div>
  <div class="meta-item"><strong>Bucket</strong> {meta["bucket"]}</div>
  <div class="meta-item"><strong>Duration</strong> {meta["duration"]}s</div>
  <div class="meta-item"><strong>Interval</strong> {meta["interval"]}s</div>
  <div class="meta-item"><strong>Cycles</strong> {meta["cycles"]}</div>
</div>

<div class="section">
<h2>Performance Summary</h2>
<table>
  <thead>
    <tr>
      <th>ID</th><th>Query</th>
      <th class="num">Avg latency</th>
      <th class="num">Min</th>
      <th class="num">Max</th>
      <th class="num">Avg rows</th>
      <th class="num">Errors</th>
    </tr>
  </thead>
  <tbody>{summary_rows}
  </tbody>
</table>
</div>

{_cost_section_html(stats, meta["interval"])}

<div class="section">
<h2>Query Details &amp; Sample Output</h2>
{cards}
</div>

</body>
</html>
"""


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="InfluxDB web-client query simulation")
    ap.add_argument("--host",       default="localhost")
    ap.add_argument("--port",       default=8086,  type=int)
    ap.add_argument("--token",      default="battery-token-secret")
    ap.add_argument("--org",        default="battery-org")
    ap.add_argument("--bucket",     default="battery-data")
    ap.add_argument("--duration",   default=120, type=int, help="Total test duration (s)")
    ap.add_argument("--interval",   default=10,  type=int, help="Seconds between query cycles")
    ap.add_argument("--docs-dir",   default=str(DOCS_DIR))
    ap.add_argument("--html-dir",   default=str(HTML_DIR))
    ap.add_argument("--dry-run",    action="store_true")
    args = ap.parse_args()

    client = InfluxClient(args.host, args.port, args.token, args.org, args.bucket)

    # Connectivity check
    check_url = (f"http://{args.host}:{args.port}/query"
                 f"?db={urllib.parse.quote(args.bucket)}"
                 f"&q={urllib.parse.quote('SHOW DATABASES')}")
    req = urllib.request.Request(check_url,
                                 headers={"Authorization": f"Token {args.token}"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
        print(f"Connected to InfluxDB at {args.host}:{args.port}\n", flush=True)
    except Exception as e:
        print(f"ERROR: Cannot reach InfluxDB — {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Running {len(QUERIES)} queries every {args.interval}s "
          f"for {args.duration}s ({args.duration // args.interval} cycles)...")

    all_cycles = run_loop(client, args.duration, args.interval)
    stats, last_cycle = aggregate(all_cycles)

    meta = {
        "test_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "host":      args.host,
        "bucket":    args.bucket,
        "duration":  args.duration,
        "interval":  args.interval,
        "cycles":    len(all_cycles),
    }

    print(f"\n--- Summary ({meta['cycles']} cycles) ---")
    for qdef in QUERIES:
        s = stats[qdef["id"]]
        print(f"  {qdef['id']}: avg {fmt_ms(s['avg_ms'])}  "
              f"rows ~{s['avg_rows']}  errors {s['errors']}")

    if args.dry_run:
        print("\n[dry-run] Files not written.")
        return

    docs_dir = Path(args.docs_dir)
    html_dir = Path(args.html_dir)
    docs_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)

    md_path   = docs_dir / "telegraf_influx_queries_x86.md"
    html_path = html_dir / "telegraf_influx_queries_x86.html"

    md_path.write_text(build_markdown(stats, last_cycle, meta))
    html_path.write_text(build_html(stats, last_cycle, meta))

    print(f"\nWrote: {md_path}")
    print(f"Wrote: {html_path}")


if __name__ == "__main__":
    main()
