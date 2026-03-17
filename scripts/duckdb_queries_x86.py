#!/usr/bin/env python3
"""
duckdb_queries_x86.py — DuckDB query performance test against live parquet store.

Runs 6 web-client equivalent queries against /home/phil/data/parquet,
mirrors the InfluxDB query test in telegraf_influx_queries_x86.py.

Usage:
    python3 duckdb_queries_x86.py [--base BASE_PATH] [--cycles N] [--interval S]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed — run: pip3 install duckdb")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--base",     default="/home/phil/data/parquet",
                   help="Base parquet path (default: /home/phil/data/parquet)")
    p.add_argument("--cycles",   type=int, default=6,
                   help="Query cycles (default: 6)")
    p.add_argument("--interval", type=float, default=10.0,
                   help="Seconds between cycles (default: 10)")
    p.add_argument("--json",     action="store_true",
                   help="Emit JSON report to stdout")
    return p.parse_args()


def run_query(con, label, sql):
    t0 = time.perf_counter()
    rows = con.execute(sql).fetchall()
    ms = (time.perf_counter() - t0) * 1000
    desc = con.execute(sql).description
    cols = [d[0] for d in desc]
    return {
        "label": label,
        "ms": round(ms, 2),
        "rows": len(rows),
        "data": [dict(zip(cols, r)) for r in rows[:5]],
        "cols": cols,
    }


def date_glob_paths(base, source, project_id, site_id, window_seconds):
    """Return glob paths covering only date dirs that overlap the query window."""
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(seconds=window_seconds)
    paths = []
    d = start.date()
    while d <= now.date():
        paths.append(
            f"{base}/{source}/project={project_id}/site={site_id}"
            f"/{d.year}/{d.month:02d}/{d.day:02d}/*.parquet"
        )
        d += timedelta(days=1)
    return paths


def build_queries(base):
    hot = f"{base}/hot.parquet"
    # Q2/Q3 use date-scoped paths — only scan files in today's (and yesterday's if
    # crossing midnight) date directory rather than the entire site partition.
    bat_today = date_glob_paths(base, "batteries", 0, 0, window_seconds=3600)
    bat_all   = f"{base}/batteries/project=0/**/*.parquet"
    sol = f"{base}/solar/project=0/**/*.parquet"

    return [
        (
            "Q1",
            "Latest snapshot — all battery cells, site 0 (hot file)",
            f"""
            SELECT site_id, rack_id, module_id, cell_id,
                   arg_max(voltage,     timestamp) AS voltage,
                   arg_max(current,     timestamp) AS current,
                   arg_max(temperature, timestamp) AS temperature,
                   arg_max(soc,         timestamp) AS soc,
                   arg_max(soh,         timestamp) AS soh
            FROM read_parquet('{hot}', union_by_name=true)
            WHERE site_id = 0 AND voltage IS NOT NULL
            GROUP BY ALL
            ORDER BY rack_id, module_id, cell_id
            """,
        ),
        (
            "Q2",
            "Time series — single cell history, last hour (date-scoped partitioned store)",
            f"""
            SELECT timestamp, voltage, current, temperature, soc
            FROM read_parquet({bat_today}, union_by_name=true)
            WHERE site_id=0 AND rack_id=0 AND module_id=0 AND cell_id=0
              AND timestamp > epoch(now()) - 3600
            ORDER BY timestamp DESC
            LIMIT 300
            """,
        ),
        (
            "Q3",
            "Rack aggregates — mean voltage & temperature per rack (10s buckets, last hour)",
            f"""
            SELECT site_id, rack_id,
                   time_bucket(INTERVAL '10 seconds', to_timestamp(timestamp)) AS bucket,
                   round(avg(voltage),     3) AS mean_voltage,
                   round(avg(temperature), 2) AS mean_temp,
                   round(avg(soc),         2) AS mean_soc
            FROM read_parquet({bat_today}, union_by_name=true)
            WHERE timestamp > epoch(now()) - 3600
            GROUP BY ALL
            ORDER BY bucket DESC, site_id, rack_id
            LIMIT 288
            """,
        ),
        (
            "Q4",
            "Alert scan — battery cells with voltage outside 3.2–4.1 V (hot file)",
            f"""
            WITH latest AS (
                SELECT site_id, rack_id, module_id, cell_id,
                       arg_max(voltage, timestamp) AS voltage
                FROM read_parquet('{hot}', union_by_name=true)
                WHERE voltage IS NOT NULL
                GROUP BY ALL
            )
            SELECT * FROM latest
            WHERE voltage < 3.2 OR voltage > 4.1
            ORDER BY voltage
            """,
        ),
        (
            "Q5",
            "Solar overview — last irradiance & AC power, all panels (hot file)",
            f"""
            SELECT site_id, rack_id, module_id, cell_id,
                   round(arg_max(irradiance, timestamp), 1) AS irradiance,
                   round(arg_max(ac_power,   timestamp), 1) AS ac_power,
                   round(arg_max(efficiency, timestamp), 2) AS efficiency
            FROM read_parquet('{hot}', union_by_name=true)
            WHERE irradiance IS NOT NULL
            GROUP BY ALL
            ORDER BY site_id, rack_id, module_id, cell_id
            """,
        ),
        (
            "Q6",
            "Fleet health — active cell count & data freshness (hot file)",
            f"""
            SELECT source_type, site_id,
                   count(*) AS readings,
                   round(epoch(now()) - max(timestamp), 1) AS age_seconds
            FROM (
                SELECT 'batteries' AS source_type, site_id, timestamp
                FROM read_parquet('{hot}', union_by_name=true)
                WHERE voltage IS NOT NULL
                UNION ALL
                SELECT 'solar' AS source_type, site_id, timestamp
                FROM read_parquet('{hot}', union_by_name=true)
                WHERE irradiance IS NOT NULL
            )
            GROUP BY ALL
            ORDER BY source_type, site_id
            """,
        ),
    ]


def main():
    args = parse_args()
    base = args.base

    hot = Path(base) / "hot.parquet"
    if not hot.exists():
        sys.exit(f"hot.parquet not found at {hot} — is the writer running?")

    queries = build_queries(base)
    con = duckdb.connect()

    # per-query accumulators
    stats = {qid: {"ms_list": [], "rows_list": [], "errors": 0, "last_data": []}
             for qid, _, _ in queries}

    print(f"=== DuckDB parquet query test ===")
    print(f"Base: {base}")
    print(f"Cycles: {args.cycles}  Interval: {args.interval}s\n")

    for cycle in range(args.cycles):
        print(f"--- cycle {cycle + 1}/{args.cycles} ---")
        for qid, label, sql in queries:
            try:
                r = run_query(con, label, sql)
                stats[qid]["ms_list"].append(r["ms"])
                stats[qid]["rows_list"].append(r["rows"])
                stats[qid]["last_data"] = r["data"]
                print(f"  {qid}  {r['ms']:7.1f} ms   {r['rows']:5d} rows   {label[:55]}")
            except Exception as e:
                stats[qid]["errors"] += 1
                print(f"  {qid}  ERROR: {e}")
        if cycle < args.cycles - 1:
            time.sleep(args.interval)

    # Summary
    print(f"\n{'='*70}")
    print(f"{'ID':<4} {'Query':<48} {'Avg':>7} {'Min':>7} {'Max':>7} {'Rows':>6} {'Err':>4}")
    print(f"{'-'*70}")
    for qid, label, _ in queries:
        s = stats[qid]
        if s["ms_list"]:
            avg = sum(s["ms_list"]) / len(s["ms_list"])
            mn  = min(s["ms_list"])
            mx  = max(s["ms_list"])
            avgr = sum(s["rows_list"]) / len(s["rows_list"])
            print(f"{qid:<4} {label[:47]:<48} {avg:6.1f}ms {mn:6.1f}ms {mx:6.1f}ms {avgr:6.0f} {s['errors']:4d}")

    # Sample output from last cycle
    print(f"\n--- Sample output (last cycle, up to 5 rows each) ---")
    for qid, label, _ in queries:
        print(f"\n{qid}: {label}")
        for row in stats[qid]["last_data"]:
            print(f"  {row}")

    if args.json:
        report = {
            "base": base,
            "cycles": args.cycles,
            "interval": args.interval,
            "queries": [
                {
                    "id": qid,
                    "label": label,
                    "avg_ms": round(sum(stats[qid]["ms_list"]) / len(stats[qid]["ms_list"]), 2) if stats[qid]["ms_list"] else None,
                    "min_ms": round(min(stats[qid]["ms_list"]), 2) if stats[qid]["ms_list"] else None,
                    "max_ms": round(max(stats[qid]["ms_list"]), 2) if stats[qid]["ms_list"] else None,
                    "avg_rows": round(sum(stats[qid]["rows_list"]) / len(stats[qid]["rows_list"]), 1) if stats[qid]["rows_list"] else None,
                    "errors": stats[qid]["errors"],
                    "sample": stats[qid]["last_data"],
                }
                for qid, label, _ in queries
            ],
        }
        print("\n--- JSON ---")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
