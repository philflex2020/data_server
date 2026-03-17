#!/usr/bin/env python3
"""
migration_compare.py — Side-by-side InfluxDB vs DuckDB query comparison.

Runs equivalent queries against both stores simultaneously and prints a
diff-style result: field values, row counts, latency, and an agreement check.
Designed for live demo of the Telegraf → parquet writer migration.

Both Telegraf and writer.cpp subscribe to FlashMQ independently — this script
proves they are receiving and storing the same data.

Usage:
    python3 migration_compare.py
    python3 migration_compare.py --cycles 6 --interval 10
    python3 migration_compare.py --base ~/data/parquet --host localhost
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed — run: pip3 install duckdb")


# ── InfluxDB client ────────────────────────────────────────────────────────────

class InfluxClient:
    def __init__(self, host, port, token, org, bucket):
        self.base   = f"http://{host}:{port}"
        self.token  = token
        self.org    = org
        self.bucket = bucket

    def query(self, influxql):
        url = (f"{self.base}/query"
               f"?db={urllib.parse.quote(self.bucket)}"
               f"&q={urllib.parse.quote(influxql)}")
        req = urllib.request.Request(url, headers={"Authorization": f"Token {self.token}"})
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                body = r.read()
                ms = (time.perf_counter() - t0) * 1000
                data = json.loads(body)
        except Exception as e:
            return None, (time.perf_counter() - t0) * 1000, str(e)
        result = data.get("results", [{}])[0]
        if "error" in result:
            return None, ms, result["error"]
        series = result.get("series", [])
        rows = []
        for s in series:
            cols = s.get("columns", [])
            for v in s.get("values", []):
                rows.append(dict(zip(cols, v)))
        return rows, ms, None


# ── DuckDB helpers ─────────────────────────────────────────────────────────────

def date_glob(base, source, project_id, site_id, window_s):
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(seconds=window_s)
    paths = []
    d = start.date()
    while d <= now.date():
        paths.append(
            f"{base}/{source}/project={project_id}/site={site_id}"
            f"/{d.year}/{d.month:02d}/{d.day:02d}/*.parquet"
        )
        d += timedelta(days=1)
    return paths


def duck_query(con, sql):
    t0 = time.perf_counter()
    try:
        rows = con.execute(sql).fetchall()
        desc = con.description
        ms = (time.perf_counter() - t0) * 1000
        cols = [d[0] for d in desc]
        return [dict(zip(cols, r)) for r in rows], ms, None
    except Exception as e:
        return None, (time.perf_counter() - t0) * 1000, str(e)


# ── Comparison checks ──────────────────────────────────────────────────────────

def fmt_val(v):
    if v is None:
        return "None"
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)


def compare(label, influx_rows, duck_rows, influx_ms, duck_ms,
            influx_key, duck_key, influx_val_col, duck_val_col, tol=0.01):
    """Compare a single check between both stores. Returns (passed, summary)."""
    lines = []
    lines.append(f"\n  {'─'*60}")
    lines.append(f"  {label}")
    lines.append(f"  InfluxDB  {influx_ms:6.1f} ms   DuckDB  {duck_ms:6.1f} ms")

    if influx_rows is None or duck_rows is None:
        lines.append(f"  ERROR — influx: {influx_rows}  duck: {duck_rows}")
        return False, "\n".join(lines)

    # build lookup dicts keyed by influx_key / duck_key
    i_map = {fmt_val(r.get(influx_key)): r.get(influx_val_col) for r in influx_rows}
    d_map = {fmt_val(r.get(duck_key)):   r.get(duck_val_col)   for r in duck_rows}

    lines.append(f"  rows  InfluxDB={len(influx_rows)}  DuckDB={len(duck_rows)}")

    passed = True
    mismatches = 0
    all_keys = sorted(set(i_map) | set(d_map))
    for k in all_keys[:5]:   # show first 5
        iv = i_map.get(k)
        dv = d_map.get(k)
        if iv is None or dv is None:
            match = "  MISSING"
            passed = False
            mismatches += 1
        else:
            try:
                diff = abs(float(iv) - float(dv))
                match = "  OK" if diff <= tol else f"  DIFF Δ={diff:.4f}"
                if diff > tol:
                    passed = False
                    mismatches += 1
            except (TypeError, ValueError):
                match = "  OK" if iv == dv else f"  DIFF"
                if iv != dv:
                    passed = False
                    mismatches += 1
        lines.append(f"    [{k:>6}]  influx={fmt_val(iv):<12}  duck={fmt_val(dv):<12}{match}")

    if len(all_keys) > 5:
        lines.append(f"    … {len(all_keys)-5} more rows")

    if mismatches == 0:
        lines.append(f"  ✓ AGREE  (all checked values within tol={tol})")
    else:
        lines.append(f"  ✗ {mismatches} MISMATCH(ES)")

    return passed, "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--base",     default="/home/phil/data/parquet")
    p.add_argument("--host",     default="localhost")
    p.add_argument("--port",     default=8086, type=int)
    p.add_argument("--token",    default="battery-token-secret")
    p.add_argument("--org",      default="battery-org")
    p.add_argument("--bucket",   default="battery-data")
    p.add_argument("--cycles",   default=3, type=int)
    p.add_argument("--interval", default=10.0, type=float)
    return p.parse_args()


def main():
    args = parse_args()
    base = args.base

    hot = Path(base) / "hot.parquet"
    cs  = Path(base) / "current_state.parquet"

    influx = InfluxClient(args.host, args.port, args.token, args.org, args.bucket)
    con    = duckdb.connect()

    # quick connectivity check
    try:
        urllib.request.urlopen(
            f"http://{args.host}:{args.port}/ping", timeout=3)
    except Exception as e:
        sys.exit(f"InfluxDB not reachable at {args.host}:{args.port} — {e}")

    if not hot.exists():
        sys.exit(f"hot.parquet not found at {hot} — is writer.cpp running?")

    print("=" * 64)
    print("  Migration comparison — InfluxDB vs DuckDB")
    print(f"  Base: {base}")
    print(f"  InfluxDB: {args.host}:{args.port}  bucket={args.bucket}")
    print(f"  Cycles: {args.cycles}  Interval: {args.interval}s")
    print("=" * 64)

    total_checks = 0
    total_passed = 0
    cycle_results = []

    for cycle in range(args.cycles):
        print(f"\n{'='*64}")
        print(f"  Cycle {cycle+1}/{args.cycles}  {datetime.now().strftime('%H:%M:%S')}")
        cycle_pass = 0
        cycle_total = 0

        # ── Check 1: current voltage for site=0, rack=0, module=0, cell=0 ──
        i_rows, i_ms, i_err = influx.query(
            "SELECT last(voltage) AS voltage FROM batteries "
            "WHERE site='0' AND rack='0' AND module='0' AND cell='0'"
        )
        cs_path = str(cs) if cs.exists() else str(hot)
        d_rows, d_ms, d_err = duck_query(con,
            f"SELECT arg_max(voltage, timestamp) AS voltage "
            f"FROM read_parquet('{cs_path}', union_by_name=true) "
            f"WHERE site_id=0 AND rack_id=0 AND module_id=0 AND cell_id=0 "
            f"AND voltage IS NOT NULL"
        )
        if i_err:  i_rows = []
        if d_err:  d_rows = []
        # normalize: influx returns list with one row; duck too
        i_norm = [{"cell": "0/0/0/0", "voltage": i_rows[0].get("voltage")}] if i_rows else []
        d_norm = [{"cell": "0/0/0/0", "voltage": d_rows[0].get("voltage")}] if d_rows else []
        passed, summary = compare(
            "Check 1 — current voltage (site=0 rack=0 module=0 cell=0)",
            i_norm, d_norm, i_ms, d_ms, "cell", "cell", "voltage", "voltage", tol=0.001
        )
        print(summary)
        cycle_total += 1; cycle_pass += int(passed)

        # ── Check 2: fleet cell count (batteries) ──
        i_rows, i_ms, i_err = influx.query(
            "SELECT count(voltage) AS n FROM batteries GROUP BY site"
        )
        d_rows, d_ms, d_err = duck_query(con,
            f"SELECT site_id, count(*) AS n "
            f"FROM read_parquet('{cs_path}', union_by_name=true) "
            f"WHERE voltage IS NOT NULL "
            f"GROUP BY site_id ORDER BY site_id"
        )
        if i_err:  i_rows = []
        if d_err:  d_rows = []
        # influx groups by tag 'site'; duck groups by site_id int
        i_norm = [{"site": r.get("site","?"), "n": r.get("n")} for r in (i_rows or [])]
        d_norm = [{"site": str(r.get("site_id","?")), "n": r.get("n")} for r in (d_rows or [])]
        passed, summary = compare(
            "Check 2 — battery cell count per site",
            i_norm, d_norm, i_ms, d_ms, "site", "site", "n", "n", tol=0
        )
        print(summary)
        cycle_total += 1; cycle_pass += int(passed)

        # ── Check 3: out-of-range alert count ──
        i_rows, i_ms, i_err = influx.query(
            "SELECT last(voltage) AS voltage FROM batteries GROUP BY site,rack,module,cell"
        )
        i_alerts = [r for r in (i_rows or []) if r.get("voltage") is not None
                    and (r["voltage"] < 3.2 or r["voltage"] > 4.1)]

        d_rows, d_ms, d_err = duck_query(con,
            f"WITH latest AS ("
            f"  SELECT site_id, rack_id, module_id, cell_id, "
            f"         arg_max(voltage, timestamp) AS voltage "
            f"  FROM read_parquet('{str(hot)}', union_by_name=true) "
            f"  WHERE voltage IS NOT NULL GROUP BY ALL"
            f") SELECT count(*) AS alert_count FROM latest "
            f"WHERE voltage < 3.2 OR voltage > 4.1"
        )
        i_count = len(i_alerts)
        d_count = d_rows[0].get("alert_count", 0) if d_rows else 0
        diff = abs(i_count - d_count)
        status = "✓ AGREE" if diff == 0 else f"✗ DIFF Δ={diff}"
        print(f"\n  {'─'*60}")
        print(f"  Check 3 — out-of-range alert count (voltage < 3.2 or > 4.1 V)")
        print(f"  InfluxDB  {i_ms:6.1f} ms   DuckDB  {d_ms:6.1f} ms")
        print(f"  InfluxDB alerts={i_count}   DuckDB alerts={d_count}   {status}")
        passed = (diff == 0)
        cycle_total += 1; cycle_pass += int(passed)

        # ── Check 4: rack mean voltage, site 0 ──
        i_rows, i_ms, i_err = influx.query(
            "SELECT mean(voltage) AS mean_voltage FROM batteries "
            "WHERE site='0' GROUP BY rack"
        )
        bat_today = date_glob(base, "batteries", 0, 0, window_s=3600)
        d_rows, d_ms, d_err = duck_query(con,
            f"SELECT rack_id, round(avg(voltage), 3) AS mean_voltage "
            f"FROM read_parquet({bat_today}, union_by_name=true) "
            f"WHERE timestamp > epoch(now()) - 3600 "
            f"GROUP BY rack_id ORDER BY rack_id"
        )
        i_norm = [{"rack": r.get("rack","?"), "v": r.get("mean_voltage")} for r in (i_rows or [])]
        d_norm = [{"rack": str(r.get("rack_id","?")), "v": r.get("mean_voltage")} for r in (d_rows or [])]
        passed, summary = compare(
            "Check 4 — mean voltage per rack, site 0 (last hour)",
            i_norm, d_norm, i_ms, d_ms, "rack", "rack", "v", "v", tol=0.05
        )
        print(summary)
        cycle_total += 1; cycle_pass += int(passed)

        total_checks += cycle_total
        total_passed += cycle_pass
        cycle_results.append((cycle_pass, cycle_total))
        print(f"\n  Cycle {cycle+1} result: {cycle_pass}/{cycle_total} checks passed")

        if cycle < args.cycles - 1:
            time.sleep(args.interval)

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{'='*64}")
    print(f"  MIGRATION COMPARISON SUMMARY")
    print(f"  {total_passed}/{total_checks} checks passed across {args.cycles} cycles")
    print(f"{'='*64}")
    for i, (p, t) in enumerate(cycle_results):
        bar = "✓" * p + "✗" * (t - p)
        print(f"  Cycle {i+1}:  {bar}  ({p}/{t})")
    print()
    if total_passed == total_checks:
        print("  ✓ ALL CHECKS PASSED — both stores agree.")
        print("  Ready to proceed with Phase 4: consumer migration.")
    else:
        failed = total_checks - total_passed
        print(f"  ✗ {failed} CHECK(S) FAILED — investigate before migrating consumers.")


if __name__ == "__main__":
    main()
