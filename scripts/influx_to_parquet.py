#!/usr/bin/env python3
"""
influx_to_parquet.py
====================
Query an InfluxDB2 instance, convert annotated-CSV results to Parquet,
run five noise-analysis queries and emit an HTML report.

Handles two schemas automatically:
  EMS schema   — _field ∈ {float,integer,boolean_integer,string} is a dtype hint;
                 actual signal name is in the 'point_name' tag.
  Standard     — _field is the metric/signal name (Telegraf, etc.).
                 All _values are treated as float unless overridden.

Usage
-----
  python3 influx_to_parquet.py \\
      --url    http://192.168.86.51:8086 \\
      --token  MY_TOKEN \\
      --org    battery-org \\
      --bucket battery-data \\
      --start  -3h \\
      --out-dir /tmp/parquet \\
      --report  /tmp/noise_report.html

Arguments
---------
  --url           InfluxDB2 base URL           [http://localhost:8086]
  --token         API token  (or INFLUX_TOKEN env var)
  --org           Organisation name or ID      []
  --bucket        Bucket to query              (required)
  --start         Flux range start             [-3h]
  --stop          Flux range stop              [now()]
  --measurements  Comma-separated _measurement values  [auto-discover]
  --signal-col    Column to use as signal name [auto: point_name or _field]
  --out-dir       Parquet output directory     [./parquet]
  --report        HTML report output path      [./noise_report.html]
  --no-report     Skip HTML report generation
  --limit         Max rows per DuckDB result   [40]
  --skip-fetch    Use existing parquet in --out-dir, skip InfluxDB queries
"""

import argparse
import datetime
import html as htmllib
import io
import os
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import duckdb


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="InfluxDB2 → Parquet + noise-analysis HTML report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--url",          default="http://localhost:8086")
    p.add_argument("--token",        default=os.environ.get("INFLUX_TOKEN", ""))
    p.add_argument("--org",          default="")
    p.add_argument("--bucket",       required=True)
    p.add_argument("--start",        default="-3h")
    p.add_argument("--stop",         default="now()")
    p.add_argument("--measurements", default="",
                   help="Comma-separated list; empty = auto-discover")
    p.add_argument("--signal-col",   default="",
                   help="Column to use as signal name (auto-detected if empty)")
    p.add_argument("--out-dir",      default="./parquet")
    p.add_argument("--report",       default="./noise_report.html")
    p.add_argument("--no-report",    action="store_true")
    p.add_argument("--limit",        type=int, default=40)
    p.add_argument("--skip-fetch",   action="store_true",
                   help="Skip InfluxDB queries; use existing parquet in --out-dir")
    return p.parse_args()


# ── InfluxDB2 HTTP helpers ────────────────────────────────────────────────────

def influx_headers(token: str) -> dict:
    h = {"Content-Type": "application/json", "Accept": "application/csv"}
    if token:
        h["Authorization"] = f"Token {token}"
    return h


def flux_query_stream(url: str, token: str, org: str, flux: str):
    endpoint = url.rstrip("/") + "/api/v2/query"
    params   = {"org": org} if org else {}
    r = requests.post(
        endpoint, params=params, json={"query": flux, "type": "flux"},
        headers=influx_headers(token), stream=True, timeout=600,
    )
    if r.status_code != 200:
        raise RuntimeError(
            f"InfluxDB query failed [{r.status_code}]: {r.text[:400]}"
        )
    return r.iter_lines(decode_unicode=True)


def discover_measurements(url: str, token: str, org: str, bucket: str) -> list:
    flux = f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{bucket}")'
    lines = list(flux_query_stream(url, token, org, flux))
    result = []
    header = None
    for line in lines:
        if line.startswith("#") or line == "":
            header = None
            continue
        if ",result,table" in line[:30]:
            header = line
            continue
        if header:
            cols = header.split(",")
            vals = line.split(",")
            row  = dict(zip(cols, vals))
            v    = row.get("_value", "").strip()
            if v:
                result.append(v)
    return result


# ── Annotated-CSV parser ──────────────────────────────────────────────────────

# InfluxDB EMS schema: _field holds dtype hints, not signal names
_EMS_DTYPES = {"float", "integer", "boolean_integer", "string"}


def parse_influx_csv_lines(line_iter) -> pd.DataFrame:
    """
    Parse InfluxDB annotated CSV (multiple table blocks).
    Auto-detects EMS schema vs standard Telegraf schema.
    Returns a DataFrame with columns:
      ts, measurement, point_name, dtype (EMS) or field (standard),
      value_f, value_i, + any tag columns
    """
    chunks = []
    header = None
    buf    = []

    def flush():
        nonlocal buf
        if buf and header:
            chunks.append(pd.read_csv(
                io.StringIO("\n".join([header] + buf)), low_memory=False,
            ))
        buf = []

    for line in line_iter:
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        line = line.rstrip("\n\r")
        if line.startswith("#"):
            flush(); header = None; continue
        if line == "":
            flush(); header = None; continue
        if ",result,table" in line[:30]:
            flush(); header = line; continue
        if header is not None:
            buf.append(line)

    flush()

    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)

    # Drop InfluxDB bookkeeping columns
    df.drop(columns=[c for c in ["", "Unnamed: 0", "result", "table", "_start", "_stop"]
                     if c in df.columns], inplace=True, errors="ignore")

    df.rename(columns={"_time": "ts", "_measurement": "measurement"}, inplace=True)

    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")

    if "_value" not in df.columns or "_field" not in df.columns:
        return df

    raw       = df.pop("_value")
    field_col = df.pop("_field")

    # Detect schema
    unique_fields = set(field_col.dropna().unique())
    is_ems = unique_fields.issubset(_EMS_DTYPES | {"string"})

    if is_ems:
        # EMS: _field is dtype hint; point_name tag is the signal name
        df["dtype"]   = field_col
        mask_int = field_col.isin(["integer", "boolean_integer"])
        df["value_f"] = pd.to_numeric(raw.where(~mask_int), errors="coerce")
        df["value_i"] = pd.to_numeric(raw.where(mask_int),  errors="coerce").astype("Int64")
    else:
        # Standard Telegraf: _field is the signal name
        # Ensure point_name column exists (rename _field → point_name)
        df["point_name"] = field_col
        df["value_f"]    = pd.to_numeric(raw, errors="coerce")

    return df


def detect_signal_col(df: pd.DataFrame, override: str = "") -> str:
    """Return the column name that holds signal/metric names."""
    if override and override in df.columns:
        return override
    if "point_name" in df.columns:
        return "point_name"
    # fall back to any tag column that looks like signal names
    for candidate in ["field_name", "_field", "field"]:
        if candidate in df.columns:
            return candidate
    return ""


# ── Fetch one measurement ─────────────────────────────────────────────────────

def fetch_measurement(
    url, token, org, bucket, measurement, start, stop, out_dir,
) -> str | None:
    flux = f"""
from(bucket: "{bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "{measurement}")
"""
    t0 = time.time()
    print(f"  {measurement!r:30s}", end="", flush=True)

    try:
        line_iter = flux_query_stream(url, token, org, flux)
        df        = parse_influx_csv_lines(line_iter)
    except Exception as e:
        print(f"  ERROR: {e}")
        return None

    t1 = time.time()

    if df.empty:
        print("  (no data)")
        return None

    sig_col  = detect_signal_col(df)
    n_sigs   = df[sig_col].nunique() if sig_col else "?"
    out_path = os.path.join(out_dir, f"{measurement}.parquet")
    table    = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy", row_group_size=250_000)
    t2       = time.time()

    size_mb  = os.path.getsize(out_path) / 1e6
    print(f"  {len(df):>10,} rows  {n_sigs:>4} signals  "
          f"{size_mb:5.1f} MB  {t1-t0:.0f}s fetch  {t2-t1:.1f}s write")
    return out_path


# ── Noise-analysis queries ────────────────────────────────────────────────────

def build_queries(glob_expr: str, signal_col: str, limit: int) -> dict:
    G = glob_expr
    S = signal_col   # e.g. "point_name" or auto-detected
    L = limit
    return {
        "cv": (
            "1 — Coefficient of Variation",
            "σ/|μ| ranks signals by relative noise. High CV = noisy relative to mean. "
            "Check mean alongside CV — signals near zero inflate CV artificially.",
            f"""
            SELECT measurement, {S} AS signal,
                   ROUND(AVG(value_f), 4)    AS mean,
                   ROUND(STDDEV(value_f), 4) AS stddev,
                   ROUND(STDDEV(value_f) / NULLIF(ABS(AVG(value_f)), 0), 6) AS cv
            FROM {G}
            WHERE value_f IS NOT NULL
            GROUP BY measurement, {S}
            HAVING STDDEV(value_f) > 0
            ORDER BY cv DESC
            LIMIT {L}
            """,
        ),
        "delta": (
            "2 — Delta Percentiles",
            "Sample-to-sample |Δ| percentiles. Use p95/p99 to size deadbands "
            "for change-on-value filtering. Large p99/p95 ratio = occasional spikes.",
            f"""
            WITH deltas AS (
              SELECT measurement, {S} AS signal,
                     ABS(value_f - LAG(value_f) OVER
                       (PARTITION BY measurement, {S} ORDER BY ts)) AS delta
              FROM {G}
              WHERE value_f IS NOT NULL
            )
            SELECT measurement, signal,
                   ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY delta), 4) AS p50,
                   ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delta), 4) AS p95,
                   ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY delta), 4) AS p99,
                   ROUND(MAX(delta), 4) AS max_delta,
                   COUNT(*) AS samples
            FROM deltas
            WHERE delta IS NOT NULL AND delta > 0
            GROUP BY measurement, signal
            ORDER BY p95 DESC
            LIMIT {L}
            """,
        ),
        "frozen": (
            "3 — Frozen / Stuck Sensors",
            "Consecutive runs of identical values (&gt;20). Analog signals with long "
            "runs may be stuck or filtered upstream. Boolean signals are expected here.",
            f"""
            WITH runs AS (
              SELECT measurement, {S} AS signal, value_f,
                     ROW_NUMBER() OVER (PARTITION BY measurement, {S} ORDER BY ts)
                   - ROW_NUMBER() OVER (PARTITION BY measurement, {S}, value_f ORDER BY ts)
                     AS grp
              FROM {G}
              WHERE value_f IS NOT NULL
            )
            SELECT measurement, signal,
                   value_f AS stuck_value, COUNT(*) AS consecutive_count
            FROM runs
            GROUP BY measurement, signal, value_f, grp
            HAVING COUNT(*) > 20
            ORDER BY consecutive_count DESC
            LIMIT {L}
            """,
        ),
        "zscore": (
            "4 — Z-score Outliers (&gt;3σ)",
            "Individual readings more than 3σ from the signal mean. May be mode "
            "transitions, sensor glitches, or genuine events.",
            f"""
            WITH stats AS (
              SELECT measurement, {S} AS signal,
                     AVG(value_f) AS mu, STDDEV(value_f) AS sigma
              FROM {G}
              WHERE value_f IS NOT NULL
              GROUP BY measurement, {S}
              HAVING STDDEV(value_f) > 0
            ),
            scored AS (
              SELECT d.measurement, d.{S} AS signal,
                     ROUND(d.value_f, 4) AS value,
                     strftime(d.ts, '%Y-%m-%dT%H:%M:%SZ') AS time,
                     ROUND(ABS(d.value_f - s.mu) / NULLIF(s.sigma, 0), 2) AS z_score
              FROM {G} d
              JOIN stats s ON d.measurement = s.measurement AND d.{S} = s.signal
              WHERE d.value_f IS NOT NULL
            )
            SELECT * FROM scored WHERE z_score > 3
            ORDER BY z_score DESC
            LIMIT {L}
            """,
        ),
        "physics": (
            "5 — Physics Consistency: Voltage vs SOC",
            "Joins voltage and SOC readings within 5 s and checks V ≈ V₀ + k×SOC. "
            "Coefficients are estimated from the data means. "
            "Large residuals = miscalibration or chemistry non-linearity.",
            f"""
            WITH params AS (
              SELECT
                AVG(CASE WHEN {S} IN ('RackVoltage','SysVoltage','voltage','Voltage')
                         THEN value_f END) AS v_mean,
                AVG(CASE WHEN {S} IN ('RackSOC','SOC','soc','StateOfCharge')
                         THEN value_f END) AS soc_mean
              FROM {G} WHERE value_f IS NOT NULL
            ),
            volt AS (
              SELECT measurement, ts, value_f AS voltage
              FROM {G}
              WHERE {S} IN ('RackVoltage','SysVoltage','voltage','Voltage')
                AND value_f IS NOT NULL
            ),
            soc AS (
              SELECT measurement, ts, value_f AS soc_pct
              FROM {G}
              WHERE {S} IN ('RackSOC','SOC','soc','StateOfCharge')
                AND value_f IS NOT NULL
            )
            SELECT v.measurement AS volt_src, s.measurement AS soc_src,
                   strftime(v.ts, '%Y-%m-%dT%H:%M:%SZ') AS time,
                   ROUND(v.voltage, 2) AS voltage, ROUND(s.soc_pct, 2) AS soc,
                   ROUND(v.voltage - (p.v_mean - 0.5*p.soc_mean + 0.5*s.soc_pct), 2) AS residual
            FROM volt v
            JOIN soc s     ON ABS(epoch(v.ts) - epoch(s.ts)) < 5
            CROSS JOIN params p
            WHERE p.v_mean IS NOT NULL AND p.soc_mean IS NOT NULL
            ORDER BY ABS(v.voltage - (p.v_mean - 0.5*p.soc_mean + 0.5*s.soc_pct)) DESC
            LIMIT {L}
            """,
        ),
    }


def run_query(con, label: str, sql: str):
    t0 = time.time()
    print(f"  {label[:55]:55s}", end="", flush=True)
    try:
        rel  = con.execute(sql)
        cols = [d[0] for d in rel.description]
        rows = rel.fetchall()
        print(f"  {len(rows):4d} rows  ({time.time()-t0:.1f}s)")
        return cols, rows, None
    except Exception as e:
        print(f"  ERROR: {e}")
        return [], [], str(e)


# ── HTML report ───────────────────────────────────────────────────────────────

HTML_STYLE = """
  :root {
    --bg:#0d1117; --surface:#161b22; --border:#30363d;
    --text:#c9d1d9; --muted:#8b949e; --accent:#58a6ff;
    --green:#3fb950; --yellow:#d29922; --red:#f85149; --orange:#e3b341;
    --code-bg:#21262d;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--text);
         font-family:'Segoe UI',system-ui,sans-serif; padding:20px; line-height:1.5; }
  h1   { color:var(--accent); font-size:1.4rem; margin-bottom:4px; }
  .meta { color:var(--muted); font-size:.82rem; margin-bottom:20px; }
  h2   { color:var(--orange); font-size:1rem; margin:28px 0 10px;
         padding-bottom:5px; border-bottom:1px solid var(--border); }
  .ocards { display:flex; flex-wrap:wrap; gap:10px; margin-bottom:24px; }
  .ocard  { background:var(--surface); border:1px solid var(--border);
             border-radius:8px; padding:12px 16px; min-width:180px; }
  .ocard-title  { font-size:.75rem; color:var(--muted); text-transform:uppercase;
                  letter-spacing:.06em; margin-bottom:4px; }
  .ocard-stat   { font-size:1.6rem; font-weight:700; color:var(--accent); }
  .ocard-label  { font-size:.72rem; color:var(--muted); }
  .ocard-detail { font-size:.72rem; color:var(--muted); margin-top:4px; }
  .qcard { background:var(--surface); border:1px solid var(--border); border-radius:8px;
           padding:14px; margin-bottom:20px; overflow-x:auto; }
  .qdesc { font-size:.8rem; color:var(--muted); margin-bottom:10px; }
  table  { border-collapse:collapse; width:100%; font-size:.78rem; }
  thead tr { background:var(--code-bg); }
  th { padding:6px 10px; text-align:left; color:var(--muted); font-weight:600;
       border-bottom:1px solid var(--border); white-space:nowrap; }
  td { padding:5px 10px; border-bottom:1px solid #1e2530; white-space:nowrap; }
  tr:hover td { background:#1a2030; }
  td:nth-child(n+3) { text-align:right; font-variant-numeric:tabular-nums; }
  tbody tr:nth-child(1) td { color:var(--red); }
  tbody tr:nth-child(2) td { color:var(--orange); }
  tbody tr:nth-child(3) td { color:var(--yellow); }
  .err  { color:var(--red);   font-size:.85rem; padding:8px; }
  .none { color:var(--muted); font-size:.85rem; padding:8px; }
  .sql-block { background:var(--code-bg); border:1px solid var(--border); border-radius:6px;
               padding:10px 14px; margin-top:10px; font-family:monospace; font-size:.72rem;
               color:#6e7681; white-space:pre-wrap; word-break:break-all; }
  details summary { cursor:pointer; color:var(--muted); font-size:.78rem; margin-top:8px; }
  @media (prefers-color-scheme: light) {
    :root {
      --bg:#fff; --surface:#f6f8fa; --border:#d0d7de; --text:#24292f;
      --muted:#57606a; --accent:#0969da; --green:#1a7f37;
      --yellow:#9a6700; --red:#cf222e; --orange:#bc4c00; --code-bg:#f6f8fa;
    }
  }
"""


def esc(v):
    return htmllib.escape(str(v)) if v is not None else "<span style='color:#555'>—</span>"


def table_html(cols, rows, err):
    if err:
        return f'<p class="err">Query error: {esc(err)}</p>'
    if not rows:
        return '<p class="none">No rows returned.</p>'
    h = ("<table><thead><tr>"
         + "".join(f"<th>{esc(c)}</th>" for c in cols)
         + "</tr></thead><tbody>")
    for row in rows:
        h += "<tr>" + "".join(f"<td>{esc(v)}</td>" for v in row) + "</tr>"
    h += "</tbody></table>"
    return h


def write_report(path, args, glob_expr, ov_cols, ov_rows, query_results):
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    cards = ""
    for row in ov_rows:
        d = dict(zip(ov_cols, row))
        meas  = d.get("measurement", "?")
        first = str(d.get("first_ts", ""))[:19].replace("T", " ")
        last  = str(d.get("last_ts",  ""))[:19].replace("T", " ")
        cards += f"""
        <div class="ocard">
          <div class="ocard-title">{esc(meas)}</div>
          <div class="ocard-stat">{d.get('total_rows', 0):,}</div>
          <div class="ocard-label">rows</div>
          <div class="ocard-detail">
            {d.get('signals','?')} signals &nbsp;·&nbsp;
            {d.get('float_rows',0):,} float &nbsp;·&nbsp;
            {d.get('int_rows',0):,} int
          </div>
          <div class="ocard-detail">{esc(first)} → {esc(last)}</div>
        </div>"""

    sections = ""
    for key, (title, desc, sql, cols, rows, err) in query_results.items():
        count_str = f"{len(rows)} rows" if not err else "error"
        sections += f"""
<h2>{esc(title)}</h2>
<div class="qcard">
  <p class="qdesc">{desc}</p>
  <p class="meta" style="margin-bottom:8px">{count_str} (limit {args.limit})</p>
  {table_html(cols, rows, err)}
  <details>
    <summary>Show SQL</summary>
    <div class="sql-block">{esc(sql.strip())}</div>
  </details>
</div>
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Noise Analysis — {esc(args.bucket)}</title>
<style>{HTML_STYLE}</style>
</head>
<body>
<h1>Noise Analysis Report — <code>{esc(args.bucket)}</code></h1>
<p class="meta">
  Generated {now} &nbsp;·&nbsp;
  <code>{esc(args.url)}</code> &nbsp;·&nbsp;
  range: <code>{esc(args.start)}</code> → <code>{esc(args.stop)}</code><br>
  Parquet: <code>{esc(glob_expr.strip("'"))}</code>
</p>
<h2>Dataset Overview</h2>
<div class="ocards">{cards}</div>
{sections}
</body>
</html>
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"\nReport → {path}  ({os.path.getsize(path)//1024} KB)")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    # ── 1. Fetch measurements → Parquet ─────────────────────────────────────
    if args.skip_fetch:
        print(f"--skip-fetch: using existing parquet in {args.out_dir}")
    else:
        if args.measurements:
            measurements = [m.strip() for m in args.measurements.split(",") if m.strip()]
        else:
            print("Discovering measurements …", end="", flush=True)
            try:
                measurements = discover_measurements(
                    args.url, args.token, args.org, args.bucket
                )
                print(f" {measurements}")
            except Exception as e:
                sys.exit(f"\nDiscovery failed: {e}\nUse --measurements to specify them.")

        if not measurements:
            sys.exit("No measurements found.")

        print(f"\nFetching {len(measurements)} measurements  "
              f"url={args.url}  bucket={args.bucket!r}  "
              f"start={args.start!r}\n")
        print(f"  {'measurement':30s}  {'rows':>10}  {'sigs':>4}  {'MB':>5}  fetch  write")
        print("  " + "-"*70)

        for meas in measurements:
            fetch_measurement(
                args.url, args.token, args.org,
                args.bucket, meas, args.start, args.stop,
                args.out_dir,
            )

    if args.no_report:
        print("\n--no-report set; done.")
        return

    # ── 2. Detect signal column from first parquet file ──────────────────────
    parquet_files = sorted(
        f for f in os.listdir(args.out_dir) if f.endswith(".parquet")
    )
    if not parquet_files:
        sys.exit(f"No parquet files in {args.out_dir}")

    # Try each parquet file until we find one with a recognised signal column
    signal_col = args.signal_col
    if not signal_col:
        for pf in parquet_files:
            sample_df  = pd.read_parquet(os.path.join(args.out_dir, pf)).head(100)
            signal_col = detect_signal_col(sample_df)
            if signal_col:
                break
    if not signal_col:
        sys.exit("Could not detect signal name column. Use --signal-col "
                 "(e.g. --signal-col point_name or --signal-col field_name).")

    print(f"\nSignal column detected: '{signal_col}'")

    # ── 3. DuckDB noise analysis ─────────────────────────────────────────────
    glob_expr = f"read_parquet('{args.out_dir}/*.parquet', union_by_name=true)"
    print(f"Running noise analysis on {glob_expr} …\n")
    con = duckdb.connect()

    # Dynamic overview — point_name or signal_col
    sig_agg = f"COUNT(DISTINCT {signal_col})" if signal_col else "NULL"
    ov_sql = f"""
    SELECT measurement,
           COUNT(*) AS total_rows,
           {sig_agg} AS signals,
           SUM(CASE WHEN value_f IS NOT NULL THEN 1 ELSE 0 END) AS float_rows,
           SUM(CASE WHEN value_i IS NOT NULL THEN 1 ELSE 0 END) AS int_rows,
           MIN(ts) AS first_ts, MAX(ts) AS last_ts
    FROM {glob_expr}
    GROUP BY measurement
    ORDER BY total_rows DESC
    """
    print("  overview …", end="", flush=True)
    t0 = time.time()
    try:
        ov_rel  = con.execute(ov_sql)
        ov_cols = [d[0] for d in ov_rel.description]
        ov_rows = ov_rel.fetchall()
        print(f" {len(ov_rows)} rows ({time.time()-t0:.1f}s)")
    except Exception as e:
        print(f" ERROR: {e}")
        ov_cols, ov_rows = [], []

    queries = build_queries(glob_expr, signal_col, args.limit)
    query_results = {}
    for key, (title, desc, sql) in queries.items():
        cols, rows, err = run_query(con, title, sql)
        query_results[key] = (title, desc, sql, cols, rows, err)

    # ── 4. Write report ──────────────────────────────────────────────────────
    write_report(args.report, args, glob_expr, ov_cols, ov_rows, query_results)


if __name__ == "__main__":
    main()
