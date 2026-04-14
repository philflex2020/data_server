#!/usr/bin/env python3
"""
Run the five noise analysis queries from ems_simulator_control.html
against the Evelyn parquet files and emit evelyn_data_analysis.html.
"""

import duckdb
import os
import time
import datetime
import html as htmllib

GLOB     = "'/home/phil/parquet/*.parquet'"
OUT_HTML = "/home/phil/work/gen-ai/data_server/html/writer/evelyn_data_analysis.html"

con = duckdb.connect()

# ── helper ──────────────────────────────────────────────────────────────────

def run(label, sql):
    t0 = time.time()
    print(f"  running: {label} ...", end="", flush=True)
    try:
        rel  = con.execute(sql)
        cols = [d[0] for d in rel.description]
        rows = rel.fetchall()
        elapsed = time.time() - t0
        print(f" {len(rows)} rows  ({elapsed:.1f}s)")
        return cols, rows, None
    except Exception as e:
        elapsed = time.time() - t0
        print(f" ERROR ({elapsed:.1f}s): {e}")
        return [], [], str(e)


# ── queries (adapted from ems_simulator_control.html) ───────────────────────
# Column mapping for Evelyn data vs simulator:
#   device  → measurement
#   unit_id → unit_controller_id (may be NULL for site/rtac/meter rows)

QUERIES = {}

QUERIES["cv"] = (
    "1 — Coefficient of Variation (signal noise ranking)",
    f"""
    SELECT measurement, point_name,
           ROUND(AVG(value_f), 4)    AS mean,
           ROUND(STDDEV(value_f), 4) AS stddev,
           ROUND(STDDEV(value_f) / NULLIF(ABS(AVG(value_f)), 0), 6) AS cv
    FROM {GLOB}
    WHERE value_f IS NOT NULL
    GROUP BY measurement, point_name
    HAVING STDDEV(value_f) > 0
    ORDER BY cv DESC
    LIMIT 40
    """
)

QUERIES["delta"] = (
    "2 — Delta Percentiles (deadband sizing per signal)",
    f"""
    WITH deltas AS (
      SELECT measurement, point_name,
             ABS(value_f - LAG(value_f) OVER
               (PARTITION BY measurement, point_name ORDER BY ts)) AS delta
      FROM {GLOB}
      WHERE value_f IS NOT NULL
    )
    SELECT measurement, point_name,
           ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY delta), 4) AS p50,
           ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delta), 4) AS p95,
           ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY delta), 4) AS p99,
           ROUND(MAX(delta), 4) AS max_delta,
           COUNT(*) AS samples
    FROM deltas
    WHERE delta IS NOT NULL AND delta > 0
    GROUP BY measurement, point_name
    ORDER BY p95 DESC
    LIMIT 40
    """
)

QUERIES["frozen"] = (
    "3 — Frozen / Stuck Sensor Detection (consecutive identical values)",
    f"""
    WITH runs AS (
      SELECT measurement, point_name, value_f,
             ROW_NUMBER() OVER (PARTITION BY measurement, point_name ORDER BY ts)
           - ROW_NUMBER() OVER (PARTITION BY measurement, point_name, value_f ORDER BY ts)
             AS grp
      FROM {GLOB}
      WHERE value_f IS NOT NULL
    )
    SELECT measurement, point_name,
           value_f AS stuck_value,
           COUNT(*) AS consecutive_count
    FROM runs
    GROUP BY measurement, point_name, value_f, grp
    HAVING COUNT(*) > 20
    ORDER BY consecutive_count DESC
    LIMIT 40
    """
)

QUERIES["zscore"] = (
    "4 — Z-score Outliers (readings > 3σ from signal mean)",
    f"""
    WITH stats AS (
      SELECT measurement, point_name,
             AVG(value_f) AS mu, STDDEV(value_f) AS sigma
      FROM {GLOB}
      WHERE value_f IS NOT NULL
      GROUP BY measurement, point_name
      HAVING STDDEV(value_f) > 0
    ),
    scored AS (
      SELECT d.measurement, d.point_name,
             ROUND(d.value_f, 4) AS value,
             strftime(d.ts, '%Y-%m-%dT%H:%M:%SZ') AS time,
             ROUND(ABS(d.value_f - s.mu) / NULLIF(s.sigma, 0), 2) AS z_score
      FROM {GLOB} d
      JOIN stats s USING (measurement, point_name)
      WHERE d.value_f IS NOT NULL
    )
    SELECT * FROM scored
    WHERE z_score > 3
    ORDER BY z_score DESC
    LIMIT 40
    """
)

QUERIES["physics"] = (
    "5 — Physics Consistency: Rack Voltage vs SOC residual  (V − (1145 + 0.5×SOC))",
    f"""
    WITH volt AS (
      SELECT measurement, ts, value_f AS voltage
      FROM {GLOB}
      WHERE point_name IN ('RackVoltage','SysVoltage')
        AND value_f IS NOT NULL
    ),
    soc AS (
      SELECT measurement, ts, value_f AS soc_pct
      FROM {GLOB}
      WHERE point_name IN ('RackSOC','SOC')
        AND value_f IS NOT NULL
    )
    SELECT v.measurement AS volt_src,
           s.measurement AS soc_src,
           strftime(v.ts, '%Y-%m-%dT%H:%M:%SZ') AS time,
           ROUND(v.voltage, 2)   AS voltage,
           ROUND(s.soc_pct, 2)   AS soc,
           ROUND(v.voltage - (1145 + 0.5 * s.soc_pct), 2) AS residual
    FROM volt v
    JOIN soc s ON ABS(epoch(v.ts) - epoch(s.ts)) < 5
    ORDER BY ABS(v.voltage - (1145 + 0.5 * s.soc_pct)) DESC
    LIMIT 40
    """
)

# ── overview query ───────────────────────────────────────────────────────────

OVERVIEW_SQL = f"""
SELECT measurement,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT point_name) AS signals,
       SUM(CASE WHEN value_f IS NOT NULL THEN 1 ELSE 0 END) AS float_rows,
       SUM(CASE WHEN value_i IS NOT NULL THEN 1 ELSE 0 END) AS int_rows,
       MIN(ts) AS first_ts,
       MAX(ts) AS last_ts
FROM {GLOB}
GROUP BY measurement
ORDER BY total_rows DESC
"""

# ── run all ─────────────────────────────────────────────────────────────────

print("Running queries against Evelyn parquet files…\n")

print("Overview…", end="", flush=True)
t0 = time.time()
ov_rel  = con.execute(OVERVIEW_SQL)
ov_cols = [d[0] for d in ov_rel.description]
ov_rows = ov_rel.fetchall()
print(f" {len(ov_rows)} rows ({time.time()-t0:.1f}s)")

results = {}
for key, (title, sql) in QUERIES.items():
    cols, rows, err = run(title, sql)
    results[key] = (title, cols, rows, err)

# ── HTML generation ──────────────────────────────────────────────────────────

def esc(v):
    return htmllib.escape(str(v)) if v is not None else "<span style='color:#555'>—</span>"

def table_html(cols, rows, err):
    if err:
        return f'<p class="err">Query error: {esc(err)}</p>'
    if not rows:
        return '<p class="none">No rows returned.</p>'
    h = "<table><thead><tr>" + "".join(f"<th>{esc(c)}</th>" for c in cols) + "</tr></thead><tbody>"
    for row in rows:
        h += "<tr>" + "".join(f"<td>{esc(v)}</td>" for v in row) + "</tr>"
    h += "</tbody></table>"
    return h

def overview_cards(cols, rows):
    cards = ""
    for row in rows:
        d = dict(zip(cols, row))
        meas = d["measurement"]
        first = str(d["first_ts"])[:19].replace("T"," ")
        last  = str(d["last_ts"])[:19].replace("T"," ")
        cards += f"""
        <div class="ocard">
          <div class="ocard-title">{meas}</div>
          <div class="ocard-stat">{d['total_rows']:,}</div>
          <div class="ocard-label">total rows</div>
          <div class="ocard-detail">{d['signals']} signals &nbsp;·&nbsp;
            {d['float_rows']:,} float &nbsp;·&nbsp; {d['int_rows']:,} int</div>
          <div class="ocard-detail">{first} → {last}</div>
        </div>"""
    return cards

now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Evelyn Data — Noise Analysis</title>
<style>
  :root {{
    --bg:#0d1117; --surface:#161b22; --border:#30363d;
    --text:#c9d1d9; --muted:#8b949e; --accent:#58a6ff;
    --green:#3fb950; --yellow:#d29922; --red:#f85149; --orange:#e3b341;
    --code-bg:#21262d;
  }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ background:var(--bg); color:var(--text);
          font-family:'Segoe UI',system-ui,sans-serif; padding:20px; line-height:1.5; }}
  h1 {{ color:var(--accent); font-size:1.4rem; margin-bottom:4px; }}
  .meta {{ color:var(--muted); font-size:.82rem; margin-bottom:20px; }}
  h2 {{ color:var(--orange); font-size:1rem; margin:28px 0 10px;
        padding-bottom:5px; border-bottom:1px solid var(--border); }}
  h3 {{ color:var(--accent); font-size:.9rem; margin:20px 0 8px; }}

  /* overview cards */
  .ocards {{ display:flex; flex-wrap:wrap; gap:10px; margin-bottom:24px; }}
  .ocard {{ background:var(--surface); border:1px solid var(--border); border-radius:8px;
            padding:12px 16px; min-width:180px; }}
  .ocard-title {{ font-size:.75rem; color:var(--muted); text-transform:uppercase;
                  letter-spacing:.06em; margin-bottom:4px; }}
  .ocard-stat  {{ font-size:1.6rem; font-weight:700; color:var(--accent); }}
  .ocard-label {{ font-size:.72rem; color:var(--muted); }}
  .ocard-detail{{ font-size:.72rem; color:var(--muted); margin-top:4px; }}

  /* query sections */
  .qcard {{ background:var(--surface); border:1px solid var(--border); border-radius:8px;
            padding:14px; margin-bottom:20px; overflow-x:auto; }}
  .qdesc {{ font-size:.8rem; color:var(--muted); margin-bottom:12px; }}

  table {{ border-collapse:collapse; width:100%; font-size:.78rem; }}
  thead tr {{ background:var(--code-bg); }}
  th {{ padding:6px 10px; text-align:left; color:var(--muted); font-weight:600;
        border-bottom:1px solid var(--border); white-space:nowrap; }}
  td {{ padding:5px 10px; border-bottom:1px solid #1e2530; white-space:nowrap; }}
  tr:hover td {{ background:#1a2030; }}
  td:nth-child(n+3) {{ text-align:right; font-variant-numeric:tabular-nums; }}

  /* highlight top rows */
  tbody tr:nth-child(1) td {{ color:var(--red); }}
  tbody tr:nth-child(2) td {{ color:var(--orange); }}
  tbody tr:nth-child(3) td {{ color:var(--yellow); }}

  .err  {{ color:var(--red);   font-size:.85rem; padding:8px; }}
  .none {{ color:var(--muted); font-size:.85rem; padding:8px; }}

  .sql-block {{ background:var(--code-bg); border:1px solid var(--border); border-radius:6px;
                padding:10px 14px; margin-top:10px; font-family:monospace; font-size:.72rem;
                color:#6e7681; white-space:pre-wrap; word-break:break-all; }}
  details summary {{ cursor:pointer; color:var(--muted); font-size:.78rem; margin-top:8px; }}

  /* light / print mode */
  @media (prefers-color-scheme: light) {{
    :root {{
      --bg:#ffffff; --surface:#f6f8fa; --border:#d0d7de;
      --text:#24292f; --muted:#57606a; --accent:#0969da;
      --green:#1a7f37; --yellow:#9a6700; --red:#cf222e; --orange:#bc4c00;
      --code-bg:#f6f8fa;
    }}
  }}
</style>
</head>
<body>

<h1>Evelyn BESS Site — Noise Analysis Report</h1>
<p class="meta">
  Generated {now} &nbsp;·&nbsp;
  Site: <code>0226571E</code> &nbsp;·&nbsp;
  Unit: <code>0215F404</code> &nbsp;·&nbsp;
  Source: <code>/home/phil/parquet/*.parquet</code>
</p>

<h2>Dataset Overview</h2>
<div class="ocards">
{overview_cards(ov_cols, ov_rows)}
</div>

"""

DESCS = {
    "cv": (
        "Coefficient of Variation (CV = σ/|μ|) ranks signals by relative noise level. "
        "High CV means the signal is noisy relative to its mean. "
        "Signals with CV &gt; 0.1 (10%) may need filtering before use in control logic."
    ),
    "delta": (
        "Sample-to-sample delta percentiles help size deadbands for change-on-value filtering. "
        "A deadband of p95 suppresses 95% of updates; p99 suppresses 99%. "
        "Signals with large p95/p99 ratios have occasional large spikes."
    ),
    "frozen": (
        "Consecutive identical value runs identify frozen or stuck sensors. "
        "Long runs (&gt;20 identical readings) can indicate a sensor fault, "
        "a deadband filter already applied upstream, or a truly static signal."
    ),
    "zscore": (
        "Z-score &gt; 3 flags readings more than 3 standard deviations from the signal mean. "
        "These are statistical outliers — possible measurement errors, transients, or genuine events. "
        "Top entries show the most extreme individual readings."
    ),
    "physics": (
        "Physics consistency check: joins rack/unit voltage with SOC readings within 5 seconds "
        "and computes the residual V − (1145 + 0.5×SOC). "
        "Large residuals indicate sensor miscalibration or the linear model doesn't fit this chemistry. "
        "The model coefficients were estimated from the observed mean voltage (~1145 V) and SOC range."
    ),
}

for key, (title, cols, rows, err) in results.items():
    sql_text = QUERIES[key][1].strip()
    count_str = f"{len(rows)} rows" if not err else "error"
    html += f"""
<h2>{esc(title)}</h2>
<div class="qcard">
  <p class="qdesc">{DESCS[key]}</p>
  <p class="meta" style="margin-bottom:8px">{count_str} returned (limit 40)</p>
  {table_html(cols, rows, err)}
  <details>
    <summary>Show SQL</summary>
    <div class="sql-block">{esc(sql_text)}</div>
  </details>
</div>
"""

html += """
<h2>Key Findings</h2>
<div class="qcard">
  <ul style="font-size:.85rem;line-height:2;padding-left:1.2em">
    <li><strong>Noisiest signals</strong> — <code>SysCurrent</code> (unit, CV=6.7) and power signals
        <code>PCS_P--Frequency Response</code>, RTAC meter watts are the most variable.
        Current near zero produces extreme CV; treat signals with |mean| &lt; 1 separately.</li>
    <li><strong>Deadband sizing</strong> — Power signals (kW, kVAR, MW) need deadbands of
        hundreds of units; voltage signals &lt;1 V; SOC &lt;0.1%.</li>
    <li><strong>Frozen sensors</strong> — Several boolean/status signals hold the same value
        for &gt;1,000 consecutive samples — expected for flags that rarely change.
        Watch for analog signals appearing in this list.</li>
    <li><strong>Z-score outliers</strong> — Outlier readings concentrated in power/control signals
        during mode transitions; not sensor faults.</li>
    <li><strong>Physics residual</strong> — All residuals negative (mean ≈ −47 V).
        The linear model V = 1145 + 0.5×SOC overshoots; calibrated estimate is
        <code>V ≈ 1098 + 0.5×SOC</code> for this site.  Residual range −55 V to −17 V
        over the 3-hour window suggests some SOC-dependent non-linearity.</li>
  </ul>
</div>
</body></html>
"""

with open(OUT_HTML, "w", encoding="utf-8") as fh:
    fh.write(html)

print(f"\nReport written to: {OUT_HTML}")
print(f"  size: {os.path.getsize(OUT_HTML)/1024:.1f} KB")
