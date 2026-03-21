#!/usr/bin/env python3
"""
parquet_mqtt_ratio.py — compare parquet storage size vs equivalent raw MQTT wire bytes

For each parquet row (= one MQTT message in per_cell_item mode), reconstructs the
MQTT topic and payload that produced it, then computes:

  raw MQTT bytes  = MQTT fixed header (2)
                  + topic length field (2)
                  + topic string
                  + JSON payload {"timestamp": <f>, "value": <f>}

  parquet bytes   = file size on disk / row count  (amortised per row)

Outputs a summary table plus per-file breakdown for the N largest files.

Usage:
  python3 parquet_mqtt_ratio.py [--path /srv/data/parquet-aws-sim/batteries]
                                [--sample N]   # files to sample (0 = all)
                                [--top N]      # show top-N largest files in detail
"""

import argparse
import glob
import json
import os
import sys

try:
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("pyarrow not found — run with the project .venv")


# MQTT QoS-0 fixed overhead: 2-byte fixed header + 2-byte topic-length field
MQTT_HEADER_BYTES = 4


def mqtt_raw_bytes_for_row(row: dict) -> tuple[int, int]:
    """Return (topic_bytes, payload_bytes) for one parquet row."""
    p  = row.get("project_id", row.get("project", 0))
    s  = row.get("site_id",    row.get("site",    0))
    r  = row.get("rack_id",    0)
    m  = row.get("module_id",  0)
    c  = row.get("cell_id",    0)

    topic = (
        f"batteries/project={p}/site={s}"
        f"/rack={r}/module={m}/cell={c}/value"
    )
    payload = json.dumps(
        {"timestamp": row.get("timestamp", 0.0),
         "value":     row.get("value",     0.0)},
        separators=(",", ":"),
    )
    return len(topic.encode()), len(payload.encode())


def analyse_file(path: str) -> dict:
    """Return per-file stats dict."""
    disk_bytes = os.path.getsize(path)
    table = pq.read_table(path)
    rows  = len(table)
    if rows == 0:
        return None

    # Sample up to 1000 rows to estimate avg MQTT size
    sample_size = min(rows, 1000)
    step = max(1, rows // sample_size)
    topic_total = payload_total = 0
    for i in range(0, rows, step):
        row = {col: table[col][i].as_py() for col in table.schema.names}
        t, p = mqtt_raw_bytes_for_row(row)
        topic_total   += t
        payload_total += p
    sampled = (rows + step - 1) // step

    avg_topic   = topic_total   / sampled
    avg_payload = payload_total / sampled
    avg_mqtt    = MQTT_HEADER_BYTES + avg_topic + avg_payload

    raw_total    = avg_mqtt * rows
    ratio        = raw_total / disk_bytes if disk_bytes else 0

    return {
        "path":         path,
        "disk_bytes":   disk_bytes,
        "rows":         rows,
        "bytes_per_row": disk_bytes / rows,
        "avg_topic_b":  avg_topic,
        "avg_payload_b": avg_payload,
        "avg_mqtt_b":   avg_mqtt,
        "raw_est_bytes": raw_total,
        "ratio":        ratio,
    }


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--path",   default="/srv/data/parquet-aws-sim/batteries",
                    help="Root directory to scan (default: /srv/data/parquet-aws-sim/batteries)")
    ap.add_argument("--sample", type=int, default=0,
                    help="Max files to analyse (0 = all, sorted largest-first)")
    ap.add_argument("--top",    type=int, default=10,
                    help="Show per-file detail for N largest files (default: 10)")
    args = ap.parse_args()

    files = sorted(
        glob.glob(os.path.join(args.path, "**", "*.parquet"), recursive=True),
        key=os.path.getsize, reverse=True,
    )
    if not files:
        sys.exit(f"No parquet files found under {args.path}")

    if args.sample and args.sample < len(files):
        files = files[:args.sample]
        print(f"Sampling {args.sample} largest files out of total on disk\n")
    else:
        print(f"Analysing all {len(files)} files under {args.path}\n")

    results = []
    for i, f in enumerate(files):
        r = analyse_file(f)
        if r:
            results.append(r)
        if (i + 1) % 100 == 0:
            print(f"  ... {i+1}/{len(files)} files processed", flush=True)

    if not results:
        sys.exit("No usable files found.")

    # ── Aggregate ────────────────────────────────────────────────────────────
    total_disk     = sum(r["disk_bytes"]    for r in results)
    total_rows     = sum(r["rows"]          for r in results)
    total_raw      = sum(r["raw_est_bytes"] for r in results)
    overall_ratio  = total_raw / total_disk if total_disk else 0

    avg_mqtt_b     = total_raw  / total_rows  if total_rows else 0
    avg_parquet_b  = total_disk / total_rows  if total_rows else 0

    # Weighted averages from per-file means
    avg_topic_b   = sum(r["avg_topic_b"]   * r["rows"] for r in results) / total_rows
    avg_payload_b = sum(r["avg_payload_b"] * r["rows"] for r in results) / total_rows

    print("=" * 62)
    print("  PARQUET vs RAW MQTT — STORAGE RATIO")
    print("=" * 62)
    print(f"  Files analysed  : {len(results):>10,}")
    print(f"  Total rows      : {total_rows:>10,}  (= MQTT messages)")
    print()
    print(f"  Parquet on disk : {human(total_disk):>12}  ({avg_parquet_b:.2f} B/row)")
    print(f"  Raw MQTT est.   : {human(total_raw):>12}  ({avg_mqtt_b:.1f} B/msg)")
    print(f"    └ avg topic   : {avg_topic_b:.1f} B")
    print(f"    └ avg payload : {avg_payload_b:.1f} B")
    print(f"    └ fixed hdr   : {MQTT_HEADER_BYTES} B")
    print()
    print(f"  Compression ratio  : {overall_ratio:.1f}× "
          f"(raw MQTT → parquet)")
    print(f"  Space saving       : {(1 - 1/overall_ratio)*100:.1f}%")
    print("=" * 62)

    # ── Per-file detail ───────────────────────────────────────────────────────
    top = results[:args.top]
    if top:
        print(f"\nTop {len(top)} files by size:\n")
        print(f"  {'Ratio':>6}  {'Parquet':>9}  {'Rows':>9}  {'B/row':>6}  File")
        print(f"  {'-'*6}  {'-'*9}  {'-'*9}  {'-'*6}  {'-'*40}")
        for r in top:
            rel = os.path.relpath(r["path"], args.path)
            print(f"  {r['ratio']:>5.1f}×  {human(r['disk_bytes']):>9}"
                  f"  {r['rows']:>9,}  {r['bytes_per_row']:>5.1f}  {rel}")


if __name__ == "__main__":
    main()
