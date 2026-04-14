"""
extract_ems_template.py — extract unique EMS topic entries from a Longbow CSV recording.

Input CSV format (columns):
  topic, payload, qos, retained, recv_ts, latency

Topic format in the recording:
  ems/site/{site_id}/unit/{unit_id}/{device}/{instance}/{point_name}/{dtype}

Output JSON:
  {
    "site_id": "0215D1D8",
    "unit_ids": ["0215F562", ...],          # all 46 unit IDs observed
    "template": [
      ["bms", "bms_1", "Batt1_Current", "float"],
      ...
    ]
  }

The template contains one entry per unique (device, instance, point_name, dtype)
combination — independent of unit_id.  The stress runner uses this to build
per-unit topics by prepending unit/{unit_id}/.

Usage:
    python3 tools/extract_ems_template.py \\
        --input /tmp/longbow_recording.csv.gz \\
        --output source/stress_runner/ems_topic_template.json \\
        --site-id 0215D1D8

    # Dry run — print stats, don't write
    python3 tools/extract_ems_template.py --input /tmp/longbow_recording.csv.gz --dry-run
"""

import argparse
import csv
import gzip
import json
import sys
from pathlib import Path


def open_input(path: str):
    """Open plain or gzip-compressed CSV."""
    p = Path(path)
    if p.suffix == ".gz":
        return gzip.open(path, "rt", newline="", encoding="utf-8", errors="replace")
    return open(path, newline="", encoding="utf-8", errors="replace")


def extract(input_path: str, target_site: str | None) -> dict:
    """Return {'site_id': str, 'unit_ids': [...], 'template': [[device, instance, point, dtype], ...]}"""
    seen_entries: set[tuple] = set()   # (device, instance, point_name, dtype)
    unit_ids: set[str] = set()
    site_ids: set[str] = set()
    skipped_wrong_site = 0
    skipped_bad_topic  = 0
    total              = 0

    with open_input(input_path) as f:
        reader = csv.reader(f)
        for row in reader:
            total += 1
            if not row:
                continue
            topic = row[0]

            # Only handle ems/site/{site_id}/unit/{unit_id}/... topics
            # Skip: Fractal/..., ems/site/SiteSN/..., root/... etc.
            if not topic.startswith("ems/site/"):
                skipped_bad_topic += 1
                continue

            segs = topic.split("/")
            # Expected: ems / site / {site_id} / unit / {unit_id} / {device} / {instance} / {point} / {dtype}
            #            0     1       2           3      4            5          6             7          8
            if len(segs) < 9 or segs[3] != "unit":
                skipped_bad_topic += 1
                continue

            site_id  = segs[2]
            unit_id  = segs[4]
            device   = segs[5]
            instance = segs[6]
            # point_name may contain slashes? No — the format is positional, point_name is seg 7
            # and dtype is seg 8.  Segments beyond 8 indicate a deeper nesting (root topics etc.)
            if len(segs) != 9:
                skipped_bad_topic += 1
                continue

            point    = segs[7]
            dtype    = segs[8]

            # Skip non-battery-cell topics (root/IP_address etc.)
            if device == "root":
                skipped_bad_topic += 1
                continue

            # Filter by site if requested
            if target_site and site_id != target_site:
                # SiteSN is a known meta site — count it separately
                if site_id != "SiteSN":
                    skipped_wrong_site += 1
                else:
                    skipped_bad_topic += 1
                continue

            site_ids.add(site_id)
            unit_ids.add(unit_id)
            seen_entries.add((device, instance, point, dtype))

    # Resolve site_id
    if target_site:
        resolved_site = target_site
    elif len(site_ids) == 1:
        resolved_site = next(iter(site_ids))
    elif site_ids:
        resolved_site = sorted(site_ids)[0]
        print(f"WARNING: multiple site IDs found {sorted(site_ids)!r} — using {resolved_site!r}",
              file=sys.stderr)
    else:
        resolved_site = "UNKNOWN"

    # Sort for determinism (same output regardless of CSV row order)
    template = sorted(seen_entries)  # sort by (device, instance, point, dtype)
    unit_list = sorted(unit_ids)

    print(f"Rows processed : {total:,}", file=sys.stderr)
    print(f"Skipped (bad)  : {skipped_bad_topic:,}", file=sys.stderr)
    if skipped_wrong_site:
        print(f"Skipped (site) : {skipped_wrong_site:,}", file=sys.stderr)
    print(f"Site ID        : {resolved_site}", file=sys.stderr)
    print(f"Units          : {len(unit_list)}", file=sys.stderr)
    print(f"Template entries: {len(template):,}", file=sys.stderr)

    return {
        "site_id":  resolved_site,
        "unit_ids": unit_list,
        "template": [list(e) for e in template],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract EMS topic template from Longbow CSV recording")
    parser.add_argument("--input",   required=True, help="Path to CSV or CSV.GZ recording")
    parser.add_argument("--output",  default="",    help="Output JSON path (default: print to stdout)")
    parser.add_argument("--site-id", default="",    help="Filter to this site ID (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true", help="Print stats only, don't write output")
    args = parser.parse_args()

    result = extract(args.input, args.site_id or None)

    if args.dry_run:
        print(f"Template: {len(result['template'])} entries × {len(result['unit_ids'])} units "
              f"= {len(result['template']) * len(result['unit_ids']):,} unique topics/sweep")
        return

    out_json = json.dumps(result, indent=None, separators=(",", ":"))

    if args.output:
        Path(args.output).write_text(out_json + "\n")
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(out_json)


if __name__ == "__main__":
    main()
