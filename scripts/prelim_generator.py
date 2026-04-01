#!/usr/bin/env python3
"""
prelim_generator.py — BESS parquet data generator using real prelim schema.

Reads point names and seed values from /tmp/prelim_schema.json and generates
synthetic parquet files in narrow, wide-device, and wide-point-name formats.

Supports:
  --time-jitter N   Add ±N ms random scatter per point within each scan cycle
  --bucket N        Snap timestamps to N ms grid before pivoting (aligns jitter)
  --cov             Apply change-on-value filter to wide-device rows
  --split           Generate separate fast/slow wide-device files per device;
                    COV applied only to the slow file (fast signals change every
                    cycle so per-row COV would never fire)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_PATH = "/tmp/prelim_schema.json"
SITE_ID = "0215D1EC"
START_TS = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

REAL_UNITS = [
    "01C4F65E", "01C4F68D", "01C4F6D2",
    "01C69110", "01C69113", "01C69117", "01C6910A",
]
NA_UNIT = "NA"
SYNTHETIC_PREFIX = "01C991"


def fmt_bytes(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f} MB"
    if n >= 1_000:
        return f"{n/1_000:.1f} KB"
    return f"{n} B"


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def load_schema(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def build_unit_ids(n_units: int) -> list:
    ids = REAL_UNITS[:min(n_units, len(REAL_UNITS))]
    extra = n_units - len(ids)
    for i in range(extra):
        ids.append(f"{SYNTHETIC_PREFIX}{i:02X}")
    return ids


# ---------------------------------------------------------------------------
# Value simulation — stateful, signal-speed-aware
# ---------------------------------------------------------------------------

# Change probability per scan cycle by signal class.
# "fast" signals change every cycle (current, power).
# "slow" signals rarely change (temp, SOC) — COV will fire on these.
FAST_KEYWORDS  = ("current", "dcka", "dca", "pcs_p", "pcs_q", "hz", "aph", "i_avg",
                  "allowedmax", "allowedmin", "p_correction", "frequency response",
                  "agc", "correction", "response")
SLOW_KEYWORDS  = ("soc", "soh", "temp", "_t", "cellv", "voltage", "volt",
                  "minv", "maxv", "mincell", "maxcell", "avgcell", "dcv")

def _signal_class(point_name: str) -> str:
    low = point_name.lower()
    if any(k in low for k in FAST_KEYWORDS):
        return "fast"
    if any(k in low for k in SLOW_KEYWORDS):
        return "slow"
    return "medium"


def build_state(schema: dict, unit_ids: list, rng: np.random.Generator) -> dict:
    """
    Pre-build current-value state for every (unit, device, point_name).
    Returns {(unit, device, point_name): current_float_value}.
    """
    state = {}
    for dev, pts in schema.items():
        units = [None] if dev == "NA" else unit_ids
        for unit in units:
            for pt in pts:
                key = (unit, dev, pt["name"])
                state[key] = float(pt["val"]) if pt["val"] is not None else 0.0
    return state


def step_value(
    key: tuple,           # (unit, dev, point_name)
    state: dict,
    seed_val,
    rng: np.random.Generator,
) -> float:
    """
    Advance the stateful value for one scan cycle.
    Change probability depends on signal class.
    """
    current = state[key]
    point_name = key[2]
    val = float(seed_val) if seed_val is not None else 0.0

    # Bool/int-like: small flip probability
    if isinstance(seed_val, bool) or (
        isinstance(seed_val, (int, float)) and seed_val in (0.0, 1.0, 0, 1) and abs(val) <= 1
    ):
        if rng.random() < 0.02:          # 2% flip per cycle
            current = 1.0 - current
        state[key] = current
        return current

    cls = _signal_class(point_name)
    if cls == "fast":
        p_change = 0.95                  # nearly always changes
    elif cls == "slow":
        p_change = 0.08                  # ~8% per cycle → stable for ~12 cycles avg
    else:
        p_change = 0.40                  # medium: changes ~40% of cycles

    if rng.random() < p_change:
        scale = abs(val) * 0.005 if val != 0.0 else 0.01   # tighter noise (0.5%)
        current = current + rng.normal(0, scale)
    state[key] = current
    return current


# ---------------------------------------------------------------------------
# Row generation
# ---------------------------------------------------------------------------

def generate_rows(
    schema: dict,
    unit_ids: list,
    n_cycles: int,
    interval_s: int,
    rng: np.random.Generator,
    jitter_ms: int = 0,
) -> list:
    """
    Generate all narrow-format rows using stateful value simulation.
    Slow signals (temps, SOC) stay stable across many cycles — COV fires on them.
    Fast signals (current, power) change on nearly every cycle.
    jitter_ms: if > 0, add ±jitter_ms random ms offset per point per cycle.
    """
    rows = []
    na_device_points = schema.get("NA", [])
    bess_devices = {dev: pts for dev, pts in schema.items() if dev != "NA"}

    # Initialise stateful values
    state = build_state(schema, unit_ids, rng)

    for cycle in range(n_cycles):
        base_ts = pd.Timestamp(START_TS) + pd.Timedelta(seconds=cycle * interval_s)

        # Site-level (NA unit, NA device)
        for pt in na_device_points:
            key = (None, "NA", pt["name"])
            val = step_value(key, state, pt["val"], rng)
            rows.append({
                "timestamp": base_ts,
                "site": SITE_ID,
                "unit": NA_UNIT,
                "device": "NA",
                "point_name": pt["name"],
                "value": val,
            })

        # Per-unit, per-device
        for unit in unit_ids:
            for dev, pts in bess_devices.items():
                for pt in pts:
                    key = (unit, dev, pt["name"])
                    val = step_value(key, state, pt["val"], rng)
                    if jitter_ms > 0:
                        offset_ms = int(rng.integers(-jitter_ms, jitter_ms + 1))
                        ts = base_ts + pd.Timedelta(milliseconds=offset_ms)
                    else:
                        ts = base_ts
                    rows.append({
                        "timestamp": ts,
                        "site": SITE_ID,
                        "unit": unit,
                        "device": dev,
                        "point_name": pt["name"],
                        "value": val,
                    })

    return rows


# ---------------------------------------------------------------------------
# Time bucketing
# ---------------------------------------------------------------------------

def bucket_timestamps(df: pd.DataFrame, bucket_ms: int) -> pd.DataFrame:
    """Snap timestamps to bucket_ms grid: floor(ts_ns / bucket_ns) * bucket_ns."""
    if bucket_ms <= 0:
        return df
    df = df.copy()
    bucket_ns = bucket_ms * 1_000_000
    ts_ns = df["timestamp"].astype("int64")
    df["timestamp"] = pd.to_datetime((ts_ns // bucket_ns) * bucket_ns)
    return df


# ---------------------------------------------------------------------------
# COV filtering
# ---------------------------------------------------------------------------

def apply_cov(df: pd.DataFrame) -> tuple:
    """
    Apply change-on-value filter to a wide-format dataframe.
    Drops rows where all data columns are unchanged vs the previous row
    for the same unit. Returns (filtered_df, n_dropped).
    """
    data_cols = [c for c in df.columns if c not in ("timestamp", "unit")]
    if not data_cols:
        return df, 0

    df = df.sort_values(["unit", "timestamp"]).reset_index(drop=True)
    keep_mask = pd.Series(True, index=df.index)

    for unit, grp in df.groupby("unit", sort=False):
        if len(grp) <= 1:
            continue
        data = grp[data_cols].values.astype(float)
        # Always keep first row; drop subsequent rows where nothing changed
        changed = np.any(data[1:] != data[:-1], axis=1)
        drop_idx = grp.index[1:][~changed]
        keep_mask[drop_idx] = False

    filtered = df[keep_mask].reset_index(drop=True)
    n_dropped = len(df) - len(filtered)
    return filtered, n_dropped


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

CODECS = [
    ("snappy",       "snappy"),
    ("zstd",         "zstd"),
    ("uncompressed", "none"),
]


def write_parquet(df: pd.DataFrame, path: str, codec: str) -> int:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression=codec)
    return os.path.getsize(path)


def write_variants(df: pd.DataFrame, base_path: str, base_name: str, extra: dict = None) -> list:
    infos = []
    suffix_map = {
        "snappy": base_name,
        "zstd":   base_name.replace(".parquet", "_zstd.parquet"),
        "none":   base_name.replace(".parquet", "_uncompressed.parquet"),
    }
    for codec_name, codec_arg in CODECS:
        fname = suffix_map[codec_arg]
        fpath = os.path.join(base_path, fname)
        size = write_parquet(df, fpath, codec_arg)
        rows, cols = df.shape
        info = {
            "filename": fname,
            "codec": codec_name,
            "size_bytes": size,
            "rows": rows,
            "cols": cols,
            "bytes_per_row": round(size / max(rows, 1), 1),
        }
        if extra:
            info.update(extra)
        infos.append(info)
    return infos


# ---------------------------------------------------------------------------
# Format generators
# ---------------------------------------------------------------------------

def apply_cov_narrow(df: pd.DataFrame) -> tuple:
    """
    Per-signal COV on narrow format: keep a row only when value changed
    vs the previous reading for that (unit, device, point_name).
    Returns (filtered_df, n_dropped).
    """
    df = df.sort_values(["unit", "device", "point_name", "timestamp"]).reset_index(drop=True)
    grp_cols = ["unit", "device", "point_name"]
    prev_val = df.groupby(grp_cols, sort=False)["value"].shift(1)
    # Always keep first occurrence (prev_val is NaN), drop where value == prev
    keep = prev_val.isna() | (df["value"] != prev_val)
    filtered = df[keep].reset_index(drop=True)
    return filtered, len(df) - len(filtered)


def gen_narrow(rows: list, out_dir: str, bucket_ms: int = 0, use_cov: bool = False) -> tuple:
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["value"] = df["value"].astype("float64")
    for col in ("site", "unit", "device", "point_name"):
        df[col] = df[col].astype("string")

    if bucket_ms > 0:
        df = bucket_timestamps(df, bucket_ms)

    rows_before = len(df)
    n_dropped = 0
    if use_cov:
        df, n_dropped = apply_cov_narrow(df)

    cov_stats = {
        "rows_before": rows_before,
        "rows_after":  len(df),
        "rows_dropped": n_dropped,
        "pct_dropped": round(n_dropped / max(rows_before, 1) * 100, 1),
    }

    suffix = "_cov" if use_cov else ""
    base = f"narrow_10min{suffix}.parquet"
    infos = write_variants(df, out_dir, base, {"format": "narrow"})
    return infos, cov_stats


def gen_wide_device(
    schema: dict, rows: list, out_dir: str,
    bucket_ms: int = 0, use_cov: bool = False, split: bool = False,
) -> tuple:
    """
    Returns (infos, cov_stats_by_device).
    cov_stats_by_device: {device: {rows_before, rows_after, rows_dropped, pct_dropped}}

    When split=True, generates two files per device:
      wide_device_{dev}_fast.parquet  — fast-class signals only (no COV)
      wide_device_{dev}_slow.parquet  — slow+medium+bool signals (COV applied if use_cov)
    Split stats are recorded under cov_stats[dev]["split"].
    """
    df_all = pd.DataFrame(rows)
    df_all["timestamp"] = pd.to_datetime(df_all["timestamp"])
    if bucket_ms > 0:
        df_all = bucket_timestamps(df_all, bucket_ms)

    all_infos = []
    cov_stats = {}

    for dev in schema.keys():
        df_dev = df_all[df_all["device"] == dev].copy()
        if df_dev.empty:
            continue

        pivot = df_dev.pivot_table(
            index=["timestamp", "unit"],
            columns="point_name",
            values="value",
            aggfunc="first",
        ).reset_index()
        pivot.columns.name = None

        rows_before = len(pivot)
        n_dropped = 0

        if not split:
            # Original behaviour: one file per device, optional COV
            if use_cov:
                pivot, n_dropped = apply_cov(pivot)
            rows_after = len(pivot)

            cov_stats[dev] = {
                "rows_before": rows_before,
                "rows_after":  rows_after,
                "rows_dropped": n_dropped,
                "pct_dropped": round(n_dropped / max(rows_before, 1) * 100, 1),
            }

            safe_dev = dev.replace("/", "_")
            suffix = "_cov" if use_cov else ""
            base = f"wide_device_{safe_dev}{suffix}.parquet"
            infos = write_variants(pivot, out_dir, base, {"format": "wide_device", "device": dev})
            all_infos.extend(infos)

        else:
            # Split mode: separate fast and slow files per device
            meta_cols = ["timestamp", "unit"]
            data_cols = [c for c in pivot.columns if c not in meta_cols]

            fast_cols = [c for c in data_cols if _signal_class(c) == "fast"]
            slow_cols = [c for c in data_cols if c not in fast_cols]

            safe_dev = dev.replace("/", "_")
            cov_suffix = "_cov" if use_cov else ""

            split_info = {
                "fast_cols": len(fast_cols),
                "slow_cols": len(slow_cols),
                "rows": rows_before,
            }

            # Fast file — no COV (fast signals change every cycle, COV is pointless)
            if fast_cols:
                df_fast = pivot[meta_cols + fast_cols]
                base_fast = f"wide_device_{safe_dev}_fast.parquet"
                infos_fast = write_variants(
                    df_fast, out_dir, base_fast,
                    {"format": "wide_device_fast", "device": dev}
                )
                all_infos.extend(infos_fast)

            # Slow file — COV applied if requested
            slow_dropped = 0
            slow_after = rows_before
            if slow_cols:
                df_slow = pivot[meta_cols + slow_cols]
                slow_before = len(df_slow)
                if use_cov:
                    df_slow, slow_dropped = apply_cov(df_slow)
                slow_after = len(df_slow)
                pct_slow = round(slow_dropped / max(slow_before, 1) * 100, 1)
                split_info["slow_rows_before"] = slow_before
                split_info["slow_rows_after"]  = slow_after
                split_info["slow_pct_dropped"] = pct_slow

                base_slow = f"wide_device_{safe_dev}_slow{cov_suffix}.parquet"
                infos_slow = write_variants(
                    df_slow, out_dir, base_slow,
                    {"format": "wide_device_slow", "device": dev}
                )
                all_infos.extend(infos_slow)

            cov_stats[dev] = {
                "rows_before": rows_before,
                "rows_after":  rows_before,   # fast file always full rows; slow may be less
                "rows_dropped": 0,
                "pct_dropped": 0.0,
                "split": split_info,
            }

    return all_infos, cov_stats


def gen_wide_point_name(
    schema: dict, rows: list, out_dir: str,
    bucket_ms: int = 0, use_cov: bool = False,
) -> tuple:
    """Returns (infos, cov_stats)."""
    df_all = pd.DataFrame(rows)
    df_all["timestamp"] = pd.to_datetime(df_all["timestamp"])
    if bucket_ms > 0:
        df_all = bucket_timestamps(df_all, bucket_ms)

    df_all["col_name"] = df_all["device"] + "__" + df_all["point_name"]

    pivot = df_all.pivot_table(
        index=["timestamp", "unit"],
        columns="col_name",
        values="value",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None

    rows_before = len(pivot)
    n_dropped = 0
    if use_cov:
        pivot, n_dropped = apply_cov(pivot)

    cov_stats = {
        "rows_before": rows_before,
        "rows_after":  len(pivot),
        "rows_dropped": n_dropped,
        "pct_dropped": round(n_dropped / max(rows_before, 1) * 100, 1),
    }

    suffix = "_cov" if use_cov else ""
    base = f"wide_point_name_10min{suffix}.parquet"
    infos = write_variants(pivot, out_dir, base, {"format": "wide_point_name"})
    return infos, cov_stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic BESS parquet data from prelim schema.")
    parser.add_argument("--format", choices=["narrow", "wide_device", "wide_point_name", "all"],
                        default="all")
    parser.add_argument("--duration",    type=int,   default=600,  help="Seconds of data (default: 600)")
    parser.add_argument("--interval",    type=int,   default=10,   help="Scan interval seconds (default: 10)")
    parser.add_argument("--units",       type=int,   default=8,    help="Number of BESS units (default: 8)")
    parser.add_argument("--out",         default="/tmp/prelim_generated", help="Output directory")
    parser.add_argument("--seed",        type=int,   default=42)
    parser.add_argument("--time-jitter", type=int,   default=0,
                        metavar="MS",  help="±MS random timestamp scatter per point (default: 0)")
    parser.add_argument("--bucket",      type=int,   default=0,
                        metavar="MS",  help="Snap timestamps to MS grid before pivot (default: 0 = off)")
    parser.add_argument("--cov",         action="store_true",
                        help="Apply change-on-value filter to wide-format rows")
    parser.add_argument("--split",       action="store_true",
                        help="Generate separate fast/slow wide-device files per device (--cov applies to slow file only)")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print(f"Loading schema from {SCHEMA_PATH} ...")
    schema = load_schema(SCHEMA_PATH)
    total_points = sum(len(v) for v in schema.values())

    unit_ids = build_unit_ids(args.units)
    n_cycles = args.duration // args.interval

    na_points   = len(schema.get("NA", []))
    bess_points = sum(len(v) for dev, v in schema.items() if dev != "NA")

    print(f"Schema   : {len(schema)} devices, {total_points} points total")
    print(f"Units    : {len(unit_ids)} BESS units + NA site-level")
    print(f"Cycles   : {n_cycles}  ({args.duration}s / {args.interval}s interval)")
    if args.time_jitter:
        print(f"Jitter   : ±{args.time_jitter} ms per point")
    if args.bucket:
        print(f"Bucket   : {args.bucket} ms time grid")
    if args.cov:
        print(f"COV      : enabled")
    if args.split:
        print(f"Split    : enabled (fast/slow wide-device files)")
    print(f"Expected narrow rows: {n_cycles * (na_points + len(unit_ids) * bess_points):,}")
    print()

    rng = np.random.default_rng(args.seed)

    print("Generating rows ...")
    t0 = time.time()
    rows = generate_rows(schema, unit_ids, n_cycles, args.interval, rng, args.time_jitter)
    print(f"Generated {len(rows):,} rows in {time.time()-t0:.1f}s")
    print()

    all_file_infos = []
    all_cov_stats  = {}
    fmt = args.format

    if fmt in ("narrow", "all"):
        label = "narrow" + (" + COV (per-signal)" if args.cov else "")
        print(f"Writing {label} format ...")
        infos, cov_stats = gen_narrow(rows, args.out, args.bucket, args.cov)
        all_file_infos.extend(infos)
        all_cov_stats["narrow"] = cov_stats

    if fmt in ("wide_device", "all"):
        label = "wide-device"
        if args.split:
            label += " + split (fast/slow)"
        if args.cov:
            label += " + COV"
        print(f"Writing {label} format (9 devices × 3 codecs) ...")
        infos, cov_stats = gen_wide_device(
            schema, rows, args.out, args.bucket, args.cov, split=args.split
        )
        all_file_infos.extend(infos)
        all_cov_stats["wide_device"] = cov_stats

    if fmt in ("wide_point_name", "all"):
        label = "wide-point-name" + (" + COV" if args.cov else "")
        print(f"Writing {label} format ...")
        infos, cov_stats = gen_wide_point_name(schema, rows, args.out, args.bucket, args.cov)
        all_file_infos.extend(infos)
        all_cov_stats["wide_point_name"] = cov_stats

    # ── Print summary ────────────────────────────────────────────────────────
    print()
    print("=" * 76)
    narrow_snappy_size = next(
        (i["size_bytes"] for i in all_file_infos
         if i.get("format") == "narrow" and i.get("codec") == "snappy"), None
    )

    for info in all_file_infos:
        fname = info["filename"]
        size  = info["size_bytes"]
        rows_n = info["rows"]
        bpr   = info["bytes_per_row"]
        vs = ""
        if narrow_snappy_size and info.get("codec") == "snappy" and info.get("format") != "narrow":
            ratio = size / narrow_snappy_size
            vs = f"  ({ratio:.1f}× narrow)"
        print(f"  {fname:<52} {size:>12,} B  {bpr:>6.1f} B/row  {rows_n:>8,} rows{vs}")

    print()

    # Wide-device totals
    if fmt in ("wide_device", "all"):
        by_codec = {}
        for info in all_file_infos:
            if info.get("format") == "wide_device":
                c = info["codec"]
                by_codec.setdefault(c, 0)
                by_codec[c] += info["size_bytes"]
        for c, total in by_codec.items():
            print(f"  Wide-device total ({c}): {total:,} B ({fmt_bytes(total)})")
        print()

    # COV summary
    if args.cov and all_cov_stats:
        print("COV results:")
        for fmt_name, stats in all_cov_stats.items():
            if isinstance(stats, dict) and "rows_before" in stats:
                # narrow or wide_point_name
                label = "per-signal" if fmt_name == "narrow" else "per-row"
                print(f"  {fmt_name} ({label}): {stats['rows_before']:,} → {stats['rows_after']:,} rows "
                      f"({stats['pct_dropped']:.1f}% dropped)")
            else:
                # wide_device: dict of devices
                total_before = sum(s["rows_before"] for s in stats.values())
                total_after  = sum(s["rows_after"]  for s in stats.values())
                pct = (total_before - total_after) / max(total_before, 1) * 100
                print(f"  {fmt_name} (per-row): {total_before:,} → {total_after:,} rows ({pct:.1f}% dropped)")
                for dev, s in sorted(stats.items()):
                    print(f"    {dev:<22} {s['rows_before']:>6,} → {s['rows_after']:>6,}"
                          f"  ({s['pct_dropped']:>5.1f}% dropped)")
        print()

    # Split summary
    if args.split and "wide_device" in all_cov_stats:
        print("Split results (wide-device):")
        wd_stats = all_cov_stats["wide_device"]
        for dev, s in sorted(wd_stats.items()):
            sp = s.get("split", {})
            fast_c = sp.get("fast_cols", 0)
            slow_c = sp.get("slow_cols", 0)
            rows_n = sp.get("rows", s.get("rows_before", 0))
            slow_before = sp.get("slow_rows_before", rows_n)
            slow_after  = sp.get("slow_rows_after",  rows_n)
            slow_pct    = sp.get("slow_pct_dropped",  0.0)
            slow_detail = (f"{slow_before:,}→{slow_after:,} rows ({slow_pct:.1f}% COV drop)"
                           if args.cov else f"{rows_n:,} rows")
            print(f"  {dev:<22}  fast: {fast_c} cols,  {rows_n:,} rows   "
                  f"slow: {slow_c} cols, {slow_detail}")
        print()

    # ── Write summary.json ───────────────────────────────────────────────────
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "params": {
            "format":      fmt,
            "duration":    args.duration,
            "interval":    args.interval,
            "units":       args.units,
            "time_jitter": args.time_jitter,
            "bucket_ms":   args.bucket,
            "cov":         args.cov,
            "split":       args.split,
        },
        "schema": {
            "site":                 SITE_ID,
            "units":                unit_ids,
            "na_unit":              NA_UNIT,
            "devices":              list(schema.keys()),
            "total_points":         total_points,
            "na_points":            na_points,
            "bess_points_per_unit": bess_points,
            "scan_cycles_per_unit": n_cycles,
            "total_rows_narrow":    len(rows),
        },
        "cov_stats": all_cov_stats,
        "split_stats": (
            {dev: s.get("split", {})
             for dev, s in all_cov_stats.get("wide_device", {}).items()
             if isinstance(s, dict) and "split" in s}
            if args.split else {}
        ),
        "files": all_file_infos,
    }

    summary_path = os.path.join(args.out, "summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    main()
