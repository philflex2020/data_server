#!/usr/bin/env python3
"""
compact_parquet.py — merge flush files in each leaf partition directory
into a single compacted parquet file.

Usage:
  python3 scripts/compact_parquet.py <base_dir> [--dry-run] [--chunk-minutes N]

Each leaf directory (e.g. site=SITE_A/2026/04/10/) gets its flush files
(YYYYMMDDTHHMMSSZ.parquet) merged into one compact_<dir>.parquet file.
The originals are removed unless --dry-run is set.

Example:
  python3 scripts/compact_parquet.py /mnt/tort-sdi/bench/bench-wide
  python3 scripts/compact_parquet.py /mnt/tort-sdi/bench/bench-norm-long --dry-run
"""
import os, sys, argparse, collections, tempfile, shutil
import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser(description="Compact parquet flush files")
parser.add_argument("base_dir", help="Base directory to compact recursively")
parser.add_argument("--dry-run", action="store_true",
                    help="Print what would be done without writing")
parser.add_argument("--chunk-minutes", type=int, default=0,
                    help="Group files into N-minute chunks (0 = one file per leaf dir)")
parser.add_argument("--compression", default="snappy",
                    choices=["snappy","zstd","gzip","none"],
                    help="Parquet compression codec (default: snappy)")
args = parser.parse_args()

base = os.path.abspath(args.base_dir)
if not os.path.isdir(base):
    print(f"ERROR: {base} is not a directory", file=sys.stderr)
    sys.exit(1)

# collect leaf directories (dirs with no subdirs)
leaf_dirs = {}
for root, dirs, fnames in os.walk(base):
    pq_files = sorted(f for f in fnames if f.endswith(".parquet") and not f.startswith("compact_"))
    if pq_files and not dirs:
        leaf_dirs[root] = [os.path.join(root, f) for f in pq_files]

if not leaf_dirs:
    print(f"No parquet files found under {base}")
    sys.exit(0)

total_in_files = sum(len(v) for v in leaf_dirs.values())
total_in_bytes = sum(os.path.getsize(f) for flist in leaf_dirs.values() for f in flist)
print(f"Found {len(leaf_dirs)} leaf dirs, {total_in_files} files, "
      f"{total_in_bytes//1024} KB total")
if args.dry_run:
    print("DRY RUN — no files will be written\n")

total_out_files, total_out_bytes, total_rows = 0, 0, 0

for leaf, files in sorted(leaf_dirs.items()):
    rel = os.path.relpath(leaf, base)
    in_bytes = sum(os.path.getsize(f) for f in files)

    if args.dry_run:
        print(f"  {rel}: {len(files)} files → 1  ({in_bytes//1024} KB)")
        total_out_files += 1
        total_out_bytes += in_bytes  # estimate
        continue

    try:
        tables = [pq.read_table(f) for f in files]
        merged = pa.concat_tables(tables)
        out_name = f"compact_{os.path.basename(leaf)}.parquet"
        out_path = os.path.join(leaf, out_name)
        codec = None if args.compression == "none" else args.compression
        pq.write_table(merged, out_path, compression=codec)
        out_bytes = os.path.getsize(out_path)
        total_out_files += 1
        total_out_bytes += out_bytes
        total_rows += merged.num_rows
        # remove originals
        for f in files:
            os.remove(f)
        ratio = in_bytes / out_bytes if out_bytes else 1
        print(f"  {rel}: {len(files)} → 1  {in_bytes//1024}→{out_bytes//1024} KB  "
              f"ratio={ratio:.2f}x  rows={merged.num_rows:,}")
    except Exception as e:
        print(f"  ERROR {rel}: {e}", file=sys.stderr)

print(f"\n{'DRY RUN ' if args.dry_run else ''}Summary:")
print(f"  Input : {total_in_files} files  {total_in_bytes//1024} KB")
print(f"  Output: {total_out_files} files  {total_out_bytes//1024} KB")
if not args.dry_run and total_rows:
    print(f"  Rows  : {total_rows:,}")
if total_in_bytes and total_out_bytes:
    print(f"  Ratio : {total_in_bytes/total_out_bytes:.2f}x")
