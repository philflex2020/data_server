#!/usr/bin/env python3
"""
dump_parquet.py — print rows from parquet files to verify topic parsing

Usage:
  python3 dump_parquet.py                      # latest file from k3s pod, 200 rows
  python3 dump_parquet.py 50                   # first 50 rows
  python3 dump_parquet.py --all                # all files
  python3 dump_parquet.py --pod [namespace]    # k3s pod (default: data-capture)
  python3 dump_parquet.py --dir <path>         # local directory
  python3 dump_parquet.py --col <col>          # filter by column value, e.g. --col device=bms_1
"""

import subprocess, sys, os, tempfile, glob, argparse
import pyarrow.parquet as pq
import pyarrow.compute as pc


def fetch_from_pod(namespace):
    result = subprocess.run(
        ['kubectl', 'get', 'pod', '-n', namespace,
         '-l', 'app=parquet-writer',
         '-o', 'jsonpath={.items[0].metadata.name}'],
        capture_output=True, text=True
    )
    pod = result.stdout.strip()
    if not pod:
        sys.exit(f'No parquet-writer pod found in namespace {namespace}')
    tmp = tempfile.mkdtemp()
    dest = os.path.join(tmp, 'data')
    subprocess.run(['kubectl', 'cp', f'{namespace}/{pod}:/data', dest], check=True)
    return sorted(glob.glob(f'{dest}/**/*.parquet', recursive=True))


def fetch_from_dir(path):
    return sorted(glob.glob(f'{path}/**/*.parquet', recursive=True))


def dump_file(path, max_rows, filters):
    t = pq.read_table(path)
    name = os.path.basename(path)

    # apply column filters
    for col, val in filters:
        if col in t.schema.names:
            mask = pc.equal(t.column(col), val)
            t = t.filter(mask)

    total = len(t)
    t = t.slice(0, max_rows)
    shown = len(t)

    print(f'\n=== {name}  ({total} rows total, showing {shown}) ===')
    if shown == 0:
        print('  (no rows match filters)')
        return

    # column widths
    cols = t.schema.names
    col_data = {c: [str(t.column(c)[i].as_py()) for i in range(shown)] for c in cols}
    widths   = {c: max(len(c), max((len(v) for v in col_data[c]), default=0)) for c in cols}

    header = '  ' + '  '.join(c.ljust(widths[c]) for c in cols)
    sep    = '  ' + '  '.join('-' * widths[c] for c in cols)
    print(header)
    print(sep)
    for i in range(shown):
        print('  ' + '  '.join(col_data[c][i].ljust(widths[c]) for c in cols))


# --- arg parse ---
ap = argparse.ArgumentParser(
    description='Dump rows from parquet files to verify topic parsing.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
ap.add_argument('rows',      nargs='?', type=int, default=200,
                help='Max rows to show per file')
ap.add_argument('--all',     action='store_true',
                help='Show all files (default: latest only)')
ap.add_argument('--pod',     nargs='?', const='data-capture', default=None,
                metavar='NAMESPACE', help='Read from k3s pod')
ap.add_argument('--dir',     metavar='PATH', help='Read from local directory')
ap.add_argument('--col',     action='append', default=[], metavar='COL=VAL',
                help='Filter rows by column value (repeatable)')
args = ap.parse_args()

filters = []
for f in args.col:
    if '=' not in f:
        sys.exit(f'--col must be COL=VAL, got: {f}')
    k, v = f.split('=', 1)
    filters.append((k, v))

# fetch files
if args.dir:
    files = fetch_from_dir(args.dir)
elif args.pod is not None:
    files = fetch_from_pod(args.pod)
else:
    files = fetch_from_pod('data-capture')

if not files:
    sys.exit('No parquet files found.')

if not args.all:
    files = [files[-1]]   # latest only

for f in files:
    dump_file(f, args.rows, filters)
