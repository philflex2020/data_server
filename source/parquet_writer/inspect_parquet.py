#!/usr/bin/env python3
# inspect_parquet.py — summarise parquet files in the test container volume
# Usage: python3 inspect_parquet.py [volume_name]
import subprocess, sys, os, tempfile
import pyarrow.parquet as pq

VOL = sys.argv[1] if len(sys.argv) > 1 else 'test_parquet-out'

# List parquet files inside the volume
result = subprocess.run(
    ['docker', 'run', '--rm', '-v', f'{VOL}:/data', 'alpine',
     'find', '/data', '-name', '*.parquet'],
    capture_output=True, text=True
)
files = [f.strip() for f in result.stdout.splitlines() if f.strip()]

if not files:
    print(f'No parquet files found in volume {VOL}')
    sys.exit(0)

print(f'{len(files)} file(s) in {VOL}\n')

with tempfile.TemporaryDirectory() as tmp:
    for src in sorted(files):
        fname = os.path.basename(src)
        dst   = os.path.join(tmp, fname)

        subprocess.run(
            ['docker', 'run', '--rm',
             '-v', f'{VOL}:/data',
             '-v', f'{tmp}:/out',
             'alpine', 'cp', src, f'/out/{fname}'],
            check=True, capture_output=True
        )

        t    = pq.read_table(dst)
        rows = len(t)
        cols = t.schema.names
        size = os.path.getsize(dst)

        print(f'  {fname}')
        print(f'    rows   : {rows}')
        print(f'    columns: {len(cols)}  {cols}')
        print(f'    size   : {size/1024:.1f} kB')
        if rows:
            ts_col = t.column('ts')
            print(f'    ts range: {ts_col[0].as_py()} -> {ts_col[-1].as_py()}')
            if 'point_name' in cols:
                pts = t.column('point_name').unique().to_pylist()
                print(f'    signals : {len(pts)}  {pts[:5]}{"..." if len(pts)>5 else ""}')
        print()
