#!/usr/bin/env python3
# inspect_parquet.py — summarise parquet files from a Docker volume or k3s pod
#
# Usage:
#   python3 inspect_parquet.py                        # Docker volume: test_parquet-out
#   python3 inspect_parquet.py --vol <volume>         # Docker volume: named volume
#   python3 inspect_parquet.py --pod [namespace]      # k3s/K8s: copy from writer pod
#   python3 inspect_parquet.py --dir <path>           # local directory

import subprocess, sys, os, tempfile, glob
import pyarrow.parquet as pq


def print_file(path, label=None):
    t    = pq.read_table(path)
    rows = len(t)
    cols = t.schema.names
    size = os.path.getsize(path)
    name = label or os.path.basename(path)
    print(f'  {name}')
    print(f'    rows   : {rows}')
    print(f'    columns: {len(cols)}  {cols}')
    print(f'    size   : {size/1024:.1f} kB')
    if rows:
        ts_col = t.column('ts')
        print(f'    ts range: {ts_col[0].as_py()} -> {ts_col[-1].as_py()}')
        if 'point_name' in cols:
            pts = t.column('point_name').unique().to_pylist()
            print(f'    signals : {len(pts)}  {pts[:5]}{"..." if len(pts) > 5 else ""}')
    print()


def from_docker_volume(vol):
    result = subprocess.run(
        ['docker', 'run', '--rm', '-v', f'{vol}:/data', 'alpine',
         'find', '/data', '-name', '*.parquet'],
        capture_output=True, text=True
    )
    files = [f.strip() for f in result.stdout.splitlines() if f.strip()]
    if not files:
        print(f'No parquet files found in volume {vol}')
        return
    print(f'{len(files)} file(s) in Docker volume {vol}\n')
    with tempfile.TemporaryDirectory() as tmp:
        for src in sorted(files):
            fname = os.path.basename(src)
            dst   = os.path.join(tmp, fname)
            subprocess.run(
                ['docker', 'run', '--rm',
                 '-v', f'{vol}:/data', '-v', f'{tmp}:/out',
                 'alpine', 'cp', src, f'/out/{fname}'],
                check=True, capture_output=True
            )
            print_file(dst)


def from_k8s_pod(namespace):
    # find the writer pod
    result = subprocess.run(
        ['kubectl', 'get', 'pod', '-n', namespace,
         '-l', 'app=parquet-writer',
         '-o', 'jsonpath={.items[0].metadata.name}'],
        capture_output=True, text=True
    )
    pod = result.stdout.strip()
    if not pod:
        print(f'No parquet-writer pod found in namespace {namespace}')
        return
    print(f'Copying from pod {pod} in namespace {namespace}...')
    with tempfile.TemporaryDirectory() as tmp:
        dest = os.path.join(tmp, 'data')
        subprocess.run(
            ['kubectl', 'cp', f'{namespace}/{pod}:/data', dest],
            check=True
        )
        files = sorted(glob.glob(f'{dest}/**/*.parquet', recursive=True))
        if not files:
            print('No parquet files found in pod /data')
            return
        print(f'{len(files)} file(s) from pod {pod}\n')
        for f in files:
            print_file(f)


def from_local_dir(path):
    files = sorted(glob.glob(f'{path}/**/*.parquet', recursive=True))
    if not files:
        print(f'No parquet files found in {path}')
        return
    print(f'{len(files)} file(s) in {path}\n')
    for f in files:
        print_file(f)


# --- main ---
args = sys.argv[1:]

if '--pod' in args:
    idx = args.index('--pod')
    ns  = args[idx + 1] if idx + 1 < len(args) and not args[idx+1].startswith('--') else 'data-capture'
    from_k8s_pod(ns)

elif '--dir' in args:
    idx  = args.index('--dir')
    path = args[idx + 1]
    from_local_dir(path)

elif '--vol' in args:
    idx = args.index('--vol')
    vol = args[idx + 1]
    from_docker_volume(vol)

else:
    # default: Docker volume test_parquet-out
    vol = args[0] if args else 'test_parquet-out'
    from_docker_volume(vol)
