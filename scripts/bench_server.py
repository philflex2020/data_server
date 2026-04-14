#!/usr/bin/env python3
"""
bench_server.py — static file server with CORS + parquet API endpoints.

Usage:
  python3 scripts/bench_server.py [--port 8780] [--directory /mnt/tort-sdi/bench]

Endpoints:
  GET  /                          — directory listing (HTML)
  GET  /<file>                    — static file (CORS headers added)
  GET  /api/ls?path=<relpath>     — JSON directory tree under base_dir
  GET  /api/parquet?path=<rel>&rows=100  — parquet file as JSON {schema, rows}
  GET  /api/storage                      — parquet + influx disk/row/time stats
"""
import http.server, json, os, sys, urllib.parse, argparse, subprocess, glob as _glob

parser = argparse.ArgumentParser()
parser.add_argument("--port",      type=int, default=8780)
parser.add_argument("--directory", default=".")
parser.add_argument("--influx-token",  default="xEbFxkBI0kQ4_wnMZOQ6M8ERvtIGhQt8kx7O9yI6ovr_ihwDVeo8gTI4-XYnu-zRMIxldPd73glCesccK3E9Dg==")
parser.add_argument("--influx-org",    default="longbow")
parser.add_argument("--influx-bucket", default="sbess3")
parser.add_argument("--influx-engine", default="/mnt/tort-sdi/influxdb/engine")
parser.add_argument("--parquet-dir",   default="bench-wide")
args = parser.parse_args()

BASE_DIR = os.path.abspath(args.directory)

def safe_path(rel):
    """Resolve rel path under BASE_DIR, reject traversal."""
    p = os.path.normpath(os.path.join(BASE_DIR, rel.lstrip("/")))
    if not p.startswith(BASE_DIR):
        return None
    return p

def api_ls(rel):
    root = safe_path(rel) if rel else BASE_DIR
    if not root or not os.path.isdir(root):
        return None, 404
    result = []
    for entry in sorted(os.scandir(root), key=lambda e: (e.is_file(), e.name)):
        info = {"name": entry.name,
                "path": os.path.relpath(entry.path, BASE_DIR),
                "type": "file" if entry.is_file() else "dir"}
        if entry.is_file():
            info["size"] = entry.stat().st_size
        result.append(info)
    return result, 200

def api_parquet(rel, max_rows=200):
    p = safe_path(rel)
    if not p or not os.path.isfile(p):
        return {"error": f"file not found: {rel}"}, 404
    try:
        import pyarrow.parquet as pq, pyarrow as pa
        tbl = pq.read_table(p)
        # limit rows
        tbl = tbl.slice(0, max_rows)
        schema = [{"name": f.name, "type": str(f.type)} for f in tbl.schema]
        # convert to list-of-dicts, handling timestamps
        rows = []
        cols = {f.name: tbl.column(f.name).to_pylist() for f in tbl.schema}
        for i in range(tbl.num_rows):
            row = {}
            for f in tbl.schema:
                v = cols[f.name][i]
                if hasattr(v, 'isoformat'):
                    v = v.isoformat()
                elif v is None:
                    v = None
                row[f.name] = v
            rows.append(row)
        total_rows = pq.read_metadata(p).num_rows
        return {"schema": schema, "rows": rows,
                "total_rows": total_rows, "shown": len(rows),
                "file": rel, "size_bytes": os.path.getsize(p)}, 200
    except Exception as e:
        return {"error": str(e)}, 500



def _dir_bytes(path):
    """Walk a directory tree and sum file sizes."""
    total = 0
    try:
        for root, dirs, files in os.walk(path):
            for fn in files:
                try:
                    total += os.path.getsize(os.path.join(root, fn))
                except OSError:
                    pass
    except Exception:
        pass
    return total


def _influx_bytes_from_metrics(url='http://localhost:8086'):
    """Sum TSM + WAL bytes from InfluxDB Prometheus /metrics (no auth needed)."""
    import urllib.request as _ur
    try:
        with _ur.urlopen(f'{url}/metrics', timeout=5) as r:
            content = r.read().decode()
        total = 0.0
        for line in content.splitlines():
            if line.startswith('#') or not line.strip():
                continue
            if 'storage_tsm_files_disk_bytes{' in line or 'storage_wal_size{' in line:
                try:
                    total += float(line.split('}')[-1].strip())
                except (ValueError, IndexError):
                    pass
        return int(total)
    except Exception:
        return 0


def _influx_query(token, org, query):
    """Run a Flux query via influx CLI; return stdout string."""
    try:
        r = subprocess.run(
            ['influx', 'query', '--org', org, '--token', token, query],
            capture_output=True, text=True, timeout=20
        )
        return r.stdout
    except Exception as e:
        return str(e)


def _parse_influx_scalar(output):
    """Extract the last numeric value from annotated CSV output."""
    for line in reversed(output.strip().splitlines()):
        if line.startswith('#') or line.startswith(','):
            continue
        parts = line.strip().split(',')
        for p in reversed(parts):
            p = p.strip()
            try:
                return int(p)
            except ValueError:
                try:
                    return float(p)
                except ValueError:
                    pass
    return None


def _parse_influx_ts(output):
    """Extract first ISO timestamp from annotated CSV — skip header lines."""
    import re as _re
    for line in output.strip().splitlines():
        if line.startswith('#') or line.startswith(',_'):
            continue
        m = _re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.\d]*Z)', line)
        if m:
            return m.group(1)
    return None


def api_storage():
    result = {}

    # ── Parquet ──────────────────────────────────────────────────────────────
    pq_dir   = safe_path(args.parquet_dir) if args.parquet_dir else None
    pq_bytes = _dir_bytes(pq_dir) if pq_dir else 0
    pq_files = 0
    pq_rows  = 0
    pq_first = None
    pq_last  = None

    if pq_dir and os.path.isdir(pq_dir):
        files = sorted(_glob.glob(os.path.join(pq_dir, '**', '*.parquet'), recursive=True))
        pq_files = len(files)
        try:
            import pyarrow.parquet as pq
            for fp in files:
                pq_rows += pq.read_metadata(fp).num_rows
            if files:
                # first ts: min across all values in first file
                # last ts:  max across all values in last file
                t0 = pq.read_table(files[0],  columns=['ts'])['ts'].to_pylist()
                t1 = pq.read_table(files[-1], columns=['ts'])['ts'].to_pylist()
                v0 = [t for t in t0 if t]
                v1 = [t for t in t1 if t]
                if v0:
                    pq_first = min(v0).isoformat()
                if v1:
                    pq_last  = max(v1).isoformat()
        except Exception as e:
            result['parquet_error'] = str(e)

    result['parquet'] = {
        'bytes': pq_bytes, 'files': pq_files, 'rows': pq_rows,
        'first_ts': pq_first, 'last_ts': pq_last,
        'dir': pq_dir or ''
    }

    # ── InfluxDB2 ─────────────────────────────────────────────────────────────
    # Bytes: read from Prometheus /metrics (no auth, no dir permission needed)
    inf_bytes = _influx_bytes_from_metrics()
    inf_rows  = None
    inf_first = None
    inf_last  = None

    tok = args.influx_token
    org = args.influx_org
    bkt = args.influx_bucket

    if tok:
        # row count — use 6h window matching parquet run duration
        q_count = (f'from(bucket:"{bkt}") |> range(start:-6h)'
                   ' |> count() |> group() |> sum(column:"_value")')
        inf_rows = _parse_influx_scalar(_influx_query(tok, org, q_count))

        # first timestamp — use 6h window, filter to avoid stale tombstoned data
        q_first = (f'from(bucket:"{bkt}") |> range(start:-6h)'
                   f' |> filter(fn:(r)=>r._measurement=="bench")'
                   ' |> first() |> group() |> min(column:"_time")')
        inf_first = _parse_influx_ts(_influx_query(tok, org, q_first))

        # last timestamp
        q_last = (f'from(bucket:"{bkt}") |> range(start:-6h)'
                  f' |> filter(fn:(r)=>r._measurement=="bench")'
                  ' |> last() |> group() |> max(column:"_time")')
        inf_last = _parse_influx_ts(_influx_query(tok, org, q_last))

    result['influx'] = {
        'bytes': inf_bytes, 'rows': inf_rows,
        'first_ts': inf_first, 'last_ts': inf_last,
        'engine': args.influx_engine
    }

    return result, 200

class BenchHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=BASE_DIR, **kw)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200); self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs     = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/api/ls":
            data, code = api_ls(qs.get("path", [""])[0])
            self._json(data, code)
        elif parsed.path == "/api/storage":
            data, code = api_storage()
            self._json(data, code)
        elif parsed.path == "/api/parquet":
            rel  = qs.get("path", [""])[0]
            rows = int(qs.get("rows", ["200"])[0])
            data, code = api_parquet(rel, rows)
            self._json(data, code)
        else:
            super().do_GET()

    def _json(self, data, code=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(fmt % args, flush=True)


print(f"Bench server on :{args.port}  serving {BASE_DIR}", flush=True)
http.server.test(BenchHandler, http.server.HTTPServer,
                 port=args.port, bind="0.0.0.0")
