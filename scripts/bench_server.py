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
"""
import http.server, json, os, sys, urllib.parse, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--port",      type=int, default=8780)
parser.add_argument("--directory", default=".")
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
