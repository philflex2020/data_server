#!/usr/bin/env python3
"""
query_server.py — DuckDB HTTP query server for parquet-writer data

Endpoints:
  GET  /health              pod status, parquet file count, disk free
  GET  /sites               distinct unit_ids in the data directory
  GET  /latest?site=X&n=100 last N rows (default 100), optional unit_id filter
  POST /query               body: {"sql": "SELECT * FROM data LIMIT 10"}

'data' is a pre-registered DuckDB view over all parquet files in DATA_PATH.
Use it in any SQL:  SELECT site_id, point_name, val FROM data WHERE ts > now() - INTERVAL 1 HOUR

Environment:
  DATA_PATH    parquet root directory (default: /data/site-capture)
  QUERY_PORT   listen port            (default: 8772)
"""

import os, glob, shutil, json, datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import duckdb

DATA_PATH = os.environ.get("DATA_PATH", "/data/site-capture")
PORT      = int(os.environ.get("QUERY_PORT", 8772))
GLOB      = f"{DATA_PATH}/**/*.parquet"


def new_con():
    """Open a fresh in-memory DuckDB connection with a 'data' view over all parquet files."""
    db    = duckdb.connect(":memory:")
    files = glob.glob(GLOB, recursive=True)
    if files:
        db.execute(
            f"CREATE VIEW data AS "
            f"SELECT * FROM read_parquet('{GLOB}', hive_partitioning=true, union_by_name=true)"
        )
    return db, len(files)


def rows_to_json(result):
    """Convert a DuckDB result to a JSON-serialisable list of dicts."""
    cols = [d[0] for d in result.description]
    out  = []
    for row in result.fetchall():
        d = {}
        for k, v in zip(cols, row):
            if isinstance(v, (datetime.datetime, datetime.date)):
                d[k] = v.isoformat()
            elif hasattr(v, "item"):   # numpy scalar
                d[k] = v.item()
            else:
                d[k] = v
        out.append(d)
    return cols, out


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # suppress default per-request noise; errors still go to stderr

    def send_json(self, code, obj):
        body = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def read_body_json(self):
        n = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(n) if n else b"{}"
        try:
            return json.loads(raw)
        except Exception:
            return None

    # ------------------------------------------------------------------
    def do_GET(self):
        parsed = urlparse(self.path)
        qs     = parse_qs(parsed.query)
        path   = parsed.path

        if path == "/health":
            files = glob.glob(GLOB, recursive=True)
            free  = shutil.disk_usage(DATA_PATH).free // (1024 ** 3) \
                    if os.path.exists(DATA_PATH) else -1
            self.send_json(200, {
                "status":        "ok",
                "data_path":     DATA_PATH,
                "parquet_files": len(files),
                "disk_free_gb":  free,
            })

        elif path == "/sites":
            db, n = new_con()
            if n == 0:
                self.send_json(200, {"sites": []})
                return
            try:
                _, rows = rows_to_json(
                    db.execute("SELECT DISTINCT unit_id FROM data ORDER BY 1")
                )
                self.send_json(200, {"sites": [r["unit_id"] for r in rows]})
            except Exception as e:
                self.send_json(500, {"error": str(e)})

        elif path == "/latest":
            site = qs.get("site", [None])[0]
            n    = int(qs.get("n", ["100"])[0])
            db, fc = new_con()
            if fc == 0:
                self.send_json(200, {"rows": [], "count": 0})
                return
            try:
                where = f"WHERE unit_id = '{site}'" if site else ""
                cols, rows = rows_to_json(
                    db.execute(f"SELECT * FROM data {where} ORDER BY ts DESC LIMIT {n}")
                )
                self.send_json(200, {"rows": rows, "count": len(rows), "columns": cols})
            except Exception as e:
                self.send_json(500, {"error": str(e)})

        else:
            self.send_json(404, {"error": f"unknown path: {path}"})

    # ------------------------------------------------------------------
    def do_POST(self):
        if self.path != "/query":
            self.send_json(404, {"error": "POST /query is the only POST endpoint"})
            return

        body = self.read_body_json()
        if body is None:
            self.send_json(400, {"error": "invalid JSON body"})
            return

        sql = body.get("sql", "").strip()
        if not sql:
            self.send_json(400, {"error": "missing 'sql' in request body"})
            return

        db, _ = new_con()
        try:
            cols, rows = rows_to_json(db.execute(sql))
            self.send_json(200, {"rows": rows, "count": len(rows), "columns": cols})
        except Exception as e:
            self.send_json(400, {"error": str(e)})


if __name__ == "__main__":
    print(f"query_server  port={PORT}  data={DATA_PATH}", flush=True)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
