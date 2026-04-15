#!/usr/bin/env python3
"""
run_query.py — CLI for the parquet-query pod (DuckDB HTTP server)

Usage:
  python3 run_query.py health
  python3 run_query.py sites
  python3 run_query.py latest [--site 0215D1D8] [--n 50]
  python3 run_query.py sql "SELECT site_id, point_name, val FROM data LIMIT 20"
  python3 run_query.py sql "SELECT * FROM data WHERE ts > now() - INTERVAL 1 HOUR"

Output options (any subcommand):
  --json        raw JSON
  --csv         CSV with header

Host / port:
  --host HOST   query server host  (default: $WRITER_HOST or current hostname)
  --port PORT   query server port  (default: $QUERY_PORT or 8772)

  export WRITER_HOST=10.0.1.42   # set once, all tools use it
"""

import argparse, json, os, socket, sys, csv, io, urllib.request, urllib.error

# --------------------------------------------------------------------------
# defaults
# --------------------------------------------------------------------------
_default_host = os.environ.get("WRITER_HOST", socket.gethostname())
_default_port = int(os.environ.get("QUERY_PORT", 8772))


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------
def _get(host, port, path):
    url = f"http://{host}:{port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.URLError as e:
        sys.exit(f"error: cannot reach {url} — {e}")


def _post(host, port, path, body):
    url  = f"http://{host}:{port}{path}"
    data = json.dumps(body).encode()
    req  = urllib.request.Request(url, data=data,
                                   headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        payload = json.loads(e.read())
        sys.exit(f"error: {payload.get('error', e)}")
    except urllib.error.URLError as e:
        sys.exit(f"error: cannot reach {url} — {e}")


# --------------------------------------------------------------------------
# output formatters
# --------------------------------------------------------------------------
def _print_json(data):
    print(json.dumps(data, indent=2, default=str))


def _print_csv(cols, rows):
    w = csv.DictWriter(sys.stdout, fieldnames=cols)
    w.writeheader()
    w.writerows(rows)


def _print_table(cols, rows):
    if not rows:
        print("(no rows)")
        return
    widths = [max(len(str(c)), max((len(str(r.get(c, ""))) for r in rows), default=0))
              for c in cols]
    sep  = "  ".join("-" * w for w in widths)
    head = "  ".join(str(c).ljust(w) for c, w in zip(cols, widths))
    print(head)
    print(sep)
    for row in rows:
        print("  ".join(str(row.get(c, "")).ljust(w) for c, w in zip(cols, widths)))
    print(f"\n{len(rows)} row(s)")


# --------------------------------------------------------------------------
# subcommands
# --------------------------------------------------------------------------
def cmd_health(args):
    data = _get(args.host, args.port, "/health")
    if args.json:
        _print_json(data)
        return
    print(f"\n  http://{args.host}:{args.port}/health")
    print(f"  status         {data.get('status','?')}")
    print(f"  data_path      {data.get('data_path','?')}")
    print(f"  parquet_files  {data.get('parquet_files','?')}")
    print(f"  disk_free_gb   {data.get('disk_free_gb','?')}")
    print()


def cmd_sites(args):
    data = _get(args.host, args.port, "/sites")
    if args.json:
        _print_json(data)
        return
    sites = data.get("sites", [])
    if not sites:
        print("(no sites found)")
        return
    for s in sites:
        print(s)


def cmd_latest(args):
    qs   = f"?n={args.n}" + (f"&site={args.site}" if args.site else "")
    data = _get(args.host, args.port, f"/latest{qs}")
    if "error" in data:
        sys.exit(f"error: {data['error']}")
    _output(args, data)


def cmd_sql(args):
    sql  = " ".join(args.sql)
    data = _post(args.host, args.port, "/query", {"sql": sql})
    if "error" in data:
        sys.exit(f"error: {data['error']}")
    _output(args, data)


def _output(args, data):
    cols = data.get("columns", [])
    rows = data.get("rows", [])
    if args.json:
        _print_json(data)
    elif args.csv:
        if not cols and rows:
            cols = list(rows[0].keys())
        _print_csv(cols, rows)
    else:
        if not cols and rows:
            cols = list(rows[0].keys())
        _print_table(cols, rows)


# --------------------------------------------------------------------------
# arg parsing
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Query the parquet-query pod DuckDB HTTP server.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--host", default=_default_host,
                    help=f"query server host (default: {_default_host})")
    ap.add_argument("--port", type=int, default=_default_port,
                    help=f"query server port (default: {_default_port})")
    ap.add_argument("--json", action="store_true", help="output raw JSON")
    ap.add_argument("--csv",  action="store_true", help="output CSV")

    sub = ap.add_subparsers(dest="cmd", metavar="COMMAND")
    sub.required = True

    sub.add_parser("health", help="pod status and file count")

    sub.add_parser("sites",  help="list distinct site_ids in the data store")

    p_lat = sub.add_parser("latest", help="last N rows, optional site filter")
    p_lat.add_argument("--site", default=None, help="filter to this site_id")
    p_lat.add_argument("--n",    type=int, default=100, help="row limit (default 100)")

    p_sql = sub.add_parser("sql", help="run arbitrary SQL (table 'data' = all parquet files)")
    p_sql.add_argument("sql", nargs="+", help="SQL statement (quote if it contains spaces)")

    args = ap.parse_args()

    {"health": cmd_health,
     "sites":  cmd_sites,
     "latest": cmd_latest,
     "sql":    cmd_sql}[args.cmd](args)


if __name__ == "__main__":
    main()
