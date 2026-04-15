#!/usr/bin/env python3
"""
show_health.py — display parquet_writer /health endpoint

Usage:
  python3 show_health.py                   # default host (current machine)
  python3 show_health.py 192.168.86.51
  python3 show_health.py 192.168.86.51 8771
"""

import sys, json, urllib.request, urllib.error, socket

host = sys.argv[1] if len(sys.argv) > 1 else socket.gethostname()
port = int(sys.argv[2]) if len(sys.argv) > 2 else 8771
url  = f'http://{host}:{port}/health'

try:
    with urllib.request.urlopen(url, timeout=5) as r:
        h = json.loads(r.read())
except urllib.error.URLError as e:
    sys.exit(f'error: cannot reach {url} — {e}')

status = h.get('status', '?')
ok     = status == 'ok'
flag   = '' if ok else '  *** NOT OK ***'

print(f'\n  {url}')
print(f'  {"─" * len(url)}')
print(f'  version      {h.get("version","?")}  ({h.get("git_hash","?")})')
print(f'  status       {status}{flag}')
print(f'  site_id      {h.get("site_id","") or "(empty)"}')
print()
print(f'  msgs_received      {h["msgs_received"]:>10,}')
print(f'  buffer_rows        {h["buffer_rows"]:>10,}')
print(f'  total_rows_written {h["total_rows_written"]:>10,}')
print(f'  flush_count        {h["flush_count"]:>10,}')
print(f'  last_flush_rows    {h["last_flush_rows"]:>10,}')
print(f'  last_flush_ms      {h["last_flush_ms"]:>10,}')
print(f'  flush_avg_ms       {h["flush_avg_ms"]:>10,}')
print(f'  flush_max_ms       {h["flush_max_ms"]:>10,}')
print()

drops = {k: v for k, v in h.items()
         if 'drop' in k or 'truncat' in k and v != 0}
if any(v != 0 for v in drops.values()):
    print('  *** DROPS / TRUNCATIONS ***')
    for k, v in drops.items():
        if v:
            print(f'  {k:<24} {v:>8,}')
    print()

print(f'  ring         {h["ring_used"]:,} / {h["ring_capacity"]:,} used')
print(f'  disk_free    {h["disk_free_gb"]:.1f} GB')
print()

topics = h.get('seen_topics', [])
print(f'  seen_topics  ({len(topics)}):')
for t in topics:
    print(f'    {t}')
print()
