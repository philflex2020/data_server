#!/usr/bin/env python3
# get_health.py — query the writer /health endpoint and pretty-print the response
# Usage: python3 get_health.py [host] [port]
import sys, json, urllib.request, urllib.error

host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
port = int(sys.argv[2]) if len(sys.argv) > 2 else 8771
url  = f'http://{host}:{port}/health'

try:
    with urllib.request.urlopen(url, timeout=5) as r:
        data = json.loads(r.read())
except urllib.error.URLError as e:
    print(f'ERROR: could not reach {url} — {e.reason}')
    sys.exit(1)

# Top-level stats
print(f'status       : {data.get("status")}')
print(f'site_id      : {data.get("site_id")}')
print(f'msgs_received: {data.get("msgs_received")}')
print(f'buffer_rows  : {data.get("buffer_rows")}')
print(f'flush_count  : {data.get("flush_count")}')
print(f'total_written: {data.get("total_rows_written")}')
print(f'last_flush_ms: {data.get("last_flush_ms")}')
print(f'disk_free_gb : {data.get("disk_free_gb")}')
drops = data.get("overflow_drops",0) + data.get("ring_drops",0) + data.get("sync_drops",0)
print(f'drops        : {drops}  (overflow={data.get("overflow_drops")} ring={data.get("ring_drops")} sync={data.get("sync_drops")})')

# Seen topics
topics = data.get('seen_topics', [])
print(f'\nseen_topics  : {len(topics)}')
for t in topics:
    print(f'  {t}')
