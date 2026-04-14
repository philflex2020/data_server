#!/usr/bin/env python3
"""
Tiny InfluxDB2 proxy — lets browser pages query InfluxDB without CORS/encoding issues.
Listens on :8088, forwards /base/* to influxdb-baseline and /filt/* to influxdb-filtered.

Usage: python3 influx_proxy.py [--base host:port] [--filt host:port] [--listen port]
"""
import http.server, urllib.request, urllib.error, sys, argparse

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--base',   default='192.168.86.48:8086')
    p.add_argument('--filt',   default='192.168.86.48:8087')
    p.add_argument('--listen', default=8088, type=int)
    return p.parse_args()

ARGS = parse_args()

class Handler(http.server.BaseHTTPRequestHandler):
    def cors(self):
        self.send_header('Access-Control-Allow-Origin',  '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Authorization, Content-Type, Accept')

    def do_OPTIONS(self):
        self.send_response(204)
        self.cors()
        self.end_headers()

    def do_POST(self):
        # route: /base/...  →  influxdb-baseline
        #        /filt/...  →  influxdb-filtered
        if self.path.startswith('/filt'):
            target = f'http://{ARGS.filt}' + self.path[5:]
        elif self.path.startswith('/base'):
            target = f'http://{ARGS.base}' + self.path[5:]
        else:
            self.send_error(404, 'Use /base/... or /filt/...')
            return

        length = int(self.headers.get('Content-Length', 0))
        body   = self.rfile.read(length)

        req = urllib.request.Request(target, data=body, headers={
            'Authorization': self.headers.get('Authorization', ''),
            'Content-Type':  self.headers.get('Content-Type', 'application/vnd.flux'),
        })
        try:
            with urllib.request.urlopen(req) as r:
                data = r.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.cors()
            self.end_headers()
            self.wfile.write(data)
        except urllib.error.HTTPError as e:
            msg = e.read()
            self.send_response(e.code)
            self.send_header('Content-Type', 'text/plain')
            self.cors()
            self.end_headers()
            self.wfile.write(msg)

    def log_message(self, fmt, *args):
        pass  # silent

if __name__ == '__main__':
    addr = ('0.0.0.0', ARGS.listen)
    print(f'influx_proxy listening on :{ARGS.listen}  base={ARGS.base}  filt={ARGS.filt}')
    http.server.HTTPServer(addr, Handler).serve_forever()
