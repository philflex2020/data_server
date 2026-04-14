#!/usr/bin/env python3
"""
demo_server.py — HTTP control server for Telegraf demo stack
Port: 8788

Handles start/stop/status of the evelyn-compare stack on:
  phil-dev  (local)            scripts/start_46_influx.sh
  gx10      (192.168.86.48)    scripts/evelyn-compare-native-start.sh
  fractal   (192.168.86.51)    scripts/start_46_influx.sh

Usage:
  python source/demo_server/demo_server.py
"""

import json
import os
import subprocess
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import paramiko

PORT = 8788

REPO_LOCAL = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

HOSTS = {
    'phil-dev': {
        'local':      True,
        'ip':         '127.0.0.1',
        'user':       None,
        'repo':       REPO_LOCAL,
        'start_cmd':  'bash {repo}/scripts/start_46_influx.sh {opts}',
        'stop_cmd':   'bash {repo}/scripts/stop_46_influx.sh',
        'base_port':  8096,
        'filt_port':  8097,
        'status_pat': ['telegraf-46', 'influx-46', 'ems_site_simulator'],
    },
    'gx10': {
        'local':      False,
        'ip':         '192.168.86.48',
        'user':       'phil',
        'repo':       '/home/phil/work/gen-ai/data_server',
        'start_cmd':  'bash {repo}/scripts/evelyn-compare-native-start.sh {opts}',
        'stop_cmd':   'bash {repo}/scripts/evelyn-compare-native-stop.sh',
        'base_port':  8096,
        'filt_port':  8097,
        'status_pat': ['telegraf-native', 'influx-base', 'ems_site_simulator'],
    },
    'fractal': {
        'local':      False,
        'ip':         '192.168.86.51',
        'user':       'phil',
        'repo':       '/home/phil/data_server',
        'start_cmd':  'bash {repo}/scripts/start_46_influx.sh {opts}',
        'stop_cmd':   'bash {repo}/scripts/stop_46_influx.sh',
        'base_port':  8096,
        'filt_port':  8097,
        'status_pat': ['telegraf-46', 'influx-46', 'ems_site_simulator'],
    },
}

# ── command execution ─────────────────────────────────────────────────────────

def run_local(cmd, timeout=60):
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode == 0, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, 'timeout'
    except Exception as e:
        return False, str(e)

def run_remote(host_cfg, cmd, timeout=60):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            host_cfg['ip'], username=host_cfg['user'],
            timeout=10, auth_timeout=10,
            look_for_keys=True, allow_agent=True,
        )
        _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode() + stderr.read().decode()
        rc  = stdout.channel.recv_exit_status()
        client.close()
        return rc == 0, out
    except Exception as e:
        return False, str(e)

def run_on(host_name, cmd, timeout=60):
    h = HOSTS[host_name]
    if h['local']:
        return run_local(cmd, timeout)
    return run_remote(h, cmd, timeout)

# ── status check ──────────────────────────────────────────────────────────────

def check_status(host_name):
    h = HOSTS[host_name]
    patterns = h['status_pat']
    ps_cmd = 'ps aux | grep -E "{}" | grep -v grep || true'.format('|'.join(patterns))
    ok, out = run_on(host_name, ps_cmd, timeout=10)
    procs = []
    for line in out.strip().splitlines():
        parts = line.split()
        if len(parts) > 10:
            procs.append({'pid': parts[1], 'cmd': ' '.join(parts[10:])[:80]})
    # Check InfluxDB ports
    port_cmd = 'ss -tlnp 2>/dev/null | grep -E ":{}|:{}" || true'.format(
        h['base_port'], h['filt_port'])
    _, ports_out = run_on(host_name, port_cmd, timeout=5)
    base_up = str(h['base_port']) in ports_out
    filt_up = str(h['filt_port']) in ports_out
    return {
        'host':     host_name,
        'procs':    procs,
        'base_up':  base_up,
        'filt_up':  filt_up,
        'base_port': h['base_port'],
        'filt_port': h['filt_port'],
        'ip':       h['ip'],
    }

# ── start/stop ────────────────────────────────────────────────────────────────

def build_opts(params):
    """Build option string from {filtered, units, rate, no_gen}."""
    opts = []
    if params.get('filtered'):  opts.append('--filtered')
    if params.get('no_gen'):    opts.append('--no-gen')
    if params.get('units'):     opts.append(f'--units {params["units"]}')
    if params.get('rate'):      opts.append(f'--rate {params["rate"]}')
    return ' '.join(opts)

def start_host(host_name, params):
    h = HOSTS[host_name]
    opts = build_opts(params)
    cmd = h['start_cmd'].format(repo=h['repo'], opts=opts)
    return run_on(host_name, cmd, timeout=90)

def stop_host(host_name):
    h = HOSTS[host_name]
    cmd = h['stop_cmd'].format(repo=h['repo'])
    return run_on(host_name, cmd, timeout=30)

# ── HTTP handler ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f'[demo_server] {self.address_string()} {fmt % args}')

    def send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        if self.path == '/status':
            results = {}
            for name in HOSTS:
                try:
                    results[name] = check_status(name)
                except Exception as e:
                    results[name] = {'host': name, 'error': str(e)}
            self.send_json(200, results)

        elif self.path == '/hosts':
            self.send_json(200, {
                name: {'ip': h['ip'], 'base_port': h['base_port'], 'filt_port': h['filt_port']}
                for name, h in HOSTS.items()
            })

        elif self.path == '/health':
            self.send_json(200, {'ok': True, 'port': PORT})

        else:
            self.send_json(404, {'error': 'not found'})

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body   = json.loads(self.rfile.read(length) or b'{}')
        host   = body.get('host', 'phil-dev')

        if host not in HOSTS:
            self.send_json(400, {'error': f'unknown host: {host}'})
            return

        if self.path == '/start':
            ok, out = start_host(host, body)
            self.send_json(200 if ok else 500, {'ok': ok, 'host': host, 'output': out})

        elif self.path == '/stop':
            ok, out = stop_host(host)
            self.send_json(200 if ok else 500, {'ok': ok, 'host': host, 'output': out})

        elif self.path == '/status':
            try:
                result = check_status(host)
                self.send_json(200, result)
            except Exception as e:
                self.send_json(500, {'error': str(e)})

        else:
            self.send_json(404, {'error': 'not found'})


if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f'demo_server listening on :{PORT}')
    print(f'  GET  /health  — liveness check')
    print(f'  GET  /hosts   — host config')
    print(f'  GET  /status  — all host process status')
    print(f'  POST /start   — {{"host":"phil-dev","filtered":true,"units":1,"rate":175}}')
    print(f'  POST /stop    — {{"host":"phil-dev"}}')
    server.serve_forever()
