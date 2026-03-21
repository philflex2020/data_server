#!/usr/bin/env python3
"""
torture_api.py — HTTP control/monitor API for the Docker torture test stack.

Run on phil-256g:
    python3 docker/torture/torture_api.py

Endpoints:
    GET  /writers          — all 4 writer health endpoints aggregated
    GET  /stress           — per-stress-runner publish rates parsed from logs
    GET  /docker/ps        — docker compose ps as JSON
    GET  /logs/{service}   — last 80 lines of container logs
    POST /fault/run        — start a fault scenario (body: JSON {scenario, target, cycle})
    POST /fault/stop       — stop any running fault container
    POST /stack/up         — docker compose up -d
    POST /stack/down       — docker compose down
    POST /stack/restart    — docker compose restart (writers only)
"""
import json, os, re, subprocess, sys, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

PORT        = int(os.environ.get("TORTURE_API_PORT", 8780))
COMPOSE_DIR = os.path.join(os.path.dirname(__file__))
WRITER_PORTS = {"a": 8771, "b": 8772, "c": 8773, "d": 8774}
STRESS_CONTAINERS = ["torture_stress_a", "torture_stress_b",
                     "torture_stress_c", "torture_stress_d"]
SYNC_INTERVAL_S = 10.0   # matches stress_runner default sync_interval_seconds
CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def compose(*args, **kw):
    return subprocess.run(
        ["docker", "compose"] + list(args),
        capture_output=True, text=True, cwd=COMPOSE_DIR, **kw
    )


def writer_health(writer_id, port):
    try:
        r = urllib.request.urlopen(f"http://localhost:{port}/health", timeout=2)
        data = json.loads(r.read())
        data["_ok"] = True
    except Exception as e:
        data = {"_ok": False, "_error": str(e)}
    data["_writer"] = f"writer-{writer_id}"
    data["_port"]   = port
    return data


class Handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(204)
        for k, v in CORS.items():
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/")

        if path == "/writers":
            data = {f"writer-{w}": writer_health(w, port)
                    for w, port in WRITER_PORTS.items()}
            self.respond(data)

        elif path == "/stress":
            result = {}
            for cname in STRESS_CONTAINERS:
                key = cname.replace("torture_stress_", "stress-")
                try:
                    r = subprocess.run(
                        ["docker", "logs", "--tail", "120", cname],
                        capture_output=True, text=True, timeout=5
                    )
                    lines = (r.stdout + r.stderr).splitlines()
                    sync_line = next(
                        (l for l in reversed(lines) if "Sync #" in l), None
                    )
                    if sync_line:
                        m = re.search(r'interval=(\d+)', sync_line)
                        msgs_per_sec = round(int(m.group(1)) / SYNC_INTERVAL_S) if m else 0
                        result[key] = {"msgs_per_sec": msgs_per_sec, "ok": True,
                                       "last_sync": sync_line.strip()}
                    else:
                        result[key] = {"msgs_per_sec": 0, "ok": False,
                                       "error": "no sync line"}
                except Exception as exc:
                    result[key] = {"msgs_per_sec": 0, "ok": False, "error": str(exc)}
            self.respond(result)

        elif path == "/docker/ps":
            r = compose("ps", "--format", "json")
            containers = []
            for line in r.stdout.strip().splitlines():
                line = line.strip()
                if line:
                    try:
                        containers.append(json.loads(line))
                    except Exception:
                        pass
            self.respond(containers)

        elif path.startswith("/logs/"):
            service = path.split("/logs/", 1)[1]
            r = compose("logs", "--no-log-prefix", "--tail", "80", service)
            self.respond({"service": service,
                          "lines": (r.stdout + r.stderr).splitlines()})

        elif path == "/health":
            self.respond({"status": "ok", "api": "torture_api"})

        else:
            self.respond({"error": f"unknown path: {path}"}, 404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(body) if body else {}
        except Exception:
            payload = {}

        p    = urlparse(self.path)
        path = p.path.rstrip("/")

        if path == "/fault/run":
            scenario = payload.get("scenario", "all")
            target   = payload.get("target",   "torture_writer_d")
            cycle    = str(payload.get("cycle", 60))
            env_extra = {
                "SCENARIO":     scenario,
                "TARGET":       target,
                "CYCLE_SECONDS": cycle,
            }
            r = compose(
                "--profile", "fault", "run", "-d", "--rm", "fault",
                env={**os.environ, **env_extra}
            )
            self.respond({
                "started":  r.returncode == 0,
                "scenario": scenario,
                "target":   target,
                "cycle":    cycle,
                "stderr":   r.stderr.strip() or None,
            })

        elif path == "/fault/stop":
            # docker compose run creates containers named torture-fault-run-XXXX
            # find all of them by name prefix and stop
            ls = subprocess.run(
                ["docker", "ps", "-q", "--filter", "name=torture.fault"],
                capture_output=True, text=True
            )
            ids = ls.stdout.split()
            if ids:
                r = subprocess.run(["docker", "stop"] + ids, capture_output=True, text=True)
                self.respond({"stopped": r.returncode == 0, "containers": len(ids)})
            else:
                self.respond({"stopped": False, "containers": 0})

        elif path == "/stack/up":
            r = compose("up", "-d")
            self.respond({"ok": r.returncode == 0, "output": r.stdout.strip()})

        elif path == "/stack/down":
            r = compose("down")
            self.respond({"ok": r.returncode == 0, "output": r.stdout.strip()})

        elif path == "/stack/restart":
            services = payload.get("services",
                                   ["writer-a", "writer-b", "writer-c", "writer-d"])
            r = compose("restart", *services)
            self.respond({"ok": r.returncode == 0})

        else:
            self.respond({"error": f"unknown path: {path}"}, 404)

    def respond(self, data, code=200):
        body = json.dumps(data, indent=2).encode()
        self.send_response(code)
        for k, v in CORS.items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress per-request noise; uncomment for debugging
        # print(f"[torture_api] {self.address_string()} {fmt % args}", file=sys.stderr)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"torture_api listening on 0.0.0.0:{PORT}  (compose dir: {COMPOSE_DIR})",
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\ntorture_api stopped")
