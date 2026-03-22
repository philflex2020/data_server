#!/usr/bin/env python3
"""
spark_api.py — HTTP monitor/control API for the spark1 native torture stack.

Run on spark1 (192.168.86.48):
    python3 docker/torture/spark_api.py

Endpoints (API-compatible with torture_api.py where possible):
    GET  /writers          — 4 writer health endpoints aggregated
    GET  /stress           — per-stress publish rates parsed from log files
    GET  /docker/ps        — native process status (same JSON shape as docker compose ps)
    GET  /logs/{service}   — last 80 lines of process log file
    GET  /system           — machine description (cpu, ram, type, stress_max_mps)
    GET  /health           — liveness check
    POST /stack/up         — start full stack via spark_start.sh
    POST /stack/down       — stop all stack processes
    POST /stack/restart    — restart writers only
"""
import json, os, re, subprocess, sys, time, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

PORT         = int(os.environ.get("SPARK_API_PORT", 8780))
WRITER_PORTS = {"a": 8771, "b": 8772, "c": 8773, "d": 8774}
REPO         = os.path.expanduser("~/work/gen-ai/data_server")
LOG_DIR      = "/tmp"

LOG_MAP = {
    "broker":   "/tmp/flashmq.log",
    "writer-a": "/tmp/writer-spark-a.log",
    "writer-b": "/tmp/writer-spark-b.log",
    "writer-c": "/tmp/writer-spark-c.log",
    "writer-d": "/tmp/writer-spark-d.log",
    "stress-a": "/tmp/stress-spark-a.log",
    "stress-b": "/tmp/stress-spark-b.log",
    "stress-c": "/tmp/stress-spark-c.log",
    "stress-d": "/tmp/stress-spark-d.log",
}

PROCESS_MAP = [
    # (display-name, pgrep-pattern)
    ("broker",   "flashmq"),
    ("writer-a", "writer-spark-a"),
    ("writer-b", "writer-spark-b"),
    ("writer-c", "writer-spark-c"),
    ("writer-d", "writer-spark-d"),
    ("stress-a", "stress-a"),
    ("stress-b", "stress-b"),
    ("stress-c", "stress-c"),
    ("stress-d", "stress-d"),
]

START_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark_start.sh")

CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

SYSTEM_INFO = {
    "host":           "spark1",
    "ip":             "192.168.86.48",
    "cpu":            "ARM Cortex-X925 × 10 (3.9 GHz) + A725 × 10 (2.8 GHz) — 20 cores",
    "ram_gb":         120,
    "storage":        "931 GB NVMe PCIe 4.0 (3.3 GB/s) + 1.8 TB USB NVMe (1.6 GB/s)",
    "gpu":            "NVIDIA GB10 Blackwell (1 PFLOP FP8)",
    "broker":         "FlashMQ 20 threads (native ARM build)",
    "type":           "native",
    "stress_max_mps": 1000000,
    "description":    "ASUS NUC GX10 — ARM big.LITTLE, 120 GB unified, PCIe 4.0 NVMe",
}


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
        p    = urlparse(self.path)
        path = p.path.rstrip("/")

        if path == "/writers":
            data = {f"writer-{w}": writer_health(w, port)
                    for w, port in WRITER_PORTS.items()}
            self.respond(data)

        elif path == "/stress":
            result = {}
            for s in ["a", "b", "c", "d"]:
                key      = f"stress-{s}"
                log_file = LOG_MAP[key]
                try:
                    with open(log_file) as f:
                        lines = f.readlines()[-120:]
                    line = next(
                        (l for l in reversed(lines)
                         if "[stress_pub]" in l and "msg/s" in l), None
                    )
                    if line:
                        m   = re.search(r'\[\s*stress_pub\s*\]\s+(\d+)\s+msg/s', line)
                        mps = int(m.group(1)) if m else 0
                        result[key] = {"msgs_per_sec": mps, "ok": True,
                                       "last_sync": line.strip()}
                    else:
                        result[key] = {"msgs_per_sec": 0, "ok": False,
                                       "error": "no rate line found"}
                except Exception as exc:
                    result[key] = {"msgs_per_sec": 0, "ok": False, "error": str(exc)}
            self.respond(result)

        elif path == "/docker/ps":
            # Same JSON shape as docker compose ps so monitor works unchanged
            procs = []
            for name, pattern in PROCESS_MAP:
                r       = subprocess.run(["pgrep", "-f", pattern],
                                         capture_output=True, text=True)
                running = r.returncode == 0
                pids    = r.stdout.strip().replace("\n", ",")
                procs.append({
                    "Name":   name,
                    "Image":  "native-arm64",
                    "State":  "running" if running else "exited",
                    "Status": f"Up (pid {pids})" if running else "Exited",
                })
            self.respond(procs)

        elif path.startswith("/logs/"):
            service  = path.split("/logs/", 1)[1]
            log_file = LOG_MAP.get(service)
            if not log_file:
                self.respond({"service": service,
                               "lines": [f"unknown service: {service}"]})
                return
            try:
                with open(log_file) as f:
                    lines = f.readlines()[-80:]
                self.respond({"service": service,
                               "lines": [l.rstrip() for l in lines]})
            except Exception as e:
                self.respond({"service": service, "lines": [str(e)]})

        elif path == "/system":
            self.respond(SYSTEM_INFO)

        elif path == "/health":
            self.respond({"status": "ok", "api": "spark_api"})

        else:
            self.respond({"error": f"unknown path: {path}"}, 404)

    def do_POST(self):
        length  = int(self.headers.get("Content-Length", 0))
        body    = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(body) if body else {}
        except Exception:
            payload = {}

        p    = urlparse(self.path)
        path = p.path.rstrip("/")

        if path == "/stack/up":
            r = subprocess.run(["bash", START_SCRIPT, "--no-api"],
                                capture_output=True, text=True, timeout=60)
            self.respond({"ok": r.returncode == 0,
                           "output": r.stdout.strip(),
                           "stderr": r.stderr.strip() or None})

        elif path == "/stack/down":
            subprocess.run(["pkill", "-f", "flashmq"],      capture_output=True)
            subprocess.run(["pkill", "-f", "parquet_writer"], capture_output=True)
            subprocess.run(["pkill", "-f", "stress_pub"],   capture_output=True)
            self.respond({"ok": True})

        elif path == "/stack/restart":
            services    = payload.get("services",
                                      ["writer-a", "writer-b", "writer-c", "writer-d"])
            writer_bin  = os.path.join(REPO, "source/parquet_writer_cpp/parquet_writer")
            subprocess.run(["pkill", "-f", "parquet_writer"], capture_output=True)
            time.sleep(1)
            for svc in services:
                letter = svc[-1]
                cfg    = f"/tmp/writer-spark-{letter}.yaml"
                log    = f"/tmp/writer-spark-{letter}.log"
                if os.path.exists(cfg) and os.path.exists(writer_bin):
                    with open(log, "a") as lf:
                        subprocess.Popen([writer_bin, "--config", cfg],
                                         stdout=lf, stderr=lf)
            self.respond({"ok": True})

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
        pass  # suppress per-request noise


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"spark_api listening on 0.0.0.0:{PORT}  (repo: {REPO})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nspark_api stopped")
