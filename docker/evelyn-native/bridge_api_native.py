#!/usr/bin/env python3
"""
bridge_api_native.py — stack management API for native (no-Docker) deployment.

Same HTTP API surface as bridge_api.py (Docker version).
Manages systemd services instead of Docker containers.

Endpoints
---------
GET  /status          service state + basic health summary
POST /start           systemctl start flashmq parquet-writer
POST /stop            systemctl stop flashmq parquet-writer (+ kill sim)
GET  /storage         current parquet output path (from writer config)
POST /storage         {"path":"..."} — update writer yaml, mkdir, restart writer
GET  /bridge          current FlashMQ bridge config
POST /bridge          {"address","port","topic"} — write conf, restart flashmq
POST /bridge/clear    remove bridge, restart flashmq

Configuration
-------------
Adjust the constants below to match the target installation paths.
"""
import asyncio, json, os, re, subprocess, sys

PORT            = 8772
FLASHMQ_CONF    = "/etc/evelyn/flashmq.conf"
WRITER_YAML     = "/etc/evelyn/writer.yaml"
SVC_BROKER      = "flashmq"
SVC_WRITER      = "parquet-writer"
SIM_PID         = "/tmp/evelyn34_sim.pid"
DEFAULT_PATH    = "/data/parquet-evelyn"

CORS = (
    "Access-Control-Allow-Origin: *\r\n"
    "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
    "Access-Control-Allow-Headers: Content-Type\r\n"
)

# ── flashmq.conf templates ────────────────────────────────────────────────────
BASE_CONF = """\
listen {
    port 1883
    protocol mqtt
    tcp_nodelay true
}

allow_anonymous true
log_level warning
storage_dir /var/lib/flashmq

client_initial_buffer_size 4194304
max_packet_size 1048576
max_qos_msg_pending_per_client 65535
thread_count 2
"""

BRIDGE_TEMPLATE = """\

bridge {{
    address {address}
    port {port}
    topic {topic}  in  0
    clientid evelyn-bridge
}}
"""

# ── helpers ───────────────────────────────────────────────────────────────────
def run(cmd, timeout=35):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return r.returncode == 0, (r.stdout + r.stderr).strip()

def respond(writer, status, body_dict):
    body = json.dumps(body_dict).encode()
    writer.write(
        f"HTTP/1.1 {status}\r\n"
        f"Content-Type: application/json\r\n"
        f"{CORS}"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n".encode()
        + body
    )

def parse_body(raw):
    try:
        body_start = raw.index("\r\n\r\n") + 4
        return json.loads(raw[body_start:])
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(str(e))

# ── systemd service management ────────────────────────────────────────────────
def service_state(name):
    """Returns (active-state, sub-state) e.g. ('active', 'running')."""
    r = subprocess.run(
        ["systemctl", "is-active", name],
        capture_output=True, text=True
    )
    state = r.stdout.strip()   # 'active', 'inactive', 'failed', 'activating'
    # get sub-state for richer info
    r2 = subprocess.run(
        ["systemctl", "show", name, "--property=SubState", "--value"],
        capture_output=True, text=True
    )
    sub = r2.stdout.strip()
    return state, sub

def service_start(*names):
    return run(["systemctl", "start"] + list(names))

def service_stop(*names):
    return run(["systemctl", "stop"] + list(names))

def service_restart(name):
    return run(["systemctl", "restart", name])

# ── sim process ───────────────────────────────────────────────────────────────
def sim_running():
    if not os.path.exists(SIM_PID):
        return False, None
    try:
        pid = int(open(SIM_PID).read().strip())
        os.kill(pid, 0)
        return True, pid
    except (ValueError, OSError):
        return False, None

def sim_kill():
    running, pid = sim_running()
    if running:
        try:
            os.kill(pid, 15)
        except OSError:
            pass
    try:
        os.remove(SIM_PID)
    except OSError:
        pass
    return running

# ── flashmq.conf ──────────────────────────────────────────────────────────────
def read_conf():
    try:
        return open(FLASHMQ_CONF).read()
    except FileNotFoundError:
        return BASE_CONF

def parse_bridge(conf):
    clean = '\n'.join(l for l in conf.splitlines() if not l.strip().startswith('#'))
    m = re.search(
        r'bridge\s*\{[^}]*address\s+(\S+)[^}]*port\s+(\d+)[^}]*topic\s+(\S+)',
        clean, re.DOTALL
    )
    if not m:
        return {"enabled": False, "address": "", "port": 1883, "topic": "ems/#"}
    return {"enabled": True, "address": m.group(1),
            "port": int(m.group(2)), "topic": m.group(3)}

def write_conf(bridge=None):
    conf = BASE_CONF + (BRIDGE_TEMPLATE.format(**bridge) if bridge else "")
    open(FLASHMQ_CONF, "w").write(conf)

# ── writer yaml ───────────────────────────────────────────────────────────────
def read_writer_yaml():
    try:
        return open(WRITER_YAML).read()
    except FileNotFoundError:
        return ""

def current_path():
    """Extract output.base_path from writer yaml."""
    for line in read_writer_yaml().splitlines():
        m = re.match(r'\s*base_path\s*:\s*(.+)', line)
        if m:
            return m.group(1).strip().strip('"\'')
    return DEFAULT_PATH

def set_writer_path(new_path):
    """Rewrite base_path line in writer yaml."""
    yaml = read_writer_yaml()
    updated = re.sub(
        r'(\s*base_path\s*:\s*).*',
        lambda m: m.group(1) + new_path,
        yaml
    )
    if updated == yaml and 'base_path' not in yaml:
        return False, "base_path key not found in writer yaml"
    open(WRITER_YAML, "w").write(updated)
    return True, ""

# ── request handlers ──────────────────────────────────────────────────────────
def handle_status():
    b_state, b_sub = service_state(SVC_BROKER)
    w_state, w_sub = service_state(SVC_WRITER)
    sim_up, sim_pid = sim_running()
    path = current_path()
    return {
        "broker":  {"state": b_state, "sub": b_sub},
        "writer":  {"state": w_state, "sub": w_sub},
        "sim":     {"running": sim_up, "pid": sim_pid},
        "storage": path,
        "bridge":  parse_bridge(read_conf()),
    }

def handle_start():
    ok, out = service_start(SVC_BROKER, SVC_WRITER)
    return {"ok": ok, "output": out[:500]}

def handle_stop():
    killed = sim_kill()
    ok, out = service_stop(SVC_BROKER, SVC_WRITER)
    return {"ok": ok, "output": out[:500], "sim_killed": killed}

def handle_get_storage():
    path = current_path()
    exists   = os.path.isdir(path)
    writable = os.access(path, os.W_OK) if exists else False
    return {"path": path, "exists": exists, "writable": writable}

def handle_set_storage(body):
    path = str(body.get("path", "")).strip()
    if not path:
        return None, "path required"

    if not os.path.isdir(path):
        r = subprocess.run(["mkdir", "-p", path], capture_output=True, text=True)
        if r.returncode != 0:
            return None, (f"mkdir failed: {r.stderr.strip()}  "
                          f"Run: sudo mkdir -p {path} && sudo chown $USER {path}")

    ok_yaml, err = set_writer_path(path)
    if not ok_yaml:
        return None, err

    ok, out = service_restart(SVC_WRITER)
    return {"ok": ok, "path": path, "output": out[:400]}, None

def handle_set_bridge(body):
    address = str(body.get("address", "")).strip()
    port    = int(body.get("port", 1883))
    topic   = str(body.get("topic", "ems/#")).strip()
    if not address:
        return None, "address required"
    write_conf({"address": address, "port": port, "topic": topic})
    ok, err = service_restart(SVC_BROKER)
    return {"ok": ok, "bridge": parse_bridge(read_conf()), "error": err}, None

def handle_clear_bridge():
    write_conf(bridge=None)
    ok, err = service_restart(SVC_BROKER)
    return {"ok": ok, "bridge": None, "error": err}

# ── main loop ─────────────────────────────────────────────────────────────────
async def handle(reader, writer):
    try:
        raw = await asyncio.wait_for(reader.read(8192), timeout=5)
    except asyncio.TimeoutError:
        writer.close(); return

    req = raw.decode(errors="replace")
    lines = req.split("\r\n")
    method, path = "", ""
    try:
        method, path, _ = lines[0].split(" ", 2)
    except ValueError:
        writer.close(); return

    if method == "OPTIONS":
        writer.write(
            f"HTTP/1.1 204 No Content\r\n{CORS}Content-Length: 0\r\nConnection: close\r\n\r\n"
            .encode()
        )
        await writer.drain(); writer.close(); return

    result = err = None
    try:
        if   method == "GET"  and path == "/status":
            result = handle_status()
        elif method == "POST" and path == "/start":
            result = handle_start()
        elif method == "POST" and path == "/stop":
            result = handle_stop()
        elif method == "GET"  and path == "/storage":
            result = handle_get_storage()
        elif method == "POST" and path == "/storage":
            result, err = handle_set_storage(parse_body(req))
        elif method == "GET"  and path == "/bridge":
            result = parse_bridge(read_conf())
        elif method == "POST" and path == "/bridge/clear":
            result = handle_clear_bridge()
        elif method == "POST" and path == "/bridge":
            result, err = handle_set_bridge(parse_body(req))
        else:
            respond(writer, "404 Not Found", {"error": "not found"})
            await writer.drain(); writer.close(); return
    except Exception as e:
        respond(writer, "500 Internal Server Error", {"error": str(e)})
        await writer.drain(); writer.close(); return

    if err:
        respond(writer, "400 Bad Request", {"error": err})
    else:
        respond(writer, "200 OK", result)

    await writer.drain()
    writer.close()

async def main():
    srv = await asyncio.start_server(handle, "0.0.0.0", PORT)
    print(f"bridge_api_native listening on :{PORT}  "
          f"(broker={SVC_BROKER}  writer={SVC_WRITER})", flush=True)
    async with srv:
        await srv.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
