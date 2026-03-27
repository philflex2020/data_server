#!/usr/bin/env python3
"""
bridge_api.py — stack management API for the evelyn34 demo stack.

Endpoints
---------
GET  /status          container state + basic health summary
POST /start           docker compose up -d  (+ optional sim start)
POST /stop            docker compose down   (+ kill sim)
GET  /storage         current PARQUET_PATH
POST /storage         {"path":"..."} — update .env, mkdir, recreate writer
GET  /bridge          current FlashMQ bridge config
POST /bridge          {"address","port","topic"} — write conf, restart broker
POST /bridge/clear    remove bridge, restart broker

Runs on port 8772 (native on .34 host).
"""
import asyncio, json, os, re, subprocess, sys, time, glob
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

# ── duckdb (optional) ─────────────────────────────────────────────────────────
try:
    import duckdb as _duckdb
    _DUCKDB_OK = True
except ImportError:
    _DUCKDB_OK = False

# ── file-list cache ───────────────────────────────────────────────────────────
_file_cache = {"files": [], "ts": 0.0}
_FILE_CACHE_TTL = 60  # seconds

def _parse_file_ts(filename):
    """Extract UTC datetime from filename like 20260327T195715Z.parquet"""
    m = re.search(r'(\d{8}T\d{6}Z)', filename)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
    except ValueError:
        return None

def _build_file_list(parquet_path):
    pattern = os.path.join(parquet_path, "**", "*.parquet")
    files = []
    for fpath in glob.glob(pattern, recursive=True):
        fname = os.path.basename(fpath)
        ts = _parse_file_ts(fname)
        if ts is None:
            continue
        try:
            size_kb = os.path.getsize(fpath) / 1024.0
        except OSError:
            size_kb = 0.0
        files.append({"name": fname, "path": fpath,
                      "ts_iso": ts.isoformat(), "ts": ts,
                      "size_kb": round(size_kb, 1)})
    files.sort(key=lambda x: x["ts"], reverse=True)
    return files

def _get_file_list(parquet_path):
    now = time.time()
    if now - _file_cache["ts"] > _FILE_CACHE_TTL:
        _file_cache["files"] = _build_file_list(parquet_path)
        _file_cache["ts"] = now
    return _file_cache["files"]

# ── query handlers ─────────────────────────────────────────────────────────────
def handle_query_files():
    if not _DUCKDB_OK:
        return None, "duckdb not available", 503
    path = current_path()
    files = _get_file_list(path)
    return {
        "files": [{"name": f["name"], "ts_iso": f["ts_iso"], "size_kb": f["size_kb"]}
                  for f in files],
        "total": len(files),
        "path": path,
    }, None, 200

def handle_query_signals():
    if not _DUCKDB_OK:
        return None, "duckdb not available", 503
    path = current_path()
    files = _get_file_list(path)
    recent = files[:20]
    SKIP = {"project_id", "project", "site"}
    signals = set()
    con = _duckdb.connect()
    for f in recent:
        try:
            rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f['path']}') LIMIT 0").fetchall()
            for row in rows:
                col = row[0]
                if col not in SKIP:
                    signals.add(col)
        except Exception:
            pass
    con.close()
    return {"signals": sorted(signals), "from_files": len(recent)}, None, 200

def handle_query_data(params):
    if not _DUCKDB_OK:
        return None, "duckdb not available", 503
    signal = params.get("signal", [""])[0].strip()
    from_s = params.get("from", [""])[0].strip()
    to_s   = params.get("to",   [""])[0].strip()
    limit  = int(params.get("limit", ["500"])[0])
    if not signal:
        return None, "signal parameter required", 400
    if not from_s or not to_s:
        return None, "from and to parameters required", 400
    try:
        from_dt = datetime.fromisoformat(from_s.replace("Z", "+00:00"))
        to_dt   = datetime.fromisoformat(to_s.replace("Z", "+00:00"))
        if from_dt.tzinfo is None:
            from_dt = from_dt.replace(tzinfo=timezone.utc)
        if to_dt.tzinfo is None:
            to_dt = to_dt.replace(tzinfo=timezone.utc)
    except ValueError as e:
        return None, f"invalid from/to: {e}", 400

    path = current_path()
    files = _get_file_list(path)
    in_range = [f for f in files if from_dt <= f["ts"] <= to_dt]
    in_range.sort(key=lambda x: x["ts"])  # ascending for chart

    con = _duckdb.connect()
    points = []
    for f in in_range:
        if len(points) >= limit:
            break
        try:
            rows = con.execute(
                f'SELECT "{signal}" FROM read_parquet(\'{f["path"]}\') '
                f'WHERE "{signal}" IS NOT NULL LIMIT 1'
            ).fetchall()
            if rows:
                points.append({"ts": f["ts_iso"], "value": rows[0][0]})
        except Exception:
            pass  # column not found or other error — skip file
    con.close()
    return {
        "signal": signal,
        "from": from_s,
        "to": to_s,
        "points": points,
        "count": len(points),
    }, None, 200

def handle_query_snapshot(params):
    if not _DUCKDB_OK:
        return None, "duckdb not available", 503
    ts_s = params.get("ts", [""])[0].strip()
    if not ts_s:
        return None, "ts parameter required", 400
    try:
        ts_dt = datetime.fromisoformat(ts_s.replace("Z", "+00:00"))
        if ts_dt.tzinfo is None:
            ts_dt = ts_dt.replace(tzinfo=timezone.utc)
    except ValueError as e:
        return None, f"invalid ts: {e}", 400

    path = current_path()
    files = _get_file_list(path)
    if not files:
        return None, "no parquet files found", 404

    # find nearest file by ts
    nearest = min(files, key=lambda f: abs((f["ts"] - ts_dt).total_seconds()))

    SKIP = {"project_id", "project", "site"}
    con = _duckdb.connect()
    signals = []
    try:
        rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{nearest['path']}') LIMIT 0"
        ).fetchall()
        col_names = [r[0] for r in rows if r[0] not in SKIP]
        for col in col_names:
            try:
                vrows = con.execute(
                    f'SELECT "{col}" FROM read_parquet(\'{nearest["path"]}\') '
                    f'WHERE "{col}" IS NOT NULL LIMIT 1'
                ).fetchall()
                if vrows:
                    signals.append({"name": col, "value": vrows[0][0]})
            except Exception:
                pass
    except Exception as e:
        con.close()
        return None, f"error reading file: {e}", 500
    con.close()
    signals.sort(key=lambda x: x["name"])
    return {
        "file_ts": nearest["ts_iso"],
        "file": nearest["name"],
        "signals": signals,
    }, None, 200

PORT       = 8772
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONF_PATH  = os.path.join(SCRIPT_DIR, "configs", "flashmq.conf")
ENV_PATH   = os.path.join(SCRIPT_DIR, ".env")
SIM_PID    = "/tmp/evelyn34_sim.pid"
BROKER     = "evelyn34_broker"
WRITER     = "evelyn34_writer"
DEFAULT_PATH = "/mnt/tort-sdf/evelyn"

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
    clientid evelyn34-bridge
}}
"""

# ── helpers ───────────────────────────────────────────────────────────────────
def run(cmd, timeout=35):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=SCRIPT_DIR)
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

# ── bridge ────────────────────────────────────────────────────────────────────
def read_conf():
    try:
        return open(CONF_PATH).read()
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
    open(CONF_PATH, "w").write(conf)

# ── storage / .env ────────────────────────────────────────────────────────────
def read_env():
    env = {}
    try:
        for line in open(ENV_PATH):
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

def write_env(env):
    lines = [f"{k}={v}" for k, v in env.items()]
    open(ENV_PATH, "w").write('\n'.join(lines) + '\n')

def current_path():
    return read_env().get("PARQUET_PATH", DEFAULT_PATH)

# ── container status ──────────────────────────────────────────────────────────
def container_state(name):
    # Try with health status first; fall back if no healthcheck configured
    r = subprocess.run(
        ["docker", "inspect", name, "--format", "{{.State.Status}}"],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        return "absent", ""
    state = r.stdout.strip() or "unknown"
    # Try to get health separately (containers without HEALTHCHECK won't have it)
    rh = subprocess.run(
        ["docker", "inspect", name, "--format", "{{.State.Health.Status}}"],
        capture_output=True, text=True
    )
    health = rh.stdout.strip() if rh.returncode == 0 else ""
    return state, health

def sim_running():
    if not os.path.exists(SIM_PID):
        return False, None
    try:
        pid = int(open(SIM_PID).read().strip())
        os.kill(pid, 0)
        return True, pid
    except (ValueError, OSError):
        return False, None

# ── request handlers ──────────────────────────────────────────────────────────
def handle_status():
    b_state, b_health = container_state(BROKER)
    w_state, _        = container_state(WRITER)
    sim_up, sim_pid   = sim_running()
    return {
        "broker":  {"state": b_state, "health": b_health},
        "writer":  {"state": w_state},
        "sim":     {"running": sim_up, "pid": sim_pid},
        "storage": current_path(),
        "bridge":  parse_bridge(read_conf()),
    }

def handle_start():
    ok, out = run(["docker", "compose", "up", "-d"])
    return {"ok": ok, "output": out[:500]}

def handle_stop():
    # kill sim if running
    sim_up, pid = sim_running()
    if sim_up:
        try:
            os.kill(pid, 15)
        except OSError:
            pass
        try:
            os.remove(SIM_PID)
        except OSError:
            pass
    ok, out = run(["docker", "compose", "down"])
    return {"ok": ok, "output": out[:500], "sim_killed": sim_up}

def handle_get_storage():
    path = current_path()
    exists = os.path.isdir(path)
    writable = os.access(path, os.W_OK) if exists else False
    return {"path": path, "exists": exists, "writable": writable}

def handle_set_storage(body):
    path = str(body.get("path", "")).strip()
    if not path:
        return None, "path required"

    # try mkdir
    mkdir_err = ""
    if not os.path.isdir(path):
        r = subprocess.run(["mkdir", "-p", path], capture_output=True, text=True)
        if r.returncode != 0:
            mkdir_err = r.stderr.strip()
            # return helpful hint — don't block, user can pre-create
            return None, (f"mkdir failed: {mkdir_err}  "
                          f"Run:  sudo mkdir -p {path} && sudo chown phil:phil {path}")

    # update .env
    env = read_env()
    env["PARQUET_PATH"] = path
    write_env(env)

    # recreate writer with new volume
    ok, out = run(["docker", "compose", "up", "-d", "--force-recreate", "writer"])
    return {"ok": ok, "path": path, "output": out[:400]}, None

def handle_set_bridge(body):
    address = str(body.get("address", "")).strip()
    port    = int(body.get("port", 1883))
    topic   = str(body.get("topic", "ems/#")).strip()
    if not address:
        return None, "address required"
    bridge = {"address": address, "port": port, "topic": topic}
    write_conf(bridge)
    ok, err = run(["docker", "restart", BROKER])
    return {"ok": ok, "bridge": parse_bridge(read_conf()), "error": err}, None

def handle_clear_bridge():
    write_conf(bridge=None)
    ok, err = run(["docker", "restart", BROKER])
    return {"ok": ok, "bridge": None, "error": err}

# ── main loop ─────────────────────────────────────────────────────────────────
async def handle(reader, writer):
    try:
        raw = await asyncio.wait_for(reader.read(8192), timeout=5)
    except asyncio.TimeoutError:
        writer.close(); return

    req = raw.decode(errors="replace")
    lines = req.split("\r\n")
    method, raw_path = "", ""
    try:
        method, raw_path, _ = lines[0].split(" ", 2)
    except ValueError:
        writer.close(); return

    # parse query string
    if "?" in raw_path:
        path, qs = raw_path.split("?", 1)
        params = parse_qs(qs, keep_blank_values=False)
    else:
        path, params = raw_path, {}

    if method == "OPTIONS":
        writer.write(
            f"HTTP/1.1 204 No Content\r\n{CORS}Content-Length: 0\r\nConnection: close\r\n\r\n"
            .encode()
        )
        await writer.drain(); writer.close(); return

    result = err = None
    status_code = 200

    try:
        if method == "GET" and path == "/status":
            result = handle_status()

        elif method == "POST" and path == "/start":
            result = handle_start()

        elif method == "POST" and path == "/stop":
            result = handle_stop()

        elif method == "GET" and path == "/storage":
            result = handle_get_storage()

        elif method == "POST" and path == "/storage":
            body = parse_body(req)
            result, err = handle_set_storage(body)

        elif method == "GET" and path == "/bridge":
            result = parse_bridge(read_conf())

        elif method == "POST" and path == "/bridge/clear":
            result = handle_clear_bridge()

        elif method == "POST" and path == "/bridge":
            body = parse_body(req)
            result, err = handle_set_bridge(body)

        elif method == "GET" and path == "/query/files":
            result, err, status_code = handle_query_files()

        elif method == "GET" and path == "/query/signals":
            result, err, status_code = handle_query_signals()

        elif method == "GET" and path == "/query/data":
            result, err, status_code = handle_query_data(params)

        elif method == "GET" and path == "/query/snapshot":
            result, err, status_code = handle_query_snapshot(params)

        else:
            respond(writer, "404 Not Found", {"error": "not found"})
            await writer.drain(); writer.close(); return

    except Exception as e:
        respond(writer, "500 Internal Server Error", {"error": str(e)})
        await writer.drain(); writer.close(); return

    if err:
        status_str = {400: "400 Bad Request", 404: "404 Not Found",
                      503: "503 Service Unavailable"}.get(status_code, "400 Bad Request")
        respond(writer, status_str, {"error": err})
    else:
        respond(writer, "200 OK", result)

    await writer.drain()
    writer.close()

async def main():
    srv = await asyncio.start_server(handle, "0.0.0.0", PORT)
    print(f"bridge_api listening on :{PORT}", flush=True)
    async with srv:
        await srv.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
