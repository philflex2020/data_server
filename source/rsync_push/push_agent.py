"""
rsync_push_agent.py — push Parquet files from phil-dev to fractal-phil

Runs a periodic rsync loop and exposes a small HTTP control API so
parquet_monitor.html can start / stop / query it the same way it
controls the pull-rsync on the subscriber-api.

HTTP API (port 8770 by default):
  GET  /ping          → 204
  GET  /rsync/status  → JSON stats
  POST /rsync/start   → start push loop
  POST /rsync/stop    → stop push loop

Config (config.yaml):
  rsync:
    src:      /srv/data/parquet/
    dst:      phil@192.168.86.51:/srv/data/parquet-aws-sim/
    interval: 5          # seconds between pushes
  http:
    port: 8770
"""

import argparse
import asyncio
import json
import logging
import subprocess
import time
from pathlib import Path

import yaml

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── State ────────────────────────────────────────────────────────────────────
_state = {
    "running":    False,
    "start_time": None,
    "run_count":  0,
    "last_ms":    0.0,
    "file_count": 0,
    "total_mb":   0.0,
}
_loop_task: asyncio.Task | None = None


# ── Rsync loop ───────────────────────────────────────────────────────────────
async def _rsync_loop(src: str, dst: str, interval: int) -> None:
    log.info("rsync push loop started: %s → %s every %ss", src, dst, interval)
    while _state["running"]:
        t0 = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                "rsync", "-a", "--stats", src, dst,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            elapsed_ms = (time.monotonic() - t0) * 1000
            _state["run_count"] += 1
            _state["last_ms"] = round(elapsed_ms, 1)

            if proc.returncode == 0:
                # Parse --stats output for file count and transfer size
                for line in stdout.decode(errors="replace").splitlines():
                    if "Number of regular files transferred" in line:
                        try:
                            _state["file_count"] = int(line.split(":")[-1].strip().replace(",", ""))
                        except ValueError:
                            pass
                    if "Total transferred file size" in line:
                        try:
                            val = line.split(":")[-1].strip().replace(",", "").split()[0]
                            _state["total_mb"] = round(int(val) / 1_048_576, 2)
                        except (ValueError, IndexError):
                            pass
                log.info("rsync #%d done in %.0f ms — %d files, %.2f MB",
                         _state["run_count"], elapsed_ms,
                         _state["file_count"], _state["total_mb"])
            else:
                log.warning("rsync exit %d: %s", proc.returncode,
                            stderr.decode(errors="replace").strip()[:200])
        except Exception as exc:
            log.error("rsync error: %s", exc)

        await asyncio.sleep(interval)

    log.info("rsync push loop stopped")


# ── Local retention cleanup ───────────────────────────────────────────────────
async def _cleanup_loop(src: str, retain_hours: float, interval: int) -> None:
    log.info("retention cleanup: keeping last %.1fh in %s, checking every %ds",
             retain_hours, src, interval)
    while _state["running"]:
        await asyncio.sleep(interval)
        cutoff = time.time() - retain_hours * 3600
        removed = 0
        for f in Path(src).rglob("*.parquet"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except Exception:
                pass
        if removed:
            log.info("retention: removed %d file(s) older than %.1fh", removed, retain_hours)


# ── HTTP server ──────────────────────────────────────────────────────────────
CORS = [
    ("Access-Control-Allow-Origin",  "*"),
    ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
    ("Access-Control-Allow-Headers", "Content-Type"),
]


def _response(writer, status: int, headers: list, body: bytes) -> None:
    reason = {200: "OK", 204: "No Content", 404: "Not Found"}.get(status, "OK")
    writer.write(f"HTTP/1.1 {status} {reason}\r\n".encode())
    for k, v in headers:
        writer.write(f"{k}: {v}\r\n".encode())
    writer.write(f"Content-Length: {len(body)}\r\n\r\n".encode())
    if body:
        writer.write(body)


async def _handle(reader, writer, cfg: dict) -> None:
    try:
        raw = await asyncio.wait_for(reader.read(4096), timeout=5)
    except asyncio.TimeoutError:
        writer.close()
        return

    line = raw.split(b"\r\n")[0].decode(errors="replace")
    parts = line.split()
    if len(parts) < 2:
        writer.close()
        return
    method, path = parts[0], parts[1].split("?")[0]

    json_hdr = CORS + [("Content-Type", "application/json")]

    if method == "OPTIONS":
        _response(writer, 204, CORS, b"")

    elif path == "/ping" or path == "/health":
        _response(writer, 204, CORS, b"")

    elif path == "/rsync/status":
        body = json.dumps({**_state,
                           "elapsed_s": round(time.monotonic() - _state["start_time"], 1)
                           if _state["start_time"] else 0,
                           "interval_s": cfg["rsync"].get("interval", 5)}).encode()
        _response(writer, 200, json_hdr, body)

    elif path == "/rsync/start" and method == "POST":
        global _loop_task
        if not _state["running"]:
            _state["running"]    = True
            _state["start_time"] = time.monotonic()
            _state["run_count"]  = 0
            src          = cfg["rsync"]["src"]
            dst          = cfg["rsync"]["dst"]
            interval     = cfg["rsync"].get("interval", 5)
            retain_hours = cfg["rsync"].get("retain_hours", 0)
            loop = asyncio.get_event_loop()
            _loop_task = loop.create_task(_rsync_loop(src, dst, interval))
            if retain_hours > 0:
                loop.create_task(_cleanup_loop(src, retain_hours, max(interval * 6, 60)))
            log.info("rsync push started (retain_hours=%s)", retain_hours or "unlimited")
        _response(writer, 200, json_hdr, b'{"ok":true,"msg":"started"}')

    elif path == "/rsync/stop" and method == "POST":
        if _state["running"]:
            _state["running"] = False
            if _loop_task:
                _loop_task.cancel()
            log.info("rsync push stopped")
        _response(writer, 200, json_hdr, b'{"ok":true,"msg":"stopped"}')

    elif path == "/parquet_stats":
        src = cfg["rsync"]["src"].rstrip("/")
        try:
            files = list(Path(src).rglob("*.parquet"))
            file_count = len(files)
            total_mb   = round(sum(f.stat().st_size for f in files) / 1_048_576, 2)
            latest_age = None
            if files:
                latest_mtime = max(f.stat().st_mtime for f in files)
                latest_age   = round(time.time() - latest_mtime, 0)
            body = json.dumps({"file_count": file_count, "total_mb": total_mb,
                               "latest_age_s": latest_age}).encode()
        except Exception as exc:
            body = json.dumps({"error": str(exc)}).encode()
        _response(writer, 200, json_hdr, body)

    else:
        _response(writer, 404, CORS, b'{"error":"not found"}')

    await writer.drain()
    writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    port = cfg.get("http", {}).get("port", 8770)

    async def handler(r, w):
        await _handle(r, w, cfg)

    server = await asyncio.start_server(handler, "0.0.0.0", port)
    log.info("rsync push agent listening on :%d", port)
    log.info("src: %s  dst: %s  interval: %ss",
             cfg["rsync"]["src"], cfg["rsync"]["dst"],
             cfg["rsync"].get("interval", 5))

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
