"""
Data Server Process Manager — starts, stops, and monitors all pipeline
components, streaming their log output to a browser dashboard via WebSocket.

WebSocket API (ws://localhost:8760):

  Client → Server:
    {"type": "get_status"}
    {"type": "start",     "component": "generator"}
    {"type": "stop",      "component": "generator"}
    {"type": "start_all"}
    {"type": "stop_all"}

  Server → Client:
    {"type": "status",  "components": {"name": {"status": "running"|"stopped"|"error", "pid": int}}}
    {"type": "log",     "component": "name", "line": "...", "stream": "stdout"|"stderr"}

Usage:
  python manager.py
  python manager.py --config config.yaml
"""

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import sys
from collections import deque
from typing import Dict, Optional, Set

import websockets
import yaml

# PID file path (per manager port) — used to kill orphaned processes on restart
g_pid_file: str = ""


def _save_pids() -> None:
    """Persist {name: pid} for all running components to the PID file."""
    if not g_pid_file:
        return
    pids = {name: c.pid for name, c in g_components.items() if c.pid is not None}
    try:
        with open(g_pid_file, "w") as f:
            json.dump(pids, f)
    except OSError as e:
        log.warning("Could not write PID file %s: %s", g_pid_file, e)


def _cleanup_orphans() -> None:
    """On startup, kill any PIDs left over from a previous manager session."""
    if not g_pid_file or not os.path.exists(g_pid_file):
        return
    try:
        with open(g_pid_file) as f:
            pids: dict = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.warning("Could not read PID file %s: %s", g_pid_file, e)
        return

    for name, pid in pids.items():
        try:
            os.kill(pid, 0)          # check if process still exists
            log.info("Killing orphaned %s (pid %d)", name, pid)
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass                     # already gone
        except PermissionError:
            log.warning("No permission to kill orphaned %s (pid %d)", name, pid)

    os.remove(g_pid_file)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


async def _kill_port_occupants(port: int, label: str, broadcast_fn) -> bool:
    """Kill any process already listening on *port* before we try to start.

    Uses ``ss`` to find the PID(s), sends SIGTERM, waits 0.5 s, then
    SIGKILL if still alive.  Broadcasts a log line for each kill so the
    dashboard shows exactly what happened.  Returns True if anything was
    killed.
    """
    try:
        result = subprocess.run(
            ["ss", "-Htlnp", f"sport = :{port}"],
            capture_output=True, text=True, timeout=3,
        )
        pids = list(set(re.findall(r"pid=(\d+)", result.stdout)))
        if not pids:
            return False
        for pid_str in pids:
            pid = int(pid_str)
            msg = f"[MANAGER] port {port} occupied by PID {pid} — sending SIGTERM"
            log.warning("%s: %s", label, msg)
            await broadcast_fn({"type": "log", "component": "manager", "line": msg, "stream": "stderr"})
            try:
                os.kill(pid, signal.SIGTERM)
                await asyncio.sleep(0.5)
                try:
                    os.kill(pid, 0)          # still alive?
                    os.kill(pid, signal.SIGKILL)
                    msg2 = f"[MANAGER] PID {pid} did not exit — sent SIGKILL"
                    log.warning("%s: %s", label, msg2)
                    await broadcast_fn({"type": "log", "component": "manager", "line": msg2, "stream": "stderr"})
                except ProcessLookupError:
                    pass                     # clean exit after SIGTERM
            except ProcessLookupError:
                pass
        await asyncio.sleep(0.3)             # give OS time to release the port
        return True
    except Exception as exc:
        log.warning("Port %d occupant check failed: %s", port, exc)
        return False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # data_server root

# ---------------------------------------------------------------------------
# Component definition
# ---------------------------------------------------------------------------

class Component:
    def __init__(self, name: str, cfg: dict):
        self.name    = name
        cmd          = list(cfg["cmd"])
        # Resolve project-root-relative executables (e.g. ".venv/bin/python") against
        # BASE_DIR. Skip paths starting with "./" — those are intentionally relative
        # to the component's own cwd (e.g. "./minio").
        if cmd and not os.path.isabs(cmd[0]) and os.sep in cmd[0] and not cmd[0].startswith('./'):
            cmd[0] = os.path.join(BASE_DIR, cmd[0])
        self.cmd     = cmd
        self.cwd     = os.path.join(BASE_DIR, cfg.get("cwd", "."))
        self.env     = cfg.get("env", {})
        self.label   = cfg.get("label", name)
        self.ports: list = cfg.get("ports", [])
        self.process: Optional[asyncio.subprocess.Process] = None
        self.status  = "stopped"   # stopped | starting | running | error
        self.pid: Optional[int] = None
        self.logs: deque = deque(maxlen=200)
        self._reader_tasks: list = []

    def to_dict(self) -> dict:
        return {"status": self.status, "pid": self.pid, "label": self.label}

    async def start(self, broadcast_fn) -> None:
        if self.status == "running":
            return
        self.status = "starting"
        await broadcast_fn(self._status_msg())

        for port in self.ports:
            await _kill_port_occupants(port, self.label, broadcast_fn)

        env = {**os.environ, **self.env}
        try:
            self.process = await asyncio.create_subprocess_exec(
                *self.cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.cwd,
                env=env,
            )
            self.pid    = self.process.pid
            self.status = "running"
            log.info("Started %s (pid %d)", self.name, self.pid)
            _save_pids()
            await broadcast_fn(self._status_msg())

            # Stream stdout and stderr
            self._reader_tasks = [
                asyncio.create_task(self._stream(self.process.stdout, "stdout", broadcast_fn)),
                asyncio.create_task(self._stream(self.process.stderr, "stderr", broadcast_fn)),
                asyncio.create_task(self._wait(broadcast_fn)),
            ]
        except FileNotFoundError as exc:
            self.status = "error"
            self.pid    = None
            msg = f"[ERROR] Could not start: {exc}"
            self.logs.append(msg)
            await broadcast_fn({"type": "log", "component": self.name, "line": msg, "stream": "stderr"})
            await broadcast_fn(self._status_msg())

    async def stop(self, broadcast_fn) -> None:
        if self.process and self.status == "running":
            log.info("Stopping %s (pid %d)", self.name, self.pid)
            # Cancel readers and wait for them to finish so no further
            # log output is broadcast after this point
            for t in self._reader_tasks:
                t.cancel()
            await asyncio.gather(*self._reader_tasks, return_exceptions=True)
            self._reader_tasks = []
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()
        self.status  = "stopped"
        self.pid     = None
        self.process = None
        _save_pids()
        await broadcast_fn(self._status_msg())

    async def _stream(self, stream, stream_name: str, broadcast_fn) -> None:
        try:
            async for raw in stream:
                line = raw.decode(errors="replace").rstrip()
                if not line:
                    continue
                self.logs.append(line)
                await broadcast_fn({"type": "log", "component": self.name, "line": line, "stream": stream_name})
        except Exception:
            pass

    async def _wait(self, broadcast_fn) -> None:
        await self.process.wait()
        rc = self.process.returncode
        if self.status == "running":  # didn't stop intentionally
            self.status = "error" if rc != 0 else "stopped"
            self.pid    = None
            msg = f"[EXITED] return code {rc}"
            self.logs.append(msg)
            await broadcast_fn({"type": "log", "component": self.name, "line": msg, "stream": "stderr"})
            await broadcast_fn(self._status_msg())

    def _status_msg(self) -> dict:
        return {"type": "status", "components": {self.name: self.to_dict()}}


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

g_components: Dict[str, Component] = {}
g_connected: Set = set()


async def broadcast(msg: dict) -> None:
    if not g_connected:
        return
    data = json.dumps(msg, default=str)
    await asyncio.gather(*[ws.send(data) for ws in list(g_connected)], return_exceptions=True)


async def send_full_status(ws) -> None:
    await ws.send(json.dumps({
        "type":       "status",
        "components": {name: c.to_dict() for name, c in g_components.items()},
    }))
    # Replay last 20 log lines only for running components
    for name, comp in g_components.items():
        if comp.status != "running":
            continue
        for line in list(comp.logs)[-20:]:
            await ws.send(json.dumps({"type": "log", "component": name, "line": line, "stream": "stdout"}))


async def ws_handler(websocket) -> None:
    g_connected.add(websocket)
    log.info("Dashboard client connected (%d total)", len(g_connected))
    await send_full_status(websocket)

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = msg.get("type")

            if t == "get_status":
                await send_full_status(websocket)

            elif t == "start":
                name = msg.get("component")
                if name in g_components:
                    asyncio.create_task(g_components[name].start(broadcast))

            elif t == "stop":
                name = msg.get("component")
                if name in g_components:
                    asyncio.create_task(g_components[name].stop(broadcast))

            elif t == "start_all":
                async def _start_all():
                    for comp in g_components.values():
                        await comp.start(broadcast)
                asyncio.create_task(_start_all())

            elif t == "stop_all":
                async def _stop_all():
                    # Cancel every reader task immediately so logging stops at once
                    all_readers = [t for c in g_components.values() for t in c._reader_tasks]
                    for t in all_readers:
                        t.cancel()
                    await asyncio.gather(*all_readers, return_exceptions=True)
                    for c in g_components.values():
                        c._reader_tasks = []
                    # Now terminate all processes concurrently
                    await asyncio.gather(*[c.stop(broadcast) for c in g_components.values()], return_exceptions=True)
                asyncio.create_task(_stop_all())

            elif t == "rescan":
                # Adopt any processes already running that match our components.
                # Useful when processes were started outside the manager (e.g. by scripts).
                # We can't stream their logs but status/PID will show correctly.
                adopted = []
                for name, comp in g_components.items():
                    if comp.status == "running":
                        continue
                    if not comp.cmd:
                        continue
                    # Search for a process whose cmdline contains the component's script/binary
                    search = comp.cmd[-1] if len(comp.cmd) > 1 else comp.cmd[0]
                    result = subprocess.run(
                        ["pgrep", "-f", search],
                        capture_output=True, text=True,
                    )
                    pids = result.stdout.split()
                    if pids:
                        pid = int(pids[0])
                        comp.status = "running"
                        comp.pid    = pid
                        msg = f"[RESCAN] adopted {name} (pid {pid})"
                        log.info(msg)
                        comp.logs.append(msg)
                        adopted.append(name)
                        await broadcast(comp._status_msg())
                        await broadcast({"type": "log", "component": name, "line": msg, "stream": "stderr"})
                if not adopted:
                    await broadcast({"type": "log", "component": "manager",
                                     "line": "[RESCAN] no new processes adopted", "stream": "stderr"})
                _save_pids()

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        g_connected.discard(websocket)
        log.info("Dashboard client disconnected (%d remaining)", len(g_connected))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main_async(cfg: dict) -> None:
    global g_components, g_pid_file

    order = cfg.get("order", list(cfg["components"].keys()))
    for name in order:
        comp_cfg = cfg["components"][name]
        g_components[name] = Component(name, comp_cfg)

    ws_cfg  = cfg.get("websocket", {})
    ws_host = ws_cfg.get("host", "0.0.0.0")
    ws_port = ws_cfg.get("port", 8760)

    g_pid_file = f"/tmp/ds_manager_{ws_port}.pids"
    _cleanup_orphans()

    log.info("Manager WebSocket on ws://%s:%d", ws_host, ws_port)
    log.info("Components: %s", ", ".join(g_components.keys()))

    auto_start = cfg.get("auto_start", False)

    async with websockets.serve(ws_handler, ws_host, ws_port):
        if auto_start:
            log.info("auto_start: starting all components")
            for comp in g_components.values():
                await comp.start(broadcast)
        await asyncio.Event().wait()


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data server process manager")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    try:
        asyncio.run(main_async(load_config(args.config)))
    except KeyboardInterrupt:
        log.info("Shutting down — stopping all components")
        async def stop_all():
            for comp in g_components.values():
                await comp.stop(lambda m: None)
        asyncio.run(stop_all())
