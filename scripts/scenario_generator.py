#!/usr/bin/env python3
"""
scenario_generator.py — drive 4 EMS simulators via WebSocket on a scripted timeline.

Usage:
    python3 scripts/scenario_generator.py --scenario scripts/scenarios/overtemp_cascade.yaml
    python3 scripts/scenario_generator.py --scenario scripts/scenarios/overtemp_cascade.yaml \
        --host 192.168.86.48 --ports 8769,8779,8789,8799

Scenario YAML format:
    name: my_scenario
    steps:
      - at: 30      # seconds from scenario start
        sim: 1      # 1-4, or "all"
        command: inject_fault
        units: [ALL]          # or list of unit IDs
        fault: overtemp
        value: 52.0
        rack: -1              # -1 = all racks

      - at: 60
        sim: all
        command: set_mode
        units: [ALL]
        mode: discharge
        current_a: 300

      - at: 120
        sim: 1
        command: clear_faults
        units: [ALL]

Supported commands and their extra fields:
    set_mode       mode (standby|charge|discharge|offline), current_a
    set_contactor  closed (true|false)
    inject_fault   fault (overtemp|low_soc|undervolt|overvolt|overcurrent|soc_imbalance),
                   rack, value, current_a
    clear_faults   (no extra fields)
    set_noise      amplitude
    set_rate       rate
    get_status     (no extra fields)
"""

import asyncio
import json
import sys
import time
import argparse
import struct
import socket
import hashlib
import base64
import os

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed — run: pip install pyyaml")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Minimal WebSocket client (no external ws library needed)
# ---------------------------------------------------------------------------

class SimpleWS:
    """Minimal synchronous WebSocket client over raw TCP."""

    def __init__(self, host: str, port: int, label: str):
        self.host  = host
        self.port  = port
        self.label = label
        self.sock  = None

    def connect(self, timeout: float = 5.0):
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock = socket.create_connection((self.host, self.port), timeout=timeout)
        self.sock.settimeout(timeout)
        hs = (
            f"GET / HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(hs.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            resp += self.sock.recv(4096)
        # Verify 101
        if b"101" not in resp:
            raise ConnectionError(f"WebSocket upgrade failed: {resp[:200]}")
        self.sock.settimeout(None)

    def send_text(self, text: str):
        data = text.encode("utf-8")
        n = len(data)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        hdr = bytearray()
        hdr.append(0x81)  # FIN + text opcode
        if n <= 125:
            hdr.append(0x80 | n)
        elif n <= 65535:
            hdr.append(0x80 | 126)
            hdr += struct.pack(">H", n)
        else:
            hdr.append(0x80 | 127)
            hdr += struct.pack(">Q", n)
        hdr += mask
        self.sock.sendall(bytes(hdr) + masked)

    def close(self):
        if self.sock:
            try:
                self.sock.sendall(b"\x88\x00")  # close frame
            except OSError:
                pass
            self.sock.close()
            self.sock = None


# ---------------------------------------------------------------------------
# Command builder
# ---------------------------------------------------------------------------

COMMAND_FIELDS = {
    "set_mode":       ["mode", "current_a"],
    "set_contactor":  ["closed"],
    "inject_fault":   ["fault", "rack", "value", "current_a"],
    "clear_faults":   [],
    "set_noise":      ["amplitude"],
    "set_rate":       ["rate"],
    "get_status":     [],
}


def build_command(step: dict) -> dict:
    cmd_type = step["command"]
    if cmd_type not in COMMAND_FIELDS:
        raise ValueError(f"Unknown command: {cmd_type!r}. "
                         f"Valid: {list(COMMAND_FIELDS)}")
    cmd = {"type": cmd_type, "units": step.get("units", ["ALL"])}
    for field in COMMAND_FIELDS[cmd_type]:
        if field in step:
            cmd[field] = step[field]
    return cmd


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

def run_scenario(scenario: dict, connections: dict):
    steps = sorted(scenario.get("steps", []), key=lambda s: s["at"])
    name  = scenario.get("name", "unnamed")
    print(f"[scenario] starting '{name}'  ({len(steps)} steps)")

    t_start = time.time()

    for step in steps:
        target_t = t_start + step["at"]
        wait     = target_t - time.time()
        if wait > 0:
            time.sleep(wait)

        elapsed = time.time() - t_start
        sim_key = step.get("sim", "all")
        try:
            cmd = build_command(step)
        except ValueError as e:
            print(f"[t+{elapsed:.1f}s] ERROR: {e}")
            continue

        payload = json.dumps(cmd)

        if str(sim_key).lower() == "all":
            targets = list(connections.values())
            label   = "all"
        else:
            idx = int(sim_key)
            if idx not in connections:
                print(f"[t+{elapsed:.1f}s] ERROR: sim {idx} not connected")
                continue
            targets = [connections[idx]]
            label   = f"sim-{idx}"

        for ws in targets:
            try:
                ws.send_text(payload)
            except OSError as e:
                print(f"[t+{elapsed:.1f}s] WARN: send to {ws.label} failed: {e}")

        print(f"[t+{elapsed:.1f}s] {label:6s} → {cmd['type']}  {json.dumps({k:v for k,v in cmd.items() if k not in ('type','units')})}")

    total = time.time() - t_start
    print(f"[scenario] '{name}' complete in {total:.1f}s")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="EMS scenario generator")
    parser.add_argument("--scenario", required=True, help="YAML scenario file")
    parser.add_argument("--host",  default="localhost",
                        help="Target host (default: localhost)")
    parser.add_argument("--ports", default="8769,8779,8789,8799",
                        help="Comma-separated WS ports for sim 1..4 (default: 8769,8779,8789,8799)")
    args = parser.parse_args()

    with open(args.scenario) as f:
        scenario = yaml.safe_load(f)

    ports = [int(p.strip()) for p in args.ports.split(",")]

    # Connect to each simulator
    connections = {}
    for i, port in enumerate(ports, start=1):
        ws = SimpleWS(args.host, port, f"sim-{i}")
        try:
            ws.connect(timeout=5.0)
            connections[i] = ws
            print(f"[sim-{i}] connected to {args.host}:{port}")
        except (OSError, ConnectionError) as e:
            print(f"[sim-{i}] WARNING: could not connect to {args.host}:{port}: {e}")

    if not connections:
        print("ERROR: no simulators reachable — are they running?")
        sys.exit(1)

    try:
        run_scenario(scenario, connections)
    except KeyboardInterrupt:
        print("\n[scenario] interrupted")
    finally:
        for ws in connections.values():
            ws.close()


if __name__ == "__main__":
    main()
