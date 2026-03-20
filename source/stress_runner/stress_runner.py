"""
Stress runner — simulates multiple projects/sites publishing battery data 24/7.

Each (project, site) pair runs as its own asyncio Task with its own cell
simulation loop.  A single shared paho-mqtt client (loop_start mode) is used
for all publishes.

Cell simulation is provided by the shared cell_generator module.  Each project
can define its own measurement schema via an optional per-project `cell_data`
block in the config; projects without one inherit the top-level `cell_data`
(which itself falls back to DEFAULT_CELL_DATA from cell_generator).

Topic format:
  {topic_prefix}/project={project_id}/site={site_id}/rack={rack_id}/module={module_id}/cell={cell_id}

Config example:

  cell_data:                        # default schema for all projects
    voltage:     {min: 3.0, max: 4.2, nominal: 3.7}
    current:     {min: -50, max: 50,  nominal: 0}
    temperature: {min: 20,  max: 45,  nominal: 25}
    soc:         {min: 0,   max: 100, nominal: 80}

  projects:
    - project_id: 0
      sites: [{site_id: 0, racks: 4, modules_per_rack: 8, cells_per_module: 12}]

    - project_id: 1
      cell_data:                    # project-specific schema override
        voltage:     {min: 2.5, max: 4.35, nominal: 3.6}
        temperature: {min: -10, max: 60,   nominal: 22}
        soc:         {min: 0,   max: 100,  nominal: 75}
      sites: [{site_id: 0, racks: 2, modules_per_rack: 4, cells_per_module: 8}]

WebSocket API (ws://localhost:8769):
  Server → Client (every second and on connect):
    {"type": "stats", "total_published": 12345, "mps": 288, "active_tasks": 4,
     "projects": [{"project_id": 0,
                   "sites": [{"site_id": 0, "cells": 384, "published": 6000}]}]}
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
import uuid
from typing import Dict, List, Optional, Set

import paho.mqtt.client as mqtt
import websockets
import yaml

# Shared simulation core
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "generator"))
from cell_generator import DEFAULT_CELL_DATA, build_cells  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared global state
# ---------------------------------------------------------------------------

g_site_counters:   Dict[tuple, int] = {}
g_total_published: int = 0
g_mps:             int = 0
g_active_tasks:    int = 0
g_topology:        List[dict] = []
g_ws_clients:      Set = set()
g_mqtt_client:     Optional[mqtt.Client] = None
g_stop:            asyncio.Event
g_interval:        float = 1.0   # seconds between publish sweeps — set_rate changes this live
g_topic_mode:      str   = "per_cell"  # "per_cell" | "per_cell_item" — set_mode changes this live
g_session_id:      str   = ""    # unique ID per run — reset on restart, lets consumers detect gaps
g_sync_seq:        int   = 0     # monotonically increasing sync counter
g_last_sync_total: int   = 0     # g_total_published snapshot at last sync publish


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------

def make_mqtt_client(mqtt_cfg: dict) -> mqtt.Client:
    client = mqtt.Client(client_id=mqtt_cfg.get("client_id", "stress-runner"))
    username = mqtt_cfg.get("username", "")
    if username:
        client.username_pw_set(username, mqtt_cfg.get("password", ""))
    client.on_connect    = lambda c, u, f, rc: log.info(
        "MQTT connected to %s:%d (rc=%d)", mqtt_cfg["host"], mqtt_cfg["port"], rc)
    client.on_disconnect = lambda c, u, rc: log.warning("MQTT disconnected (rc=%d)", rc)
    return client


def connect_mqtt_with_backoff(client: mqtt.Client, host: str, port: int) -> None:
    delay = 1.0
    while True:
        try:
            client.connect(host, port, keepalive=60)
            client.loop_start()
            return
        except Exception as exc:
            log.warning("MQTT connect failed (%s) — retrying in %.0fs", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)


# ---------------------------------------------------------------------------
# Per-(project, site) publish loop
# ---------------------------------------------------------------------------

async def site_publish_loop(project_id: int, site_cfg: dict, config: dict,
                            cell_data: dict) -> None:
    global g_total_published, g_active_tasks

    site_id      = site_cfg["site_id"]
    topic_prefix = config["topic_prefix"]

    cells = build_cells(
        project_id       = project_id,
        site_id          = site_id,
        racks            = site_cfg["racks"],
        modules_per_rack = site_cfg["modules_per_rack"],
        cells_per_module = site_cfg["cells_per_module"],
        cell_data        = cell_data,
    )
    key = (project_id, site_id)
    g_site_counters[key] = 0

    log.info("Project %d site %d: %d cells  interval=%.1fs  schema=%s",
             project_id, site_id, len(cells), g_interval, list(cell_data.keys()))

    g_active_tasks += 1
    try:
        while not g_stop.is_set():
            t_start  = time.monotonic()
            interval = g_interval          # read live — set_rate updates this
            batch    = 0

            mode = g_topic_mode   # read live — set_mode updates this
            for cell in cells:
                if g_stop.is_set():
                    break
                # Drop messages when paho's outbound queue is backing up — paho has no
                # built-in size limit for QoS 0 packets, so without this guard the deque
                # grows without bound if the broker is slow (e.g. cold start after reboot).
                if len(g_mqtt_client._out_packet) > 20000:
                    batch += 8 if mode == "per_cell_item" else 1   # count as sent for stats
                    continue
                # QoS 0 (fire-and-forget) avoids PUBACK backlog that causes FlashMQ to
                # drop messages for slow subscribers before they even reach telegraf.
                # TODO: discuss QoS tradeoffs with team — QoS 1 is correct for production
                # but at high rates the PUBACK round-trip saturates broker queues.
                if mode == "per_cell_item":
                    ts     = time.time()
                    topic  = cell.topic          # cache — property rebuilds f-string each call
                    values = {name: state.step() for name, state in cell.measurements.items()}
                    if "voltage" in values and "current" in values:
                        values["power"] = round(values["voltage"] * values["current"], 4)
                    for name, val in values.items():
                        g_mqtt_client.publish(
                            f"{topic}/{name}",
                            json.dumps({"timestamp": ts, "value": val}),
                            qos=0,
                        )
                        batch += 1
                else:
                    g_mqtt_client.publish(cell.topic, json.dumps(cell.payload()), qos=0)
                    batch += 1

            g_site_counters[key] += batch
            g_total_published    += batch

            elapsed   = time.monotonic() - t_start
            sleep_for = max(0.0, interval - elapsed)
            if elapsed > interval:
                log.warning("Project %d site %d: loop %.3fs > interval %.1fs",
                            project_id, site_id, elapsed, interval)
            await asyncio.sleep(sleep_for if sleep_for > 0 else 0)
    finally:
        g_active_tasks -= 1
        log.info("Project %d site %d: task exiting", project_id, site_id)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

def build_stats_message() -> dict:
    projects_out = []
    for proj in g_topology:
        pid = proj["project_id"]
        sites_out = [
            {"site_id": s["site_id"], "cells": s["cells"],
             "published": g_site_counters.get((pid, s["site_id"]), 0)}
            for s in proj["sites"]
        ]
        projects_out.append({"project_id": pid, "sites": sites_out})
    return {
        "type":            "stats",
        "total_published": g_total_published,
        "mps":             g_mps,
        "active_tasks":    g_active_tasks,
        "interval":        g_interval,
        "topic_mode":      g_topic_mode,
        "session_id":      g_session_id,
        "sync_seq":        g_sync_seq,
        "projects":        projects_out,
    }


async def ws_handler(websocket) -> None:
    global g_interval, g_topic_mode
    g_ws_clients.add(websocket)
    log.info("WebSocket client connected (%d total)", len(g_ws_clients))
    await websocket.send(json.dumps(build_stats_message()))
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "get_status":
                await websocket.send(json.dumps(build_stats_message()))
            elif msg.get("type") == "set_rate":
                new_interval = float(msg["interval"])
                new_interval = max(0.05, min(60.0, new_interval))  # clamp 50ms–60s
                g_interval = new_interval
                log.info("Rate changed → interval=%.2fs (~%.0f mps)", g_interval,
                         sum(s["cells"] for p in g_topology for s in p["sites"]) / g_interval)
                await websocket.send(json.dumps(build_stats_message()))
            elif msg.get("type") == "set_mode":
                new_mode = msg.get("mode", "per_cell")
                if new_mode in ("per_cell", "per_cell_item"):
                    g_topic_mode = new_mode
                    total_cells = sum(s["cells"] for p in g_topology for s in p["sites"])
                    fields_est  = 8  # typical fields per cell (voltage/current/temp/soc/soh/res/cap/power)
                    est_mps     = total_cells * (fields_est if new_mode == "per_cell_item" else 1) / g_interval
                    log.info("Mode changed → %s (~%.0f mps estimated)", g_topic_mode, est_mps)
                await websocket.send(json.dumps(build_stats_message()))
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        g_ws_clients.discard(websocket)
        log.info("WebSocket client disconnected (%d remaining)", len(g_ws_clients))


async def stats_broadcaster() -> None:
    global g_mps
    last_total = 0
    last_ts    = time.monotonic()
    while not g_stop.is_set():
        await asyncio.sleep(1.0)
        now        = time.monotonic()
        g_mps      = round((g_total_published - last_total) / max(now - last_ts, 0.001))
        last_total = g_total_published
        last_ts    = now
        if g_ws_clients:
            msg = json.dumps(build_stats_message())
            await asyncio.gather(*[ws.send(msg) for ws in list(g_ws_clients)],
                                 return_exceptions=True)


# ---------------------------------------------------------------------------
# Sync publisher — chain-of-custody heartbeat
# ---------------------------------------------------------------------------

async def sync_publisher(sync_interval: float) -> None:
    """Publish periodic _sync messages so downstream consumers can detect drops.

    Each message carries the number of MQTT messages published since the previous
    sync, allowing writer.cpp, subscriber-api, etc. to compare their received count
    against the expected count and report the gap.
    """
    global g_sync_seq, g_last_sync_total
    while not g_stop.is_set():
        await asyncio.sleep(sync_interval)
        if g_mqtt_client is None:
            continue
        g_sync_seq   += 1
        total         = g_total_published
        interval_msgs = total - g_last_sync_total
        g_last_sync_total = total
        msg = {
            "type":               "sync",
            "session_id":         g_session_id,
            "seq":                g_sync_seq,
            "total_published":    total,
            "interval_published": interval_msgs,
            "timestamp":          time.time(),
            "topic_mode":         g_topic_mode,
        }
        g_mqtt_client.publish("_sync", json.dumps(msg), qos=0)
        log.info("Sync #%d: total=%d interval=%d session=%s",
                 g_sync_seq, total, interval_msgs, g_session_id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main_async(config: dict) -> None:
    global g_mqtt_client, g_stop, g_topology, g_interval, g_topic_mode, g_session_id

    g_stop       = asyncio.Event()
    g_interval   = float(config.get("sample_interval_seconds", 1.0))
    g_topic_mode = config.get("topic_mode", "per_cell")
    g_session_id = uuid.uuid4().hex[:8]
    log.info("Session ID: %s", g_session_id)

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT,  lambda: (log.info("SIGINT"),  g_stop.set()))
    loop.add_signal_handler(signal.SIGTERM, lambda: (log.info("SIGTERM"), g_stop.set()))

    # Top-level default schema (falls back to hardcoded DEFAULT_CELL_DATA)
    default_cell_data = config.get("cell_data", DEFAULT_CELL_DATA)

    # Build topology metadata for stats reporting
    g_topology = []
    for proj_cfg in config["projects"]:
        pid = proj_cfg["project_id"]
        g_topology.append({
            "project_id": pid,
            "sites": [
                {"site_id": s["site_id"],
                 "cells": s["racks"] * s["modules_per_rack"] * s["cells_per_module"]}
                for s in proj_cfg["sites"]
            ],
        })

    # MQTT
    mqtt_cfg = config["mqtt"]
    g_mqtt_client = make_mqtt_client(mqtt_cfg)
    log.info("Connecting to MQTT %s:%d ...", mqtt_cfg["host"], mqtt_cfg["port"])
    connect_mqtt_with_backoff(g_mqtt_client, mqtt_cfg["host"], mqtt_cfg["port"])

    # WebSocket server
    ws_cfg  = config.get("websocket", {})
    ws_host = ws_cfg.get("host", "0.0.0.0")
    ws_port = ws_cfg.get("port", 8769)
    log.info("WebSocket server on ws://%s:%d", ws_host, ws_port)

    # Launch one asyncio task per (project, site)
    tasks: List[asyncio.Task] = []
    for proj_cfg in config["projects"]:
        pid = proj_cfg["project_id"]
        # Per-project schema; falls back to top-level default
        proj_cell_data = proj_cfg.get("cell_data", default_cell_data)
        for site_cfg in proj_cfg["sites"]:
            tasks.append(asyncio.create_task(
                site_publish_loop(pid, site_cfg, config, proj_cell_data),
                name=f"p{pid}/s{site_cfg['site_id']}",
            ))

    log.info("Launched %d site task(s)", len(tasks))

    sync_interval = float(config.get("sync_interval_seconds", 10.0))
    async with websockets.serve(ws_handler, ws_host, ws_port):
        broadcaster = asyncio.create_task(stats_broadcaster())
        syncer      = asyncio.create_task(sync_publisher(sync_interval))
        await asyncio.gather(*tasks, return_exceptions=True)
        broadcaster.cancel()
        syncer.cancel()
        try:
            await broadcaster
        except asyncio.CancelledError:
            pass

    g_mqtt_client.loop_stop()
    g_mqtt_client.disconnect()
    log.info("Shutdown complete")


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Battery telemetry stress runner")
    parser.add_argument("--config", default="config.spark.yaml")
    args = parser.parse_args()
    asyncio.run(main_async(load_config(args.config)))
