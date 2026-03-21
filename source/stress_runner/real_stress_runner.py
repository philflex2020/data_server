"""
real_stress_runner — replays real EMS MQTT topology from Longbow production data.

Publishes synthetic values using the ACTUAL topic structure recorded from a live
Longbow site: ems/site/{site_id}/unit/{unit_id}/{device}/{instance}/{point}/{dtype}

Key differences vs stress_runner.py:
  - Topic format matches real production (ems/site/... not batteries/...)
  - Payload uses ISO 8601 timestamps: {"ts": "2024-11-15T21:27:52.775Z", "value": N}
  - 1770 unique topics per unit (bms/pcs/rack points from real recording)
  - Data types: integer (71%), float (18%), boolean_integer (11%) per template
  - Topic lengths average ~109 bytes → avg MQTT frame ~166 bytes (vs 109 B sim)
  - At 46 units × 1770 topics/unit ≈ 81,420 msgs/sweep → ~17× parquet compression

Topic template loaded from ems_topic_template.json (same directory as this file).
This file is extracted from /tmp/longbow_recording.csv.gz using tools/extract_ems_template.py.

WebSocket API (ws://localhost:8769) — identical to stress_runner.py:
  Server → Client (every second and on connect):
    {"type": "stats", "total_published": 12345, "mps": 288, "active_tasks": 4,
     "session_id": "abc123", "sync_seq": 7,
     "sites": [{"site_id": "0215D1D8", "units": 46, "published": 81420}]}
"""

import argparse
import asyncio
import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import paho.mqtt.client as mqtt
import websockets
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Topic template — loaded once at startup from ems_topic_template.json
# ---------------------------------------------------------------------------

def load_template(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def build_topic_list(unit_ids: List[str], template: List[list]) -> List[tuple]:
    """Return list of (topic_str, dtype) for all units × template entries.

    Topic format: unit/{unit_id}/{device}/{instance}/{point_name}/{dtype}
    site_id is NOT in the topic — it lives in the partition path on disk and
    is injected by real_writer from its config.
    """
    topics = []
    for unit_id in unit_ids:
        for device, instance, point, dtype in template:
            topic = f"unit/{unit_id}/{device}/{instance}/{point}/{dtype}"
            topics.append((topic, dtype))
    return topics


# ---------------------------------------------------------------------------
# Value generation by dtype
# ---------------------------------------------------------------------------

# Pre-allocate a random pool for integer/boolean to avoid per-call overhead
_INT_POOL   = [random.randint(0, 100) for _ in range(65536)]
_BOOL_POOL  = [0] * 52 + [1] * 12   # ~81% false / ~19% true (faults mostly clear)
_FLOAT_BASE = [random.uniform(0.0, 100.0) for _ in range(65536)]
_pool_idx = 0


def _next_value(dtype: str) -> float | int:
    global _pool_idx
    _pool_idx = (_pool_idx + 1) & 0xFFFF
    if dtype == "float":
        return round(_FLOAT_BASE[_pool_idx], 6)
    elif dtype == "boolean_integer":
        return random.choice(_BOOL_POOL)
    else:  # integer
        return _INT_POOL[_pool_idx]


def _iso_now() -> str:
    """Current UTC time as ISO 8601 with milliseconds: 2024-11-15T21:27:52.775Z"""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

g_total_published: int = 0
g_mps:             int = 0
g_active_tasks:    int = 0
g_site_published:  Dict[str, int] = {}
g_ws_clients:      Set = set()
g_mqtt_client:     Optional[mqtt.Client] = None
g_stop:            asyncio.Event
g_interval:        float = 1.0
g_session_id:      str   = ""
g_sync_seq:        int   = 0
g_last_sync_total: int   = 0

# Topology metadata (set in main_async)
g_site_id:         str   = ""
g_unit_ids:        List[str] = []
g_topics:          List[tuple] = []    # [(topic, dtype), ...]


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------

def make_mqtt_client(mqtt_cfg: dict) -> mqtt.Client:
    client = mqtt.Client(client_id=mqtt_cfg.get("client_id", "real-stress-runner"))
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
# Publish loop
# ---------------------------------------------------------------------------

async def publish_loop() -> None:
    global g_total_published, g_active_tasks, g_site_published

    log.info("Publish loop: %d units × %d topics/unit = %d total topics per sweep",
             len(g_unit_ids), len(g_topics) // max(len(g_unit_ids), 1), len(g_topics))

    g_active_tasks += 1
    try:
        while not g_stop.is_set():
            t_start  = time.monotonic()
            interval = g_interval
            batch    = 0

            ts_str = _iso_now()

            for topic, dtype in g_topics:
                if g_stop.is_set():
                    break

                # Drop guard: skip publish when paho's outbound queue backs up
                if len(g_mqtt_client._out_packet) > 20000:
                    batch += 1
                    continue

                value = _next_value(dtype)
                payload = json.dumps({"ts": ts_str, "value": value})
                g_mqtt_client.publish(topic, payload, qos=0)
                batch += 1

            g_total_published += batch
            g_site_published[g_site_id] = g_site_published.get(g_site_id, 0) + batch

            elapsed   = time.monotonic() - t_start
            sleep_for = max(0.0, interval - elapsed)
            if elapsed > interval:
                log.warning("Publish loop: %.3fs > interval %.1fs — consider fewer units",
                            elapsed, interval)
            await asyncio.sleep(sleep_for if sleep_for > 0 else 0)
    finally:
        g_active_tasks -= 1
        log.info("Publish loop exiting")


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

def build_stats_message() -> dict:
    return {
        "type":            "stats",
        "total_published": g_total_published,
        "mps":             g_mps,
        "active_tasks":    g_active_tasks,
        "interval":        g_interval,
        "session_id":      g_session_id,
        "sync_seq":        g_sync_seq,
        # sites list mirrors stress_runner API shape so parquet_live_demo.html works
        "projects": [
            {
                "project_id": 0,
                "sites": [
                    {
                        "site_id":   0,
                        "ems_site":  g_site_id,
                        "units":     len(g_unit_ids),
                        "topics":    len(g_topics),
                        "published": g_site_published.get(g_site_id, 0),
                    }
                ],
            }
        ],
    }


async def ws_handler(websocket) -> None:
    global g_interval
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
                new_interval = max(0.05, min(60.0, new_interval))
                g_interval = new_interval
                log.info("Rate changed → interval=%.2fs", g_interval)
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
# Sync publisher — chain-of-custody heartbeat (same as stress_runner.py)
# ---------------------------------------------------------------------------

async def sync_publisher(sync_interval: float) -> None:
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
        }
        g_mqtt_client.publish("_sync", json.dumps(msg), qos=0)
        log.info("Sync #%d: total=%d interval=%d", g_sync_seq, total, interval_msgs)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main_async(config: dict) -> None:
    global g_mqtt_client, g_stop, g_interval, g_session_id
    global g_site_id, g_unit_ids, g_topics

    g_stop       = asyncio.Event()
    g_interval   = float(config.get("publish_interval_seconds", 1.0))
    g_session_id = uuid.uuid4().hex[:8]
    log.info("Session ID: %s", g_session_id)

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT,  lambda: (log.info("SIGINT"),  g_stop.set()))
    loop.add_signal_handler(signal.SIGTERM, lambda: (log.info("SIGTERM"), g_stop.set()))

    # Load topic template
    template_path = config.get("template_file",
                               os.path.join(_HERE, "ems_topic_template.json"))
    template_data = load_template(template_path)
    g_site_id     = config.get("site_id", template_data["site_id"])
    all_units     = config.get("unit_ids", list(template_data.get("unit_ids", [])))

    # If unit_ids not in config, use N units from the template or a synthetic list
    if not all_units:
        # Generate synthetic hex unit IDs similar to real recording
        n_units = int(config.get("num_units", 4))
        base    = 0x0215F5DD
        all_units = [f"{base + i:08X}" for i in range(n_units)]

    g_unit_ids = all_units
    g_topics   = build_topic_list(g_unit_ids, template_data["template"])

    log.info("Site: %s  Units: %d  Topics/sweep: %d  Interval: %.2fs  Est.mps: %.0f",
             g_site_id, len(g_unit_ids), len(g_topics), g_interval,
             len(g_topics) / g_interval)

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

    sync_interval = float(config.get("sync_interval_seconds", 10.0))

    async with websockets.serve(ws_handler, ws_host, ws_port):
        pub_task    = asyncio.create_task(publish_loop(), name="publish")
        broadcaster = asyncio.create_task(stats_broadcaster(), name="broadcaster")
        syncer      = asyncio.create_task(sync_publisher(sync_interval), name="syncer")
        await asyncio.gather(pub_task, return_exceptions=True)
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
    parser = argparse.ArgumentParser(description="Real EMS telemetry stress runner (Longbow schema)")
    parser.add_argument("--config", default=os.path.join(_HERE, "real_stress_runner_config.yaml"))
    args = parser.parse_args()
    asyncio.run(main_async(load_config(args.config)))
