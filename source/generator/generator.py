"""
Data generator — publishes simulated asset data to an MQTT broker.
Supports multiple concurrent source types: battery, solar, wind, localgen.
Exposes a WebSocket server for browser-based control and live data viewing.

WebSocket API (ws://localhost:8765):
  Client → Server:
    {"type": "start"}
    {"type": "stop"}
    {"type": "update_config", "config": {"sources": [...], "sample_interval_seconds": 1, ...}}
    {"type": "get_status"}

  Server → Client:
    {"type": "status",  "running": bool, "config": {...}, "cell_count": int}
    {"type": "stats",   "total_published": int, "mps": int, "last_loop_ms": float}
    {"type": "samples", "samples": [...]}
"""

import argparse
import asyncio
import json
import logging
import random
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import paho.mqtt.client as mqtt
import websockets
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source type defaults
# ---------------------------------------------------------------------------

SOURCE_DEFAULTS: Dict[str, dict] = {
    "battery": {
        "topic_prefix": "batteries",
        "sites": 2, "racks_per_site": 3, "modules_per_rack": 4, "cells_per_module": 6,
        "cell_data": {
            "voltage":     {"min": 3.0,   "max": 4.2,   "nominal": 3.7,   "unit": "V",   "drift": 0.005},
            "current":     {"min": -50.0, "max": 50.0,  "nominal": 0.0,   "unit": "A",   "drift": 0.5},
            "temperature": {"min": 20.0,  "max": 45.0,  "nominal": 25.0,  "unit": "C",   "drift": 0.1},
            "soc":         {"min": 0.0,   "max": 100.0, "nominal": 80.0,  "unit": "%",   "drift": 0.05},
            "soh":         {"min": 80.0,  "max": 100.0, "nominal": 98.0,  "unit": "%",   "drift": 0.01},
            "resistance":  {"min": 0.001, "max": 0.05,  "nominal": 0.01,  "unit": "ohm", "drift": 0.0001},
            "capacity":    {"min": 80.0,  "max": 105.0, "nominal": 100.0, "unit": "Ah",  "drift": 0.01},
        },
    },
    "solar": {
        "topic_prefix": "solar",
        "sites": 2, "racks_per_site": 4, "modules_per_rack": 6, "cells_per_module": 10,
        "cell_data": {
            "irradiance":  {"min": 0,    "max": 1200, "nominal": 800, "unit": "W/m2", "drift": 5.0},
            "panel_temp":  {"min": -10,  "max": 85,   "nominal": 35,  "unit": "C",    "drift": 0.2},
            "dc_voltage":  {"min": 250,  "max": 1000, "nominal": 600, "unit": "V",    "drift": 2.0},
            "dc_current":  {"min": 0,    "max": 20,   "nominal": 8,   "unit": "A",    "drift": 0.1},
            "ac_power":    {"min": 0,    "max": 250,  "nominal": 180, "unit": "W",    "drift": 2.0},
            "efficiency":  {"min": 10,   "max": 25,   "nominal": 20,  "unit": "%",    "drift": 0.05},
        },
    },
    "wind": {
        "topic_prefix": "wind",
        "sites": 2, "racks_per_site": 5, "modules_per_rack": 1, "cells_per_module": 1,
        "cell_data": {
            "wind_speed":  {"min": 0,    "max": 30,   "nominal": 12,   "unit": "m/s", "drift": 0.2},
            "rotor_rpm":   {"min": 0,    "max": 25,   "nominal": 15,   "unit": "RPM", "drift": 0.1},
            "ac_power":    {"min": 0,    "max": 3000, "nominal": 1500, "unit": "kW",  "drift": 10.0},
            "frequency":   {"min": 49.5, "max": 50.5, "nominal": 50,   "unit": "Hz",  "drift": 0.005},
            "pitch_angle": {"min": 0,    "max": 90,   "nominal": 15,   "unit": "deg", "drift": 0.1},
            "temperature": {"min": -20,  "max": 80,   "nominal": 40,   "unit": "C",   "drift": 0.2},
        },
    },
    "localgen": {
        "topic_prefix": "localgen",
        "sites": 1, "racks_per_site": 2, "modules_per_rack": 1, "cells_per_module": 1,
        "cell_data": {
            "fuel_level":  {"min": 0,   "max": 100,  "nominal": 75,  "unit": "%",   "drift": 0.02},
            "rpm":         {"min": 0,   "max": 3600, "nominal": 1500, "unit": "RPM", "drift": 5.0},
            "ac_power":    {"min": 0,   "max": 500,  "nominal": 250, "unit": "kW",  "drift": 2.0},
            "frequency":   {"min": 49,  "max": 51,   "nominal": 50,  "unit": "Hz",  "drift": 0.01},
            "voltage":     {"min": 380, "max": 420,  "nominal": 400, "unit": "V",   "drift": 0.5},
            "temperature": {"min": 60,  "max": 120,  "nominal": 90,  "unit": "C",   "drift": 0.3},
        },
    },
}


def default_source(source_type: str, enabled: bool = False) -> dict:
    """Return a fully-populated source config for the given type."""
    d = SOURCE_DEFAULTS.get(source_type, SOURCE_DEFAULTS["battery"])
    return {
        "id":               source_type,
        "type":             source_type,
        "enabled":          enabled,
        "topic_prefix":     d["topic_prefix"],
        "sites":            d["sites"],
        "racks_per_site":   d["racks_per_site"],
        "modules_per_rack": d["modules_per_rack"],
        "cells_per_module": d["cells_per_module"],
        "cell_data":        d["cell_data"],
    }


def normalize_config(config: dict) -> dict:
    """Ensure config has a 'sources' list.

    Backward-compatible: if the old flat keys are present and no 'sources' key
    exists, wrap them as a single enabled battery source.  Any missing source
    types are added as disabled defaults so the UI always shows all four.
    """
    if "sources" not in config:
        src = {
            "id":               "battery",
            "type":             "battery",
            "enabled":          True,
            "topic_prefix":     config.get("topic_prefix", "batteries"),
            "sites":            config.get("sites", 2),
            "racks_per_site":   config.get("racks_per_site", 3),
            "modules_per_rack": config.get("modules_per_rack", 4),
            "cells_per_module": config.get("cells_per_module", 6),
            "cell_data":        config.get("cell_data", SOURCE_DEFAULTS["battery"]["cell_data"]),
        }
        config["sources"] = [src]

    # Fill in any missing source types as disabled
    existing_ids = {s["id"] for s in config["sources"]}
    for src_type in SOURCE_DEFAULTS:
        if src_type not in existing_ids:
            config["sources"].append(default_source(src_type, enabled=False))

    return config


# ---------------------------------------------------------------------------
# Cell simulation
# ---------------------------------------------------------------------------

@dataclass
class MeasurementState:
    value: float
    min: float
    max: float
    drift: float = 0.01

    def step(self) -> float:
        self.value += random.uniform(-self.drift, self.drift) * (self.max - self.min)
        self.value = max(self.min, min(self.max, self.value))
        return round(self.value, 4)


@dataclass
class CellState:
    source_id:    str
    topic_prefix: str
    site_id:      int
    rack_id:      int
    module_id:    int
    cell_id:      int
    cell_data_cfg: dict = field(default_factory=dict)   # for unit lookup
    measurements:  Dict[str, MeasurementState] = field(default_factory=dict)

    @property
    def cell_path(self) -> str:
        return (
            f"{self.source_id}/"
            f"site={self.site_id}/rack={self.rack_id}"
            f"/module={self.module_id}/cell={self.cell_id}"
        )


def build_cells(source: dict) -> List[CellState]:
    cells = []
    cell_data = source["cell_data"]
    for site in range(source["sites"]):
        for rack in range(source["racks_per_site"]):
            for module in range(source["modules_per_rack"]):
                for cell in range(source["cells_per_module"]):
                    state = CellState(
                        source_id=source["id"],
                        topic_prefix=source["topic_prefix"],
                        site_id=site, rack_id=rack, module_id=module, cell_id=cell,
                        cell_data_cfg=cell_data,
                    )
                    for name, cfg in cell_data.items():
                        nominal = cfg.get("nominal", (cfg["min"] + cfg["max"]) / 2)
                        initial = nominal + random.uniform(-0.05, 0.05) * (cfg["max"] - cfg["min"])
                        initial = max(cfg["min"], min(cfg["max"], initial))
                        state.measurements[name] = MeasurementState(
                            value=initial, min=cfg["min"], max=cfg["max"]
                        )
                    cells.append(state)
    count = len(cells)
    log.info(
        "Source %r: %d sites × %d racks × %d modules × %d cells = %d assets",
        source["id"], source["sites"], source["racks_per_site"],
        source["modules_per_rack"], source["cells_per_module"], count,
    )
    return cells


def build_all_cells(config: dict) -> List[CellState]:
    cells = []
    for src in config.get("sources", []):
        if src.get("enabled", False):
            cells.extend(build_cells(src))
    return cells


def publish_cell(client: mqtt.Client, cell: CellState, config: dict) -> int:
    mode      = config["topic_mode"]
    timestamp = time.time()
    values    = {name: state.step() for name, state in cell.measurements.items()}

    if mode == "per_cell":
        topic = f"{cell.topic_prefix}/{cell.cell_path}"
        payload = {
            "timestamp": timestamp,
            "source_id": cell.source_id,
            "site_id":   cell.site_id,
            "rack_id":   cell.rack_id,
            "module_id": cell.module_id,
            "cell_id":   cell.cell_id,
            **{
                name: {"value": val, "unit": cell.cell_data_cfg[name]["unit"]}
                for name, val in values.items()
            },
        }
        client.publish(topic, json.dumps(payload), qos=1)
        return 1

    elif mode == "per_cell_item":
        for name, val in values.items():
            topic = f"{cell.topic_prefix}/{cell.cell_path}/{name}"
            payload = {
                "timestamp": timestamp,
                "value": val,
                "unit": cell.cell_data_cfg[name]["unit"],
            }
            client.publish(topic, json.dumps(payload), qos=1)
        return len(values)

    raise ValueError(f"Unknown topic_mode: {mode!r}")


# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

g_running:     bool               = False
g_cells:       List[CellState]    = []
g_config:      dict               = {}
g_stats:       dict               = {"total_published": 0, "mps": 0, "last_loop_ms": 0.0}
g_connected:   Set                = set()
g_mqtt_client: Optional[mqtt.Client] = None


def public_config() -> dict:
    cells_by_source: Dict[str, int] = {}
    for cell in g_cells:
        cells_by_source[cell.source_id] = cells_by_source.get(cell.source_id, 0) + 1

    sources_out = []
    for src in g_config.get("sources", []):
        sources_out.append({**src, "cell_count": cells_by_source.get(src["id"], 0)})

    return {
        "sources":                sources_out,
        "sample_interval_seconds": g_config.get("sample_interval_seconds", 1),
        "topic_mode":             g_config.get("topic_mode", "per_cell"),
    }


async def broadcast(message: dict) -> None:
    if not g_connected:
        return
    data = json.dumps(message)
    await asyncio.gather(
        *[ws.send(data) for ws in list(g_connected)],
        return_exceptions=True,
    )


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

async def ws_handler(websocket) -> None:
    global g_running, g_cells, g_config

    g_connected.add(websocket)
    log.info("WebSocket client connected (%d total)", len(g_connected))

    await websocket.send(json.dumps({
        "type":       "status",
        "running":    g_running,
        "config":     public_config(),
        "cell_count": len(g_cells),
    }))
    await websocket.send(json.dumps({"type": "stats", **g_stats}))

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = msg.get("type")

            if t == "start":
                g_running = True
                log.info("Generation started via WebSocket")
                await broadcast({
                    "type": "status", "running": True,
                    "config": public_config(), "cell_count": len(g_cells),
                })

            elif t == "stop":
                g_running = False
                log.info("Generation stopped via WebSocket")
                await broadcast({
                    "type": "status", "running": False,
                    "config": public_config(), "cell_count": len(g_cells),
                })

            elif t == "update_config":
                updates = msg.get("config", {})
                # Accept either a full sources array or legacy flat keys
                if "sources" in updates:
                    g_config["sources"] = updates["sources"]
                # Top-level interval / topic_mode
                for key in ("sample_interval_seconds", "topic_mode"):
                    if key in updates:
                        g_config[key] = updates[key]
                # Legacy flat topology keys — apply to first enabled source
                flat_keys = {"sites", "racks_per_site", "modules_per_rack", "cells_per_module"}
                flat_updates = {k: v for k, v in updates.items() if k in flat_keys}
                if flat_updates and g_config.get("sources"):
                    g_config["sources"][0].update(flat_updates)

                g_cells = build_all_cells(g_config)
                log.info("Config updated — %d total assets across %d sources",
                         len(g_cells),
                         sum(1 for s in g_config.get("sources", []) if s.get("enabled")))
                await broadcast({
                    "type": "status", "running": g_running,
                    "config": public_config(), "cell_count": len(g_cells),
                })

            elif t == "get_status":
                await websocket.send(json.dumps({
                    "type":       "status",
                    "running":    g_running,
                    "config":     public_config(),
                    "cell_count": len(g_cells),
                }))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        g_connected.discard(websocket)
        log.info("WebSocket client disconnected (%d remaining)", len(g_connected))


# ---------------------------------------------------------------------------
# Generator loop
# ---------------------------------------------------------------------------

async def generator_loop() -> None:
    global g_stats

    while True:
        interval = g_config.get("sample_interval_seconds", 1)

        if g_running and g_cells and g_mqtt_client:
            t_start = time.monotonic()

            total = 0
            for cell in g_cells:
                total += publish_cell(g_mqtt_client, cell, g_config)

            elapsed = time.monotonic() - t_start

            g_stats = {
                "total_published": g_stats["total_published"] + total,
                "mps":             round(total / max(elapsed, 0.001)),
                "last_loop_ms":    round(elapsed * 1000, 1),
            }

            await broadcast({"type": "stats", **g_stats})

            sample_cells = random.sample(g_cells, min(10, len(g_cells)))
            samples = [
                {
                    "cell_path":    c.cell_path,
                    "source_id":    c.source_id,
                    "timestamp":    round(time.time(), 3),
                    "measurements": {
                        name: {"value": round(s.value, 4), "unit": c.cell_data_cfg[name]["unit"]}
                        for name, s in c.measurements.items()
                    },
                }
                for c in sample_cells
            ]
            await broadcast({"type": "samples", "samples": samples})

            if elapsed > interval:
                log.warning("Loop took %.3fs > interval %ds", elapsed, interval)

            await asyncio.sleep(max(0.0, interval - elapsed))
        else:
            await asyncio.sleep(0.1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main_async(config: dict) -> None:
    global g_config, g_cells, g_mqtt_client

    g_config = normalize_config(config)
    g_cells  = build_all_cells(g_config)

    broker = config["broker"]
    g_mqtt_client = mqtt.Client(client_id=broker.get("client_id", "data-generator"))
    if broker.get("username"):
        g_mqtt_client.username_pw_set(broker["username"], broker.get("password", ""))

    g_mqtt_client.on_connect = lambda c, u, f, rc: log.info(
        "MQTT connected to %s:%s (rc=%d)", broker["host"], broker["port"], rc
    )
    g_mqtt_client.on_disconnect = lambda c, u, rc: log.warning("MQTT disconnected (rc=%d)", rc)

    log.info("Connecting to MQTT broker %s:%s ...", broker["host"], broker["port"])
    g_mqtt_client.connect(broker["host"], broker["port"], keepalive=60)
    g_mqtt_client.loop_start()

    ws_cfg  = config.get("websocket", {})
    ws_host = ws_cfg.get("host", "0.0.0.0")
    ws_port = ws_cfg.get("port", 8765)
    log.info("WebSocket server listening on ws://%s:%d", ws_host, ws_port)

    async with websockets.serve(ws_handler, ws_host, ws_port):
        await generator_loop()


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-source MQTT data generator")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    cfg = load_config(args.config)
    try:
        asyncio.run(main_async(cfg))
    except KeyboardInterrupt:
        log.info("Shutting down")
        if g_mqtt_client:
            g_mqtt_client.loop_stop()
            g_mqtt_client.disconnect()
