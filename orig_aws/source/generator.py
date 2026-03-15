"""
Battery data generator for the FlashMQ → Telegraf → InfluxDB2 pipeline.

Publishes JSON-encoded cell measurements to MQTT topics:
  batteries/project=0/site=0/rack=1/module=2/cell=3

Payload schema (matches telegraf.conf json_v2 parser):
  {
    "timestamp":   1700000000.0, # unix seconds (float)
    "project_id":  0,
    "site_id":     0,
    "rack_id":     1,
    "module_id":   2,
    "cell_id":     3,
    "voltage":     3.72,
    "current":     -1.5,
    "temperature": 28.4,
    "soc":         72.3,
    "soh":         98.1,
    "resistance":  0.0023,
    "capacity":    98.5,
    "power":       -5.58
  }

Usage:
  pip install paho-mqtt pyyaml
  python generator.py
  python generator.py --config config.yaml
"""

import argparse
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import List

import paho.mqtt.client as mqtt
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Measurement bounds ────────────────────────────────────────────────────────

MEASUREMENTS = {
    # name:         (min,    max,   nominal, drift_scale)
    "voltage":      (3.0,    4.2,   3.7,    0.005),
    "current":      (-10.0,  10.0,  0.0,    0.2),
    "temperature":  (15.0,   45.0,  28.0,   0.1),
    "soc":          (10.0,   95.0,  70.0,   0.05),
    "soh":          (80.0,   100.0, 98.0,   0.01),
    "resistance":   (0.001,  0.01,  0.003,  0.00005),
    "capacity":     (80.0,   105.0, 100.0,  0.01),
}


@dataclass
class MeasurementState:
    min_val: float
    max_val: float
    value: float
    drift: float

    def step(self) -> float:
        """Random-walk with drift reversal at bounds."""
        self.drift += random.gauss(0, self.drift * 0.1 + 1e-6)
        self.drift  = max(-abs(self.drift), min(abs(self.drift), (self.max_val - self.min_val) * 0.02))
        self.value += self.drift
        if self.value > self.max_val:
            self.value = self.max_val
            self.drift = -abs(self.drift)
        elif self.value < self.min_val:
            self.value = self.min_val
            self.drift = abs(self.drift)
        return round(self.value, 5)


@dataclass
class CellState:
    project_id: int
    site_id:    int
    rack_id:    int
    module_id:  int
    cell_id:    int
    states:     dict = field(default_factory=dict)

    def __post_init__(self):
        for name, (lo, hi, nominal, drift) in MEASUREMENTS.items():
            jitter = random.uniform(-0.1, 0.1) * (hi - lo)
            self.states[name] = MeasurementState(
                min_val = lo,
                max_val = hi,
                value   = max(lo, min(hi, nominal + jitter)),
                drift   = random.uniform(-drift, drift),
            )

    @property
    def topic(self) -> str:
        return (f"batteries/project={self.project_id}/site={self.site_id}"
                f"/rack={self.rack_id}/module={self.module_id}/cell={self.cell_id}")

    def payload(self) -> dict:
        p = {
            "timestamp":  time.time(),
            "project_id": self.project_id,
            "site_id":    self.site_id,
            "rack_id":    self.rack_id,
            "module_id":  self.module_id,
            "cell_id":    self.cell_id,
        }
        for name, state in self.states.items():
            p[name] = state.step()
        # Derived field
        p["power"] = round(p["voltage"] * p["current"], 4)
        return p


def build_cells(cfg: dict) -> List[CellState]:
    h = cfg["hierarchy"]
    project_id = cfg.get("project_id", 0)
    cells = []
    for s in range(h["sites"]):
        for r in range(h["racks_per_site"]):
            for m in range(h["modules_per_rack"]):
                for c in range(h["cells_per_module"]):
                    cells.append(CellState(project_id, s, r, m, c))
    return cells


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def run(cfg: dict) -> None:
    cells    = build_cells(cfg)
    interval = cfg["rate"]["interval_seconds"]
    total    = len(cells)
    log.info("Generator started: %d cells, interval=%.1fs → %.0f msg/s",
             total, interval, total / interval)

    client = mqtt.Client(client_id="battery-generator")
    client.on_connect = lambda c, u, f, rc: log.info(
        "MQTT connected rc=%d", rc)
    client.on_disconnect = lambda c, u, rc: log.warning(
        "MQTT disconnected rc=%d", rc)

    mqtt_cfg = cfg["mqtt"]
    client.connect(mqtt_cfg["host"], mqtt_cfg["port"], keepalive=60)
    client.loop_start()

    published = 0
    t_start   = time.monotonic()

    try:
        while True:
            loop_start = time.monotonic()
            for cell in cells:
                payload = json.dumps(cell.payload())
                client.publish(cell.topic, payload, qos=1)
                published += 1

            elapsed = time.monotonic() - t_start
            if published % (total * 10) == 0:
                rate = published / elapsed if elapsed > 0 else 0
                log.info("Published %d messages  (%.0f msg/s)", published, rate)

            sleep_time = interval - (time.monotonic() - loop_start)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info("Shutting down — published %d messages total", published)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Battery MQTT data generator")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    run(load_config(args.config))
