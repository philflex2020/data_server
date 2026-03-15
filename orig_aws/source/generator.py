"""
Battery data generator — thin wrapper around the shared cell_generator core.

Publishes JSON-encoded cell measurements to MQTT topics:
  batteries/project=0/site=0/rack=1/module=2/cell=3

Payload (all projects share this base schema; extra fields added via cell_data):
  {
    "timestamp":   1700000000.0,   # unix seconds (float)
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
    "power":       -5.58          # derived: voltage * current
  }

Usage:
  pip install paho-mqtt pyyaml
  python generator.py
  python generator.py --config config.yaml
"""

import argparse
import json
import logging
import os
import sys
import time

import paho.mqtt.client as mqtt
import yaml

# Locate the shared generator module regardless of working directory
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "source", "generator"))
from cell_generator import DEFAULT_CELL_DATA, build_cells  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def run(cfg: dict) -> None:
    h          = cfg["hierarchy"]
    project_id = cfg.get("project_id", 0)
    cell_data  = cfg.get("cell_data", DEFAULT_CELL_DATA)
    interval   = cfg["rate"]["interval_seconds"]

    # Build all cells across all sites
    cells = []
    for s in range(h["sites"]):
        cells.extend(build_cells(
            project_id       = project_id,
            site_id          = s,
            racks            = h["racks_per_site"],
            modules_per_rack = h["modules_per_rack"],
            cells_per_module = h["cells_per_module"],
            cell_data        = cell_data,
        ))

    total = len(cells)
    log.info("Generator started: project=%d  %d cells  interval=%.1fs → %.0f msg/s",
             project_id, total, interval, total / interval)

    mqtt_cfg = cfg["mqtt"]
    client   = mqtt.Client(client_id=f"battery-generator-p{project_id}")
    client.on_connect    = lambda c, u, f, rc: log.info("MQTT connected rc=%d", rc)
    client.on_disconnect = lambda c, u, rc:    log.warning("MQTT disconnected rc=%d", rc)
    client.connect(mqtt_cfg["host"], mqtt_cfg["port"], keepalive=60)
    client.loop_start()

    published = 0
    t_start   = time.monotonic()

    try:
        while True:
            loop_start = time.monotonic()
            for cell in cells:
                client.publish(cell.topic, json.dumps(cell.payload()), qos=1)
                published += 1

            elapsed = time.monotonic() - t_start
            if published % (total * 10) == 0:
                log.info("Published %d messages  (%.0f msg/s)", published, published / elapsed)

            sleep_time = interval - (time.monotonic() - loop_start)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info("Shutting down — published %d messages total", published)
    finally:
        client.loop_stop()
        client.disconnect()


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Battery MQTT data generator")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    run(load_config(args.config))
