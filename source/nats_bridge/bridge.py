"""
MQTT → NATS JetStream bridge.

Subscribes to FlashMQ via MQTT and publishes every message into the
BATTERY_DATA JetStream stream.  This is the single point where data
enters the durable pipeline — everything downstream (Parquet Writer,
Subscriber API) consumes from NATS, not from MQTT directly.

Topic conversion:
  MQTT:  batteries/site=0/rack=1/module=2/cell=3
  NATS:  batteries.site=0.rack=1.module=2.cell=3

Usage:
  pip install aiomqtt nats-py pyyaml
  python bridge.py
  python bridge.py --config config.yaml
"""

import argparse
import asyncio
import logging
import yaml

import aiomqtt
import nats
from nats.js.api import RetentionPolicy, StorageType, StreamConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def mqtt_topic_to_nats_subject(topic: str) -> str:
    """Replace MQTT '/' separators with NATS '.' separators."""
    return topic.replace("/", ".")


async def ensure_stream(js, cfg: dict) -> None:
    """Create the JetStream stream if it doesn't exist yet."""
    stream_name = cfg["nats"]["stream_name"]
    subjects    = cfg["nats"]["stream_subjects"]
    max_age_hours = int(cfg["nats"].get("max_age_hours", 48))

    max_bytes = int(cfg["nats"].get("max_bytes", 8 * 1024 ** 3))  # default 8 GB

    try:
        await js.stream_info(stream_name)
        log.info("Stream %s already exists — skipping update", stream_name)
    except nats.js.errors.NotFoundError:
        await js.add_stream(StreamConfig(
            name      = stream_name,
            subjects  = subjects,
            storage   = StorageType.FILE,
            retention = RetentionPolicy.LIMITS,
            max_bytes = max_bytes,
        ))
        log.info("Created stream %s  max_bytes=%dGB  subjects=%s",
                 stream_name, max_bytes // 1024 ** 3, subjects)


async def run(cfg: dict) -> None:
    nats_url  = cfg["nats"]["url"]
    mqtt_cfg  = cfg["mqtt"]
    reconnect_delay = 2

    while True:
        try:
            # Connect to NATS
            nc = await nats.connect(nats_url)
            js = nc.jetstream()
            await ensure_stream(js, cfg)
            log.info("Connected to NATS at %s", nats_url)

            published = 0

            async with aiomqtt.Client(
                hostname = mqtt_cfg["host"],
                port     = mqtt_cfg["port"],
                username = mqtt_cfg.get("username") or None,
                password = mqtt_cfg.get("password") or None,
                identifier = mqtt_cfg.get("client_id", "nats-bridge"),
            ) as mqtt_client:
                await mqtt_client.subscribe(mqtt_cfg["topic"], qos=mqtt_cfg.get("qos", 1))
                log.info("Subscribed to MQTT topic %s on %s:%s",
                         mqtt_cfg["topic"], mqtt_cfg["host"], mqtt_cfg["port"])

                async for message in mqtt_client.messages:
                    subject = mqtt_topic_to_nats_subject(str(message.topic))
                    await js.publish(subject, message.payload)
                    published += 1
                    if published % 1000 == 0:
                        log.info("Bridged %d messages", published)

        except aiomqtt.MqttError as exc:
            log.warning("MQTT error: %s — reconnecting in %ds", exc, reconnect_delay)
        except nats.errors.Error as exc:
            log.warning("NATS error: %s — reconnecting in %ds", exc, reconnect_delay)
        except Exception as exc:
            log.error("Unexpected error: %s — reconnecting in %ds", exc, reconnect_delay)
        finally:
            try:
                await nc.drain()
            except Exception:
                pass

        await asyncio.sleep(reconnect_delay)


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT → NATS JetStream bridge")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    try:
        asyncio.run(run(load_config(args.config)))
    except KeyboardInterrupt:
        log.info("Shutting down")
