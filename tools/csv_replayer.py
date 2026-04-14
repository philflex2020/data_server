"""
csv_replayer.py — replay a Longbow CSV recording to MQTT for testing real_writer.

Reads the CSV produced by the Longbow MQTT recorder and republishes each
message to a local (or remote) MQTT broker so that real_writer.cpp can
process it exactly as it would in production.

Topic rewriting
---------------
The recording uses:
    ems/site/{site_id}/unit/{unit_id}/{device}/{instance}/{point_name}/{dtype}

real_writer.cpp subscribes to:
    unit/#

By default the replayer strips the leading "ems/site/{site_id}/" prefix so
the topic arriving at the writer is:
    unit/{unit_id}/{device}/{instance}/{point_name}/{dtype}

Use --no-strip-prefix to publish the original topic verbatim (requires the
broker/writer to be configured for the ems/site/... namespace).

Replay modes
------------
  --mode realtime    Honour the recv_ts column and sleep between messages to
                     approximate the original cadence (default).
  --mode fast        Publish as fast as possible — no sleep.
  --mode rate N      Publish at a fixed rate of N messages/sec.

CSV column layout (no header row in the recording):
  0  topic
  1  payload (JSON string, possibly double-quoted)
  2  qos
  3  retained  (True/False)
  4  recv_ts   (Unix epoch float)
  5  latency

Usage:
    # Replay at original speed to localhost:1883
    python3 tools/csv_replayer.py --input /tmp/longbow_recording.csv.gz

    # Fast replay, only bms topics, 4 units
    python3 tools/csv_replayer.py --input /tmp/longbow_recording.csv.gz \\
        --mode fast --device bms \\
        --units 0215F562 0215F5A0 0215F5BF 0215F5CF

    # Stats only, no publish
    python3 tools/csv_replayer.py --input /tmp/longbow_recording.csv.gz --dry-run

    # Fixed 10k msgs/sec
    python3 tools/csv_replayer.py --input /tmp/longbow_recording.csv.gz --mode rate --rate 10000
"""

import argparse
import csv
import gzip
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------

def open_csv(path: str):
    p = Path(path)
    if p.suffix == ".gz":
        return gzip.open(path, "rt", newline="", encoding="utf-8", errors="replace")
    return open(path, newline="", encoding="utf-8", errors="replace")


def iter_rows(input_path: str,
              site_filter: Optional[str],
              device_filter: Optional[set],
              unit_filter: Optional[set]):
    """
    Yield (topic_out, payload_bytes, recv_ts) for each valid EMS row.

    topic_out has the "ems/site/{site_id}/" prefix stripped:
        ems/site/0215D1D8/unit/0215F562/bms/bms_1/Batt1_Current/float
    →   unit/0215F562/bms/bms_1/Batt1_Current/float
    """
    with open_csv(input_path) as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 5:
                continue
            topic   = row[0]
            payload = row[1]
            # recv_ts is column 4
            try:
                recv_ts = float(row[4])
            except (ValueError, IndexError):
                recv_ts = 0.0

            # Only handle ems/site/{site_id}/unit/... topics
            if not topic.startswith("ems/site/"):
                continue
            segs = topic.split("/")
            # ems/site/{site_id}/unit/{unit_id}/{device}/{instance}/{point}/{dtype}
            #  0   1      2        3     4         5        6          7      8
            if len(segs) != 9 or segs[3] != "unit":
                continue
            site_id = segs[2]
            if site_id == "SiteSN":
                continue
            if site_filter and site_id != site_filter:
                continue

            unit_id = segs[4]
            device  = segs[5]

            if unit_filter and unit_id not in unit_filter:
                continue
            if device_filter and device not in device_filter:
                continue
            if device == "root":
                continue

            # Strip prefix → unit/{unit_id}/...
            topic_out = "unit/" + "/".join(segs[4:])

            yield topic_out, payload.encode(), recv_ts


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------

def make_client(host: str, port: int, client_id: str) -> mqtt.Client:
    client = mqtt.Client(client_id=client_id)
    client.on_connect    = lambda c, u, f, rc: log.info("MQTT connected (rc=%d)", rc)
    client.on_disconnect = lambda c, u, rc:    log.warning("MQTT disconnected (rc=%d)", rc)
    delay = 1.0
    while True:
        try:
            client.connect(host, port, keepalive=60)
            client.loop_start()
            log.info("Connected to %s:%d", host, port)
            return client
        except Exception as exc:
            log.warning("Connect failed (%s) — retry in %.0fs", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


# ---------------------------------------------------------------------------
# Replay loop
# ---------------------------------------------------------------------------

def replay(args) -> None:
    unit_filter  = set(args.units)  if args.units  else None
    device_filter = set(args.device) if args.device else None

    # Dry run: just count and exit
    if args.dry_run:
        count = 0
        first_ts = last_ts = 0.0
        for topic, _payload, recv_ts in iter_rows(args.input, args.site_id or None,
                                                   device_filter, unit_filter):
            count += 1
            if count == 1:
                first_ts = recv_ts
            last_ts = recv_ts
        span = last_ts - first_ts
        log.info("Dry run: %d messages over %.1f s (%.0f msg/s avg)",
                 count, span, count / max(span, 0.001))
        return

    client = make_client(args.host, args.port, args.client_id)
    time.sleep(0.3)  # wait for CONNACK

    mode         = args.mode
    target_rate  = args.rate if mode == "rate" else 0
    rate_bucket  = 1.0       # for token-bucket pacing

    first_recv_ts = 0.0
    first_wall    = 0.0
    published     = 0
    dropped       = 0
    last_log_ts   = time.monotonic()
    last_log_pub  = 0

    log.info("Starting replay: mode=%s input=%s", mode, args.input)

    for topic, payload, recv_ts in iter_rows(args.input, args.site_id or None,
                                              device_filter, unit_filter):
        # Drop guard — don't overwhelm paho's outbound queue
        if len(client._out_packet) > 20000:
            dropped += 1
            continue

        if mode == "realtime":
            if first_recv_ts == 0.0:
                first_recv_ts = recv_ts
                first_wall    = time.monotonic()
            target_wall = first_wall + (recv_ts - first_recv_ts)
            now = time.monotonic()
            if target_wall > now:
                time.sleep(target_wall - now)

        elif mode == "rate":
            # Simple token bucket
            rate_bucket -= 1
            if rate_bucket <= 0:
                time.sleep(1.0 / max(target_rate, 1))
                rate_bucket = 1.0

        client.publish(topic, payload, qos=0)
        published += 1

        now = time.monotonic()
        if now - last_log_ts >= 5.0:
            elapsed  = now - last_log_ts
            interval = published - last_log_pub
            log.info("Published %d total | %.0f msg/s | dropped %d",
                     published, interval / elapsed, dropped)
            last_log_ts  = now
            last_log_pub = published

    log.info("Replay complete: %d published, %d dropped", published, dropped)
    client.loop_stop()
    client.disconnect()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay Longbow CSV recording to MQTT for real_writer testing"
    )
    parser.add_argument("--input",      required=True,
                        help="Path to CSV or CSV.GZ recording")
    parser.add_argument("--host",       default="localhost")
    parser.add_argument("--port",       type=int, default=1883)
    parser.add_argument("--client-id",  default="csv-replayer")
    parser.add_argument("--site-id",    default="",
                        help="Filter to this site ID (default: all non-SiteSN sites)")
    parser.add_argument("--units",      nargs="+", metavar="UNIT_ID",
                        help="Only replay messages from these unit IDs")
    parser.add_argument("--device",     nargs="+", metavar="DEVICE",
                        help="Only replay messages for these devices (bms, pcs, rack)")
    parser.add_argument("--mode",       choices=["realtime", "fast", "rate"],
                        default="realtime",
                        help="Replay speed mode (default: realtime)")
    parser.add_argument("--rate",       type=float, default=10000,
                        help="Target msg/sec for --mode rate (default: 10000)")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Count messages and print stats without publishing")
    args = parser.parse_args()
    replay(args)


if __name__ == "__main__":
    main()
