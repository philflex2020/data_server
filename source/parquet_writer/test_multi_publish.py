#!/usr/bin/env python3
"""
test_multi_publish.py — test dynamic topic addition to a running writer

Publishes signals in three phases with a gap between each, demonstrating
that the writer picks up new topics at any time without restart:

  Phase 1: BMS signals only
  Phase 2: PCS signals added mid-stream
  Phase 3: A third device (meter) added mid-stream

After each phase, queries /health to show seen_topics growing.

Usage:
  python3 test_multi_publish.py [mqtt_host] [health_host] [unit_id]
"""
import sys, json, time, datetime, urllib.request, urllib.error
import paho.mqtt.client as mqtt

MQTT_HOST   = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
HEALTH_HOST = sys.argv[2] if len(sys.argv) > 2 else MQTT_HOST
UNIT        = sys.argv[3] if len(sys.argv) > 3 else '0215D1D8'
HEALTH_URL  = f'http://{HEALTH_HOST}:8771/health'

PHASES = [
    {
        'name': 'Phase 1 — BMS only',
        'signals': [
            ('bms', 'bms_1', 'SysSOC',     88.0),
            ('bms', 'bms_1', 'SysSOH',     99.5),
            ('bms', 'bms_1', 'SysVoltage', 752.3),
        ],
    },
    {
        'name': 'Phase 2 — PCS added',
        'signals': [
            ('bms', 'bms_1', 'SysSOC',     88.0),
            ('bms', 'bms_1', 'SysSOH',     99.5),
            ('bms', 'bms_1', 'SysVoltage', 752.3),
            ('pcs', 'pcs_1', 'kW',          45.2),   # new
            ('pcs', 'pcs_1', 'kVar',        12.1),   # new
            ('pcs', 'pcs_1', 'GridFreq',    50.01),  # new
        ],
    },
    {
        'name': 'Phase 3 — meter device added',
        'signals': [
            ('bms', 'bms_1', 'SysSOC',      88.0),
            ('bms', 'bms_1', 'SysSOH',      99.5),
            ('bms', 'bms_1', 'SysVoltage',  752.3),
            ('pcs', 'pcs_1', 'kW',           45.2),
            ('pcs', 'pcs_1', 'kVar',         12.1),
            ('pcs', 'pcs_1', 'GridFreq',     50.01),
            ('meter', 'meter_1', 'kWh_import', 1234.5),  # new device type
            ('meter', 'meter_1', 'kWh_export',  456.7),  # new
        ],
    },
]

SWEEPS_PER_PHASE = 8
SWEEP_INTERVAL   = 0.5   # seconds between sweeps within a phase
PHASE_GAP        = 2.0   # seconds between phases


def ts_now():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond//1000:03d}Z'


def get_seen_topics():
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=3) as r:
            return json.loads(r.read()).get('seen_topics', [])
    except urllib.error.URLError:
        return None


def publish_phase(client, phase, sweep_offset):
    signals = phase['signals']
    for i in range(SWEEPS_PER_PHASE):
        ts = ts_now()
        for dev_type, dev, point, base in signals:
            topic   = f'bench/{UNIT}/{dev_type}/{dev}/{point}/float'
            payload = json.dumps({'ts': ts, 'value': round(base + (sweep_offset + i) * 0.1, 2)})
            client.publish(topic, payload)
        time.sleep(SWEEP_INTERVAL)
    return sweep_offset + SWEEPS_PER_PHASE


# Connect
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_HOST, 1883)
client.loop_start()
print(f'connected to MQTT {MQTT_HOST}:1883  health -> {HEALTH_URL}\n')

topics_before = get_seen_topics() or []
sweep_offset  = 0

for phase in PHASES:
    print(f'--- {phase["name"]} ({len(phase["signals"])} signals, {SWEEPS_PER_PHASE} sweeps) ---')
    sweep_offset = publish_phase(client, phase, sweep_offset)

    topics_after = get_seen_topics()
    if topics_after is None:
        print('  health endpoint unreachable')
    else:
        new = [t for t in topics_after if t not in topics_before]
        print(f'  seen_topics total : {len(topics_after)}')
        if new:
            print(f'  newly seen ({len(new)}):')
            for t in new:
                print(f'    + {t}')
        topics_before = topics_after

    print()
    if phase is not PHASES[-1]:
        time.sleep(PHASE_GAP)

client.loop_stop()
total = sum(len(p["signals"]) for p in PHASES) * SWEEPS_PER_PHASE
print(f'done — {total} messages published across {len(PHASES)} phases')
