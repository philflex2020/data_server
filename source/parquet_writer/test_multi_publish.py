#!/usr/bin/env python3
"""
test_multi_publish.py — test dynamic topic addition to a running writer

Publishes signals in three phases with a gap between each, demonstrating
that the writer picks up new topics at any time without restart:

  bench format (default):
    Phase 1: BMS signals only
    Phase 2: PCS signals added mid-stream
    Phase 3: A third device (meter) added mid-stream

  fractal format (--format fractal):
    Phase 1: Unit BMS signals (9-seg)
    Phase 2: Unit PCS + unit-root signal (9-seg + 8-seg)
    Phase 3: Site-device meter (7-seg) + site-root signal (6-seg)

After each phase, queries /health to show seen_topics growing.

Usage:
  python3 test_multi_publish.py [mqtt_host] [health_host] [unit_id]
  python3 test_multi_publish.py [mqtt_host] [health_host] [unit_id] --format fractal
"""
import sys, json, time, datetime, urllib.request, urllib.error
import paho.mqtt.client as mqtt

MQTT_HOST   = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
HEALTH_HOST = sys.argv[2] if len(sys.argv) > 2 else MQTT_HOST
UNIT        = sys.argv[3] if len(sys.argv) > 3 else '0215D1D8'
SITE        = 'SITE_A'
HEALTH_URL  = f'http://{HEALTH_HOST}:8771/health'
FMT         = 'fractal' if '--format' in sys.argv and sys.argv[sys.argv.index('--format') + 1] == 'fractal' else 'bench'

# ── bench phases ───────────────────────────────────────────────────────────────
# Each signal: (device_type, device, point_name, base_value)
# Topic: bench/{UNIT}/{device_type}/{device}/{point_name}/float

BENCH_PHASES = [
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

# ── fractal phases ─────────────────────────────────────────────────────────────
# Signals are tuples of (topic_template, base_value) where template uses
# {site}, {unit}, {device}, {instance}, {point}, {dtype} placeholders.
#
# Pattern coverage per phase:
#   Phase 1: 9-seg unit device signals
#   Phase 2: 9-seg + 8-seg (unit root, no instance)
#   Phase 3: + 7-seg (site device, no unit) + 6-seg (site root)

FRACTAL_PHASES = [
    {
        'name': 'Phase 1 — BMS unit signals (9-seg)',
        'signals': [
            # 9-seg: ems/site/{site}/unit/{unit}/{device}/{instance}/{point}/{dtype}
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysSOC/float',     88.0),
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysSOH/float',     99.5),
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysVoltage/float', 752.3),
        ],
    },
    {
        'name': 'Phase 2 — PCS added (9-seg) + unit root signal (8-seg)',
        'signals': [
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysSOC/float',     88.0),
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysSOH/float',     99.5),
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysVoltage/float', 752.3),
            (f'ems/site/{SITE}/unit/{UNIT}/pcs/pcs_1/kW/float',          45.2),   # new 9-seg
            (f'ems/site/{SITE}/unit/{UNIT}/pcs/pcs_1/kVar/float',        12.1),   # new 9-seg
            (f'ems/site/{SITE}/unit/{UNIT}/pcs/pcs_1/GridFreq/float',    50.01),  # new 9-seg
            # 8-seg: ems/site/{site}/unit/{unit}/root/{point}/{dtype}
            (f'ems/site/{SITE}/unit/{UNIT}/root/SystemMode/string',       None),  # new 8-seg (string)
        ],
    },
    {
        'name': 'Phase 3 — site meter (7-seg) + site root signal (6-seg)',
        'signals': [
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysSOC/float',     88.0),
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysSOH/float',     99.5),
            (f'ems/site/{SITE}/unit/{UNIT}/bms/bms_1/SysVoltage/float', 752.3),
            (f'ems/site/{SITE}/unit/{UNIT}/pcs/pcs_1/kW/float',          45.2),
            (f'ems/site/{SITE}/unit/{UNIT}/pcs/pcs_1/kVar/float',        12.1),
            (f'ems/site/{SITE}/unit/{UNIT}/pcs/pcs_1/GridFreq/float',    50.01),
            (f'ems/site/{SITE}/unit/{UNIT}/root/SystemMode/string',       None),
            # 7-seg: ems/site/{site}/{device}/{instance}/{point}/{dtype}
            (f'ems/site/{SITE}/meter/meter_1/kWh_import/float', 1234.5),  # new 7-seg
            (f'ems/site/{SITE}/meter/meter_1/kWh_export/float',  456.7),  # new 7-seg
            # 6-seg: ems/site/{site}/root/{point}/{dtype}
            (f'ems/site/{SITE}/root/SiteAlarm/string',            None),  # new 6-seg
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


def publish_bench_phase(client, phase, sweep_offset):
    for i in range(SWEEPS_PER_PHASE):
        ts = ts_now()
        for dev_type, dev, point, base in phase['signals']:
            topic   = f'bench/{UNIT}/{dev_type}/{dev}/{point}/float'
            payload = json.dumps({'ts': ts, 'value': round(base + (sweep_offset + i) * 0.1, 2)})
            client.publish(topic, payload)
        time.sleep(SWEEP_INTERVAL)
    return sweep_offset + SWEEPS_PER_PHASE


def publish_fractal_phase(client, phase, sweep_offset):
    for i in range(SWEEPS_PER_PHASE):
        ts = ts_now()
        for topic, base in phase['signals']:
            if base is None:
                # string payload
                payload = json.dumps({'ts': ts, 'value': 'ok'})
            else:
                payload = json.dumps({'ts': ts, 'value': round(base + (sweep_offset + i) * 0.1, 2)})
            client.publish(topic, payload)
        time.sleep(SWEEP_INTERVAL)
    return sweep_offset + SWEEPS_PER_PHASE


# Connect
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_HOST, 1883)
client.loop_start()
print(f'connected to MQTT {MQTT_HOST}:1883  health -> {HEALTH_URL}')
print(f'format: {FMT}\n')

phases       = FRACTAL_PHASES if FMT == 'fractal' else BENCH_PHASES
publish_fn   = publish_fractal_phase if FMT == 'fractal' else publish_bench_phase

topics_before = get_seen_topics() or []
sweep_offset  = 0

for phase in phases:
    sig_count = len(phase['signals'])
    print(f'--- {phase["name"]} ({sig_count} signals, {SWEEPS_PER_PHASE} sweeps) ---')
    sweep_offset = publish_fn(client, phase, sweep_offset)

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
    if phase is not phases[-1]:
        time.sleep(PHASE_GAP)

client.loop_stop()
total = sum(len(p['signals']) for p in phases) * SWEEPS_PER_PHASE
print(f'done — {total} messages published across {len(phases)} phases')
