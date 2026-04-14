import paho.mqtt.client as mqtt
import json, time, datetime

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect('localhost', 1883)
client.loop_start()

SIGNALS = [
    ('bms', 'bms_1', 'SysSOC',     88.0),
    ('bms', 'bms_1', 'SysSOH',     99.5),
    ('bms', 'bms_1', 'SysVoltage', 752.3),
    ('pcs', 'pcs_1', 'kW',         45.2),
    ('pcs', 'pcs_1', 'kVar',       12.1),
]
UNIT = '0215D1D8'

for i in range(10):
    ts = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.') + \
         f'{datetime.datetime.utcnow().microsecond//1000:03d}Z'
    for dev_type, dev, point, base in SIGNALS:
        topic   = f'bench/{UNIT}/{dev_type}/{dev}/{point}/float'
        payload = json.dumps({'ts': ts, 'value': round(base + i * 0.1, 2)})
        client.publish(topic, payload)
    time.sleep(0.5)

client.loop_stop()
print(f'published {10*len(SIGNALS)} messages')
