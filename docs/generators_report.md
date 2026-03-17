# Generator Test Report
*2026-03-17 10:42 UTC — sample window 10s*

## Summary

| | |
|---|---|
| **MQTT broker** | `localhost:1884` (Mosquitto) |
| **Total MQTT rate** | 1392.1 msg/s |
| **Multi-generator assets** | 624 cells (battery, solar) |
| **Stress-runner assets** | 768 cells @ 767 msg/s |
| **Result** | 14 passed · 0 failed · 0 warnings |

## MQTT Sources

| Topic prefix | Rate | Messages | Format | Measurements |
|---|---|---|---|---|
| `batteries` | 912.1 msg/s | 9121 | nested | voltage, current, temperature, soc, soh |
| `solar` | 480.0 msg/s | 4800 | nested | irradiance, panel_temp, dc_voltage, dc_current, ac_power |

## Sample Topics & Payloads

### `batteries` — example topics
  - `batteries/battery/site=0/rack=0/module=0/cell=0`
  - `batteries/battery/site=0/rack=0/module=0/cell=1`
  - `batteries/battery/site=0/rack=0/module=0/cell=2`

**Sample payload:**
```json
{
  "timestamp": 1773744144.2223377,
  "source_id": "battery",
  "site_id": 0,
  "rack_id": 0,
  "module_id": 0,
  "cell_id": 0,
  "voltage": {
    "value": 3.627,
    "unit": "V"
  },
  "current": {
    "value": 6.2766,
    "unit": "A"
  },
  "temperature": {
    "value": 24.1518,
    "unit": "C"
  },
  "soc": {
    "value": 84.5839,
    "unit": "%"
  },
  "soh": {
    "value": 98.2054,
    "unit": "%"
  },
  "resistance": {
    "value": 0.0024,
    "unit": "ohm"
  },
  "capacity": {
    "value": 100.0333,
    "unit": "Ah"
  }
}
```

### `solar` — example topics
  - `solar/solar/site=0/rack=0/module=0/cell=0`
  - `solar/solar/site=0/rack=0/module=0/cell=1`
  - `solar/solar/site=0/rack=0/module=0/cell=2`

**Sample payload:**
```json
{
  "timestamp": 1773744144.225893,
  "source_id": "solar",
  "site_id": 0,
  "rack_id": 0,
  "module_id": 0,
  "cell_id": 0,
  "irradiance": {
    "value": 616.6283,
    "unit": "W/m2"
  },
  "panel_temp": {
    "value": 53.0881,
    "unit": "C"
  },
  "dc_voltage": {
    "value": 631.4051,
    "unit": "V"
  },
  "dc_current": {
    "value": 8.4363,
    "unit": "A"
  },
  "ac_power": {
    "value": 184.4936,
    "unit": "W"
  },
  "efficiency": {
    "value": 21.7815,
    "unit": "%"
  }
}
```

## Test Results

| | Test | Detail |
|---|---|---|
| ✅ | reachable |  |
| ✅ | running | running=True |
| ✅ | has assets | cell_count=624 |
| ✅ | active sources | battery, solar |
| ✅ | reachable |  |
| ✅ | active tasks | active_tasks=2 |
| ✅ | publishing | mps=767  total=420864 |
| ✅ | project topology | 1 project(s), 768 cells |
| ✅ | MQTT connect | broker localhost:1884 |
| ✅ |   batteries             912.1 msg/s  9121 total |  |
| ✅ |   solar                 480.0 msg/s  4800 total |  |
| ✅ |   batteries: nested (7 measurements) |  |
| ✅ |   solar: nested (6 measurements) |  |
| ✅ | data flowing on batteries | WS mps=767  MQTT batteries≈9121/s |
