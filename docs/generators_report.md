# Generator Test Report
*2026-03-17 10:36 UTC — sample window 10s*

## Summary

| | |
|---|---|
| **Total MQTT rate** | 1392.1 msg/s |
| **Multi-generator assets** | 624 cells (battery, solar) |
| **Stress-runner assets** | 768 cells @ 768 msg/s |
| **Result** | 14 passed · 0 failed · 0 warnings |

## MQTT Sources

| Topic prefix | Rate | Messages | Format | Measurements |
|---|---|---|---|---|
| `batteries` | 912.1 msg/s | 9121 | nested | voltage, current, temperature, soc, soh |
| `solar` | 480.0 msg/s | 4800 | nested | irradiance, panel_temp, dc_voltage, dc_current, ac_power |

## Test Results

| | Test | Detail |
|---|---|---|
| ✅ | reachable |  |
| ✅ | running | running=True |
| ✅ | has assets | cell_count=624 |
| ✅ | active sources | battery, solar |
| ✅ | reachable |  |
| ✅ | active tasks | active_tasks=2 |
| ✅ | publishing | mps=768  total=124416 |
| ✅ | project topology | 1 project(s), 768 cells |
| ✅ | MQTT connect |  |
| ✅ |   batteries             912.1 msg/s  9121 total |  |
| ✅ |   solar                 480.0 msg/s  4800 total |  |
| ✅ |   batteries: nested (7 measurements) |  |
| ✅ |   solar: nested (6 measurements) |  |
| ✅ | data flowing on batteries | WS mps=768  MQTT batteries≈9121/s |
