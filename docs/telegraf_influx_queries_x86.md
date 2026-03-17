# InfluxDB Query Performance — Simulated Web Client (x86_64)

**Test date:** 2026-03-17 11:41 UTC
**Host:** localhost  |  **Bucket:** battery-data
**Duration:** 60s  |  **Interval:** 10s  |  **Cycles:** 6

Queries simulate a web dashboard making requests to InfluxDB every 10 seconds.
All queries target the last 5 minutes of data (1-minute aggregates where noted).

---

## Query Performance Summary

| Query | Title | Avg latency | Min | Max | Avg rows | Errors |
|-------|-------|------------:|----:|----:|---------:|-------:|
| Q1 | Latest snapshot — all battery cells, site 0 (last 5 min… | 7.8 ms | 6.7 ms | 8.7 ms | 72.0 | 0 |
| Q2 | Time series — single cell history (last 5 min)… | 2.1 ms | 1.5 ms | 3.2 ms | 299.2 | 0 |
| Q3 | Rack aggregates — mean voltage & temperature per rack (… | 86.9 ms | 76.3 ms | 106.8 ms | 36.0 | 0 |
| Q4 | Alert scan — cells with voltage outside 3.2–4.1 V (last… | 55.5 ms | 50.1 ms | 65.0 ms | 144.0 | 0 |
| Q5 | Solar overview — last irradiance & AC power, all panels… | 235.6 ms | 211.1 ms | 268.1 ms | 480.0 | 0 |
| Q6 | Fleet health — active cell count & data freshness (last… | 27.6 ms | 25.3 ms | 30.7 ms | 2.0 | 0 |

---

## Query Cost Impact on AWS

Based on ~0.6 MB scanned per cycle (1033 rows × 15× scan multiplier).  One web client querying every 10s.

| Metric | 1× test load | 6× scale | 24× (full fleet) |
|--------|-------------:|---------:|------------------:|
| Rows returned / cycle | 1,033 | 6,199 | 24,797 |
| MB scanned / cycle | 0.6 | 3.5 | 14.2 |
| GB scanned / month (1 client) | 150 | 898 | 3592 |
| AWS Timestream query cost/mo | **$1.50** | **$8.98** | **$35.92** |
| InfluxDB Cloud query cost/mo | **$1.20** | **$7.18** | **$28.73** |
| Heavy query CPU (Q3+Q5) | 322 ms | — | — |

> **Per additional web client:** multiply query costs by client count.
> At 80 sites (24×) with 5 concurrent clients, Timestream query costs reach **~$180/month** on top of the write charges.
> Q3 (rack aggregates, 87 ms) and Q5 (solar overview, 236 ms) dominate — GROUP BY over large time windows. Use pre-aggregated continuous queries to reduce scan cost.

---

## Q1 — Latest snapshot — all battery cells, site 0 (last 5 min)

*Web dashboard: 'current status' panel — last reading per cell.*

```sql
SELECT last(voltage), last(current), last(temperature), last(soc), last(soh) FROM battery_cell WHERE time > now()-5m AND site_id='0' AND source_id='battery' GROUP BY site_id, rack_id, module_id, cell_id
```

**Avg latency:** 7.8 ms  |  **Rows returned:** ~72.0  |  **Cycles run:** 6

**Sample output (last cycle, first 5 rows):**

```
[cell_id=0 module_id=0 rack_id=0 site_id=0] t=1773747384  last=3.4546  last_1=20.8343  last_2=26.9923  last_3=85.0773  last_4=97.6730
[cell_id=0 module_id=0 rack_id=1 site_id=0] t=1773747384  last=3.4714  last_1=33.5195  last_2=23.4183  last_3=94.9455  last_4=97.7074
[cell_id=0 module_id=0 rack_id=2 site_id=0] t=1773747384  last=3.6589  last_1=19.7608  last_2=25.1748  last_3=77.8936  last_4=94.5093
[cell_id=0 module_id=1 rack_id=0 site_id=0] t=1773747384  last=3.6200  last_1=17.3462  last_2=28.2446  last_3=81.8885  last_4=96.6510
[cell_id=0 module_id=1 rack_id=1 site_id=0] t=1773747384  last=3.5602  last_1=-17.1313  last_2=33.4724  last_3=70.3714  last_4=98.3975
```

---

## Q2 — Time series — single cell history (last 5 min)

*Web dashboard: 'cell detail' chart — voltage/temp trend.*

```sql
SELECT voltage, current, temperature, soc FROM battery_cell WHERE time > now()-5m   AND site_id='0' AND rack_id='0' AND module_id='0' AND cell_id='0' ORDER BY time DESC LIMIT 300
```

**Avg latency:** 2.1 ms  |  **Rows returned:** ~299.2  |  **Cycles run:** 6

**Sample output (last cycle, first 5 rows):**

```
t=1773747683  voltage=3.4546  current=20.8343  temperature=26.9923  soc=85.0773
t=1773747682  voltage=3.4646  current=20.4470  temperature=27.2097  soc=84.3786
t=1773747681  voltage=3.4648  current=19.7049  temperature=27.3218  soc=83.3798
t=1773747680  voltage=3.4754  current=20.5134  temperature=27.4210  soc=83.7420
t=1773747679  voltage=3.4700  current=19.8187  temperature=27.1905  soc=83.3815
```

---

## Q3 — Rack aggregates — mean voltage & temperature per rack (last 5 min, 1-min buckets)

*Web dashboard: 'rack health' heatmap — averaged per 1-minute window.*

```sql
SELECT mean(voltage), mean(temperature), mean(soc) FROM battery_cell WHERE time > now()-5m AND source_id='battery' GROUP BY site_id, rack_id, time(1m) FILL(none)
```

**Avg latency:** 86.9 ms  |  **Rows returned:** ~36.0  |  **Cycles run:** 6

**Sample output (last cycle, first 5 rows):**

```
[rack_id=0 site_id=0] t=1773747360  mean=3.6825  mean_1=24.8658  mean_2=80.4075
[rack_id=0 site_id=0] t=1773747420  mean=3.6931  mean_1=24.7820  mean_2=79.3090
[rack_id=0 site_id=0] t=1773747480  mean=3.6838  mean_1=24.8233  mean_2=77.8818
[rack_id=0 site_id=0] t=1773747540  mean=3.6848  mean_1=24.9550  mean_2=76.4305
[rack_id=0 site_id=0] t=1773747600  mean=3.6973  mean_1=25.0389  mean_2=75.3738
```

---

## Q4 — Alert scan — cells with voltage outside 3.2–4.1 V (last 5 min)

*Web dashboard: alert panel — any cell outside safe voltage window.*

```sql
SELECT last(voltage) FROM battery_cell WHERE time > now()-5m AND source_id='battery' GROUP BY site_id, rack_id, module_id, cell_id
```

**Avg latency:** 55.5 ms  |  **Rows returned:** ~144.0  |  **Cycles run:** 6

**Sample output (last cycle, first 5 rows):**

```
[cell_id=0 module_id=0 rack_id=0 site_id=0] t=1773747683  last=3.4546
[cell_id=0 module_id=0 rack_id=0 site_id=1] t=1773747683  last=3.4185
[cell_id=0 module_id=0 rack_id=1 site_id=0] t=1773747683  last=3.4714
[cell_id=0 module_id=0 rack_id=1 site_id=1] t=1773747683  last=3.6160
[cell_id=0 module_id=0 rack_id=2 site_id=0] t=1773747683  last=3.6589
```

> After filter (filtered to voltage < 3.2 V or > 4.1 V): **6 rows**

---

## Q5 — Solar overview — last irradiance & AC power, all panels (last 5 min)

*Web dashboard: 'solar fleet' overview panel.*

```sql
SELECT last(irradiance), last(ac_power), last(efficiency) FROM battery_cell WHERE time > now()-5m AND source_id='solar' GROUP BY site_id, rack_id, module_id, cell_id
```

**Avg latency:** 235.6 ms  |  **Rows returned:** ~480.0  |  **Cycles run:** 6

**Sample output (last cycle, first 5 rows):**

```
[cell_id=0 module_id=0 rack_id=0 site_id=0] t=1773747384  last=749.6363  last_1=176.9199  last_2=24.6550
[cell_id=0 module_id=0 rack_id=0 site_id=1] t=1773747384  last=794.6028  last_1=165.9921  last_2=17.2589
[cell_id=0 module_id=0 rack_id=1 site_id=0] t=1773747384  last=985.4084  last_1=157.4547  last_2=19.8388
[cell_id=0 module_id=0 rack_id=1 site_id=1] t=1773747384  last=562.4818  last_1=157.7562  last_2=24.6921
[cell_id=0 module_id=0 rack_id=2 site_id=0] t=1773747384  last=767.8668  last_1=233.3280  last_2=22.8760
```

---

## Q6 — Fleet health — active cell count & data freshness (last 1 min)

*Web dashboard: 'system health' badge — confirm data is flowing.*

```sql
SELECT count(voltage) FROM battery_cell WHERE time > now()-1m AND source_id='battery' GROUP BY site_id
```

**Avg latency:** 27.6 ms  |  **Rows returned:** ~2.0  |  **Cycles run:** 6

**Sample output (last cycle, first 5 rows):**

```
[site_id=0] t=1773747624  count=4248
[site_id=1] t=1773747624  count=4248
```

---
