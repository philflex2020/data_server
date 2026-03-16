# Study 1 — Data Sources & Message Rates

**HTML version:** [`html/study_data_sources.html`](../html/study_data_sources.html)

---

## Overview and Deliverable

The battery telemetry pipeline is running and generating real data. The generator is built
around a config file (`generator/config.yaml`) that defines the topology and fields. The
task is to create config files for three additional asset types — **Solar**, **Wind**, and
**Grid** — so that a modified generator can load and simulate any source.

**Your deliverable:**

1. Produce one `config_<type>.yaml` file per asset type (Solar, Wind, Grid) plus a revised Battery config.
2. Each config must specify: fields, units, value ranges, MQTT topic pattern, sample interval, expected msg/s.
3. Include notes on realistic signal behaviour (solar ramps with daylight, wind is stochastic, grid near-constant).
4. Confirm the pipeline (NATS bridge → Parquet writer → Subscriber API) passes the new messages without schema changes.
5. Document the expected message rate for a typical deployment of each asset type.

---

## Current Battery Baseline

### Pipeline

```
Generator → FlashMQ (MQTT :1883) → NATS Bridge → NATS JetStream :4222
         → Parquet Writer → MinIO/S3 → Subscriber API (:8767 WS / :8768 HTTP)
```

### Generator (phil-dev) — per_cell mode

| Parameter       | Value                                  |
|----------------|----------------------------------------|
| Sites          | 2                                      |
| Racks/site     | 3                                      |
| Modules/rack   | 4                                      |
| Cells/module   | 6                                      |
| **Total cells**| **144**                                |
| **msgs/s**     | **144** (1 msg per cell per second)    |
| Payload size   | ~250 bytes JSON                        |
| Ingest         | ~35 KB/s · ~2.1 MB/min · ~126 MB/hr   |

MQTT topic: `batteries/site=0/rack=1/module=2/cell=3`
NATS subject: `batteries.site=0.rack=1.module=2.cell=3`
Mode: `per_cell` — nested `{"value": N, "unit": "X"}` objects, no `project_id` field.

### Stress Runner (spark-22b6) — flat mode

| Parameter       | Value                                    |
|----------------|------------------------------------------|
| Projects       | 1                                        |
| Sites          | 3                                        |
| Racks/site     | 4                                        |
| Modules/rack   | 8                                        |
| Cells/module   | 12                                       |
| **Total cells**| **1,152**                                |
| **msgs/s**     | **1,152** (1 msg per cell per second)    |
| Payload size   | ~220–280 bytes JSON                      |
| Ingest         | ~285 KB/s · ~24 MB/min · ~1.4 GB/hr     |

MQTT topic: `batteries/project=0/site=0/rack=1/module=2/cell=3`
Mode: `flat` — all fields at top level, includes `project_id`, `soh`, `resistance`, `capacity`, and computed `power`.

### Payload Formats

**Generator payload (per_cell mode):**

```json
{
  "timestamp": 1741234567.891,
  "site_id": 0,
  "rack_id": 1,
  "module_id": 2,
  "cell_id": 3,
  "voltage":     {"value": 3.714, "unit": "V"},
  "current":     {"value": -0.42, "unit": "A"},
  "temperature": {"value": 28.3,  "unit": "C"},
  "soc":         {"value": 71.2,  "unit": "percent"},
  "internal_resistance": {"value": 0.0118, "unit": "ohm"}
}
```

**Stress runner payload (flat mode):**

```json
{
  "timestamp": 1741234567.891,
  "project_id": 0,
  "site_id": 0,
  "rack_id": 1,
  "module_id": 2,
  "cell_id": 3,
  "voltage":     3.714,
  "current":    -0.42,
  "temperature": 28.3,
  "soc":         71.2,
  "soh":         97.8,
  "resistance":  0.0118,
  "capacity":    99.4,
  "power":       -1.560
}
```

`power` is computed as `voltage × current` in the generator.

### S3 Partition Layout

```
s3://battery-data/project={p}/site={s}/{year}/{month}/{day}/{YYYYMMDDTHHMMSSz}.parquet
```

- Flush interval: 60 seconds or 10,000 messages, whichever comes first
- Compression: Snappy
- At 1,152 msgs/s the 10,000 message cap triggers first (~every 8.7 seconds)

---

## Asset Type Specifications to Research and Design

### Solar

**Suggested MQTT topic:**
```
solar/project=N/site=N/array=N/panel=N
```

**Suggested payload fields:**

| Field         | Unit  | Typical Range     | Notes                              |
|--------------|-------|-------------------|------------------------------------|
| dc_voltage   | V     | 300–600           | PV string voltage — varies by config |
| dc_current   | A     | 0–10              | Per panel or string                |
| ac_power     | W     | 0–5,000           | After inverter                     |
| irradiance   | W/m²  | 0–1,200           | Global horizontal irradiance       |
| cell_temp    | C     | 15–75             | Panel cell temperature             |
| efficiency   | %     | 14–22             | Depends on panel technology        |
| energy_today | Wh    | 0–40,000          | Cumulative daily energy            |

**Open questions:**
- Are readings per-panel or per-inverter (one inverter serves multiple panels)?
- What is the real SCADA poll interval in field deployments: 1s, 5s, or 15s?
- How should nighttime/zero-irradiance periods be handled in the generator?
- Should `status_code` be included for fault states (shading, bypass, fault)?

**Typical msg rate estimate:** 500 panels × 1 msg/s = 500 msgs/s per site.
Verify actual granularity before coding.

---

### Wind

**Suggested MQTT topic:**
```
wind/project=N/site=N/turbine=N
```

**Suggested payload fields:**

| Field           | Unit   | Typical Range  | Notes                         |
|----------------|--------|----------------|-------------------------------|
| wind_speed     | m/s    | 0–25           | Anemometer reading at hub     |
| wind_direction | deg    | 0–359          | Meteorological convention     |
| rotor_rpm      | RPM    | 0–20           | Low-speed shaft, varies       |
| ac_power       | W      | 0–3,000,000    | Turbine rated output          |
| nacelle_temp   | C      | -10–60         | Internal nacelle temperature  |
| blade_pitch    | deg    | 0–90           | Collective pitch angle        |
| yaw_angle      | deg    | 0–359          | Nacelle orientation           |
| status_code    | int    | 0–255          | Operational state bitmask     |

**Open questions:**
- Is `blade_pitch` per-blade or collective (3-blade turbines have independent pitch)?
- Typical SCADA poll interval for wind: usually 10-minute averages in reporting, but 1s for control — what does the generator need?
- `status_code` encoding — define a bitmask or use string states?
- Should power curve data be included or derived externally?

**Typical msg rate estimate:** 50 turbines × 1 msg/s = 50 msgs/s per site.

---

### Grid

**Suggested MQTT topic:**
```
grid/project=N/site=N/meter=N/phase=N
```
or flat per-meter (all phases in one message):
```
grid/project=N/site=N/meter=N
```

**Suggested payload fields (per meter, all phases):**

| Field          | Unit  | Typical Range  | Notes                              |
|---------------|-------|----------------|------------------------------------|
| voltage_l1     | V     | 220–240        | Line-to-neutral                    |
| voltage_l2     | V     | 220–240        |                                    |
| voltage_l3     | V     | 220–240        |                                    |
| current_l1     | A     | 0–1,000        | Per phase                          |
| current_l2     | A     | 0–1,000        |                                    |
| current_l3     | A     | 0–1,000        |                                    |
| active_power   | W     | -500,000–500,000 | Negative = export                |
| reactive_power | VAr   | varies         |                                    |
| power_factor   | —     | -1.0–1.0       |                                    |
| frequency      | Hz    | 49.5–50.5      | Grid frequency                     |
| energy_import  | Wh    | 0–very large   | Cumulative                         |
| energy_export  | Wh    | 0–very large   | Cumulative                         |

**Open questions:**
- Smart meter poll interval: 1s for real-time control, 15-minute intervals for billing — which is needed?
- Single-phase vs three-phase meter distinction in the payload?
- Should `energy_import`/`energy_export` be cumulative counters or interval deltas?

**Typical msg rate estimate:** 6 meters × 1 msg/s = 6 msgs/s per site (much lower than battery/solar).

---

## Generator Extension Guide

### How cell_generator.py Works

`cell_generator.py` contains the shared core used by both the main generator and the stress
runner. It is responsible for:

1. Reading topology from `config.yaml`
2. Iterating over all devices (sites × racks × modules × cells for battery)
3. Generating randomized field values within configured ranges
4. Constructing the payload dict
5. Publishing to MQTT with the correct topic

To add a new asset type:

1. Add a new top-level section to `config.yaml` (e.g. `solar:`, `wind:`, `grid:`).
2. Add a new generator class or function in `cell_generator.py` following the battery pattern:
   - Define topology iteration (sites × arrays × panels for solar, etc.)
   - Define field generation with ranges from config
   - Construct topic string matching the new scheme
   - Return the payload dict
3. Add a new command-line flag or config key in `generator.py` to select the asset type.
4. The NATS bridge, Parquet writer, and Subscriber API should work unchanged as long as
   the payload is valid JSON and the MQTT topic follows `type/key=val/...` conventions.

### Config Keys to Add

```yaml
# New asset type follows this pattern:
<asset_type>:
  mode: per_<device>          # name the leaf device
  interval_ms: 1000           # publish interval
  topology:
    sites: N
    <level2>_per_site: N      # name the hierarchy levels
    <leaf>_per_<level2>: N
  ranges:
    <field>: [min, max]       # all numeric fields
```

---

## Data Rate Reference Table

| Asset Type     | Topology Example                      | Devices | msgs/s | KB/s  | MB/min | GB/hr |
|---------------|---------------------------------------|---------|--------|-------|--------|-------|
| Battery (gen) | 2 sites × 3 racks × 4 mod × 6 cells  | 144     | 144    | ~35   | ~2.1   | ~0.13 |
| Battery (stress) | 1 proj × 3 sites × 4 racks × 8 mod × 12 cells | 1,152 | 1,152 | ~285 | ~24 | ~1.4 |
| Solar (est.)  | 1 site × 10 arrays × 50 panels        | 500     | 500    | ~107  | ~6.4   | ~0.38 |
| Wind (est.)   | 1 site × 50 turbines                  | 50      | 50     | ~12   | ~0.7   | ~0.04 |
| Grid (est.)   | 1 site × 6 meters                     | 6       | 6      | ~1.6  | ~0.10  | ~0.006|

Note: "est." values are based on suggested topology. Verify against real deployment sizes.

---

## Sample Payloads for All Asset Types

### Battery (implemented)

See payload formats above.

### Solar (design target)

```json
{
  "timestamp": 1741234567.891,
  "project_id": 0,
  "site_id": 0,
  "array_id": 2,
  "panel_id": 14,
  "dc_voltage":   385.2,
  "dc_current":   8.4,
  "ac_power":     3150.0,
  "irradiance":   875.0,
  "cell_temp":    52.3,
  "efficiency":   18.7,
  "energy_today": 12400.0
}
```

### Wind (design target)

```json
{
  "timestamp": 1741234567.891,
  "project_id": 0,
  "site_id": 0,
  "turbine_id": 7,
  "wind_speed":     11.4,
  "wind_direction": 247,
  "rotor_rpm":      14.2,
  "ac_power":       1850000.0,
  "nacelle_temp":   32.1,
  "blade_pitch":    3.5,
  "yaw_angle":      245,
  "status_code":    1
}
```

### Grid (design target)

```json
{
  "timestamp": 1741234567.891,
  "project_id": 0,
  "site_id": 0,
  "meter_id": 1,
  "voltage_l1":    230.1,
  "voltage_l2":    229.8,
  "voltage_l3":    230.4,
  "current_l1":    142.5,
  "current_l2":    138.2,
  "current_l3":    145.0,
  "active_power":  97800.0,
  "reactive_power": 12300.0,
  "power_factor":  0.992,
  "frequency":     50.01,
  "energy_import": 1842700.0,
  "energy_export": 0.0
}
```

---

## Open Questions and Research Tasks

1. **Solar granularity:** Confirm whether real inverter systems expose per-panel data or only per-string/inverter. This determines the `devices` count and realistic msg/s.

2. **Wind poll interval:** Determine whether the generator should simulate the 10-minute SCADA average (standard for wind reporting) or 1-second control-loop readings.

3. **Grid meter types:** Decide whether to model single-phase and three-phase meters separately or always emit all three phases (with zeros for single-phase).

4. **NATS bridge passthrough:** Verify that the NATS bridge forwards non-battery topics. Check whether it has a topic filter or passes all `#` / `>` subjects.

5. **Parquet schema flexibility:** Confirm that the Parquet writer handles varying column sets across asset types, or whether it requires a fixed schema per topic prefix.

6. **Partition key for new types:** Decide the S3 partition path for solar/wind/grid. Recommend following `s3://<bucket>/project={p}/site={s}/{year}/{month}/{day}/` for consistency.

7. **Subscriber API query support:** Confirm whether `query_history` can filter by asset type when solar/wind/grid data lands in separate S3 prefixes.
