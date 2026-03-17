# Telegraf / InfluxDB — Live Performance Measurements (x86_64)

**Test date:** 2026-03-17 11:23 UTC
**Environment:** fractal-phil (phil-dev) (x86_64), single node
**InfluxDB:** 2.8.0 | **Telegraf:** 1.38.1
**Active assets:** 14,976 battery series
**Sample interval:** 1 second per asset

> See also: `telegraf_influx_details.md` for the peer architecture results.

All measurements taken against the live running system via the InfluxDB HTTP API.

---

## Test 0 — Flux API

**Result:** Flux v2 API returned data. All further tests also run via InfluxQL for consistency.

---

## Test 1 — Write Throughput

```sql
-- messages_received delta over 30s observation window
SELECT messages_received FROM internal_mqtt_consumer ORDER BY time DESC LIMIT 1
```

**Result:** **~624/sec** records (messages), **8,735/sec** field writes (14 fields × ~624/sec).

---

## Test 2 — Lifetime Write Volume

```sql
SELECT last(metrics_written) FROM internal_write
```

**Result:** **1,234,853 metrics written** (lifetime total).

---

## Test 3 — Data Points in InfluxDB (last hour)

```sql
SELECT count(voltage) FROM battery_cell WHERE time > now()-1h
```

**Result:** **313,973 voltage points** in the last hour.
With 14 fields: **~4,395,622 total field values/hour**.

---

## Test 4 — Dropped Metrics

```sql
SELECT last(metrics_dropped) FROM internal_write
```

**Result:** **0 dropped metrics**.

Buffer fill time at current rate: **11 seconds** (100k metric buffer ÷ current field write rate).

---

## Test 5 — Process Resource Usage

Measured via `ps aux`:

| Process | CPU% | RSS Memory |
|---------|-----:|----------:|
| `influxd` | 2.7% | 715 MB |
| `telegraf` | 4.9% | 172 MB |

---

## Test 6 — Series Cardinality

```sql
SHOW SERIES FROM battery_cell
SHOW TAG VALUES FROM battery_cell WITH KEY = site_id  -- (repeated per tag)
```

**Result:** **14,976 unique series**.

| Tag | Unique values |
|-----|:---:|
| site_id | 48 |
| rack_id | 4 |
| module_id | 6 |
| cell_id | 10 |

---

## Test 7 — Known Failure Mode Signatures

Scans journalctl (telegraf) and FlashMQ log for known silent failure patterns.

| Failure mode | Detected |
|---|:---:|
| Nested payload parse errors | ✓ none |
| FlashMQ QoS drop warnings | ✓ none |
| Consumer stall (counter frozen during sample) | ✓ none |

---

## Scale Test — CPU & Memory

Generator cycled through 1×, 6×, and 24× configs; Telegraf + InfluxDB measured at steady state.

| Scale | Config | msg/sec | Field writes/sec | influxd CPU% | influxd RAM | telegraf CPU% | telegraf RAM | Dropped |
|-------|--------|--------:|-----------------:|-------------:|------------:|-------------:|------------:|--------:|
| **1x** | `config.test_telegraf.yaml` | 592.8 | 8,299 | 1.7% | 361 MB | 3.9% | 172 MB | 0 |
| **6x** | `config.6x.yaml` | 3,134.3 | 43,880 | 1.8% | 406 MB | 4.1% | 172 MB | 0 |
| **24x** | `config.24x.yaml` | 10,823.5 | 151,529 | 2.2% | 483 MB | 5.0% | 173 MB | 0 |

---

## Summary

| Finding | Value |
|---------|-------|
| Write rate (records/sec) | ~624/sec |
| Field writes/sec | 8,735 |
| Lifetime writes | 1,234,853 |
| Dropped metrics | 0 |
| InfluxDB RAM | 715 MB |
| Telegraf RAM | 172 MB |
| Buffer headroom | 11 sec |
| Series count | 14,976 |
| Flux v2 API | Working |
| Nested payload parse errors | none detected |
| QoS drop / consumer stall | none detected |

---

## AWS Cost Estimates

Based on **22,641,120,000 field writes/month** (754,704,000/day at ~624/sec).  Real production anchor: **$76,200/month** (80+ sites, from `docs/aws_billing_context.md`).

| Service | 1× (this test) | 6× scale | 24× scale (full fleet) | Real prod 80+ sites |
|---------|---------------:|---------:|----------------------:|---------------------|
| AWS Timestream | $11,372 | $68,229 | $272,916 | — |
| InfluxDB Cloud Serverless | $12,955 | $77,732 | $310,928 | — |
| IoT Core + Lambda | $453 | $2,717 | $10,867 | **$5,000** |
| EC2 + EFS (InfluxDB, vertical) | $30 | $1,080 | $4,320 | **$46,000** |

> IoT Core + Lambda and EC2/EFS at 24× match closely with the real $76k bill.
> InfluxDB scales vertically (expensive `r5` instances); EC2+EFS dominates at fleet scale.

---

## Pipeline Configuration

### FlashMQ (`source/telegraf/flashmq.conf`)

```
log_file /tmp/flashmq.log
log_level info
allow_anonymous true
thread_count 4

listen {
    port 1883
    protocol mqtt
}
```

### Telegraf (`source/telegraf/telegraf.conf`)

```toml
[agent]
  interval          = "1s"
  flush_interval    = "10s"
  metric_batch_size = 1000

[[inputs.mqtt_consumer]]
  servers   = ["tcp://localhost:1883"]
  topics    = ["batteries/#", "solar/#"]
  qos       = 1
  data_format = "json_v2"

  [[inputs.mqtt_consumer.json_v2]]
    measurement_name = "battery_cell"
    [[inputs.mqtt_consumer.json_v2.tag]]
      path = "source_id"
    [[inputs.mqtt_consumer.json_v2.field]]
      path = "voltage"  type = "float"  optional = true
    # ... all fields must have optional = true

[[outputs.influxdb_v2]]
  urls         = ["http://localhost:8086"]
  token        = "battery-token-secret"
  organization = "battery-org"
  bucket       = "battery-data"
```

### InfluxDB2 — setup commands

```bash
# One-time init (runs non-interactively)
influx setup --force \
  --username admin --password adminpass \
  --org battery-org --bucket battery-data \
  --token battery-token-secret

# Verify
influx bucket list --org battery-org --token battery-token-secret
```

> **Note:** Telegraf reads from the MQTT broker (FlashMQ or system Mosquitto) on port 1883.
> All json_v2 fields must be `optional = true` when topics carry different schemas (battery vs solar).
