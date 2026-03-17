# NATS Pipeline — Scale Test & Data Path Report (x86_64)

> **Historical reference.** This report documents performance of the old
> `bridge.py → NATS JetStream → writer.py → MinIO` pipeline.  That pipeline has been
> replaced by `writer.cpp` (FlashMQ → C++ writer → local parquet → rsync → S3).
> Current design and KPIs: see [`docs/parquet_writer_cpp.md`](parquet_writer_cpp.md)
> and [`html/nats_report_x86.html`](../html/nats_report_x86.html).

---

**Test date:** 2026-03-17 12:47 UTC
**Host:** fractal-phil  |  **NATS:** 2.12.5  |  **Sample:** 30s per scale

Data path under test (old pipeline):

```
Generator → MQTT:1884 → bridge.py → NATS JetStream (host:4222)
                                  → leaf node → NATS hub sim (AWS:4333)
                                  → writer.py → MinIO (S3 sim)
```

---

## Scale Test — Throughput & Resources

| Scale | sensor msg/s | msg/s (host NATS) | msg/s (AWS NATS) | raw KB/s | NATS CPU | NATS RAM | bridge CPU | writer CPU |
|-------|-------------:|------------------:|-----------------:|---------:|---------:|---------:|-----------:|-----------:|
| **1x** | 1,398.3 | 2,796.3 | 1,398.1 | 556.8 | 5.6% | 75 MB | 17.8% | 11.1% |
| **6x** | 3,833.8 | 7,668.2 | 3,833.8 | 1,691.8 | 10.6% | 185 MB | 17.9% | 11.0% |
| **24x** | 4,199.8 | 8,400.1 | 4,199.8 | 1,951.0 | 13.5% | 317 MB | 18.2% | 11.0% |

> Leaf node connections to AWS sim: 1
>
> **Note**: `sensor msg/s` = JetStream window delta (unique data messages). Host/AWS NATS msg/s includes JetStream ACKs and protocol overhead (~2×). 1× measurement may be inflated by MQTT retained-message burst at bridge connect; 6× result (3,801 vs 3,744 expected) confirms measurement accuracy. 24× saturation reflects single-threaded bridge.py throughput ceiling (~4k msg/s).

---

## JetStream State

| Scale | Messages stored | Storage (MB) |
|-------|----------------:|-------------:|
| **1x** | 56,395 | 21.4 MB |
| **6x** | 190,314 | 78.5 MB |
| **24x** | 337,668 | 144.7 MB |

> Stream: `BATTERY_DATA`  |  Retention: 48h (production) → reduce to 12 min for AWS hub

---

## AWS Cost Projection (NATS + S3 path)

No IoT Core, no Lambda, no InfluxDB.  EC2 = t3.small NATS hub + t3.small Subscriber API.

| Scale | S3 data/month | S3 storage | S3 PUTs | EC2 (flat) | **Total/month** |
|-------|-------------:|----------:|--------:|----------:|----------------:|
| **1x** | 196.6 GB | $4.52 | $0.04 | $30 | **$34.56** |
| **6x** | 597.4 GB | $13.74 | $0.04 | $30 | **$43.78** |
| **24x** | 689.0 GB | $15.85 | $0.04 | $30 | **$45.89** |

> Compare: InfluxDB stack at 24× scale = **$46,000+/month** (EC2+EFS alone).

---

## Architecture Notes

**Leaf node**: host NATS dials out to AWS hub — no inbound firewall rules needed.
All messages published on host automatically appear on AWS NATS.

**Gap coverage (old pipeline)**: gap.parquet written on host, rsynced to S3 every 5 min.

> **Current design**: the gap is covered by `current_state.parquet` written locally on
> AWS by `writer.cpp` running in current_state mode — no rsync lag.  The host
> flush interval is extended to 15 min (`flush_interval_seconds: 900`) now that
> current_state on AWS eliminates the need for a short-interval hot file.

**Real-time (old)**: web clients subscribed to AWS NATS subjects.
JetStream on AWS: 12-minute retention (reconnect buffer only).

> **Current design**: real-time delivery via FlashMQ bridge → AWS FlashMQ.
> Web clients subscribe to MQTT topics directly via the Subscriber API.
