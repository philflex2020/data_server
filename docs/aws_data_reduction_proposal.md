# AWS Data Reduction Proposal

**Date:** 2026-03-17
**Author:** Phil
**Status:** Ready for review

---

## Executive Summary

The current AWS bill is **~$76,000/month**, driven by InfluxDB's vertical scaling
requirement, AWS IoT Core per-message charges, and Lambda invocations.

A replacement pipeline has been designed, implemented, and validated on phil-dev:
`writer.cpp` (C++17, Apache Arrow, simdjson) subscribes to FlashMQ directly, writes
partitioned Parquet files locally, and rsyncs to S3.  DuckDB replaces InfluxDB for
historical queries.  A local `current_state.parquet` written by `writer.cpp` on AWS
covers the real-time gap with no rsync lag.

**Projected saving: ~$75,140/month (~$900k/year) at 80 sites.**

The migration carries near-zero risk: both pipelines subscribe to the same FlashMQ
broker independently — no existing service needs to change until the new pipeline is
validated.

---

## 1. Current Cost Breakdown

| Service | Monthly cost | Root cause |
|---------|-------------:|------------|
| EC2 | $36,000 | InfluxDB requires `r5.xlarge` (32 GB RAM) per cluster node; scales vertically |
| EFS | $10,000 | InfluxDB TSM storage — $0.30/GB vs S3's $0.023/GB |
| Data Transfer | $3,600 | MQTT messages ingested via IoT Core → Lambda → cross-region transfer |
| S3 | $3,000 | Parquet archive (efficient; this cost is retained) |
| IoT Core | $3,000 | $0.30/million messages; at 8,000 msg/s = ~20 billion msg/month |
| CloudWatch | $2,600 | High-resolution custom metrics + log retention |
| Lambda | $2,000 | Per-message invocations for MQTT → InfluxDB transforms |
| **Total** | **~$76,000** | |

### Why InfluxDB is expensive at scale

InfluxDB's TSM engine maintains an in-memory cardinality index — one entry per unique
time series (site × rack × module × cell × measurement).  At 80 sites × 1,152 cells
× 8 measurements = ~738,000 series, the index requires 32–64 GB RAM.  Adding sites
requires vertical instance upgrades, not horizontal scaling.  The cost grows
**superlinearly** with fleet size.

---

## 2. Proposed Architecture

```
SITE (on-premise)
  Generator → FlashMQ :1883
                │                        │ FlashMQ bridge (outbound TLS)
                ▼                        ▼
           writer.cpp              AWS FlashMQ :1883
           (C++17 · Arrow)               │
                │                        ▼
           /srv/data/parquet/       writer.cpp (AWS)
           rsync → S3 (15 min)      current_state mode
                                         │
                                    current_state.parquet
                                    (local · no rsync lag)
                                         │
AWS                               DuckDB ◄──── S3 parquet
  t3.small EC2                    (history + current state)
  Subscriber API
```

### Key components

| Component | Technology | Replaces |
|-----------|-----------|---------|
| Site ingest | `writer.cpp` — FlashMQ subscribe, Arrow write | NATS bridge + writer.py |
| Message durability | FlashMQ QoS-1 persistent session | NATS JetStream |
| Live forward to AWS | FlashMQ bridge (outbound TLS) | NATS Leaf Node |
| Historical store | S3 Parquet, rsync every 15 min | EFS + InfluxDB TSM |
| Current-state queries | `current_state.parquet` on AWS (writer.cpp) | InfluxDB `last()` in-memory index |
| Historical queries | DuckDB, date-scoped glob paths | InfluxDB Flux / InfluxQL |
| AWS ingest | writer.cpp on AWS (no IoT Core, no Lambda) | AWS IoT Core + Lambda |

### Why Parquet + DuckDB is cheaper at scale

- **Fixed schema overhead:** Parquet column encoding scales with data volume, not
  cardinality.  Adding 1,000 new cell IDs costs storage, not RAM.
- **No per-message cloud charge:** IoT Core ($0.30/million) and Lambda ($0.20/million)
  are eliminated entirely.  Writer.cpp is a local process.
- **S3 pricing:** $0.023/GB vs EFS $0.30/GB — 13× cheaper per GB stored.
- **DuckDB is stateless:** runs anywhere, scales horizontally, no licensing cost.
- **One shared EC2 `t3.small`** serves all 80 sites for the Subscriber API.

---

## 3. Technical Validation

`writer.cpp` has been implemented and tested on phil-dev (x86_64, Ubuntu 24.04,
Arrow 23.0.1, g++ 11.4.0).

### writer.cpp KPI results

| Metric | Measured | Target | Headroom |
|--------|---------|--------|---------|
| Parse throughput (battery) | 430,000 msg/s | > 14,976 msg/s (24× fleet) | **29×** |
| Parquet write (1k rows, snappy) | 962,000 rows/s | > 500,000 rows/s | **1.9×** |
| Flush 10 s of fleet data (149,760 rows → 96 files) | 0.124 s | < 1 s | **8×** |
| Unit tests | 37 / 37 pass | — | — |

### DuckDB query performance vs InfluxDB

| Query | DuckDB local | InfluxDB ref | Verdict |
|-------|-------------|-------------|---------|
| Q1 — Latest snapshot, site 0 (current_state) | 4.7 ms | 7.8 ms | **1.7× faster** |
| Q2 — Single cell time series, last hour | 60 ms | 2.1 ms | Slower (InfluxDB hot index) |
| Q3 — Rack aggregates, 10 s buckets | 54 ms | 86.9 ms | **1.6× faster** |
| Q4 — Alert scan, all cells (hot file) | 7.6 ms | 55.5 ms | **7× faster** |
| Q5 — Solar overview, all panels (hot file) | 9.0 ms | 235.6 ms | **26× faster** |
| Q6 — Fleet health, cell count + freshness | 5.4 ms | 27.6 ms | **5× faster** |

> Q2 is slower than InfluxDB because InfluxDB keeps a hot in-memory inverted index per
> series.  DuckDB scans Parquet files (date-scoped, ~60 files at production flush rate).
> At 60 s production flush rate this is expected to come in under 15 ms.  All
> operationally critical queries (current state, alerts, solar overview, fleet health)
> are faster.

---

## 4. Cost Comparison

### Per site (stress load: 1,152 cells, ~8,000 msg/s)

| Component | Current (InfluxDB) | Proposed (writer.cpp + DuckDB) | Saving |
|-----------|-------------------:|------------------------------:|-------:|
| EC2 | $182 (`r5.xlarge`) | $0.25 (shared `t3.small`) | $181.75 |
| EBS / EFS | $25 | $0 | $25 |
| S3 storage | $10 | $10 | $0 |
| IoT Core | $6 | $0 | $6 |
| Lambda | $5 | $0 | $5 |
| CloudWatch | $8 | $1 | $7 |
| Data transfer | ~$45 | $0.35 | ~$44.65 |
| **Total/site** | **~$281** | **~$11.60** | **~$269** |

### At 80 sites

| | Current | Proposed | Annual saving |
|--|--------:|--------:|-------------:|
| EC2 (shared) | $36,000 | $20 | $431,760 |
| EFS | $10,000 | $0 | $120,000 |
| S3 | $3,000 | $800 | $26,400 |
| Data transfer | $3,600 | $28 | $43,344 |
| IoT Core | $3,000 | $0 | $36,000 |
| Lambda | $2,000 | $0 | $24,000 |
| CloudWatch | $2,600 | $10 | $31,080 |
| **Total** | **$76,200** | **~$858** | **~$901,704/year** |

---

## 5. Storage Cost — 10-Year Projection

**Assumptions:** 80 sites · 1,152 cells/site · 1,152 msg/s/site · 15-min flush ·
~8 MB/file (Parquet snappy, ~8 bytes/row compressed) · 96 flushes/site/day →
~2 TB new data per month (all 80 sites).
S3: $0.023/GB/month. EFS: $0.30/GB/month.
InfluxDB has a 30-day rolling delete — EFS cost is fixed at steady state; data older than 30 days is permanently gone.

### Three scenarios

| Scenario | Retention | Monthly cost | 10-year total | Historical data |
|----------|-----------|-------------:|-------------:|----------------|
| InfluxDB EFS | 30 days (rolling delete) | $10,000 flat | $1,200,000 | None beyond 30 days |
| S3 Parquet — 30-day lifecycle | 30 days (matching InfluxDB) | $46 flat | $5,520 | None beyond 30 days |
| S3 Parquet — indefinite retention | All data, forever | $46 → $5,520 by Y10 | $334,000 | Full 10-year history |

> **S3 with a 30-day lifecycle policy costs $5,520 over 10 years vs InfluxDB's $1,200,000 — for identical retention. That is 217× cheaper. And keeping everything (indefinite retention, 10 years of history) still costs less than InfluxDB loses each month.**

### Year-by-year storage cost breakdown (80 sites)

| Year | Total data stored | InfluxDB EFS / month | S3 indefinite / month | S3 30-day / month | InfluxDB cumulative | S3 indefinite cumulative |
|------|------------------|---------------------:|---------------------:|------------------:|--------------------:|------------------------:|
| 1 | 24 TB | $10,000 | $552 | $46 | $120,000 | $3,588 |
| 2 | 48 TB | $10,000 | $1,104 | $46 | $240,000 | $13,800 |
| 3 | 72 TB | $10,000 | $1,656 | $46 | $360,000 | $30,636 |
| 4 | 96 TB | $10,000 | $2,208 | $46 | $480,000 | $54,648 |
| 5 | 120 TB | $10,000 | $2,760 | $46 | $600,000 | $84,180 |
| 6 | 144 TB | $10,000 | $3,312 | $46 | $720,000 | $120,888 |
| 7 | 168 TB | $10,000 | $3,864 | $46 | $840,000 | $164,220 |
| 8 | 192 TB | $10,000 | $4,416 | $46 | $960,000 | $214,632 |
| 9 | 216 TB | $10,000 | $4,968 | $46 | $1,080,000 | $271,524 |
| **10** | **240 TB** | **$10,000** | **$5,520** | **$46** | **$1,200,000** | **$334,000** |

Key 10-year figures:

- **InfluxDB EFS total:** $1,200,000 — 30-day window only, zero long-term history
- **S3 indefinite total:** $334,000 — full 10-year history retained (240 TB)
- **S3 30-day lifecycle total:** $5,520 — same retention as InfluxDB, 217× cheaper
- **InfluxDB data retained at year 10:** 0 TB beyond 30-day window

> **Note on EC2:** InfluxDB EC2 costs may grow superlinearly. The cardinality index (738k series at 80 sites) grows with every new site and cell type. At some threshold the 32 GB `r5.xlarge` is no longer sufficient and a vertical upgrade to `r5.2xlarge` ($364/site/month) or larger is required. The table above holds EC2 flat — the real 10-year InfluxDB cost may be substantially higher.

---

## 6. Migration Strategy

The migration carries **no flag day** and **no data loss risk**.  Both pipelines can
run simultaneously against the same FlashMQ broker.

| Phase | Action | Risk |
|-------|--------|------|
| 1 · Deploy alongside | Start `writer.cpp` on host, subscribed to FlashMQ `:1883` with a distinct `client_id`.  Telegraf and InfluxDB continue unchanged. | None |
| 2 · Validate data | Run `scripts/migration_compare.py` — compares InfluxDB and DuckDB results side-by-side across 4 checks (current voltage, cell count, alert count, rack averages). | None — read-only |
| 3 · Deploy AWS writer | Start `writer.cpp` on AWS in current_state mode.  Validate `current_state.parquet` against InfluxDB `last()`. | None — additive |
| 4 · Migrate consumers | Switch dashboard panels from InfluxQL → DuckDB one at a time.  Extend host flush to 15 min (`flush_interval_seconds: 900`). | Incremental — per-panel rollback |
| 5a · Historical export | Optional: export pre-cutover InfluxDB history to Parquet.  Not required — InfluxDB can remain as a read-only cold archive. | Optional |
| 5b · Decommission | Stop Telegraf and InfluxDB once all consumers are on DuckDB and the parallel run has been stable. | Irreversible — confirm retention policy first |

### Why parallel operation works

FlashMQ maintains a separate QoS-1 session per `client_id`.  Telegraf's MQTT input
plugin and `writer.cpp` each hold their own persistent session and receive independent
copies of every published message.  Neither can starve or block the other.

---

## 7. Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|-----------|
| writer.cpp data quality mismatch | Low — 37 unit tests, live comparison script | `migration_compare.py` validates before any consumer migration |
| DuckDB query latency regression | Low for critical queries | Q2/Q3 slower than InfluxDB at high file counts; date-scoped paths and 15 min flush mitigate |
| AWS writer downtime (current_state gap) | Low — QoS-1 reconnect, FlashMQ queues | writer.cpp auto-reconnects; gap bounded by downtime duration |
| Parquet store growth (no retention policy) | Medium — no lifecycle rule configured | Add S3 lifecycle rule before decommission (see aws_deploy.md) |
| Pre-cutover InfluxDB history gap | Low — InfluxDB retained as cold archive | Phase 5a export is optional; use InfluxDB for pre-cutover history queries |

---

## 8. Recommendation & Next Steps

1. **Approve parallel deployment** (Phase 1) — zero risk, immediate data collection starts.
2. **Run transition demo** — start `writer.cpp` alongside Telegraf, run
   `migration_compare.py --cycles 6`.  Demo proves both pipelines agree and the
   migration story is credible to stakeholders.
3. **Deploy AWS writer** (Phase 3) — enables 15-min flush interval on host, reducing
   file count and S3 rsync overhead.
4. **Set target decommission date** for InfluxDB + IoT Core + Lambda — the three
   largest cost components.  Monthly saving from decommission: **~$41,000/month**
   (EC2 + IoT Core + Lambda alone).

The implementation is complete.  The migration can begin at any time without
disrupting existing services.
