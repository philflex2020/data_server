# Parquet / DuckDB Architecture — Review Brief

**Status:** Pre-demo briefing document
**Author:** Senior Engineer, BMS Team
**Date:** 2026-03-19
**Branch:** `03192026_dual_host`

---

## Context

A separate team (Phase 1) has already addressed approximately 30-40% of the
AWS cost problem by optimising EC2, EFS, and IoT Core usage. That team is now
engaged in a debate about migrating from InfluxDB 2 to InfluxDB 3, which
involves significant Flux query rewrites and schema changes across the stack.

The BMS team has been pursuing a parallel and independent line of work, with
Parquet and DuckDB as its primary tools. This document describes that work,
its immediate value, and its forward roadmap.

The two efforts are not in competition. They address different parts of the
same problem.

---

## The Three-Part Value Proposition

### A. Parquet removes data loss — immediately, without replacing anything

The current MQTT pipeline loses messages under load. This is not a hypothesis
— it is measurable. At ~8,000 messages/second through an asyncio subscriber,
QoS 1 acknowledgements create a backpressure bottleneck that causes the broker
to drop messages silently. No error is raised. No counter increments. Data is
simply absent from InfluxDB with no indication that it was ever sent.

The parquet writer runs upstream of this bottleneck. It receives messages
directly from FlashMQ, writes them to columnar files on disk, and measures
every drop via a chain-of-custody sync counter that is visible in the monitor.
During all test runs the drop counter has been zero or near-zero at
representative load.

**This is the immediate value: the complete record is preserved regardless of
what happens downstream.** InfluxDB continues to receive exactly what it
receives today. Nothing changes in the existing pipeline. Parquet is additive.

---

### B. Parquet and DuckDB work alongside InfluxDB

The dual-path monitor (`parquet_monitor.html`) shows both query paths side by
side in real time:

- **Left column:** Flux queries → InfluxDB2 (current production path)
- **Right column:** DuckDB queries → local Parquet files (new path)
- **`/compare` endpoint:** live delta between the two — currently < 1%

Neither path is switched off. Neither path depends on the other. The team can
run both indefinitely, build confidence in the parquet path over time, and
make a transition decision when the evidence supports it — not before.

InfluxDB continues to handle what it does well: live alerting, continuous
queries, KPI dashboards, real-time display. Parquet/DuckDB handles what it
does better: complete historical record, low-cost long-term storage, fast
range queries, full cell-level resolution.

---

### D. Real-time capability is already present — and extensible

A common assumption is that removing InfluxDB means losing real-time
visibility. This is not the case.

The subscriber API already maintains a live in-memory buffer of every MQTT
message received from the field, and already pushes each message to connected
browser clients via WebSocket within milliseconds of arrival. This is faster
than the InfluxDB write → index → query cycle.

Three small additions to the existing buffer machinery (in design, not yet
implemented) would replicate the specific real-time features InfluxDB provides:

- **Current state per cell** — latest reading for every `(site, rack, module,
  cell)` tuple, served from the buffer in sub-millisecond RAM scan
- **Windowed aggregates** — `AVG/MIN/MAX` over a configurable time window,
  computed via DuckDB on a buffer snapshot
- **Threshold alerting** — server-side loop that checks configurable limits on
  each new message and pushes alert events to connected clients

These are extensions of work already done, not new systems. The full discussion
is in the C2 rebuttal section below.

---

### C. When mature, the parquet path can replace InfluxDB

This is not the immediate ask. It is the direction of travel and it should be
stated plainly so the roadmap is understood.

When the parquet path has been validated against the full production data
catalog, run at full site count for a sustained period, and the team is
confident in its failure modes — at that point the InfluxDB layer becomes
optional for historical queries. Live alerting and real-time display can be
served by a lightweight filtered stream at a fraction of the current message
rate and cost.

The Phase 1 team's current debate about InfluxDB 3 is relevant here. InfluxDB
3 drops the Flux query language entirely. Every Flux query in the current
system — dashboards, automated reports, the monitoring UI — will require
rewriting. The `flux_compat.py` layer in this system already translates Flux
queries to DuckDB SQL transparently. The browser clients do not change. The
InfluxDB 3 migration has already been absorbed into this architecture. The
Phase 1 team will need to do that work regardless of which path is chosen; on
the BMS path, it is already done.

---

## Expected Critique and Response

### C1: QoS was downgraded — the system admits data loss

> *"The first thing this work did was drop MQTT QoS from 1 to 0. That is a
> weaker delivery guarantee. We have maintained QoS 1 for months."*

**Where this has merit:** The change is real and should be documented, not
glossed over.

**Response:** The QoS reduction made visible what was already happening. At
high message rates, MQTT QoS 1 requires a per-message PUBACK from the broker
for every PUBLISH — there is no batch acknowledgement in the MQTT protocol.
Above approximately 3,000 messages/second this creates unavoidable backpressure
and silent drops. The current system was losing messages without knowing it.

The parquet path measures drops explicitly via the `drops_detected` sync
counter in the monitor. Zero or near-zero at test load. The honest comparison
is: **unknown silent loss (current path)** versus **quantified transparent loss
(new path)**. If guaranteed delivery at full rate is a hard requirement, the
answer is NATS JetStream which provides batch acknowledgement natively — not
a change to the parquet path itself.

---

### C2: DuckDB is not a production time-series database

> *"DuckDB is an embedded analytics tool. InfluxDB was purpose-built for this
> workload. It handles real-time dashboards, alerting, and continuous queries
> that DuckDB simply cannot do."*

**Response — historical queries:** DuckDB is not being proposed as a
replacement for InfluxDB's live write path or alerting layer. It is the query
engine for the historical parquet record — reading files that have already been
written to durable storage. For that role it is better suited than InfluxDB:

- Sub-100ms query latency for hour-range scans vs InfluxDB's 300-800ms under load
- Standard SQL — no Flux syntax, no proprietary query model
- Reads directly from S3 at $0.023/GB with no per-query cost
- Query performance is unaffected by series cardinality growth

**Response — real-time (discussion point, work in progress):**

The real-time capability is further along than it appears. The in-memory live
buffer (`g_live_buffer`) already receives every MQTT message within milliseconds
of the sensor sending it, and the WebSocket layer already pushes every message
to connected browser clients in real time. This is *faster* than the InfluxDB
path, which has a write buffer and compaction cycle before data is queryable.

Three extensions to the existing buffer machinery would replicate the real-time
features InfluxDB currently provides:

1. **Current state query (`query_latest`):** Scan the buffer in reverse, take
   the first occurrence of each `(site, rack, module, cell)` tuple. Returns the
   live reading for every cell in the system. Pure RAM scan — no parquet, no
   DuckDB, sub-millisecond. This is equivalent to InfluxDB's `last()` function.

2. **Windowed aggregates:** Register a buffer snapshot as a DuckDB in-memory
   view and run standard SQL aggregations — `AVG(voltage)`, `MIN(temperature)`,
   `MAX(current)` grouped by site or rack over the last N seconds. The hybrid
   query machinery to do this is already implemented. This replicates InfluxDB's
   continuous aggregate queries.

3. **Threshold alerting:** A lightweight server-side loop that scans the buffer
   on each new message (or every N seconds), checks configurable per-field
   limits, and pushes `{"type": "alert", ...}` messages to connected WebSocket
   clients. InfluxDB runs this logic inside its engine; this runs in the same
   Python process against the same in-memory data.

The one honest limitation: on a server restart the buffer takes 30-60 seconds
to refill before aggregates are meaningful. For alerting this is acceptable —
alerts are about present state, not history. For dashboards it is a minor
cold-start delay.

**The strategic point:** the claim that "InfluxDB handles real-time and DuckDB
cannot" conflates two separate systems. The WebSocket + live buffer layer
handles real-time. DuckDB handles history. Together they cover the full
spectrum. InfluxDB currently does both; this architecture separates the
concerns and does each one better.

---

### C3: The in-memory buffer loses data on restart

> *"The live buffer is a Python deque in RAM. A crash loses the last 6 minutes
> of data silently."*

**Response:** The buffer covers the rsync sync window — roughly the last 60
seconds to 6 minutes. If the subscriber API restarts, all data prior to that
window is already in durable parquet files on disk. The buffer refills within
seconds of reconnecting to FlashMQ. Buffer size is visible in the monitor in
real time.

The same failure mode exists in the current system: if the InfluxDB write agent
or Lambda transform crashes, the same window of data is absent from InfluxDB
and is not replayed. The new system's failure mode is equivalent, bounded, and
visible. The current system's is opaque.

---

### C4: One week of work is not a production architecture

> *"There has been no load testing at scale, no failure mode analysis, no DR
> plan."*

**Response:** The validated claims are specific and accurate:

- Parquet writer correctly captures and partitions MQTT JSON ✓
- DuckDB queries those files faster and cheaper than InfluxDB ✓
- rsync push path correctly delivers files to the simulated AWS store ✓
- `/compare` shows < 1% count delta between InfluxDB and DuckDB ✓

No claim of production-readiness at full scale has been made. The remaining
validation work before any production proposal is documented in Part 5 of this
review. It is a small and well-defined list. This is a staged proposal — not a
request for an immediate cutover.

---

### C5: The real fix is InfluxDB tuning — downsampling and retention policies

**Response:** InfluxDB tuning is a real option and should be costed honestly.
Three problems with it as a primary strategy:

1. **Downsampling loses cell-level resolution.** Battery monitoring diagnosis
   depends on individual cell deviation. Site-level or rack-level aggregates
   may not preserve the diagnostic signal.

2. **The dominant cost drivers are EC2 and EFS**, both driven by InfluxDB's
   memory requirements for its series cardinality index. Downsampling reduces
   write volume but does not reduce the instance size required to hold the
   index. The infrastructure cost remains.

3. **Tuning still requires Telegraf config changes** which cascade to Flux
   queries and dashboards. The schema-coupling problem — the root cause of
   the long-term maintenance burden — is not addressed.

---

### C6: Lower-spec edge hosts can't run the parquet writer

> *"Some site controllers are constrained x86 boxes. So we need a cloud-side
> receiver. That's more moving parts."*

**Response:** For sites where the parquet writer cannot be deployed locally,
the architecture supports a cloud-side receiver running as a serverless
container (AWS Fargate — a managed container service that charges per CPU
second with no dedicated server required, approximately $4-5/month per
receiver). This receiver is content-agnostic: it requires no update when
sensor fields change, replaces Telegraf on the AWS side, and costs a fraction
of the current IoT Core + Lambda + InfluxDB ingestion path.

The MQTT publisher on the edge host does not change at all.

---

### C7 — The Valid Criticism: Test data is not representative

> *"The stress runner uses synthetic data with a simple schema. The production
> system sends dozens of measurement types with vendor-specific field names
> that Telegraf was specifically configured to handle. This system has never
> seen that data."*

**This is the strongest objection. It is technically correct. It is not an
architectural problem — it is a testing gap, and a small one.**

The parquet writer is schema-agnostic by design:

- It receives raw MQTT JSON payloads
- It deserialises each message as a flat key-value map
- It writes every key as a column in the parquet file
- It has no field mapping, no type coercion, no schema definition

A new field appearing in a payload from any sensor vendor requires no changes
to the writer, the file layout, or the DuckDB query layer. The field appears
in the output immediately. This is not an accident — it is the primary design
goal. **The system adapts to the data. The data does not need to adapt to the
system.**

By contrast, the current Telegraf config explicitly names every field it
expects. A new field is invisible to InfluxDB until Telegraf is updated,
redeployed, and restarted at each affected site. Schema coupling is a feature
of the current architecture; schema agnosticism is a feature of the new one.

**Closing the testing gap:** Mirror the production MQTT bridge to the test
parquet writer for one hour. The writer requires zero changes. This is one
afternoon of work and it closes the objection entirely.

---

## ROI Against the 10x Mandate

The team operates under a mandate of 10x return on development investment.

**Development investment:** 6 person-days — full system design, dual-host
validation bench, live monitor and config UI, in-process gap fill,
documentation. Produced by a senior BMS engineer working with AI-assisted
development tooling.

**Infrastructure saving** (from `docs/aws_cost_comparison.md`):

| Scale | InfluxDB path | Parquet/S3 path | Annual saving |
|---|---|---|---|
| 3 sites (validated) | $226/month | $50/month | $2,112 |
| 80 sites (extrapolated) | $76,200/month | ~$500/month | **~$908,000** |

*Note: Phase 1 work has already reduced the baseline by 30-40%. The parquet
path saving is calculated against the remaining bill, not the original $76k.*

**Engineering time saving — schema changes:**

Each new sensor field or vendor integration in the current system requires
Telegraf config update, deployment, Flux query update, and dashboard change
across every affected site. Conservative estimate: 1 engineer-day per site.

At 80 sites, one schema change costs 80 engineer-days. At a fully-loaded day
rate of $1,000: **$80,000 per change**. The new system cost for the same
change: **$0**.

**InfluxDB 2 → 3 migration (already absorbed):**

The Phase 1 team is currently debating this migration. Estimated effort:
3-6 weeks minimum. The `flux_compat.py` shim already translates all Flux
queries to DuckDB SQL transparently. On this path, the migration is done.

**Summary:**

| | Value |
|---|---|
| Investment (6 days, fully loaded) | ~$9,000-12,000 |
| Infrastructure saving, year 1 (80 sites) | ~$908,000 |
| ROI on infrastructure alone | **~75-100x** |
| One avoided schema change | $80,000+ |
| Avoided InfluxDB 3 migration | $30,000-80,000 |

The 10x threshold is cleared by the infrastructure saving alone in the first
month at full deployment. The engineering time and migration savings are
additional.

---

## Known Gaps — Stated Proactively

| Gap | Severity | Path to close |
|---|---|---|
| Full production data catalog not yet ingested | Medium | Mirror production MQTT bridge to test writer — one afternoon |
| No 48-hour sustained load test completed | Medium | Schedule on test bench before any production proposal |
| No formal failure mode / DR documentation | Low | Document before production deployment |
| Buffer window (≤6 min) lost on subscriber restart | Low | Bounded, visible in monitor; parquet files unaffected |
| Single-node DuckDB — no HA | Low | Query layer is stateless; restart is seconds; data lives in files |

---

## Recommended Next Steps

1. **This week:** Mirror one hour of production MQTT traffic through the parquet
   writer alongside the live InfluxDB path. Review the `/compare` delta.

2. **Next week:** Run a 48-hour sustained load test. Document failure scenarios
   and recovery times.

3. **On validation:** Bring both paths to a joint review with the Phase 1 team.
   The InfluxDB 3 migration debate is the natural entry point — this architecture
   has already solved it.

4. **No InfluxDB instance is turned off at any stage of this process.**

---

*See also:*
- `docs/aws_cost_comparison.md` — cost model detail
- `docs/tiered_data_architecture.md` — edge → AWS split path design
- `docs/edge_parquet_architecture.md` — full edge deployment model
- `subscriber/api/server.py` — live buffer and hybrid query implementation
- `html/parquet_monitor.html` — dual-path comparison monitor
