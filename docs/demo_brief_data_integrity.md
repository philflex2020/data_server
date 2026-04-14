# Demo Brief — No-Loss Data Path

**Date:** 2026-03-20
**Author:** Senior Engineer, BMS Team
**Scope:** Data integrity + cost reduction — deploy alongside existing stack

---

## Origin — What Was Asked and What Was Found

The original request was straightforward: review the existing codebase with
AI assistance and identify improvements. That work began as directed.

In the course of that review it became clear that the BMS team already had
significant relevant work in progress — a Parquet-based data capture and
query architecture that had been developed independently and was at an advanced
stage. The question became whether to produce a list of incremental code
improvements to the existing system, or to use the AI-assisted review as the
vehicle to integrate the BMS team's existing work in a way that opened a
direct pathway to the AWS cost reduction the business had already mandated.

The decision was made to divert from the original ask for one specific reason:

> The value of identifying a no-loss data path that reduces AWS ingestion costs
> by more than 50% — with a clear pathway to further reduction as we validate
> against production data — far exceeds the value of a code review. No existing
> component is modified. No running site is touched. The current pipeline
> continues operating exactly as it does today.

The cost of that diversion was six working days. The projected return is
documented below. The original code review remains available to be completed
on request — it was not abandoned, it was deferred in favour of higher-value
work with a clear and immediate business case.

---

## The Single Point

The current MQTT pipeline loses data under load. We have built a zero-loss
capture path that runs alongside the existing stack, touches nothing currently
running, and costs a fraction of the current InfluxDB ingestion path.

That is the scope of this demo. Nothing is being replaced today.

---

## The Problem — Data Is Being Lost

At high message rates, MQTT QoS 1 requires the broker to issue a per-message
acknowledgement (PUBACK) for every PUBLISH. There is no batch acknowledgement
in the MQTT protocol. Above approximately 3,000 messages/second this creates
backpressure that causes the broker to drop messages silently.

No error is raised. No counter increments in InfluxDB. The data is simply
absent from the time-series store with no indication it was ever sent.

For a battery management system — where a single cell deviation is the primary
diagnostic signal — this is a data integrity problem, not a performance problem.

---

## The Solution — A No-Loss Capture Path

A lightweight C++ parquet writer subscribes to the same MQTT broker as
InfluxDB. It receives every message, writes it to a columnar parquet file on
disk, and measures any drops explicitly via a chain-of-custody sync counter.

**Key properties:**

- **Zero loss by design** — the writer runs upstream of the InfluxDB ingestion
  bottleneck. It does not depend on InfluxDB, Telegraf, or any downstream
  component. If InfluxDB drops a message the parquet file still has it.

- **Content agnostic** — the writer has no schema definition. It deserialises
  each MQTT JSON payload as a flat key-value map and writes every field as a
  column. New sensor types, new vendors, new fields — all captured
  automatically with no configuration change.

- **Measurable** — a chain-of-custody sync counter tracks messages published
  vs messages received and reports the delta in real time. The current InfluxDB
  path has no equivalent visibility.

- **Additive** — the writer runs as an additional subscriber alongside the
  existing Telegraf/InfluxDB path. Nothing is reconfigured. Nothing is turned
  off. InfluxDB continues to receive exactly what it receives today.

---

## The Cost — A Fraction of the Current Path

The no-loss parquet path stores data in open columnar files on S3. Parquet
with Snappy compression is 8-15× smaller than equivalent raw MQTT payloads.
S3 storage costs $0.023/GB/month.

| Path | Component costs | Relative cost |
|---|---|---|
| Current (InfluxDB) | EC2 + EFS + IoT Core + Lambda + CloudWatch | baseline |
| No-loss parquet path | EC2 `t3.large` + S3 | **&gt; 50% reduction** |

These figures are based on stress-test load with synthetic data. A cost
estimate against the actual production data volume is in progress and will
be provided before any production deployment proposal. The directional saving
is clear; the precise figure will be confirmed with real data.

The parquet path scales horizontally at constant unit cost. InfluxDB scales
vertically and its cost accelerates with site count — the saving is expected
to widen at full deployment.

*Note: Phase 1 work has already reduced the AWS baseline by 30-40%. The
parquet path addresses the remaining ingestion and storage cost.*

---

## What This Is Not

- This is not a proposal to replace InfluxDB
- This is not a request to change the existing pipeline
- This is not dependent on any new AWS infrastructure
- This does not require any change to the MQTT publishers on the edge

The parquet writer is an additional subscriber. Deploy it, point it at the
existing broker, and data starts landing in files. That is the entire
deployment procedure.

---

## What the Demo Shows

1. **The parquet writer running** alongside the existing stack on the test bench
2. **Files landing on disk** — file count and size growing in real time
3. **The drop counter** — chain-of-custody tracking of messages published vs
   received, live in the monitor
4. **Both paths active simultaneously** — InfluxDB receiving data normally,
   parquet writer capturing the same stream independently

---

## Deployment Path

The system has been validated on two machines replicating the edge → AWS
topology:

- `phil-dev` (test_host): MQTT publishers + parquet writer → local parquet files
- `fractal-phil` (test_aws): rsync'd parquet store + subscriber API

This mirrors a real deployment where the writer runs at the site controller
and files are pushed to S3. The deployment on existing site hardware requires
no changes to any currently running component.

---

## What Is Not In Scope Today

The following capabilities exist and are available for a follow-on discussion
but are not part of this demo:

- Historical queries against the parquet archive
- Real-time query layer and live aggregates
- Full InfluxDB query compatibility and migration path
- Long-term replacement of the InfluxDB path

---

*Full technical detail: `docs/architecture_review_duckdb_parquet.md`*
*Cost model: `docs/aws_cost_comparison.md`*
