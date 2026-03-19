# Tiered Data Architecture — Edge → AWS

## Problem

Sending the full real-time telemetry stream to AWS is expensive and unnecessary.
At 768 cells × 7 fields = 5376 msg/s, MQTT QoS 1 requires a per-message PUBACK
(~21k packets/s) and InfluxDB ingestion cost scales linearly — a major driver of
the $76k/yr AWS bill.

MQTT QoS 0 avoids the ack overhead but drops messages under load (~16% at 5376
msg/s through an asyncio subscriber). There is no batch-ack in MQTT; every
PUBLISH gets its own PUBACK.

## Solution: Two Separate Paths

```
┌─────────────── Edge (phil-dev / on-site gateway) ────────────────┐
│                                                                   │
│  sensors / stress_runner                                          │
│       │                                                           │
│       └──► FlashMQ :1883                                         │
│                 │                                                 │
│                 ├──► writer.cpp ──► ALL data ──► parquet files    │
│                 │                        │                        │
│                 │                  rsync / S3 push ───────────────┼──► AWS S3
│                 │                                                 │
│                 └──► bridge ──► SELECTED topics only ─────────────┼──► AWS NATS
└───────────────────────────────────────────────────────────────────┘

┌─────────────── AWS ───────────────────────────────────────────────┐
│                                                                   │
│  NATS JetStream  ◄── selected real-time stream                    │
│       ├──► Telegraf ──► InfluxDB  (alerts, KPIs, live display)    │
│       └──► subscriber-api WS      (live dashboard feed)           │
│                                                                   │
│  S3 Parquet (complete record)                                     │
│       └──► DuckDB  (historical queries, analytics, audit)         │
└───────────────────────────────────────────────────────────────────┘
```

## Two Paths, Two Purposes

| | Real-time stream | Parquet / S3 |
|---|---|---|
| Content | Selected — site averages, alerts, KPIs | Everything — every cell, every field |
| Rate | 50–200 msg/s (filtered) | Full rate, columnar compressed |
| Delivery | NATS JetStream, at-least-once, batch-ack | rsync / S3 sync, complete |
| AWS cost | Low — small persistent stream | Low — Parquet 8–15× smaller than raw JSON |
| Use case | Live dashboard, alerting, anomaly detection | Historical analysis, billing, audit, ML |

## Why the Writer Stays at the Edge

The writer is the only component that sees the full firehose. It buffers in memory
and flushes to Parquet in batches (every N seconds or M messages). Parquet's
columnar format and Snappy/Zstd compression make the files 8–15× smaller than
the equivalent raw MQTT payloads, so the S3 transfer and storage cost is low even
for complete data.

AWS never needs to handle 5376 msg/s. InfluxDB on AWS only ingests the filtered
stream — a fraction of the full rate.

## What "Selected" Means in Practice

- Site-level aggregates (avg voltage per rack) rather than individual cell readings
- Out-of-range / threshold alerts only
- One-in-N sampling for trend visualisation
- State changes rather than continuous readings

The selection filter lives in the bridge config (`source/bridge/bridge.yaml`) —
topic groups control exactly which subjects cross to AWS.

## Why NATS JetStream Over MQTT QoS 1 on the AWS Side

NATS JetStream is a persistent log. Consumers hold a cursor and acknowledge
batches ("all messages up to sequence N") rather than individual messages. At
5k+ msg/s this eliminates the per-message ack overhead that makes MQTT QoS 1
impractical at high rates. Replay on restart is built in.

The edge stays MQTT (sensors speak MQTT). A NATS leaf-node or bridge replaces
the current FlashMQ bridge on the AWS side.

## Current State (branch 03192026_dual_host)

The dual-host validation run uses rsync push as a stand-in for S3:

- phil-dev runs writer.cpp → `/srv/data/parquet/`
- push_agent.py (:8770) rsyncs to fractal-phil every 5s
- fractal-phil DuckDB queries `/srv/data/parquet-aws-sim/`
- `/compare` shows < 1% delta between InfluxDB and DuckDB counts

This validates the split-path architecture before moving to real S3 and NATS.
