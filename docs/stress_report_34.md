# phil-256g (.34) — Torture Test Stress Report

**Date:** 2026-03-27
**Duration:** ~3 days continuous (started ~2026-03-24)
**Machine:** phil-256g — 2× Xeon E5-2690 v4 @ 2.60 GHz (56 threads), 256 GB RAM, 1.8 TB `/dev/sdb3`

---

## Stack

All components run in Docker (Docker 26).

| Container | Image | Role |
|---|---|---|
| `torture_broker` | `flashmq:torture` | FlashMQ MQTT broker · `:11883` |
| `torture_stress_a/b/c/d` | `stress-pub:torture` | 4× stress generators · 150,000 msg/s each |
| `torture_writer_a/b/c/d` | `parquet-writer:latest` | 4× parquet writers · health `:8771–8774` |

**Generator config** (per instance): `batteries_{a,b,c,d}/#` · 12 racks × 8 modules × 52 cells · 1 connection · rate 150,000 msg/s
**Writer config**: `flush_interval_seconds: 10` · `max_messages_per_part: 50,000` · `snappy` compression · WAL disabled · compactor **not enabled**

---

## Performance Summary (snapshot at 2026-03-27 ~15:43 UTC)

### CPU & Memory

| Container | CPU % | CPU equiv (of 56 cores) | RSS |
|---|---|---|---|
| `torture_writer_a` | 167% | 1.67 cores | 752 MB |
| `torture_writer_b` | 163% | 1.63 cores | 751 MB |
| `torture_writer_c` | 153% | 1.53 cores | 746 MB |
| `torture_writer_d` | 160% | 1.60 cores | 744 MB |
| **4× writers total** | **643%** | **6.4 cores** | **~2.99 GB** |
| `torture_broker` | 141% | 1.41 cores | 105 MB |
| `torture_stress_a/b/c/d` | ~420% | 4.2 cores | ~21 MB |
| **Full stack total** | **~1204%** | **~12 cores / 56** | **~3.1 GB** |

System load average: **11.17** (≈ 20% of 56 cores)
System memory used: **4.1 GB / 256 GB** (1.6%)

> Machine is running at **~21% CPU** for the full torture stack. Matches the observed ~23% including OS overhead.

### Writer Health (per instance)

| Metric | writer-a | writer-b | writer-c | writer-d |
|---|---|---|---|---|
| msgs received | 40,220,872,238 | 40,220,947,912 | 40,220,874,915 | 40,220,911,490 |
| total rows written | 40,220,855,620 | 40,220,904,323 | 40,220,865,950 | 40,220,855,959 |
| flush count | 1,482,291 | 1,469,061 | 1,435,689 | 1,427,280 |
| flush avg ms | 42 | 43 | 46 | 45 |
| flush max ms | 375 | 310 | 431 | 420 |
| last flush rows | 50,010 | 6,536 | 19,396 | 3,156 |
| overflow drops | 0 | 0 | 0 | 0 |
| ring drops | 0 | 0 | 0 | 0 |
| wal replay rows | 0 | 0 | 0 | 0 |
| disk free | 853.8 GB | 853.8 GB | 854.7 GB | 854.7 GB |

**Zero data loss** — no overflow drops, no ring drops across all four writers over 3 days at ~600,000 msg/s combined ingest.

### Parquet Storage

| Mount | Size |
|---|---|
| `/mnt/tort-sdf/data-a/` | 7.7 GB |
| `/mnt/tort-sdf/data-b/` | 7.7 GB |
| `/mnt/tort-sdf/data-c/` | (same drive, unmeasured separately) |
| `/mnt/tort-sdf/data-d/` | (same drive, unmeasured separately) |
| **Total disk used** | ~1.1 TB (of 1.8 TB) |

Flush files are 60-second / 50k-row chunks. **No compaction** — the binary supports it (`compact_enabled` config key, default `false`, 10-minute interval) but the torture config does not enable it.

---

## Key Findings

1. **Sustained 600k msg/s with zero drops** for 3 days. Ring buffer (`ring_cap: 262144`) is working correctly — `ring_used` stays well below capacity.

2. **~160% CPU per writer** (~1.6 cores each) at 150k msg/s. Linear scaling across 4 instances — no contention observed.

3. **~750 MB RSS per writer** — consistent across all four. No memory growth over 3 days (stable).

4. **Flush latency**: avg 42–46 ms, max 310–431 ms. The 10-second flush interval produces files of 3k–50k rows depending on instantaneous rate.

5. **FlashMQ at 141% CPU / 105 MB** handling 600k msg/s fan-out to 4 subscribers. Very efficient.

6. **Compactor not enabled** — 60-second files accumulate. For production use at lower rates (Evelyn: 175–81,420 msg/s) compaction to 10-minute+ chunks should be enabled to avoid millions of small files.

---

## Next Steps

- Stop torture stack; keep `parquet-writer:latest` image
- Rebuild image with compactor enabled in config for new Evelyn demo stack
- New stack: 1× `ems_site_simulator` (smooth mode, native on host) → FlashMQ → 4× `parquet_writer` containers
- Rate: dinky Evelyn (175 msg/s / 1 unit) → scale to 81,420 msg/s / 46 units
- Enable `compact: enabled: true` in writer config (10-min chunks)

---

## Config Reference

```yaml
# writer-a.yaml (torture)
mqtt:
  client_id: parquet-writer-a
  host: broker
  port: 1883
  topic: "batteries_a/#"
  qos: 0
output:
  base_path: /data/parquet
  compression: snappy
  flush_interval_seconds: 10
  max_messages_per_part: 50000
  max_total_buffer_rows: 500000
health:
  enabled: true
  port: 8771
wal:
  enabled: false
  path: /wal
guard:
  min_free_gb: 1.0
# compact: not present — defaults to disabled
```
