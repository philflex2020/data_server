# Broker Configuration Notes

## FlashMQ — client_initial_buffer_size

### The problem

At ~80k msg/s (QoS 0), system **mosquitto** silently drops messages because its
per-client socket write buffer is fixed at ~64 KB. Under load the buffer fills
faster than it drains, and QoS-0 packets are discarded without any error.

Symptoms observed on gx10-d94c:
- Writer C: 0 msg/s (complete stall)
- Writer D: ~7,800 msg/s vs expected 19,470 msg/s
- Writer A health port dead

### The fix

Use **FlashMQ** as the broker and set:

```
client_initial_buffer_size 4194304   # 4 MB per client
```

This gives each connected client a 4 MB write buffer, absorbing bursts without
dropping packets. At 81,420 msg/s with ~1 KB average payload that's ~870 ms of
headroom before any drops occur.

### Where it's applied

| Machine | Config file | Managed by |
|---|---|---|
| phil-256g (.34) | `/tmp/flashmq.conf` (written by `spark_start.sh`) | `docker/torture/spark_start.sh` |
| gx10 (.48) | `/tmp/flashmq-gx10-evelyn.conf` (written by start script) | `scripts/gx10-evelyn-4sim-start.sh` |

### gx10 config (written at runtime)

```
allow_anonymous true
storage_dir /tmp/flashmq-evelyn-data
client_initial_buffer_size 4194304
```

The start script (`gx10-evelyn-4sim-start.sh`) now owns the broker lifecycle:
stops any existing FlashMQ instance, writes this config, starts FlashMQ on
`:1883`, verifies it, then starts writers and simulators.

### Why not mosquitto

Mosquitto's write buffer size is not configurable — it's set at compile time.
FlashMQ exposes it as `client_initial_buffer_size` and is designed for
high-throughput pub/sub at 100k+ msg/s.

### Thread count

On NUMA machines (e.g. spark-22b6 dual-Xeon), set `thread_count 2` to avoid
cross-NUMA memory traffic. On single-socket machines (gx10, typical edge hosts)
leave unset — FlashMQ auto-detects core count.
