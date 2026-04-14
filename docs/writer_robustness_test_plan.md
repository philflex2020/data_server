# parquet_writer Robustness Features — Test Plan

Test one feature at a time.  The existing `config.yaml` is unchanged throughout —
add the relevant stanza to a **test config** (`config_test.yaml`) and point the
writer at that.  Production writer on lp3 continues unaffected.

---

## Prerequisites

```bash
# From lp3 or phil-dev, repo root
cd source/parquet_writer_cpp
make parquet_writer

# Confirm help works and shows all three features
./parquet_writer --help
```

---

## Test 1 — Flush Metrics (always on, no config needed)

**What it does:** Prints row count, duration ms, and cumulative totals after every
flush.  Also collected in atomics for health endpoint.  Zero overhead.

**Test:**

```bash
./parquet_writer --config writer_config.yaml
# After first flush you should see:
# [flush] 12345 rows → /srv/data/parquet/batteries/...
# [flush] total=12345 rows in 42ms
```

**Pass criteria:** `[flush] total=N rows in Xms` line appears after each flush.
No change to Parquet output, no change to flush timing.

---

## Test 2 — Health Endpoint

**config_test.yaml additions:**
```yaml
health:
  enabled: true
  port: 8771
```

**Test:**
```bash
./parquet_writer --config config_test.yaml &
sleep 2
curl http://localhost:8771/health | python3 -m json.tool
```

**Expected response:**
```json
{
  "msgs_received": 12345,
  "buffer_rows": 800,
  "overflow_drops": 0,
  "flush_count": 2,
  "total_rows_written": 24000,
  "last_flush_rows": 12000,
  "last_flush_ms": 38,
  "wal_replay_rows": 0,
  "disk_free_gb": 42.3
}
```

**Pass criteria:**
- HTTP 200 with valid JSON on every poll
- `msgs_received` climbs at ~79k/sec with full stress runner
- `buffer_rows` oscillates between 0 and max before each flush
- `overflow_drops` stays 0 under normal load
- `wal_replay_rows` is 0 (WAL not enabled yet)

---

## Test 3 — Disk Space Guard

**config_test.yaml additions:**
```yaml
guard:
  min_free_gb: 1.0
```

**Test A — normal operation (plenty of space):**
```bash
./parquet_writer --config config_test.yaml
# Verify flushes proceed as normal — no [disk] log lines
```

**Pass criteria:** No `[disk]` messages, flushes continue normally.

**Test B — trigger the guard (safe simulation):**
```bash
# Temporarily set threshold above actual free space to force a skip
# Edit config_test.yaml: min_free_gb: 99999.0
./parquet_writer --config config_test.yaml
```

**Expected log:**
```
[disk] LOW SPACE: 42.3 GB free, minimum 99999.0 GB
[flush] SKIPPED — insufficient disk space
```

**Pass criteria:**
- Flush is skipped, not crashed
- Writer keeps running and accepting MQTT messages
- Next flush (after restoring normal threshold) succeeds and recovers buffered rows

---

## Test 4 — WAL (Write-Ahead Log)

**config_test.yaml additions:**
```yaml
wal:
  enabled: true
  # path: ""   # leave blank → {base_path}/.wal/
```

### 4a — Normal operation: WAL written and cleaned up

```bash
./parquet_writer --config config_test.yaml &
PID=$!

# Wait for one flush cycle, then check
sleep 70
ls /srv/data/parquet/.wal/    # should be EMPTY — WAL deleted after successful flush
```

**Pass criteria:** `.wal/` directory exists but is empty after each successful flush.
Log shows no `[wal]` error lines.

### 4b — Crash recovery: simulate hard kill mid-flush

This is the core test.  It proves the WAL survives a crash and is replayed.

```bash
# Step 1: start writer, let it fill buffer
./parquet_writer --config config_test.yaml &
PID=$!
sleep 65   # let buffer fill (just before flush at 60s, or wait for first flush log)

# Step 2: once you see the WAL write log line, kill hard
# Watch for: [wal] writing .wal.arrow
kill -9 $PID

# Step 3: confirm WAL file survived
ls -lh /srv/data/parquet/.wal/
# Should show one or more .wal.arrow files

# Step 4: restart — WAL replay should happen before MQTT connects
./parquet_writer --config config_test.yaml
# Expected log on startup:
# [wal] found 1 WAL file(s) — replaying
# [wal] replaying 12345 rows from batteries_0_20260320T...wal.arrow
# [wal] flushed 12345 rows → /srv/data/parquet/batteries/...
# [writer] topic='batteries/#' flush every 60s ...
```

**Pass criteria:**
- `.wal/` is empty after restart (WAL deleted post-replay)
- Replayed Parquet file exists and is readable:
  ```bash
  python3 -c "import pyarrow.parquet as pq; t = pq.read_table('/srv/data/parquet/batteries/...'); print(t.num_rows)"
  ```
- `wal_replay_rows` in health endpoint shows the recovered count
- No duplicate rows in production Parquet (WAL rows go to a new timestamped file)

### 4c — WAL survives multiple crashes (idempotent)

```bash
# Kill before replay completes (kill during replay flush)
# Restart again — second replay should handle the same WAL file safely
# Pass: no crash, no corruption, WAL eventually cleared
```

---

## Test 5 — All Three Together

Run with all features enabled simultaneously under full stress runner load
(4 units, ~7k msgs/sec) for 30 minutes.

**Pass criteria:**
- Zero overflow drops
- WAL directory stays empty during normal operation
- Health endpoint responsive throughout
- `flush_count` increments every ~60s
- `total_rows_written` matches expected: ~7000 rows/sec × 1800s = ~12.6M rows
- Parquet files in output directory are valid and queryable via DuckDB:

```sql
SELECT count(*) FROM read_parquet('/srv/data/parquet/batteries/**/*.parquet');
```

---

## Rollback

If any test causes unexpected behaviour, stop the test writer and restart
the production writer with the original config:

```bash
kill %1   # or pkill -f parquet_writer
./parquet_writer --config writer_config.yaml &
```

The original `config.yaml` has none of the new stanzas so all robustness
features remain disabled and behaviour is identical to pre-change.
