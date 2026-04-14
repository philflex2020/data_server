# Subscriber API — Aggressive Critical Code Review

**Reviewed:** `subscriber/api/server.py` + `subscriber/api/flux_compat.py`
**Date:** 2026-03-21

---

## Executive Summary

Reasonably structured but has **4 critical correctness/thread-safety bugs** that cause data
corruption or crashes under real load. The 80k msg/sec MQTT path will saturate the event loop.
The hand-rolled HTTP server has injection and parse-safety holes.

---

## CRITICAL

**C1 — DuckDB shared connection used from multiple threads without locking**
`server.py` lines 316, 493–495, 755 · `flux_compat.py` line 611

`g_duckdb` is a single `duckdb.DuckDBPyConnection` shared across all `run_in_executor`
thread-pool calls. `duckdb.DuckDBPyConnection` is not thread-safe. In "both" router mode,
`_influx_query` and `_duckdb_max_ts` are dispatched simultaneously (`asyncio.gather`,
`flux_compat.py` lines 938–941) while a WebSocket history query may also be in flight —
guaranteed concurrent `execute()` calls. `run_hybrid_query` also calls
`g_duckdb.register("_live_buffer_view", buf_df)` (server.py line 717), mutating shared
connection state mid-query.

**Fix:** per-request `duckdb.connect()` or a single-threaded dedicated DuckDB executor.

---

**C2 — `run_buffer_query` blocks the event loop for 100–500ms**
`server.py` lines 483, 676–693

The buffer-only query path calls `run_buffer_query` directly on the event loop thread (no
`run_in_executor`). On an 8M-row buffer, `to_dataframe()` allocates a 64 MB index array,
fancy-indexes 14 columns, and constructs a pandas DataFrame — all blocking MQTT ingestion
and all WebSocket clients. The hybrid and parquet paths correctly use `run_in_executor`.
This inconsistency is unintentional.

**Fix:** one-line `await loop.run_in_executor(None, run_buffer_query, ...)` wrap.

---

**C3 — `oldest_ts()` wrong for out-of-order messages → wrong query routing → silent data gaps**
`server.py` lines 157–162

When `_count < _maxlen`, `oldest_ts()` returns `self._cols["timestamp"][0]` (slot 0). This
is only correct for monotonically increasing timestamps. At 80k msg/sec over an MQTT bridge,
out-of-order delivery is common. A query routed to "buffer-only" because
`from_ts >= buf_oldest` (line 479) may silently return incomplete data instead of falling
through to parquet. User gets wrong data with no error message.

**Fix:** track `_min_ts` as a separate atomic updated on every insert.

---

**C4 — Deque fallback path crashes on `.oldest_ts()` and `.to_dataframe()` calls**
`server.py` lines 209–210, 475–476

If numpy is unavailable, `g_live_buffer` is a `collections.deque`. The deque has no
`oldest_ts()`, `newest_ts()`, or `to_dataframe()`. The guard at line 477 is
`has_buf = g_live_state_enabled and bool(g_live_buffer)` — it checks `live_state_enabled`,
not numpy availability. With `live_state: enabled: true` in config and numpy absent, the
server crashes with `AttributeError` on the first history query.

**Fix:** add `and hasattr(g_live_buffer, 'oldest_ts')` to the guard.

---

## MAJOR

**M1 — Event loop saturated at 80k msg/sec**
`server.py` lines 572–575, 591–674

`json.loads` + 14 numpy scalar writes happen on the event loop for every message. At 80k
msg/sec, `json.loads` alone consumes ~400ms–1.2s of CPU per wall-second in CPython. No
headroom for queries or broadcasts.

**Quick fix:** `orjson` + move `append()` to `run_in_executor`.

---

**M2 — No backpressure on `broadcast()`; one slow client stalls the server**
`server.py` lines 364–368

`ws.send()` with no timeout blocks on slow TCP windows. One backgrounded browser tab stalls
all MQTT processing until the client's kernel buffer drains.

**Fix:** `asyncio.wait_for(ws.send(...), timeout=0.1)` per client, drop slow clients.

---

**M3 — SQL injection via `_parse_filters`**
`flux_compat.py` lines 379–383, 481–484

Flux filter values are interpolated as `'...'` with no escaping. Port 8768 has no
authentication.

**Fix:** parameterized queries via `duckdb_conn.execute(sql, params)`.

---

**M4 — Flux query path scans all parquet files regardless of time range**
`flux_compat.py` lines 422–424

`flux_to_sql` uses `project=*/**/*.parquet` with no date scoping. `run_history_query` in
server.py correctly uses `_date_paths()`. The Flux path doesn't. At scale (weeks of 80k
msg/sec data) this will be multi-second per query. Existing TODO at line 408 admits this.

---

**M5 — `g_duckdb.register()` in hybrid query overwrites concurrent query's view**
`server.py` line 717

Two simultaneous hybrid queries both call `g_duckdb.register("_live_buffer_view", buf_df)`.
The second overwrites the first's registered DataFrame mid-execution. Silent data corruption,
compounded by C1.

---

**M6 — `_gap_fill_exists_flux` makes an S3 API call on every Flux query**
`flux_compat.py` lines 521–545

No caching, no TTL. Every Flux query synchronously calls boto3 `list_objects_v2`. Under
"both" route mode with dashboard polling this runs constantly.

**Fix:** cache result with a 60-second TTL.

---

**M7 — `asyncio.get_event_loop()` deprecated**
`flux_compat.py` lines 242, 344

Deprecated since Python 3.10 in async contexts; raises in Python 3.12+.

**Fix:** replace with `asyncio.get_running_loop()`.

---

## MINOR

- **m1** Dead variable `g_stream_first_ts` (server.py line 205) — NATS leftover, remove
- **m2** `_rate_window_start = 0.0` means first-second rate is always wrong and subsampling stride is 1 (floods clients on startup) — init to `time.monotonic()`
- **m3** Module default `BUFFER_MAXLEN = 1_080_000` conflicts with config `8_640_000` — running without config gives 8× smaller buffer than intended
- **m4** `get_status` response omits `live_state_enabled` and `buffer_size` that `stats` includes — protocol inconsistency
- **m5** `_telegraf_set_buffer` calls `time.sleep(1.5)` in thread pool (flux_compat.py line 775) — blocks a thread-pool worker for no reason
- **m6** boto3 `head_bucket` in `check_s3` has no explicit timeout — can block thread-pool for 60 seconds
- **m7** Missing `Connection: close` header in HTTP responses (same push_agent bug)
- **m8** README describes NATS JetStream; server uses MQTT — wrong document for current code

---

## C++ Verdict

**Rewrite the MQTT ingest path only. Keep Python for DuckDB, WebSocket, and HTTP.**

The DuckDB query engine is already C++. Python overhead there is negligible. The WebSocket
and HTTP layers are not bottlenecks. Only the MQTT → ring buffer path genuinely exceeds
Python's capacity at 80k msg/sec.

**Options in order of effort:**

1. **1 day** — `orjson` + move `append()` to `run_in_executor` → extends to ~60–70k msg/sec
2. **1 week** — Dedicated ingest thread with `queue.Queue`, batch numpy writes
3. **2–4 weeks** — C++ MQTT ingest writing to shared memory ring buffer; Python reads via `numpy.frombuffer` (zero copy, no GIL)

**The ring buffer may be the wrong architecture.** writer.cpp flushes every ~60 seconds.
Covering that gap at 80k msg/sec requires 4.8M entries per minute — the 8,640,000 entry
buffer covers ~108 seconds. If writer.cpp flushed every 10–15 seconds the gap-fill buffer
would be 10× smaller and the whole ring buffer complexity disappears.

**Fix the four critical bugs first. They cause wrong data today, regardless of throughput.**
