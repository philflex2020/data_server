# writer.cpp — Aggressive Code Review

**6 CRITICAL, 14 MAJOR, 11 MINOR**

---

## CRITICAL

**C1 — Signal handler calls `notify_all` on `condition_variable` — undefined behaviour (lines 917–920)**
`std::condition_variable::notify_all()` is not async-signal-safe. If the signal fires while `g_mutex` is held by another thread, the notify path may deadlock. Only async-signal-safe operations (setting an `atomic_flag`, writing to a self-pipe/eventfd) are legal inside a signal handler.

**C2 — WAL write failure silently proceeds with the Parquet write — crash safety guarantee is void (lines 672–678)**
When `write_wal_ipc` fails, the code logs a warning and continues flushing Parquet. If the Parquet write then crashes, the data is permanently lost with no WAL to recover from. The flush for that partition should abort if the WAL cannot be written. Also, `writer->Close()` failure on line 510 is checked but `out->Close()` on line 511 has its return value discarded — the WAL file may be corrupt but the caller sees `OK`.

**C3 — WAL replay hard-codes `source_type = "batteries"` — silent data misrouting (line 577)**
The WAL filename encodes the source type (`{source_type}_{pval}_{ts}.wal.arrow`) but the replay loop never parses it. All replayed data, regardless of original source type, goes into a `batteries/` directory tree.

**C4 — `on_message` holds `g_mutex` for every single message at ~80k msg/sec — severe lock contention, blocks MQTT network thread (lines 878–900)**
The flush thread copies `g_current_state` (up to 10k `std::map` entries with heap-allocated keys) while holding the same lock. During that copy, the MQTT thread blocks and mosquitto's receive buffer fills. This is both a throughput bottleneck and a risk of missed keepalives causing spurious reconnects.

**C5 — `g_health_buffer_rows` not updated when buffer is flushed — health endpoint reports stale (inflated) buffer row count (lines 597, 761–763, 897)**
`g_total_buffered` is reset to 0 inside `flush_thread_fn` but `g_health_buffer_rows` is only updated in `on_message`. A consumer of the health endpoint will see the pre-flush count until the next message arrives.

**C6 — WAL replay only reads `chunk(0)` per column — silently drops all subsequent chunks in multi-chunk tables (lines 537–541)**
If the WAL IPC file contains multiple record batches, `arrow::ChunkedArray::chunk(0)` gives only the first chunk. All other rows are silently discarded on replay.

---

## MAJOR

**M1 — `parse_payload` default-constructs `Config{}` as fallback — wrong config silently used if argument omitted (line 241)**

**M2 — `g_received_since_sync` cast to `int64_t` can produce misleading "OK" sync logs when a large number was received before the first sync of a session (lines 823–824)**

**M3 — `flush_partition` and `write_wal_ipc` are ~60-line duplicates — schema-building logic is copy-pasted and has already diverged in variable names (lines 356–414 vs 472–512)**

**M4 — `make_output_path` uses flush time, not collection time — WAL-replayed data is written into today's date partition, not the partition for when it was collected (lines 314–343)**

**M5 — `g_current_state` is copied under `g_mutex` at flush time — O(N) heap allocations while the MQTT thread is blocked (lines 762–763)**

**M6 — `recv` return value is ignored in health thread; `buf` is uninitialised on error; slow HTTP clients with fragmented requests are not handled (line 617)**

**M7 — Health endpoint sends HTTP 200 to any TCP connection with no path or method parsing — port scanners and keepalive probes get JSON responses (lines 617–633)**

**M8 — `check_disk_space` skips updating `g_disk_free_gb_x10` when guard is disabled — health endpoint always reports 0.0 GB free unless `min_free_gb` is set (lines 440–452)**

**M9 — `mosquitto_disconnect` called after `loop_stop(force=true)` — DISCONNECT packet is never sent; broker retains session state. Correct order: disconnect → wait → loop_stop (lines 1044–1046)**

**M10 — `g_mosq` is a plain pointer with no atomic/fence — technically a data race under the C++ memory model between main-thread write and network-thread reads (lines 1005, 1038)**

**M11 — Final flush runs before `mosquitto_loop_stop` — messages delivered between flush thread exit and loop stop are silently discarded (lines 1041–1044). Guaranteed data loss window on every graceful shutdown.**

**M12 — No config validation — `flush_interval_seconds <= 0` creates a spin-flush loop; `max_total_buffer_rows <= 0` drops every message; invalid `compression` string silently falls through to Snappy (lines 117–168)**

**M13 — `source_type` from MQTT topic is injected directly into filesystem paths without sanitisation — a rogue publisher with a topic like `../../etc/cron.d/...` can write files anywhere writable by the process (line 334)**

**M14 — Flat integer scalars not in `int_cols` are silently promoted to `double` — values > 2^53 lose precision; if a payload field name matches a topic-derived int column the same field appears in both `row.ints` and `row.floats`, producing duplicate Parquet columns (lines 289–292)**

---

## MINOR

**m1 — Magic numbers: listen backlog 4, MAX_RETRIES 3, backoff 1/2/4s, reconnect delay 2/30s, keepalive 60s, retry sleep 5s — none configurable (lines 609, 683, 687, 1011, 1015, 1018)**

**m2 — Overflow `notify_one` only wakes the flush thread if it is sleeping; during an active flush the overflow drop continues unabated for the full flush duration (lines 880–887)**

**m3 — Parquet column order is non-deterministic across flush cycles (determined by first-seen order in row set) — confusing for debugging, though DuckDB handles it (lines 362–368)**

**m4 — `g_cfg` is a raw pointer to a stack `Config` in `main` — null dereference if any function referencing `g_cfg` is called from tests before the pointer is set (line 992)**

**m5 — `wal_path_for` calls `fs::create_directories` on every flush, once per partition — should be done once at startup (line 468)**

**m6 — `time_suffix()` and `make_output_path` call `system_clock::now()` independently — WAL filename and Parquet filename can differ by one second at a second boundary (lines 458–464, 316–325)**

**m7 — First sync after a new session always reports OK even if messages were dropped before the first `_sync` arrived — documented but not prominently (line 820)**

**m8 — `parse_topic` silently overwrites duplicate key-value segments in a topic path (lines 227–234)**

**m9 — `mosquitto_subscribe` failure in `on_connect` is logged but not retried — writer silently receives zero messages (lines 789–793)**

**m10 — `recv` buffer of 255 bytes is too small for Chrome/Firefox HTTP GET headers; partial reads silently ignored (line 617)**

**m11 — `mosquitto_lib_init()` return value is not checked; `lib_cleanup()` is called unconditionally even if init failed (lines 997, 1047)**

---

## Overall Verdict

The code is a solid functional prototype that works reliably under normal conditions but is **not production-hardened**. The core data path (MQTT → buffer → Parquet flush) is architecturally sound. The serious problems are concentrated in the robustness features (WAL, signal handling, shutdown):

- Any SIGTERM during a flush invokes undefined behaviour (C1)
- The WAL can silently mis-route recovered data (C3) and does not abort on write failure (C2)
- Every graceful shutdown discards a window of messages (M11)
- Health endpoint `buffer_rows` and `disk_free_gb` report incorrect values in default config (C5, M8)
- Path traversal via MQTT `source_type` is a real attack surface if exposed to untrusted publishers (M13)

**Recommended fix priority:** C1 → M11 → C3 → C2 → M9 → C5/M8 → M12 → C4 → C6 → M13
