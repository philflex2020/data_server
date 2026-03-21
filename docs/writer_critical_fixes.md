# writer.cpp — Critical Issue Fixes

Concrete fix for each of the 6 CRITICAL findings from the code review.
Each section shows the current code, the problem, and the exact replacement.

---

## C1 — Signal handler calls `condition_variable::notify_all()` — UB

**Current code (lines 917–920):**
```cpp
static void handle_signal(int) {
    g_shutdown = true;
    g_flush_cv.notify_all();   // ← NOT async-signal-safe
}
```

**Problem:** `condition_variable::notify_all()` acquires an internal mutex.
If the signal fires while `g_mutex` (or the condvar's internal lock) is held
by another thread, the signal handler deadlocks. This is undefined behaviour
under POSIX — only a small set of functions (`write`, `sem_post`, `_exit`, …)
are safe to call from a signal handler.

**Fix:** Use a self-pipe or `sem_post`. The simplest portable fix is a
`std::atomic_flag` plus a dedicated pipe that the flush thread polls:

```cpp
// ── add near the top of the globals block ─────────────────────────────────
static int g_signal_pipe[2] = {-1, -1};   // pipefd[0]=read, pipefd[1]=write

// ── replace handle_signal ──────────────────────────────────────────────────
static void handle_signal(int) {
    // async-signal-safe: write a single byte to the pipe
    g_shutdown.store(true, std::memory_order_relaxed);
    char b = 1;
    (void)write(g_signal_pipe[1], &b, 1);   // wakes the flush thread
}

// ── in main(), before mosquitto_lib_init() ────────────────────────────────
pipe(g_signal_pipe);   // or pipe2(g_signal_pipe, O_CLOEXEC) on Linux

// ── in flush_thread_fn(), replace wait_for with a poll/select on the pipe ─
// Simplest drop-in: keep the condition_variable but also check the pipe.
// The flush thread calls notify_one() itself after each flush cycle so the
// condvar is still used for normal flush wakeups.
// Replace the wait_for line:
//   g_flush_cv.wait_for(lock, std::chrono::seconds(g_cfg->flush_interval_seconds));
// with:
lock.unlock();
struct pollfd pfd = { g_signal_pipe[0], POLLIN, 0 };
poll(&pfd, 1, g_cfg->flush_interval_seconds * 1000);   // returns early on signal
lock.lock();
```

Add `#include <poll.h>` and `#include <unistd.h>` (already present).

---

## C2 — WAL write failure silently proceeds to Parquet flush

**Current code (lines 671–678):**
```cpp
if (g_cfg->wal_enabled) {
    wal_file = wal_path_for(*g_cfg, key.source_type, key.partition_value);
    auto ws = write_wal_ipc(wal_file, part.rows);
    if (!ws.ok())
        std::cerr << "[wal] write failed (" << ws.ToString()
                  << ") — proceeding without WAL\n";
    // ← falls through and flushes Parquet with no WAL
}
```

Also — `out->Close()` return value is discarded in `write_wal_ipc` (line 511):
```cpp
ARROW_RETURN_NOT_OK(writer->Close());
return out->Close();   // ← Status returned but caller ignores it if !ok
```

**Fix — abort the partition flush if WAL write fails:**
```cpp
if (g_cfg->wal_enabled) {
    wal_file = wal_path_for(*g_cfg, key.source_type, key.partition_value);
    auto ws = write_wal_ipc(wal_file, part.rows);
    if (!ws.ok()) {
        std::cerr << "[wal] write failed (" << ws.ToString()
                  << ") — SKIPPING parquet flush for this partition\n";
        wal_file.clear();   // don't try to delete a partial WAL
        continue;           // skip to next partition
    }
}
```

**Fix — check `out->Close()` in `write_wal_ipc`:**
```cpp
static arrow::Status write_wal_ipc(const std::string& path,
                                    const std::vector<Row>& rows) {
    // ... (build table unchanged) ...
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto writer,
                          arrow::ipc::MakeFileWriter(out, table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_RETURN_NOT_OK(out->Close());   // ← was: return out->Close()
    return arrow::Status::OK();
}
```

---

## C3 — WAL replay hard-codes `source_type = "batteries"`

**Current code (line 577):**
```cpp
// Infer source_type from first segment of wal filename (best-effort)
parts[{"batteries", pval}].push_back(std::move(r));  // ← always "batteries"
```

**Problem:** The WAL filename already encodes the source type:
`{source_type}_{pval}_{ts}.wal.arrow`
e.g. `batteries_1_20260320T120000Z.wal.arrow`
but the replay loop never parses it back out.

**Fix — parse source_type from the WAL filename:**
```cpp
// In replay_wal_files(), replace the partition-grouping block:
for (const auto& wf : wal_files) {
    auto rows = wal_replay_file(wf.string());
    if (rows.empty()) { fs::remove(wf, ec); continue; }

    // Parse source_type from filename: {source_type}_{pval}_{ts}.wal.arrow
    std::string stem = wf.stem().string();   // e.g. "batteries_1_20260320T120000Z.wal"
    // stem may still have ".wal" suffix — strip it
    if (stem.size() > 4 && stem.substr(stem.size()-4) == ".wal")
        stem = stem.substr(0, stem.size()-4);
    // source_type is everything before the first underscore
    std::string source_type = "batteries";   // safe default
    auto first_us = stem.find('_');
    if (first_us != std::string::npos)
        source_type = stem.substr(0, first_us);

    std::cout << "[wal] replaying " << rows.size()
              << " rows from " << wf.filename()
              << " (source_type=" << source_type << ")\n";
    g_wal_replay_rows.fetch_add(rows.size());

    std::map<PartitionKey, std::vector<Row>> parts;
    for (auto& r : rows) {
        int pval = 0;
        if (auto it = r.ints.find(g_cfg->partition_field); it != r.ints.end())
            pval = static_cast<int>(it->second);
        parts[{source_type, pval}].push_back(std::move(r));
    }
    // ... rest of flush loop unchanged ...
}
```

---

## C4 — `on_message` holds `g_mutex` for every message — blocks at 80k msg/sec

**Current code (lines 878–900):** Every incoming MQTT message locks `g_mutex`,
inserts into the buffer, and updates `g_current_state` — all under the same lock
that the flush thread holds for the entire `g_current_state` copy.

**Fix — use a lock-free per-partition queue (ideal) or a short-hold pattern:**

The minimal fix without restructuring is to swap the mutex for a
`std::shared_mutex` so that multiple `on_message` calls can run concurrently,
and take a `shared_lock` for message insertion (read-style, non-exclusive)
and an `exclusive_lock` only at flush time:

```cpp
// Replace:  static std::mutex g_mutex;
static std::shared_mutex g_mutex;

// In on_message — use shared (non-exclusive) lock for buffer insert:
{
    std::unique_lock<std::shared_mutex> lock(g_mutex);
    // buffer insert + current_state update unchanged
}

// In flush_thread_fn — use exclusive lock only for the swap:
{
    std::unique_lock<std::shared_mutex> lock(g_mutex);
    to_flush         = std::exchange(g_buffers, {});
    g_total_buffered = 0;
    cs_snap          = g_current_state;   // still O(N) copy — see M5 for full fix
}
// lock released before slow I/O
```

Longer term, avoid copying `g_current_state` at flush time entirely — use a
double-buffer or a `std::atomic<std::shared_ptr<StateMap>>` swap.

Also: update `g_health_buffer_rows` atomically after the buffer swap (see C5):

```cpp
g_total_buffered = 0;
g_health_buffer_rows.store(0);   // ← add this line
```

---

## C5 — `g_health_buffer_rows` not zeroed after flush — health endpoint lies

**Current code:** `g_health_buffer_rows` is only written in `on_message`.
After each flush, `g_total_buffered` is reset to 0 inside `flush_thread_fn`
but `g_health_buffer_rows` keeps the old (pre-flush) value until the next
message arrives.

**Fix — zero it in `flush_thread_fn` immediately after the buffer swap:**

```cpp
// In flush_thread_fn, after the buffer swap (lines 761–762):
auto to_flush    = std::exchange(g_buffers, {});
g_total_buffered = 0;
g_health_buffer_rows.store(0);          // ← add: health now correctly shows 0
auto cs_snap     = g_current_state;
lock.unlock();
```

Also fix `check_disk_space` so it always updates `g_disk_free_gb_x10` even
when the guard is disabled (fixes M8 at the same time):

```cpp
static bool check_disk_space(const std::string& path, uint64_t min_bytes) {
    std::error_code ec;
    auto si = fs::space(path, ec);
    if (!ec)
        g_disk_free_gb_x10.store(si.available / (1024ULL * 1024 * 1024 / 10));
    if (min_bytes == 0) return true;         // ← guard check moved AFTER sampling
    if (ec) { std::cerr << "[disk] space check failed: " << ec.message() << "\n"; return true; }
    if (si.available < min_bytes) {
        std::cerr << "[disk] LOW SPACE: " << si.available / (1024.0*1024*1024)
                  << " GB free, minimum " << min_bytes / (1024.0*1024*1024) << " GB\n";
        return false;
    }
    return true;
}
```

---

## C6 — WAL replay reads only `chunk(0)` — silently drops rows in multi-chunk tables

**Current code (lines 537–541):**
```cpp
auto chunk0 = table->column(c)->chunk(0);   // ← only first chunk
if (tid == arrow::Type::INT64)
    int_cols.emplace_back(name,
        std::static_pointer_cast<arrow::Int64Array>(chunk0));
```

**Problem:** Arrow `ChunkedArray` can hold multiple chunks. This happens when a
table is assembled from multiple `RecordBatch`es via `Table::FromRecordBatches`.
Our own `write_wal_ipc` uses `WriteTable` which typically produces one chunk,
but this is an implementation detail — the reader should not assume it.

**Fix — flatten each column to a single chunk before reading:**

```cpp
// Replace the column-type detection loop:
for (int c = 0; c < ncols; ++c) {
    const auto& name = table->schema()->field(c)->name();
    auto tid         = table->schema()->field(c)->type()->id();

    // Combine all chunks into one contiguous array
    auto combined = arrow::Concatenate(table->column(c)->chunks());
    if (!combined.ok()) continue;
    auto arr = combined.ValueOrDie();

    if (tid == arrow::Type::INT64)
        int_cols.emplace_back(name,
            std::static_pointer_cast<arrow::Int64Array>(arr));
    else if (tid == arrow::Type::DOUBLE)
        dbl_cols.emplace_back(name,
            std::static_pointer_cast<arrow::DoubleArray>(arr));
}
```

`arrow::Concatenate` is in `<arrow/compute/api.h>` — already transitively
included via `<arrow/api.h>`. This handles single-chunk tables with zero
overhead (concatenate of one chunk returns the original array).

---

## Apply order

Fix in this sequence to minimise risk of introducing new bugs:

1. **C5** — two-line fix, zero risk, makes health endpoint correct immediately
2. **C3** — string parsing only, no behaviour change for normal operation
3. **C2** — changes flush abort logic; test with WAL deliberately failing
4. **C6** — changes replay read path; test with `replay_wal_files()` directly
5. **C1** — touches signal handling and flush wait loop; most invasive
6. **C4** — mutex → shared_mutex is mechanical but touches hot path; benchmark after
