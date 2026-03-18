/**
 * parquet_sync_checker — chain-of-custody verifier at the Parquet layer.
 *
 * Subscribes to the same _sync heartbeats that writer.cpp and subscriber-api
 * monitor.  For each sync window it queries the Parquet files on disk and
 * compares the row count against the expected message count published by
 * stress_runner.  This is the deepest checkpoint in the chain:
 *
 *   stress_runner
 *     │
 *     ├─[MQTT :1883]─► writer.cpp        in-memory counter  (wire check)
 *     ├─[MQTT :1884]─► subscriber-api    in-memory counter  (wire check)
 *     └─[MQTT :1883]─► this tool         Parquet row count  (disk check) ← NEW
 *
 * A deficit at this level means messages arrived on the wire but were NOT
 * written to disk — write buffer overflow, flush failure, or process crash.
 *
 * Important: writer.cpp flushes every 60 s (configurable).  Rows from the
 * most recent flush window will appear as "missing" until the flush fires.
 * Include current_state.parquet (gap writer) with --dir to minimise the lag.
 *
 * Build:
 *   make
 *
 * Usage:
 *   ./parquet_sync_checker --dir /srv/data/parquet/ [--host HOST] [--port PORT]
 *   ./parquet_sync_checker --dir /srv/data/parquet/ --host localhost --port 1883 -v
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <mosquitto.h>
#include <simdjson.h>

// Arrow / Parquet — used by writer.cpp; same libraries available here.
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

// ── Config ────────────────────────────────────────────────────────────────────

struct Config {
    std::string parquet_dir = "/srv/data/parquet/";
    std::string mqtt_host   = "localhost";
    int         mqtt_port   = 1883;
    std::string client_id   = "parquet-sync-checker";
    bool        verbose     = false;
};

// ── Global state ──────────────────────────────────────────────────────────────

static Config            g_cfg;
static std::atomic<bool> g_stop{false};

// Per-session tracking
static std::string g_session_id;
static double      g_last_sync_ts   = 0.0;
static int64_t     g_cum_expected   = 0;   // cumulative expected rows (MQTT published)
static int64_t     g_cum_found      = 0;   // cumulative rows found in Parquet
static int64_t     g_cum_still_buf  = 0;   // estimated still in write buffer
static int         g_sync_count     = 0;   // syncs processed this session

// simdjson — one parser per thread (thread_local handles the single MQTT thread)
static thread_local simdjson::ondemand::parser g_parser;

// ── Parquet row counting ──────────────────────────────────────────────────────

/**
 * Return true if a Parquet file's row-group statistics indicate it *might*
 * contain rows with timestamp in [t0, t1).  Used to skip files cheaply.
 * Returns true if statistics are unavailable (conservative: always read).
 */
static bool file_might_overlap(const std::string& path, double t0, double t1) {
    auto open = arrow::io::ReadableFile::Open(path);
    if (!open.ok()) return true;  // can't tell — include

    parquet::arrow::FileReaderBuilder builder;
    if (!builder.Open(*open).ok()) return true;

    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::ArrowReaderProperties props;
    if (!builder.properties(props)->Build(&reader).ok()) return true;

    auto metadata = reader->parquet_reader()->metadata();
    int ts_col = metadata->schema()->ColumnIndex("timestamp");
    if (ts_col < 0) return true;   // no timestamp column — include

    // Check each row group's min/max statistics for the timestamp column.
    for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
        auto col_chunk = metadata->RowGroup(rg)->ColumnChunk(ts_col);
        auto stats     = col_chunk->statistics();
        if (!stats || !stats->HasMinMax()) return true;   // no stats — be safe

        // Statistics are stored as encoded bytes; for DOUBLE columns the
        // min/max are IEEE-754 doubles in little-endian order.
        const auto& min_enc = stats->EncodeMin();
        const auto& max_enc = stats->EncodeMax();
        if (min_enc.size() != 8 || max_enc.size() != 8) return true;

        double ts_min, ts_max;
        std::memcpy(&ts_min, min_enc.data(), 8);
        std::memcpy(&ts_max, max_enc.data(), 8);

        // Row group overlaps the window if ts_max >= t0 AND ts_min < t1
        if (ts_max >= t0 && ts_min < t1) return true;
    }
    return false;   // all row groups are outside [t0, t1)
}

/**
 * Count rows in a single Parquet file with timestamp in [t0, t1).
 * Reads only the "timestamp" column; all other columns are skipped.
 */
static int64_t count_rows_in_file(const std::string& path, double t0, double t1) {
    auto open = arrow::io::ReadableFile::Open(path);
    if (!open.ok()) {
        if (g_cfg.verbose)
            std::cerr << "  [skip] cannot open " << path << "\n";
        return 0;
    }

    parquet::arrow::FileReaderBuilder builder;
    if (!builder.Open(*open).ok()) return 0;

    std::unique_ptr<parquet::arrow::FileReader> reader;
    if (!builder.Build(&reader).ok()) return 0;

    // Locate the physical column index for "timestamp".
    auto pq_schema = reader->parquet_reader()->metadata()->schema();
    int pq_ts_col  = pq_schema->ColumnIndex("timestamp");
    if (pq_ts_col < 0) return 0;   // file has no timestamp column

    // Read only the timestamp column as a ChunkedArray.
    std::shared_ptr<arrow::ChunkedArray> ts_col;
    if (!reader->ReadColumn(pq_ts_col, &ts_col).ok()) return 0;

    int64_t count = 0;
    for (int c = 0; c < ts_col->num_chunks(); ++c) {
        // writer.cpp stores timestamp as DOUBLE (unix seconds).
        if (ts_col->chunk(c)->type_id() != arrow::Type::DOUBLE) continue;
        auto arr = std::static_pointer_cast<arrow::DoubleArray>(ts_col->chunk(c));
        for (int64_t i = 0; i < arr->length(); ++i) {
            if (arr->IsValid(i)) {
                double ts = arr->Value(i);
                if (ts >= t0 && ts < t1) ++count;
            }
        }
    }

    if (g_cfg.verbose && count > 0)
        std::cout << "    " << fs::path(path).filename().string()
                  << "  rows=" << count << "\n";
    return count;
}

/**
 * Recursively scan g_cfg.parquet_dir for all .parquet files and sum rows
 * with timestamp in [t0, t1).  Skips files whose row-group statistics
 * prove they cannot overlap the window.
 */
static int64_t count_parquet_rows(double t0, double t1) {
    std::error_code ec;
    if (!fs::exists(g_cfg.parquet_dir, ec)) {
        std::cerr << "[error] parquet_dir not found: " << g_cfg.parquet_dir << "\n";
        return -1;
    }

    int64_t total     = 0;
    int     checked   = 0;
    int     skipped   = 0;

    for (const auto& entry : fs::recursive_directory_iterator(g_cfg.parquet_dir, ec)) {
        if (ec) { ec.clear(); continue; }
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".parquet") continue;

        const std::string path = entry.path().string();

        // Quick stats-based filter before reading any column data.
        if (!file_might_overlap(path, t0, t1)) {
            ++skipped;
            continue;
        }

        total += count_rows_in_file(path, t0, t1);
        ++checked;
    }

    if (g_cfg.verbose)
        std::cout << "    files checked=" << checked
                  << " skipped=" << skipped << "\n";
    return total;
}

// ── _sync handler ─────────────────────────────────────────────────────────────

static void handle_sync(const void* payload, int len) {
    // Parse _sync JSON using simdjson
    simdjson::padded_string json(static_cast<const char*>(payload), len);
    simdjson::ondemand::document doc;
    if (g_parser.iterate(json).get(doc) != simdjson::SUCCESS) return;

    std::string_view session_sv;
    int64_t seq = 0, interval_pub = 0, total_pub = 0;
    double  sync_ts = 0.0;
    std::string_view mode_sv;

    if (doc["session_id"].get(session_sv)          != simdjson::SUCCESS) return;
    if (doc["seq"].get(seq)                         != simdjson::SUCCESS) return;
    if (doc["interval_published"].get(interval_pub) != simdjson::SUCCESS) return;
    if (doc["total_published"].get(total_pub)       != simdjson::SUCCESS) return;
    if (doc["timestamp"].get(sync_ts)               != simdjson::SUCCESS) return;
    doc["topic_mode"].get(mode_sv);   // optional

    std::string session(session_sv);
    std::string mode(mode_sv.empty() ? "per_cell" : mode_sv);

    // ── New session ───────────────────────────────────────────────────────────
    if (session != g_session_id) {
        if (!g_session_id.empty()) {
            // Print final summary for the old session
            int64_t cum_delta = g_cum_found - g_cum_expected;
            std::cout << "\n  ── Session " << g_session_id << " ended"
                      << "  expected=" << g_cum_expected
                      << "  found="    << g_cum_found
                      << "  delta="    << cum_delta
                      << (cum_delta >= 0 ? "  ✓" : "  ✗") << "\n\n";
        }
        g_session_id    = session;
        g_last_sync_ts  = sync_ts;
        g_cum_expected  = 0;
        g_cum_found     = 0;
        g_cum_still_buf = 0;
        g_sync_count    = 0;
        std::cout << "  New session: " << session << "  mode=" << mode << "\n";
        return;   // skip comparison — no window established yet
    }

    double t_start = g_last_sync_ts;
    double t_end   = sync_ts;
    ++g_sync_count;

    // ── Query Parquet files ───────────────────────────────────────────────────
    if (g_cfg.verbose)
        std::cout << "  Querying [" << std::fixed << std::setprecision(1)
                  << t_start << ", " << t_end << ")...\n";

    int64_t found = count_parquet_rows(t_start, t_end);
    int64_t delta = (found >= 0) ? (found - interval_pub) : 0;

    g_cum_expected += interval_pub;
    if (found >= 0) g_cum_found += found;

    // ── Format and print ──────────────────────────────────────────────────────
    char tbuf[9];
    auto now = std::chrono::system_clock::now();
    auto tt  = std::chrono::system_clock::to_time_t(now);
    std::strftime(tbuf, sizeof(tbuf), "%H:%M:%S", std::localtime(&tt));

    std::cout << tbuf
              << "  #" << std::setw(4) << std::left << seq
              << "  window=" << std::fixed << std::setprecision(0)
              << t_start << "–" << t_end
              << "  expected=" << std::setw(7) << std::right << interval_pub
              << "  parquet=" << std::setw(7) << (found >= 0 ? found : -1)
              << "  delta=" << std::setw(7) << delta;

    if (found < 0) {
        std::cout << "  [DIR NOT FOUND]\n";
    } else if (delta >= 0) {
        std::cout << "  OK";
        if (delta > 0)
            std::cout << "  (+" << delta << " extra — possible row group overlap)";
        std::cout << "\n";
    } else {
        // Negative delta: rows missing from Parquet
        // writer.cpp flush_interval is typically 60 s; rows in the last flush
        // window won't appear until the next flush fires.
        std::cout << "  MISSING " << -delta << " rows"
                  << "  (may still be in 60 s write buffer)\n";
    }

    // Cumulative summary every 6 syncs (≈ 1 minute at 10 s sync interval)
    if (g_sync_count % 6 == 0) {
        int64_t cum_delta = g_cum_found - g_cum_expected;
        std::cout << "\n  ── #" << seq << " cumulative"
                  << "  expected=" << g_cum_expected
                  << "  found="    << g_cum_found
                  << "  delta="    << cum_delta;
        if (cum_delta < 0)
            std::cout << "  ⚠  deficit (flush lag or real drops)\n\n";
        else
            std::cout << "  ✓\n\n";
    }

    g_last_sync_ts = sync_ts;
}

// ── MQTT callbacks ────────────────────────────────────────────────────────────

static struct mosquitto* g_mosq = nullptr;

static void on_connect(struct mosquitto* mosq, void* /*userdata*/, int rc) {
    if (rc != 0) {
        std::cerr << "[mqtt] connect failed rc=" << rc << "\n";
        return;
    }
    std::cout << "[mqtt] connected → " << g_cfg.mqtt_host << ":" << g_cfg.mqtt_port
              << "  subscribing to _sync\n";
    mosquitto_subscribe(mosq, nullptr, "_sync", 0);
}

static void on_message(struct mosquitto* /*mosq*/, void* /*userdata*/,
                       const struct mosquitto_message* msg) {
    if (!msg || !msg->payload) return;
    if (std::string_view(msg->topic) != "_sync") return;
    handle_sync(msg->payload, msg->payloadlen);
}

static void on_disconnect(struct mosquitto* /*mosq*/, void* /*userdata*/, int rc) {
    if (rc != 0)
        std::cerr << "[mqtt] unexpected disconnect rc=" << rc << "\n";
}

// ── Signal handler ────────────────────────────────────────────────────────────

static void sig_handler(int) { g_stop = true; }

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--dir"  && i+1 < argc) g_cfg.parquet_dir = argv[++i];
        else if (a == "--host" && i+1 < argc) g_cfg.mqtt_host   = argv[++i];
        else if (a == "--port" && i+1 < argc) g_cfg.mqtt_port   = std::stoi(argv[++i]);
        else if (a == "--id"   && i+1 < argc) g_cfg.client_id   = argv[++i];
        else if (a == "-v" || a == "--verbose") g_cfg.verbose    = true;
        else if (a == "--help") {
            std::cout <<
                "parquet_sync_checker — verifies Parquet row counts at _sync checkpoints\n\n"
                "Usage: parquet_sync_checker [options]\n\n"
                "Options:\n"
                "  --dir DIR     Parquet directory to scan  (default: /srv/data/parquet/)\n"
                "  --host HOST   MQTT broker host           (default: localhost)\n"
                "  --port PORT   MQTT broker port           (default: 1883)\n"
                "  --id ID       MQTT client identifier\n"
                "  -v            Verbose: show per-file row counts and skip stats\n"
                "  --help        This message\n\n"
                "Notes:\n"
                "  - Connects to MQTT and waits for _sync heartbeats (every 10 s by default).\n"
                "  - For each window, counts Parquet rows with timestamp in that range.\n"
                "  - Rows in writer.cpp's 60 s write buffer will appear as 'MISSING' until\n"
                "    the next flush.  Run alongside the gap writer for tighter coverage.\n"
                "  - A negative cumulative delta after >60 s indicates real data loss.\n";
            return 0;
        }
    }

    // Normalise directory path
    if (!g_cfg.parquet_dir.empty() && g_cfg.parquet_dir.back() != '/')
        g_cfg.parquet_dir += '/';

    std::cout << "parquet_sync_checker\n"
              << "  dir:  " << g_cfg.parquet_dir << "\n"
              << "  mqtt: " << g_cfg.mqtt_host << ":" << g_cfg.mqtt_port << "\n"
              << "  note: writer.cpp flush_interval=60 s — recent rows may\n"
              << "        appear as MISSING until the flush fires.\n\n"
              << "  Waiting for _sync heartbeats...\n\n";

    std::signal(SIGINT,  sig_handler);
    std::signal(SIGTERM, sig_handler);

    mosquitto_lib_init();
    g_mosq = mosquitto_new(g_cfg.client_id.c_str(), /*clean_session=*/true, nullptr);
    if (!g_mosq) {
        std::cerr << "mosquitto_new failed\n";
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_connect_callback_set   (g_mosq, on_connect);
    mosquitto_message_callback_set   (g_mosq, on_message);
    mosquitto_disconnect_callback_set(g_mosq, on_disconnect);

    // Connect with exponential back-off
    double delay = 1.0;
    while (!g_stop) {
        int rc = mosquitto_connect(g_mosq, g_cfg.mqtt_host.c_str(), g_cfg.mqtt_port, 60);
        if (rc == MOSQ_ERR_SUCCESS) break;
        std::cerr << "[mqtt] connect failed (" << mosquitto_strerror(rc)
                  << ") — retrying in " << static_cast<int>(delay) << " s\n";
        std::this_thread::sleep_for(std::chrono::duration<double>(delay));
        delay = std::min(delay * 2.0, 30.0);
    }

    mosquitto_loop_start(g_mosq);

    while (!g_stop)
        std::this_thread::sleep_for(100ms);

    mosquitto_loop_stop(g_mosq, false);
    mosquitto_disconnect(g_mosq);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();

    // Final session summary
    if (!g_session_id.empty() && g_sync_count > 0) {
        int64_t cum_delta = g_cum_found - g_cum_expected;
        std::cout << "\nFinal summary  session=" << g_session_id << "\n"
                  << "  syncs processed : " << g_sync_count << "\n"
                  << "  expected (MQTT) : " << g_cum_expected << " rows\n"
                  << "  found (Parquet) : " << g_cum_found    << " rows\n"
                  << "  delta           : " << cum_delta      << "\n";
        if (cum_delta < 0)
            std::cout << "  ⚠  Deficit — check flush_interval and overflow drops\n";
        else
            std::cout << "  ✓  All rows accounted for in Parquet\n";
    }

    return 0;
}
