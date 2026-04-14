/**
 * kpi_real_writer.cpp  —  throughput benchmarks for real_writer.cpp
 *
 * KPIs measured:
 *   1. Parse throughput      — parse_topic_positional + parse_iso8601 + parse_payload msg/s
 *                              Target: >> 350 000 msg/s (single stress_real_pub instance on gx10)
 *   2. Parquet write speed   — flush_partition with EMS string columns
 *                              at batch sizes 1k / 10k / 50k rows
 *   3. WAL round-trip speed  — write_wal_ipc + wal_replay_file at 10k rows
 *   4. End-to-end ingest sim — parse + buffer + flush at gx10 target rate
 *                              (4 writers × 350k/s = 1.4M msg/s; per-writer: 350k/s)
 *
 * Build & run:
 *   make kpi_real
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#include "../real_writer.cpp"
#pragma GCC diagnostic pop

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using Sec   = std::chrono::duration<double>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string tmp_dir() {
    char tmpl[] = "/tmp/kpi_rw_XXXXXX";
    return std::string(mkdtemp(tmpl));
}

static double elapsed_sec(Clock::time_point t0) {
    return Sec(Clock::now() - t0).count();
}

static void print_kpi(const std::string& label, double elapsed, long n,
                       long bytes = 0) {
    double rate = n / elapsed;
    std::cout << std::left  << std::setw(48) << label
              << std::right << std::setw(12) << std::fixed << std::setprecision(0) << rate;
    if (bytes > 0) {
        double mbs = (bytes / 1e6) / elapsed;
        std::cout << " ops/s   " << std::setw(7) << std::setprecision(1) << mbs << " MB/s";
    } else {
        std::cout << " ops/s";
    }
    std::cout << "   (" << std::setprecision(3) << elapsed << "s for " << n << ")\n";
}

// EMS topic segments: unit/{unit_id}/{device}/{instance}/{point_name}/{dtype_hint}
static const std::vector<std::string> EMS_SEGMENTS {
    "_", "unit_id", "device", "instance", "point_name", "dtype_hint"
};

// Representative EMS payload
static const std::string EMS_PAYLOAD =
    "{\"ts\":\"2024-11-15T21:27:52.775Z\",\"value\":0.999393}";

// Representative ISO8601 timestamp
static const std::string EMS_TS = "2024-11-15T21:27:52.775Z";

// Build a realistic EMS topic for unit u, device index d, point p
static std::string ems_topic(int u, int d, int p) {
    static const char* devices[]  = {"ems", "bms", "pcs"};
    static const char* points[]   = {"voltage", "current", "temperature", "soc", "power", "frequency"};
    return "unit/u" + std::to_string(u) + "/" +
           devices[d % 3] + "/0/" +
           points[p % 6] + "/float";
}

// Build a config matching the positional EMS parser
static Config make_ems_cfg() {
    Config cfg;
    cfg.topic_parser   = TopicParser::POSITIONAL;
    cfg.topic_segments = EMS_SEGMENTS;
    cfg.site_id        = "site-A";
    cfg.timestamp_field  = "ts";
    cfg.timestamp_format = "iso8601";
    cfg.wal_path = "";
    return cfg;
}

// ---------------------------------------------------------------------------
// KPI 1 — parse throughput
// ---------------------------------------------------------------------------

static void kpi_parse_throughput() {
    std::cout << "\n--- KPI 1: parse throughput ---\n";

    // Build a topic set matching one stress_real_pub instance on gx10:
    // 4 units × 12 racks × 10 devices × 6 points = 2880 topics
    std::vector<std::string> topics;
    for (int u = 0; u < 4; u++)
        for (int d = 0; d < 30; d++)
            for (int p = 0; p < 6; p++)
                topics.push_back(ems_topic(u, d, p));

    const long N = 1'000'000;
    const Config cfg = make_ems_cfg();

    // parse_topic_positional only
    {
        auto t0 = Clock::now();
        volatile size_t sink = 0;
        for (long i = 0; i < N; i++) {
            auto r = parse_topic_positional(topics[i % topics.size()], EMS_SEGMENTS);
            sink += r->strings.size();
        }
        print_kpi("parse_topic_positional (1M)", elapsed_sec(t0), N);
    }

    // parse_iso8601 only
    {
        auto t0 = Clock::now();
        volatile double sink = 0;
        for (long i = 0; i < N; i++)
            sink += parse_iso8601(EMS_TS);
        print_kpi("parse_iso8601 (1M)", elapsed_sec(t0), N);
    }

    // Full ingest path: parse_topic + parse_payload
    {
        auto t0 = Clock::now();
        volatile double sink = 0;
        for (long i = 0; i < N; i++) {
            auto ti  = *parse_topic_positional(topics[i % topics.size()], EMS_SEGMENTS);
            auto row = parse_payload(EMS_PAYLOAD, ti, 0, cfg);
            sink += row.floats.size();
        }
        print_kpi("parse_topic_positional + parse_payload (1M)", elapsed_sec(t0), N);
    }

    std::cout << "  gx10 target per writer: 350 000 msg/s  (× 4 writers = 1.4M/s)\n";
}

// ---------------------------------------------------------------------------
// KPI 2 — parquet write throughput (EMS schema with string columns)
// ---------------------------------------------------------------------------

static void kpi_parquet_write() {
    std::cout << "\n--- KPI 2: parquet write throughput (EMS schema) ---\n";

    auto dir = tmp_dir();

    auto make_row = [](int u, int i) -> Row {
        Row r;
        r.ints["project_id"]      = 0;
        r.strings["unit_id"]      = "u" + std::to_string(u);
        r.strings["device"]       = (i % 3 == 0) ? "ems" : (i % 3 == 1) ? "bms" : "pcs";
        r.strings["instance"]     = "0";
        r.strings["point_name"]   = (i % 6 == 0) ? "voltage"     :
                                    (i % 6 == 1) ? "current"     :
                                    (i % 6 == 2) ? "temperature" :
                                    (i % 6 == 3) ? "soc"         :
                                    (i % 6 == 4) ? "power"       : "frequency";
        r.floats["ts"]            = 1700000000.0 + i;
        r.floats["value"]         = 3.5 + (i % 100) * 0.007;
        return r;
    };

    for (int batch : {1000, 10000, 50000}) {
        std::vector<Row> rows;
        rows.reserve(batch);
        for (int i = 0; i < batch; i++) rows.push_back(make_row(i % 4, i));

        const int REPS = std::max(1, 200000 / batch);
        auto t0 = Clock::now();
        for (int rep = 0; rep < REPS; rep++) {
            auto p = dir + "/ems_b" + std::to_string(batch) + "_" + std::to_string(rep) + ".parquet";
            (void)flush_partition(p, rows, "snappy");
        }
        double elapsed    = elapsed_sec(t0);
        long total_rows   = (long)batch * REPS;
        long total_bytes  = (long)fs::file_size(
            dir + "/ems_b" + std::to_string(batch) + "_0.parquet") * REPS;

        std::string label = "flush_partition EMS snappy batch=" + std::to_string(batch);
        print_kpi(label, elapsed, total_rows, total_bytes);
    }

    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// KPI 3 — WAL round-trip speed
// ---------------------------------------------------------------------------

static void kpi_wal() {
    std::cout << "\n--- KPI 3: WAL write + replay round-trip ---\n";

    auto dir = tmp_dir();

    auto make_row = [](int i) -> Row {
        Row r;
        r.ints["project_id"]   = 0;
        r.strings["unit_id"]   = "u" + std::to_string(i % 4);
        r.strings["point_name"]= "voltage";
        r.floats["ts"]         = 1700000000.0 + i;
        r.floats["value"]      = 3.5 + (i % 100) * 0.007;
        return r;
    };

    for (int batch : {1000, 10000}) {
        std::vector<Row> rows;
        rows.reserve(batch);
        for (int i = 0; i < batch; i++) rows.push_back(make_row(i));

        const int REPS = std::max(1, 50000 / batch);

        // Write
        {
            auto t0 = Clock::now();
            for (int rep = 0; rep < REPS; rep++) {
                auto p = dir + "/w" + std::to_string(batch) + "_" + std::to_string(rep) + ".wal.arrow";
                (void)write_wal_ipc(p, rows);
            }
            print_kpi("write_wal_ipc batch=" + std::to_string(batch),
                      elapsed_sec(t0), (long)batch * REPS);
        }

        // Replay
        {
            auto p  = dir + "/w" + std::to_string(batch) + "_0.wal.arrow";
            auto t0 = Clock::now();
            long total = 0;
            for (int rep = 0; rep < REPS; rep++) {
                auto back = wal_replay_file(p);
                total += (long)back.size();
            }
            print_kpi("wal_replay_file batch=" + std::to_string(batch),
                      elapsed_sec(t0), total);
        }
    }

    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// KPI 4 — end-to-end ingest simulation at gx10 per-writer rate
// ---------------------------------------------------------------------------

static void kpi_ingest_simulation() {
    std::cout << "\n--- KPI 4: end-to-end ingest sim (parse + buffer, no I/O) ---\n";

    // Simulate one stress_real_pub instance: 4 units × ~720 topics each = ~2880 topics
    // stress_real_pub at --rate 350000 on gx10
    std::vector<std::pair<std::string,std::string>> messages;
    for (int u = 0; u < 4; u++)
        for (int d = 0; d < 30; d++)
            for (int p = 0; p < 6; p++)
                messages.emplace_back(ems_topic(u, d, p), EMS_PAYLOAD);

    const Config cfg = make_ems_cfg();

    // 10 rounds ≈ simulate 10× the full topic set
    const long N_ROUNDS = 10;
    const long TOTAL    = (long)messages.size() * N_ROUNDS;

    std::map<PartitionKey, Partition> local_buf;
    auto t0 = Clock::now();

    for (long round = 0; round < N_ROUNDS; round++) {
        for (auto& [topic, payload] : messages) {
            auto ti = parse_topic_positional(topic, EMS_SEGMENTS);
            if (!ti) continue;
            Row row = parse_payload(payload, *ti, 0, cfg);
            PartitionKey key{ti->source_type, cfg.site_id};
            local_buf[key].rows.push_back(std::move(row));
        }
    }

    double elapsed = elapsed_sec(t0);
    long buf_rows = 0;
    for (auto& [k, p] : local_buf) buf_rows += p.rows.size();

    print_kpi("parse+buffer " + std::to_string(messages.size()) + " topics × " +
              std::to_string(N_ROUNDS) + " rounds", elapsed, TOTAL);
    std::cout << "  Buffered partitions: " << local_buf.size()
              << "   total rows: " << buf_rows << "\n";

    // Flush timing
    auto dir = tmp_dir();
    auto t1  = Clock::now();
    for (auto& [key, part] : local_buf) {
        auto path = dir + "/" + key.source_type + "_" + key.partition_value + ".parquet";
        (void)flush_partition(path, part.rows, "snappy");
    }
    double flush_elapsed = elapsed_sec(t1);
    std::cout << "  Flush " << buf_rows << " buffered rows → "
              << local_buf.size() << " parquet files: "
              << std::fixed << std::setprecision(3) << flush_elapsed << "s  ("
              << std::setprecision(0) << buf_rows / flush_elapsed << " rows/s)\n";
    fs::remove_all(dir);

    std::cout << "  gx10 target per writer: 350 000 msg/s"
              << "  (parse+buffer must sustain >> this)\n";
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    std::cout << "=== real_writer KPI benchmarks ===\n";
    kpi_parse_throughput();
    kpi_parquet_write();
    kpi_wal();
    kpi_ingest_simulation();
    std::cout << "\nDone.\n";
    return 0;
}
