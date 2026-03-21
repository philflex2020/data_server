/**
 * kpi_writer.cpp  —  throughput benchmarks for writer.cpp
 *
 * KPIs measured:
 *   1. Parse throughput      — parse_topic + parse_payload msg/s
 *                              Target: >> 14 000 msg/s (24× fleet load)
 *   2. Parquet write speed   — flush_partition rows/s and MB/s
 *                              at batch sizes 1k / 10k / 50k rows
 *   3. End-to-end ingest sim — interleaved parse + buffer + flush, 14k msg/s load
 *
 * Build & run:
 *   make kpi
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#include "../writer.cpp"  // compiled with -DPARQUET_WRITER_NO_MAIN
#pragma GCC diagnostic pop

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// Default config for KPI tests — uses same defaults as Config struct
static const Config g_kpi_cfg;
using Clock  = std::chrono::steady_clock;
using Ms     = std::chrono::duration<double, std::milli>;
using Sec    = std::chrono::duration<double>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string tmp_dir() {
    char tmpl[] = "/tmp/kpi_XXXXXX";
    return std::string(mkdtemp(tmpl));
}

static double elapsed_sec(Clock::time_point t0) {
    return Sec(Clock::now() - t0).count();
}

static void print_kpi(const std::string& label, double elapsed, long n,
                       long bytes = 0) {
    double rate = n / elapsed;
    std::cout << std::left << std::setw(42) << label
              << std::right << std::setw(10) << std::fixed << std::setprecision(0) << rate;
    if (bytes > 0) {
        double mbs = (bytes / 1e6) / elapsed;
        std::cout << " ops/s   " << std::setw(7) << std::setprecision(1) << mbs << " MB/s";
    } else {
        std::cout << " ops/s";
    }
    std::cout << "   (" << std::setprecision(3) << elapsed << "s for " << n << ")\n";
}

// Pre-built representative payloads (nested {value,unit} format)
static const std::string BATTERY_PAYLOAD =
    "{\"timestamp\":1700000000.0,"
    "\"voltage\":{\"value\":3.7,\"unit\":\"V\"},"
    "\"current\":{\"value\":-5.2,\"unit\":\"A\"},"
    "\"temperature\":{\"value\":25.0,\"unit\":\"C\"},"
    "\"soc\":{\"value\":80.0,\"unit\":\"%\"},"
    "\"soh\":{\"value\":98.0,\"unit\":\"%\"},"
    "\"resistance\":{\"value\":0.01,\"unit\":\"ohm\"},"
    "\"capacity\":{\"value\":100.0,\"unit\":\"Ah\"}}";

static const std::string SOLAR_PAYLOAD =
    "{\"timestamp\":1700000000.0,"
    "\"irradiance\":{\"value\":800.0,\"unit\":\"W/m2\"},"
    "\"panel_temp\":{\"value\":35.0,\"unit\":\"C\"},"
    "\"dc_voltage\":{\"value\":600.0,\"unit\":\"V\"},"
    "\"dc_current\":{\"value\":8.0,\"unit\":\"A\"},"
    "\"ac_power\":{\"value\":180.0,\"unit\":\"W\"},"
    "\"efficiency\":{\"value\":20.0,\"unit\":\"%\"}}";

// Build a topic string for site s, rack r, module m, cell c
static std::string bat_topic(int s, int r, int m, int c) {
    return "batteries/site=" + std::to_string(s)
         + "/rack="   + std::to_string(r)
         + "/module=" + std::to_string(m)
         + "/cell="   + std::to_string(c);
}

// ---------------------------------------------------------------------------
// KPI 1 — parse throughput
// ---------------------------------------------------------------------------

static void kpi_parse_throughput() {
    std::cout << "\n--- KPI 1: parse throughput ---\n";

    // Pre-generate 3456 distinct battery topics (48 sites × 3 racks × 4 modules × 6 cells)
    // mirrors config.nats_24x.yaml scale
    std::vector<std::string> topics;
    for (int s = 0; s < 48; s++)
        for (int r = 0; r < 3; r++)
            for (int m = 0; m < 4; m++)
                for (int c = 0; c < 6; c++)
                    topics.push_back(bat_topic(s, r, m, c));

    const long N = 1'000'000;

    // parse_topic only
    {
        auto t0 = Clock::now();
        volatile size_t sink = 0;
        for (long i = 0; i < N; i++) {
            auto r = parse_topic(topics[i % topics.size()]);
            sink += r->kv.size();
        }
        print_kpi("parse_topic (1M calls)", elapsed_sec(t0), N);
    }

    // parse_topic + parse_payload (full ingest path, no MQTT overhead)
    {
        auto t0 = Clock::now();
        volatile double sink = 0;
        for (long i = 0; i < N; i++) {
            auto ti = *parse_topic(topics[i % topics.size()]);
            auto row = parse_payload(BATTERY_PAYLOAD, ti, 0, g_kpi_cfg);
            sink += row.floats.size();
        }
        print_kpi("parse_topic + parse_payload (1M)", elapsed_sec(t0), N);
    }

    // Solar payload variant
    {
        std::string solar_topic = "solar/site=0/rack=0/module=0/cell=0";
        auto t0 = Clock::now();
        volatile double sink = 0;
        for (long i = 0; i < N; i++) {
            auto ti = *parse_topic(solar_topic);
            auto row = parse_payload(SOLAR_PAYLOAD, ti, 0, g_kpi_cfg);
            sink += row.floats.size();
        }
        print_kpi("parse_topic + parse_payload solar (1M)", elapsed_sec(t0), N);
    }

    std::cout << "  Fleet target (24× scale): 14 976 msg/s — parse must be >> this\n";
}

// ---------------------------------------------------------------------------
// KPI 2 — parquet write throughput
// ---------------------------------------------------------------------------

static void kpi_parquet_write() {
    std::cout << "\n--- KPI 2: parquet write throughput ---\n";

    auto dir = tmp_dir();

    // Build a realistic battery row
    auto make_row = [](int site, int i) -> Row {
        Row r;
        r.ints["project_id"] = 0;
        r.ints["site_id"]    = site;
        r.ints["rack_id"]    = (i / 24) % 3;
        r.ints["module_id"]  = (i /  6) % 4;
        r.ints["cell_id"]    = i % 6;
        r.floats["timestamp"]   = 1700000000.0 + i;
        r.floats["voltage"]     = 3.5 + (i % 100) * 0.007;
        r.floats["current"]     = -5.0 + (i % 50)  * 0.2;
        r.floats["temperature"] = 20.0 + (i % 30)  * 0.5;
        r.floats["soc"]         = 50.0 + (i % 50);
        r.floats["soh"]         = 95.0 + (i % 5);
        r.floats["resistance"]  = 0.01 + i * 1e-6;
        r.floats["capacity"]    = 100.0;
        return r;
    };

    for (int batch : {1000, 10000, 50000}) {
        std::vector<Row> rows;
        rows.reserve(batch);
        for (int i = 0; i < batch; i++) rows.push_back(make_row(0, i));

        auto path   = dir + "/batch_" + std::to_string(batch) + ".parquet";
        auto t0     = Clock::now();
        const int REPS = std::max(1, 200000 / batch);   // ~200k rows total effort
        for (int rep = 0; rep < REPS; rep++) {
            auto p = dir + "/b" + std::to_string(batch) + "_" + std::to_string(rep) + ".parquet";
            (void)flush_partition(p, rows, "snappy");
        }
        double elapsed = elapsed_sec(t0);
        long total_rows  = (long)batch * REPS;
        long total_bytes = (long)fs::file_size(
            dir + "/b" + std::to_string(batch) + "_0.parquet") * REPS;

        std::string label = "flush_partition snappy batch=" + std::to_string(batch);
        print_kpi(label, elapsed, total_rows, total_bytes);
    }

    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// KPI 3 — end-to-end ingest simulation at 14k msg/s
// ---------------------------------------------------------------------------

static void kpi_ingest_simulation() {
    std::cout << "\n--- KPI 3: end-to-end ingest sim (parse + buffer, no I/O) ---\n";

    // Build topic set matching 24× fleet: 3456 battery + 11520 solar = 14976 topics
    std::vector<std::pair<std::string,std::string>> messages;
    messages.reserve(14976);
    for (int s = 0; s < 48; s++)
        for (int r = 0; r < 3; r++)
            for (int m = 0; m < 4; m++)
                for (int c = 0; c < 6; c++)
                    messages.emplace_back(bat_topic(s,r,m,c), BATTERY_PAYLOAD);
    for (int s = 0; s < 48; s++)
        for (int r = 0; r < 4; r++)
            for (int m = 0; m < 6; m++)
                for (int c = 0; c < 10; c++)
                    messages.emplace_back(
                        "solar/site=" + std::to_string(s) +
                        "/rack=" + std::to_string(r) +
                        "/module=" + std::to_string(m) +
                        "/cell=" + std::to_string(c),
                        SOLAR_PAYLOAD);

    const long N_ROUNDS = 10;   // simulate 10 seconds of fleet data
    const long TOTAL    = (long)messages.size() * N_ROUNDS;

    std::map<PartitionKey, Partition> local_buf;
    auto t0 = Clock::now();

    for (long round = 0; round < N_ROUNDS; round++) {
        for (auto& [topic, payload] : messages) {
            auto ti = parse_topic(topic);
            if (!ti) continue;
            Row row  = parse_payload(payload, *ti, 0, g_kpi_cfg);
            int site = row.ints.count("site_id") ? (int)row.ints.at("site_id") : 0;
            PartitionKey key{ti->source_type, site};
            local_buf[key].rows.push_back(std::move(row));
        }
    }

    double elapsed = elapsed_sec(t0);
    long buf_rows = 0;
    for (auto& [k, p] : local_buf) buf_rows += p.rows.size();

    print_kpi("parse+buffer 14976 msg × 10 rounds", elapsed, TOTAL);
    std::cout << "  Buffered partitions: " << local_buf.size()
              << "   total rows: " << buf_rows << "\n";

    // How long would one flush of this buffer take?
    auto dir = tmp_dir();
    auto t1 = Clock::now();
    for (auto& [key, part] : local_buf) {
        auto path = dir + "/" + key.source_type + "_part" +
                    std::to_string(key.partition_value) + ".parquet";
        (void)flush_partition(path, part.rows, "snappy");
    }
    double flush_elapsed = elapsed_sec(t1);
    std::cout << "  Flush " << buf_rows << " buffered rows → "
              << local_buf.size() << " parquet files: "
              << std::fixed << std::setprecision(3) << flush_elapsed << "s  ("
              << std::setprecision(0) << buf_rows / flush_elapsed << " rows/s)\n";
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    std::cout << "=== parquet_writer KPI benchmarks ===\n";
    kpi_parse_throughput();
    kpi_parquet_write();
    kpi_ingest_simulation();
    std::cout << "\nDone.\n";
    return 0;
}
