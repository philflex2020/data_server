/**
 * writer.cpp  —  FlashMQ MQTT → partitioned Parquet writer
 *
 * Subscribes to a FlashMQ broker and flushes buffered messages as partitioned
 * Parquet files to a local directory tree.
 *
 * Topic structure:
 *   {source_type}/{key}={val}/...
 *   e.g.  batteries/site=0/rack=1/module=2/cell=3
 *
 * Payload (JSON, nested {value,unit} format from generator):
 *   {"timestamp": 1.23, "voltage": {"value": 3.7, "unit": "V"}, ...}
 *   Flat scalar payloads are also accepted.
 *
 * Output layout (configurable via partitions list):
 *   {base_path}/{source_type}/project={project_id}/site={site_id}/{year}/{month}/{day}/{ts}.parquet
 *
 * Usage:
 *   ./parquet_writer [--config config.yaml]
 *
 * Build:
 *   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
 */

#include <mosquitto.h>
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <simdjson.h>
#include <yaml-cpp/yaml.h>

// POSIX socket for optional health endpoint
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

struct Config {
    // MQTT
    std::string mqtt_host      {"localhost"};
    int         mqtt_port      {1883};
    std::string mqtt_client_id {"parquet-writer"};
    std::string mqtt_topic     {"#"};
    int         mqtt_qos       {1};

    // Topic field mapping: topic kv key → row column name (all stored as int64)
    // e.g. "site" → "site_id" means batteries/site=3/... produces row.ints["site_id"] = 3
    std::map<std::string, std::string> topic_kv_map {
        {"site", "site_id"}, {"rack", "rack_id"}, {"module", "module_id"}, {"cell", "cell_id"}
    };

    // Which column (a value from topic_kv_map) to use as the buffer partition key
    std::string partition_field {"site_id"};

    // Output
    std::string base_path      {"/srv/data/parquet"};
    int         project_id     {0};
    std::vector<std::string> partitions {
        "project={project_id}", "site={site_id}", "{year}", "{month}", "{day}"
    };
    std::string compression    {"snappy"};
    int flush_interval_seconds {60};
    int max_messages_per_part  {50000};
    int max_total_buffer_rows  {500000};  // global OOM guard — drop + flush when exceeded

    // Hot file — flat parquet written on every flush, rsynced separately at higher frequency.
    // Empty string disables the feature.  Covers the gap between rsync cycles for DuckDB.
    std::string hot_file_path  {""};

    // Current-state file — one row per unique sensor, always the latest reading.
    // Rewritten atomically on every flush.  Intended for the AWS writer instance:
    // writer.cpp subscribes to the FlashMQ bridge on AWS and writes this file locally
    // so DuckDB on AWS can query live state without any rsync lag.
    // Empty string disables the feature.
    std::string current_state_path {""};

    // WAL — Arrow IPC write-ahead log.  Written before each parquet flush, deleted
    // on success.  Surviving WAL files are replayed on startup before live data is
    // accepted.  Disabled by default (wal_enabled: false in config).
    bool        wal_enabled  {false};
    std::string wal_path     {""};    // default: {base_path}/.wal/

    // Health HTTP endpoint — GET /health returns JSON metrics.
    // Disabled by default (health_enabled: false in config).
    bool        health_enabled {false};
    int         health_port    {8771};

    // Disk space guard — skip flush if free space below threshold.
    // 0 = disabled (default).
    uint64_t    min_free_bytes {0};   // set via min_free_gb in config
};

Config load_config(const std::string& path) {
    Config cfg;
    try {
        auto y = YAML::LoadFile(path);
        if (auto m = y["mqtt"]) {
            if (m["host"])      cfg.mqtt_host      = m["host"].as<std::string>();
            if (m["port"])      cfg.mqtt_port      = m["port"].as<int>();
            if (m["client_id"]) cfg.mqtt_client_id = m["client_id"].as<std::string>();
            if (m["topic"])     cfg.mqtt_topic     = m["topic"].as<std::string>();
            if (m["qos"])       cfg.mqtt_qos       = m["qos"].as<int>();
            if (auto km = m["topic_kv_map"]) {
                cfg.topic_kv_map.clear();
                for (const auto& pair : km)
                    cfg.topic_kv_map[pair.first.as<std::string>()] = pair.second.as<std::string>();
            }
            if (m["partition_field"])
                cfg.partition_field = m["partition_field"].as<std::string>();
        }
        if (auto o = y["output"]) {
            if (o["base_path"])              cfg.base_path              = o["base_path"].as<std::string>();
            if (o["project_id"])             cfg.project_id             = o["project_id"].as<int>();
            if (o["compression"])            cfg.compression            = o["compression"].as<std::string>();
            if (o["flush_interval_seconds"]) cfg.flush_interval_seconds = o["flush_interval_seconds"].as<int>();
            if (o["max_messages_per_part"])  cfg.max_messages_per_part  = o["max_messages_per_part"].as<int>();
            if (o["max_total_buffer_rows"])   cfg.max_total_buffer_rows   = o["max_total_buffer_rows"].as<int>();
            if (o["hot_file_path"])          cfg.hot_file_path          = o["hot_file_path"].as<std::string>();
            if (o["current_state_path"])     cfg.current_state_path     = o["current_state_path"].as<std::string>();
            if (o["partitions"]) {
                cfg.partitions.clear();
                for (const auto& p : o["partitions"])
                    cfg.partitions.push_back(p.as<std::string>());
            }
        }
        if (auto w = y["wal"]) {
            if (w["enabled"]) cfg.wal_enabled = w["enabled"].as<bool>();
            if (w["path"])    cfg.wal_path    = w["path"].as<std::string>();
        }
        if (auto h = y["health"]) {
            if (h["enabled"]) cfg.health_enabled = h["enabled"].as<bool>();
            if (h["port"])    cfg.health_port    = h["port"].as<int>();
        }
        if (auto g = y["guard"]) {
            if (g["min_free_gb"]) {
                double gb = g["min_free_gb"].as<double>();
                cfg.min_free_bytes = static_cast<uint64_t>(gb * 1024 * 1024 * 1024);
            }
        }
    } catch (const YAML::Exception& e) {
        std::cerr << "[config] " << e.what() << " — using defaults\n";
    }
    return cfg;
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

// A single decoded message row: integer metadata + float measurements
struct Row {
    std::map<std::string, int64_t> ints;
    std::map<std::string, double>  floats;
};

struct PartitionKey {
    std::string source_type;
    int         partition_value;   // value of cfg.partition_field for this buffer
    bool operator<(const PartitionKey& o) const {
        if (source_type != o.source_type) return source_type < o.source_type;
        return partition_value < o.partition_value;
    }
};

// Key that uniquely identifies one physical sensor: source type + all integer identity fields.
// Used as the key in g_current_state to track the latest reading per sensor.
struct SensorKey {
    std::string                    source_type;
    std::map<std::string, int64_t> ints;   // project_id, site_id, rack_id, module_id, cell_id, …
    bool operator<(const SensorKey& o) const {
        if (source_type != o.source_type) return source_type < o.source_type;
        return ints < o.ints;
    }
};

struct Partition {
    std::vector<Row>     rows;
    Clock::time_point    created {Clock::now()};
};

// ---------------------------------------------------------------------------
// Topic / payload parsing
// ---------------------------------------------------------------------------

struct TopicInfo {
    std::string              source_type;
    std::map<std::string, int> kv;   // site, rack, module, cell
    std::string              field_name; // per_cell_item: last non-kv segment (e.g. "voltage")
};

std::optional<TopicInfo> parse_topic(const std::string& topic) {
    TopicInfo info;
    std::istringstream ss(topic);
    std::string segment;
    bool first = true;

    while (std::getline(ss, segment, '/')) {
        if (first) {
            info.source_type = segment;
            first = false;
            continue;
        }
        auto eq = segment.find('=');
        if (eq != std::string::npos) {
            try { info.kv[segment.substr(0, eq)] = std::stoi(segment.substr(eq + 1)); }
            catch (...) {}
        } else if (!segment.empty()) {
            // No '=' — per_cell_item field name (e.g. "voltage" in .../cell=3/voltage)
            info.field_name = segment;
        }
    }
    if (info.source_type.empty()) return std::nullopt;
    return info;
}

Row parse_payload(const std::string& payload_str, const TopicInfo& topic, int project_id,
                  const Config& cfg = Config{}) {
    Row row;

    // Metadata from topic — driven by cfg.topic_kv_map
    row.ints["project_id"] = project_id;
    for (const auto& [kv_key, col_name] : cfg.topic_kv_map) {
        if (auto it = topic.kv.find(kv_key); it != topic.kv.end())
            row.ints[col_name] = it->second;
    }

    // Build the set of int column names (topic-derived cols + project_id)
    std::unordered_set<std::string> int_cols{"project_id"};
    for (const auto& [_, col] : cfg.topic_kv_map) int_cols.insert(col);

    // Payload — simdjson DOM parser (thread-local, reused across calls)
    thread_local simdjson::dom::parser sj_parser;
    simdjson::dom::element doc;
    if (sj_parser.parse(payload_str).get(doc) != simdjson::SUCCESS) {
        std::cerr << "[parse] simdjson: invalid JSON\n";
        return row;
    }
    simdjson::dom::object obj;
    if (doc.get(obj) != simdjson::SUCCESS) return row;

    for (auto [key_sv, val] : obj) {
        std::string k(key_sv);

        if (int_cols.count(k)) {
            int64_t iv;
            if (val.get(iv) == simdjson::SUCCESS) row.ints[k] = iv;

        } else if (val.type() == simdjson::dom::element_type::OBJECT) {
            // Nested {value, unit} format
            simdjson::dom::element value_el;
            if (val["value"].get(value_el) == simdjson::SUCCESS) {
                double d;
                if (value_el.get(d) == simdjson::SUCCESS) {
                    row.floats[k] = d;
                } else {
                    int64_t iv;  // integer value field (e.g. whole-number readings)
                    if (value_el.get(iv) == simdjson::SUCCESS) row.floats[k] = static_cast<double>(iv);
                }
            }
        } else {
            // Flat scalar
            double d;
            if (val.get(d) == simdjson::SUCCESS) {
                row.floats[k] = d;
            } else {
                int64_t iv;  // flat integer (e.g. "site_id": 3 in payload)
                if (val.get(iv) == simdjson::SUCCESS) row.floats[k] = static_cast<double>(iv);
            }
        }
    }

    return row;
}

// ---------------------------------------------------------------------------
// Output path
// ---------------------------------------------------------------------------

static std::string replace_vars(std::string tmpl,
                                 const std::map<std::string, std::string>& vars) {
    for (const auto& [k, v] : vars) {
        std::string ph = "{" + k + "}";
        size_t pos;
        while ((pos = tmpl.find(ph)) != std::string::npos)
            tmpl.replace(pos, ph.size(), v);
    }
    return tmpl;
}

std::string make_output_path(const Config& cfg,
                              const std::string& source_type, int partition_value) {
    auto now = std::chrono::system_clock::now();
    auto t   = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);

    char year[5], month[3], day[3], ts[20];
    strftime(year,  sizeof(year),  "%Y",            &tm);
    strftime(month, sizeof(month), "%m",            &tm);
    strftime(day,   sizeof(day),   "%d",            &tm);
    strftime(ts,    sizeof(ts),    "%Y%m%dT%H%M%SZ",&tm);

    std::map<std::string, std::string> vars {
        {"project_id",       std::to_string(cfg.project_id)},
        {cfg.partition_field, std::to_string(partition_value)},
        {"year", year}, {"month", month}, {"day", day},
    };

    fs::path dir(cfg.base_path);
    dir /= source_type;
    for (const auto& part : cfg.partitions)
        dir /= replace_vars(part, vars);

    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) std::cerr << "[path] create_directories: " << ec.message() << "\n";

    return (dir / (std::string(ts) + ".parquet")).string();
}

// ---------------------------------------------------------------------------
// Parquet flush
// ---------------------------------------------------------------------------

static parquet::Compression::type to_parquet_compression(const std::string& s) {
    if (s == "zstd")  return parquet::Compression::ZSTD;
    if (s == "gzip")  return parquet::Compression::GZIP;
    if (s == "none")  return parquet::Compression::UNCOMPRESSED;
    return parquet::Compression::SNAPPY;
}

arrow::Status flush_partition(const std::string& path,
                               const std::vector<Row>& rows,
                               const std::string& compression) {
    if (rows.empty()) return arrow::Status::OK();

    // Collect ordered column names (stable order across files)
    std::vector<std::string> int_cols, float_cols;
    {
        std::map<std::string, bool> seen_i, seen_f;
        for (const auto& r : rows) {
            for (const auto& [k, _] : r.ints)   if (!seen_i[k]) { seen_i[k]=true; int_cols.push_back(k); }
            for (const auto& [k, _] : r.floats) if (!seen_f[k]) { seen_f[k]=true; float_cols.push_back(k); }
        }
    }

    std::vector<std::shared_ptr<arrow::Field>>  fields;
    std::vector<std::shared_ptr<arrow::Array>>  arrays;

    // Integer columns
    for (const auto& col : int_cols) {
        arrow::Int64Builder b;
        for (const auto& r : rows) {
            auto it = r.ints.find(col);
            if (it != r.ints.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
            else                    ARROW_RETURN_NOT_OK(b.AppendNull());
        }
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::int64()));
        arrays.push_back(arr);
    }

    // Float columns
    for (const auto& col : float_cols) {
        arrow::DoubleBuilder b;
        for (const auto& r : rows) {
            auto it = r.floats.find(col);
            if (it != r.floats.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
            else                      ARROW_RETURN_NOT_OK(b.AppendNull());
        }
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::float64()));
        arrays.push_back(arr);
    }

    auto table = arrow::Table::Make(arrow::schema(fields), arrays);

    auto props = parquet::WriterProperties::Builder()
        .compression(to_parquet_compression(compression))
        ->build();

    ARROW_ASSIGN_OR_RAISE(auto outfile, arrow::io::FileOutputStream::Open(path));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), outfile,
        static_cast<int64_t>(rows.size()), props));

    return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Forward declarations for globals defined in "Buffer + flush thread" section
// ---------------------------------------------------------------------------

static const Config*         g_cfg{nullptr};
static std::atomic<bool>     g_shutdown{false};
static std::atomic<uint64_t> g_overflow_drops{0};
static int                   g_signal_pipe[2] = {-1, -1};

// ---------------------------------------------------------------------------
// Flush metrics (always present, zero overhead when not watched)
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_msgs_received    {0};
static std::atomic<uint64_t> g_flush_count      {0};
static std::atomic<uint64_t> g_total_rows_written{0};
static std::atomic<uint64_t> g_last_flush_rows  {0};
static std::atomic<uint64_t> g_last_flush_ms    {0};
static std::atomic<uint64_t> g_disk_free_gb_x10 {0};
static std::atomic<uint64_t> g_wal_replay_rows  {0};

// ---------------------------------------------------------------------------
// Disk space guard (only active when min_free_bytes > 0)
// ---------------------------------------------------------------------------

static bool check_disk_space(const std::string& path, uint64_t min_bytes) {
    std::error_code ec;
    auto si = fs::space(path, ec);
    if (!ec)
        g_disk_free_gb_x10.store(si.available / (1024ULL * 1024 * 1024 / 10)); // C5: always sample
    if (min_bytes == 0) return true;                                             // guard moved after sample
    if (ec) { std::cerr << "[disk] space check failed: " << ec.message() << "\n"; return true; }
    if (si.available < min_bytes) {
        std::cerr << "[disk] LOW SPACE: " << si.available / (1024.0*1024*1024)
                  << " GB free, minimum " << min_bytes / (1024.0*1024*1024) << " GB\n";
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// WAL — Arrow IPC write-ahead log (only active when wal_enabled: true)
// ---------------------------------------------------------------------------

static std::string time_suffix() {
    auto now = std::chrono::system_clock::now();
    auto t   = std::chrono::system_clock::to_time_t(now);
    std::tm tm{}; gmtime_r(&t, &tm);
    char buf[20]; strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
    return buf;
}

static std::string wal_path_for(const Config& cfg, const std::string& source_type, int pval) {
    std::string wp = cfg.wal_path.empty() ? cfg.base_path + "/.wal" : cfg.wal_path;
    std::error_code ec; fs::create_directories(wp, ec);
    return wp + "/" + source_type + "_" + std::to_string(pval) + "_" + time_suffix() + ".wal.arrow";
}

static arrow::Status write_wal_ipc(const std::string& path, const std::vector<Row>& rows) {
    if (rows.empty()) return arrow::Status::OK();

    // Build Arrow table from rows (ints + floats only — same as flush_partition)
    std::vector<std::string> int_cols, float_cols;
    {
        std::map<std::string, bool> si, sf;
        for (const auto& r : rows) {
            for (const auto& [k, _] : r.ints)   if (!si[k]) { si[k]=true; int_cols.push_back(k); }
            for (const auto& [k, _] : r.floats) if (!sf[k]) { sf[k]=true; float_cols.push_back(k); }
        }
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (const auto& col : int_cols) {
        arrow::Int64Builder b;
        for (const auto& r : rows) {
            auto it = r.ints.find(col);
            if (it != r.ints.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
            else                    ARROW_RETURN_NOT_OK(b.AppendNull());
        }
        std::shared_ptr<arrow::Array> arr; ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::int64())); arrays.push_back(arr);
    }
    for (const auto& col : float_cols) {
        arrow::DoubleBuilder b;
        for (const auto& r : rows) {
            auto it = r.floats.find(col);
            if (it != r.floats.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
            else                      ARROW_RETURN_NOT_OK(b.AppendNull());
        }
        std::shared_ptr<arrow::Array> arr; ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::float64())); arrays.push_back(arr);
    }
    auto table = arrow::Table::Make(arrow::schema(fields), arrays);
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out, table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_RETURN_NOT_OK(out->Close());   // C2: was discarded — WAL file could be corrupt but caller saw OK
    return arrow::Status::OK();
}

static std::vector<Row> wal_replay_file(const std::string& path) {
    std::vector<Row> rows;
    try {
        auto r_in = arrow::io::ReadableFile::Open(path);
        if (!r_in.ok()) return rows;
        auto r_rd = arrow::ipc::RecordBatchFileReader::Open(r_in.ValueOrDie());
        if (!r_rd.ok()) return rows;
        auto reader = r_rd.ValueOrDie();
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (int i = 0; i < reader->num_record_batches(); ++i) {
            auto rb = reader->ReadRecordBatch(i);
            if (rb.ok()) batches.push_back(rb.ValueOrDie());
        }
        auto rt = arrow::Table::FromRecordBatches(batches);
        if (!rt.ok()) return rows;
        auto table = rt.ValueOrDie();
        int ncols = static_cast<int>(table->num_columns());
        int nrows = static_cast<int>(table->num_rows());
        std::vector<std::pair<std::string, std::shared_ptr<arrow::Int64Array>>>  int_cols;
        std::vector<std::pair<std::string, std::shared_ptr<arrow::DoubleArray>>> dbl_cols;
        for (int c = 0; c < ncols; ++c) {
            const auto& name = table->schema()->field(c)->name();
            auto tid = table->schema()->field(c)->type()->id();
            // C6: flatten all chunks into one contiguous array — chunk(0) silently drops multi-chunk tables
            auto combined = arrow::Concatenate(table->column(c)->chunks());
            if (!combined.ok()) continue;
            auto arr = combined.ValueOrDie();
            if (tid == arrow::Type::INT64)
                int_cols.emplace_back(name, std::static_pointer_cast<arrow::Int64Array>(arr));
            else if (tid == arrow::Type::DOUBLE)
                dbl_cols.emplace_back(name, std::static_pointer_cast<arrow::DoubleArray>(arr));
        }
        rows.reserve(nrows);
        for (int r = 0; r < nrows; ++r) {
            Row row;
            for (auto& [n, a] : int_cols) if (a && !a->IsNull(r)) row.ints[n] = a->Value(r);
            for (auto& [n, a] : dbl_cols) if (a && !a->IsNull(r)) row.floats[n] = a->Value(r);
            rows.push_back(std::move(row));
        }
    } catch (const std::exception& e) {
        std::cerr << "[wal] replay error for " << path << ": " << e.what() << "\n";
    }
    return rows;
}

static void replay_wal_files() {
    std::string wp = g_cfg->wal_path.empty() ? g_cfg->base_path + "/.wal" : g_cfg->wal_path;
    std::error_code ec;
    if (!fs::exists(wp, ec)) return;
    std::vector<fs::path> wal_files;
    for (const auto& e : fs::directory_iterator(wp, ec))
        if (e.path().extension() == ".arrow") wal_files.push_back(e.path());
    if (wal_files.empty()) return;
    std::cout << "[wal] found " << wal_files.size() << " WAL file(s) — replaying\n";
    for (const auto& wp : wal_files) {
        auto rows = wal_replay_file(wp.string());
        if (rows.empty()) { fs::remove(wp, ec); continue; }

        // C3: parse source_type from filename: {source_type}_{pval}_{ts}.wal.arrow
        std::string stem = wp.stem().string();          // e.g. "batteries_1_20260320T120000Z.wal"
        if (stem.size() > 4 && stem.substr(stem.size()-4) == ".wal")
            stem = stem.substr(0, stem.size()-4);       // strip trailing ".wal"
        std::string source_type;
        auto first_us = stem.find('_');
        if (first_us != std::string::npos)
            source_type = stem.substr(0, first_us);
        if (source_type.empty())
            source_type = "data";   // safe fallback — filename didn't match expected pattern

        std::cout << "[wal] replaying " << rows.size() << " rows from " << wp.filename()
                  << " (source_type=" << source_type << ")\n";
        g_wal_replay_rows.fetch_add(rows.size());
        // Group by partition key and flush to production parquet
        std::map<PartitionKey, std::vector<Row>> parts;
        for (auto& r : rows) {
            int pval = 0;
            if (auto it = r.ints.find(g_cfg->partition_field); it != r.ints.end())
                pval = static_cast<int>(it->second);
            parts[{source_type, pval}].push_back(std::move(r));
        }
        bool all_ok = true;
        for (auto& [key, part_rows] : parts) {
            auto path   = make_output_path(*g_cfg, key.source_type, key.partition_value);
            auto status = flush_partition(path, part_rows, g_cfg->compression);
            if (status.ok())
                std::cout << "[wal] flushed " << part_rows.size() << " rows → " << path << "\n";
            else {
                std::cerr << "[wal] replay flush failed: " << status.ToString() << "\n";
                all_ok = false;
            }
        }
        if (all_ok) fs::remove(wp, ec);
    }
}

// ---------------------------------------------------------------------------
// Health HTTP endpoint (only active when health_enabled: true)
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_health_buffer_rows{0};  // updated by on_message

static void health_thread_fn(int port) {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { std::cerr << "[health] socket failed\n"; return; }
    int one = 1; setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(srv, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[health] bind :" << port << " failed\n"; close(srv); return;
    }
    listen(srv, 4);
    std::cout << "[health] listening on :" << port << "\n";
    while (!g_shutdown) {
        fd_set fds; FD_ZERO(&fds); FD_SET(srv, &fds);
        timeval tv{1, 0};
        if (select(srv + 1, &fds, nullptr, nullptr, &tv) <= 0) continue;
        int cli = accept(srv, nullptr, nullptr);
        if (cli < 0) continue;
        char buf[256]; recv(cli, buf, sizeof(buf) - 1, 0);
        std::string body =
            "{\"msgs_received\":"    + std::to_string(g_msgs_received.load())     +
            ",\"buffer_rows\":"      + std::to_string(g_health_buffer_rows.load())+
            ",\"overflow_drops\":"   + std::to_string(g_overflow_drops.load())    +
            ",\"flush_count\":"      + std::to_string(g_flush_count.load())       +
            ",\"total_rows_written\":"+ std::to_string(g_total_rows_written.load())+
            ",\"last_flush_rows\":"  + std::to_string(g_last_flush_rows.load())   +
            ",\"last_flush_ms\":"    + std::to_string(g_last_flush_ms.load())     +
            ",\"wal_replay_rows\":"  + std::to_string(g_wal_replay_rows.load())   +
            ",\"disk_free_gb\":"     + std::to_string(g_disk_free_gb_x10.load() / 10.0) +
            "}\n";
        std::string resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                           "Content-Length: " + std::to_string(body.size()) +
                           "\r\nConnection: close\r\n\r\n" + body;
        send(cli, resp.c_str(), resp.size(), 0);
        close(cli);
    }
    close(srv);
}

// ---------------------------------------------------------------------------
// Buffer + flush thread
// ---------------------------------------------------------------------------

static std::map<PartitionKey, Partition> g_buffers;
static std::map<SensorKey, Row>          g_current_state;  // latest row per sensor
static std::mutex                        g_mutex;
static std::condition_variable           g_flush_cv;
// g_shutdown, g_cfg, g_overflow_drops declared above (before robustness section)
static size_t                            g_total_buffered{0};    // total rows in g_buffers (under g_mutex)
static uint64_t                          g_received_since_sync{0}; // msgs received since last _sync (under g_mutex)
static uint64_t                          g_sync_drops_detected{0}; // cumulative drops reported via sync
static std::string                       g_sync_session_id;      // detect stress_runner restarts

static void do_flush(std::map<PartitionKey, Partition> to_flush,
                     std::map<SensorKey, Row>          cs_snapshot) {
    // Disk space guard (skips entire flush if enabled and space is low)
    if (!check_disk_space(g_cfg->base_path, g_cfg->min_free_bytes)) {
        std::cerr << "[flush] SKIPPED — insufficient disk space\n";
        return;
    }

    auto flush_start = std::chrono::steady_clock::now();
    uint64_t flush_rows = 0;
    std::vector<Row> hot_rows;   // accumulated for hot file; empty if feature disabled
    const bool want_hot = !g_cfg->hot_file_path.empty();

    for (auto& [key, part] : to_flush) {
        if (part.rows.empty()) continue;

        if (want_hot)
            for (const auto& r : part.rows) hot_rows.push_back(r);

        // WAL: write before parquet (only if enabled)
        std::string wal_file;
        if (g_cfg->wal_enabled) {
            wal_file = wal_path_for(*g_cfg, key.source_type, key.partition_value);
            auto ws = write_wal_ipc(wal_file, part.rows);
            if (!ws.ok()) {
                std::cerr << "[wal] write failed (" << ws.ToString()
                          << ") — SKIPPING parquet flush for this partition\n";
                wal_file.clear();   // C2: don't try to delete a partial WAL on success path
                continue;           // C2: abort — no WAL means no crash-safe guarantee
            }
        }

        auto path = make_output_path(*g_cfg, key.source_type, key.partition_value);
        arrow::Status status;
        constexpr int MAX_RETRIES = 3;
        for (int attempt = 0; attempt <= MAX_RETRIES; ++attempt) {
            if (attempt > 0) {
                std::cerr << "[flush] retry " << attempt << "/" << MAX_RETRIES
                          << " for " << path << "\n";
                std::this_thread::sleep_for(std::chrono::seconds(1 << (attempt - 1))); // 1s, 2s, 4s
            }
            status = flush_partition(path, part.rows, g_cfg->compression);
            if (status.ok()) break;
            std::cerr << "[flush] attempt " << attempt + 1 << " failed: " << status.ToString() << "\n";
        }
        if (status.ok()) {
            flush_rows += part.rows.size();
            std::cout << "[flush] " << part.rows.size() << " rows → " << path << "\n";
            if (!wal_file.empty()) { std::error_code ec; fs::remove(wal_file, ec); }
        } else {
            std::cerr << "[flush] PERMANENT ERROR — " << part.rows.size() << " rows LOST for "
                      << key.source_type << "/" << g_cfg->partition_field
                      << "=" << key.partition_value << ": " << status.ToString() << "\n";
            if (!wal_file.empty())
                std::cerr << "[wal] WAL retained for recovery: " << wal_file << "\n";
        }
    }

    // Update flush metrics
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - flush_start).count();
    g_flush_count.fetch_add(1);
    g_total_rows_written.fetch_add(flush_rows);
    g_last_flush_rows.store(flush_rows);
    g_last_flush_ms.store(static_cast<uint64_t>(elapsed_ms));
    if (flush_rows > 0)
        std::cout << "[flush] total=" << flush_rows << " rows in " << elapsed_ms << "ms\n";

    // Hot file — flat merge of all partitions, written atomically via temp+rename.
    // Covers the gap between the last rsync and now for DuckDB queries on AWS.
    if (want_hot && !hot_rows.empty()) {
        auto tmp    = g_cfg->hot_file_path + ".tmp";
        auto status = flush_partition(tmp, hot_rows, g_cfg->compression);
        if (status.ok()) {
            std::error_code ec;
            fs::rename(tmp, g_cfg->hot_file_path, ec);
            if (ec)
                std::cerr << "[hot] rename failed: " << ec.message() << "\n";
            else
                std::cout << "[hot] " << hot_rows.size() << " rows → " << g_cfg->hot_file_path << "\n";
        } else {
            std::cerr << "[hot] write failed: " << status.ToString() << "\n";
            fs::remove(tmp);
        }
    }

    // Current-state file — one row per unique sensor, always the latest reading.
    // Written from the snapshot taken under g_mutex in flush_thread_fn.
    if (!g_cfg->current_state_path.empty() && !cs_snapshot.empty()) {
        std::vector<Row> cs_rows;
        cs_rows.reserve(cs_snapshot.size());
        for (auto& [k, r] : cs_snapshot) cs_rows.push_back(r);

        auto tmp    = g_cfg->current_state_path + ".tmp";
        auto status = flush_partition(tmp, cs_rows, g_cfg->compression);
        if (status.ok()) {
            std::error_code ec;
            fs::rename(tmp, g_cfg->current_state_path, ec);
            if (ec)
                std::cerr << "[cs] rename failed: " << ec.message() << "\n";
            else
                std::cout << "[cs] " << cs_rows.size() << " sensors → " << g_cfg->current_state_path << "\n";
        } else {
            std::cerr << "[cs] write failed: " << status.ToString() << "\n";
            fs::remove(tmp);
        }
    }
}

static void flush_thread_fn() {
    while (!g_shutdown) {
        std::unique_lock<std::mutex> lock(g_mutex);
        lock.unlock();
        struct pollfd pfd =
            { g_signal_pipe[0], POLLIN, 0 };
        poll(&pfd, 1,
            g_cfg->flush_interval_seconds * 1000);
        lock.lock();

        auto to_flush  = std::exchange(g_buffers, {});
        g_total_buffered = 0;                       // reset counter atomically with buffer swap
        g_health_buffer_rows.store(0);              // C5: health endpoint now correctly shows 0 after flush
        auto cs_snap   = g_current_state;           // copy under same lock — atomic with buffer swap
        lock.unlock();                              // release before slow I/O
        do_flush(std::move(to_flush), std::move(cs_snap));
    }

    // Final flush on shutdown
    std::unique_lock<std::mutex> lock(g_mutex);
    auto to_flush    = std::exchange(g_buffers, {});
    g_total_buffered = 0;
    g_health_buffer_rows.store(0);
    auto cs_snap     = g_current_state;
    lock.unlock();
    do_flush(std::move(to_flush), std::move(cs_snap));
}

// ---------------------------------------------------------------------------
// MQTT callbacks
// ---------------------------------------------------------------------------

static struct mosquitto* g_mosq{nullptr};  // set in main before loop_start

static void on_connect(struct mosquitto*, void*, int rc) {
    if (rc == 0) {
        std::cout << "[mqtt] connected to " << g_cfg->mqtt_host
                  << ":" << g_cfg->mqtt_port << "\n";
        // Subscribe here so reconnect after broker restart automatically resubscribes.
        // mosquitto_loop_start handles TCP reconnect but not resubscription.
        int sub_rc = mosquitto_subscribe(g_mosq, nullptr,
                                         g_cfg->mqtt_topic.c_str(), g_cfg->mqtt_qos);
        if (sub_rc != MOSQ_ERR_SUCCESS)
            std::cerr << "[mqtt] subscribe failed: " << mosquitto_strerror(sub_rc) << "\n";
    } else {
        std::cerr << "[mqtt] connect failed: " << mosquitto_connack_string(rc) << "\n";
    }
}

// Called under g_mutex when a _sync message arrives.
static void handle_sync_message(const std::string& payload) {
    thread_local simdjson::dom::parser sj;
    simdjson::dom::element doc;
    if (sj.parse(payload).get(doc) != simdjson::SUCCESS) return;

    std::string_view session_sv;
    int64_t seq = 0, interval_pub = 0, total_pub = 0;
    if (doc["session_id"].get(session_sv)        != simdjson::SUCCESS) return;
    if (doc["seq"].get(seq)                       != simdjson::SUCCESS) return;
    if (doc["interval_published"].get(interval_pub) != simdjson::SUCCESS) return;
    if (doc["total_published"].get(total_pub) != simdjson::SUCCESS) total_pub = 0;

    std::string session(session_sv);

    // New session = stress_runner restarted; reset counters and note it.
    if (session != g_sync_session_id) {
        if (!g_sync_session_id.empty())
            std::cout << "[sync] new session " << session
                      << " (was " << g_sync_session_id << ") — counters reset\n";
        g_sync_session_id     = session;
        g_received_since_sync = 0;
        return;  // skip comparison for first sync of a new session
    }

    int64_t received = static_cast<int64_t>(g_received_since_sync);
    int64_t dropped  = interval_pub - received;
    g_received_since_sync = 0;

    if (dropped > 0) {
        g_sync_drops_detected += static_cast<uint64_t>(dropped);
        std::cerr << "[sync] #" << seq
                  << "  DROPS: expected=" << interval_pub
                  << "  received=" << received
                  << "  dropped=" << dropped
                  << "  cumulative=" << g_sync_drops_detected << "\n";
    } else {
        std::cout << "[sync] #" << seq
                  << "  OK  received=" << received
                  << "  expected=" << interval_pub
                  << "  total_published=" << total_pub << "\n";
    }
}

static void on_message(struct mosquitto*, void*, const struct mosquitto_message* msg) {
    if (!msg->payloadlen) return;

    std::string topic(msg->topic);
    std::string payload(static_cast<const char*>(msg->payload), msg->payloadlen);

    // Intercept sync messages before normal processing
    if (topic == "_sync") {
        std::lock_guard<std::mutex> lock(g_mutex);
        handle_sync_message(payload);
        return;
    }

    auto info_opt = parse_topic(topic);
    if (!info_opt) return;

    Row row = parse_payload(payload, *info_opt, g_cfg->project_id, *g_cfg);

    // per_cell_item mode: rename generic "value" field to the actual measurement name
    // (e.g. topic .../cell=3/voltage → field_name="voltage", payload {"value": 3.7})
    if (!info_opt->field_name.empty()) {
        auto it = row.floats.find("value");
        if (it != row.floats.end()) {
            row.floats[info_opt->field_name] = it->second;
            row.floats.erase(it);
        }
    }

    int pval = 0;
    if (auto it = row.ints.find(g_cfg->partition_field); it != row.ints.end())
        pval = static_cast<int>(it->second);

    PartitionKey key{info_opt->source_type, pval};

    g_msgs_received.fetch_add(1);

    std::lock_guard<std::mutex> lock(g_mutex);

    // Overflow guard: drop incoming row if buffer is full, wake flush thread immediately.
    if (g_total_buffered >= static_cast<size_t>(g_cfg->max_total_buffer_rows)) {
        auto drops = g_overflow_drops.fetch_add(1) + 1;
        if (drops == 1 || drops % 10000 == 0)
            std::cerr << "[buffer] OVERFLOW — dropped " << drops
                      << " rows (max_total_buffer_rows=" << g_cfg->max_total_buffer_rows << ")\n";
        g_flush_cv.notify_one();
        return;
    }

    // Maintain current-state map: always keep the latest row per unique sensor.
    if (!g_cfg->current_state_path.empty())
        g_current_state[SensorKey{info_opt->source_type, row.ints}] = row;

    auto& part = g_buffers[key];
    part.rows.push_back(std::move(row));
    ++g_total_buffered;
    g_health_buffer_rows.store(g_total_buffered);
    ++g_received_since_sync;
    if (static_cast<int>(part.rows.size()) >= g_cfg->max_messages_per_part)
        g_flush_cv.notify_one();
}

static void on_disconnect(struct mosquitto*, void*, int rc) {
    if (rc != 0)
        std::cerr << "[mqtt] unexpected disconnect (rc=" << rc << ") — will reconnect\n";
}

static void on_log(struct mosquitto*, void*, int level, const char* str) {
    if (level == MOSQ_LOG_ERR || level == MOSQ_LOG_WARNING)
        std::cerr << "[mqtt] " << str << "\n";
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

static void handle_signal(int) {
    g_shutdown.store(true, std::memory_order_relaxed);
    char b = 1;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    write(g_signal_pipe[1], &b, 1);   // intentionally unchecked in signal context
#pragma GCC diagnostic pop
}

// ---------------------------------------------------------------------------
// Main  (omitted when PARQUET_WRITER_NO_MAIN is defined, e.g. for unit tests)
// ---------------------------------------------------------------------------

#ifndef PARQUET_WRITER_NO_MAIN

static void print_help(const char* prog) {
    std::cout <<
"Usage: " << prog << " [--config <path>] [--help]\n"
"\n"
"MQTT → partitioned Parquet writer for battery telemetry.\n"
"\n"
"Options:\n"
"  --config <path>   YAML config file (default: config.yaml)\n"
"  --help            Show this help and exit\n"
"\n"
"Core config (output section):\n"
"  base_path              Local directory for Parquet output\n"
"  flush_interval_seconds Seconds between flushes (default 60)\n"
"  max_messages_per_part  Flush a partition early when it hits this count\n"
"  max_total_buffer_rows  Global OOM guard — drop + force flush when exceeded\n"
"  compression            snappy | zstd | gzip | none (default snappy)\n"
"\n"
"Robustness features (all disabled by default — add to config to enable):\n"
"\n"
"  wal:\n"
"    enabled: true\n"
"    path: \"\"           # default: {base_path}/.wal/\n"
"\n"
"    Write-ahead log using Arrow IPC format.  Before each Parquet flush the\n"
"    batch is written to a .wal.arrow file.  On successful Parquet write the\n"
"    WAL file is deleted.  On restart any surviving WAL files are replayed\n"
"    and flushed to production Parquet before live MQTT data is accepted.\n"
"    Max data loss on hard crash = one flush interval of rows.\n"
"\n"
"  health:\n"
"    enabled: true\n"
"    port: 8771\n"
"\n"
"    Lightweight HTTP endpoint (raw socket, no extra deps).\n"
"    GET http://host:8771/health  returns JSON:\n"
"      msgs_received, buffer_rows, overflow_drops, flush_count,\n"
"      total_rows_written, last_flush_rows, last_flush_ms,\n"
"      wal_replay_rows, disk_free_gb\n"
"\n"
"  guard:\n"
"    min_free_gb: 1.0\n"
"\n"
"    Before each flush, check available disk space on base_path.\n"
"    If free space is below min_free_gb the flush is skipped and logged.\n"
"    0.0 disables the check (default).\n"
"\n"
"Flush metrics are always collected (zero overhead) and printed to stdout\n"
"on each flush: total rows, duration ms, cumulative totals.\n"
"They are also exposed via the health endpoint when enabled.\n"
"\n"
"Examples:\n"
"  ./parquet_writer\n"
"  ./parquet_writer --config /etc/writer/config.yaml\n"
"  curl http://localhost:8771/health | python3 -m json.tool\n";
}

int main(int argc, char* argv[]) {
    std::string config_path = "config.yaml";
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "--help") { print_help(argv[0]); return 0; }
        if (std::string(argv[i]) == "--config" && i + 1 < argc) config_path = argv[++i];
    }

    Config cfg = load_config(config_path);
    g_cfg = &cfg;

    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    mosquitto_lib_init();
    if (pipe(g_signal_pipe) != 0) {
        std::cerr << "[init] pipe() failed: " << strerror(errno) << "\n";
        return 1;
    }

    // clean_session=false: FlashMQ queues QoS-1 messages while we're offline
    auto* mosq = mosquitto_new(cfg.mqtt_client_id.c_str(), /*clean_session=*/false, nullptr);
    if (!mosq) {
        std::cerr << "[mqtt] mosquitto_new failed\n";
        return 1;
    }
    g_mosq = mosq;  // make available to on_connect for resubscription

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_log_callback_set(mosq, on_log);
    mosquitto_reconnect_delay_set(mosq, 2, 30, /*exponential=*/true);

    // Initial connect (retry until broker is up); subscription happens in on_connect
    while (!g_shutdown) {
        int rc = mosquitto_connect(mosq, cfg.mqtt_host.c_str(), cfg.mqtt_port, /*keepalive=*/60);
        if (rc == MOSQ_ERR_SUCCESS) break;
        std::cerr << "[mqtt] connect: " << mosquitto_strerror(rc) << " — retrying in 5s\n";
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    std::cout << "[writer] topic='" << cfg.mqtt_topic
              << "'  flush every " << cfg.flush_interval_seconds << "s"
              << "  max_per_part=" << cfg.max_messages_per_part
              << "  max_total=" << cfg.max_total_buffer_rows
              << "  wal=" << (cfg.wal_enabled ? "on" : "off")
              << "  health=" << (cfg.health_enabled ? std::to_string(cfg.health_port) : "off")
              << "\n";

    // Replay any WAL files from a previous crash before going live
    if (cfg.wal_enabled) replay_wal_files();

    // Optional health endpoint thread
    std::thread health_thr;
    if (cfg.health_enabled)
        health_thr = std::thread(health_thread_fn, cfg.health_port);

    // Background MQTT network loop (handles reconnects automatically)
    mosquitto_loop_start(mosq);

    std::thread flusher(flush_thread_fn);
    flusher.join();   // wait here until shutdown

    if (health_thr.joinable()) health_thr.join();
    mosquitto_loop_stop(mosq, /*force=*/true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    return 0;
}
#endif // PARQUET_WRITER_NO_MAIN
