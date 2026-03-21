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
 *   ./parquet_writer [--config writer_config.yaml]
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
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>

#include "mqtt_ring.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <limits>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
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

    // MQTT tuning — rarely need changing
    int mqtt_keepalive_seconds     {60};   // broker keepalive interval
    int mqtt_reconnect_min_seconds {2};    // exponential backoff min
    int mqtt_reconnect_max_seconds {30};   // exponential backoff max
    int mqtt_connect_retry_seconds {5};    // initial connect retry wait

    // Flush retry tuning
    int flush_max_retries          {3};    // attempts per partition per flush cycle
    int flush_retry_base_seconds   {1};    // base for exponential backoff: 1s, 2s, 4s, …
};

// M12: validate config — catch bad values before they cause spin-loops or silent drops
static bool validate_config(const Config& cfg) {
    bool ok = true;
    if (cfg.flush_interval_seconds <= 0) {
        std::cerr << "[config] flush_interval_seconds must be > 0 (got "
                  << cfg.flush_interval_seconds << ")\n"; ok = false;
    }
    if (cfg.max_total_buffer_rows <= 0) {
        std::cerr << "[config] max_total_buffer_rows must be > 0 (got "
                  << cfg.max_total_buffer_rows << ")\n"; ok = false;
    }
    if (cfg.max_messages_per_part <= 0) {
        std::cerr << "[config] max_messages_per_part must be > 0 (got "
                  << cfg.max_messages_per_part << ")\n"; ok = false;
    }
    static const std::set<std::string> valid_compression{
        "snappy", "gzip", "brotli", "lz4", "zstd", "none"};
    if (!valid_compression.count(cfg.compression)) {
        std::cerr << "[config] unknown compression '" << cfg.compression
                  << "' — valid: snappy gzip brotli lz4 zstd none\n"; ok = false;
    }
    if (cfg.base_path.empty()) {
        std::cerr << "[config] output.base_path must be set\n"; ok = false;
    }
    return ok;
}

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
            if (m["keepalive_seconds"])
                cfg.mqtt_keepalive_seconds     = m["keepalive_seconds"].as<int>();
            if (m["reconnect_min_seconds"])
                cfg.mqtt_reconnect_min_seconds = m["reconnect_min_seconds"].as<int>();
            if (m["reconnect_max_seconds"])
                cfg.mqtt_reconnect_max_seconds = m["reconnect_max_seconds"].as<int>();
            if (m["connect_retry_seconds"])
                cfg.mqtt_connect_retry_seconds = m["connect_retry_seconds"].as<int>();
        }
        if (auto o = y["output"]) {
            if (o["base_path"])              cfg.base_path              = o["base_path"].as<std::string>();
            if (o["project_id"])             cfg.project_id             = o["project_id"].as<int>();
            if (o["compression"])            cfg.compression            = o["compression"].as<std::string>();
            if (o["flush_interval_seconds"]) cfg.flush_interval_seconds = o["flush_interval_seconds"].as<int>();
            if (o["max_messages_per_part"])  cfg.max_messages_per_part  = o["max_messages_per_part"].as<int>();
            if (o["max_total_buffer_rows"])    cfg.max_total_buffer_rows   = o["max_total_buffer_rows"].as<int>();
            if (o["flush_max_retries"])        cfg.flush_max_retries        = o["flush_max_retries"].as<int>();
            if (o["flush_retry_base_seconds"]) cfg.flush_retry_base_seconds = o["flush_retry_base_seconds"].as<int>();
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

// M13: strip path-traversal characters from topic-derived path segments
static std::string sanitize_path_segment(std::string s) {
    // truncate at any slash (belt-and-suspenders — topic is already split on '/')
    auto slash = s.find_first_of("/\\");
    if (slash != std::string::npos) s = s.substr(0, slash);
    if (s.empty() || s == "." || s == "..") return "";
    return s;
}

std::optional<TopicInfo> parse_topic(const std::string& topic) {
    TopicInfo info;
    std::istringstream ss(topic);
    std::string segment;
    bool first = true;

    while (std::getline(ss, segment, '/')) {
        if (first) {
            info.source_type = sanitize_path_segment(segment);  // M13
            first = false;
            if (info.source_type.empty()) return std::nullopt;
            continue;
        }
        auto eq = segment.find('=');
        if (eq != std::string::npos) {
            std::string kv_key = segment.substr(0, eq);
            int kv_val;
            try { kv_val = std::stoi(segment.substr(eq + 1)); }
            catch (...) { continue; }
            if (info.kv.count(kv_key))
                std::cerr << "[topic] duplicate key '" << kv_key << "' in: " << topic << "\n";
            info.kv[kv_key] = kv_val;
        } else if (!segment.empty()) {
            // No '=' — per_cell_item field name (e.g. "voltage" in .../cell=3/voltage)
            info.field_name = segment;
        }
    }
    if (info.source_type.empty()) return std::nullopt;
    return info;
}

Row parse_payload(const std::string& payload_str, const TopicInfo& topic, int project_id,
                  const Config& cfg) {
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
                if (row.ints.count(k) == 0)              // M14: don't duplicate topic-derived int cols
                    row.floats[k] = d;
            } else {
                int64_t iv;
                if (val.get(iv) == simdjson::SUCCESS && row.ints.count(k) == 0)
                    row.floats[k] = static_cast<double>(iv);  // M14: skip if already an int col
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
                              const std::string& source_type, int partition_value,
                              std::optional<std::chrono::system_clock::time_point> collection_ts = {}) {
    // M4: use provided timestamp (WAL replay) or current time (normal flush)
    auto now = collection_ts.value_or(std::chrono::system_clock::now());
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

// M3: shared Arrow table builder — was duplicated verbatim in flush_partition and write_wal_ipc
static arrow::Result<std::shared_ptr<arrow::Table>>
rows_to_arrow_table(const std::vector<Row>& rows) {
    // m3: use std::set so column order is always alphabetically sorted, not first-seen order
    std::vector<std::string> int_cols, float_cols;
    {
        std::set<std::string> seen_i, seen_f;
        for (const auto& r : rows) {
            for (const auto& [k, _] : r.ints)   seen_i.insert(k);
            for (const auto& [k, _] : r.floats) seen_f.insert(k);
        }
        int_cols.assign(seen_i.begin(), seen_i.end());
        float_cols.assign(seen_f.begin(), seen_f.end());
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
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::int64()));
        arrays.push_back(arr);
    }
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
    return arrow::Table::Make(arrow::schema(fields), arrays);
}

arrow::Status flush_partition(const std::string& path,
                               const std::vector<Row>& rows,
                               const std::string& compression) {
    if (rows.empty()) return arrow::Status::OK();
    ARROW_ASSIGN_OR_RAISE(auto table, rows_to_arrow_table(rows));
    auto props = parquet::WriterProperties::Builder()
        .compression(to_parquet_compression(compression))
        ->build();
    // NC2 fix: write to .tmp then rename so a partial write never leaves a corrupt final file
    std::string tmp = path + ".tmp";
    ARROW_ASSIGN_OR_RAISE(auto outfile, arrow::io::FileOutputStream::Open(tmp));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), outfile,
        static_cast<int64_t>(rows.size()), props));
    ARROW_RETURN_NOT_OK(outfile->Close());
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) {
        fs::remove(tmp, ec);
        return arrow::Status::IOError("flush_partition rename failed: ", ec.message());
    }
    return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Forward declarations for globals defined in "Buffer + flush thread" section
// ---------------------------------------------------------------------------

static Config                g_cfg_storage{};
static const Config*         g_cfg{&g_cfg_storage};  // m4: never null — safe in tests before main()
static std::atomic<bool>     g_shutdown{false};
static std::atomic<bool>     g_flush_needed{false};  // m2: sticky flag survives a busy flush thread
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

static std::string time_suffix(std::chrono::system_clock::time_point tp) {
    auto t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm{}; gmtime_r(&t, &tm);
    char buf[20]; strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
    return buf;
}

// m5: directory created once at startup, not on every flush call
// m6: accepts flush_ts so WAL and Parquet filenames share the same clock read
static std::string wal_path_for(const Config& cfg, const std::string& source_type, int pval,
                                  std::chrono::system_clock::time_point tp) {
    std::string wp = cfg.wal_path.empty() ? cfg.base_path + "/.wal" : cfg.wal_path;
    return wp + "/" + source_type + "_" + std::to_string(pval) + "_" + time_suffix(tp) + ".wal.arrow";
}

static arrow::Status write_wal_ipc(const std::string& path, const std::vector<Row>& rows) {
    if (rows.empty()) return arrow::Status::OK();

    ARROW_ASSIGN_OR_RAISE(auto table, rows_to_arrow_table(rows));  // M3: shared helper
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
        // M4: derive collection timestamp from oldest row so WAL replay writes to the
        // correct date partition, not today's date.
        // NC1 fix: timestamp is stored as r.floats (flat scalar double, Unix seconds).
        double min_ts_sec = std::numeric_limits<double>::max();
        for (const auto& r : rows) {
            if (auto it = r.floats.find("timestamp"); it != r.floats.end())
                min_ts_sec = std::min(min_ts_sec, it->second);
        }
        std::optional<std::chrono::system_clock::time_point> collection_ts;
        if (min_ts_sec < std::numeric_limits<double>::max()) {
            auto dur = std::chrono::duration_cast<std::chrono::system_clock::duration>(
                           std::chrono::duration<double>(min_ts_sec));
            collection_ts = std::chrono::system_clock::time_point{dur};
        }

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
            auto path   = make_output_path(*g_cfg, key.source_type, key.partition_value, collection_ts);
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
        struct pollfd srv_pfd = { srv, POLLIN, 0 };
        if (poll(&srv_pfd, 1, 1000) <= 0) continue;
        int cli = accept(srv, nullptr, nullptr);
        if (cli < 0) continue;
        char buf[4096] = {};                                     // m10: 4 KB covers Chrome/Firefox headers
        auto n = recv(cli, buf, sizeof(buf) - 1, 0);
        if (n <= 0) { close(cli); continue; }
        // M7: only respond to GET /health
        std::string req(buf, static_cast<size_t>(n));
        bool valid_request = (req.rfind("GET ", 0) == 0) &&
                             (req.find(" /health ") != std::string::npos ||
                              req.find(" /health\r") != std::string::npos);
        if (!valid_request) {
            const char* r404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            send(cli, r404, strlen(r404), 0);
            close(cli); continue;
        }
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

using StateMap = std::map<SensorKey, Row>;

static std::map<PartitionKey, Partition> g_buffers;
static std::shared_ptr<StateMap>         g_current_state{std::make_shared<StateMap>()}; // C4-A: ptr swap at flush
static std::mutex                        g_mutex;
// g_flush_cv removed: flush thread uses poll() on g_signal_pipe[0]; notify via pipe write
// g_shutdown, g_cfg, g_overflow_drops declared above (before robustness section)
static size_t                            g_total_buffered{0};    // total rows in g_buffers (under g_mutex)
static uint64_t                          g_received_since_sync{0}; // msgs received since last _sync (under g_mutex)
static uint64_t                          g_sync_drops_detected{0}; // cumulative drops reported via sync
static std::string                       g_sync_session_id;      // detect stress_runner restarts

static void do_flush(std::map<PartitionKey, Partition> to_flush,
                     std::shared_ptr<StateMap>         cs_snapshot) {
    // Disk space guard (skips entire flush if enabled and space is low)
    if (!check_disk_space(g_cfg->base_path, g_cfg->min_free_bytes)) {
        std::cerr << "[flush] SKIPPED — insufficient disk space\n";
        return;
    }

    auto flush_ts    = std::chrono::system_clock::now();  // m6: single clock read for WAL+Parquet filenames
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
            wal_file = wal_path_for(*g_cfg, key.source_type, key.partition_value, flush_ts);
            auto ws = write_wal_ipc(wal_file, part.rows);
            if (!ws.ok()) {
                std::cerr << "[wal] write failed (" << ws.ToString()
                          << ") — SKIPPING parquet flush for this partition\n";
                wal_file.clear();   // C2: don't try to delete a partial WAL on success path
                continue;           // C2: abort — no WAL means no crash-safe guarantee
            }
        }

        auto path = make_output_path(*g_cfg, key.source_type, key.partition_value, flush_ts);
        arrow::Status status;
        const int max_retries  = g_cfg->flush_max_retries;
        const int retry_base_s = g_cfg->flush_retry_base_seconds;
        for (int attempt = 0; attempt <= max_retries; ++attempt) {
            if (attempt > 0) {
                std::cerr << "[flush] retry " << attempt << "/" << max_retries
                          << " for " << path << "\n";
                std::this_thread::sleep_for(std::chrono::seconds(retry_base_s << (attempt - 1)));
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
            std::cerr << "[flush] PERMANENT ERROR after " << max_retries << " retries — "
                      << part.rows.size() << " rows LOST for "
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
            { std::error_code ec; fs::remove(tmp, ec); }
        }
    }

    // Current-state file — one row per unique sensor, always the latest reading.
    // Written from the snapshot taken under g_mutex in flush_thread_fn.
    if (!g_cfg->current_state_path.empty() && cs_snapshot && !cs_snapshot->empty()) {
        std::vector<Row> cs_rows;
        cs_rows.reserve(cs_snapshot->size());
        for (auto& [k, r] : *cs_snapshot) cs_rows.push_back(r);

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
            { std::error_code ec; fs::remove(tmp, ec); }
        }
    }
}

static void flush_thread_fn() {
    while (!g_shutdown) {
        // m2: skip the poll if a flush was requested while we were busy last cycle
        if (!g_flush_needed.exchange(false)) {
            struct pollfd pfd = { g_signal_pipe[0], POLLIN, 0 };
            poll(&pfd, 1, g_cfg->flush_interval_seconds * 1000);
            // drain any wake bytes so the pipe never fills (pipe is O_NONBLOCK)
            char drain[64]; while (::read(g_signal_pipe[0], drain, sizeof(drain)) > 0) {}
        }
        std::unique_lock<std::mutex> lock(g_mutex);
        auto to_flush  = std::exchange(g_buffers, {});
        g_total_buffered = 0;                       // reset counter atomically with buffer swap
        g_health_buffer_rows.store(0);              // C5: health endpoint now correctly shows 0 after flush
        auto cs_snap   = std::move(g_current_state);            // C4-A: O(1) pointer move, not O(N) copy
        g_current_state = std::make_shared<StateMap>();         // fresh empty map for next cycle
        lock.unlock();                              // release before slow I/O
        do_flush(std::move(to_flush), std::move(cs_snap));
    }
    // M11: final flush moved to main(), after mosquitto_loop_stop(), so it catches
    // all messages delivered up to loop_stop rather than up to flush thread exit.
}

// ---------------------------------------------------------------------------
// MQTT callbacks
// ---------------------------------------------------------------------------

static std::atomic<struct mosquitto*> g_mosq{nullptr};  // M10: atomic — written by main, read by callbacks

// Lock-free SPSC ring: on_message (MQTT thread) → parse_thread_fn (parse thread).
// g_mqtt_stopped is set by main() after mosquitto_loop_stop() to tell the parse
// thread that no more pushes will occur and it may drain to empty then exit.
static MqttRing              g_mqtt_ring;
static std::atomic<bool>     g_mqtt_stopped{false};

static void on_connect(struct mosquitto*, void*, int rc) {
    if (rc == 0) {
        std::cout << "[mqtt] connected to " << g_cfg->mqtt_host
                  << ":" << g_cfg->mqtt_port << "\n";
        // Subscribe here so reconnect after broker restart automatically resubscribes.
        // mosquitto_loop_start handles TCP reconnect but not resubscription.
        int sub_rc = mosquitto_subscribe(g_mosq.load(), nullptr,
                                         g_cfg->mqtt_topic.c_str(), g_cfg->mqtt_qos);
        if (sub_rc != MOSQ_ERR_SUCCESS) {
            std::cerr << "[mqtt] subscribe failed (" << mosquitto_strerror(sub_rc)
                      << ") — disconnecting to force reconnect\n";
            mosquitto_disconnect(g_mosq.load());  // m9: triggers on_connect again rather than silently receiving nothing
        }
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
                      << " (was " << g_sync_session_id << ") — counters reset"
                      << " (drops before first _sync of new session are not counted)\n";
        g_sync_session_id     = session;
        g_received_since_sync = 0;
        g_sync_drops_detected = 0;  // M2: reset cumulative drop count for new session
        return;  // skip comparison for first sync of a new session
    }

    // M2: clamp to avoid misleading results if counter somehow exceeds int64 range
    int64_t received = (g_received_since_sync > static_cast<uint64_t>(INT64_MAX))
                       ? INT64_MAX
                       : static_cast<int64_t>(g_received_since_sync);
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

// Processes one raw MQTT message.  Called from parse_thread_fn with topic/payload
// already copied out of the ring.  Owns g_mutex only for the brief buffer-insert.
static void process_raw_message(const char* topic_cstr, const char* payload_cstr) {
    std::string topic(topic_cstr);
    std::string payload(payload_cstr);

    if (topic == "_sync") {
        std::lock_guard<std::mutex> lock(g_mutex);
        handle_sync_message(payload);
        return;
    }

    auto info_opt = parse_topic(topic);
    if (!info_opt) return;

    Row row = parse_payload(payload, *info_opt, g_cfg->project_id, *g_cfg);

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

    if (g_total_buffered >= static_cast<size_t>(g_cfg->max_total_buffer_rows)) {
        auto drops = g_overflow_drops.fetch_add(1) + 1;
        if (drops == 1 || drops % 10000 == 0)
            std::cerr << "[buffer] OVERFLOW — dropped " << drops
                      << " rows (max_total_buffer_rows=" << g_cfg->max_total_buffer_rows << ")\n";
        g_flush_needed.store(true);
        { char b = 1; if (::write(g_signal_pipe[1], &b, 1) < 0) {} }
        return;
    }

    if (!g_cfg->current_state_path.empty()) {
        std::map<std::string, int64_t> key_ints;
        for (const auto& [_, col] : g_cfg->topic_kv_map)
            if (auto it = row.ints.find(col); it != row.ints.end())
                key_ints[col] = it->second;
        (*g_current_state)[SensorKey{info_opt->source_type, std::move(key_ints)}] = row;
    }

    auto& part = g_buffers[key];
    part.rows.push_back(std::move(row));
    ++g_total_buffered;
    g_health_buffer_rows.store(g_total_buffered);
    ++g_received_since_sync;
    if (static_cast<int>(part.rows.size()) >= g_cfg->max_messages_per_part) {
        g_flush_needed.store(true);
        { char b = 1; if (::write(g_signal_pipe[1], &b, 1) < 0) {} }
    }
}

// Drains g_mqtt_ring and processes each message.  Runs until MQTT is stopped
// (g_mqtt_stopped == true) and the ring is empty.
static void parse_thread_fn() {
    MqSlot slot;
    while (true) {
        if (g_mqtt_ring.pop(slot)) {
            process_raw_message(slot.topic, slot.payload);
        } else if (g_mqtt_stopped.load(std::memory_order_acquire) && g_mqtt_ring.empty()) {
            break;
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }
}

// on_message: called from libmosquitto network thread — must return immediately.
// Just pushes raw bytes into the lock-free ring; parse_thread_fn does the work.
static void on_message(struct mosquitto*, void*, const struct mosquitto_message* msg) {
    if (!msg->payloadlen) return;

    bool ok = g_mqtt_ring.push(
        msg->topic,                                         // null-terminated
        static_cast<int>(std::strlen(msg->topic)),
        static_cast<const char*>(msg->payload),
        msg->payloadlen
    );

    if (!ok) {
        // Ring full — count as overflow (same semantics as buffer overflow)
        auto drops = g_overflow_drops.fetch_add(1) + 1;
        if (drops == 1 || drops % 10000 == 0)
            std::cerr << "[ring] FULL — dropped " << drops << " msgs\n";
    }
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
"Usage: " << prog << " [--config <path>] [options] [--help]\n"
"\n"
"MQTT → partitioned Parquet writer for battery telemetry.\n"
"\n"
"Options (override config file values):\n"
"  --config <path>        YAML config file (default: writer_config.yaml)\n"
"  --host <hostname>      MQTT broker host (overrides mqtt.host)\n"
"  --port <n>             MQTT broker port (overrides mqtt.port)\n"
"  --client-id <id>       MQTT client ID (overrides mqtt.client_id) — MUST be unique per instance\n"
"  --output <path>        Parquet output directory (overrides output.base_path) — MUST be unique per instance\n"
"  --health-port <n>      Health endpoint port (overrides health.port) — MUST be unique per instance\n"
"  --flush-interval <n>   Flush interval seconds (overrides output.flush_interval_seconds)\n"
"  --help                 Show this help and exit\n"
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
"  ./parquet_writer --config /etc/writer/writer_config.yaml\n"
"  curl http://localhost:8771/health | python3 -m json.tool\n";
}

int main(int argc, char* argv[]) {
    std::string config_path = "writer_config.yaml";
    // CLI overrides applied after config load — take precedence over config file
    std::string cli_host, cli_output, cli_client_id;
    int cli_port = 0, cli_health_port = 0, cli_flush_interval = 0;
    for (int i = 1; i < argc; i++) {
        std::string a(argv[i]);
        if (a == "--help")                               { print_help(argv[0]); return 0; }
        if (a == "--config"        && i + 1 < argc)     config_path      = argv[++i];
        else if (a == "--host"     && i + 1 < argc)     cli_host         = argv[++i];
        else if (a == "--port"     && i + 1 < argc)     cli_port         = std::atoi(argv[++i]);
        else if (a == "--output"   && i + 1 < argc)     cli_output       = argv[++i];
        else if (a == "--client-id"&& i + 1 < argc)     cli_client_id    = argv[++i];
        else if (a == "--health-port" && i + 1 < argc)  cli_health_port  = std::atoi(argv[++i]);
        else if (a == "--flush-interval" && i+1 < argc) cli_flush_interval = std::atoi(argv[++i]);
    }

    Config cfg = load_config(config_path);
    // Apply CLI overrides
    if (!cli_host.empty())         cfg.mqtt_host              = cli_host;
    if (cli_port > 0)              cfg.mqtt_port              = cli_port;
    if (!cli_output.empty())       cfg.base_path              = cli_output;
    if (!cli_client_id.empty())    cfg.mqtt_client_id         = cli_client_id;
    if (cli_health_port > 0)       cfg.health_port            = cli_health_port;
    if (cli_flush_interval > 0)    cfg.flush_interval_seconds = cli_flush_interval;

    if (!validate_config(cfg)) return 1;   // M12
    g_cfg_storage = cfg;  // m4: copy into static storage so g_cfg is never null

    // m5: create WAL directory once at startup rather than on every flush call
    if (cfg.wal_enabled) {
        std::string wp = cfg.wal_path.empty() ? cfg.base_path + "/.wal" : cfg.wal_path;
        std::error_code ec;
        fs::create_directories(wp, ec);
        if (ec) { std::cerr << "[wal] mkdir failed: " << ec.message() << "\n"; return 1; }
    }

    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    // m11: check return value — undefined behaviour if init fails
    if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) {
        std::cerr << "[init] mosquitto_lib_init failed\n";
        return 1;
    }
    if (pipe(g_signal_pipe) != 0) {
        std::cerr << "[init] pipe() failed: " << strerror(errno) << "\n";
        return 1;
    }
    // O_NONBLOCK on read end: drain loop in flush_thread_fn won't block after wakeup
    fcntl(g_signal_pipe[0], F_SETFL, O_NONBLOCK);

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
    mosquitto_reconnect_delay_set(mosq, cfg.mqtt_reconnect_min_seconds,
                                       cfg.mqtt_reconnect_max_seconds, /*exponential=*/true);

    // Initial connect (retry until broker is up); subscription happens in on_connect
    while (!g_shutdown) {
        int rc = mosquitto_connect(mosq, cfg.mqtt_host.c_str(), cfg.mqtt_port,
                                   cfg.mqtt_keepalive_seconds);
        if (rc == MOSQ_ERR_SUCCESS) break;
        std::cerr << "[mqtt] connect: " << mosquitto_strerror(rc)
                  << " — retrying in " << cfg.mqtt_connect_retry_seconds << "s\n";
        std::this_thread::sleep_for(std::chrono::seconds(cfg.mqtt_connect_retry_seconds));
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

    // parse_thread drains g_mqtt_ring → process_raw_message → g_buffers
    std::thread parser(parse_thread_fn);

    std::thread flusher(flush_thread_fn);
    flusher.join();   // wait for flush thread to exit its normal loop on g_shutdown

    if (health_thr.joinable()) health_thr.join();

    // Stop MQTT *before* signalling parse_thread so the ring is fully written
    // when g_mqtt_stopped is set.  loop_stop blocks until the network thread exits.
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, /*force=*/false);

    // Signal parse_thread that no more pushes will occur, then wait for it to
    // drain the ring completely before doing the final flush.
    g_mqtt_stopped.store(true, std::memory_order_release);
    parser.join();

    // Final flush in main — catches everything buffered between flush thread's last
    // cycle and loop_stop. flusher is already joined so no locking needed.
    {
        auto to_flush = std::exchange(g_buffers, {});
        g_total_buffered = 0;
        g_health_buffer_rows.store(0);
        auto cs_snap = std::move(g_current_state);
        g_current_state = std::make_shared<StateMap>();
        do_flush(std::move(to_flush), std::move(cs_snap));
    }

    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    return 0;
}
#endif // PARQUET_WRITER_NO_MAIN
