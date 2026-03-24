/**
 * real_writer.cpp  —  FlashMQ MQTT → partitioned Parquet writer (EMS / Longbow schema)
 *
 * Topic format:  unit/{unit_id}/{device}/{instance}/{point_name}/{dtype_hint}
 * Payload:       {"ts":"2024-11-15T21:27:52.775Z","value":0.999393}
 *
 * Key design decisions:
 *   - site_id comes from config, not topic (partition directory context)
 *   - dtype_hint drives INT64 vs FLOAT64 value storage; not written as a column
 *   - Parquet dictionary encoding handles repeated point_name strings for free
 *
 * Robustness features:
 *   1. WAL (write-ahead log)
 *      Before each parquet flush, rows are written to a WAL file in {base_path}/.wal/.
 *      On successful parquet write the WAL file is deleted.  On restart, surviving
 *      WAL files are replayed and immediately flushed before live data is accepted.
 *      Max data loss = rows received since last flush cycle (bounded by max_messages_per_part).
 *
 *   2. Current-state snapshot
 *      Written atomically (tmp+rename) on every flush.  One row per unique sensor,
 *      always the most recent reading.  Seed for dashboards/on-call after restart.
 *
 *   3. Health HTTP endpoint
 *      GET http://host:{health_port}/health → JSON with msgs_received, flush_count,
 *      buffer_rows, overflow_drops, sync_drops, last_flush_ms, disk_free_gb.
 *
 *   4. Disk space guard
 *      Checks free space before every flush.  Skips and logs if below min_free_bytes.
 *
 *   5. Startup validation
 *      Fails fast if site_id is missing, base_path is not writable, or disk is nearly full.
 *
 *   6. Flush metrics
 *      Every flush logs row count, duration, rows/sec, and cumulative totals.
 *
 * Config:  real_writer_config.yaml
 * Build:   make real_writer
 */

#include <mosquitto.h>
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <simdjson.h>
#include <yaml-cpp/yaml.h>

// POSIX socket for health endpoint
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <ctime>
#include <filesystem>
#include <fstream>
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
using Clock  = std::chrono::steady_clock;
using SysClock = std::chrono::system_clock;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

enum class TopicParser { KV, POSITIONAL };

struct Config {
    // MQTT
    std::string mqtt_host      {"localhost"};
    int         mqtt_port      {1883};
    std::string mqtt_client_id {"real-writer"};
    std::string mqtt_topic     {"unit/#"};
    int         mqtt_qos       {0};

    // Topic parser
    TopicParser topic_parser   {TopicParser::POSITIONAL};
    std::map<std::string, std::string> topic_kv_map {};
    // "dtype_hint" is reserved: consumed for type casting, not stored as a column.
    std::vector<std::string> topic_segments {};

    // Site ID injected into every row from config (not from topic).
    std::string site_id        {""};
    std::string partition_field{"site_id"};

    // Timestamp
    std::string timestamp_field {"ts"};
    std::string timestamp_format{"iso8601"};

    // Output
    std::string base_path      {"/srv/data/parquet-ems"};
    int         project_id     {0};
    std::vector<std::string> partitions {"site={site_id}", "{year}", "{month}", "{day}"};
    std::string compression    {"snappy"};
    int flush_interval_seconds {60};
    int max_messages_per_part  {5000000};
    int max_total_buffer_rows  {2000000};
    std::string hot_file_path     {""};
    std::string current_state_path{""};

    // WAL — write-ahead log for crash recovery
    bool        wal_enabled         {true};
    std::string wal_path            {""};   // default: {base_path}/.wal/

    // Disk space guard — skip flush and warn if free space falls below this
    uint64_t    min_free_bytes      {1ULL * 1024 * 1024 * 1024};  // 1 GB

    // Health HTTP endpoint
    bool        health_enabled      {true};
    int         health_port         {8771};

    // Compactor — merges small flush files into larger ones in the background
    bool        compact_enabled         {false};
    int         compact_interval_seconds{600};   // how often to scan (default: 10 min)
    int         compact_min_files       {3};     // min files in a dir to trigger compaction
    int         compact_min_age_seconds {0};     // 0 = 2 × flush_interval_seconds
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
            if (m["topic_parser"]) {
                auto s = m["topic_parser"].as<std::string>();
                cfg.topic_parser = (s == "kv") ? TopicParser::KV : TopicParser::POSITIONAL;
            }
            if (auto km = m["topic_kv_map"]) {
                cfg.topic_kv_map.clear();
                for (const auto& pair : km)
                    cfg.topic_kv_map[pair.first.as<std::string>()] = pair.second.as<std::string>();
            }
            if (auto ts = m["topic_segments"]) {
                cfg.topic_segments.clear();
                for (const auto& s : ts)
                    cfg.topic_segments.push_back(s.as<std::string>());
            }
            if (m["partition_field"])  cfg.partition_field  = m["partition_field"].as<std::string>();
            if (m["timestamp_field"])  cfg.timestamp_field  = m["timestamp_field"].as<std::string>();
            if (m["timestamp_format"]) cfg.timestamp_format = m["timestamp_format"].as<std::string>();
        }
        if (auto o = y["output"]) {
            if (o["base_path"])              cfg.base_path              = o["base_path"].as<std::string>();
            if (o["project_id"])             cfg.project_id             = o["project_id"].as<int>();
            if (o["compression"])            cfg.compression            = o["compression"].as<std::string>();
            if (o["flush_interval_seconds"]) cfg.flush_interval_seconds = o["flush_interval_seconds"].as<int>();
            if (o["max_messages_per_part"])  cfg.max_messages_per_part  = o["max_messages_per_part"].as<int>();
            if (o["max_total_buffer_rows"])  cfg.max_total_buffer_rows  = o["max_total_buffer_rows"].as<int>();
            if (o["hot_file_path"])          cfg.hot_file_path          = o["hot_file_path"].as<std::string>();
            if (o["current_state_path"])     cfg.current_state_path     = o["current_state_path"].as<std::string>();
            if (o["site_id"])                cfg.site_id                = o["site_id"].as<std::string>();
            if (o["partitions"]) {
                cfg.partitions.clear();
                for (const auto& p : o["partitions"])
                    cfg.partitions.push_back(p.as<std::string>());
            }
        }
        if (auto w = y["wal"]) {
            if (w["enabled"])  cfg.wal_enabled = w["enabled"].as<bool>();
            if (w["path"])     cfg.wal_path    = w["path"].as<std::string>();
        }
        if (auto h = y["health"]) {
            if (h["enabled"]) cfg.health_enabled = h["enabled"].as<bool>();
            if (h["port"])    cfg.health_port    = h["port"].as<int>();
        }
        if (auto c = y["compact"]) {
            if (c["enabled"])           cfg.compact_enabled           = c["enabled"].as<bool>();
            if (c["interval_seconds"])  cfg.compact_interval_seconds  = c["interval_seconds"].as<int>();
            if (c["min_files"])         cfg.compact_min_files         = c["min_files"].as<int>();
            if (c["min_age_seconds"])   cfg.compact_min_age_seconds   = c["min_age_seconds"].as<int>();
        }
        if (auto g = y["guard"]) {
            if (g["min_free_gb"])
                cfg.min_free_bytes = static_cast<uint64_t>(
                    g["min_free_gb"].as<double>() * 1024 * 1024 * 1024);
        }
    } catch (const YAML::Exception& e) {
        std::cerr << "[config] " << e.what() << " — using defaults\n";
    }
    if (cfg.wal_path.empty())
        cfg.wal_path = cfg.base_path + "/.wal";
    return cfg;
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

struct Row {
    std::map<std::string, int64_t>     ints;
    std::map<std::string, double>      floats;
    std::map<std::string, std::string> strings;
};

struct PartitionKey {
    std::string source_type;
    std::string partition_value;
    bool operator<(const PartitionKey& o) const {
        if (source_type != o.source_type) return source_type < o.source_type;
        return partition_value < o.partition_value;
    }
};

struct SensorKey {
    std::string                        source_type;
    std::map<std::string, int64_t>     ints;
    std::map<std::string, std::string> strings;
    bool operator<(const SensorKey& o) const {
        if (source_type != o.source_type) return source_type < o.source_type;
        if (ints        != o.ints)        return ints        < o.ints;
        return strings < o.strings;
    }
};

struct Partition {
    std::vector<Row>  rows;
    Clock::time_point created {Clock::now()};
};

// ---------------------------------------------------------------------------
// Topic parsing
// ---------------------------------------------------------------------------

struct TopicInfo {
    std::string                source_type;
    std::map<std::string, int> kv;
    std::string                field_name;
    std::map<std::string, std::string> strings;
    std::string                dtype_hint;
};

std::optional<TopicInfo> parse_topic_kv(const std::string& topic) {
    TopicInfo info;
    std::istringstream ss(topic);
    std::string segment;
    bool first = true;
    while (std::getline(ss, segment, '/')) {
        if (first) { info.source_type = segment; first = false; continue; }
        auto eq = segment.find('=');
        if (eq != std::string::npos) {
            try { info.kv[segment.substr(0, eq)] = std::stoi(segment.substr(eq + 1)); }
            catch (...) {}
        } else if (!segment.empty()) {
            info.field_name = segment;
        }
    }
    if (info.source_type.empty()) return std::nullopt;
    return info;
}

std::optional<TopicInfo> parse_topic_positional(
        const std::string& topic,
        const std::vector<std::string>& segments_map) {
    TopicInfo info;
    std::vector<std::string> parts;
    std::istringstream ss(topic);
    std::string seg;
    while (std::getline(ss, seg, '/'))
        parts.push_back(seg);

    if (parts.empty()) return std::nullopt;
    info.source_type = parts[0];

    for (size_t i = 0; i < segments_map.size() && i < parts.size(); ++i) {
        const auto& col = segments_map[i];
        if (col == "_" || col.empty()) continue;
        if (col == "dtype_hint")
            info.dtype_hint = parts[i];
        else
            info.strings[col] = parts[i];
    }
    return info;
}

// ---------------------------------------------------------------------------
// ISO 8601 timestamp parsing
// ---------------------------------------------------------------------------

static double parse_iso8601(std::string_view sv) {
    if (sv.size() < 20) return -1.0;
    char buf[32];
    size_t n = std::min(sv.size(), sizeof(buf) - 1);
    sv.copy(buf, n);
    buf[n] = '\0';

    struct tm t{};
    const char* p = strptime(buf, "%Y-%m-%dT%H:%M:%S", &t);
    if (!p) return -1.0;

    double epoch = static_cast<double>(timegm(&t));
    if (*p == '.') {
        ++p;
        int ms = 0, digits = 0;
        while (*p >= '0' && *p <= '9' && digits < 3) { ms = ms * 10 + (*p++ - '0'); ++digits; }
        while (digits++ < 3) ms *= 10;
        epoch += ms / 1000.0;
    }
    return epoch;
}

// ---------------------------------------------------------------------------
// Payload parsing
// ---------------------------------------------------------------------------

Row parse_payload(const std::string& payload_str, const TopicInfo& topic,
                  int project_id, const Config& cfg) {
    Row row;
    row.ints["project_id"] = project_id;
    row.strings = topic.strings;

    for (const auto& [kv_key, col_name] : cfg.topic_kv_map)
        if (auto it = topic.kv.find(kv_key); it != topic.kv.end())
            row.ints[col_name] = it->second;

    std::unordered_set<std::string> int_cols{"project_id"};
    for (const auto& [_, col] : cfg.topic_kv_map) int_cols.insert(col);

    thread_local simdjson::dom::parser sj_parser;
    simdjson::dom::element doc;
    if (sj_parser.parse(payload_str).get(doc) != simdjson::SUCCESS) return row;
    simdjson::dom::object obj;
    if (doc.get(obj) != simdjson::SUCCESS) return row;

    for (auto [key_sv, val] : obj) {
        std::string k(key_sv);
        if (!cfg.timestamp_field.empty() && k == cfg.timestamp_field) {
            if (cfg.timestamp_format == "iso8601") {
                std::string_view sv;
                if (val.get(sv) == simdjson::SUCCESS) {
                    double epoch = parse_iso8601(sv);
                    if (epoch >= 0.0) row.floats["ts"] = epoch;
                }
            } else {
                double d;
                if (val.get(d) == simdjson::SUCCESS) row.floats["ts"] = d;
            }
            continue;
        }
        if (int_cols.count(k)) {
            int64_t iv;
            if (val.get(iv) == simdjson::SUCCESS) row.ints[k] = iv;
        } else if (val.type() == simdjson::dom::element_type::OBJECT) {
            simdjson::dom::element ve;
            if (val["value"].get(ve) == simdjson::SUCCESS) {
                double d;
                if (ve.get(d) == simdjson::SUCCESS) row.floats[k] = d;
                else { int64_t iv; if (ve.get(iv) == simdjson::SUCCESS) row.floats[k] = static_cast<double>(iv); }
            }
        } else {
            double d;
            if (val.get(d) == simdjson::SUCCESS) row.floats[k] = d;
            else { int64_t iv; if (val.get(iv) == simdjson::SUCCESS) row.floats[k] = static_cast<double>(iv); }
        }
    }

    if (!topic.field_name.empty()) {
        auto it = row.floats.find("value");
        if (it != row.floats.end()) { row.floats[topic.field_name] = it->second; row.floats.erase(it); }
    }
    return row;
}

// ---------------------------------------------------------------------------
// Output path helpers
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

static std::string time_suffix() {
    auto t  = SysClock::to_time_t(SysClock::now());
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[20];
    strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
    return buf;
}

std::string make_output_path(const Config& cfg,
                              const std::string& source_type,
                              const std::string& partition_str) {
    auto t = SysClock::to_time_t(SysClock::now());
    std::tm tm{};
    gmtime_r(&t, &tm);
    char year[5], month[3], day[3], ts[20];
    strftime(year,  sizeof(year),  "%Y",            &tm);
    strftime(month, sizeof(month), "%m",            &tm);
    strftime(day,   sizeof(day),   "%d",            &tm);
    strftime(ts,    sizeof(ts),    "%Y%m%dT%H%M%SZ",&tm);

    std::map<std::string, std::string> vars {
        {"project_id",        std::to_string(cfg.project_id)},
        {cfg.partition_field, partition_str},
        {"site_id",           cfg.site_id},
        {"year", year}, {"month", month}, {"day", day},
    };

    fs::path dir(cfg.base_path);
    dir /= source_type;
    for (const auto& part : cfg.partitions)
        dir /= replace_vars(part, vars);

    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) std::cerr << "[path] create_directories: " << ec.message() << "\n" << std::flush;

    return (dir / (std::string(ts) + ".parquet")).string();
}

// ---------------------------------------------------------------------------
// Arrow table builder  (shared by parquet flush and IPC WAL)
// ---------------------------------------------------------------------------

static parquet::Compression::type to_parquet_compression(const std::string& s) {
    if (s == "zstd") return parquet::Compression::ZSTD;
    if (s == "gzip") return parquet::Compression::GZIP;
    if (s == "none") return parquet::Compression::UNCOMPRESSED;
    return parquet::Compression::SNAPPY;
}

// Build an Arrow table from a batch of rows.
// "value" is renamed to value_i / value_f when both int and float points appear
// in the same partition — avoids schema collision.
static arrow::Result<std::shared_ptr<arrow::Table>>
build_table(const std::vector<Row>& rows) {
    std::vector<std::string> int_cols, float_cols, str_cols;
    {
        std::map<std::string, bool> si, sf, ss;
        for (const auto& r : rows) {
            for (const auto& [k, _] : r.ints)    if (!si[k])  { si[k]=true;  int_cols.push_back(k); }
            for (const auto& [k, _] : r.floats)  if (!sf[k])  { sf[k]=true;  float_cols.push_back(k); }
            for (const auto& [k, _] : r.strings) if (!ss[k])  { ss[k]=true;  str_cols.push_back(k); }
        }
    }

    bool has_iv = std::find(int_cols.begin(),   int_cols.end(),   "value") != int_cols.end();
    bool has_fv = std::find(float_cols.begin(), float_cols.end(), "value") != float_cols.end();
    bool split  = has_iv && has_fv;
    if (split) {
        std::replace(int_cols.begin(),   int_cols.end(),   std::string("value"), std::string("value_i"));
        std::replace(float_cols.begin(), float_cols.end(), std::string("value"), std::string("value_f"));
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    for (const auto& col : int_cols) {
        const std::string lk = (split && col == "value_i") ? "value" : col;
        arrow::Int64Builder b;
        for (const auto& r : rows) {
            auto it = r.ints.find(lk);
            if (it != r.ints.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
            else                    ARROW_RETURN_NOT_OK(b.AppendNull());
        }
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::int64()));
        arrays.push_back(arr);
    }
    for (const auto& col : float_cols) {
        const std::string lk = (split && col == "value_f") ? "value" : col;
        arrow::DoubleBuilder b;
        for (const auto& r : rows) {
            auto it = r.floats.find(lk);
            if (it != r.floats.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
            else                      ARROW_RETURN_NOT_OK(b.AppendNull());
        }
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::float64()));
        arrays.push_back(arr);
    }
    for (const auto& col : str_cols) {
        arrow::StringBuilder b;
        for (const auto& r : rows) {
            auto it = r.strings.find(col);
            if (it != r.strings.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
            else                       ARROW_RETURN_NOT_OK(b.AppendNull());
        }
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b.Finish(&arr));
        fields.push_back(arrow::field(col, arrow::utf8()));
        arrays.push_back(arr);
    }

    return arrow::Table::Make(arrow::schema(fields), arrays);
}

// ---------------------------------------------------------------------------
// Parquet flush  (production path only)
// ---------------------------------------------------------------------------

arrow::Status flush_partition(const std::string& path,
                               const std::vector<Row>& rows,
                               const std::string& compression) {
    if (rows.empty()) return arrow::Status::OK();
    ARROW_ASSIGN_OR_RAISE(auto table, build_table(rows));
    auto props = parquet::WriterProperties::Builder()
        .compression(to_parquet_compression(compression))
        ->build();
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), out,
        static_cast<int64_t>(rows.size()), props));
    return arrow::Status::OK();
}

// Write a pre-built Arrow Table directly to a Parquet file.
// Used by the compactor, which works at the Table level to avoid Row round-trips.
static arrow::Status write_parquet_table(const std::string& path,
                                          const std::shared_ptr<arrow::Table>& table,
                                          const std::string& compression) {
    auto props = parquet::WriterProperties::Builder()
        .compression(to_parquet_compression(compression))
        ->build();
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), out,
        static_cast<int64_t>(table->num_rows()), props));
    return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// WAL — write-ahead log for crash recovery  (Arrow IPC format)
// ---------------------------------------------------------------------------
// Arrow IPC is C++17-compatible (no std::span dependency), small overhead,
// and lossless.  WAL files are internal — not intended for DuckDB queries.

static std::string wal_path_for(const Config& cfg,
                                  const std::string& source_type,
                                  const std::string& partition_str) {
    std::error_code ec;
    fs::create_directories(cfg.wal_path, ec);
    return cfg.wal_path + "/" + source_type + "_" + partition_str + "_" + time_suffix() + ".wal.arrow";
}

static arrow::Status write_wal_ipc(const std::string& path, const std::vector<Row>& rows) {
    if (rows.empty()) return arrow::Status::OK();
    ARROW_ASSIGN_OR_RAISE(auto table, build_table(rows));
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(out, table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(writer->Close());
    return out->Close();
}

// Read a WAL Arrow IPC file back into a vector of Rows.
static std::vector<Row> wal_replay_file(const std::string& path) {
    std::vector<Row> rows;
    try {
        auto r_infile = arrow::io::ReadableFile::Open(path);
        if (!r_infile.ok()) return rows;
        auto infile = r_infile.ValueOrDie();

        auto r_reader = arrow::ipc::RecordBatchFileReader::Open(infile);
        if (!r_reader.ok()) return rows;
        auto reader = r_reader.ValueOrDie();

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (int i = 0; i < reader->num_record_batches(); ++i) {
            auto r_batch = reader->ReadRecordBatch(i);
            if (!r_batch.ok()) return rows;
            batches.push_back(r_batch.ValueOrDie());
        }
        auto r_table = arrow::Table::FromRecordBatches(batches);
        if (!r_table.ok()) return rows;
        auto table = r_table.ValueOrDie();

        int ncols = static_cast<int>(table->num_columns());
        int nrows = static_cast<int>(table->num_rows());

        std::vector<std::pair<std::string, std::shared_ptr<arrow::Int64Array>>>  int_cols;
        std::vector<std::pair<std::string, std::shared_ptr<arrow::DoubleArray>>> dbl_cols;
        std::vector<std::pair<std::string, std::shared_ptr<arrow::StringArray>>> str_cols;

        for (int c = 0; c < ncols; ++c) {
            const auto& name = table->schema()->field(c)->name();
            auto type_id = table->schema()->field(c)->type()->id();
            auto col = table->column(c);
            auto chunk0 = col->chunk(0);
            if (type_id == arrow::Type::INT64)
                int_cols.emplace_back(name, std::static_pointer_cast<arrow::Int64Array>(chunk0));
            else if (type_id == arrow::Type::DOUBLE)
                dbl_cols.emplace_back(name, std::static_pointer_cast<arrow::DoubleArray>(chunk0));
            else if (type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING)
                str_cols.emplace_back(name, std::static_pointer_cast<arrow::StringArray>(chunk0));
        }

        rows.reserve(nrows);
        for (int r = 0; r < nrows; ++r) {
            Row row;
            for (auto& [name, arr] : int_cols)
                if (arr && !arr->IsNull(r)) row.ints[name] = arr->Value(r);
            for (auto& [name, arr] : dbl_cols)
                if (arr && !arr->IsNull(r)) row.floats[name] = arr->Value(r);
            for (auto& [name, arr] : str_cols)
                if (arr && !arr->IsNull(r)) row.strings[name] = arr->GetString(r);
            rows.push_back(std::move(row));
        }
    } catch (const std::exception& e) {
        std::cerr << "[wal] replay error for " << path << ": " << e.what() << "\n" << std::flush;
    }
    return rows;
}

// g_cfg and g_shutdown declared here (before compactor) so compact_thread_fn can reference them.
// All other globals remain in the "Global state" section below.
static std::atomic<bool> g_shutdown{false};
static const Config*     g_cfg{nullptr};

// ---------------------------------------------------------------------------
// Compactor — merges small flush files into larger ones, runs independently
//
// Operates entirely on the filesystem — no locks, no interaction with writer
// hot path.  Safety rules:
//   1. Only touches files older than compact_min_age_seconds (default: 2 × flush_interval)
//      so it never races with the current flush.
//   2. write-to-.tmp → rename → delete sources — same atomic pattern as flush.
//   3. Skips *.tmp, current_state.parquet, hot.parquet (non-timestamp names).
//   4. Both flush files (YYYYMMDDTHHMMSSZ.parquet) and previous compact files
//      (compact_*.parquet) are eligible — naturally produces larger files each round.
// ---------------------------------------------------------------------------

// Read a parquet file into an Arrow Table (used by compactor — avoids Row deserialization).
static arrow::Result<std::shared_ptr<arrow::Table>>
read_parquet_table(const std::string& path) {
    ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto reader,
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
    return table;
}

// Compact all files in one leaf directory.  Returns count of source files consumed.
static int compact_directory(const fs::path& dir,
                               const std::vector<fs::path>& files,
                               const Config& cfg) {
    std::vector<std::shared_ptr<arrow::Table>> tables;
    tables.reserve(files.size());
    for (const auto& f : files) {
        auto r = read_parquet_table(f.string());
        if (!r.ok()) {
            std::cerr << "[compact] skip " << f.filename().string()
                      << ": " << r.status().ToString() << "\n" << std::flush;
            return 0;  // bail — don't delete anything if any read fails
        }
        tables.push_back(*r);
    }

    // Unify schemas across partitions that may have different column sets.
    arrow::ConcatenateTablesOptions opts;
    opts.unify_schemas = true;
    auto r_merged = arrow::ConcatenateTables(tables, opts);
    if (!r_merged.ok()) {
        std::cerr << "[compact] merge failed in " << dir.string()
                  << ": " << r_merged.status().ToString() << "\n" << std::flush;
        return 0;
    }
    auto merged = *r_merged;

    auto ts       = time_suffix();
    auto tmp_path = (dir / ("compact_" + ts + ".parquet.tmp")).string();
    auto out_path = (dir / ("compact_" + ts + ".parquet")).string();

    auto st = write_parquet_table(tmp_path, merged, cfg.compression);
    if (!st.ok()) {
        std::cerr << "[compact] write failed: " << st.ToString() << "\n" << std::flush;
        std::error_code ec; fs::remove(tmp_path, ec);
        return 0;
    }

    std::error_code ec;
    fs::rename(tmp_path, out_path, ec);
    if (ec) {
        std::cerr << "[compact] rename failed: " << ec.message() << "\n" << std::flush;
        fs::remove(tmp_path, ec);
        return 0;
    }

    int deleted = 0;
    for (const auto& f : files) {
        fs::remove(f, ec);
        if (!ec) ++deleted;
        else std::cerr << "[compact] remove " << f.filename().string()
                       << " failed: " << ec.message() << "\n" << std::flush;
    }

    std::cout << "[compact] " << deleted << " → " << out_path
              << " (" << merged->num_rows() << " rows)\n";
    return deleted;
}

static void compact_thread_fn() {
    const int min_age_s = g_cfg->compact_min_age_seconds > 0
        ? g_cfg->compact_min_age_seconds
        : g_cfg->flush_interval_seconds * 2;

    std::cout << "[compact] thread started"
              << "  interval=" << g_cfg->compact_interval_seconds << "s"
              << "  min_files=" << g_cfg->compact_min_files
              << "  min_age=" << min_age_s << "s\n";

    while (!g_shutdown) {
        for (int i = 0; i < g_cfg->compact_interval_seconds && !g_shutdown; ++i)
            std::this_thread::sleep_for(std::chrono::seconds(1));
        if (g_shutdown) break;

        auto cutoff = fs::file_time_type::clock::now()
                      - std::chrono::seconds(min_age_s);

        // Scan base_path recursively; group eligible files by parent directory.
        std::error_code ec;
        std::map<fs::path, std::vector<fs::path>> dir_files;

        for (const auto& entry :
             fs::recursive_directory_iterator(g_cfg->base_path, ec)) {
            if (!entry.is_regular_file(ec)) continue;
            const auto& p = entry.path();
            if (p.extension() != ".parquet") continue;

            auto fname = p.filename().string();
            // Skip temp files
            if (fname.size() > 4 && fname.substr(fname.size() - 4) == ".tmp") continue;
            // Only compact flush files (start with digit) or previous compact files
            bool is_flush   = !fname.empty() && std::isdigit((unsigned char)fname[0]);
            bool is_compact = fname.size() > 8 && fname.substr(0, 8) == "compact_";
            if (!is_flush && !is_compact) continue;

            auto mtime = entry.last_write_time(ec);
            if (ec || mtime > cutoff) continue;

            dir_files[p.parent_path()].push_back(p);
        }
        if (ec) std::cerr << "[compact] scan error: " << ec.message() << "\n" << std::flush;

        int total_consumed = 0;
        for (auto& [dir, files] : dir_files) {
            if ((int)files.size() < g_cfg->compact_min_files) continue;
            std::sort(files.begin(), files.end());   // timestamp order
            total_consumed += compact_directory(dir, files, *g_cfg);
        }
        if (total_consumed > 0)
            std::cout << "[compact] cycle done — consumed " << total_consumed << " files\n";
    }
    std::cout << "[compact] thread exiting\n";
}

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

static std::map<PartitionKey, Partition> g_buffers;
static std::map<SensorKey, Row>          g_current_state;
static std::mutex                        g_mutex;
static std::condition_variable           g_flush_cv;
// g_shutdown and g_cfg declared earlier (before compactor thread)
static size_t                            g_total_buffered{0};

// Metrics — all atomics so health thread can read without lock
static std::atomic<uint64_t> g_msgs_received    {0};
static std::atomic<uint64_t> g_overflow_drops   {0};
static std::atomic<uint64_t> g_flush_count      {0};
static std::atomic<uint64_t> g_total_rows_written{0};
static std::atomic<uint64_t> g_last_flush_rows  {0};
static std::atomic<uint64_t> g_last_flush_ms    {0};
static std::atomic<uint64_t> g_wal_replay_rows  {0};
static std::atomic<uint64_t> g_disk_free_gb_x10 {0};  // free GB × 10 (avoid float atomic)

// Sync tracking (under g_mutex)
static uint64_t g_received_since_sync {0};
static uint64_t g_sync_drops_detected {0};
static std::string g_sync_session_id;

// ---------------------------------------------------------------------------
// Disk space check
// ---------------------------------------------------------------------------

static bool check_disk_space(const std::string& path, uint64_t min_bytes) {
    std::error_code ec;
    auto si = fs::space(path, ec);
    if (ec) {
        std::cerr << "[disk] space check failed: " << ec.message() << "\n" << std::flush;
        return true;  // don't block flush on stat failure
    }
    uint64_t free_gb_x10 = si.available / (1024 * 1024 * 1024 / 10);
    g_disk_free_gb_x10.store(free_gb_x10);
    if (si.available < min_bytes) {
        std::cerr << "[disk] LOW SPACE: " << si.available / (1024.0*1024*1024)
                  << " GB free, minimum is " << min_bytes / (1024.0*1024*1024) << " GB\n";
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// do_flush — WAL → parquet → delete WAL
// ---------------------------------------------------------------------------

static void do_flush(std::map<PartitionKey, Partition> to_flush,
                     std::map<SensorKey, Row>          cs_snapshot) {

    if (!check_disk_space(g_cfg->base_path, g_cfg->min_free_bytes)) {
        std::cerr << "[flush] SKIPPED — insufficient disk space\n";
        return;
    }

    auto flush_start = Clock::now();
    uint64_t flush_rows = 0;
    std::vector<Row> hot_rows;
    const bool want_hot = !g_cfg->hot_file_path.empty();

    for (auto& [key, part] : to_flush) {
        if (part.rows.empty()) continue;
        if (want_hot)
            for (const auto& r : part.rows) hot_rows.push_back(r);

        // --- WAL: write before parquet (Arrow IPC — C++17, no parquet read dependency) ---
        // Write to .tmp first, then rename atomically so a mid-write kill never
        // leaves a partial file in the .wal/ directory (replay skips non-.arrow).
        std::string wal_file;
        if (g_cfg->wal_enabled) {
            wal_file = wal_path_for(*g_cfg, key.source_type, key.partition_value);
            const std::string wal_tmp = wal_file + ".tmp";
            auto ws = write_wal_ipc(wal_tmp, part.rows);
            if (!ws.ok()) {
                std::cerr << "[wal] write failed (" << ws.ToString()
                          << ") — proceeding without WAL for this batch\n";
                std::error_code ec; fs::remove(wal_tmp, ec);
                wal_file.clear();
            } else {
                std::error_code ec;
                fs::rename(wal_tmp, wal_file, ec);
                if (ec) {
                    std::cerr << "[wal] rename failed: " << ec.message()
                              << " — proceeding without WAL for this batch\n";
                    fs::remove(wal_tmp, ec);
                    wal_file.clear();
                }
            }
        }

        // --- Production parquet write (with retries) ---
        auto path = make_output_path(*g_cfg, key.source_type, key.partition_value);
        arrow::Status status;
        constexpr int MAX_RETRIES = 3;
        for (int attempt = 0; attempt <= MAX_RETRIES; ++attempt) {
            if (attempt > 0) {
                std::cerr << "[flush] retry " << attempt << "/" << MAX_RETRIES
                          << " for " << path << "\n" << std::flush;
                std::this_thread::sleep_for(std::chrono::seconds(1 << (attempt - 1)));
            }
            status = flush_partition(path, part.rows, g_cfg->compression);
            if (status.ok()) break;
            std::cerr << "[flush] attempt " << attempt + 1 << " failed: "
                      << status.ToString() << "\n" << std::flush;
        }

        if (status.ok()) {
            flush_rows += part.rows.size();
            std::cout << "[flush] " << part.rows.size() << " rows → " << path << "\n" << std::flush;
            // WAL no longer needed — parquet is safe
            if (!wal_file.empty()) {
                std::error_code ec;
                fs::remove(wal_file, ec);
                if (ec) std::cerr << "[wal] remove failed: " << ec.message() << "\n" << std::flush;
            }
        } else {
            std::cerr << "[flush] PERMANENT ERROR — " << part.rows.size()
                      << " rows LOST for " << key.source_type << "/" << key.partition_value
                      << ": " << status.ToString() << "\n" << std::flush;
            if (!wal_file.empty())
                std::cerr << "[wal] WAL retained for recovery: " << wal_file << "\n" << std::flush;
        }
    }

    // Hot file
    if (want_hot && !hot_rows.empty()) {
        auto tmp    = g_cfg->hot_file_path + ".tmp";
        auto status = flush_partition(tmp, hot_rows, g_cfg->compression);
        if (status.ok()) {
            std::error_code ec;
            fs::rename(tmp, g_cfg->hot_file_path, ec);
            if (ec) std::cerr << "[hot] rename failed: " << ec.message() << "\n" << std::flush;
            else    std::cout << "[hot] " << hot_rows.size() << " rows → "
                              << g_cfg->hot_file_path << "\n" << std::flush;
        } else {
            std::cerr << "[hot] write failed: " << status.ToString() << "\n" << std::flush;
            fs::remove(tmp);
        }
    }

    // Current-state file
    if (!g_cfg->current_state_path.empty() && !cs_snapshot.empty()) {
        std::vector<Row> cs_rows;
        cs_rows.reserve(cs_snapshot.size());
        for (auto& [k, r] : cs_snapshot) cs_rows.push_back(r);
        auto tmp    = g_cfg->current_state_path + ".tmp";
        auto status = flush_partition(tmp, cs_rows, g_cfg->compression);
        if (status.ok()) {
            std::error_code ec;
            fs::rename(tmp, g_cfg->current_state_path, ec);
            if (ec) std::cerr << "[cs] rename failed: " << ec.message() << "\n" << std::flush;
            else    std::cout << "[cs] " << cs_rows.size() << " sensors → "
                              << g_cfg->current_state_path << "\n" << std::flush;
        } else {
            std::cerr << "[cs] write failed: " << status.ToString() << "\n" << std::flush;
            fs::remove(tmp);
        }
    }

    // Metrics
    auto flush_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - flush_start).count();
    g_flush_count.fetch_add(1);
    g_total_rows_written.fetch_add(flush_rows);
    g_last_flush_rows.store(flush_rows);
    g_last_flush_ms.store(static_cast<uint64_t>(flush_ms));

    if (flush_rows > 0) {
        double rps = flush_rows / std::max(flush_ms / 1000.0, 0.001);
        std::cout << "[flush] #" << g_flush_count.load()
                  << "  rows=" << flush_rows
                  << "  duration=" << flush_ms << "ms"
                  << "  rows/s=" << static_cast<uint64_t>(rps)
                  << "  total=" << g_total_rows_written.load() << "\n" << std::flush;
    }
}

// ---------------------------------------------------------------------------
// WAL startup replay
// ---------------------------------------------------------------------------

// Find and replay all WAL files left over from a previous crash.
// Called once in main() before the flush thread and MQTT loop start.
static void replay_wal_files() {
    std::error_code ec;
    if (!fs::exists(g_cfg->wal_path, ec)) return;

    std::vector<fs::path> wal_files;
    for (const auto& entry : fs::directory_iterator(g_cfg->wal_path, ec)) {
        if (entry.path().extension() == ".arrow")
            wal_files.push_back(entry.path());
    }
    if (wal_files.empty()) return;

    std::cout << "[wal] found " << wal_files.size()
              << " WAL file(s) — replaying before accepting live data\n";

    for (const auto& wp : wal_files) {
        auto rows = wal_replay_file(wp.string());
        if (rows.empty()) {
            std::cerr << "[wal] " << wp.filename() << " — empty or unreadable, skipping\n";
            fs::remove(wp, ec);
            continue;
        }

        std::cout << "[wal] replaying " << rows.size() << " rows from " << wp.filename() << "\n" << std::flush;
        g_wal_replay_rows.fetch_add(rows.size());

        // Immediately flush replayed rows to production parquet (don't hold in buffer)
        // Group by partition key first
        std::map<PartitionKey, std::vector<Row>> replay_parts;
        for (auto& r : rows) {
            std::string pval;
            auto sit = r.strings.find(g_cfg->partition_field);
            if (sit != r.strings.end()) pval = sit->second;
            else {
                auto iit = r.ints.find(g_cfg->partition_field);
                if (iit != r.ints.end()) pval = std::to_string(iit->second);
            }
            // source_type: derive from topic or default to "unit"
            replay_parts[{"unit", pval}].push_back(std::move(r));
        }

        bool all_ok = true;
        for (auto& [key, part_rows] : replay_parts) {
            auto path   = make_output_path(*g_cfg, key.source_type, key.partition_value);
            auto status = flush_partition(path, part_rows, g_cfg->compression);
            if (status.ok()) {
                std::cout << "[wal] flushed " << part_rows.size() << " replayed rows → " << path << "\n" << std::flush;
            } else {
                std::cerr << "[wal] replay flush failed: " << status.ToString() << "\n" << std::flush;
                all_ok = false;
            }
        }
        if (all_ok) {
            fs::remove(wp, ec);
        } else {
            std::cerr << "[wal] keeping " << wp.filename() << " (flush failed)\n";
        }
    }
}

// ---------------------------------------------------------------------------
// Flush thread
// ---------------------------------------------------------------------------

static void flush_thread_fn() {
    while (!g_shutdown) {
        std::unique_lock<std::mutex> lock(g_mutex);
        g_flush_cv.wait_for(lock, std::chrono::seconds(g_cfg->flush_interval_seconds));
        auto to_flush    = std::exchange(g_buffers, {});
        g_total_buffered = 0;
        auto cs_snap     = g_current_state;
        lock.unlock();
        do_flush(std::move(to_flush), std::move(cs_snap));
    }
    // Final flush on shutdown
    std::unique_lock<std::mutex> lock(g_mutex);
    auto to_flush    = std::exchange(g_buffers, {});
    g_total_buffered = 0;
    auto cs_snap     = g_current_state;
    lock.unlock();
    do_flush(std::move(to_flush), std::move(cs_snap));
}

// ---------------------------------------------------------------------------
// Health HTTP endpoint
// ---------------------------------------------------------------------------

static void health_thread_fn() {
    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { std::cerr << "[health] socket() failed\n"; return; }

    int opt = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(g_cfg->health_port));

    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "[health] bind() failed on port " << g_cfg->health_port << "\n" << std::flush;
        ::close(server_fd);
        return;
    }
    ::listen(server_fd, 4);
    std::cout << "[health] listening on :" << g_cfg->health_port << "/health\n";

    while (!g_shutdown) {
        // Short accept timeout so we can check g_shutdown
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(server_fd, &fds);
        timeval tv{1, 0};
        if (::select(server_fd + 1, &fds, nullptr, nullptr, &tv) <= 0) continue;

        int client_fd = ::accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) continue;

        // Read request (we don't inspect it — any GET returns health JSON)
        char rbuf[256];
        ::recv(client_fd, rbuf, sizeof(rbuf) - 1, 0);

        double disk_gb = g_disk_free_gb_x10.load() / 10.0;
        uint64_t buffered;
        {
            std::lock_guard<std::mutex> lk(g_mutex);
            buffered = g_total_buffered;
        }

        std::ostringstream body;
        body << "{"
             << "\"status\":\"ok\","
             << "\"site_id\":\"" << g_cfg->site_id << "\","
             << "\"msgs_received\":"     << g_msgs_received.load()    << ","
             << "\"buffer_rows\":"       << buffered                   << ","
             << "\"overflow_drops\":"    << g_overflow_drops.load()    << ","
             << "\"sync_drops\":"        << g_sync_drops_detected      << ","
             << "\"flush_count\":"       << g_flush_count.load()       << ","
             << "\"total_rows_written\":" << g_total_rows_written.load() << ","
             << "\"last_flush_rows\":"   << g_last_flush_rows.load()   << ","
             << "\"last_flush_ms\":"     << g_last_flush_ms.load()     << ","
             << "\"wal_replay_rows\":"   << g_wal_replay_rows.load()   << ","
             << "\"disk_free_gb\":"      << disk_gb
             << "}";

        std::string b = body.str();
        std::ostringstream resp;
        resp << "HTTP/1.1 200 OK\r\n"
             << "Content-Type: application/json\r\n"
             << "Content-Length: " << b.size() << "\r\n"
             << "Connection: close\r\n\r\n"
             << b;
        std::string rs = resp.str();
        ::send(client_fd, rs.c_str(), rs.size(), 0);
        ::close(client_fd);
    }
    ::close(server_fd);
}

// ---------------------------------------------------------------------------
// MQTT callbacks
// ---------------------------------------------------------------------------

static struct mosquitto* g_mosq{nullptr};

static void on_connect(struct mosquitto*, void*, int rc) {
    if (rc == 0) {
        std::cout << "[mqtt] connected to " << g_cfg->mqtt_host << ":" << g_cfg->mqtt_port << "\n" << std::flush;
        int sub_rc = mosquitto_subscribe(g_mosq, nullptr, g_cfg->mqtt_topic.c_str(), g_cfg->mqtt_qos);
        if (sub_rc != MOSQ_ERR_SUCCESS)
            std::cerr << "[mqtt] subscribe failed: " << mosquitto_strerror(sub_rc) << "\n" << std::flush;
    } else {
        std::cerr << "[mqtt] connect failed: " << mosquitto_connack_string(rc) << "\n" << std::flush;
    }
}

static void handle_sync_message(const std::string& payload) {
    thread_local simdjson::dom::parser sj;
    simdjson::dom::element doc;
    if (sj.parse(payload).get(doc) != simdjson::SUCCESS) return;

    std::string_view session_sv;
    int64_t seq = 0, interval_pub = 0, total_pub = 0;
    if (doc["session_id"].get(session_sv)          != simdjson::SUCCESS) return;
    if (doc["seq"].get(seq)                         != simdjson::SUCCESS) return;
    if (doc["interval_published"].get(interval_pub) != simdjson::SUCCESS) return;
    if (doc["total_published"].get(total_pub) != simdjson::SUCCESS) total_pub = 0;

    std::string session(session_sv);
    if (session != g_sync_session_id) {
        if (!g_sync_session_id.empty())
            std::cout << "[sync] new session " << session
                      << " (was " << g_sync_session_id << ") — counters reset\n";
        g_sync_session_id     = session;
        g_received_since_sync = 0;
        return;
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
                  << "  cumulative=" << g_sync_drops_detected << "\n" << std::flush;
    } else {
        std::cout << "[sync] #" << seq
                  << "  OK  received=" << received
                  << "  expected=" << interval_pub
                  << "  total_published=" << total_pub << "\n" << std::flush;
    }
}

static void on_message(struct mosquitto*, void*, const struct mosquitto_message* msg) {
    if (!msg->payloadlen) return;

    std::string topic(msg->topic);
    std::string payload(static_cast<const char*>(msg->payload), msg->payloadlen);

    if (topic == "_sync") {
        std::lock_guard<std::mutex> lock(g_mutex);
        handle_sync_message(payload);
        return;
    }

    std::optional<TopicInfo> info_opt;
    if (g_cfg->topic_parser == TopicParser::POSITIONAL)
        info_opt = parse_topic_positional(topic, g_cfg->topic_segments);
    else
        info_opt = parse_topic_kv(topic);
    if (!info_opt) return;

    Row row = parse_payload(payload, *info_opt, g_cfg->project_id, *g_cfg);

    if (!g_cfg->site_id.empty())
        row.strings["site_id"] = g_cfg->site_id;

    if (!info_opt->dtype_hint.empty() &&
        (info_opt->dtype_hint == "integer" || info_opt->dtype_hint == "boolean_integer")) {
        auto it = row.floats.find("value");
        if (it != row.floats.end()) {
            row.ints["value"] = static_cast<int64_t>(std::llround(it->second));
            row.floats.erase(it);
        }
    }

    std::string pval;
    {
        auto sit = row.strings.find(g_cfg->partition_field);
        if (sit != row.strings.end()) pval = sit->second;
        else {
            auto iit = row.ints.find(g_cfg->partition_field);
            if (iit != row.ints.end()) pval = std::to_string(iit->second);
        }
    }

    PartitionKey key{info_opt->source_type, pval};
    std::lock_guard<std::mutex> lock(g_mutex);

    if (g_total_buffered >= static_cast<size_t>(g_cfg->max_total_buffer_rows)) {
        auto drops = g_overflow_drops.fetch_add(1) + 1;
        if (drops == 1 || drops % 10000 == 0)
            std::cerr << "[buffer] OVERFLOW — " << drops << " rows dropped\n";
        g_flush_cv.notify_one();
        return;
    }

    if (!g_cfg->current_state_path.empty())
        g_current_state[SensorKey{info_opt->source_type, row.ints, row.strings}] = row;

    g_msgs_received.fetch_add(1);
    auto& part = g_buffers[key];
    part.rows.push_back(std::move(row));
    ++g_total_buffered;
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
        std::cerr << "[mqtt] " << str << "\n" << std::flush;
}

static void handle_signal(int) { g_shutdown = true; g_flush_cv.notify_all(); }

// ---------------------------------------------------------------------------
// Startup validation
// ---------------------------------------------------------------------------

static bool validate_config(const Config& cfg) {
    bool ok = true;
    if (cfg.site_id.empty()) {
        std::cerr << "[startup] ERROR: output.site_id is required\n"; ok = false;
    }
    if (cfg.base_path.empty()) {
        std::cerr << "[startup] ERROR: output.base_path is required\n"; ok = false;
    }
    if (!ok) return false;

    // Ensure base_path exists and is writable
    std::error_code ec;
    fs::create_directories(cfg.base_path, ec);
    if (ec) {
        std::cerr << "[startup] ERROR: cannot create base_path " << cfg.base_path
                  << ": " << ec.message() << "\n" << std::flush;
        return false;
    }
    // Write-test
    auto test = fs::path(cfg.base_path) / ".write_test";
    std::ofstream f(test);
    if (!f) {
        std::cerr << "[startup] ERROR: base_path " << cfg.base_path << " is not writable\n";
        return false;
    }
    f.close();
    fs::remove(test, ec);

    // Disk space
    if (!check_disk_space(cfg.base_path, cfg.min_free_bytes)) {
        std::cerr << "[startup] WARNING: low disk space at startup\n";
        // warn only, don't abort
    }

    // topic_segments must be set in positional mode
    if (cfg.topic_parser == TopicParser::POSITIONAL && cfg.topic_segments.empty()) {
        std::cerr << "[startup] WARNING: positional parser with empty topic_segments\n";
    }

    return true;
}

// ---------------------------------------------------------------------------
// Main  (omitted when REAL_WRITER_NO_MAIN is defined, e.g. for unit tests)
// ---------------------------------------------------------------------------

#ifndef REAL_WRITER_NO_MAIN
int main(int argc, char* argv[]) {
    std::string config_path = "real_writer_config.yaml";
    for (int i = 1; i < argc - 1; i++)
        if (std::string(argv[i]) == "--config") config_path = argv[i + 1];

    Config cfg = load_config(config_path);
    g_cfg = &cfg;

    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    // Validate before touching anything
    if (!validate_config(cfg)) return 1;

    std::cout << "[real_writer] site=" << cfg.site_id
              << "  topic='" << cfg.mqtt_topic << "'"
              << "  flush=" << cfg.flush_interval_seconds << "s"
              << "  wal=" << (cfg.wal_enabled ? "on" : "off")
              << "  health=:" << cfg.health_port << "\n" << std::flush;

    // Replay any WAL files from a previous crash before going live
    if (cfg.wal_enabled) replay_wal_files();

    mosquitto_lib_init();
    auto* mosq = mosquitto_new(cfg.mqtt_client_id.c_str(), /*clean_session=*/true, nullptr);
    if (!mosq) { std::cerr << "[mqtt] mosquitto_new failed\n"; return 1; }
    g_mosq = mosq;

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_log_callback_set(mosq, on_log);
    mosquitto_reconnect_delay_set(mosq, 2, 30, /*exponential=*/true);

    while (!g_shutdown) {
        int rc = mosquitto_connect(mosq, cfg.mqtt_host.c_str(), cfg.mqtt_port, /*keepalive=*/60);
        if (rc == MOSQ_ERR_SUCCESS) break;
        std::cerr << "[mqtt] connect: " << mosquitto_strerror(rc) << " — retrying in 5s\n";
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    mosquitto_loop_start(mosq);

    std::thread flusher(flush_thread_fn);
    std::thread health_t;
    if (cfg.health_enabled)
        health_t = std::thread(health_thread_fn);
    std::thread compact_t;
    if (cfg.compact_enabled)
        compact_t = std::thread(compact_thread_fn);

    flusher.join();
    if (health_t.joinable())  health_t.join();
    if (compact_t.joinable()) compact_t.join();

    mosquitto_loop_stop(mosq, /*force=*/true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}
#endif // REAL_WRITER_NO_MAIN
