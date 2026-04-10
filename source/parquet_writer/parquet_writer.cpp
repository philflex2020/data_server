/**
 * parquet_writer.cpp  —  FlashMQ MQTT → partitioned Parquet writer (EMS / Longbow schema)
 *
 * Topic format:  unit/{unit_id}/{device}/{instance}/{point_name}/{dtype_hint}
 * Payload:       {"ts":"2024-11-15T21:27:52.775Z","value":0.999393}
 *
 * Key design decisions:
 *   - site_id comes from config, not topic (partition directory context)
 *   - dtype_hint drives INT64 vs FLOAT64 value storage; not written as a column
 *   - Parquet dictionary encoding handles repeated point_name strings for free
 *   - Lock-free SPSC ring buffer decouples libmosquitto network thread from
 *     JSON parsing; on_message does only a memcpy so FlashMQ never drops at 80k+ msg/s
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
 *      buffer_rows, overflow_drops, ring_drops, sync_drops, last_flush_ms, disk_free_gb.
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
 * Config:  writer_config.yaml  (or any path passed via --config)
 * Build:   make parquet_writer
 */

#include "mqtt_ring.h"
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
#include <fcntl.h>
#include <poll.h>
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
#include <set>
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
    std::string mqtt_client_id {"parquet-writer"};
    std::string mqtt_topic     {"unit/#"};
    int         mqtt_qos       {0};

    // Topic parser
    TopicParser topic_parser   {TopicParser::POSITIONAL};
    std::map<std::string, std::string> topic_kv_map {};
    // "dtype_hint" is reserved: consumed for type casting, not stored as a column.
    // "field_name" renames the "value" float to the signal name (wide-sparse schema).
    // Any other name stores the segment as a string column (long schema).
    std::vector<std::string> topic_segments {};

    // compound_point_name: join these segment columns with '/' → stored as row.strings["point_name"].
    // Named columns are consumed (removed) from the row.
    std::vector<std::string> compound_point_name {};

    // compound_field_name: wide (pivot) schema.
    // Join these segment columns with '/' and use the result as the value column name.
    // Each unique signal path becomes its own parquet column; all others are null per row.
    std::vector<std::string> compound_field_name {};

    // drop_point_names: discard any message whose parsed point_name matches.
    // Applied before compound field assembly — cheapest possible filter.
    std::unordered_set<std::string> drop_point_names {};

    // drop_columns: erase these columns from every row before writing.
    // Applied after compound_point_name / compound_field_name.
    std::vector<std::string> drop_columns {};

    // Multi-depth topic patterns (like Telegraf topic_parsing).
    // Each pattern: {match, segments}  where match uses + (any segment) and # (remainder).
    // First matching pattern wins.  Ignored when topic_segments is non-empty.
    struct TopicPattern {
        std::vector<std::string> match;   // pre-split on '/'
        std::vector<std::string> segs;
        bool reverse {false};  // if true, segs[0] = last topic segment (right-aligned)
    };
    std::vector<TopicPattern> topic_patterns {};

    // Site ID injected into every row from config (not from topic).
    std::string site_id        {""};
    std::string partition_field{"site_id"};

    // Timestamp
    std::string timestamp_field {"ts"};
    std::string timestamp_format{"iso8601"};

    // Optional column flags — all true by default; set false to suppress from parquet output.
    bool        store_mqtt_topic  {true};
    bool        store_project_id  {true};
    bool        store_sample_count{true};
    // wide_point_name: after partition key is captured, drop site_id and project_id.
    // Use with compound_point_name or compound_field_name — they're already in the path.
    bool        wide_point_name   {false};

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
    std::string filename_prefix            {""};   // prepended to timestamp filename: {prefix}_{ts}.parquet
    bool        partition_as_filename_prefix{false}; // prepend partition value to filename: {unit_id}_{ts}.parquet
    int         time_window_ms             {0};     // 0=disabled; >0: coalesce all msgs within this wall-clock window under one shared timestamp

    // WAL — write-ahead log for crash recovery
    bool        wal_enabled         {true};
    std::string wal_path            {""};   // default: {base_path}/.wal/

    // Disk space guard — skip flush and warn if free space falls below this
    uint64_t    min_free_bytes      {1ULL * 1024 * 1024 * 1024};  // 1 GB

    // Health HTTP endpoint
    bool        health_enabled      {true};
    int         health_port         {8771};

    // MQTT tuning
    int mqtt_keepalive_seconds     {60};
    int mqtt_reconnect_min_seconds {2};
    int mqtt_reconnect_max_seconds {30};
    int mqtt_connect_retry_seconds {5};

    // Flush retry tuning
    int flush_max_retries          {3};
    int flush_retry_base_seconds   {1};

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
            if (auto cp = m["compound_point_name"]) {
                cfg.compound_point_name.clear();
                for (const auto& s : cp) cfg.compound_point_name.push_back(s.as<std::string>());
            }
            if (auto cf = m["compound_field_name"]) {
                cfg.compound_field_name.clear();
                for (const auto& s : cf) cfg.compound_field_name.push_back(s.as<std::string>());
            }
            if (auto dp = m["drop_point_names"]) {
                cfg.drop_point_names.clear();
                for (const auto& s : dp) cfg.drop_point_names.insert(s.as<std::string>());
            }
            if (auto dc = m["drop_columns"]) {
                cfg.drop_columns.clear();
                for (const auto& s : dc) cfg.drop_columns.push_back(s.as<std::string>());
            }
            if (auto tp = m["topic_patterns"]) {
                cfg.topic_patterns.clear();
                for (const auto& entry : tp) {
                    Config::TopicPattern pat;
                    std::string match_str = entry["match"].as<std::string>();
                    std::istringstream ms(match_str);
                    std::string seg;
                    while (std::getline(ms, seg, '/')) pat.match.push_back(seg);
                    if (auto segs = entry["segments"]) {
                        for (const auto& s : segs)
                            pat.segs.push_back(s.as<std::string>());
                    }
                    if (entry["reverse"] && entry["reverse"].as<bool>())
                        pat.reverse = true;
                    cfg.topic_patterns.push_back(std::move(pat));
                }
            }
            if (m["partition_field"])       cfg.partition_field          = m["partition_field"].as<std::string>();
            if (m["timestamp_field"])       cfg.timestamp_field          = m["timestamp_field"].as<std::string>();
            if (m["timestamp_format"])      cfg.timestamp_format         = m["timestamp_format"].as<std::string>();
            if (m["keepalive_seconds"])     cfg.mqtt_keepalive_seconds     = m["keepalive_seconds"].as<int>();
            if (m["reconnect_min_seconds"]) cfg.mqtt_reconnect_min_seconds = m["reconnect_min_seconds"].as<int>();
            if (m["reconnect_max_seconds"]) cfg.mqtt_reconnect_max_seconds = m["reconnect_max_seconds"].as<int>();
            if (m["connect_retry_seconds"]) cfg.mqtt_connect_retry_seconds = m["connect_retry_seconds"].as<int>();
        }
        if (auto o = y["output"]) {
            if (o["base_path"])              cfg.base_path              = o["base_path"].as<std::string>();
            if (o["project_id"])             cfg.project_id             = o["project_id"].as<int>();
            if (o["compression"])            cfg.compression            = o["compression"].as<std::string>();
            if (o["flush_interval_seconds"]) cfg.flush_interval_seconds = o["flush_interval_seconds"].as<int>();
            if (o["max_messages_per_part"])  cfg.max_messages_per_part  = o["max_messages_per_part"].as<int>();
            if (o["max_total_buffer_rows"])  cfg.max_total_buffer_rows  = o["max_total_buffer_rows"].as<int>();
            if (o["flush_max_retries"])        cfg.flush_max_retries        = o["flush_max_retries"].as<int>();
            if (o["flush_retry_base_seconds"]) cfg.flush_retry_base_seconds = o["flush_retry_base_seconds"].as<int>();
            if (o["hot_file_path"])          cfg.hot_file_path          = o["hot_file_path"].as<std::string>();
            if (o["current_state_path"])     cfg.current_state_path     = o["current_state_path"].as<std::string>();
            if (o["filename_prefix"])                 cfg.filename_prefix                 = o["filename_prefix"].as<std::string>();
            if (o["partition_as_filename_prefix"])    cfg.partition_as_filename_prefix    = o["partition_as_filename_prefix"].as<bool>();
            if (o["time_window_ms"])                  cfg.time_window_ms                  = o["time_window_ms"].as<int>();
            if (o["site_id"])                cfg.site_id                = o["site_id"].as<std::string>();
            if (o["store_mqtt_topic"])   cfg.store_mqtt_topic   = o["store_mqtt_topic"].as<bool>();
            if (o["store_project_id"])   cfg.store_project_id   = o["store_project_id"].as<bool>();
            if (o["store_sample_count"]) cfg.store_sample_count = o["store_sample_count"].as<bool>();
            if (o["wide_point_name"])    cfg.wide_point_name    = o["wide_point_name"].as<bool>();
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

// Apply a segments mapping to already-split topic parts.
static void apply_segments(TopicInfo& info,
                            const std::vector<std::string>& parts,
                            const std::vector<std::string>& segs) {
    for (size_t i = 0; i < segs.size() && i < parts.size(); ++i) {
        const auto& col = segs[i];
        if (col == "_" || col.empty()) continue;
        if (col == "dtype_hint")
            info.dtype_hint = parts[i];
        else if (col == "field_name")
            info.field_name = parts[i];   // wide-sparse: renames "value" → signal name
        else
            info.strings[col] = parts[i]; // narrow: stored as a named string column
    }
}

// Returns true if topic_parts matches pattern_parts (+ = any one segment, # = remainder).
static bool match_topic_pattern(const std::vector<std::string>& topic_parts,
                                 const std::vector<std::string>& pattern_parts) {
    for (size_t i = 0; i < pattern_parts.size(); ++i) {
        if (pattern_parts[i] == "#") return true;   // matches remainder
        if (i >= topic_parts.size()) return false;
        if (pattern_parts[i] != "+" && pattern_parts[i] != topic_parts[i]) return false;
    }
    return topic_parts.size() == pattern_parts.size();
}

std::optional<TopicInfo> parse_topic_positional(
        const std::string& topic,
        const std::vector<std::string>& segments_map,
        const std::vector<Config::TopicPattern>& patterns = {}) {
    TopicInfo info;
    std::vector<std::string> parts;
    std::istringstream ss(topic);
    std::string seg;
    while (std::getline(ss, seg, '/'))
        parts.push_back(seg);

    if (parts.empty()) return std::nullopt;
    // source_type only used as path prefix when NOT using topic_patterns
    // (positional with patterns encodes all structure via partitions config)
    if (patterns.empty()) info.source_type = parts[0];

    // Multi-pattern matching (like Telegraf topic_parsing) — first match wins.
    if (!patterns.empty()) {
        for (const auto& pat : patterns) {
            if (match_topic_pattern(parts, pat.match)) {
                if (pat.reverse) {
                    // Map segs right-to-left: segs[0] = last part, segs[1] = second-to-last…
                    std::vector<std::string> rev_parts(parts.rbegin(), parts.rend());
                    apply_segments(info, rev_parts, pat.segs);
                } else {
                    apply_segments(info, parts, pat.segs);
                }
                return info;
            }
        }
        // No pattern matched — log once per unique depth+prefix for discovery
        {
            static std::mutex s_mtx;
            static std::unordered_set<std::string> s_seen;
            std::string key = std::to_string(parts.size()) + ":" +
                              (parts.size() > 0 ? parts[0] : "") + "/" +
                              (parts.size() > 1 ? parts[1] : "");
            std::lock_guard<std::mutex> lk(s_mtx);
            if (s_seen.insert(key).second)
                std::cerr << "[topic] unmatched structure depth=" << parts.size()
                          << " prefix=" << (parts.size()>1 ? parts[0]+"/"+parts[1] : topic)
                          << " example: " << topic << "\n";
        }
        return info;  // row still written — value captured, tags empty
    }

    // Single segments_map fallback
    apply_segments(info, parts, segments_map);
    return info;
}

// ---------------------------------------------------------------------------
// ISO 8601 timestamp parsing
// ---------------------------------------------------------------------------

// Returns microseconds since epoch, or -1 on parse failure.
static int64_t parse_iso8601(std::string_view sv) {
    if (sv.size() < 20) return -1;
    char buf[32];
    size_t n = std::min(sv.size(), sizeof(buf) - 1);
    sv.copy(buf, n);
    buf[n] = '\0';

    struct tm t{};
    const char* p = strptime(buf, "%Y-%m-%dT%H:%M:%S", &t);
    if (!p) return -1;

    int64_t epoch_us = static_cast<int64_t>(timegm(&t)) * 1'000'000LL;
    if (*p == '.') {
        ++p;
        int64_t frac = 0; int digits = 0;
        while (*p >= '0' && *p <= '9' && digits < 6) { frac = frac * 10 + (*p++ - '0'); ++digits; }
        while (digits++ < 6) frac *= 10;
        epoch_us += frac;
    }
    return epoch_us;
}

// ---------------------------------------------------------------------------
// Payload parsing
// ---------------------------------------------------------------------------

Row parse_payload(const std::string& payload_str, const TopicInfo& topic,
                  const Config& cfg) {
    Row row;
    row.strings = topic.strings;

    for (const auto& [kv_key, col_name] : cfg.topic_kv_map)
        if (auto it = topic.kv.find(kv_key); it != topic.kv.end())
            row.ints[col_name] = it->second;

    std::unordered_set<std::string> int_cols{};
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
                    int64_t us = parse_iso8601(sv);
                    if (us >= 0) row.ints["ts"] = us;
                }
            } else {
                // Numeric epoch: accept seconds (float) or microseconds (int)
                int64_t iv;
                if (val.get(iv) == simdjson::SUCCESS) row.ints["ts"] = iv;
                else { double d; if (val.get(d) == simdjson::SUCCESS) row.ints["ts"] = static_cast<int64_t>(d * 1'000'000LL); }
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
            else {
                int64_t iv;
                if (val.get(iv) == simdjson::SUCCESS) row.floats[k] = static_cast<double>(iv);
                else {
                    std::string_view sv;
                    if (val.get(sv) == simdjson::SUCCESS) row.strings[k] = std::string(sv);
                }
            }
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
        {cfg.partition_field, partition_str},
        {"site_id",           cfg.site_id},
        {"year", year}, {"month", month}, {"day", day},
    };

    fs::path dir(cfg.base_path);
    if (!source_type.empty()) dir /= source_type;
    for (const auto& part : cfg.partitions)
        dir /= replace_vars(part, vars);

    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) std::cerr << "[path] create_directories: " << ec.message() << "\n" << std::flush;

    std::string fname;
    if (cfg.partition_as_filename_prefix && !partition_str.empty())
        fname = partition_str + "_" + ts;
    else if (!cfg.filename_prefix.empty())
        fname = cfg.filename_prefix + "_" + ts;
    else
        fname = ts;
    return (dir / (fname + ".parquet")).string();
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
    // Use std::set so column order is always alphabetically sorted across flushes —
    // required for DuckDB to unify schemas when reading multiple parquet files.
    std::vector<std::string> int_cols, float_cols, str_cols;
    {
        std::set<std::string> si, sf, ss;
        for (const auto& r : rows) {
            for (const auto& [k, _] : r.ints)    si.insert(k);
            for (const auto& [k, _] : r.floats)  sf.insert(k);
            for (const auto& [k, _] : r.strings) ss.insert(k);
        }
        int_cols.assign(si.begin(), si.end());
        float_cols.assign(sf.begin(), sf.end());
        str_cols.assign(ss.begin(), ss.end());
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

    static const auto ts_type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    for (const auto& col : int_cols) {
        const std::string lk = (split && col == "value_i") ? "value" : col;
        std::shared_ptr<arrow::Array> arr;
        if (col == "ts") {
            arrow::TimestampBuilder b(ts_type, arrow::default_memory_pool());
            for (const auto& r : rows) {
                auto it = r.ints.find(lk);
                if (it != r.ints.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
                else                    ARROW_RETURN_NOT_OK(b.AppendNull());
            }
            ARROW_RETURN_NOT_OK(b.Finish(&arr));
            fields.push_back(arrow::field(col, ts_type));
        } else {
            arrow::Int64Builder b;
            for (const auto& r : rows) {
                auto it = r.ints.find(lk);
                if (it != r.ints.end()) ARROW_RETURN_NOT_OK(b.Append(it->second));
                else                    ARROW_RETURN_NOT_OK(b.AppendNull());
            }
            ARROW_RETURN_NOT_OK(b.Finish(&arr));
            fields.push_back(arrow::field(col, arrow::int64()));
        }
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
// Wide pivot merge — coalesce rows sharing the same ts into one dense row.
// Called before build_table when compound_field_name is active so that all
// signals arriving within a time window produce one row per timestamp instead
// of one sparse row per signal.
// ---------------------------------------------------------------------------
static std::vector<Row> merge_wide_rows(const std::vector<Row>& rows) {
    std::vector<int64_t> order;
    std::unordered_map<int64_t, Row> by_ts;
    for (const auto& r : rows) {
        auto ts_it = r.ints.find("ts");
        if (ts_it == r.ints.end()) continue;
        int64_t ts = ts_it->second;
        auto it = by_ts.find(ts);
        if (it == by_ts.end()) {
            by_ts[ts] = r;
            order.push_back(ts);
        } else {
            for (const auto& [k, v] : r.floats)  it->second.floats[k]  = v;
            for (const auto& [k, v] : r.ints)    it->second.ints[k]    = v;
            for (const auto& [k, v] : r.strings) it->second.strings[k] = v;
        }
    }
    std::vector<Row> merged;
    merged.reserve(order.size());
    for (auto ts : order) merged.push_back(std::move(by_ts[ts]));
    return merged;
}

// ---------------------------------------------------------------------------
// Parquet flush  (production path only)
// ---------------------------------------------------------------------------

arrow::Status flush_partition(const std::string& path,
                               const std::vector<Row>& rows,
                               const std::string& compression,
                               bool merge_wide = false) {
    if (rows.empty()) return arrow::Status::OK();
    std::vector<Row> merged;
    const std::vector<Row>* rp = &rows;
    if (merge_wide) { merged = merge_wide_rows(rows); rp = &merged; }
    ARROW_ASSIGN_OR_RAISE(auto table, build_table(*rp));
    auto props = parquet::WriterProperties::Builder()
        .compression(to_parquet_compression(compression))
        ->build();
    // Write to .tmp then rename atomically — a mid-write crash never leaves a corrupt final file.
    std::string tmp = path + ".tmp";
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(tmp));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), out,
        static_cast<int64_t>(rp->size()), props));
    ARROW_RETURN_NOT_OK(out->Close());
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) {
        fs::remove(tmp, ec);
        return arrow::Status::IOError("flush_partition rename failed: ", ec.message());
    }
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
            // Use Concatenate to flatten all chunks — chunk(0) silently drops data from
            // multi-chunk IPC tables (e.g. WAL files that spanned multiple record batches).
            auto combined = arrow::Concatenate(table->column(c)->chunks());
            if (!combined.ok()) continue;
            auto arr = combined.ValueOrDie();
            if (type_id == arrow::Type::INT64)
                int_cols.emplace_back(name, std::static_pointer_cast<arrow::Int64Array>(arr));
            else if (type_id == arrow::Type::DOUBLE)
                dbl_cols.emplace_back(name, std::static_pointer_cast<arrow::DoubleArray>(arr));
            else if (type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING)
                str_cols.emplace_back(name, std::static_pointer_cast<arrow::StringArray>(arr));
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

// SPSC ring — producer: libmosquitto network thread  consumer: parse_thread
// on_message does only a memcpy into a slot; all parsing happens off the network thread.
static MqttRing              g_mqtt_ring;
static std::atomic<bool>     g_mqtt_stopped{false};  // set after mosquitto_loop_stop

// Metrics — all atomics so health thread can read without lock
static std::atomic<uint64_t> g_msgs_received    {0};
static std::atomic<uint64_t> g_overflow_drops   {0};
static std::atomic<uint64_t> g_ring_drops       {0};  // ring full — parse thread too slow
static std::atomic<uint64_t> g_flush_count      {0};
static std::atomic<uint64_t> g_total_rows_written{0};
static std::atomic<uint64_t> g_last_flush_rows  {0};
static std::atomic<uint64_t> g_last_flush_ms    {0};  // total flush duration
static std::atomic<uint64_t> g_last_wal_ms      {0};  // WAL portion of last flush
static std::atomic<uint64_t> g_last_parquet_ms  {0};  // parquet portion of last flush
static std::atomic<uint64_t> g_flush_max_ms     {0};  // worst-case total flush ever
static std::atomic<uint64_t> g_flush_total_ms   {0};  // sum of all flush durations (÷ count = avg)
static std::atomic<bool>     g_flush_active     {false};
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

    g_flush_active.store(true, std::memory_order_release);
    auto flush_start = Clock::now();
    uint64_t flush_rows = 0;
    uint64_t wal_us = 0, parquet_us = 0;
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
            auto t0 = Clock::now();
            auto ws = write_wal_ipc(wal_tmp, part.rows);
            wal_us += std::chrono::duration_cast<std::chrono::microseconds>(
                          Clock::now() - t0).count();
            if (!ws.ok()) {
                std::cerr << "[wal] write failed (" << ws.ToString()
                          << ") — SKIPPING parquet flush for this partition\n" << std::flush;
                std::error_code ec; fs::remove(wal_tmp, ec);
                continue;  // no WAL = no crash-safe guarantee; skip this partition
            }
            std::error_code ec;
            fs::rename(wal_tmp, wal_file, ec);
            if (ec) {
                std::cerr << "[wal] rename failed: " << ec.message()
                          << " — SKIPPING parquet flush for this partition\n" << std::flush;
                fs::remove(wal_tmp, ec);
                continue;
            }
        }

        // --- Production parquet write (with retries) ---
        auto path = make_output_path(*g_cfg, key.source_type, key.partition_value);
        arrow::Status status;
        auto t0 = Clock::now();
        for (int attempt = 0; attempt <= g_cfg->flush_max_retries; ++attempt) {
            if (attempt > 0) {
                std::cerr << "[flush] retry " << attempt << "/" << g_cfg->flush_max_retries
                          << " for " << path << "\n" << std::flush;
                std::this_thread::sleep_for(
                    std::chrono::seconds(g_cfg->flush_retry_base_seconds << (attempt - 1)));
            }
            status = flush_partition(path, part.rows, g_cfg->compression,
                                         !g_cfg->compound_field_name.empty());
            if (status.ok()) break;
            std::cerr << "[flush] attempt " << attempt + 1 << " failed: "
                      << status.ToString() << "\n" << std::flush;
        }
        parquet_us += std::chrono::duration_cast<std::chrono::microseconds>(
                          Clock::now() - t0).count();

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
            std::cerr << "[flush] PERMANENT ERROR after " << g_cfg->flush_max_retries
                      << " retries — " << part.rows.size()
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
            std::error_code ec; fs::remove(tmp, ec);
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
            std::error_code ec; fs::remove(tmp, ec);
        }
    }

    // Metrics
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - flush_start).count();
    g_flush_count.fetch_add(1);
    g_total_rows_written.fetch_add(flush_rows);
    g_last_flush_rows.store(flush_rows);
    g_last_flush_ms.store(static_cast<uint64_t>(elapsed_ms));
    g_last_wal_ms.store(wal_us / 1000);
    g_last_parquet_ms.store(parquet_us / 1000);
    g_flush_total_ms.fetch_add(static_cast<uint64_t>(elapsed_ms));
    uint64_t prev = g_flush_max_ms.load(std::memory_order_relaxed);
    while (static_cast<uint64_t>(elapsed_ms) > prev &&
           !g_flush_max_ms.compare_exchange_weak(prev, static_cast<uint64_t>(elapsed_ms),
                                                  std::memory_order_relaxed)) {}
    g_flush_active.store(false, std::memory_order_release);

    if (flush_rows > 0) {
        uint64_t fc = g_flush_count.load();
        uint64_t avg_ms = fc > 0 ? g_flush_total_ms.load() / fc : 0;
        std::cout << "[flush] #" << fc
                  << "  rows=" << flush_rows
                  << "  total=" << elapsed_ms << "ms"
                  << "  wal=" << wal_us/1000 << "ms"
                  << "  parquet=" << parquet_us/1000 << "ms"
                  << "  avg=" << avg_ms << "ms"
                  << "  max=" << g_flush_max_ms.load() << "ms"
                  << "  cumulative=" << g_total_rows_written.load() << "\n" << std::flush;
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

        // Parse source_type from WAL filename: {source_type}_{pval}_{ts}.wal.arrow
        std::string stem = wp.stem().string();   // e.g. "unit_evelyn_20260320T120000Z.wal"
        if (stem.size() > 4 && stem.substr(stem.size() - 4) == ".wal")
            stem = stem.substr(0, stem.size() - 4);
        std::string source_type;
        auto first_us = stem.find('_');
        if (first_us != std::string::npos)
            source_type = stem.substr(0, first_us);
        if (source_type.empty()) source_type = "unit";

        std::cout << "[wal] replaying " << rows.size() << " rows from " << wp.filename()
                  << " (source_type=" << source_type << ")\n" << std::flush;
        g_wal_replay_rows.fetch_add(rows.size());

        // Derive collection timestamp from oldest row so replay writes to the correct
        // date partition, not today's date.
        int64_t min_ts = std::numeric_limits<int64_t>::max();
        for (const auto& r : rows)
            if (auto it = r.ints.find("ts"); it != r.ints.end())
                min_ts = std::min(min_ts, it->second);
        std::string collection_suffix;
        if (min_ts < std::numeric_limits<int64_t>::max()) {
            auto tp = SysClock::time_point{
                std::chrono::duration_cast<SysClock::duration>(
                    std::chrono::microseconds(min_ts))};
            auto t = SysClock::to_time_t(tp);
            std::tm tm{}; gmtime_r(&t, &tm);
            char buf[20]; strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
            collection_suffix = buf;
        }

        // Group by partition key and flush
        std::map<PartitionKey, std::vector<Row>> replay_parts;
        for (auto& r : rows) {
            std::string pval;
            auto sit = r.strings.find(g_cfg->partition_field);
            if (sit != r.strings.end()) pval = sit->second;
            else {
                auto iit = r.ints.find(g_cfg->partition_field);
                if (iit != r.ints.end()) pval = std::to_string(iit->second);
            }
            replay_parts[{source_type, pval}].push_back(std::move(r));
        }

        bool all_ok = true;
        for (auto& [key, part_rows] : replay_parts) {
            // Use collection_suffix in path if we recovered a timestamp, else use now
            auto path = make_output_path(*g_cfg, key.source_type, key.partition_value);
            // If we have a historical timestamp, override the date portion of the path
            // by calling make_output_path with a temporarily overridden clock is complex;
            // instead log the suffix so operators can see which date the data belongs to.
            if (!collection_suffix.empty())
                std::cout << "[wal] data timestamp: " << collection_suffix << "\n";
            auto status = flush_partition(path, part_rows, g_cfg->compression,
                                              !g_cfg->compound_field_name.empty());
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
    // Final flush on shutdown — parse_thread may still be inserting rows here;
    // main() does one more flush after parse_thread is joined to catch the residue.
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
        // 100ms poll timeout — exits within 100ms of g_shutdown
        struct pollfd pfd = { server_fd, POLLIN, 0 };
        if (::poll(&pfd, 1, 100) <= 0) continue;

        int client_fd = ::accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) continue;

        char rbuf[4096] = {};
        auto n = ::recv(client_fd, rbuf, sizeof(rbuf) - 1, 0);
        if (n <= 0) { ::close(client_fd); continue; }

        // Only respond to GET /health; return 404 for everything else
        std::string req(rbuf, static_cast<size_t>(n));
        bool valid = (req.rfind("GET ", 0) == 0) &&
                     (req.find(" /health ") != std::string::npos ||
                      req.find(" /health\r") != std::string::npos);
        if (!valid) {
            const char* r404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            ::send(client_fd, r404, strlen(r404), MSG_NOSIGNAL);
            ::close(client_fd); continue;
        }

        double disk_gb = g_disk_free_gb_x10.load() / 10.0;
        uint64_t buffered;
        {
            std::lock_guard<std::mutex> lk(g_mutex);
            buffered = g_total_buffered;
        }
        uint64_t ring_used = g_mqtt_ring.write_pos.load(std::memory_order_relaxed) -
                             g_mqtt_ring.read_pos.load(std::memory_order_relaxed);
        uint64_t fc      = g_flush_count.load();
        uint64_t avg_ms  = fc > 0 ? g_flush_total_ms.load() / fc : 0;

        std::ostringstream body;
        body << "{"
             << "\"status\":\"ok\","
             << "\"site_id\":\"" << g_cfg->site_id << "\","
             << "\"msgs_received\":"       << g_msgs_received.load()     << ","
             << "\"buffer_rows\":"         << buffered                    << ","
             << "\"overflow_drops\":"      << g_overflow_drops.load()     << ","
             << "\"ring_used\":"           << ring_used                   << ","
             << "\"ring_capacity\":"       << MQRING_CAPACITY             << ","
             << "\"ring_drops\":"          << g_ring_drops.load()         << ","
             << "\"topic_truncations\":"   << g_mqtt_ring.topic_truncations.load()    << ","
             << "\"payload_truncations\":" << g_mqtt_ring.payload_truncations.load()  << ","
             << "\"sync_drops\":"          << g_sync_drops_detected       << ","
             << "\"flush_active\":"        << (g_flush_active.load() ? 1 : 0) << ","
             << "\"flush_count\":"         << fc                          << ","
             << "\"last_flush_ms\":"       << g_last_flush_ms.load()      << ","
             << "\"last_wal_ms\":"         << g_last_wal_ms.load()        << ","
             << "\"last_parquet_ms\":"     << g_last_parquet_ms.load()    << ","
             << "\"flush_avg_ms\":"        << avg_ms                      << ","
             << "\"flush_max_ms\":"        << g_flush_max_ms.load()       << ","
             << "\"last_flush_rows\":"     << g_last_flush_rows.load()    << ","
             << "\"total_rows_written\":"  << g_total_rows_written.load() << ","
             << "\"wal_replay_rows\":"     << g_wal_replay_rows.load()    << ","
             << "\"disk_free_gb\":"        << disk_gb
             << "}";

        std::string b = body.str();
        std::ostringstream resp;
        resp << "HTTP/1.1 200 OK\r\n"
             << "Content-Type: application/json\r\n"
             << "Access-Control-Allow-Origin: *\r\n"
             << "Content-Length: " << b.size() << "\r\n"
             << "Connection: close\r\n\r\n"
             << b;
        std::string rs = resp.str();
        ::send(client_fd, rs.c_str(), rs.size(), MSG_NOSIGNAL);
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
        if (sub_rc != MOSQ_ERR_SUCCESS) {
            std::cerr << "[mqtt] subscribe failed (" << mosquitto_strerror(sub_rc)
                      << ") — disconnecting to force reconnect\n" << std::flush;
            mosquitto_disconnect(g_mosq);  // triggers on_connect again rather than silently receiving nothing
        }
    } else {
        std::cerr << "[mqtt] connect failed: " << mosquitto_connack_string(rc) << "\n" << std::flush;
    }
}

// Called from libmosquitto network thread — must return as fast as possible.
// Only copies topic+payload into the lock-free ring; parse_thread does all the work.
static void on_message(struct mosquitto*, void*, const struct mosquitto_message* msg) {
    if (!msg->payloadlen) return;
    if (!g_mqtt_ring.push(msg->topic, static_cast<int>(strlen(msg->topic)),
                          static_cast<const char*>(msg->payload), msg->payloadlen)) {
        auto drops = g_ring_drops.fetch_add(1) + 1;
        if (drops == 1 || drops % 10000 == 0)
            std::cerr << "[ring] FULL — " << drops << " messages dropped\n" << std::flush;
    }
}

static void on_disconnect(struct mosquitto*, void*, int rc) {
    if (rc != 0)
        std::cerr << "[mqtt] unexpected disconnect (rc=" << rc << ") — will reconnect\n";
}

static void on_log(struct mosquitto*, void*, int level, const char* str) {
    if (level == MOSQ_LOG_ERR || level == MOSQ_LOG_WARNING)
        std::cerr << "[mqtt] " << str << "\n" << std::flush;
}

// ---------------------------------------------------------------------------
// Sync message handler  (called from parse_thread under g_mutex)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Parse thread — drains g_mqtt_ring, does all JSON / topic parsing off the
// libmosquitto network thread so on_message stays at memcpy speed.
// ---------------------------------------------------------------------------

static void process_message(const char* topic, const char* payload) {
    std::string topic_str(topic);
    std::string payload_str(payload);

    if (topic_str == "_sync") {
        std::lock_guard<std::mutex> lock(g_mutex);
        handle_sync_message(payload_str);
        return;
    }

    std::optional<TopicInfo> info_opt;
    if (g_cfg->topic_parser == TopicParser::POSITIONAL)
        info_opt = parse_topic_positional(topic_str, g_cfg->topic_segments, g_cfg->topic_patterns);
    else
        info_opt = parse_topic_kv(topic_str);
    if (!info_opt) return;

    Row row = parse_payload(payload_str, *info_opt, *g_cfg);

    if (g_cfg->store_mqtt_topic)   row.strings["mqtt_topic"]  = topic_str;
    if (g_cfg->store_sample_count) row.ints["sample_count"]   = 1;
    if (g_cfg->store_project_id)   row.ints["project_id"]     = g_cfg->project_id;

    // dtype_hint: only store in long-format (no compound_field_name);
    // in wide-pivot it is meaningless (each row has mixed-dtype signals).
    if (!info_opt->dtype_hint.empty() && g_cfg->compound_field_name.empty())
        row.strings["dtype_hint"] = info_opt->dtype_hint;

    // drop_point_names: discard message before any further processing.
    if (!g_cfg->drop_point_names.empty()) {
        auto it = row.strings.find("point_name");
        if (it != row.strings.end() && g_cfg->drop_point_names.count(it->second))
            return;
    }

    // compound_field_name: build wide-schema column name early (consuming segments),
    // then apply to the value column below after string-value rename.
    std::string compound_field;
    if (!g_cfg->compound_field_name.empty()) {
        compound_field.reserve(80);
        for (const auto& col : g_cfg->compound_field_name) {
            if (!compound_field.empty()) compound_field += '/';
            auto it = row.strings.find(col);
            if (it != row.strings.end()) { compound_field += it->second; row.strings.erase(it); }
            else {
                auto ii = row.ints.find(col);
                if (ii != row.ints.end()) { compound_field += std::to_string(ii->second); row.ints.erase(ii); }
            }
        }
    }

    // compound_point_name: join segment columns into row.strings["point_name"].
    if (!g_cfg->compound_point_name.empty()) {
        std::string compound;
        compound.reserve(64);
        for (const auto& col : g_cfg->compound_point_name) {
            if (!compound.empty()) compound += '/';
            auto it = row.strings.find(col);
            if (it != row.strings.end()) { compound += it->second; row.strings.erase(it); }
            else {
                auto ii = row.ints.find(col);
                if (ii != row.ints.end()) { compound += std::to_string(ii->second); row.ints.erase(ii); }
            }
        }
        row.strings["point_name"] = std::move(compound);
    }

    for (const auto& col : g_cfg->drop_columns) {
        row.strings.erase(col);
        row.ints.erase(col);
        row.floats.erase(col);
    }

    // Apply config site_id only if topic parsing did not already provide one.
    if (!g_cfg->site_id.empty() && row.strings.find("site_id") == row.strings.end())
        row.strings["site_id"] = g_cfg->site_id;

    // Integer/boolean values stay as double in row.floats["value"] — EMS integers
    // (status bits, 0/1 booleans, small counts) are exact in float64, and a single
    // "value" column avoids COALESCE(value_f, value_i) in every query.

    // String payloads (dtype_hint="string"): rename "value" → "value_str".
    {
        auto it = row.strings.find("value");
        if (it != row.strings.end()) {
            row.strings["value_str"] = std::move(it->second);
            row.strings.erase(it);
        }
    }

    // Apply compound_field_name: move "value" float into the signal-named column.
    if (!compound_field.empty()) {
        auto fi = row.floats.find("value");
        if (fi != row.floats.end()) {
            row.floats[compound_field] = fi->second;
            row.floats.erase(fi);
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

    // wide_point_name: drop site_id and project_id after partition key captured.
    if (g_cfg->wide_point_name) {
        row.strings.erase("site_id");
        row.ints.erase("project_id");
    }

    // wide-pivot: partition value is already in the filename — drop it as a column.
    if (!g_cfg->compound_field_name.empty()) {
        row.strings.erase(g_cfg->partition_field);
        row.ints.erase(g_cfg->partition_field);
    }

    // Time window coalescing + last-write-wins dedup.
    // Statics are safe here — process_message is only called from parse_thread (single thread).
    static int64_t s_window_open_us = 0;
    static int64_t s_window_ts_us   = 0;
    // dedup_key → index in g_buffers[key].rows; cleared when window expires or after flush.
    static std::unordered_map<std::string, size_t> s_dedup;

    if (g_cfg->time_window_ms > 0) {
        auto now_us = static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
        int64_t window_dur_us = int64_t(g_cfg->time_window_ms) * 1000;
        auto ts_it = row.ints.find("ts");
        if (ts_it != row.ints.end()) {
            if (s_window_open_us == 0 || (now_us - s_window_open_us) >= window_dur_us) {
                s_window_open_us = now_us;
                s_window_ts_us   = ts_it->second;
                s_dedup.clear();
            }
            ts_it->second = s_window_ts_us;
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
    ++g_received_since_sync;

    // Last-write-wins: if this (partition, topic) already has a row in the current
    // window, overwrite it in-place rather than appending a duplicate.
    // Stale indices (from a flush that cleared g_buffers) are detected by the
    // size check — after std::exchange the partition starts at rows.size()==0.
    if (g_cfg->time_window_ms > 0) {
        std::string dk = pval + '\x00' + topic_str;
        auto& part = g_buffers[key];
        auto dit = s_dedup.find(dk);
        if (dit != s_dedup.end() && dit->second < part.rows.size()) {
            part.rows[dit->second] = std::move(row);   // overwrite — no row count change
            return;
        }
        s_dedup[dk] = part.rows.size();   // record index of the row we're about to push
    }

    auto& part = g_buffers[key];
    part.rows.push_back(std::move(row));
    ++g_total_buffered;
    if (static_cast<int>(part.rows.size()) >= g_cfg->max_messages_per_part)
        g_flush_cv.notify_one();
}

static void parse_thread_fn() {
    MqSlot slot;
    while (true) {
        if (g_mqtt_ring.pop(slot)) {
            process_message(slot.topic, slot.payload);
        } else if (g_mqtt_stopped.load(std::memory_order_acquire) && g_mqtt_ring.empty()) {
            break;
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }
    std::cout << "[parse] thread exiting\n" << std::flush;
}

// ---------------------------------------------------------------------------
// Signal handler
// ---------------------------------------------------------------------------

static void handle_signal(int) { g_shutdown = true; g_flush_cv.notify_all(); }

// ---------------------------------------------------------------------------
// Startup validation
// ---------------------------------------------------------------------------

static bool validate_config(const Config& cfg) {
    bool ok = true;
    // site_id may be empty when extracted from topic (partition_field = site_id)
    if (cfg.base_path.empty()) {
        std::cerr << "[startup] ERROR: output.base_path is required\n"; ok = false;
    }
    if (cfg.flush_interval_seconds <= 0) {
        std::cerr << "[startup] ERROR: flush_interval_seconds must be > 0 (got "
                  << cfg.flush_interval_seconds << ")\n"; ok = false;
    }
    if (cfg.max_total_buffer_rows <= 0) {
        std::cerr << "[startup] ERROR: max_total_buffer_rows must be > 0 (got "
                  << cfg.max_total_buffer_rows << ")\n"; ok = false;
    }
    if (cfg.max_messages_per_part <= 0) {
        std::cerr << "[startup] ERROR: max_messages_per_part must be > 0 (got "
                  << cfg.max_messages_per_part << ")\n"; ok = false;
    }
    static const std::unordered_set<std::string> valid_compression{
        "snappy", "gzip", "zstd", "none"};
    if (!valid_compression.count(cfg.compression)) {
        std::cerr << "[startup] ERROR: unknown compression '" << cfg.compression
                  << "' — valid: snappy gzip zstd none\n"; ok = false;
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

    // topic_segments or topic_patterns must be set in positional mode
    if (cfg.topic_parser == TopicParser::POSITIONAL &&
        cfg.topic_segments.empty() && cfg.topic_patterns.empty()) {
        std::cerr << "[startup] WARNING: positional parser with no topic_segments or topic_patterns\n";
    }

    return true;
}

// ---------------------------------------------------------------------------
// Main  (omitted when PARQUET_WRITER_NO_MAIN is defined, e.g. for unit tests)
// ---------------------------------------------------------------------------

#ifndef PARQUET_WRITER_NO_MAIN

static void print_help(const char* prog) {
    std::cout <<
"Usage: " << prog << " [--config <path>] [options] [--help]\n"
"\n"
"MQTT → partitioned Parquet writer (EMS / Longbow schema).\n"
"\n"
"Options (override config file values):\n"
"  --config <path>        YAML config file (default: writer_config.yaml)\n"
"  --host <hostname>      MQTT broker host (overrides mqtt.host)\n"
"  --port <n>             MQTT broker port (overrides mqtt.port)\n"
"  --client-id <id>       MQTT client ID — MUST be unique per instance\n"
"  --output <path>        Parquet output directory — MUST be unique per instance\n"
"  --health-port <n>      Health endpoint port — MUST be unique per instance\n"
"  --flush-interval <n>   Flush interval in seconds\n"
"  --help                 Show this help and exit\n";
}

int main(int argc, char* argv[]) {
    std::string config_path = "writer_config.yaml";
    std::string cli_host, cli_output, cli_client_id;
    int cli_port = 0, cli_health_port = 0, cli_flush_interval = 0;

    for (int i = 1; i < argc; i++) {
        std::string a(argv[i]);
        if (a == "--help")                                { print_help(argv[0]); return 0; }
        if      (a == "--config"        && i+1 < argc)   config_path       = argv[++i];
        else if (a == "--host"          && i+1 < argc)   cli_host          = argv[++i];
        else if (a == "--port"          && i+1 < argc)   cli_port          = std::atoi(argv[++i]);
        else if (a == "--output"        && i+1 < argc)   cli_output        = argv[++i];
        else if (a == "--client-id"     && i+1 < argc)   cli_client_id     = argv[++i];
        else if (a == "--health-port"   && i+1 < argc)   cli_health_port   = std::atoi(argv[++i]);
        else if (a == "--flush-interval"&& i+1 < argc)   cli_flush_interval= std::atoi(argv[++i]);
    }

    Config cfg = load_config(config_path);
    if (!cli_host.empty())       cfg.mqtt_host              = cli_host;
    if (cli_port > 0)            cfg.mqtt_port              = cli_port;
    if (!cli_output.empty())     cfg.base_path              = cli_output;
    if (!cli_client_id.empty())  cfg.mqtt_client_id         = cli_client_id;
    if (cli_health_port > 0)     cfg.health_port            = cli_health_port;
    if (cli_flush_interval > 0)  cfg.flush_interval_seconds = cli_flush_interval;

    g_cfg = &cfg;

    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGPIPE, SIG_IGN);  // health endpoint client disconnect must not kill the writer

    // Validate before touching anything
    if (!validate_config(cfg)) return 1;

    // Create WAL directory once at startup
    if (cfg.wal_enabled) {
        std::error_code ec;
        fs::create_directories(cfg.wal_path, ec);
        if (ec) { std::cerr << "[wal] mkdir failed: " << ec.message() << "\n"; return 1; }
    }

    std::cout << "[parquet_writer] site=" << cfg.site_id
              << "  topic='" << cfg.mqtt_topic << "'"
              << "  flush=" << cfg.flush_interval_seconds << "s"
              << "  wal=" << (cfg.wal_enabled ? "on" : "off")
              << "  health=:" << cfg.health_port
              << "  ring=" << MQRING_CAPACITY << " slots\n" << std::flush;

    // Replay any WAL files from a previous crash before going live
    if (cfg.wal_enabled) replay_wal_files();

    if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) {
        std::cerr << "[mqtt] mosquitto_lib_init failed\n"; return 1;
    }
    auto* mosq = mosquitto_new(cfg.mqtt_client_id.c_str(), /*clean_session=*/true, nullptr);
    if (!mosq) { std::cerr << "[mqtt] mosquitto_new failed\n"; return 1; }
    g_mosq = mosq;

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_log_callback_set(mosq, on_log);
    mosquitto_reconnect_delay_set(mosq, cfg.mqtt_reconnect_min_seconds,
                                       cfg.mqtt_reconnect_max_seconds, /*exponential=*/true);

    while (!g_shutdown) {
        int rc = mosquitto_connect(mosq, cfg.mqtt_host.c_str(), cfg.mqtt_port,
                                   cfg.mqtt_keepalive_seconds);
        if (rc == MOSQ_ERR_SUCCESS) break;
        std::cerr << "[mqtt] connect: " << mosquitto_strerror(rc)
                  << " — retrying in " << cfg.mqtt_connect_retry_seconds << "s\n";
        std::this_thread::sleep_for(std::chrono::seconds(cfg.mqtt_connect_retry_seconds));
    }

    mosquitto_loop_start(mosq);

    std::thread parse_t(parse_thread_fn);
    std::thread flusher(flush_thread_fn);
    std::thread health_t;
    if (cfg.health_enabled)
        health_t = std::thread(health_thread_fn);
    std::thread compact_t;
    if (cfg.compact_enabled)
        compact_t = std::thread(compact_thread_fn);

    // Shutdown sequence:
    // 1. g_shutdown fires → flusher wakes, does final flush, exits
    flusher.join();
    if (health_t.joinable())  health_t.join();
    if (compact_t.joinable()) compact_t.join();

    // 2. Stop MQTT (no new messages can arrive after this)
    mosquitto_loop_stop(mosq, /*force=*/true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    // 3. Signal parse_thread to drain remaining ring entries and exit
    g_mqtt_stopped.store(true, std::memory_order_release);
    parse_t.join();

    // 4. Final flush for rows parse_thread inserted after flusher's last sweep
    {
        std::unique_lock<std::mutex> lk(g_mutex);
        auto to_flush    = std::exchange(g_buffers, {});
        g_total_buffered = 0;
        auto cs_snap     = g_current_state;
        lk.unlock();
        if (!to_flush.empty()) {
            std::cout << "[shutdown] flushing " << to_flush.size()
                      << " partition(s) from ring drain\n" << std::flush;
            do_flush(std::move(to_flush), std::move(cs_snap));
        }
    }

    return 0;
}
#endif // PARQUET_WRITER_NO_MAIN
