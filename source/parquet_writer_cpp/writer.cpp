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
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <simdjson.h>
#include <yaml-cpp/yaml.h>

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
// Buffer + flush thread
// ---------------------------------------------------------------------------

static std::map<PartitionKey, Partition> g_buffers;
static std::map<SensorKey, Row>          g_current_state;  // latest row per sensor
static std::mutex                        g_mutex;
static std::condition_variable           g_flush_cv;
static std::atomic<bool>                 g_shutdown{false};
static const Config*                     g_cfg{nullptr};
static size_t                            g_total_buffered{0};    // total rows in g_buffers (under g_mutex)
static std::atomic<uint64_t>             g_overflow_drops{0};    // rows dropped due to buffer full
static uint64_t                          g_received_since_sync{0}; // msgs received since last _sync (under g_mutex)
static uint64_t                          g_sync_drops_detected{0}; // cumulative drops reported via sync
static std::string                       g_sync_session_id;      // detect stress_runner restarts

static void do_flush(std::map<PartitionKey, Partition> to_flush,
                     std::map<SensorKey, Row>          cs_snapshot) {
    std::vector<Row> hot_rows;   // accumulated for hot file; empty if feature disabled
    const bool want_hot = !g_cfg->hot_file_path.empty();

    for (auto& [key, part] : to_flush) {
        if (part.rows.empty()) continue;

        if (want_hot)
            for (const auto& r : part.rows) hot_rows.push_back(r);

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
        if (status.ok())
            std::cout << "[flush] " << part.rows.size() << " rows → " << path << "\n";
        else
            std::cerr << "[flush] PERMANENT ERROR — " << part.rows.size() << " rows LOST for "
                      << key.source_type << "/" << g_cfg->partition_field
                      << "=" << key.partition_value << ": " << status.ToString() << "\n";
    }

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
        g_flush_cv.wait_for(lock, std::chrono::seconds(g_cfg->flush_interval_seconds));
        auto to_flush  = std::exchange(g_buffers, {});
        g_total_buffered = 0;                       // reset counter atomically with buffer swap
        auto cs_snap   = g_current_state;           // copy under same lock — atomic with buffer swap
        lock.unlock();                              // release before slow I/O
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
    (void)doc["total_published"].get(total_pub);  // optional, used only for logging

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
    g_shutdown = true;
    g_flush_cv.notify_all();
}

// ---------------------------------------------------------------------------
// Main  (omitted when PARQUET_WRITER_NO_MAIN is defined, e.g. for unit tests)
// ---------------------------------------------------------------------------

#ifndef PARQUET_WRITER_NO_MAIN
int main(int argc, char* argv[]) {
    std::string config_path = "config.yaml";
    for (int i = 1; i < argc - 1; i++)
        if (std::string(argv[i]) == "--config") config_path = argv[i + 1];

    Config cfg = load_config(config_path);
    g_cfg = &cfg;

    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    mosquitto_lib_init();

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
              << "  max_total=" << cfg.max_total_buffer_rows << "\n";

    // Background MQTT network loop (handles reconnects automatically)
    mosquitto_loop_start(mosq);

    std::thread flusher(flush_thread_fn);
    flusher.join();   // wait here until shutdown

    mosquitto_loop_stop(mosq, /*force=*/true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    return 0;
}
#endif // PARQUET_WRITER_NO_MAIN
