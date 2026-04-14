/**
 * test_real_writer.cpp  —  unit tests for real_writer.cpp pure functions
 *
 * Covers:
 *   parse_topic_positional()  — EMS positional topic → TopicInfo
 *   parse_topic_kv()          — KV topic → TopicInfo
 *   parse_iso8601()           — ISO 8601 timestamp → epoch seconds
 *   parse_payload()           — EMS {"ts":..., "value":...} → Row
 *   flush_partition()         — Row vector with string columns → valid Parquet
 *   write_wal_ipc()           — rows → Arrow IPC WAL file
 *   wal_replay_file()         — Arrow IPC file → rows (with string columns)
 *
 * Build & run:
 *   make test_real_writer
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#include "../real_writer.cpp"
#pragma GCC diagnostic pop
#include "test_runner.h"

#include <cstdlib>   // mkdtemp
#include <fstream>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string tmp_dir() {
    char tmpl[] = "/tmp/rwtest_XXXXXX";
    return std::string(mkdtemp(tmpl));
}

// PAR1 magic byte check
static bool is_valid_parquet(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return false;
    char head[4], tail[4];
    f.read(head, 4);
    f.seekg(-4, std::ios::end);
    f.read(tail, 4);
    return std::string(head, 4) == "PAR1" && std::string(tail, 4) == "PAR1";
}

// EMS topic segments for unit/{unit_id}/{device}/{instance}/{point_name}/{dtype_hint}
static const std::vector<std::string> g_ems_segments {
    "_",           // [0] "unit" — prefix, skip
    "unit_id",     // [1]
    "device",      // [2]
    "instance",    // [3]
    "point_name",  // [4]
    "dtype_hint",  // [5]
};

// Default config for positional parsing with EMS segments
static Config make_ems_config() {
    Config cfg;
    cfg.topic_parser   = TopicParser::POSITIONAL;
    cfg.topic_segments = g_ems_segments;
    cfg.site_id        = "site-A";
    return cfg;
}

// ---------------------------------------------------------------------------
// parse_topic_positional
// ---------------------------------------------------------------------------

TEST("parse_topic_positional: EMS unit topic extracts all string fields") {
    auto r = parse_topic_positional(
        "unit/u001/ems/0/voltage/float",
        g_ems_segments);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->source_type,        std::string("unit"));
    EXPECT_EQ(r->strings.at("unit_id"),    std::string("u001"));
    EXPECT_EQ(r->strings.at("device"),     std::string("ems"));
    EXPECT_EQ(r->strings.at("instance"),   std::string("0"));
    EXPECT_EQ(r->strings.at("point_name"), std::string("voltage"));
    EXPECT_EQ(r->dtype_hint,               std::string("float"));
}

TEST("parse_topic_positional: dtype_hint not stored in strings map") {
    auto r = parse_topic_positional(
        "unit/u001/ems/0/voltage/float",
        g_ems_segments);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->strings.count("dtype_hint"), 0u);
    EXPECT_EQ(r->dtype_hint, std::string("float"));
}

TEST("parse_topic_positional: underscore slots are skipped") {
    // "_" at position 0 means the first segment (source_type) is not stored in strings
    auto r = parse_topic_positional(
        "unit/u001/ems/0/voltage/float",
        g_ems_segments);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->strings.count("_"), 0u);
}

TEST("parse_topic_positional: short topic — missing trailing segments ok") {
    auto r = parse_topic_positional("unit/u002/ems", g_ems_segments);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->strings.at("unit_id"), std::string("u002"));
    EXPECT_EQ(r->strings.at("device"),  std::string("ems"));
    EXPECT_EQ(r->strings.count("instance"), 0u);
}

TEST("parse_topic_positional: empty topic returns nullopt") {
    auto r = parse_topic_positional("", g_ems_segments);
    EXPECT_FALSE(r.has_value());
}

TEST("parse_topic_positional: single-segment topic sets source_type only") {
    auto r = parse_topic_positional("unit", g_ems_segments);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->source_type, std::string("unit"));
    EXPECT_TRUE(r->strings.empty());
}

// ---------------------------------------------------------------------------
// parse_topic_kv
// ---------------------------------------------------------------------------

TEST("parse_topic_kv: basic kv parsing") {
    auto r = parse_topic_kv("batteries/site=2/rack=1/module=3/cell=5");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->source_type,  std::string("batteries"));
    EXPECT_EQ(r->kv.at("site"),   2);
    EXPECT_EQ(r->kv.at("rack"),   1);
    EXPECT_EQ(r->kv.at("module"), 3);
    EXPECT_EQ(r->kv.at("cell"),   5);
}

TEST("parse_topic_kv: non-kv trailing segment becomes field_name") {
    auto r = parse_topic_kv("batteries/site=0/rack=0/voltage");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->field_name, std::string("voltage"));
}

TEST("parse_topic_kv: empty topic returns nullopt") {
    EXPECT_FALSE(parse_topic_kv("").has_value());
}

// ---------------------------------------------------------------------------
// parse_iso8601
// ---------------------------------------------------------------------------

TEST("parse_iso8601: basic datetime no fractional seconds") {
    double t = parse_iso8601("2024-11-15T21:27:52Z");
    EXPECT_TRUE(t > 0.0);
}

TEST("parse_iso8601: datetime with milliseconds") {
    double t = parse_iso8601("2024-11-15T21:27:52.775Z");
    EXPECT_TRUE(t > 0.0);
    // .775 → 0.775 extra seconds
    double t_base = parse_iso8601("2024-11-15T21:27:52Z");
    EXPECT_NEAR(t - t_base, 0.775, 0.001);
}

TEST("parse_iso8601: too-short string returns -1") {
    EXPECT_NEAR(parse_iso8601("2024-11"), -1.0, 0.01);
}

TEST("parse_iso8601: empty string returns -1") {
    EXPECT_NEAR(parse_iso8601(""), -1.0, 0.01);
}

TEST("parse_iso8601: non-datetime garbage returns -1") {
    EXPECT_NEAR(parse_iso8601("not-a-timestamp-at-all!!"), -1.0, 0.01);
}

// ---------------------------------------------------------------------------
// parse_payload  (EMS schema)
// ---------------------------------------------------------------------------

TEST("parse_payload: EMS iso8601 timestamp parsed to float ts column") {
    Config cfg = make_ems_config();
    TopicInfo topic;
    topic.source_type = "unit";
    topic.strings["unit_id"] = "u001";

    auto r = parse_payload(
        R"({"ts":"2024-11-15T21:27:52.775Z","value":0.999})",
        topic, 0, cfg);

    EXPECT_TRUE(r.floats.count("ts") > 0u);
    EXPECT_TRUE(r.floats.at("ts") > 0.0);
}

TEST("parse_payload: value field stored as float") {
    Config cfg = make_ems_config();
    TopicInfo topic;
    topic.source_type = "unit";

    auto r = parse_payload(
        R"({"ts":"2024-11-15T21:27:52.000Z","value":42.5})",
        topic, 0, cfg);

    EXPECT_NEAR(r.floats.at("value"), 42.5, 1e-9);
}

TEST("parse_payload: string columns from topic copied to row") {
    Config cfg = make_ems_config();
    TopicInfo topic;
    topic.source_type     = "unit";
    topic.strings["unit_id"]    = "u007";
    topic.strings["device"]     = "ems";
    topic.strings["point_name"] = "current";

    auto r = parse_payload(R"({"ts":"2024-01-01T00:00:00.000Z","value":1.5})",
                           topic, 0, cfg);

    EXPECT_EQ(r.strings.at("unit_id"),    std::string("u007"));
    EXPECT_EQ(r.strings.at("device"),     std::string("ems"));
    EXPECT_EQ(r.strings.at("point_name"), std::string("current"));
}

TEST("parse_payload: project_id injected as int column") {
    Config cfg = make_ems_config();
    TopicInfo topic;
    topic.source_type = "unit";

    auto r = parse_payload(R"({"ts":"2024-01-01T00:00:00.000Z","value":1.0})",
                           topic, 99, cfg);

    EXPECT_EQ(r.ints.at("project_id"), 99);
}

TEST("parse_payload: bad JSON returns row with topic strings intact") {
    Config cfg = make_ems_config();
    TopicInfo topic;
    topic.source_type         = "unit";
    topic.strings["unit_id"]  = "u001";

    auto r = parse_payload("{not valid json!!}", topic, 0, cfg);
    EXPECT_EQ(r.strings.at("unit_id"), std::string("u001"));
    EXPECT_TRUE(r.floats.empty());
}

TEST("parse_payload: field_name renames value column") {
    Config cfg = make_ems_config();
    TopicInfo topic;
    topic.source_type = "unit";
    topic.field_name  = "voltage";  // rename value → voltage

    auto r = parse_payload(R"({"ts":"2024-01-01T00:00:00.000Z","value":3.7})",
                           topic, 0, cfg);

    EXPECT_TRUE(r.floats.count("voltage") > 0u);
    EXPECT_EQ(r.floats.count("value"), 0u);
    EXPECT_NEAR(r.floats.at("voltage"), 3.7, 1e-9);
}

// ---------------------------------------------------------------------------
// flush_partition  (EMS rows with string columns)
// ---------------------------------------------------------------------------

static Row make_ems_row(const std::string& unit_id, const std::string& point,
                         double value, double ts) {
    Row r;
    r.ints["project_id"]      = 0;
    r.strings["unit_id"]      = unit_id;
    r.strings["point_name"]   = point;
    r.floats["ts"]            = ts;
    r.floats["value"]         = value;
    return r;
}

TEST("flush_partition: EMS rows with string columns produce valid parquet") {
    auto dir  = tmp_dir();
    auto path = dir + "/ems.parquet";
    std::vector<Row> rows{
        make_ems_row("u001", "voltage", 3.7, 1700000001.0),
        make_ems_row("u001", "current", 5.2, 1700000002.0),
    };
    auto st = flush_partition(path, rows, "snappy");
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(is_valid_parquet(path));
    fs::remove_all(dir);
}

TEST("flush_partition: empty rows writes no file") {
    auto dir  = tmp_dir();
    auto path = dir + "/empty.parquet";
    auto st   = flush_partition(path, {}, "snappy");
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(fs::exists(path));
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// WAL round-trip  (with string columns — real_writer schema)
// ---------------------------------------------------------------------------

TEST("wal: empty rows — no file written") {
    auto dir  = tmp_dir();
    auto path = dir + "/empty.wal.arrow";
    EXPECT_TRUE(write_wal_ipc(path, {}).ok());
    EXPECT_FALSE(fs::exists(path));
    fs::remove_all(dir);
}

TEST("wal: single EMS row round-trips int, float, and string columns") {
    auto dir  = tmp_dir();
    auto path = dir + "/ems.wal.arrow";
    std::vector<Row> rows{ make_ems_row("u001", "voltage", 3.7, 1700000001.0) };
    EXPECT_TRUE(write_wal_ipc(path, rows).ok());
    EXPECT_TRUE(fs::exists(path));

    auto back = wal_replay_file(path);
    EXPECT_EQ(back.size(), 1u);
    EXPECT_NEAR(back[0].floats.at("value"), 3.7,           1e-9);
    EXPECT_NEAR(back[0].floats.at("ts"),    1700000001.0,  1.0);
    EXPECT_EQ(back[0].strings.at("unit_id"),    std::string("u001"));
    EXPECT_EQ(back[0].strings.at("point_name"), std::string("voltage"));
    EXPECT_EQ(back[0].ints.at("project_id"),    0);
    fs::remove_all(dir);
}

TEST("wal: N rows round-trip — count preserved") {
    auto dir  = tmp_dir();
    auto path = dir + "/batch.wal.arrow";
    std::vector<Row> rows;
    for (int i = 0; i < 100; i++)
        rows.push_back(make_ems_row("u00" + std::to_string(i%4), "temp",
                                    20.0 + i, 1700000000.0 + i));
    EXPECT_TRUE(write_wal_ipc(path, rows).ok());
    auto back = wal_replay_file(path);
    EXPECT_EQ(back.size(), 100u);
    fs::remove_all(dir);
}

TEST("wal: replay nonexistent file returns empty") {
    auto rows = wal_replay_file("/tmp/no_such_rw_file_xyz.wal.arrow");
    EXPECT_TRUE(rows.empty());
}

TEST("wal: replay corrupt file returns empty") {
    auto dir  = tmp_dir();
    auto path = dir + "/corrupt.wal.arrow";
    std::ofstream f(path, std::ios::binary);
    f << "not arrow ipc\x00\x01garbage";
    f.close();
    auto rows = wal_replay_file(path);
    EXPECT_TRUE(rows.empty());
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Compactor
// ---------------------------------------------------------------------------

// Write N rows to path using flush_partition (simulates a writer flush file)
static void write_flush_file(const std::string& path, int n_rows) {
    std::vector<Row> rows;
    for (int i = 0; i < n_rows; i++)
        rows.push_back(make_ems_row("u00" + std::to_string(i % 4), "voltage",
                                    3.5 + i * 0.001, 1700000000.0 + i));
    auto st = flush_partition(path, rows, "snappy");
    (void)st;
}

TEST("compact_directory: merges N files into one, deletes sources") {
    auto dir = tmp_dir();
    Config cfg = make_ems_config();
    cfg.compression = "snappy";

    // Write 3 small flush files (20 rows each)
    std::vector<fs::path> files;
    for (int i = 0; i < 3; i++) {
        auto p = dir + "/2026030" + std::to_string(i) + "T120000Z.parquet";
        write_flush_file(p, 20);
        files.push_back(p);
    }

    int consumed = compact_directory(dir, files, cfg);

    EXPECT_EQ(consumed, 3);
    // Source files deleted
    for (const auto& f : files)
        EXPECT_FALSE(fs::exists(f));
    // Exactly one compact file produced
    int compact_count = 0;
    for (const auto& e : fs::directory_iterator(dir))
        if (e.path().filename().string().substr(0, 8) == "compact_") ++compact_count;
    EXPECT_EQ(compact_count, 1);

    fs::remove_all(dir);
}

TEST("compact_directory: compacted file has sum of source row counts") {
    auto dir = tmp_dir();
    Config cfg = make_ems_config();
    cfg.compression = "snappy";

    std::vector<fs::path> files;
    for (int i = 0; i < 4; i++) {
        auto p = dir + "/2026030" + std::to_string(i) + "T120000Z.parquet";
        write_flush_file(p, 50);
        files.push_back(p);
    }

    compact_directory(dir, files, cfg);

    // Find the compact file and verify row count via parquet magic check
    std::string compact_path;
    for (const auto& e : fs::directory_iterator(dir))
        if (e.path().filename().string().substr(0, 8) == "compact_")
            compact_path = e.path().string();

    EXPECT_FALSE(compact_path.empty());
    EXPECT_TRUE(is_valid_parquet(compact_path));

    // Verify via read-back
    auto r = read_parquet_table(compact_path);
    EXPECT_TRUE(r.ok());
    EXPECT_EQ((*r)->num_rows(), 200);  // 4 files × 50 rows

    fs::remove_all(dir);
}

TEST("compact_directory: empty files list returns 0") {
    auto dir = tmp_dir();
    Config cfg = make_ems_config();
    std::vector<fs::path> files;
    int consumed = compact_directory(dir, files, cfg);
    EXPECT_EQ(consumed, 0);
    fs::remove_all(dir);
}

TEST("compact_directory: bad source file — bails, no partial output, sources intact") {
    auto dir = tmp_dir();
    Config cfg = make_ems_config();
    cfg.compression = "snappy";

    // One valid file, one corrupt file
    auto good = dir + "/20260301T120000Z.parquet";
    auto bad  = dir + "/20260302T120000Z.parquet";
    write_flush_file(good, 10);
    { std::ofstream f(bad, std::ios::binary); f << "not parquet"; }

    std::vector<fs::path> files{good, bad};
    int consumed = compact_directory(dir, files, cfg);

    EXPECT_EQ(consumed, 0);
    EXPECT_TRUE(fs::exists(good));   // sources untouched on failure
    EXPECT_TRUE(fs::exists(bad));

    fs::remove_all(dir);
}

TEST("compact_directory: no .tmp file left on success") {
    auto dir = tmp_dir();
    Config cfg = make_ems_config();
    cfg.compression = "snappy";

    std::vector<fs::path> files;
    for (int i = 0; i < 3; i++) {
        auto p = dir + "/2026030" + std::to_string(i) + "T120000Z.parquet";
        write_flush_file(p, 5);
        files.push_back(p);
    }
    compact_directory(dir, files, cfg);

    for (const auto& e : fs::directory_iterator(dir)) {
        auto name = e.path().filename().string();
        EXPECT_TRUE(name.size() < 4 || name.substr(name.size() - 4) != ".tmp");
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    RUN_ALL_TESTS("real_writer unit tests");
}
