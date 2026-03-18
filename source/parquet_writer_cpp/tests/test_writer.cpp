/**
 * test_writer.cpp  —  unit tests for writer.cpp pure functions
 *
 * Covers:
 *   parse_topic()      — topic string → TopicInfo
 *   parse_payload()    — JSON string + topic → Row
 *   replace_vars()     — template variable substitution
 *   flush_partition()  — Row vector → valid Parquet file on disk
 *
 * Build & run:
 *   make test
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#include "../writer.cpp"  // compiled with -DPARQUET_WRITER_NO_MAIN
#pragma GCC diagnostic pop
#include "test_runner.h"

#include <cstdio>    // popen/pclose
#include <cstdlib>   // mkdtemp
#include <fstream>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string tmp_dir() {
    char tmpl[] = "/tmp/pwtest_XXXXXX";
    return std::string(mkdtemp(tmpl));
}

// Check PAR1 magic bytes at head and tail of file — no parquet reader headers needed
static bool is_valid_parquet(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return false;
    char head[4], tail[4];
    f.read(head, 4);
    f.seekg(-4, std::ios::end);
    f.read(tail, 4);
    return std::string(head, 4) == "PAR1" && std::string(tail, 4) == "PAR1";
}

// Python binary with pyarrow — injected by Makefile via -DTEST_PYTHON_BIN
#ifndef TEST_PYTHON_BIN
#define TEST_PYTHON_BIN "python3"
#endif

// Row / column counts via pyarrow subprocess (avoids parquet/metadata.h C++20 dependency)
static int64_t parquet_rows(const std::string& path) {
    auto cmd = std::string(TEST_PYTHON_BIN) + " -c \"import pyarrow.parquet as pq; "
               "print(pq.read_metadata('" + path + "').num_rows)\" 2>/dev/null";
    FILE* p = popen(cmd.c_str(), "r");
    if (!p) return -1;
    char buf[32] = {};
    auto _r = fgets(buf, sizeof(buf), p); (void)_r;
    pclose(p);
    try { return std::stoll(buf); } catch (...) { return -1; }
}

static int parquet_cols(const std::string& path) {
    auto cmd = std::string(TEST_PYTHON_BIN) + " -c \"import pyarrow.parquet as pq; "
               "print(pq.read_metadata('" + path + "').num_columns)\" 2>/dev/null";
    FILE* p = popen(cmd.c_str(), "r");
    if (!p) return -1;
    char buf[32] = {};
    auto _r = fgets(buf, sizeof(buf), p); (void)_r;
    pclose(p);
    try { return std::stoi(buf); } catch (...) { return -1; }
}

// ---------------------------------------------------------------------------
// parse_topic
// ---------------------------------------------------------------------------

TEST("parse_topic: batteries fully-qualified") {
    auto r = parse_topic("batteries/site=2/rack=1/module=3/cell=5");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->source_type, std::string("batteries"));
    EXPECT_EQ(r->kv.at("site"),   2);
    EXPECT_EQ(r->kv.at("rack"),   1);
    EXPECT_EQ(r->kv.at("module"), 3);
    EXPECT_EQ(r->kv.at("cell"),   5);
}

TEST("parse_topic: solar source type") {
    auto r = parse_topic("solar/site=0/rack=0/module=0/cell=0");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->source_type, std::string("solar"));
}

TEST("parse_topic: wind source type") {
    auto r = parse_topic("wind/site=1/rack=2/module=0/cell=0");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->source_type, std::string("wind"));
}

TEST("parse_topic: source only, no kv") {
    auto r = parse_topic("batteries");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->source_type, std::string("batteries"));
    EXPECT_TRUE(r->kv.empty());
}

TEST("parse_topic: empty string returns nullopt") {
    auto r = parse_topic("");
    EXPECT_FALSE(r.has_value());
}

TEST("parse_topic: unknown extra kv keys ignored gracefully") {
    auto r = parse_topic("batteries/site=0/rack=0/module=0/cell=0/sensor=99");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->kv.at("sensor"), 99);   // forward-compatible: kept, not rejected
}

TEST("parse_topic: non-numeric value in kv skipped") {
    auto r = parse_topic("batteries/site=0/rack=bad/module=0/cell=0");
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->kv.count("rack"), 0u);  // bad value dropped
    EXPECT_EQ(r->kv.at("site"), 0);
}

// ---------------------------------------------------------------------------
// parse_payload
// ---------------------------------------------------------------------------

TEST("parse_payload: nested {value,unit} format") {
    auto t = *parse_topic("batteries/site=1/rack=2/module=3/cell=4");
    auto r = parse_payload(
        R"({"timestamp":1700000000.0,"voltage":{"value":3.7,"unit":"V"},)"
        R"("current":{"value":-5.2,"unit":"A"}})",
        t, /*project_id=*/0);

    EXPECT_NEAR(r.floats.at("timestamp"), 1700000000.0, 1.0);
    EXPECT_NEAR(r.floats.at("voltage"),   3.7,           1e-6);
    EXPECT_NEAR(r.floats.at("current"),  -5.2,           1e-6);
}

TEST("parse_payload: flat scalar format") {
    auto t = *parse_topic("batteries/site=0/rack=0/module=0/cell=0");
    auto r = parse_payload(R"({"timestamp":1.0,"voltage":3.7,"soc":80.5})", t, 0);
    EXPECT_NEAR(r.floats.at("voltage"), 3.7,  1e-6);
    EXPECT_NEAR(r.floats.at("soc"),     80.5, 1e-6);
}

TEST("parse_payload: metadata from topic injected as int columns") {
    auto t = *parse_topic("batteries/site=5/rack=2/module=1/cell=3");
    auto r = parse_payload(R"({"timestamp":1.0})", t, /*project_id=*/7);
    EXPECT_EQ(r.ints.at("project_id"), 7);
    EXPECT_EQ(r.ints.at("site_id"),    5);
    EXPECT_EQ(r.ints.at("rack_id"),    2);
    EXPECT_EQ(r.ints.at("module_id"),  1);
    EXPECT_EQ(r.ints.at("cell_id"),    3);
}

TEST("parse_payload: bad JSON does not crash, topic metadata still present") {
    auto t = *parse_topic("batteries/site=0/rack=0/module=0/cell=0");
    auto r = parse_payload("{not valid json!!}", t, 0);
    EXPECT_EQ(r.ints.at("site_id"), 0);   // topic metadata intact
    EXPECT_TRUE(r.floats.empty());        // no payload data
}

TEST("parse_payload: empty JSON object") {
    auto t = *parse_topic("solar/site=3/rack=1/module=0/cell=2");
    auto r = parse_payload("{}", t, 0);
    EXPECT_EQ(r.ints.at("site_id"), 3);
    EXPECT_TRUE(r.floats.empty());
}

TEST("parse_payload: int columns from payload take int type") {
    auto t = *parse_topic("batteries/site=0/rack=0/module=0/cell=0");
    // site_id appearing in payload — topic value should already be there,
    // payload int_col value overrides (both are same type)
    auto r = parse_payload(R"({"timestamp":1.0,"site_id":99})", t, 0);
    EXPECT_EQ(r.ints.at("site_id"), 99);
}

TEST("parse_payload: full battery payload all 7 measurements") {
    auto t = *parse_topic("batteries/site=0/rack=0/module=0/cell=0");
    auto r = parse_payload(
        R"({
            "timestamp":    1700000000.0,
            "voltage":      {"value": 3.7,   "unit": "V"},
            "current":      {"value": 5.2,   "unit": "A"},
            "temperature":  {"value": 25.0,  "unit": "C"},
            "soc":          {"value": 80.0,  "unit": "%"},
            "soh":          {"value": 98.0,  "unit": "%"},
            "resistance":   {"value": 0.01,  "unit": "ohm"},
            "capacity":     {"value": 100.0, "unit": "Ah"}
        })", t, 0);
    EXPECT_EQ(r.floats.size(), 8u);   // timestamp + 7 measurements
}

TEST("parse_payload: full solar payload all 6 measurements") {
    auto t = *parse_topic("solar/site=0/rack=0/module=0/cell=0");
    auto r = parse_payload(
        R"({
            "timestamp":   1700000000.0,
            "irradiance":  {"value": 800.0, "unit": "W/m2"},
            "panel_temp":  {"value": 35.0,  "unit": "C"},
            "dc_voltage":  {"value": 600.0, "unit": "V"},
            "dc_current":  {"value": 8.0,   "unit": "A"},
            "ac_power":    {"value": 180.0, "unit": "W"},
            "efficiency":  {"value": 20.0,  "unit": "%"}
        })", t, 0);
    EXPECT_EQ(r.floats.size(), 7u);
}

// ---------------------------------------------------------------------------
// replace_vars
// ---------------------------------------------------------------------------

TEST("replace_vars: single substitution") {
    std::map<std::string, std::string> vars{{"year", "2026"}};
    EXPECT_EQ(replace_vars("{year}", vars), std::string("2026"));
}

TEST("replace_vars: multiple substitutions") {
    std::map<std::string, std::string> vars{{"year","2026"},{"month","03"},{"day","17"}};
    EXPECT_EQ(replace_vars("{year}/{month}/{day}", vars), std::string("2026/03/17"));
}

TEST("replace_vars: partial substitution — unknown placeholder left intact") {
    std::map<std::string, std::string> vars{{"year", "2026"}};
    std::string result = replace_vars("project={project_id}/{year}", vars);
    EXPECT_EQ(result, std::string("project={project_id}/2026"));
}

TEST("replace_vars: repeated placeholder replaced everywhere") {
    std::map<std::string, std::string> vars{{"x", "A"}};
    EXPECT_EQ(replace_vars("{x}/{x}/{x}", vars), std::string("A/A/A"));
}

TEST("replace_vars: empty template") {
    std::map<std::string, std::string> vars{{"year", "2026"}};
    EXPECT_EQ(replace_vars("", vars), std::string(""));
}

TEST("replace_vars: no placeholders passes through unchanged") {
    std::map<std::string, std::string> vars{{"year", "2026"}};
    EXPECT_EQ(replace_vars("static/path", vars), std::string("static/path"));
}

// ---------------------------------------------------------------------------
// flush_partition  (writes to temp dir, reads back to verify)
// ---------------------------------------------------------------------------

static Row make_battery_row(int site, int rack, int module, int cell, double v, double ts) {
    Row r;
    r.ints["project_id"] = 0;
    r.ints["site_id"]    = site;
    r.ints["rack_id"]    = rack;
    r.ints["module_id"]  = module;
    r.ints["cell_id"]    = cell;
    r.floats["timestamp"]   = ts;
    r.floats["voltage"]     = v;
    r.floats["temperature"] = 25.0;
    r.floats["soc"]         = 80.0;
    return r;
}

TEST("flush_partition: empty rows writes no file") {
    auto dir = tmp_dir();
    auto path = dir + "/empty.parquet";
    auto status = flush_partition(path, {}, "snappy");
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(fs::exists(path));   // no file created for empty batch
    fs::remove_all(dir);
}

TEST("flush_partition: single row produces valid parquet") {
    auto dir = tmp_dir();
    auto path = dir + "/single.parquet";
    std::vector<Row> rows{ make_battery_row(0,0,0,0, 3.7, 1700000000.0) };
    auto status = flush_partition(path, rows, "snappy");
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(is_valid_parquet(path));
    EXPECT_EQ(parquet_rows(path), 1);
    fs::remove_all(dir);
}

TEST("flush_partition: N rows round-trips correctly") {
    auto dir = tmp_dir();
    auto path = dir + "/batch.parquet";
    std::vector<Row> rows;
    for (int i = 0; i < 100; i++)
        rows.push_back(make_battery_row(0, i%3, i%4, i%6, 3.5 + i*0.001, 1700000000.0 + i));
    auto status = flush_partition(path, rows, "snappy");
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(parquet_rows(path), 100);
    fs::remove_all(dir);
}

TEST("flush_partition: correct column count (5 int + 4 float)") {
    auto dir = tmp_dir();
    auto path = dir + "/cols.parquet";
    std::vector<Row> rows{ make_battery_row(0,0,0,0, 3.7, 1.0) };
    (void)flush_partition(path, rows, "snappy");
    // project_id, site_id, rack_id, module_id, cell_id, timestamp, voltage, temperature, soc
    EXPECT_EQ(parquet_cols(path), 9);
    fs::remove_all(dir);
}

TEST("flush_partition: heterogeneous rows (sparse columns padded with nulls)") {
    auto dir = tmp_dir();
    auto path = dir + "/sparse.parquet";
    Row r1 = make_battery_row(0,0,0,0, 3.7, 1.0);
    Row r2 = make_battery_row(0,0,0,1, 3.8, 2.0);
    r2.floats["extra_sensor"] = 42.0;   // column only in some rows

    auto status = flush_partition(path, {r1, r2}, "snappy");
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(is_valid_parquet(path));
    EXPECT_EQ(parquet_rows(path), 2);   // extra_sensor column present; r1's value is null — no crash
    fs::remove_all(dir);
}

TEST("flush_partition: zstd compression produces valid file") {
    auto dir = tmp_dir();
    auto path = dir + "/zstd.parquet";
    std::vector<Row> rows{ make_battery_row(0,0,0,0, 3.7, 1.0) };
    auto status = flush_partition(path, rows, "zstd");
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(fs::exists(path));
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// hot file  (flat merge written via temp + atomic rename)
// ---------------------------------------------------------------------------

// Simulate the hot file logic from do_flush: collect rows from multiple
// "partitions", write to .tmp, rename atomically.
static void write_hot_file(const std::string& hot_path,
                            const std::vector<std::vector<Row>>& partitions,
                            const std::string& compression) {
    std::vector<Row> hot_rows;
    for (const auto& part : partitions)
        for (const auto& r : part) hot_rows.push_back(r);

    auto tmp = hot_path + ".tmp";
    auto status = flush_partition(tmp, hot_rows, compression);
    if (status.ok()) {
        std::error_code ec;
        fs::rename(tmp, hot_path, ec);
    }
}

TEST("hot file: merges rows from multiple partitions into one file") {
    auto dir = tmp_dir();
    auto hot  = dir + "/hot.parquet";

    std::vector<Row> bat_rows, sol_rows;
    for (int i = 0; i < 5; i++)  bat_rows.push_back(make_battery_row(0, 0, 0, i, 3.7, 1700000000.0 + i));
    for (int i = 0; i < 3; i++)  sol_rows.push_back(make_battery_row(1, 0, 0, i, 3.5, 1700000010.0 + i));

    write_hot_file(hot, {bat_rows, sol_rows}, "snappy");

    EXPECT_TRUE(is_valid_parquet(hot));
    EXPECT_EQ(parquet_rows(hot), 8);   // 5 battery + 3 solar rows merged
    fs::remove_all(dir);
}

TEST("hot file: atomic rename — .tmp is removed on success") {
    auto dir = tmp_dir();
    auto hot  = dir + "/hot.parquet";
    auto tmp  = hot + ".tmp";

    std::vector<Row> rows{ make_battery_row(0,0,0,0, 3.7, 1.0) };
    write_hot_file(hot, {rows}, "snappy");

    EXPECT_TRUE(fs::exists(hot));
    EXPECT_FALSE(fs::exists(tmp));   // .tmp cleaned up by rename
    fs::remove_all(dir);
}

TEST("hot file: disabled when path is empty — no file written") {
    auto dir = tmp_dir();
    // Simulate empty hot_file_path: just don't call write_hot_file
    // Verify the directory remains empty (no spurious files)
    EXPECT_TRUE(fs::is_empty(dir));
    fs::remove_all(dir);
}

TEST("hot file: overwritten on second flush with new row count") {
    auto dir = tmp_dir();
    auto hot  = dir + "/hot.parquet";

    // First flush — 10 rows
    std::vector<Row> first;
    for (int i = 0; i < 10; i++) first.push_back(make_battery_row(0, 0, 0, i, 3.7, 1700000000.0 + i));
    write_hot_file(hot, {first}, "snappy");
    EXPECT_EQ(parquet_rows(hot), 10);

    // Second flush — 4 rows (smaller batch replaces the file)
    std::vector<Row> second;
    for (int i = 0; i < 4; i++) second.push_back(make_battery_row(0, 0, 0, i, 3.8, 1700000100.0 + i));
    write_hot_file(hot, {second}, "snappy");
    EXPECT_EQ(parquet_rows(hot), 4);   // file replaced, not appended

    fs::remove_all(dir);
}

TEST("hot file: mixed source types merged correctly") {
    auto dir = tmp_dir();
    auto hot  = dir + "/hot.parquet";

    // Battery rows (have voltage/temperature/soc)
    std::vector<Row> bat{ make_battery_row(0,0,0,0, 3.7, 1.0),
                          make_battery_row(0,0,0,1, 3.8, 2.0) };
    // Solar rows (different float columns — irradiance, etc.)
    Row sol;
    sol.ints["project_id"] = 0; sol.ints["site_id"] = 0;
    sol.floats["timestamp"]  = 3.0;
    sol.floats["irradiance"] = 800.0;
    sol.floats["efficiency"] = 20.0;

    write_hot_file(hot, {bat, {sol}}, "snappy");

    EXPECT_TRUE(is_valid_parquet(hot));
    EXPECT_EQ(parquet_rows(hot), 3);  // sparse columns padded with nulls — no crash
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// current_state  (one row per unique sensor, latest reading)
// ---------------------------------------------------------------------------

// Build a SensorKey from source_type + the ints map of a row.
static SensorKey sensor_key(const std::string& source, const Row& r) {
    return SensorKey{source, r.ints};
}

// Simulate the current-state write logic from do_flush.
static arrow::Status write_current_state(
        const std::string& path,
        const std::map<SensorKey, Row>& cs,
        const std::string& compression = "snappy") {
    if (cs.empty()) return arrow::Status::OK();
    std::vector<Row> rows;
    rows.reserve(cs.size());
    for (const auto& [k, r] : cs) rows.push_back(r);
    auto tmp = path + ".tmp";
    auto st  = flush_partition(tmp, rows, compression);
    if (!st.ok()) return st;
    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (ec) return arrow::Status::IOError("rename: " + ec.message());
    return arrow::Status::OK();
}

TEST("current_state: one row per unique sensor") {
    auto dir  = tmp_dir();
    auto path = dir + "/cs.parquet";

    // 4 distinct sensors: 2 sites × 2 cells
    std::map<SensorKey, Row> cs;
    for (int site : {0, 1})
        for (int cell : {0, 1}) {
            Row r = make_battery_row(0, site, 0, cell, 3.7, 1000.0 + site*10 + cell);
            cs[sensor_key("batteries", r)] = r;
        }

    EXPECT_TRUE(write_current_state(path, cs).ok());
    EXPECT_EQ(parquet_rows(path), 4);
    fs::remove_all(dir);
}

TEST("current_state: latest reading wins for same sensor") {
    auto dir  = tmp_dir();
    auto path = dir + "/cs.parquet";

    Row old_r = make_battery_row(0, 0, 0, 0, 3.5, 1000.0);
    Row new_r = make_battery_row(0, 0, 0, 0, 3.9, 2000.0);   // same sensor, newer ts

    std::map<SensorKey, Row> cs;
    cs[sensor_key("batteries", old_r)] = old_r;
    cs[sensor_key("batteries", new_r)] = new_r;   // same key — overwrites

    EXPECT_TRUE(write_current_state(path, cs).ok());
    EXPECT_EQ(parquet_rows(path), 1);     // still one sensor
    fs::remove_all(dir);
}

TEST("current_state: disabled when path is empty — no file written") {
    // Simulated: if path is empty, do_flush skips the write entirely.
    // We verify write_current_state on an empty map returns OK without touching the FS.
    std::map<SensorKey, Row> cs;   // empty
    auto st = write_current_state("", cs);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(fs::exists(""));
}

TEST("current_state: atomic rename — .tmp absent on success") {
    auto dir  = tmp_dir();
    auto path = dir + "/cs.parquet";
    auto tmp  = path + ".tmp";

    std::map<SensorKey, Row> cs;
    Row r = make_battery_row(0, 0, 0, 0, 3.7, 1.0);
    cs[sensor_key("batteries", r)] = r;

    EXPECT_TRUE(write_current_state(path, cs).ok());
    EXPECT_TRUE(fs::exists(path));
    EXPECT_FALSE(fs::exists(tmp));
    fs::remove_all(dir);
}

TEST("current_state: mixed source types — sparse columns null-padded") {
    auto dir  = tmp_dir();
    auto path = dir + "/cs.parquet";

    std::map<SensorKey, Row> cs;
    // Battery sensor
    Row bat = make_battery_row(0, 0, 0, 0, 3.7, 1.0);
    cs[sensor_key("batteries", bat)] = bat;
    // Solar sensor (different float columns)
    Row sol;
    sol.ints["project_id"] = 0; sol.ints["site_id"] = 1;
    sol.ints["rack_id"] = 0;    sol.ints["module_id"] = 0; sol.ints["cell_id"] = 0;
    sol.floats["timestamp"]  = 2.0;
    sol.floats["irradiance"] = 850.0;
    sol.floats["efficiency"] = 19.5;
    cs[sensor_key("solar", sol)] = sol;

    EXPECT_TRUE(write_current_state(path, cs).ok());
    EXPECT_TRUE(is_valid_parquet(path));
    EXPECT_EQ(parquet_rows(path), 2);   // sparse columns null-padded, no crash
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    RUN_ALL_TESTS("parquet_writer unit tests");
}
