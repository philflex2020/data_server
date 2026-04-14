/**
 * ems_site_simulator.cpp — EMS/Longbow MQTT site simulator with physics + WebSocket control
 *
 * Each unit has a live state machine (STANDBY / CHARGING / DISCHARGING / OFFLINE).
 * SOC, voltage, current, and temperature are physically correlated.
 * Fault flags fire automatically on threshold crossings.
 * All state is controllable at runtime via WebSocket JSON commands.
 *
 * Topic format:  ems/site/{site_id}/unit/{unit_id}/{device}/{instance}/{point_name}/{dtype}
 * Payload:       {"ts":"2024-11-15T21:27:52.775Z","value":...}
 *
 * Build:
 *   make ems_site_simulator       (from source/stress_runner/)
 *   g++ -O2 -std=c++17 ems_site_simulator.cpp -o ems_site_simulator -lmosquitto -lsimdjson
 *
 * Options:
 *   --host     <host>     MQTT broker            (default: localhost)
 *   --port     <port>     MQTT port              (default: 1883)
 *   --template <path>     ems_topic_template.json (default: auto-detect)
 *   --units    <n>        synthetic unit count   (default: 4)
 *   --unit-id  <id>       explicit unit ID, repeatable
 *   --rate     <n>        target msg/sec, 0=unlimited (default: 0)
 *   --qos      <0|1>      MQTT QoS               (default: 0)
 *   --id       <str>      MQTT client base ID    (default: ems-stress)
 *   --ws-port  <port>     WebSocket control port (default: 8769)
 *   --soc      <pct>      initial SOC for all units (default: 50.0)
 *
 * WebSocket commands (ws://host:8769), JSON text frames:
 *
 *   {"type":"set_mode",      "units":["ALL"|"ID"...], "mode":"standby|charge|discharge|offline",
 *                             "current_a":200}
 *   {"type":"set_contactor", "units":["ALL"|"ID"...], "closed":true|false}
 *   {"type":"inject_fault",  "units":["ALL"|"ID"...],
 *                             "fault":"overtemp|low_soc|undervolt|overvolt|overcurrent|soc_imbalance",
 *                             "rack":0,         (0-based rack index, -1=all)
 *                             "value":55.0,     (temp °C for overtemp; SOC% for low_soc; spread% for imbalance)
 *                             "current_a":650}  (for overcurrent)
 *   {"type":"set_current",    "units":["ALL"|"ID"...], "current_a":200}
 *   {"type":"clear_faults",  "units":["ALL"|"ID"...]}
 *   {"type":"set_noise",     "units":["ALL"|"ID"...], "amplitude":2.5}
 *   {"type":"set_rate",      "rate":81420}
 *   {"type":"get_status"}
 *
 * Server broadcasts status JSON every 1 s to connected WebSocket clients.
 */

#include <simdjson.h>
#include <mosquitto.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// ============================================================================
// Compact SHA-1 (public domain) — used for WebSocket handshake only
// ============================================================================

static uint32_t sha1_rotl(uint32_t v, int n) { return (v << n) | (v >> (32 - n)); }

static void sha1_block(uint32_t h[5], const uint8_t b[64]) {
    uint32_t w[80];
    for (int i = 0; i < 16; ++i)
        w[i] = ((uint32_t)b[4*i]<<24)|((uint32_t)b[4*i+1]<<16)|
               ((uint32_t)b[4*i+2]<<8)|(uint32_t)b[4*i+3];
    for (int i = 16; i < 80; ++i)
        w[i] = sha1_rotl(w[i-3]^w[i-8]^w[i-14]^w[i-16], 1);
    uint32_t a=h[0],b_=h[1],c=h[2],d=h[3],e=h[4];
    for (int i = 0; i < 80; ++i) {
        uint32_t f, k;
        if      (i<20){f=(b_&c)|((~b_)&d);k=0x5A827999;}
        else if (i<40){f=b_^c^d;           k=0x6ED9EBA1;}
        else if (i<60){f=(b_&c)|(b_&d)|(c&d);k=0x8F1BBCDC;}
        else          {f=b_^c^d;           k=0xCA62C1D6;}
        uint32_t t=sha1_rotl(a,5)+f+e+k+w[i];
        e=d;d=c;c=sha1_rotl(b_,30);b_=a;a=t;
    }
    h[0]+=a;h[1]+=b_;h[2]+=c;h[3]+=d;h[4]+=e;
}

static void sha1_hash(const uint8_t* msg, size_t len, uint8_t out[20]) {
    uint32_t h[5]={0x67452301,0xEFCDAB89,0x98BADCFE,0x10325476,0xC3D2E1F0};
    uint8_t blk[64];
    size_t i = 0;
    for (; i+64 <= len; i+=64) sha1_block(h, msg+i);
    size_t r = len - i;
    memcpy(blk, msg+i, r);
    blk[r++] = 0x80;
    if (r > 56) { memset(blk+r,0,64-r); sha1_block(h,blk); r=0; }
    memset(blk+r, 0, 56-r);
    uint64_t bits = (uint64_t)len*8;
    for (int j=7;j>=0;--j){blk[56+j]=bits&0xFF;bits>>=8;}
    sha1_block(h, blk);
    for (int j=0;j<5;++j){
        out[4*j]=(h[j]>>24)&0xFF; out[4*j+1]=(h[j]>>16)&0xFF;
        out[4*j+2]=(h[j]>>8)&0xFF; out[4*j+3]=h[j]&0xFF;
    }
}

static std::string base64_enc(const uint8_t* d, size_t n) {
    static const char T[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string o; o.reserve(((n+2)/3)*4);
    for (size_t i=0;i<n;i+=3){
        uint32_t v=(uint32_t)d[i]<<16;
        if(i+1<n)v|=(uint32_t)d[i+1]<<8;
        if(i+2<n)v|=d[i+2];
        o+=T[(v>>18)&63]; o+=T[(v>>12)&63];
        o+=(i+1<n)?T[(v>>6)&63]:'=';
        o+=(i+2<n)?T[v&63]:'=';
    }
    return o;
}

// ============================================================================
// Signal classification — determined at template load time, O(1) at publish
// ============================================================================

enum class Sig {
    SYS_SOC, SYS_CURRENT, SYS_VOLTAGE, SYS_POWER,
    AVG_CELL_V, MAX_CELL_V, MIN_CELL_V,
    RACK_SOC, RACK_TEMP, RACK_VOLTAGE, RACK_CURRENT,
    FAULT_OVERTEMP, FAULT_OVERCURR, FAULT_UNDERVOLT, FAULT_OVERVOLT,
    FAULT_LOW_SOC, FAULT_OTHER,
    PCS_KW, PCS_HZ, PCS_PF, PCS_V, PCS_MODE,
    CHARGE_KWH, DISCHARGE_KWH,
    GENERIC_FLOAT, GENERIC_INT,
};

struct TopicEntry {
    std::string topic;
    bool        is_int;
    Sig         sig;
    int         unit_idx;   // index into g_units
    int         rack_idx;   // 0-based; -1 = system-level
    uint32_t    pool_idx;   // for GENERIC_FLOAT pool
    int         slow_interval_ms = 0;    // 0 = every sweep; >0 = min ms between publishes
    mutable int64_t last_pub_ms  = 0;   // wall-clock ms of last publish
};

// ============================================================================
// Unit state
// ============================================================================

enum class Mode { STANDBY, CHARGING, DISCHARGING, OFFLINE };

struct RackState {
    float soc  = 50.0f;  // %
    float temp = 25.0f;  // °C
    bool  inject_overvolt = false;
    float drift = 0.0f;  // accumulated SOC divergence from system mean (%)
};

struct UnitState {
    std::string id;
    Mode   mode      = Mode::STANDBY;
    bool   contactor = true;    // true = closed (normal)
    float  soc       = 50.0f;  // % system
    float  current   = 0.0f;   // A (positive = charge or discharge, sign in mode)
    float  temp_avg  = 25.0f;  // °C
    float  capacity  = 300.0f; // Ah
    float  charge_kwh    = 2112.0f;
    float  discharge_kwh = 677.0f;
    bool   inject_overcurr = false;
    float  noise_pct = 0.0f;   // ±% of value added as uniform noise (0 = off)
    RackState racks[5];

    void init_racks() {
        for (auto& r : racks) { r.soc = soc; r.temp = temp_avg; }
    }
};

// ============================================================================
// Physics helpers
// ============================================================================

// Piecewise OCV — calibrated to Evelyn BESS site (0226571E) real measurements.
// evelyn_data_analysis.html §5: residual V−(1145+0.5×SOC) mean=−47 V → base≈1098+0.5×SOC.
// At SOC=94%: real V=1137 V → soft top-end compression modelled above 85%.
// Range: 1070 V (SOC=0%) → ~1139 V (SOC=100%).
static float soc_to_voltage(float soc) {
    soc = std::max(0.0f, std::min(100.0f, soc));
    if (soc < 15.0f) return 1070.0f + 1.20f * soc;            // 1070..1088
    if (soc < 85.0f) return 1088.0f + 0.65f * (soc - 15.0f); // 1088..1133.5
    if (soc < 95.0f) return 1133.5f + 0.40f * (soc - 85.0f); // 1133.5..1137.5 (soft plateau)
    return             1137.5f + 0.25f * (soc - 95.0f);       // 1137.5..1138.75 (saturation)
}
// Terminal voltage: OCV ± I × R_int; R_int rises at SOC extremes (SOC-dependent polarisation).
static float soc_to_voltage_t(float soc, float current_a, bool charging) {
    float r_factor = 1.0f + 1.5f * std::exp(-0.12f * std::min(soc, 100.0f - soc));
    return soc_to_voltage(soc) + (charging ? 1.0f : -1.0f) * current_a * 0.008f * r_factor;
}
// ~325 cells: evelyn pack at 94% SOC → 1137 V / 3.50 V/cell ≈ 325 cells
static float soc_to_cell_v(float soc) { return soc_to_voltage(soc) / 325.0f; }

// ============================================================================
// Global state
// ============================================================================

static std::vector<UnitState>   g_units;
static std::vector<TopicEntry>  g_topics;
static std::mutex               g_state_mtx;
static std::atomic<bool>        g_stop{false};
static std::atomic<uint64_t>    g_published{0};
static std::atomic<int>         g_rate{0};
static std::atomic<int>         g_slow_interval_ms{1000}; // 1000=smooth (default), 20000=burst
static std::atomic<uint64_t>    g_last_mps{0};
static std::string              g_site_id;         // first site (backward compat)
static std::vector<std::string> g_site_ids;        // all sites (parsed from --site-id a,b,c)
static std::string              g_topic_prefix;    // optional prefix; if set, topics are {prefix}/unit/... else ems/site/{site}/unit/...

// WebSocket broadcast — list of connected client fds
static std::mutex               g_ws_mtx;
static std::vector<int>         g_ws_fds;

static void ws_broadcast(const std::string& msg);  // forward declaration
static void on_connect(struct mosquitto*, void*, int);   // forward declaration

// ============================================================================
// Signal classification
// ============================================================================

static Sig classify(const std::string& device, const std::string& instance,
                    const std::string& point, bool is_int, int& rack_idx) {
    rack_idx = -1;

    auto has = [&](const char* s){ return point.find(s) != std::string::npos; };

    if (device == "pcs") {
        if (point == "kW")  return Sig::PCS_KW;
        if (point == "Hz")  return Sig::PCS_HZ;
        if (point == "PF")  return Sig::PCS_PF;
        if (has("PhVph"))   return Sig::PCS_V;
        if (has("Operation_mode") || has("Input_75_")) return Sig::PCS_MODE;
        return Sig::GENERIC_FLOAT;
    }

    if (device == "rack") {
        auto p = instance.rfind('_');
        if (p != std::string::npos) rack_idx = std::stoi(instance.substr(p+1)) - 1;
        if (point == "RackSOC")                   return Sig::RACK_SOC;
        if (has("Temp") || has("temp"))           return Sig::RACK_TEMP;
        if (has("Voltage") || has("voltage"))     return Sig::RACK_VOLTAGE;
        if (has("Current") || has("current"))     return Sig::RACK_CURRENT;
        return Sig::GENERIC_FLOAT;
    }

    // device == "bms"

    // Extract SBMU rack index from point name (SBMU1→0, SBMU2→1 ...)
    auto sbmu_rack = [&]() -> int {
        auto pos = point.find("SBMU");
        if (pos == std::string::npos) return -1;
        size_t n = pos + 4;
        if (n < point.size() && isdigit(point[n]))
            return point[n] - '1';
        return -1;
    };

    if (has("_SOC") || has("SysSOC")) {
        int sr = sbmu_rack();
        if (sr >= 0 && !has("MBMS")) { rack_idx = sr; return Sig::RACK_SOC; }
        return Sig::SYS_SOC;
    }
    if (has("System_Current"))            return Sig::SYS_CURRENT;
    if (has("System_voltage") || has("System_Voltage")) return Sig::SYS_VOLTAGE;
    if (has("System_power"))              return Sig::SYS_POWER;
    if (has("Avg._cell_voltage"))         return Sig::AVG_CELL_V;
    if (has("Max_cell_voltage"))          return Sig::MAX_CELL_V;
    if (has("Min_cell_voltage"))          return Sig::MIN_CELL_V;

    if (has("Subsystem_Voltage")) {
        rack_idx = sbmu_rack();  return Sig::RACK_VOLTAGE;
    }
    if (has("Subsystem_Current")) {
        rack_idx = sbmu_rack();  return Sig::RACK_CURRENT;
    }

    // Charge/discharge energy counters
    if (has("Charge_kWh") && !has("Discharge")) return Sig::CHARGE_KWH;
    if (has("Discharge_kWh"))                   return Sig::DISCHARGE_KWH;

    // Fault flags
    if (has("Big_temperature_difference"))       return Sig::FAULT_OVERTEMP;
    if (has("over-current") || has("Charge_over-current") ||
        has("Discharge_over-curr"))              return Sig::FAULT_OVERCURR;
    if (has("under-voltage"))                    return Sig::FAULT_UNDERVOLT;
    if (has("over-voltage"))                     return Sig::FAULT_OVERVOLT;
    if (has("Low_SOC") || has("SOC_low") ||
        has("low_SOC"))                          return Sig::FAULT_LOW_SOC;

    return is_int ? Sig::FAULT_OTHER : Sig::GENERIC_FLOAT;
}

// ============================================================================
// Value generator — reads from UnitState, no string ops at publish time
// ============================================================================

static float generic_pool[65536];
static float noise_pool[65536];      // uniform samples in [-0.5, +0.5]
static std::atomic<uint32_t> noise_idx{0};
static bool  pool_init = false;

// Return a small noise offset: val * noise_pct/100 * sample in [-0.5,+0.5]
static inline double apply_noise(double val, float noise_pct) {
    if (noise_pct <= 0.0f) return val;
    uint32_t idx = noise_idx.fetch_add(1, std::memory_order_relaxed) & 0xFFFF;
    return val + val * (noise_pct / 100.0) * noise_pool[idx];
}

static double gen_value(const TopicEntry& e) {
    std::lock_guard<std::mutex> lk(g_state_mtx);
    const UnitState& u = g_units[e.unit_idx];

    bool offline = (!u.contactor || u.mode == Mode::OFFLINE);
    int ri = (e.rack_idx >= 0 && e.rack_idx < 5) ? e.rack_idx : 0;
    const RackState& rack = u.racks[ri];

    const float np = u.noise_pct;
    // When offline: voltage/current/power = 0; SOC preserved (BMS still reports)
    switch (e.sig) {
        case Sig::SYS_SOC:      return apply_noise(u.soc, np);
        case Sig::SYS_CURRENT:  return offline ? 0.0 : apply_noise(u.current, np);
        case Sig::SYS_VOLTAGE:  return offline ? 0.0 :
                                    apply_noise(soc_to_voltage_t(u.soc, u.current,
                                                u.mode==Mode::CHARGING), np);
        case Sig::SYS_POWER:    return offline ? 0.0 :
                                    apply_noise(soc_to_voltage_t(u.soc, u.current,
                                                u.mode==Mode::CHARGING) * u.current / 1000.0f, np);
        case Sig::AVG_CELL_V:   return apply_noise(soc_to_cell_v(u.soc), np);
        case Sig::MAX_CELL_V:   return apply_noise(soc_to_cell_v(u.soc) + 0.003, np);
        case Sig::MIN_CELL_V:   return apply_noise(soc_to_cell_v(u.soc) - 0.003, np);

        case Sig::RACK_SOC:     return apply_noise(rack.soc, np);
        case Sig::RACK_TEMP:    return apply_noise(rack.temp, np);
        case Sig::RACK_VOLTAGE: return offline ? 0.0 :
                                    apply_noise(soc_to_voltage_t(rack.soc, u.current,
                                                u.mode==Mode::CHARGING) / 5.0f, np);
        case Sig::RACK_CURRENT: return offline ? 0.0 : apply_noise(u.current, np);

        // Fault flags: no noise — discrete 0/1
        case Sig::FAULT_OVERTEMP:  return (rack.temp > 45.0f) ? 1.0 : 0.0;
        case Sig::FAULT_OVERCURR:  return (u.inject_overcurr || std::abs(u.current)>600.0f) ? 1.0 : 0.0;
        case Sig::FAULT_UNDERVOLT: return (soc_to_cell_v(rack.soc) < 2.8f) ? 1.0 : 0.0;
        case Sig::FAULT_OVERVOLT:  return (rack.inject_overvolt || soc_to_cell_v(rack.soc)>3.65f) ? 1.0 : 0.0;
        case Sig::FAULT_LOW_SOC:   return (u.soc < 10.0f) ? 1.0 : 0.0;
        case Sig::FAULT_OTHER:     return 0.0;

        case Sig::PCS_KW:   return 0.0;
        case Sig::PCS_HZ:   return apply_noise(60.01, np);
        case Sig::PCS_PF:   return apply_noise(0.999, np);
        case Sig::PCS_V:    return apply_noise(373.0 + 0.1 * (e.pool_idx & 15), np);
        case Sig::PCS_MODE: return (u.mode == Mode::STANDBY || u.mode == Mode::OFFLINE) ? 6.0 : 1.0;

        case Sig::CHARGE_KWH:    return u.charge_kwh;
        case Sig::DISCHARGE_KWH: return u.discharge_kwh;

        case Sig::GENERIC_FLOAT: return apply_noise(generic_pool[e.pool_idx & 0xFFFF], np);
        case Sig::GENERIC_INT:   return 0.0;
    }
    return 0.0;
}

// ============================================================================
// Template loader
// ============================================================================

// Read unit_ids array from the template JSON (used when --unit-id is not specified).
static std::vector<std::string> load_unit_ids_from_template(const std::string& path) {
    simdjson::ondemand::parser parser;
    auto json = simdjson::padded_string::load(path);
    auto doc  = parser.iterate(json);
    std::vector<std::string> ids;
    simdjson::ondemand::array arr;
    if (doc["unit_ids"].get(arr) != simdjson::SUCCESS) return ids;
    for (auto id : arr) {
        std::string_view sv;
        if (id.get_string().get(sv) == simdjson::SUCCESS)
            ids.emplace_back(sv);
    }
    return ids;
}

static std::vector<TopicEntry> build_topics(const std::string& path,
                                             const std::vector<std::string>& unit_ids) {
    simdjson::ondemand::parser parser;
    auto json = simdjson::padded_string::load(path);
    auto doc  = parser.iterate(json);

    struct Entry { std::string device, instance, point, dtype; };
    std::vector<Entry> entries;
    for (auto item : doc["template"]) {
        auto arr = item.get_array();
        std::string_view dv, iv, pv, tv;
        auto it = arr.begin();
        dv = (*it).get_string(); ++it;
        iv = (*it).get_string(); ++it;
        pv = (*it).get_string(); ++it;
        tv = (*it).get_string();
        entries.push_back({std::string(dv),std::string(iv),std::string(pv),std::string(tv)});
    }

    std::vector<TopicEntry> result;
    result.reserve(unit_ids.size() * entries.size());
    uint32_t pool = 0;

    for (size_t ui = 0; ui < unit_ids.size(); ++ui) {
        // Assign unit to site round-robin
        const std::string& site = g_site_ids.empty() ? g_site_id :
                                  g_site_ids[ui % g_site_ids.size()];
        const std::string site_pfx = g_topic_prefix.empty()
            ? "ems/site/" + site + "/unit/"
            : g_topic_prefix + "/unit/";
        for (const auto& e : entries) {
            bool is_int = (e.dtype == "integer" || e.dtype == "boolean_integer");
            int rack_idx = -1;
            Sig sig = classify(e.device, e.instance, e.point, is_int, rack_idx);
            std::string topic = site_pfx + unit_ids[ui] + "/" + e.device + "/" +
                                e.instance + "/" + e.point + "/" + e.dtype;
            TopicEntry te{ std::move(topic), is_int, sig,
                           (int)ui, rack_idx, pool++ };
            // Slow-changing signals: publish at most every 20 s
            switch (sig) {
                case Sig::MAX_CELL_V:
                case Sig::MIN_CELL_V:
                case Sig::RACK_TEMP:
                case Sig::CHARGE_KWH:
                case Sig::DISCHARGE_KWH:
                case Sig::PCS_HZ:
                case Sig::PCS_PF:
                case Sig::PCS_V:
                    te.slow_interval_ms = 20000; break;
                default: break;
            }
            result.push_back(std::move(te));
        }
    }
    return result;
}

// ============================================================================
// Peripheral simulation globals
// ============================================================================

static std::string g_mqtt_host_g;
static int         g_mqtt_port_g = 1883;

// Slow-varying peripheral state — written by physics_thread_fn (under g_state_mtx),
// read by peripheral_thread_fn (captured under same lock during snapshot).
static float g_agc_setpoint  = -10000.0f;  // kW  HS-CONTROL-AGC; slow random walk
static float g_freq_response =     0.0f;   // kW  PCS_P--Frequency Response; Hz-droop
static float g_p_correction  =     0.0f;   // kW  PCS_P--P Correction; mean-reverting

static void pub_f(struct mosquitto* m, int qos,
                  const char* topic, double v, const char* ts) {
    char buf[192];
    int n = snprintf(buf, sizeof(buf), "{\"ts\":\"%s\",\"value\":%.4f}", ts, v);
    mosquitto_publish(m, nullptr, topic, n, buf, qos, false);
}
static void pub_i(struct mosquitto* m, int qos,
                  const char* topic, int v, const char* ts) {
    char buf[128];
    int n = snprintf(buf, sizeof(buf), "{\"ts\":\"%s\",\"value\":%d}", ts, v);
    mosquitto_publish(m, nullptr, topic, n, buf, qos, false);
}

// Publish site / meter / PCS peripheral topics at 1 Hz
static void peripheral_thread_fn(const std::string& client_id, int qos) {
    struct mosquitto* mosq = mosquitto_new(client_id.c_str(), true, nullptr);
    if (!mosq) return;
    mosquitto_connect_callback_set(mosq, on_connect);

    while (!g_stop.load()) {
        if (mosquitto_connect(mosq, g_mqtt_host_g.c_str(), g_mqtt_port_g, 60) == MOSQ_ERR_SUCCESS)
            break;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (g_stop.load()) { mosquitto_destroy(mosq); return; }
    mosquitto_loop_start(mosq);

    char ts[64], topic[256];

    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Timestamp
        {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch()).count();
            time_t sec = ms/1000; int msec = ms%1000;
            struct tm tm_val; gmtime_r(&sec, &tm_val);
            snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                     tm_val.tm_year+1900, tm_val.tm_mon+1, tm_val.tm_mday,
                     tm_val.tm_hour, tm_val.tm_min, tm_val.tm_sec, msec);
        }

        // Snapshot unit states + peripheral slow state
        struct PCSSnap { std::string uid; float p_kw, p_kvar, cmd_p; bool acbrk, ena; };
        std::vector<PCSSnap> snaps;
        float total_kw = 0, total_kvar = 0, mean_soc = 0;
        float agc_kw = 0, freq_resp_kw = 0, p_corr_kw = 0;
        {
            std::lock_guard<std::mutex> lk(g_state_mtx);
            if (g_units.empty()) continue;
            for (const auto& u : g_units) {
                float sign = (u.mode == Mode::DISCHARGING) ?  1.0f :
                             (u.mode == Mode::CHARGING)    ? -1.0f : 0.0f;
                float v    = soc_to_voltage_t(u.soc, u.current, u.mode==Mode::CHARGING);
                float p_kw   = (!u.contactor||u.mode==Mode::OFFLINE) ? 0.0f :
                               sign * u.current * v / 1000.0f;
                float p_kvar = p_kw * 0.12f;
                total_kw   += p_kw;
                total_kvar += p_kvar;
                mean_soc   += u.soc;
                bool acbrk = u.contactor && u.mode != Mode::OFFLINE;
                bool ena   = u.mode != Mode::OFFLINE;
                snaps.push_back({u.id, p_kw, p_kvar, p_kw, acbrk, ena});
            }
            mean_soc    /= (float)g_units.size();
            agc_kw       = g_agc_setpoint;
            freq_resp_kw = g_freq_response;
            p_corr_kw    = g_p_correction;
        }

        // Derived meter values
        float meter_kw   = -total_kw  * 1.02f;
        float meter_kvar = -total_kvar;
        float meter_hz   = (float)apply_noise(60.0, 0.05);
        float meter_v    = (float)apply_noise(131.6, 0.3);
        float s_va       = std::sqrt(total_kw*total_kw + total_kvar*total_kvar);
        float meter_i    = meter_v > 0 ? std::abs(total_kw*1000.0f)/(meter_v*1.732f) : 0.0f;
        float meter_pf   = s_va > 0.1f ? total_kw/s_va : 0.0f;

        const std::vector<std::string>& all_sites =
            g_site_ids.empty() ? std::vector<std::string>{g_site_id} : g_site_ids;
        const size_t n_sites = all_sites.size();

#define PT(fmt, ...) snprintf(topic, sizeof(topic), fmt, __VA_ARGS__)

        float bess_load_kw = -total_kw;
        static const float feeder_share[5] = {0.47f, 0.18f, 0.21f, 0.07f, 0.07f};

        for (size_t si = 0; si < n_sites; ++si) {
            const char* sid = all_sites[si].c_str();

            // ── Site ──────────────────────────────────────────────────────
            PT("ems/site/%s/root/SOC/float",                    sid);  pub_f(mosq,qos,topic,mean_soc,ts);
            PT("ems/site/%s/root/BESSkW/float",                 sid);  pub_f(mosq,qos,topic,total_kw,ts);
            PT("ems/site/%s/root/Ptot/float",                   sid);  pub_f(mosq,qos,topic,total_kw,ts);
            PT("ems/site/%s/root/Qtot/float",                   sid);  pub_f(mosq,qos,topic,total_kvar,ts);
            PT("ems/site/%s/root/HS-CONTROL-f/float",           sid);  pub_f(mosq,qos,topic,meter_hz,ts);
            PT("ems/site/%s/root/HS-CONTROL-P/float",           sid);  pub_f(mosq,qos,topic,total_kw,ts);
            PT("ems/site/%s/root/HS-CONTROL-Q/float",           sid);  pub_f(mosq,qos,topic,total_kvar,ts);
            PT("ems/site/%s/root/HS-CONTROL-P-Measured/float",  sid);  pub_f(mosq,qos,topic,total_kw*0.995f,ts);
            PT("ems/site/%s/root/HS-CONTROL-AGC/float",         sid);  pub_f(mosq,qos,topic,agc_kw,ts);
            PT("ems/site/%s/root/Group1_P/float",                sid);  pub_f(mosq,qos,topic,total_kw,ts);
            PT("ems/site/%s/root/Group2_Q/float",                sid);  pub_f(mosq,qos,topic,0.0,ts);
            PT("ems/site/%s/root/RTAC_P/float",                  sid);  pub_f(mosq,qos,topic,total_kw,ts);
            PT("ems/site/%s/root/PCS_P/float",                   sid);  pub_f(mosq,qos,topic,total_kw,ts);
            PT("ems/site/%s/root/PCS_P--P Correction/float",     sid);  pub_f(mosq,qos,topic,p_corr_kw,ts);
            PT("ems/site/%s/root/PCS_P--Frequency Response/float",sid); pub_f(mosq,qos,topic,freq_resp_kw,ts);

            // ── Meter ─────────────────────────────────────────────────────
            PT("ems/site/%s/meter/meter_1/kW/float",             sid);  pub_f(mosq,qos,topic,meter_kw,ts);
            PT("ems/site/%s/meter/meter_1/kVAR/float",           sid);  pub_f(mosq,qos,topic,meter_kvar,ts);
            PT("ems/site/%s/meter/meter_1/Hz/float",             sid);  pub_f(mosq,qos,topic,meter_hz,ts);
            PT("ems/site/%s/meter/meter_1/Voltage/float",        sid);  pub_f(mosq,qos,topic,meter_v,ts);
            PT("ems/site/%s/meter/meter_1/Current/float",        sid);  pub_f(mosq,qos,topic,meter_i,ts);
            PT("ems/site/%s/meter/meter_1/PF/float",             sid);  pub_f(mosq,qos,topic,meter_pf,ts);
            PT("ems/site/%s/meter/meter_1/CLR_NET_LOAD_MW/float",sid);  pub_f(mosq,qos,topic,-total_kw,ts);
            PT("ems/site/%s/meter/meter_1/CLR_NET_MW/float",     sid);  pub_f(mosq,qos,topic,-total_kw,ts);

            // ── PCS per unit (only units assigned to this site) ───────────
            for (size_t ui = 0; ui < snaps.size(); ++ui) {
                if (ui % n_sites != si) continue;
                const auto& s = snaps[ui];
                const char* uid = s.uid.c_str();
                PT("ems/site/%s/unit/%s/pcs/pcs_1/PCS_P/float",           sid,uid);  pub_f(mosq,qos,topic,s.p_kw,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/PCS_Q/float",           sid,uid);  pub_f(mosq,qos,topic,s.p_kvar,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/CMD_P/float",           sid,uid);  pub_f(mosq,qos,topic,s.cmd_p,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/kW/float",              sid,uid);  pub_f(mosq,qos,topic,s.p_kw*0.97f,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/DCkW/float",            sid,uid);  pub_f(mosq,qos,topic,s.p_kw,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/Hz/float",              sid,uid);  pub_f(mosq,qos,topic,meter_hz,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/StatusSymmCurrentComp_CurrentRealPS/float",sid,uid);
                pub_f(mosq,qos,topic,s.p_kw*1.92f,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/ACBreaker/boolean_integer",sid,uid);    pub_i(mosq,qos,topic,s.acbrk?1:0,ts);
                PT("ems/site/%s/unit/%s/pcs/pcs_1/EnabledStatus/boolean_integer",sid,uid);pub_i(mosq,qos,topic,s.ena?1:0,ts);
            }

            // ── RTAC ──────────────────────────────────────────────────────
            for (int f = 1; f <= 5; ++f) {
                float f_p  = (float)apply_noise(total_kw * feeder_share[f-1], 20.0);
                float f_pf = (float)apply_noise(0.09, 30.0);
                PT("ems/site/%s/rtac/rtac_1/MET_F%d_P_WATTS/float", sid, f);  pub_f(mosq,qos,topic,f_p,ts);
                PT("ems/site/%s/rtac/rtac_1/MET_F%d_PF/float",      sid, f);  pub_f(mosq,qos,topic,f_pf,ts);
            }
            PT("ems/site/%s/rtac/rtac_1/QSE_Line_flows_MW/float",                  sid);  pub_f(mosq,qos,topic,total_kw/1000.0f,ts);
            PT("ems/site/%s/rtac/rtac_1/QSE_Transformer_flows_MW/float",           sid);  pub_f(mosq,qos,topic,total_kw/1000.0f,ts);
            PT("ems/site/%s/rtac/rtac_1/QSE_BESS_LOAD_MW/float",                   sid);  pub_f(mosq,qos,topic,bess_load_kw,ts);
            PT("ems/site/%s/rtac/rtac_1/QSE_CLR_Scheduled_Power_Consumption/float",sid);  pub_f(mosq,qos,topic,bess_load_kw,ts);
            PT("ems/site/%s/rtac/rtac_1/QSE_CLR_Net_Load_MW/float",                sid);  pub_f(mosq,qos,topic,bess_load_kw,ts);
        }
#undef PT
    }

    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
}

// ============================================================================
// Physics update — runs every 100 ms
// ============================================================================

static void physics_thread_fn() {
    const float DT = 0.1f;   // seconds per step
    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::lock_guard<std::mutex> lk(g_state_mtx);
        for (auto& u : g_units) {
            if (u.mode == Mode::OFFLINE || !u.contactor) {
                u.current = 0.0f;
                // Temp decays toward ambient
                for (auto& r : u.racks)
                    r.temp += (25.0f - r.temp) * 0.001f * DT;
                continue;
            }

            float I = 0.0f;
            if      (u.mode == Mode::CHARGING)    I = u.current;
            else if (u.mode == Mode::DISCHARGING)  I = u.current;
            // STANDBY: I stays 0

            // SOC drift with coulombic efficiency: 96% charge, ~102% discharge (1/0.98)
            if (u.mode != Mode::STANDBY) {
                float eta  = (u.mode == Mode::CHARGING) ? 0.96f : (1.0f / 0.98f);
                float dsoc = (I / u.capacity) * (100.0f / 3600.0f) * DT * eta;
                if (u.mode == Mode::CHARGING)    u.soc += dsoc;
                else                             u.soc -= dsoc;
                u.soc = std::max(0.0f, std::min(100.0f, u.soc));

                // Rack SOC: drift diverges slowly each cycle (max ±3% spread at full divergence)
                for (int ri = 0; ri < 5; ++ri) {
                    u.racks[ri].drift += (float)(ri - 2) * 2e-4f * std::abs(dsoc);
                    u.racks[ri].drift  = std::max(-3.0f, std::min(3.0f, u.racks[ri].drift));
                    u.racks[ri].soc    = std::max(0.0f, std::min(100.0f, u.soc + u.racks[ri].drift));
                }

                // Update energy counters
                float dkwh = soc_to_voltage(u.soc) * I * DT / (1000.0f * 3600.0f);
                if (u.mode == Mode::CHARGING)    u.charge_kwh    += dkwh;
                else                             u.discharge_kwh += dkwh;
            }

            // Temperature: rises from current, decays toward ambient
            float heat_rate = (std::abs(I) / 600.0f) * 25.0f;  // max +25°C at 600A
            float dtemp = (heat_rate - (u.temp_avg - 25.0f)) * DT / 300.0f;
            u.temp_avg += dtemp;
            for (auto& r : u.racks) {
                float rdtemp = (heat_rate - (r.temp - 25.0f)) * DT / 300.0f;
                r.temp += rdtemp;
            }

            // Auto-trip: protect battery
            if (u.soc >= 99.5f && u.mode == Mode::CHARGING) {
                u.mode = Mode::STANDBY; u.current = 0.0f;
                fprintf(stderr, "[physics] %s: fully charged — STANDBY\n", u.id.c_str());
            }
            if (u.soc <= 5.0f && u.mode == Mode::DISCHARGING) {
                u.mode = Mode::STANDBY; u.current = 0.0f;
                fprintf(stderr, "[physics] %s: SOC critical — STANDBY\n", u.id.c_str());
            }
        }

        // Update slow-varying peripheral state (inside lock, after per-unit loop)
        // AGC: slow random walk, mean-biased toward −10 MW, clamped [−40, +5] MW
        g_agc_setpoint += ((float)rand() / RAND_MAX - 0.52f) * 200.0f * DT;
        g_agc_setpoint  = std::max(-40000.0f, std::min(5000.0f, g_agc_setpoint));
        // Freq response: proportional to simulated ±20 mHz Hz deviation, 20 MW/Hz droop
        float hz_dev    = ((float)rand() / RAND_MAX - 0.5f) * 0.04f;
        g_freq_response = hz_dev * -200000.0f;
        // P correction: mean-reverting random (time constant ~20 s)
        g_p_correction += ((float)rand() / RAND_MAX - 0.5f) * 500.0f * DT;
        g_p_correction *= (1.0f - 0.05f * DT);
    }
}

// ============================================================================
// MQTT publish worker
// ============================================================================

static void on_connect(struct mosquitto*, void*, int rc) {
    if (rc) fprintf(stderr, "[mqtt] connect failed rc=%d\n", rc);
}
static void on_disconnect(struct mosquitto*, void*, int rc) {
    if (rc && !g_stop.load()) fprintf(stderr, "[mqtt] disconnected rc=%d\n", rc);
}
static std::atomic<bool> g_stop_flag{false};
static void sig_handler(int) { g_stop.store(true); g_stop_flag.store(true); }

static void publish_thread_fn(const std::string& host, int port,
                               const std::string& client_id, int qos) {
    struct mosquitto* mosq = mosquitto_new(client_id.c_str(), true, nullptr);
    if (!mosq) { fprintf(stderr, "mosquitto_new failed\n"); return; }
    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_reconnect_delay_set(mosq, 1, 5, false);

    while (!g_stop.load()) {
        if (mosquitto_connect(mosq, host.c_str(), port, 60) == MOSQ_ERR_SUCCESS) break;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (g_stop.load()) { mosquitto_destroy(mosq); return; }
    mosquitto_loop_start(mosq);

    char ts_buf[64], payload[256];
    uint64_t loop = 0;

    while (!g_stop.load()) {
        auto sweep_start = std::chrono::steady_clock::now();

        // Build timestamp once per sweep
        {
            auto now = std::chrono::system_clock::now();
            auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now.time_since_epoch()).count();
            time_t sec = ms / 1000; int msec = ms % 1000;
            struct tm tm_val; gmtime_r(&sec, &tm_val);
            snprintf(ts_buf, sizeof(ts_buf),
                     "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                     tm_val.tm_year+1900, tm_val.tm_mon+1, tm_val.tm_mday,
                     tm_val.tm_hour, tm_val.tm_min, tm_val.tm_sec, msec);
        }

        for (const auto& e : g_topics) {
            if (g_stop.load()) break;
            // Skip slow signals until their interval has elapsed
            if (e.slow_interval_ms > 0) {
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                int interval = g_slow_interval_ms.load();
                if (now_ms - e.last_pub_ms < interval) continue;
                e.last_pub_ms = now_ms;
            }
            double val = gen_value(e);
            int plen;
            if (e.is_int)
                plen = snprintf(payload, sizeof(payload),
                                "{\"ts\":\"%s\",\"value\":%d}", ts_buf, (int)val);
            else
                plen = snprintf(payload, sizeof(payload),
                                "{\"ts\":\"%s\",\"value\":%.6f}", ts_buf, val);

            while (!g_stop.load()) {
                int rc = mosquitto_publish(mosq, nullptr, e.topic.c_str(),
                                           plen, payload, qos, false);
                if (rc == MOSQ_ERR_SUCCESS) break;
                if (rc == MOSQ_ERR_NOMEM) std::this_thread::yield();
                else if (rc == MOSQ_ERR_NO_CONN)
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                else break;
            }
            g_published.fetch_add(1, std::memory_order_relaxed);
        }

        int rate = g_rate.load();
        if (rate > 0) {
            int msgs = (int)g_topics.size();
            auto wake = sweep_start + std::chrono::duration<double>((double)msgs / rate);
            while (!g_stop.load() && std::chrono::steady_clock::now() < wake)
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        ++loop;
    }
    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
}

// ============================================================================
// WebSocket — handshake + frame I/O
// ============================================================================

static bool ws_handshake(int fd) {
    char buf[4096]; buf[0]='\0';
    int n = (int)recv(fd, buf, sizeof(buf)-1, 0);
    if (n <= 0) return false;
    buf[n] = '\0';
    const char* k = strstr(buf, "Sec-WebSocket-Key:");
    if (!k) return false;
    k += 18; while (*k==' ') ++k;
    const char* ke = strpbrk(k, "\r\n");
    if (!ke) return false;
    std::string key(k, ke-k);
    key += "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    uint8_t sha[20];
    sha1_hash((const uint8_t*)key.data(), key.size(), sha);
    std::string accept = base64_enc(sha, 20);
    std::string resp =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
    return send(fd, resp.data(), resp.size(), 0) == (ssize_t)resp.size();
}

static std::string ws_read_frame(int fd) {
    uint8_t hdr[2];
    if (recv(fd, hdr, 2, MSG_WAITALL) != 2) return "";
    int opcode = hdr[0] & 0x0F;
    if (opcode == 8) return "";   // close
    bool masked = (hdr[1] & 0x80) != 0;
    uint64_t plen = hdr[1] & 0x7F;
    if (plen == 126) {
        uint8_t e[2]; recv(fd, e, 2, MSG_WAITALL);
        plen = ((uint64_t)e[0]<<8)|e[1];
    } else if (plen == 127) {
        uint8_t e[8]; recv(fd, e, 8, MSG_WAITALL);
        plen=0; for(int i=0;i<8;++i) plen=(plen<<8)|e[i];
    }
    uint8_t mask[4]={};
    if (masked) recv(fd, mask, 4, MSG_WAITALL);
    if (plen == 0 || plen > 65536) return "";
    std::string payload(plen, '\0');
    recv(fd, payload.data(), plen, MSG_WAITALL);
    if (masked) for (size_t i=0;i<plen;++i) payload[i]^=mask[i&3];
    return (opcode==1||opcode==0) ? payload : "";
}

static bool ws_send(int fd, const std::string& msg) {
    size_t n = msg.size();
    uint8_t hdr[10]; int hl;
    hdr[0] = 0x81;  // FIN + text
    if (n < 126)      { hdr[1]=(uint8_t)n; hl=2; }
    else if (n<65536) { hdr[1]=126; hdr[2]=(n>>8)&0xFF; hdr[3]=n&0xFF; hl=4; }
    else              { hdr[1]=127; for(int i=0;i<8;++i) hdr[2+i]=(n>>(56-8*i))&0xFF; hl=10; }
    if (send(fd,hdr,hl,MSG_NOSIGNAL)!=hl) return false;
    return send(fd,msg.data(),n,MSG_NOSIGNAL)==(ssize_t)n;
}

static void ws_broadcast(const std::string& msg) {
    std::lock_guard<std::mutex> lk(g_ws_mtx);
    for (int fd : g_ws_fds) ws_send(fd, msg);
}

// ============================================================================
// Status JSON builder
// ============================================================================

static std::string build_status(uint64_t mps) {
    std::string s;
    s.reserve(4096);
    s += "{\"type\":\"status\",\"mps\":"; s += std::to_string(mps);
    s += ",\"rate\":"; s += std::to_string(g_rate.load());
    s += ",\"smooth_burst\":"; s += (g_slow_interval_ms.load() <= 1000 ? "true" : "false");
    s += ",\"site_id\":\""; s += g_site_id; s += "\"";
    s += ",\"units\":[";

    std::lock_guard<std::mutex> lk(g_state_mtx);
    for (size_t i=0;i<g_units.size();++i) {
        const auto& u = g_units[i];
        if (i) s += ",";
        const char* mstr = u.mode==Mode::STANDBY     ? "standby"    :
                           u.mode==Mode::CHARGING     ? "charging"   :
                           u.mode==Mode::DISCHARGING  ? "discharging":
                                                        "offline";
        char buf[512];
        snprintf(buf, sizeof(buf),
                 "{\"id\":\"%s\",\"mode\":\"%s\",\"contactor\":%s,"
                 "\"soc\":%.1f,\"current_a\":%.1f,\"voltage_v\":%.0f,"
                 "\"temp_c\":%.1f,\"noise_pct\":%.1f,"
                 "\"faults\":{\"overtemp\":%s,\"low_soc\":%s,"
                              "\"overcurr\":%s,\"undervolt\":%s,\"overvolt\":%s},"
                 "\"racks\":[",
                 u.id.c_str(), mstr,
                 u.contactor ? "true" : "false",
                 u.soc, u.current, soc_to_voltage(u.soc),
                 u.temp_avg, u.noise_pct,
                 u.racks[0].temp>45?"true":"false",
                 u.soc<10?"true":"false",
                 u.inject_overcurr?"true":"false",
                 soc_to_cell_v(u.soc)<2.8?"true":"false",
                 (u.racks[0].inject_overvolt||u.racks[1].inject_overvolt||
                  u.racks[2].inject_overvolt||u.racks[3].inject_overvolt||
                  u.racks[4].inject_overvolt)?"true":"false");
        s += buf;
        for (int ri=0;ri<5;++ri) {
            char rb[128];
            snprintf(rb, sizeof(rb), "%s{\"soc\":%.1f,\"temp\":%.1f}",
                     ri?",":"", u.racks[ri].soc, u.racks[ri].temp);
            s += rb;
        }
        s += "]}";
    }
    s += "]}";
    return s;
}

// ============================================================================
static std::string read_sys_stats();  // defined in stats section below

// Command handler
// ============================================================================

static void handle_command(const std::string& raw) {
    try {
        simdjson::ondemand::parser parser;
        simdjson::padded_string ps(raw);
        auto doc = parser.iterate(ps);
        std::string_view type = doc["type"].get_string();

        std::lock_guard<std::mutex> lk(g_state_mtx);

        if (type == "set_rate") {
            g_rate.store((int)doc["rate"].get_int64());
            return;
        }
        if (type == "set_smooth_burst") {
            bool enabled = doc["enabled"].get_bool();
            g_slow_interval_ms.store(enabled ? 1000 : 20000);
            return;
        }
        if (type == "get_sys_stats") {
            ws_broadcast(read_sys_stats());
            return;
        }
        if (type == "get_status") return;  // status sent on next broadcast

        // Resolve target units from "units" array (["ALL"] or list of IDs)
        std::vector<UnitState*> targets;
        simdjson::ondemand::array units_arr;
        if (!doc["units"].get_array().get(units_arr)) {
            for (auto u : units_arr) {
                std::string_view id;
                if (u.get_string().get(id)) continue;
                if (id == "ALL") {
                    targets.clear();
                    for (auto& unit : g_units) targets.push_back(&unit);
                    break;
                }
                for (auto& unit : g_units)
                    if (unit.id == id) { targets.push_back(&unit); break; }
            }
        }

        if (type == "set_mode") {
            std::string_view mode_sv = doc["mode"].get_string();
            float current_a = 0.0f;
            auto cv = doc["current_a"];
            if (!cv.error()) current_a = (float)cv.get_double();

            Mode m = Mode::STANDBY;
            if (mode_sv=="charge")      m = Mode::CHARGING;
            else if (mode_sv=="discharge") m = Mode::DISCHARGING;
            else if (mode_sv=="offline")   m = Mode::OFFLINE;

            for (auto* u : targets) {
                u->mode    = m;
                u->current = current_a;
                if (m == Mode::OFFLINE) { u->contactor = false; u->current = 0; }
                else                    { u->contactor = true; }
            }

        } else if (type == "set_current") {
            float current_a = 0.0f;
            auto cv = doc["current_a"]; if (!cv.error()) current_a = (float)cv.get_double();
            for (auto* u : targets) u->current = current_a;

        } else if (type == "set_contactor") {
            bool closed = doc["closed"].get_bool();
            for (auto* u : targets) {
                u->contactor = closed;
                if (!closed) { u->mode = Mode::STANDBY; u->current = 0; }
            }

        } else if (type == "inject_fault") {
            std::string_view fault = doc["fault"].get_string();
            int rack = -1;
            auto rv = doc["rack"]; if (!rv.error()) rack = (int)rv.get_int64();
            float value = 55.0f;
            auto vv = doc["value"]; if (!vv.error()) value = (float)vv.get_double();

            for (auto* u : targets) {
                if (fault == "overtemp") {
                    if (rack < 0) for (auto& r : u->racks) r.temp = value;
                    else if (rack < 5) u->racks[rack].temp = value;

                } else if (fault == "low_soc") {
                    u->soc = value;
                    for (auto& r : u->racks) r.soc = value;

                } else if (fault == "undervolt") {
                    // Force rack SOC very low so cell_v < 2.8V
                    float low_soc = 3.0f;  // cell_v ≈ 2.78V
                    if (rack < 0) for (auto& r : u->racks) r.soc = low_soc;
                    else if (rack < 5) u->racks[rack].soc = low_soc;

                } else if (fault == "overvolt") {
                    if (rack < 0) for (auto& r : u->racks) r.inject_overvolt = true;
                    else if (rack < 5) u->racks[rack].inject_overvolt = true;

                } else if (fault == "overcurrent") {
                    float ca = 650.0f;
                    auto cav = doc["current_a"]; if (!cav.error()) ca = (float)cav.get_double();
                    u->inject_overcurr = true;
                    u->current = ca;

                } else if (fault == "soc_imbalance") {
                    // Spread rack SOCs by ±spread/2 around system SOC
                    float half = value / 2.0f;
                    float step = (5 > 1) ? value / 4.0f : 0;
                    for (int ri=0;ri<5;++ri)
                        u->racks[ri].soc = std::max(0.0f,
                            std::min(100.0f, u->soc - half + ri * step));
                }
            }

        } else if (type == "clear_faults") {
            for (auto* u : targets) {
                u->inject_overcurr = false;
                for (auto& r : u->racks) {
                    r.temp = u->temp_avg;
                    r.soc  = u->soc;
                    r.inject_overvolt = false;
                }
            }

        } else if (type == "set_noise") {
            // "amplitude": 0–100 (% of signal value, peak-to-peak ÷ 2)
            float amp = 0.0f;
            double d; if (!doc["amplitude"].get_double().get(d)) amp = (float)d;
            amp = std::max(0.0f, std::min(100.0f, amp));
            for (auto* u : targets) u->noise_pct = amp;
        }
    } catch (...) {
        fprintf(stderr, "[ws] bad command: %s\n", raw.c_str());
    }
}

// ============================================================================
// WebSocket server — one connection at a time
// ============================================================================

static void ws_handler(int fd) {
    {
        std::lock_guard<std::mutex> lk(g_ws_mtx);
        g_ws_fds.push_back(fd);
    }
    fprintf(stderr, "[ws] client connected fd=%d\n", fd);

    struct pollfd pfd{fd, POLLIN, 0};
    while (!g_stop.load()) {
        int rc = poll(&pfd, 1, 1000);
        if (rc < 0) break;
        if (rc == 0) continue;  // timeout — broadcaster will send status
        if (pfd.revents & (POLLHUP|POLLERR)) break;
        std::string frame = ws_read_frame(fd);
        if (frame.empty()) break;
        handle_command(frame);
    }

    {
        std::lock_guard<std::mutex> lk(g_ws_mtx);
        g_ws_fds.erase(std::remove(g_ws_fds.begin(), g_ws_fds.end(), fd),
                        g_ws_fds.end());
    }
    close(fd);
    fprintf(stderr, "[ws] client disconnected fd=%d\n", fd);
}

static void ws_server_fn(int port) {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET; addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("[ws] bind"); close(srv); return;
    }
    listen(srv, 4);
    fprintf(stderr, "[ws] listening on :%d\n", port);

    struct pollfd pfd{srv, POLLIN, 0};
    while (!g_stop.load()) {
        if (poll(&pfd, 1, 500) <= 0) continue;
        int fd = accept(srv, nullptr, nullptr);
        if (fd < 0) continue;
        if (!ws_handshake(fd)) { close(fd); continue; }
        std::thread(ws_handler, fd).detach();
    }
    close(srv);
}

// ============================================================================
// Stats broadcaster + stdout logger
// ============================================================================

static std::string read_sys_stats() {
    float la1=0, la5=0, la15=0;
    FILE* f = fopen("/proc/loadavg", "r");
    if (f) { fscanf(f, "%f %f %f", &la1, &la5, &la15); fclose(f); }

    long rss_kb = 0;
    FILE* s = fopen("/proc/self/status", "r");
    if (s) {
        char line[256];
        while (fgets(line, sizeof(line), s))
            if (strncmp(line, "VmRSS:", 6) == 0) { sscanf(line+6, "%ld", &rss_kb); break; }
        fclose(s);
    }

    char buf[256];
    snprintf(buf, sizeof(buf),
        "{\"type\":\"sys_stats\",\"load_1\":%.2f,\"load_5\":%.2f,\"load_15\":%.2f"
        ",\"rss_mb\":%.1f,\"mps\":%llu}",
        la1, la5, la15, rss_kb / 1024.0, (unsigned long long)g_last_mps.load());
    return buf;
}

static void stats_fn(int n_topics) {
    uint64_t last = 0;
    auto     t_last = std::chrono::steady_clock::now();
    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto     now = std::chrono::steady_clock::now();
        uint64_t cur = g_published.load();
        double   dt  = std::chrono::duration<double>(now - t_last).count();
        uint64_t mps = (uint64_t)((cur - last) / dt);
        g_last_mps.store(mps);

        fprintf(stdout, "[ems] %7lu msg/s  topics/sweep=%d  rate=%d  units=%zu\n",
                (unsigned long)mps, n_topics, g_rate.load(), g_units.size());
        fflush(stdout);

        ws_broadcast(build_status(mps));

        last = cur; t_last = now;
    }
}

// ============================================================================
// Template auto-detect
// ============================================================================

static std::string find_template(const char* argv0) {
    static const char* cands[] = {
        "ems_topic_template.json",
        "../stress_runner/ems_topic_template.json",
        "source/stress_runner/ems_topic_template.json",
        nullptr
    };
    for (int i = 0; cands[i]; ++i) {
        if (FILE* f=fopen(cands[i],"r")) { fclose(f); return cands[i]; }
    }
    std::string bin(argv0);
    auto sl = bin.rfind('/');
    if (sl != std::string::npos) {
        std::string p = bin.substr(0, sl+1) + "ems_topic_template.json";
        if (FILE* f=fopen(p.c_str(),"r")) { fclose(f); return p; }
    }
    return "";
}

// ============================================================================
// main
// ============================================================================

int main(int argc, char* argv[]) {
    std::string host   = "localhost";
    int         port   = 1883;
    std::string tpl    = "";
    int         n_units= 4;
    std::vector<std::string> unit_ids_arg;
    int         rate   = 0;
    int         qos    = 0;
    std::string base_id= "ems-stress";
    int         ws_port= 8769;
    float       init_soc = 50.0f;

    for (int i=1;i<argc;++i) {
        if      (!strcmp(argv[i],"--host")     && i+1<argc) host       = argv[++i];
        else if (!strcmp(argv[i],"--port")     && i+1<argc) port       = atoi(argv[++i]);
        else if (!strcmp(argv[i],"--template") && i+1<argc) tpl        = argv[++i];
        else if (!strcmp(argv[i],"--units")    && i+1<argc) n_units    = atoi(argv[++i]);
        else if (!strcmp(argv[i],"--unit-id")  && i+1<argc) unit_ids_arg.push_back(argv[++i]);
        else if (!strcmp(argv[i],"--rate")     && i+1<argc) rate       = atoi(argv[++i]);
        else if (!strcmp(argv[i],"--qos")      && i+1<argc) qos        = atoi(argv[++i]);
        else if (!strcmp(argv[i],"--id")       && i+1<argc) base_id    = argv[++i];
        else if (!strcmp(argv[i],"--ws-port")  && i+1<argc) ws_port    = atoi(argv[++i]);
        else if (!strcmp(argv[i],"--soc")      && i+1<argc) init_soc   = atof(argv[++i]);
        else if (!strcmp(argv[i],"--site-id") && i+1<argc) {
            std::string arg = argv[++i];
            std::stringstream ss(arg); std::string tok;
            while (std::getline(ss, tok, ',')) if (!tok.empty()) g_site_ids.push_back(tok);
            if (!g_site_ids.empty()) g_site_id = g_site_ids[0];
        }
        else if (!strcmp(argv[i],"--topic-prefix") && i+1<argc) g_topic_prefix = argv[++i];
    }

    if (tpl.empty()) { tpl = find_template(argv[0]); }
    if (tpl.empty()) {
        fprintf(stderr,"ERROR: ems_topic_template.json not found — use --template <path>\n");
        return 1;
    }

    // Resolve unit IDs
    std::vector<std::string> unit_ids;
    if (!unit_ids_arg.empty()) {
        unit_ids = unit_ids_arg;
    } else {
        // Prefer unit_ids from the template JSON (real deployment IDs).
        // Fall back to synthetic hex IDs only if the template has none.
        unit_ids = load_unit_ids_from_template(tpl);
        if (unit_ids.empty()) {
            uint32_t base = 0x0215F5DD;
            for (int i=0;i<n_units;++i) {
                char buf[16]; snprintf(buf,sizeof(buf),"%08X",base+i);
                unit_ids.push_back(buf);
            }
        } else if (n_units != 4) {
            // --units was explicitly set; truncate or warn
            if ((int)unit_ids.size() > n_units) unit_ids.resize(n_units);
        }
    }

    // Init unit states
    g_units.resize(unit_ids.size());
    for (size_t i=0;i<unit_ids.size();++i) {
        g_units[i].id  = unit_ids[i];
        g_units[i].soc = init_soc;
        g_units[i].init_racks();
    }

    // Init generic float pool and noise pool
    srand(42);
    for (int i=0;i<65536;++i) {
        generic_pool[i] = 3.0f + (i%10000)*0.01f;
        noise_pool[i]   = ((float)rand() / RAND_MAX) - 0.5f;  // [-0.5, +0.5]
    }
    pool_init = true;

    // Load topics
    mosquitto_lib_init();
    // Default to two site IDs if none provided
    if (g_site_ids.empty()) {
        g_site_ids = {"0215D1D8", "0227C829"};
        g_site_id  = g_site_ids[0];
    }

    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler);
    g_rate.store(rate);

    try {
        g_topics = build_topics(tpl, unit_ids);
    } catch (const std::exception& ex) {
        fprintf(stderr,"ERROR loading template '%s': %s\n", tpl.c_str(), ex.what());
        mosquitto_lib_cleanup(); return 1;
    }

    std::string sites_str;
    for (size_t i=0;i<g_site_ids.size();++i){ if(i) sites_str+=','; sites_str+=g_site_ids[i]; }
    fprintf(stdout,
        "[ems] host=%s:%d  sites=%s  units=%zu  topics/sweep=%zu  rate=%s  ws=:%d  soc=%.0f%%\n",
        host.c_str(), port, sites_str.c_str(),
        unit_ids.size(), g_topics.size(), rate?std::to_string(rate).c_str():"unlimited",
        ws_port, init_soc);
    fflush(stdout);

    g_mqtt_host_g = host;
    g_mqtt_port_g = port;

    std::thread ph(physics_thread_fn);
    std::thread pu(publish_thread_fn, host, port, base_id, qos);
    std::thread ws(ws_server_fn, ws_port);
    std::thread st(stats_fn, (int)g_topics.size());
    std::thread periph(peripheral_thread_fn, base_id + "-periph", qos);

    pu.join();
    g_stop.store(true);
    ph.join(); ws.join(); st.join(); periph.join();

    mosquitto_lib_cleanup();
    return 0;
}
