/**
 * stress_pub.cpp — high-throughput C++ MQTT stress publisher
 *
 * Uses libmosquitto with loop_start() (dedicated network thread).
 * Publishes fake battery telemetry in per_cell_item format matching
 * the Python stress_runner, so existing writers and subscribers work
 * without change.
 *
 * Build:
 *   g++ -O2 -std=c++17 -o stress_pub stress_pub.cpp -lmosquitto
 *
 * Usage:
 *   ./stress_pub [options]
 *     --host    <host>       broker host      (default: localhost)
 *     --port    <port>       broker port      (default: 1883)
 *     --prefix  <prefix>     topic prefix     (default: batteries_a)
 *     --racks   <n>          racks            (default: 12)
 *     --modules <n>          modules per rack (default: 8)
 *     --cells   <n>          cells per module (default: 52)
 *     --conns   <n>          parallel MQTT connections (default: 1)
 *     --qos     <0|1>        QoS level        (default: 0)
 *     --id      <client_id>  base client ID   (default: stress-cpp)
 *     --rate    <n>          target total msgs/sec across all workers (default: 0 = unlimited)
 */

#include <mosquitto.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>
#include <vector>
#include <csignal>

static std::atomic<bool>     g_stop{false};
static std::atomic<uint64_t> g_published{0};

static void on_connect(struct mosquitto* mosq, void*, int rc) {
    if (rc != 0)
        fprintf(stderr, "[mqtt] connect failed rc=%d\n", rc);
}

static void on_disconnect(struct mosquitto*, void*, int rc) {
    if (rc != 0 && !g_stop.load())
        fprintf(stderr, "[mqtt] disconnected rc=%d\n", rc);
}

static void sig_handler(int) { g_stop.store(true); }

// ---------------------------------------------------------------------------
// One publish worker: owns one mosquitto instance, runs publish loop in
// the calling thread.  loop_start() handles the network I/O on a second
// thread.
// ---------------------------------------------------------------------------

struct WorkerCfg {
    std::string host;
    int         port;
    std::string prefix;
    int         racks;
    int         modules;
    int         cells;
    int         qos;
    std::string client_id;
    int         worker_id;
    int         rate_per_worker = 0;
};

static void worker(WorkerCfg cfg) {
    struct mosquitto* mosq = mosquitto_new(cfg.client_id.c_str(), true, nullptr);
    if (!mosq) { fprintf(stderr, "mosquitto_new failed\n"); return; }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_reconnect_delay_set(mosq, 1, 5, false);

    // Connect with retry
    while (!g_stop.load()) {
        int rc = mosquitto_connect(mosq, cfg.host.c_str(), cfg.port, 60);
        if (rc == MOSQ_ERR_SUCCESS) break;
        fprintf(stderr, "[%s] connect failed: %s — retrying\n",
                cfg.client_id.c_str(), mosquitto_strerror(rc));
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (g_stop.load()) { mosquitto_destroy(mosq); return; }

    mosquitto_loop_start(mosq);   // network I/O thread

    // Pre-build topic and payload templates
    // Topic: {prefix}/project=0/site=0/rack=R/module=M/cell=C/{field}
    static const char* FIELDS[] = {
        "voltage", "current", "temperature", "soc",
        "soh", "resistance", "capacity", nullptr
    };
    static const double VALS[] = {
        3.700, 0.0, 25.0, 80.0, 98.0, 0.001, 100.0
    };

    char   topic[256];
    char   payload[128];
    double ts_base = static_cast<double>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count()) / 1000.0;

    uint64_t loop_count = 0;

    while (!g_stop.load()) {
        auto sweep_start = std::chrono::steady_clock::now();
        double ts = ts_base + loop_count * 0.001;   // synthetic 1ms increment

        for (int r = 0; r < cfg.racks && !g_stop.load(); ++r) {
            for (int m = 0; m < cfg.modules && !g_stop.load(); ++m) {
                for (int c = 0; c < cfg.cells && !g_stop.load(); ++c) {
                    for (int f = 0; FIELDS[f]; ++f) {
                        int n = snprintf(topic, sizeof(topic),
                            "%s/project=0/site=0/rack=%d/module=%d/cell=%d/%s",
                            cfg.prefix.c_str(), r, m, c, FIELDS[f]);
                        (void)n;

                        // Tiny variance so values aren't all identical
                        double val = VALS[f] + (loop_count % 100) * 0.001;
                        int plen = snprintf(payload, sizeof(payload),
                            "{\"timestamp\":%.3f,\"value\":%.4f}", ts, val);

                        // Spin if the outgoing queue is deep (back-pressure)
                        while (!g_stop.load()) {
                            int rc = mosquitto_publish(mosq, nullptr,
                                topic, plen, payload, cfg.qos, false);
                            if (rc == MOSQ_ERR_SUCCESS) break;
                            if (rc == MOSQ_ERR_NOMEM) {
                                // Queue full — yield and retry
                                std::this_thread::yield();
                            } else if (rc == MOSQ_ERR_NO_CONN) {
                                // Disconnected — wait for auto-reconnect
                                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            } else {
                                fprintf(stderr, "publish err %d\n", rc);
                                break;
                            }
                        }
                        g_published.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }

        // Rate limiting: sleep remainder of sweep window if needed
        if (cfg.rate_per_worker > 0) {
            int    msgs_per_sweep = cfg.racks * cfg.modules * cfg.cells * 7;
            double target_s  = (double)msgs_per_sweep / cfg.rate_per_worker;
            double elapsed_s = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - sweep_start).count();
            if (elapsed_s < target_s)
                std::this_thread::sleep_for(
                    std::chrono::duration<double>(target_s - elapsed_s));
        }

        ++loop_count;
    }

    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);
    mosquitto_destroy(mosq);
}

// ---------------------------------------------------------------------------
// Stats printer
// ---------------------------------------------------------------------------

static void stats_loop(int racks, int modules, int cells, int rate_total) {
    uint64_t last = 0;
    auto     t_last = std::chrono::steady_clock::now();

    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto     now = std::chrono::steady_clock::now();
        uint64_t cur = g_published.load(std::memory_order_relaxed);
        double   dt  = std::chrono::duration<double>(now - t_last).count();
        uint64_t mps = static_cast<uint64_t>((cur - last) / dt);
        int      total_cells = racks * modules * cells;
        if (rate_total > 0) {
            fprintf(stdout, "[stress_pub] %7lu msg/s  (%d cells × 7 fields = %d expected/s)"
                    "  target=%d/s\n",
                    (unsigned long)mps, total_cells, total_cells * 7, rate_total);
        } else {
            fprintf(stdout, "[stress_pub] %7lu msg/s  (%d cells × 7 fields = %d expected/s)\n",
                    (unsigned long)mps, total_cells, total_cells * 7);
        }
        fflush(stdout);
        last   = cur;
        t_last = now;
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    std::string host   = "localhost";
    int         port   = 1883;
    std::string prefix = "batteries_a";
    int         racks  = 12;
    int         modules = 8;
    int         cells  = 52;
    int         conns  = 1;
    int         qos    = 0;
    std::string base_id = "stress-cpp";
    int         rate_total = 0;

    for (int i = 1; i < argc; ++i) {
        if      (!strcmp(argv[i], "--host")    && i+1<argc) host       = argv[++i];
        else if (!strcmp(argv[i], "--port")    && i+1<argc) port       = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--prefix")  && i+1<argc) prefix     = argv[++i];
        else if (!strcmp(argv[i], "--racks")   && i+1<argc) racks      = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--modules") && i+1<argc) modules    = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--cells")   && i+1<argc) cells      = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--conns")   && i+1<argc) conns      = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--qos")     && i+1<argc) qos        = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--id")      && i+1<argc) base_id    = argv[++i];
        else if (!strcmp(argv[i], "--rate")    && i+1<argc) rate_total = atoi(argv[++i]);
    }

    mosquitto_lib_init();
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    fprintf(stdout,
        "[stress_pub] host=%s:%d  prefix=%s  %d×%d×%d cells  "
        "%d conns  qos=%d  rate=%s\n",
        host.c_str(), port, prefix.c_str(),
        racks, modules, cells, conns, qos,
        rate_total > 0 ? std::to_string(rate_total).c_str() : "unlimited");
    fflush(stdout);

    // Launch worker threads (one per connection)
    int rate_per_worker = (rate_total > 0 && conns > 0) ? rate_total / conns : 0;

    std::vector<std::thread> workers;
    workers.reserve(conns);
    for (int i = 0; i < conns; ++i) {
        WorkerCfg cfg;
        cfg.host            = host;
        cfg.port            = port;
        cfg.prefix          = prefix;
        cfg.racks           = racks;
        cfg.modules         = modules;
        cfg.cells           = cells;
        cfg.qos             = qos;
        cfg.client_id       = base_id + "-" + std::to_string(i);
        cfg.worker_id       = i;
        cfg.rate_per_worker = rate_per_worker;
        workers.emplace_back(worker, cfg);
    }

    std::thread stats(stats_loop, racks, modules, cells, rate_total);

    for (auto& w : workers) w.join();
    g_stop.store(true);
    stats.join();

    mosquitto_lib_cleanup();
    return 0;
}
