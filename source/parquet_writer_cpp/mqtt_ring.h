/**
 * mqtt_ring.h — lock-free SPSC ring for raw MQTT messages
 *
 * Producer: libmosquitto network thread (on_message callback)
 * Consumer: parse_thread_fn
 *
 * Design:
 *   on_message does nothing but memcpy topic+payload into a slot and advance
 *   write_pos.  No mutex, no JSON parsing, no map lookup — just a store.
 *   The libmosquitto network thread stays fast enough to drain the TCP socket
 *   at 80k+ msg/s without FlashMQ dropping QoS-0 messages.
 *
 *   parse_thread_fn drains the ring and does the full parse → g_buffer insert.
 *   It owns the mutex only for the brief buffer-insert step (< 1 µs), not during
 *   JSON parsing.
 *
 * Sizing:
 *   262144 slots × ~390 bytes/slot ≈ 100 MB.
 *   At 300k msg/s, 262144 slots covers ~870 ms of burst — enough to absorb
 *   slow parquet flushes (observed up to ~300 ms on the torture server).
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

static constexpr uint32_t MQRING_CAPACITY  = 1u << 18;  // 262144 — must be power-of-2
static constexpr uint32_t MQRING_TOPIC_MAX = 256;  // real EMS topics reach 136 bytes; 256 gives safe headroom
static constexpr uint32_t MQRING_PAY_MAX   = 256;

struct MqSlot {
    uint16_t topic_len;
    uint16_t payload_len;
    char     topic  [MQRING_TOPIC_MAX];
    char     payload[MQRING_PAY_MAX];
};

struct MqttRing {
    // Separate cache lines for write_pos and read_pos to avoid false sharing
    alignas(64) std::atomic<uint64_t> write_pos{0};
    alignas(64) std::atomic<uint64_t> read_pos{0};
    alignas(64) MqSlot slots[MQRING_CAPACITY];

    // Counts silent truncations — exposed in /health so operator sees them without log diving.
    // Truncation corrupts parse_topic() silently; bump MQRING_TOPIC_MAX/MQRING_PAY_MAX if nonzero.
    std::atomic<uint64_t> topic_truncations{0};
    std::atomic<uint64_t> payload_truncations{0};

    // Called from on_message (libmosquitto network thread).
    // Returns false when ring is full — caller counts as overflow drop.
    bool push(const char* topic,   int tlen,
              const char* payload, int plen) noexcept {
        uint64_t w = write_pos.load(std::memory_order_relaxed);
        uint64_t r = read_pos.load(std::memory_order_acquire);
        if (w - r >= MQRING_CAPACITY) return false;

        auto& s       = slots[w & (MQRING_CAPACITY - 1)];
        if (tlen >= (int)MQRING_TOPIC_MAX) topic_truncations.fetch_add(1, std::memory_order_relaxed);
        if (plen >= (int)MQRING_PAY_MAX)   payload_truncations.fetch_add(1, std::memory_order_relaxed);
        s.topic_len   = static_cast<uint16_t>(std::min(tlen,   (int)MQRING_TOPIC_MAX - 1));
        s.payload_len = static_cast<uint16_t>(std::min(plen,   (int)MQRING_PAY_MAX   - 1));
        std::memcpy(s.topic,   topic,   s.topic_len);   s.topic  [s.topic_len]   = '\0';
        std::memcpy(s.payload, payload, s.payload_len); s.payload[s.payload_len] = '\0';

        write_pos.store(w + 1, std::memory_order_release);
        return true;
    }

    // Called from parse_thread.  Returns false when ring is empty.
    bool pop(MqSlot& out) noexcept {
        uint64_t r = read_pos.load(std::memory_order_relaxed);
        uint64_t w = write_pos.load(std::memory_order_acquire);
        if (r == w) return false;

        out = slots[r & (MQRING_CAPACITY - 1)];
        read_pos.store(r + 1, std::memory_order_release);
        return true;
    }

    bool empty() const noexcept {
        return read_pos.load(std::memory_order_acquire) ==
               write_pos.load(std::memory_order_acquire);
    }
};
