/*
 * fast_load_control.ino
 * Fast DAC Load Controller for Heltec LoRa32 V2/V3
 * Version: 1.3.3
 *
 * Based on load_controller_lora32.ino v1.5.5
 *
 * CHANGES FROM ORIGINAL:
 *   - V2 (ESP32 true DAC): replaced dacWrite() with I2S DMA DAC output
 *     at SAMPLE_RATE (default 5000 Hz). Profile ramp interpolation now
 *     runs per-sample inside dacTask (Core 0), giving smooth 200µs steps
 *     instead of the original ~100Hz (10ms) steps.
 *   - V3 (ESP32-S3, no native DAC): PWM unchanged but PWM_FREQ raised to
 *     SAMPLE_RATE to match. An RC filter on the PWM pin is still required.
 *   - Main loop delay(10) replaced with delay(1) — WiFi/WS unaffected,
 *     but profile execution is now owned by dacTask, not loop().
 *   - All WebSocket, serial, profile load/save, display, OTA, CAN, GPIO
 *     code is identical to the original.
 *
 * I2S DAC wiring (V2 only — no external parts needed):
 *   GPIO25 → DAC1 (Voltage channel, 0–3.3 V)
 *   GPIO26 → DAC2 (Current channel, 0–3.3 V)
 *
 * I2S sample format (I2S_CHANNEL_FMT_RIGHT_LEFT + I2S_MODE_DAC_BUILT_IN):
 *   Each 32-bit word = [ LEFT16 | RIGHT16 ]
 *   LEFT  → DAC2 (GPIO26), RIGHT → DAC1 (GPIO25)
 *   Only the upper 8 bits of each 16-bit half are sent to the 8-bit DAC,
 *   so scale: dac_8bit → (uint16_t)(dac_8bit) << 8
 *
 * Boards supported:
 *   HELTEC_V3     — Heltec LoRa32 V3 (ESP32-S3), PWM output
 *   LILYGO_TDISPLAY — LILYGO T-Display V1.1 (ESP32), I2S DAC
 *   (default)     — Heltec LoRa32 V2 (ESP32), I2S DAC
 *
 * Library requirements (same as original + driver/i2s.h which is built-in):
 *   ArduinoJson, WebSocketsServer, ArduinoOTA, Preferences,
 *   U8g2 (V2) or TFT_eSPI (T-Display)
 */

// ─── TFT inline config (must precede #include <TFT_eSPI.h>) ──────────────────
#ifdef LILYGO_TDISPLAY
  #define USER_SETUP_LOADED
  #define ST7789_DRIVER
  #define TFT_WIDTH      135
  #define TFT_HEIGHT     240
  #define TFT_MOSI        19
  #define TFT_SCLK        18
  #define TFT_CS           5
  #define TFT_DC          16
  #define TFT_RST         23
  #define LOAD_GLCD
  #define LOAD_FONT2
  #define LOAD_FONT4
  #define SPI_FREQUENCY   27000000
#endif

#include <WiFi.h>
#include <WiFiUdp.h>
#include <ArduinoOTA.h>
#include <WebSocketsServer.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#ifdef LILYGO_TDISPLAY
  #include <TFT_eSPI.h>
#else
  #include <Wire.h>
  #include <U8g2lib.h>
#endif
#include "driver/twai.h"

// FAST: I2S driver for DMA DAC output (V2 / T-Display only)
#if !defined(HELTEC_V3)
  #include "driver/i2s.h"
#endif

// ─── Program identity ─────────────────────────────────────────────────────────
#define PROG_NAME     "FastLoad"
#define PROG_VERSION  "1.3.3"

// ─── FAST: DAC output rate ────────────────────────────────────────────────────
#define SAMPLE_RATE        5000   // Hz — DAC update rate (I2S DMA on V2)
#define I2S_DMA_BUF_LEN    64    // Samples per DMA buffer (12.8 ms per buf)
#define I2S_DMA_BUF_COUNT  4     // Number of DMA buffers (51.2 ms total)
// On V3 (PWM), this also sets the PWM carrier frequency
#define PWM_CARRIER_FREQ   SAMPLE_RATE

// ─── Board pin definitions ────────────────────────────────────────────────────
#ifdef HELTEC_V3
  // Heltec LoRa32 V3 (ESP32-S3) — no native DAC, use PWM + RC filter
  #define LED_PIN       35
  #define OLED_SDA      17
  #define OLED_SCL      18
  #define OLED_RST      21
  #define DAC1_PIN      1        // PWM voltage output (RC filter → analog)
  #define DAC2_PIN      2        // PWM current output (RC filter → analog)
  #define USE_PWM_DAC   1
  #define ADC1_PIN      4
  #define ADC2_PIN      5
  #define CAN_TX_PIN    6
  #define CAN_RX_PIN    7
  #define GPIO_IN_COUNT   4
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_IN_PINS[GPIO_IN_COUNT]   = {8, 9, 10, 11};
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {12, 13, 14};
  #define PWM_RESOLUTION 12
  #warning "Heltec V3: no native DAC — PWM output at SAMPLE_RATE Hz. Add RC filter."

#elif defined(LILYGO_TDISPLAY)
  // LILYGO T-Display V1.1 (ESP32) — true DAC, I2S DMA
  #define LED_PIN       2
  #define TFT_BL_PIN    4
  #define DAC1_PIN      25       // I2S DAC1 (RIGHT channel)
  #define DAC2_PIN      26       // I2S DAC2 (LEFT channel)
  #define USE_PWM_DAC   0
  #define ADC1_PIN      36
  #define ADC2_PIN      39
  #define CAN_TX_PIN    21
  #define CAN_RX_PIN    22
  #define GPIO_IN_COUNT   4
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_IN_PINS[GPIO_IN_COUNT]   = {12, 13, 14, 27};
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {32, 33, 17};

#else
  // Heltec LoRa32 V2 (ESP32) — true DAC on GPIO25/26, I2S DMA
  #define LED_PIN       2
  #define OLED_SDA      4
  #define OLED_SCL      15
  #define OLED_RST      16
  #define DAC1_PIN      25       // I2S DAC1 (RIGHT channel) — voltage
  #define DAC2_PIN      26       // I2S DAC2 (LEFT channel)  — current
  #define USE_PWM_DAC   0
  #define ADC1_PIN      36
  #define ADC2_PIN      39
  #define CAN_TX_PIN    21
  #define CAN_RX_PIN    22
  #define GPIO_IN_COUNT   4
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_IN_PINS[GPIO_IN_COUNT]   = {12, 13, 14, 27};
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {17, 23, 33};
#endif

// ─── Configuration ────────────────────────────────────────────────────────────
#define WS_PORT       81
#define SERIAL_BAUD   115200
#define DAC_MAX       4095      // 12-bit internal representation
#define MAX_VOLTAGE   60.0f
#define MAX_CURRENT   200.0f
#define DISPLAY_UPDATE_MS 250
#define MAX_PROFILE_STEPS 100

// Default credentials compiled in — overridden by NVS list if saved
#define WIFI_SSID_0   "2009google"
#define WIFI_PASS_0   "Butt3r12"
#define WIFI_SSID_1   ""          // ATT network — fill in or add via serial/WS
#define WIFI_PASS_1   ""
#define OTA_HOSTNAME  "fastload"
#define OTA_PORT      3232
#define OTA_PASSWORD  "bms2026"
#define PREF_NAMESPACE "loadctrl"

// ─── Display ──────────────────────────────────────────────────────────────────
#ifdef LILYGO_TDISPLAY
  static uint8_t tftBuf[sizeof(TFT_eSPI)] __attribute__((aligned(4)));
  TFT_eSPI* tftPtr = nullptr;
  #define tft (*tftPtr)
#else
  U8G2_SSD1306_128X64_NONAME_F_HW_I2C display(U8G2_R0, OLED_RST, OLED_SCL, OLED_SDA);
#endif

// ─── Global objects ───────────────────────────────────────────────────────────
Preferences       preferences;
WebSocketsServer  webSocket(WS_PORT);

// ─── WiFi credential list (up to 4 entries, tried in order on connect) ───────
#define WIFI_MAX_CREDS 4
struct WifiCred { String ssid; String pass; };
WifiCred wifiCreds[WIFI_MAX_CREDS];
int      wifiCredCount   = 0;
int      wifiCredActive  = -1;  // index of currently connected entry, -1 = none
String   wifiSSID        = "";  // active SSID (mirrors wifiCreds[wifiCredActive])
String   wifiPass        = "";  // active pass
bool     wifiConnected   = false;
String   locationName   = "";
bool     wsServerStarted = false;
bool     otaInProgress   = false;
int      otaProgress     = 0;

// LED
enum LedMode { LED_OFF, LED_ON, LED_BLINK };
LedMode ledMode = LED_OFF;
unsigned long lastLedToggle = 0;
bool ledState = false;

// ─── DAC / output state ───────────────────────────────────────────────────────
// These are written by the main loop / WebSocket and read by dacTask.
// Declared volatile so the compiler doesn't cache them in registers.
volatile uint16_t dac1Value   = 0;   // 0–4095, voltage channel (GPIO25)
volatile uint16_t dac2Value   = 0;   // 0–4095, current channel (GPIO26)
volatile float    voltageValue = 0.0f;
volatile float    currentValue = 0.0f;

// ─── FAST: cycle counters (written by dacTask, read by broadcastStatus) ───────
volatile uint32_t dacBufCount       = 0;  // increments each i2s_write (expect ~78/s)
volatile uint32_t dacStepTransitions = 0; // increments on each profile step advance

enum DacChannel { CHANNEL_VOLTAGE = 0, CHANNEL_CURRENT = 1 };
DacChannel displayChannel = CHANNEL_VOLTAGE;

// Mutex protecting profile state shared between loop() and dacTask()
portMUX_TYPE profileMux = portMUX_INITIALIZER_UNLOCKED;

// ─── Profile state ────────────────────────────────────────────────────────────
struct ProfileStep {
    String   name;
    float    value;
    uint32_t durationMs;
    uint32_t rampMs;
};

ProfileStep profileV[MAX_PROFILE_STEPS];
int   profileVCount       = 0;
bool  profileVRunning     = false;
bool  profileVLoop        = false;   // true = wrap at end; false = stop and hold
int   profileVStep        = 0;
unsigned long profileVStepStart  = 0;
float profileVStepStartVal       = 0;
bool  profileVInRamp      = false;

ProfileStep profileC[MAX_PROFILE_STEPS];
int   profileCCount       = 0;
bool  profileCRunning     = false;
bool  profileCLoop        = false;   // true = wrap at end; false = stop and hold
int   profileCStep        = 0;
unsigned long profileCStepStart  = 0;
float profileCStepStartVal       = 0;
bool  profileCInRamp      = false;

// ─── Counters, CAN, GPIO (identical to original) ──────────────────────────────
uint32_t wsMessageCount = 0;
uint32_t wsTxCount      = 0;
uint8_t  wsClientCount  = 0;

uint8_t gpioOutState[GPIO_OUT_COUNT] = {0};
uint8_t gpioInState[GPIO_IN_COUNT]   = {0};

bool     canEnabled    = false;
bool     canInstalled  = false;
uint32_t canSpeed      = 500;
uint32_t canTxCount    = 0, canRxCount = 0, canErrorCount = 0;

#define CAN_RX_BUFFER_SIZE 16
struct CanMessage {
    uint32_t     id;
    uint8_t      len;
    uint8_t      data[8];
    bool         extended;
    unsigned long timestamp;
};
CanMessage       canRxBuffer[CAN_RX_BUFFER_SIZE];
volatile uint8_t canRxHead = 0;
volatile uint8_t canRxTail = 0;

unsigned long lastDisplayUpdate = 0;

void startProfile(bool voltage, bool current);  // forward declaration for loadProfileFromPrefs

void saveWifiCreds() {
    preferences.begin(PREF_NAMESPACE, false);
    preferences.putInt("wifiCount", wifiCredCount);
    for (int i = 0; i < wifiCredCount; i++) {
        preferences.putString(("wssid" + String(i)).c_str(), wifiCreds[i].ssid);
        preferences.putString(("wpass" + String(i)).c_str(), wifiCreds[i].pass);
    }
    preferences.end();
}

// ─── Profile persistence helpers ─────────────────────────────────────────────
// Fixed-width binary layout for NVS — avoids the String member in ProfileStep.
struct StoredStep {
    char     name[17];   // null-terminated, max 16 chars
    float    value;
    uint32_t durationMs;
    uint32_t rampMs;
};  // 29 bytes; 100 steps × 2 channels = 5800 bytes, well within NVS limits

void saveProfileToPrefs() {
    StoredStep buf[MAX_PROFILE_STEPS];
    preferences.begin(PREF_NAMESPACE, false);

    // Voltage channel
    for (int i = 0; i < profileVCount; i++) {
        strncpy(buf[i].name, profileV[i].name.c_str(), 16); buf[i].name[16] = '\0';
        buf[i].value      = profileV[i].value;
        buf[i].durationMs = profileV[i].durationMs;
        buf[i].rampMs     = profileV[i].rampMs;
    }
    preferences.putInt("pVCount",   profileVCount);
    preferences.putBool("pVLoop",   profileVLoop);
    preferences.putBool("pVRun",    profileVRunning);
    if (profileVCount > 0)
        preferences.putBytes("pVSteps", buf, profileVCount * sizeof(StoredStep));

    // Current channel
    for (int i = 0; i < profileCCount; i++) {
        strncpy(buf[i].name, profileC[i].name.c_str(), 16); buf[i].name[16] = '\0';
        buf[i].value      = profileC[i].value;
        buf[i].durationMs = profileC[i].durationMs;
        buf[i].rampMs     = profileC[i].rampMs;
    }
    preferences.putInt("pCCount",   profileCCount);
    preferences.putBool("pCLoop",   profileCLoop);
    preferences.putBool("pCRun",    profileCRunning);
    if (profileCCount > 0)
        preferences.putBytes("pCSteps", buf, profileCCount * sizeof(StoredStep));

    preferences.end();
    Serial.printf("[prefs] profile saved: %d V steps, %d A steps\r\n",
                  profileVCount, profileCCount);
}

void loadProfileFromPrefs() {
    preferences.begin(PREF_NAMESPACE, true);   // read-only
    int  vc  = preferences.getInt("pVCount",  0);
    int  cc  = preferences.getInt("pCCount",  0);
    bool vl  = preferences.getBool("pVLoop",  false);
    bool cl  = preferences.getBool("pCLoop",  false);
    bool vRun = preferences.getBool("pVRun",  false);
    bool cRun = preferences.getBool("pCRun",  false);

    if (vc > 0 && vc <= MAX_PROFILE_STEPS) {
        StoredStep buf[MAX_PROFILE_STEPS];
        size_t got = preferences.getBytes("pVSteps", buf, vc * sizeof(StoredStep));
        if (got == (size_t)(vc * sizeof(StoredStep))) {
            profileVCount = vc;  profileVLoop = vl;
            for (int i = 0; i < vc; i++) {
                profileV[i].name       = buf[i].name;
                profileV[i].value      = buf[i].value;
                profileV[i].durationMs = buf[i].durationMs;
                profileV[i].rampMs     = buf[i].rampMs;
            }
            Serial.printf("[prefs] restored %d V steps\r\n", vc);
        }
    }
    if (cc > 0 && cc <= MAX_PROFILE_STEPS) {
        StoredStep buf[MAX_PROFILE_STEPS];
        size_t got = preferences.getBytes("pCSteps", buf, cc * sizeof(StoredStep));
        if (got == (size_t)(cc * sizeof(StoredStep))) {
            profileCCount = cc;  profileCLoop = cl;
            for (int i = 0; i < cc; i++) {
                profileC[i].name       = buf[i].name;
                profileC[i].value      = buf[i].value;
                profileC[i].durationMs = buf[i].durationMs;
                profileC[i].rampMs     = buf[i].rampMs;
            }
            Serial.printf("[prefs] restored %d A steps\r\n", cc);
        }
    }
    preferences.end();

    // Re-start whichever channels were running before the reset
    if ((vRun && profileVCount > 0) || (cRun && profileCCount > 0)) {
        startProfile(vRun, cRun);
        Serial.printf("[prefs] auto-started: V=%d C=%d\r\n", vRun, cRun);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FAST: I2S DMA DAC initialisation (V2 / T-Display only)
// ─────────────────────────────────────────────────────────────────────────────
#if !USE_PWM_DAC
static void i2s_dac_init() {
    i2s_config_t cfg = {
        .mode                 = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX
                                              | I2S_MODE_DAC_BUILT_IN),
        .sample_rate          = SAMPLE_RATE,
        .bits_per_sample      = I2S_BITS_PER_SAMPLE_16BIT,
        // RIGHT_LEFT gives both DAC channels:
        //   RIGHT → DAC1 (GPIO25), LEFT → DAC2 (GPIO26)
        .channel_format       = I2S_CHANNEL_FMT_RIGHT_LEFT,
        .communication_format = I2S_COMM_FORMAT_STAND_MSB,
        .intr_alloc_flags     = 0,
        .dma_buf_count        = I2S_DMA_BUF_COUNT,
        .dma_buf_len          = I2S_DMA_BUF_LEN,
        .use_apll             = false,
        .tx_desc_auto_clear   = true,
    };
    i2s_driver_install(I2S_NUM_0, &cfg, 0, NULL);
    i2s_set_dac_mode(I2S_DAC_CHANNEL_BOTH_EN);  // enable GPIO25 + GPIO26
    Serial.printf("I2S DAC: %d Hz, %d bufs × %d samples\n",
                  SAMPLE_RATE, I2S_DMA_BUF_COUNT, I2S_DMA_BUF_LEN);
}

// ─────────────────────────────────────────────────────────────────────────────
// FAST: dacTask — runs on Core 0, feeds DMA buffer at SAMPLE_RATE Hz
//
// Per-sample profile interpolation replaces the original handleProfileV/C()
// which ran at ~100 Hz from loop(). Now it runs at SAMPLE_RATE (5 kHz).
// ─────────────────────────────────────────────────────────────────────────────
static void dacTask(void* /*arg*/) {
    // Local DMA write buffer: stereo 16-bit samples (LEFT|RIGHT per frame)
    // Buffer size = I2S_DMA_BUF_LEN frames × 4 bytes/frame
    static uint32_t buf[I2S_DMA_BUF_LEN];

    const float dt_ms = 1000.0f / SAMPLE_RATE; // ms per sample

    // Local shadow of profile state to avoid holding the mutex per-sample
    // during a ramp. We re-read from shared state at each step boundary.
    float  lv_start = 0, lv_target = 0, lv_now = 0;
    float  lc_start = 0, lc_target = 0, lc_now = 0;
    float  lv_ramp_ms = 0,  lv_hold_ms = 0,  lv_elapsed_ms = 0;
    float  lc_ramp_ms = 0,  lc_hold_ms = 0,  lc_elapsed_ms = 0;
    bool   lv_in_ramp = false, lc_in_ramp = false;
    int    lv_step = 0, lc_step = 0;
    bool   lv_run = false, lc_run = false;
    int    lv_count = 0, lc_count = 0;

    for (;;) {
        // Sync local ramp state from shared profile state when profile
        // starts/stops — detected on any step-boundary refresh below.
        portENTER_CRITICAL(&profileMux);
        lv_run   = profileVRunning; lv_count = profileVCount;
        lc_run   = profileCRunning; lc_count = profileCCount;
        portEXIT_CRITICAL(&profileMux);

        // Fill one DMA buffer
        for (int i = 0; i < I2S_DMA_BUF_LEN; i++) {

            // ── Voltage ramp ──────────────────────────────────────────────
            if (lv_run && lv_count > 0) {
                lv_elapsed_ms += dt_ms;
                const ProfileStep& sv = profileV[lv_step];

                if (lv_in_ramp) {
                    float p = lv_elapsed_ms / lv_ramp_ms;
                    if (p >= 1.0f) { p = 1.0f; lv_in_ramp = false; lv_elapsed_ms = 0; }
                    lv_now = lv_start + (lv_target - lv_start) * p;
                } else {
                    // Holding at target
                    if (lv_elapsed_ms >= lv_hold_ms) {
                        lv_elapsed_ms = 0;
                        int next = lv_step + 1;
                        if (next >= lv_count) {
                            if (profileVLoop) {
                                next = 0;  // wrap — loop mode
                            } else {
                                // End of sequence — stop and hold last value
                                portENTER_CRITICAL(&profileMux);
                                profileVRunning = false;
                                portEXIT_CRITICAL(&profileMux);
                                lv_run = false;
                                Serial.printf("Profile V complete (held at %.2f)\r\n", lv_now);
                                goto lv_done;
                            }
                        }
                        lv_step = next;
                        dacStepTransitions++;
                        const ProfileStep& ns = profileV[lv_step];
                        lv_start  = lv_now;
                        lv_target = ns.value;
                        lv_ramp_ms = (float)ns.rampMs;
                        lv_hold_ms = (float)ns.durationMs;
                        lv_in_ramp = (lv_ramp_ms > 0.0f);
                        if (!lv_in_ramp) lv_now = lv_target;
                    }
                }
                lv_done:
                // Push value back to shared state (every buffer, not every sample)
                if (i == I2S_DMA_BUF_LEN - 1) {
                    voltageValue = lv_now;
                    dac1Value    = (uint16_t)(constrain(lv_now, 0, MAX_VOLTAGE)
                                             / MAX_VOLTAGE * DAC_MAX);
                }
            }

            // ── Current ramp ──────────────────────────────────────────────
            if (lc_run && lc_count > 0) {
                lc_elapsed_ms += dt_ms;
                const ProfileStep& sc = profileC[lc_step];

                if (lc_in_ramp) {
                    float p = lc_elapsed_ms / lc_ramp_ms;
                    if (p >= 1.0f) { p = 1.0f; lc_in_ramp = false; lc_elapsed_ms = 0; }
                    lc_now = lc_start + (lc_target - lc_start) * p;
                } else {
                    if (lc_elapsed_ms >= lc_hold_ms) {
                        lc_elapsed_ms = 0;
                        int next = lc_step + 1;
                        if (next >= lc_count) {
                            if (profileCLoop) {
                                next = 0;  // wrap — loop mode
                            } else {
                                portENTER_CRITICAL(&profileMux);
                                profileCRunning = false;
                                portEXIT_CRITICAL(&profileMux);
                                lc_run = false;
                                Serial.printf("Profile C complete (held at %.2f)\r\n", lc_now);
                                goto lc_done;
                            }
                        }
                        lc_step = next;
                        dacStepTransitions++;
                        const ProfileStep& ns = profileC[lc_step];
                        lc_start  = lc_now;
                        lc_target = ns.value;
                        lc_ramp_ms = (float)ns.rampMs;
                        lc_hold_ms = (float)ns.durationMs;
                        lc_in_ramp = (lc_ramp_ms > 0.0f);
                        if (!lc_in_ramp) lc_now = lc_target;
                    }
                }
                lc_done:
                if (i == I2S_DMA_BUF_LEN - 1) {
                    currentValue = lc_now;
                    dac2Value    = (uint16_t)(constrain(lc_now, 0, MAX_CURRENT)
                                             / MAX_CURRENT * DAC_MAX);
                }
            }

            // ── Build stereo I2S sample ───────────────────────────────────
            // DAC is 8-bit; I2S upper byte of each 16-bit half → DAC output.
            // RIGHT half (bits 15:0)  → DAC1 GPIO25 (voltage)
            // LEFT  half (bits 31:16) → DAC2 GPIO26 (current)
            uint8_t v8 = (uint8_t)(dac1Value >> 4);  // 12-bit → 8-bit
            uint8_t c8 = (uint8_t)(dac2Value >> 4);
            buf[i] = ((uint32_t)(c8) << 24)   // LEFT high byte  → DAC2
                   | ((uint32_t)(v8) <<  8);   // RIGHT high byte → DAC1
        }

        // Write buffer to I2S DMA (blocks until DMA accepts it)
        size_t written = 0;
        i2s_write(I2S_NUM_0, buf, sizeof(buf), &written, portMAX_DELAY);
        dacBufCount++;

        // Re-sync run flags and write step indices back every buffer (~12.8 ms)
        portENTER_CRITICAL(&profileMux);
        bool new_lv = profileVRunning, new_lc = profileCRunning;
        profileVStep = lv_step;   // publish current step so display can read it
        profileCStep = lc_step;
        portEXIT_CRITICAL(&profileMux);

        // Reset local interpolation state if profile just started
        if (new_lv && !lv_run) {
            lv_count = profileVCount;  // refresh before using as guard
            lv_step = 0; lv_elapsed_ms = 0; lv_in_ramp = false;
            lv_now = voltageValue;
            if (lv_count > 0) {
                lv_target  = profileV[0].value;
                lv_ramp_ms = (float)profileV[0].rampMs;
                lv_hold_ms = (float)profileV[0].durationMs;
                lv_in_ramp = (lv_ramp_ms > 0.0f);
                if (!lv_in_ramp) lv_now = lv_target;
            }
        }
        if (new_lc && !lc_run) {
            lc_count = profileCCount;  // refresh before using as guard
            lc_step = 0; lc_elapsed_ms = 0; lc_in_ramp = false;
            lc_now = currentValue;
            if (lc_count > 0) {
                lc_target  = profileC[0].value;
                lc_ramp_ms = (float)profileC[0].rampMs;
                lc_hold_ms = (float)profileC[0].durationMs;
                lc_in_ramp = (lc_ramp_ms > 0.0f);
                if (!lc_in_ramp) lc_now = lc_target;
            }
        }
        lv_run = new_lv; lv_count = profileVCount;
        lc_run = new_lc; lc_count = profileCCount;
    }
}
#endif // !USE_PWM_DAC

// ─────────────────────────────────────────────────────────────────────────────
// writeDacHardware — update dac1Value / dac2Value.
//   V2: dacTask picks up the new value on the next DMA buffer fill.
//   V3: write to LEDC PWM immediately (unchanged from original).
// ─────────────────────────────────────────────────────────────────────────────
void writeDacHardware(int channel, uint16_t value12bit) {
#if USE_PWM_DAC
    if (channel == 0) ledcWrite(DAC1_PIN, value12bit);
    else              ledcWrite(DAC2_PIN, value12bit);
#else
    // FAST: just update the shared value — dacTask sends it to I2S
    if (channel == 0) dac1Value = value12bit;
    else              dac2Value = value12bit;
#endif
}

// ─────────────────────────────────────────────────────────────────────────────
// setVoltage / setCurrent / setDAC — identical API to original
// ─────────────────────────────────────────────────────────────────────────────
void setVoltage(float volts) {
    voltageValue = constrain(volts, 0, MAX_VOLTAGE);
    dac1Value    = (uint16_t)(voltageValue / MAX_VOLTAGE * DAC_MAX);
    writeDacHardware(0, dac1Value);
}

void setCurrent(float amps) {
    currentValue = constrain(amps, 0, MAX_CURRENT);
    dac2Value    = (uint16_t)(currentValue / MAX_CURRENT * DAC_MAX);
    writeDacHardware(1, dac2Value);
}

void setDAC(int channel, int value) {
    value = constrain(value, 0, DAC_MAX);
    if (channel == 0) {
        dac1Value    = value;
        writeDacHardware(0, dac1Value);
        voltageValue = (float)dac1Value / DAC_MAX * MAX_VOLTAGE;
        Serial.printf("dac1: %d (%.1f V)\n", dac1Value, (float)voltageValue);
    } else {
        dac2Value    = value;
        writeDacHardware(1, dac2Value);
        currentValue = (float)dac2Value / DAC_MAX * MAX_CURRENT;
        Serial.printf("dac2: %d (%.1f A)\n", dac2Value, (float)currentValue);
    }
    broadcastStatus();
}

// ─────────────────────────────────────────────────────────────────────────────
// setup()
// ─────────────────────────────────────────────────────────────────────────────
void setup() {
    Serial.begin(SERIAL_BAUD);
    Serial.printf("\n%s v%s\n", PROG_NAME, PROG_VERSION);
    Serial.printf("Sample rate: %d Hz  Buffer: %d × %d samples\n",
                  SAMPLE_RATE, I2S_DMA_BUF_COUNT, I2S_DMA_BUF_LEN);

    pinMode(LED_PIN, OUTPUT);

    // GPIO outputs
    for (int i = 0; i < GPIO_OUT_COUNT; i++) {
        pinMode(GPIO_OUT_PINS[i], OUTPUT);
        digitalWrite(GPIO_OUT_PINS[i], LOW);
    }
    for (int i = 0; i < GPIO_IN_COUNT; i++) {
        pinMode(GPIO_IN_PINS[i], INPUT_PULLUP);
    }

    // DAC / PWM init
#if USE_PWM_DAC
    ledcAttach(DAC1_PIN, PWM_CARRIER_FREQ, PWM_RESOLUTION);
    ledcAttach(DAC2_PIN, PWM_CARRIER_FREQ, PWM_RESOLUTION);
    ledcWrite(DAC1_PIN, 0);
    ledcWrite(DAC2_PIN, 0);
    Serial.printf("PWM DAC: %d Hz, %d-bit (add RC filter!)\n",
                  PWM_CARRIER_FREQ, PWM_RESOLUTION);
#else
    // FAST: init I2S in DAC mode and start dacTask on Core 0
    i2s_dac_init();
    xTaskCreatePinnedToCore(
        dacTask,     // task function
        "dacTask",   // name
        4096,        // stack (bytes)
        NULL,        // arg
        2,           // priority (higher than loop = 1)
        NULL,        // handle
        0            // Core 0 (loop() runs on Core 1)
    );
    Serial.println("dacTask started on Core 0");
#endif

    // Display init
#ifdef LILYGO_TDISPLAY
    tftPtr = new(tftBuf) TFT_eSPI();
    pinMode(TFT_BL_PIN, OUTPUT); digitalWrite(TFT_BL_PIN, HIGH);
    tft.init(); tft.setRotation(1); tft.fillScreen(TFT_BLACK);
#else
    display.begin();
    display.setFont(u8g2_font_6x10_tf);
#endif
    displaySplash();

    // Load preferences — credential list + location
    preferences.begin(PREF_NAMESPACE, false);
    locationName = preferences.getString("location", "");
    wifiCredCount = preferences.getInt("wifiCount", 0);
    for (int i = 0; i < wifiCredCount && i < WIFI_MAX_CREDS; i++) {
        wifiCreds[i].ssid = preferences.getString(("wssid" + String(i)).c_str(), "");
        wifiCreds[i].pass = preferences.getString(("wpass" + String(i)).c_str(), "");
    }
    // Seed defaults if nothing saved yet (also migrates old single-cred key)
    if (wifiCredCount == 0) {
        String s = preferences.getString("ssid", "");
        String p = preferences.getString("pass", "");
        if (s.length() > 0) {
            wifiCreds[wifiCredCount++] = {s, p};           // migrated old entry
        } else {
            if (strlen(WIFI_SSID_0) > 0) wifiCreds[wifiCredCount++] = {WIFI_SSID_0, WIFI_PASS_0};
            if (strlen(WIFI_SSID_1) > 0) wifiCreds[wifiCredCount++] = {WIFI_SSID_1, WIFI_PASS_1};
        }
    }
    preferences.end();
    loadProfileFromPrefs();   // restore last loaded sequence

    // WiFi — try each credential in order, stop at first success
    if (wifiCredCount > 0) {
        for (int i = 0; i < wifiCredCount && !wifiConnected; i++) {
            if (wifiCreds[i].ssid.length() == 0) continue;
            Serial.printf("Trying WiFi %d/%d: %s\n", i+1, wifiCredCount, wifiCreds[i].ssid.c_str());
            WiFi.begin(wifiCreds[i].ssid.c_str(), wifiCreds[i].pass.c_str());
            unsigned long t0 = millis();
            while (WiFi.status() != WL_CONNECTED && millis() - t0 < 8000) {
                delay(500); Serial.print(".");
            }
            if (WiFi.status() == WL_CONNECTED) {
                wifiConnected   = true;
                wifiCredActive  = i;
                wifiSSID        = wifiCreds[i].ssid;
                wifiPass        = wifiCreds[i].pass;
                Serial.printf("\nWiFi: %s  IP: %s\n", wifiSSID.c_str(),
                              WiFi.localIP().toString().c_str());
                setupOTA();
                webSocket.begin();
                webSocket.onEvent(webSocketEvent);
                wsServerStarted = true;
            } else {
                Serial.printf("\n  %s failed\n", wifiCreds[i].ssid.c_str());
                WiFi.disconnect(true);
                delay(200);
            }
        }
        if (!wifiConnected) Serial.println("All WiFi credentials failed — running offline");
    } else {
        Serial.println("No WiFi credentials. Use: wifi add <ssid> <pass>");
    }

    updateDisplay();
    Serial.println("Ready.");
}

// ─────────────────────────────────────────────────────────────────────────────
// loop() — WiFi / WebSocket / serial / display / LED / GPIO / CAN
//
// FAST: profile execution removed from loop() — it runs in dacTask at 5 kHz.
//       delay(10) replaced with delay(1) so WebSocket stays responsive.
// ─────────────────────────────────────────────────────────────────────────────
void loop() {
    if (otaInProgress) { delay(10); return; }

    handleSerial();

    // ── WiFi watchdog / auto-reconnect ────────────────────────────────────────
    if (wifiCredCount > 0) {
        bool wifiUp = (WiFi.status() == WL_CONNECTED);
        if (wifiConnected && !wifiUp) {
            wifiConnected  = false;
            wsServerStarted = false;
            Serial.println("WiFi lost — retry in 10 s");
        }
        static unsigned long lastReconnectMs = 0;
        static int reconnectIdx = 0;
        if (!wifiConnected && !wifiUp && millis() - lastReconnectMs > 10000) {
            lastReconnectMs = millis();
            // Rotate through credential list on each reconnect attempt
            reconnectIdx = reconnectIdx % wifiCredCount;
            Serial.printf("Reconnecting to %s...\n", wifiCreds[reconnectIdx].ssid.c_str());
            WiFi.begin(wifiCreds[reconnectIdx].ssid.c_str(), wifiCreds[reconnectIdx].pass.c_str());
            reconnectIdx++;
        }
        if (!wifiConnected && wifiUp) {
            wifiConnected = true;
            Serial.printf("WiFi up: %s\n", WiFi.localIP().toString().c_str());
            if (!wsServerStarted) {
                setupOTA();
                webSocket.begin();
                webSocket.onEvent(webSocketEvent);
                wsServerStarted = true;
                Serial.println("WS + OTA restarted");
            }
        }
    }
    // ─────────────────────────────────────────────────────────────────────────

    if (wifiConnected && wsServerStarted) {
        webSocket.loop();
        ArduinoOTA.handle();
    }

    handleLED();
    pollCAN();

    if (millis() - lastDisplayUpdate > DISPLAY_UPDATE_MS) {
        updateGpioInputs();
        updateDisplay();
        broadcastStatus();
        lastDisplayUpdate = millis();
    }

    // FAST: delay(10) → delay(1).
    // Profile ramp is in dacTask; loop() only needs to stay responsive to WS.
    delay(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Profile start/stop helpers — identical to original but wrap profileMux
// ─────────────────────────────────────────────────────────────────────────────
void startProfile(bool voltage, bool current) {
    portENTER_CRITICAL(&profileMux);
    if (voltage && profileVCount > 0) {
        profileVRunning    = true;
        profileVStep       = 0;
        profileVStepStart  = millis();
        profileVStepStartVal = voltageValue;
        profileVInRamp     = (profileV[0].rampMs > 0);
        if (!profileVInRamp) setVoltage(profileV[0].value);
        Serial.println("Profile V started");
    }
    if (current && profileCCount > 0) {
        profileCRunning    = true;
        profileCStep       = 0;
        profileCStepStart  = millis();
        profileCStepStartVal = currentValue;
        profileCInRamp     = (profileC[0].rampMs > 0);
        if (!profileCInRamp) setCurrent(profileC[0].value);
        Serial.println("Profile C started");
    }
    portEXIT_CRITICAL(&profileMux);
}

void stopProfile(bool voltage, bool current) {
    portENTER_CRITICAL(&profileMux);
    if (voltage) { profileVRunning = false; Serial.println("Profile V stopped"); }
    if (current) { profileCRunning = false; Serial.println("Profile C stopped"); }
    portEXIT_CRITICAL(&profileMux);
}

// ─────────────────────────────────────────────────────────────────────────────
// Analog inputs
// ─────────────────────────────────────────────────────────────────────────────
uint16_t readAnalogInput(int ch)   { return analogRead(ch == 0 ? ADC1_PIN : ADC2_PIN); }
float    readAnalogVoltage(int ch) { return readAnalogInput(ch) * 3.3f / 4095.0f; }

// ─────────────────────────────────────────────────────────────────────────────
// GPIO helpers
// ─────────────────────────────────────────────────────────────────────────────
void setGpioOutput(int idx, bool state) {
    if (idx < 0 || idx >= GPIO_OUT_COUNT) return;
    gpioOutState[idx] = state ? 1 : 0;
    digitalWrite(GPIO_OUT_PINS[idx], state ? HIGH : LOW);
}

void updateGpioInputs() {
    for (int i = 0; i < GPIO_IN_COUNT; i++)
        gpioInState[i] = digitalRead(GPIO_IN_PINS[i]);
}

// ─────────────────────────────────────────────────────────────────────────────
// LED
// ─────────────────────────────────────────────────────────────────────────────
void handleLED() {
    if (ledMode == LED_OFF) { digitalWrite(LED_PIN, LOW);  return; }
    if (ledMode == LED_ON)  { digitalWrite(LED_PIN, HIGH); return; }
    if (millis() - lastLedToggle >= 500) {
        ledState = !ledState;
        digitalWrite(LED_PIN, ledState);
        lastLedToggle = millis();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OTA
// ─────────────────────────────────────────────────────────────────────────────
void setupOTA() {
    ArduinoOTA.setHostname(OTA_HOSTNAME);
    ArduinoOTA.setPassword(OTA_PASSWORD);
    ArduinoOTA.onStart([](){ otaInProgress = true; Serial.println("OTA start"); });
    ArduinoOTA.onEnd([](){   otaInProgress = false; Serial.println("\nOTA done"); });
    ArduinoOTA.onProgress([](unsigned int p, unsigned int t){
        otaProgress = (int)((float)p / t * 100);
        if (otaProgress % 10 == 0) Serial.printf("OTA: %d%%\n", otaProgress);
    });
    ArduinoOTA.onError([](ota_error_t e){ Serial.printf("OTA err %u\n", e); otaInProgress = false; });
    ArduinoOTA.begin();
}

// ─────────────────────────────────────────────────────────────────────────────
// CAN helpers (stub — full implementation in load_controller_lora32.ino)
// ─────────────────────────────────────────────────────────────────────────────
void pollCAN() {
    if (!canEnabled || !canInstalled) return;
    twai_message_t msg;
    while (twai_receive(&msg, 0) == ESP_OK) {
        canRxCount++;
        CanMessage& m = canRxBuffer[canRxHead];
        m.id        = msg.identifier;
        m.len       = msg.data_length_code;
        m.extended  = msg.extd;
        m.timestamp = millis();
        memcpy(m.data, msg.data, m.len);
        canRxHead = (canRxHead + 1) % CAN_RX_BUFFER_SIZE;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// broadcastStatus — send JSON status to all WS clients
// ─────────────────────────────────────────────────────────────────────────────
void broadcastStatus() {
    if (!wsServerStarted) return;

    // ── FAST: compute per-second rates from counters ──────────────────────────
    static uint32_t lastBufCount  = 0;
    static uint32_t lastStepCount = 0;
    static unsigned long lastRateMs = 0;
    static uint32_t bufsPerSec  = 0;
    static uint32_t stepsPerSec = 0;

    unsigned long now = millis();
    unsigned long elapsed = now - lastRateMs;
    if (elapsed >= 1000) {
        uint32_t b = dacBufCount,  s = dacStepTransitions;
        bufsPerSec  = (uint32_t)((b - lastBufCount)  * 1000UL / elapsed);
        stepsPerSec = (uint32_t)((s - lastStepCount) * 1000UL / elapsed);
        lastBufCount  = b;
        lastStepCount = s;
        lastRateMs    = now;
    }
    // ─────────────────────────────────────────────────────────────────────────

    // Snapshot step state (approximate read — display only, mutex not required)
    int   vStep  = profileVStep;
    int   cStep  = profileCStep;
    const char* vName = (profileVRunning && profileVCount > 0 && vStep < profileVCount)
                        ? profileV[vStep].name.c_str() : "";
    const char* cName = (profileCRunning && profileCCount > 0 && cStep < profileCCount)
                        ? profileC[cStep].name.c_str() : "";

    StaticJsonDocument<768> doc;
    doc["type"]        = "status";
    doc["voltage"]     = roundf(voltageValue * 10) / 10.0f;
    doc["current"]     = roundf(currentValue * 10) / 10.0f;
    doc["dac1"]        = (int)dac1Value;
    doc["dac2"]        = (int)dac2Value;
    doc["profileV"]    = profileVRunning;
    doc["profileC"]    = profileCRunning;
    doc["vStep"]       = vStep;
    doc["vStepCount"]  = profileVCount;
    doc["vStepName"]   = vName;
    doc["cStep"]       = cStep;
    doc["cStepCount"]  = profileCCount;
    doc["cStepName"]   = cName;
    doc["sampleRate"]  = SAMPLE_RATE;
    doc["bufsPerSec"]  = bufsPerSec;
    doc["stepsPerSec"] = stepsPerSec;
    doc["location"]    = locationName;
    char buf[768];
    serializeJson(doc, buf, sizeof(buf));
    webSocket.broadcastTXT(buf);
    wsTxCount++;
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket event handler
// ─────────────────────────────────────────────────────────────────────────────
void webSocketEvent(uint8_t num, WStype_t type, uint8_t* payload, size_t length) {
    switch (type) {
        case WStype_CONNECTED:
            wsClientCount++;
            Serial.printf("WS[%d] connected\n", num);
            broadcastStatus();
            break;
        case WStype_DISCONNECTED:
            if (wsClientCount > 0) wsClientCount--;
            Serial.printf("WS[%d] disconnected\n", num);
            break;
        case WStype_TEXT: {
            wsMessageCount++;
            Serial.printf("WS rx: %u bytes\r\n", (unsigned)length);
            // load_profile with 100 steps × 2 channels needs up to ~14 KB in ArduinoJson.
            // DynamicJsonDocument allocates on the heap, not the stack.
            DynamicJsonDocument doc(16384);
            DeserializationError err = deserializeJson(doc, payload, length);
            if (err) {
                Serial.printf("JSON parse error: %s\r\n", err.c_str());
                return;
            }
            handleWsCommand(num, doc);
            break;
        }
        default: break;
    }
}

void handleWsCommand(uint8_t num, JsonDocument& doc) {
    const char* cmd = doc["cmd"] | "";

    if      (strcmp(cmd, "set_voltage") == 0) { setVoltage(doc["value"]); broadcastStatus(); }
    else if (strcmp(cmd, "set_current") == 0) { setCurrent(doc["value"]); broadcastStatus(); }
    else if (strcmp(cmd, "set_dac")     == 0) { setDAC(doc["channel"] | 0, doc["value"] | 0); }
    else if (strcmp(cmd, "set_led")     == 0) {
        const char* s = doc["state"] | "off";
        ledMode = strcmp(s,"on")==0 ? LED_ON : strcmp(s,"blink")==0 ? LED_BLINK : LED_OFF;
    }
    else if (strcmp(cmd, "set_gpio")    == 0) {
        setGpioOutput(doc["pin"] | 0, doc["state"] | false);
        broadcastStatus();
    }
    else if (strcmp(cmd, "get_status")  == 0) { broadcastStatus(); }
    else if (strcmp(cmd, "load_profile") == 0) {
        loadProfileFromJson(doc);
    }
    else if (strcmp(cmd, "start_profile") == 0) {
        const char* ch = doc["channel"] | "both";
        startProfile(strcmp(ch,"current")!=0, strcmp(ch,"voltage")!=0);
        saveProfileToPrefs();   // persist running state for auto-restart after reset
        broadcastStatus();
    }
    else if (strcmp(cmd, "stop_profile") == 0) {
        const char* ch = doc["channel"] | "both";
        stopProfile(strcmp(ch,"current")!=0, strcmp(ch,"voltage")!=0);
        saveProfileToPrefs();   // persist stopped state
        broadcastStatus();
    }
    else if (strcmp(cmd, "get_version") == 0) {
        StaticJsonDocument<128> r;
        r["type"]    = "version";
        r["version"] = PROG_VERSION;
        r["name"]    = PROG_NAME;
        char buf[128]; serializeJson(r, buf, sizeof(buf));
        webSocket.sendTXT(num, buf);
    }
    else if (strcmp(cmd, "wifi_list") == 0) {
        StaticJsonDocument<512> r;
        r["type"]   = "wifi_list";
        r["active"] = wifiCredActive;
        JsonArray arr = r.createNestedArray("creds");
        for (int i = 0; i < wifiCredCount; i++) {
            JsonObject o = arr.createNestedObject();
            o["idx"]  = i;
            o["ssid"] = wifiCreds[i].ssid;
            // password intentionally omitted from response
        }
        char buf[512]; serializeJson(r, buf, sizeof(buf));
        webSocket.sendTXT(num, buf);
    }
    else if (strcmp(cmd, "wifi_add") == 0) {
        if (wifiCredCount < WIFI_MAX_CREDS) {
            wifiCreds[wifiCredCount].ssid = doc["ssid"] | "";
            wifiCreds[wifiCredCount].pass = doc["pass"] | "";
            if (wifiCreds[wifiCredCount].ssid.length() > 0) {
                wifiCredCount++;
                saveWifiCreds();
                webSocket.sendTXT(num, "{\"type\":\"ok\",\"msg\":\"wifi added\"}");
            }
        } else {
            webSocket.sendTXT(num, "{\"type\":\"error\",\"msg\":\"list full (max 4)\"}");
        }
    }
    else if (strcmp(cmd, "wifi_remove") == 0) {
        int idx = doc["idx"] | -1;
        if (idx >= 0 && idx < wifiCredCount) {
            for (int i = idx; i < wifiCredCount - 1; i++) wifiCreds[i] = wifiCreds[i+1];
            wifiCredCount--;
            if (wifiCredActive == idx) wifiCredActive = -1;
            else if (wifiCredActive > idx) wifiCredActive--;
            saveWifiCreds();
            webSocket.sendTXT(num, "{\"type\":\"ok\",\"msg\":\"wifi removed\"}");
        }
    }
    else if (strcmp(cmd, "set_location") == 0) {
        locationName = doc["value"] | "";
        preferences.begin(PREF_NAMESPACE, false);
        preferences.putString("location", locationName);
        preferences.end();
        broadcastStatus();
    }
}

void loadProfileFromJson(JsonDocument& doc) {
    Serial.printf("loadProfile: hasV=%d hasC=%d docMem=%u\r\n",
        doc.containsKey("voltage"), doc.containsKey("current"),
        (unsigned)doc.memoryUsage());

    bool doLoop = doc["loop"] | false;

    portENTER_CRITICAL(&profileMux);
    profileVRunning = false;
    profileCRunning = false;
    profileVLoop = doLoop;
    profileCLoop = doLoop;

    // Voltage steps
    JsonArray va = doc["voltage"];
    profileVCount = 0;
    if (va) {
        for (JsonObject step : va) {
            if (profileVCount >= MAX_PROFILE_STEPS) break;
            profileV[profileVCount].name       = step["name"] | "";
            profileV[profileVCount].value      = step["value"] | 0.0f;
            profileV[profileVCount].durationMs = step["durationMs"] | 1000;
            profileV[profileVCount].rampMs     = step["rampMs"]     | 0;
            profileVCount++;
        }
    }

    // Current steps
    JsonArray ca = doc["current"];
    profileCCount = 0;
    if (ca) {
        for (JsonObject step : ca) {
            if (profileCCount >= MAX_PROFILE_STEPS) break;
            profileC[profileCCount].name       = step["name"] | "";
            profileC[profileCCount].value      = step["value"] | 0.0f;
            profileC[profileCCount].durationMs = step["durationMs"] | 1000;
            profileC[profileCCount].rampMs     = step["rampMs"]     | 0;
            profileCCount++;
        }
    }
    portEXIT_CRITICAL(&profileMux);

    saveProfileToPrefs();   // persist so sequence survives restart
    Serial.printf("Profile loaded: %d V steps, %d A steps\r\n", profileVCount, profileCCount);
    for (int i = 0; i < profileVCount; i++)
        Serial.printf("  V[%d] name=%-16s val=%6.2f dur=%5lu ramp=%5lu\r\n",
            i, profileV[i].name.c_str(), profileV[i].value,
            profileV[i].durationMs, profileV[i].rampMs);
    for (int i = 0; i < profileCCount; i++)
        Serial.printf("  C[%d] name=%-16s val=%6.2f dur=%5lu ramp=%5lu\r\n",
            i, profileC[i].name.c_str(), profileC[i].value,
            profileC[i].durationMs, profileC[i].rampMs);
    broadcastStatus();
}

// ─────────────────────────────────────────────────────────────────────────────
// Serial command handler (same commands as original)
// ─────────────────────────────────────────────────────────────────────────────
void handleSerial() {
    if (!Serial.available()) return;
    String line = Serial.readStringUntil('\n');
    line.trim();
    if (line.length() == 0) return;

    if (line.startsWith("wifi ")) {
        String args = line.substring(5);
        if (args.startsWith("add ")) {
            // wifi add <ssid> <pass>
            String rest = args.substring(4);
            int sp = rest.indexOf(' ');
            if (sp > 0 && wifiCredCount < WIFI_MAX_CREDS) {
                wifiCreds[wifiCredCount].ssid = rest.substring(0, sp);
                wifiCreds[wifiCredCount].pass = rest.substring(sp + 1);
                wifiCredCount++;
                saveWifiCreds();
                Serial.printf("Added [%d] %s\n", wifiCredCount-1, wifiCreds[wifiCredCount-1].ssid.c_str());
            } else if (wifiCredCount >= WIFI_MAX_CREDS) {
                Serial.println("List full (max 4). Use wifi remove <idx> first.");
            } else {
                Serial.println("Usage: wifi add <ssid> <pass>");
            }
        } else if (args.startsWith("remove ")) {
            int idx = args.substring(7).toInt();
            if (idx >= 0 && idx < wifiCredCount) {
                Serial.printf("Removed [%d] %s\n", idx, wifiCreds[idx].ssid.c_str());
                for (int i = idx; i < wifiCredCount - 1; i++) wifiCreds[i] = wifiCreds[i+1];
                wifiCredCount--;
                if (wifiCredActive == idx) wifiCredActive = -1;
                else if (wifiCredActive > idx) wifiCredActive--;
                saveWifiCreds();
            } else { Serial.println("Invalid index"); }
        } else if (args == "list") {
            Serial.printf("WiFi credentials (%d/%d):\n", wifiCredCount, WIFI_MAX_CREDS);
            for (int i = 0; i < wifiCredCount; i++)
                Serial.printf("  [%d] %s%s\n", i, wifiCreds[i].ssid.c_str(),
                              i == wifiCredActive ? " <active>" : "");
        } else if (args == "status") {
            Serial.printf("WiFi: %s  SSID: %s  IP: %s\n",
                wifiConnected ? "CONNECTED" : "OFFLINE",
                wifiConnected ? wifiSSID.c_str() : "-",
                wifiConnected ? WiFi.localIP().toString().c_str() : "N/A");
        } else if (args == "clear") {
            wifiCredCount = 0; wifiCredActive = -1;
            saveWifiCreds();
            Serial.println("All WiFi credentials cleared.");
        } else {
            Serial.println("wifi add <ssid> <pass> | wifi remove <idx> | wifi list | wifi status | wifi clear");
        }
    }
    else if (line.startsWith("set ")) {
        String args = line.substring(4);
        if (args.startsWith("voltage "))     { setVoltage(args.substring(8).toFloat()); broadcastStatus(); }
        else if (args.startsWith("current ")) { setCurrent(args.substring(8).toFloat()); broadcastStatus(); }
        else if (args.startsWith("dac1 "))    { setDAC(0, args.substring(5).toInt()); }
        else if (args.startsWith("dac2 "))    { setDAC(1, args.substring(5).toInt()); }
        else if (args.startsWith("led ")) {
            String s = args.substring(4);
            ledMode = s=="on" ? LED_ON : s=="blink" ? LED_BLINK : LED_OFF;
        }
    }
    else if (line.startsWith("profile ")) {
        String args = line.substring(8);
        if (args == "start") { startProfile(true, true); saveProfileToPrefs(); }
        else if (args == "stop") { stopProfile(true, true); saveProfileToPrefs(); }
    }
    else if (line == "version") {
        Serial.printf("%s v%s\n", PROG_NAME, PROG_VERSION);
    }
    else if (line == "status") {
        Serial.printf("V: %.2f V  I: %.2f A  dac1: %d  dac2: %d\n",
                      (float)voltageValue, (float)currentValue,
                      (int)dac1Value, (int)dac2Value);
        Serial.printf("ProfileV: %s  ProfileC: %s\n",
                      profileVRunning ? "running" : "stopped",
                      profileCRunning ? "running" : "stopped");
        Serial.printf("WiFi: %s  SSID: %s  IP: %s  WS clients: %d\n",
                      wifiConnected ? "UP" : "DOWN",
                      wifiConnected ? wifiSSID.c_str() : "-",
                      wifiConnected ? WiFi.localIP().toString().c_str() : "N/A",
                      wsClientCount);
        Serial.printf("Sample rate: %d Hz\n", SAMPLE_RATE);
    }
    else if (line == "help") {
        Serial.println("Commands:");
        Serial.println("  wifi add <ssid> <pass>   wifi remove <idx>   wifi list");
        Serial.println("  wifi status              wifi clear");
        Serial.println("  set voltage <0-60>       set current <0-200>");
        Serial.println("  set dac1 <0-4095>        set dac2 <0-4095>   set led <on|off|blink>");
        Serial.println("  profile start            profile stop");
        Serial.println("  version                  status");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Display (unchanged from original — board-specific via #ifdef)
// ─────────────────────────────────────────────────────────────────────────────
void displaySplash() {
#ifdef LILYGO_TDISPLAY
    tft.setTextFont(4); tft.setTextColor(TFT_CYAN, TFT_BLACK);
    tft.setCursor(30, 30); tft.print(PROG_NAME);
    tft.setTextFont(2); tft.setTextColor(TFT_WHITE, TFT_BLACK);
    tft.setCursor(60, 80); tft.print("v"); tft.print(PROG_VERSION);
    delay(1200);
#else
    display.clearBuffer();
    display.setFont(u8g2_font_helvB12_tf);
    display.drawStr(10, 20, PROG_NAME);
    display.setFont(u8g2_font_6x10_tf);
    display.drawStr(10, 35, "v"); display.drawStr(18, 35, PROG_VERSION);
    display.drawStr(10, 50, "Starting...");
    display.sendBuffer();
    delay(1200);
#endif
}

void updateDisplay() {
    // Snapshot profile state under mutex — dacTask updates step indices on Core 0
    int  snapVStep, snapCStep, snapVCount, snapCCount;
    bool snapVRun,  snapCRun;
    portENTER_CRITICAL(&profileMux);
    snapVStep  = profileVStep;   snapCStep  = profileCStep;
    snapVCount = profileVCount;  snapCCount = profileCCount;
    snapVRun   = profileVRunning; snapCRun  = profileCRunning;
    portEXIT_CRITICAL(&profileMux);

    char buf[32];
#ifdef LILYGO_TDISPLAY
    tft.fillScreen(TFT_BLACK);
    tft.setTextFont(2); tft.setTextColor(TFT_WHITE, TFT_BLACK);
    snprintf(buf, sizeof(buf), "V: %.1f V", (float)voltageValue);
    tft.setCursor(4, 10); tft.print(buf);
    snprintf(buf, sizeof(buf), "I: %.1f A", (float)currentValue);
    tft.setCursor(4, 30); tft.print(buf);
    if (wifiConnected) { tft.setCursor(4, 50); tft.print(WiFi.localIP().toString()); }
    if (snapVRun || snapCRun) {
        int step  = snapVRun ? snapVStep  : snapCStep;
        int count = snapVRun ? snapVCount : snapCCount;
        snprintf(buf, sizeof(buf), "step %d/%d", step + 1, count);
        tft.setCursor(4, 50); tft.setTextColor(TFT_GREEN, TFT_BLACK); tft.print(buf);
    }
    snprintf(buf, sizeof(buf), "%d Hz", SAMPLE_RATE);
    tft.setCursor(4, 70); tft.setTextColor(TFT_CYAN, TFT_BLACK); tft.print(buf);
#else
    display.clearBuffer();

    // Row 1: V / I values
    display.setFont(u8g2_font_helvB08_tf);
    snprintf(buf, sizeof(buf), "V:%.1f I:%.1f", (float)voltageValue, (float)currentValue);
    display.drawStr(0, 12, buf);

    // Row 2: IP or step name — step index from mutex snapshot, not direct read
    display.setFont(u8g2_font_6x10_tf);
    if (snapVRun || snapCRun) {
        int   activeStep  = snapVRun ? snapVStep  : snapCStep;
        int   activeCount = snapVRun ? snapVCount : snapCCount;
        // Name is read-only after load_profile so no lock needed, but bounds-check
        String activeName = "";
        if (snapVRun && activeStep < profileVCount)
            activeName = profileV[activeStep].name;
        else if (snapCRun && activeStep < profileCCount)
            activeName = profileC[activeStep].name;
        char stepBuf[22];
        snprintf(stepBuf, sizeof(stepBuf), "%d/%d %s",
                 activeStep + 1, activeCount, activeName.c_str());
        display.drawStr(0, 26, stepBuf);
    } else if (wifiConnected) {
        display.drawStr(0, 26, WiFi.localIP().toString().c_str());
    } else {
        display.drawStr(0, 26, "No WiFi");
    }

    // Row 3: Load bar
    float val = (displayChannel == CHANNEL_VOLTAGE)
              ? voltageValue / MAX_VOLTAGE
              : currentValue / MAX_CURRENT;
    int barW = (int)(val * 120);
    display.drawFrame(0, 34, 122, 10);
    if (barW > 0) display.drawBox(1, 35, barW, 8);

    // Row 4: sample rate + running indicator
    display.setFont(u8g2_font_5x7_tf);
    snprintf(buf, sizeof(buf), "%d Hz", SAMPLE_RATE);
    display.drawStr(0, 58, buf);
    if (snapVRun || snapCRun)
        display.drawStr(90, 58, "RUNNING");

    display.sendBuffer();
#endif
}
