/*
 * Chiller Controller for Heltec LoRa32 V2/V3 + LILYGO T-Display V1.1
 * Version: 1.1.0
 *
 * Simulates / controls a liquid cooling chiller unit.
 * DAC1 (GPIO25) → compressor speed reference (0-3.3V = 0-100%)
 * DAC2 (GPIO26) → fan speed reference (0-3.3V = 0-100%)
 * GPIO OUT[0]   → pump relay
 * GPIO OUT[1]   → compressor contactor
 * ADC1 (GPIO36) → inlet temp 4-20mA (optional)
 * ADC2 (GPIO39) → outlet temp 4-20mA (optional)
 *
 * WebSocket Commands:
 *   {"cmd":"get_status"}
 *   {"cmd":"set_setpoint","value":18.0}        // outlet temp °C
 *   {"cmd":"set_compressor","value":75.0}      // % (overrides auto)
 *   {"cmd":"set_fan","value":60.0}             // %
 *   {"cmd":"set_mode","mode":"cooling"}        // cooling|standby|manual
 *   {"cmd":"set_pump","state":true}
 *   {"cmd":"clear_fault"}
 *   {"cmd":"ping","t":N}
 */

// ─────────────────────────────────────────────────────────
// Board auto-detection (must be before includes)
// ─────────────────────────────────────────────────────────
#if defined(ARDUINO_HELTEC_WIFI_LORA_32_V2)
  #define HELTEC_V2
#elif defined(ARDUINO_HELTEC_WIFI_LORA_32_V3) || defined(ARDUINO_HELTEC_WIFI_LORA_32_V4)
  #define HELTEC_V3
#elif defined(ARDUINO_TTGO_T1)
  #define LILYGO_TDISPLAY
#else
  #warning "Board not auto-detected — defaulting to V2 pin layout"
  #define HELTEC_V2
#endif

// TFT_eSPI inline config for LILYGO T-Display V1.1 (must precede #include <TFT_eSPI.h>)
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

#define PROG_NAME     "Chiller"
#define PROG_VERSION  "1.1.0"

// ─────────────────────────────────────────────────────────
// Board-specific pin definitions
// ─────────────────────────────────────────────────────────
#ifdef HELTEC_V3
  // ESP32-S3 — no native DAC, use PWM + RC filter (10kΩ + 10µF)
  #define LED_PIN        35
  #define OLED_SDA       17
  #define OLED_SCL       18
  #define OLED_RST       21
  #define DAC1_PIN        1  // PWM — compressor speed ref
  #define DAC2_PIN        2  // PWM — fan speed ref
  #define ADC1_PIN        4  // Inlet temp sense
  #define ADC2_PIN        5  // Outlet temp sense
  #define GPIO_OUT_COUNT  2
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {12, 13};  // pump, compressor
  #define USE_PWM_DAC     1
  #warning "Heltec V3/V4: no native DAC — add RC filter on DAC1/DAC2 pins"
#elif defined(LILYGO_TDISPLAY)
  // LILYGO T-Display V1.1 — ESP32, true DAC, ST7789V 135×240 color TFT
  #define LED_PIN         2
  #define TFT_BL_PIN      4
  #define DAC1_PIN       25  // True DAC — compressor speed ref
  #define DAC2_PIN       26  // True DAC — fan speed ref
  #define ADC1_PIN       36  // Inlet temp sense
  #define ADC2_PIN       39  // Outlet temp sense
  #define GPIO_OUT_COUNT  2
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {32, 33};  // pump, compressor
  #define USE_PWM_DAC     0
#else
  // ESP32 V2 — true 8-bit DAC
  #define LED_PIN        2
  #define OLED_SDA       4
  #define OLED_SCL       15
  #define OLED_RST       16
  #define DAC1_PIN       25  // Compressor speed
  #define DAC2_PIN       26  // Fan speed
  #define ADC1_PIN       36  // Inlet temp sense
  #define ADC2_PIN       39  // Outlet temp sense
  #define GPIO_OUT_COUNT  2
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {17, 23};  // pump, compressor
  #define USE_PWM_DAC     0
#endif

// Config
#define WS_PORT       81
#define SERIAL_BAUD   115200
#define OTA_HOSTNAME  "chiller-ctrl"
#define OTA_PASSWORD  "bms2026"
#define PREF_NS       "chiller"

// Physics limits
#define MIN_TEMP      -10.0f
#define MAX_TEMP       50.0f
#define AMBIENT_TEMP   28.0f    // Simulated ambient

#ifdef LILYGO_TDISPLAY
  TFT_eSPI tft = TFT_eSPI();
#else
  U8G2_SSD1306_128X64_NONAME_F_HW_I2C display(U8G2_R0, OLED_RST, OLED_SCL, OLED_SDA);
#endif

Preferences preferences;
WebSocketsServer webSocket(WS_PORT);

// WiFi
String wifiSSID, wifiPass;
bool wifiConnected = false;
bool wsServerStarted = false;
bool otaInProgress = false;

// Chiller state
enum ChillerMode { MODE_STANDBY, MODE_COOLING, MODE_MANUAL, MODE_FAULT };
ChillerMode chillerMode = MODE_STANDBY;

float tempSetpoint  = 18.0f;   // Target outlet temp °C
float tempInlet     = AMBIENT_TEMP;
float tempOutlet    = AMBIENT_TEMP;
float compressorPct = 0.0f;    // 0–100 %
float fanPct        = 0.0f;    // 0–100 %
float flowRateLpm   = 0.0f;    // L/min
float pressureBar   = 0.0f;    // refrigerant pressure bar
bool  pumpOn        = false;
bool  compressorOn  = false;

// Alarm bits
bool alarmHighTemp    = false;
bool alarmLowFlow     = false;
bool alarmCompFault   = false;
uint32_t alarmBitmap  = 0;

// LED
enum LedMode { LED_OFF, LED_ON, LED_BLINK };
LedMode ledMode = LED_OFF;
unsigned long lastLedToggle = 0;
bool ledState = false;

// Counters
uint32_t wsRxCount = 0, wsTxCount = 0;
uint8_t  wsClients = 0;
unsigned long lastUpdate = 0;
unsigned long lastSimStep = 0;
String serialBuf = "";

// Location name (persistent, set from HTML or serial)
String locationName = "";

// ─────────────────────────────────────────────────────────
// WiFi / OTA / WebSocket
// ─────────────────────────────────────────────────────────
void connectWiFi() {
#ifndef LILYGO_TDISPLAY
    display.clearBuffer();
    display.setFont(u8g2_font_6x10_tf);
    display.drawStr(0, 20, "Connecting WiFi...");
    display.drawStr(0, 35, wifiSSID.c_str());
    display.sendBuffer();
#endif
    WiFi.mode(WIFI_STA);
    WiFi.begin(wifiSSID.c_str(), wifiPass.c_str());
    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts++ < 30) {
        delay(500);
        Serial.print(".");
    }
    if (WiFi.status() == WL_CONNECTED) {
        wifiConnected = true;
        Serial.printf("\nConnected: %s\n", WiFi.localIP().toString().c_str());
        ArduinoOTA.setHostname(OTA_HOSTNAME);
        ArduinoOTA.setPassword(OTA_PASSWORD);
        ArduinoOTA.onStart([]() { otaInProgress = true; });
        ArduinoOTA.onEnd([]() { otaInProgress = false; });
        ArduinoOTA.onProgress([](unsigned int p, unsigned int t) {
            Serial.printf("OTA: %u%%\r", p * 100 / t);
        });
        ArduinoOTA.begin();
        webSocket.begin();
        webSocket.onEvent(wsEvent);
        wsServerStarted = true;
        ledMode = LED_BLINK;
    } else {
        Serial.println("\nWiFi failed");
        ledMode = LED_ON;
    }
}

// ─────────────────────────────────────────────────────────
// DAC output helpers
// ─────────────────────────────────────────────────────────
void dacOut(uint8_t pin, uint8_t val8) {
#if USE_PWM_DAC
    ledcWrite(pin, (uint32_t)val8 * 16);  // 8-bit → 12-bit (0-4095)
#else
    dacWrite(pin, val8);
#endif
}

void setCompressor(float pct) {
    compressorPct = constrain(pct, 0, 100);
    dacOut(DAC1_PIN, (uint8_t)(compressorPct / 100.0f * 255));
    compressorOn = compressorPct > 5.0f;
    digitalWrite(GPIO_OUT_PINS[1], compressorOn ? HIGH : LOW);
}

void setFan(float pct) {
    fanPct = constrain(pct, 0, 100);
    dacOut(DAC2_PIN, (uint8_t)(fanPct / 100.0f * 255));
}

void setPump(bool on) {
    pumpOn = on;
    digitalWrite(GPIO_OUT_PINS[0], on ? HIGH : LOW);
    flowRateLpm = on ? (8.0f + (float)random(-5, 5) * 0.1f) : 0.0f;
}

// ─────────────────────────────────────────────────────────
// Simulation step — called every 500ms
// ─────────────────────────────────────────────────────────
void simStep() {
    tempInlet += (AMBIENT_TEMP - tempInlet) * 0.02f + ((float)random(-10, 10) * 0.01f);

    if (compressorOn && pumpOn) {
        float coolingPower = compressorPct / 100.0f * 15.0f;
        float target = tempInlet - coolingPower;
        tempOutlet += (target - tempOutlet) * 0.05f + ((float)random(-5, 5) * 0.01f);
        pressureBar = 8.0f + compressorPct / 100.0f * 12.0f + (float)random(-2, 2) * 0.05f;
        flowRateLpm = 8.0f + (float)random(-3, 3) * 0.1f;
    } else {
        tempOutlet += (tempInlet - tempOutlet) * 0.03f + ((float)random(-3, 3) * 0.01f);
        pressureBar = pumpOn ? (2.0f + (float)random(-1, 1) * 0.05f) : 0.0f;
        flowRateLpm = pumpOn ? (7.5f + (float)random(-3, 3) * 0.1f) : 0.0f;
    }

    if (chillerMode == MODE_COOLING) {
        float err = tempOutlet - tempSetpoint;
        if (err > 0.5f) {
            float demand = constrain(err * 10.0f, 10.0f, 100.0f);
            setCompressor(demand);
            setFan(demand * 0.8f);
            setPump(true);
        } else if (err < -0.5f) {
            setCompressor(constrain(compressorPct - 5.0f, 0, 100));
        } else {
            setFan(compressorPct * 0.7f);
        }
    }

    alarmHighTemp  = (tempOutlet > 35.0f);
    alarmLowFlow   = (pumpOn && flowRateLpm < 2.0f);
    alarmCompFault = (compressorOn && pressureBar < 1.0f);

    alarmBitmap = (alarmHighTemp ? 1 : 0) | (alarmLowFlow ? 2 : 0) | (alarmCompFault ? 4 : 0);
    if (alarmBitmap && chillerMode != MODE_FAULT) {
        chillerMode = MODE_FAULT;
        setCompressor(0);
        ledMode = LED_ON;
    }
}

// ─────────────────────────────────────────────────────────
// Display
// ─────────────────────────────────────────────────────────
void updateDisplay() {
    const char* modeStr = (chillerMode == MODE_COOLING) ? "COOLING" :
                          (chillerMode == MODE_MANUAL)  ? "MANUAL"  :
                          (chillerMode == MODE_FAULT)   ? "FAULT"   : "STANDBY";
#ifdef LILYGO_TDISPLAY
    // Color TFT 240×135 landscape
    tft.fillScreen(TFT_BLACK);
    char buf[40];

    // Row 1: Name + version + WiFi dot
    tft.setTextFont(4);
    tft.setTextColor(TFT_CYAN, TFT_BLACK);
    tft.setCursor(0, 5);
    tft.print(PROG_NAME);
    tft.setTextFont(1);
    tft.setTextColor(TFT_DARKGREY, TFT_BLACK);
    tft.setCursor(100, 10);
    tft.print("v"); tft.print(PROG_VERSION);
    if (wifiConnected) tft.fillCircle(228, 12, 6, TFT_GREEN);
    else               tft.fillCircle(228, 12, 6, TFT_RED);

    // Row 2: IP / location
    tft.setTextFont(2);
    tft.setCursor(0, 40);
    if (wifiConnected) {
        tft.setTextColor(TFT_WHITE, TFT_BLACK);
        tft.print(WiFi.localIP().toString());
        if (locationName.length()) {
            tft.setTextColor(TFT_DARKGREY, TFT_BLACK);
            tft.print("  "); tft.print(locationName);
        }
    } else {
        tft.setTextColor(TFT_RED, TFT_BLACK);
        tft.print("No WiFi");
    }

    // Row 3: Temperatures
    tft.setTextColor(TFT_WHITE, TFT_BLACK);
    snprintf(buf, sizeof(buf), "In:%.1fC  Out:%.1fC", tempInlet, tempOutlet);
    tft.setCursor(0, 62);
    tft.print(buf);

    // Row 4: Compressor + fan
    snprintf(buf, sizeof(buf), "Cmp:%.0f%%  Fan:%.0f%%", compressorPct, fanPct);
    tft.setCursor(0, 84);
    tft.print(buf);

    // Row 5: Setpoint + pressure
    snprintf(buf, sizeof(buf), "Set:%.1fC  %.1fbar", tempSetpoint, pressureBar);
    tft.setCursor(0, 106);
    tft.print(buf);

    // Row 6: Mode (colored) + alarm
    uint16_t modeColor = (chillerMode == MODE_COOLING) ? TFT_CYAN   :
                         (chillerMode == MODE_MANUAL)  ? TFT_YELLOW :
                         (chillerMode == MODE_FAULT)   ? TFT_RED    : TFT_GREEN;
    tft.setTextFont(2);
    tft.setTextColor(modeColor, TFT_BLACK);
    tft.setCursor(0, 116);
    tft.print(modeStr);
    if (alarmBitmap) {
        tft.setTextColor(TFT_RED, TFT_BLACK);
        tft.setCursor(170, 116);
        tft.print("ALM");
    }

#else
    // Monochrome 128×64 OLED
    display.clearBuffer();
    display.setFont(u8g2_font_helvB08_tf);
    display.drawStr(0, 9, PROG_NAME);
    display.setFont(u8g2_font_5x7_tf);
    display.drawStr(55, 9, "v"); display.drawStr(60, 9, PROG_VERSION);
    if (wifiConnected) display.drawDisc(120, 5, 4);
    else               display.drawCircle(120, 5, 4);

    if (wifiConnected) display.drawStr(0, 19, WiFi.localIP().toString().c_str());
    else               display.drawStr(0, 19, "No WiFi");

    char buf[32];
    snprintf(buf, sizeof(buf), "In:%.1fC  Out:%.1fC", tempInlet, tempOutlet);
    display.drawStr(0, 29, buf);
    snprintf(buf, sizeof(buf), "Cmp:%.0f%%  Fan:%.0f%%", compressorPct, fanPct);
    display.drawStr(0, 39, buf);
    snprintf(buf, sizeof(buf), "Set:%.1fC  %.1fbar", tempSetpoint, pressureBar);
    display.drawStr(0, 49, buf);
    display.drawStr(0, 63, modeStr);
    if (alarmBitmap) { display.drawStr(70, 63, "ALM"); }
    display.sendBuffer();
#endif
}

// ─────────────────────────────────────────────────────────
// WebSocket
// ─────────────────────────────────────────────────────────
void buildStatus(StaticJsonDocument<512>& doc) {
    doc["type"]        = "status";
    doc["version"]     = PROG_VERSION;
    doc["hostname"]    = OTA_HOSTNAME;
    doc["device_type"] = "chiller_control";
    doc["location"]    = locationName;
    doc["mode"]    = (chillerMode == MODE_COOLING) ? "cooling" :
                     (chillerMode == MODE_MANUAL)  ? "manual"  :
                     (chillerMode == MODE_FAULT)   ? "fault"   : "standby";
    doc["temp_setpoint"]   = tempSetpoint;
    doc["temp_inlet"]      = tempInlet;
    doc["temp_outlet"]     = tempOutlet;
    doc["compressor_pct"]  = compressorPct;
    doc["fan_pct"]         = fanPct;
    doc["flow_lpm"]        = flowRateLpm;
    doc["pressure_bar"]    = pressureBar;
    doc["pump_on"]         = pumpOn;
    doc["compressor_on"]   = compressorOn;
    doc["alarm_bitmap"]    = alarmBitmap;
    doc["alarm_high_temp"] = alarmHighTemp;
    doc["alarm_low_flow"]  = alarmLowFlow;
    doc["alarm_comp_fault"]= alarmCompFault;
    doc["wifi_connected"]  = wifiConnected;
}

void sendStatus(uint8_t num) {
    StaticJsonDocument<512> doc;
    buildStatus(doc);
    String json; serializeJson(doc, json);
    webSocket.sendTXT(num, json);
    wsTxCount++;
}

void broadcastStatus() {
    StaticJsonDocument<512> doc;
    buildStatus(doc);
    String json; serializeJson(doc, json);
    webSocket.broadcastTXT(json);
    wsTxCount += wsClients;
}

void handleWsMessage(uint8_t num, char* payload, size_t len) {
    StaticJsonDocument<256> doc;
    if (deserializeJson(doc, payload, len)) return;
    String cmd = doc["cmd"] | "";

    if (cmd == "get_status") {
        sendStatus(num);
    } else if (cmd == "set_setpoint") {
        tempSetpoint = constrain(doc["value"].as<float>(), MIN_TEMP, MAX_TEMP);
        broadcastStatus();
    } else if (cmd == "set_compressor") {
        if (chillerMode != MODE_FAULT) {
            chillerMode = MODE_MANUAL;
            setCompressor(doc["value"].as<float>());
            broadcastStatus();
        }
    } else if (cmd == "set_fan") {
        setFan(doc["value"].as<float>());
        broadcastStatus();
    } else if (cmd == "set_pump") {
        setPump(doc["state"].as<bool>());
        broadcastStatus();
    } else if (cmd == "set_mode") {
        String m = doc["mode"] | "standby";
        if (m == "cooling") {
            chillerMode = MODE_COOLING;
        } else if (m == "manual") {
            chillerMode = MODE_MANUAL;
        } else if (m == "standby") {
            chillerMode = MODE_STANDBY;
            setCompressor(0); setFan(0);
        }
        broadcastStatus();
    } else if (cmd == "clear_fault") {
        alarmBitmap = 0; alarmHighTemp = false; alarmLowFlow = false; alarmCompFault = false;
        chillerMode = MODE_STANDBY;
        setCompressor(0); setFan(0);
        ledMode = LED_BLINK;
        broadcastStatus();
    } else if (cmd == "set_location") {
        locationName = doc["value"] | "";
        locationName.trim();
        preferences.putString("location", locationName);
        broadcastStatus();
    } else if (cmd == "ping") {
        StaticJsonDocument<64> r;
        r["type"] = "pong"; r["t"] = doc["t"].as<unsigned long>();
        String j; serializeJson(r, j);
        webSocket.sendTXT(num, j);
    }
}

void wsEvent(uint8_t num, WStype_t type, uint8_t* payload, size_t length) {
    if (type == WStype_DISCONNECTED) { if (wsClients) wsClients--; }
    else if (type == WStype_CONNECTED) { wsClients++; sendStatus(num); }
    else if (type == WStype_TEXT) { wsRxCount++; handleWsMessage(num, (char*)payload, length); }
}

// ─────────────────────────────────────────────────────────
// LED
// ─────────────────────────────────────────────────────────
void handleLED() {
    if (ledMode == LED_BLINK && millis() - lastLedToggle > 500) {
        ledState = !ledState;
        digitalWrite(LED_PIN, ledState);
        lastLedToggle = millis();
    } else if (ledMode == LED_ON)  digitalWrite(LED_PIN, HIGH);
    else if (ledMode == LED_OFF)   digitalWrite(LED_PIN, LOW);
}

// ─────────────────────────────────────────────────────────
// Serial commands
// ─────────────────────────────────────────────────────────
void handleSerial() {
    while (Serial.available()) {
        char c = Serial.read();
        if (c == '\n') {
            serialBuf.trim();
            if (serialBuf.startsWith("wifi ssid ")) {
                wifiSSID = serialBuf.substring(10);
                preferences.putString("ssid", wifiSSID);
                Serial.println("SSID saved");
            } else if (serialBuf.startsWith("wifi pass ")) {
                wifiPass = serialBuf.substring(10);
                preferences.putString("pass", wifiPass);
                Serial.println("Pass saved");
            } else if (serialBuf == "wifi connect") {
                connectWiFi();
            } else if (serialBuf == "status") {
                Serial.printf("Mode: %d  Tout:%.1f  Tset:%.1f  Cmp:%.0f%%  Fan:%.0f%%\n",
                    chillerMode, tempOutlet, tempSetpoint, compressorPct, fanPct);
                Serial.printf("Location: %s\n", locationName.length() ? locationName.c_str() : "(none)");
            } else if (serialBuf.startsWith("set location ")) {
                locationName = serialBuf.substring(13);
                locationName.trim();
                preferences.putString("location", locationName);
                Serial.printf("Location set: %s\n", locationName.c_str());
            }
            serialBuf = "";
        } else {
            serialBuf += c;
        }
    }
}

// ─────────────────────────────────────────────────────────
// Setup / Loop
// ─────────────────────────────────────────────────────────
void setup() {
    Serial.begin(SERIAL_BAUD);
    delay(500);
    Serial.println("\n=== Chiller Controller ===");

#ifdef LILYGO_TDISPLAY
    pinMode(TFT_BL_PIN, OUTPUT);
    digitalWrite(TFT_BL_PIN, HIGH);  // Backlight on
    tft.init();
    tft.setRotation(1);  // Landscape: 240×135
    tft.fillScreen(TFT_BLACK);
    tft.setTextFont(4);
    tft.setTextColor(TFT_CYAN, TFT_BLACK);
    tft.setCursor(40, 45);
    tft.print(PROG_NAME);
    tft.setTextFont(2);
    tft.setTextColor(TFT_WHITE, TFT_BLACK);
    tft.setCursor(80, 95);
    tft.print("v"); tft.print(PROG_VERSION);
    delay(1000);
#else
    display.begin();
    display.setFont(u8g2_font_6x10_tf);
    display.clearBuffer();
    display.drawStr(10, 30, PROG_NAME);
    display.drawStr(30, 45, "v" PROG_VERSION);
    display.sendBuffer();
    delay(1000);
#endif

    pinMode(LED_PIN, OUTPUT);
    for (int i = 0; i < GPIO_OUT_COUNT; i++) {
        pinMode(GPIO_OUT_PINS[i], OUTPUT);
        digitalWrite(GPIO_OUT_PINS[i], LOW);
    }
#if USE_PWM_DAC
    ledcAttach(DAC1_PIN, 5000, 12);
    ledcAttach(DAC2_PIN, 5000, 12);
    ledcWrite(DAC1_PIN, 0);
    ledcWrite(DAC2_PIN, 0);
#else
    dacWrite(DAC1_PIN, 0);
    dacWrite(DAC2_PIN, 0);
#endif
    analogReadResolution(12);

    preferences.begin(PREF_NS, false);
    wifiSSID      = preferences.getString("ssid", "");
    wifiPass      = preferences.getString("pass", "");
    locationName  = preferences.getString("location", "");

    if (wifiSSID.length() > 0) connectWiFi();
    else Serial.println("No WiFi — use serial: wifi ssid/pass/connect");

    updateDisplay();
}

void loop() {
    if (wifiConnected) {
        ArduinoOTA.handle();
        if (otaInProgress) { delay(10); return; }
        if (wsServerStarted) webSocket.loop();
    }
    handleSerial();
    handleLED();

    if (millis() - lastSimStep > 500) {
        simStep();
        lastSimStep = millis();
    }

    if (millis() - lastUpdate > 250) {
        updateDisplay();
        broadcastStatus();
        lastUpdate = millis();
    }
    delay(10);
}
