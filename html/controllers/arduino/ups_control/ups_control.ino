/*
 * UPS Controller for Heltec LoRa32 V2/V3 + LILYGO T-Display V1.1
 * Version: 1.1.0
 *
 * Simulates / controls an Uninterruptible Power Supply.
 * DAC1 (GPIO25) → output voltage reference (0-3.3V = 0-240V AC)
 * DAC2 (GPIO26) → battery current sense (0-3.3V = 0-100A)
 * GPIO OUT[0]   → mains contactor (bypass)
 * GPIO OUT[1]   → inverter enable
 * GPIO OUT[2]   → alarm relay
 *
 * WebSocket Commands:
 *   {"cmd":"get_status"}
 *   {"cmd":"set_mode","mode":"normal"}     // normal|bypass|battery|test|fault
 *   {"cmd":"battery_test"}                 // initiate battery test
 *   {"cmd":"set_output_voltage","value":230.0}
 *   {"cmd":"set_load","value":60.0}        // % load (0-100, sim only)
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

#define PROG_NAME     "UPSCtrl"
#define PROG_VERSION  "1.1.0"

// ─────────────────────────────────────────────────────────
// Board-specific pin definitions
// ─────────────────────────────────────────────────────────
#ifdef HELTEC_V3
  #define LED_PIN        35
  #define OLED_SDA       17
  #define OLED_SCL       18
  #define OLED_RST       21
  #define DAC1_PIN        1  // PWM — output voltage ref
  #define DAC2_PIN        2  // PWM — battery current ref
  #define ADC1_PIN        4
  #define ADC2_PIN        5
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {12, 13, 14};
  #define USE_PWM_DAC     1
  #warning "Heltec V3/V4: no native DAC — add RC filter on DAC1/DAC2 pins"
#elif defined(LILYGO_TDISPLAY)
  // LILYGO T-Display V1.1 — ESP32, true DAC, ST7789V 135×240 color TFT
  #define LED_PIN         2
  #define TFT_BL_PIN      4
  #define DAC1_PIN       25  // True DAC — output voltage ref
  #define DAC2_PIN       26  // True DAC — battery current ref
  #define ADC1_PIN       36
  #define ADC2_PIN       39
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {32, 33, 21};
  #define USE_PWM_DAC     0
#else
  #define LED_PIN        2
  #define OLED_SDA       4
  #define OLED_SCL       15
  #define OLED_RST       16
  #define DAC1_PIN       25
  #define DAC2_PIN       26
  #define ADC1_PIN       36
  #define ADC2_PIN       39
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {17, 23, 33};
  #define USE_PWM_DAC     0
#endif

#define WS_PORT       81
#define SERIAL_BAUD   115200
#define OTA_HOSTNAME  "ups-ctrl"
#define OTA_PASSWORD  "bms2026"
#define PREF_NS       "upsctrl"

#define NOM_OUTPUT_V    230.0f
#define MAX_LOAD_W      10000.0f
#define NOM_BAT_V       48.0f
#define BAT_CAPACITY_AH 100.0f

#ifdef LILYGO_TDISPLAY
  TFT_eSPI tft = TFT_eSPI();
#else
  U8G2_SSD1306_128X64_NONAME_F_HW_I2C display(U8G2_R0, OLED_RST, OLED_SCL, OLED_SDA);
#endif

Preferences preferences;
WebSocketsServer webSocket(WS_PORT);

String wifiSSID, wifiPass;
bool wifiConnected = false, wsServerStarted = false, otaInProgress = false;

// UPS state
enum UPSMode { UPS_NORMAL, UPS_BATTERY, UPS_BYPASS, UPS_TEST, UPS_FAULT };
UPSMode upsMode = UPS_NORMAL;

float mainsVoltage    = 230.0f;
float mainsFrequency  = 50.0f;
float outputVoltage   = 230.0f;
float outputVoltageRef= 230.0f;
float outputCurrent   = 0.0f;
float outputPower     = 0.0f;
float loadPct         = 30.0f;
float batteryVoltage  = 51.2f;
float batterySOC      = 92.0f;
float batteryCurrent  = 0.0f;
float runtimeMin      = 0.0f;
float transferTimeMs  = 8.0f;
bool  mainsPresent    = true;
bool  mainsContactor  = true;
bool  inverterOn      = false;

bool   batTestRunning = false;
float  batTestStartSOC = 0;
unsigned long batTestStart = 0;

// Alarms
bool alarmMainsFail   = false;
bool alarmBattLow     = false;
bool alarmOverload    = false;
bool alarmOvertemp    = false;
float tempDegC        = 38.0f;
uint32_t alarmBitmap  = 0;

enum LedMode { LED_OFF, LED_ON, LED_BLINK };
LedMode ledMode = LED_OFF;
unsigned long lastLedToggle = 0;
bool ledState = false;

uint32_t wsRxCount = 0, wsTxCount = 0;
uint8_t  wsClients = 0;
unsigned long lastUpdate = 0, lastSimStep = 0;
String serialBuf = "";
String locationName = "";

// ─────────────────────────────────────────────────────────
// WiFi / OTA
// ─────────────────────────────────────────────────────────
void connectWiFi() {
    WiFi.mode(WIFI_STA);
    WiFi.begin(wifiSSID.c_str(), wifiPass.c_str());
    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts++ < 30) { delay(500); Serial.print("."); }
    if (WiFi.status() == WL_CONNECTED) {
        wifiConnected = true;
        Serial.printf("\nConnected: %s\n", WiFi.localIP().toString().c_str());
        ArduinoOTA.setHostname(OTA_HOSTNAME);
        ArduinoOTA.setPassword(OTA_PASSWORD);
        ArduinoOTA.onStart([]() { otaInProgress = true; });
        ArduinoOTA.onEnd([]() { otaInProgress = false; });
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

void dacOut(uint8_t pin, uint8_t val8) {
#if USE_PWM_DAC
    ledcWrite(pin, (uint32_t)val8 * 16);
#else
    dacWrite(pin, val8);
#endif
}

void updateDAC() {
    dacOut(DAC1_PIN, (uint8_t)(outputVoltageRef / 240.0f * 255));
    dacOut(DAC2_PIN, (uint8_t)(abs(batteryCurrent) / 100.0f * 255));
}

// ─────────────────────────────────────────────────────────
// Simulation step (500ms)
// ─────────────────────────────────────────────────────────
void simStep() {
    mainsVoltage   = 230.0f + (float)random(-40, 40) * 0.2f;
    mainsFrequency = 50.0f  + (float)random(-3, 3) * 0.01f;
    bool mainsOK   = (mainsVoltage > 195.0f && mainsVoltage < 255.0f &&
                      mainsFrequency > 47.5f && mainsFrequency < 52.5f);

    outputCurrent = (loadPct / 100.0f * MAX_LOAD_W) / max(outputVoltageRef, 1.0f);
    outputPower   = outputVoltageRef * outputCurrent;

    tempDegC += (38.0f + (loadPct / 100.0f) * 20.0f - tempDegC) * 0.02f + (float)random(-2,2)*0.05f;

    switch (upsMode) {
        case UPS_NORMAL:
            if (!mainsOK) {
                upsMode = UPS_BATTERY;
                mainsContactor = false;
                inverterOn = true;
                transferTimeMs = 4.0f + (float)random(0, 8);
                digitalWrite(GPIO_OUT_PINS[0], LOW);
                digitalWrite(GPIO_OUT_PINS[1], HIGH);
                Serial.println("Mains failed — on battery");
                ledMode = LED_BLINK;
            } else {
                mainsContactor = true; inverterOn = false;
                outputVoltage = mainsVoltage + (float)random(-5,5)*0.1f;
                batteryCurrent = constrain((100.0f - batterySOC) * 1.5f, 0, 20.0f);
                batterySOC = constrain(batterySOC + batteryCurrent * 0.5f / (BAT_CAPACITY_AH * 3600.0f) * 1800.0f, 0, 100);
                batteryVoltage = 44.0f + batterySOC / 100.0f * 10.0f + (float)random(-2,2)*0.05f;
            }
            break;

        case UPS_BATTERY:
            if (mainsOK && !batTestRunning) {
                upsMode = UPS_NORMAL;
                mainsContactor = true; inverterOn = false;
                digitalWrite(GPIO_OUT_PINS[0], HIGH);
                digitalWrite(GPIO_OUT_PINS[1], LOW);
                ledMode = LED_BLINK;
                Serial.println("Mains restored — switching back");
            } else {
                outputVoltage = outputVoltageRef + (float)random(-3,3)*0.1f;
                batteryCurrent = -outputCurrent * outputVoltageRef / max(batteryVoltage, 1.0f);
                batterySOC = constrain(batterySOC + batteryCurrent * 0.5f / (BAT_CAPACITY_AH * 3600.0f) * 1800.0f, 0, 100);
                batteryVoltage = 44.0f + batterySOC / 100.0f * 10.0f + (float)random(-2,2)*0.05f;
                float drawA = abs(batteryCurrent);
                runtimeMin = (drawA > 0.1f) ? (batterySOC / 100.0f * BAT_CAPACITY_AH / drawA * 60.0f) : 999.0f;
            }
            if (batTestRunning && millis() - batTestStart > 30000) {
                batTestRunning = false;
                upsMode = UPS_NORMAL;
                Serial.println("Battery test complete");
            }
            break;

        case UPS_BYPASS:
            mainsContactor = true; inverterOn = false;
            outputVoltage = mainsVoltage;
            batteryCurrent = 0; batterySOC = constrain(batterySOC + 0.01f, 0, 100);
            break;

        case UPS_FAULT:
            mainsContactor = false; inverterOn = false;
            outputVoltage = 0; outputCurrent = 0; outputPower = 0;
            break;

        default: break;
    }

    alarmMainsFail = (!mainsOK && upsMode != UPS_BATTERY && upsMode != UPS_BYPASS);
    alarmBattLow   = (batterySOC < 15.0f);
    alarmOverload  = (loadPct > 100.0f);
    alarmOvertemp  = (tempDegC > 70.0f);

    alarmBitmap = (alarmMainsFail ? 1 : 0) | (alarmBattLow  ? 2 : 0) |
                  (alarmOverload  ? 4 : 0) | (alarmOvertemp ? 8 : 0);

    if (alarmBattLow && upsMode == UPS_BATTERY) {
        upsMode = UPS_FAULT;
        digitalWrite(GPIO_OUT_PINS[1], LOW);
        ledMode = LED_ON;
        Serial.println("Battery critical — shutdown");
    }

    mainsPresent = mainsOK;
    updateDAC();
}

// ─────────────────────────────────────────────────────────
// Display
// ─────────────────────────────────────────────────────────
void updateDisplay() {
    const char* modeStr = (upsMode == UPS_BATTERY) ? "BATTERY" :
                          (upsMode == UPS_BYPASS)   ? "BYPASS"  :
                          (upsMode == UPS_FAULT)    ? "FAULT"   :
                          (upsMode == UPS_TEST)     ? "BAT TEST" : "NORMAL";
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

    // Row 3: Mains
    tft.setTextColor(mainsPresent ? TFT_WHITE : TFT_RED, TFT_BLACK);
    snprintf(buf, sizeof(buf), "Mains:%.0fV  %.1fHz", mainsVoltage, mainsFrequency);
    tft.setCursor(0, 62);
    tft.print(buf);

    // Row 4: Output
    tft.setTextColor(TFT_WHITE, TFT_BLACK);
    snprintf(buf, sizeof(buf), "Out:%.0fV  %.1fA  %.0f%%", outputVoltage, outputCurrent, loadPct);
    tft.setCursor(0, 84);
    tft.print(buf);

    // Row 5: Battery
    uint16_t batColor = (batterySOC < 20.0f) ? TFT_RED : (batterySOC < 50.0f) ? TFT_YELLOW : TFT_GREEN;
    tft.setTextColor(batColor, TFT_BLACK);
    snprintf(buf, sizeof(buf), "Bat:%.1fV  %.0f%%  %.0fm", batteryVoltage, batterySOC, runtimeMin);
    tft.setCursor(0, 106);
    tft.print(buf);

    // Row 6: Mode (colored) + alarm
    uint16_t modeColor = (upsMode == UPS_BATTERY) ? TFT_YELLOW :
                         (upsMode == UPS_BYPASS)   ? TFT_CYAN   :
                         (upsMode == UPS_FAULT)    ? TFT_RED    :
                         (upsMode == UPS_TEST)     ? TFT_MAGENTA : TFT_GREEN;
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
    snprintf(buf, sizeof(buf), "Mains:%.0fV  %.1fHz", mainsVoltage, mainsFrequency);
    display.drawStr(0, 29, buf);
    snprintf(buf, sizeof(buf), "Out:%.0fV  %.1fA  %.0f%%", outputVoltage, outputCurrent, loadPct);
    display.drawStr(0, 39, buf);
    snprintf(buf, sizeof(buf), "Bat:%.1fV  %.0f%%  %.0fm", batteryVoltage, batterySOC, runtimeMin);
    display.drawStr(0, 49, buf);
    display.drawStr(0, 63, modeStr);
    if (alarmBitmap) display.drawStr(70, 63, "ALM");
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
    doc["device_type"] = "ups_control";
    doc["location"]    = locationName;
    doc["mode"]        = (upsMode == UPS_BATTERY) ? "battery" :
                      (upsMode == UPS_BYPASS)   ? "bypass"  :
                      (upsMode == UPS_FAULT)    ? "fault"   :
                      (upsMode == UPS_TEST)     ? "test"    : "normal";
    doc["mains_voltage"]    = mainsVoltage;
    doc["mains_frequency"]  = mainsFrequency;
    doc["mains_present"]    = mainsPresent;
    doc["output_voltage"]   = outputVoltage;
    doc["output_current"]   = outputCurrent;
    doc["output_power_w"]   = outputPower;
    doc["load_pct"]         = loadPct;
    doc["battery_voltage"]  = batteryVoltage;
    doc["battery_soc"]      = batterySOC;
    doc["battery_current"]  = batteryCurrent;
    doc["runtime_min"]      = runtimeMin;
    doc["transfer_time_ms"] = transferTimeMs;
    doc["temp_degc"]        = tempDegC;
    doc["bat_test_running"] = batTestRunning;
    doc["alarm_bitmap"]     = alarmBitmap;
    doc["alarm_mains_fail"] = alarmMainsFail;
    doc["alarm_batt_low"]   = alarmBattLow;
    doc["alarm_overload"]   = alarmOverload;
    doc["alarm_overtemp"]   = alarmOvertemp;
    doc["wifi_connected"]   = wifiConnected;
}

void sendStatus(uint8_t num) {
    StaticJsonDocument<512> doc; buildStatus(doc);
    String j; serializeJson(doc, j);
    webSocket.sendTXT(num, j); wsTxCount++;
}

void broadcastStatus() {
    StaticJsonDocument<512> doc; buildStatus(doc);
    String j; serializeJson(doc, j);
    webSocket.broadcastTXT(j); wsTxCount += wsClients;
}

void handleWsMessage(uint8_t num, char* payload, size_t len) {
    StaticJsonDocument<256> doc;
    if (deserializeJson(doc, payload, len)) return;
    String cmd = doc["cmd"] | "";

    if (cmd == "get_status") {
        sendStatus(num);
    } else if (cmd == "set_mode") {
        String m = doc["mode"] | "normal";
        if (m == "bypass")      { upsMode = UPS_BYPASS; mainsContactor = true; inverterOn = false; }
        else if (m == "normal") { upsMode = UPS_NORMAL; }
        broadcastStatus();
    } else if (cmd == "battery_test") {
        if (upsMode == UPS_NORMAL && mainsPresent) {
            batTestRunning = true;
            batTestStart = millis();
            batTestStartSOC = batterySOC;
            upsMode = UPS_BATTERY;
            mainsContactor = false; inverterOn = true;
            digitalWrite(GPIO_OUT_PINS[0], LOW);
            digitalWrite(GPIO_OUT_PINS[1], HIGH);
            Serial.println("Battery test started");
            broadcastStatus();
        }
    } else if (cmd == "set_output_voltage") {
        outputVoltageRef = constrain(doc["value"].as<float>(), 100, 240);
        broadcastStatus();
    } else if (cmd == "set_load") {
        loadPct = constrain(doc["value"].as<float>(), 0, 125);
        broadcastStatus();
    } else if (cmd == "clear_fault") {
        alarmBitmap = 0; alarmMainsFail = alarmBattLow = alarmOverload = alarmOvertemp = false;
        upsMode = UPS_NORMAL;
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

void handleLED() {
    if (ledMode == LED_BLINK && millis() - lastLedToggle > 500) {
        ledState = !ledState; digitalWrite(LED_PIN, ledState); lastLedToggle = millis();
    } else if (ledMode == LED_ON)  digitalWrite(LED_PIN, HIGH);
    else if (ledMode == LED_OFF)   digitalWrite(LED_PIN, LOW);
}

void handleSerial() {
    while (Serial.available()) {
        char c = Serial.read();
        if (c == '\n') {
            serialBuf.trim();
            if (serialBuf.startsWith("wifi ssid ")) { wifiSSID = serialBuf.substring(10); preferences.putString("ssid", wifiSSID); }
            else if (serialBuf.startsWith("wifi pass ")) { wifiPass = serialBuf.substring(10); preferences.putString("pass", wifiPass); }
            else if (serialBuf == "wifi connect") connectWiFi();
            else if (serialBuf == "status") {
                Serial.printf("Mode:%d Mains:%.0fV Bat:%.0f%%  Load:%.0f%%\n", upsMode, mainsVoltage, batterySOC, loadPct);
                Serial.printf("Location: %s\n", locationName.length() ? locationName.c_str() : "(none)");
            } else if (serialBuf.startsWith("set location ")) {
                locationName = serialBuf.substring(13);
                locationName.trim();
                preferences.putString("location", locationName);
                Serial.printf("Location set: %s\n", locationName.c_str());
            }
            serialBuf = "";
        } else serialBuf += c;
    }
}

void setup() {
    Serial.begin(SERIAL_BAUD); delay(500);
    Serial.println("=== UPS Controller ===");

#ifdef LILYGO_TDISPLAY
    pinMode(TFT_BL_PIN, OUTPUT);
    digitalWrite(TFT_BL_PIN, HIGH);
    tft.init();
    tft.setRotation(1);  // Landscape: 240×135
    tft.fillScreen(TFT_BLACK);
    tft.setTextFont(4);
    tft.setTextColor(TFT_CYAN, TFT_BLACK);
    tft.setCursor(20, 45);
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
    display.drawStr(25, 45, "v" PROG_VERSION);
    display.sendBuffer(); delay(1000);
#endif

    pinMode(LED_PIN, OUTPUT);
    for (int i = 0; i < GPIO_OUT_COUNT; i++) { pinMode(GPIO_OUT_PINS[i], OUTPUT); digitalWrite(GPIO_OUT_PINS[i], LOW); }
#if USE_PWM_DAC
    ledcAttach(DAC1_PIN, 5000, 12);
    ledcAttach(DAC2_PIN, 5000, 12);
    ledcWrite(DAC1_PIN, 0); ledcWrite(DAC2_PIN, 0);
#else
    dacWrite(DAC1_PIN, 0); dacWrite(DAC2_PIN, 0);
#endif
    analogReadResolution(12);

    // Mains contactor starts closed
    digitalWrite(GPIO_OUT_PINS[0], HIGH);
    mainsContactor = true;

    preferences.begin(PREF_NS, false);
    wifiSSID     = preferences.getString("ssid", "");
    wifiPass     = preferences.getString("pass", "");
    locationName = preferences.getString("location", "");
    if (wifiSSID.length() > 0) connectWiFi();
    else Serial.println("No WiFi — use serial");
    updateDisplay();
}

void loop() {
    if (wifiConnected) {
        ArduinoOTA.handle();
        if (otaInProgress) { delay(10); return; }
        if (wsServerStarted) webSocket.loop();
    }
    handleSerial(); handleLED();
    if (millis() - lastSimStep > 500) { simStep(); lastSimStep = millis(); }
    if (millis() - lastUpdate > 250) { updateDisplay(); broadcastStatus(); lastUpdate = millis(); }
    delay(10);
}
