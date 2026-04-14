/*
 * PCS Controller for Heltec LoRa32 V2/V3 + LILYGO T-Display V1.1
 * Version: 1.1.0
 *
 * Simulates / controls a Power Conversion System (bidirectional AC↔DC).
 * DAC1 (GPIO25) → DC voltage reference (0-3.3V = 0-800V DC)
 * DAC2 (GPIO26) → current limit reference (0-3.3V = 0-500A)
 * GPIO OUT[0]   → grid contactor
 * GPIO OUT[1]   → DC bus enable
 * GPIO OUT[2]   → alarm relay
 *
 * WebSocket Commands:
 *   {"cmd":"get_status"}
 *   {"cmd":"set_mode","mode":"charge"}       // charge|discharge|standby|fault
 *   {"cmd":"set_dc_voltage","value":750.0}   // V
 *   {"cmd":"set_current_limit","value":200.0}// A
 *   {"cmd":"set_soc_target","value":80.0}    // %
 *   {"cmd":"set_power","value":50000.0}      // W (signed: + charge, - discharge)
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

#define PROG_NAME     "PCSCtrl"
#define PROG_VERSION  "1.1.0"

// ─────────────────────────────────────────────────────────
// Board-specific pin definitions
// ─────────────────────────────────────────────────────────
#ifdef HELTEC_V3
  #define LED_PIN        35
  #define OLED_SDA       17
  #define OLED_SCL       18
  #define OLED_RST       21
  #define DAC1_PIN        1  // PWM — DC voltage ref
  #define DAC2_PIN        2  // PWM — current limit ref
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
  #define DAC1_PIN       25  // True DAC — DC voltage ref
  #define DAC2_PIN       26  // True DAC — current limit ref
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
  #define DAC1_PIN       25  // DC voltage ref
  #define DAC2_PIN       26  // Current limit ref
  #define ADC1_PIN       36
  #define ADC2_PIN       39
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {17, 23, 33};
  #define USE_PWM_DAC     0
#endif

#define WS_PORT       81
#define SERIAL_BAUD   115200
#define OTA_HOSTNAME  "pcs-ctrl"
#define OTA_PASSWORD  "bms2026"
#define PREF_NS       "pcsctrl"

// PCS limits
#define MAX_DC_VOLTAGE  800.0f
#define MAX_AC_VOLTAGE  480.0f
#define MAX_CURRENT     500.0f
#define MAX_POWER       200000.0f
#define NOM_FREQ        50.0f

#ifdef LILYGO_TDISPLAY
  TFT_eSPI tft = TFT_eSPI();
#else
  U8G2_SSD1306_128X64_NONAME_F_HW_I2C display(U8G2_R0, OLED_RST, OLED_SCL, OLED_SDA);
#endif

Preferences preferences;
WebSocketsServer webSocket(WS_PORT);

String wifiSSID, wifiPass;
bool wifiConnected = false, wsServerStarted = false, otaInProgress = false;

// PCS state
enum PCSMode { PCS_STANDBY, PCS_CHARGE, PCS_DISCHARGE, PCS_FAULT };
PCSMode pcsMode = PCS_STANDBY;

float acVoltage     = 400.0f;
float acFrequency   = 50.0f;
float dcVoltage     = 0.0f;
float dcVoltageRef  = 750.0f;
float dcCurrent     = 0.0f;
float currentLimit  = 200.0f;
float activePower   = 0.0f;
float reactivePower = 0.0f;
float powerFactor   = 0.98f;
float efficiency    = 0.0f;
float socTarget     = 80.0f;
float socActual     = 50.0f;
float tempDegC      = 35.0f;
bool  gridContactor = false;
bool  dcBusEnabled  = false;

// Alarms
bool alarmGridFault  = false;
bool alarmOvertemp   = false;
bool alarmOvercurrent= false;
bool alarmDCFault    = false;
uint32_t alarmBitmap = 0;

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

// ─────────────────────────────────────────────────────────
// DAC helpers
// ─────────────────────────────────────────────────────────
void dacOut(uint8_t pin, uint8_t val8) {
#if USE_PWM_DAC
    ledcWrite(pin, (uint32_t)val8 * 16);
#else
    dacWrite(pin, val8);
#endif
}

void updateDACOutputs() {
    dacOut(DAC1_PIN, (uint8_t)(dcVoltageRef / MAX_DC_VOLTAGE * 255));
    dacOut(DAC2_PIN, (uint8_t)(currentLimit  / MAX_CURRENT   * 255));
}

void setGridContactor(bool on) {
    gridContactor = on;
    digitalWrite(GPIO_OUT_PINS[0], on ? HIGH : LOW);
}

void setDCBus(bool on) {
    dcBusEnabled = on;
    digitalWrite(GPIO_OUT_PINS[1], on ? HIGH : LOW);
}

// ─────────────────────────────────────────────────────────
// Simulation step (500ms)
// ─────────────────────────────────────────────────────────
void simStep() {
    acVoltage   = 400.0f + (float)random(-50, 50) * 0.1f;
    acFrequency = 50.0f  + (float)random(-5, 5) * 0.01f;

    alarmGridFault = (acVoltage < 340.0f || acVoltage > 440.0f ||
                      acFrequency < 47.5f || acFrequency > 52.5f);

    if (pcsMode == PCS_STANDBY || pcsMode == PCS_FAULT) {
        dcCurrent   = 0;
        activePower = 0;
        dcVoltage   += (0.0f - dcVoltage) * 0.1f;
        efficiency   = 0;
        setGridContactor(false);
        setDCBus(false);
    } else if (pcsMode == PCS_CHARGE) {
        setGridContactor(!alarmGridFault);
        setDCBus(true);
        dcVoltage += (dcVoltageRef - dcVoltage) * 0.08f;
        dcVoltage += (float)random(-5, 5) * 0.05f;
        float powerDemand = constrain((socTarget - socActual) * 2000.0f, 0, MAX_POWER);
        dcCurrent = constrain(powerDemand / max(dcVoltage, 1.0f), 0, currentLimit);
        activePower = dcVoltage * dcCurrent;
        reactivePower = activePower * 0.05f;
        powerFactor = activePower / sqrt(activePower*activePower + reactivePower*reactivePower + 0.001f);
        efficiency = 95.0f + (float)random(-5, 5) * 0.1f;
        socActual = constrain(socActual + (activePower / 1000000.0f), 0, 100);
    } else if (pcsMode == PCS_DISCHARGE) {
        setGridContactor(!alarmGridFault);
        setDCBus(true);
        dcVoltage += (dcVoltageRef - dcVoltage) * 0.08f;
        dcVoltage += (float)random(-5, 5) * 0.05f;
        float powerDemand = constrain((socActual - socTarget) * 1500.0f, 0, MAX_POWER);
        dcCurrent = -constrain(powerDemand / max(dcVoltage, 1.0f), 0, currentLimit);
        activePower = dcVoltage * dcCurrent;
        efficiency = 94.0f + (float)random(-5, 5) * 0.1f;
        socActual = constrain(socActual - (abs(activePower) / 1200000.0f), 0, 100);
    }

    float thermalLoad = abs(activePower) / MAX_POWER;
    tempDegC += (35.0f + thermalLoad * 25.0f - tempDegC) * 0.02f + (float)random(-2, 2) * 0.05f;

    alarmOvertemp    = (tempDegC > 75.0f);
    alarmOvercurrent = (abs(dcCurrent) > currentLimit * 1.05f);
    alarmDCFault     = (dcBusEnabled && dcVoltage < 100.0f && abs(dcCurrent) > 10.0f);

    alarmBitmap = (alarmGridFault   ? 1 : 0) | (alarmOvertemp    ? 2 : 0) |
                  (alarmOvercurrent ? 4 : 0) | (alarmDCFault     ? 8 : 0);

    if ((alarmOvertemp || alarmOvercurrent) && pcsMode != PCS_FAULT) {
        pcsMode = PCS_FAULT;
        setGridContactor(false);
        setDCBus(false);
        ledMode = LED_ON;
    }

    updateDACOutputs();
}

// ─────────────────────────────────────────────────────────
// Display
// ─────────────────────────────────────────────────────────
void updateDisplay() {
    const char* modeStr = (pcsMode == PCS_CHARGE)    ? "CHARGE" :
                          (pcsMode == PCS_DISCHARGE)  ? "DISCHARGE" :
                          (pcsMode == PCS_FAULT)      ? "FAULT" : "STANDBY";
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

    // Row 3: AC voltage + frequency
    tft.setTextColor(TFT_WHITE, TFT_BLACK);
    snprintf(buf, sizeof(buf), "AC:%.0fV  %.1fHz", acVoltage, acFrequency);
    tft.setCursor(0, 62);
    tft.print(buf);

    // Row 4: DC voltage + current
    snprintf(buf, sizeof(buf), "DC:%.0fV  %.0fA", dcVoltage, dcCurrent);
    tft.setCursor(0, 84);
    tft.print(buf);

    // Row 5: Power + SOC
    snprintf(buf, sizeof(buf), "P:%.1fkW  SOC:%.0f%%", activePower / 1000.0f, socActual);
    tft.setCursor(0, 106);
    tft.print(buf);

    // Row 6: Mode (colored) + alarm
    uint16_t modeColor = (pcsMode == PCS_CHARGE)    ? TFT_GREEN  :
                         (pcsMode == PCS_DISCHARGE)  ? TFT_YELLOW :
                         (pcsMode == PCS_FAULT)      ? TFT_RED    : TFT_DARKGREY;
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
    snprintf(buf, sizeof(buf), "AC:%.0fV  %.1fHz", acVoltage, acFrequency);
    display.drawStr(0, 29, buf);
    snprintf(buf, sizeof(buf), "DC:%.0fV  %.0fA", dcVoltage, dcCurrent);
    display.drawStr(0, 39, buf);
    snprintf(buf, sizeof(buf), "P:%.1fkW  SOC:%.0f%%", activePower/1000.0f, socActual);
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
    doc["device_type"] = "pcs_control";
    doc["location"]    = locationName;
    doc["mode"]     = (pcsMode == PCS_CHARGE)    ? "charge" :
                      (pcsMode == PCS_DISCHARGE)  ? "discharge" :
                      (pcsMode == PCS_FAULT)      ? "fault" : "standby";
    doc["ac_voltage"]    = acVoltage;
    doc["ac_frequency"]  = acFrequency;
    doc["dc_voltage"]    = dcVoltage;
    doc["dc_voltage_ref"]= dcVoltageRef;
    doc["dc_current"]    = dcCurrent;
    doc["current_limit"] = currentLimit;
    doc["active_power_w"]= activePower;
    doc["power_factor"]  = powerFactor;
    doc["efficiency_pct"]= efficiency;
    doc["soc_actual"]    = socActual;
    doc["soc_target"]    = socTarget;
    doc["temp_degc"]     = tempDegC;
    doc["grid_contactor"]= gridContactor;
    doc["dc_bus_enabled"]= dcBusEnabled;
    doc["alarm_bitmap"]  = alarmBitmap;
    doc["alarm_grid"]    = alarmGridFault;
    doc["alarm_overtemp"]= alarmOvertemp;
    doc["alarm_overcurrent"] = alarmOvercurrent;
    doc["wifi_connected"]= wifiConnected;
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
        String m = doc["mode"] | "standby";
        if (m == "charge")         pcsMode = PCS_CHARGE;
        else if (m == "discharge") pcsMode = PCS_DISCHARGE;
        else if (m == "standby")   pcsMode = PCS_STANDBY;
        broadcastStatus();
    } else if (cmd == "set_dc_voltage") {
        dcVoltageRef = constrain(doc["value"].as<float>(), 0, MAX_DC_VOLTAGE);
        broadcastStatus();
    } else if (cmd == "set_current_limit") {
        currentLimit = constrain(doc["value"].as<float>(), 0, MAX_CURRENT);
        broadcastStatus();
    } else if (cmd == "set_soc_target") {
        socTarget = constrain(doc["value"].as<float>(), 0, 100);
        broadcastStatus();
    } else if (cmd == "set_power") {
        float pw = doc["value"].as<float>();
        if (pw > 0)      pcsMode = PCS_CHARGE;
        else if (pw < 0) pcsMode = PCS_DISCHARGE;
        else             pcsMode = PCS_STANDBY;
        broadcastStatus();
    } else if (cmd == "clear_fault") {
        alarmBitmap = 0; alarmGridFault = alarmOvertemp = alarmOvercurrent = alarmDCFault = false;
        pcsMode = PCS_STANDBY;
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
                Serial.printf("Mode:%d DC:%.0fV I:%.0fA P:%.1fkW SOC:%.0f%%\n", pcsMode, dcVoltage, dcCurrent, activePower/1000, socActual);
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
    Serial.println("=== PCS Controller ===");

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
    display.drawStr(5, 30, PROG_NAME);
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
