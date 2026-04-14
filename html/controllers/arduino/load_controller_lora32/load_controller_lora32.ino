/*
 * Load Controller for Heltec LoRa32 V2/V3
 * Version: 1.5.4 (03052026_ping_playhead_sync)
 * 
 * Features:
 *   - OLED display showing status, IP, load bar
 *   - WiFi configuration via serial commands
 *   - WebSocket server for browser control
 *   - Dual DAC output (V2 only: GPIO25/26) for analog voltage/current
 *   - LED status indication
 *   - Profile/waveform execution with ramp rates
 *   - Analog inputs (ADC1 channels, usable with WiFi)
 *   - LoRa peer-to-peer (optional)
 *   - Preferences storage for persistent config
 * 
 * Serial Commands:
 *   wifi ssid <name>       - Set WiFi SSID
 *   wifi pass <password>   - Set WiFi password
 *   wifi connect           - Connect to WiFi
 *   wifi status            - Show WiFi status
 *   wifi clear             - Clear saved credentials
 *   set voltage <0-60>     - Set voltage output (V)
 *   set current <0-200>    - Set current output (A)
 *   set dac1 <0-4095>      - Set DAC1 raw value (voltage)
 *   set dac2 <0-4095>      - Set DAC2 raw value (current)
 *   set led <on|off|blink> - LED control
 *   profile start          - Start current profile
 *   profile stop           - Stop profile execution
 *   status                 - Show system status
 *   help                   - Show commands
 * 
 * WebSocket Messages (JSON):
 *   {"cmd": "set_voltage", "value": 48.0}
 *   {"cmd": "set_current", "value": 100.0}
 *   {"cmd": "set_dac", "channel": 0, "value": 2048}
 *   {"cmd": "set_led", "state": "blink"}
 *   {"cmd": "get_status"}
 *   {"cmd": "load_profile", "profile": {...}}
 *   {"cmd": "start_profile", "channel": "voltage"|"current"|"both"}
 *   {"cmd": "stop_profile", "channel": "voltage"|"current"|"both"}
 *   {"cmd": "set_display", "channel": "voltage"|"current"}
 * 
 * Author: BMS Team
 * Date: 2026-02-24
 */

#include <WiFi.h>
#include <WiFiUdp.h>
#include <ArduinoOTA.h>
#include <WebSocketsServer.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <U8g2lib.h>
#include <ArduinoOTA.h>
#include "driver/twai.h"

// Program info
#define PROG_NAME     "LoadCtrl"
#define PROG_VERSION  "1.5.4"

//=============================================================================
// BOARD SELECTION - Uncomment ONE:
//=============================================================================
#define HELTEC_V2
//#define HELTEC_V3

//=============================================================================
// Board-specific pin definitions
//=============================================================================
#ifdef HELTEC_V3
  // Heltec LoRa32 V3 (ESP32-S3) - NO NATIVE DAC!
  #define LED_PIN       35      // Built-in LED
  #define OLED_SDA      17
  #define OLED_SCL      18
  #define OLED_RST      21
  // V3 uses PWM for analog output (requires external RC filter or I2C DAC)
  #define DAC1_PIN      1       // PWM output for voltage (needs RC filter)
  #define DAC2_PIN      2       // PWM output for current (needs RC filter)
  #define USE_PWM_DAC   1       // Flag to use PWM instead of true DAC
  // Analog inputs (ADC1 only - works with WiFi)
  #define ADC1_PIN      4       // ADC input 1
  #define ADC2_PIN      5       // ADC input 2
  // CAN bus pins (directly connected, no conflict)
  #define CAN_TX_PIN    6
  #define CAN_RX_PIN    7
  // GPIO pins
  #define GPIO_IN_COUNT   4
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_IN_PINS[GPIO_IN_COUNT]   = {8, 9, 10, 11};
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {12, 13, 14};
  #warning "Heltec V3 (ESP32-S3) has NO native DAC! Using PWM - add RC filter for true analog output."
#else
  // Heltec LoRa32 V2 (ESP32) - Has true DAC
  #define LED_PIN       2       // Use GPIO2 (external LED recommended, built-in is GPIO25=DAC1)
  #define OLED_SDA      4
  #define OLED_SCL      15
  #define OLED_RST      16
  // True 8-bit DAC outputs
  #define DAC1_PIN      25      // DAC1 - Voltage channel (0-3.3V)
  #define DAC2_PIN      26      // DAC2 - Current channel (0-3.3V)
  #define USE_PWM_DAC   0       // Use true DAC
  // Analog inputs (ADC1 only - works with WiFi)
  // Note: ADC2 (GPIO0,2,4,12-15,25-27) conflicts with WiFi
  #define ADC1_PIN      36      // ADC1_CH0 (SVP) - safe with WiFi
  #define ADC2_PIN      39      // ADC1_CH3 (SVN) - safe with WiFi
  // CAN bus pins (user selectable, avoid OLED/LoRa conflicts)
  // GPIO4 = OLED_SDA on V2, so use different pins
  #define CAN_TX_PIN    21      // Free GPIO for CAN TX
  #define CAN_RX_PIN    22      // Free GPIO for CAN RX
  // GPIO pins
  #define GPIO_IN_COUNT   4
  #define GPIO_OUT_COUNT  3
  const uint8_t GPIO_IN_PINS[GPIO_IN_COUNT]   = {12, 13, 14, 27};
  const uint8_t GPIO_OUT_PINS[GPIO_OUT_COUNT] = {17, 23, 33};  // GPIO22 used for CAN
#endif

// Configuration
#define WS_PORT       81      // WebSocket port
#define SERIAL_BAUD   115200
#define DAC_MAX       4095    // 12-bit internal representation

// PWM configuration for V3 (no native DAC)
#if USE_PWM_DAC
  #define PWM_FREQ      5000    // 5kHz PWM frequency
  #define PWM_RESOLUTION 12     // 12-bit resolution (0-4095)
  #define PWM_CH_DAC1   0       // LEDC channel for DAC1
  #define PWM_CH_DAC2   1       // LEDC channel for DAC2
#endif

// Channel configuration
#define MAX_VOLTAGE   60.0f   // Maximum voltage output (V)
#define MAX_CURRENT   200.0f  // Maximum current output (A)

// WiFi Configuration - EDIT THESE FOR YOUR NETWORK
#define WIFI_SSID     ""              // Your WiFi network name
#define WIFI_PASS     ""              // Your WiFi password

// OTA Configuration
#define OTA_HOSTNAME  "loadctrl"
#define OTA_PORT      3232
#define OTA_PASSWORD  "bms2026"

// Preferences namespace
#define PREF_NAMESPACE "loadctrl"

// OLED Display (128x64, SSD1306)
U8G2_SSD1306_128X64_NONAME_F_HW_I2C display(U8G2_R0, OLED_RST, OLED_SCL, OLED_SDA);

// Global objects
Preferences preferences;
WebSocketsServer webSocket(WS_PORT);

// WiFi credentials
String wifiSSID = WIFI_SSID;
String wifiPass = WIFI_PASS;
bool wifiConnected = false;
bool wsServerStarted = false;  // Track if WebSocket server has been initialized
bool otaInProgress = false;    // Track OTA update status
int otaProgress = 0;           // OTA progress percentage

// LED state
enum LedMode { LED_OFF, LED_ON, LED_BLINK };
LedMode ledMode = LED_OFF;
unsigned long lastLedToggle = 0;
bool ledState = false;

// DAC/Channel state
enum DacChannel { CHANNEL_VOLTAGE = 0, CHANNEL_CURRENT = 1 };
uint16_t dac1Value = 0;       // Voltage DAC (GPIO25)
uint16_t dac2Value = 0;       // Current DAC (GPIO26)
float voltageValue = 0.0f;    // Voltage output (V)
float currentValue = 0.0f;    // Current output (A)
// Display channel (what OLED load bar tracks)
DacChannel displayChannel = CHANNEL_VOLTAGE;

// Message counters
uint32_t wsMessageCount = 0;
uint32_t wsTxCount = 0;
uint8_t wsClientCount = 0;

// GPIO state
uint8_t gpioOutState[GPIO_OUT_COUNT] = {0};  // Current output states
uint8_t gpioInState[GPIO_IN_COUNT] = {0};    // Cached input states

// CAN bus state
#define CAN_RX_QUEUE_SIZE 32
bool canEnabled = false;
bool canInstalled = false;
uint32_t canSpeed = 500;  // kbps (default 500kbps)
uint32_t canTxCount = 0;
uint32_t canRxCount = 0;
uint32_t canErrorCount = 0;

// CAN receive buffer (circular)
#define CAN_RX_BUFFER_SIZE 16
struct CanMessage {
    uint32_t id;
    uint8_t len;
    uint8_t data[8];
    bool extended;
    unsigned long timestamp;
};
CanMessage canRxBuffer[CAN_RX_BUFFER_SIZE];
volatile uint8_t canRxHead = 0;
volatile uint8_t canRxTail = 0;

// Display update
unsigned long lastDisplayUpdate = 0;
#define DISPLAY_UPDATE_MS 250

// Profile execution
struct ProfileStep {
    String name;
    float value;          // Generic value (V for voltage, A for current)
    uint32_t durationMs;
    uint32_t rampMs;      // Ramp time to reach this value
};

#define MAX_PROFILE_STEPS 20

// Voltage profile
ProfileStep profileV[MAX_PROFILE_STEPS];
int profileVCount = 0;
bool profileVRunning = false;
int profileVStep = 0;
unsigned long profileVStepStart = 0;
float profileVStepStartVal = 0;
bool profileVInRamp = false;

// Current profile
ProfileStep profileC[MAX_PROFILE_STEPS];
int profileCCount = 0;
bool profileCRunning = false;
int profileCStep = 0;
unsigned long profileCStepStart = 0;
float profileCStepStartVal = 0;
bool profileCInRamp = false;

// Serial command buffer
String serialBuffer = "";

//=============================================================================
// Setup
//=============================================================================
void setup() {
    Serial.begin(SERIAL_BAUD);
    delay(1000);
    
    Serial.println("\n==================================");
    Serial.println("  Load Controller - LoRa32");
    Serial.println("==================================");
    
    // Initialize OLED display
    display.begin();
    display.setFont(u8g2_font_6x10_tf);
    displaySplash();
    
    // Initialize pins
    pinMode(LED_PIN, OUTPUT);
    digitalWrite(LED_PIN, LOW);
    
    // Initialize DAC outputs (true DAC on V2, PWM on V3)
#if USE_PWM_DAC
    // V3: Setup PWM channels for analog output
    ledcSetup(PWM_CH_DAC1, PWM_FREQ, PWM_RESOLUTION);
    ledcSetup(PWM_CH_DAC2, PWM_FREQ, PWM_RESOLUTION);
    ledcAttachPin(DAC1_PIN, PWM_CH_DAC1);
    ledcAttachPin(DAC2_PIN, PWM_CH_DAC2);
    ledcWrite(PWM_CH_DAC1, 0);
    ledcWrite(PWM_CH_DAC2, 0);
    Serial.println("PWM DAC initialized (V3 mode - add RC filter!)");
#else
    // V2: True DAC outputs
    dacWrite(DAC1_PIN, 0);  // Voltage channel
    dacWrite(DAC2_PIN, 0);  // Current channel
    Serial.println("True DAC initialized (V2 mode)");
#endif
    
    // Initialize analog inputs
    pinMode(ADC1_PIN, INPUT);
    pinMode(ADC2_PIN, INPUT);
    analogReadResolution(12);  // 12-bit ADC (0-4095)
    
    // Initialize GPIO inputs (with pull-up)
    for (int i = 0; i < GPIO_IN_COUNT; i++) {
        pinMode(GPIO_IN_PINS[i], INPUT_PULLUP);
    }
    
    // Initialize GPIO outputs
    for (int i = 0; i < GPIO_OUT_COUNT; i++) {
        pinMode(GPIO_OUT_PINS[i], OUTPUT);
        digitalWrite(GPIO_OUT_PINS[i], LOW);
        gpioOutState[i] = 0;
    }
    
    // Load saved preferences (use #define constants as fallback)
    preferences.begin(PREF_NAMESPACE, false);
    wifiSSID = preferences.getString("ssid", WIFI_SSID);
    wifiPass = preferences.getString("pass", WIFI_PASS);
    
    Serial.print("SSID: ");
    Serial.println(wifiSSID.length() > 0 ? wifiSSID : "(none)");
    
    // Auto-connect if credentials available
    if (wifiSSID.length() > 0) {
        connectWiFi();
    } else {
        Serial.println("\nNo WiFi configured. Use serial commands:");
        Serial.println("  wifi ssid <name>");
        Serial.println("  wifi pass <password>");
        Serial.println("  wifi connect");
    }
    
    // NOTE: WebSocket server is started AFTER WiFi connects (in connectWiFi())
    // Starting it before WiFi causes LWIP crash: "Invalid mbox"
    
    Serial.println("\nType 'help' for commands");
    Serial.println("==================================");
    
    // Initial display update
    updateDisplay();
}

//=============================================================================
// Main Loop
//=============================================================================
void loop() {
    // Handle ArduinoOTA (must be called frequently)
    if (wifiConnected) {
        ArduinoOTA.handle();
        
        // Skip other processing during OTA update
        if (otaInProgress) {
            delay(10);
            return;
        }
    }
    
    // Handle serial commands
    handleSerial();
    
    // Handle WebSocket and OTA (only if WiFi connected and server started)
    if (wifiConnected && wsServerStarted) {
        webSocket.loop();
        ArduinoOTA.handle();
    }
    
    // Handle LED blinking
    handleLED();
    
    // Handle profile execution (both channels independently)
    handleProfileV();
    handleProfileC();
    
    // Poll CAN bus for received messages
    pollCAN();
    
    // Update display, GPIO inputs, and broadcast status periodically
    if (millis() - lastDisplayUpdate > DISPLAY_UPDATE_MS) {
        updateGpioInputs();
        updateDisplay();
        broadcastStatus();
        // Refresh DAC registers — ESP32 WiFi activity can glitch DAC output
        writeDacHardware(0, dac1Value);
        writeDacHardware(1, dac2Value);
        lastDisplayUpdate = millis();
    }
    
    // Brief delay
    delay(10);
}

//=============================================================================
// OLED Display Functions
//=============================================================================
void displaySplash() {
    display.clearBuffer();
    display.setFont(u8g2_font_helvB12_tf);
    display.drawStr(20, 25, PROG_NAME);
    display.setFont(u8g2_font_6x10_tf);
    display.drawStr(35, 40, "v");
    display.drawStr(42, 40, PROG_VERSION);
    display.drawStr(25, 55, "Starting...");
    display.sendBuffer();
    delay(1500);
}

void updateDisplay() {
    display.clearBuffer();
    
    // Line 1: Program name & version (bold font)
    display.setFont(u8g2_font_helvB08_tf);
    display.drawStr(0, 9, PROG_NAME);
    display.setFont(u8g2_font_5x7_tf);
    display.drawStr(55, 9, "v");
    display.drawStr(60, 9, PROG_VERSION);
    
    // Connection indicator (right side)
    if (wifiConnected) {
        display.drawDisc(120, 5, 4);  // Filled circle = connected
    } else {
        display.drawCircle(120, 5, 4);  // Empty circle = disconnected
    }
    
    // Line 2: WiFi IP / Status
    display.setFont(u8g2_font_5x7_tf);
    display.drawStr(0, 19, "IP:");
    if (wifiConnected) {
        display.drawStr(18, 19, WiFi.localIP().toString().c_str());
        display.drawStr(90, 19, ":");
        char portStr[6];
        sprintf(portStr, "%d", WS_PORT);
        display.drawStr(95, 19, portStr);
    } else {
        display.drawStr(18, 19, "Not connected");
    }
    
    // Line 3: Connection status & client count
    display.drawStr(0, 29, "WS:");
    if (wsClientCount > 0) {
        char clientStr[16];
        sprintf(clientStr, "%d client%s", wsClientCount, wsClientCount > 1 ? "s" : "");
        display.drawStr(18, 29, clientStr);
    } else {
        display.drawStr(18, 29, "No clients");
    }
    
    // Line 4: Message counts
    display.drawStr(0, 39, "Rx:");
    char rxStr[12];
    sprintf(rxStr, "%lu", wsMessageCount);
    display.drawStr(18, 39, rxStr);
    display.drawStr(60, 39, "Tx:");
    char txStr[12];
    sprintf(txStr, "%lu", wsTxCount);
    display.drawStr(78, 39, txStr);
    
    // Line 5: Power value
    // Line 5: Voltage and Current values
    display.setFont(u8g2_font_helvB08_tf);
    char valStr[24];
    sprintf(valStr, "%.1fV  %.1fA", voltageValue, currentValue);
    display.drawStr(0, 51, valStr);
    
    // Profile status (right side)
    display.setFont(u8g2_font_5x7_tf);
    if (profileVRunning || profileCRunning) {
        char stepStr[12];
        sprintf(stepStr, "[%s]", displayChannel == CHANNEL_VOLTAGE ? "V" : "A");
        display.drawStr(100, 51, stepStr);
    }

    // Bottom: Load bar (based on display channel)
    int barY = 55;
    int barHeight = 8;
    int barWidth = 126;
    float maxVal = (displayChannel == CHANNEL_VOLTAGE) ? MAX_VOLTAGE : MAX_CURRENT;
    float curVal = (displayChannel == CHANNEL_VOLTAGE) ? voltageValue : currentValue;
    int loadPercent = (int)(curVal / maxVal * 100);
    int fillWidth = (int)(curVal / maxVal * barWidth);
    
    // Bar outline
    display.drawFrame(0, barY, barWidth, barHeight);
    
    // Filled portion
    if (fillWidth > 0) {
        display.drawBox(1, barY + 1, fillWidth - 1, barHeight - 2);
    }
    
    // Percentage text (right of bar or inside if fits)
    char pctStr[6];
    sprintf(pctStr, "%d%%", loadPercent);
    display.setFont(u8g2_font_5x7_tf);
    // Draw percentage inside bar if > 30%, otherwise outside
    if (loadPercent > 30) {
        display.setDrawColor(0);  // Black text on white fill
        display.drawStr(fillWidth - 18, barY + 7, pctStr);
        display.setDrawColor(1);
    } else {
        display.drawStr(fillWidth + 3, barY + 7, pctStr);
    }
    
    display.sendBuffer();
}

//=============================================================================
// ArduinoOTA Setup
//=============================================================================
void setupOTA() {
    ArduinoOTA.setHostname(OTA_HOSTNAME);
    ArduinoOTA.setPort(OTA_PORT);
    ArduinoOTA.setPassword(OTA_PASSWORD);
    
    ArduinoOTA.onStart([]() {
        String type = (ArduinoOTA.getCommand() == U_FLASH) ? "sketch" : "filesystem";
        Serial.println("\n*** OTA Update Started ***");
        Serial.print("Type: ");
        Serial.println(type);
        otaInProgress = true;
        otaProgress = 0;
        
        // Show OTA on display
        display.clearBuffer();
        display.setFont(u8g2_font_helvB12_tf);
        display.drawStr(20, 25, "OTA UPDATE");
        display.setFont(u8g2_font_6x10_tf);
        display.drawStr(15, 45, "Starting...");
        display.sendBuffer();
    });
    
    ArduinoOTA.onEnd([]() {
        Serial.println("\n*** OTA Update Complete ***");
        Serial.println("Rebooting...");
        otaInProgress = false;
        
        display.clearBuffer();
        display.setFont(u8g2_font_helvB12_tf);
        display.drawStr(20, 25, "OTA UPDATE");
        display.setFont(u8g2_font_6x10_tf);
        display.drawStr(15, 45, "Complete!");
        display.drawStr(15, 58, "Rebooting...");
        display.sendBuffer();
    });
    
    ArduinoOTA.onProgress([](unsigned int progress, unsigned int total) {
        otaProgress = (progress / (total / 100));
        Serial.printf("Progress: %u%%\r", otaProgress);
        
        // Update display with progress bar
        display.clearBuffer();
        display.setFont(u8g2_font_helvB12_tf);
        display.drawStr(20, 20, "OTA UPDATE");
        display.setFont(u8g2_font_6x10_tf);
        
        // Progress percentage
        char buf[16];
        snprintf(buf, sizeof(buf), "%d%%", otaProgress);
        display.drawStr(55, 38, buf);
        
        // Progress bar
        int barWidth = (otaProgress * 100) / 100;
        display.drawFrame(10, 45, 108, 12);
        display.drawBox(12, 47, barWidth, 8);
        
        display.sendBuffer();
    });
    
    ArduinoOTA.onError([](ota_error_t error) {
        otaInProgress = false;
        Serial.printf("\n*** OTA Error[%u]: ", error);
        String errMsg = "Unknown";
        switch (error) {
            case OTA_AUTH_ERROR:    errMsg = "Auth Failed"; break;
            case OTA_BEGIN_ERROR:   errMsg = "Begin Failed"; break;
            case OTA_CONNECT_ERROR: errMsg = "Connect Failed"; break;
            case OTA_RECEIVE_ERROR: errMsg = "Receive Failed"; break;
            case OTA_END_ERROR:     errMsg = "End Failed"; break;
        }
        Serial.println(errMsg);
        
        display.clearBuffer();
        display.setFont(u8g2_font_helvB12_tf);
        display.drawStr(20, 25, "OTA ERROR");
        display.setFont(u8g2_font_6x10_tf);
        display.drawStr(15, 45, errMsg.c_str());
        display.sendBuffer();
        delay(2000);
    });
    
    ArduinoOTA.begin();
    
    Serial.println("ArduinoOTA initialized");
    Serial.print("OTA Hostname: ");
    Serial.println(OTA_HOSTNAME);
    Serial.print("OTA Port: ");
    Serial.println(OTA_PORT);
}

//=============================================================================
// WiFi Functions
//=============================================================================
void connectWiFi() {
    if (wifiSSID.length() == 0) {
        Serial.println("ERROR: No SSID configured");
        return;
    }
    
    Serial.print("Connecting to: ");
    Serial.println(wifiSSID);
    
    WiFi.begin(wifiSSID.c_str(), wifiPass.c_str());
    
    int timeout = 20;  // 10 seconds
    while (WiFi.status() != WL_CONNECTED && timeout > 0) {
        delay(500);
        Serial.print(".");
        timeout--;
    }
    
    if (WiFi.status() == WL_CONNECTED) {
        wifiConnected = true;
        Serial.println("\n\n*** WiFi Connected! ***");
        Serial.print("IP Address: ");
        Serial.println(WiFi.localIP());
        Serial.print("WebSocket: ws://");
        Serial.print(WiFi.localIP());
        Serial.print(":");
        Serial.println(WS_PORT);
        
        // Start WebSocket server AFTER WiFi is connected
        // (Starting before causes LWIP "Invalid mbox" crash)
        if (!wsServerStarted) {
            webSocket.begin();
            webSocket.onEvent(webSocketEvent);
            wsServerStarted = true;
            Serial.println("WebSocket server started");
            
            // Initialize OTA updates
            ArduinoOTA.setHostname("load-controller");
            ArduinoOTA.setPassword("bms2026");
            ArduinoOTA.onStart([]() {
                Serial.println("OTA Update starting...");
            });
            ArduinoOTA.onEnd([]() {
                Serial.println("\nOTA Update complete!");
            });
            ArduinoOTA.onProgress([](unsigned int progress, unsigned int total) {
                Serial.printf("OTA Progress: %u%%\r", (progress / (total / 100)));
            });
            ArduinoOTA.onError([](ota_error_t error) {
                Serial.printf("OTA Error[%u]: ", error);
                if (error == OTA_AUTH_ERROR) Serial.println("Auth Failed");
                else if (error == OTA_BEGIN_ERROR) Serial.println("Begin Failed");
                else if (error == OTA_CONNECT_ERROR) Serial.println("Connect Failed");
                else if (error == OTA_RECEIVE_ERROR) Serial.println("Receive Failed");
                else if (error == OTA_END_ERROR) Serial.println("End Failed");
            });
            ArduinoOTA.begin();
            Serial.println("OTA updates enabled (password: bms2026)");
        }
        
        ledMode = LED_ON;
    } else {
        wifiConnected = false;
        Serial.println("\nWiFi connection failed!");
        ledMode = LED_BLINK;
    }
}

void printWiFiStatus() {
    Serial.println("\n--- WiFi Status ---");
    Serial.print("SSID: ");
    Serial.println(wifiSSID.length() > 0 ? wifiSSID : "(not set)");
    Serial.print("Connected: ");
    Serial.println(wifiConnected ? "Yes" : "No");
    if (wifiConnected) {
        Serial.print("IP: ");
        Serial.println(WiFi.localIP());
        Serial.print("RSSI: ");
        Serial.print(WiFi.RSSI());
        Serial.println(" dBm");
    }
    Serial.println("-------------------");
}

//=============================================================================
// Serial Command Handler
//=============================================================================
void handleSerial() {
    while (Serial.available()) {
        char c = Serial.read();
        if (c == '\n' || c == '\r') {
            if (serialBuffer.length() > 0) {
                processCommand(serialBuffer);
                serialBuffer = "";
            }
        } else {
            serialBuffer += c;
        }
    }
}

void processCommand(String cmd) {
    cmd.trim();
    Serial.print("> ");
    Serial.println(cmd);
    
    if (cmd.startsWith("wifi ")) {
        handleWifiCommand(cmd.substring(5));
    } else if (cmd.startsWith("set ")) {
        handleSetCommand(cmd.substring(4));
    } else if (cmd.startsWith("profile ")) {
        handleProfileCommand(cmd.substring(8));
    } else if (cmd.startsWith("gpio ")) {
        handleGpioCommand(cmd.substring(5));
    } else if (cmd == "gpio") {
        printGpioStatus();
    } else if (cmd.startsWith("can ")) {
        handleCanCommand(cmd.substring(4));
    } else if (cmd == "can") {
        printCanStatus();
    } else if (cmd == "status") {
        printStatus();
    } else if (cmd == "help") {
        printHelp();
    } else {
        Serial.println("Unknown command. Type 'help' for list.");
    }
}

void handleCanCommand(String args) {
    args.trim();
    
    if (args.startsWith("init ") || args.startsWith("start ")) {
        // can init <speed_kbps>
        uint32_t speed = args.substring(args.indexOf(' ') + 1).toInt();
        if (speed == 0) speed = 500;
        initCAN(speed);
    } else if (args == "init" || args == "start") {
        initCAN(500);  // Default 500 kbps
    } else if (args == "stop") {
        stopCAN();
    } else if (args.startsWith("send ")) {
        // can send <id> <hex_data>
        // Example: can send 0x123 AABBCCDD
        String rest = args.substring(5);
        rest.trim();
        int spaceIdx = rest.indexOf(' ');
        if (spaceIdx > 0) {
            String idStr = rest.substring(0, spaceIdx);
            String dataStr = rest.substring(spaceIdx + 1);
            dataStr.replace(" ", "");
            
            uint32_t id;
            bool extended = false;
            if (idStr.startsWith("0x") || idStr.startsWith("0X")) {
                id = strtoul(idStr.c_str(), NULL, 16);
            } else {
                id = idStr.toInt();
            }
            if (id > 0x7FF) extended = true;  // Extended frame if > 11 bits
            
            uint8_t data[8];
            uint8_t len = 0;
            for (int i = 0; i < dataStr.length() && len < 8; i += 2) {
                data[len++] = strtol(dataStr.substring(i, i+2).c_str(), NULL, 16);
            }
            
            sendCAN(id, data, len, extended);
        } else {
            Serial.println("Usage: can send <id> <hex_data>");
            Serial.println("  Example: can send 0x123 AABBCCDD");
        }
    } else if (args == "status") {
        printCanStatus();
    } else if (args == "read" || args == "rx") {
        // Print buffered CAN messages
        if (canRxHead == canRxTail) {
            Serial.println("CAN RX buffer empty");
        } else {
            Serial.println("CAN RX Buffer:");
            while (canRxTail != canRxHead) {
                CanMessage& m = canRxBuffer[canRxTail];
                Serial.printf("  ID=0x%X %s len=%d data=", 
                    m.id, m.extended ? "EXT" : "STD", m.len);
                for (int i = 0; i < m.len; i++) {
                    Serial.printf("%02X ", m.data[i]);
                }
                Serial.printf("@%lu\n", m.timestamp);
                canRxTail = (canRxTail + 1) % CAN_RX_BUFFER_SIZE;
            }
        }
    } else {
        Serial.println("CAN Commands:");
        Serial.println("  can init [speed]   - Initialize CAN (default 500 kbps)");
        Serial.println("  can stop           - Stop CAN driver");
        Serial.println("  can send <id> <hex>- Send CAN message");
        Serial.println("  can read           - Read buffered messages");
        Serial.println("  can status         - Show CAN status");
    }
}

void printCanStatus() {
    Serial.println("=== CAN Status ===");
    Serial.printf("  Enabled:    %s\n", canEnabled ? "Yes" : "No");
    Serial.printf("  Speed:      %d kbps\n", canSpeed);
    Serial.printf("  TX Count:   %d\n", canTxCount);
    Serial.printf("  RX Count:   %d\n", canRxCount);
    Serial.printf("  Errors:     %d\n", canErrorCount);
    int pending = (canRxHead >= canRxTail) ? 
                  (canRxHead - canRxTail) : 
                  (CAN_RX_BUFFER_SIZE - canRxTail + canRxHead);
    Serial.printf("  RX Pending: %d\n", pending);
    Serial.printf("  TX Pin:     GPIO%d\n", CAN_TX_PIN);
    Serial.printf("  RX Pin:     GPIO%d\n", CAN_RX_PIN);
}

void handleGpioCommand(String args) {
    args.trim();
    
    if (args.startsWith("out ")) {
        // gpio out <index> <0|1>
        String rest = args.substring(4);
        rest.trim();
        int spaceIdx = rest.indexOf(' ');
        if (spaceIdx > 0) {
            int index = rest.substring(0, spaceIdx).toInt();
            int state = rest.substring(spaceIdx + 1).toInt();
            setGpioOutput(index, state != 0);
        } else {
            Serial.println("Usage: gpio out <index> <0|1>");
        }
    } else if (args == "status" || args == "") {
        printGpioStatus();
    } else {
        Serial.println("Usage: gpio [out <idx> <0|1>|status]");
    }
}

void handleWifiCommand(String args) {
    args.trim();
    
    if (args.startsWith("ssid ")) {
        wifiSSID = args.substring(5);
        preferences.putString("ssid", wifiSSID);
        Serial.print("SSID set to: ");
        Serial.println(wifiSSID);
    } else if (args.startsWith("pass ")) {
        wifiPass = args.substring(5);
        preferences.putString("pass", wifiPass);
        Serial.println("Password saved");
    } else if (args == "connect") {
        connectWiFi();
    } else if (args == "status") {
        printWiFiStatus();
    } else if (args == "clear") {
        preferences.clear();
        wifiSSID = "";
        wifiPass = "";
        Serial.println("WiFi credentials cleared");
    } else {
        Serial.println("Usage: wifi [ssid|pass|connect|status|clear]");
    }
}

void handleSetCommand(String args) {
    args.trim();
    
    if (args.startsWith("dac1 ") || args.startsWith("voltage ")) {
        // set dac1 <0-4095> OR set voltage <0-60>
        String val = args.startsWith("dac1 ") ? args.substring(5) : args.substring(8);
        if (args.startsWith("dac1 ")) {
            setDAC(0, val.toInt());
        } else {
            setVoltage(val.toFloat());
        }
    } else if (args.startsWith("dac2 ") || args.startsWith("current ")) {
        // set dac2 <0-4095> OR set current <0-200>
        String val = args.startsWith("dac2 ") ? args.substring(5) : args.substring(8);
        if (args.startsWith("dac2 ")) {
            setDAC(1, val.toInt());
        } else {
            setCurrent(val.toFloat());
        }
    } else if (args.startsWith("led ")) {
        String mode = args.substring(4);
        if (mode == "on") {
            ledMode = LED_ON;
            Serial.println("LED: ON");
        } else if (mode == "off") {
            ledMode = LED_OFF;
            Serial.println("LED: OFF");
        } else if (mode == "blink") {
            ledMode = LED_BLINK;
            Serial.println("LED: BLINK");
        }
    } else {
        Serial.println("Usage: set [dac1|dac2|voltage|current|led]");
        Serial.println("  set voltage <0-60>   - Set voltage (V)");
        Serial.println("  set current <0-200>  - Set current (A)");
        Serial.println("  set dac1 <0-4095>    - Raw DAC1 value");
        Serial.println("  set dac2 <0-4095>    - Raw DAC2 value");
    }
}

void handleProfileCommand(String args) {
    args.trim();

    if (args == "start") {
        startProfile(CHANNEL_VOLTAGE);
        startProfile(CHANNEL_CURRENT);
    } else if (args == "stop") {
        stopProfile(CHANNEL_VOLTAGE);
        stopProfile(CHANNEL_CURRENT);
    } else if (args == "status") {
        printProfileStatus();
    } else {
        Serial.println("Usage: profile [start|stop|status]");
    }
}

//=============================================================================
// DAC / Power Control
//=============================================================================
// Helper: Write to DAC hardware (handles V2 true DAC vs V3 PWM)
void writeDacHardware(int channel, uint16_t value12bit) {
#if USE_PWM_DAC
    // V3: PWM output (12-bit directly)
    if (channel == 0) {
        ledcWrite(PWM_CH_DAC1, value12bit);
    } else {
        ledcWrite(PWM_CH_DAC2, value12bit);
    }
#else
    // V2: True DAC (8-bit, so shift down)
    if (channel == 0) {
        dacWrite(DAC1_PIN, value12bit >> 4);
    } else {
        dacWrite(DAC2_PIN, value12bit >> 4);
    }
#endif
}

// Set voltage DAC (DAC1) — no broadcast/serial here; called at 100Hz during ramp
void setVoltage(float volts) {
    voltageValue = constrain(volts, 0, MAX_VOLTAGE);
    dac1Value = (uint16_t)(voltageValue / MAX_VOLTAGE * DAC_MAX);
    writeDacHardware(0, dac1Value);
}

// Set current DAC (DAC2) — no broadcast/serial here; called at 100Hz during ramp
void setCurrent(float amps) {
    currentValue = constrain(amps, 0, MAX_CURRENT);
    dac2Value = (uint16_t)(currentValue / MAX_CURRENT * DAC_MAX);
    writeDacHardware(1, dac2Value);
}

// Set DAC by channel (0=voltage/dac1Value, 1=current/dac2Value)
void setDAC(int channel, int value) {
    value = constrain(value, 0, DAC_MAX);
    if (channel == 0) {
        dac1Value = value;
        writeDacHardware(0, dac1Value);
        voltageValue = (float)dac1Value / DAC_MAX * MAX_VOLTAGE;
        Serial.printf("dac1Value: %d (%.1f V)\n", dac1Value, voltageValue);
    } else {
        dac2Value = value;
        writeDacHardware(1, dac2Value);
        currentValue = (float)dac2Value / DAC_MAX * MAX_CURRENT;
        Serial.printf("dac2Value: %d (%.1f A)\n", dac2Value, currentValue);
    }
    broadcastStatus();
}


//=============================================================================
// Analog Input Reading
//=============================================================================
uint16_t readAnalogInput(int channel) {
    if (channel == 0) {
        return analogRead(ADC1_PIN);
    } else {
        return analogRead(ADC2_PIN);
    }
}

float readAnalogVoltage(int channel) {
    // Convert 12-bit ADC to voltage (0-3.3V)
    return readAnalogInput(channel) * 3.3f / 4095.0f;
}

//=============================================================================
// CAN Bus Functions
//=============================================================================
twai_timing_config_t getCanTimingConfig(uint32_t speedKbps) {
    switch (speedKbps) {
        case 1000: return TWAI_TIMING_CONFIG_1MBITS();
        case 800:  return TWAI_TIMING_CONFIG_800KBITS();
        case 500:  return TWAI_TIMING_CONFIG_500KBITS();
        case 250:  return TWAI_TIMING_CONFIG_250KBITS();
        case 125:  return TWAI_TIMING_CONFIG_125KBITS();
        case 100:  return TWAI_TIMING_CONFIG_100KBITS();
        case 50:   return TWAI_TIMING_CONFIG_50KBITS();
        case 25:   return TWAI_TIMING_CONFIG_25KBITS();
        default:   return TWAI_TIMING_CONFIG_500KBITS();
    }
}

bool initCAN(uint32_t speedKbps) {
    // Stop existing driver if running
    if (canInstalled) {
        twai_stop();
        twai_driver_uninstall();
        canInstalled = false;
    }
    
    canSpeed = speedKbps;
    
    // Configure TWAI driver
    twai_general_config_t g_config = TWAI_GENERAL_CONFIG_DEFAULT(
        (gpio_num_t)CAN_TX_PIN, 
        (gpio_num_t)CAN_RX_PIN, 
        TWAI_MODE_NORMAL
    );
    g_config.rx_queue_len = CAN_RX_QUEUE_SIZE;
    g_config.tx_queue_len = 5;
    
    twai_timing_config_t t_config = getCanTimingConfig(speedKbps);
    twai_filter_config_t f_config = TWAI_FILTER_CONFIG_ACCEPT_ALL();
    
    // Install driver
    esp_err_t result = twai_driver_install(&g_config, &t_config, &f_config);
    if (result != ESP_OK) {
        Serial.printf("CAN driver install failed: %d\n", result);
        return false;
    }
    
    // Start driver
    result = twai_start();
    if (result != ESP_OK) {
        Serial.printf("CAN start failed: %d\n", result);
        twai_driver_uninstall();
        return false;
    }
    
    canInstalled = true;
    canEnabled = true;
    canTxCount = 0;
    canRxCount = 0;
    canErrorCount = 0;
    
    Serial.printf("CAN initialized: %d kbps (TX=GPIO%d, RX=GPIO%d)\n", 
                  speedKbps, CAN_TX_PIN, CAN_RX_PIN);
    return true;
}

void stopCAN() {
    if (canInstalled) {
        twai_stop();
        twai_driver_uninstall();
        canInstalled = false;
        canEnabled = false;
        Serial.println("CAN stopped");
    }
}

bool sendCAN(uint32_t id, const uint8_t* data, uint8_t len, bool extended) {
    if (!canEnabled || !canInstalled) {
        Serial.println("CAN not enabled");
        return false;
    }
    
    twai_message_t msg;
    msg.identifier = id;
    msg.data_length_code = len > 8 ? 8 : len;
    msg.extd = extended ? 1 : 0;
    msg.rtr = 0;
    msg.ss = 0;
    msg.self = 0;
    msg.dlc_non_comp = 0;
    
    for (int i = 0; i < msg.data_length_code; i++) {
        msg.data[i] = data[i];
    }
    
    esp_err_t result = twai_transmit(&msg, pdMS_TO_TICKS(100));
    if (result == ESP_OK) {
        canTxCount++;
        Serial.printf("CAN TX: ID=0x%X len=%d\n", id, len);
        return true;
    } else {
        canErrorCount++;
        Serial.printf("CAN TX failed: %d\n", result);
        return false;
    }
}

void pollCAN() {
    if (!canEnabled || !canInstalled) return;
    
    twai_message_t msg;
    while (twai_receive(&msg, 0) == ESP_OK) {
        // Store in circular buffer
        uint8_t nextHead = (canRxHead + 1) % CAN_RX_BUFFER_SIZE;
        if (nextHead != canRxTail) {  // Buffer not full
            canRxBuffer[canRxHead].id = msg.identifier;
            canRxBuffer[canRxHead].len = msg.data_length_code;
            canRxBuffer[canRxHead].extended = msg.extd;
            canRxBuffer[canRxHead].timestamp = millis();
            for (int i = 0; i < msg.data_length_code; i++) {
                canRxBuffer[canRxHead].data[i] = msg.data[i];
            }
            canRxHead = nextHead;
            canRxCount++;
        }
    }
}

// Get CAN status for status messages
void getCanStatus(JsonObject& can) {
    can["enabled"] = canEnabled;
    can["speed_kbps"] = canSpeed;
    can["tx_count"] = canTxCount;
    can["rx_count"] = canRxCount;
    can["error_count"] = canErrorCount;
    can["rx_pending"] = (canRxHead >= canRxTail) ? 
                        (canRxHead - canRxTail) : 
                        (CAN_RX_BUFFER_SIZE - canRxTail + canRxHead);
}

// Send CAN RX buffer contents via WebSocket
void sendCanMessages(uint8_t clientNum) {
    StaticJsonDocument<1024> doc;
    doc["type"] = "can_messages";
    
    JsonArray messages = doc.createNestedArray("messages");
    int count = 0;
    
    while (canRxTail != canRxHead && count < 10) {
        CanMessage& m = canRxBuffer[canRxTail];
        JsonObject msg = messages.createNestedObject();
        
        msg["id"] = m.id;
        msg["extended"] = m.extended;
        msg["len"] = m.len;
        msg["timestamp"] = m.timestamp;
        
        // Data as hex string
        char hexData[24];
        int pos = 0;
        for (int i = 0; i < m.len; i++) {
            pos += sprintf(hexData + pos, "%02X", m.data[i]);
            if (i < m.len - 1) hexData[pos++] = ' ';
        }
        hexData[pos] = '\0';
        msg["data"] = hexData;
        
        // Also provide raw array
        JsonArray dataArr = msg.createNestedArray("raw");
        for (int i = 0; i < m.len; i++) {
            dataArr.add(m.data[i]);
        }
        
        canRxTail = (canRxTail + 1) % CAN_RX_BUFFER_SIZE;
        count++;
    }
    
    doc["count"] = count;
    doc["total_rx"] = canRxCount;
    
    String json;
    serializeJson(doc, json);
    webSocket.sendTXT(clientNum, json);
    wsTxCount++;
}

// Broadcast CAN message to all WebSocket clients
void broadcastCanMessage(const CanMessage& m) {
    StaticJsonDocument<256> doc;
    doc["type"] = "can_rx";
    doc["id"] = m.id;
    doc["extended"] = m.extended;
    doc["len"] = m.len;
    doc["timestamp"] = m.timestamp;
    
    char hexData[24];
    int pos = 0;
    for (int i = 0; i < m.len; i++) {
        pos += sprintf(hexData + pos, "%02X", m.data[i]);
        if (i < m.len - 1) hexData[pos++] = ' ';
    }
    hexData[pos] = '\0';
    doc["data"] = hexData;
    
    JsonArray dataArr = doc.createNestedArray("raw");
    for (int i = 0; i < m.len; i++) {
        dataArr.add(m.data[i]);
    }
    
    String json;
    serializeJson(doc, json);
    webSocket.broadcastTXT(json);
    wsTxCount += wsClientCount;
}

//=============================================================================
// LED Control
//=============================================================================
void handleLED() {
    switch (ledMode) {
        case LED_OFF:
            digitalWrite(LED_PIN, LOW);
            break;
        case LED_ON:
            digitalWrite(LED_PIN, HIGH);
            break;
        case LED_BLINK:
            if (millis() - lastLedToggle > 500) {
                ledState = !ledState;
                digitalWrite(LED_PIN, ledState);
                lastLedToggle = millis();
            }
            break;
    }
}

//=============================================================================
// Profile Execution
//=============================================================================
void startProfile(DacChannel ch) {
    if (ch == CHANNEL_VOLTAGE) {
        if (profileVCount == 0) { Serial.println("No voltage profile loaded"); return; }
        profileVRunning = true;
        profileVStep = 0;
        profileVStepStart = millis();
        profileVStepStartVal = voltageValue;
        profileVInRamp = (profileV[0].rampMs > 0);
        if (!profileVInRamp) { setVoltage(profileV[0].value); }
        Serial.printf("Voltage profile started - Step: %s\n", profileV[0].name.c_str());
    } else {
        if (profileCCount == 0) { Serial.println("No current profile loaded"); return; }
        profileCRunning = true;
        profileCStep = 0;
        profileCStepStart = millis();
        profileCStepStartVal = currentValue;
        profileCInRamp = (profileC[0].rampMs > 0);
        if (!profileCInRamp) { setCurrent(profileC[0].value); }
        Serial.printf("Current profile started - Step: %s\n", profileC[0].name.c_str());
    }
    broadcastStatus();
}

void stopProfile(DacChannel ch) {
    if (ch == CHANNEL_VOLTAGE) {
        profileVRunning = false;
        Serial.println("Voltage profile stopped");
    } else {
        profileCRunning = false;
        Serial.println("Current profile stopped");
    }
    broadcastStatus();
}

void handleProfileV() {
    if (!profileVRunning || profileVCount == 0) return;

    unsigned long now = millis();
    unsigned long elapsed = now - profileVStepStart;
    ProfileStep& step = profileV[profileVStep];

    if (profileVInRamp) {
        if (elapsed >= step.rampMs) {
            setVoltage(step.value);
            profileVInRamp = false;
            profileVStepStart = now;
        } else {
            float progress = (float)elapsed / step.rampMs;
            setVoltage(profileVStepStartVal + (step.value - profileVStepStartVal) * progress);
        }
    } else {
        if (elapsed >= step.durationMs) {
            profileVStep++;
            if (profileVStep >= profileVCount) profileVStep = 0;  // Loop
            profileVStepStartVal = voltageValue;
            profileVStepStart = now;
            profileVInRamp = (profileV[profileVStep].rampMs > 0);
            if (!profileVInRamp) { setVoltage(profileV[profileVStep].value); }
            Serial.printf("V Step: %s\n", profileV[profileVStep].name.c_str());
        }
    }
}

void handleProfileC() {
    if (!profileCRunning || profileCCount == 0) return;

    unsigned long now = millis();
    unsigned long elapsed = now - profileCStepStart;
    ProfileStep& step = profileC[profileCStep];

    if (profileCInRamp) {
        if (elapsed >= step.rampMs) {
            setCurrent(step.value);
            profileCInRamp = false;
            profileCStepStart = now;
        } else {
            float progress = (float)elapsed / step.rampMs;
            setCurrent(profileCStepStartVal + (step.value - profileCStepStartVal) * progress);
        }
    } else {
        if (elapsed >= step.durationMs) {
            profileCStep++;
            if (profileCStep >= profileCCount) profileCStep = 0;  // Loop
            profileCStepStartVal = currentValue;
            profileCStepStart = now;
            profileCInRamp = (profileC[profileCStep].rampMs > 0);
            if (!profileCInRamp) { setCurrent(profileC[profileCStep].value); }
            Serial.printf("C Step: %s\n", profileC[profileCStep].name.c_str());
        }
    }
}

void printProfileStatus() {
    Serial.println("\n--- Profile Status ---");
    Serial.printf("Voltage: %d steps, %s", profileVCount, profileVRunning ? "RUNNING" : "stopped");
    if (profileVRunning && profileVCount > 0) {
        Serial.printf(", step %d (%s)\n", profileVStep, profileV[profileVStep].name.c_str());
    } else { Serial.println(); }
    Serial.printf("Current: %d steps, %s", profileCCount, profileCRunning ? "RUNNING" : "stopped");
    if (profileCRunning && profileCCount > 0) {
        Serial.printf(", step %d (%s)\n", profileCStep, profileC[profileCStep].name.c_str());
    } else { Serial.println(); }
    Serial.println("----------------------");
}

//=============================================================================
// GPIO Functions
//=============================================================================
void updateGpioInputs() {
    for (int i = 0; i < GPIO_IN_COUNT; i++) {
        gpioInState[i] = digitalRead(GPIO_IN_PINS[i]) ? 1 : 0;
    }
}

void setGpioOutput(int index, bool state) {
    if (index < 0 || index >= GPIO_OUT_COUNT) return;
    gpioOutState[index] = state ? 1 : 0;
    digitalWrite(GPIO_OUT_PINS[index], state ? HIGH : LOW);
    Serial.printf("GPIO OUT %d (pin %d) = %s\n", index, GPIO_OUT_PINS[index], state ? "HIGH" : "LOW");
}

void printGpioStatus() {
    Serial.println("\n--- GPIO Status ---");
    Serial.print("Inputs:  ");
    for (int i = 0; i < GPIO_IN_COUNT; i++) {
        Serial.printf("[%d]=%d ", GPIO_IN_PINS[i], gpioInState[i]);
    }
    Serial.println();
    Serial.print("Outputs: ");
    for (int i = 0; i < GPIO_OUT_COUNT; i++) {
        Serial.printf("[%d]=%d ", GPIO_OUT_PINS[i], gpioOutState[i]);
    }
    Serial.println();
}

//=============================================================================
// WebSocket Handler
//=============================================================================
void webSocketEvent(uint8_t num, WStype_t type, uint8_t* payload, size_t length) {
    switch (type) {
        case WStype_DISCONNECTED:
            Serial.printf("[WS] Client %u disconnected\n", num);
            if (wsClientCount > 0) wsClientCount--;
            break;
            
        case WStype_CONNECTED:
            {
                wsClientCount++;
                IPAddress ip = webSocket.remoteIP(num);
                Serial.printf("[WS] Client %u connected from %s\n", num, ip.toString().c_str());
                // Send current status
                sendStatus(num);
            }
            break;
            
        case WStype_TEXT:
            wsMessageCount++;
            handleWebSocketMessage(num, (char*)payload, length);
            break;
    }
}

void handleWebSocketMessage(uint8_t num, char* payload, size_t length) {
    StaticJsonDocument<1024> doc;
    DeserializationError error = deserializeJson(doc, payload, length);
    
    if (error) {
        Serial.print("[WS] JSON parse error: ");
        Serial.println(error.c_str());
        return;
    }
    
    String cmd = doc["cmd"].as<String>();
    
    if (cmd == "get_status") {
        sendStatus(num);
    } else if (cmd == "set_voltage") {
        float volts = doc["value"].as<float>();
        setVoltage(volts);
        broadcastStatus();
    } else if (cmd == "set_current") {
        float amps = doc["value"].as<float>();
        setCurrent(amps);
        broadcastStatus();
    } else if (cmd == "set_dac") {
        int channel = doc["channel"].as<int>();  // 0=voltage, 1=current
        int value = doc["value"].as<int>();
        setDAC(channel, value);
    } else if (cmd == "set_led") {
        String state = doc["state"].as<String>();
        if (state == "on") ledMode = LED_ON;
        else if (state == "off") ledMode = LED_OFF;
        else if (state == "blink") ledMode = LED_BLINK;
        broadcastStatus();
    } else if (cmd == "load_profile") {
        loadProfileFromJson(doc["profile"]);
    } else if (cmd == "set_display") {
        String mode = doc["channel"].as<String>();
        displayChannel = (mode == "current") ? CHANNEL_CURRENT : CHANNEL_VOLTAGE;
        broadcastStatus();
    } else if (cmd == "start_profile") {
        String ch = doc["channel"] | "voltage";
        if (ch == "current") startProfile(CHANNEL_CURRENT);
        else if (ch == "both") { startProfile(CHANNEL_VOLTAGE); startProfile(CHANNEL_CURRENT); }
        else startProfile(CHANNEL_VOLTAGE);
    } else if (cmd == "stop_profile") {
        String ch = doc["channel"] | "voltage";
        if (ch == "current") stopProfile(CHANNEL_CURRENT);
        else if (ch == "both") { stopProfile(CHANNEL_VOLTAGE); stopProfile(CHANNEL_CURRENT); }
        else stopProfile(CHANNEL_VOLTAGE);
    } else if (cmd == "get_gpio") {
        sendGpioStatus(num);
    } else if (cmd == "set_gpio") {
        int index = doc["index"].as<int>();
        bool state = doc["state"].as<bool>();
        setGpioOutput(index, state);
        sendGpioStatus(num);
    } else if (cmd == "ping") {
        StaticJsonDocument<64> resp;
        resp["type"] = "pong";
        resp["t"] = doc["t"].as<unsigned long>();
        String json;
        serializeJson(resp, json);
        webSocket.sendTXT(num, json);
    }
    // CAN bus commands
    else if (cmd == "can_init" || cmd == "can_start") {
        uint32_t speed = doc["speed_kbps"] | 500;
        bool ok = initCAN(speed);
        StaticJsonDocument<128> resp;
        resp["type"] = "can_init";
        resp["success"] = ok;
        resp["speed_kbps"] = canSpeed;
        String json;
        serializeJson(resp, json);
        webSocket.sendTXT(num, json);
    } else if (cmd == "can_stop") {
        stopCAN();
        StaticJsonDocument<64> resp;
        resp["type"] = "can_stop";
        resp["success"] = true;
        String json;
        serializeJson(resp, json);
        webSocket.sendTXT(num, json);
    } else if (cmd == "can_send") {
        uint32_t id = doc["id"] | 0;
        bool extended = doc["extended"] | false;
        uint8_t data[8] = {0};
        uint8_t len = 0;
        
        // Accept data as array of bytes
        if (doc.containsKey("data") && doc["data"].is<JsonArray>()) {
            JsonArray arr = doc["data"].as<JsonArray>();
            for (JsonVariant v : arr) {
                if (len < 8) data[len++] = v.as<uint8_t>();
            }
        }
        // Or as hex string "AA BB CC"
        else if (doc.containsKey("hex")) {
            String hex = doc["hex"].as<String>();
            hex.replace(" ", "");
            for (int i = 0; i < hex.length() && len < 8; i += 2) {
                data[len++] = strtol(hex.substring(i, i+2).c_str(), NULL, 16);
            }
        }
        
        bool ok = sendCAN(id, data, len, extended);
        StaticJsonDocument<128> resp;
        resp["type"] = "can_send";
        resp["success"] = ok;
        resp["id"] = id;
        resp["len"] = len;
        String json;
        serializeJson(resp, json);
        webSocket.sendTXT(num, json);
    } else if (cmd == "can_get" || cmd == "get_can") {
        sendCanMessages(num);
    } else if (cmd == "can_status") {
        StaticJsonDocument<256> resp;
        resp["type"] = "can_status";
        JsonObject can = resp.createNestedObject("can");
        getCanStatus(can);
        String json;
        serializeJson(resp, json);
        webSocket.sendTXT(num, json);
    } else {
        Serial.print("[WS] Unknown command: ");
        Serial.println(cmd);
    }
}

void loadProfileFromJson(JsonObject profileObj) {
    String channel = profileObj["channel"].as<String>();
    JsonArray steps = profileObj["steps"].as<JsonArray>();

    if (channel == "current") {
        profileCCount = 0;
        for (JsonObject step : steps) {
            if (profileCCount >= MAX_PROFILE_STEPS) break;
            profileC[profileCCount].name = step["name"].as<String>();
            profileC[profileCCount].value = step["value"].as<float>();
            profileC[profileCCount].durationMs = step["duration_ms"].as<uint32_t>();
            profileC[profileCCount].rampMs = step["ramp_ms"].as<uint32_t>();
            profileCCount++;
        }
        Serial.printf("[WS] Current profile loaded: %d steps\n", profileCCount);
    } else {
        profileVCount = 0;
        for (JsonObject step : steps) {
            if (profileVCount >= MAX_PROFILE_STEPS) break;
            profileV[profileVCount].name = step["name"].as<String>();
            profileV[profileVCount].value = step["value"].as<float>();
            profileV[profileVCount].durationMs = step["duration_ms"].as<uint32_t>();
            profileV[profileVCount].rampMs = step["ramp_ms"].as<uint32_t>();
            profileVCount++;
        }
        Serial.printf("[WS] Voltage profile loaded: %d steps\n", profileVCount);
    }

    broadcastStatus();
}

void sendStatus(uint8_t num) {
    StaticJsonDocument<1024> doc;

    doc["type"] = "status";
    doc["version"] = PROG_VERSION;
    doc["wifi_connected"] = wifiConnected;
    doc["ip"] = wifiConnected ? WiFi.localIP().toString() : "";

    // Dual DAC outputs
    doc["voltage"] = voltageValue;
    doc["current"] = currentValue;
    doc["dac1_value"] = dac1Value;
    doc["dac2_value"] = dac2Value;
    doc["display_channel"] = (displayChannel == CHANNEL_VOLTAGE) ? "voltage" : "current";

    // Analog inputs
    doc["adc1_raw"] = readAnalogInput(0);
    doc["adc2_raw"] = readAnalogInput(1);
    doc["adc1_voltage"] = readAnalogVoltage(0);
    doc["adc2_voltage"] = readAnalogVoltage(1);

    // CAN bus status
    JsonObject can = doc.createNestedObject("can");
    getCanStatus(can);

    doc["led_mode"] = (ledMode == LED_ON) ? "on" : (ledMode == LED_OFF) ? "off" : "blink";

    // Voltage profile status
    doc["profile_v_running"] = profileVRunning;
    doc["profile_v_step"] = profileVStep;
    doc["profile_v_step_count"] = profileVCount;
    if (profileVRunning && profileVCount > 0) {
        doc["profile_v_step_name"] = profileV[profileVStep].name;
    }

    // Current profile status
    doc["profile_c_running"] = profileCRunning;
    doc["profile_c_step"] = profileCStep;
    doc["profile_c_step_count"] = profileCCount;
    if (profileCRunning && profileCCount > 0) {
        doc["profile_c_step_name"] = profileC[profileCStep].name;
    }

    // Backward compat (maps to voltage profile)
    doc["profile_running"] = profileVRunning;
    doc["profile_step"] = profileVStep;
    doc["profile_step_count"] = profileVCount;
    if (profileVRunning && profileVCount > 0) {
        doc["profile_step_name"] = profileV[profileVStep].name;
    }

    String json;
    serializeJson(doc, json);
    webSocket.sendTXT(num, json);
    wsTxCount++;
}

void broadcastStatus() {
    StaticJsonDocument<1024> doc;

    doc["type"] = "status";
    doc["version"] = PROG_VERSION;
    doc["wifi_connected"] = wifiConnected;

    // Dual DAC outputs
    doc["voltage"] = voltageValue;
    doc["current"] = currentValue;
    doc["dac1_value"] = dac1Value;
    doc["dac2_value"] = dac2Value;
    doc["display_channel"] = (displayChannel == CHANNEL_VOLTAGE) ? "voltage" : "current";

    // Analog inputs
    doc["adc1_raw"] = readAnalogInput(0);
    doc["adc2_raw"] = readAnalogInput(1);
    doc["adc1_voltage"] = readAnalogVoltage(0);
    doc["adc2_voltage"] = readAnalogVoltage(1);

    // CAN bus status
    JsonObject can = doc.createNestedObject("can");
    getCanStatus(can);

    doc["led_mode"] = (ledMode == LED_ON) ? "on" : (ledMode == LED_OFF) ? "off" : "blink";

    // Voltage profile status
    doc["profile_v_running"] = profileVRunning;
    doc["profile_v_step"] = profileVStep;
    doc["profile_v_step_count"] = profileVCount;
    if (profileVRunning && profileVCount > 0) {
        doc["profile_v_step_name"] = profileV[profileVStep].name;
    }

    // Current profile status
    doc["profile_c_running"] = profileCRunning;
    doc["profile_c_step"] = profileCStep;
    doc["profile_c_step_count"] = profileCCount;
    if (profileCRunning && profileCCount > 0) {
        doc["profile_c_step_name"] = profileC[profileCStep].name;
    }

    // Backward compat (maps to voltage profile)
    doc["profile_running"] = profileVRunning;
    doc["profile_step"] = profileVStep;
    doc["profile_step_count"] = profileVCount;
    if (profileVRunning && profileVCount > 0) {
        doc["profile_step_name"] = profileV[profileVStep].name;
    }

    String json;
    serializeJson(doc, json);
    webSocket.broadcastTXT(json);
    wsTxCount += wsClientCount;
}

void sendGpioStatus(uint8_t num) {
    StaticJsonDocument<512> doc;
    
    doc["type"] = "gpio";
    
    JsonArray inputs = doc.createNestedArray("inputs");
    for (int i = 0; i < GPIO_IN_COUNT; i++) {
        JsonObject inp = inputs.createNestedObject();
        inp["index"] = i;
        inp["pin"] = GPIO_IN_PINS[i];
        inp["state"] = gpioInState[i];
    }
    
    JsonArray outputs = doc.createNestedArray("outputs");
    for (int i = 0; i < GPIO_OUT_COUNT; i++) {
        JsonObject out = outputs.createNestedObject();
        out["index"] = i;
        out["pin"] = GPIO_OUT_PINS[i];
        out["state"] = gpioOutState[i];
    }
    
    String json;
    serializeJson(doc, json);
    webSocket.sendTXT(num, json);
    wsTxCount++;
}

//=============================================================================
// Status & Help
//=============================================================================
void printStatus() {
    Serial.println("\n========= Status =========");
    Serial.print("WiFi: ");
    Serial.println(wifiConnected ? "Connected" : "Disconnected");
    if (wifiConnected) {
        Serial.print("  IP: ");
        Serial.println(WiFi.localIP());
        Serial.print("  WebSocket: ws://");
        Serial.print(WiFi.localIP());
        Serial.print(":");
        Serial.println(WS_PORT);
    }
    Serial.print("Voltage: ");
    Serial.print(voltageValue, 1);
    Serial.println(" V");
    Serial.print("Current: ");
    Serial.print(currentValue, 1);
    Serial.println(" A");
    Serial.print("DAC1/DAC2: ");
    Serial.print(dac1Value);
    Serial.print("/");
    Serial.println(dac2Value);
    Serial.print("LED: ");
    Serial.println((ledMode == LED_ON) ? "ON" : (ledMode == LED_OFF) ? "OFF" : "BLINK");
    Serial.printf("V Profile: %d steps, %s\n", profileVCount, profileVRunning ? "RUNNING" : "stopped");
    Serial.printf("C Profile: %d steps, %s\n", profileCCount, profileCRunning ? "RUNNING" : "stopped");
    Serial.printf("Display: %s\n", displayChannel == CHANNEL_VOLTAGE ? "Voltage" : "Current");
    Serial.print("GPIO In:  ");
    for (int i = 0; i < GPIO_IN_COUNT; i++) Serial.printf("%d ", gpioInState[i]);
    Serial.println();
    Serial.print("GPIO Out: ");
    for (int i = 0; i < GPIO_OUT_COUNT; i++) Serial.printf("%d ", gpioOutState[i]);
    Serial.println();
    Serial.println("==========================");
}

void printHelp() {
    Serial.println("\n========= Commands =========");
    Serial.println("WiFi:");
    Serial.println("  wifi ssid <name>     - Set SSID");
    Serial.println("  wifi pass <password> - Set password");
    Serial.println("  wifi connect         - Connect");
    Serial.println("  wifi status          - Show status");
    Serial.println("  wifi clear           - Clear saved");
    Serial.println("\nControl:");
    Serial.println("  set voltage <0-60>   - Set voltage (V)");
    Serial.println("  set current <0-200>  - Set current (A)");
    Serial.println("  set dac1 <0-4095>    - Set DAC1 raw");
    Serial.println("  set dac2 <0-4095>    - Set DAC2 raw");
    Serial.println("  set led <on|off|blink>");
    Serial.println("\nProfile:");
    Serial.println("  profile start        - Start");
    Serial.println("  profile stop         - Stop");
    Serial.println("  profile status       - Show");
    Serial.println("\nGPIO:");
    Serial.println("  gpio                 - Show GPIO status");
    Serial.printf("  gpio out <idx> <0|1> - Set output 0-%d\n", GPIO_OUT_COUNT-1);
    Serial.printf("  Inputs:  pins %d,%d,%d,%d\n", GPIO_IN_PINS[0], GPIO_IN_PINS[1], GPIO_IN_PINS[2], GPIO_IN_PINS[3]);
    Serial.print("  Outputs: pins ");
    for (int i = 0; i < GPIO_OUT_COUNT; i++) {
        Serial.printf("%d%s", GPIO_OUT_PINS[i], i < GPIO_OUT_COUNT-1 ? "," : "\n");
    }
    Serial.println("\nCAN Bus:");
    Serial.println("  can init [speed]     - Init (default 500 kbps)");
    Serial.println("  can stop             - Stop CAN driver");
    Serial.println("  can send <id> <hex>  - Send message");
    Serial.println("  can read             - Read RX buffer");
    Serial.println("  can status           - Show CAN status");
    Serial.printf("  TX: GPIO%d, RX: GPIO%d\n", CAN_TX_PIN, CAN_RX_PIN);
    Serial.println("\nOther:");
    Serial.println("  status               - System status");
    Serial.println("  help                 - This help");
    Serial.println("=============================");
}
