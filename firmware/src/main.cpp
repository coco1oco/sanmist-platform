#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <time.h>
#include "DHT.h"

// Hardware Pin Definitions (aligned with firmware/diagram.json)
#define PIN_DHT          32    // Matches diagram.json esp:32 -> dht:SDA
#define PIN_POT          33    // Matches diagram.json esp:33 -> flow_pot:SIG (ADC1 CH5)
#define PIN_BUZZER       19    // Matches diagram.json esp:19 -> buzzer:2
#define PIN_ATOMIZER     23    // Matches diagram.json esp:23 -> led_mist:A
#define PIN_BTN_START    18    // Matches diagram.json esp:18 -> btn_start (INPUT_PULLUP)
#define PIN_BTN_ESTOP    5     // Matches diagram.json esp:5 -> btn_estop (INPUT_PULLUP)
#define PIN_HOCL_FLOAT   35    // Matches diagram.json esp:35 -> hocl_float:2 (input-only, external SPDT 3V3/GND)

#define DHTTYPE DHT22
DHT dht(PIN_DHT, DHTTYPE);

// Manual cycle defaults (never overwritten by remote triggers)
const unsigned long DEFAULT_MIST_DURATION_MS = 30000; // default 30s
const float DEFAULT_ROOM_VOLUME_M3 = 1.0f;            // default 1.0 m^3

// Remote trigger runtime parameters
unsigned long remoteMistDurationMs = DEFAULT_MIST_DURATION_MS;
float remoteRoomVolumeM3 = DEFAULT_ROOM_VOLUME_M3;
bool remoteTriggerRequested = false;
bool cycleInProgress = false;

// Wokwi WiFi Credentials
const char* ssid = "Wokwi-GUEST";
const char* password = "";

// MQTT Broker (MQTTS over TLS 8883 with Root CA verification + payload auth_key)
const char* mqtt_server_host = "broker.emqx.io";
const int mqtt_port = 8883;
const char* mqtt_username = "";
const char* mqtt_password = "";
const char* device_station_id = "MIST-BOH-01";
const char* device_auth_key = "sanmist-edge-key-2026";
const unsigned int BUZZER_FREQ_HZ = 2000;
const char* telemetry_topic = "sanmist/devices/MIST-BOH-01/telemetry";
const char* status_topic = "sanmist/devices/MIST-BOH-01/status";
const char* control_topic = "sanmist/control/trigger";

// Verified Root CA (DigiCert Global Root G2) for broker.emqx.io:8883
// Replace with local Mosquitto ca.crt when targeting a self-hosted Docker broker.
static const char* mqtt_ca_cert = R"EOF(
-----BEGIN CERTIFICATE-----
MIIDjjCCAnagAwIBAgIQAzrx5qcRqaC7KGSxHQn65TANBgkqhkiG9w0BAQsFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBH
MjAeFw0xMzA4MDExMjAwMDBaFw0zODAxMTUxMjAwMDBaMGExCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IEcyMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuzfNNNx7a8myaJCtSnX/RrohCgiN9RlUyfuI
2/Ou8jqJkTx65qsGGmvPrC3oXgkkRLpimn7Wo6h+4FR1IAWsULecYxpsMNzaHxmx
1x7e/dfgy5SDN67sH0NO3Xss0r0upS/kqbitOtSZpLYl6ZtrAGCSYP9PIUkY92eQ
q2EGnI/yuum06ZIya7XzV+hdG82MHauVBJVJ8zUtluNJbd134/tJS7SsVQepj5Wz
tCO7TG1F8PapspUwtP1MVYwnSlcUfIKdzXOS0xZKBgyMUNGPHgm+F6HmIcr9g+UQ
vIOlCsRnKPZzFBQ9RnbDhxSJITRNrw9FDKZJobq7nMWxM4MphQIDAQABo0IwQDAP
BgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBhjAdBgNVHQ4EFgQUTiJUIBiV
5uNu5g/6+rkS7QYXjzkwDQYJKoZIhvcNAQELBQADggEBAGBnKJRvDkhj6zHd6mcY
1Yl9PMWLSn/pvtsrF9+wX3N3KjITOYFnQoQj8kVnNeyIv/iPsGEMNKSuIEyExtv4
NeF22d+mQrvHRAiGfzZ0JFrabA0UWTW98kndth/Jsw1HKj2ZL7tcu7XUIOGZX1NG
Fdtom/DzMNU+MeKNhJ7jitralj41E6Vf8PlwUHBHQRFXGU7Aj64GxJUTFy8bJZ91
8rGOmaFvE7FBcf6IKshPECBV1/MUReXgRPTqh5Uykw7+U0b6LJ3/iyK5S9kJRaTe
pLiaWN0bfVKfjllDiIGknibVb63dDcY3fe0Dkhvld1927jyNxF1WW6LZZm6zNTfl
MrY=
-----END CERTIFICATE-----
)EOF";

// Forward declarations for compiler scope resolution
void runSanitizationCycle(unsigned long durationMs, float roomVol);
void publishTelemetry(float temp, float hum, float flow, float durationSec, float roomVol, bool estop);
void reconnectMQTT(bool force = false);

WiFiClientSecure espClient;
PubSubClient client(espClient);

volatile bool estop_active = false;
unsigned long lastMqttRetry = 0;
unsigned long lastSensorPoll = 0;
float lastReportedTemp = -999.0f;
float lastReportedHum = -999.0f;
float lastReportedFlow = -999.0f;
bool lastReportedHoclLow = false;
bool hasPublishedSensorStatus = false;
bool statusSyncRequested = false;

bool isHoclLow() {
    return digitalRead(PIN_HOCL_FLOAT) == LOW;
}

void IRAM_ATTR handleEmergencyStop() {
    estop_active = true;
    digitalWrite(PIN_ATOMIZER, LOW);
    digitalWrite(PIN_BUZZER, LOW);
}

// -------------------------------------------------------------
// Inbound MQTT Message Listener (Remote Dispatch)
// -------------------------------------------------------------
void onMqttMessage(char* topic, byte* payload, unsigned int length) {
    char jsonBuffer[512];
    if (length >= sizeof(jsonBuffer)) {
        length = sizeof(jsonBuffer) - 1;
    }
    memcpy(jsonBuffer, payload, length);
    jsonBuffer[length] = '\0';

    JsonDocument doc;
    DeserializationError error = deserializeJson(doc, jsonBuffer);
    if (error) {
        Serial.print("[MQTT] JSON parse failed: ");
        Serial.println(error.c_str());
        return;
    }

    const char* targetStation = doc["station_id"] | "";
    if (strlen(targetStation) > 0 && strcmp(targetStation, device_station_id) != 0) {
        return;
    }

    const char* providedKey = doc["auth_key"] | "";
    if (strlen(providedKey) == 0 || strcmp(providedKey, device_auth_key) != 0) {
        Serial.println("[MQTT AUTH] Rejected control command with missing or invalid auth_key.");
        return;
    }

    const char* command = doc["command"] | "";

    // 1. Handle Remote UI Emergency Stop
    if (strcmp(command, "STOP") == 0) {
        Serial.println("[HALT] Remote UI Emergency Stop received!");
        estop_active = true;
        digitalWrite(PIN_ATOMIZER, LOW);
        digitalWrite(PIN_BUZZER, LOW);
        return;
    }

    // 2. Handle Remote Trigger START (defer execution to loop() to avoid re-entrant MQTT callback)
    if (strcmp(command, "START") == 0) {
        if (isHoclLow()) {
            Serial.println("[INTERLOCK] Remote START rejected: HOCl disinfectant reservoir depleted!");
            statusSyncRequested = true;
            return;
        }
        if (cycleInProgress || remoteTriggerRequested) {
            Serial.println("[WARN] Trigger ignored: Cycle already in progress.");
            return;
        }
        int durationSec = doc["duration_sec"] | 30;
        if (durationSec < 5) durationSec = 5;
        if (durationSec > 300) durationSec = 300;
        float roomVol = doc["room_volume_m3"] | 1.0f;
        if (roomVol <= 0.0f) roomVol = DEFAULT_ROOM_VOLUME_M3;
        remoteMistDurationMs = (unsigned long)durationSec * 1000UL;
        remoteRoomVolumeM3 = roomVol;
        remoteTriggerRequested = true;
        Serial.print("[REMOTE TRIGGER] Queued! Duration: ");
        Serial.print(durationSec);
        Serial.println(" s");
    }
}

void setupWiFi() {
    delay(10);
    Serial.println();
    Serial.print("[WIFI] Connecting to ");
    Serial.println(ssid);
    WiFi.begin(ssid, password);
    while (WiFi.status() != WL_CONNECTED) {
        delay(300);
        Serial.print(".");
    }

    // Configure public DNS servers to avoid ISP DNS interception while keeping DHCP lease
    WiFi.config(WiFi.localIP(), WiFi.gatewayIP(), WiFi.subnetMask(), IPAddress(8, 8, 8, 8), IPAddress(1, 1, 1, 1));

    Serial.println("\n[WIFI] Connected! IP Address: ");
    Serial.println(WiFi.localIP());

    // Sync SNTP clock briefly so mbedTLS certificate validity checks succeed
    configTime(0, 0, "pool.ntp.org", "time.nist.gov");
    unsigned long ntpStart = millis();
    while (time(nullptr) < 1700000000L && (millis() - ntpStart) < 4000UL) {
        delay(200);
    }
    Serial.printf("[TLS] System UTC epoch: %ld\n", (long)time(nullptr));
}

void readSanitizedEnvironment(float& outTemp, float& outHum) {
    float rawTemp = dht.readTemperature();
    float rawHum = dht.readHumidity();
    outTemp = isnan(rawTemp) ? ((lastReportedTemp > -100.0f) ? lastReportedTemp : 28.5f) : rawTemp;
    outHum = isnan(rawHum) ? ((lastReportedHum > -100.0f) ? lastReportedHum : 65.0f) : rawHum;
}

void pollAndPublishLiveSensors(bool forcePublish = false) {
    float temp = 28.5f;
    float hum = 65.0f;
    readSanitizedEnvironment(temp, hum);

    int potValue = analogRead(PIN_POT);
    float flow = map(potValue, 0, 4095, 10, 50); // mL/min proxy
    bool hoclLow = isHoclLow();

    if (forcePublish ||
        !hasPublishedSensorStatus ||
        hoclLow != lastReportedHoclLow ||
        fabs(temp - lastReportedTemp) >= 0.2f ||
        fabs(hum - lastReportedHum) >= 0.5f ||
        fabs(flow - lastReportedFlow) >= 1.0f) {
        lastReportedTemp = temp;
        lastReportedHum = hum;
        lastReportedFlow = flow;
        lastReportedHoclLow = hoclLow;
        hasPublishedSensorStatus = true;

        Serial.printf(
            "[SENSOR] Live DHT22 & Flow -> Temp: %.1f C | Hum: %.1f %% | Flow: %.1f mL/min | HOCl: %s\n",
            temp,
            hum,
            flow,
            hoclLow ? "LOW (DEPLETED)" : "OK"
        );

        if (client.connected()) {
            JsonDocument doc;
            doc["station_id"] = "MIST-BOH-01";
            doc["ambient_temp_c"] = temp;
            doc["relative_humidity_pct"] = hum;
            doc["flow_rate_ml_min"] = flow;
            doc["hocl_low_level"] = hoclLow;
            doc["cycle_in_progress"] = cycleInProgress;
            doc["auth_key"] = device_auth_key;

            char buffer[384];
            serializeJson(doc, buffer, sizeof(buffer));
            client.publish(status_topic, buffer);
        }
    }
}

// Non-blocking MQTTS (TLS) connection with hostname verification
void reconnectMQTT(bool force) {
    if (client.connected()) return;

    unsigned long now = millis();
    if (force || now - lastMqttRetry > 2000) {
        lastMqttRetry = now;
        Serial.print("[MQTTS] Attempting TLS connection to ");
        Serial.print(mqtt_server_host);
        Serial.print(":");
        Serial.print(mqtt_port);
        Serial.print("... ");

        // Hardware-seeded randomized client ID prevents broker session collisions
        String clientId = "MistSan-ESP32-" + String(micros(), HEX) + "-" + String(random(1000, 9999));

        client.setServer(mqtt_server_host, mqtt_port);
        bool connected = (strlen(mqtt_username) > 0)
            ? client.connect(clientId.c_str(), mqtt_username, mqtt_password)
            : client.connect(clientId.c_str());

        if (connected) {
            Serial.println("connected (TLS)!");
            client.subscribe(control_topic);
            Serial.print("[MQTT SUB] Subscribed to topic: ");
            Serial.println(control_topic);
            pollAndPublishLiveSensors(true);
        } else {
            Serial.print("failed, rc=");
            Serial.print(client.state());
            Serial.println(" (will retry in 2s)");
        }
    }
}

void publishTelemetry(float temp, float hum, float flow, float durationSec, float roomVol, bool estop) {
    client.setBufferSize(512);

    // CRITICAL: Reconnect BEFORE publishing if the connection dropped during misting
    if (!client.connected()) {
        Serial.println("[MQTT] Socket disconnected during misting! Reconnecting before publish...");
        reconnectMQTT(true);
    }

    JsonDocument doc;
    doc["station_id"] = "MIST-BOH-01";
    doc["ambient_temp_c"] = isnan(temp) ? 25.0f : temp;
    doc["relative_humidity_pct"] = isnan(hum) ? 65.0f : hum;
    doc["flow_rate_ml_min"] = (estop) ? 0.0f : flow;
    doc["cycle_duration_sec"] = durationSec;
    doc["room_volume_m3"] = roomVol;
    doc["delivery_confirmed"] = (flow > 5.0f && !estop);
    doc["estop_triggered"] = estop;
    doc["auth_key"] = device_auth_key;

    char buffer[384];
    serializeJson(doc, buffer, sizeof(buffer));

    bool sent = client.publish(telemetry_topic, buffer);
    if (sent) {
        Serial.print("[MQTT PUB SUCCESS] Sent Payload: ");
        Serial.println(buffer);
    } else {
        Serial.print("[MQTT PUB FAILED] Dropped payload: ");
        Serial.println(buffer);
    }
}

void runSanitizationCycle(unsigned long durationMs, float roomVol) {
    if (isHoclLow()) {
        Serial.println("[INTERLOCK] Cycle blocked: HOCl disinfectant reservoir depleted!");
        tone(PIN_BUZZER, BUZZER_FREQ_HZ);
        delay(250);
        noTone(PIN_BUZZER);
        digitalWrite(PIN_BUZZER, LOW);
        statusSyncRequested = true;
        return;
    }

    cycleInProgress = true;
    estop_active = false;
    unsigned long cycleStart = millis();

    float latestTemp = 28.5f;
    float latestHum = 65.0f;
    readSanitizedEnvironment(latestTemp, latestHum);

    // --- Pre-Cycle Warning Phase (10 second audible alarm) ---
    Serial.println("[START] Pre-cycle audible warning initiated...");
    tone(PIN_BUZZER, BUZZER_FREQ_HZ);

    unsigned long bzStart = millis();
    while (millis() - bzStart < 10000) {
        if (isHoclLow()) {
            noTone(PIN_BUZZER);
            digitalWrite(PIN_BUZZER, LOW);
            Serial.println("[INTERLOCK] Pre-cycle warning aborted: HOCl reservoir depleted!");
            publishTelemetry(latestTemp, latestHum, 0.0f, (millis() - cycleStart) / 1000.0f, roomVol, false);
            cycleInProgress = false;
            statusSyncRequested = true;
            return;
        }
        // Check physical pin or ISR flag
        if (estop_active || digitalRead(PIN_BTN_ESTOP) == LOW) {
            estop_active = true;
            noTone(PIN_BUZZER);
            digitalWrite(PIN_BUZZER, LOW);
            Serial.println("[HALT] E-Stop triggered during pre-cycle warning!");
            publishTelemetry(latestTemp, latestHum, 0.0f, (millis() - cycleStart) / 1000.0f, roomVol, true);
            cycleInProgress = false;
            return;
        }
        client.loop(); // Keep MQTT alive during warning
        delay(10);
    }
    noTone(PIN_BUZZER);
    digitalWrite(PIN_BUZZER, LOW);

    // --- Active Misting Phase ---
    Serial.println("[MISTING] Atomizer active...");
    digitalWrite(PIN_ATOMIZER, HIGH);

    unsigned long mistStart = millis();
    float totalFlow = 0.0f;
    int samples = 0;

    while (millis() - mistStart < durationMs) {
        if (isHoclLow()) {
            digitalWrite(PIN_ATOMIZER, LOW);
            Serial.println("[INTERLOCK] Misting halted: HOCl reservoir depleted mid-cycle!");
            readSanitizedEnvironment(latestTemp, latestHum);
            float earlyDuration = (millis() - mistStart) / 1000.0f;
            publishTelemetry(latestTemp, latestHum, 0.0f, earlyDuration, roomVol, false);
            cycleInProgress = false;
            statusSyncRequested = true;
            return;
        }
        // Poll emergency stop button
        if (estop_active || digitalRead(PIN_BTN_ESTOP) == LOW) {
            estop_active = true;
            Serial.println("[HALT] EMERGENCY STOP ACTIVATED DURING MISTING!");
            digitalWrite(PIN_ATOMIZER, LOW);
            readSanitizedEnvironment(latestTemp, latestHum);
            float earlyDuration = (millis() - mistStart) / 1000.0f;
            publishTelemetry(latestTemp, latestHum, 0.0f, earlyDuration, roomVol, true);
            cycleInProgress = false;
            return;
        }

        // Read atomizer flow proxy via potentiometer
        int potValue = analogRead(PIN_POT);
        float currentFlow = (float)map(potValue, 0, 4095, 10, 50); // mL/min proxy
        totalFlow += currentFlow;
        samples++;

        // Sample DHT22 environment sensor periodically (~2 sec cadence)
        if (samples % 20 == 0) {
            readSanitizedEnvironment(latestTemp, latestHum);
        }

        // Keep MQTT connection alive to prevent broker timeout
        client.loop();
        delay(100);
    }

    // --- Completion Phase ---
    digitalWrite(PIN_ATOMIZER, LOW);
    cycleInProgress = false;
    Serial.println("[COMPLETE] Sanitization cycle finished!");

    float actualDurationSec = (millis() - mistStart) / 1000.0f;
    float avgFlow = (samples > 0) ? (totalFlow / samples) : 0.0f;
    readSanitizedEnvironment(latestTemp, latestHum);

    lastReportedTemp = latestTemp;
    lastReportedHum = latestHum;
    lastReportedFlow = avgFlow;

    publishTelemetry(latestTemp, latestHum, avgFlow, actualDurationSec, roomVol, false);
}

void setup() {
    Serial.begin(115200);
    dht.begin();

    pinMode(PIN_BUZZER, OUTPUT);
    pinMode(PIN_ATOMIZER, OUTPUT);
    pinMode(PIN_POT, INPUT);
    pinMode(PIN_BTN_START, INPUT_PULLUP);
    pinMode(PIN_BTN_ESTOP, INPUT_PULLUP);
    pinMode(PIN_HOCL_FLOAT, INPUT);

    attachInterrupt(digitalPinToInterrupt(PIN_BTN_ESTOP), handleEmergencyStop, FALLING);

    setupWiFi();
    espClient.setCACert(mqtt_ca_cert);
    espClient.setTimeout(15000);          // milliseconds (Arduino-ESP32 3.x NetworkClientSecure)
    espClient.setHandshakeTimeout(30);    // seconds (generous for Wokwi simulated TLS handshake)
    client.setServer(mqtt_server_host, mqtt_port);
    client.setCallback(onMqttMessage);
    client.setBufferSize(512);
    client.setSocketTimeout(15);

    Serial.println("--- MistSan ESP32 Firmware Ready (MQTTS TLS) ---");
}

void loop() {
    if (!client.connected()) {
        reconnectMQTT();
    } else {
        client.loop();
    }

    unsigned long now = millis();
    if (statusSyncRequested || (hasPublishedSensorStatus && isHoclLow() != lastReportedHoclLow)) {
        statusSyncRequested = false;
        pollAndPublishLiveSensors(true);
    }
    if (now - lastSensorPoll >= 2500) {
        lastSensorPoll = now;
        pollAndPublishLiveSensors();
    }

    // Physical manual start button check uses fixed manual defaults
    if (!cycleInProgress && digitalRead(PIN_BTN_START) == LOW) {
        delay(30); // Debounce
        if (digitalRead(PIN_BTN_START) == LOW) {
            runSanitizationCycle(DEFAULT_MIST_DURATION_MS, DEFAULT_ROOM_VOLUME_M3);
            while (digitalRead(PIN_BTN_START) == LOW) {
                delay(10);
            }
        }
    }

    // Remote Web MQTT Trigger uses requested remote parameters
    if (remoteTriggerRequested) {
        remoteTriggerRequested = false;
        runSanitizationCycle(remoteMistDurationMs, remoteRoomVolumeM3);
    }

    delay(10); // Prevents CPU starvation and watchdog timeouts
}
