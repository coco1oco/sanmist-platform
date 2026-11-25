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
#define PIN_PIR          27    // Matches diagram.json esp:27 -> pir:OUT (HC-SR501 presence sensor)

#define DHTTYPE DHT22
DHT dht(PIN_DHT, DHTTYPE);

// Manual cycle defaults (never overwritten by remote triggers)
const unsigned long DEFAULT_MIST_DURATION_MS = 30000; // default 30s
const float DEFAULT_ROOM_VOLUME_M3 = 1.0f;            // default 1.0 m^3
const float HOCL_CAPACITY_ML = 500.0f;                // full disinfectant reservoir capacity
const float HOCL_LOW_THRESHOLD_ML = 50.0f;            // 10% reserve safety interlock threshold

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
const int mqtt_plain_port = 1883;
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
void runSanitizationCycle(unsigned long durationMs, float roomVol, bool enforcePresence = false, const char* triggerType = "MANUAL_WEB");
void publishTelemetry(float temp, float hum, float flow, float durationSec, float roomVol, bool estop, const char* triggerType = "MANUAL_WEB", bool presenceAborted = false, bool hoclAborted = false);
void reconnectMQTT(bool force = false);

WiFiClientSecure espClient;
WiFiClient plainClient;
PubSubClient client(espClient);
bool usingWokwiPlainFallback = false;

bool isWokwiGuestNetwork() {
    IPAddress ip = WiFi.localIP();
    bool isWokwiSubnet = (ip[0] == 10 && ip[1] == 13 && ip[2] == 37);
    return (strcmp(ssid, "Wokwi-GUEST") == 0) || isWokwiSubnet;
}

volatile bool estop_active = false;
unsigned long lastMqttRetry = 0;
unsigned long lastSensorPoll = 0;
float lastReportedTemp = -999.0f;
float lastReportedHum = -999.0f;
float lastReportedFlow = -999.0f;
bool lastReportedHoclLow = false;
bool lastReportedCycleInProgress = false;
bool hasPublishedSensorStatus = false;
bool backendSynced = false;
bool statusSyncRequested = false;
float hoclRemainingMl = HOCL_CAPACITY_ML;
bool lastPhysicalFloatLow = false;
float lastReportedHoclMl = -1.0f;
bool presenceSensorEnabled = true;
bool lastReportedPresence = false;
bool remoteTriggerIsScheduled = false;
char remoteTriggerType[32] = "MANUAL_WEB";

bool isPresenceDetected() {
    return digitalRead(PIN_PIR) == HIGH;
}

bool isPhysicalFloatLow() {
    return digitalRead(PIN_HOCL_FLOAT) == LOW;
}

bool isHoclLow() {
    return isPhysicalFloatLow() || (hoclRemainingMl <= HOCL_LOW_THRESHOLD_ML);
}

float estimateRequiredMl(unsigned long durationMs) {
    int potValue = analogRead(PIN_POT);
    float currentFlow = (float)map(potValue, 0, 4095, 10, 50); // mL/min proxy
    return currentFlow * (durationMs / 60000.0f);
}

bool hasEnoughHoclFor(unsigned long durationMs) {
    if (isHoclLow()) return false;
    float usableMl = hoclRemainingMl - HOCL_LOW_THRESHOLD_ML;
    return usableMl >= estimateRequiredMl(durationMs);
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
    char jsonBuffer[640];
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

    // 1b. Handle Remote Reservoir Refill
    if (strcmp(command, "REFILL") == 0) {
        float targetMl = doc["hocl_remaining_ml"] | HOCL_CAPACITY_ML;
        if (targetMl < 0.0f) targetMl = 0.0f;
        if (targetMl > HOCL_CAPACITY_ML) targetMl = HOCL_CAPACITY_ML;
        hoclRemainingMl = targetMl;
        backendSynced = true;
        Serial.printf("[REFILL] Reservoir refilled to %.1f / %.1f mL\n", hoclRemainingMl, HOCL_CAPACITY_ML);
        statusSyncRequested = true;
        return;
    }

    // 1c. Handle Device Configuration (Presence Interlock Toggle & Boot Reservoir Sync)
    if (strcmp(command, "CONFIG") == 0) {
        if (!doc["presence_sensor_enabled"].isNull()) {
            presenceSensorEnabled = doc["presence_sensor_enabled"].as<bool>();
            Serial.printf(
                "[CONFIG] Presence sensor interlock (scheduled cycles) -> %s\n",
                presenceSensorEnabled ? "ENABLED" : "DISABLED"
            );
        }
        if (!doc["hocl_remaining_ml"].isNull()) {
            float targetMl = doc["hocl_remaining_ml"].as<float>();
            if (targetMl < 0.0f) targetMl = 0.0f;
            if (targetMl > HOCL_CAPACITY_ML) targetMl = HOCL_CAPACITY_ML;
            hoclRemainingMl = targetMl;
            Serial.printf("[CONFIG] Synced persisted HOCl reservoir -> %.1f / %.1f mL\n", hoclRemainingMl, HOCL_CAPACITY_ML);
        }
        backendSynced = true;
        statusSyncRequested = true;
        return;
    }

    // 2. Handle Remote Trigger START (defer execution & safety interlocks to loop() so ledger telemetry can publish safely)
    if (strcmp(command, "START") == 0) {
        if (cycleInProgress || remoteTriggerRequested) {
            Serial.println("[WARN] Trigger ignored: Cycle already in progress.");
            return;
        }
        const char* trigType = doc["trigger_type"] | "MANUAL_WEB";
        bool isScheduled = (strcmp(trigType, "SCHEDULED") == 0);
        int durationSec = doc["duration_sec"] | 30;
        if (durationSec < 5) durationSec = 5;
        if (durationSec > 300) durationSec = 300;
        unsigned long requestedMs = (unsigned long)durationSec * 1000UL;
        float roomVol = doc["room_volume_m3"] | 1.0f;
        if (roomVol <= 0.0f) roomVol = DEFAULT_ROOM_VOLUME_M3;
        remoteMistDurationMs = requestedMs;
        remoteRoomVolumeM3 = roomVol;
        remoteTriggerIsScheduled = isScheduled;
        strncpy(remoteTriggerType, trigType, sizeof(remoteTriggerType) - 1);
        remoteTriggerType[sizeof(remoteTriggerType) - 1] = '\0';
        remoteTriggerRequested = true;
        Serial.print("[REMOTE TRIGGER] Queued (");
        Serial.print(remoteTriggerType);
        Serial.print(")! Duration: ");
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

    // Preserve Wokwi DHCP configuration untouched when DHCP already provided DNS
    IPAddress dhcpDns = WiFi.dnsIP(0);
    if (dhcpDns == INADDR_NONE || dhcpDns == IPAddress(0, 0, 0, 0)) {
        WiFi.config(WiFi.localIP(), WiFi.gatewayIP(), WiFi.subnetMask(), IPAddress(8, 8, 8, 8), IPAddress(1, 1, 1, 1));
    }

    Serial.print("\n[WIFI] Connected! IP Address: ");
    Serial.print(WiFi.localIP());
    Serial.print(" | DNS: ");
    Serial.println(WiFi.dnsIP(0));

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
    bool presenceDetected = isPresenceDetected();

    if (forcePublish ||
        !hasPublishedSensorStatus ||
        hoclLow != lastReportedHoclLow ||
        presenceDetected != lastReportedPresence ||
        cycleInProgress != lastReportedCycleInProgress ||
        fabs(hoclRemainingMl - lastReportedHoclMl) >= 1.0f ||
        fabs(temp - lastReportedTemp) >= 0.2f ||
        fabs(hum - lastReportedHum) >= 0.5f ||
        fabs(flow - lastReportedFlow) >= 1.0f) {
        lastReportedTemp = temp;
        lastReportedHum = hum;
        lastReportedFlow = flow;
        lastReportedHoclLow = hoclLow;
        lastReportedPresence = presenceDetected;
        lastReportedCycleInProgress = cycleInProgress;
        lastReportedHoclMl = hoclRemainingMl;
        hasPublishedSensorStatus = true;

        Serial.printf(
            "[SENSOR] Live DHT22 & Flow -> Temp: %.1f C | Hum: %.1f %% | Flow: %.1f mL/min | HOCl: %.1f/%.0f mL (%s) | PIR: %s (%s)\n",
            temp,
            hum,
            flow,
            hoclRemainingMl,
            HOCL_CAPACITY_ML,
            hoclLow ? "LOW (DEPLETED)" : "OK",
            presenceDetected ? "OCCUPIED" : "CLEAR",
            presenceSensorEnabled ? "ON" : "OFF"
        );

        if (client.connected()) {
            JsonDocument doc;
            doc["station_id"] = device_station_id;
            doc["ambient_temp_c"] = temp;
            doc["relative_humidity_pct"] = hum;
            doc["flow_rate_ml_min"] = flow;
            doc["hocl_low_level"] = hoclLow;
            doc["hocl_remaining_ml"] = roundf(hoclRemainingMl * 10.0f) / 10.0f;
            doc["hocl_capacity_ml"] = HOCL_CAPACITY_ML;
            doc["presence_detected"] = presenceDetected;
            doc["presence_sensor_enabled"] = presenceSensorEnabled;
            doc["cycle_in_progress"] = cycleInProgress;
            if (!backendSynced) {
                doc["boot_sync"] = true;
            }
            doc["auth_key"] = device_auth_key;

            char buffer[448];
            serializeJson(doc, buffer, sizeof(buffer));
            client.publish(status_topic, buffer);
        }
    }
}

// Non-blocking MQTTS (TLS 8883) with Wokwi NAT fallback to authenticated port 1883
void reconnectMQTT(bool force) {
    if (client.connected()) return;

    unsigned long now = millis();
    if (force || now - lastMqttRetry > 3000) {
        lastMqttRetry = now;

        IPAddress brokerIp;
        if (!WiFi.hostByName(mqtt_server_host, brokerIp)) {
            Serial.println("[MQTT] DNS lookup failed (will retry in 3s)");
            return;
        }

        // Hardware-seeded randomized client ID prevents broker session collisions
        String clientId = "MistSan-ESP32-" + String(micros(), HEX) + "-" + String(random(1000, 9999));
        bool connected = false;
        int activePort = mqtt_port;

        if (!usingWokwiPlainFallback) {
            Serial.print("[MQTTS] Attempting TLS connection to ");
            Serial.print(mqtt_server_host);
            Serial.print(":");
            Serial.print(mqtt_port);
            Serial.print("... ");

            espClient.setCACert(mqtt_ca_cert);
            client.setClient(espClient);
            client.setServer(mqtt_server_host, mqtt_port);
            connected = (strlen(mqtt_username) > 0)
                ? client.connect(clientId.c_str(), mqtt_username, mqtt_password)
                : client.connect(clientId.c_str());

            if (!connected) {
                char tlsErr[128] = {0};
                int tlsCode = espClient.lastError(tlsErr, sizeof(tlsErr));
                espClient.stop();
                Serial.print("failed, rc=");
                Serial.print(client.state());
                Serial.print(", ip=");
                Serial.print(brokerIp);
                Serial.print(", tls=");
                Serial.print(tlsCode);
                if (tlsErr[0] != '\0') {
                    Serial.print(" (");
                    Serial.print(tlsErr);
                    Serial.print(")");
                }
                Serial.println();

                if (isWokwiGuestNetwork()) {
                    if (tlsCode != -80) {
                        Serial.println("[WOKWI TLS] Retrying port 8883 with insecure TLS mode...");
                        espClient.setInsecure();
                        client.setClient(espClient);
                        client.setServer(mqtt_server_host, mqtt_port);
                        connected = (strlen(mqtt_username) > 0)
                            ? client.connect(clientId.c_str(), mqtt_username, mqtt_password)
                            : client.connect(clientId.c_str());
                        if (!connected) {
                            espClient.stop();
                        }
                    }
                    if (!connected) {
                        usingWokwiPlainFallback = true;
                        Serial.println("[WOKWI NAT FALLBACK] Port 8883 reset on Wokwi-GUEST; connecting to broker.emqx.io:1883 (payload auth_key enforced)...");
                    }
                }
            }
        }

        if (usingWokwiPlainFallback) {
            activePort = mqtt_plain_port;
            plainClient.stop();
            plainClient.setTimeout(15000); // milliseconds (Arduino-ESP32 3.x NetworkClient)
            client.setClient(plainClient);
            client.setServer(mqtt_server_host, mqtt_plain_port);
            connected = (strlen(mqtt_username) > 0)
                ? client.connect(clientId.c_str(), mqtt_username, mqtt_password)
                : client.connect(clientId.c_str());
            if (!connected) {
                plainClient.stop();
                Serial.printf(
                    "[WOKWI NAT FALLBACK] Connect to %s:%d failed, rc=%d (will retry in 3s)\n",
                    mqtt_server_host,
                    mqtt_plain_port,
                    client.state()
                );
            }
        }

        if (connected) {
            Serial.println(activePort == mqtt_port ? "connected (TLS)!" : "connected (Wokwi 1883 fallback)!");
            client.subscribe(control_topic);
            Serial.print("[MQTT SUB] Subscribed to topic: ");
            Serial.println(control_topic);
            pollAndPublishLiveSensors(true);
        } else if (!usingWokwiPlainFallback) {
            Serial.println("[MQTTS] Will retry TLS in 3s");
        }
    }
}

void publishTelemetry(float temp, float hum, float flow, float durationSec, float roomVol, bool estop, const char* triggerType, bool presenceAborted, bool hoclAborted) {
    client.setBufferSize(640);

    // CRITICAL: Reconnect BEFORE publishing if the connection dropped during misting
    if (!client.connected()) {
        Serial.println("[MQTT] Socket disconnected during misting! Reconnecting before publish...");
        reconnectMQTT(true);
    }

    JsonDocument doc;
    doc["station_id"] = device_station_id;
    doc["ambient_temp_c"] = isnan(temp) ? 25.0f : temp;
    doc["relative_humidity_pct"] = isnan(hum) ? 65.0f : hum;
    doc["flow_rate_ml_min"] = (estop || presenceAborted || hoclAborted) ? 0.0f : flow;
    doc["cycle_duration_sec"] = durationSec;
    doc["room_volume_m3"] = roomVol;
    doc["delivery_confirmed"] = (flow > 5.0f && !estop && !presenceAborted && !hoclAborted);
    doc["estop_triggered"] = estop;
    doc["presence_aborted"] = presenceAborted;
    doc["hocl_aborted"] = hoclAborted;
    doc["presence_detected"] = isPresenceDetected();
    doc["trigger_type"] = triggerType;
    doc["hocl_remaining_ml"] = roundf(hoclRemainingMl * 10.0f) / 10.0f;
    doc["hocl_capacity_ml"] = HOCL_CAPACITY_ML;
    doc["auth_key"] = device_auth_key;

    char buffer[640];
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

void runSanitizationCycle(unsigned long durationMs, float roomVol, bool enforcePresence, const char* triggerType) {
    if (enforcePresence && presenceSensorEnabled && isPresenceDetected()) {
        float latestTemp = 28.5f;
        float latestHum = 65.0f;
        readSanitizedEnvironment(latestTemp, latestHum);
        Serial.println("[INTERLOCK] Scheduled cycle blocked: person detected in room!");
        tone(PIN_BUZZER, BUZZER_FREQ_HZ);
        delay(250);
        noTone(PIN_BUZZER);
        digitalWrite(PIN_BUZZER, LOW);
        publishTelemetry(latestTemp, latestHum, 0.0f, 0.0f, roomVol, false, "SCHEDULED_BLOCKED_PRESENCE", true, false);
        statusSyncRequested = true;
        return;
    }

    if (!hasEnoughHoclFor(durationMs)) {
        float latestTemp = 28.5f;
        float latestHum = 65.0f;
        readSanitizedEnvironment(latestTemp, latestHum);
        float neededMl = estimateRequiredMl(durationMs);
        float usableMl = max(0.0f, hoclRemainingMl - HOCL_LOW_THRESHOLD_ML);
        Serial.printf(
            "[INTERLOCK] Cycle blocked: needs %.1f mL, only %.1f mL usable (%.1f/%.0f mL remaining)\n",
            neededMl,
            usableMl,
            hoclRemainingMl,
            HOCL_CAPACITY_ML
        );
        tone(PIN_BUZZER, BUZZER_FREQ_HZ);
        delay(250);
        noTone(PIN_BUZZER);
        digitalWrite(PIN_BUZZER, LOW);
        if (strcmp(triggerType, "SCHEDULED") == 0) {
            publishTelemetry(latestTemp, latestHum, 0.0f, 0.0f, roomVol, false, "SCHEDULED_BLOCKED_HOCL", false, true);
        }
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
        if (enforcePresence && presenceSensorEnabled && isPresenceDetected()) {
            noTone(PIN_BUZZER);
            digitalWrite(PIN_BUZZER, LOW);
            Serial.println("[INTERLOCK] Scheduled warning aborted: person detected in room!");
            publishTelemetry(latestTemp, latestHum, 0.0f, (millis() - cycleStart) / 1000.0f, roomVol, false, "SCHEDULED_ABORTED_PRESENCE", true, false);
            cycleInProgress = false;
            statusSyncRequested = true;
            return;
        }
        if (isHoclLow()) {
            noTone(PIN_BUZZER);
            digitalWrite(PIN_BUZZER, LOW);
            Serial.println("[INTERLOCK] Pre-cycle warning aborted: HOCl reservoir depleted!");
            publishTelemetry(latestTemp, latestHum, 0.0f, (millis() - cycleStart) / 1000.0f, roomVol, false, triggerType, false, true);
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
            publishTelemetry(latestTemp, latestHum, 0.0f, (millis() - cycleStart) / 1000.0f, roomVol, true, triggerType, false, false);
            cycleInProgress = false;
            statusSyncRequested = true;
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
    unsigned long lastFlowTickMs = mistStart;
    float totalFlow = 0.0f;
    int samples = 0;

    while (millis() - mistStart < durationMs) {
        if (enforcePresence && presenceSensorEnabled && isPresenceDetected()) {
            digitalWrite(PIN_ATOMIZER, LOW);
            Serial.println("[INTERLOCK] Scheduled misting aborted mid-cycle: person detected in room!");
            readSanitizedEnvironment(latestTemp, latestHum);
            float earlyDuration = (millis() - mistStart) / 1000.0f;
            publishTelemetry(latestTemp, latestHum, 0.0f, earlyDuration, roomVol, false, "SCHEDULED_ABORTED_PRESENCE", true, false);
            cycleInProgress = false;
            statusSyncRequested = true;
            return;
        }
        if (isHoclLow()) {
            digitalWrite(PIN_ATOMIZER, LOW);
            Serial.println("[INTERLOCK] Misting halted: HOCl reservoir depleted mid-cycle!");
            readSanitizedEnvironment(latestTemp, latestHum);
            float earlyDuration = (millis() - mistStart) / 1000.0f;
            publishTelemetry(latestTemp, latestHum, 0.0f, earlyDuration, roomVol, false, triggerType, false, true);
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
            publishTelemetry(latestTemp, latestHum, 0.0f, earlyDuration, roomVol, true, triggerType, false, false);
            cycleInProgress = false;
            statusSyncRequested = true;
            return;
        }

        // Read atomizer flow proxy via potentiometer and subtract dispensed mL
        unsigned long nowMs = millis();
        float dtSec = (nowMs - lastFlowTickMs) / 1000.0f;
        lastFlowTickMs = nowMs;

        int potValue = analogRead(PIN_POT);
        float currentFlow = (float)map(potValue, 0, 4095, 10, 50); // mL/min proxy
        hoclRemainingMl = max(0.0f, hoclRemainingMl - (currentFlow * (dtSec / 60.0f)));
        totalFlow += currentFlow;
        samples++;

        // Sample DHT22 environment sensor and stream live reservoir level (~2 sec cadence)
        if (samples % 20 == 0) {
            readSanitizedEnvironment(latestTemp, latestHum);
            pollAndPublishLiveSensors(true);
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

    publishTelemetry(latestTemp, latestHum, avgFlow, actualDurationSec, roomVol, false, triggerType, false);
    statusSyncRequested = true;
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
    pinMode(PIN_PIR, INPUT);
    lastPhysicalFloatLow = isPhysicalFloatLow();

    attachInterrupt(digitalPinToInterrupt(PIN_BTN_ESTOP), handleEmergencyStop, FALLING);

    setupWiFi();
    espClient.setCACert(mqtt_ca_cert);
    espClient.setTimeout(30000);          // milliseconds (Arduino-ESP32 3.x NetworkClientSecure)
    espClient.setHandshakeTimeout(60);    // seconds (generous for Wokwi simulated TLS handshake)
    client.setServer(mqtt_server_host, mqtt_port);
    client.setCallback(onMqttMessage);
    client.setBufferSize(640);
    client.setSocketTimeout(30);

    Serial.println("--- MistSan ESP32 Firmware Ready (MQTTS TLS) ---");
}

void loop() {
    if (!client.connected()) {
        reconnectMQTT();
    } else {
        client.loop();
    }

    unsigned long now = millis();
    bool physLow = isPhysicalFloatLow();
    if (lastPhysicalFloatLow && !physLow) {
        hoclRemainingMl = HOCL_CAPACITY_ML;
        backendSynced = true;
        Serial.printf("[REFILL] Float switch restored -> reservoir reset to %.1f mL\n", hoclRemainingMl);
        statusSyncRequested = true;
    }
    lastPhysicalFloatLow = physLow;

    if (statusSyncRequested || (hasPublishedSensorStatus && (isHoclLow() != lastReportedHoclLow || isPresenceDetected() != lastReportedPresence))) {
        statusSyncRequested = false;
        pollAndPublishLiveSensors(true);
    }
    if (now - lastSensorPoll >= 2500) {
        lastSensorPoll = now;
        pollAndPublishLiveSensors();
    }

    // Physical manual start button check uses fixed manual defaults (disregards presence sensor)
    if (!cycleInProgress && digitalRead(PIN_BTN_START) == LOW) {
        delay(30); // Debounce
        if (digitalRead(PIN_BTN_START) == LOW) {
            runSanitizationCycle(DEFAULT_MIST_DURATION_MS, DEFAULT_ROOM_VOLUME_M3, false, "MANUAL_BUTTON");
            while (digitalRead(PIN_BTN_START) == LOW) {
                delay(10);
            }
        }
    }

    // Remote Web / Scheduled MQTT Trigger uses requested parameters (presence enforced only on SCHEDULED)
    if (remoteTriggerRequested) {
        remoteTriggerRequested = false;
        runSanitizationCycle(remoteMistDurationMs, remoteRoomVolumeM3, remoteTriggerIsScheduled, remoteTriggerType);
    }

    delay(10); // Prevents CPU starvation and watchdog timeouts
}
