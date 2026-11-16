#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include "DHT.h"

// Hardware Pin Definitions (aligned with firmware/diagram.json)
#define PIN_DHT          32    // Matches diagram.json esp:32 -> dht:SDA
#define PIN_POT          33    // Matches diagram.json esp:33 -> flow_pot:SIG (ADC1 CH5)
#define PIN_BUZZER       19    // Matches diagram.json esp:19 -> buzzer:2
#define PIN_ATOMIZER     23    // Matches diagram.json esp:23 -> led_mist:A
#define PIN_BTN_START    18    // Matches diagram.json esp:18 -> btn_start (INPUT_PULLUP)
#define PIN_BTN_ESTOP    5     // Matches diagram.json esp:5 -> btn_estop (INPUT_PULLUP)

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

// MQTT Broker (supports private/authenticated broker + shared device payload auth)
const char* mqtt_server_host = "broker.emqx.io";
const char* mqtt_server_fallback_ip = "44.232.241.40";
const int mqtt_port = 1883;
const char* mqtt_username = "";
const char* mqtt_password = "";
const char* device_auth_key = "sanmist-edge-key-2026";
const unsigned int BUZZER_FREQ_HZ = 2000;
const char* telemetry_topic = "sanmist/devices/MIST-BOH-01/telemetry";
const char* status_topic = "sanmist/devices/MIST-BOH-01/status";
const char* control_topic = "sanmist/control/trigger";

WiFiClient espClient;
PubSubClient client(espClient);

volatile bool estop_active = false;
unsigned long lastMqttRetry = 0;
unsigned long lastSensorPoll = 0;
float lastReportedTemp = -999.0f;
float lastReportedHum = -999.0f;
float lastReportedFlow = -999.0f;

void IRAM_ATTR handleEmergencyStop() {
    estop_active = true;
    digitalWrite(PIN_ATOMIZER, LOW);
    digitalWrite(PIN_BUZZER, LOW);
}

// -------------------------------------------------------------
// Inbound MQTT Message Listener (Remote Dispatch)
// -------------------------------------------------------------
void onMqttMessage(char* topic, byte* payload, unsigned int length) {
    char message[256];
    size_t copyLen = (length < sizeof(message) - 1) ? length : (sizeof(message) - 1);
    memcpy(message, payload, copyLen);
    message[copyLen] = '\0';

    Serial.print("[MQTT INBOUND] Topic: ");
    Serial.println(topic);
    Serial.print("[MQTT INBOUND] Payload: ");
    Serial.println(message);

    if (String(topic) == control_topic) {
        JsonDocument doc;
        DeserializationError error = deserializeJson(doc, payload, length);

        if (!error) {
            const char* incomingKey = doc["auth_key"] | "";
            if (strlen(device_auth_key) > 0 && strcmp(incomingKey, device_auth_key) != 0) {
                Serial.println("[MQTT AUTH] Rejected trigger: invalid or missing auth_key.");
                return;
            }
            const char* command = doc["command"];
            if (command && strcmp(command, "START") == 0) {
                if (cycleInProgress) {
                    Serial.println("[REMOTE TRIGGER] Ignored: cycle already in progress.");
                    return;
                }

                int durationSec = doc["duration_sec"] | 30;
                float roomVol = doc["room_volume_m3"] | 1.0f;

                remoteMistDurationMs = (unsigned long)durationSec * 1000UL;
                remoteRoomVolumeM3 = roomVol;
                remoteTriggerRequested = true;

                Serial.println("==================================================");
                Serial.printf("[REMOTE TRIGGER] Accepted! Duration: %d s | Vol: %.2f m3\n", durationSec, roomVol);
                Serial.println("==================================================");
            }
        } else {
            Serial.print("[ERROR] Deserialization failed: ");
            Serial.println(error.f_str());
        }
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

    if (forcePublish ||
        fabs(temp - lastReportedTemp) >= 0.2f ||
        fabs(hum - lastReportedHum) >= 0.5f ||
        fabs(flow - lastReportedFlow) >= 1.0f) {
        lastReportedTemp = temp;
        lastReportedHum = hum;
        lastReportedFlow = flow;

        Serial.printf("[SENSOR] Live DHT22 & Flow -> Temp: %.1f C | Hum: %.1f %% | Flow: %.1f mL/min\n", temp, hum, flow);

        if (client.connected()) {
            JsonDocument doc;
            doc["station_id"] = "MIST-BOH-01";
            doc["ambient_temp_c"] = temp;
            doc["relative_humidity_pct"] = hum;
            doc["flow_rate_ml_min"] = flow;
            doc["cycle_in_progress"] = cycleInProgress;
            doc["auth_key"] = device_auth_key;

            char buffer[320];
            serializeJson(doc, buffer, sizeof(buffer));
            client.publish(status_topic, buffer);
        }
    }
}

// Non-blocking MQTT connection with hostname-first resolution and IPv4 fallback
void reconnectMQTT() {
    if (client.connected()) return;

    unsigned long now = millis();
    if (now - lastMqttRetry > 2000) {
        lastMqttRetry = now;
        Serial.print("[MQTT] Attempting connection to ");
        Serial.print(mqtt_server_host);
        Serial.print("... ");

        // Hardware-seeded randomized client ID prevents broker session collisions
        String clientId = "MistSan-ESP32-" + String(micros(), HEX) + "-" + String(random(1000, 9999));

        client.setServer(mqtt_server_host, mqtt_port);
        bool connected = (strlen(mqtt_username) > 0)
            ? client.connect(clientId.c_str(), mqtt_username, mqtt_password)
            : client.connect(clientId.c_str());

        if (!connected) {
            Serial.print("failed (rc=");
            Serial.print(client.state());
            Serial.print("), trying fallback IP ");
            Serial.print(mqtt_server_fallback_ip);
            Serial.print("... ");
            client.setServer(mqtt_server_fallback_ip, mqtt_port);
            connected = (strlen(mqtt_username) > 0)
                ? client.connect(clientId.c_str(), mqtt_username, mqtt_password)
                : client.connect(clientId.c_str());
        }

        if (connected) {
            Serial.println("connected!");
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
        reconnectMQTT();
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
    cycleInProgress = true;
    estop_active = false;
    unsigned long cycleStart = millis();

    float latestTemp = 28.5f;
    float latestHum = 65.0f;
    readSanitizedEnvironment(latestTemp, latestHum);

    // --- Pre-Cycle Warning Phase (1 second audible alarm) ---
    Serial.println("[START] Pre-cycle audible warning initiated...");
    tone(PIN_BUZZER, BUZZER_FREQ_HZ);

    unsigned long bzStart = millis();
    while (millis() - bzStart < 1000) {
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

    attachInterrupt(digitalPinToInterrupt(PIN_BTN_ESTOP), handleEmergencyStop, FALLING);

    setupWiFi();
    client.setServer(mqtt_server_host, mqtt_port);
    client.setCallback(onMqttMessage);
    client.setBufferSize(512);

    Serial.println("--- MistSan ESP32 Firmware Ready ---");
}

void loop() {
    if (!client.connected()) {
        reconnectMQTT();
    } else {
        client.loop();
    }

    unsigned long now = millis();
    if (now - lastSensorPoll >= 2500) {
        lastSensorPoll = now;
        pollAndPublishLiveSensors();
    }

    // Physical manual start button check uses fixed manual defaults
    if (digitalRead(PIN_BTN_START) == LOW) {
        delay(30); // Debounce
        if (digitalRead(PIN_BTN_START) == LOW) {
            runSanitizationCycle(DEFAULT_MIST_DURATION_MS, DEFAULT_ROOM_VOLUME_M3);
        }
    }

    // Remote Web MQTT Trigger uses requested remote parameters
    if (remoteTriggerRequested) {
        remoteTriggerRequested = false;
        runSanitizationCycle(remoteMistDurationMs, remoteRoomVolumeM3);
    }

    delay(10); // Prevents CPU starvation and watchdog timeouts
}
