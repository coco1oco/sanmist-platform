#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include "DHT.h"

// Hardware Pin Definitions
#define PIN_DHT          15
#define PIN_POT          34    // YF-S401 analog proxy (ADC1 CH6)
#define PIN_BUZZER       19
#define PIN_ATOMIZER     4     // Relay / LED indicator
#define PIN_BTN_START    18    // Active-LOW (INPUT_PULLUP)
#define PIN_BTN_ESTOP    5     // Active-LOW (INPUT_PULLUP)

#define DHTTYPE DHT22
DHT dht(PIN_DHT, DHTTYPE);

// Dynamic Runtime Parameters
unsigned long currentMistDurationMs = 30000; // default 30s
float currentRoomVolumeM3 = 1.0;            // default 1.0 m³
bool remoteTriggerRequested = false;

// Wokwi WiFi Credentials
const char* ssid = "Wokwi-GUEST";
const char* password = "";

// Public MQTT Broker & Topics
const char* mqtt_server = "broker.emqx.io";
const int mqtt_port = 1883;
const char* telemetry_topic = "sanmist/devices/MIST-BOH-01/telemetry";
const char* control_topic = "sanmist/control/trigger";

WiFiClient espClient;
PubSubClient client(espClient);

volatile bool estop_active = false;
unsigned long lastMqttRetry = 0;

void IRAM_ATTR handleEmergencyStop() {
    estop_active = true;
    digitalWrite(PIN_ATOMIZER, LOW);
    digitalWrite(PIN_BUZZER, LOW);
}

// -------------------------------------------------------------
// Inbound MQTT Message Listener (Remote Dispatch)
// -------------------------------------------------------------
void onMqttMessage(char* topic, byte* payload, unsigned int length) {
    char message[length + 1];
    memcpy(message, payload, length);
    message[length] = '\0';

    Serial.print("[MQTT INBOUND] Topic: ");
    Serial.println(topic);
    Serial.print("[MQTT INBOUND] Payload: ");
    Serial.println(message);

    if (String(topic) == control_topic) {
        StaticJsonDocument<256> doc;
        DeserializationError error = deserializeJson(doc, message);

        if (!error) {
            const char* command = doc["command"];
            if (command && strcmp(command, "START") == 0) {
                int durationSec = doc["duration_sec"] | 30;
                float roomVol = doc["room_volume_m3"] | 1.0;

                currentMistDurationMs = (unsigned long)durationSec * 1000;
                currentRoomVolumeM3 = roomVol;
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
    Serial.println("\n[WIFI] Connected! IP Address: ");
    Serial.println(WiFi.localIP());
}

// Non-blocking MQTT connection with unique client ID
void reconnectMQTT() {
    if (client.connected()) return;

    unsigned long now = millis();
    if (now - lastMqttRetry > 2000) {
        lastMqttRetry = now;
        Serial.print("[MQTT] Attempting connection... ");

        // Hardware-seeded randomized client ID prevents broker session collisions
        String clientId = "MistSan-ESP32-" + String(micros(), HEX) + "-" + String(random(1000, 9999));

        if (client.connect(clientId.c_str())) {
            Serial.println("connected!");
            client.subscribe(control_topic);
            Serial.print("[MQTT SUB] Subscribed to topic: ");
            Serial.println(control_topic);
        } else {
            Serial.print("failed, rc=");
            Serial.print(client.state());
            Serial.println(" (will retry in 2s)");
        }
    }
}

void publishTelemetry(float temp, float hum, float flow, float duration, float roomVol, bool estop) {
    StaticJsonDocument<256> doc;
    doc["station_id"] = "MIST-BOH-01";
    doc["ambient_temp_c"] = temp;
    doc["relative_humidity_pct"] = hum;
    doc["flow_rate_ml_min"] = flow;
    doc["cycle_duration_sec"] = duration;
    doc["room_volume_m3"] = roomVol;
    doc["delivery_confirmed"] = (flow > 5.0 && !estop);
    doc["estop_triggered"] = estop;

    char buffer[256];
    serializeJson(doc, buffer);
    client.publish(telemetry_topic, buffer);
    Serial.print("[MQTT PUB] Sent Payload: ");
    Serial.println(buffer);
}

void runSanitizationCycle(unsigned long durationMs, float roomVol) {
    estop_active = false;
    unsigned long cycleStart = millis();

    Serial.println("[START] Pre-cycle audible warning initiated...");
    digitalWrite(PIN_BUZZER, HIGH);
    
    // Non-blocking 1-second pre-warning check for E-Stop
    unsigned long bzStart = millis();
    while (millis() - bzStart < 1000) {
        if (estop_active) {
            digitalWrite(PIN_BUZZER, LOW);
            Serial.println("[HALT] E-Stop triggered during pre-cycle warning!");
            publishTelemetry(0, 0, 0, (millis() - cycleStart) / 1000.0, roomVol, true);
            return;
        }
        delay(10);
    }
    digitalWrite(PIN_BUZZER, LOW);

    Serial.println("[MISTING] Atomizer active...");
    digitalWrite(PIN_ATOMIZER, HIGH);

    unsigned long mistStart = millis();
    float totalFlow = 0;
    int samples = 0;

    while (millis() - mistStart < durationMs) {
        if (estop_active) {
            Serial.println("[HALT] EMERGENCY STOP ACTIVATED DURING MISTING!");
            digitalWrite(PIN_ATOMIZER, LOW);
            publishTelemetry(0, 0, 0, (millis() - mistStart) / 1000.0, roomVol, true);
            return;
        }

        int potValue = analogRead(PIN_POT);
        float currentFlow = map(potValue, 0, 4095, 10, 50); // mL/min proxy
        totalFlow += currentFlow;
        samples++;

        // Keep MQTT connection alive and responsive
        client.loop();
        delay(100);
    }

    digitalWrite(PIN_ATOMIZER, LOW);
    Serial.println("[COMPLETE] Sanitization cycle finished!");

    float avgFlow = (samples > 0) ? (totalFlow / samples) : 0;
    float temp = dht.readTemperature();
    float hum = dht.readHumidity();

    if (isnan(temp)) temp = 28.5;
    if (isnan(hum)) hum = 65.0;

    publishTelemetry(temp, hum, avgFlow, durationMs / 1000.0, roomVol, false);
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
    client.setServer(mqtt_server, mqtt_port);
    client.setCallback(onMqttMessage);

    Serial.println("--- MistSan ESP32 Firmware Ready ---");
}

void loop() {
    if (!client.connected()) {
        reconnectMQTT();
    } else {
        client.loop();
    }

    // Physical manual start button check
    if (digitalRead(PIN_BTN_START) == LOW) {
        delay(30); // Debounce
        if (digitalRead(PIN_BTN_START) == LOW) {
            runSanitizationCycle(currentMistDurationMs, currentRoomVolumeM3);
        }
    }

    // Remote Web MQTT Trigger
    if (remoteTriggerRequested) {
        remoteTriggerRequested = false;
        runSanitizationCycle(currentMistDurationMs, currentRoomVolumeM3);
    }

    delay(10); // Prevents CPU starvation and watchdog timeouts
}