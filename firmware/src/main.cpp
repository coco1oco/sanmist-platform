#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include "DHT.h"

// Hardware Pin Definitions
#define PIN_DHT 15
#define PIN_POT 34         // Analog proxy para sa YF-S401 Flow Sensor
#define PIN_BUZZER 19
#define PIN_ATOMIZER 4      // Mist Relay indicator
#define PIN_BTN_START 18
#define PIN_BTN_ESTOP 5

#define DHTTYPE DHT22
DHT dht(PIN_DHT, DHTTYPE);

// Wokwi WiFi Credentials
const char* ssid = "Wokwi-GUEST";
const char* password = "";

// Public MQTT Broker
const char* mqtt_server = "broker.emqx.io";
const int mqtt_port = 1883;
const char* telemetry_topic = "sanmist/devices/MIST-BOH-01/telemetry";
const char* events_topic = "sanmist/devices/MIST-BOH-01/events";

WiFiClient espClient;
PubSubClient client(espClient);

volatile bool estop_active = false;

void IRAM_ATTR handleEmergencyStop() {
    estop_active = true;
    digitalWrite(PIN_ATOMIZER, LOW);
    digitalWrite(PIN_BUZZER, LOW);
}

void setupWiFi() {
    delay(10);
    Serial.println();
    Serial.print("[WIFI] Connecting to ");
    Serial.println(ssid);
    WiFi.begin(ssid, password);
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("\n[WIFI] Connected! IP Address: ");
    Serial.println(WiFi.localIP());
}

void reconnectMQTT() {
    while (!client.connected()) {
        Serial.print("[MQTT] Attempting connection...");
        String clientId = "ESP32-SanMist-" + String(random(0xffff), HEX);
        if (client.connect(clientId.c_str())) {
            Serial.println(" connected!");
            client.publish(events_topic, "{\"status\": \"ONLINE\", \"station_id\": \"MIST-BOH-01\"}");
        } else {
            Serial.print(" failed, rc=");
            Serial.print(client.state());
            Serial.println(" retrying in 2 seconds...");
            delay(2000);
        }
    }
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

    Serial.println("--- MistSan ESP32 Firmware Ready ---");
}

void publishTelemetry(float temp, float hum, float flow, float duration, bool estop) {
    StaticJsonDocument<256> doc;
    doc["station_id"] = "MIST-BOH-01";
    doc["ambient_temp_c"] = temp;
    doc["relative_humidity_pct"] = hum;
    doc["flow_rate_ml_min"] = flow;
    doc["cycle_duration_sec"] = duration;
    doc["room_volume_m3"] = 25.0;
    doc["delivery_confirmed"] = (flow > 5.0);
    doc["estop_triggered"] = estop;

    char buffer[256];
    serializeJson(doc, buffer);
    client.publish(telemetry_topic, buffer);
    Serial.print("[MQTT PUB] Sent Payload: ");
    Serial.println(buffer);
}

void runSanitizationCycle() {
    estop_active = false;
    Serial.println("[START] Pre-cycle audible warning initiated...");
    digitalWrite(PIN_BUZZER, HIGH);
    delay(1000);
    digitalWrite(PIN_BUZZER, LOW);

    if (estop_active) {
        Serial.println("[HALT] E-Stop during pre-cycle!");
        publishTelemetry(0, 0, 0, 0, true);
        return;
    }

    Serial.println("[MISTING] Atomizer active...");
    digitalWrite(PIN_ATOMIZER, HIGH);

    unsigned long startTime = millis();
    unsigned long duration = 10000; // 10 seconds misting test
    float totalFlow = 0;
    int samples = 0;

    while (millis() - startTime < duration) {
        if (estop_active) {
            Serial.println("[HALT] EMERGENCY STOP ACTIVATED DURING MISTING!");
            digitalWrite(PIN_ATOMIZER, LOW);
            publishTelemetry(0, 0, 0, (millis() - startTime) / 1000.0, true);
            return;
        }

        int potValue = analogRead(PIN_POT);
        float currentFlow = map(potValue, 0, 4095, 10, 50); // mL/min proxy
        totalFlow += currentFlow;
        samples++;
        delay(200);
    }

    digitalWrite(PIN_ATOMIZER, LOW);
    Serial.println("[COMPLETE] Sanitization cycle finished!");

    float avgFlow = (samples > 0) ? (totalFlow / samples) : 0;
    float temp = dht.readTemperature();
    float hum = dht.readHumidity();

    if (isnan(temp)) temp = 28.5;
    if (isnan(hum)) hum = 65.0;

    publishTelemetry(temp, hum, avgFlow, duration / 1000.0, false);
}

void loop() {
    if (!client.connected()) {
        reconnectMQTT();
    }
    client.loop();

    if (digitalRead(PIN_BTN_START) == LOW) {
        delay(50); // Debounce
        if (digitalRead(PIN_BTN_START) == LOW) {
            runSanitizationCycle();
        }
    }
}