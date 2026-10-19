#include <Arduino.h>

const int PIN_START = 18;
const int PIN_ESTOP = 5;
const int PIN_MIST = 23;
const int PIN_BUZZER = 19;
const int PIN_FLOW = 33;

void setup() {
  Serial.begin(115200);
  pinMode(PIN_START, INPUT_PULLUP);
  pinMode(PIN_ESTOP, INPUT_PULLUP);
  pinMode(PIN_MIST, OUTPUT);
  pinMode(PIN_BUZZER, OUTPUT);
  digitalWrite(PIN_MIST, LOW);
  digitalWrite(PIN_BUZZER, LOW);
  Serial.println("--- SanMist ESP32 Firmware Ready ---");
}

void loop() {
  if (digitalRead(PIN_ESTOP) == LOW) {
    digitalWrite(PIN_MIST, LOW);
    digitalWrite(PIN_BUZZER, LOW);
    Serial.println("[HALT] EMERGENCY STOP ACTIVATED!");
    delay(500);
    return;
  }

  if (digitalRead(PIN_START) == LOW) {
    Serial.println("[START] Pre-cycle alert (Buzzer ON)...");
    digitalWrite(PIN_BUZZER, HIGH);
    delay(1000);
    digitalWrite(PIN_BUZZER, LOW);

    Serial.println("[ACTIVE] Atomizer Relay ON (Fogging)...");
    digitalWrite(PIN_MIST, HIGH);
    
    int flowRaw = analogRead(PIN_FLOW);
    Serial.print("Raw Flow Sensor Value: ");
    Serial.println(flowRaw);

    delay(3000);
    digitalWrite(PIN_MIST, LOW);
    Serial.println("[DONE] Sanitization Cycle Completed.");
  }

  delay(100);
}