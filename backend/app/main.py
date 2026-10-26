import os
import joblib
import sqlite3
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(
    title="MistSan Telemetry & Compliance Ingestion Engine",
    description="Real-time ML-evaluated dry-fog sanitization audit microservice (PD 856 / RA 10611 compliant)",
    version="1.0.0"
)

# Enable CORS for the future frontend dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. Resolve Path to Trained Model Artifact
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "ml-pipeline", "models", "xgboost_sei_classifier.pkl"))

model = None
try:
    model = joblib.load(MODEL_PATH)
    print(f"[INIT] Loaded XGBoost classifier from: {MODEL_PATH}")
except Exception as e:
    print(f"[WARN] Could not load model: {e}")

# 2. Initialize SQLite Audit Trail Database
DB_PATH = os.path.join(BASE_DIR, "sanmist_audit.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cycle_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            timestamp TEXT,
            ambient_temp_c REAL,
            relative_humidity_pct REAL,
            flow_rate_ml_min REAL,
            cycle_duration_sec REAL,
            room_volume_m3 REAL,
            sei_score REAL,
            predicted_class INTEGER,
            efficacy_label TEXT,
            delivery_confirmed INTEGER,
            estop_triggered INTEGER
        )
    """)
    conn.commit()
    conn.close()

init_db()

# 3. Request Data Schemas
class CycleTelemetryInput(BaseModel):
    station_id: str = Field(default="MIST-BOH-01")
    ambient_temp_c: float = Field(..., ge=15.0, le=50.0, description="Temperature in Celsius from DHT22")
    relative_humidity_pct: float = Field(..., ge=20.0, le=100.0, description="Relative Humidity % from DHT22")
    flow_rate_ml_min: float = Field(..., ge=0.0, le=100.0, description="Flow rate in mL/min from YF-S401 proxy")
    cycle_duration_sec: float = Field(..., ge=10.0, le=600.0, description="Misting duration in seconds")
    room_volume_m3: float = Field(default=25.0, ge=5.0, le=200.0, description="Kitchen zone volume in m³")
    delivery_confirmed: bool = Field(default=True)
    estop_triggered: bool = Field(default=False)

# 4. API Endpoints
@app.get("/")
def health_check():
    return {
        "status": "online",
        "service": "MistSan Compliance API",
        "model_loaded": model is not None
    }

@app.post("/api/v1/telemetry/evaluate")
def evaluate_and_log_cycle(data: CycleTelemetryInput):
    if model is None:
        raise HTTPException(status_code=503, detail="XGBoost model not loaded.")

    # Empirical Saturation Efficiency Index (SEI) calculation
    sei = ((data.flow_rate_ml_min * (data.cycle_duration_sec / 60.0)) / data.room_volume_m3) * (1.0 - (data.relative_humidity_pct / 150.0))
    
    # Feature vector matching training columns
    features = [[
        data.ambient_temp_c,
        data.relative_humidity_pct,
        data.flow_rate_ml_min,
        data.cycle_duration_sec,
        data.room_volume_m3
    ]]
    
    # Model inference
    pred_class = int(model.predict(features)[0])
    label_map = {0: "Fail", 1: "Marginal", 2: "Pass"}
    efficacy_label = label_map.get(pred_class, "Unknown")
    
    # If emergency stop was triggered, override to Fail
    if data.estop_triggered:
        pred_class = 0
        efficacy_label = "Fail (E-Stop Triggered)"

    # Persist record to SQLite
    now = datetime.utcnow().isoformat() + "Z"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO cycle_records (
            station_id, timestamp, ambient_temp_c, relative_humidity_pct,
            flow_rate_ml_min, cycle_duration_sec, room_volume_m3,
            sei_score, predicted_class, efficacy_label, delivery_confirmed, estop_triggered
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.station_id, now, data.ambient_temp_c, data.relative_humidity_pct,
        data.flow_rate_ml_min, data.cycle_duration_sec, data.room_volume_m3,
        round(sei, 3), pred_class, efficacy_label, int(data.delivery_confirmed), int(data.estop_triggered)
    ))
    conn.commit()
    cycle_id = cursor.lastrowid
    conn.close()

    return {
        "cycle_id": cycle_id,
        "timestamp": now,
        "station_id": data.station_id,
        "sei_score": round(sei, 3),
        "predicted_class": pred_class,
        "efficacy_label": efficacy_label,
        "compliance_status": "AUDIT_VERIFIED" if pred_class == 2 else "REQUIRES_ATTENTION"
    }

@app.get("/api/v1/records")
def get_audit_records(limit: int = 20):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cycle_records ORDER BY id DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

import json
import paho.mqtt.client as mqtt

# --- Background MQTT Telemetry Subscriber ---
def on_mqtt_connect(client, userdata, flags, rc):
    print(f"\n[MQTT] Connected to EMQX public broker (code {rc})")
    client.subscribe("sanmist/devices/+/telemetry")
    print("[MQTT] Subscribed to topic: sanmist/devices/+/telemetry")

def on_mqtt_message(client, userdata, msg):
    try:
        raw_payload = msg.payload.decode()
        print(f"\n[MQTT RECV] Inbound ESP32 Telemetry: {raw_payload}")
        data_dict = json.loads(raw_payload)
        
        # Validate schema, run XGBoost prediction, and save to SQLite
        cycle_in = CycleTelemetryInput(**data_dict)
        result = evaluate_and_log_cycle(cycle_in)
        print(f"[ML EVALUATION RESULT] Station: {result['station_id']} | Class: {result['efficacy_label']} | SEI Score: {result['sei_score']} | Status: {result['compliance_status']}")
    except Exception as err:
        print(f"[MQTT ERROR] Failed to process payload: {err}")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_mqtt_connect
mqtt_client.on_message = on_mqtt_message
try:
    mqtt_client.connect_async("broker.emqx.io", 1883, 60)
    mqtt_client.loop_start()
    print("[MQTT] Background thread started, listening on broker.emqx.io:1883...")
except Exception as e:
    print(f"[WARN] Could not connect to MQTT broker: {e}")