import os
import joblib
import sqlite3
from datetime import datetime
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="MistSan Ingestion & Compliance Engine",
    description="Real-time ML-evaluated dry-fog sanitization audit microservice (PD 856 / RA 10611 compliant)",
    version="1.0.0"
)

# Pahintulutan ang browser CORS para sa dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    
    # 1. Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cycle_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            station_id TEXT NOT NULL,
            ambient_temp_c REAL NOT NULL,
            relative_humidity_pct REAL NOT NULL,
            flow_rate_ml_min REAL NOT NULL,
            cycle_duration_sec REAL NOT NULL,
            room_volume_m3 REAL NOT NULL,
            sei_score REAL NOT NULL,
            predicted_class INTEGER NOT NULL,
            efficacy_label TEXT NOT NULL,
            estop_triggered INTEGER NOT NULL DEFAULT 0
        )
    """)
    
    # 2. Add compliance_status if it is missing
    cursor.execute("PRAGMA table_info(cycle_records)")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    if "compliance_status" not in existing_columns:
        cursor.execute("ALTER TABLE cycle_records ADD COLUMN compliance_status TEXT DEFAULT 'AUDIT VERIFIED'")
        print("[DB] Added missing column 'compliance_status' to cycle_records.")
        
    conn.commit()
    conn.close()
    print(f"[DB] Initialized/verified schema at: {DB_PATH}")

init_db()

# 3. Request Data Schemas
class CycleTelemetryInput(BaseModel):
    station_id: str = Field(default="MIST-BOH-01")
    ambient_temp_c: float = Field(..., ge=0.0, le=60.0, description="Temperature in Celsius (0.0 allowed on E-Stop/abort)")
    relative_humidity_pct: float = Field(..., ge=0.0, le=100.0, description="Relative Humidity % (0.0 allowed on E-Stop/abort)")
    flow_rate_ml_min: float = Field(..., ge=0.0, le=100.0, description="Flow rate in mL/min from YF-S401 proxy")
    cycle_duration_sec: float = Field(..., ge=0.0, le=600.0, description="Misting duration in seconds (sub-10s allowed on E-Stop)")
    room_volume_m3: float = Field(default=1.0, gt=0, le=200.0, description="Target room/zone volume in m³")
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
def evaluate_and_log_cycle(cycle: CycleTelemetryInput) -> dict:
    # ---------------------------------------------------------
    # 1. E-Stop vs Normal Cycle Branching
    # ---------------------------------------------------------
    if cycle.estop_triggered:
        sei_score = 0.0
        predicted_class = 0
        efficacy_label = "Fail"
        compliance_status = "SAFETY INTERLOCK TRIPPED"
        print(f"[SAFETY] Emergency Stop activated! Cycle halted at {cycle.cycle_duration_sec:.1f}s.")
    else:
        # Standard Sanitization Efficacy Index (SEI) calculation
        duration_min = cycle.cycle_duration_sec / 60.0
        sei_score = round((cycle.flow_rate_ml_min * duration_min) / cycle.room_volume_m3, 3)

        # Predict with XGBoost model if loaded, otherwise fallback
        if "model" in globals() and model is not None:
            features = [[
                cycle.ambient_temp_c,
                cycle.relative_humidity_pct,
                cycle.flow_rate_ml_min,
                cycle.cycle_duration_sec,
                cycle.room_volume_m3
            ]]
            predicted_class = int(model.predict(features)[0])
            label_map = {0: "Fail", 1: "Marginal", 2: "Pass"}
            efficacy_label = label_map.get(predicted_class, "Pass" if sei_score >= 2.800 else "Fail")
        else:
            predicted_class = 2 if sei_score >= 2.800 else 0
            efficacy_label = "Pass" if sei_score >= 2.800 else "Fail"

        compliance_status = "AUDIT VERIFIED" if (predicted_class == 2 or sei_score >= 2.800) else "REQUIRES ATTENTION"

    # ---------------------------------------------------------
    # 2. Commit Record to SQLite Audit Ledger
    # ---------------------------------------------------------
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO cycle_records (
            timestamp, station_id, ambient_temp_c, relative_humidity_pct,
            flow_rate_ml_min, cycle_duration_sec, room_volume_m3, sei_score,
            predicted_class, efficacy_label, compliance_status, estop_triggered
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        cycle.station_id,
        cycle.ambient_temp_c,
        cycle.relative_humidity_pct,
        cycle.flow_rate_ml_min,
        cycle.cycle_duration_sec,
        cycle.room_volume_m3,
        sei_score,
        predicted_class,
        efficacy_label,
        compliance_status,
        1 if cycle.estop_triggered else 0
    ))
    conn.commit()
    inserted_id = cursor.lastrowid
    conn.close()

    print(f"[DB] Stored cycle #{inserted_id} into SQLite audit ledger.")

    return {
        "id": inserted_id,
        "station_id": cycle.station_id,
        "sei_score": sei_score,
        "predicted_class": predicted_class,
        "efficacy_label": efficacy_label,
        "compliance_status": compliance_status,
        "estop_triggered": cycle.estop_triggered
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
        
        # Schema validation (accepts 0 values from relaxed Pydantic model)
        cycle_in = CycleTelemetryInput(**data_dict)
        
        # Branching, inference, and persistence
        result = evaluate_and_log_cycle(cycle_in)
        print(f"[ML EVALUATION RESULT] Station: {result['station_id']} | "
              f"Class: {result['efficacy_label']} | "
              f"SEI Score: {result['sei_score']} | "
              f"Status: {result['compliance_status']}")
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

import json

class CycleTriggerRequest(BaseModel):
    duration_sec: int = Field(default=30, ge=5, le=300, description="Misting duration in seconds")
    room_volume_m3: float = Field(default=1.0, gt=0, description="Target room volume in m3")

@app.post("/api/v1/trigger")
def trigger_cycle(req: CycleTriggerRequest):
    """Dispatches a remote misting start command to the station over MQTT."""
    payload = json.dumps({
        "command": "START",
        "duration_sec": req.duration_sec,
        "room_volume_m3": req.room_volume_m3
    })
    # Publish to the control topic
    mqtt_client.publish("sanmist/control/trigger", payload)
    return {
        "status": "DISPATCHED",
        "duration_sec": req.duration_sec,
        "room_volume_m3": req.room_volume_m3
    }