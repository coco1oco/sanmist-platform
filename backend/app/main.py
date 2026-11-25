import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
from typing import Optional, List
import urllib.request

import joblib
import paho.mqtt.client as mqtt
from fastapi import Depends, FastAPI, HTTPException, Response, Header, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from backend.app.scheduler import normalize_days_of_week, normalize_hhmm, run_schedule_tick
from backend.app.security import (
    configure_mqtt_client_security,
    load_mqtt_security_config,
    parse_cors_origins,
    register_security_headers_middleware,
)

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except Exception as e:
    xgb = None
    XGB_AVAILABLE = False
    print(f"[WARN] XGBoost C-extension failed to load ({e}). Running with rule-based SEI fallback.")

# =========================================================
# 1. PATHS & CONFIGURATION
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.abspath(os.path.join(BASE_DIR, "../../frontend"))
MODEL_JSON_PATH = os.path.abspath(
    os.path.join(BASE_DIR, "..", "..", "ml-pipeline", "models", "xgboost_sei_classifier.json")
)
MODEL_PKL_PATH = os.path.abspath(
    os.path.join(BASE_DIR, "..", "..", "ml-pipeline", "models", "xgboost_sei_classifier.pkl")
)
DB_PATH = os.getenv("SANMIST_DB_PATH", os.path.join(BASE_DIR, "sanmist_audit.db"))

MQTT_SECURITY_CONFIG = load_mqtt_security_config()
MQTT_BROKER_HOST = MQTT_SECURITY_CONFIG.host
MQTT_BROKER_PORT = MQTT_SECURITY_CONFIG.port
MQTT_BROKER_USERNAME = MQTT_SECURITY_CONFIG.username
MQTT_BROKER_PASSWORD = MQTT_SECURITY_CONFIG.password
MQTT_USE_TLS = MQTT_SECURITY_CONFIG.use_tls
MQTT_DEVICE_KEY = os.getenv("SANMIST_DEVICE_KEY", "sanmist-edge-key-2026")
MQTT_REQUIRE_AUTH = MQTT_SECURITY_CONFIG.require_payload_auth
MQTT_TELEMETRY_TOPIC = "sanmist/devices/+/telemetry"
MQTT_STATUS_TOPIC = "sanmist/devices/+/status"
MQTT_CONTROL_TOPIC = "sanmist/control/trigger"
SESSION_TTL_HOURS = int(os.getenv("SANMIST_SESSION_TTL_HOURS", "24"))
GENESIS_HASH = "0" * 64
DEFAULT_HOCL_CAPACITY_ML = 500.0
DEFAULT_HOCL_LOW_THRESHOLD_ML = 50.0
ADMIN_ROLES = {"Administrator", "Safety Administrator"}

# =========================================================
# 2. PRE-PROVISIONED DEVICES REGISTRY (WHITELIST)
# =========================================================
ALLOWED_DEVICES_REGISTRY = {
    "MIST-BOH-01": {
        "auth_key": "sanmist-edge-key-2026",
        "model_name": "MistSan Station BOH Pro",
        "default_volume": 1.0,
    },
    "MIST-BOH-02": {
        "auth_key": "prep-zone-key-2026",
        "model_name": "MistSan Cold Prep Unit",
        "default_volume": 1.5,
    },
    "MIST-COLD-01": {
        "auth_key": "cold-storage-key-2026",
        "model_name": "MistSan Walk-in Freezer Guard",
        "default_volume": 2.5,
    },
    "MIST-DISH-01": {
        "auth_key": "dishwash-area-key-2026",
        "model_name": "MistSan Dishwashing Sanitizer",
        "default_volume": 1.2,
    }
}

# Live snapshot per station for multi-device fleet health tracking
live_station_telemetry: dict[str, dict] = {
    "MIST-BOH-01": {
        "station_id": "MIST-BOH-01",
        "ambient_temp_c": 25.0,
        "relative_humidity_pct": 65.0,
        "flow_rate_ml_min": 30.0,
        "hocl_low_level": False,
        "hocl_float_low": False,
        "hocl_remaining_ml": DEFAULT_HOCL_CAPACITY_ML,
        "hocl_capacity_ml": DEFAULT_HOCL_CAPACITY_ML,
        "hocl_low_threshold_ml": DEFAULT_HOCL_LOW_THRESHOLD_ML,
        "presence_detected": False,
        "presence_sensor_enabled": True,
        "is_defect": False,
        "cycle_in_progress": False,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
}

# =========================================================
# 3. LOAD TRAINED XGBOOST CLASSIFIER
# =========================================================
model = None
try:
    if os.path.exists(MODEL_JSON_PATH) and xgb is not None:
        loaded_xgb = xgb.XGBClassifier()
        loaded_xgb.load_model(MODEL_JSON_PATH)
        model = loaded_xgb
        print(f"[INIT] Loaded native XGBoost JSON model from: {MODEL_JSON_PATH}")
    elif os.path.exists(MODEL_PKL_PATH):
        model = joblib.load(MODEL_PKL_PATH)
        print(f"[INIT] Loaded fallback pickle model from: {MODEL_PKL_PATH}")
    else:
        print("[WARN] No trained model artifact found; using SEI regulatory rules.")
except Exception as exc:
    print(f"[WARN] Could not load model: {exc}")

# =========================================================
# 4. DATABASE & CRYPTO HELPERS
# =========================================================
def get_db_connection(isolation_level: Optional[str] = "") -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0, isolation_level=isolation_level)
    conn.row_factory = sqlite3.Row
    return conn

def _hash_password(password: str, salt_hex: Optional[str] = None) -> tuple[str, str]:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120_000)
    return digest.hex(), salt.hex()

def _verify_password(password: str, expected_hash_hex: str, salt_hex: str) -> bool:
    computed_hex, _ = _hash_password(password, salt_hex)
    return hmac.compare_digest(computed_hex, expected_hash_hex)

def _compute_record_hash(
    prev_hash: str,
    timestamp: str,
    station_id: str,
    ambient_temp_c: float,
    relative_humidity_pct: float,
    flow_rate_ml_min: float,
    cycle_duration_sec: float,
    room_volume_m3: float,
    sei_score: float,
    predicted_class: int,
    efficacy_label: str,
    compliance_status: str,
    delivery_confirmed: bool | int,
    estop_triggered: bool | int,
    trigger_type: str = "MANUAL_WEB",
) -> str:
    payload = [
        str(prev_hash),
        str(timestamp),
        str(station_id),
        str(trigger_type),
        f"{float(ambient_temp_c) + 0.0:.4f}",
        f"{float(relative_humidity_pct) + 0.0:.4f}",
        f"{float(flow_rate_ml_min) + 0.0:.4f}",
        f"{float(cycle_duration_sec) + 0.0:.4f}",
        f"{float(room_volume_m3) + 0.0:.4f}",
        f"{float(sei_score) + 0.0:.4f}",
        str(int(predicted_class)),
        str(efficacy_label),
        str(compliance_status),
        "1" if bool(delivery_confirmed) else "0",
        "1" if bool(estop_triggered) else "0",
    ]
    canonical_str = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

def _compute_row_hash(row: sqlite3.Row, prev_hash: str) -> str:
    row_keys = row.keys()
    trigger_type = row["trigger_type"] if "trigger_type" in row_keys and row["trigger_type"] else "MANUAL_WEB"
    return _compute_record_hash(
        prev_hash=prev_hash,
        timestamp=row["timestamp"],
        station_id=row["station_id"],
        ambient_temp_c=row["ambient_temp_c"],
        relative_humidity_pct=row["relative_humidity_pct"],
        flow_rate_ml_min=row["flow_rate_ml_min"],
        cycle_duration_sec=row["cycle_duration_sec"],
        room_volume_m3=row["room_volume_m3"],
        sei_score=row["sei_score"],
        predicted_class=row["predicted_class"],
        efficacy_label=row["efficacy_label"],
        compliance_status=row["compliance_status"],
        delivery_confirmed=row["delivery_confirmed"],
        estop_triggered=row["estop_triggered"],
        trigger_type=trigger_type,
    )

def _compute_record_hash_v1(
    prev_hash: str,
    timestamp: str,
    station_id: str,
    ambient_temp_c: float,
    relative_humidity_pct: float,
    flow_rate_ml_min: float,
    cycle_duration_sec: float,
    room_volume_m3: float,
    sei_score: float,
    predicted_class: int,
    efficacy_label: str,
    compliance_status: str,
    delivery_confirmed: bool | int,
    estop_triggered: bool | int,
) -> str:
    payload = [
        str(prev_hash),
        str(timestamp),
        str(station_id),
        f"{float(ambient_temp_c) + 0.0:.4f}",
        f"{float(relative_humidity_pct) + 0.0:.4f}",
        f"{float(flow_rate_ml_min) + 0.0:.4f}",
        f"{float(cycle_duration_sec) + 0.0:.4f}",
        f"{float(room_volume_m3) + 0.0:.4f}",
        f"{float(sei_score) + 0.0:.4f}",
        str(int(predicted_class)),
        str(efficacy_label),
        str(compliance_status),
        "1" if bool(delivery_confirmed) else "0",
        "1" if bool(estop_triggered) else "0",
    ]
    canonical_str = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

def _compute_row_hash_v1(row: sqlite3.Row, prev_hash: str) -> str:
    return _compute_record_hash_v1(
        prev_hash=prev_hash,
        timestamp=row["timestamp"],
        station_id=row["station_id"],
        ambient_temp_c=row["ambient_temp_c"],
        relative_humidity_pct=row["relative_humidity_pct"],
        flow_rate_ml_min=row["flow_rate_ml_min"],
        cycle_duration_sec=row["cycle_duration_sec"],
        room_volume_m3=row["room_volume_m3"],
        sei_score=row["sei_score"],
        predicted_class=row["predicted_class"],
        efficacy_label=row["efficacy_label"],
        compliance_status=row["compliance_status"],
        delivery_confirmed=row["delivery_confirmed"],
        estop_triggered=row["estop_triggered"],
    )

def _verify_ledger_chain(cursor: sqlite3.Cursor) -> dict:
    cursor.execute("SELECT * FROM cycle_records ORDER BY id ASC")
    rows = cursor.fetchall()
    expected_prev_hash = GENESIS_HASH
    tampered_ids: list[int] = []
    count = 0

    for row in rows:
        count += 1
        expected_hash = _compute_row_hash(row, expected_prev_hash)
        row_prev = row["prev_hash"]
        row_hash = row["record_hash"]
        hash_matches = row_hash == expected_hash
        if not hash_matches and row_prev == expected_prev_hash:
            row_keys = row.keys()
            trig = row["trigger_type"] if "trigger_type" in row_keys and row["trigger_type"] else "MANUAL_WEB"
            if trig == "MANUAL_WEB":
                hash_matches = row_hash == _compute_row_hash_v1(row, expected_prev_hash)
        if row_prev != expected_prev_hash or not hash_matches:
            tampered_ids.append(int(row["id"]))
        expected_prev_hash = row_hash if row_hash else ""

    return {
        "verified": len(tampered_ids) == 0,
        "checked_records": count,
        "tampered_ids": tampered_ids,
        "latest_hash": expected_prev_hash,
    }

def init_db() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cycle_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            station_id TEXT NOT NULL,
            trigger_type TEXT NOT NULL DEFAULT 'MANUAL_WEB',
            ambient_temp_c REAL NOT NULL,
            relative_humidity_pct REAL NOT NULL,
            flow_rate_ml_min REAL NOT NULL,
            cycle_duration_sec REAL NOT NULL,
            room_volume_m3 REAL NOT NULL,
            sei_score REAL NOT NULL,
            predicted_class INTEGER NOT NULL,
            efficacy_label TEXT NOT NULL,
            compliance_status TEXT NOT NULL DEFAULT 'REQUIRES ATTENTION',
            delivery_confirmed INTEGER NOT NULL DEFAULT 1,
            ml_confidence REAL,
            estop_triggered INTEGER NOT NULL DEFAULT 0,
            prev_hash TEXT,
            record_hash TEXT
        )
    """)

    cursor.execute("PRAGMA table_info(cycle_records)")
    existing_columns = {col[1] for col in cursor.fetchall()}

    if "trigger_type" not in existing_columns:
        cursor.execute(
            "ALTER TABLE cycle_records ADD COLUMN trigger_type TEXT NOT NULL DEFAULT 'MANUAL_WEB'"
        )
    if "compliance_status" not in existing_columns:
        cursor.execute(
            "ALTER TABLE cycle_records ADD COLUMN compliance_status TEXT NOT NULL DEFAULT 'REQUIRES ATTENTION'"
        )
    if "delivery_confirmed" not in existing_columns:
        cursor.execute(
            "ALTER TABLE cycle_records ADD COLUMN delivery_confirmed INTEGER NOT NULL DEFAULT 1"
        )
    if "ml_confidence" not in existing_columns:
        cursor.execute("ALTER TABLE cycle_records ADD COLUMN ml_confidence REAL")
    if "prev_hash" not in existing_columns:
        cursor.execute("ALTER TABLE cycle_records ADD COLUMN prev_hash TEXT")
    if "record_hash" not in existing_columns:
        cursor.execute("ALTER TABLE cycle_records ADD COLUMN record_hash TEXT")

    # Normalize contradictory compliance_status only on unhashed legacy rows
    cursor.execute("""
        UPDATE cycle_records
        SET compliance_status = 'SAFETY INTERLOCK TRIPPED'
        WHERE estop_triggered = 1 AND compliance_status = 'AUDIT VERIFIED'
          AND (record_hash IS NULL OR record_hash = '')
    """)
    cursor.execute("""
        UPDATE cycle_records
        SET compliance_status = 'REQUIRES ATTENTION'
        WHERE estop_triggered = 0 AND efficacy_label != 'Pass' AND compliance_status = 'AUDIT VERIFIED'
          AND (record_hash IS NULL OR record_hash = '')
    """)

    # Backfill SHA-256 hash chain for unhashed legacy rows while preserving existing hashes
    cursor.execute(
        "SELECT 1 FROM cycle_records WHERE record_hash IS NULL OR record_hash = '' LIMIT 1"
    )
    if cursor.fetchone() is not None:
        cursor.execute("SELECT * FROM cycle_records ORDER BY id ASC")
        rows = cursor.fetchall()
        expected_prev_hash = GENESIS_HASH
        backfilled = 0
        for row in rows:
            existing_hash = row["record_hash"]
            if existing_hash is None or existing_hash == "":
                computed_hash = _compute_row_hash(row, expected_prev_hash)
                cursor.execute(
                    "UPDATE cycle_records SET prev_hash = ?, record_hash = ? WHERE id = ?",
                    (expected_prev_hash, computed_hash, row["id"]),
                )
                expected_prev_hash = computed_hash
                backfilled += 1
            else:
                expected_prev_hash = existing_hash
        print(f"[DB] Backfilled SHA-256 hash chain for {backfilled} legacy cycle record(s).")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS auth_sessions (
            token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            serial_number TEXT PRIMARY KEY,
            model_name TEXT NOT NULL DEFAULT 'MistSan Standard',
            auth_key TEXT NOT NULL DEFAULT 'sanmist-edge-key-2026',
            is_claimed INTEGER NOT NULL DEFAULT 0,
            hocl_capacity_ml REAL NOT NULL DEFAULT 500.0,
            hocl_remaining_ml REAL NOT NULL DEFAULT 500.0,
            hocl_low_threshold_ml REAL NOT NULL DEFAULT 50.0,
            presence_sensor_enabled INTEGER NOT NULL DEFAULT 1,
            registered_at TEXT NOT NULL
        )
    """)

    cursor.execute("PRAGMA table_info(devices)")
    dev_columns = {col[1] for col in cursor.fetchall()}
    if "hocl_capacity_ml" not in dev_columns:
        cursor.execute("ALTER TABLE devices ADD COLUMN hocl_capacity_ml REAL NOT NULL DEFAULT 500.0")
    if "hocl_remaining_ml" not in dev_columns:
        cursor.execute("ALTER TABLE devices ADD COLUMN hocl_remaining_ml REAL NOT NULL DEFAULT 500.0")
    if "hocl_low_threshold_ml" not in dev_columns:
        cursor.execute("ALTER TABLE devices ADD COLUMN hocl_low_threshold_ml REAL NOT NULL DEFAULT 50.0")
    if "presence_sensor_enabled" not in dev_columns:
        cursor.execute("ALTER TABLE devices ADD COLUMN presence_sensor_enabled INTEGER NOT NULL DEFAULT 1")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            serial_number TEXT NOT NULL,
            nickname TEXT NOT NULL,
            room_volume_m3 REAL DEFAULT 1.0,
            paired_at TEXT NOT NULL,
            FOREIGN KEY (serial_number) REFERENCES devices(serial_number) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS device_schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            serial_number TEXT NOT NULL,
            days_of_week TEXT NOT NULL DEFAULT 'MON,TUE,WED,THU,FRI,SAT,SUN',
            start_time_pht TEXT NOT NULL,
            duration_sec INTEGER NOT NULL DEFAULT 32,
            room_volume_m3 REAL NOT NULL DEFAULT 1.0,
            dosage_ml REAL NOT NULL DEFAULT 10.0,
            is_active INTEGER NOT NULL DEFAULT 1
        )
    """)

    cursor.execute("PRAGMA table_info(device_schedules)")
    sched_columns = {col[1] for col in cursor.fetchall()}
    if "room_volume_m3" not in sched_columns:
        cursor.execute("ALTER TABLE device_schedules ADD COLUMN room_volume_m3 REAL NOT NULL DEFAULT 1.0")
    if "dosage_ml" not in sched_columns:
        cursor.execute("ALTER TABLE device_schedules ADD COLUMN dosage_ml REAL NOT NULL DEFAULT 10.0")
    if "last_triggered_slot" not in sched_columns:
        cursor.execute("ALTER TABLE device_schedules ADD COLUMN last_triggered_slot TEXT")
    if "last_triggered_at" not in sched_columns:
        cursor.execute("ALTER TABLE device_schedules ADD COLUMN last_triggered_at TEXT")

    # Seed Pre-Provisioned Devices Whitelist
    for s_num, d_info in ALLOWED_DEVICES_REGISTRY.items():
        cursor.execute("""
            INSERT OR IGNORE INTO devices (serial_number, model_name, auth_key, is_claimed, registered_at)
            VALUES (?, ?, ?, 0, datetime('now'))
        """, (s_num, d_info["model_name"], d_info["auth_key"]))

    # Deduplicate any pre-existing user_devices rows and enforce uniqueness
    cursor.execute("""
        DELETE FROM user_devices
        WHERE id NOT IN (
            SELECT MIN(id) FROM user_devices GROUP BY user_id, serial_number
        )
    """)
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_user_devices_user_serial
        ON user_devices(user_id, serial_number)
    """)

    # Seed Admin User (and default admin station only on first-time initialization)
    cursor.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cursor.fetchone()[0] == 0:
        now_iso = datetime.now(timezone.utc).isoformat()
        raw_password = os.getenv("SANMIST_ADMIN_PASSWORD", "sanmist2026")
        pwd_hash, salt_hex = _hash_password(raw_password)
        cursor.execute("""
            INSERT INTO users (username, password_hash, salt, full_name, role, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("admin", pwd_hash, salt_hex, "SanMist Lead Auditor", "Administrator", now_iso))
        cursor.execute("""
            INSERT OR IGNORE INTO user_devices (user_id, serial_number, nickname, room_volume_m3, paired_at)
            VALUES ('admin', 'MIST-BOH-01', 'Kitchen Prep Area', 1.0, datetime('now'))
        """)
        cursor.execute("UPDATE devices SET is_claimed = 1 WHERE serial_number = 'MIST-BOH-01'")

    conn.commit()
    conn.close()
    print(f"[DB] Initialized schema and device whitelist at: {DB_PATH}")

init_db()

# =========================================================
# 5. SCHEMAS & AUTH DEPENDENCY
# =========================================================
class CycleTelemetryInput(BaseModel):
    station_id: str = Field(default="MIST-BOH-01", min_length=1, max_length=64)
    ambient_temp_c: float = Field(..., ge=0.0, le=60.0)
    relative_humidity_pct: float = Field(..., ge=0.0, le=100.0)
    flow_rate_ml_min: float = Field(..., ge=0.0, le=100.0)
    cycle_duration_sec: float = Field(..., ge=0.0, le=600.0)
    room_volume_m3: float = Field(default=1.0, gt=0, le=200.0)
    delivery_confirmed: bool = Field(default=True)
    estop_triggered: bool = Field(default=False)
    presence_aborted: bool = Field(default=False)
    hocl_aborted: bool = Field(default=False)
    presence_detected: Optional[bool] = Field(default=None)
    trigger_type: Optional[str] = Field(default="MANUAL_WEB")
    hocl_remaining_ml: Optional[float] = Field(default=None, ge=0.0, le=10000.0)
    hocl_capacity_ml: Optional[float] = Field(default=None, gt=0.0, le=10000.0)
    auth_key: Optional[str] = Field(default=None)
    signature: Optional[str] = Field(default=None)

class DeviceConfigPayload(BaseModel):
    presence_sensor_enabled: bool

class CycleTriggerRequest(BaseModel):
    station_id: Optional[str] = "MIST-BOH-01"
    trigger_type: Optional[str] = "MANUAL_WEB"
    duration_sec: int = Field(default=30, ge=5, le=300)
    room_volume_m3: float = Field(default=1.0, gt=0, le=200.0)

class EStopRequest(BaseModel):
    station_id: Optional[str] = "MIST-BOH-01"

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=128)

class GoogleAuthPayload(BaseModel):
    id_token: str

class ClaimDevicePayload(BaseModel):
    username: str
    serial_number: str
    auth_key: str
    nickname: str
    room_volume_m3: float = 1.0

class SchedulePayload(BaseModel):
    serial_number: str
    days_of_week: str
    start_time_pht: str
    duration_sec: int = Field(..., ge=5, le=300)
    room_volume_m3: Optional[float] = Field(default=1.0, gt=0, le=200.0)
    dosage_ml: Optional[float] = 10.0

def require_auth(authorization: Optional[str] = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authentication required")
    token = authorization.split(" ")[1]

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT token, username, full_name, role, expires_at FROM auth_sessions WHERE token = ?", (token,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=401, detail="Invalid session token")

    try:
        raw_exp = row["expires_at"]
        expires_at = datetime.fromisoformat(raw_exp.replace("Z", "+00:00")) if isinstance(raw_exp, str) else raw_exp
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        is_expired = datetime.now(timezone.utc) >= expires_at
    except Exception:
        is_expired = True

    if is_expired:
        cursor.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
        conn.commit()
        conn.close()
        raise HTTPException(status_code=401, detail="Session expired")

    user_info = {"token": row["token"], "username": row["username"], "full_name": row["full_name"], "role": row["role"]}
    conn.close()
    return user_info


def _ensure_station_access(user: dict, station_id: str) -> None:
    if user.get("role") in ADMIN_ROLES:
        return
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM user_devices WHERE user_id = ? AND serial_number = ?",
            (user["username"], station_id),
        )
        if cursor.fetchone() is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Station is not linked to your account.",
            )
    finally:
        conn.close()


def _verify_telemetry_request_auth(
    cycle: CycleTelemetryInput,
    authorization: Optional[str],
    x_device_key: Optional[str],
) -> None:
    if authorization:
        require_auth(authorization)
        return

    station_key = _get_device_auth_key(cycle.station_id)
    provided_header_key = (x_device_key or "").strip()
    if provided_header_key:
        if hmac.compare_digest(provided_header_key, MQTT_DEVICE_KEY) or (
            station_key != MQTT_DEVICE_KEY
            and hmac.compare_digest(provided_header_key, station_key)
        ):
            return
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid device authentication key.",
        )

    if cycle.auth_key or cycle.signature:
        if _verify_mqtt_auth(cycle.model_dump()):
            return
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid payload authentication credentials.",
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required (Bearer token or X-Device-Key).",
    )

# =========================================================
# 6. SEI & MQTT PIPELINE
# =========================================================
def compute_evaporation_adjusted_sei(cycle: CycleTelemetryInput) -> float:
    evap_loss = (cycle.ambient_temp_c - 25.0) * 0.003 - (cycle.relative_humidity_pct - 65.0) * 0.001
    loss_factor = max(0.85, min(1.15, 1.0 - evap_loss))
    effective_flow = cycle.flow_rate_ml_min * loss_factor
    duration_min = cycle.cycle_duration_sec / 60.0
    return round((effective_flow * duration_min) / cycle.room_volume_m3, 3)

def evaluate_telemetry_cycle(
    cycle: CycleTelemetryInput,
    deduplicate_window_sec: float = 0.0,
    compliance_status_override: Optional[str] = None,
) -> dict:
    now_dt = datetime.now(timezone.utc)
    recorded_at = now_dt.isoformat()
    ml_confidence = None
    trigger_type = cycle.trigger_type or "MANUAL_WEB"

    if cycle.estop_triggered:
        sei_score = 0.0
        predicted_class = 0
        efficacy_label = "Fail"
        compliance_status = "SAFETY INTERLOCK TRIPPED"
    elif compliance_status_override:
        sei_score = 0.0
        predicted_class = 0
        efficacy_label = "Fail"
        compliance_status = compliance_status_override
    elif cycle.presence_aborted or trigger_type in {"SCHEDULED_ABORTED_PRESENCE", "SCHEDULED_BLOCKED_PRESENCE"}:
        sei_score = 0.0
        predicted_class = 0
        efficacy_label = "Fail"
        compliance_status = (
            "OCCUPANCY INTERLOCK TRIPPED"
            if trigger_type == "SCHEDULED_ABORTED_PRESENCE"
            else "OCCUPANCY INTERLOCK BLOCKED"
        )
    elif cycle.hocl_aborted or trigger_type == "SCHEDULED_BLOCKED_HOCL":
        sei_score = 0.0
        predicted_class = 0
        efficacy_label = "Fail"
        compliance_status = (
            "HOCL INTERLOCK BLOCKED"
            if trigger_type == "SCHEDULED_BLOCKED_HOCL"
            else "HOCL INTERLOCK TRIPPED"
        )
    else:
        sei_score = compute_evaporation_adjusted_sei(cycle)
        is_compliant = sei_score >= 2.800
        pred = None
        probs = None
        if model is not None:
            try:
                features = [[float(cycle.ambient_temp_c), float(cycle.relative_humidity_pct), float(cycle.flow_rate_ml_min), float(cycle.cycle_duration_sec), float(cycle.room_volume_m3)]]
                pred = int(model.predict(features)[0])
                if hasattr(model, "predict_proba"):
                    probs = model.predict_proba(features)[0]
            except Exception:
                pass

        if is_compliant:
            predicted_class = 2
            efficacy_label = "Pass"
            compliance_status = "AUDIT VERIFIED"
            if probs is not None and len(probs) > 2:
                ml_confidence = round(float(probs[2]) * 100.0, 1)
        else:
            compliance_status = "REQUIRES ATTENTION"
            predicted_class = pred if pred in (0, 1) else (1 if sei_score >= 1.800 else 0)
            efficacy_label = "Marginal" if predicted_class == 1 else "Fail"
            if probs is not None and len(probs) > predicted_class:
                ml_confidence = round(float(probs[predicted_class]) * 100.0, 1)

    estop_int = 1 if cycle.estop_triggered else 0
    conn = get_db_connection(isolation_level=None)
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")

        if deduplicate_window_sec > 0.0:
            if estop_int == 1:
                cursor.execute(
                    """
                    SELECT id, timestamp, prev_hash, record_hash FROM cycle_records
                    WHERE station_id = ?
                      AND estop_triggered = 1
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (cycle.station_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, timestamp, prev_hash, record_hash FROM cycle_records
                    WHERE station_id = ?
                      AND flow_rate_ml_min = ?
                      AND cycle_duration_sec = ?
                      AND estop_triggered = ?
                      AND trigger_type = ?
                      AND compliance_status = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (
                        cycle.station_id,
                        cycle.flow_rate_ml_min,
                        cycle.cycle_duration_sec,
                        estop_int,
                        trigger_type,
                        compliance_status,
                    ),
                )
            recent = cursor.fetchone()
            if recent is not None:
                try:
                    last_ts = recent["timestamp"]
                    if isinstance(last_ts, str):
                        if not last_ts.endswith("Z") and "+" not in last_ts:
                            last_ts += "Z"
                        last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    else:
                        last_dt = last_ts
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    if (now_dt - last_dt).total_seconds() <= deduplicate_window_sec:
                        cursor.execute("ROLLBACK")
                        print(
                            f"[MQTT DEDUP] Ignored duplicate MQTT telemetry already recorded as cycle #{recent['id']}"
                        )
                        if cycle.hocl_remaining_ml is not None or cycle.presence_detected is not None:
                            prev_tel = live_station_telemetry.get(cycle.station_id, {})
                            if cycle.hocl_remaining_ml is not None:
                                reservoir = _update_station_reservoir(
                                    cycle.station_id,
                                    remaining_ml=cycle.hocl_remaining_ml,
                                    capacity_ml=cycle.hocl_capacity_ml,
                                )
                            else:
                                reservoir = _get_station_reservoir(cycle.station_id)
                            float_low = bool(prev_tel.get("hocl_float_low", False))
                            effective_low = float_low or (
                                reservoir["hocl_remaining_ml"] <= reservoir["hocl_low_threshold_ml"]
                            )
                            presence_val = (
                                bool(cycle.presence_detected)
                                if cycle.presence_detected is not None
                                else bool(prev_tel.get("presence_detected", False))
                            )
                            live_station_telemetry[cycle.station_id] = {
                                "station_id": cycle.station_id,
                                "ambient_temp_c": float(prev_tel.get("ambient_temp_c", cycle.ambient_temp_c)),
                                "relative_humidity_pct": float(prev_tel.get("relative_humidity_pct", cycle.relative_humidity_pct)),
                                "flow_rate_ml_min": float(prev_tel.get("flow_rate_ml_min", cycle.flow_rate_ml_min)),
                                "hocl_low_level": effective_low,
                                "hocl_float_low": float_low,
                                "hocl_remaining_ml": reservoir["hocl_remaining_ml"],
                                "hocl_capacity_ml": reservoir["hocl_capacity_ml"],
                                "hocl_low_threshold_ml": reservoir["hocl_low_threshold_ml"],
                                "presence_detected": presence_val,
                                "presence_sensor_enabled": _get_station_presence_enabled(cycle.station_id),
                                "is_defect": bool(prev_tel.get("is_defect", False)),
                                "cycle_in_progress": False,
                                "updated_at": now_dt.isoformat(),
                            }
                        return {
                            "duplicate": True,
                            "id": recent["id"],
                            "timestamp": recent["timestamp"],
                            "station_id": cycle.station_id,
                            "trigger_type": trigger_type,
                            "sei_score": sei_score,
                            "predicted_class": predicted_class,
                            "efficacy_label": efficacy_label,
                            "compliance_status": compliance_status,
                            "delivery_confirmed": cycle.delivery_confirmed,
                            "ml_confidence": ml_confidence,
                            "estop_triggered": cycle.estop_triggered,
                            "prev_hash": recent["prev_hash"],
                            "record_hash": recent["record_hash"],
                        }
                except ValueError:
                    pass

        cursor.execute("SELECT record_hash FROM cycle_records ORDER BY id DESC LIMIT 1")
        prev_row = cursor.fetchone()
        prev_hash = prev_row["record_hash"] if prev_row and prev_row["record_hash"] else GENESIS_HASH

        record_hash = _compute_record_hash(
            prev_hash=prev_hash,
            timestamp=recorded_at,
            station_id=cycle.station_id,
            ambient_temp_c=cycle.ambient_temp_c,
            relative_humidity_pct=cycle.relative_humidity_pct,
            flow_rate_ml_min=cycle.flow_rate_ml_min,
            cycle_duration_sec=cycle.cycle_duration_sec,
            room_volume_m3=cycle.room_volume_m3,
            sei_score=sei_score,
            predicted_class=predicted_class,
            efficacy_label=efficacy_label,
            compliance_status=compliance_status,
            delivery_confirmed=cycle.delivery_confirmed,
            estop_triggered=cycle.estop_triggered,
            trigger_type=trigger_type,
        )

        cursor.execute("""
            INSERT INTO cycle_records (
                timestamp, station_id, trigger_type, ambient_temp_c, relative_humidity_pct,
                flow_rate_ml_min, cycle_duration_sec, room_volume_m3, sei_score,
                predicted_class, efficacy_label, compliance_status, delivery_confirmed,
                ml_confidence, estop_triggered, prev_hash, record_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            recorded_at, cycle.station_id, trigger_type, cycle.ambient_temp_c, cycle.relative_humidity_pct,
            cycle.flow_rate_ml_min, cycle.cycle_duration_sec, cycle.room_volume_m3, sei_score,
            predicted_class, efficacy_label, compliance_status, 1 if cycle.delivery_confirmed else 0,
            ml_confidence, estop_int, prev_hash, record_hash
        ))
        inserted_id = cursor.lastrowid
        cursor.execute("COMMIT")
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()

    is_presence_interlock = cycle.presence_aborted or trigger_type in {
        "SCHEDULED_ABORTED_PRESENCE",
        "SCHEDULED_BLOCKED_PRESENCE",
    }
    is_hocl_interlock = cycle.hocl_aborted or trigger_type == "SCHEDULED_BLOCKED_HOCL"
    consumed_ml = (
        0.0
        if (cycle.estop_triggered or compliance_status_override is not None or is_presence_interlock or is_hocl_interlock)
        else round((cycle.flow_rate_ml_min * (cycle.cycle_duration_sec / 60.0)), 2)
    )
    reservoir = _update_station_reservoir(
        cycle.station_id,
        remaining_ml=cycle.hocl_remaining_ml,
        consumed_ml=consumed_ml,
        capacity_ml=cycle.hocl_capacity_ml,
    )
    prev_tel = live_station_telemetry.get(cycle.station_id, {})
    float_low = bool(prev_tel.get("hocl_float_low", False))
    effective_low = float_low or (reservoir["hocl_remaining_ml"] <= reservoir["hocl_low_threshold_ml"])
    presence_val = (
        bool(cycle.presence_detected)
        if cycle.presence_detected is not None
        else bool(prev_tel.get("presence_detected", False) or is_presence_interlock)
    )
    live_station_telemetry[cycle.station_id] = {
        "station_id": cycle.station_id,
        "ambient_temp_c": float(prev_tel.get("ambient_temp_c", cycle.ambient_temp_c)),
        "relative_humidity_pct": float(prev_tel.get("relative_humidity_pct", cycle.relative_humidity_pct)),
        "flow_rate_ml_min": float(prev_tel.get("flow_rate_ml_min", cycle.flow_rate_ml_min)),
        "hocl_low_level": effective_low,
        "hocl_float_low": float_low,
        "hocl_remaining_ml": reservoir["hocl_remaining_ml"],
        "hocl_capacity_ml": reservoir["hocl_capacity_ml"],
        "hocl_low_threshold_ml": reservoir["hocl_low_threshold_ml"],
        "presence_detected": presence_val,
        "presence_sensor_enabled": _get_station_presence_enabled(cycle.station_id),
        "is_defect": bool(prev_tel.get("is_defect", False)),
        "cycle_in_progress": False,
        "updated_at": recorded_at,
    }

    return {
        "duplicate": False,
        "id": inserted_id,
        "timestamp": recorded_at,
        "station_id": cycle.station_id,
        "trigger_type": trigger_type,
        "sei_score": sei_score,
        "predicted_class": predicted_class,
        "efficacy_label": efficacy_label,
        "compliance_status": compliance_status,
        "delivery_confirmed": cycle.delivery_confirmed,
        "ml_confidence": ml_confidence,
        "estop_triggered": cycle.estop_triggered,
        "hocl_consumed_ml": consumed_ml,
        "hocl_remaining_ml": reservoir["hocl_remaining_ml"],
        "hocl_capacity_ml": reservoir["hocl_capacity_ml"],
        "prev_hash": prev_hash,
        "record_hash": record_hash,
    }

def _compute_mqtt_signature(data_dict: dict, secret: str = MQTT_DEVICE_KEY) -> str:
    """Compute deterministic HMAC-SHA256 over canonical MQTT payload fields."""
    station_id = str(data_dict.get("station_id") or "MIST-BOH-01")
    if "command" in data_dict:
        cmd = str(data_dict.get("command") or "")
        if cmd == "CONFIG":
            msg = (
                f"CMD|CONFIG|{station_id}|"
                f"{1 if data_dict.get('presence_sensor_enabled', True) else 0}|"
                f"{float(data_dict.get('hocl_remaining_ml', -1.0)):.2f}"
            )
        elif cmd == "REFILL":
            msg = (
                f"CMD|REFILL|{station_id}|"
                f"{float(data_dict.get('hocl_remaining_ml', DEFAULT_HOCL_CAPACITY_ML)):.2f}"
            )
        else:
            msg = (
                f"CMD|{cmd}|{station_id}|{data_dict.get('trigger_type', '')}|"
                f"{int(data_dict.get('duration_sec', 30))}|"
                f"{float(data_dict.get('room_volume_m3', 1.0)):.2f}"
            )
    elif "cycle_duration_sec" in data_dict:
        msg = (
            f"TEL|{station_id}|"
            f"{float(data_dict.get('ambient_temp_c', 0.0)):.2f}|"
            f"{float(data_dict.get('relative_humidity_pct', 0.0)):.2f}|"
            f"{float(data_dict.get('flow_rate_ml_min', 0.0)):.2f}|"
            f"{float(data_dict.get('cycle_duration_sec', 0.0)):.2f}|"
            f"{float(data_dict.get('room_volume_m3', 1.0)):.2f}|"
            f"{1 if data_dict.get('estop_triggered') else 0}"
        )
    else:
        msg = (
            f"STA|{station_id}|"
            f"{float(data_dict.get('ambient_temp_c', 0.0)):.2f}|"
            f"{float(data_dict.get('relative_humidity_pct', 0.0)):.2f}|"
            f"{float(data_dict.get('flow_rate_ml_min', 0.0)):.2f}"
        )
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

def _get_device_auth_key(station_id: str) -> str:
    device_info = ALLOWED_DEVICES_REGISTRY.get(str(station_id or "").strip())
    if device_info and device_info.get("auth_key"):
        return str(device_info["auth_key"])
    return MQTT_DEVICE_KEY

def _verify_mqtt_auth(data_dict: dict) -> bool:
    """Verify HMAC-SHA256 signature or shared device auth_key on inbound MQTT JSON."""
    if not isinstance(data_dict, dict):
        return False
    provided_sig = str(data_dict.get("signature") or "").strip()
    provided_key = str(data_dict.get("auth_key") or "").strip()
    station_id = str(data_dict.get("station_id") or "").strip()
    device_key = _get_device_auth_key(station_id)

    if provided_sig:
        expected_global = _compute_mqtt_signature(data_dict, MQTT_DEVICE_KEY)
        if hmac.compare_digest(provided_sig.lower(), expected_global.lower()):
            return True
        if device_key != MQTT_DEVICE_KEY:
            expected_device = _compute_mqtt_signature(data_dict, device_key)
            if hmac.compare_digest(provided_sig.lower(), expected_device.lower()):
                return True
        return False

    if provided_key:
        if hmac.compare_digest(provided_key, MQTT_DEVICE_KEY):
            return True
        if device_key != MQTT_DEVICE_KEY and hmac.compare_digest(provided_key, device_key):
            return True
        return False

    return not MQTT_REQUIRE_AUTH

def _on_mqtt_connect(client, userdata, flags, reason_code, properties=None):
    transport = "MQTTS (TLS)" if MQTT_USE_TLS else "MQTT (plaintext)"
    print(f"[MQTT] Connected to {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT} via {transport} (rc={reason_code})")
    client.subscribe(MQTT_TELEMETRY_TOPIC)
    client.subscribe(MQTT_STATUS_TOPIC)


def _on_mqtt_disconnect(client, userdata, *args):
    print(f"[MQTT] Disconnected from {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}; automatic reconnect active.")

def _on_mqtt_message(client, userdata, msg):
    try:
        raw_payload = msg.payload.decode("utf-8")
        topic_str = str(getattr(msg, "topic", "") or "")
        data_dict = json.loads(raw_payload)

        if not _verify_mqtt_auth(data_dict):
            print("[MQTT AUTH] Rejected unauthorized or forged MQTT payload.")
            return

        station_id = str(data_dict.get("station_id") or "MIST-BOH-01")

        if topic_str.endswith("/status"):
            reported_low = bool(data_dict.get("hocl_low_level", False))
            raw_remaining = data_dict.get("hocl_remaining_ml")
            raw_capacity = data_dict.get("hocl_capacity_ml")
            is_boot_sync = bool(data_dict.get("boot_sync", False))
            if is_boot_sync:
                reservoir = _get_station_reservoir(station_id)
            elif raw_remaining is not None:
                reservoir = _update_station_reservoir(
                    station_id,
                    remaining_ml=float(raw_remaining),
                    capacity_ml=float(raw_capacity) if raw_capacity is not None else None,
                )
            else:
                reservoir = _get_station_reservoir(station_id)
            effective_low = reported_low or (reservoir["hocl_remaining_ml"] <= reservoir["hocl_low_threshold_ml"])
            float_low = bool(reported_low and reservoir["hocl_remaining_ml"] > reservoir["hocl_low_threshold_ml"])
            presence_enabled = _get_station_presence_enabled(station_id)
            presence_detected = bool(data_dict.get("presence_detected", False))
            live_station_telemetry[station_id] = {
                "station_id": station_id,
                "ambient_temp_c": float(data_dict.get("ambient_temp_c", 25.0)),
                "relative_humidity_pct": float(data_dict.get("relative_humidity_pct", 65.0)),
                "flow_rate_ml_min": float(data_dict.get("flow_rate_ml_min", 30.0)),
                "hocl_low_level": effective_low,
                "hocl_float_low": float_low,
                "hocl_remaining_ml": reservoir["hocl_remaining_ml"],
                "hocl_capacity_ml": reservoir["hocl_capacity_ml"],
                "hocl_low_threshold_ml": reservoir["hocl_low_threshold_ml"],
                "presence_detected": presence_detected,
                "presence_sensor_enabled": presence_enabled,
                "is_defect": bool(data_dict.get("is_defect", False)),
                "cycle_in_progress": bool(data_dict.get("cycle_in_progress", False)),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if is_boot_sync and mqtt_client.is_connected():
                _publish_device_config(
                    station_id,
                    presence_enabled,
                    triggered_by="boot_sync",
                    hocl_remaining_ml=reservoir["hocl_remaining_ml"],
                )
            elif (
                "presence_sensor_enabled" in data_dict
                and bool(data_dict["presence_sensor_enabled"]) != presence_enabled
                and mqtt_client.is_connected()
            ):
                _publish_device_config(station_id, presence_enabled, triggered_by="config_sync")
            return

        cycle_in = CycleTelemetryInput(**data_dict)
        evaluate_telemetry_cycle(cycle_in, deduplicate_window_sec=3.0)
    except Exception as err:
        print(f"[MQTT ERROR] {err}")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2) if hasattr(mqtt, "CallbackAPIVersion") else mqtt.Client()
mqtt_client.on_connect = _on_mqtt_connect
mqtt_client.on_disconnect = _on_mqtt_disconnect
mqtt_client.on_message = _on_mqtt_message
_mqtt_security_configured = False
SCHEDULER_INTERVAL_SEC = max(5, int(os.getenv("SANMIST_SCHEDULER_INTERVAL_SEC", "15")))


def _publish_start_command(
    station_id: str,
    duration_sec: int,
    room_volume_m3: float,
    trigger_type: str = "MANUAL_WEB",
    triggered_by: str = "system",
) -> tuple[bool, Optional[str]]:
    if not mqtt_client.is_connected():
        return False, "MQTT broker is disconnected; unable to dispatch cycle."

    device_key = _get_device_auth_key(station_id)
    cmd_dict = {
        "command": "START",
        "station_id": station_id,
        "trigger_type": trigger_type,
        "duration_sec": duration_sec,
        "room_volume_m3": room_volume_m3,
        "triggered_by": triggered_by,
        "auth_key": device_key,
    }
    cmd_dict["signature"] = _compute_mqtt_signature(cmd_dict, device_key)
    payload = json.dumps(cmd_dict)

    info = mqtt_client.publish(MQTT_CONTROL_TOPIC, payload)
    if info is None or info.rc != mqtt.MQTT_ERR_SUCCESS:
        return False, f"MQTT publish failed (rc={getattr(info, 'rc', 'unknown')})."
    return True, None


def _get_station_reservoir(station_id: str) -> dict:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT hocl_capacity_ml, hocl_remaining_ml, hocl_low_threshold_ml FROM devices WHERE serial_number = ?",
            (station_id,),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        return {
            "hocl_capacity_ml": DEFAULT_HOCL_CAPACITY_ML,
            "hocl_remaining_ml": DEFAULT_HOCL_CAPACITY_ML,
            "hocl_low_threshold_ml": DEFAULT_HOCL_LOW_THRESHOLD_ML,
        }
    return {
        "hocl_capacity_ml": round(float(row["hocl_capacity_ml"]), 2),
        "hocl_remaining_ml": round(float(row["hocl_remaining_ml"]), 2),
        "hocl_low_threshold_ml": round(float(row["hocl_low_threshold_ml"]), 2),
    }


def _update_station_reservoir(
    station_id: str,
    remaining_ml: Optional[float] = None,
    consumed_ml: float = 0.0,
    capacity_ml: Optional[float] = None,
) -> dict:
    conn = get_db_connection(isolation_level=None)
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            "SELECT hocl_capacity_ml, hocl_remaining_ml, hocl_low_threshold_ml FROM devices WHERE serial_number = ?",
            (station_id,),
        )
        row = cursor.fetchone()
        if row is not None:
            cap = float(capacity_ml) if capacity_ml is not None else float(row["hocl_capacity_ml"])
            thresh = float(row["hocl_low_threshold_ml"])
            cur_rem = float(row["hocl_remaining_ml"])
        else:
            cap = float(capacity_ml) if capacity_ml is not None else DEFAULT_HOCL_CAPACITY_ML
            thresh = DEFAULT_HOCL_LOW_THRESHOLD_ML
            cur_rem = cap

        if remaining_ml is not None:
            new_rem = float(remaining_ml)
        else:
            new_rem = cur_rem - max(0.0, float(consumed_ml))
        new_rem = round(max(0.0, min(cap, new_rem)), 2)
        cap = round(cap, 2)
        thresh = round(thresh, 2)

        if row is not None:
            cursor.execute(
                "UPDATE devices SET hocl_capacity_ml = ?, hocl_remaining_ml = ? WHERE serial_number = ?",
                (cap, new_rem, station_id),
            )
        cursor.execute("COMMIT")
        return {
            "hocl_capacity_ml": cap,
            "hocl_remaining_ml": new_rem,
            "hocl_low_threshold_ml": thresh,
        }
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _get_station_presence_enabled(station_id: str) -> bool:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT presence_sensor_enabled FROM devices WHERE serial_number = ?",
            (station_id,),
        )
        row = cursor.fetchone()
    finally:
        conn.close()
    if row is None or row["presence_sensor_enabled"] is None:
        return True
    return bool(row["presence_sensor_enabled"])


def _set_station_presence_enabled(station_id: str, enabled: bool) -> bool:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE devices SET presence_sensor_enabled = ? WHERE serial_number = ?",
            (1 if enabled else 0, station_id),
        )
        conn.commit()
    finally:
        conn.close()
    return bool(enabled)


def _publish_device_config(
    station_id: str,
    presence_sensor_enabled: bool,
    triggered_by: str = "system",
    hocl_remaining_ml: Optional[float] = None,
) -> None:
    if not mqtt_client.is_connected():
        return
    device_key = _get_device_auth_key(station_id)
    cfg_cmd = {
        "command": "CONFIG",
        "station_id": station_id,
        "presence_sensor_enabled": bool(presence_sensor_enabled),
        "triggered_by": triggered_by,
        "auth_key": device_key,
    }
    if hocl_remaining_ml is not None:
        cfg_cmd["hocl_remaining_ml"] = round(float(hocl_remaining_ml), 2)
    cfg_cmd["signature"] = _compute_mqtt_signature(cfg_cmd, device_key)
    mqtt_client.publish(MQTT_CONTROL_TOPIC, json.dumps(cfg_cmd))


def _is_station_hocl_low(station_id: str) -> bool:
    if bool(live_station_telemetry.get(station_id, {}).get("hocl_low_level", False)):
        return True
    reservoir = _get_station_reservoir(station_id)
    return reservoir["hocl_remaining_ml"] <= reservoir["hocl_low_threshold_ml"]


def _check_station_hocl_sufficient(station_id: str, duration_sec: int) -> tuple[bool, dict]:
    reservoir = _get_station_reservoir(station_id)
    tel = live_station_telemetry.get(station_id, {})
    flow_ml_min = float(tel.get("flow_rate_ml_min") or 30.0)
    if flow_ml_min <= 0.0:
        flow_ml_min = 30.0
    required_ml = round(flow_ml_min * (max(0.0, float(duration_sec)) / 60.0), 2)
    remaining_ml = reservoir["hocl_remaining_ml"]
    threshold_ml = reservoir["hocl_low_threshold_ml"]
    usable_ml = round(max(0.0, remaining_ml - threshold_ml), 2)
    is_low = (
        bool(tel.get("hocl_float_low", False))
        or bool(tel.get("hocl_low_level", False))
        or (remaining_ml <= threshold_ml)
    )
    sufficient = (not is_low) and (usable_ml >= required_ml)
    return sufficient, {
        "remaining_ml": remaining_ml,
        "capacity_ml": reservoir["hocl_capacity_ml"],
        "threshold_ml": threshold_ml,
        "usable_ml": usable_ml,
        "required_ml": required_ml,
        "flow_ml_min": flow_ml_min,
        "is_low": is_low,
    }


def _dispatch_scheduled_cycle(
    station_id: str,
    duration_sec: int,
    room_volume_m3: float,
    trigger_type: str = "SCHEDULED",
    triggered_by: str = "scheduler",
) -> bool | str:
    sufficient, info = _check_station_hocl_sufficient(station_id, duration_sec)
    if not sufficient:
        tel = live_station_telemetry.get(station_id, {})
        blocked_cycle = CycleTelemetryInput(
            station_id=station_id,
            ambient_temp_c=float(tel.get("ambient_temp_c", 25.0)),
            relative_humidity_pct=float(tel.get("relative_humidity_pct", 65.0)),
            flow_rate_ml_min=0.0,
            cycle_duration_sec=0.0,
            room_volume_m3=room_volume_m3,
            delivery_confirmed=False,
            estop_triggered=False,
            trigger_type="SCHEDULED_BLOCKED_HOCL",
        )
        evaluate_telemetry_cycle(
            blocked_cycle,
            compliance_status_override="HOCL INTERLOCK BLOCKED",
        )
        print(
            f"[SCHEDULER] Blocked scheduled cycle for {station_id} and logged to ledger: "
            f"needs {info['required_ml']:.1f} mL, {info['usable_ml']:.1f} mL usable "
            f"({info['remaining_ml']:.1f} mL remaining)."
        )
        return "BLOCKED_HOCL"
    if _get_station_presence_enabled(station_id) and bool(
        live_station_telemetry.get(station_id, {}).get("presence_detected", False)
    ):
        tel = live_station_telemetry.get(station_id, {})
        blocked_cycle = CycleTelemetryInput(
            station_id=station_id,
            ambient_temp_c=float(tel.get("ambient_temp_c", 25.0)),
            relative_humidity_pct=float(tel.get("relative_humidity_pct", 65.0)),
            flow_rate_ml_min=0.0,
            cycle_duration_sec=0.0,
            room_volume_m3=room_volume_m3,
            delivery_confirmed=False,
            estop_triggered=False,
            presence_aborted=True,
            trigger_type="SCHEDULED_BLOCKED_PRESENCE",
        )
        evaluate_telemetry_cycle(
            blocked_cycle,
            compliance_status_override="OCCUPANCY INTERLOCK BLOCKED",
        )
        print(
            f"[SCHEDULER] Blocked scheduled cycle for {station_id} and logged to ledger: "
            "person detected in room."
        )
        return "BLOCKED_PRESENCE"
    ok, err = _publish_start_command(
        station_id=station_id,
        duration_sec=duration_sec,
        room_volume_m3=room_volume_m3,
        trigger_type=trigger_type,
        triggered_by=triggered_by,
    )
    if ok:
        print(f"[SCHEDULER] Dispatched scheduled cycle for {station_id} ({duration_sec}s / {room_volume_m3} m3)")
    elif err:
        print(f"[SCHEDULER] Deferred scheduled cycle for {station_id}: {err}")
    return ok


async def _scheduler_worker_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(
                run_schedule_tick, get_db_connection, _dispatch_scheduled_cycle
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[SCHEDULER ERROR] {exc}")
        await asyncio.sleep(SCHEDULER_INTERVAL_SEC)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _mqtt_security_configured
    scheduler_task: Optional[asyncio.Task] = None
    if os.getenv("SANMIST_DISABLE_MQTT", "0") != "1":
        try:
            if not _mqtt_security_configured:
                sec_meta = configure_mqtt_client_security(mqtt_client, MQTT_SECURITY_CONFIG)
                _mqtt_security_configured = True
                print(
                    f"[MQTT] Security configured: tls={sec_meta['tls_enabled']} "
                    f"creds={sec_meta['credentials_configured']} ca_cert={sec_meta['ca_cert_used']}"
                )
            mqtt_client.connect_async(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
            mqtt_client.loop_start()
            print(f"[MQTT] Thread running on {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        except Exception as exc:
            print(f"[WARN] MQTT setup skipped: {exc}")
    if os.getenv("SANMIST_DISABLE_SCHEDULER", "0") != "1":
        scheduler_task = asyncio.create_task(_scheduler_worker_loop())
        print(f"[SCHEDULER] PHT schedule worker active (interval={SCHEDULER_INTERVAL_SEC}s)")
    yield
    if scheduler_task is not None:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
    try:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception:
        pass

# =========================================================
# 7. FASTAPI APPLICATION
# =========================================================
app = FastAPI(title="SanMist IoT Fleet Engine", version="2.2.0", lifespan=lifespan)

register_security_headers_middleware(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=parse_cors_origins(os.getenv("SANMIST_CORS_ORIGINS")),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count", "X-Filtered-Count"],
)

# =========================================================
# ROUTES: AUTHENTICATION
# =========================================================
@app.post("/api/v1/auth/login")
def login(credentials: LoginRequest) -> dict:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT username, password_hash, salt, full_name, role FROM users WHERE username = ?", (credentials.username.strip(),))
        user_row = cursor.fetchone()

        if not user_row or not _verify_password(credentials.password, user_row["password_hash"], user_row["salt"]):
            raise HTTPException(status_code=401, detail="Invalid username or password")

        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=SESSION_TTL_HOURS)
        cursor.execute(
            "INSERT INTO auth_sessions (token, username, full_name, role, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
            (token, user_row["username"], user_row["full_name"], user_row["role"], now.isoformat(), expires_at.isoformat())
        )
        conn.commit()
        return {"access_token": token, "token_type": "bearer", "user": {"username": user_row["username"], "full_name": user_row["full_name"], "role": user_row["role"]}}
    finally:
        conn.close()

@app.post("/api/v1/auth/google")
def auth_google(payload: GoogleAuthPayload):
    conn = None
    try:
        token_url = f"https://oauth2.googleapis.com/tokeninfo?id_token={payload.id_token}"
        req = urllib.request.Request(token_url, headers={"User-Agent": "SanMist-Platform"})
        with urllib.request.urlopen(req, timeout=10) as response:
            google_data = json.loads(response.read().decode("utf-8"))

        email = google_data.get("email")
        full_name = google_data.get("name", email.split("@")[0] if email else "Operator")
        sub = google_data.get("sub", "oauth")
        if not email:
            raise HTTPException(status_code=400, detail="Invalid Google Profile.")

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, full_name, role FROM users WHERE username = ?", (email,))
        user = cursor.fetchone()

        if not user:
            cursor.execute("INSERT INTO users (username, password_hash, salt, full_name, role, created_at) VALUES (?, ?, ?, ?, ?, datetime('now'))", (email, "GOOGLE_OAUTH", "OAUTH", full_name, "Operator"))
            conn.commit()
            user_profile = {"username": email, "full_name": full_name, "role": "Operator"}
        else:
            user_profile = {"username": user["username"], "full_name": user["full_name"], "role": user["role"]}

        session_token = f"sanmist_g_{sub[:16]}_{hashlib.sha256(os.urandom(16)).hexdigest()[:12]}"
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(days=7)
        cursor.execute("INSERT OR REPLACE INTO auth_sessions (token, username, full_name, role, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)", (session_token, user_profile["username"], user_profile["full_name"], user_profile["role"], now.isoformat(), expires_at.isoformat()))
        conn.commit()
        return {"access_token": session_token, "user": user_profile}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Google Authentication Error: {str(e)}")
    finally:
        if conn is not None:
            conn.close()

@app.post("/api/v1/auth/logout")
def logout(user: dict = Depends(require_auth)) -> dict:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM auth_sessions WHERE token = ?", (user["token"],))
    conn.commit()
    conn.close()
    return {"status": "logged_out"}

@app.get("/api/v1/auth/me")
def get_current_user(user: dict = Depends(require_auth)) -> dict:
    return {
        "username": user["username"],
        "full_name": user["full_name"],
        "role": user["role"],
    }

@app.post("/api/v1/telemetry/evaluate")
def evaluate_and_log_cycle(
    cycle: CycleTelemetryInput,
    authorization: Optional[str] = Header(None),
    x_device_key: Optional[str] = Header(None, alias="X-Device-Key"),
) -> dict:
    _verify_telemetry_request_auth(cycle, authorization, x_device_key)
    return evaluate_telemetry_cycle(cycle)

# =========================================================
# ROUTES: FLEET & MULTI-DEVICE MANAGEMENT
# =========================================================
@app.get("/api/v1/user/{username}/devices")
def get_user_devices(username: str, user: dict = Depends(require_auth)):
    if username != user["username"] and user.get("role") not in ADMIN_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot inspect another user's linked devices.",
        )
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ud.serial_number, ud.nickname, ud.room_volume_m3, d.model_name
            FROM user_devices ud
            JOIN devices d ON ud.serial_number = d.serial_number
            WHERE ud.user_id = ?
            ORDER BY ud.id ASC
        """, (username,))
        rows = cursor.fetchall()
        return [{"serial_number": row["serial_number"], "nickname": row["nickname"], "room_volume_m3": row["room_volume_m3"], "model_name": row["model_name"]} for row in rows]
    finally:
        conn.close()

@app.post("/api/v1/devices/claim")
def claim_device(payload: ClaimDevicePayload, user: dict = Depends(require_auth)):
    serial = payload.serial_number.strip()
    provided_key = payload.auth_key.strip()
    nickname = payload.nickname.strip()

    if serial not in ALLOWED_DEVICES_REGISTRY:
        raise HTTPException(
            status_code=400,
            detail=f"Serial '{serial}' not found in factory registry. Authorized devices: {', '.join(ALLOWED_DEVICES_REGISTRY.keys())}"
        )

    expected_info = ALLOWED_DEVICES_REGISTRY[serial]
    if provided_key != expected_info["auth_key"]:
        raise HTTPException(status_code=401, detail="Invalid Device Authentication Key.")

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM user_devices WHERE user_id = ? AND serial_number = ?", (user["username"], serial))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="This station is already linked to your account.")

        cursor.execute("""
            INSERT INTO user_devices (user_id, serial_number, nickname, room_volume_m3, paired_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        """, (user["username"], serial, nickname, payload.room_volume_m3 or expected_info["default_volume"]))
        cursor.execute("UPDATE devices SET is_claimed = 1 WHERE serial_number = ?", (serial,))
        conn.commit()
    finally:
        conn.close()

    # Initialize live telemetry slot if not yet populated
    if serial not in live_station_telemetry:
        reservoir = _get_station_reservoir(serial)
        live_station_telemetry[serial] = {
            "station_id": serial,
            "ambient_temp_c": 25.0,
            "relative_humidity_pct": 65.0,
            "flow_rate_ml_min": 30.0,
            "hocl_low_level": reservoir["hocl_remaining_ml"] <= reservoir["hocl_low_threshold_ml"],
            "hocl_float_low": False,
            "hocl_remaining_ml": reservoir["hocl_remaining_ml"],
            "hocl_capacity_ml": reservoir["hocl_capacity_ml"],
            "hocl_low_threshold_ml": reservoir["hocl_low_threshold_ml"],
            "presence_detected": False,
            "presence_sensor_enabled": _get_station_presence_enabled(serial),
            "is_defect": False,
            "cycle_in_progress": False,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

    return {"status": "SUCCESS", "serial_number": serial, "nickname": nickname}

@app.post("/api/v1/devices/{serial_number}/refill")
def refill_device_reservoir(serial_number: str, user: dict = Depends(require_auth)):
    _ensure_station_access(user, serial_number)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT serial_number FROM devices WHERE serial_number = ?", (serial_number,))
        if cursor.fetchone() is None:
            raise HTTPException(status_code=404, detail="Device not found.")
    finally:
        conn.close()

    current = _get_station_reservoir(serial_number)
    updated = _update_station_reservoir(serial_number, remaining_ml=current["hocl_capacity_ml"])

    prev = live_station_telemetry.get(serial_number, {})
    live_station_telemetry[serial_number] = {
        "station_id": serial_number,
        "ambient_temp_c": float(prev.get("ambient_temp_c", 25.0)),
        "relative_humidity_pct": float(prev.get("relative_humidity_pct", 65.0)),
        "flow_rate_ml_min": float(prev.get("flow_rate_ml_min", 30.0)),
        "hocl_low_level": False,
        "hocl_float_low": False,
        "hocl_remaining_ml": updated["hocl_remaining_ml"],
        "hocl_capacity_ml": updated["hocl_capacity_ml"],
        "hocl_low_threshold_ml": updated["hocl_low_threshold_ml"],
        "presence_detected": bool(prev.get("presence_detected", False)),
        "presence_sensor_enabled": _get_station_presence_enabled(serial_number),
        "is_defect": bool(prev.get("is_defect", False)),
        "cycle_in_progress": bool(prev.get("cycle_in_progress", False)),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    if mqtt_client.is_connected():
        device_key = _get_device_auth_key(serial_number)
        refill_cmd = {
            "command": "REFILL",
            "station_id": serial_number,
            "hocl_remaining_ml": updated["hocl_capacity_ml"],
            "triggered_by": user["username"],
            "auth_key": device_key,
        }
        refill_cmd["signature"] = _compute_mqtt_signature(refill_cmd, device_key)
        mqtt_client.publish(MQTT_CONTROL_TOPIC, json.dumps(refill_cmd))

    return {
        "status": "REFILLED",
        "station_id": serial_number,
        "hocl_remaining_ml": updated["hocl_remaining_ml"],
        "hocl_capacity_ml": updated["hocl_capacity_ml"],
    }

@app.post("/api/v1/devices/{serial_number}/config")
def update_device_config(
    serial_number: str,
    payload: DeviceConfigPayload,
    user: dict = Depends(require_auth),
):
    _ensure_station_access(user, serial_number)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT serial_number FROM devices WHERE serial_number = ?", (serial_number,))
        if cursor.fetchone() is None:
            raise HTTPException(status_code=404, detail="Device not found.")
    finally:
        conn.close()

    enabled = _set_station_presence_enabled(serial_number, payload.presence_sensor_enabled)
    prev = live_station_telemetry.get(serial_number, {})
    reservoir = _get_station_reservoir(serial_number)
    live_station_telemetry[serial_number] = {
        "station_id": serial_number,
        "ambient_temp_c": float(prev.get("ambient_temp_c", 25.0)),
        "relative_humidity_pct": float(prev.get("relative_humidity_pct", 65.0)),
        "flow_rate_ml_min": float(prev.get("flow_rate_ml_min", 30.0)),
        "hocl_low_level": bool(prev.get("hocl_low_level", False)),
        "hocl_float_low": bool(prev.get("hocl_float_low", False)),
        "hocl_remaining_ml": reservoir["hocl_remaining_ml"],
        "hocl_capacity_ml": reservoir["hocl_capacity_ml"],
        "hocl_low_threshold_ml": reservoir["hocl_low_threshold_ml"],
        "presence_detected": bool(prev.get("presence_detected", False)),
        "presence_sensor_enabled": enabled,
        "is_defect": bool(prev.get("is_defect", False)),
        "cycle_in_progress": bool(prev.get("cycle_in_progress", False)),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _publish_device_config(serial_number, enabled, triggered_by=user["username"])
    return {
        "status": "UPDATED",
        "station_id": serial_number,
        "presence_sensor_enabled": enabled,
    }

@app.delete("/api/v1/devices/{serial_number}")
def remove_device(serial_number: str, user: dict = Depends(require_auth)):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM user_devices WHERE user_id = ? AND serial_number = ?", (user["username"], serial_number))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Device not found in your linked fleet.")

        cursor.execute("DELETE FROM user_devices WHERE user_id = ? AND serial_number = ?", (user["username"], serial_number))
        cursor.execute("DELETE FROM device_schedules WHERE serial_number = ?", (serial_number,))
        cursor.execute("UPDATE devices SET is_claimed = 0 WHERE serial_number = ?", (serial_number,))
        conn.commit()
        return {"status": "DELETED", "serial_number": serial_number}
    finally:
        conn.close()

# =========================================================
# ROUTES: SCHEDULES (Safe SQLite Check-then-Update)
# =========================================================
@app.get("/api/v1/schedules/{serial_number}")
def get_station_schedule(serial_number: str, user: dict = Depends(require_auth)):
    _ensure_station_access(user, serial_number)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT serial_number, days_of_week, start_time_pht, duration_sec,
                   room_volume_m3, dosage_ml, is_active, last_triggered_slot, last_triggered_at
            FROM device_schedules
            WHERE serial_number = ?
            """,
            (serial_number,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No schedule found")
        return dict(row)
    finally:
        conn.close()

@app.post("/api/v1/schedules")
def save_schedule(payload: SchedulePayload, user: dict = Depends(require_auth)):
    _ensure_station_access(user, payload.serial_number)
    days = normalize_days_of_week(payload.days_of_week)
    if not days:
        raise HTTPException(
            status_code=400,
            detail="Select at least one valid weekday (MON-SUN).",
        )
    hhmm = normalize_hhmm(payload.start_time_pht)
    if hhmm is None:
        raise HTTPException(
            status_code=400,
            detail="Invalid schedule time; expected HH:MM (24-hour PHT).",
        )
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        dur = payload.duration_sec
        vol = payload.room_volume_m3 if payload.room_volume_m3 is not None else 1.0
        dose = payload.dosage_ml if payload.dosage_ml is not None else 10.0

        cursor.execute(
            "SELECT id, days_of_week, start_time_pht FROM device_schedules WHERE serial_number = ?",
            (payload.serial_number,),
        )
        existing = cursor.fetchone()

        if existing:
            slot_changed = (
                str(existing["days_of_week"] or "") != payload.days_of_week
                or str(existing["start_time_pht"] or "") != payload.start_time_pht
            )
            if slot_changed:
                cursor.execute(
                    """
                    UPDATE device_schedules
                    SET days_of_week = ?, start_time_pht = ?, duration_sec = ?,
                        room_volume_m3 = ?, dosage_ml = ?, is_active = 1, last_triggered_slot = NULL
                    WHERE serial_number = ?
                    """,
                    (payload.days_of_week, payload.start_time_pht, dur, vol, dose, payload.serial_number),
                )
            else:
                cursor.execute(
                    """
                    UPDATE device_schedules
                    SET days_of_week = ?, start_time_pht = ?, duration_sec = ?,
                        room_volume_m3 = ?, dosage_ml = ?, is_active = 1
                    WHERE serial_number = ?
                    """,
                    (payload.days_of_week, payload.start_time_pht, dur, vol, dose, payload.serial_number),
                )
        else:
            cursor.execute(
                """
                INSERT INTO device_schedules (serial_number, days_of_week, start_time_pht, duration_sec, room_volume_m3, dosage_ml, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (payload.serial_number, payload.days_of_week, payload.start_time_pht, dur, vol, dose),
            )

        conn.commit()
        return {"status": "SCHEDULE_SAVED", "serial_number": payload.serial_number}
    finally:
        conn.close()

# =========================================================
# ROUTES: AUDIT LEDGER & MULTI-DEVICE OVERVIEW STATS
# =========================================================
@app.get("/api/v1/records")
def get_audit_records(
    response: Response,
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    station_id: Optional[str] = None,
    since: Optional[str] = None,
    user: dict = Depends(require_auth),
) -> list[dict]:
    safe_limit = max(1, min(limit, 5000))
    safe_offset = max(0, offset)
    selected_status = (status or status_filter or "").strip().lower()
    search_term = (search or "").strip()
    station_term = (station_id or "").strip()
    since_term = (since or "").strip()

    where_clauses = []
    params = []

    if station_term:
        where_clauses.append("station_id = ?")
        params.append(station_term)

    if since_term:
        try:
            since_dt = datetime.fromisoformat(since_term.replace("Z", "+00:00"))
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            since_utc_prefix = since_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid 'since' timestamp; expected ISO-8601 format.",
            )
        where_clauses.append("REPLACE(substr(timestamp, 1, 19), ' ', 'T') >= ?")
        params.append(since_utc_prefix)

    if selected_status and selected_status != "all":
        if selected_status in {"e-stop", "estop", "halted"}:
            where_clauses.append("(estop_triggered = 1 OR compliance_status = 'SAFETY INTERLOCK TRIPPED')")
        elif selected_status in {"pass", "marginal", "fail"}:
            where_clauses.append("(estop_triggered = 0 AND LOWER(efficacy_label) = ?)")
            params.append(selected_status)

    if search_term:
        like_term = f"%{search_term}%"
        where_clauses.append("(station_id LIKE ? OR compliance_status LIKE ? OR trigger_type LIKE ? OR CAST(id AS TEXT) LIKE ?)")
        params.extend([like_term, like_term, like_term, like_term])

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cycle_records")
        total_count = int(cursor.fetchone()[0])

        cursor.execute(f"SELECT COUNT(*) FROM cycle_records {where_sql}", tuple(params))
        filtered_count = int(cursor.fetchone()[0])

        cursor.execute(f"SELECT * FROM cycle_records {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?", (*params, safe_limit, safe_offset))
        rows = cursor.fetchall()
    finally:
        conn.close()

    response.headers["X-Total-Count"] = str(total_count)
    response.headers["X-Filtered-Count"] = str(filtered_count)
    return [dict(r) for r in rows]

@app.get("/api/v1/stats")
def get_audit_stats(station_id: Optional[str] = None, user: dict = Depends(require_auth)) -> dict:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) AS total_cycles,
                COALESCE(SUM(CASE WHEN estop_triggered = 0 AND efficacy_label = 'Pass' THEN 1 ELSE 0 END), 0) AS pass_count,
                COALESCE(SUM(CASE WHEN estop_triggered = 0 AND efficacy_label = 'Marginal' THEN 1 ELSE 0 END), 0) AS marginal_count,
                COALESCE(SUM(CASE WHEN estop_triggered = 0 AND efficacy_label = 'Fail' THEN 1 ELSE 0 END), 0) AS fail_count,
                COALESCE(SUM(CASE WHEN estop_triggered = 1 THEN 1 ELSE 0 END), 0) AS estop_count
            FROM cycle_records
        """)
        row = cursor.fetchone()
        ledger_status = _verify_ledger_chain(cursor)

        # Fetch user's claimed devices to calculate multi-station health breakdown
        cursor.execute("SELECT serial_number, nickname FROM user_devices WHERE user_id = ?", (user["username"],))
        claimed = cursor.fetchall()
    finally:
        conn.close()

    fleet_summary = []
    active_count = 0
    low_hocl_count = 0
    defect_count = 0

    for dev in claimed:
        s_num = dev["serial_number"]
        reservoir = _get_station_reservoir(s_num)
        base_tel = live_station_telemetry.get(s_num, {
            "station_id": s_num,
            "ambient_temp_c": 25.0,
            "relative_humidity_pct": 65.0,
            "flow_rate_ml_min": 30.0,
            "hocl_low_level": False,
            "is_defect": False,
        })
        float_low = bool(base_tel.get("hocl_float_low", False))
        is_low = float_low or bool(base_tel.get("hocl_low_level", False)) or (
            reservoir["hocl_remaining_ml"] <= reservoir["hocl_low_threshold_ml"]
        )
        telemetry = {
            **base_tel,
            "hocl_remaining_ml": reservoir["hocl_remaining_ml"],
            "hocl_capacity_ml": reservoir["hocl_capacity_ml"],
            "hocl_low_threshold_ml": reservoir["hocl_low_threshold_ml"],
            "hocl_low_level": is_low,
            "presence_detected": bool(base_tel.get("presence_detected", False)),
            "presence_sensor_enabled": _get_station_presence_enabled(s_num),
        }

        is_defect = telemetry.get("is_defect", False)

        if is_defect:
            dev_status = "DEFECT"
            defect_count += 1
        elif is_low:
            dev_status = "LOW_HOCL"
            low_hocl_count += 1
        else:
            dev_status = "ACTIVE"
            active_count += 1

        fleet_summary.append({
            "serial_number": s_num,
            "nickname": dev["nickname"],
            "status": dev_status,
            "telemetry": telemetry
        })

    total_cycles = int(row["total_cycles"])
    pass_count = int(row["pass_count"])
    pass_rate_pct = round((pass_count / total_cycles) * 100.0, 1) if total_cycles > 0 else 0.0
    target_station = (station_id or "").strip() or "MIST-BOH-01"
    target_reservoir = _get_station_reservoir(target_station)
    target_base_tel = live_station_telemetry.get(target_station, {
        "station_id": target_station,
        "ambient_temp_c": 25.0,
        "relative_humidity_pct": 65.0,
        "flow_rate_ml_min": 30.0,
        "hocl_low_level": False,
    })
    target_is_low = (
        bool(target_base_tel.get("hocl_float_low", False))
        or bool(target_base_tel.get("hocl_low_level", False))
        or (target_reservoir["hocl_remaining_ml"] <= target_reservoir["hocl_low_threshold_ml"])
    )
    target_live_telemetry = {
        **target_base_tel,
        "hocl_remaining_ml": target_reservoir["hocl_remaining_ml"],
        "hocl_capacity_ml": target_reservoir["hocl_capacity_ml"],
        "hocl_low_threshold_ml": target_reservoir["hocl_low_threshold_ml"],
        "hocl_low_level": target_is_low,
        "presence_detected": bool(target_base_tel.get("presence_detected", False)),
        "presence_sensor_enabled": _get_station_presence_enabled(target_station),
    }

    return {
        "total_cycles": total_cycles,
        "pass_count": pass_count,
        "marginal_count": int(row["marginal_count"]),
        "fail_count": int(row["fail_count"]),
        "estop_count": int(row["estop_count"]),
        "pass_rate_pct": pass_rate_pct,
        "ledger_verified": ledger_status["verified"],
        "latest_hash": ledger_status["latest_hash"],
        "mqtt_connected": mqtt_client.is_connected(),
        "mqtt_broker_host": MQTT_BROKER_HOST,
        "mqtt_broker_port": MQTT_BROKER_PORT,
        "mqtt_tls_enabled": MQTT_USE_TLS,
        "mqtt_credentials_configured": bool(MQTT_BROKER_USERNAME),
        "fleet_health": {
            "total_attached": len(claimed),
            "active_count": active_count,
            "low_hocl_count": low_hocl_count,
            "defect_count": defect_count,
            "devices": fleet_summary
        },
        "live_telemetry": target_live_telemetry
    }

@app.get("/api/v1/ledger/verify")
def verify_ledger(user: dict = Depends(require_auth)) -> dict:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        return _verify_ledger_chain(cursor)
    finally:
        conn.close()

# =========================================================
# ROUTES: DISPATCH TRIGGER & E-STOP
# =========================================================
@app.post("/api/v1/trigger")
def trigger_cycle(req: CycleTriggerRequest, user: dict = Depends(require_auth)) -> dict:
    trigger_type = req.trigger_type or "MANUAL_WEB"
    station_id = req.station_id or "MIST-BOH-01"
    _ensure_station_access(user, station_id)
    sufficient, info = _check_station_hocl_sufficient(station_id, req.duration_sec)
    if not sufficient:
        if info["is_low"]:
            detail = (
                f"HOCl disinfectant reservoir depleted ({info['remaining_ml']:.1f} mL remaining); "
                "refill before starting a cycle."
            )
        else:
            detail = (
                f"Insufficient HOCl for {req.duration_sec}s cycle: needs {info['required_ml']:.1f} mL, "
                f"only {info['usable_ml']:.1f} mL usable above {info['threshold_ml']:.0f} mL reserve "
                f"({info['remaining_ml']:.1f} mL remaining)."
            )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail,
        )

    ok, err = _publish_start_command(
        station_id=station_id,
        duration_sec=req.duration_sec,
        room_volume_m3=req.room_volume_m3,
        trigger_type=trigger_type,
        triggered_by=user["username"],
    )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=err or "MQTT dispatch failed.",
        )

    return {
        "status": "DISPATCHED",
        "station_id": station_id,
        "trigger_type": trigger_type,
        "duration_sec": req.duration_sec,
        "room_volume_m3": req.room_volume_m3,
        "triggered_by": user["username"],
    }

@app.post("/api/v1/control/estop")
def emergency_stop_cycle(
    req: Optional[EStopRequest] = None,
    user: dict = Depends(require_auth),
):
    station_id = (req.station_id if req and req.station_id else "MIST-BOH-01").strip() or "MIST-BOH-01"
    _ensure_station_access(user, station_id)
    device_key = _get_device_auth_key(station_id)
    abort_payload = {
        "command": "STOP",
        "station_id": station_id,
        "triggered_by": user["username"],
        "auth_key": device_key,
    }
    abort_payload["signature"] = _compute_mqtt_signature(abort_payload, device_key)
    if mqtt_client.is_connected():
        mqtt_client.publish(MQTT_CONTROL_TOPIC, json.dumps(abort_payload))

    live_tel = live_station_telemetry.get(station_id, {})
    cycle_abort = CycleTelemetryInput(
        station_id=station_id,
        ambient_temp_c=float(live_tel.get("ambient_temp_c", 25.0)),
        relative_humidity_pct=float(live_tel.get("relative_humidity_pct", 65.0)),
        flow_rate_ml_min=0.0,
        cycle_duration_sec=0.0,
        room_volume_m3=1.0,
        delivery_confirmed=False,
        estop_triggered=True,
        trigger_type="EMERGENCY_STOP",
    )
    evaluate_telemetry_cycle(cycle_abort)
    return {
        "status": "HALTED",
        "station_id": station_id,
        "triggered_by": user["username"],
        "message": "Emergency Stop dispatched",
    }

@app.get("/health")
def health_check() -> dict:
    return {
        "status": "online",
        "service": "MistSan Compliance API",
        "model_loaded": model is not None,
        "mqtt_connected": mqtt_client.is_connected(),
        "mqtt_broker_host": MQTT_BROKER_HOST,
        "mqtt_broker_port": MQTT_BROKER_PORT,
        "mqtt_tls_enabled": MQTT_USE_TLS,
        "mqtt_credentials_configured": bool(MQTT_BROKER_USERNAME),
    }

@app.get("/api/v1/config")
def get_public_config() -> dict:
    return {
        "google_client_id": os.getenv("SANMIST_GOOGLE_CLIENT_ID", "").strip(),
    }

if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
    FRONTEND_IMG_DIR = os.path.join(FRONTEND_DIR, "img")
    if os.path.exists(FRONTEND_IMG_DIR):
        app.mount("/img", StaticFiles(directory=FRONTEND_IMG_DIR), name="img")

    @app.get("/favicon.ico")
    def serve_favicon():
        logo_path = os.path.join(FRONTEND_DIR, "img", "logo.png")
        if os.path.exists(logo_path):
            return FileResponse(logo_path, media_type="image/png")
        raise HTTPException(status_code=404, detail="Favicon not found")

    @app.get("/privacy")
    def serve_privacy():
        privacy_path = os.path.join(FRONTEND_DIR, "privacy.html")
        if os.path.exists(privacy_path):
            return FileResponse(privacy_path)
        raise HTTPException(status_code=404, detail="Privacy policy not found")

    @app.get("/terms")
    def serve_terms():
        terms_path = os.path.join(FRONTEND_DIR, "terms.html")
        if os.path.exists(terms_path):
            return FileResponse(terms_path)
        raise HTTPException(status_code=404, detail="Terms of service not found")

    @app.get("/")
    def serve_frontend():
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return {"status": "online"}