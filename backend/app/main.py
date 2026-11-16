from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
from typing import Optional

import urllib.request
from pydantic import BaseModel
from fastapi import HTTPException

import joblib
import paho.mqtt.client as mqtt
from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except Exception as e:
    xgb = None
    XGB_AVAILABLE = False
    print(f"[WARN] XGBoost/Pandas C-extension failed to load on this Python runtime ({e}). Running with rule-based SEI fallback.")

# 1. Paths & Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_JSON_PATH = os.path.abspath(
    os.path.join(BASE_DIR, "..", "..", "ml-pipeline", "models", "xgboost_sei_classifier.json")
)
MODEL_PKL_PATH = os.path.abspath(
    os.path.join(BASE_DIR, "..", "..", "ml-pipeline", "models", "xgboost_sei_classifier.pkl")
)
DB_PATH = os.path.join(BASE_DIR, "sanmist_audit.db")

MQTT_BROKER_HOST = os.getenv("SANMIST_MQTT_BROKER", "broker.emqx.io")
MQTT_BROKER_PORT = int(os.getenv("SANMIST_MQTT_PORT", "1883"))
MQTT_BROKER_USERNAME = os.getenv("SANMIST_MQTT_USERNAME", "")
MQTT_BROKER_PASSWORD = os.getenv("SANMIST_MQTT_PASSWORD", "")
MQTT_DEVICE_KEY = os.getenv("SANMIST_DEVICE_KEY", "sanmist-edge-key-2026")
MQTT_REQUIRE_AUTH = os.getenv("SANMIST_MQTT_REQUIRE_AUTH", "0") == "1"
MQTT_TELEMETRY_TOPIC = "sanmist/devices/+/telemetry"
MQTT_STATUS_TOPIC = "sanmist/devices/+/status"
MQTT_CONTROL_TOPIC = "sanmist/control/trigger"
SESSION_TTL_HOURS = int(os.getenv("SANMIST_SESSION_TTL_HOURS", "24"))
GENESIS_HASH = "0" * 64
live_station_telemetry: dict[str, dict] = {}

# 2. Load Trained XGBoost Classifier (prefer native JSON artifact)
model = None
try:
    if os.path.exists(MODEL_JSON_PATH):
        loaded_xgb = xgb.XGBClassifier()
        loaded_xgb.load_model(MODEL_JSON_PATH)
        model = loaded_xgb
        print(f"[INIT] Loaded native XGBoost JSON model from: {MODEL_JSON_PATH}")
    elif os.path.exists(MODEL_PKL_PATH):
        model = joblib.load(MODEL_PKL_PATH)
        print(f"[INIT] Loaded fallback pickle model from: {MODEL_PKL_PATH}")
    else:
        print("[WARN] No trained model artifact found; using SEI regulatory rules only.")
except Exception as exc:
    print(f"[WARN] Could not load model: {exc}")


# 3. Database & Authentication Helpers
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
) -> str:
    """Compute deterministic SHA-256 hash over canonical cycle fields and prev_hash."""
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


def _compute_row_hash(row: sqlite3.Row, prev_hash: str) -> str:
    """Compute record hash from an existing SQLite cycle_records row."""
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
    )


def _verify_ledger_chain(cursor: sqlite3.Cursor) -> dict:
    """Verify full SHA-256 hash chain integrity across cycle_records."""
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
        if row_prev != expected_prev_hash or row_hash != expected_hash:
            tampered_ids.append(int(row["id"]))
        expected_prev_hash = row_hash if row_hash else ""

    return {
        "verified": len(tampered_ids) == 0,
        "checked_records": count,
        "tampered_ids": tampered_ids,
        "latest_hash": expected_prev_hash,
    }


def _seed_default_users(cursor: sqlite3.Cursor) -> None:
    # Remove legacy secondary 'operator' account so only the single admin user remains
    cursor.execute("DELETE FROM users WHERE username = 'operator'")
    cursor.execute("DELETE FROM auth_sessions WHERE username = 'operator'")

    cursor.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cursor.fetchone()[0] > 0:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    raw_password = os.getenv("SANMIST_ADMIN_PASSWORD", "sanmist2026")
    pwd_hash, salt_hex = _hash_password(raw_password)
    cursor.execute(
        """
        INSERT INTO users (username, password_hash, salt, full_name, role, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("admin", pwd_hash, salt_hex, "SanMist Lead Auditor", "Safety Administrator", now_iso),
    )
    print("[AUTH] Seeded default dashboard administrator account (admin).")


def init_db() -> None:
    """Initialize SQLite ledger, auth tables, and safe schema migrations."""
    conn = get_db_connection()
    cursor = conn.cursor()

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

    if "compliance_status" not in existing_columns:
        cursor.execute(
            "ALTER TABLE cycle_records ADD COLUMN compliance_status TEXT NOT NULL DEFAULT 'REQUIRES ATTENTION'"
        )
        print("[DB] Added missing column 'compliance_status' to cycle_records.")

    if "delivery_confirmed" not in existing_columns:
        cursor.execute(
            "ALTER TABLE cycle_records ADD COLUMN delivery_confirmed INTEGER NOT NULL DEFAULT 1"
        )
        print("[DB] Added missing column 'delivery_confirmed' to cycle_records.")

    if "ml_confidence" not in existing_columns:
        cursor.execute("ALTER TABLE cycle_records ADD COLUMN ml_confidence REAL")
        print("[DB] Added missing column 'ml_confidence' to cycle_records.")

    if "prev_hash" not in existing_columns:
        cursor.execute("ALTER TABLE cycle_records ADD COLUMN prev_hash TEXT")
        print("[DB] Added missing column 'prev_hash' to cycle_records.")

    if "record_hash" not in existing_columns:
        cursor.execute("ALTER TABLE cycle_records ADD COLUMN record_hash TEXT")
        print("[DB] Added missing column 'record_hash' to cycle_records.")

    # Normalize any legacy rows whose compliance_status conflicted with E-Stop or efficacy_label
    cursor.execute("""
        UPDATE cycle_records
        SET compliance_status = 'SAFETY INTERLOCK TRIPPED'
        WHERE estop_triggered = 1 AND compliance_status = 'AUDIT VERIFIED'
    """)
    cursor.execute("""
        UPDATE cycle_records
        SET compliance_status = 'REQUIRES ATTENTION'
        WHERE estop_triggered = 0 AND efficacy_label != 'Pass' AND compliance_status = 'AUDIT VERIFIED'
    """)

    # Backfill SHA-256 hash chain for any unhashed legacy rows while preserving existing hashes
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

    # --- Multi-Device & Serial Number Management ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            serial_number TEXT PRIMARY KEY,
            model_name TEXT NOT NULL DEFAULT 'MistSan Standard',
            auth_key TEXT NOT NULL DEFAULT 'sanmist-edge-key-2026',
            is_claimed INTEGER NOT NULL DEFAULT 0,
            registered_at TEXT NOT NULL
        )
    """)

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
            dosage_ml REAL NOT NULL DEFAULT 10.0,
            is_active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (serial_number) REFERENCES devices(serial_number) ON DELETE CASCADE
        )
    """)

    # Seed the current simulation station so Wokwi stays paired and working immediately
    cursor.execute("""
        INSERT OR IGNORE INTO devices (serial_number, model_name, auth_key, is_claimed, registered_at)
        VALUES ('MIST-BOH-01', 'MistSan Station BOH Pro', 'sanmist-edge-key-2026', 1, datetime('now'))
    """)

    # Link MIST-BOH-01 to your default admin account
    cursor.execute("""
        INSERT OR IGNORE INTO user_devices (id, user_id, serial_number, nickname, room_volume_m3, paired_at)
        VALUES (1, 'admin', 'MIST-BOH-01', 'Kitchen Prep Area', 1.0, datetime('now'))
    """)

    _seed_default_users(cursor)

    conn.commit()
    conn.close()
    print(f"[DB] Initialized/verified schema at: {DB_PATH}")

init_db()

# 4. Request & Auth Schemas
class CycleTelemetryInput(BaseModel):
    station_id: str = Field(default="MIST-BOH-01", min_length=1, max_length=64)
    ambient_temp_c: float = Field(
        ..., ge=0.0, le=60.0, description="Temperature in Celsius (0.0 allowed on E-Stop/abort)"
    )
    relative_humidity_pct: float = Field(
        ..., ge=0.0, le=100.0, description="Relative Humidity % (0.0 allowed on E-Stop/abort)"
    )
    flow_rate_ml_min: float = Field(
        ..., ge=0.0, le=100.0, description="Flow rate in mL/min from YF-S401 proxy"
    )
    cycle_duration_sec: float = Field(
        ..., ge=0.0, le=600.0, description="Misting duration in seconds (sub-10s allowed on E-Stop)"
    )
    room_volume_m3: float = Field(
        default=1.0, gt=0, le=200.0, description="Target room/zone volume in m3"
    )
    delivery_confirmed: bool = Field(default=True)
    estop_triggered: bool = Field(default=False)


class CycleTriggerRequest(BaseModel):
    duration_sec: int = Field(default=30, ge=5, le=300, description="Misting duration in seconds")
    room_volume_m3: float = Field(default=1.0, gt=0, le=200.0, description="Target room volume in m3")


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=128)


# 5. Auth Dependency
security = HTTPBearer(auto_error=False)


def require_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> dict:
    if credentials is None or credentials.scheme.lower() != "bearer" or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT token, username, full_name, role, expires_at FROM auth_sessions WHERE token = ?",
        (token,),
    )
    session_row = cursor.fetchone()

    if session_row is None:
        conn.close()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        raw_exp = session_row["expires_at"]
        if isinstance(raw_exp, str):
            # Siguraduhing napapalitan ang Z ng +00:00 kung ISO string ito
            expires_at = datetime.fromisoformat(raw_exp.replace("Z", "+00:00"))
        else:
            expires_at = raw_exp

        # Kung walang timezone metadata ang nakuha mula sa database, lagyan ng UTC
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        # Ngayon pareho na silang timezone-aware sa UTC
        is_expired = datetime.now(timezone.utc) >= expires_at
    except (ValueError, TypeError):
        is_expired = True

    if is_expired:
        cursor.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
        conn.commit()
        conn.close()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired; please sign in again",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_info = {
        "token": session_row["token"],
        "username": session_row["username"],
        "full_name": session_row["full_name"],
        "role": session_row["role"],
    }
    conn.close()
    return user_info


# 6. Core SEI Computation, ML Inference & MQTT Subscriber
def compute_evaporation_adjusted_sei(cycle: CycleTelemetryInput) -> float:
    """Compute evaporation-adjusted SEI aligned with ml-pipeline/generate_dataset.py."""
    evap_loss = (cycle.ambient_temp_c - 25.0) * 0.003 - (cycle.relative_humidity_pct - 65.0) * 0.001
    loss_factor = max(0.85, min(1.15, 1.0 - evap_loss))
    effective_flow = cycle.flow_rate_ml_min * loss_factor
    duration_min = cycle.cycle_duration_sec / 60.0
    return round((effective_flow * duration_min) / cycle.room_volume_m3, 3)


def evaluate_telemetry_cycle(
    cycle: CycleTelemetryInput,
    deduplicate_window_sec: float = 0.0,
) -> dict:
    """Evaluate safety interlocks, SEI compliance, XGBoost class, and persist to SQLite."""
    now_dt = datetime.now(timezone.utc)
    recorded_at = now_dt.isoformat()
    ml_confidence: Optional[float] = None

    if cycle.estop_triggered:
        sei_score = 0.0
        predicted_class = 0
        efficacy_label = "Fail"
        compliance_status = "SAFETY INTERLOCK TRIPPED"
        print(f"[SAFETY] Emergency Stop activated! Cycle halted at {cycle.cycle_duration_sec:.1f}s.")
    else:
        sei_score = compute_evaporation_adjusted_sei(cycle)
        is_compliant = sei_score >= 2.800

        pred = None
        probs = None
        if model is not None:
            try:
                features = [[
                    float(cycle.ambient_temp_c),
                    float(cycle.relative_humidity_pct),
                    float(cycle.flow_rate_ml_min),
                    float(cycle.cycle_duration_sec),
                    float(cycle.room_volume_m3),
                ]]
                pred = int(model.predict(features)[0])
                if hasattr(model, "predict_proba"):
                    probs = model.predict_proba(features)[0]
            except Exception as exc:
                print(f"[WARN] ML Inference failed, falling back to SEI rule: {exc}")

        label_map = {0: "Fail", 1: "Marginal", 2: "Pass"}

        if is_compliant:
            predicted_class = 2
            efficacy_label = "Pass"
            compliance_status = "AUDIT VERIFIED"
            if probs is not None and len(probs) > 2:
                ml_confidence = round(float(probs[2]) * 100.0, 1)
        else:
            # Below PD 856 Pass threshold (2.800): never log contradictory "Pass" + "REQUIRES ATTENTION"
            compliance_status = "REQUIRES ATTENTION"
            if pred is not None and pred in (0, 1):
                predicted_class = pred
            else:
                predicted_class = 1 if sei_score >= 1.800 else 0
            efficacy_label = label_map.get(predicted_class, "Fail")
            if probs is not None and len(probs) > predicted_class:
                ml_confidence = round(float(probs[predicted_class]) * 100.0, 1)

    estop_int = 1 if cycle.estop_triggered else 0
    conn = get_db_connection(isolation_level=None)
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")

        if deduplicate_window_sec > 0.0:
            cursor.execute(
                """
                SELECT id, timestamp, prev_hash, record_hash FROM cycle_records
                WHERE station_id = ?
                  AND flow_rate_ml_min = ?
                  AND cycle_duration_sec = ?
                  AND estop_triggered = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    cycle.station_id,
                    cycle.flow_rate_ml_min,
                    cycle.cycle_duration_sec,
                    estop_int,
                ),
            )
            recent = cursor.fetchone()
            if recent is not None:
                try:
                    last_ts = recent["timestamp"]
                    if not last_ts.endswith("Z") and "+" not in last_ts:
                        last_ts += "Z"
                    last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    if (now_dt - last_dt).total_seconds() <= deduplicate_window_sec:
                        cursor.execute("ROLLBACK")
                        print(
                            f"[MQTT DEDUP] Ignored duplicate MQTT telemetry already recorded as cycle #{recent['id']}"
                        )
                        return {
                            "duplicate": True,
                            "id": recent["id"],
                            "timestamp": recent["timestamp"],
                            "station_id": cycle.station_id,
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
        prev_hash = (
            prev_row["record_hash"]
            if prev_row is not None and prev_row["record_hash"]
            else GENESIS_HASH
        )
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
        )

        cursor.execute(
            """
            INSERT INTO cycle_records (
                timestamp, station_id, ambient_temp_c, relative_humidity_pct,
                flow_rate_ml_min, cycle_duration_sec, room_volume_m3, sei_score,
                predicted_class, efficacy_label, compliance_status, delivery_confirmed,
                ml_confidence, estop_triggered, prev_hash, record_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recorded_at,
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
                1 if cycle.delivery_confirmed else 0,
                ml_confidence,
                estop_int,
                prev_hash,
                record_hash,
            ),
        )
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

    print(
        f"[DB] Stored cycle #{inserted_id}: Class={predicted_class} | "
        f"Label={efficacy_label} | Status={compliance_status} | Time={recorded_at}"
    )

    return {
        "duplicate": False,
        "id": inserted_id,
        "timestamp": recorded_at,
        "station_id": cycle.station_id,
        "sei_score": sei_score,
        "predicted_class": predicted_class,
        "efficacy_label": efficacy_label,
        "compliance_status": compliance_status,
        "delivery_confirmed": cycle.delivery_confirmed,
        "ml_confidence": ml_confidence,
        "estop_triggered": cycle.estop_triggered,
        "prev_hash": prev_hash,
        "record_hash": record_hash,
    }


def _compute_mqtt_signature(data_dict: dict, secret: str = MQTT_DEVICE_KEY) -> str:
    """Compute deterministic HMAC-SHA256 over canonical MQTT payload fields."""
    station_id = str(data_dict.get("station_id") or "MIST-BOH-01")
    if "command" in data_dict:
        msg = (
            f"CMD|{data_dict.get('command')}|"
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


def _verify_mqtt_auth(data_dict: dict) -> bool:
    """Verify HMAC-SHA256 signature or shared device auth_key on inbound MQTT JSON."""
    if not isinstance(data_dict, dict):
        return False
    provided_sig = str(data_dict.get("signature") or "").strip()
    provided_key = str(data_dict.get("auth_key") or "").strip()

    if provided_sig:
        expected_sig = _compute_mqtt_signature(data_dict, MQTT_DEVICE_KEY)
        if not hmac.compare_digest(provided_sig.lower(), expected_sig.lower()):
            return False
        return True

    if provided_key:
        return hmac.compare_digest(provided_key, MQTT_DEVICE_KEY)

    return not MQTT_REQUIRE_AUTH


def _on_mqtt_connect(client, userdata, flags, reason_code, properties=None):
    print(f"\n[MQTT] Connected to broker {MQTT_BROKER_HOST} (code {reason_code})")
    client.subscribe(MQTT_TELEMETRY_TOPIC)
    client.subscribe(MQTT_STATUS_TOPIC)
    print(f"[MQTT] Subscribed to topics: {MQTT_TELEMETRY_TOPIC}, {MQTT_STATUS_TOPIC}")


def _on_mqtt_message(client, userdata, msg):
    try:
        raw_payload = msg.payload.decode("utf-8")
        topic_str = str(getattr(msg, "topic", "") or "")
        data_dict = json.loads(raw_payload)

        if not _verify_mqtt_auth(data_dict):
            print("[MQTT AUTH] Rejected unauthorized or forged MQTT payload.")
            return

        if topic_str.endswith("/status"):
            station_id = str(data_dict.get("station_id") or "MIST-BOH-01")
            live_snapshot = {
                "station_id": station_id,
                "ambient_temp_c": float(data_dict.get("ambient_temp_c", 28.5)),
                "relative_humidity_pct": float(data_dict.get("relative_humidity_pct", 65.0)),
                "flow_rate_ml_min": float(data_dict.get("flow_rate_ml_min", 30.0)),
                "cycle_in_progress": bool(data_dict.get("cycle_in_progress", False)),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            live_station_telemetry[station_id] = live_snapshot
            print(
                f"[MQTT LIVE] {station_id}: Temp={live_snapshot['ambient_temp_c']:.1f}C | "
                f"Hum={live_snapshot['relative_humidity_pct']:.1f}% | "
                f"Flow={live_snapshot['flow_rate_ml_min']:.1f}mL/min"
            )
            return

        print(f"\n[MQTT RECV] Inbound ESP32 Telemetry: {raw_payload}")
        cycle_in = CycleTelemetryInput(**data_dict)
        result = evaluate_telemetry_cycle(cycle_in, deduplicate_window_sec=3.0)
        if not result.get("duplicate"):
            print(
                f"[ML EVALUATION RESULT] Station: {result['station_id']} | "
                f"Class: {result['efficacy_label']} | SEI Score: {result['sei_score']} | "
                f"Status: {result['compliance_status']}"
            )
    except Exception as err:
        print(f"[MQTT ERROR] Failed to process payload: {err}")


if hasattr(mqtt, "CallbackAPIVersion"):
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
else:
    mqtt_client = mqtt.Client()

mqtt_client.on_connect = _on_mqtt_connect
mqtt_client.on_message = _on_mqtt_message


@asynccontextmanager
async def lifespan(_: FastAPI):
    if os.getenv("SANMIST_DISABLE_MQTT", "0") != "1":
        try:
            if MQTT_BROKER_USERNAME:
                mqtt_client.username_pw_set(MQTT_BROKER_USERNAME, MQTT_BROKER_PASSWORD)
            mqtt_client.connect_async(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
            mqtt_client.loop_start()
            print(f"[MQTT] Background thread started, listening on {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}...")
        except Exception as exc:
            print(f"[WARN] Could not start MQTT background client: {exc}")
    yield
    try:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception:
        pass



# 7. FastAPI Application & Routes
app = FastAPI(
    title="MistSan Telemetry & Compliance Ingestion Engine",
    description="Real-time ML-evaluated dry-fog sanitization audit microservice (PD 856 / RA 10611 compliant)",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count", "X-Filtered-Count"],
)

# ==========================================
# MULTI-DEVICE & SCHEDULE MANAGEMENT ENDPOINTS
# ==========================================

from pydantic import BaseModel
from typing import Optional

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
    duration_sec: int
    dosage_ml: float

@app.get("/api/v1/user/{username}/devices")
def get_user_devices(username: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ud.serial_number, ud.nickname, ud.room_volume_m3, d.model_name
        FROM user_devices ud
        JOIN devices d ON ud.serial_number = d.serial_number
        WHERE ud.user_id = ?
    """, (username,))
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "serial_number": row[0],
            "nickname": row[1],
            "room_volume_m3": row[2],
            "model_name": row[3]
        }
        for row in rows
    ]

@app.post("/api/v1/devices/claim")
def claim_device(payload: ClaimDevicePayload):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Verify serial number exists in registry
    cursor.execute("SELECT auth_key, is_claimed FROM devices WHERE serial_number = ?", (payload.serial_number,))
    device = cursor.fetchone()
    if not device:
        conn.close()
        raise HTTPException(status_code=404, detail="Serial number not recognized in registry.")
    
    if device[0] != payload.auth_key:
        conn.close()
        raise HTTPException(status_code=401, detail="Invalid Device Authentication Key.")

    # 2. Bind to user account
    cursor.execute("""
        INSERT INTO user_devices (user_id, serial_number, nickname, room_volume_m3, paired_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (payload.username, payload.serial_number, payload.nickname, payload.room_volume_m3))
    
    cursor.execute("UPDATE devices SET is_claimed = 1 WHERE serial_number = ?", (payload.serial_number,))
    conn.commit()
    conn.close()
    return {"status": "SUCCESS", "serial_number": payload.serial_number}

@app.post("/api/v1/schedules")
def create_schedule(payload: SchedulePayload):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO device_schedules (serial_number, days_of_week, start_time_pht, duration_sec, dosage_ml, is_active)
        VALUES (?, ?, ?, ?, ?, 1)
    """, (payload.serial_number, payload.days_of_week, payload.start_time_pht, payload.duration_sec, payload.dosage_ml))
    conn.commit()
    conn.close()
    return {"status": "SCHEDULE_SAVED"}

@app.get("/")
def health_check() -> dict:
    return {
        "status": "online",
        "service": "MistSan Compliance API",
        "model_loaded": model is not None,
        "mqtt_connected": mqtt_client.is_connected(),
    }


@app.post("/api/v1/auth/login")
def login(credentials: LoginRequest) -> dict:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT username, password_hash, salt, full_name, role FROM users WHERE username = ?",
        (credentials.username.strip(),),
    )
    user_row = cursor.fetchone()

    if user_row is None or not _verify_password(
        credentials.password, user_row["password_hash"], user_row["salt"]
    ):
        conn.close()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=SESSION_TTL_HOURS)
    cursor.execute(
        """
        INSERT INTO auth_sessions (token, username, full_name, role, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            token,
            user_row["username"],
            user_row["full_name"],
            user_row["role"],
            now.isoformat(),
            expires_at.isoformat(),
        ),
    )
    conn.commit()
    conn.close()

    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "username": user_row["username"],
            "full_name": user_row["full_name"],
            "role": user_row["role"],
        },
    }


class GoogleAuthPayload(BaseModel):
    id_token: str

@app.post("/api/v1/auth/google")
def auth_google(payload: GoogleAuthPayload):
    try:
        # I-verify ang ID token gamit ang opisyal na tokeninfo endpoint ng Google
        token_url = f"https://oauth2.googleapis.com/tokeninfo?id_token={payload.id_token}"
        req = urllib.request.Request(token_url, headers={"User-Agent": "MistSan-Platform"})
        with urllib.request.urlopen(req, timeout=10) as response:
            google_data = json.loads(response.read().decode("utf-8"))

        email = google_data.get("email")
        full_name = google_data.get("name", email.split("@")[0] if email else "Operator")
        sub = google_data.get("sub")

        if not email:
            raise HTTPException(status_code=400, detail="Invalid Google profile: Email missing.")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Suriin kung rehistrado na ang user; kung hindi pa, i-save sa database
        cursor.execute("SELECT id, username, full_name, role FROM users WHERE username = ?", (email,))
        user = cursor.fetchone()

        if not user:
            cursor.execute("""
                INSERT INTO users (username, password_hash, salt, full_name, role, created_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
            """, (email, "GOOGLE_OAUTH", "OAUTH", full_name, "Operator"))
            conn.commit()
            user_profile = {"username": email, "full_name": full_name, "role": "Operator"}
        else:
            user_profile = {"username": user[1], "full_name": user[2], "role": user[3]}

        # Gumawa ng session token
        session_token = f"mistsan_g_{sub}"
        cursor.execute("""
            INSERT OR REPLACE INTO auth_sessions (token, username, full_name, role, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now'), datetime('now', '+7 days'))
        """, (session_token, user_profile["username"], user_profile["full_name"], user_profile["role"]))

        conn.commit()
        conn.close()

        return {"access_token": session_token, "user": user_profile}
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Google Authentication Failed: {str(e)}")


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
def evaluate_and_log_cycle(cycle: CycleTelemetryInput) -> dict:
    return evaluate_telemetry_cycle(cycle)


@app.get("/api/v1/records")
def get_audit_records(
    response: Response,
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    user: dict = Depends(require_auth),
) -> list[dict]:
    safe_limit = max(1, min(limit, 5000))
    safe_offset = max(0, offset)
    selected_status = (status or status_filter or "").strip().lower()
    search_term = (search or "").strip()

    where_clauses = []
    params: list = []

    if selected_status and selected_status != "all":
        if selected_status in {"e-stop", "estop", "halted"}:
            where_clauses.append("(estop_triggered = 1 OR compliance_status = 'SAFETY INTERLOCK TRIPPED')")
        elif selected_status == "pass":
            where_clauses.append("(estop_triggered = 0 AND efficacy_label = 'Pass')")
        elif selected_status == "marginal":
            where_clauses.append("(estop_triggered = 0 AND efficacy_label = 'Marginal')")
        elif selected_status == "fail":
            where_clauses.append("(estop_triggered = 0 AND efficacy_label = 'Fail')")

    if search_term:
        like_term = f"%{search_term}%"
        where_clauses.append("(station_id LIKE ? OR compliance_status LIKE ? OR CAST(id AS TEXT) LIKE ?)")
        params.extend([like_term, like_term, like_term])

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM cycle_records")
    total_count = int(cursor.fetchone()[0])

    cursor.execute(f"SELECT COUNT(*) FROM cycle_records {where_sql}", tuple(params))
    filtered_count = int(cursor.fetchone()[0])

    cursor.execute(
        f"SELECT * FROM cycle_records {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?",
        (*params, safe_limit, safe_offset),
    )
    rows = cursor.fetchall()
    conn.close()

    response.headers["X-Total-Count"] = str(total_count)
    response.headers["X-Filtered-Count"] = str(filtered_count)
    return [dict(row) for row in rows]


@app.get("/api/v1/stats")
def get_audit_stats(user: dict = Depends(require_auth)) -> dict:
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
    finally:
        conn.close()

    total_cycles = int(row["total_cycles"])
    pass_count = int(row["pass_count"])
    pass_rate_pct = round((pass_count / total_cycles) * 100.0, 1) if total_cycles > 0 else 0.0

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
        "live_telemetry": live_station_telemetry.get("MIST-BOH-01"),
    }


@app.get("/api/v1/ledger/verify")
def verify_ledger(user: dict = Depends(require_auth)) -> dict:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        return _verify_ledger_chain(cursor)
    finally:
        conn.close()


@app.post("/api/v1/trigger")
def trigger_cycle(req: CycleTriggerRequest, user: dict = Depends(require_auth)) -> dict:
    """Dispatches a remote misting start command to the station over MQTT."""
    if not mqtt_client.is_connected():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MQTT broker is disconnected; unable to dispatch cycle.",
        )

    cmd_dict = {
        "command": "START",
        "duration_sec": req.duration_sec,
        "room_volume_m3": req.room_volume_m3,
        "triggered_by": user["username"],
        "auth_key": MQTT_DEVICE_KEY,
    }
    cmd_dict["signature"] = _compute_mqtt_signature(cmd_dict)
    payload = json.dumps(cmd_dict)
    info = mqtt_client.publish(MQTT_CONTROL_TOPIC, payload)
    if info is None or info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"MQTT publish failed (rc={getattr(info, 'rc', 'unknown')}).",
        )

    return {
        "status": "DISPATCHED",
        "duration_sec": req.duration_sec,
        "room_volume_m3": req.room_volume_m3,
        "triggered_by": user["username"],
    }

@app.post("/api/v1/control/estop")
def emergency_stop_cycle():
        """Broadcasts a STOP command to MQTT control topic to abort active cycles immediately."""
        abort_payload = {
            "command": "STOP",
            "station_id": "MIST-BOH-01",
            "triggered_by": "ui_operator",
            "auth_key": MQTT_DEVICE_KEY,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        mqtt_client.publish(MQTT_CONTROL_TOPIC, json.dumps(abort_payload))
        print("[MQTT CONTROL] Broadcasted emergency STOP command to all stations.")
        return {"status": "HALTED", "message": "Emergency abort command dispatched."}