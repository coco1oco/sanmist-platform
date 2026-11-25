from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
import sqlite3

PHT_TZ = timezone(timedelta(hours=8))
WEEKDAY_CODES = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
VALID_WEEKDAYS = set(WEEKDAY_CODES)


def normalize_days_of_week(days_str: Optional[str]) -> set[str]:
    if not days_str:
        return set()
    return {
        token.strip().upper()
        for token in str(days_str).split(",")
        if token.strip().upper() in VALID_WEEKDAYS
    }


def normalize_hhmm(time_str: Optional[str]) -> Optional[str]:
    if not time_str:
        return None
    parts = str(time_str).strip().split(":")
    if len(parts) < 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return f"{hour:02d}:{minute:02d}"


def run_schedule_tick(
    conn_factory: Callable[[], sqlite3.Connection],
    dispatch_fn: Callable[..., bool | str],
    now_utc: Optional[datetime] = None,
) -> list[dict]:
    """Evaluate active PHT schedules once for the current minute slot and dispatch due stations."""
    current_utc = now_utc or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=timezone.utc)
    now_pht = current_utc.astimezone(PHT_TZ)

    today_code = WEEKDAY_CODES[now_pht.weekday()]
    current_hhmm = now_pht.strftime("%H:%M")
    current_slot = now_pht.strftime("%Y-%m-%d %H:%M")

    dispatched_records: list[dict] = []
    conn = conn_factory()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, serial_number, days_of_week, start_time_pht,
                   duration_sec, room_volume_m3, last_triggered_slot
            FROM device_schedules
            WHERE is_active = 1
            """
        )
        schedules = cursor.fetchall()

        for row in schedules:
            days = normalize_days_of_week(row["days_of_week"])
            target_hhmm = normalize_hhmm(row["start_time_pht"])
            if today_code not in days or target_hhmm != current_hhmm:
                continue
            if row["last_triggered_slot"] == current_slot:
                continue

            serial = str(row["serial_number"])
            duration_sec = int(row["duration_sec"] or 30)
            room_volume_m3 = float(row["room_volume_m3"] or 1.0)

            try:
                result = dispatch_fn(
                    station_id=serial,
                    duration_sec=duration_sec,
                    room_volume_m3=room_volume_m3,
                    trigger_type="SCHEDULED",
                    triggered_by="scheduler",
                )
                if result:
                    triggered_iso = current_utc.isoformat()
                    cursor.execute(
                        """
                        UPDATE device_schedules
                        SET last_triggered_slot = ?, last_triggered_at = ?
                        WHERE id = ?
                        """,
                        (current_slot, triggered_iso, row["id"]),
                    )
                    conn.commit()
                    dispatched_records.append(
                        {
                            "schedule_id": int(row["id"]),
                            "station_id": serial,
                            "duration_sec": duration_sec,
                            "room_volume_m3": room_volume_m3,
                            "slot_pht": current_slot,
                            "triggered_at": triggered_iso,
                            "status": result if isinstance(result, str) else "DISPATCHED",
                        }
                    )
            except Exception as exc:
                print(f"[SCHEDULER WARN] Failed schedule #{row['id']} ({serial}): {exc}")
    finally:
        conn.close()

    return dispatched_records
