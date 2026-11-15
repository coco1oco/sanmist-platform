import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import time
import types
import unittest

# Prevent background live MQTT thread when importing app in unit tests
os.environ["SANMIST_DISABLE_MQTT"] = "1"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import backend.app.main as main  # noqa: E402


async def _call_asgi(
    method: str,
    path: str,
    body: dict | None = None,
    headers: dict[str, str] | None = None,
    query_string: str = "",
) -> tuple[int, dict[str, str], dict | list | str]:
    raw_body = json.dumps(body).encode("utf-8") if body is not None else b""
    req_headers: list[tuple[bytes, bytes]] = []
    if body is not None:
        req_headers.append((b"content-type", b"application/json"))
        req_headers.append((b"content-length", str(len(raw_body)).encode("ascii")))
    for k, v in (headers or {}).items():
        req_headers.append((k.lower().encode("ascii"), v.encode("ascii")))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": query_string.encode("ascii"),
        "headers": req_headers,
        "client": ("127.0.0.1", 50000),
        "server": ("testserver", 8000),
    }

    sent_once = False
    status_code = 500
    resp_headers: dict[str, str] = {}
    chunks: list[bytes] = []

    async def receive():
        nonlocal sent_once
        if not sent_once:
            sent_once = True
            return {"type": "http.request", "body": raw_body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        nonlocal status_code, resp_headers
        if message["type"] == "http.response.start":
            status_code = message["status"]
            resp_headers = {
                k.decode("ascii").lower(): v.decode("ascii")
                for k, v in message.get("headers", [])
            }
        elif message["type"] == "http.response.body":
            chunks.append(message.get("body", b""))

    await main.app(scope, receive, send)
    text = b"".join(chunks).decode("utf-8")
    try:
        parsed = json.loads(text) if text else {}
    except json.JSONDecodeError:
        parsed = text
    return status_code, resp_headers, parsed


def call_api(
    method: str,
    path: str,
    body: dict | None = None,
    headers: dict[str, str] | None = None,
    query_string: str = "",
) -> tuple[int, dict[str, str], dict | list | str]:
    return asyncio.run(_call_asgi(method, path, body=body, headers=headers, query_string=query_string))


class SanMistPlatformTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.orig_db_path = main.DB_PATH
        main.DB_PATH = os.path.join(self.tmpdir.name, "test_sanmist.db")
        main.init_db()

    def tearDown(self) -> None:
        main.DB_PATH = self.orig_db_path
        self.tmpdir.cleanup()

    def _login_admin(self) -> str:
        code, _, data = call_api(
            "POST",
            "/api/v1/auth/login",
            body={"username": "admin", "password": os.getenv("SANMIST_ADMIN_PASSWORD", "sanmist2026")},
        )
        self.assertEqual(code, 200)
        return data["access_token"]

    def test_unauthenticated_requests_rejected(self) -> None:
        code_rec, _, _ = call_api("GET", "/api/v1/records")
        code_stats, _, _ = call_api("GET", "/api/v1/stats")
        code_verify, _, _ = call_api("GET", "/api/v1/ledger/verify")
        code_trig, _, _ = call_api("POST", "/api/v1/trigger", body={"duration_sec": 30, "room_volume_m3": 1.0})
        code_bad_tok, _, _ = call_api("GET", "/api/v1/auth/me", headers={"Authorization": "Bearer bogus-token"})

        self.assertEqual(code_rec, 401)
        self.assertEqual(code_stats, 401)
        self.assertEqual(code_verify, 401)
        self.assertEqual(code_trig, 401)
        self.assertEqual(code_bad_tok, 401)

    def test_login_me_and_logout_lifecycle(self) -> None:
        bad_code, _, _ = call_api(
            "POST", "/api/v1/auth/login", body={"username": "admin", "password": "wrong-password"}
        )
        self.assertEqual(bad_code, 401)

        op_code, _, _ = call_api(
            "POST", "/api/v1/auth/login", body={"username": "operator", "password": "operator123"}
        )
        self.assertEqual(op_code, 401)

        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}

        me_code, _, me_data = call_api("GET", "/api/v1/auth/me", headers=auth_headers)
        self.assertEqual(me_code, 200)
        self.assertEqual(me_data["username"], "admin")

        out_code, _, _ = call_api("POST", "/api/v1/auth/logout", headers=auth_headers)
        self.assertEqual(out_code, 200)

        after_code, _, _ = call_api("GET", "/api/v1/auth/me", headers=auth_headers)
        self.assertEqual(after_code, 401)

    def test_telemetry_timestamps_sei_consistency_and_total_count(self) -> None:
        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}

        # Create 25 cycles (Pass, Marginal, Fail, E-Stop) and ensure distinct timestamps
        sample_payloads = [
            # Pass (high SEI)
            {"station_id": "MIST-TEST-01", "ambient_temp_c": 26.0, "relative_humidity_pct": 65.0, "flow_rate_ml_min": 40.0, "cycle_duration_sec": 30.0, "room_volume_m3": 1.0},
            # Marginal (~2.25 SEI)
            {"station_id": "MIST-TEST-01", "ambient_temp_c": 25.0, "relative_humidity_pct": 65.0, "flow_rate_ml_min": 30.0, "cycle_duration_sec": 45.0, "room_volume_m3": 10.0},
            # Borderline raw vs evap_loss (~2.78 SEI -> must NEVER label Pass + REQUIRES ATTENTION)
            {"station_id": "MIST-TEST-01", "ambient_temp_c": 38.0, "relative_humidity_pct": 40.0, "flow_rate_ml_min": 40.0, "cycle_duration_sec": 42.0, "room_volume_m3": 9.8},
            # Fail (<1.8 SEI)
            {"station_id": "MIST-TEST-02", "ambient_temp_c": 30.0, "relative_humidity_pct": 55.0, "flow_rate_ml_min": 15.0, "cycle_duration_sec": 20.0, "room_volume_m3": 10.0},
            # E-Stop
            {"station_id": "MIST-TEST-02", "ambient_temp_c": 0.0, "relative_humidity_pct": 0.0, "flow_rate_ml_min": 0.0, "cycle_duration_sec": 3.4, "room_volume_m3": 1.0, "delivery_confirmed": False, "estop_triggered": True},
        ]

        timestamps = []
        for idx in range(25):
            payload = sample_payloads[idx % len(sample_payloads)]
            code, _, res = call_api("POST", "/api/v1/telemetry/evaluate", body=payload)
            self.assertEqual(code, 200)
            timestamps.append(res["timestamp"])
            # Critical invariant: never save Pass alongside REQUIRES ATTENTION
            if res["compliance_status"] == "REQUIRES ATTENTION":
                self.assertNotEqual(res["efficacy_label"], "Pass")
            if idx == 0:
                time.sleep(0.015)

        self.assertNotEqual(timestamps[0], timestamps[1], "Cycle timestamps must not freeze at startup")

        # Verify X-Total-Count, X-Filtered-Count, and server-side offset pagination
        rec_code, rec_headers, rec_rows = call_api(
            "GET", "/api/v1/records", headers=auth_headers, query_string="limit=10&offset=0"
        )
        self.assertEqual(rec_code, 200)
        self.assertEqual(len(rec_rows), 10)
        self.assertEqual(rec_headers.get("x-total-count"), "25")
        self.assertEqual(rec_headers.get("x-filtered-count"), "25")

        p3_code, p3_headers, page3_rows = call_api(
            "GET", "/api/v1/records", headers=auth_headers, query_string="limit=10&offset=20"
        )
        self.assertEqual(p3_code, 200)
        self.assertEqual(len(page3_rows), 5)
        self.assertEqual(p3_headers.get("x-total-count"), "25")
        self.assertEqual(p3_headers.get("x-filtered-count"), "25")
        page1_ids = {r["id"] for r in rec_rows}
        page3_ids = {r["id"] for r in page3_rows}
        self.assertTrue(page1_ids.isdisjoint(page3_ids), "Offset page 3 must not overlap page 1")

        # Verify filtering by status & search updates X-Filtered-Count while preserving X-Total-Count
        fail_code, fail_headers, fail_rows = call_api(
            "GET", "/api/v1/records", headers=auth_headers, query_string="status=Fail&search=MIST-TEST-02"
        )
        self.assertEqual(fail_code, 200)
        self.assertEqual(len(fail_rows), 5)
        self.assertEqual(fail_headers.get("x-filtered-count"), "5")
        self.assertEqual(fail_headers.get("x-total-count"), "25")
        self.assertTrue(all(r["efficacy_label"] == "Fail" and r["estop_triggered"] == 0 for r in fail_rows))

        # Verify /api/v1/stats breakdown sums cleanly
        stats_code, _, stats = call_api("GET", "/api/v1/stats", headers=auth_headers)
        self.assertEqual(stats_code, 200)
        self.assertEqual(stats["total_cycles"], 25)
        self.assertEqual(
            stats["pass_count"] + stats["marginal_count"] + stats["fail_count"] + stats["estop_count"],
            25,
        )

    def test_trigger_checks_mqtt_connection_and_publish(self) -> None:
        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}

        orig_is_connected = main.mqtt_client.is_connected
        orig_publish = main.mqtt_client.publish
        try:
            main.mqtt_client.is_connected = lambda: False
            disconn_code, _, _ = call_api(
                "POST",
                "/api/v1/trigger",
                headers=auth_headers,
                body={"duration_sec": 20, "room_volume_m3": 1.0},
            )
            self.assertEqual(disconn_code, 503)

            main.mqtt_client.is_connected = lambda: True
            main.mqtt_client.publish = lambda topic, payload: types.SimpleNamespace(rc=0)
            ok_code, _, ok_data = call_api(
                "POST",
                "/api/v1/trigger",
                headers=auth_headers,
                body={"duration_sec": 20, "room_volume_m3": 1.0},
            )
            self.assertEqual(ok_code, 200)
            self.assertEqual(ok_data["status"], "DISPATCHED")
            self.assertEqual(ok_data["triggered_by"], "admin")
        finally:
            main.mqtt_client.is_connected = orig_is_connected
            main.mqtt_client.publish = orig_publish

    def test_mqtt_deduplication_ignores_duplicate_within_window(self) -> None:
        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}

        raw = json.dumps({
            "station_id": "MIST-BOH-01",
            "ambient_temp_c": 24.0,
            "relative_humidity_pct": 40.0,
            "flow_rate_ml_min": 10.0,
            "cycle_duration_sec": 30.0,
            "room_volume_m3": 1.0,
            "delivery_confirmed": True,
            "estop_triggered": False,
        }).encode("utf-8")
        msg = types.SimpleNamespace(payload=raw)

        main._on_mqtt_message(None, None, msg)
        main._on_mqtt_message(None, None, msg)

        stats_code, _, stats = call_api("GET", "/api/v1/stats", headers=auth_headers)
        self.assertEqual(stats_code, 200)
        self.assertEqual(stats["total_cycles"], 1)

    def _assert_sha256_hex(self, value: str) -> None:
        self.assertIsInstance(value, str)
        self.assertRegex(value, r"^[0-9a-f]{64}$")

    def test_hash_chain_linkage_and_ledger_verification(self) -> None:
        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}
        genesis_hash = "0" * 64

        empty_verify_code, _, empty_verify = call_api("GET", "/api/v1/ledger/verify", headers=auth_headers)
        self.assertEqual(empty_verify_code, 200)
        self.assertTrue(empty_verify["verified"])
        self.assertEqual(empty_verify["checked_records"], 0)
        self.assertEqual(empty_verify["tampered_ids"], [])
        self.assertEqual(empty_verify["latest_hash"], genesis_hash)

        empty_stats_code, _, empty_stats = call_api("GET", "/api/v1/stats", headers=auth_headers)
        self.assertEqual(empty_stats_code, 200)
        self.assertIs(empty_stats["ledger_verified"], True)
        self.assertEqual(empty_stats["latest_hash"], genesis_hash)

        payloads = [
            {"station_id": "MIST-CHAIN-01", "ambient_temp_c": 25.0, "relative_humidity_pct": 65.0, "flow_rate_ml_min": 40.0, "cycle_duration_sec": 30.0, "room_volume_m3": 1.0},
            {"station_id": "MIST-CHAIN-01", "ambient_temp_c": 25.0, "relative_humidity_pct": 65.0, "flow_rate_ml_min": 30.0, "cycle_duration_sec": 45.0, "room_volume_m3": 10.0},
            {"station_id": "MIST-CHAIN-02", "ambient_temp_c": 0.0, "relative_humidity_pct": 0.0, "flow_rate_ml_min": 0.0, "cycle_duration_sec": 4.2, "room_volume_m3": 1.0, "delivery_confirmed": False, "estop_triggered": True},
        ]

        eval_results = []
        for payload in payloads:
            code, _, res = call_api("POST", "/api/v1/telemetry/evaluate", body=payload)
            self.assertEqual(code, 200)
            self._assert_sha256_hex(res["prev_hash"])
            self._assert_sha256_hex(res["record_hash"])
            eval_results.append(res)

        rec_code, _, rec_rows = call_api("GET", "/api/v1/records", headers=auth_headers, query_string="limit=10")
        self.assertEqual(rec_code, 200)
        rows_asc = sorted(rec_rows, key=lambda r: r["id"])
        self.assertEqual(len(rows_asc), 3)

        self.assertEqual(rows_asc[0]["prev_hash"], genesis_hash)
        for idx, row in enumerate(rows_asc):
            self._assert_sha256_hex(row["record_hash"])
            expected_prev = genesis_hash if idx == 0 else rows_asc[idx - 1]["record_hash"]
            self.assertEqual(row["prev_hash"], expected_prev)
            self.assertEqual(row["prev_hash"], eval_results[idx]["prev_hash"])
            self.assertEqual(row["record_hash"], eval_results[idx]["record_hash"])
            recomputed = main._compute_record_hash(
                prev_hash=row["prev_hash"],
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
            self.assertEqual(row["record_hash"], recomputed)

        verify_code, _, verify_data = call_api("GET", "/api/v1/ledger/verify", headers=auth_headers)
        self.assertEqual(verify_code, 200)
        self.assertIs(verify_data["verified"], True)
        self.assertEqual(verify_data["checked_records"], 3)
        self.assertEqual(verify_data["tampered_ids"], [])
        self.assertEqual(verify_data["latest_hash"], rows_asc[-1]["record_hash"])

        stats_code, _, stats = call_api("GET", "/api/v1/stats", headers=auth_headers)
        self.assertEqual(stats_code, 200)
        self.assertIs(stats["ledger_verified"], True)
        self.assertEqual(stats["latest_hash"], rows_asc[-1]["record_hash"])

    def test_ledger_verify_detects_sql_tampering(self) -> None:
        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}

        payloads = [
            {"station_id": "MIST-TAMP-01", "ambient_temp_c": 25.0, "relative_humidity_pct": 65.0, "flow_rate_ml_min": 40.0, "cycle_duration_sec": 30.0, "room_volume_m3": 1.0},
            {"station_id": "MIST-TAMP-01", "ambient_temp_c": 28.0, "relative_humidity_pct": 55.0, "flow_rate_ml_min": 18.0, "cycle_duration_sec": 20.0, "room_volume_m3": 10.0},
            {"station_id": "MIST-TAMP-01", "ambient_temp_c": 25.0, "relative_humidity_pct": 65.0, "flow_rate_ml_min": 35.0, "cycle_duration_sec": 45.0, "room_volume_m3": 5.0},
        ]
        inserted_ids = []
        for payload in payloads:
            code, _, res = call_api("POST", "/api/v1/telemetry/evaluate", body=payload)
            self.assertEqual(code, 200)
            inserted_ids.append(res["id"])

        target_id = inserted_ids[1]
        conn = sqlite3.connect(main.DB_PATH)
        try:
            conn.execute(
                "UPDATE cycle_records SET sei_score = ?, efficacy_label = ? WHERE id = ?",
                (9.999, "Pass", target_id),
            )
            conn.commit()
        finally:
            conn.close()

        verify_code, _, verify_data = call_api("GET", "/api/v1/ledger/verify", headers=auth_headers)
        self.assertEqual(verify_code, 200)
        self.assertIs(verify_data["verified"], False)
        self.assertEqual(verify_data["checked_records"], 3)
        self.assertIn(target_id, verify_data["tampered_ids"])
        self.assertEqual(verify_data["tampered_ids"], [target_id])

        stats_code, _, stats = call_api("GET", "/api/v1/stats", headers=auth_headers)
        self.assertEqual(stats_code, 200)
        self.assertIs(stats["ledger_verified"], False)

        # Re-running init_db must preserve existing hashes and never silently heal tampered rows
        main.init_db()
        reverify_code, _, reverify_data = call_api("GET", "/api/v1/ledger/verify", headers=auth_headers)
        self.assertEqual(reverify_code, 200)
        self.assertIs(reverify_data["verified"], False)
        self.assertIn(target_id, reverify_data["tampered_ids"])

    def test_init_db_migrates_and_backfills_legacy_rows(self) -> None:
        legacy_db_path = os.path.join(self.tmpdir.name, "legacy_sanmist.db")
        conn = sqlite3.connect(legacy_db_path)
        try:
            conn.execute("""
                CREATE TABLE cycle_records (
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
                    estop_triggered INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.execute(
                """
                INSERT INTO cycle_records (
                    timestamp, station_id, ambient_temp_c, relative_humidity_pct,
                    flow_rate_ml_min, cycle_duration_sec, room_volume_m3, sei_score,
                    predicted_class, efficacy_label, compliance_status, delivery_confirmed,
                    ml_confidence, estop_triggered
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("2026-11-10T01:00:00+00:00", "MIST-LEG-01", 25.0, 65.0, 40.0, 30.0, 1.0, 20.0, 2, "Pass", "AUDIT VERIFIED", 1, 99.5, 0),
            )
            conn.execute(
                """
                INSERT INTO cycle_records (
                    timestamp, station_id, ambient_temp_c, relative_humidity_pct,
                    flow_rate_ml_min, cycle_duration_sec, room_volume_m3, sei_score,
                    predicted_class, efficacy_label, compliance_status, delivery_confirmed,
                    ml_confidence, estop_triggered
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("2026-11-10T02:00:00+00:00", "MIST-LEG-01", 26.0, 60.0, 15.0, 20.0, 10.0, 0.5, 0, "Fail", "AUDIT VERIFIED", 1, 95.0, 0),
            )
            conn.commit()
        finally:
            conn.close()

        main.DB_PATH = legacy_db_path
        main.init_db()
        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}

        verify_code, _, verify_data = call_api("GET", "/api/v1/ledger/verify", headers=auth_headers)
        self.assertEqual(verify_code, 200)
        self.assertIs(verify_data["verified"], True)
        self.assertEqual(verify_data["checked_records"], 2)
        self.assertEqual(verify_data["tampered_ids"], [])

        rec_code, _, rec_rows = call_api("GET", "/api/v1/records", headers=auth_headers, query_string="limit=10")
        self.assertEqual(rec_code, 200)
        rows_asc = sorted(rec_rows, key=lambda r: r["id"])
        self.assertEqual(rows_asc[0]["prev_hash"], "0" * 64)
        self.assertEqual(rows_asc[1]["prev_hash"], rows_asc[0]["record_hash"])
        self.assertEqual(rows_asc[1]["compliance_status"], "REQUIRES ATTENTION")

    def test_mqtt_auth_rejects_forged_key_or_signature_and_signs_trigger(self) -> None:
        token = self._login_admin()
        auth_headers = {"Authorization": f"Bearer {token}"}

        base_payload = {
            "station_id": "MIST-BOH-01",
            "ambient_temp_c": 24.0,
            "relative_humidity_pct": 40.0,
            "flow_rate_ml_min": 10.0,
            "cycle_duration_sec": 30.0,
            "room_volume_m3": 1.0,
            "delivery_confirmed": True,
            "estop_triggered": False,
        }

        orig_require_auth = main.MQTT_REQUIRE_AUTH
        orig_is_connected = main.mqtt_client.is_connected
        orig_publish = main.mqtt_client.publish
        try:
            main.MQTT_REQUIRE_AUTH = True

            # 1. Unsigned payload must be rejected when SANMIST_MQTT_REQUIRE_AUTH is enabled
            main._on_mqtt_message(
                None,
                None,
                types.SimpleNamespace(
                    topic="sanmist/devices/MIST-BOH-01/telemetry",
                    payload=json.dumps(base_payload).encode("utf-8"),
                ),
            )
            # 2. Wrong auth_key must be rejected
            main._on_mqtt_message(
                None,
                None,
                types.SimpleNamespace(
                    topic="sanmist/devices/MIST-BOH-01/telemetry",
                    payload=json.dumps({**base_payload, "auth_key": "wrong-key"}).encode("utf-8"),
                ),
            )
            # 3. Forged HMAC signature must be rejected
            main._on_mqtt_message(
                None,
                None,
                types.SimpleNamespace(
                    topic="sanmist/devices/MIST-BOH-01/telemetry",
                    payload=json.dumps({**base_payload, "signature": "0" * 64}).encode("utf-8"),
                ),
            )

            _, _, stats_after_rejects = call_api("GET", "/api/v1/stats", headers=auth_headers)
            self.assertEqual(stats_after_rejects["total_cycles"], 0)

            # 4. Valid HMAC signature must be accepted and recorded
            valid_sig = main._compute_mqtt_signature(base_payload, main.MQTT_DEVICE_KEY)
            main._on_mqtt_message(
                None,
                None,
                types.SimpleNamespace(
                    topic="sanmist/devices/MIST-BOH-01/telemetry",
                    payload=json.dumps({**base_payload, "signature": valid_sig}).encode("utf-8"),
                ),
            )
            _, _, stats_after_sig = call_api("GET", "/api/v1/stats", headers=auth_headers)
            self.assertEqual(stats_after_sig["total_cycles"], 1)

            # 5. Valid device auth_key must also be accepted for edge status/telemetry
            keyed_payload = {**base_payload, "cycle_duration_sec": 45.0, "auth_key": main.MQTT_DEVICE_KEY}
            main._on_mqtt_message(
                None,
                None,
                types.SimpleNamespace(
                    topic="sanmist/devices/MIST-BOH-01/telemetry",
                    payload=json.dumps(keyed_payload).encode("utf-8"),
                ),
            )
            _, _, stats_after_key = call_api("GET", "/api/v1/stats", headers=auth_headers)
            self.assertEqual(stats_after_key["total_cycles"], 2)

            # 6. Outbound remote trigger must include auth_key + verifiable HMAC signature
            published = {}

            def _fake_publish(topic, payload):
                published["topic"] = topic
                published["payload"] = json.loads(payload)
                return types.SimpleNamespace(rc=0)

            main.mqtt_client.is_connected = lambda: True
            main.mqtt_client.publish = _fake_publish
            trig_code, _, _ = call_api(
                "POST",
                "/api/v1/trigger",
                headers=auth_headers,
                body={"duration_sec": 32, "room_volume_m3": 1.0},
            )
            self.assertEqual(trig_code, 200)
            self.assertEqual(published["payload"]["auth_key"], main.MQTT_DEVICE_KEY)
            self.assertEqual(
                published["payload"]["signature"],
                main._compute_mqtt_signature(published["payload"], main.MQTT_DEVICE_KEY),
            )
            self.assertTrue(main._verify_mqtt_auth(published["payload"]))
        finally:
            main.MQTT_REQUIRE_AUTH = orig_require_auth
            main.mqtt_client.is_connected = orig_is_connected
            main.mqtt_client.publish = orig_publish


if __name__ == "__main__":
    unittest.main(verbosity=2)
