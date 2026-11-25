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
            code, _, res = call_api(
                "POST", "/api/v1/telemetry/evaluate", body=payload, headers=auth_headers
            )
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
            "auth_key": main.MQTT_DEVICE_KEY,
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
            code, _, res = call_api(
                "POST", "/api/v1/telemetry/evaluate", body=payload, headers=auth_headers
            )
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
            code, _, res = call_api(
                "POST", "/api/v1/telemetry/evaluate", body=payload, headers=auth_headers
            )
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


class SanMistSecurityTests(unittest.TestCase):
    def test_mqtt_security_config_defaults_to_tls_8883_and_required_auth(self) -> None:
        from backend.app.security import load_mqtt_security_config

        cfg = load_mqtt_security_config({})
        self.assertEqual(cfg.host, "broker.emqx.io")
        self.assertTrue(cfg.use_tls)
        self.assertEqual(cfg.port, 8883)
        self.assertFalse(cfg.tls_insecure)
        self.assertTrue(cfg.require_payload_auth)

        plain_cfg = load_mqtt_security_config({"SANMIST_MQTT_TLS": "0"})
        self.assertFalse(plain_cfg.use_tls)
        self.assertEqual(plain_cfg.port, 1883)

    def test_configure_mqtt_client_applies_tls_credentials_and_reconnect_backoff(self) -> None:
        import ssl
        from backend.app.security import configure_mqtt_client_security, load_mqtt_security_config

        calls: dict[str, object] = {}

        class FakeMqttClient:
            def username_pw_set(self, username, password=None):
                calls["credentials"] = (username, password)

            def tls_set_context(self, context):
                calls["tls_context"] = context

            def reconnect_delay_set(self, min_delay=1, max_delay=120):
                calls["reconnect_delay"] = (min_delay, max_delay)

        cfg = load_mqtt_security_config({
            "SANMIST_MQTT_BROKER": "mosquitto",
            "SANMIST_MQTT_PORT": "8883",
            "SANMIST_MQTT_TLS": "1",
            "SANMIST_MQTT_USERNAME": "sanmist_svc",
            "SANMIST_MQTT_PASSWORD": "secret-password",
        })
        client = FakeMqttClient()
        meta = configure_mqtt_client_security(client, cfg)

        self.assertTrue(meta["tls_enabled"])
        self.assertTrue(meta["credentials_configured"])
        self.assertEqual(calls["credentials"], ("sanmist_svc", "secret-password"))
        self.assertEqual(calls["reconnect_delay"], (1, 30))
        ctx = calls["tls_context"]
        self.assertIsInstance(ctx, ssl.SSLContext)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(ctx.check_hostname)
        self.assertEqual(ctx.minimum_version, ssl.TLSVersion.TLSv1_2)

    def test_configure_mqtt_client_fails_closed_when_ca_cert_missing(self) -> None:
        from backend.app.security import configure_mqtt_client_security, load_mqtt_security_config

        class FakeMqttClient:
            def username_pw_set(self, username, password=None):
                pass

            def tls_set_context(self, context):
                pass

            def reconnect_delay_set(self, min_delay=1, max_delay=120):
                pass

        cfg = load_mqtt_security_config({
            "SANMIST_MQTT_TLS": "1",
            "SANMIST_MQTT_CA_CERT": "/nonexistent/path/ca.crt",
        })
        with self.assertRaises(FileNotFoundError):
            configure_mqtt_client_security(FakeMqttClient(), cfg)

    def test_parse_cors_origins_defaults_away_from_wildcard(self) -> None:
        from backend.app.security import parse_cors_origins

        defaults = parse_cors_origins(None)
        self.assertNotIn("*", defaults)
        self.assertIn("http://localhost:3000", defaults)
        self.assertIn("http://127.0.0.1:3000", defaults)

        custom = parse_cors_origins("https://sanmist.example.com, https://admin.example.com ")
        self.assertEqual(custom, ["https://sanmist.example.com", "https://admin.example.com"])

    def test_mosquitto_and_compose_enforce_tls_and_disable_anonymous(self) -> None:
        mosq_conf_path = os.path.join(PROJECT_ROOT, "mosquitto", "config", "mosquitto.conf")
        compose_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
        with open(mosq_conf_path, "r", encoding="utf-8") as f:
            mosq_conf = f.read()
        with open(compose_path, "r", encoding="utf-8") as f:
            compose_yml = f.read()

        self.assertIn("listener 8883", mosq_conf)
        self.assertIn("allow_anonymous false", mosq_conf)
        self.assertIn("password_file", mosq_conf)
        self.assertIn("tls_version tlsv1.2", mosq_conf)
        self.assertNotIn("allow_anonymous true", mosq_conf)
        self.assertIn('SANMIST_MQTT_TLS: "1"', compose_yml)
        self.assertIn('SANMIST_MQTT_PORT: "8883"', compose_yml)
        self.assertIn("SANMIST_MQTT_CA_CERT: /mosquitto/certs/ca.crt", compose_yml)


class SanMistRouteAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.orig_db_path = main.DB_PATH
        main.DB_PATH = os.path.join(self.tmpdir.name, "test_auth.db")
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

    def _create_operator_session(self, username: str = "op1") -> dict[str, str]:
        pwd_hash, salt_hex = main._hash_password("operator-secret")
        conn = sqlite3.connect(main.DB_PATH)
        try:
            conn.execute(
                "INSERT INTO users (username, password_hash, salt, full_name, role, created_at) VALUES (?, ?, ?, ?, ?, datetime('now'))",
                (username, pwd_hash, salt_hex, "Kitchen Operator", "Operator"),
            )
            conn.commit()
        finally:
            conn.close()
        code, _, data = call_api(
            "POST",
            "/api/v1/auth/login",
            body={"username": username, "password": "operator-secret"},
        )
        self.assertEqual(code, 200)
        return {"Authorization": f"Bearer {data['access_token']}"}

    def test_telemetry_evaluate_and_estop_require_auth(self) -> None:
        sample = {
            "station_id": "MIST-BOH-01",
            "ambient_temp_c": 26.0,
            "relative_humidity_pct": 65.0,
            "flow_rate_ml_min": 40.0,
            "cycle_duration_sec": 30.0,
            "room_volume_m3": 1.0,
        }
        eval_code, _, _ = call_api("POST", "/api/v1/telemetry/evaluate", body=sample)
        bad_key_code, _, _ = call_api(
            "POST",
            "/api/v1/telemetry/evaluate",
            body=sample,
            headers={"X-Device-Key": "wrong-device-key"},
        )
        estop_code, _, _ = call_api(
            "POST", "/api/v1/control/estop", body={"station_id": "MIST-BOH-01"}
        )

        self.assertEqual(eval_code, 401)
        self.assertEqual(bad_key_code, 401)
        self.assertEqual(estop_code, 401)

        # Valid X-Device-Key succeeds on telemetry/evaluate
        ok_device_code, _, ok_device = call_api(
            "POST",
            "/api/v1/telemetry/evaluate",
            body=sample,
            headers={"X-Device-Key": main._get_device_auth_key("MIST-BOH-01")},
        )
        self.assertEqual(ok_device_code, 200)
        self.assertEqual(ok_device["station_id"], "MIST-BOH-01")

        # Valid Bearer token succeeds on telemetry/evaluate and control/estop
        admin_headers = {"Authorization": f"Bearer {self._login_admin()}"}
        ok_bearer_code, _, _ = call_api(
            "POST", "/api/v1/telemetry/evaluate", body=sample, headers=admin_headers
        )
        ok_estop_code, _, ok_estop = call_api(
            "POST",
            "/api/v1/control/estop",
            body={"station_id": "MIST-BOH-01"},
            headers=admin_headers,
        )
        self.assertEqual(ok_bearer_code, 200)
        self.assertEqual(ok_estop_code, 200)
        self.assertEqual(ok_estop["status"], "HALTED")

    def test_operator_cannot_access_unowned_devices_or_schedules(self) -> None:
        op_headers = self._create_operator_session("op_isolated")

        other_devs_code, _, _ = call_api(
            "GET", "/api/v1/user/admin/devices", headers=op_headers
        )
        save_sched_code, _, _ = call_api(
            "POST",
            "/api/v1/schedules",
            headers=op_headers,
            body={
                "serial_number": "MIST-BOH-01",
                "days_of_week": "MON,TUE",
                "start_time_pht": "22:00:00",
                "duration_sec": 30,
                "room_volume_m3": 1.0,
            },
        )
        get_sched_code, _, _ = call_api(
            "GET", "/api/v1/schedules/MIST-BOH-01", headers=op_headers
        )
        estop_code, _, _ = call_api(
            "POST",
            "/api/v1/control/estop",
            headers=op_headers,
            body={"station_id": "MIST-BOH-01"},
        )

        self.assertEqual(other_devs_code, 403)
        self.assertEqual(save_sched_code, 403)
        self.assertEqual(get_sched_code, 403)
        self.assertEqual(estop_code, 403)

    def test_security_headers_and_tls_metadata_exposed(self) -> None:
        code, headers, health = call_api(
            "GET", "/health", headers={"X-Forwarded-Proto": "https"}
        )
        self.assertEqual(code, 200)
        self.assertEqual(headers.get("x-content-type-options"), "nosniff")
        self.assertEqual(headers.get("x-frame-options"), "DENY")
        self.assertIn("strict-transport-security", headers)
        self.assertIn("mqtt_tls_enabled", health)
        self.assertIn("mqtt_broker_host", health)
        self.assertIn("mqtt_broker_port", health)

        admin_headers = {"Authorization": f"Bearer {self._login_admin()}"}
        stats_code, _, stats = call_api("GET", "/api/v1/stats", headers=admin_headers)
        self.assertEqual(stats_code, 200)
        self.assertIn("mqtt_tls_enabled", stats)
        self.assertIn("mqtt_broker_host", stats)
        self.assertIn("mqtt_broker_port", stats)

    def test_hocl_low_status_updates_stats_and_blocks_manual_and_scheduled_triggers(self) -> None:
        admin_headers = {"Authorization": f"Bearer {self._login_admin()}"}
        orig_is_connected = main.mqtt_client.is_connected
        orig_publish = main.mqtt_client.publish
        orig_telemetry = dict(main.live_station_telemetry.get("MIST-BOH-01", {}))
        published: list[tuple[str, str]] = []

        class _FakeMsg:
            def __init__(self, topic: str, payload: dict) -> None:
                self.topic = topic
                self.payload = json.dumps(payload).encode("utf-8")

        class _FakePubInfo:
            rc = main.mqtt.MQTT_ERR_SUCCESS

        try:
            main.mqtt_client.is_connected = lambda: True

            def fake_publish(topic, payload):
                published.append((topic, payload))
                return _FakePubInfo()

            main.mqtt_client.publish = fake_publish

            device_key = main._get_device_auth_key("MIST-BOH-01")
            main._on_mqtt_message(
                None,
                None,
                _FakeMsg(
                    "sanmist/devices/MIST-BOH-01/status",
                    {
                        "station_id": "MIST-BOH-01",
                        "ambient_temp_c": 26.0,
                        "relative_humidity_pct": 62.0,
                        "flow_rate_ml_min": 30.0,
                        "hocl_low_level": True,
                        "cycle_in_progress": False,
                        "auth_key": device_key,
                    },
                ),
            )

            stats_code, _, stats = call_api(
                "GET",
                "/api/v1/stats",
                headers=admin_headers,
                query_string="station_id=MIST-BOH-01",
            )
            self.assertEqual(stats_code, 200)
            self.assertTrue(stats["live_telemetry"]["hocl_low_level"])
            self.assertEqual(stats["fleet_health"]["low_hocl_count"], 1)
            boh_entry = next(
                d for d in stats["fleet_health"]["devices"] if d["serial_number"] == "MIST-BOH-01"
            )
            self.assertEqual(boh_entry["status"], "LOW_HOCL")

            trig_code, _, trig_err = call_api(
                "POST",
                "/api/v1/trigger",
                headers=admin_headers,
                body={"station_id": "MIST-BOH-01", "duration_sec": 30, "room_volume_m3": 1.0},
            )
            self.assertEqual(trig_code, 409)
            self.assertIn("depleted", trig_err["detail"].lower())
            self.assertFalse(main._dispatch_scheduled_cycle("MIST-BOH-01", 30, 1.0))
            self.assertEqual(len(published), 0)

            main._on_mqtt_message(
                None,
                None,
                _FakeMsg(
                    "sanmist/devices/MIST-BOH-01/status",
                    {
                        "station_id": "MIST-BOH-01",
                        "ambient_temp_c": 26.0,
                        "relative_humidity_pct": 62.0,
                        "flow_rate_ml_min": 30.0,
                        "hocl_low_level": False,
                        "cycle_in_progress": False,
                        "auth_key": device_key,
                    },
                ),
            )

            restored_stats_code, _, restored_stats = call_api(
                "GET",
                "/api/v1/stats",
                headers=admin_headers,
                query_string="station_id=MIST-BOH-01",
            )
            self.assertEqual(restored_stats_code, 200)
            self.assertFalse(restored_stats["live_telemetry"]["hocl_low_level"])
            self.assertEqual(restored_stats["fleet_health"]["low_hocl_count"], 0)

            ok_code, _, ok_data = call_api(
                "POST",
                "/api/v1/trigger",
                headers=admin_headers,
                body={"station_id": "MIST-BOH-01", "duration_sec": 30, "room_volume_m3": 1.0},
            )
            self.assertEqual(ok_code, 200)
            self.assertEqual(ok_data["status"], "DISPATCHED")
            self.assertEqual(len(published), 1)
        finally:
            main.mqtt_client.is_connected = orig_is_connected
            main.mqtt_client.publish = orig_publish
            if orig_telemetry:
                main.live_station_telemetry["MIST-BOH-01"] = orig_telemetry
            else:
                main.live_station_telemetry.setdefault("MIST-BOH-01", {})["hocl_low_level"] = False

    def test_hocl_reservoir_subtracts_per_cycle_trips_low_and_refills(self) -> None:
        admin_headers = {"Authorization": f"Bearer {self._login_admin()}"}
        orig_is_connected = main.mqtt_client.is_connected
        orig_publish = main.mqtt_client.publish
        orig_telemetry = dict(main.live_station_telemetry.get("MIST-BOH-01", {}))
        published: list[tuple[str, str]] = []

        class _FakePubInfo:
            rc = main.mqtt.MQTT_ERR_SUCCESS

        try:
            main.mqtt_client.is_connected = lambda: True

            def fake_publish(topic, payload):
                published.append((topic, payload))
                return _FakePubInfo()

            main.mqtt_client.publish = fake_publish

            # 1. Initial stats start at full 500.0 mL reservoir
            init_code, _, init_stats = call_api(
                "GET", "/api/v1/stats", headers=admin_headers, query_string="station_id=MIST-BOH-01"
            )
            self.assertEqual(init_code, 200)
            self.assertEqual(init_stats["live_telemetry"]["hocl_remaining_ml"], 500.0)
            self.assertEqual(init_stats["live_telemetry"]["hocl_capacity_ml"], 500.0)
            self.assertFalse(init_stats["live_telemetry"]["hocl_low_level"])

            # 2. A 30s cycle at 30 mL/min consumes 15.0 mL -> 485.0 mL remaining in stats & DB
            device_key = main._get_device_auth_key("MIST-BOH-01")
            ev1_code, _, ev1 = call_api(
                "POST",
                "/api/v1/telemetry/evaluate",
                headers={"X-Device-Key": device_key},
                body={
                    "station_id": "MIST-BOH-01",
                    "ambient_temp_c": 25.0,
                    "relative_humidity_pct": 65.0,
                    "flow_rate_ml_min": 30.0,
                    "cycle_duration_sec": 30.0,
                    "room_volume_m3": 1.0,
                },
            )
            self.assertEqual(ev1_code, 200)
            self.assertEqual(ev1["hocl_consumed_ml"], 15.0)
            self.assertEqual(ev1["hocl_remaining_ml"], 485.0)

            s1_code, _, s1 = call_api(
                "GET", "/api/v1/stats", headers=admin_headers, query_string="station_id=MIST-BOH-01"
            )
            self.assertEqual(s1_code, 200)
            self.assertEqual(s1["live_telemetry"]["hocl_remaining_ml"], 485.0)
            self.assertFalse(s1["live_telemetry"]["hocl_low_level"])

            conn = sqlite3.connect(main.DB_PATH)
            try:
                db_rem = conn.execute(
                    "SELECT hocl_remaining_ml FROM devices WHERE serial_number = 'MIST-BOH-01'"
                ).fetchone()[0]
                self.assertEqual(db_rem, 485.0)
            finally:
                conn.close()

            # 3. Dropping below 50 mL threshold sets hocl_low_level=True and blocks triggers (409)
            ev2_code, _, ev2 = call_api(
                "POST",
                "/api/v1/telemetry/evaluate",
                headers={"X-Device-Key": device_key},
                body={
                    "station_id": "MIST-BOH-01",
                    "ambient_temp_c": 25.0,
                    "relative_humidity_pct": 65.0,
                    "flow_rate_ml_min": 30.0,
                    "cycle_duration_sec": 30.0,
                    "room_volume_m3": 1.0,
                    "hocl_remaining_ml": 40.0,
                    "hocl_capacity_ml": 500.0,
                },
            )
            self.assertEqual(ev2_code, 200)
            self.assertEqual(ev2["hocl_remaining_ml"], 40.0)

            s2_code, _, s2 = call_api(
                "GET", "/api/v1/stats", headers=admin_headers, query_string="station_id=MIST-BOH-01"
            )
            self.assertEqual(s2_code, 200)
            self.assertTrue(s2["live_telemetry"]["hocl_low_level"])
            self.assertEqual(s2["fleet_health"]["low_hocl_count"], 1)

            blocked_code, _, _ = call_api(
                "POST",
                "/api/v1/trigger",
                headers=admin_headers,
                body={"station_id": "MIST-BOH-01", "duration_sec": 30, "room_volume_m3": 1.0},
            )
            self.assertEqual(blocked_code, 409)
            self.assertEqual(len(published), 0)

            # 4. Refill endpoint resets reservoir to 500.0 mL, notifies ESP32 via MQTT, and unlocks trigger
            refill_code, _, refill_data = call_api(
                "POST", "/api/v1/devices/MIST-BOH-01/refill", headers=admin_headers
            )
            self.assertEqual(refill_code, 200)
            self.assertEqual(refill_data["status"], "REFILLED")
            self.assertEqual(refill_data["hocl_remaining_ml"], 500.0)
            self.assertEqual(len(published), 1)
            refill_msg = json.loads(published[0][1])
            self.assertEqual(refill_msg["command"], "REFILL")
            self.assertEqual(refill_msg["hocl_remaining_ml"], 500.0)
            self.assertTrue(main._verify_mqtt_auth(refill_msg))

            s3_code, _, s3 = call_api(
                "GET", "/api/v1/stats", headers=admin_headers, query_string="station_id=MIST-BOH-01"
            )
            self.assertEqual(s3_code, 200)
            self.assertEqual(s3["live_telemetry"]["hocl_remaining_ml"], 500.0)
            self.assertFalse(s3["live_telemetry"]["hocl_low_level"])
            self.assertEqual(s3["fleet_health"]["low_hocl_count"], 0)

            ok_code, _, ok_data = call_api(
                "POST",
                "/api/v1/trigger",
                headers=admin_headers,
                body={"station_id": "MIST-BOH-01", "duration_sec": 30, "room_volume_m3": 1.0},
            )
            self.assertEqual(ok_code, 200)
            self.assertEqual(ok_data["status"], "DISPATCHED")
        finally:
            main.mqtt_client.is_connected = orig_is_connected
            main.mqtt_client.publish = orig_publish
            if orig_telemetry:
                main.live_station_telemetry["MIST-BOH-01"] = orig_telemetry
            else:
                main.live_station_telemetry.setdefault("MIST-BOH-01", {})["hocl_low_level"] = False


class SanMistSchedulerAndExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.orig_db_path = main.DB_PATH
        main.DB_PATH = os.path.join(self.tmpdir.name, "test_sched_export.db")
        main.init_db()

    def tearDown(self) -> None:
        main.DB_PATH = self.orig_db_path
        self.tmpdir.cleanup()

    def _login_admin(self) -> dict[str, str]:
        code, _, data = call_api(
            "POST",
            "/api/v1/auth/login",
            body={"username": "admin", "password": os.getenv("SANMIST_ADMIN_PASSWORD", "sanmist2026")},
        )
        self.assertEqual(code, 200)
        return {"Authorization": f"Bearer {data['access_token']}"}

    def test_scheduler_executes_matching_pht_schedule_once_and_recovers_after_disconnect(self) -> None:
        from datetime import datetime, timezone
        from backend.app.scheduler import run_schedule_tick

        admin_headers = self._login_admin()
        save_code, _, _ = call_api(
            "POST",
            "/api/v1/schedules",
            headers=admin_headers,
            body={
                "serial_number": "MIST-BOH-01",
                "days_of_week": "MON, TUE, WED, THU, FRI",
                "start_time_pht": "23:00:00",
                "duration_sec": 45,
                "room_volume_m3": 2.5,
                "dosage_ml": 12.0,
            },
        )
        self.assertEqual(save_code, 200)

        dispatched: list[dict] = []
        broker_online = False

        def fake_dispatch(
            station_id: str,
            duration_sec: int,
            room_volume_m3: float,
            trigger_type: str,
            triggered_by: str,
        ) -> bool:
            if not broker_online:
                return False
            dispatched.append({
                "station_id": station_id,
                "duration_sec": duration_sec,
                "room_volume_m3": room_volume_m3,
                "trigger_type": trigger_type,
                "triggered_by": triggered_by,
            })
            return True

        # Tuesday 2026-11-24 23:00 PHT == 2026-11-24 15:00 UTC
        tue_2300_utc = datetime(2026, 11, 24, 15, 0, 5, tzinfo=timezone.utc)
        tue_2300_later_utc = datetime(2026, 11, 24, 15, 0, 35, tzinfo=timezone.utc)
        tue_2259_utc = datetime(2026, 11, 24, 14, 59, 0, tzinfo=timezone.utc)
        sat_2300_utc = datetime(2026, 11, 28, 15, 0, 5, tzinfo=timezone.utc)

        # 1. Wrong time or wrong day never dispatches
        broker_online = True
        self.assertEqual(run_schedule_tick(main.get_db_connection, fake_dispatch, now_utc=tue_2259_utc), [])
        self.assertEqual(run_schedule_tick(main.get_db_connection, fake_dispatch, now_utc=sat_2300_utc), [])
        self.assertEqual(len(dispatched), 0)

        # 2. Matching slot while broker offline fails cleanly without burning the slot
        broker_online = False
        self.assertEqual(run_schedule_tick(main.get_db_connection, fake_dispatch, now_utc=tue_2300_utc), [])
        self.assertEqual(len(dispatched), 0)

        # 3. Broker recovers within same minute -> schedule dispatches once
        broker_online = True
        fired = run_schedule_tick(main.get_db_connection, fake_dispatch, now_utc=tue_2300_later_utc)
        self.assertEqual(len(fired), 1)
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(dispatched[0]["station_id"], "MIST-BOH-01")
        self.assertEqual(dispatched[0]["duration_sec"], 45)
        self.assertEqual(dispatched[0]["room_volume_m3"], 2.5)
        self.assertEqual(dispatched[0]["trigger_type"], "SCHEDULED")

        # 4. Subsequent tick in same slot does NOT double-trigger
        self.assertEqual(run_schedule_tick(main.get_db_connection, fake_dispatch, now_utc=tue_2300_later_utc), [])
        self.assertEqual(len(dispatched), 1)

        # 5. GET schedule exposes last_triggered_at metadata
        get_code, _, sched = call_api("GET", "/api/v1/schedules/MIST-BOH-01", headers=admin_headers)
        self.assertEqual(get_code, 200)
        self.assertIsNotNone(sched.get("last_triggered_at"))

    def test_records_pagination_beyond_1000_and_since_filter(self) -> None:
        admin_headers = self._login_admin()

        conn = sqlite3.connect(main.DB_PATH)
        try:
            rows = []
            # 150 older rows from 2026-09-01 + 1100 recent rows from 2026-11-20
            for i in range(150):
                rows.append((
                    f"2026-09-01T00:00:{i % 60:02d}+00:00",
                    "MIST-BOH-01",
                    "MANUAL_WEB",
                    25.0,
                    65.0,
                    35.0,
                    30.0,
                    1.0,
                    17.5,
                    2,
                    "Pass",
                    "AUDIT VERIFIED",
                    1,
                    99.0,
                    0,
                    "0" * 64,
                    "a" * 64,
                ))
            for i in range(1100):
                rows.append((
                    f"2026-11-20T{(i // 60) % 24:02d}:{i % 60:02d}:00+00:00",
                    "MIST-BOH-01",
                    "SCHEDULED",
                    25.0,
                    65.0,
                    35.0,
                    30.0,
                    1.0,
                    17.5,
                    2,
                    "Pass",
                    "AUDIT VERIFIED",
                    1,
                    99.0,
                    0,
                    "0" * 64,
                    "b" * 64,
                ))
            conn.executemany(
                """
                INSERT INTO cycle_records (
                    timestamp, station_id, trigger_type, ambient_temp_c, relative_humidity_pct,
                    flow_rate_ml_min, cycle_duration_sec, room_volume_m3, sei_score,
                    predicted_class, efficacy_label, compliance_status, delivery_confirmed,
                    ml_confidence, estop_triggered, prev_hash, record_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
        finally:
            conn.close()

        # Paginate all 1,250 records in chunks of 500 (no 1,000-record cap)
        collected_ids: set[int] = set()
        offset = 0
        while True:
            code, hdrs, batch = call_api(
                "GET",
                "/api/v1/records",
                headers=admin_headers,
                query_string=f"limit=500&offset={offset}",
            )
            self.assertEqual(code, 200)
            for item in batch:
                collected_ids.add(item["id"])
            if len(batch) < 500 or offset + len(batch) >= int(hdrs["x-filtered-count"]):
                break
            offset += len(batch)

        self.assertEqual(len(collected_ids), 1250)

        # Server-side `since` filter returns only the 1,100 recent records
        since_code, since_hdrs, since_page1 = call_api(
            "GET",
            "/api/v1/records",
            headers=admin_headers,
            query_string="limit=500&offset=0&since=2026-11-01T00:00:00Z",
        )
        self.assertEqual(since_code, 200)
        self.assertEqual(int(since_hdrs["x-filtered-count"]), 1100)
        self.assertEqual(int(since_hdrs["x-total-count"]), 1250)
        self.assertEqual(len(since_page1), 500)


if __name__ == "__main__":
    unittest.main(verbosity=2)
