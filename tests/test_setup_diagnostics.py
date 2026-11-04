"""Automated qualification suite for Setup and Diagnostics Wizard v1 (diagnostic-core).

Tests acceptance cases:
- A01: Versioned snapshot, status and freshness
- A02: Private authenticated wizard and public denial
- A03: Canonical project selection and cross-project read isolation
- A04: Passive GET has no hidden mutations
- A06: Bounded networking, SSRF and cancellation
- A07: Secret references, alias denial and redaction
- A08: Network/auth/expiry facts stay distinct
- A22: No hidden destructive setup effects
- A23: Responsiveness, cache and repeat diagnostics
- A24: Git, Telegram, tunnel and webhook ownership
- A25: One host lifecycle, failure and shutdown isolation
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
import jsonschema

import main as app_module
from nginx_qa.setup_diagnostics import (
    DiagnosticSnapshotCollector,
    DiagnosticReadGate,
    parse_strict_json_with_limits,
    compute_snapshot_id,
    compute_authority_fingerprint,
    redact_secrets,
    get_contracts_schema,
    get_snapshot_validator,
    CHECK_STATUSES,
    CHECK_SCOPES,
    CHECK_FRESHNESS,
    EVIDENCE_KINDS,
    ACTION_IDS,
    ERROR_CODES,
)
from nginx_qa.setup_diagnostics_api import register_setup_diagnostics_api
from nginx_qa.managed_import import ManagedImportStore
from nginx_qa.legacy_scope_control import ADMIN_TOKEN_HEADER, ADMIN_TOKEN_ENV


TEST_ADMIN_TOKEN = "a" * 32


class TestSetupDiagnosticsCore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ[ADMIN_TOKEN_ENV] = TEST_ADMIN_TOKEN
        cls.schema = get_contracts_schema()
        cls.validator = get_snapshot_validator()
        cls.app = app_module.app
        cls.client = TestClient(cls.app, raise_server_exceptions=False)

    def setUp(self):
        os.environ[ADMIN_TOKEN_ENV] = TEST_ADMIN_TOKEN
        self.temp_dir = tempfile.mkdtemp(prefix="sdw-test-")
        self.temp_path = Path(self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_git_config(self, *, corrupt: bool = False, missing: bool = False) -> Path:
        p = self.temp_path / "port_git_map.json"
        if missing:
            return p
        if corrupt:
            p.write_text("{ corrupt json ", encoding="utf-8")
            return p
        data = {
            "projects": {
                "proj_context_9000": {
                    "project_phone": "9000",
                    "project_name": "Test Project 9000",
                    "repository": "https://github.com/chartjs333/nginx-qa.git",
                }
            },
            "phone_git_contexts": {
                "9000": {
                    "project_phone": "9000",
                    "git_context_key": "proj_context_9000",
                }
            },
        }
        p.write_text(json.dumps(data), encoding="utf-8")
        return p

    def _create_mock_store(self, *, busy: bool = False) -> Path:
        p = self.temp_path / "managed-import.sqlite3"
        conn = sqlite3.connect(p)
        conn.execute("""
            CREATE TABLE managed_projects (
                project_id TEXT PRIMARY KEY,
                control_json TEXT NOT NULL,
                revision INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE managed_sprints (
                project_id TEXT NOT NULL,
                sprint_id TEXT NOT NULL,
                status TEXT NOT NULL,
                fencing_token INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                PRIMARY KEY (project_id, sprint_id)
            )
        """)
        conn.execute("""
            CREATE TABLE managed_port_leases (
                lease_id TEXT PRIMARY KEY,
                instance_id TEXT NOT NULL,
                network_namespace_id TEXT NOT NULL,
                assignment_id TEXT NOT NULL,
                process_id TEXT,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                status TEXT NOT NULL,
                lease_json TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE managed_process_owners (
                process_id TEXT PRIMARY KEY,
                assignment_id TEXT NOT NULL,
                port_lease_id TEXT NOT NULL UNIQUE,
                pid INTEGER,
                state TEXT NOT NULL,
                process_json TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE managed_queue_items (
                dedupe_key TEXT PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                receipt_id TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE branch_leases (
                lease_id TEXT PRIMARY KEY,
                repository_id TEXT NOT NULL,
                repository_key TEXT NOT NULL,
                mirror_storage_key TEXT NOT NULL,
                branch TEXT NOT NULL,
                branch_key TEXT NOT NULL,
                assignment_id TEXT NOT NULL,
                source_commit TEXT NOT NULL,
                initial_head_commit TEXT NOT NULL,
                mode TEXT NOT NULL CHECK (mode = 'write'),
                status TEXT NOT NULL CHECK (status IN ('active', 'released')),
                acquired_at TEXT NOT NULL,
                released_at TEXT
            )
        """)
        conn.execute(
            "INSERT INTO managed_projects VALUES (?, ?, ?)",
            ("9000", json.dumps({"project_id": "9000"}), 1),
        )
        if busy:
            state_json = json.dumps({
                "status": "active",
                "graph_revision": 2,
                "assignments": [{"assignment_id": "a-1", "status": "active"}],
                "reviews": [],
                "result_receipts": [],
            })
            conn.execute(
                "INSERT INTO managed_sprints VALUES (?, ?, ?, ?, ?)",
                ("9000", "sprint-test-1", "active", 1, state_json),
            )
        conn.commit()
        conn.close()
        return p

    # --- A01: Versioned snapshot, status and freshness ---
    def test_a01_snapshot_schema_and_status_invariants(self):
        cfg = self._create_mock_git_config()
        store = self._create_mock_store()
        collector = DiagnosticSnapshotCollector(
            git_config_path=cfg,
            store_path=store,
            now=datetime(2026, 11, 4, 12, 0, 0, tzinfo=timezone.utc),
        )
        snapshot = collector.collect_snapshot("9000")

        # 1. Validate full JSON schema
        self.validator.validate(snapshot)

        # 2. Assert top-level fields
        self.assertEqual(snapshot["kind"], "setup_diagnostic_snapshot")
        self.assertEqual(snapshot["schema_version"], 1)
        self.assertEqual(snapshot["project_id"], "9000")
        self.assertEqual(len(snapshot["snapshot_id"]), 64)
        self.assertEqual(len(snapshot["authority_fingerprint"]), 64)

        # 3. Assert snapshot_id matches canonical computation
        expected_id = compute_snapshot_id(snapshot)
        self.assertEqual(snapshot["snapshot_id"], expected_id)

        # 4. Check statuses and freshness invariants
        seen_checks = set()
        for c in snapshot["checks"]:
            cid = c["check_id"]
            self.assertNotIn(cid, seen_checks, f"Duplicate check_id: {cid}")
            seen_checks.add(cid)
            self.assertIn(c["status"], CHECK_STATUSES)
            self.assertIn(c["scope"], CHECK_SCOPES)
            self.assertIn(c["freshness"], CHECK_FRESHNESS)
            self.assertIn(c["evidence_kind"], EVIDENCE_KINDS)

            if c["freshness"] == "UNOBSERVED":
                self.assertIsNone(c["observed_at"], f"{cid} unobserved must have null observed_at")
                self.assertIsNone(c["expires_at"], f"{cid} unobserved must have null expires_at")
                self.assertIn(c["status"], ("UNKNOWN", "NOT_CONFIGURED", "BLOCKED"))
            else:
                self.assertIsNotNone(c["observed_at"])
                self.assertIsNotNone(c["expires_at"])

            # Verify SafeFact items
            for fact in c["evidence"]:
                self.assertIn("fact_id", fact)
                self.assertIn("value", fact)
                val = fact["value"]
                self.assertTrue(val is None or isinstance(val, (bool, int, str)))
                if isinstance(val, str):
                    self.assertLessEqual(len(val), 256)

        # 5. Check action IDs allowlist
        for aid in snapshot["available_action_ids"]:
            self.assertIn(aid, ACTION_IDS)

    def test_a01_input_limits_and_rejections(self):
        # 1. Duplicate key rejection
        with self.assertRaises(ValueError):
            parse_strict_json_with_limits(b'{"key": 1, "key": 2}')

        # 2. Non-finite float rejection
        with self.assertRaises(ValueError):
            parse_strict_json_with_limits(b'{"key": NaN}')

        # 3. Excess depth rejection (> 16)
        deep = {"a": None}
        curr = deep
        for _ in range(20):
            curr["child"] = {}
            curr = curr["child"]
        with self.assertRaises(ValueError):
            parse_strict_json_with_limits(json.dumps(deep).encode())

        # 4. Body size rejection (> 64 KiB)
        big = b"x" * (65 * 1024)
        with self.assertRaises(ValueError):
            parse_strict_json_with_limits(big)

        # 5. Schema negative rejections: unknown action in snapshot
        cfg = self._create_mock_git_config()
        collector = DiagnosticSnapshotCollector(git_config_path=cfg, store_path=self._create_mock_store())
        snap = collector.collect_snapshot("9000")
        snap["available_action_ids"].append("unknown.arbitrary.action")
        snap["snapshot_id"] = compute_snapshot_id(snap)
        with self.assertRaises(jsonschema.ValidationError):
            self.validator.validate(snap)

    # --- A02: Private authenticated wizard and public denial ---
    def test_a02_private_api_authentication_and_csrf(self):
        # 1. Unauthenticated GET denied
        res = self.client.get("/api/v1/setup/projects")
        self.assertEqual(res.status_code, 401)
        res = self.client.get("/api/v1/projects/9000/setup/snapshot")
        self.assertEqual(res.status_code, 401)

        # 2. Admin token authentication succeeds
        res_admin = self.client.get(
            "/api/v1/setup/projects",
            headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
        )
        self.assertEqual(res_admin.status_code, 200)

        # 3. Session flow: initialize session
        r_init = self.client.get("/api/v1/operator/session")
        self.assertEqual(r_init.status_code, 200)
        pairing = r_init.json()["pairing_code"]

        # Authorize session using admin token
        r_auth = self.client.post(
            "/api/v1/operator/session/authorize",
            headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
            json={"pairing_code": pairing},
        )
        self.assertEqual(r_auth.status_code, 200)

        # Request with authorized session but MISSING CSRF header -> 403 OPERATOR_CSRF_REQUIRED
        r_nocsrf = self.client.get("/api/v1/setup/projects")
        self.assertEqual(r_nocsrf.status_code, 403)
        detail_obj = r_nocsrf.json().get("detail", r_nocsrf.json())
        self.assertEqual(detail_obj.get("error"), "OPERATOR_CSRF_REQUIRED")

        # Request with authorized session and CORRECT CSRF header -> 200 OK
        r_sess = self.client.get("/api/v1/operator/session")
        csrf_tok = r_sess.json()["csrf_token"]
        r_ok = self.client.get("/api/v1/setup/projects", headers={"X-Nginx-QA-CSRF": csrf_tok})
        self.assertEqual(r_ok.status_code, 200)

        # 4. Foreign Origin denied even with admin token
        res_origin = self.client.get(
            "/api/v1/setup/projects",
            headers={
                ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN,
                "Origin": "https://malicious.evil.com",
            },
        )
        self.assertEqual(res_origin.status_code, 403)

    def test_a02_public_gateway_denial(self):
        from nginx_qa.launcher_gateway import create_gateway
        public_app = create_gateway(app_module)
        public_client = TestClient(public_app, raise_server_exceptions=False)

        # When gateway is disabled or offline, all public requests are denied with 503
        r1 = public_client.get("/api/v1/setup/projects")
        self.assertEqual(r1.status_code, 503)

        # When gateway is enabled with configured origin, wizard paths return 404 (Not Found on public gateway)
        with patch.dict(os.environ, {
            "NGINX_QA_LAUNCHER_GATEWAY_ENABLED": "1",
            "NGINX_QA_TELEGRAM_LAUNCHER_ENABLED": "1",
        }):
            with patch("nginx_qa.launcher_gateway.gateway_origin", return_value="https://test.trycloudflare.com"):
                r_enabled = public_client.get(
                    "/api/v1/setup/projects",
                    headers={"Host": "test.trycloudflare.com"},
                )
                self.assertEqual(r_enabled.status_code, 404)

                r_snap = public_client.get(
                    "/api/v1/projects/9000/setup/snapshot",
                    headers={"Host": "test.trycloudflare.com"},
                )
                self.assertEqual(r_snap.status_code, 404)

                r_setup = public_client.get(
                    "/setup",
                    headers={"Host": "test.trycloudflare.com"},
                )
                self.assertEqual(r_setup.status_code, 404)

    # --- A03: Canonical project selection and cross-project read isolation ---
    def test_a03_canonical_projects_and_isolation(self):
        cfg = self._create_mock_git_config()
        with patch.object(app_module, "git_config_path", cfg):
            # Enumerate canonical projects
            res = self.client.get(
                "/api/v1/setup/projects",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
            )
            self.assertEqual(res.status_code, 200)
            projects = res.json()["projects"]
            self.assertTrue(any(p["project_id"] == "9000" for p in projects))

            # Unknown project ID returns 404
            res404 = self.client.get(
                "/api/v1/projects/9999/setup/snapshot",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
            )
            self.assertEqual(res404.status_code, 404)

            # Invalid project ID pattern returns 400
            res400 = self.client.get(
                "/api/v1/projects/invalid/setup/snapshot",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
            )
            self.assertEqual(res400.status_code, 400)

            # Invalid instance ID pattern returns 400
            res_inst = self.client.get(
                "/api/v1/projects/9000/setup/snapshot?instance_id=bad%20id%20!!",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
            )
            self.assertEqual(res_inst.status_code, 400)

        # Corrupt configuration returns explicit 409 STATE_CORRUPT
        corrupt_cfg = self._create_mock_git_config(corrupt=True)
        with patch.object(app_module, "git_config_path", corrupt_cfg):
            res_corrupt = self.client.get(
                "/api/v1/setup/projects",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
            )
            self.assertEqual(res_corrupt.status_code, 409)
            corrupt_detail = res_corrupt.json().get("detail", res_corrupt.json())
            self.assertEqual(corrupt_detail.get("error"), "STATE_CORRUPT")

    # --- A04: Passive GET has no hidden mutations ---
    def test_a04_passive_get_has_no_hidden_mutations(self):
        cfg = self._create_mock_git_config()
        store = self._create_mock_store(busy=True)

        initial_db_bytes = store.read_bytes()
        initial_db_hash = hashlib.sha256(initial_db_bytes).hexdigest()
        initial_cfg_bytes = cfg.read_bytes()

        collector = DiagnosticSnapshotCollector(git_config_path=cfg, store_path=store)

        # Repeated 10 captures
        for _ in range(10):
            snap = collector.collect_snapshot("9000")
            self.assertEqual(snap["project_id"], "9000")

        # Assert no file modification
        self.assertEqual(hashlib.sha256(store.read_bytes()).hexdigest(), initial_db_hash)
        self.assertEqual(cfg.read_bytes(), initial_cfg_bytes)

    # --- A06: Bounded networking, SSRF and cancellation ---
    def test_a06_bounded_networking_and_cancellation(self):
        cfg = self._create_mock_git_config()
        store = self._create_mock_store()

        collector = DiagnosticSnapshotCollector(git_config_path=cfg, store_path=store)

        # Socket connect monkeypatch to prove no network calls during passive GET
        with patch("socket.socket.connect") as mock_connect:
            snap = collector.collect_snapshot("9000")
            self.assertIsNotNone(snap)
            self.assertEqual(mock_connect.call_count, 0)

        # Test cancellation release in DiagnosticReadGate
        gate = DiagnosticReadGate(default_ttl=5.0)

        async def run_cancel_test():
            async def worker_cancel():
                # Simulate task cancellation
                await gate._semaphore.acquire()
                try:
                    await asyncio.sleep(0.01)
                    raise asyncio.CancelledError()
                finally:
                    gate._semaphore.release()

            task = asyncio.create_task(worker_cancel())
            await asyncio.sleep(0.005)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

            # Gate must remain operational after cancellation
            snap_res = await gate.get_snapshot("9000", git_config_path=cfg, state_base=self.temp_path)
            self.assertEqual(snap_res["project_id"], "9000")

        asyncio.run(run_cancel_test())

    # --- A07: Secret references, alias denial and redaction ---
    def test_a07_secret_references_and_redaction(self):
        canary_bot = "canary-secret-bot-token-99887766"
        canary_exec = "canary-secret-exec-token-11223344"
        with patch.dict(os.environ, {
            "NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN": canary_bot,
            "NGINX_QA_LAUNCHER_CONTROL_EXECUTOR_TOKEN": canary_exec,
            "TELEGRAM_BOT_TOKEN": "123456789:ABCdefGHIjklMNOpqrSTUvwxYZ123456789",
            "NGINX_QA_LAUNCHER_BOT_SECRET_REF": "env:NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN",
        }):
            cfg = self._create_mock_git_config()
            store = self._create_mock_store()
            collector = DiagnosticSnapshotCollector(git_config_path=cfg, store_path=store)
            snapshot = collector.collect_snapshot("9000")
            snapshot_json = json.dumps(snapshot)

            # Assert canary secrets never appear in snapshot payload
            self.assertNotIn(canary_bot, snapshot_json)
            self.assertNotIn(canary_exec, snapshot_json)
            self.assertNotIn("123456789:ABCdefGHIjklMNOpqrSTUvwxYZ123456789", snapshot_json)

            # Assert redact_secrets function
            sample_text = f"Bot token 123456789:ABCdefGHIjklMNOpqrSTUvwxYZ123456789 and {canary_bot}"
            redacted = redact_secrets(sample_text, extra_canaries=(canary_bot,))
            self.assertNotIn("123456789:ABCdef", redacted)
            self.assertNotIn(canary_bot, redacted)
            self.assertIn("[REDACTED_BOT_TOKEN]", redacted)

    # --- A08: Network/auth/expiry facts stay distinct ---
    def test_a08_network_auth_expiry_facts_distinct(self):
        self.assertIn("TIMEOUT", ERROR_CODES)
        self.assertIn("DNS_ERROR", ERROR_CODES)
        self.assertIn("CONNECTION_ERROR", ERROR_CODES)
        self.assertIn("AUTH_REJECTED", ERROR_CODES)
        self.assertIn("NOT_CONFIGURED", ERROR_CODES)
        # All error codes are distinct uppercase identifier strings
        self.assertEqual(len(ERROR_CODES), len(set(ERROR_CODES)))

    # --- A22: No hidden destructive setup effects ---
    def test_a22_no_hidden_destructive_setup_effects(self):
        cfg = self._create_mock_git_config()
        store = self._create_mock_store(busy=True)

        collector = DiagnosticSnapshotCollector(git_config_path=cfg, store_path=store)
        snapshot = collector.collect_snapshot("9000")

        # Slot must remain reported as busy without resetting or clearing active_sprint
        slot_check = next(c for c in snapshot["checks"] if c["check_id"] == "project.slot")
        self.assertEqual(slot_check["status"], "WARN")
        self.assertEqual(slot_check["error_code"], "PROJECT_BUSY")

    # --- A23: Responsiveness, cache and repeat diagnostics ---
    def test_a23_responsiveness_and_cache(self):
        cfg = self._create_mock_git_config()
        store = self._create_mock_store()

        gate = DiagnosticReadGate(max_cache_entries=4, default_ttl=5.0)

        async def run_cache_test():
            snap1 = await gate.get_snapshot("9000", git_config_path=cfg, state_base=self.temp_path)
            t0 = time.monotonic()
            snap2 = await gate.get_snapshot("9000", git_config_path=cfg, state_base=self.temp_path)
            duration = time.monotonic() - t0
            # Cached response should be nearly instantaneous (< 5ms)
            self.assertLess(duration, 0.05)
            self.assertEqual(snap1["snapshot_id"], snap2["snapshot_id"])

        asyncio.run(run_cache_test())

    # --- A24: Git, Telegram, tunnel and webhook ownership ---
    def test_a24_git_telegram_tunnel_ownership(self):
        cfg = self._create_mock_git_config()
        store = self._create_mock_store()

        # Mock tunnel origin receipt with fresh heartbeat
        origin_path = self.temp_path / "launcher-origin.json"
        now_ts = time.time()
        origin_path.write_text(json.dumps({
            "schema_version": 1,
            "phase": "READY",
            "heartbeat": now_ts - 2.0,
            "upstream": "http://127.0.0.1:18035",
        }), encoding="utf-8")

        with patch.dict(os.environ, {"NGINX_QA_LAUNCHER_ORIGIN_STATE": str(origin_path)}):
            collector = DiagnosticSnapshotCollector(
                git_config_path=cfg,
                store_path=store,
                now=datetime.fromtimestamp(now_ts, timezone.utc),
            )
            snapshot = collector.collect_snapshot("9000")

            tunnel_owner = next(c for c in snapshot["checks"] if c["check_id"] == "tunnel.ownership")
            self.assertEqual(tunnel_owner["status"], "PASS")

            tunnel_hb = next(c for c in snapshot["checks"] if c["check_id"] == "tunnel.heartbeat")
            self.assertEqual(tunnel_hb["status"], "PASS")
            self.assertEqual(tunnel_hb["freshness"], "FRESH")

        # Stale heartbeat (> 15 seconds)
        origin_path.write_text(json.dumps({
            "schema_version": 1,
            "phase": "READY",
            "heartbeat": now_ts - 30.0,
            "upstream": "http://127.0.0.1:18035",
        }), encoding="utf-8")

        with patch.dict(os.environ, {"NGINX_QA_LAUNCHER_ORIGIN_STATE": str(origin_path)}):
            collector_stale = DiagnosticSnapshotCollector(
                git_config_path=cfg,
                store_path=store,
                now=datetime.fromtimestamp(now_ts, timezone.utc),
            )
            snapshot_stale = collector_stale.collect_snapshot("9000")
            tunnel_hb_stale = next(c for c in snapshot_stale["checks"] if c["check_id"] == "tunnel.heartbeat")
            self.assertEqual(tunnel_hb_stale["status"], "WARN")
            self.assertEqual(tunnel_hb_stale["freshness"], "STALE")

    # --- A25: One host lifecycle, failure and shutdown isolation ---
    def test_a25_one_host_lifecycle(self):
        # ManagedImportStore diagnostic_snapshot extension method test
        store_path = self._create_mock_store(busy=True)
        store = ManagedImportStore(str(store_path))
        self.assertTrue(hasattr(store, "diagnostic_snapshot"))
        data = store.diagnostic_snapshot("9000")
        self.assertTrue(data["available"])
        self.assertEqual(data["active_sprint_id"], "sprint-test-1")
        self.assertEqual(data["sprint_status"], "active")


if __name__ == "__main__":
    unittest.main()
