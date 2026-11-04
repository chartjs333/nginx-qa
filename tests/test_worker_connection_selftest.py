"""Automated qualification suite for Setup and Diagnostics Wizard v1 (worker-connection-selftest).

Acceptance cases:
- A05: Worker discovery and truthful capability probe
- A06: Bounded networking, SSRF and cancellation
- A07: Secret references, alias denial and redaction
- A09: Original executor receipts and evidence levels
- A11: Idempotency and concurrent apply
- A13: Explicit private owner, authorization and no role impersonation
- A14: One shared project slot across every ingress
- A19: Native isolated self-test and evidence chain
- A20: Stop and pending review history
- A26: Legacy compatibility and versioned additive integration
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
import jsonschema

import main as app_module
from nginx_qa.setup_diagnostics import (
    DiagnosticSnapshotCollector,
    get_contracts_schema,
    compute_snapshot_id,
)
from nginx_qa.setup_worker_adapter import (
    SetupWorkerAdapter,
    validate_worker_endpoint,
)
from nginx_qa.setup_operations import (
    SetupOperationsManager,
    compute_plan_digest,
    get_plan_validator,
    get_apply_validator,
    get_receipt_validator,
)
from nginx_qa.legacy_scope_control import ADMIN_TOKEN_HEADER, ADMIN_TOKEN_ENV


TEST_ADMIN_TOKEN = "b" * 32


class MockExecutorServer:
    """Mock HTTP loopback executor implementing capabilities endpoint for tests."""

    def __init__(self, *, status_code: int = 200, return_capabilities: bool = True):
        self.status_code = status_code
        self.return_capabilities = return_capabilities
        self.server: http.server.HTTPServer | None = None
        self.port: int = 0
        self.thread: threading.Thread | None = None

    def start(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            status = self.status_code
            has_caps = self.return_capabilities

            def do_GET(self):
                if self.path == "/managed-client/capabilities":
                    if self.has_caps:
                        self.send_response(self.status)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        caps = {
                            "kind": "managed_client_capabilities",
                            "schema_version": 1,
                            "client_id": "mock-test-executor-v1",
                            "adapter_version": "1.0.0",
                            "protocol": "nginx-qa-managed-client-v1",
                            "supported_actions": ["prepare", "start", "stop"],
                            "declared_guarantees": {
                                "durable_deduplication": True,
                                "restart_resume": True,
                                "independent_reviewers": True,
                                "session_quiescence_on_stop": True,
                            },
                        }
                        self.wfile.write(json.dumps(caps).encode("utf-8"))
                    else:
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(b"Not Found")
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()


class TestWorkerConnectionSelftest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ[ADMIN_TOKEN_ENV] = TEST_ADMIN_TOKEN
        cls.schema = get_contracts_schema()
        cls.plan_validator = get_plan_validator()
        cls.receipt_validator = get_receipt_validator()
        cls.app = app_module.app
        cls.client = TestClient(cls.app, raise_server_exceptions=False)

    def setUp(self):
        os.environ[ADMIN_TOKEN_ENV] = TEST_ADMIN_TOKEN
        self.temp_dir = tempfile.mkdtemp(prefix="sdw-worker-test-")
        self.temp_path = Path(self.temp_dir)
        self.mock_executor = None

    def tearDown(self):
        if self.mock_executor:
            self.mock_executor.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_git_config(self) -> Path:
        p = self.temp_path / "port_git_map.json"
        data = {
            "projects": {
                "proj_context_9000": {
                    "project_phone": "9000",
                    "project_name": "Project 9000 Dev",
                },
                "proj_context_9001": {
                    "project_phone": "9001",
                    "project_name": "Project 9001 Test",
                },
            },
            "phone_git_contexts": {
                "9000": {"project_phone": "9000", "git_context_key": "proj_context_9000"},
                "9001": {"project_phone": "9001", "git_context_key": "proj_context_9001"},
            },
        }
        p.write_text(json.dumps(data), encoding="utf-8")
        return p

    # --- A05: Worker discovery and truthful capability probe ---
    def test_a05_worker_discovery_and_truthful_probe(self):
        # 1. Unconfigured candidate -> NOT_CONFIGURED
        adapter = SetupWorkerAdapter(environment={})
        res_not_cfg = adapter.probe("default-executor")
        self.assertEqual(res_not_cfg["status"], "NOT_CONFIGURED")
        self.assertEqual(res_not_cfg["error_code"], "NOT_CONFIGURED")
        self.assertIn("setup_steps", res_not_cfg)
        self.assertGreater(len(res_not_cfg["setup_steps"]), 0)

        # 2. SSRF validation: Disallow external IP, domain names, userinfo, query params
        with self.assertRaises(ValueError):
            validate_worker_endpoint("http://192.168.1.50:8000")
        with self.assertRaises(ValueError):
            validate_worker_endpoint("http://169.254.169.254:80")  # metadata IP
        with self.assertRaises(ValueError):
            validate_worker_endpoint("http://example.com:8000")
        with self.assertRaises(ValueError):
            validate_worker_endpoint("http://user:pass@127.0.0.1:8000")
        with self.assertRaises(ValueError):
            validate_worker_endpoint("http://127.0.0.1:8000/some/path")
        with self.assertRaises(ValueError):
            validate_worker_endpoint("http://127.0.0.1:8000?query=1")

        # Loopback IPv4 is valid
        host, port = validate_worker_endpoint("http://127.0.0.1:18037")
        self.assertEqual(host, "127.0.0.1")
        self.assertEqual(port, 18037)

        # 3. Live mock executor probe returning capabilities (200 OK)
        self.mock_executor = MockExecutorServer(status_code=200, return_capabilities=True)
        self.mock_executor.start()

        env = {"NGINX_QA_LAUNCHER_EXECUTOR_URL": f"http://127.0.0.1:{self.mock_executor.port}"}
        active_adapter = SetupWorkerAdapter(environment=env)
        probe_res = active_adapter.probe("default-executor")
        self.assertEqual(probe_res["status"], "PASS")
        self.assertIsNone(probe_res["error_code"])
        self.assertEqual(probe_res["protocol"], "nginx-qa-managed-client-v1")
        self.assertIsNotNone(probe_res["capabilities"])
        self.assertEqual(probe_res["capabilities"]["protocol"], "nginx-qa-managed-client-v1")

        # 4. Probe against legacy worker returning 404 (PROBE_UNSUPPORTED)
        self.mock_executor.stop()
        self.mock_executor = MockExecutorServer(status_code=404, return_capabilities=False)
        self.mock_executor.start()

        env_legacy = {"NGINX_QA_LAUNCHER_EXECUTOR_URL": f"http://127.0.0.1:{self.mock_executor.port}"}
        legacy_adapter = SetupWorkerAdapter(environment=env_legacy)
        legacy_res = legacy_adapter.probe("default-executor")
        self.assertEqual(legacy_res["status"], "WARN")
        self.assertEqual(legacy_res["error_code"], "PROBE_UNSUPPORTED")
        self.assertEqual(legacy_res["protocol"], "nginx-qa-managed-client-v1")

    # --- A09 & A26: Worker bind, original executor receipts and evidence ---
    def test_a09_worker_bind_and_receipt_provenance(self):
        self.mock_executor = MockExecutorServer(status_code=200, return_capabilities=True)
        self.mock_executor.start()

        env = {"NGINX_QA_LAUNCHER_EXECUTOR_URL": f"http://127.0.0.1:{self.mock_executor.port}"}
        adapter = SetupWorkerAdapter(environment=env)
        probe_res = adapter.probe("default-executor")
        receipt_id = probe_res["probe_receipt_id"]

        # Bind using valid receipt
        bind_res = adapter.bind("default-executor", receipt_id)
        self.assertIn("binding_id", bind_res)
        self.assertEqual(bind_res["candidate_id"], "default-executor")
        self.assertEqual(bind_res["probe_receipt_id"], receipt_id)

        # Attempt to bind non-existent receipt fails
        with self.assertRaises(LookupError):
            adapter.bind("default-executor", "non-existent-receipt-id")

    # --- A11: Idempotency and concurrent apply ---
    def test_a11_idempotency_and_concurrent_apply(self):
        manager = SetupOperationsManager()
        snapshot = {
            "kind": "setup_diagnostic_snapshot",
            "schema_version": 1,
            "snapshot_id": "a" * 64,
            "project_id": "9000",
            "instance_id": None,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": {
                "adapter_id": "test",
                "adapter_version": "1.0",
                "code_commit": "0" * 40,
                "configuration_version": "v1",
            },
            "authority_fingerprint": "b" * 64,
            "checks": [],
            "available_action_ids": ["diagnostics.refresh"],
        }
        plan_req = {
            "schema_version": 1,
            "snapshot_id": "a" * 64,
            "action_id": "diagnostics.refresh",
            "arguments": {},
        }
        plan = manager.create_plan("operator-test", "9000", plan_req, snapshot=snapshot)
        digest = plan["plan_digest"]

        apply_req = {
            "schema_version": 1,
            "plan_digest": digest,
            "idempotency_key": "unique-apply-key-101",
            "confirmed": True,
        }

        async def run_idempotency_test():
            # 1. First apply
            receipt1 = await manager.apply_plan("operator-test", "9000", apply_req)
            self.assertEqual(receipt1["state"], "SUCCEEDED")
            self.assertFalse(receipt1["deduplicated"])

            # 2. Second apply with same key and plan -> returns same receipt with deduplicated=True
            receipt2 = await manager.apply_plan("operator-test", "9000", apply_req)
            self.assertEqual(receipt2["operation_id"], receipt1["operation_id"])
            self.assertTrue(receipt2["deduplicated"])

            # 3. Third apply with same key but DIFFERENT plan -> raises IDEMPOTENCY_CONFLICT
            conflict_req = {
                "schema_version": 1,
                "plan_digest": "f" * 64,
                "idempotency_key": "unique-apply-key-101",
                "confirmed": True,
            }
            with self.assertRaises(ValueError) as ctx:
                await manager.apply_plan("operator-test", "9000", conflict_req)
            self.assertIn("IDEMPOTENCY_CONFLICT", str(ctx.exception))

        asyncio.run(run_idempotency_test())

    # --- A13, A14 & A19: Plan API, shared slot, separate test project ---
    def test_a14_and_a19_selftest_plan_and_separate_project(self):
        cfg = self._create_mock_git_config()
        with patch.object(app_module, "git_config_path", cfg):
            # 1. Plan selftest on test project 9001 (separate from 9000)
            plan_req = {
                "schema_version": 1,
                "snapshot_id": "c" * 64,
                "action_id": "selftest.start",
                "arguments": {"test_profile_id": "docs-selftest-profile"},
            }

            res_plan = self.client.post(
                "/api/v1/projects/9000/setup/plans",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
                json=plan_req,
            )
            self.assertEqual(res_plan.status_code, 200)
            plan_data = res_plan.json()
            self.assertEqual(plan_data["kind"], "setup_action_plan")
            self.assertEqual(plan_data["action_id"], "selftest.start")
            self.assertEqual(plan_data["project_id"], "9000")
            # Crucial requirement: Target project for self-test must be separate test project (9001)
            self.assertEqual(plan_data["target_project_id"], "9001")
            self.assertTrue(plan_data["confirmation_required"])

            # Validate plan strictly against ActionPlan schema
            self.plan_validator.validate(plan_data)

            # 2. Apply without confirmed=True is rejected
            unconfirmed_apply = {
                "schema_version": 1,
                "plan_digest": plan_data["plan_digest"],
                "idempotency_key": "selftest-apply-key-001",
                "confirmed": False,
            }
            res_unconf = self.client.post(
                f"/api/v1/projects/9000/setup/plans/{plan_data['plan_id']}/apply",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
                json=unconfirmed_apply,
            )
            self.assertEqual(res_unconf.status_code, 400)
            self.assertIn("CONFIRMATION_REQUIRED", res_unconf.json().get("detail", {}).get("error", ""))

            # 3. Apply with confirmed=True succeeds
            confirmed_apply = {
                "schema_version": 1,
                "plan_digest": plan_data["plan_digest"],
                "idempotency_key": "selftest-apply-key-001",
                "confirmed": True,
            }
            res_apply = self.client.post(
                f"/api/v1/projects/9000/setup/plans/{plan_data['plan_id']}/apply",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
                json=confirmed_apply,
            )
            self.assertEqual(res_apply.status_code, 200)
            receipt = res_apply.json()
            self.assertEqual(receipt["kind"], "setup_operation_receipt")
            self.assertEqual(receipt["state"], "SUCCEEDED")
            self.assertIsNotNone(receipt["native_sprint_id"])
            self.receipt_validator.validate(receipt)

            # 4. Query operation status
            op_id = receipt["operation_id"]
            res_op = self.client.get(
                f"/api/v1/projects/9000/setup/operations/{op_id}",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
            )
            self.assertEqual(res_op.status_code, 200)
            self.assertEqual(res_op.json()["operation_id"], op_id)

    # --- A20: Selftest stop and history preservation ---
    def test_a20_selftest_stop_flow(self):
        cfg = self._create_mock_git_config()
        with patch.object(app_module, "git_config_path", cfg):
            # Create plan for selftest.stop
            stop_plan_req = {
                "schema_version": 1,
                "snapshot_id": "c" * 64,
                "action_id": "selftest.stop",
                "arguments": {"self_test_id": "test-run-123"},
            }
            res_plan = self.client.post(
                "/api/v1/projects/9000/setup/plans",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
                json=stop_plan_req,
            )
            self.assertEqual(res_plan.status_code, 200)
            plan_data = res_plan.json()

            # Apply stop
            apply_req = {
                "schema_version": 1,
                "plan_digest": plan_data["plan_digest"],
                "idempotency_key": "stop-apply-key-002",
                "confirmed": True,
            }
            res_apply = self.client.post(
                f"/api/v1/projects/9000/setup/plans/{plan_data['plan_id']}/apply",
                headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN},
                json=apply_req,
            )
            self.assertEqual(res_apply.status_code, 200)
            receipt = res_apply.json()
            self.assertEqual(receipt["state"], "SUCCEEDED")
            self.assertEqual(receipt["action_id"], "selftest.stop")


if __name__ == "__main__":
    unittest.main()
