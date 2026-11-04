"""Automated qualification suite for Setup and Diagnostics Wizard v1 (safe-operations-recovery).

Acceptance cases:
- A10: Stale plan and current authority fences
- A12: Restart and ambiguous external effects
- A15: Typed actions and native recovery only
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import unittest

from fastapi.testclient import TestClient
import jsonschema

import main as app_module
from nginx_qa.setup_diagnostics import (
    get_contracts_schema,
    compute_authority_fingerprint,
)
from nginx_qa.setup_operations import (
    SetupOperationsManager,
    compute_plan_digest,
    get_plan_validator,
    get_apply_validator,
    get_receipt_validator,
)
from nginx_qa.legacy_scope_control import ADMIN_TOKEN_HEADER, ADMIN_TOKEN_ENV


TEST_ADMIN_TOKEN = "c" * 32


class TestSafeOperationsRecovery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ[ADMIN_TOKEN_ENV] = TEST_ADMIN_TOKEN
        cls.schema = get_contracts_schema()
        cls.plan_validator = get_plan_validator()
        cls.apply_validator = get_apply_validator()
        cls.receipt_validator = get_receipt_validator()
        cls.app = app_module.app
        cls.client = TestClient(cls.app, raise_server_exceptions=False)

    def setUp(self):
        os.environ[ADMIN_TOKEN_ENV] = TEST_ADMIN_TOKEN
        self.temp_dir = tempfile.mkdtemp(prefix="sdw-ops-test-")
        self.temp_path = Path(self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _sample_snapshot(self) -> dict:
        return {
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
            "authority_fingerprint": compute_authority_fingerprint("9000", policy_generation=1, control_revision=1),
            "checks": [],
            "available_action_ids": ["diagnostics.refresh", "recovery.open"],
        }

    # --- A10: Stale plan and current authority fences ---
    def test_a10_stale_plan_and_authority_fences(self):
        manager = SetupOperationsManager(durable_store_dir=self.temp_path)
        snapshot = self._sample_snapshot()

        plan_req = {
            "schema_version": 1,
            "snapshot_id": "a" * 64,
            "action_id": "diagnostics.refresh",
            "arguments": {},
        }
        plan = manager.create_plan("actor-1", "9000", plan_req, snapshot=snapshot)
        digest = plan["plan_digest"]

        apply_req = {
            "schema_version": 1,
            "plan_digest": digest,
            "idempotency_key": "stale-fence-key-001",
            "confirmed": True,
        }

        async def run_fences_test():
            # 1. Simulate state change: authority fingerprint changes on server
            changed_fp = "f" * 64
            with self.assertRaises(ValueError) as ctx:
                await manager.apply_plan(
                    "actor-1",
                    "9000",
                    apply_req,
                    current_authority_fingerprint=changed_fp,
                )
            self.assertIn("PLAN_STALE", str(ctx.exception))
            self.assertIn("Authority fingerprint mismatch", str(ctx.exception))

            # 2. Simulate expired plan
            plan["expires_at"] = "2020-01-01T00:00:00+00:00"  # past
            with self.assertRaises(ValueError) as ctx2:
                await manager.apply_plan("actor-1", "9000", apply_req)
            self.assertIn("PLAN_STALE", str(ctx2.exception))
            self.assertIn("expired", str(ctx2.exception))

            # 3. Simulate policy revocation: bumping actor policy generation
            manager.revoke_actor_policy("actor-1")
            new_plan = manager.create_plan("actor-1", "9000", plan_req, snapshot=snapshot)
            # Old plan has old policy_generation fingerprint, cannot be applied
            with self.assertRaises(ValueError) as ctx3:
                await manager.apply_plan("actor-1", "9000", apply_req)
            self.assertIn("PLAN_STALE", str(ctx3.exception))

        asyncio.run(run_fences_test())

    # --- A12: Restart and ambiguous external effects ---
    def test_a12_restart_and_durable_intent_replay(self):
        durable_dir = self.temp_path / "durable_ops"
        manager = SetupOperationsManager(durable_store_dir=durable_dir)
        snapshot = self._sample_snapshot()

        plan_req = {
            "schema_version": 1,
            "snapshot_id": "a" * 64,
            "action_id": "recovery.open",
            "arguments": {"instance_id": "inst-test-999"},
        }
        plan = manager.create_plan("actor-operator", "9000", plan_req, snapshot=snapshot)
        digest = plan["plan_digest"]

        apply_req = {
            "schema_version": 1,
            "plan_digest": digest,
            "idempotency_key": "recovery-key-restart-002",
            "confirmed": True,
        }

        async def run_restart_test():
            # 1. Execute apply and get receipt
            receipt1 = await manager.apply_plan("actor-operator", "9000", apply_req)
            self.assertEqual(receipt1["state"], "SUCCEEDED")
            op_id = receipt1["operation_id"]

            # 2. Simulate server restart: create new manager loading from same durable directory
            restarted_manager = SetupOperationsManager(durable_store_dir=durable_dir)

            # Re-read operation by ID after restart
            replayed_op = restarted_manager.get_operation("9000", op_id)
            self.assertEqual(replayed_op["operation_id"], op_id)
            self.assertEqual(replayed_op["state"], "SUCCEEDED")

            # Duplicate apply after restart returns deduplicated receipt
            restarted_manager._plans[digest] = plan
            receipt2 = await restarted_manager.apply_plan("actor-operator", "9000", apply_req)
            self.assertEqual(receipt2["operation_id"], op_id)
            self.assertTrue(receipt2["deduplicated"])

        asyncio.run(run_restart_test())

    # --- A15: Typed actions, native recovery and rejection of arbitrary commands ---
    def test_a15_typed_actions_and_injection_rejection(self):
        manager = SetupOperationsManager(durable_store_dir=self.temp_path)
        snapshot = self._sample_snapshot()

        # 1. Reject unknown action
        with self.assertRaises(ValueError):
            manager.create_plan(
                "actor-1",
                "9000",
                {
                    "schema_version": 1,
                    "snapshot_id": "a" * 64,
                    "action_id": "arbitrary.shell.exec",
                    "arguments": {"command": "rm -rf /"},
                },
                snapshot=snapshot,
            )

        # 2. Reject shell injection or unknown arguments via schema validator
        invalid_args_plan_req = {
            "schema_version": 1,
            "snapshot_id": "a" * 64,
            "action_id": "diagnostics.refresh",
            "arguments": {"raw_sql": "DROP TABLE managed_projects;"},
        }
        with self.assertRaises(jsonschema.ValidationError):
            manager.create_plan("actor-1", "9000", invalid_args_plan_req, snapshot=snapshot)

        # 3. Cross-project isolation: get_operation with wrong project ID fails with 404
        plan_req = {
            "schema_version": 1,
            "snapshot_id": "a" * 64,
            "action_id": "diagnostics.refresh",
            "arguments": {},
        }
        plan = manager.create_plan("actor-1", "9000", plan_req, snapshot=snapshot)

        async def run_cross_proj():
            receipt = await manager.apply_plan(
                "actor-1",
                "9000",
                {
                    "schema_version": 1,
                    "plan_digest": plan["plan_digest"],
                    "idempotency_key": "cross-proj-key",
                    "confirmed": True,
                },
            )
            op_id = receipt["operation_id"]
            # Correct project works
            self.assertEqual(manager.get_operation("9000", op_id)["operation_id"], op_id)
            # Wrong project raises LookupError
            with self.assertRaises(LookupError):
                manager.get_operation("9001", op_id)

        asyncio.run(run_cross_proj())


if __name__ == "__main__":
    unittest.main()
