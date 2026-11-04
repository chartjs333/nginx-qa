"""Regression tests for real fencing and durable intent semantics."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

import jsonschema
from nginx_qa.setup_operations import SetupOperationsManager


class TestSafeOperationsRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fp = "a" * 64
        self.allowed = True
        self.calls = []
        self.manager = self.make_manager()
        self.snapshot = {"snapshot_id": "b" * 64, "project_id": "9000",
            "authority_fingerprint": self.fp,
            "available_action_ids": ["diagnostics.refresh", "selftest.start", "selftest.stop", "provider.probe"]}

    def authorize(self, actor, project, action):
        if not self.allowed:
            raise PermissionError("revoked")

    def action(self, **scope):
        self.calls.append(scope)
        return {"state": "SUCCEEDED", "evidence_ids": ["real-fixture-effect"]}

    def make_manager(self):
        return SetupOperationsManager(durable_store_dir=self.root, authorize=self.authorize,
            authority=lambda a, p: self.fp, action_handlers={"diagnostics.refresh": self.action})

    def plan(self, action="diagnostics.refresh", arguments=None):
        return self.manager.create_plan("alice", "9000", {"schema_version": 1,
            "snapshot_id": self.snapshot["snapshot_id"], "action_id": action, "arguments": arguments or {}},
            snapshot=self.snapshot)

    @staticmethod
    def request(plan, key="same-client-retry-key"):
        return {"schema_version": 1, "plan_digest": plan["plan_digest"], "confirmed": True, "idempotency_key": key}

    def test_actor_path_project_and_current_grant_are_checked_before_replay(self):
        plan = self.plan()
        for actor, project, path in (("bob", "9000", plan["plan_id"]), ("alice", "9001", plan["plan_id"]),
                                     ("alice", "9000", "plan-unrelated")):
            with self.assertRaises(PermissionError):
                asyncio.run(self.manager.apply_plan(actor, project, self.request(plan), plan_id=path))
        result = asyncio.run(self.manager.apply_plan("alice", "9000", self.request(plan), plan_id=plan["plan_id"]))
        self.assertEqual("SUCCEEDED", result["state"])
        with self.assertRaises(PermissionError):
            self.manager.get_operation("9000", result["operation_id"], actor_id="bob")
        self.allowed = False
        with self.assertRaises(PermissionError):
            asyncio.run(self.manager.apply_plan("alice", "9000", self.request(plan)))
        self.assertEqual(1, len(self.calls))

    def test_forged_snapshot_arguments_and_stale_authority_fail(self):
        with self.assertRaises(ValueError):
            self.manager.create_plan("alice", "9000", {"schema_version": 1, "snapshot_id": "c" * 64,
                "action_id": "diagnostics.refresh", "arguments": {}}, snapshot=self.snapshot)
        with self.assertRaises(jsonschema.ValidationError):
            self.plan(arguments={"shell": "arbitrary"})
        plan = self.plan()
        self.fp = "d" * 64
        with self.assertRaisesRegex(ValueError, "PLAN_STALE"):
            asyncio.run(self.manager.apply_plan("alice", "9000", self.request(plan)))
        self.assertEqual([], self.calls)

    def test_restart_duplicate_and_concurrent_servers_execute_effect_once(self):
        plan = self.plan()
        managers = [self.make_manager(), self.make_manager()]
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda m: asyncio.run(m.apply_plan("alice", "9000", self.request(plan))), managers))
        self.assertEqual(1, len(self.calls))
        self.assertEqual(1, len({r["operation_id"] for r in results}))
        self.assertEqual([False, True], sorted(r["deduplicated"] for r in results))
        self.assertEqual("SUCCEEDED", self.make_manager().get_operation("9000", results[0]["operation_id"], actor_id="alice")["state"])

    def test_crash_intent_cannot_repeat_an_ambiguous_effect(self):
        class Crash(BaseException):
            pass
        def crash(**scope):
            self.calls.append(scope)
            raise Crash()
        self.manager.action_handlers["diagnostics.refresh"] = crash
        plan = self.plan()
        with self.assertRaises(Crash):
            asyncio.run(self.manager.apply_plan("alice", "9000", self.request(plan)))
        replay = asyncio.run(self.make_manager().apply_plan("alice", "9000", self.request(plan)))
        self.assertEqual("UNKNOWN", replay["state"])
        self.assertTrue(replay["deduplicated"])
        self.assertFalse(replay["retryable"])
        self.assertEqual(1, len(self.calls))

    def test_unconfigured_selftest_has_no_fabricated_ids_or_success(self):
        with self.assertRaisesRegex(ValueError, "SELFTEST_NOT_CONFIGURED"):
            self.plan("selftest.start", {"test_profile_id": "small-docs-selftest"})
        plan = self.plan("selftest.stop", {"self_test_id": "unowned-id"})
        receipt = asyncio.run(self.manager.apply_plan("alice", "9000", self.request(plan)))
        self.assertEqual("BLOCKED", receipt["state"])
        self.assertIsNone(receipt["native_sprint_id"])
        self.assertIsNone(receipt["native_operation_id"])

    def test_missing_durable_store_and_policy_fail_closed(self):
        with self.assertRaises(PermissionError):
            SetupOperationsManager().create_plan("alice", "9000",
                {"schema_version": 1, "snapshot_id": "b" * 64, "action_id": "diagnostics.refresh", "arguments": {}},
                snapshot=self.snapshot)
        plan = self.plan()
        first = asyncio.run(self.manager.apply_plan("alice", "9000", self.request(plan)))
        second = self.plan("provider.probe", {"provider_id": "configured"})
        with self.assertRaisesRegex(ValueError, "IDEMPOTENCY_CONFLICT"):
            asyncio.run(self.manager.apply_plan("alice", "9000", self.request(second)))
        self.assertEqual("SUCCEEDED", first["state"])
