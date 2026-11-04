"""Private ingress exercises real native prepare/activation/cancellation in temp Git."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from fastapi import HTTPException

from nginx_qa.setup_native import NativeSelfTest, OperatorPrincipal, OperatorOperations, reconcile_operator_receipts
from nginx_qa.setup_policy import SetupPolicy
from nginx_qa.git_provider import RepositorySpec
from nginx_qa.telegram_launcher_store import LauncherOperations
from nginx_qa.telegram_launcher import Principal
from nginx_qa.sprint_types import canonical_json_bytes
from tests.test_managed_import import ManagedImportFixture
from tests.test_telegram_launcher_launch_modes import FakeExistingClient


class NativeSetupTests(ManagedImportFixture):
    def setUp(self):
        super().setUp()
        self.imp = self.importer()
        self.manifest["git"]["expected_source_commit"] = self.commit
        self.git("checkout", "-b", "launcher/setup-fixture", cwd=self.source)
        (self.source / self.manifest_path).write_bytes(canonical_json_bytes(self.manifest))
        self.git("add", self.manifest_path, cwd=self.source)
        self.git("commit", "-m", "Publish pinned fixture manifest", cwd=self.source)
        published = self.git("rev-parse", "HEAD", cwd=self.source)
        self.git("push", str(self.remote), "HEAD:refs/heads/launcher/setup-fixture", cwd=self.source)
        self.policy_file = self.base / "setup-policy.json"
        self.policy_data = {"schema_version": 1, "grants": [
            {"actor_id": "alice", "projects": ["9000", self.project_id],
             "actions": ["read", "selftest.start", "selftest.stop"]}],
            "selftest_profiles": {"small-docs-selftest": {"target_project_id": self.project_id,
                "repository_id": "main", "ref": "refs/heads/launcher/setup-fixture", "commit": published,
                "manifest_path": self.manifest_path, "manifest": self.manifest}}}
        self.policy_file.write_text(json.dumps(self.policy_data), encoding="utf-8")
        self.client = FakeExistingClient(self)
        self.host = SimpleNamespace(load_managed_runtime_config=lambda: self.runtime_config,
            managed_sprint_importer_factory=lambda c, r: self.imp)
        self.worker = SimpleNamespace(native_executor=lambda **scope: self.client,
                                      active_binding=lambda **scope: {"binding_id": "binding-fixture"},
                                      original_executor=lambda binding_id, **scope: self.client)
        self.service = NativeSelfTest(self.host, SetupPolicy(self.policy_file), lambda: {}, self.worker, self.imp.store.database_path)
        source_registry = {"main": RepositorySpec(repository_id="main", canonical_remote="example.invalid/other/source",
                                                  transport_url=str(self.remote))}
        self.service._project = lambda project: ({}, {}, self.registry if project == self.project_id else source_registry)
        self.scope = {"actor_id": "alice", "project_id": "9000", "authority_fingerprint": "a" * 64,
                      "arguments": {"test_profile_id": "small-docs-selftest"}, "operation_id": "op-test-fixture"}

    def test_real_prepare_activate_stop_and_cross_ingress_ownership(self):
        record = self.service.start(**self.scope)
        self.assertEqual("Running", record["lifecycle"])
        self.assertTrue(record["sprint_id"].startswith("msv1-"))
        state = self.imp.store.runtime_state(self.project_id, record["sprint_id"])
        self.assertEqual("active", state["status"])
        self.assertEqual(1, len(state["active_assignment_ids"]))
        receipt = self.service.status("alice", "9000", record["operation_id"])
        self.assertEqual("APPLYING", receipt["state"])
        self.assertEqual(record["operation_id"], self.service.status("alice", "9000", "op-test-fixture")["native_operation_id"])
        # Telegram cannot become the private operator even when project is equal.
        with self.assertRaises(HTTPException):
            LauncherOperations(self.imp).get(record["operation_id"], Principal("alice", "alice", self.project_id, frozenset({"read"})))
        with self.assertRaises(PermissionError):
            self.service.status("bob", "9000", record["operation_id"])
        with self.assertRaises(PermissionError):
            self.service.status("alice", "9001", record["operation_id"])
        stopped = self.service.stop(**{**self.scope, "operation_id": "op-stop-fixture",
                                      "arguments": {"self_test_id": record["operation_id"]}})
        self.assertEqual("SUCCEEDED", stopped["state"])
        final = self.imp.store.runtime_state(self.project_id, record["sprint_id"])
        self.assertEqual("stopped", final["status"])
        self.assertEqual([], final["active_assignment_ids"])
        self.assertEqual(state["assignments"], final["assignments"])

    def test_native_success_requires_actual_result_and_two_reviews(self):
        self.client.finish = True
        record = self.service.start(**self.scope)
        self.assertEqual("Completed", record["lifecycle"])
        status = self.service.status("alice", "9000", record["operation_id"])
        self.assertEqual("SUCCEEDED", status["state"])
        state = self.imp.store.runtime_state(self.project_id, record["sprint_id"])
        self.assertEqual(2, len(state["reviews"]))
        self.assertEqual("completed", state["status"])

    def test_target_grant_revocation_and_repository_alias_are_rejected(self):
        self.policy_data["grants"][0]["projects"] = ["9000"]
        self.policy_file.write_text(json.dumps(self.policy_data), encoding="utf-8")
        with self.assertRaises(PermissionError):
            self.service.start(**self.scope)
        self.policy_data["grants"][0]["projects"].append(self.project_id)
        self.policy_file.write_text(json.dumps(self.policy_data), encoding="utf-8")
        self.service._project = lambda project: ({}, {}, self.registry)
        with self.assertRaisesRegex(ValueError, "SELFTEST_REPOSITORY_NOT_ISOLATED"):
            self.service.start(**self.scope)

    def test_profile_change_during_ready_prevents_activation(self):
        def changed():
            self.policy_data['selftest_profiles']['small-docs-selftest']['manifest_path'] = 'different.json'
            self.policy_file.write_text(json.dumps(self.policy_data), encoding='utf-8')
        self.client.ready_callback = changed
        with self.assertRaisesRegex(ValueError, 'START_AUTHORITY_CHANGED'):
            self.service.start(**self.scope)
        self.assertEqual({}, self.client.starts)
        self.assertIsNone(self.imp.store.active_runtime_state(self.project_id))

    def test_revoking_start_keeps_original_owned_stop_and_no_telegram_notice(self):
        record = self.service.start(**self.scope)
        self.policy_data['grants'][0]['actions'].remove('selftest.start')
        self.policy_file.write_text(json.dumps(self.policy_data), encoding='utf-8')
        stopped = self.service.stop(**{**self.scope, 'arguments': {'self_test_id':record['operation_id']}})
        self.assertEqual('SUCCEEDED', stopped['state'])
        with self.imp.store._transaction() as connection:
            self.assertEqual(0, connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])

    def test_existing_maintenance_tick_retires_async_completed_private_slot(self):
        from nginx_qa.telegram_launcher_execution import descriptor
        record = self.service.start(**self.scope)
        assignments = record['executor_receipt']['initial_assignment_ids']
        self.client.starts.clear()
        self.client.finish = True
        self.client.start(descriptor(record), assignments, record['executor_ready'])
        with self.imp.store._transaction() as connection:
            self.assertEqual(1, connection.execute('SELECT holds_slot FROM telegram_launcher_operations WHERE operation_id=?',
                                                  (record['operation_id'],)).fetchone()[0])
        reconcile_operator_receipts(self.client.runtime)
        with self.imp.store._transaction() as connection:
            row = connection.execute('SELECT holds_slot,record_json FROM telegram_launcher_operations WHERE operation_id=?',
                                     (record['operation_id'],)).fetchone()
            self.assertEqual(0, row[0])
            self.assertEqual('Completed', json.loads(row[1])['lifecycle'])
            self.assertEqual(0, connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])
