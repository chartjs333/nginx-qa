from __future__ import annotations

import copy
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from nginx_qa.managed_import import ManagedImportError
from nginx_qa.git_provider import RepositorySpec
from nginx_qa.managed_import import TransactionalSprintImporter
from nginx_qa.sprint_types import canonical_json_bytes
from nginx_qa.telegram_launcher import Principal
from nginx_qa import telegram_launcher_intake as intake
from fastapi import HTTPException
from nginx_qa.sprint_types import StartSprintFromGitRequest, managed_project_control_invariant_issues, managed_activation_invariant_issues
from nginx_qa.telegram_launcher_store import LauncherOperations
from tests.test_managed_import import ManagedImportFixture, SimulatedCrash
from tests import test_telegram_launcher_intake as intake_tests


class LauncherPreparationTests(intake_tests.LauncherIntakeFixture):
    def published(self, number=1):
        with patch.object(LauncherOperations, "prepare", lambda ops, operation_id, principal: ops.get(operation_id, principal)):
            result = self.ingest(self.update(number))
        operation_id = result["operation_id"]
        return operation_id, self.operations.preparation_request(operation_id, self.principal)

    def assert_parked(self, operation_id):
        operation = self.operations.get(operation_id, self.principal)
        attempt = self.imp.store.lookup(self.project_id, operation_id)
        self.assertEqual("PREPARED", attempt["status"])
        state = self.imp.store.runtime_state(self.project_id, operation["sprint_id"])
        self.assertIsNone(state["workflow"])
        self.assertEqual("preparing", state["status"])
        with self.imp.store._transaction() as connection:
            control, _ = self.imp.store._load_control(connection, self.project_id)
            self.assertIsNone(control["activation_lease"])
            self.assertEqual((), managed_project_control_invariant_issues(control))
            for table in ("managed_queue_items", "managed_process_owners", "managed_port_leases"):
                self.assertEqual(0, connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        self.assertIsNone(self.imp.store.active_runtime_state(self.project_id))
        self.assertEqual((), self.imp.store.retryable_import_attempts())
        for workspace in attempt["evidence"]["prepared_workspaces"]:
            self.assertTrue(Path(workspace["expected_root"]).is_dir())
        return attempt

    def test_manual_preparation_has_real_workspaces_but_no_work_and_releases_short_lease(self):
        operation_id, request = self.published()
        result = self.operations.prepare(operation_id, self.principal)
        self.assertEqual("Waiting", result["lifecycle"])
        self.assertEqual(operation_id, result["instance_id"])
        attempt = self.assert_parked(operation_id)
        replay = self.importer().prepare(self.project_id, request, operation_id=operation_id)
        self.assertTrue(replay.response["deduplicated"])
        self.assertEqual(attempt, self.assert_parked(operation_id))

    def test_legacy_start_and_startup_cannot_activate_manual_import(self):
        operation_id, request = self.published()
        self.operations.prepare(operation_id, self.principal)
        before = self.assert_parked(operation_id)
        for action in (lambda: self.importer().start(self.project_id, request),
                       lambda: self.importer().resume_import_attempt(self.project_id, before["attempt_id"]),
                       lambda: self.importer().prepare(self.project_id, request, operation_id="launch-" + "0" * 32)):
            with self.assertRaises(ManagedImportError):
                action()
        self.assertEqual(before, self.assert_parked(operation_id))

    def test_fresh_fence_activates_existing_prepared_workspaces_once(self):
        operation_id, request = self.published()
        self.operations.prepare(operation_id, self.principal)
        before = self.assert_parked(operation_id)
        result = self.importer().activate_prepared(self.project_id, request, operation_id=operation_id)
        self.assertEqual(201, result.http_status)
        after = self.imp.store.lookup(self.project_id, operation_id)
        self.assertGreater(after["fencing_token"], before["fencing_token"])
        self.assertEqual("SUCCEEDED", after["status"])
        state = self.imp.store.active_runtime_state(self.project_id)
        self.assertEqual(before["sprint_id"], state["sprint_id"])
        replay = self.importer().activate_prepared(self.project_id, request, operation_id=operation_id)
        self.assertTrue(replay.response["deduplicated"])
        self.assertEqual(result.response["initial_assignment_ids"], replay.response["initial_assignment_ids"])

    def test_concurrent_prepare_calls_share_one_instance(self):
        operation_id, request = self.published()
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda _: self.importer().prepare(self.project_id, request, operation_id=operation_id), range(3)))
        self.assertEqual([200, 200, 201], sorted(result.http_status for result in results))
        self.assert_parked(operation_id)

    def test_crash_before_and_after_park_replays_without_start(self):
        operation_id, request = self.published()
        for fault in ("after_workspace", "after_prepared_park"):
            def crash(point, _):
                if point == fault:
                    raise SimulatedCrash()
            with self.assertRaises(SimulatedCrash):
                self.importer(fault_injector=crash).prepare(self.project_id, request, operation_id=operation_id)
        restarted = self.importer()
        receipt = restarted.prepare(self.project_id, request, operation_id=operation_id)
        self.assertFalse(receipt.response["activated"])
        self.assert_parked(operation_id)

    def test_mutated_workspace_is_not_activated(self):
        operation_id, request = self.published()
        self.operations.prepare(operation_id, self.principal)
        attempt = self.assert_parked(operation_id)
        root = Path(attempt["evidence"]["prepared_workspaces"][0]["expected_root"])
        (root / "unexpected.txt").write_text("uncommitted", encoding="utf-8")
        with self.assertRaises(ManagedImportError):
            self.importer().activate_prepared(self.project_id, request, operation_id=operation_id)
        self.assertIsNone(self.imp.store.active_runtime_state(self.project_id))

    def test_unpublished_or_changed_request_cannot_use_reservation(self):
        record, _ = self.reserve()
        operation_id = record["operation_id"]
        request = StartSprintFromGitRequest("main", self.commit, self.manifest_path, operation_id)
        with self.assertRaises(ManagedImportError):
            self.imp.prepare(self.project_id, request, operation_id=operation_id)
        self.assertIsNone(self.imp.store.lookup(self.project_id, operation_id))

    def test_auto_preparation_still_does_not_activate_without_explicit_launch_step(self):
        self.envelope["autonomous_mode"] = True
        operation_id, _ = self.published()
        operation = self.operations.prepare(operation_id, self.principal)
        self.assertEqual("Prepared", operation["lifecycle"])
        self.assert_parked(operation_id)

    def test_source_branch_advancing_after_publication_keeps_original_source(self):
        operation_id, request = self.published()
        (self.source / "service.py").write_text("newer source", encoding="utf-8")
        self.git("add", ".", cwd=self.source)
        self.git("commit", "-m", "advance source", cwd=self.source)
        self.git("push", str(self.remote), "main", cwd=self.source)
        self.operations.prepare(operation_id, self.principal)
        attempt = self.assert_parked(operation_id)
        self.assertEqual(self.commit, attempt["workspace_source_commit"])
        root = Path(attempt["evidence"]["prepared_workspaces"][0]["expected_root"])
        self.assertEqual(self.commit, self.git("rev-parse", "HEAD", cwd=root))

    def test_changed_published_ref_is_rejected_before_import_registration(self):
        operation_id, request = self.published()
        self.git("--git-dir", str(self.remote), "update-ref", request.ref, self.commit)
        with self.assertRaises(ManagedImportError) as exc:
            self.imp.prepare(self.project_id, request, operation_id=operation_id)
        self.assertEqual("LAUNCHER_MANIFEST_CHANGED", exc.exception.code)
        self.assertIsNone(self.imp.store.lookup(self.project_id, operation_id))

    def test_expired_preparation_retains_history_and_does_not_activate(self):
        operation_id, request = self.published()
        self.operations.prepare(operation_id, self.principal)
        before = self.assert_parked(operation_id)
        later = datetime.now(timezone.utc) + timedelta(days=2)
        with self.assertRaises(ManagedImportError) as exc:
            self.importer(clock=lambda: later).activate_prepared(self.project_id, request, operation_id=operation_id)
        self.assertEqual("PREPARATION_EXPIRED", exc.exception.code)
        self.assertEqual(before, self.assert_parked(operation_id))

    def test_two_projects_use_distinct_instances_and_failure_stays_scoped(self):
        first, request = self.published()
        self.operations.prepare(first, self.principal)
        first_attempt = self.assert_parked(first)
        second_remote = self.base / "second-upstream.git"
        self.git("clone", "--bare", str(self.source), str(second_remote))
        second_registry = {"main": RepositorySpec("main", "example.invalid/acme/second", str(second_remote))}
        second_imp = TransactionalSprintImporter(self.runtime_config, second_registry, allow_local_transport=True,
                                               port_reservations=self.port_reservations)
        second_ops = LauncherOperations(second_imp)
        principal = Principal(self.principal.user_id, self.principal.chat_id, "9003", self.principal.actions)
        envelope = copy.deepcopy(self.envelope)
        envelope["project_id"] = principal.project_id
        envelope["manifest"]["git_address"] = "https://example.invalid/acme/second.git"
        repository, binding = intake.validate_source(second_imp, envelope, envelope["manifest"]["git_address"],
                                                     self.policy["projects"][self.project_id])
        record = second_ops.reserve(principal, second_ops.keys("456789", 2, 2, principal), envelope, {}, binding)
        intent = second_imp.git_provider.create_launcher_manifest_commit(repository, operation_id=record["operation_id"],
            source_commit=self.commit, manifest_blob=canonical_json_bytes(envelope["manifest"]), created_at=record["created_at"])
        second_ops.update(record["operation_id"], principal, provision_intent=intent)
        second_imp.git_provider.publish_launcher_manifest(repository, intent)
        second_ops.update(record["operation_id"], principal, published=True)
        second = second_ops.prepare(record["operation_id"], principal)
        second_attempt = second_imp.store.lookup(principal.project_id, record["operation_id"])
        second_state = second_imp.store.runtime_state(principal.project_id, second["sprint_id"])
        self.assertNotEqual(first, second["instance_id"])
        self.assertNotEqual(first_attempt["sprint_id"], second["sprint_id"])
        self.assertNotEqual(first_attempt["evidence"]["prepared_workspaces"][0]["expected_root"],
                            second_attempt["evidence"]["prepared_workspaces"][0]["expected_root"])
        root = Path(first_attempt["evidence"]["prepared_workspaces"][0]["expected_root"])
        (root / "dirty.txt").write_text("failed first project", encoding="utf-8")
        with self.assertRaises(ManagedImportError):
            self.imp.activate_prepared(self.project_id, request, operation_id=first)
        self.assertEqual(second_attempt, second_imp.store.lookup(principal.project_id, record["operation_id"]))
        self.assertEqual(second_state, second_imp.store.runtime_state(principal.project_id, second["sprint_id"]))

    def test_quota_and_invalid_operator_limits_cannot_be_overridden_by_upload(self):
        record, repository = self.reserve()
        second_principal = Principal("123456", "123456", "9003", self.principal.actions)
        second = copy.deepcopy(self.envelope)
        second["project_id"] = "9003"
        with patch.dict(os.environ, {"NGINX_QA_LAUNCHER_PREPARED_LIMIT": "1"}):
            with self.assertRaises(HTTPException) as exc:
                self.operations.reserve(second_principal, self.operations.keys("456789", 2, 2, second_principal),
                    second, {}, intake.repository_binding(repository.spec))
        self.assertEqual("PREPARED_QUOTA_EXCEEDED", exc.exception.detail["error"])
        with patch.dict(os.environ, {"NGINX_QA_LAUNCHER_PREPARED_TTL_SECONDS": "0"}):
            with self.assertRaises(HTTPException) as exc:
                self.reserve(3)
        self.assertEqual("LIMIT_CONFIG_INVALID", exc.exception.detail["error"])

    def test_cancelled_manual_run_preserves_history_then_next_run_gets_new_namespace(self):
        first, request = self.published()
        self.operations.prepare(first, self.principal)
        before = self.assert_parked(first)
        stop_principal = Principal(self.principal.user_id, self.principal.chat_id, self.project_id,
                                   self.principal.actions | {"stop"})
        with self.assertRaises(HTTPException):
            self.operations.cancel_prepared(first, self.principal)
        stopped = self.operations.cancel_prepared(first, stop_principal)
        self.assertEqual("Stopped", stopped["lifecycle"])
        self.assertEqual(stopped, self.operations.cancel_prepared(first, stop_principal))
        saved = self.imp.store.runtime_state(self.project_id, before["sprint_id"])
        self.assertEqual("failed", saved["status"])
        forged = copy.deepcopy(saved)
        forged["import_attempts"][0]["status"] = "running"
        issues = managed_activation_invariant_issues(forged)
        self.assertIn("PREPARATION_CANCELLATION_INVALID", issues)
        self.assertIn("TERMINAL_FAILURE_EVIDENCE_MISSING", issues)
        forged = copy.deepcopy(saved)
        forged["preparation_cancellation"]["attempt_id"] = "another-attempt"
        self.assertIn("PREPARATION_CANCELLATION_INVALID", managed_activation_invariant_issues(forged))
        old_root = Path(before["evidence"]["prepared_workspaces"][0]["expected_root"])
        self.assertTrue(old_root.is_dir())
        second, _ = self.published(2)
        self.operations.prepare(second, self.principal)
        second_attempt = self.assert_parked(second)
        self.assertNotEqual(first, second)
        self.assertNotEqual(before["sprint_id"], second_attempt["sprint_id"])
        self.assertNotEqual(str(old_root), second_attempt["evidence"]["prepared_workspaces"][0]["expected_root"])
        self.assertEqual(saved, self.imp.store.runtime_state(self.project_id, before["sprint_id"]))
        with self.assertRaises(ManagedImportError):
            self.imp.activate_prepared(self.project_id, request, operation_id=first)

    def test_cancel_cannot_touch_an_activated_instance(self):
        operation_id, request = self.published()
        self.operations.prepare(operation_id, self.principal)
        self.imp.activate_prepared(self.project_id, request, operation_id=operation_id)
        before = self.imp.store.active_runtime_state(self.project_id)
        stop_principal = Principal(self.principal.user_id, self.principal.chat_id, self.project_id,
                                   self.principal.actions | {"stop"})
        with self.assertRaises(ManagedImportError):
            self.operations.cancel_prepared(operation_id, stop_principal)
        self.assertEqual(before, self.imp.store.active_runtime_state(self.project_id))

    def test_prepare_without_reservation_cannot_fall_through_to_legacy_activation(self):
        for operation_id in (None, "", "not-a-reservation", False):
            with self.assertRaises(ManagedImportError):
                self.imp.prepare(self.project_id, self.request, operation_id=operation_id)
        self.assertFalse(self.imp.store.database_path.exists())
