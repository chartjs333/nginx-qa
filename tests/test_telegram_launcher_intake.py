from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException

from nginx_qa import telegram_launcher as launcher
from nginx_qa import telegram_launcher_intake as intake
from nginx_qa.git_provider import ManagedGitError
from nginx_qa.managed_import import ManagedImportError
from nginx_qa.sprint_types import StartSprintFromGitRequest, canonical_json_bytes, canonical_json_sha256
from nginx_qa.telegram_launcher_store import LauncherOperations
from tests.test_managed_import import ManagedImportFixture
from tests.test_telegram_launcher_prompt import message, policy, request


class LauncherIntakeTests(ManagedImportFixture):
    def setUp(self):
        super().setUp()
        self.imp = self.importer()
        self.operations = LauncherOperations(self.imp)
        self.principal = launcher.Principal("123456", "123456", self.project_id,
            frozenset({"prepare", "manifest_publish", "read"}))
        self.manifest["git"]["expected_source_commit"] = self.commit
        self.envelope = {"kind": intake.KIND, "schema_version": 1, "project_id": self.project_id,
                         "autonomous_mode": False, "manifest": self.manifest}
        self.policy = policy()
        self.policy["projects"] = {self.project_id: {"repository_id": "main", "source_ref": "refs/heads/main", "checksum_paths": ["service.py"]}}
        self.policy["grants"][0].update(project_id=self.project_id, actions=sorted(self.principal.actions))
        self.env = patch.dict(os.environ, {launcher.ENABLED_ENV: "true", launcher.POLICY_ENV: json.dumps(self.policy),
            "TELEGRAM_WEBHOOK_SECRET": "test-webhook", "TELEGRAM_BOT_TOKEN": "456789:test-token-for-fixture"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.host = SimpleNamespace(read_git_config=AsyncMock(return_value={}),
            project_for_group_api=Mock(return_value=("key", self.project_id, {"git_address": self.manifest["git_address"]}, {})),
            managed_repository_registry_for_project=Mock(return_value=self.registry),
            load_managed_runtime_config=Mock(return_value=self.runtime_config),
            managed_sprint_importer_factory=Mock(return_value=self.imp), telegram_api_json=Mock())

    def update(self, number=1, envelope=None):
        return {"update_id": number, "message": {**message(json.dumps(envelope or self.envelope)), "message_id": number}}

    def ingest(self, update=None):
        return asyncio.run(launcher.maybe_handle_prompt(request(update or self.update()), self.host))

    def reserve(self, number=1):
        repository, binding = intake.validate_source(self.imp, self.envelope, self.manifest["git_address"], self.policy["projects"][self.project_id])
        record = self.operations.reserve(self.principal, self.operations.keys("456789", number, number, self.principal),
            self.envelope, {"document_id": None, "document_unique_id": None}, binding)
        return record, repository

    def assert_no_activation(self):
        self.assertFalse((Path(self.runtime_config["managed_root"]) / "projects").exists())
        if self.imp.store.database_path.exists():
            with self.imp.store._transaction() as connection:
                for table in ("managed_sprints", "managed_queue_items", "managed_process_owners", "managed_port_leases"):
                    self.assertEqual(0, connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def test_text_upload_publishes_exact_manifest_replays_and_preserves_main(self):
        first = self.ingest()
        self.assertEqual("Created", first["status"])
        self.assertEqual("PREPARATION_REQUIRED", first["blocked_reason"])
        self.assertFalse(first["activated"])
        record = self.operations.get(first["operation_id"], self.principal)
        intent = record["provision_intent"]
        self.assertTrue(record["published"])
        self.assertEqual(self.commit, self.git("--git-dir", str(self.remote), "rev-parse", "main"))
        self.assertEqual(intent["commit"], self.git("--git-dir", str(self.remote), "rev-parse", intent["ref"]))
        blob = self.git("--git-dir", str(self.remote), "show", f"{intent['commit']}:{intent['manifest_path']}")
        self.assertEqual(self.manifest, json.loads(blob))
        self.assertEqual(self.commit, self.git("--git-dir", str(self.remote), "rev-parse", intent["commit"] + "^"))
        again = self.ingest()
        self.assertEqual(first["operation_id"], again["operation_id"])
        self.assertTrue(again["deduplicated"])
        self.assert_no_activation()

    def test_missing_publication_permission_is_resumable_not_fake_success(self):
        self.policy["grants"][0]["actions"].remove("manifest_publish")
        os.environ[launcher.POLICY_ENV] = json.dumps(self.policy)
        blocked = self.ingest()
        self.assertEqual("PUBLICATION_REQUIRED", blocked["blocked_reason"])
        self.assertIsNone(blocked["instance_id"])
        self.assertEqual("", self.git("--git-dir", str(self.remote), "for-each-ref", "refs/heads/launcher/"))
        self.policy["grants"][0]["actions"].append("manifest_publish")
        os.environ[launcher.POLICY_ENV] = json.dumps(self.policy)
        resumed = self.ingest()
        self.assertEqual(blocked["operation_id"], resumed["operation_id"])
        self.assertEqual("PREPARATION_REQUIRED", resumed["blocked_reason"])
        self.assert_no_activation()

    def test_malformed_unauthorized_and_invalid_graph_create_nothing(self):
        invalid = []
        for field, value in (("autonomous_mode", "true"), ("schema_version", 2), ("runtime_root", "C:/outside")):
            candidate = copy.deepcopy(self.envelope)
            candidate[field] = value
            invalid.append(candidate)
        broken = copy.deepcopy(self.envelope)
        broken["manifest"]["nodes"][0]["transitions"]["DONE"] = "missing"
        invalid.append(broken)
        broken = copy.deepcopy(self.envelope)
        broken["manifest"]["git"]["assigned_branch"] = "main"
        invalid.append(broken)
        broken = copy.deepcopy(self.envelope)
        broken["manifest"]["files"][0]["sha256"] = "0" * 64
        invalid.append(broken)
        for candidate in invalid:
            with self.assertRaises(HTTPException):
                self.ingest(self.update(envelope=candidate))
        denied = self.update()
        denied["message"]["from"]["id"] = 999
        with self.assertRaises(HTTPException) as exc:
            self.ingest(denied)
        self.assertEqual(403, exc.exception.status_code)
        self.assertFalse(self.imp.store.database_path.exists())
        self.assert_no_activation()

    def test_conflicting_identity_and_foreign_user_do_not_replay(self):
        self.ingest()
        changed = copy.deepcopy(self.envelope)
        changed["autonomous_mode"] = True
        with self.assertRaises(HTTPException) as exc:
            self.ingest(self.update(envelope=changed))
        self.assertEqual("IDEMPOTENCY_CONFLICT", exc.exception.detail["error"])
        keys = self.operations.keys("456789", 1, 1, self.principal)
        other = launcher.Principal("999", "999", self.project_id, frozenset({"prepare"}))
        with self.assertRaises(HTTPException) as exc:
            self.operations.lookup(other, keys, self.envelope, {"document_id": None, "document_unique_id": None})
        self.assertEqual(403, exc.exception.status_code)

    def test_replay_cannot_publish_after_source_policy_is_revoked(self):
        self.policy["grants"][0]["actions"].remove("manifest_publish")
        os.environ[launcher.POLICY_ENV] = json.dumps(self.policy)
        record = self.ingest()
        self.policy["grants"][0]["actions"].append("manifest_publish")
        self.policy["projects"][self.project_id]["source_ref"] = "refs/heads/new-policy"
        os.environ[launcher.POLICY_ENV] = json.dumps(self.policy)
        with self.assertRaises(HTTPException) as exc:
            self.ingest()
        self.assertEqual("SOURCE_REF_FORBIDDEN", exc.exception.detail["error"])
        self.assertIsNone(self.operations.get(record["operation_id"], self.principal)["provision_intent"])

    def test_three_concurrent_reservations_have_one_winner(self):
        _, binding = intake.validate_source(self.imp, self.envelope, self.manifest["git_address"], self.policy["projects"][self.project_id])
        def reserve(number):
            try:
                return self.operations.reserve(self.principal, self.operations.keys("456789", number, number, self.principal), self.envelope, {}, binding)["operation_id"]
            except ManagedImportError as exc:
                return exc.code
        with ThreadPoolExecutor(max_workers=3) as pool:
            result = list(pool.map(reserve, (1, 2, 3)))
        self.assertEqual(2, result.count("PROJECT_BUSY"))
        self.assertEqual(1, sum(r.startswith("launch-") for r in result))
        self.assert_no_activation()

    def test_existing_import_and_new_reservation_exclude_each_other(self):
        record, _ = self.reserve()
        request_value = StartSprintFromGitRequest("main", "refs/heads/main", self.manifest_path, "old-start")
        with self.assertRaises(ManagedImportError) as exc:
            self.imp.start(self.project_id, request_value)
        self.assertEqual("PROJECT_BUSY", exc.exception.code)
        self.assertEqual(record["operation_id"], self.operations.get(record["operation_id"], self.principal)["operation_id"])
        self.assert_no_activation()

    def test_old_inflight_import_prevents_reservation_even_after_lease_expiry(self):
        req = StartSprintFromGitRequest("main", "refs/heads/main", self.manifest_path, "old-first")
        self.imp.store.register_or_resume(self.project_id, req, None, None,
            clock=lambda: datetime(2020, 1, 1, tzinfo=timezone.utc), attempt_id_factory=lambda: "old-attempt")
        with self.assertRaises(HTTPException) as exc:
            self.reserve()
        self.assertEqual("PROJECT_BUSY", exc.exception.detail["error"])
        with self.imp.store._transaction() as connection:
            self.assertEqual(0, connection.execute("SELECT COUNT(*) FROM telegram_launcher_operations").fetchone()[0])

    def test_real_active_engine_sprint_is_preserved(self):
        req = StartSprintFromGitRequest("main", "refs/heads/main", self.manifest_path, "existing-active")
        started = self.imp.start(self.project_id, req)
        before = self.imp.store.active_runtime_state(self.project_id)
        self.assertEqual("active", started.response["status"])
        with self.assertRaises(HTTPException) as exc:
            self.reserve()
        self.assertEqual("PROJECT_BUSY", exc.exception.detail["error"])
        self.assertEqual(before, self.imp.store.active_runtime_state(self.project_id))

    def test_old_registration_racing_reservation_has_one_winner(self):
        import threading
        _, binding = intake.validate_source(self.imp, self.envelope, self.manifest["git_address"], self.policy["projects"][self.project_id])
        barrier = threading.Barrier(2)
        def old():
            barrier.wait()
            try:
                self.imp.store.register_or_resume(self.project_id,
                    StartSprintFromGitRequest("main", "refs/heads/main", self.manifest_path, "old-race"), None, None,
                    clock=lambda: datetime.now(timezone.utc), attempt_id_factory=lambda: "old-race-attempt")
                return "won"
            except ManagedImportError as exc:
                return exc.code
        def new():
            barrier.wait()
            try:
                self.operations.reserve(self.principal, self.operations.keys("456789", 1, 1, self.principal), self.envelope, {}, binding)
                return "won"
            except (HTTPException, ManagedImportError) as exc:
                return exc.code if isinstance(exc, ManagedImportError) else exc.detail["error"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            a, b = pool.submit(old), pool.submit(new)
            result = [a.result(), b.result()]
        self.assertCountEqual(["won", "PROJECT_BUSY"], result)

    def test_provision_restarts_reuse_commit_and_lost_ack_is_reconciled(self):
        record, repository = self.reserve()
        provider = self.imp.git_provider
        self.assertEqual(self.commit, provider.pinned_commit(repository,
            owner_id="launcher-source:" + canonical_json_sha256(self.manifest)))
        kwargs = dict(operation_id=record["operation_id"], source_commit=self.commit,
            manifest_blob=canonical_json_bytes(self.manifest), created_at=record["created_at"])
        intent = provider.create_launcher_manifest_commit(repository, **kwargs)
        self.assertEqual(intent, provider.create_launcher_manifest_commit(repository, **kwargs))
        self.operations.update(record["operation_id"], self.principal, provision_intent=intent)
        original = provider.runner.run
        def lost_ack(args, **kw):
            result = original(args, **kw)
            if "push" in args:
                raise ManagedGitError("MANIFEST_PUBLICATION_FAILED", "lost ACK")
            return result
        with patch.object(provider.runner, "run", side_effect=lost_ack):
            provider.publish_launcher_manifest(repository, intent)
        # Crash after remote commit, before setting published. The same delivery
        # discovers the original intent and confirms the exact existing ref.
        result = self.ingest()
        self.assertEqual(record["operation_id"], result["operation_id"])
        self.assertTrue(self.operations.get(record["operation_id"], self.principal)["published"])
        self.assert_no_activation()

    def test_publication_never_overwrites_an_existing_topic(self):
        record, repository = self.reserve()
        provider = self.imp.git_provider
        intent = provider.create_launcher_manifest_commit(repository, operation_id=record["operation_id"],
            source_commit=self.commit, manifest_blob=canonical_json_bytes(self.manifest), created_at=record["created_at"])
        self.git("--git-dir", str(self.remote), "update-ref", intent["ref"], self.commit)
        with self.assertRaises(ManagedGitError) as exc:
            provider.publish_launcher_manifest(repository, intent)
        self.assertEqual("MANIFEST_PUBLICATION_CONFLICT", exc.exception.code)
        self.assertEqual(self.commit, self.git("--git-dir", str(self.remote), "rev-parse", intent["ref"]))

    def test_document_authority_is_checked_before_download_and_after_await(self):
        update = self.update()
        update["message"].pop("text")
        update["message"]["document"] = {"file_name": "managed-sprint.json", "file_id": "file", "file_unique_id": "unique"}
        denied = copy.deepcopy(update)
        denied["message"]["from"]["id"] = 999
        with patch.object(intake, "download_document") as download:
            with self.assertRaises(HTTPException):
                self.ingest(denied)
            download.assert_not_called()
        def revoke(*args):
            replacement = copy.deepcopy(self.policy)
            replacement["grants"][0]["actions"] = ["read"]
            os.environ[launcher.POLICY_ENV] = json.dumps(replacement)
            return canonical_json_bytes(self.envelope)
        with patch.object(intake, "download_document", side_effect=revoke):
            with self.assertRaises(HTTPException) as exc:
                self.ingest(update)
        self.assertEqual(403, exc.exception.status_code)
        self.assertFalse(self.imp.store.database_path.exists())

    def test_document_success_and_actual_byte_bound(self):
        update = self.update()
        update["message"].pop("text")
        update["message"]["document"] = {"file_name": "managed-sprint.json", "file_id": "file", "file_unique_id": "unique"}
        self.host.telegram_api_json.return_value = {"ok": True, "result": {"file_path": "documents/file.json"}}
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = canonical_json_bytes(self.envelope)
        opener = Mock()
        opener.open.return_value = response
        with patch.object(intake.urllib.request, "build_opener", return_value=opener):
            result = self.ingest(update)
            self.assertEqual("Created", result["status"])
            response.read.assert_called_once_with(launcher.MAX_UPDATE_BYTES + 1)
            response.read.return_value = b"x" * (launcher.MAX_UPDATE_BYTES + 1)
            with self.assertRaises(HTTPException) as exc:
                intake.download_document(self.host, "fixture-token", update["message"]["document"])
        self.assertEqual(413, exc.exception.status_code)
        self.assert_no_activation()

    def test_strict_limits_and_legacy_fallthrough(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b'{"x":"\xff"}',
                    b'{"a":' + b'[' * 65 + b'0' + b']' * 65 + b'}',
                    json.dumps({"a": [0] * 32769}).encode()):
            with self.assertRaises(HTTPException):
                intake.bounded_object(raw)
        fenced = intake.text_object("```json\n" + json.dumps(self.envelope) + "\n```")
        self.assertEqual(self.envelope, fenced)
        self.assertIsNone(self.ingest(self.update(envelope={"git_address": self.manifest["git_address"], "agents": {}})))
        import main
        with self.assertRaises(HTTPException) as exc:
            main.legacy_sprint_import_dispatch(self.manifest)
        self.assertIn("MANAGED_GIT_START_REQUIRED", str(exc.exception.detail))
        self.assertFalse(self.imp.store.database_path.exists())

    def test_terminal_workspace_extension_types_do_not_raise_typeerror(self):
        # Terminal extension fields are allowed by the installed schema. They
        # neither declare an executable process nor change dispatch into 500.
        self.policy["grants"][0]["actions"].remove("manifest_publish")
        os.environ[launcher.POLICY_ENV] = json.dumps(self.policy)
        for index, value in enumerate((None, False, 1), 1):
            candidate = copy.deepcopy(self.envelope)
            terminal = next(n for n in candidate["manifest"]["nodes"] if n.get("type") == "terminal")
            terminal["workspace"] = value
            intake.validate_envelope(candidate)
            try:
                result = self.ingest(self.update(index, candidate))
                self.assertEqual("Created", result["status"])
            except HTTPException as exc:
                self.assertEqual(409, exc.status_code)
                self.assertEqual("PROJECT_BUSY", exc.detail["error"])
        self.assert_no_activation()

    def test_unselected_legacy_documents_channels_and_text_keep_dispatch(self):
        import main
        self.policy["grants"][0]["actions"] = ["prompt"]
        os.environ[launcher.POLICY_ENV] = json.dumps(self.policy)
        legacy = {"agents": {"items": []}}
        cases = [
            {"update_id": 1, "channel_post": {"text": json.dumps(legacy)}},
            {"update_id": 2, "message": {**message(), "text": None,
                "document": {"file_name": "agents.json", "file_id": "old", "file_unique_id": "old-unique"}}},
            {"update_id": 3, "message": message('{"agents":{},"old":1,"old":2}')},
            {"update_id": 4, "message": message('{"agents":{},"extension":NaN}')},
        ]
        for update in cases:
            req = request(update)
            async def legacy_handler(current_request, mode):
                self.assertIs(req, current_request)
                self.assertEqual(update, json.loads(await current_request.body()))
                return {"legacy": True}
            with patch.object(main, "_telegram_actor_import_for_mode", side_effect=legacy_handler) as old, patch.object(intake, "download_document") as download:
                self.assertEqual({"legacy": True}, asyncio.run(main.telegram_actor_import_for_mode(req, "sequential")))
                old.assert_called_once()
                download.assert_not_called()
        self.assertFalse(self.imp.store.database_path.exists())

    def test_marked_unsupported_updates_and_namespace_cannot_use_legacy(self):
        import main
        for key in ("channel_post", "edited_message", "edited_channel_post"):
            update = {"update_id": 1, key: self.update()["message"]}
            with self.assertRaises(HTTPException) as exc:
                self.ingest(update)
            self.assertEqual("UNSUPPORTED_UPDATE", exc.exception.detail["error"])
        for kind in (intake.KIND, "telegram_managed_sprint_v999"):
            with self.assertRaises(HTTPException) as exc:
                main.legacy_sprint_import_dispatch({"kind": kind, "agents": {"items": []}})
            self.assertEqual("LAUNCHER_TRANSPORT_REQUIRED", exc.exception.detail["error"])
        ambiguous = '{"kind":"telegram_managed_sprint_v1","kind":"old","agents":{}}'
        self.assertTrue(intake.launcher_text_selected(ambiguous))
        with self.assertRaises(HTTPException) as exc:
            self.ingest({"update_id": 1, "message": message(ambiguous)})
        self.assertEqual("INVALID_JSON", exc.exception.detail["error"])
        self.assertFalse(self.imp.store.database_path.exists())

    def test_explicit_document_caption_binds_project_before_and_after_download(self):
        update = self.update()
        update["message"].pop("text")
        update["message"].update(document={"file_name": "any.json", "file_id": "file", "file_unique_id": "unique"},
                                  caption=f"/sprint_import {self.project_id}")
        mismatched = copy.deepcopy(self.envelope)
        mismatched["project_id"] = "9001"
        with patch.object(intake, "download_document", return_value=canonical_json_bytes(mismatched)):
            with self.assertRaises(HTTPException) as exc:
                self.ingest(update)
        self.assertEqual("PROJECT_IDENTITY_MISMATCH", exc.exception.detail["error"])
        update["message"]["caption"] = "/sprint_import 9001"
        with patch.object(intake, "download_document") as download:
            with self.assertRaises(HTTPException) as exc:
                self.ingest(update)
            download.assert_not_called()
        self.assertEqual(403, exc.exception.status_code)
        update["message"]["caption"] = f"/sprint_import {self.project_id}"
        self.policy["grants"][0]["actions"].remove("manifest_publish")
        os.environ[launcher.POLICY_ENV] = json.dumps(self.policy)
        with patch.object(intake, "download_document", return_value=canonical_json_bytes(self.envelope)):
            result = self.ingest(update)
        self.assertEqual("PUBLICATION_REQUIRED", result["blocked_reason"])


if __name__ == "__main__":
    unittest.main()
