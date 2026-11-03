from __future__ import annotations

import asyncio
import copy
import unittest
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from fastapi import HTTPException

from nginx_qa.managed_continuity import ManagedContinuityRuntime
from nginx_qa.managed_import import ManagedImportError
from nginx_qa.sprint_types import canonical_json_bytes
from nginx_qa.telegram_launcher_store import LauncherOperations
from nginx_qa import telegram_launcher_execution as execution
from nginx_qa.telegram_launcher import POLICY_ENV, Principal
from tests import test_telegram_launcher_intake as intake_tests
from tests import test_telegram_launcher_preparation as preparation_tests


class FakeExistingClient:
    """Deterministic client of the production identity/result/review API."""
    def __init__(self, fixture, *, finish=False):
        self.fixture = fixture
        self.runtime = ManagedContinuityRuntime(fixture.importer())
        self.finish = finish
        self.starts = {}
        self.ready_callback = lambda: None
        self.lose_ack = False

    def prepare(self, instance):
        self.ready_callback()
        return {**instance, "status": "READY", "client_session_id": "fixture-existing-client"}

    def stop(self, instance, ready):
        return {**instance, "status": "STOPPED", "client_session_id": ready["client_session_id"]}

    def start(self, instance, assignments, ready):
        key = instance["operation_id"]
        if key not in self.starts:
            identity = self.runtime.current_identity(instance["project_id"], "2864")
            self.fixture.assertEqual(assignments, [identity["assignment"]["assignment_id"]])
            self.fixture.assertEqual(instance["sprint_id"], identity["sprint_id"])
            self.starts[key] = identity
            if self.finish:
                workspace = identity["workspace"]
                root = Path(workspace["actual_git_toplevel"])
                branch = workspace["assigned_branch"]
                self.fixture.git("push", "origin", "HEAD:refs/heads/" + branch, cwd=root)
                head = self.fixture.git("rev-parse", "HEAD", cwd=root)
                payload = {"assignment_id": assignments[0], "status": "DONE", "result": "Disposable fixture work verified",
                    "from_commit": identity["assignment"]["initial_head_commit"], "git_commit": head, "git_branch": branch}
                accepted = self.runtime.submit_if_managed(instance["project_id"], "2864", canonical_json_bytes(payload), "fixture-result")
                self.fixture.assertEqual("REVIEWS_PENDING", accepted.response["status"])
                review_ids = []
                for phone in ("2891", "2892"):
                    review = self.runtime.current_identity(instance["project_id"], phone)
                    review_ids.append(review["assignment"]["assignment_id"])
                    response = self.runtime.submit_if_managed(instance["project_id"], phone,
                        canonical_json_bytes({"assignment_id": review_ids[-1], "status": "APPROVE", "feedback": "Independent fixture reviewer " + phone}),
                        "fixture-review-" + phone)
                    self.fixture.assertEqual("REVIEW_ACCEPTED", response.response["status"])
                self.fixture.assertEqual(2, len(set(review_ids)))
        if self.lose_ack:
            self.lose_ack = False
            raise execution.ExecutorUnavailable("fixture lost ACK")
        return {**instance, "initial_assignment_ids": assignments, "status": "ACCEPTED", "client_session_id": ready["client_session_id"]}


class LauncherLaunchModeTests(intake_tests.LauncherIntakeFixture):
    published = preparation_tests.LauncherPreparationTests.published

    def allow_start(self, client=None):
        actions = self.principal.actions | {"start", "stop"}
        self.principal = Principal(self.principal.user_id, self.principal.chat_id, self.project_id, actions)
        self.policy["grants"][0]["actions"] = sorted(actions)
        os.environ[POLICY_ENV] = json.dumps(self.policy)
        self.host.managed_launcher_executor_factory = lambda: client

    def launch(self, operation_id):
        return asyncio.run(execution.launch(self.operations, operation_id, self.principal,
            self.policy["projects"][self.project_id], self.host))

    def stop(self, operation_id):
        return asyncio.run(execution.stop(self.operations, operation_id, self.principal,
            self.policy["projects"][self.project_id], self.host))

    def test_active_stop_preserves_history_and_retires_only_owned_resources(self):
        client = FakeExistingClient(self)
        self.allow_start(client)
        result = self.ingest()
        self.launch(result["operation_id"])
        before = self.imp.store.active_runtime_state(self.project_id)
        stopped = self.stop(result["operation_id"])
        self.assertEqual("Stopped", stopped["lifecycle"])
        state = self.imp.store.runtime_state(self.project_id, result["sprint_id"])
        self.assertEqual("stopped", state["status"])
        for field in ("assignments", "workflow", "result_receipts", "reviews", "review_assignments", "outbox"):
            self.assertEqual(before[field], state[field])
        self.assertTrue(all(lease["status"] == "released" for lease in state["branch_leases"]))
        self.assertEqual([], state["active_assignment_ids"])
        from nginx_qa.sprint_types import managed_activation_invariant_issues
        for field, value in (("operator_cancellation", None), ("status", "active"), ("active_assignment_ids", before["active_assignment_ids"])):
            damaged = copy.deepcopy(state)
            damaged[field] = value
            self.assertTrue(managed_activation_invariant_issues(damaged))
        self.assertIsNone(client.runtime.current_identity(self.project_id, "2864"))
        self.assertEqual(0, client.runtime.drain_outbox(self.project_id, result["sprint_id"]))
        client.runtime.reconcile_all()
        self.assertEqual("Stopped", self.launch(result["operation_id"])["lifecycle"])
        self.assertEqual("Stopped", self.stop(result["operation_id"])["lifecycle"])
        with self.imp.store._transaction() as connection:
            self.assertEqual(0, connection.execute("SELECT holds_slot FROM telegram_launcher_operations WHERE operation_id = ?", (result["operation_id"],)).fetchone()[0])
        self.envelope["manifest"]["git"]["assigned_branch"] = "agent/next-after-stop"
        second = self.ingest(self.update(2))
        self.assertNotEqual(result["sprint_id"], second["sprint_id"])
        self.assertEqual("Waiting", second["status"])

    def test_stop_without_client_ack_keeps_fence_and_project_reservation(self):
        client = FakeExistingClient(self)
        self.allow_start(client)
        result = self.ingest()
        self.launch(result["operation_id"])
        self.host.managed_launcher_executor_factory = lambda: None
        self.assertEqual("STOP_PENDING", self.stop(result["operation_id"])["blocked_reason"])
        self.allow_start(client)
        with patch.object(client, "stop", side_effect=execution.ExecutorUnavailable("fixture unavailable")):
            pending = self.stop(result["operation_id"])
        self.assertEqual("STOP_PENDING", pending["blocked_reason"])
        self.assertIsNone(client.runtime.current_identity(self.project_id, "2864"))
        with self.assertRaises(HTTPException):
            self.ingest(self.update(2))
        state = self.imp.store.active_runtime_state(self.project_id)
        self.assertTrue(state["operator_stop_requested"])
        self.assertTrue(any(lease["status"] == "active" for lease in state["branch_leases"]))
        self.assertEqual("STOP_PENDING", self.launch(result["operation_id"])["blocked_reason"])
        self.assertEqual("Stopped", self.stop(result["operation_id"])["lifecycle"])

    def test_stop_pending_reviews_keeps_actual_result_and_no_invented_decisions(self):
        client = FakeExistingClient(self)
        self.allow_start(client)
        result = self.ingest()
        self.launch(result["operation_id"])
        identity = client.runtime.current_identity(self.project_id, "2864")
        workspace = identity["workspace"]
        self.git("push", "origin", "HEAD:refs/heads/" + workspace["assigned_branch"], cwd=Path(workspace["actual_git_toplevel"]))
        payload = {"assignment_id": identity["assignment"]["assignment_id"], "status": "DONE", "result": "Real fixture result before operator cancellation",
            "from_commit": identity["assignment"]["initial_head_commit"], "git_commit": identity["assignment"]["initial_head_commit"], "git_branch": workspace["assigned_branch"]}
        client.runtime.submit_if_managed(self.project_id, "2864", canonical_json_bytes(payload), "fixture-pre-stop-result")
        before = self.imp.store.active_runtime_state(self.project_id)
        self.assertEqual(2, len(before["review_assignments"]))
        review = client.runtime.current_identity(self.project_id, "2891")
        with patch.object(client, "stop", side_effect=execution.ExecutorUnavailable("fixture offline")):
            self.stop(result["operation_id"])
        from nginx_qa.managed_continuity import ManagedContinuityError
        before_paths = {str(p) for p in self.base.rglob(".git")}
        with patch.object(client.runtime, "_prepare_assignment", side_effect=AssertionError("post-stop rework preparation")):
            with self.assertRaises(ManagedContinuityError):
                client.runtime.submit_if_managed(self.project_id, "2891", canonical_json_bytes({
                    "assignment_id": review["assignment"]["assignment_id"], "status": "REJECT", "feedback": "Late review after stop"}), "late-review")
        self.assertEqual(before_paths, {str(p) for p in self.base.rglob(".git")})
        self.stop(result["operation_id"])
        after = self.imp.store.runtime_state(self.project_id, result["sprint_id"])
        self.assertEqual(before["review_assignments"], after["review_assignments"])
        self.assertEqual(before["result_receipts"], after["result_receipts"])
        self.assertEqual([], after["reviews"])
        self.assertIsNone(client.runtime.current_identity(self.project_id, "2891"))

    def test_restart_resumes_authorized_auto_but_never_starts_manual_waiting(self):
        from nginx_qa.telegram_launcher_intake import reconcile_operations
        client = FakeExistingClient(self)
        self.allow_start(client)
        manual = self.ingest()
        asyncio.run(reconcile_operations(self.host))
        self.assertEqual({}, client.starts)
        self.assertEqual("Waiting", self.operations.get(manual["operation_id"], self.principal)["lifecycle"])
        self.stop(manual["operation_id"])
        self.envelope["autonomous_mode"] = True
        self.allow_start(None)
        auto = self.ingest(self.update(2))
        self.assertEqual("EXECUTOR_UNAVAILABLE", auto["blocked_reason"])
        self.allow_start(client)
        asyncio.run(reconcile_operations(self.host))
        self.assertEqual("Running", self.operations.get(auto["operation_id"], self.principal)["lifecycle"])
        self.assertEqual(1, len(client.starts))
        asyncio.run(reconcile_operations(self.host))
        self.assertEqual(1, len(client.starts))

    def test_created_and_failed_preparations_cancel_without_losing_failure_receipt(self):
        self.allow_start(None)
        created, _ = self.reserve()
        self.assertEqual("Stopped", self.stop(created["operation_id"])["lifecycle"])
        operation_id, request = self.published(number=2)
        def fail_prepare(point, _):
            if point == "after_validate":
                raise RuntimeError("fixture failed before activation")
        failed = self.importer(fault_injector=fail_prepare)
        with self.assertRaises(ManagedImportError):
            failed.prepare(self.project_id, request, operation_id=operation_id)
        before = self.imp.store.lookup(self.project_id, operation_id)
        self.assertEqual("Stopped", self.stop(operation_id)["lifecycle"])
        after = self.imp.store.lookup(self.project_id, operation_id)
        self.assertEqual(before["error"], after["error"])
        self.assertEqual(before["updated_at"], after["updated_at"])
        runtime = ManagedContinuityRuntime(self.importer())
        self.assertIsNone(runtime.current_identity(self.project_id, "2860"))
        runtime.reconcile_all()
        self.assertEqual("Stopped", self.operations.get(operation_id, self.principal)["lifecycle"])

    def native_stop(self, *, start_process):
        from tests.test_process_supervisor import ManagedProcessSupervisorTests as NativeFixture
        from nginx_qa.process_supervisor import ManagedProcessSupervisor, process_identity
        # Trusted operator fixture: uploaded process commands remain forbidden.
        checksum = NativeFixture.install_service(self)
        self.manifest["files"] = [{"path": "service.py", "sha256": checksum}]
        self.manifest["nodes"][0]["workspace"]["process"] = NativeFixture.process_spec("--mode", "serve")
        self.commit = NativeFixture.push_manifest(self, self.manifest, "trusted native fixture")
        self.manifest["git"]["expected_source_commit"] = self.commit
        start, end = NativeFixture.available_port_range(3)
        self.runtime_config.update(child_port_start=start, child_port_end=end)
        self.imp = self.importer()
        self.host.managed_sprint_importer_factory.return_value = self.imp
        self.operations = LauncherOperations(self.imp)
        record, repository = self.reserve()
        intent = self.imp.git_provider.create_launcher_manifest_commit(repository, operation_id=record["operation_id"],
            source_commit=self.commit, manifest_blob=canonical_json_bytes(self.manifest), created_at=record["created_at"])
        self.operations.update(record["operation_id"], self.principal, provision_intent=intent)
        self.imp.git_provider.publish_launcher_manifest(repository, intent)
        self.operations.update(record["operation_id"], self.principal, published=True)
        self.operations.prepare(record["operation_id"], self.principal)
        supervisor = ManagedProcessSupervisor(self.runtime_config, self.imp.store, self.port_reservations,
            health_timeout_seconds=10, stop_timeout_seconds=8, poll_interval_seconds=0.05)
        self.host.managed_process_supervisor = supervisor
        client = FakeExistingClient(self)
        self.allow_start(client)
        running = self.launch(record["operation_id"])
        try:
            if start_process:
                outcomes = supervisor.reconcile(project_id=self.project_id, sprint_id=running["sprint_id"])
                self.assertEqual(["HEALTHY"], [item.state for item in outcomes])
            before = self.imp.store.active_runtime_state(self.project_id)
            pid = before["processes"][0]["pid"]
            if start_process:
                self.assertIsNotNone(process_identity(pid))
            self.assertEqual("Stopped", self.stop(record["operation_id"])["lifecycle"])
            after = self.imp.store.runtime_state(self.project_id, running["sprint_id"])
            self.assertEqual("STOPPED" if start_process else "FAILED", after["processes"][0]["state"])
            self.assertEqual("released", after["port_leases"][0]["status"])
            if start_process:
                self.assertIsNone(process_identity(pid))
        finally:
            for context in supervisor._repository.contexts():
                if context.process["state"] in {"PREPARED", "STARTING", "HEALTHY", "STOPPING"}:
                    supervisor.stop(context.project_id, context.sprint_id, context.process["process_id"])
            supervisor.close()

    def test_active_stop_waits_for_native_process_death_and_port_release(self):
        self.native_stop(start_process=True)

    def test_stop_cancels_native_prepared_process_without_spawning(self):
        self.native_stop(start_process=False)

    def test_old_start_api_and_new_auto_import_share_one_project_guard(self):
        client = FakeExistingClient(self)
        self.allow_start(client)
        self.envelope["autonomous_mode"] = True
        def old_start():
            try:
                return self.imp.start(self.project_id, self.request).response["status"]
            except ManagedImportError as exc:
                return exc.code
        def new_start():
            try:
                return self.ingest()["status"]
            except HTTPException as exc:
                return exc.detail["error"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            old, new = pool.submit(old_start), pool.submit(new_start)
            values = [old.result(), new.result()]
        self.assertEqual(1, sum(v in {"active", "Running"} for v in values))
        self.assertIn("PROJECT_BUSY", values)
        with self.imp.store._transaction() as connection:
            self.assertEqual(1, connection.execute("SELECT COUNT(*) FROM managed_sprints WHERE status = 'active'").fetchone()[0])

    def test_cancel_serializes_with_manifest_publication(self):
        import threading
        self.allow_start(None)
        entered, release = threading.Event(), threading.Event()
        original = self.imp.git_provider.publish_launcher_manifest
        def blocked_publish(repository, intent):
            entered.set()
            if not release.wait(10):
                raise AssertionError("fixture publication release timeout")
            return original(repository, intent)
        with patch.object(self.imp.git_provider, "publish_launcher_manifest", side_effect=blocked_publish):
            with ThreadPoolExecutor(max_workers=2) as pool:
                pending = pool.submit(self.ingest)
                self.assertTrue(entered.wait(10))
                with self.imp.store._transaction() as connection:
                    operation_id = connection.execute("SELECT operation_id FROM telegram_launcher_operations").fetchone()[0]
                cancellation = pool.submit(self.stop, operation_id)
                release.set()
                pending.result()
                stopped = cancellation.result()
        self.assertEqual("Stopped", stopped["lifecycle"])
        self.assertTrue(stopped["published"])
        self.assertEqual(stopped["provision_intent"]["commit"], self.git("--git-dir", str(self.remote), "rev-parse", stopped["provision_intent"]["ref"]))
        self.assertEqual("Stopped", self.operations.get(operation_id, self.principal)["lifecycle"])

    def test_auto_uses_real_identity_result_and_two_review_apis_to_completion(self):
        client = FakeExistingClient(self, finish=True)
        self.allow_start(client)
        self.envelope["autonomous_mode"] = True
        result = self.ingest()
        self.assertEqual("Completed", result["status"])
        self.assertTrue(result["activated"])
        self.assertEqual(1, len(client.starts))
        state = self.imp.store.runtime_state(self.project_id, result["sprint_id"])
        self.assertEqual("completed", state["status"])
        self.assertEqual(2, len(state["reviews"]))
        self.assertEqual(result["operation_id"], self.ingest()["operation_id"])
        self.assertEqual(1, len(client.starts))

    def test_manual_waits_and_duplicate_concurrent_start_activates_once(self):
        client = FakeExistingClient(self)
        self.allow_start(client)
        result = self.ingest()
        self.assertEqual("Waiting", result["status"])
        self.assertEqual({}, client.starts)
        self.assert_no_activation()
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda _: self.launch(result["operation_id"]), range(3)))
        self.assertTrue(all(r["lifecycle"] == "Running" for r in results))
        self.assertEqual(1, len(client.starts))
        self.assertEqual(1, len(self.imp.store.active_runtime_state(self.project_id)["assignments"]))

    def test_missing_executor_keeps_manual_waiting_without_activation(self):
        self.allow_start(None)
        result = self.ingest()
        blocked = self.launch(result["operation_id"])
        self.assertEqual("Waiting", blocked["lifecycle"])
        self.assertEqual("EXECUTOR_UNAVAILABLE", blocked["blocked_reason"])
        self.assert_no_activation()

    def test_auto_without_start_permission_prepares_but_does_not_activate(self):
        self.envelope["autonomous_mode"] = True
        result = self.ingest()
        self.assertEqual("Prepared", result["status"])
        self.assertEqual("START_PERMISSION_REQUIRED", result["blocked_reason"])
        self.assert_no_activation()

    def test_revocation_during_executor_ready_denies_before_activation(self):
        client = FakeExistingClient(self)
        self.allow_start(client)
        result = self.ingest()
        def revoke():
            changed = json.loads(os.environ[POLICY_ENV])
            changed["grants"][0]["actions"].remove("start")
            os.environ[POLICY_ENV] = json.dumps(changed)
        client.ready_callback = revoke
        with self.assertRaises(HTTPException) as exc:
            self.launch(result["operation_id"])
        self.assertEqual(403, exc.exception.status_code)
        self.assert_no_activation()
        self.assertEqual({}, client.starts)

    def test_lost_executor_ack_retries_existing_activation_and_same_client_job(self):
        client = FakeExistingClient(self)
        client.lose_ack = True
        self.allow_start(client)
        result = self.ingest()
        blocked = self.launch(result["operation_id"])
        self.assertEqual("Prepared", blocked["lifecycle"])
        self.assertEqual("EXECUTOR_UNAVAILABLE", blocked["blocked_reason"])
        before = self.imp.store.active_runtime_state(self.project_id)
        self.operations = LauncherOperations(self.importer())
        resumed = self.launch(result["operation_id"])
        self.assertEqual("Running", resumed["lifecycle"])
        self.assertEqual(1, len(client.starts))
        self.assertEqual(before["assignments"], self.imp.store.active_runtime_state(self.project_id)["assignments"])

    def test_three_new_auto_uploads_have_one_winner(self):
        client = FakeExistingClient(self)
        self.allow_start(client)
        self.envelope["autonomous_mode"] = True
        def ingest(number):
            try:
                return self.ingest(self.update(number))["status"]
            except HTTPException as exc:
                return exc.detail["error"]
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(ingest, (1, 2, 3)))
        self.assertEqual(["PROJECT_BUSY", "PROJECT_BUSY", "Running"], sorted(results))
        self.assertEqual(1, len(client.starts))

    def recovered_preparation(self):
        operation_id, request = self.published()
        def fail_prepare(point, _):
            if point == "after_validate":
                raise RuntimeError("fixture prepare failure")
        failed = self.importer(fault_injector=fail_prepare)
        with self.assertRaises(ManagedImportError):
            failed.prepare(self.project_id, request, operation_id=operation_id)
        parent = failed.store.lookup(self.project_id, operation_id)
        self.assertEqual("Failed", self.operations.get(operation_id, self.principal)["lifecycle"])
        runtime = ManagedContinuityRuntime(self.importer())
        identity = runtime.current_identity(self.project_id, "2860")
        self.assertEqual("coordinator", identity["assignment_kind"])
        payload = {"assignment_id": identity["assignment"]["assignment_id"],
            "idempotency_key": "fixture-coordinator-retry", "action": "RETRY_IMPORT",
            "parameters": {"failed_attempt_id": parent["attempt_id"], "recovery_idempotency_key": "fixture-retry-child"}}
        result = runtime.submit_if_managed(self.project_id, "2860", canonical_json_bytes(payload), "fixture-recovery")
        self.assertEqual("RECOVERY_COMPLETED", result.response["status"])
        child_id = result.response["produced_record_ids"][0]
        runtime.reconcile_all()
        child = self.imp.store.lookup_attempt(self.project_id, child_id)
        self.assertEqual("PREPARED", child["status"])
        self.assertEqual(parent["evidence"]["launcher"], child["evidence"]["launcher"])
        self.assertEqual(parent, self.imp.store.lookup(self.project_id, operation_id))
        self.assertEqual("Waiting", self.operations.get(operation_id, self.principal)["lifecycle"])
        self.assertEqual((), self.imp.store.retryable_import_attempts())
        self.assertIsNone(runtime.current_identity(self.project_id, "2864"))
        self.assertIsNone(self.imp.store.active_runtime_state(self.project_id))
        state = self.imp.store.runtime_state(self.project_id, parent["sprint_id"])
        self.assertEqual(["failed", "running"], [item["status"] for item in state["import_attempts"]])
        self.assertEqual([], state["assignments"])
        self.assertEqual([], state["review_assignments"])
        self.assertEqual([], state["processes"])
        with self.imp.store._transaction() as connection:
            self.assertEqual(0, connection.execute("SELECT COUNT(*) FROM managed_queue_items WHERE event_type = 'ASSIGNMENT_ENQUEUE'").fetchone()[0])
        child_request = self.operations.preparation_request(operation_id, self.principal)
        self.assertEqual("fixture-retry-child", child_request.idempotency_key)
        with self.assertRaises(ManagedImportError):
            self.imp.start(self.project_id, child_request)
        return operation_id, child_request, parent

    def test_coordinator_retry_keeps_manual_owner_and_returns_to_waiting(self):
        operation_id, child_request, parent = self.recovered_preparation()
        activated = self.imp.activate_prepared(self.project_id, child_request, operation_id=operation_id)
        self.assertEqual("active", activated.response["status"])
        self.assertEqual(parent["sprint_id"], activated.response["sprint_id"])

    def test_recovered_preparation_can_stop_without_deleting_coordinator_history(self):
        operation_id, child_request, parent = self.recovered_preparation()
        before = self.imp.store.runtime_state(self.project_id, parent["sprint_id"])
        self.allow_start(None)
        self.assertEqual("Stopped", self.stop(operation_id)["lifecycle"])
        after = self.imp.store.runtime_state(self.project_id, parent["sprint_id"])
        self.assertEqual(before["outbox"], after["outbox"])
        self.assertEqual(before["coordinator_contexts"], after["coordinator_contexts"])
        self.assertEqual(before["recovery_records"], after["recovery_records"])
        self.assertEqual(parent, self.imp.store.lookup(self.project_id, operation_id))
        self.assertEqual("FAILED", self.imp.store.lookup(self.project_id, child_request.idempotency_key)["status"])


class ExistingClientAdapterContractTests(unittest.TestCase):
    def test_endpoint_is_operator_loopback_only(self):
        for endpoint in ("https://example.com", "http://localhost:1234", "http://127.0.0.1", "http://127.0.0.1:1234/admin", "http://u:p@127.0.0.1:1234", "http://127.0.0.1:bad", "http://127.0.0.1:1234/?key=value"):
            with self.subTest(endpoint=endpoint), self.assertRaises(execution.ExecutorUnavailable):
                execution.ExistingClientHTTPAdapter(endpoint, "env:FIXTURE_CLIENT", lambda _: "fixture")
        execution.ExistingClientHTTPAdapter("http://127.0.0.1:1234", "env:FIXTURE_CLIENT", lambda _: "fixture")

    def test_exact_scoped_receipt_and_session_required(self):
        expected = {"protocol": execution.PROTOCOL, "operation_id": "launch-fixture", "project_id": "p", "sprint_id": "s", "agent_phones": ["1", "2"]}
        good = {**expected, "status": "READY", "client_session_id": "fixture-session"}
        self.assertEqual(good, execution.validate_receipt(good, expected, "READY"))
        for delta in ({"operation_id": "foreign"}, {"status": "ACCEPTED"}, {"client_session_id": "bad/session"}, {"secret": "extra"}, {"agent_phones": ["1"]}):
            with self.subTest(delta=delta), self.assertRaises(execution.ExecutorUnavailable):
                execution.validate_receipt({**good, **delta}, expected, "READY")

    def test_missing_executor_is_not_a_fake_success(self):
        from types import SimpleNamespace
        with patch.dict(os.environ, {"NGINX_QA_LAUNCHER_EXECUTOR_URL": "", "NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF": ""}):
            with self.assertRaises(execution.ExecutorUnavailable):
                execution.executor_for(SimpleNamespace())


    def test_loopback_http_uses_scoped_header_and_rejects_redirects(self):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        received = []
        class Handler(BaseHTTPRequestHandler):
            redirect = False
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                received.append((self.path, self.headers.get("Authorization"), payload))
                if self.redirect:
                    self.send_response(302)
                    self.send_header("Location", "http://example.invalid/not-followed")
                    self.end_headers()
                    return
                answer = {**payload, "status": "READY", "client_session_id": "fixture-http-session"}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(answer).encode())
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            adapter = execution.ExistingClientHTTPAdapter(f"http://127.0.0.1:{server.server_port}", "env:FIXTURE", lambda _: "fixture-only-value")
            instance = {"protocol": execution.PROTOCOL, "operation_id": "o", "project_id": "p", "sprint_id": "s", "agent_phones": ["1"]}
            self.assertEqual("READY", adapter.prepare(instance)["status"])
            self.assertEqual(("/managed-client/prepare", "Bearer fixture-only-value", instance), received[0])
            Handler.redirect = True
            with self.assertRaises(execution.ExecutorUnavailable):
                adapter.prepare(instance)
            self.assertEqual(2, len(received))
        finally:
            server.shutdown()
            server.server_close()
            worker.join(2)


class LauncherReconciliationLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_unexpected_project_failure_drains_thread_backed_siblings_before_retry_or_shutdown(self):
        import sqlite3
        import threading
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        import main
        import nginx_qa.telegram_launcher_intake as intake
        for error in (sqlite3.OperationalError("fixture locked database"), AttributeError("fixture corrupt receipt")):
            with self.subTest(error=type(error).__name__):
                stop_event, entered, release = threading.Event(), threading.Event(), threading.Event()
                workers = []
                def pending(host, config, project_id, policy):
                    if project_id == "bad":
                        raise error
                    return object(), [{"user_id": "1", "chat_id": "1", "operation_id": project_id}]
                def held_worker():
                    workers.append(1)
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("fixture cleanup deadline")
                async def resume(*args):
                    await asyncio.to_thread(held_worker)
                runtime = unittest.mock.Mock()
                host = SimpleNamespace(read_git_config=AsyncMock(return_value={}))
                original = intake.reconcile_operations
                async def batch(ignored, **kwargs):
                    return await original(host, **kwargs)
                with patch.dict(os.environ, {"NGINX_QA_TELEGRAM_LAUNCHER_ENABLED": "true"}), \
                     patch.object(intake, "load_policy", return_value={"projects": {"bad": {}, "healthy": {}}}), \
                     patch.object(intake, "pending_project_operations", side_effect=pending), \
                     patch.object(execution, "current_principal", side_effect=lambda p, *args: p), \
                     patch.object(intake, "continue_operation", side_effect=resume), \
                     patch.object(intake, "reconcile_operations", side_effect=batch):
                    thread = threading.Thread(target=main._reconcile_managed_continuity_background,
                        args=(runtime, stop_event, 0.005, asyncio.get_running_loop()), daemon=True)
                    thread.start()
                    try:
                        self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                        await asyncio.sleep(0.06)
                        self.assertEqual(1, len(workers), "failed sibling must not release single-flight barrier")
                        self.assertGreater(runtime.reconcile_all.call_count, 2)
                        stop_event.set()
                        await asyncio.to_thread(thread.join, 0.03)
                        self.assertTrue(thread.is_alive(), "a failed sibling is not shutdown quiescence")
                        release.set()
                        await asyncio.to_thread(thread.join, 2)
                        self.assertFalse(thread.is_alive())
                        self.assertEqual(1, len(workers))
                    finally:
                        stop_event.set()
                        release.set()
                        await asyncio.to_thread(thread.join, 2)

    async def test_malformed_stored_executor_receipt_is_a_safe_domain_error(self):
        for field in ("executor_receipt", "executor_stop_receipt"):
            with self.subTest(field=field), self.assertRaises(HTTPException) as raised:
                LauncherOperations._validate_client_receipts({field: []})
            self.assertEqual(409, raised.exception.status_code)
            self.assertEqual("OPERATION_CORRUPT", raised.exception.detail["error"])

    async def test_background_transport_recovery_uses_the_asgi_event_loop(self):
        import threading
        import main
        from unittest.mock import Mock
        import nginx_qa.telegram_launcher_intake as intake
        loop = asyncio.get_running_loop()
        stop_event = threading.Event()
        loops = []
        async def recover(host, **kwargs):
            loops.append(asyncio.get_running_loop())
            stop_event.set()
        with patch.object(intake, "reconcile_operations", side_effect=recover):
            thread = threading.Thread(target=main._reconcile_managed_continuity_background,
                args=(Mock(), stop_event, 0.001, loop), daemon=True)
            thread.start()
            try:
                await asyncio.to_thread(thread.join, 5)
                self.assertFalse(thread.is_alive())
                self.assertEqual([loop], loops)
            finally:
                stop_event.set()


    async def test_blocked_transport_does_not_block_ticks_and_shutdown_waits_for_it(self):
        import threading
        import main
        from unittest.mock import Mock
        import nginx_qa.telegram_launcher_intake as intake
        stop_event = threading.Event()
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        async def recover(host, **kwargs):
            calls.append(1)
            entered.set()
            await release.wait()
        runtime = Mock()
        with patch.object(intake, "reconcile_operations", side_effect=recover):
            thread = threading.Thread(target=main._reconcile_managed_continuity_background,
                args=(runtime, stop_event, 0.005, asyncio.get_running_loop()), daemon=True)
            thread.start()
            try:
                await asyncio.wait_for(entered.wait(), 2)
                first = runtime.reconcile_all.call_count
                await asyncio.sleep(0.08)
                self.assertGreater(runtime.reconcile_all.call_count, first + 2)
                self.assertEqual(1, len(calls))
                stop_event.set()
                await asyncio.to_thread(thread.join, 0.03)
                self.assertTrue(thread.is_alive(), "shutdown must retain resources while recovery is in flight")
                release.set()
                await asyncio.to_thread(thread.join, 2)
                self.assertFalse(thread.is_alive())
            finally:
                stop_event.set()
                release.set()
                await asyncio.to_thread(thread.join, 2)

    async def test_failed_transport_is_observed_and_retried_without_stopping_engine(self):
        import threading
        import main
        from unittest.mock import Mock
        import nginx_qa.telegram_launcher_intake as intake
        stop_event = threading.Event()
        calls = []
        async def recover(host, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("isolated transport outage")
            stop_event.set()
        runtime = Mock()
        with patch.object(intake, "reconcile_operations", side_effect=recover):
            thread = threading.Thread(target=main._reconcile_managed_continuity_background,
                args=(runtime, stop_event, 0.005, asyncio.get_running_loop()), daemon=True)
            thread.start()
            try:
                await asyncio.to_thread(thread.join, 2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(2, len(calls))
                self.assertGreaterEqual(runtime.reconcile_all.call_count, 2)
            finally:
                stop_event.set()

    async def test_project_recovery_is_concurrent_bounded_and_stops_queued_work(self):
        import threading
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        import nginx_qa.telegram_launcher_intake as intake
        stop_event = threading.Event()
        entered, release = asyncio.Event(), asyncio.Event()
        running = set()
        seen = set()
        maximum = 0
        def pending(host, config, project_id, policy):
            return object(), [{"user_id": "1", "chat_id": "1", "operation_id": project_id}]
        async def resume(operations, record, principal, policy, host):
            nonlocal maximum
            running.add(principal.project_id)
            seen.add(principal.project_id)
            maximum = max(maximum, len(running))
            if len(running) == 4:
                entered.set()
            await release.wait()
            running.remove(principal.project_id)
        with patch.dict(os.environ, {"NGINX_QA_TELEGRAM_LAUNCHER_ENABLED": "true"}), \
             patch.object(intake, "load_policy", return_value={"projects": {str(n): {} for n in range(6)}}), \
             patch.object(intake, "pending_project_operations", side_effect=pending), \
             patch.object(execution, "current_principal", side_effect=lambda p, *args: p), \
             patch.object(intake, "continue_operation", side_effect=resume):
            task = asyncio.create_task(intake.reconcile_operations(SimpleNamespace(read_git_config=AsyncMock(return_value={})), stop_event=stop_event))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                self.assertEqual(4, len(seen))
                stop_event.set()
                release.set()
                await asyncio.wait_for(task, 2)
                self.assertEqual(4, maximum)
                self.assertEqual(4, len(seen), "queued projects must not start during shutdown")
            finally:
                stop_event.set()
                release.set()
                await task
