"""Targeted email reconciliation on real isolated managed/parallel adapters.

Existing harnesses redirect storage to fresh temporary directories. Tests call
ASGI in process, never bind a listener or access a serving instance.
"""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import main
from nginx_qa import scope_workflow as flow
from nginx_qa.decision_notification_runtime import IDENTITY_FIELDS, notification_snapshot
from nginx_qa.decision_notifications import EmailConfig
from nginx_qa.scope_runtime_adapters import assignment_identity, runtime_kind
from tests import test_groups as group_fixtures
from tests import test_managed_continuity as managed_fixtures
from tests.test_email_decision_api import request_http
from tests.test_sprint_type_contract import COMMIT, SPRINT_ID, TIMESTAMP


class RecordingProvider:
    def __init__(self):
        self.records = []

    def send(self, record):
        self.records.append(deepcopy(record))


def add_pending(state, assignment_id, *, existing_authorization=False):
    assignment = assignment_identity(state, assignment_id)
    identity = {field: deepcopy(assignment.get(field)) for field in IDENTITY_FIELDS}
    payload = {
        "assignment_id": assignment_id, "idempotency_key": "runtime-email-pending",
        "expected_execution_revision": flow.execution_revision(state),
        "expected_scope_revision": flow.scope_revision(state),
        "reason": "An additional proof prerequisite requires a human decision",
        "proposal": {"instructions": "Permit an isolated proof prerequisite",
            "retained_restrictions": ["No deployment", "Keep all review gates"],
            "node_ids": [str(assignment.get("node_id") or assignment["agent_id"])], "reviewer_ids": []},
        "source": {"repository_key": "github.com/example/runtime-email", "commit": COMMIT,
            "path": "scope.json", "sha256": "a" * 64},
    }
    if existing_authorization:
        payload["authorization_provenance"] = {"kind": "existing_human_authorization", "reference": "previous human instruction"}
    return flow.create_request(state, payload, identity, TIMESTAMP)


class EmailRuntimeAdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.groups = group_fixtures.GroupsApiTests()
        self.groups.PROJECT_PHONE = "9217"
        self.groups.setUp()
        self.root = Path(self.groups.temp_dir.name)
        self.old_managed = main.managed_continuity_runtime
        self.old_service = main.app.state.decision_notifications
        self.old_history_path, self.old_history_lock = main.sprint_history_path, main.sprint_history_lock
        main.managed_continuity_runtime = None
        main.sprint_history_path = self.root / "sprint-history.json"
        main.sprint_history_lock = asyncio.Lock()
        main.sprint_history_path.write_text(json.dumps({"schema_version": 1, "projects": {}}), encoding="utf-8")
        self.provider = RecordingProvider()
        self.service = None

    async def asyncTearDown(self):
        if self.service is not None:
            await self.service.stop()

    def tearDown(self):
        main.managed_continuity_runtime, main.app.state.decision_notifications = self.old_managed, self.old_service
        main.sprint_history_path, main.sprint_history_lock = self.old_history_path, self.old_history_lock
        self.groups.tearDown()

    async def configure_mail(self):
        self.service = main.app.state.configure_decision_notifications(
            EmailConfig(enabled=True, pending_decisions=True,
                database_path=self.root / "email-outbox.sqlite3",
                public_base_url="https://runtime-email.example.test", destination="operator@example.test",
                sender="notifications@example.test"), self.provider)
        await self.service.start(background=False)

    def queues(self):
        return deepcopy({name: list(queue) for name, queue in main.queues.items()})

    async def test_managed_reconciliation_reads_exact_project_identity_without_startup_repair(self):
        harness = managed_fixtures.ManagedContinuityTests()
        harness.setUp()
        self.addCleanup(harness.tearDown)
        main.managed_continuity_runtime = harness.runtime
        config = main.read_git_config_file()
        config[main.PROJECTS_KEY]["github.com/example/runtime-email"] = {
            "project_phone": "project-id", "project_name": "Managed Orion",
            "git_address": "https://github.com/example/runtime-email.git"}
        main.write_git_config_file(config)
        state, request = harness.runtime.scope_mutate("project-id", SPRINT_ID,
            lambda state: add_pending(state, "assignment-1"))
        self.assertNotIn("project_id", state)
        self.assertEqual(state["identity"]["project_id"], "project-id")
        initial = deepcopy(state)
        queue_before = self.queues()
        await self.configure_mail()
        with patch.object(harness.store, "active_runtime_states", side_effect=AssertionError("notification must not run startup repair")), \
                patch.object(harness.store, "_reconcile_superseded_attempts", side_effect=AssertionError("notification must not reconcile execution")), \
                patch.object(harness.runtime, "_publish_binding", side_effect=AssertionError("must not publish assignments")), \
                patch.object(harness.runtime, "drain_outbox", side_effect=AssertionError("must not drain execution outbox")):
            self.assertEqual(await self.service.reconcile_once(), 1)
            self.assertEqual((await self.service.tick_once())["status"], "delivered")
            notices = self.service.store.list_for("managed_workspace_v1", "project-id", SPRINT_ID, request["request_id"])
            self.assertEqual(len(notices), 1)
            self.assertEqual(notices[0]["project_id"], "project-id")
            self.assertEqual(notices[0]["assignment_id"], "assignment-1")
            outbox_before = self.service.store.path.read_bytes()
            for _ in range(2):
                code, card, _ = await request_http("/api/v1/decision-notifications/" + notices[0]["notification_id"])
                self.assertEqual(code, 200, card)
                self.assertTrue(card["current"])
                self.assertEqual(card["request"]["request_id"], request["request_id"])
            self.assertEqual(outbox_before, self.service.store.path.read_bytes())
        self.assertEqual(initial, harness.runtime.scope_snapshot("project-id", SPRINT_ID))
        self.assertEqual(queue_before, self.queues())
        self.assertEqual(len(self.provider.records), 1)

    async def prepare_parallel_request(self, *, existing_authorization=False):
        code, response, group = await self.groups.create_group()
        self.assertEqual(code, 200, response)
        actor = next(agent for agent in group["agents"] if agent["is_entrypoint"])
        await main.enqueue_external_group_task(group["group_id"],
            {"message": "Original isolated parallel work", "request_id": "parallel-email-first"}, 19087)
        claimed = await main.dequeue_group_agent_task(group["group_id"], actor["agent_id"], 19087)
        state = main.sequential_runtime_project_snapshot_transaction(self.groups.PROJECT_PHONE)["assignment"]
        changed, request = add_pending(state, claimed["assignment_id"], existing_authorization=existing_authorization)
        config = main.read_git_config_file()
        config[main.PROJECTS_KEY][self.groups.PROJECT_CONTEXT][main.PROJECT_AGENT_ASSIGNMENT_KEY] = changed
        main.write_git_config_file(config)
        main.sprint_history_path.write_text(json.dumps({"schema_version": 1, "projects": {
            self.groups.PROJECT_CONTEXT: {"current_sprint_id": "parallel-email-sprint", "sprints": []}}}), encoding="utf-8")
        return changed, request

    async def test_parallel_real_delivery_notification_and_get_preserve_queue_history_and_assignment(self):
        state, request = await self.prepare_parallel_request()
        self.assertEqual(state["mode"], "parallel")
        initial, queues = deepcopy(state), self.queues()
        config_before = main.git_config_path.read_bytes()
        history_before = main.history_path.read_bytes()
        await self.configure_mail()
        self.assertEqual(await self.service.reconcile_once(), 1)
        self.assertEqual((await self.service.tick_once())["status"], "delivered")
        notices = self.service.store.list_for(runtime_kind(state), self.groups.PROJECT_PHONE, "parallel-email-sprint", request["request_id"])
        self.assertEqual(len(notices), 1)
        outbox_before = self.service.store.path.read_bytes()
        code, card, _ = await request_http("/api/v1/decision-notifications/" + notices[0]["notification_id"])
        self.assertEqual(code, 200, card)
        self.assertTrue(card["current"])
        self.assertEqual(card["request"]["assignment_id"], request["assignment_id"])
        self.assertEqual(outbox_before, self.service.store.path.read_bytes())
        self.assertEqual(config_before, main.git_config_path.read_bytes())
        self.assertEqual(history_before, main.history_path.read_bytes())
        self.assertEqual(initial, main.sequential_runtime_project_snapshot_transaction(self.groups.PROJECT_PHONE)["assignment"])
        self.assertEqual(queues, self.queues())

    async def test_existing_authorization_and_stale_parallel_binding_never_send_human_request(self):
        state, request = await self.prepare_parallel_request(existing_authorization=True)
        record = flow.request_list(state)["requests"][0]
        self.assertEqual(record["status"], "approved_pending_application")
        self.assertIsNone(notification_snapshot(self.groups.PROJECT_PHONE, "parallel-email-sprint", state, record))
        await self.configure_mail()
        self.assertEqual(await self.service.reconcile_once(), 0)
        self.assertEqual((await self.service.tick_once())["status"], "idle")
        pending = deepcopy(record)
        pending.update(status="pending", authorization_provenance=None)
        stale = deepcopy(state)
        stale["assignments"][0]["status"] = "completed"
        self.assertIsNone(notification_snapshot(self.groups.PROJECT_PHONE, "parallel-email-sprint", stale, pending))
        self.assertEqual(self.provider.records, [])


if __name__ == "__main__":
    unittest.main()
