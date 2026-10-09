import json
import os
import secrets
import unittest
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from unittest.mock import patch

from nginx_qa.legacy_scope_control import LegacyScopeControlError, assignment_binding, canonical_json_bytes, canonical_json_sha256
from nginx_qa.managed_continuity import ManagedContinuityError
from nginx_qa.scope_runtime_adapters import (acknowledge_scope, active_assignments,
    apply_scope_revision, effective_scope_snapshot, normalized_scope_snapshot,
    require_scope_submission)
from nginx_qa import scope_workflow as flow
from tests import test_managed_continuity as managed_fixture
from tests import test_groups as group_fixture
from tests import test_project_actor_import as actor_fixture
from tests.test_sprint_type_contract import COMMIT, SPRINT_ID, TIMESTAMP, active_runtime_fixture
import main


class ScopeRuntimeAdapterTests(unittest.TestCase):
    def setUp(self):
        self.harness = managed_fixture.ManagedContinuityTests()
        self.harness.setUp()
        self.runtime = self.harness.runtime
        self.store = self.harness.store
        self.tokens = {phone: secrets.token_hex(32) for phone in ("2861", "2860", "2891", "2892")}
        self.environment = patch.dict(os.environ, {"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": secrets.token_hex(32), **{"NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_" + k: v for k, v in self.tokens.items()}})
        self.environment.start()

    def tearDown(self):
        self.environment.stop()
        self.harness.tearDown()

    def apply(self, state, revision=0, assignment_id="assignment-1"):
        return apply_scope_revision(state, [], assignment_id=assignment_id,
            amendment_id="scope-" + str(revision + 1),
            proposal={"instructions": "Proof and prerequisite work " + str(revision + 1),
                "retained_restrictions": ["No deployment", "Keep two approvals"],
                "node_ids": ["build", "continuity"], "reviewer_ids": ["reviewer-a", "reviewer-b"]},
            source={"repository_key": state["repository"]["repository_key"], "commit": COMMIT, "path": "scope.json", "sha256": "a" * 64},
            authorization={"request_id": "request-1", "decision_id": "decision-1", "actor": "operator"},
            expected_scope_revision=revision, applied_at=TIMESTAMP)

    def test_managed_multiple_revisions_preserve_issued_and_ack_history(self):
        original = active_runtime_fixture()
        first, _ = self.apply(original)
        first, ack = acknowledge_scope(first, "assignment-1", assignment_binding(first, "assignment-1")["scope_context"], TIMESTAMP)
        old_control = deepcopy(first["scope_control"])
        second, _ = self.apply(first, 1)
        self.assertEqual(original["assignments"], second["assignments"])
        self.assertEqual(original["workflow"], second["workflow"])
        self.assertEqual(original["graph_revisions"], second["graph_revisions"])
        self.assertEqual(old_control["amendments"], second["scope_control"]["amendments"][:1])
        self.assertEqual(old_control["binding_history"]["assignment-1"], second["scope_control"]["binding_history"]["assignment-1"][:1])
        self.assertEqual(ack["acknowledgement"], second["scope_control"]["acknowledgement_history"][ack["acknowledgement"]["ack_id"]])
        self.assertTrue(effective_scope_snapshot(second, "assignment-1")["requires_scope_ack"])
        with self.assertRaises(LegacyScopeControlError):
            require_scope_submission(second, "assignment-1", ack["acknowledgement"]["scope_context"], self.tokens["2861"])

    def test_managed_scope_ack_result_and_review_auth_are_independent(self):
        state, _ = self.runtime.scope_mutate("project-id", SPRINT_ID, self.apply)
        bound = assignment_binding(state, "assignment-1")
        body = self.harness.result_request()
        body["scope_context"] = bound["scope_context"]
        before = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        with self.assertRaises(ManagedContinuityError) as failure:
            self.runtime.submit_if_managed("project-id", "2861", canonical_json_bytes(body), supplied_role_token=self.tokens["2861"])
        self.assertEqual(failure.exception.code, "SCOPE_ACK_REQUIRED")
        self.assertEqual(before, self.runtime.scope_snapshot("project-id", SPRINT_ID))
        after, response = self.runtime.scope_mutate("project-id", SPRINT_ID, lambda s: acknowledge_scope(s, "assignment-1", bound["scope_context"], TIMESTAMP))
        for field in ("workflow", "assignments", "reviews", "review_assignments", "outbox", "transition_journal", "active_assignment_ids", "graph_revision"):
            self.assertEqual(before[field], after[field], field)
        self.assertFalse(response["graph_advanced"])
        result = self.runtime.submit_if_managed("project-id", "2861", canonical_json_bytes(body), supplied_role_token=self.tokens["2861"])
        self.assertEqual(result.response["status"], "REVIEWS_PENDING")
        state = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        self.assertEqual(len(state["review_assignments"]), 2)
        for review in state["review_assignments"]:
            review_binding = assignment_binding(state, review["assignment_id"])
            self.assertEqual(review_binding["effective_revision"], 1)
            self.assertIn("Proof and prerequisite work", review_binding["effective_core"]["profile"])
            self.assertNotEqual(review_binding["scope_context"], bound["scope_context"])
            payload = {"assignment_id": review["assignment_id"], "status": "APPROVE", "scope_context": review_binding["scope_context"]}
            with self.assertRaises(ManagedContinuityError) as failure:
                self.runtime.submit_if_managed("project-id", review["reviewer_phone"], canonical_json_bytes(payload), supplied_role_token=self.tokens[review["reviewer_phone"]])
            self.assertEqual(failure.exception.code, "SCOPE_ACK_REQUIRED")
            before_ack = self.runtime.scope_snapshot("project-id", SPRINT_ID)
            after_ack, _ = self.runtime.scope_mutate("project-id", SPRINT_ID, lambda s: acknowledge_scope(s, review["assignment_id"], review_binding["scope_context"], TIMESTAMP))
            for field in ("workflow", "reviews", "review_assignments", "outbox", "transition_journal", "status"):
                self.assertEqual(before_ack[field], after_ack[field], field)
            accepted = self.runtime.submit_if_managed("project-id", review["reviewer_phone"], canonical_json_bytes(payload), supplied_role_token=self.tokens[review["reviewer_phone"]])
            self.assertIn(accepted.response["status"], {"REVIEW_ACCEPTED", "COMPLETED"})
        finished = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        self.assertEqual(len(finished["reviews"]), 2)
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(finished["result_receipts"][0]["scope_context"], bound["scope_context"])
        self.assertTrue(all(r.get("scope_ack_id") for r in finished["reviews"]))

    def test_read_snapshot_and_effective_scope_do_not_claim_or_publish(self):
        state, _ = self.runtime.scope_mutate("project-id", SPRINT_ID, self.apply)
        with patch.object(self.runtime, "_publish_binding", side_effect=AssertionError("must not publish")), patch.object(self.runtime, "drain_outbox", side_effect=AssertionError("must not drain")):
            for _ in range(3):
                snapshot = self.runtime.scope_snapshot("project-id", SPRINT_ID)
                self.assertEqual(state, snapshot)
                self.assertFalse(effective_scope_snapshot(snapshot, "assignment-1")["mutated"])

    def test_scope_transaction_failure_rolls_back_decision_and_binding(self):
        before = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        def fail(state):
            changed, response = self.apply(state)
            changed["workflow"]["occurrences"] = []
            return changed, response
        with self.assertRaises(RuntimeError):
            self.runtime.scope_mutate("project-id", SPRINT_ID, fail)
        self.assertEqual(before, self.runtime.scope_snapshot("project-id", SPRINT_ID))

    def test_parallel_without_real_assignment_is_not_invented(self):
        state = {"mode": "parallel", "strategy": "parallel", "status": "parallel", "assignments": []}
        with self.assertRaises(LegacyScopeControlError) as failure:
            normalized_scope_snapshot(state, [])
        self.assertEqual(failure.exception.code, "SCOPE_ASSIGNMENT_NOT_ACTIVE")
        self.assertEqual(state["assignments"], [])

    def test_managed_operator_race_single_authoritative_decision(self):
        initial = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        identity = {"assignment_id": "assignment-1", "agent_id": "builder", "agent_phone": "2861", "node_id": "build"}
        proposed, _ = self.apply(initial)
        amendment = proposed["scope_control"]["amendments"][0]
        source = {"repository_key": initial["repository"]["repository_key"], "commit": COMMIT, "path": "scope.json", "sha256": "a" * 64}
        payload = {"assignment_id": "assignment-1", "idempotency_key": "request-race", "reason": "Proof prerequisite", "proposal": amendment["proposal"], "source": source, "expected_execution_revision": 0, "expected_scope_revision": 0}
        _, request = self.runtime.scope_mutate("project-id", SPRINT_ID, lambda s: flow.create_request(s, payload, identity, TIMESTAMP))
        request_id = request["request_id"]
        def apply(s, proposal, source, authorization):
            changed, receipt = apply_scope_revision(s, [], assignment_id="assignment-1", amendment_id="managed-human-1", proposal=proposal, source=source, authorization=authorization, expected_scope_revision=0, applied_at=TIMESTAMP)
            receipt["before_effective_sha256"] = canonical_json_sha256({"profile": None, "tasks": []})
            receipt["after_effective_sha256"] = canonical_json_sha256(assignment_binding(changed, "assignment-1")["effective_core"])
            return changed, receipt
        def preview(s, proposal, source, authorization):
            changed, _ = apply(s, proposal, source, authorization)
            return {"profile": None, "tasks": []}, assignment_binding(changed, "assignment-1")["effective_core"]
        expected = {"expected_execution_revision": 0, "expected_scope_revision": 0}
        validated_state, validation = self.runtime.scope_mutate("project-id", SPRINT_ID, lambda s: flow.validate_request(s, request_id, expected, identity, TIMESTAMP, preview))
        self.assertEqual(validated_state.get("execution_revision", 0), 0)
        def decide(action):
            payload = {**expected, "action": action, "idempotency_key": "decision-" + action, "validation_id": validation["validation_id"]}
            return self.runtime.scope_mutate("project-id", SPRINT_ID, lambda s: flow.decide_request(s, request_id, payload, identity, TIMESTAMP, apply))
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(decide, ("approve", "reject")))
        responses = [r for _, r in outcomes]
        self.assertEqual(sum("decision_id" in r for r in responses), 1)
        self.assertEqual(sum(r.get("status_code") == 409 for r in responses), 1)
        final = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        self.assertEqual(len(final["scope_workflow"]["decisions"]), 1)
        self.assertEqual(len(final["scope_workflow"]["attempts"]), 1)
        self.assertEqual(final["assignments"], initial["assignments"])
        self.assertEqual(final["workflow"], initial["workflow"])
        self.assertLessEqual(len((final.get("scope_control") or {}).get("amendments", [])), 1)


class ParallelScopeDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = group_fixture.GroupsApiTests()
        self.harness.setUp()

    def tearDown(self):
        self.harness.tearDown()

    async def test_real_delivery_scope_ack_and_normal_handoff(self):
        _, _, group = await self.harness.create_group()
        entrypoint = next(a for a in group["agents"] if a["is_entrypoint"])
        created = await main.enqueue_external_group_task(group["group_id"], {"message": "Original actual queued instruction", "request_id": "scope-group-first"}, 18026)
        claimed = await main.dequeue_group_agent_task(group["group_id"], entrypoint["agent_id"], 18026)
        self.assertEqual(claimed["assignment_id"], created["queue_item_id"])
        snapshot = main.sequential_runtime_project_snapshot_transaction(self.harness.PROJECT_PHONE)
        state, agents = snapshot["assignment"], snapshot["agents"]
        original_record = deepcopy(state["assignments"][0])
        tokens = {a["phone"]: secrets.token_hex(32) for a in agents}
        env = {"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": secrets.token_hex(32), **{"NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_" + k: v for k, v in tokens.items()}}
        with patch.dict(os.environ, env):
            amended, _ = apply_scope_revision(state, agents, assignment_id=claimed["assignment_id"], amendment_id="parallel-scope-1",
                proposal={"instructions": "New approved scope", "retained_restrictions": ["No deployment"], "node_ids": [a["id"] for a in agents], "reviewer_ids": []},
                source={"repository_key": self.harness.PROJECT_CONTEXT, "commit": COMMIT, "path": "scope.json", "sha256": "b" * 64},
                authorization={"actor": "operator"}, expected_scope_revision=0, applied_at=TIMESTAMP)
            config = main.read_git_config_file()
            raw_key, _, project, _ = main.project_for_group_api(config, self.harness.PROJECT_PHONE)
            project[main.PROJECT_AGENT_ASSIGNMENT_KEY] = amended
            config[main.PROJECTS_KEY][raw_key] = project
            main.write_git_config_file(config)
            binding = assignment_binding(amended, claimed["assignment_id"])
            connection = next(c for c in group["connections"] if c["id"] == "analyst-to-backend")
            payload = {"message": "Ordinary result handoff", "from_agent_id": entrypoint["agent_id"], "cycle_id": created["cycle_id"], "parent_task_id": created["task_id"], "request_id": "scope-group-handoff", "scope_context": binding["scope_context"]}
            phone = entrypoint["agent_phone"]
            with self.assertRaises(main.HTTPException) as failure:
                await main.enqueue_group_connection_task(group["group_id"], connection["id"], payload, 18026, tokens[phone])
            self.assertEqual(failure.exception.status_code, 428)
            acknowledged, receipt = acknowledge_scope(amended, claimed["assignment_id"], binding["scope_context"], TIMESTAMP)
            self.assertEqual(amended["assignments"], acknowledged["assignments"])
            project[main.PROJECT_AGENT_ASSIGNMENT_KEY] = acknowledged
            config[main.PROJECTS_KEY][raw_key] = project
            main.write_git_config_file(config)
            with patch.object(main, "parallel_scope_complete_transaction", side_effect=OSError("injected completion persistence failure")):
                with self.assertRaises(OSError):
                    await main.enqueue_group_connection_task(group["group_id"], connection["id"], payload, 18026, tokens[phone])
            queued_before_retry = sum(len(q) for q in main.queues.values())
            pending = main.sequential_runtime_project_snapshot_transaction(self.harness.PROJECT_PHONE)["assignment"]
            with self.assertRaises(LegacyScopeControlError) as failure:
                apply_scope_revision(pending, agents, assignment_id=claimed["assignment_id"], amendment_id="parallel-scope-2", proposal={"instructions": "Must not race handoff", "retained_restrictions": [], "node_ids": [a["id"] for a in agents], "reviewer_ids": []})
            self.assertEqual(failure.exception.code, "SCOPE_HANDOFF_PUBLISHING")
            handed = await main.enqueue_group_connection_task(group["group_id"], connection["id"], payload, 18026, tokens[phone])
            self.assertEqual(sum(len(q) for q in main.queues.values()), queued_before_retry)
            final = main.sequential_runtime_project_snapshot_transaction(self.harness.PROJECT_PHONE)["assignment"]
            self.assertEqual(final["assignments"][0]["issued_task"], original_record["issued_task"])
            self.assertEqual(final["assignments"][0]["status"], "completed")
            self.assertEqual(final["assignments"][0]["handoff_queue_item_id"], handed["queue_item_id"])
            with self.assertRaises(main.HTTPException):
                await main.dequeue_group_agent_task(group["group_id"], connection["to_agent_id"], 18026)
            next_task = await main.dequeue_group_agent_task(group["group_id"], connection["to_agent_id"], 18026, tokens[connection["to_phone"]])
            self.assertEqual(next_task["effective_scope"]["binding"]["effective_revision"], 1)
            self.assertTrue(next_task["effective_scope"]["requires_scope_ack"])
            with self.assertRaises(main.HTTPException):
                main.parallel_scope_raw_channel_guard(self.harness.PROJECT_PHONE, connection["to_phone"])


class QueueGraphScopeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = actor_fixture.ProjectActorImportTests()
        self.harness.setUp()

    def tearDown(self):
        self.harness.tearDown()

    async def test_ack_does_not_take_next_queue_role_and_handoff_is_recoverable(self):
        project = self.harness.PROJECT_PHONE
        status, _ = await actor_fixture.asgi_request(f"/api/v1/projects/{project}/agents/import", method="POST", payload={"project_id": project, "git_address": "https://github.com/example/actor-import.git", "agents": {"overwrite": True, "assignment_mode": "sequential", "items": [
            {"id": "queue-role-a", "name": "Queue A", "phone": "2121", "profile": "Original A", "tasks": [{"task_id": "Q-A", "queue": "worker-all", "message": "Original task A"}]},
            {"id": "queue-role-b", "name": "Queue B", "phone": "2122", "profile": "Original B", "tasks": []}]}})
        self.assertEqual(status, 201)
        current = await main.dequeue_sequential_runtime_task(project, 18026)
        state, agents = current["assignment"], current["agents"]
        assignment_id = state["current_assignment_id"]
        token_a, token_b = secrets.token_hex(32), secrets.token_hex(32)
        with patch.dict(os.environ, {"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": secrets.token_hex(32), "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2121": token_a, "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2122": token_b}):
            amended, _ = apply_scope_revision(state, agents, assignment_id=assignment_id, amendment_id="queue-scope-1", proposal={"instructions": "Approved new queue scope", "retained_restrictions": ["No deployment"], "node_ids": ["queue-role-a", "queue-role-b"], "reviewer_ids": []}, source={"repository_key": self.harness.PROJECT_CONTEXT, "commit": COMMIT, "path": "scope.json", "sha256": "a" * 64}, authorization={"actor": "operator"}, expected_scope_revision=0, applied_at=TIMESTAMP)
            config = main.read_git_config_file()
            key, _, entry, _ = main.project_for_group_api(config, project)
            def persist(s):
                entry[main.PROJECT_AGENT_ASSIGNMENT_KEY] = s
                config[main.PROJECTS_KEY][key] = entry
                main.write_git_config_file(config)
            persist(amended)
            binding = assignment_binding(amended, assignment_id)
            metadata = {"from_phone": "2121", "to_phone": "2122", "scope_context": binding["scope_context"]}
            with self.assertRaises(main.HTTPException) as failure:
                await main.enqueue_scope_aware_phone_channel("worker-all", project, "Ordinary next task", metadata, 18026, current["queue_context"], token_a)
            self.assertEqual(failure.exception.status_code, 428)
            acked, _ = acknowledge_scope(amended, assignment_id, binding["scope_context"], TIMESTAMP)
            persist(acked)
            reused = await main.dequeue_sequential_runtime_task(project, 18026, token_a)
            self.assertEqual(reused["assignment"]["current_assignment_id"], assignment_id)
            self.assertTrue(reused["identity_reused"])
            self.assertEqual(len(main.queues["worker-all"]), 0)
            normal = main.queue_graph_scope_handoff_transaction
            def fail_receipt(*args, **kwargs):
                if len(args) > 3 and args[3] is not None:
                    raise OSError("injected handoff receipt failure")
                return normal(*args, **kwargs)
            with patch.object(main, "queue_graph_scope_handoff_transaction", side_effect=fail_receipt):
                with self.assertRaises(OSError):
                    await main.enqueue_scope_aware_phone_channel("worker-all", project, "Ordinary next task", metadata, 18026, current["queue_context"], token_a)
            queued = await main.enqueue_scope_aware_phone_channel("worker-all", project, "Ordinary next task", metadata, 18026, current["queue_context"], token_a)
            self.assertTrue(queued["deduplicated"])
            self.assertEqual(len(main.queues["worker-all"]), 1)
            following = await main.dequeue_sequential_runtime_task(project, 18026, token_b)
            self.assertEqual(following["agent"]["id"], "queue-role-b")
            self.assertIn("Approved new queue scope", following["task"]["message"])
            self.assertIsNotNone(assignment_binding(following["assignment"], following["assignment"]["current_assignment_id"]))
            self.assertEqual(len(main.queues["worker-all"]), 0)
