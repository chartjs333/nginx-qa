"""Isolated v1 compatibility and multi-revision continuation integration."""
from copy import deepcopy
import json
import unittest
from unittest.mock import patch

import main
from tests import test_legacy_scope_control as fixtures
from nginx_qa.legacy_scope_control import (
    LegacyScopeControlError, apply_semantic_scope_revision, assignment_binding,
    canonical_json_sha256, validate_versioned_scope_history,
)
from nginx_qa.scope_control_migrate import migrate_document
from nginx_qa.scope_control_prestart import validate_scope_control_compatibility


class ScopeRevisionTests(fixtures.LegacyScopeControlTests):
    def _persist_state(self, state):
        document = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        document[main.PROJECTS_KEY][self.PROJECT_CONTEXT][main.PROJECT_AGENT_ASSIGNMENT_KEY] = state
        main.git_config_path.write_text(json.dumps(document), encoding="utf-8")

    def _semantic_apply(self, revision):
        state = self._stored_assignment()
        original = deepcopy(state)
        agents = main.read_agents_file()
        result, receipt = apply_semantic_scope_revision(state, agents,
            amendment_id=f"SCOPE-REVISION-{revision}",
            proposal={"instructions": f"AUTHORITATIVE REVISION {revision}: perform only the approved proof.",
                "retained_restrictions": ["No production integration", "No gate waiver"],
                "node_ids": ["continuity-coordinator", "formal-linkage"],
                "reviewer_ids": ["reviewer-one", "reviewer-two"]},
            source={"repository_key": self.PROJECT_CONTEXT, "commit": self.target_commit,
                "path": self.AMENDMENT_PATH, "sha256": self.amendment_sha256},
            authorization={"kind": "human_decision", "decision_id": f"decision-{revision}"},
            expected_scope_revision=revision-1, applied_at=f"2026-10-05T12:00:0{revision}Z")
        self.assertEqual(state, original)
        self.assertEqual({key: value for key, value in result.items() if key != "scope_control"},
            {key: value for key, value in state.items() if key != "scope_control"})
        self._persist_state(result)
        return receipt

    async def _v1_and_migrate(self):
        await self._import_sprint()
        _, current = await self._pre_amendment_round_trip()
        assignment_id = self._assignment_id(current)
        self._create_amendment_commit(assignment_id)
        status, response = await self._apply(self._amendment_payload(await self._preflight()))
        self.assertEqual(status, 200, response)
        scope = await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)
        document = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        before = deepcopy(document)
        migrated, report = migrate_document(document, migrated_at="2026-10-05T12:00:00Z", source_sha256=canonical_json_sha256(document))
        self.assertEqual(document, before)
        old = document[main.PROJECTS_KEY][self.PROJECT_CONTEXT][main.PROJECT_AGENT_ASSIGNMENT_KEY]
        new = migrated[main.PROJECTS_KEY][self.PROJECT_CONTEXT][main.PROJECT_AGENT_ASSIGNMENT_KEY]
        self.assertEqual({k:v for k,v in old.items() if k != "scope_control"}, {k:v for k,v in new.items() if k != "scope_control"})
        for field in ("amendments", "assignment_bindings", "acknowledgements", "effective_revision", "active_amendment_id"):
            self.assertEqual(old["scope_control"][field], new["scope_control"][field])
        self.assertFalse(report["ack_created"])
        main.git_config_path.write_text(json.dumps(migrated), encoding="utf-8")
        return assignment_id, scope

    async def test_versioned_second_third_scope_preserve_history_ack_and_failure_atomicity(self):
        assignment_id, original_scope = await self._v1_and_migrate()
        original_state = self._stored_assignment()
        old_control = deepcopy(original_state["scope_control"])
        queues = self._queue_snapshot()
        history = main.history_path.read_bytes()
        self._semantic_apply(2)
        before_stale = main.git_config_path.read_bytes()
        status, response = await fixtures.asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/{assignment_id}/effective-scope/ack",
            method="POST", payload={"schema_version": 1, "scope_context": original_scope["scope_context"]},
            headers=self._role_headers(self.COORDINATOR_PHONE))
        self.assertEqual(status, 409, response)
        self.assertEqual(main.git_config_path.read_bytes(), before_stale)
        state_before_ack = self._stored_assignment()
        scope2 = await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)
        state_after_ack = self._stored_assignment()
        for field in ("assignments", "pending_transition", "current_assignment_id", "current_node_id", "workflow", "active_task", "visit_counts", "status"):
            self.assertEqual(state_before_ack.get(field), state_after_ack.get(field))
        self.assertEqual(self._queue_snapshot(), queues)
        self.assertEqual(main.history_path.read_bytes(), history)
        self._semantic_apply(3)
        scope3 = await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)
        self.assertNotEqual(scope2["scope_context"], scope3["scope_context"])
        control = self._stored_assignment()["scope_control"]
        self.assertEqual(len(control["amendments"]), 3)
        self.assertEqual(len(control["binding_history"][assignment_id]), 3)
        self.assertEqual(len(control["acknowledgement_history"]), 3)
        self.assertEqual(control["amendments"][0], old_control["amendments"][0])
        self.assertEqual(control["binding_history"][assignment_id][0], old_control["assignment_bindings"][assignment_id])
        self.assertEqual(control["acknowledgement_history"][original_state["scope_control"]["acknowledgements"][assignment_id]["ack_id"]], old_control["acknowledgements"][assignment_id])
        validate_versioned_scope_history(json.loads(json.dumps(control)))
        validate_scope_control_compatibility(json.loads(main.git_config_path.read_text(encoding="utf-8")))
        stale_before = self._stored_assignment()
        with self.assertRaises(LegacyScopeControlError):
            self._semantic_apply(3)
        self.assertEqual(self._stored_assignment(), stale_before)
        broken = deepcopy(stale_before)
        broken["scope_control"]["binding_history"][assignment_id][0]["effective"]["profile"] = "tampered"
        with self.assertRaises(LegacyScopeControlError):
            assignment_binding(broken, assignment_id)

    async def test_versioned_round_trip_new_reviews_formal_no_go_reentry_and_next_amendment(self):
        assignment_id, _ = await self._v1_and_migrate()
        before = self._stored_assignment()
        historical = deepcopy(before["assignments"][:-1])
        queues_before = self._queue_snapshot()
        history_before = main.history_path.read_bytes()
        version = {"expected_execution_revision": before["revision"], "expected_scope_revision": 1}
        # The temporary Git fixture already records the proof authorization.
        # This is its explicit operator registration, not a new consent claim.
        provenance = {"kind": "existing_human_authorization",
            "reference": f"fixture-existing-authorization:{self.target_commit}:{self.AMENDMENT_PATH}"}
        payload = {**version, "assignment_id": assignment_id,
            "idempotency_key": "continuation-scope-revision-two",
            "reason": "Synchronize the fixture's already authorized proof-only scope",
            "authorization_provenance": provenance,
            "proposal": {"instructions": "AUTHORITATIVE REVISION 2: perform only the approved proof.",
                "retained_restrictions": ["No production integration", "No gate waiver"],
                "node_ids": ["continuity-coordinator", "formal-linkage"],
                "reviewer_ids": ["reviewer-one", "reviewer-two"]},
            "source": {"repository_key": self.PROJECT_CONTEXT, "commit": self.target_commit,
                "path": self.AMENDMENT_PATH}}
        requests_url = f"/api/v1/projects/{self.PROJECT_PHONE}/sprints/{self.sprint_id}/scope-requests"
        with patch.dict("os.environ", {"NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP":
                json.dumps({self.PROJECT_CONTEXT: str(self.repo_path)})}):
            code, requested = await fixtures.asgi_request(requests_url, method="POST", payload=payload,
                headers=self._role_headers(self.COORDINATOR_PHONE))
            self.assertEqual(code, 200, requested)
            self.assertNotIn("sha256", payload["source"])
            self.assertEqual(requested["source"]["sha256"], self.amendment_sha256)
            request_url = requests_url + "/" + requested["request_id"]
            code, validation = await fixtures.asgi_request(request_url + "/validate", method="POST",
                payload=version, headers=self._admin_headers())
            self.assertEqual(code, 200, validation)
            decision = {**version, "action": "record_existing_authorization",
                "validation_id": validation["validation_id"], "idempotency_key": "continuation-existing-authorization-two"}
            code, accepted = await fixtures.asgi_request(request_url + "/decisions", method="POST",
                payload=decision, headers=self._admin_headers())
            self.assertEqual(code, 200, accepted)
        applied = self._stored_assignment()
        for field in ("assignments", "current_assignment_id", "current_node_id", "pending_transition",
                "last_transition", "workflow", "active_task", "visit_counts"):
            self.assertEqual(applied.get(field), before.get(field), field)
        self.assertEqual(self._queue_snapshot(), queues_before)
        self.assertEqual(main.history_path.read_bytes(), history_before)
        self.assertFalse(accepted["graph_advanced"])
        amendment = applied["scope_control"]["amendments"][-1]
        self.assertEqual(amendment["authorization"]["request_id"], requested["request_id"])
        self.assertEqual(amendment["authorization"]["decision_id"], accepted["decision_id"])
        self.assertEqual(amendment["authorization"]["validation_id"], validation["validation_id"])
        self.assertEqual(amendment["authorization"]["origin"], "recorded_existing_authorization")
        self.assertEqual(amendment["authorization"]["provenance"], provenance)
        self.assertEqual(applied["scope_workflow"]["decisions"][-1]["action"], "record_existing_authorization")
        scope = await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)
        self.assertEqual(scope["scope_context"], accepted["receipt"]["scope_context"])
        self.assertEqual(scope["scope_context"]["amendment_id"], amendment["amendment_id"])
        acknowledged = self._stored_assignment()
        self.assertEqual(acknowledged["scope_control"]["acknowledgements"][assignment_id]["scope_context"], scope["scope_context"])
        self.assertEqual(acknowledged["assignments"], before["assignments"])
        self.assertEqual(self._queue_snapshot(), queues_before)
        await self._submit_effective(self.COORDINATOR_PHONE, assignment_id, "RESUME_FORMAL", scope["scope_context"], result="Revision two proof handoff")
        for agent_id, phone in (("reviewer-one", self.REVIEWER_ONE_PHONE), ("reviewer-two", self.REVIEWER_TWO_PHONE)):
            identity = await self._claim(agent_id)
            aid = self._assignment_id(identity)
            review_scope = await self._scope_and_ack(aid, phone)
            self.assertEqual(review_scope["scope_context"]["effective_revision"], 2)
            self.assertEqual(review_scope["scope_context"]["amendment_id"], amendment["amendment_id"])
            self.assertEqual(review_scope["scope_context"]["source_assignment_id"], assignment_id)
            self.assertIn("AUTHORITATIVE REVISION 2", review_scope["effective"]["profile"])
            await self._submit_effective(phone, aid, "APPROVE", review_scope["scope_context"], feedback="New result approved")
        formal = await self._claim("formal-linkage")
        formal_id = self._assignment_id(formal)
        formal_scope = await self._scope_and_ack(formal_id, self.FORMAL_PHONE)
        self.assertIn("AUTHORITATIVE REVISION 2", formal_scope["effective"]["profile"])
        await self._submit_effective(self.FORMAL_PHONE, formal_id, "NO_GO", formal_scope["scope_context"], result="More proof required")
        for agent_id, phone in (("reviewer-one", self.REVIEWER_ONE_PHONE), ("reviewer-two", self.REVIEWER_TWO_PHONE)):
            identity = await self._claim(agent_id)
            aid = self._assignment_id(identity)
            review_scope = await self._scope_and_ack(aid, phone)
            self.assertEqual(review_scope["scope_context"]["source_assignment_id"], formal_id)
            await self._submit_effective(phone, aid, "APPROVE", review_scope["scope_context"], feedback="Return approved")
        returned = await self._claim("continuity-coordinator")
        returned_id = self._assignment_id(returned)
        self.assertNotEqual(returned_id, assignment_id)
        returned_scope = await self._scope_and_ack(returned_id, self.COORDINATOR_PHONE)
        self.assertEqual(returned_scope["scope_context"]["effective_revision"], 2)
        self._semantic_apply(3)
        newest = await self._scope_and_ack(returned_id, self.COORDINATOR_PHONE)
        self.assertIn("AUTHORITATIVE REVISION 3", newest["effective"]["active_task"]["message"])
        self.assertNotIn('"effective_revision":2', newest["effective"]["active_task"]["message"])
        self._semantic_apply(4)
        fourth = await self._scope_and_ack(returned_id, self.COORDINATOR_PHONE)
        self.assertIn("AUTHORITATIVE REVISION 4", fourth["effective"]["active_task"]["message"])
        after = self._stored_assignment()
        self.assertEqual(after["assignments"][:len(historical)], historical)
        self.assertEqual(after["workflow"]["required_approvals"], 2)
        validate_versioned_scope_history(after["scope_control"])

    async def test_versioned_raw_git_api_requires_human_decision_without_mutation(self):
        await self._v1_and_migrate()
        self.AMENDMENT_ID = "GIT-SCOPE-REVISION-TWO"
        payload = self._amendment_payload(await self._preflight(), idempotency_key="versioned-git-2")
        payload.update(schema_version=2, expected_scope_revision=1)
        before = main.git_config_path.read_bytes()
        status, response = await self._apply(payload)
        self.assertEqual(status, 409, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_HUMAN_DECISION_REQUIRED")
        self.assertEqual(main.git_config_path.read_bytes(), before)

    async def test_versioned_amend_active_reviewer_next_review_uses_new_scope_same_result(self):
        assignment_id, _ = await self._v1_and_migrate()
        self._semantic_apply(2)
        scope2 = await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)
        await self._submit_effective(self.COORDINATOR_PHONE, assignment_id, "RESUME_FORMAL", scope2["scope_context"], result="Exact reviewed result under scope two")
        reviewer = await self._claim("reviewer-one")
        reviewer_id = self._assignment_id(reviewer)
        old_review_scope = await self._scope_and_ack(reviewer_id, self.REVIEWER_ONE_PHONE)
        pending = deepcopy(self._stored_assignment()["pending_transition"])
        self._semantic_apply(3)
        self.assertEqual(self._stored_assignment()["pending_transition"], pending)
        revised = await self._scope_and_ack(reviewer_id, self.REVIEWER_ONE_PHONE)
        self.assertEqual(revised["scope_context"]["effective_revision"], 3)
        self.assertNotEqual(revised["scope_context"], old_review_scope["scope_context"])
        await self._submit_effective(self.REVIEWER_ONE_PHONE, reviewer_id, "APPROVE", revised["scope_context"], feedback="Same exact result reviewed under latest policy")
        second = await self._claim("reviewer-two")
        second_id = self._assignment_id(second)
        second_scope = await self._scope_and_ack(second_id, self.REVIEWER_TWO_PHONE)
        self.assertEqual(second_scope["scope_context"]["effective_revision"], 3)
        self.assertEqual(second_scope["scope_context"]["source_assignment_id"], assignment_id)
        binding = assignment_binding(self._stored_assignment(), second_id)
        self.assertEqual(binding["reviewed_source_scope_context"], scope2["scope_context"])
        self.assertEqual(second_scope["scope_context"]["reviewed_source_scope_context_sha256"], canonical_json_sha256(scope2["scope_context"]))
        self.assertEqual(self._stored_assignment()["pending_transition"]["result"], pending["result"])
        await self._submit_effective(self.REVIEWER_TWO_PHONE, second_id, "APPROVE", second_scope["scope_context"], feedback="Second independent review of same exact result")
        after = self._stored_assignment()
        self.assertEqual(after["current_node_id"], "formal-linkage")
        self.assertEqual(len(after["last_transition"]["reviews"]), 2)


def load_tests(loader, tests, pattern):
    # Reuse the isolated fixture helpers without re-running its inherited seven
    # v1 tests here; those remain independently collected in their own module.
    return unittest.TestSuite(ScopeRevisionTests(name) for name in dir(ScopeRevisionTests) if name.startswith("test_versioned_"))
