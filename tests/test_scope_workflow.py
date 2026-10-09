"""Human decisions: CAS races, immutable history, source-bound edits and rollback."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import itertools
import json
import os
import threading
import unittest
from unittest.mock import patch

import main
from nginx_qa import scope_workflow as flow
from nginx_qa.legacy_scope_control import assignment_binding
from tests import test_legacy_scope_control as legacy_tests
from tests.test_legacy_scope_control import asgi_request


class DecisionLedgerTests(unittest.TestCase):
    def setUp(self):
        self.identity = {"assignment_id": "a", "agent_id": "author", "node_id": "proof"}
        self.proposal = {"instructions": "Prove the approved theorem", "retained_restrictions": ["No deployment"], "node_ids": ["proof"], "reviewer_ids": ["r1", "r2"]}
        self.payload = {"idempotency_key": "request", "assignment_id": "a", "expected_execution_revision": 4, "expected_scope_revision": 0,
            "reason": "Prerequisite needs a scope extension", "proposal": self.proposal,
            "source": {"repository_key": "github.com/example/project", "commit": "a" * 40, "path": "scope.md", "sha256": "b" * 64}}
        self.initial = {"revision": 4, "assignments": [{"assignment_id": "a"}], "reviews": [{"decision": "APPROVE", "result_id": "old"}]}
        self.state, self.request = flow.create_request(self.initial, self.payload, self.identity, "t1")

    def preview(self, state, proposal, source, authorization):
        return {"instructions": "original"}, deepcopy(proposal)

    def apply(self, state, proposal, source, authorization):
        state["scope_control"] = {"effective_revision": flow.scope_revision(state) + 1}
        state["revision"] += 1
        return state, {"effective_revision": flow.scope_revision(state),
            "before_effective_sha256": flow.canonical_json_sha256({"instructions": "original"}),
            "after_effective_sha256": flow.canonical_json_sha256(proposal)}

    def validated(self, state, edit=""):
        proposal = {**self.proposal, "instructions": edit or self.proposal["instructions"]}
        return flow.validate_request(state, self.request["request_id"],
            {"expected_execution_revision": 4, "expected_scope_revision": 0, "proposal": proposal},
            self.identity, "t2", self.preview)

    def decision(self, action, validation, key):
        return {"action": action, "validation_id": validation["validation_id"], "idempotency_key": key,
                "expected_execution_revision": 4, "expected_scope_revision": 0}

    def test_every_pair_of_concurrent_decisions_has_one_winner_and_durable_loser(self):
        for first, second in itertools.product(("approve", "reject", "edit"), repeat=2):
            with self.subTest(first=first, second=second):
                state, v1 = self.validated(deepcopy(self.state), "edited scope one" if first == "edit" else "")
                state, v2 = self.validated(state, "different edited scope two" if second == "edit" else "")
                lock, barrier = threading.Lock(), threading.Barrier(2)
                payloads = [self.decision(first, v1, "one"), self.decision(second, v2, "two")]
                def execute(payload):
                    nonlocal state
                    barrier.wait(timeout=5)
                    with lock:  # The real adapters use aggregate file/SQLite locks.
                        state, response = flow.decide_request(state, self.request["request_id"], payload, self.identity, "t3", self.apply)
                        return response
                with ThreadPoolExecutor(max_workers=2) as executor:
                    results = list(executor.map(execute, payloads))
                self.assertEqual(sum("error" not in r for r in results), 1)
                self.assertEqual(len(state["scope_workflow"]["decisions"]), 1)
                self.assertEqual(len(state["scope_workflow"]["attempts"]), 1)
                self.assertEqual(state["assignments"], self.initial["assignments"])
                self.assertEqual(state["reviews"], self.initial["reviews"])
                winner = next(i for i, response in enumerate(results) if "error" not in response)
                before = deepcopy(state)
                state, retry = flow.decide_request(state, self.request["request_id"], payloads[winner], self.identity, "t4", self.apply)
                self.assertTrue(retry["deduplicated"])
                self.assertEqual(state, before)
                expected_scope = 0 if payloads[winner]["action"] == "reject" else 1
                self.assertEqual(flow.scope_revision(state), expected_scope)

    def test_apply_failure_publishes_neither_decision_nor_partial_scope(self):
        state, validation = self.validated(self.state)
        original = deepcopy(state)
        def fail(candidate, *args):
            candidate["scope_control"] = {"effective_revision": 999}
            raise RuntimeError("injected persistence boundary failure")
        with self.assertRaises(RuntimeError):
            flow.decide_request(state, self.request["request_id"], self.decision("approve", validation, "once"), self.identity, "t3", fail)
        self.assertEqual(state, original)

    def test_edited_content_cannot_inherit_original_approval(self):
        state, validation = self.validated(self.state, "Changed content")
        state, response = flow.decide_request(state, self.request["request_id"], self.decision("approve", validation, "approve-edit"), self.identity, "t3", self.apply)
        self.assertEqual(response["error"], "SCOPE_EDIT_REQUIRES_EXPLICIT_DECISION")
        self.assertEqual(flow.scope_revision(state), 0)
        self.assertIn("No deployment", validation["diff"])

    def test_existing_permission_requires_explicit_provenance_not_inferred_from_source(self):
        state, validation = self.validated(self.state)
        state, response = flow.decide_request(state, self.request["request_id"], self.decision("record_existing_authorization", validation, "prior"), self.identity, "t3", self.apply)
        self.assertEqual(response["error"], "SCOPE_EXISTING_AUTHORIZATION_REQUIRED")

    def existing_request(self):
        payload = deepcopy(self.payload)
        payload["authorization_provenance"] = {"kind": "existing_human_authorization", "reference": "pinned prior human decision"}
        initial = deepcopy(self.initial)
        initial["assignments"][0]["status"] = "active"
        initial.update(status="active", current_assignment_id="a")
        state, requested = flow.create_request(initial, payload, self.identity, "t1")
        return initial, payload, state, requested

    def test_existing_authorization_waits_for_application_not_new_consent(self):
        from nginx_qa.execution_observability import project_execution
        initial, payload, state, requested = self.existing_request()
        self.assertEqual(requested["status"], "approved_pending_application")
        requests = flow.request_list(state)["requests"]
        self.assertEqual(requests[0]["status"], "approved_pending_application")
        self.assertEqual(requests[0]["authorization_provenance"], payload["authorization_provenance"])
        original = deepcopy(state)
        view = project_execution({"execution": state}, scope_requests=requests)
        self.assertEqual(view["execution"]["status"], "active")
        self.assertEqual(view["attention"]["state"], "waiting_for_scope_application")
        self.assertEqual(view["attention"]["pending_request_ids"], [])
        self.assertEqual(view["attention"]["pending_application_ids"], [requested["request_id"]])
        self.assertEqual(len(view["pending_decisions"]), 1)
        self.assertEqual(state, original)
        self.assertEqual(flow.scope_revision(state), 0)
        self.assertEqual(state["assignments"], initial["assignments"])
        self.assertEqual(state["reviews"], initial["reviews"])
        self.assertEqual(state["scope_workflow"]["decisions"], [])
        self.assertNotIn("scope_control", state)
        retried, response = flow.create_request(state, payload, self.identity, "t2")
        self.assertTrue(response["deduplicated"])
        self.assertEqual(response["status"], "approved_pending_application")
        self.assertEqual(retried, state)

    def test_existing_permission_requires_distinct_validated_recording_not_fake_approve(self):
        initial, _, state, requested = self.existing_request()
        version = {"expected_execution_revision": 4, "expected_scope_revision": 0}
        state, validation = flow.validate_request(state, requested["request_id"], version, self.identity, "t2", self.preview)
        state, refused = flow.decide_request(state, requested["request_id"], self.decision("approve", validation, "not-a-new-consent"), self.identity, "t3", self.apply)
        self.assertEqual(refused["error"], "SCOPE_EXISTING_AUTHORIZATION_ACTION_REQUIRED")
        self.assertEqual(state["scope_workflow"]["decisions"], [])
        decision = self.decision("record_existing_authorization", validation, "record-prior")
        state, accepted = flow.decide_request(state, requested["request_id"], decision, self.identity, "t4", self.apply)
        self.assertEqual(accepted["status"], "applied")
        self.assertEqual(flow.request_list(state)["requests"][0]["status"], "applied")
        self.assertEqual(state["scope_workflow"]["decisions"][0]["authorization"]["origin"], "recorded_existing_authorization")
        self.assertEqual(state["assignments"], initial["assignments"])
        self.assertEqual(state["reviews"], initial["reviews"])
        original = deepcopy(state)
        state, replay = flow.decide_request(state, requested["request_id"], decision, self.identity, "t5", self.apply)
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(state, original)

    def test_edited_existing_request_requires_and_records_new_explicit_semantic_decision(self):
        _, _, state, requested = self.existing_request()
        changed_proposal = {**self.proposal, "instructions": "Explicit new boundary, not inherited authorization"}
        state, validation = flow.validate_request(state, requested["request_id"],
            {"expected_execution_revision": 4, "expected_scope_revision": 0, "proposal": changed_proposal},
            self.identity, "t2", self.preview)
        state, accepted = flow.decide_request(state, requested["request_id"], self.decision("edit", validation, "explicit-new-boundary"), self.identity, "t3", self.apply)
        self.assertEqual(accepted["status"], "applied")
        decision = state["scope_workflow"]["decisions"][0]
        self.assertEqual(decision["action"], "edit")
        self.assertEqual(decision["authorization"]["origin"], "human_decision")
        self.assertEqual(decision["authorization"]["proposal_sha256"], flow.canonical_json_sha256(changed_proposal))

    def test_existing_authorization_cannot_be_reused_for_edited_content(self):
        payload = deepcopy(self.payload)
        payload["authorization_provenance"] = {"kind": "existing_human_authorization", "reference": "exact previously authorized source"}
        state, requested = flow.create_request(self.initial, payload, self.identity, "t1")
        self.assertEqual(requested["request_id"], state["scope_workflow"]["requests"][0]["request_id"])
        state, validation = flow.validate_request(state, requested["request_id"],
            {"expected_execution_revision": 4, "expected_scope_revision": 0,
             "proposal": {**self.proposal, "instructions": "Different new semantic boundary"}},
            self.identity, "t2", self.preview)
        state, response = flow.decide_request(state, requested["request_id"],
            self.decision("record_existing_authorization", validation, "cannot-borrow-approval"),
            self.identity, "t3", self.apply)
        self.assertEqual(response["error"], "SCOPE_EDIT_REQUIRES_EXPLICIT_DECISION")
        self.assertEqual(flow.scope_revision(state), 0)
        self.assertEqual(state["scope_workflow"]["decisions"], [])
        self.assertEqual(len(state["scope_workflow"]["attempts"]), 1)


class HumanScopeHTTPTests(legacy_tests.LegacyScopeControlTests):
    async def _request_human_scope(self, *, omit_source_hash=False):
        os.environ["NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP"] = json.dumps({self.PROJECT_CONTEXT: str(self.repo_path)})
        await self._import_sprint()
        current = await self._claim("continuity-coordinator")
        aid = self._assignment_id(current)
        state = self._stored_assignment()
        payload = {"idempotency_key": "scope-request-http", "assignment_id": aid,
            "expected_execution_revision": state["revision"], "expected_scope_revision": 0,
            "reason": "Execute the already bounded proof extension", "dependencies": ["Prerequisite lemma"],
            "proposal": {"instructions": "AUTHORITATIVE HUMAN SCOPE: verify proof without production changes.",
                "retained_restrictions": ["Do not deploy", "Do not bypass two reviews"],
                "node_ids": ["continuity-coordinator", "formal-linkage"], "reviewer_ids": ["reviewer-one", "reviewer-two"]},
            "source": {"repository_key": self.PROJECT_CONTEXT, "commit": self.base_commit,
                "path": self.MANIFEST_PATH, "sha256": self.base_manifest_sha256}}
        if omit_source_hash:
            del payload["source"]["sha256"]
        self.workflow_url = f"/api/v1/projects/{self.PROJECT_PHONE}/sprints/{self.sprint_id}"
        before = deepcopy(state)
        code, requested = await asgi_request(self.workflow_url + "/scope-requests", method="POST", payload=payload, headers=self._role_headers(self.COORDINATOR_PHONE))
        self.assertEqual(code, 200, requested)
        self.assertEqual(self._stored_assignment()["assignments"], before["assignments"])
        return aid, payload, requested

    async def test_http_server_resolves_source_hash_and_replay_stays_exact(self):
        _, payload, requested = await self._request_human_scope(omit_source_hash=True)
        self.assertEqual(requested["source"]["sha256"], self.base_manifest_sha256)
        before = main.git_config_path.read_bytes()
        url = self.workflow_url + "/scope-requests"
        with patch.object(main, "legacy_scope_git_command", side_effect=AssertionError("Replay must not re-read Git")):
            code, replay = await asgi_request(url, method="POST", payload=payload, headers=self._role_headers(self.COORDINATOR_PHONE))
        self.assertEqual(code, 200, replay)
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(replay["request_id"], requested["request_id"])
        self.assertEqual(main.git_config_path.read_bytes(), before)
        conflicting = deepcopy(payload)
        conflicting["source"]["path"] = "different-document.json"
        code, _ = await asgi_request(url, method="POST", payload=conflicting, headers=self._role_headers(self.COORDINATOR_PHONE))
        self.assertEqual(code, 409)
        self.assertEqual(main.git_config_path.read_bytes(), before)
        invalid = deepcopy(payload)
        invalid["idempotency_key"] = "wrong-expected-hash"
        invalid["source"]["sha256"] = "0" * 64
        code, error = await asgi_request(url, method="POST", payload=invalid, headers=self._role_headers(self.COORDINATOR_PHONE))
        self.assertNotEqual(code, 200, error)
        self.assertIn("SCOPE_SOURCE_HASH_MISMATCH", json.dumps(error))
        self.assertEqual(main.git_config_path.read_bytes(), before)

    async def test_http_human_decision_race_ack_and_read_only_observability(self):
        aid, payload, requested = await self._request_human_scope()
        req_url = self.workflow_url + "/scope-requests/" + requested["request_id"]
        version = {"expected_execution_revision": payload["expected_execution_revision"], "expected_scope_revision": 0}
        code, validation = await asgi_request(req_url + "/validate", method="POST", payload=version, headers=self._admin_headers())
        self.assertEqual(code, 200, validation)
        immutable_before = deepcopy(self._stored_assignment()["assignments"])
        queue_before = self._queue_snapshot()
        decisions = [{**version, "action": "approve", "validation_id": validation["validation_id"], "idempotency_key": "op-a"},
                     {**version, "action": "reject", "idempotency_key": "op-b"}]
        results = await asyncio.gather(*(asgi_request(req_url + "/decisions", method="POST", payload=p, headers=self._admin_headers()) for p in decisions))
        self.assertEqual(sorted(r[0] for r in results), [200, 409], results)
        state = self._stored_assignment()
        self.assertEqual(state["assignments"], immutable_before)
        self.assertEqual(self._queue_snapshot(), queue_before)
        self.assertEqual(len(state["scope_workflow"]["decisions"]), 1)
        self.assertEqual(len(state["scope_workflow"]["attempts"]), 1)
        # Approve coroutine enters aggregate lock first; reject must lose.
        self.assertEqual(state["scope_control"]["effective_revision"], 1)
        scope_url = f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/{aid}/effective-scope"
        code, effective = await asgi_request(scope_url, headers=self._role_headers(self.COORDINATOR_PHONE))
        self.assertEqual(code, 200, effective)
        self.assertIn("AUTHORITATIVE HUMAN SCOPE", effective["effective_core"]["profile"])
        before_ack = deepcopy(state)
        code, acknowledged = await asgi_request(scope_url + "/ack", method="POST", payload={"schema_version": 1, "scope_context": effective["scope_context"]}, headers=self._role_headers(self.COORDINATOR_PHONE))
        self.assertEqual(code, 200, acknowledged)
        after_ack = self._stored_assignment()
        for field in ("assignments", "current_assignment_id", "current_node_id", "workflow", "last_transition", "pending_transition"):
            self.assertEqual(before_ack.get(field), after_ack.get(field), field)
        self.assertEqual(self._queue_snapshot(), queue_before)
        files_before = {p: p.read_bytes() for p in (main.git_config_path, main.history_path, main.sprint_history_path)}
        code, view = await asgi_request(self.workflow_url + "/observability")
        self.assertEqual(code, 200, view)
        self.assertEqual({p: p.read_bytes() for p in files_before}, files_before)
        code, listing = await asgi_request(self.workflow_url + "/scope-requests")
        self.assertEqual(code, 200, listing)
        self.assertEqual(listing["requests"][0]["status"], "applied")

    async def test_http_reject_keeps_effective_scope_and_request_source_auth(self):
        aid, payload, requested = await self._request_human_scope()
        url = self.workflow_url + "/scope-requests/" + requested["request_id"] + "/decisions"
        decision = {"action": "reject", "idempotency_key": "no", "expected_execution_revision": payload["expected_execution_revision"], "expected_scope_revision": 0}
        before = deepcopy(self._stored_assignment())
        code, denied = await asgi_request(url, method="POST", payload=decision)
        self.assertEqual(code, 401, denied)
        self.assertEqual(self._stored_assignment(), before)
        code, result = await asgi_request(url, method="POST", payload=decision, headers=self._admin_headers())
        self.assertEqual(code, 200, result)
        self.assertNotIn("scope_control", self._stored_assignment())
        self.assertEqual(self._stored_assignment()["assignments"], before["assignments"])

    async def test_http_exact_accepted_retry_after_handoff_returns_old_receipt(self):
        aid, payload, requested = await self._request_human_scope()
        url = self.workflow_url + "/scope-requests/" + requested["request_id"]
        version = {"expected_execution_revision": payload["expected_execution_revision"], "expected_scope_revision": 0}
        code, validation = await asgi_request(url + "/validate", method="POST", payload=version, headers=self._admin_headers())
        self.assertEqual(code, 200, validation)
        decision = {**version, "action": "approve", "validation_id": validation["validation_id"], "idempotency_key": "retry-after-handoff"}
        code, accepted = await asgi_request(url + "/decisions", method="POST", payload=decision, headers=self._admin_headers())
        self.assertEqual(code, 200, accepted)
        scope = await self._scope_and_ack(aid, self.COORDINATOR_PHONE)
        await self._submit_effective(self.COORDINATOR_PHONE, aid, "RESUME_FORMAL", scope["scope_context"], result="Ordinary handoff after approved scope")
        self.assertNotEqual(self._stored_assignment()["current_assignment_id"], aid)
        before = main.git_config_path.read_bytes()
        code, replay = await asgi_request(url + "/decisions", method="POST", payload=decision, headers=self._admin_headers())
        self.assertEqual(code, 200, replay)
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(replay["receipt"], accepted["receipt"])
        self.assertEqual(main.git_config_path.read_bytes(), before)
        conflict = {**decision, "action": "reject"}
        code, _ = await asgi_request(url + "/decisions", method="POST", payload=conflict, headers=self._admin_headers())
        self.assertEqual(code, 409)
        self.assertEqual(main.git_config_path.read_bytes(), before)

    async def test_http_future_actor_drift_invalidates_validated_full_scope(self):
        _, payload, requested = await self._request_human_scope()
        url = self.workflow_url + "/scope-requests/" + requested["request_id"]
        version = {"expected_execution_revision": payload["expected_execution_revision"], "expected_scope_revision": 0}
        code, validation = await asgi_request(url + "/validate", method="POST", payload=version, headers=self._admin_headers())
        self.assertEqual(code, 200, validation)
        self.assertIn("formal-linkage", validation["diff"])
        self.assertIn("reviewer-one", validation["diff"])
        stored = json.loads(main.agents_path.read_text(encoding="utf-8"))
        for agent in stored["agents"]:
            if agent.get("id") == "formal-linkage":
                agent["profile"] += " unversioned external drift"
        main.agents_path.write_text(json.dumps(stored), encoding="utf-8")
        before = deepcopy(self._stored_assignment())
        queues_before = self._queue_snapshot()
        decision = {**version, "action": "approve", "validation_id": validation["validation_id"], "idempotency_key": "reject-unversioned-drift"}
        code, response = await asgi_request(url + "/decisions", method="POST", payload=decision, headers=self._admin_headers())
        self.assertEqual(code, 409, response)
        after = self._stored_assignment()
        # A losing stale attempt is append-only audit, not an accepted decision
        # or an execution mutation. Comparing file bytes would forbid that audit.
        execution = lambda value: {key: item for key, item in value.items()
                                   if key not in {"scope_workflow", "execution_audit"}}
        self.assertEqual(execution(after), execution(before))
        self.assertEqual(self._queue_snapshot(), queues_before)
        self.assertNotIn("scope_control", after)
        old_ledger, ledger = before["scope_workflow"], after["scope_workflow"]
        for field in ("requests", "validations", "decisions"):
            self.assertEqual(ledger[field], old_ledger[field], field)
        self.assertEqual(ledger["attempts"][:-1], old_ledger["attempts"])
        self.assertEqual(ledger["events"][:-1], old_ledger["events"])
        self.assertEqual(len(ledger["attempts"]), len(old_ledger["attempts"]) + 1)
        self.assertEqual(ledger["events"][-1]["kind"], "scope_decision_conflict")
        self.assertEqual(ledger["attempts"][-1]["response"]["error"], "SCOPE_VALIDATED_CONTENT_CHANGED")
        # Retrying the same rejected attempt is also immutable and idempotent.
        frozen = main.git_config_path.read_bytes()
        code, _ = await asgi_request(url + "/decisions", method="POST", payload=decision, headers=self._admin_headers())
        self.assertEqual(code, 409)
        self.assertEqual(main.git_config_path.read_bytes(), frozen)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite(loader.loadTestsFromTestCase(DecisionLedgerTests))
    suite.addTests(HumanScopeHTTPTests(name) for name in loader.getTestCaseNames(HumanScopeHTTPTests) if name.startswith("test_http_"))
    return suite


if __name__ == "__main__":
    unittest.main()
