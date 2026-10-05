"""Offline contract tests: no main import, live API, runtime writer, or services."""
from copy import deepcopy
import json
from pathlib import Path
import shutil
import subprocess
import unittest

from nginx_qa.execution_observability import append_execution_checkpoint, project_execution


def delta_fixture():
    """Revision 74 is compatibility data, never a claim about live Delta."""
    aid = "89e70d7d-7b35-43ff-b5c2-cfe96f8526c6"
    context = {"assignment_id": aid, "effective_revision": 1, "amendment_id": "ISC-S16-D01", "precedence": "effective_scope_supersedes_conflicting_issued_scope"}
    binding = {"assignment_id": aid, "effective_revision": 1, "amendment_id": "ISC-S16-D01", "occurrence": 4, "scope_context": context, "effective_core": {"profile": "Proof-only", "tasks": []}}
    return {
        "project_id": "9000", "sprint_id": "sprint-0001-783ef52c",
        "execution": {
            "mode": "sequential", "strategy": "conditional_graph", "revision": 74,
            "status": "active", "phase": "node", "current_node_id": "continuity-coordinator",
            "current_assignment_id": aid, "formal_qualification": "NO_GO",
            "workflow": {"nodes": [{"id": "continuity-coordinator", "transitions": {"READY": "formal-linkage"}}, {"id": "formal-linkage", "transitions": {"NO_GO": "continuity-coordinator", "GO": "done"}}, {"id": "conditional-unvisited", "transitions": {"DONE": "done"}}], "terminal_nodes": {"done": {"id": "done", "status": "DONE"}}, "reviewer_agent_ids": ["review-1", "review-2"]},
            "assignments": [{"assignment_id": "old-c1", "node_id": "continuity-coordinator", "agent_id": "coordinator", "status": "transitioned", "started_at": "2026-10-01T10:00:00Z", "completed_at": "2026-10-01T11:00:00Z", "outcome": "READY"}, {"assignment_id": "old-f1", "node_id": "formal-linkage", "status": "transitioned", "started_at": "2026-10-01T12:00:00Z", "completed_at": "2026-10-01T13:00:00Z", "outcome": "NO_GO"}, {"assignment_id": aid, "node_id": "continuity-coordinator", "agent_id": "isc-s16-continuity-coordinator", "agent_phone": "2750", "git_branch": "agent/isc-s16-continuous-sprint", "status": "active", "started_at": "2026-10-05T10:00:00Z"}],
            "scope_control": {"amendments": [{"amendment_id": "ISC-S16-D01", "effective_revision": 1, "applied_at": "2026-10-04T12:00:00Z"}], "assignment_bindings": {aid: binding}, "acknowledgements": {aid: {"ack_id": "scope-ack-c32b5df9df042413dd48394724696bc5", "assignment_id": aid, "scope_context": context, "acknowledged_at": "2026-10-05T10:01:00Z"}}},
        }, "pending_work": {"worker": []}, "recent_activity": [],
    }


def managed_fixture():
    return {"project_id": "9011", "sprint_id": "managed-fixture", "execution": {
        "sprint_type": "managed_workspace_v1", "status": "active", "revision": 3, "graph_revision": 1,
        "graph_revisions": [{"revision": 1, "definition": {"execution": {"mode": "parallel", "reviewers": [{"id": "alice"}, {"id": "bob"}]}, "nodes": [{"id": "build", "transitions": {"PASS": "done"}}, {"id": "done", "type": "terminal", "status": "DONE"}]}}],
        "workflow": {"execution_mode": "parallel", "occurrences": [{"occurrence_id": "occ-1", "node_id": "build", "generation": 1, "state": "reviews_pending", "assignment_ids": ["work-1"]}]},
        "active_assignment_ids": ["work-1"], "assignments": [{"assignment_id": "work-1", "node_id": "build", "occurrence_id": "occ-1", "agent_id": "builder", "status": "reviews_pending", "created_at": "2026-10-05T09:00:00Z"}],
        "review_assignments": [{"assignment_id": "ra-1", "source_assignment_id": "work-1", "result_key": "result-1", "reviewer_id": "alice", "status": "active"}],
        "result_receipts": [{"assignment_id": "work-1", "result_key": "result-1", "result_commit": "abc", "created_at": "2026-10-05T09:01:00Z"}],
        "reviews": [{"assignment_id": "historical-review", "source_assignment_id": "work-1", "result_key": "different-result", "result_commit": "abc", "decision": "APPROVE"}, {"assignment_id": "new-review", "source_assignment_id": "work-1", "result_key": "result-1", "result_commit": "abc", "reviewer_id": "alice", "decision": "APPROVE", "decided_at": "2026-10-05T09:02:00Z"}],
        "outbox": [{"event_id": "enqueue-1", "event_type": "ASSIGNMENT_ENQUEUE", "status": "pending", "payload": {"assignment_id": "work-1"}}],
    }}


class ExecutionObservabilityTests(unittest.TestCase):
    def test_read_projection_has_no_mutations(self):
        for source in (delta_fixture(), managed_fixture()):
            original = deepcopy(source)
            first = project_execution(source)
            second = project_execution(source, cursor=first["cursor"])
            self.assertEqual(source, original)
            self.assertFalse(first["mutated"])
            self.assertTrue(second["unchanged"])

    def test_delta_identity_is_fixture_and_execution_not_qualification(self):
        view = project_execution(delta_fixture())
        self.assertEqual(view["execution"]["revision"], 74)
        self.assertEqual(view["execution"]["status"], "active")
        self.assertEqual(view["execution"]["qualification"], "NO_GO")
        self.assertIsNone(view["execution"]["terminal_node"])
        self.assertEqual(view["assignments"][-1]["visit"], 4)
        self.assertEqual(view["scope_revision"], 1)

    def test_attention_does_not_change_execution(self):
        fixture = delta_fixture()
        request = {"request_id": "request-1", "status": "approved_pending_application", "reason": "Existing authorization"}
        view = project_execution(fixture, scope_requests=[request])
        self.assertEqual(view["attention"]["state"], "waiting_for_scope_application")
        self.assertEqual(view["execution"]["status"], "active")
        self.assertEqual(view["execution"]["current_assignment_ids"], [fixture["execution"]["current_assignment_id"]])

    def test_unvisited_branch_is_not_skipped(self):
        view = project_execution(delta_fixture())
        node = next(item for item in view["topology"]["nodes"] if item["id"] == "conditional-unvisited")
        self.assertEqual(node["position"], "unvisited")
        self.assertNotIn("skipped", json.dumps(view))

    def test_unknown_is_not_zero_or_success(self):
        view = project_execution({"project_id": "p", "execution": {"mode": "parallel"}})
        self.assertFalse(view["topology"]["known"])
        self.assertFalse(view["queue"]["known"])
        self.assertIsNone(view["execution"]["revision"])
        self.assertIsNone(view["execution"]["qualification"])

    def test_managed_graph_definition_and_occurrence(self):
        view = project_execution(managed_fixture())
        self.assertEqual(view["runtime_type"], "managed")
        self.assertTrue(view["topology"]["known"])
        self.assertEqual(view["topology"]["edges"], [{"source": "build", "target": "done", "outcome": "PASS"}])
        self.assertEqual(view["visits"][0]["occurrence_id"], "occ-1")
        self.assertEqual(len(view["topology"]["reviewer_roles"]), 2)

    def test_reviews_count_only_exact_result_and_commit(self):
        view = project_execution(managed_fixture())
        gate = view["review_gates"][0]
        self.assertEqual(gate["applicable_approvals"], 1)
        self.assertEqual(gate["required_approvals"], 2)
        self.assertEqual(gate["reviews"][0]["assignment_id"], "new-review")
        self.assertEqual(view["attention"]["other_gates"][0]["kind"], "result_review")

    def test_ack_old_context_does_not_authorize_new_scope(self):
        fixture = delta_fixture(); state = fixture["execution"]; aid = state["current_assignment_id"]
        state["scope_control"]["assignment_bindings"][aid]["scope_context"] = {"assignment_id": aid, "effective_revision": 2}
        view = project_execution(fixture)
        self.assertEqual(view["assignments"][-1]["scope"]["ack_status"], "pending")
        self.assertEqual(view["attention"]["state"], "waiting_for_scope_ack")

    def test_ack_exact_clears_scope_only_preserves_other_gate(self):
        fixture = delta_fixture(); fixture["execution"]["pending_transition"] = {"transition_id": "tr-1", "status": "reviewing", "reviews": []}
        view = project_execution(fixture)
        self.assertEqual(view["attention"]["state"], "scope_blocker_cleared")
        self.assertTrue(view["attention"]["other_gates"])
        self.assertEqual(view["execution"]["status"], "active")
        self.assertFalse(any(event["kind"] in {"resume", "terminal", "transition"} for event in view["timeline"]))

    def test_binding_history_exact_acks(self):
        fixture = delta_fixture(); state = fixture["execution"]; aid = state["current_assignment_id"]; control = state["scope_control"]
        old = deepcopy(control["assignment_bindings"][aid]); new = deepcopy(old); new["effective_revision"] = 2; new["scope_context"]["effective_revision"] = 2
        control["binding_history"] = {aid: [old, new]}; control["assignment_bindings"][aid] = new
        control["acknowledgement_history"] = {"historic-ack": deepcopy(control["acknowledgements"][aid])}
        lineage = project_execution(fixture)["assignments"][-1]["scope"]["binding_history"]
        self.assertIsNotNone(lineage[0]["ack"])
        self.assertIsNone(lineage[1]["ack"])

    def test_checkpoints_append_idempotently_no_recursive_data(self):
        fixture = delta_fixture(); ledger = fixture["execution"].setdefault("execution_audit", {})
        first = append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T10:02:00Z")
        repeated = append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T10:03:00Z")
        self.assertEqual(first, repeated)
        self.assertEqual(len(ledger["checkpoints"]), 1)
        self.assertNotIn("execution_audit", first["snapshot"]["execution"])

    def test_historical_snapshot_not_projected_from_current_scope(self):
        fixture = delta_fixture(); ledger = {}; point = append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T10:02:00Z")
        aid = fixture["execution"]["current_assignment_id"]
        fixture["execution"]["revision"] = 75
        fixture["execution"]["scope_control"]["assignment_bindings"][aid]["effective_core"]["profile"] = "Future scope"
        append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T10:03:00Z")
        historical = project_execution(fixture, checkpoints=ledger["checkpoints"], at_checkpoint=point["checkpoint_id"])
        self.assertEqual(historical["execution"]["revision"], 74)
        self.assertEqual(historical["assignments"][-1]["scope"]["effective_scope"]["profile"], "Proof-only")
        self.assertEqual(historical["history"]["mode"], "historical")
        self.assertEqual(historical["history"]["selected_checkpoint_id"], point["checkpoint_id"])

    def test_missing_or_corrupt_history_fails_explicitly(self):
        fixture = delta_fixture(); ledger = {}
        with self.assertRaisesRegex(ValueError, "not recorded"):
            project_execution(fixture, at_revision=73)
        append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T10:02:00Z")
        ledger["checkpoints"][0]["snapshot"]["execution"]["revision"] = 0
        with self.assertRaisesRegex(ValueError, "digest"):
            project_execution(fixture, checkpoints=ledger["checkpoints"])

    def test_poll_reconnect_keeps_old_transitions_without_duplicates(self):
        fixture = delta_fixture(); ledger = {}
        fixture["execution"]["last_transition"] = {"transition_id": "first", "resolved_at": "2026-10-05T09:00:00Z", "outcome": "NO_GO", "source_assignment_id": "old-f1"}
        append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T10:02:00Z")
        fixture["execution"]["last_transition"] = {"transition_id": "second", "resolved_at": "2026-10-05T11:00:00Z", "outcome": "READY", "source_assignment_id": "next-c"}
        fixture["execution"]["revision"] = 76
        append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T11:01:00Z")
        view = project_execution(fixture, checkpoints=ledger["checkpoints"], cursor="missed-revision")
        ids = [event["event_id"] for event in view["timeline"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertIn("transition:first", ids); self.assertIn("transition:second", ids)
        self.assertEqual(next(e for e in view["timeline"] if e["event_id"] == "transition:first")["metadata"]["outcome"], "NO_GO")

    def test_terminal_historical_view_has_no_future_events(self):
        fixture = managed_fixture(); fixture["execution"].update(status="completed", completed_at="2026-10-05T11:00:00Z", active_assignment_ids=[])
        ledger = {}; point = append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T11:00:00Z")
        future = deepcopy(fixture); future["execution"]["revision"] = 4
        future["execution"]["reviews"].append({"assignment_id": "future", "decision": "REJECT", "decided_at": "2026-10-06T10:00:00Z"})
        append_execution_checkpoint(ledger, future, recorded_at="2026-10-06T10:00:00Z")
        view = project_execution(future, checkpoints=ledger["checkpoints"], at_checkpoint=point["checkpoint_id"])
        self.assertEqual(view["execution"]["status"], "completed")
        self.assertTrue(any(event["kind"] == "terminal" for event in view["timeline"]))
        self.assertFalse(any(event.get("assignment_id") == "future" for event in view["timeline"]))

    def test_persisted_human_decision_and_stale_attempt_timeline(self):
        fixture = delta_fixture()
        fixture["scope_requests"] = [{"request_id": "r", "created_at": "2026-10-05T11:00:00Z", "status": "applied", "decision": {"decision_id": "d", "timestamp": "2026-10-05T11:01:00Z", "action": "approve"}}]
        fixture["execution"]["scope_workflow"] = {"events": [{"event_id": "req-event", "kind": "scope_request", "request_id": "r", "timestamp": "2026-10-05T11:00:00Z", "execution_revision": 74}, {"event_id": "dec-event", "kind": "human_decision", "request_id": "r", "decision_id": "d", "timestamp": "2026-10-05T11:01:00Z", "execution_revision": 75}, {"event_id": "conflict-event", "kind": "scope_decision_conflict", "request_id": "r", "timestamp": "2026-10-05T11:02:00Z", "reason": "SCOPE_DECISION_ALREADY_TAKEN"}]}
        events = project_execution(fixture)["timeline"]
        self.assertEqual(len([e for e in events if e["kind"] == "human_decision"]), 1)
        decision = next(e for e in events if e["event_id"] == "dec-event")
        self.assertEqual(decision["timestamp"], "2026-10-05T11:01:00Z")
        self.assertEqual(decision["execution_revision"], 75)
        self.assertTrue(any(e["kind"] == "scope_decision_conflict" for e in events))

    def test_same_timestamp_uses_saved_workflow_sequence_not_hash_sort(self):
        fixture = delta_fixture()
        fixture["execution"]["scope_workflow"] = {"events": [{"event_id": "z-first", "kind": "human_decision", "sequence": 9, "timestamp": "2026-10-05T11:01:00Z", "execution_revision": 75}, {"event_id": "a-second", "kind": "scope_applied", "sequence": 10, "timestamp": "2026-10-05T13:01:00+02:00", "execution_revision": 75}]}
        events = [event["event_id"] for event in project_execution(fixture)["timeline"] if event["event_id"] in {"z-first", "a-second"}]
        self.assertEqual(events, ["z-first", "a-second"])

    def test_review_drilldown_keeps_exact_saved_result(self):
        fixture = delta_fixture()
        fixture["execution"]["last_transition"] = {"transition_id": "tr-reviewed", "source_assignment_id": "old-f1", "result": "Exact historical NO_GO evidence", "resolved_at": "2026-10-01T14:00:00Z", "reviews": [{"assignment_id": "review-exact", "decision": "APPROVE", "reviewed_at": "2026-10-01T13:30:00Z"}]}
        ledger = {}; append_execution_checkpoint(ledger, fixture, recorded_at="2026-10-05T10:00:00Z")
        fixture["execution"]["last_transition"] = {"transition_id": "new", "source_assignment_id": "new", "result": "Different future evidence", "resolved_at": "2026-10-05T12:00:00Z"}
        event = next(event for event in project_execution(fixture, checkpoints=ledger["checkpoints"])["timeline"] if event["event_id"] == "review:review-exact")
        self.assertEqual(event["reviewed_result"]["result"], "Exact historical NO_GO evidence")

    def test_managed_two_reviews_and_pending_journal_not_a_transition(self):
        fixture = managed_fixture()
        fixture["execution"]["reviews"].append({"assignment_id": "second-review", "source_assignment_id": "work-1", "result_key": "result-1", "result_commit": "abc", "reviewer_id": "bob", "decision": "APPROVE", "decided_at": "2026-10-05T09:03:00Z"})
        fixture["execution"]["transition_journal"] = [{"journal_id": "j1", "result_key": "result-1", "state": "REVIEWS_PENDING", "updated_at": "2026-10-05T09:01:00Z"}]
        events = project_execution(fixture)["timeline"]
        self.assertEqual(len([event for event in events if event["kind"] == "review_decision"]), 3)
        self.assertFalse(any(event["kind"] == "transition" for event in events))
        self.assertTrue(any(event["kind"] == "transition_gate" for event in events))

    def test_managed_receipt_timestamp_and_no_duplicate_completion_result(self):
        fixture = managed_fixture(); fixture["execution"]["assignments"][0]["completed_at"] = "2026-10-05T10:00:00Z"
        fixture["execution"]["result_receipts"][0]["accepted_at"] = "2026-10-05T09:01:01Z"
        events = [event for event in project_execution(fixture)["timeline"] if event["kind"] == "result_submitted"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["timestamp"], "2026-10-05T09:01:01Z")

    @unittest.skipUnless(shutil.which("node"), "Node.js unavailable for UI contract checks")
    def test_ui_helpers_and_read_only_asset_contract(self):
        asset = Path(__file__).resolve().parents[1] / "nginx_qa" / "static" / "execution.js"
        script = r"""const assert=require('node:assert/strict'),ui=require(process.argv[1]);
assert.equal(ui.uniqueEvents([{event_id:'a'},{event_id:'b'},{event_id:'a'}]).length,2);
assert.deepEqual(ui.lines(' a\n\n b '),['a','b']);
assert.equal(ui.editable({status:'pending'}),true);
assert.equal(ui.editable({status:'approved_pending_application'}),true);
assert.equal(ui.editable({status:'applied'}),false);
const existing={status:'approved_pending_application',authorization_provenance:{kind:'existing_human_authorization',reference:'saved-human-decision'}};
assert.deepEqual(ui.decisionChoices(existing).map(choice=>choice[0]),['record_existing_authorization','reject','edit']);
assert.deepEqual(ui.decisionChoices({status:'pending'}).map(choice=>choice[0]),['approve','reject','edit']);
assert.match(ui.decisionChoices(existing)[2][1],/новое решение/);
assert.notEqual(ui.proposalText({instructions:'old'}),ui.proposalText({instructions:'edited'}));
assert.match(ui.unknown(null),/неизвестно/);"""
        result = subprocess.run([shutil.which("node"), "-e", script, str(asset)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        source = asset.read_text(encoding="utf-8")
        self.assertNotIn("innerHTML", source)
        for forbidden in ('api("/work', 'api("/api/v1/agents/whoami', '/effective-scope/ack",'):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
