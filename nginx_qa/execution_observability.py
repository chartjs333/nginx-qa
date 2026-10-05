"""Read-only, runtime-neutral execution views and append-only audit checkpoints.

Projection never claims work, acknowledges scope, or interprets a qualification
as graph completion. Historical views require an actual saved checkpoint; we do
not reconstruct an earlier execution from the current mutable assignment list.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        value = list(value.values())
    if not isinstance(value, (list, tuple)):
        return []
    return [deepcopy(dict(item)) for item in value if isinstance(item, Mapping)]


def _hash(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _revision(state: Mapping[str, Any]) -> Any:
    # A graph-definition revision is deliberately NOT an execution revision.
    return state.get("revision", state.get("execution_revision"))


def _event_order(event: Mapping[str, Any]) -> tuple:
    try:
        parsed = datetime.fromisoformat(str(event.get("timestamp")).replace("Z", "+00:00"))
        instant = parsed.replace(tzinfo=timezone.utc).timestamp() if parsed.tzinfo is None else parsed.timestamp()
    except (ValueError, TypeError, OverflowError):
        instant = float("inf")
    revision = event.get("execution_revision")
    sequence = event.get("event_sequence")
    return (instant, revision if type(revision) is int else -1,
            sequence if type(sequence) is int else -1, str(event.get("event_id")))


def _state(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return _dict(snapshot.get("execution", snapshot.get("assignment", snapshot)))


def checkpoint_payload(snapshot: Mapping[str, Any], scope_requests: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Capture only execution inputs, excluding recursive checkpoints and secrets.

    Caller must supply public runtime state (not config/environment/credentials).
    Queue/history records belong to the same locked mutation transaction when
    available; absence is explicitly represented as unknown, not an empty queue.
    """
    state = deepcopy(_state(snapshot))
    state.pop("execution_audit", None)
    state.pop("observability", None)
    return {
        "project_id": snapshot.get("project_id"),
        "sprint_id": snapshot.get("sprint_id", state.get("sprint_id")),
        "execution": state,
        "workflow": deepcopy(snapshot.get("workflow", state.get("workflow"))),
        "agents": deepcopy(snapshot.get("agents")),
        "pending_work": deepcopy(snapshot.get("pending_work")),
        "recent_activity": deepcopy(snapshot.get("recent_activity")),
        "scope_requests": deepcopy(list(scope_requests) if scope_requests else snapshot.get("scope_requests", [])),
    }


def append_execution_checkpoint(ledger: dict[str, Any], snapshot: Mapping[str, Any], *, recorded_at: str, scope_requests: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Append to a caller-owned transaction; must NOT be invoked from a GET.

    Exact repeated snapshots are no-ops. Never trims or overwrites checkpoints.
    The enclosing runtime transaction provides durability and writer exclusion.
    """
    payload = checkpoint_payload(snapshot, scope_requests)
    digest = _hash(payload)
    records = ledger.setdefault("checkpoints", [])
    if not isinstance(records, list):
        raise ValueError("Execution checkpoints must be an append-only list")
    if records and records[-1].get("snapshot_sha256") == digest:
        return deepcopy(records[-1])
    record = {
        "checkpoint_id": "checkpoint-" + _hash({"previous": records[-1]["checkpoint_id"] if records else None, "snapshot": digest})[:32],
        "sequence": len(records) + 1,
        "recorded_at": recorded_at,
        "execution_revision": _revision(payload["execution"]),
        "snapshot_sha256": digest,
        "snapshot": payload,
    }
    records.append(record)
    return deepcopy(record)


def _scope_overlay(state: Mapping[str, Any], assignment_id: Any) -> dict[str, Any]:
    control = _dict(state.get("scope_control"))
    binding = _dict(_dict(control.get("assignment_bindings")).get(str(assignment_id)))
    if not binding:
        return {"known": False, "effective_revision": None, "ack_status": "unknown", "effective_scope": None}
    candidates = _records(control.get("acknowledgements")) + _records(control.get("acknowledgement_history")) + _records(control.get("ack_history"))
    ack = next((item for item in reversed(candidates) if item.get("assignment_id") == assignment_id and item.get("scope_context") == binding.get("scope_context")), None)
    lineage = _records(_dict(control.get("binding_history")).get(str(assignment_id)))
    return {
        "known": True,
        "effective_revision": binding.get("effective_revision"),
        "amendment_id": binding.get("amendment_id"),
        "scope_context": deepcopy(binding.get("scope_context")),
        "effective_scope": deepcopy(binding.get("effective_core", binding.get("effective"))),
        "instruction_precedence": _dict(binding.get("scope_context")).get("precedence"),
        "ack_status": "accepted" if ack else "pending",
        "ack": ack,
        "binding_history": [{"effective_revision": old.get("effective_revision"),
                             "amendment_id": old.get("amendment_id"),
                             "bound_at": old.get("bound_at"),
                             "effective_scope": deepcopy(old.get("effective_core")),
                             "scope_context": deepcopy(old.get("scope_context")),
                             "ack": next((deepcopy(item) for item in candidates if item.get("scope_context") == old.get("scope_context")), None)} for old in lineage],
    }


def _managed_definition(state: Mapping[str, Any]) -> dict[str, Any]:
    revisions = _records(state.get("graph_revisions"))
    current = next((item for item in reversed(revisions) if item.get("revision") == state.get("graph_revision")), None)
    return _dict(current.get("definition")) if current else {}


def _topology(state: Mapping[str, Any], assignments: list[dict[str, Any]]) -> dict[str, Any]:
    workflow = _dict(state.get("workflow"))
    managed = state.get("sprint_type") == "managed_workspace_v1"
    definition = _managed_definition(state) if managed else workflow
    graph = _dict(definition.get("graph")) if managed and "graph" in definition else definition
    nodes = _records(graph.get("nodes"))
    terminal_nodes = _records(graph.get("terminal_nodes"))
    if isinstance(graph.get("terminal_nodes"), Mapping):
        terminal_nodes = [{"id": key, **_dict(value)} for key, value in graph["terminal_nodes"].items()]
    edges = _records(graph.get("edges"))
    for node in nodes:
        node_id = node.get("id", node.get("node_id"))
        node["id"] = node_id
        for outcome, destination in _dict(node.get("transitions")).items():
            for target in destination if isinstance(destination, list) else [destination]:
                edges.append({"source": node_id, "target": target, "outcome": outcome})
    if not nodes:
        # A parallel queue is not secretly a graph. Show known assignment nodes
        # and explicitly advertise missing topology rather than invent edges.
        ids = list(dict.fromkeys(item.get("node_id") for item in assignments if item.get("node_id")))
        nodes = [{"id": node_id} for node_id in ids]
    for terminal in terminal_nodes:
        terminal["terminal_definition"] = True
    ids = {item.get("id") for item in nodes}
    nodes.extend(item for item in terminal_nodes if item.get("id") not in ids)
    current_ids = {item.get("node_id") for item in assignments if item.get("is_current")}
    if state.get("current_node_id"):
        current_ids.add(state["current_node_id"])
    for node in nodes:
        matching = [item for item in assignments if item.get("node_id") == node["id"]]
        node["assignment_ids"] = [item["assignment_id"] for item in matching]
        node["position"] = "current" if node["id"] in current_ids else "visited" if matching else "unvisited"
        # "unvisited" is never changed to "skipped" without a graph event.
    reviewers = _records(_dict(definition.get("execution")).get("reviewers")) if managed else [{"id": value} for value in workflow.get("reviewer_agent_ids", [])]
    return {"known": bool(graph.get("nodes")), "nodes": nodes, "edges": edges, "reviewer_roles": reviewers, "graph_revision": state.get("graph_revision")}


def _assignments(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    current = set(state.get("active_assignment_ids") or [])
    if state.get("current_assignment_id"):
        current.add(state["current_assignment_id"])
    records = _records(state.get("assignments")) + _records(state.get("review_assignments"))
    counts: dict[str, int] = {}
    occurrence_by_assignment = {aid: record for record in _records(_dict(state.get("workflow")).get("occurrences")) for aid in record.get("assignment_ids", [])}
    output = []
    for record in records:
        assignment_id = record.get("assignment_id")
        if not assignment_id:
            continue
        node = str(record.get("node_id", ""))
        phase = record.get("phase") or ("review" if record.get("reviewer_id") or record.get("kind") == "transition_review" else "node")
        occurrence = occurrence_by_assignment.get(assignment_id, {})
        binding = _dict(_dict(_dict(state.get("scope_control")).get("assignment_bindings")).get(assignment_id))
        # An ordered assignment issuance list establishes visit order for legacy
        # graph nodes. Review assignments belong to the source node's visit.
        if phase == "node" and node:
            counts[node] = counts.get(node, 0) + 1
        visit = record.get("occurrence", binding.get("occurrence", counts.get(node)))
        output.append({
            **record,
            "phase": phase,
            "visit": visit,
            "occurrence_id": record.get("occurrence_id", occurrence.get("occurrence_id")),
            "agent_id": record.get("agent_id", record.get("reviewer_id")),
            "agent_phone": record.get("agent_phone", record.get("reviewer_phone")),
            "is_current": assignment_id in current or phase == "review" and record.get("status") in {"pending", "active"},
            "scope": _scope_overlay(state, assignment_id),
            "delivery_status": record.get("delivery_status", "delivered" if record.get("delivered_at") else "unknown"),
        })
    return output


def _timeline(snapshot: Mapping[str, Any], assignments: list[dict[str, Any]], requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state = _state(snapshot)
    events: dict[str, dict[str, Any]] = {}
    workflow_events = _records(_dict(state.get("scope_workflow")).get("events"))
    transition_sources = {item.get("source_assignment_id"): item for item in (state.get("pending_transition"), state.get("last_transition")) if isinstance(item, Mapping)}
    result_receipts = {item.get("result_key"): item for item in _records(state.get("result_receipts"))}
    result_assignment_ids = {item.get("assignment_id") for item in result_receipts.values()}

    def workflow_event(kind: str, field: str, value: Any) -> dict[str, Any]:
        return next((item for item in workflow_events if item.get("kind") == kind and item.get(field) == value), {})

    def add(kind: str, record: Mapping[str, Any], timestamp: Any, identity: Any = None, **links: Any) -> None:
        # Stable logical identity prevents duplicate events on polling/reconnect.
        event_id = str(identity or "event-" + _hash({"kind": kind, "record": record})[:32])
        if event_id in events:
            return
        events[event_id] = {"event_id": event_id, "kind": kind, "timestamp": timestamp,
                            "execution_revision": record.get("execution_revision", record.get("revision")),
                            "event_sequence": record.get("sequence"),
                            "assignment_id": record.get("assignment_id", links.get("assignment_id")),
                            "node_id": record.get("node_id", links.get("node_id")),
                            "visit": record.get("occurrence", links.get("visit")),
                            "metadata": deepcopy(dict(record)), **links}

    for assignment in assignments:
        aid = assignment["assignment_id"]
        issued = {key: assignment.get(key) for key in ("assignment_id", "node_id", "agent_id", "agent_phone", "phase", "git_branch", "occurrence_id", "created_at", "started_at")}
        add("reviewer_assignment" if assignment["phase"] == "review" else "assignment_issued", issued, assignment.get("started_at", assignment.get("created_at")), f"issued:{aid}", visit=assignment.get("visit"))
        if assignment.get("delivered_at") or assignment.get("claimed_at"):
            add("assignment_delivered", {"assignment_id": aid, "delivered_at": assignment.get("delivered_at", assignment.get("claimed_at"))}, assignment.get("delivered_at", assignment.get("claimed_at")), f"delivery:{aid}")
        if assignment.get("completed_at") and assignment["phase"] != "review" and aid not in result_assignment_ids:
            add("result_submitted", {key: assignment.get(key) for key in ("assignment_id", "outcome", "feedback", "result", "result_commit", "scope_context", "completed_at")}, assignment["completed_at"], f"result:{aid}", node_id=assignment.get("node_id"), visit=assignment.get("visit"))
        for review in _records(assignment.get("reviews")):
            add("review_decision", review, review.get("reviewed_at", review.get("decided_at")), "review:" + str(review.get("assignment_id") or _hash(review)), source_assignment_id=aid, reviewed_result=deepcopy(transition_sources.get(aid)))
    for field, kind, date in (
        ("result_receipts", "result_submitted", "accepted_at"),
        ("reviews", "review_decision", "decided_at"),
        ("transition_journal", "transition_gate", "updated_at"),
        ("reworks", "rework", "created_at"),
    ):
        for record in _records(state.get(field)):
            links = {"reviewed_result": deepcopy(result_receipts.get(record.get("result_key"))), "source_assignment_id": record.get("source_assignment_id")} if field == "reviews" else {}
            if field == "reviews":
                identity = "review:" + str(record.get("assignment_id") or _hash(record))
            elif field == "transition_journal":
                identity = "journal:" + str(record.get("journal_id") or record.get("result_key")) + ":" + str(record.get("state"))
            elif field == "result_receipts":
                identity = "result:" + str(record.get("assignment_id") or record.get("result_key"))
            else:
                identity = f"{field}:" + str(record.get("event_id") or record.get("rework_id") or _hash(record))
            add(kind, record, record.get(date, record.get("created_at")), identity, **links)
    for token in _records(_dict(state.get("workflow")).get("transition_tokens")):
        add("transition", token, token.get("created_at"), "transition-token:" + str(token.get("token_id")))
    for occurrence in _records(_dict(state.get("workflow")).get("occurrences")):
        add("node_visit", occurrence, occurrence.get("created_at"), "visit:" + str(occurrence.get("occurrence_id")))
    for event in _records(state.get("outbox")):
        issued = {key: event.get(key) for key in ("event_id", "event_type", "payload", "created_at")}
        add("outbox_enqueued", issued, event.get("created_at"), "outbox-issued:" + str(event.get("event_id")))
        if event.get("delivered_at"):
            add("outbox_delivered", event, event["delivered_at"], "outbox-delivered:" + str(event.get("event_id")))
    for transition in [state.get("pending_transition"), state.get("last_transition")]:
        if isinstance(transition, Mapping):
            tid = str(transition.get("transition_id") or _hash(transition))
            for review in _records(transition.get("reviews")):
                add("review_decision", review, review.get("reviewed_at"), "review:" + str(review.get("assignment_id") or _hash(review)), transition_id=tid, source_assignment_id=transition.get("source_assignment_id"), reviewed_result=deepcopy(dict(transition)))
            if transition.get("resolved_at"):
                add("transition", transition, transition["resolved_at"], "transition:" + tid)
    for record in _records(snapshot.get("recent_activity")):
        add("recorded_activity", record, record.get("timestamp", record.get("created_at")), str(record.get("event_id") or "activity:" + _hash(record)))
    control = _dict(state.get("scope_control"))
    for record in _records(control.get("amendments")):
        event = workflow_event("scope_applied", "scope_revision", record.get("effective_revision"))
        add("scope_applied", record, record.get("applied_at", record.get("created_at")), event.get("event_id") or "scope:" + str(record.get("amendment_id")), execution_revision=event.get("execution_revision", record.get("execution_revision")), event_sequence=event.get("sequence"))
    for record in _records(control.get("acknowledgements")) + _records(control.get("acknowledgement_history")) + _records(control.get("ack_history")):
        add("scope_ack", record, record.get("acknowledged_at", record.get("accepted_at")), "ack:" + str(record.get("ack_id") or record.get("acknowledgement_id") or _hash(record)))
    for request in requests:
        rid = request.get("request_id")
        original = {key: deepcopy(value) for key, value in request.items() if key not in {"decision", "status", "applied_at", "effective_revision"}}
        event = workflow_event("scope_request", "request_id", rid)
        add("scope_request", original, request.get("created_at", request.get("requested_at")), event.get("event_id") or "request:" + str(rid), event_sequence=event.get("sequence"))
        for decision in _records(request.get("decisions")) + ([request["decision"]] if isinstance(request.get("decision"), Mapping) else []):
            event = workflow_event("human_decision", "decision_id", decision.get("decision_id"))
            add("human_decision", decision, decision.get("timestamp", decision.get("decided_at", decision.get("created_at"))), event.get("event_id") or "decision:" + str(decision.get("decision_id") or _hash(decision)), request_id=rid, execution_revision=event.get("execution_revision", decision.get("execution_revision")), event_sequence=event.get("sequence"))
    for event in workflow_events:
        # Exact persisted ledger identities join the richer records above.
        # Validation/conflict events also remain observable and immutable.
        add(str(event.get("kind") or "scope_workflow_event"), event, event.get("timestamp"), event.get("event_id"))
    for record in _records(control.get("decision_attempts")) + _records(_dict(state.get("scope_workflow")).get("decision_attempts")):
        add("decision_attempt", record, record.get("created_at", record.get("attempted_at")))
    # A terminal event needs a recorded timestamp, not an inferred NO_GO label.
    if state.get("completed_at") or state.get("terminal_at"):
        add("terminal", {"status": state.get("status"), "terminal_node": deepcopy(state.get("terminal_node"))}, state.get("completed_at", state.get("terminal_at")), "terminal:" + str(state.get("sprint_id")))
    return sorted(events.values(), key=_event_order)


def _review_gates(state: Mapping[str, Any], assignments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pending = _dict(state.get("pending_transition"))
    gates = []
    if pending:
        reviews = _records(pending.get("reviews"))
        gates.append({"result_key": pending.get("transition_id"), "source_assignment_id": pending.get("source_assignment_id"), "result": pending.get("result"), "result_commit": pending.get("result_commit"), "required_approvals": 2, "applicable_approvals": sum(item.get("decision") == "APPROVE" for item in reviews), "reviews": reviews, "status": pending.get("status")})
    for receipt in _records(state.get("result_receipts")):
        key = receipt.get("result_key")
        reviews = [item for item in _records(state.get("reviews")) if key is not None and item.get("result_key") == key and item.get("result_commit") == receipt.get("result_commit") and item.get("source_assignment_id") == receipt.get("assignment_id")]
        gates.append({"result_key": key, "source_assignment_id": receipt.get("assignment_id"), "result_commit": receipt.get("result_commit"), "required_approvals": 2, "applicable_approvals": sum(item.get("decision") == "APPROVE" for item in reviews), "reviews": reviews, "status": receipt.get("status")})
    return gates


def project_execution(snapshot: Mapping[str, Any], *, scope_requests: Sequence[Mapping[str, Any]] | None = None, checkpoints: Sequence[Mapping[str, Any]] = (), cursor: str | None = None, at_revision: Any = None, at_checkpoint: str | None = None) -> dict[str, Any]:
    """Build a full idempotent refresh response; no callbacks, I/O or mutations.

    Cursor is the content hash, not a lossy event window: reconnect always gets
    the complete saved event set. Historical selection stays at one checkpoint.
    """
    for checkpoint in checkpoints:
        if _hash(checkpoint.get("snapshot")) != checkpoint.get("snapshot_sha256"):
            raise ValueError("Recorded execution checkpoint digest does not match")
    chosen = None
    if at_checkpoint is not None or at_revision is not None:
        chosen = (next((record for record in reversed(checkpoints) if record.get("checkpoint_id") == at_checkpoint), None)
                  if at_checkpoint is not None else next((record for record in reversed(checkpoints) if str(record.get("execution_revision")) == str(at_revision)), None))
        if chosen is None:
            raise ValueError("Historical execution snapshot is not recorded")
        snapshot = _dict(chosen.get("snapshot"))
        scope_requests = None
    state = _state(snapshot)
    requests = _records(scope_requests if scope_requests is not None else snapshot.get("scope_requests", _dict(state.get("scope_workflow")).get("requests", [])))
    assignments = _assignments(state)
    current = [item for item in assignments if item["is_current"]]
    pending = [item for item in requests if item.get("status") in {"pending", "pending_decision", "requested", "approved", "approved_pending_application", "applied_pending_ack"}]
    ack_pending = [item["assignment_id"] for item in current if item["scope"]["ack_status"] == "pending"]
    waiting_human = [item.get("request_id") for item in pending if item.get("status") in {"pending", "pending_decision", "requested"}]
    waiting_apply = [item.get("request_id") for item in pending if item.get("status") in {"approved", "approved_pending_application"}]
    attention = "authorization_required" if waiting_human else "waiting_for_scope_application" if waiting_apply else "waiting_for_scope_ack" if ack_pending else "scope_blocker_cleared" if any(item["scope"]["ack_status"] == "accepted" for item in current) else "none_recorded"
    workflow = _dict(state.get("workflow"))
    visits = _records(workflow.get("occurrences"))
    if not visits:
        visits = [{"node_id": item.get("node_id"), "visit": item.get("visit"), "assignment_ids": [item["assignment_id"]], "state": item.get("status"), "scope": item["scope"]} for item in assignments if item["phase"] != "review"]
    # Retain old transition/claim records after the runtime's current pointer
    # moves on. Earlier checkpoint metadata wins; never recolor old events from
    # the current assignment. Reconnect returns this complete de-duplicated set.
    saved_events: dict[str, dict[str, Any]] = {}
    for checkpoint in checkpoints:
        if chosen and checkpoint.get("sequence", 0) > chosen.get("sequence", 0):
            break
        old_snapshot = _dict(checkpoint.get("snapshot"))
        for event in _timeline(old_snapshot, _assignments(_state(old_snapshot)), _records(old_snapshot.get("scope_requests"))):
            if event.get("execution_revision") is None:
                event["observed_at_execution_revision"] = checkpoint.get("execution_revision")
            saved_events.setdefault(event["event_id"], event)
    for event in _timeline(snapshot, assignments, requests):
        saved_events.setdefault(event["event_id"], event)
    timeline = sorted(saved_events.values(), key=_event_order)
    review_gates = _review_gates(state, assignments)
    waiting_result_ids = {item["assignment_id"] for item in assignments if item.get("status") == "reviews_pending"}
    other_gates = ([{"kind": "transition_review", "transition_id": _dict(state.get("pending_transition")).get("transition_id")}] if state.get("pending_transition") else []) + _records(state.get("blocker_observations"))
    other_gates.extend({"kind": "result_review", "source_assignment_id": gate["source_assignment_id"], "result_key": gate["result_key"], "required_approvals": gate["required_approvals"], "applicable_approvals": gate["applicable_approvals"]} for gate in review_gates if gate["source_assignment_id"] in waiting_result_ids and gate["applicable_approvals"] < gate["required_approvals"])
    output = {
        "schema_version": 1,
        "project_id": snapshot.get("project_id"),
        "sprint_id": snapshot.get("sprint_id", state.get("sprint_id")),
        "runtime_type": "managed" if state.get("sprint_type") == "managed_workspace_v1" else "legacy_" + str(state.get("mode", "unknown")),
        "scope_revision": max([record.get("effective_revision", 0) for record in _records(_dict(state.get("scope_control")).get("amendments"))] or [0]),
        "execution": {"status": state.get("status"), "revision": _revision(state), "phase": state.get("phase"), "current_node_id": state.get("current_node_id"), "current_assignment_ids": [item["assignment_id"] for item in current], "terminal_node": deepcopy(state.get("terminal_node")), "blocked_reason": state.get("blocked_reason"), "qualification": deepcopy(state.get("qualification", state.get("formal_qualification")))},
        "attention": {"state": attention, "pending_request_ids": waiting_human, "pending_application_ids": waiting_apply, "pending_ack_assignment_ids": ack_pending, "other_gates": other_gates, "ack_does_not_resume_execution": True},
        "topology": _topology(state, assignments),
        "visits": visits,
        "assignments": assignments,
        "queue": {"known": snapshot.get("pending_work") is not None or "outbox" in state, "queues": deepcopy(snapshot.get("pending_work")), "outbox": deepcopy(state.get("outbox"))},
        "review_gates": review_gates,
        "scope_requests": requests,
        "pending_decisions": pending,
        "timeline": timeline,
        "history": {"mode": "historical" if chosen else "live", "selected_checkpoint_id": chosen.get("checkpoint_id") if chosen else None, "available": [{key: record.get(key) for key in ("checkpoint_id", "recorded_at", "execution_revision", "sequence")} for record in checkpoints], "before_first_checkpoint": "unavailable_not_reconstructed"},
        "mutated": False,
    }
    digest = _hash(output)
    output["cursor"] = digest
    output["unchanged"] = cursor == digest
    return output
