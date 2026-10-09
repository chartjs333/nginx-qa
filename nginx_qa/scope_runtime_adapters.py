"""Scope-control adapters without execution, publication, or queue side effects.

All functions are pure. Callers persist the returned aggregate under the same
lock/transaction used by ordinary execution. Managed graph revision is a frozen
topology identity and is deliberately not used as the scope event revision.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from .legacy_scope_control import (
    LegacyScopeControlError, acknowledgement_for_binding, acknowledgement_id,
    active_amendment, amendment_by_id, assignment_binding, make_assignment_binding,
    require_exact_scope_context, store_acknowledgement, store_assignment_binding,
    verify_role_token, attach_reviewed_source_context,
)


def runtime_kind(state: Mapping[str, Any]) -> str:
    if state.get("sprint_type") == "managed_workspace_v1":
        return "managed_workspace_v1"
    return "legacy_" + str(state.get("mode") or "parallel") + "_" + str(state.get("strategy") or "parallel")


def execution_revision(state: Mapping[str, Any]) -> int:
    return int(state.get("execution_revision") or 0) if runtime_kind(state) == "managed_workspace_v1" else int(state.get("revision") or 0)


def advance_revision(state: dict[str, Any]) -> None:
    key = "execution_revision" if runtime_kind(state) == "managed_workspace_v1" else "revision"
    state[key] = execution_revision(state) + 1


def assignment_records(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    records = [deepcopy(item) for item in state.get("assignments", []) if isinstance(item, dict)]
    if runtime_kind(state) == "managed_workspace_v1":
        originals = {item.get("assignment_id"): item for item in records}
        for item in state.get("review_assignments", []):
            if not isinstance(item, dict):
                continue
            source = originals.get(item.get("source_assignment_id"), {})
            records.append({**deepcopy(item), "agent_id": item.get("reviewer_id"),
                "agent_phone": item.get("reviewer_phone"), "node_id": source.get("node_id"),
                "occurrence_id": source.get("occurrence_id"), "graph_revision": source.get("graph_revision"),
                "phase": "review", "kind": "review"})
        for item in state.get("coordinator_contexts", []):
            if not isinstance(item, dict) or not item.get("context_id"):
                continue
            definition = _definition(state, item.get("graph_revision"))
            coordinator_node_id = (definition.get("coordinator") or {}).get("node_id")
            node = next((n for n in definition.get("nodes", []) if n.get("id") == coordinator_node_id), {})
            actor = node.get("agent") or {}
            settled = any(r.get("context_id") == item["context_id"] and r.get("status") == "completed" for r in state.get("recovery_records", []))
            records.append({**deepcopy(item), "assignment_id": item["context_id"],
                "node_id": coordinator_node_id, "agent_id": actor.get("id"), "agent_phone": actor.get("phone"),
                "status": "completed" if settled else "active", "phase": "node", "kind": "coordinator"})
    return records


def active_assignments(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [record for record in assignment_records(state) if record.get("status") in {"active", "prepared"}]


def assignment_identity(state: Mapping[str, Any], assignment_id: str) -> dict[str, Any]:
    matches = [item for item in assignment_records(state) if item.get("assignment_id") == assignment_id]
    record = next((item for item in matches if item.get("status") == "active"), matches[0] if matches else None)
    if record is None:
        raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_FOUND", "Assignment not found in the selected sprint", status_code=404)
    return record


def _definition(state: Mapping[str, Any], revision: Any = None) -> dict[str, Any]:
    wanted = int(revision or state.get("graph_revision") or 0)
    for record in state.get("graph_revisions", []):
        if isinstance(record, Mapping) and int(record.get("revision") or 0) == wanted:
            return deepcopy(record.get("definition") or {})
    return {}


def runtime_agents(state: Mapping[str, Any], agents: list[dict[str, Any]] | None = None,
                   assignment: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    if runtime_kind(state) != "managed_workspace_v1":
        return deepcopy(agents or [])
    definition = _definition(state, (assignment or {}).get("graph_revision"))
    result: dict[str, dict[str, Any]] = {}
    for node in definition.get("nodes", []):
        actor = node.get("agent") if isinstance(node, dict) else None
        if isinstance(actor, dict):
            result[str(actor.get("id"))] = {**deepcopy(actor), "profile": deepcopy(actor.get("profile")), "tasks": deepcopy(node.get("tasks") or [])}
    for reviewer in (definition.get("execution") or {}).get("reviewers", []):
        if isinstance(reviewer, dict):
            result[str(reviewer.get("id"))] = {**deepcopy(reviewer), "profile": deepcopy(reviewer.get("profile")), "tasks": deepcopy(reviewer.get("tasks") or [])}
    return list(result.values())


def normalized_scope_snapshot(state: Mapping[str, Any], agents: list[dict[str, Any]] | None = None,
                              assignment_id: str | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Adapt identity/topology for the pure authored-scope projector only."""
    adapted = deepcopy(dict(state))
    active = active_assignments(state)
    chosen = assignment_identity(state, assignment_id) if assignment_id else next((item for item in active if item.get("assignment_id") == state.get("current_assignment_id")), active[0] if active else None)
    if chosen is None or chosen.get("status") not in {"active", "prepared"}:
        raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_ACTIVE", "An exact active assignment is required")
    selected_agents = runtime_agents(state, agents, chosen)
    if runtime_kind(state) == "managed_workspace_v1":
        definition = _definition(state)
        nodes = [{**deepcopy(node), "agent_id": (node.get("agent") or {}).get("id")} for node in definition.get("nodes", []) if isinstance(node, dict) and node.get("agent")]
        reviewers = [item.get("id") for item in (definition.get("execution") or {}).get("reviewers", []) if isinstance(item, dict)]
        adapted.update({"mode": "sequential", "strategy": "conditional_graph", "status": "active", "workflow": {"enabled": True, "nodes": nodes, "reviewer_agent_ids": reviewers}, "assignments": assignment_records(state), "revision": execution_revision(state)})
    elif not isinstance(adapted.get("workflow"), dict):
        adapted["workflow"] = {"enabled": True, "nodes": [{"id": str(actor.get("id")), "agent_id": actor.get("id")} for actor in selected_agents], "reviewer_agent_ids": []}
    node_id = str(chosen.get("node_id") or chosen.get("agent_id") or "")
    adapted.update({"current_assignment_id": chosen["assignment_id"], "current_agent_id": chosen.get("agent_id"), "current_node_id": node_id, "phase": chosen.get("phase") or "node"})
    if state.get("mode") == "parallel":
        adapted["status"] = "active"
    occurrence = next((item for item in (state.get("workflow") or {}).get("occurrences", []) if item.get("occurrence_id") == chosen.get("occurrence_id")), {})
    adapted["visit_counts"] = {**(adapted.get("visit_counts") or {}), node_id: int(chosen.get("occurrence") or occurrence.get("generation") or occurrence.get("occurrence") or occurrence.get("occurrence_index") or (state.get("visit_counts") or {}).get(node_id) or 1)}
    return adapted, selected_agents


def effective_scope_snapshot(state: Mapping[str, Any], assignment_id: str) -> dict[str, Any]:
    record = assignment_identity(state, assignment_id)
    binding = assignment_binding(state, assignment_id)
    if binding is None:
        raise LegacyScopeControlError("SCOPE_BINDING_NOT_FOUND", "Assignment has no effective scope binding", status_code=404)
    acknowledgement = acknowledgement_for_binding(state.get("scope_control") or {}, binding)
    return {"assignment": record, "binding": binding, "scope_context": deepcopy(binding["scope_context"]),
        "issued": deepcopy(binding["issued"]), "effective": deepcopy(binding["effective"]),
        "effective_core": deepcopy(binding["effective_core"]), "acknowledgement": acknowledgement,
        "requires_scope_ack": acknowledgement is None, "execution_revision": execution_revision(state),
        "instruction_precedence": "effective_scope_supersedes_conflicting_issued_scope", "mutated": False}


def acknowledge_scope(state: Mapping[str, Any], assignment_id: str, context: Mapping[str, Any],
                      acknowledged_at: str) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate = deepcopy(dict(state))
    record = assignment_identity(candidate, assignment_id)
    if record.get("status") != "active":
        raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_ACTIVE", "Only an active assignment can ACK scope")
    snapshot = effective_scope_snapshot(candidate, assignment_id)
    binding = snapshot["binding"]
    require_exact_scope_context(context, binding)
    ack = snapshot["acknowledgement"]
    replay = ack is not None
    if ack is None:
        ack = {"ack_id": acknowledgement_id(binding), "assignment_id": assignment_id,
            "agent_id": binding["agent_id"], "agent_phone": binding["agent_phone"],
            "amendment_id": binding["amendment_id"], "effective_revision": binding["effective_revision"],
            "scope_context": deepcopy(binding["scope_context"]), "acknowledged_at": acknowledged_at}
        store_acknowledgement(candidate["scope_control"], ack)
        advance_revision(candidate)
    return candidate, {"acknowledgement": ack, "deduplicated": replay, "execution_revision": execution_revision(candidate), "graph_advanced": False, "queue_changed": False}


def require_scope_submission(state: Mapping[str, Any], assignment_id: str, context: Any,
                             supplied_role_token: str) -> None:
    binding = assignment_binding(state, assignment_id)
    if binding is None:
        return
    verify_role_token(binding.get("agent_phone"), supplied_role_token)
    require_exact_scope_context(context, binding)
    if acknowledgement_for_binding(state.get("scope_control") or {}, binding) is None:
        raise LegacyScopeControlError("SCOPE_ACK_REQUIRED", "ACK the exact effective scope before submitting work", status_code=428)


def link_new_execution_records(state: dict[str, Any], before: Mapping[str, Any]) -> None:
    """Add scope provenance only to new result/review records, never old ones."""
    for collection, identity_key in (("result_receipts", "result_key"), ("reviews", "assignment_id"), ("recovery_records", "recovery_id")):
        known = {item.get(identity_key) for item in before.get(collection, []) if isinstance(item, dict)}
        for item in state.get(collection, []):
            if not isinstance(item, dict) or item.get(identity_key) in known:
                continue
            binding = assignment_binding(state, item.get("assignment_id") or item.get("context_id"))
            if binding is not None:
                item["scope_context"] = deepcopy(binding["scope_context"])
                item["scope_ack_id"] = acknowledgement_id(binding)


def bind_new_assignments(state: dict[str, Any], *, rebind_active: bool = False, agents: list[dict[str, Any]] | None = None) -> None:
    """Bind new managed task/review occurrences inside their creation transaction."""
    control = state.get("scope_control")
    if not isinstance(control, dict):
        return
    amendment = active_amendment(control)
    if amendment is None:
        return
    for record in active_assignments(state):
        assignment_id = str(record.get("assignment_id") or "")
        prior = assignment_binding(state, assignment_id)
        if prior is not None and (not rebind_active or prior.get("effective_revision") == amendment.get("effective_revision")):
            continue
        selected_amendment = amendment
        source_id = str(record.get("source_assignment_id") or "")
        source_binding = assignment_binding(state, source_id) if source_id else None
        if prior is not None and prior.get("effective_revision") == selected_amendment.get("effective_revision"):
            continue
        node_id = str(record.get("node_id") or "")
        override_key = "reviewer_overrides" if record.get("kind") == "review" else "node_overrides"
        override_id = str(record.get("agent_id") or "") if record.get("kind") == "review" else node_id
        override = (selected_amendment.get(override_key) or {}).get(override_id)
        if not isinstance(override, dict):
            continue
        actors = runtime_agents(state, agents, assignment=record)
        actor = next((item for item in actors if item.get("id") == record.get("agent_id")), None)
        if actor is None:
            raise LegacyScopeControlError("SCOPE_RUNTIME_ACTOR_MISMATCH", "Managed frozen scope actor is missing", status_code=503)
        effective = {**deepcopy(actor), **deepcopy(override.get("effective") or {})}
        issued_task = deepcopy((prior or {}).get("issued", {}).get("active_task"))
        from .legacy_scope_control import project_authored_active_task
        effective_task = project_authored_active_task(issued_task, actor, effective) if issued_task is not None else None
        binding = make_assignment_binding(amendment=selected_amendment, assignment=record,
            issued_agent=actor, effective_agent=effective, node_id=node_id,
            phase=str(record.get("phase") or "node"), occurrence=int(record.get("occurrence") or next((o.get("generation") for o in (state.get("workflow") or {}).get("occurrences", []) if o.get("occurrence_id") == record.get("occurrence_id")), None) or int(record.get("rework_cycle") or 0) + 1),
            source_assignment_id=source_id, issued_active_task=issued_task, effective_active_task=effective_task,
            bound_at=str(selected_amendment.get("applied_at") if rebind_active else record.get("created_at") or state.get("updated_at") or ""))
        if runtime_kind(state) == "managed_workspace_v1" or state.get("mode") == "parallel" or state.get("strategy") == "queue_graph":
            binding["instruction_snapshot_only"] = True
        if source_binding:
            attach_reviewed_source_context(binding, source_binding["scope_context"])
        store_assignment_binding(control, binding)


def apply_scope_revision(state: Mapping[str, Any], agents: list[dict[str, Any]], *, assignment_id: str | None = None, **kwargs: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    from .legacy_scope_control import apply_semantic_scope_revision
    adapted, selected_agents = normalized_scope_snapshot(state, agents, assignment_id)
    proposal = kwargs.get("proposal") or {}
    for active in active_assignments(state):
        if (active.get("scope_handoff_intent") or {}).get("status") == "publishing" and (
            (active.get("node_id") or active.get("agent_id")) in proposal.get("node_ids", []) or active.get("agent_id") in proposal.get("reviewer_ids", [])):
            raise LegacyScopeControlError("SCOPE_HANDOFF_PUBLISHING", "Complete the accepted ordinary handoff before amending its affected scope")
    if runtime_kind(state) == "managed_workspace_v1" or state.get("mode") == "parallel" or state.get("strategy") == "queue_graph":
        kwargs["instruction_snapshot_only"] = True
    projected, receipt = apply_semantic_scope_revision(adapted, selected_agents, **kwargs)
    candidate = deepcopy(dict(state))
    candidate["scope_control"] = deepcopy(projected["scope_control"])
    bind_new_assignments(candidate, rebind_active=True, agents=agents)
    if runtime_kind(state) == "managed_workspace_v1":
        advance_revision(candidate)
        receipt["execution_revision"] = execution_revision(candidate)
    return candidate, receipt
