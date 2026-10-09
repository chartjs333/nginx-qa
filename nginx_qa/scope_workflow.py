"""Atomic, append-only human scope decisions shared by runtime adapters.

This module does no I/O. Callers hold the authoritative aggregate's lock and
persist the returned state exactly once; a failed application never publishes
the decision. Execution movement is deliberately outside this module.
"""

from copy import deepcopy
from difflib import unified_diff
import json
import re
from typing import Any, Callable

from .legacy_scope_control import canonical_json_sha256


class ScopeDecisionError(ValueError):
    def __init__(self, code: str, message: str = "", status_code: int = 409):
        super().__init__(message or code)
        self.code, self.status_code = code, status_code


def execution_revision(state: dict) -> int:
    return int(state.get("revision", state.get("execution_revision", 0)) or 0)


def scope_revision(state: dict) -> int:
    return int((state.get("scope_control") or {}).get("effective_revision", 0))


def _ledger(state: dict) -> dict:
    ledger = state.setdefault("scope_workflow", {
        "schema_version": 1, "revision": 0, "requests": [], "validations": [],
        "decisions": [], "attempts": [], "events": [],
    })
    if ledger.get("schema_version") != 1:
        raise ScopeDecisionError("SCOPE_WORKFLOW_VERSION_UNSUPPORTED", status_code=503)
    return ledger


def _event(ledger: dict, kind: str, now: str, **fields: Any) -> None:
    ledger["revision"] += 1
    event = {"sequence": ledger["revision"], "kind": kind, "timestamp": now, **fields}
    event["event_id"] = "scope-event-" + canonical_json_sha256(event)[:32]
    ledger["events"].append(event)


def _text(value: Any, field: str, *, limit: int = 65536) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ScopeDecisionError("SCOPE_REQUEST_INVALID", f"Invalid {field}", 400)
    return value.strip()


def validate_proposal(raw: Any) -> dict:
    if not isinstance(raw, dict) or set(raw) != {
        "instructions", "retained_restrictions", "node_ids", "reviewer_ids"
    }:
        raise ScopeDecisionError("SCOPE_PROPOSAL_INVALID", status_code=400)
    result = {"instructions": _text(raw["instructions"], "instructions")}
    for field in ("retained_restrictions", "node_ids", "reviewer_ids"):
        values = raw[field]
        if not isinstance(values, list) or len(values) > 256:
            raise ScopeDecisionError("SCOPE_PROPOSAL_INVALID", f"Invalid {field}", 400)
        values = [_text(value, field, limit=8192) for value in values]
        if len(set(values)) != len(values):
            raise ScopeDecisionError("SCOPE_PROPOSAL_INVALID", f"Duplicate {field}", 400)
        result[field] = values
    if not result["node_ids"]:
        raise ScopeDecisionError("SCOPE_PROPOSAL_INVALID", "At least one target is required", 400)
    return result


def validate_source(raw: Any) -> dict:
    if not isinstance(raw, dict) or set(raw) != {"repository_key", "commit", "path", "sha256"}:
        raise ScopeDecisionError("SCOPE_SOURCE_INVALID", status_code=400)
    source = {key: _text(value, key, limit=2048) for key, value in raw.items()}
    if not re.fullmatch(r"[0-9a-f]{40}", source["commit"]) or not re.fullmatch(r"[0-9a-f]{64}", source["sha256"]):
        raise ScopeDecisionError("SCOPE_SOURCE_INVALID", "Full commit and blob SHA-256 required", 400)
    if source["path"].startswith(("/", "\\")) or ".." in source["path"].replace("\\", "/").split("/") or ":" in source["path"]:
        raise ScopeDecisionError("SCOPE_SOURCE_INVALID", "Repository-relative path required", 400)
    return source


def _cas(state: dict, payload: dict) -> None:
    if type(payload.get("expected_execution_revision")) is not int or type(payload.get("expected_scope_revision")) is not int:
        raise ScopeDecisionError("SCOPE_EXPECTED_REVISION_REQUIRED", status_code=400)
    if payload["expected_execution_revision"] != execution_revision(state):
        raise ScopeDecisionError("SCOPE_EXECUTION_REVISION_CONFLICT")
    if payload["expected_scope_revision"] != scope_revision(state):
        raise ScopeDecisionError("SCOPE_REVISION_CONFLICT")


def _find_request(ledger: dict, request_id: str) -> dict:
    found = next((r for r in ledger["requests"] if r["request_id"] == request_id), None)
    if found is None:
        raise ScopeDecisionError("SCOPE_REQUEST_NOT_FOUND", status_code=404)
    return found


def _decision(ledger: dict, request_id: str) -> dict | None:
    return next((d for d in ledger["decisions"] if d["request_id"] == request_id), None)


def _undecided_status(request: dict) -> str:
    # Provenance describes a prior semantic decision, not a new consent request.
    # This presentation status grants no application authority: the operator
    # must still validate and explicitly record the unchanged prior permission.
    provenance = request.get("authorization_provenance") or {}
    return "approved_pending_application" if provenance.get("kind") == "existing_human_authorization" else "pending"


def _still_bound(state: dict, request: dict, identity: dict) -> None:
    # Adapters provide current identity, including source-result/visit bindings.
    if request["identity"] != identity or request["base_scope_revision"] != scope_revision(state):
        raise ScopeDecisionError("SCOPE_REQUEST_STALE")


def create_request(state: dict, payload: dict, identity: dict, now: str) -> tuple[dict, dict]:
    changed = deepcopy(state)
    ledger = _ledger(changed)
    key = _text(payload.get("idempotency_key"), "idempotency_key", limit=200)
    digest = canonical_json_sha256(payload)
    prior = next((r for r in ledger["requests"] if r["idempotency_key"] == key), None)
    if prior:
        if prior["request_sha256"] != digest:
            raise ScopeDecisionError("SCOPE_IDEMPOTENCY_CONFLICT")
        return changed, {**deepcopy(prior), "status": _undecided_status(prior), "deduplicated": True}
    _cas(state, payload)
    if str(payload.get("assignment_id", "")) != str(identity.get("assignment_id", "")):
        raise ScopeDecisionError("SCOPE_ASSIGNMENT_CONFLICT")
    if any(r["identity"] == identity and r["base_scope_revision"] == scope_revision(state) and _decision(ledger, r["request_id"]) is None for r in ledger["requests"]):
        raise ScopeDecisionError("SCOPE_REQUEST_ALREADY_PENDING")
    request = {
        "request_id": "scope-request-" + digest[:32], "idempotency_key": key,
        "request_sha256": digest, "identity": deepcopy(identity),
        "assignment_id": identity["assignment_id"], "base_scope_revision": scope_revision(state),
        "execution_revision": execution_revision(state), "created_at": now,
        "reason": _text(payload.get("reason"), "reason"),
        "dependencies": deepcopy(payload.get("dependencies", [])),
        "proposal": validate_proposal(payload.get("proposal")),
        "source": validate_source(payload.get("source")),
        "authorization_provenance": deepcopy(payload.get("authorization_provenance")),
    }
    if not isinstance(request["dependencies"], list) or any(not isinstance(item, str) or len(item) > 8192 for item in request["dependencies"]):
        raise ScopeDecisionError("SCOPE_REQUEST_INVALID", "Invalid dependencies", 400)
    # Provenance may document an existing decision; it NEVER grants authority.
    provenance = request["authorization_provenance"]
    if provenance is not None and (not isinstance(provenance, dict) or set(provenance) != {"kind", "reference"} or provenance["kind"] != "existing_human_authorization" or not isinstance(provenance["reference"], str)):
        raise ScopeDecisionError("SCOPE_PROVENANCE_INVALID", status_code=400)
    ledger["requests"].append(request)
    _event(ledger, "scope_request", now, request_id=request["request_id"], assignment_id=identity["assignment_id"], execution_revision=execution_revision(state))
    return changed, {**deepcopy(request), "status": _undecided_status(request), "deduplicated": False}


def request_list(state: dict) -> dict:
    ledger = state.get("scope_workflow") or {}
    requests = []
    for request in ledger.get("requests", []):
        decision = next((d for d in ledger.get("decisions", []) if d["request_id"] == request["request_id"]), None)
        stale = request["base_scope_revision"] != scope_revision(state)
        if not decision:
            records = [*state.get("assignments", []), *state.get("review_assignments", [])]
            assignment = next((a for a in reversed(records) if a.get("assignment_id") == request["assignment_id"]), None)
            stale = stale or (assignment is not None and assignment.get("status") != "active")
        request_status = ("rejected" if decision["action"] == "reject" else "applied") if decision else ("stale" if stale else _undecided_status(request))
        requests.append({**deepcopy(request), "status": request_status, "decision": deepcopy(decision)})
    return {"requests": requests, "execution_revision": execution_revision(state),
            "scope_revision": scope_revision(state), "workflow_revision": ledger.get("revision", 0)}


def validate_request(state: dict, request_id: str, payload: dict, identity: dict,
                     now: str, preview: Callable) -> tuple[dict, dict]:
    changed = deepcopy(state)
    ledger = _ledger(changed)
    request = _find_request(ledger, request_id)
    _cas(state, payload)
    _still_bound(state, request, identity)
    if _decision(ledger, request_id):
        raise ScopeDecisionError("SCOPE_DECISION_ALREADY_TAKEN")
    proposal = validate_proposal(payload.get("proposal", request["proposal"]))
    # The same adapter validator is used for original and edited proposals.
    before, after = preview(state, proposal, request["source"], {"request_id": request_id})
    validation = {"request_id": request_id, "proposal": proposal,
                  "proposal_sha256": canonical_json_sha256(proposal),
                  "execution_revision": execution_revision(state), "scope_revision": scope_revision(state),
                  "identity": deepcopy(identity), "source": deepcopy(request["source"]),
                  "before_effective_sha256": canonical_json_sha256(before),
                  "after_effective_sha256": canonical_json_sha256(after),
                  "before_effective": deepcopy(before), "after_effective": deepcopy(after)}
    validation["validation_id"] = "scope-validation-" + canonical_json_sha256(validation)[:32]
    before_text = json.dumps(before, ensure_ascii=False, sort_keys=True, indent=2).splitlines()
    after_text = json.dumps(after, ensure_ascii=False, sort_keys=True, indent=2).splitlines()
    validation["diff"] = "\n".join(unified_diff(before_text, after_text, fromfile="current-effective", tofile="proposed-effective", lineterm=""))
    if not any(v["validation_id"] == validation["validation_id"] for v in ledger["validations"]):
        validation["validated_at"] = now
        ledger["validations"].append(deepcopy(validation))
        _event(ledger, "scope_validation", now, request_id=request_id, validation_id=validation["validation_id"], execution_revision=execution_revision(state))
    return changed, validation


def decide_request(state: dict, request_id: str, payload: dict, identity: dict,
                   now: str, apply: Callable, *, actor: str = "operator",
                   precondition: Callable[[], None] | None = None) -> tuple[dict, dict]:
    changed = deepcopy(state)
    ledger = _ledger(changed)
    request = _find_request(ledger, request_id)
    key = _text(payload.get("idempotency_key"), "idempotency_key", limit=200)
    digest = canonical_json_sha256({"request_id": request_id, "payload": payload})
    for prior in [*ledger["decisions"], *ledger["attempts"]]:
        if prior["idempotency_key"] == key:
            if prior["request_sha256"] != digest:
                raise ScopeDecisionError("SCOPE_IDEMPOTENCY_CONFLICT")
            return changed, {**deepcopy(prior["response"]), "deduplicated": True}
    action = payload.get("action")
    if action not in ("approve", "reject", "edit", "record_existing_authorization"):
        raise ScopeDecisionError("SCOPE_DECISION_INVALID", status_code=400)
    def conflict(error):
        response = {"error": error.code, "status_code": error.status_code, "request_id": request_id, "mutated": False, "audit_appended": True, "deduplicated": False}
        attempt = {"request_id": request_id, "idempotency_key": key, "request_sha256": digest, "action": action, "actor": actor, "timestamp": now, "response": deepcopy(response)}
        ledger["attempts"].append(attempt)
        _event(ledger, "scope_decision_conflict", now, request_id=request_id, reason=error.code, execution_revision=execution_revision(state))
        return changed, response
    try:
        # Entry-point-specific guards run after exact receipt replay, but share
        # the same immutable conflict audit as the authoritative CAS checks.
        if precondition is not None:
            precondition()
        _cas(state, payload)
        _still_bound(state, request, identity)
        if _decision(ledger, request_id):
            raise ScopeDecisionError("SCOPE_DECISION_ALREADY_TAKEN")
        if request.get("authorization_provenance") and action == "approve":
            raise ScopeDecisionError("SCOPE_EXISTING_AUTHORIZATION_ACTION_REQUIRED", "Record prior permission, or explicitly decide edited boundaries")
        validation = None
        if action != "reject":
            validation = next((v for v in ledger["validations"] if v["validation_id"] == payload.get("validation_id") and v["request_id"] == request_id), None)
            if not validation or validation["execution_revision"] != execution_revision(state) or validation["scope_revision"] != scope_revision(state) or validation["identity"] != identity:
                raise ScopeDecisionError("SCOPE_VALIDATION_STALE")
            if action in {"approve", "record_existing_authorization"} and validation["proposal"] != request["proposal"]:
                raise ScopeDecisionError("SCOPE_EDIT_REQUIRES_EXPLICIT_DECISION")
            if action == "record_existing_authorization" and not request.get("authorization_provenance"):
                raise ScopeDecisionError("SCOPE_EXISTING_AUTHORIZATION_REQUIRED")
    except ScopeDecisionError as error:
        return conflict(error)
    decision_id = "scope-decision-" + digest[:32]
    authorization = {"request_id": request_id, "decision_id": decision_id,
                     "validation_id": validation["validation_id"] if validation else None,
                     "proposal_sha256": validation["proposal_sha256"] if validation else None,
                     "actor": actor, "origin": "recorded_existing_authorization" if action == "record_existing_authorization" else "human_decision",
                     "provenance": deepcopy(request.get("authorization_provenance"))}
    receipt = None
    if action != "reject":
        # Work on a fresh copy. Any exception leaves both scope and decision absent.
        applied, receipt = apply(deepcopy(changed), validation["proposal"], request["source"], authorization)
        if receipt.get("before_effective_sha256") != validation["before_effective_sha256"] or receipt.get("after_effective_sha256") != validation["after_effective_sha256"]:
            return conflict(ScopeDecisionError("SCOPE_VALIDATED_CONTENT_CHANGED", "Validate the exact resulting scope again"))
        changed = applied
        ledger = _ledger(changed)
    response = {"request_id": request_id, "decision_id": decision_id, "action": action,
                "status": "rejected" if action == "reject" else "applied", "receipt": deepcopy(receipt),
                "execution_revision": execution_revision(changed), "scope_revision": scope_revision(changed),
                "graph_advanced": False, "deduplicated": False}
    decision = {"decision_id": decision_id, "request_id": request_id,
                "idempotency_key": key, "request_sha256": digest, "action": action,
                "actor": actor, "timestamp": now, "reason": str(payload.get("reason", "")),
                "authorization": authorization, "response": deepcopy(response)}
    ledger["decisions"].append(decision)
    _event(ledger, "human_decision", now, request_id=request_id, decision_id=decision_id, action=action, execution_revision=execution_revision(changed))
    if receipt:
        _event(ledger, "scope_applied", now, request_id=request_id, decision_id=decision_id, scope_revision=scope_revision(changed), assignment_id=request["assignment_id"], execution_revision=execution_revision(changed))
    return changed, response
