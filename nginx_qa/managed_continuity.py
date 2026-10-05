"""Durable result, review, transition, repair, and recovery for managed sprints.

The activation importer deliberately stops once the initial assignments are
published.  This module owns every subsequent mutation.  Runtime JSON remains
the canonical aggregate, while the SQLite queue/lease/resource tables provide
the uniqueness fences needed around external side effects.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Callable, ContextManager, Iterator, Mapping, Sequence
from uuid import uuid4

from .branch_leases import (
    BranchLease,
    BranchLeaseError,
    BranchLeaseRequest,
    BranchLeaseStore,
    deterministic_branch_lease_id,
)
from .git_provider import ManagedFileLock, ManagedGitError, ManagedRepository
from .managed_import import (
    ManagedImportError,
    ManagedImportStore,
    ManagedRetryImportConflict,
    TransactionalSprintImporter,
    managed_schema_errors,
    managed_workspace_from_record,
    managed_workspace_record,
    parse_strict_json_object,
)
from .port_leases import ManagedPortReservationError
from .legacy_scope_control import LegacyScopeControlError, assignment_binding, verify_role_token
from .scope_runtime_adapters import effective_scope_snapshot, require_scope_submission
from .sprint_types import (
    apply_managed_repair_patch,
    canonical_json_bytes,
    canonical_json_sha256,
    managed_activation_invariant_issues,
    managed_blocker_fingerprint,
    managed_graph_semantic_issues,
    managed_integration_id,
    managed_integration_workspace_id,
    managed_occurrence_id,
    managed_result_key,
    managed_transition_token_id,
    repair_request_fingerprint,
    recovery_request_fingerprint,
    review_request_fingerprint,
)
from .workspace_manager import (
    ManagedWorkspace,
    WorkspaceError,
    WorkspaceRequest,
)


INVALID_MANAGED_SPRINT_REQUEST = "INVALID_MANAGED_SPRINT_REQUEST"
MANAGED_ASSIGNMENT_NOT_FOUND = "MANAGED_ASSIGNMENT_NOT_FOUND"
MANAGED_IDENTITY_MISMATCH = "MANAGED_IDENTITY_MISMATCH"
RESULT_CONFLICT = "RESULT_CONFLICT"
RESULT_COMMIT_NOT_FOUND = "RESULT_COMMIT_NOT_FOUND"
RESULT_HEAD_MISMATCH = "RESULT_HEAD_MISMATCH"
RESULT_NOT_DESCENDANT = "RESULT_NOT_DESCENDANT"
REVIEW_CONFLICT = "REVIEW_CONFLICT"
RECOVERY_CONFLICT = "RECOVERY_CONFLICT"
GRAPH_REVISION_CONFLICT = "GRAPH_REVISION_CONFLICT"
IDEMPOTENCY_KEY_CONFLICT = "IDEMPOTENCY_KEY_CONFLICT"
REPAIR_IMMUTABLE_HISTORY = "REPAIR_IMMUTABLE_HISTORY"
REPAIR_LIVE_ASSIGNMENT_TARGET_INVALID = "REPAIR_LIVE_ASSIGNMENT_TARGET_INVALID"
REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE = (
    "REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE"
)
REPAIR_PATCH_CONFLICT = "REPAIR_PATCH_CONFLICT"
CONTINUITY_RUNTIME_FAILED = "CONTINUITY_RUNTIME_FAILED"

_RESULT_SCHEMA = "managed-assignment-result-v1.schema.json"
_REVIEW_SCHEMA = "managed-review-decision-v1.schema.json"
_REPAIR_SCHEMA = "repair-sprint-v1.schema.json"
_MAX_BODY_BYTES = 4 * 1024 * 1024
_URL_CREDENTIAL = re.compile(r"(?P<prefix>://[^\s/:@]+):[^\s/@]*@")
_SECRET_LITERAL = re.compile(
    r"(?:"
    r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}|"
    r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----|"
    r"\b(?:sk-(?:proj-)?|ghp_|github_pat_|glpat-)[A-Za-z0-9_-]{8,}|"
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
    r")",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ManagedContinuityResult:
    response: dict[str, Any]
    http_status: int = 200


class ManagedContinuityError(RuntimeError):
    """Transport-neutral, value-free error returned by continuity endpoints."""

    def __init__(
        self,
        code: str,
        http_status: int,
        correlation_id: str,
        *,
        issues: Sequence[Mapping[str, Any]] | None = None,
        field: str | None = None,
    ) -> None:
        super().__init__(code)
        detail: dict[str, Any] = {
            "error": code,
            "correlation_id": correlation_id,
        }
        if issues is not None:
            detail["issues"] = [dict(issue) for issue in issues]
        if field is not None:
            detail["field"] = field
        self.code = code
        self.http_status = http_status
        self.correlation_id = correlation_id
        self.envelope = {"detail": detail}


@dataclass(frozen=True, slots=True)
class _AssignmentBinding:
    project_id: str
    sprint_id: str
    state: dict[str, Any]
    kind: str
    record: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _PreparedAssignment:
    assignment: dict[str, Any]
    workspace: dict[str, Any]
    workspace_request: WorkspaceRequest
    verified_workspace: ManagedWorkspace
    branch_request: BranchLeaseRequest | None
    branch_lease: dict[str, Any] | None
    port_leases: tuple[dict[str, Any], ...]
    processes: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class _JoinPlan:
    kind: str
    target_node_id: str
    graph_revision: int
    trigger_token_ids: tuple[str, ...]
    result_keys: tuple[str, ...]
    commits: tuple[str, ...]
    generation: int | None = None
    occurrence_id: str | None = None
    prepared_assignment: _PreparedAssignment | None = None
    integration: dict[str, Any] | None = None
    integration_workspace: dict[str, Any] | None = None


def _timestamp(clock: Callable[[], datetime]) -> str:
    value = clock()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _git_timestamp(value: str) -> str:
    """Normalize an ISO timestamp to Git's whole-second commit precision."""

    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (
        parsed.astimezone(timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def _stable_id(prefix: str, *parts: object, compact: bool = False) -> str:
    encoded = "\0".join(str(part) for part in parts).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"{prefix}-{digest[:24] if compact else digest}"


def _records_by(
    state: Mapping[str, Any], collection: str, field: str
) -> dict[str, dict[str, Any]]:
    raw = state.get(collection)
    if not isinstance(raw, list):
        raise RuntimeError(f"managed runtime collection {collection} is corrupt")
    result: dict[str, dict[str, Any]] = {}
    for value in raw:
        if not isinstance(value, dict) or not isinstance(value.get(field), str):
            raise RuntimeError(f"managed runtime collection {collection} is corrupt")
        key = str(value[field])
        if key in result:
            raise RuntimeError(f"managed runtime collection {collection} is corrupt")
        result[key] = value
    return result


def _recovery_settles_context(
    state: Mapping[str, Any], recovery: Mapping[str, Any]
) -> bool:
    """Return whether a recovery is the context's irreversible decision."""

    if recovery.get("status") == "completed":
        return True
    normalized_error = recovery.get("normalized_error")
    assignment_id = recovery.get("assignment_id")
    return bool(
        recovery.get("status") == "failed"
        and recovery.get("action") == "ROUTE_REWORK"
        and isinstance(normalized_error, Mapping)
        and normalized_error.get("code") == "REWORK_LIMIT_EXCEEDED"
        and isinstance(assignment_id, str)
        and any(
            isinstance(context, Mapping)
            and context.get("reason_code") == "REWORK_LIMIT_EXCEEDED"
            and context.get("failed_assignment_id") == assignment_id
            for context in state.get("coordinator_contexts", [])
        )
    )


def _is_rework_limit_context(context: Mapping[str, Any]) -> bool:
    """Return whether a Coordinator context is terminal cap evidence only."""

    return context.get("reason_code") == "REWORK_LIMIT_EXCEEDED"


def _graph_revision(
    state: Mapping[str, Any], revision: int
) -> dict[str, Any]:
    records = state.get("graph_revisions")
    if not isinstance(records, list):
        raise RuntimeError("managed graph revision history is corrupt")
    matches = [
        item
        for item in records
        if isinstance(item, dict) and item.get("revision") == revision
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("definition"), dict):
        raise RuntimeError("managed graph revision is missing")
    return matches[0]


def _node(
    state: Mapping[str, Any], revision: int, node_id: str
) -> dict[str, Any]:
    definition = _graph_revision(state, revision)["definition"]
    matches = [
        item
        for item in definition.get("nodes", [])
        if isinstance(item, dict) and item.get("id") == node_id
    ]
    if len(matches) != 1:
        raise RuntimeError("managed graph node is missing")
    return matches[0]


def _outcome_target(
    definition: Mapping[str, Any], source_node_id: str, outcome: str
) -> str:
    if outcome in {"STOP", "NEED_DECISION"}:
        coordinator = definition.get("coordinator")
        routes = coordinator.get("routes") if isinstance(coordinator, Mapping) else None
        target = routes.get(outcome) if isinstance(routes, Mapping) else None
    else:
        source = next(
            (
                item
                for item in definition.get("nodes", [])
                if isinstance(item, Mapping) and item.get("id") == source_node_id
            ),
            None,
        )
        transitions = source.get("transitions") if isinstance(source, Mapping) else None
        target = transitions.get(outcome) if isinstance(transitions, Mapping) else None
    if not isinstance(target, str) or not target:
        raise RuntimeError("accepted outcome has no frozen graph target")
    return target


def _allowed_outcomes(node: Mapping[str, Any]) -> list[str]:
    transitions = node.get("transitions")
    if not isinstance(transitions, Mapping):
        raise RuntimeError("managed task node has invalid transitions")
    values = list(transitions)
    for reserved in ("STOP", "NEED_DECISION"):
        if reserved not in values:
            values.append(reserved)
    return values


def _redact_summary(value: str, secret_values: Sequence[str]) -> tuple[str, bool]:
    redacted = value
    for secret in sorted(
        {item for item in secret_values if isinstance(item, str) and item},
        key=len,
        reverse=True,
    ):
        redacted = redacted.replace(secret, "[REDACTED]")
    redacted = _URL_CREDENTIAL.sub(r"\g<prefix>:[REDACTED]@", redacted)
    redacted = _SECRET_LITERAL.sub("[REDACTED]", redacted)
    if len(redacted) > 16_384:
        redacted = redacted[:16_384] + "\n[OUTPUT TRUNCATED]"
    if not redacted:
        redacted = "[REDACTED]"
    return redacted, redacted != value


def _redact_json_strings(value: Any, secret_values: Sequence[str]) -> Any:
    """Return a JSON-compatible copy with every durable string redacted."""

    if isinstance(value, str):
        redacted = value
        for secret in sorted(
            {item for item in secret_values if isinstance(item, str) and item},
            key=len,
            reverse=True,
        ):
            redacted = redacted.replace(secret, "[REDACTED]")
        redacted = _URL_CREDENTIAL.sub(r"\g<prefix>:[REDACTED]@", redacted)
        return _SECRET_LITERAL.sub("[REDACTED]", redacted)
    if isinstance(value, Mapping):
        return {
            str(key): _redact_json_strings(item, secret_values)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_json_strings(item, secret_values) for item in value]
    return deepcopy(value)


class ManagedContinuityRuntime:
    """Crash-resumable managed runtime built on validated aggregate writes."""

    def __init__(
        self,
        importer: TransactionalSprintImporter,
        *,
        process_supervisor: Any | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        result_verifier: Callable[
            [Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]],
            ContextManager[Any] | None,
        ]
        | None = None,
        secret_values: Sequence[str] = (),
        fault_injector: Callable[[str, Mapping[str, Any]], None] | None = None,
    ) -> None:
        self.importer = importer
        self.store: ManagedImportStore = importer.store
        self.process_supervisor = process_supervisor
        self.clock = clock
        self.result_verifier = result_verifier
        self.secret_values = tuple(secret_values)
        self.fault_injector = fault_injector

    def _fault(self, point: str, **context: Any) -> None:
        if self.fault_injector is not None:
            try:
                self.fault_injector(point, context)
            except BaseException as exc:
                # Fault injection models abrupt process death and must cross
                # best-effort post-commit recovery boundaries unchanged.
                # Ordinary queue/Git exceptions remain suppressible so the
                # already durable acknowledgement can still be returned.
                try:
                    setattr(exc, "_managed_fault_injection", True)
                except Exception:
                    pass
                raise

    def scope_snapshot(self, project_id: str, sprint_id: str) -> dict[str, Any]:
        """Read exact persisted state; no publication, recovery, or claim."""
        state = self.store.runtime_state(project_id, sprint_id)
        if state is None:
            raise KeyError("managed sprint runtime was not found")
        return state

    def scope_mutate(self, project_id: str, sprint_id: str, mutator: Callable) -> tuple[dict[str, Any], Any]:
        """Commit scope/audit under the same exclusion as ordinary execution."""
        def mutate(state: dict[str, Any], _connection: sqlite3.Connection) -> Any:
            changed, response = mutator(deepcopy(state))
            state.clear()
            state.update(changed)
            return response
        with self._sprint_lock(sprint_id):
            return self.store.mutate_runtime_state(project_id, sprint_id, mutate)

    @staticmethod
    def _require_scope(state: Mapping[str, Any], assignment_id: str, payload: Mapping[str, Any], supplied_role_token: str, correlation_id: str) -> None:
        try:
            require_scope_submission(state, assignment_id, payload.get("scope_context"), supplied_role_token)
        except LegacyScopeControlError as exc:
            raise ManagedContinuityError(exc.code, exc.status_code, correlation_id) from exc

    @contextmanager
    def _sprint_lock(
        self,
        sprint_id: str,
        *,
        timeout: float = 60.0,
    ) -> Iterator[None]:
        lock_digest = hashlib.sha256(sprint_id.encode("utf-8")).hexdigest()
        lock_path = (
            self.store.database_path.parent
            / ".continuity-locks"
            / f"sprint-{lock_digest}.lock"
        )
        with ManagedFileLock(lock_path, timeout=timeout):
            yield

    @staticmethod
    def parse_body(
        body: bytes, correlation_id: str, *, allow_empty: bool = False
    ) -> dict[str, Any]:
        if not body and allow_empty:
            return {}
        if not body or len(body) > _MAX_BODY_BYTES:
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
            )
        try:
            return parse_strict_json_object(body)
        except ValueError as exc:
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
            ) from exc

    @staticmethod
    def _validate_schema(
        payload: Mapping[str, Any], filename: str, correlation_id: str
    ) -> None:
        issues = managed_schema_errors(
            payload,
            filename,
            issue_code="MANAGED_REQUEST_SCHEMA_INVALID",
        )
        if issues:
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST,
                400,
                correlation_id,
                issues=issues,
            )

    def _state_bindings(
        self,
        project_id: str,
        state: Mapping[str, Any],
    ) -> tuple[_AssignmentBinding, ...]:
        result: list[_AssignmentBinding] = []
        sprint_id = str(state.get("sprint_id") or "")
        if not sprint_id:
            raise RuntimeError("managed sprint runtime identity is corrupt")
        for collection, kind in (
            ("assignments", "assignment"),
            ("review_assignments", "review"),
        ):
            for record in state.get(collection, []):
                if isinstance(record, dict):
                    result.append(
                        _AssignmentBinding(
                            project_id,
                            sprint_id,
                            dict(state),
                            kind,
                            record,
                        )
                    )
        recoveries = [
            item
            for item in state.get("recovery_records", [])
            if isinstance(item, Mapping)
        ]
        outbox = [
            item
            for item in state.get("outbox", [])
            if isinstance(item, Mapping)
        ]
        for context in state.get("coordinator_contexts", []):
            if not isinstance(context, Mapping):
                continue
            context_id = context.get("context_id")
            revision = context.get("graph_revision")
            if not isinstance(context_id, str) or not isinstance(revision, int):
                continue
            definition = _graph_revision(state, revision)["definition"]
            coordinator = definition.get("coordinator")
            coordinator_node = next(
                (
                    item
                    for item in definition.get("nodes", [])
                    if isinstance(item, Mapping)
                    and isinstance(coordinator, Mapping)
                    and item.get("id") == coordinator.get("node_id")
                ),
                None,
            )
            agent = (
                coordinator_node.get("agent")
                if isinstance(coordinator_node, Mapping)
                else None
            )
            if not isinstance(agent, Mapping):
                raise RuntimeError("managed Coordinator identity is corrupt")
            settled = next(
                (
                    item
                    for item in recoveries
                    if item.get("context_id") == context_id
                    and _recovery_settles_context(state, item)
                ),
                None,
            )
            enqueue = next(
                (
                    item
                    for item in outbox
                    if item.get("event_type") == "COORDINATOR_ENQUEUE"
                    and isinstance(item.get("payload"), Mapping)
                    and item["payload"].get("context_id") == context_id
                ),
                None,
            )
            evidence_only = _is_rework_limit_context(context)
            record = {
                "assignment_id": context_id,
                "context_id": context_id,
                "graph_revision": revision,
                "coordinator_id": str(agent["id"]),
                "coordinator_phone": str(agent["phone"]),
                "status": (
                    "decided"
                    if settled is not None or evidence_only
                    else "active"
                ),
                "created_at": (
                    enqueue.get("created_at")
                    if isinstance(enqueue, Mapping)
                    else ""
                ),
                "context": deepcopy(dict(context)),
            }
            if settled is not None:
                record["response"] = deepcopy(settled.get("response"))
            result.append(
                _AssignmentBinding(
                    project_id,
                    sprint_id,
                    dict(state),
                    "coordinator",
                    record,
                )
            )
        return tuple(result)

    def _project_binding_discovery(
        self, project_id: str
    ) -> tuple[
        tuple[_AssignmentBinding, ...], tuple[dict[str, Any], ...], bool
    ]:
        """Discover valid bindings plus fail-closed corrupt ownership hints."""

        result: list[_AssignmentBinding] = []
        corrupt_ownership: list[dict[str, Any]] = []
        project_wide_corruption = False
        for state in self.store.runtime_binding_snapshots_for_project(project_id):
            if state.get("__managed_binding_corrupt__") is True:
                project_wide = state.get("project_wide")
                ownership = state.get("ownership")
                if not isinstance(project_wide, bool) or not isinstance(
                    ownership, list
                ):
                    raise RuntimeError("managed binding ownership hint is corrupt")
                project_wide_corruption = project_wide_corruption or project_wide
                for hint in ownership:
                    if (
                        not isinstance(hint, Mapping)
                        or hint.get("kind")
                        not in {"assignment", "review", "coordinator"}
                        or not isinstance(hint.get("assignment_id"), str)
                        or (
                            hint.get("phone") is not None
                            and not isinstance(hint.get("phone"), str)
                        )
                        or not isinstance(hint.get("live"), bool)
                    ):
                        raise RuntimeError("managed binding ownership hint is corrupt")
                    corrupt_ownership.append(deepcopy(dict(hint)))
                continue
            result.extend(self._state_bindings(project_id, state))
        return (
            tuple(result),
            tuple(corrupt_ownership),
            project_wide_corruption,
        )

    def _publish_binding(self, binding: _AssignmentBinding) -> _AssignmentBinding:
        """Close publication gaps only after a request matches this sprint."""

        with self._sprint_lock(binding.sprint_id):
            self.drain_outbox(binding.project_id, binding.sprint_id)
        state = self.store.runtime_state(binding.project_id, binding.sprint_id)
        if state is None:
            raise RuntimeError("managed sprint runtime disappeared")
        matches = [
            candidate
            for candidate in self._state_bindings(binding.project_id, state)
            if candidate.kind == binding.kind
            and candidate.record.get("assignment_id")
            == binding.record.get("assignment_id")
        ]
        if len(matches) != 1:
            raise RuntimeError("managed assignment identity changed")
        return matches[0]

    def _resume_after_durable_ack(
        self,
        project_id: str,
        sprint_id: str,
        *,
        reconcile: bool = False,
    ) -> None:
        """Best-effort progress that cannot replace an immutable response.

        Once a result, review, recovery, or repair response is committed, the
        caller owns that acknowledgement.  Queue/Git/process recovery remains
        durable and the reconciler will retry it; a transient failure here
        must not turn the already accepted request into a 5xx response.
        """

        try:
            if reconcile:
                self._reconcile_locked(project_id, sprint_id)
            else:
                self.drain_outbox(project_id, sprint_id)
        except Exception as exc:
            if getattr(exc, "_managed_fault_injection", False):
                raise

    @staticmethod
    def _binding_is_live(binding: _AssignmentBinding) -> bool:
        if binding.record.get("status") not in {
            "prepared",
            "active",
            "reviews_pending",
        }:
            return False
        if binding.kind == "coordinator":
            if binding.record.get("status") != "active":
                return False
            context = binding.record.get("context")
            if isinstance(context, Mapping) and _is_rework_limit_context(context):
                return False
            if binding.state.get("status") == "active":
                return True
            return bool(
                isinstance(context, Mapping)
                and (
                    isinstance(context.get("import_attempt_id"), str)
                    or context.get("reason_code") == "HANDOFF_DELIVERY_FAILED"
                )
            )
        if binding.kind != "review":
            return True
        if binding.state.get("status") != "active":
            return False
        result_key = binding.record.get("result_key")
        source_assignment_id = binding.record.get("source_assignment_id")
        source_assignment = next(
            (
                item
                for item in binding.state.get("assignments", [])
                if isinstance(item, Mapping)
                and item.get("assignment_id") == source_assignment_id
            ),
            None,
        )
        journal = next(
            (
                item
                for item in binding.state.get("transition_journal", [])
                if isinstance(item, Mapping) and item.get("result_key") == result_key
            ),
            None,
        )
        return bool(
            isinstance(journal, Mapping)
            and isinstance(source_assignment, Mapping)
            and source_assignment.get("status") == "reviews_pending"
            and journal.get("state") == "REVIEWS_PENDING"
            and journal.get("disposition") == "open"
        )

    @staticmethod
    def _has_unresolved_assignment_context(
        state: Mapping[str, Any], assignment_id: str
    ) -> bool:
        resolved_context_ids = {
            str(recovery["context_id"])
            for recovery in state.get("recovery_records", [])
            if isinstance(recovery, Mapping)
            and _recovery_settles_context(state, recovery)
            and isinstance(recovery.get("context_id"), str)
        }
        return any(
            isinstance(context, Mapping)
            and context.get("failed_assignment_id") == assignment_id
            and context.get("context_id") not in resolved_context_ids
            for context in state.get("coordinator_contexts", [])
        )

    def _binding_for_request(
        self,
        project_id: str,
        agent_phone: str,
        payload: Mapping[str, Any],
        correlation_id: str,
        supplied_role_token: str = "",
    ) -> _AssignmentBinding | None:
        clean_phone = agent_phone.strip()
        assignment_id = payload.get("assignment_id")
        (
            bindings,
            corrupt_ownership,
            project_wide_corruption,
        ) = self._project_binding_discovery(project_id)
        if project_wide_corruption:
            raise RuntimeError("managed project binding ownership is corrupt")
        owned_live = [
            binding
            for binding in bindings
            if str(
                binding.record.get("agent_phone")
                or binding.record.get("reviewer_phone")
                or binding.record.get("coordinator_phone")
                or ""
            )
            == clean_phone
            and self._binding_is_live(binding)
        ]
        corrupt_owned_live = [
            hint
            for hint in corrupt_ownership
            if hint["phone"] == clean_phone and hint["live"] is True
        ]
        if not isinstance(assignment_id, str) or not assignment_id:
            if corrupt_owned_live:
                raise RuntimeError("targeted managed sprint runtime is corrupt")
            if owned_live:
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST,
                    400,
                    correlation_id,
                    field="assignment_id",
                )
            return None
        if any(
            hint["assignment_id"] == assignment_id
            for hint in corrupt_ownership
        ):
            raise RuntimeError("targeted managed sprint runtime is corrupt")
        matches = [
            binding
            for binding in bindings
            if binding.record.get("assignment_id") == assignment_id
        ]
        if not matches:
            if corrupt_owned_live:
                raise RuntimeError("targeted managed sprint runtime is corrupt")
            if owned_live:
                raise ManagedContinuityError(
                    MANAGED_ASSIGNMENT_NOT_FOUND, 404, correlation_id
                )
            return None
        if len(matches) != 1:
            raise RuntimeError("managed assignment identity is not unique")
        binding = matches[0]
        expected_phone = str(
            binding.record.get("agent_phone")
            or binding.record.get("reviewer_phone")
            or binding.record.get("coordinator_phone")
            or ""
        )
        if expected_phone != clean_phone:
            raise ManagedContinuityError(
                MANAGED_IDENTITY_MISMATCH, 403, correlation_id
            )
        self._require_scope(binding.state, assignment_id, payload, supplied_role_token, correlation_id)
        historical_recovery = False
        if binding.kind == "coordinator":
            idempotency_key = payload.get("idempotency_key")
            historical_recovery = isinstance(idempotency_key, str) and any(
                isinstance(recovery, Mapping)
                and recovery.get("context_id")
                == binding.record.get("context_id")
                and recovery.get("coordinator_id")
                == binding.record.get("coordinator_id")
                and recovery.get("idempotency_key") == idempotency_key
                and recovery.get("status") in {"completed", "failed"}
                for recovery in binding.state.get("recovery_records", [])
            )
        if (
            binding.record.get("status") in {"prepared", "active"}
            and not historical_recovery
        ):
            published = self._publish_binding(binding)
        else:
            # Historical result/review/recovery bindings are immutable replay
            # handles.  Their frozen response must remain available while a
            # separate pending outbox or publication is temporarily offline.
            published = binding
        published_phone = str(
            published.record.get("agent_phone")
            or published.record.get("reviewer_phone")
            or published.record.get("coordinator_phone")
            or ""
        )
        if published_phone != clean_phone:
            raise ManagedContinuityError(
                MANAGED_IDENTITY_MISMATCH, 403, correlation_id
            )
        return published

    def submit_if_managed(
        self,
        project_id: str,
        agent_phone: str,
        body: bytes,
        correlation_id: str | None = None,
        supplied_role_token: str = "",
    ) -> ManagedContinuityResult | None:
        """Claim a request only when its durable assignment is managed."""

        correlation = correlation_id or f"continuity-{uuid4().hex}"
        if not body:
            current = self.current_identity(project_id, agent_phone, supplied_role_token)
            return ManagedContinuityResult(current) if current is not None else None
        try:
            payload = self.parse_body(body, correlation)
        except ManagedContinuityError:
            # Malformed JSON belongs to this service only when the phone owns a
            # live managed assignment.  Otherwise the legacy endpoint retains
            # its historical parser/error behavior.
            if self.current_identity(project_id, agent_phone, supplied_role_token) is not None:
                raise
            return None
        if set(payload).issubset({"message"}):
            # The long-standing phone-specific whoami contract sends
            # {"message":"Кто я?"}; an empty object has the same legacy
            # lookup semantics.  These bodies carry no result fields and must
            # expose the durable managed identity rather than being mistaken
            # for a result submission that omitted assignment_id.
            current = self.current_identity(project_id, agent_phone, supplied_role_token)
            return ManagedContinuityResult(current) if current is not None else None
        binding = self._binding_for_request(
            project_id, agent_phone, payload, correlation, supplied_role_token
        )
        if binding is None:
            return None
        if binding.kind == "assignment":
            return self._submit_result(binding, payload, correlation, supplied_role_token)
        if binding.kind == "review":
            return self._submit_review(binding, payload, correlation, supplied_role_token)
        return self._submit_recovery(binding, payload, correlation, supplied_role_token)

    def current_identity(
        self, project_id: str, agent_phone: str, supplied_role_token: str = ""
    ) -> dict[str, Any] | None:
        clean_phone = agent_phone.strip()
        for _attempt in range(3):
            (
                bindings,
                corrupt_ownership,
                project_wide_corruption,
            ) = self._project_binding_discovery(project_id)
            if project_wide_corruption:
                raise RuntimeError("managed project binding ownership is corrupt")
            if any(
                hint["phone"] == clean_phone and hint["live"] is True
                for hint in corrupt_ownership
            ):
                raise RuntimeError("targeted managed sprint runtime is corrupt")
            candidates = [
                binding
                for binding in bindings
                if str(
                    binding.record.get("agent_phone")
                    or binding.record.get("reviewer_phone")
                    or binding.record.get("coordinator_phone")
                    or ""
                )
                == clean_phone
                and self._binding_is_live(binding)
            ]
            if not candidates:
                return None
            if len(candidates) != 1:
                # Parallel entry nodes and any_parent activations may legitimately
                # leave several live assignments for one logical phone.  The
                # phone-specific identity endpoint is a sequential consumer: keep
                # exposing the oldest stable binding until it settles, while
                # explicit submissions continue to address any assignment by id.
                kind_order = {"assignment": 0, "review": 1, "coordinator": 2}
                candidates.sort(
                    key=lambda binding: (
                        str(binding.record.get("created_at") or ""),
                        kind_order.get(binding.kind, 99),
                        str(binding.record.get("assignment_id") or ""),
                        binding.sprint_id,
                    )
                )
            candidate = candidates[0]
            governed = assignment_binding(candidate.state, candidate.record.get("assignment_id"))
            if governed is not None:
                try:
                    verify_role_token(clean_phone, supplied_role_token)
                except LegacyScopeControlError as exc:
                    raise ManagedContinuityError(exc.code, exc.status_code, "managed-identity") from exc
            binding = self._publish_binding(candidate)
            binding_phone = str(
                binding.record.get("agent_phone")
                or binding.record.get("reviewer_phone")
                or binding.record.get("coordinator_phone")
                or ""
            )
            if binding_phone != clean_phone or not self._binding_is_live(binding):
                # The selected oldest binding may settle while its publication
                # is drained.  Rediscover so another same-phone binding cannot
                # accidentally fall through to the legacy identity handler.
                continue
            record = deepcopy(binding.record)
            response = {
                "managed": True,
                "project_id": project_id,
                "sprint_id": binding.sprint_id,
                "assignment_kind": binding.kind,
                "assignment": record,
            }
            if governed is not None:
                response["effective_scope"] = effective_scope_snapshot(binding.state, str(record["assignment_id"]))
                response["instruction_precedence"] = "effective_scope_supersedes_conflicting_issued_scope"
            return response
        raise RuntimeError("managed identity changed during publication")

    @staticmethod
    def _result_replay(receipt: Mapping[str, Any]) -> ManagedContinuityResult:
        stored = receipt.get("response")
        if not isinstance(stored, Mapping):
            raise RuntimeError("managed result receipt response is corrupt")
        response = deepcopy(dict(stored))
        response.update({"status": "ALREADY_ACCEPTED", "deduplicated": True})
        return ManagedContinuityResult(response)

    @contextmanager
    def _default_result_verification(
        self,
        state: Mapping[str, Any],
        assignment: Mapping[str, Any],
        workspace: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> Iterator[None]:
        repository_record = state.get("repository")
        if not isinstance(repository_record, Mapping):
            raise RuntimeError("managed runtime repository identity is corrupt")
        provider = self.importer.provider_for_durable_repository(repository_record)
        repository_id = str(repository_record.get("repository_id") or "")
        try:
            repository = provider.ensure_mirror(repository_id, fetch=True)
            result_commit = str(payload["git_commit"])
            try:
                provider.assert_commit(repository, result_commit)
            except ManagedGitError as exc:
                raise ManagedContinuityError(
                    RESULT_COMMIT_NOT_FOUND,
                    409,
                    "result-verification",
                ) from exc

            assignment_revision = int(assignment["graph_revision"])
            node = _node(state, assignment_revision, str(assignment["node_id"]))
            node_workspace = node.get("workspace")
            if not isinstance(node_workspace, Mapping):
                raise RuntimeError("managed assignment workspace definition is corrupt")
            access = str(node_workspace.get("access") or "")
            branch = workspace.get("assigned_branch")
            if access == "read":
                if (
                    payload.get("git_branch") is not None
                    or result_commit != assignment.get("source_commit")
                    or result_commit != assignment.get("initial_head_commit")
                ):
                    raise ManagedContinuityError(
                        RESULT_HEAD_MISMATCH, 409, "result-verification"
                    )
            else:
                if payload.get("git_branch") != branch or not isinstance(branch, str):
                    raise ManagedContinuityError(
                        RESULT_HEAD_MISMATCH, 409, "result-verification"
                    )
                heads = provider.branch_heads(repository, branch)
                visible_heads = {
                    heads.get(f"refs/heads/{branch}"),
                    heads.get(f"refs/remotes/origin/{branch}"),
                }
                visible_heads.discard(None)
                if result_commit not in visible_heads:
                    raise ManagedContinuityError(
                        RESULT_HEAD_MISMATCH, 409, "result-verification"
                    )
            if not provider.is_ancestor(
                repository,
                str(assignment["initial_head_commit"]),
                result_commit,
            ):
                raise ManagedContinuityError(
                    RESULT_NOT_DESCENDANT, 409, "result-verification"
                )

            request = WorkspaceRequest(
                project_id=str(workspace["project_id"]),
                sprint_id=str(workspace["sprint_id"]),
                node_id=str(assignment["node_id"]),
                assignment_id=str(assignment["assignment_id"]),
                repository_id=repository_id,
                source_commit=str(assignment["source_commit"]),
                access="write" if access == "write" else "read",
                assigned_branch=str(branch) if isinstance(branch, str) else None,
                existing_branch_policy=("resume" if access == "write" else None),
            )
            with self.importer.workspace_manager.verify_result(
                request,
                repository,
                expected_root=Path(str(workspace["expected_root"])),
                result_commit=result_commit,
                branch_lease_id=(
                    str(assignment["branch_lease_id"])
                    if assignment.get("branch_lease_id") is not None
                    else None
                ),
            ):
                if access == "write":
                    assert isinstance(branch, str)
                    fenced_heads = provider.branch_heads(repository, branch)
                    fenced_visible = {
                        fenced_heads.get(f"refs/heads/{branch}"),
                        fenced_heads.get(f"refs/remotes/origin/{branch}"),
                    }
                    fenced_visible.discard(None)
                    if result_commit not in fenced_visible:
                        raise ManagedContinuityError(
                            RESULT_HEAD_MISMATCH, 409, "result-verification"
                        )
                    provider.advance_local_branch(
                        repository,
                        branch,
                        expected_head=str(assignment["initial_head_commit"]),
                        new_head=result_commit,
                    )
                provider.pin_commit(
                    repository,
                    result_commit,
                    owner_id=f"{assignment['assignment_id']}:result",
                )
                yield
        except ManagedContinuityError:
            raise
        except WorkspaceError as exc:
            code = "WORKSPACE_DIRTY" if exc.code == "WORKSPACE_DIRTY" else RESULT_HEAD_MISMATCH
            raise ManagedContinuityError(code, 409, "result-verification") from exc
        except ManagedGitError as exc:
            code = (
                RESULT_COMMIT_NOT_FOUND
                if exc.code in {"SOURCE_COMMIT_NOT_FOUND", "GIT_BLOB_NOT_FOUND"}
                else RESULT_HEAD_MISMATCH
            )
            raise ManagedContinuityError(code, 409, "result-verification") from exc

    def _verification_context(
        self,
        state: Mapping[str, Any],
        assignment: Mapping[str, Any],
        workspace: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> ContextManager[Any]:
        if self.result_verifier is None:
            return self._default_result_verification(
                state, assignment, workspace, payload
            )
        context = self.result_verifier(state, assignment, workspace, payload)
        return context if context is not None else nullcontext()

    def _resolved_secret_values(self, state: Mapping[str, Any]) -> tuple[str, ...]:
        """Resolve configured references ephemerally for result redaction."""

        values = {value for value in self.secret_values if value}
        repository = state.get("repository")
        credential_reference = (
            repository.get("credential_reference")
            if isinstance(repository, Mapping)
            else None
        )
        if isinstance(credential_reference, str):
            resolver = self.importer.git_provider.credential_resolver
            if resolver is None:
                raise LookupError("managed credential resolver is unavailable")
            resolved = resolver(credential_reference)
            if not isinstance(resolved, Mapping):
                raise LookupError("managed credential resolver returned invalid data")
            values.update(
                value for value in resolved.values() if isinstance(value, str) and value
            )

        secret_resolver = (
            getattr(self.process_supervisor, "secret_resolver", None)
            if self.process_supervisor is not None
            else None
        )
        references = {
            str(raw["secret_ref"])
            for process in state.get("processes", [])
            if isinstance(process, Mapping)
            for raw in (
                process.get("environment_redacted", {}).values()
                if isinstance(process.get("environment_redacted"), Mapping)
                else ()
            )
            if isinstance(raw, Mapping)
            and set(raw) == {"secret_ref"}
            and isinstance(raw.get("secret_ref"), str)
        }
        if references and secret_resolver is None:
            raise LookupError("managed process secret resolver is unavailable")
        for reference in references:
            resolved_value = secret_resolver(reference)
            if not isinstance(resolved_value, str) or not resolved_value:
                raise LookupError("managed process secret resolver returned invalid data")
            values.add(resolved_value)
        return tuple(values)

    def _submit_result(
        self,
        binding: _AssignmentBinding,
        payload: Mapping[str, Any],
        correlation_id: str,
        supplied_role_token: str = "",
    ) -> ManagedContinuityResult:
        with self._sprint_lock(binding.sprint_id):
            latest = self.store.runtime_state(binding.project_id, binding.sprint_id)
            if latest is None:
                raise RuntimeError("managed sprint runtime disappeared")
            current = _records_by(latest, "assignments", "assignment_id").get(
                str(binding.record.get("assignment_id"))
            )
            if not isinstance(current, dict):
                raise RuntimeError("managed assignment disappeared")
            refreshed = _AssignmentBinding(
                binding.project_id,
                binding.sprint_id,
                latest,
                binding.kind,
                current,
            )
            self._require_scope(latest, str(current["assignment_id"]), payload, supplied_role_token, correlation_id)
            return self._submit_result_locked(
                refreshed, payload, correlation_id
            )

    def _submit_result_locked(
        self,
        binding: _AssignmentBinding,
        payload: Mapping[str, Any],
        correlation_id: str,
    ) -> ManagedContinuityResult:
        self._validate_schema(payload, _RESULT_SCHEMA, correlation_id)
        request_fingerprint = canonical_json_sha256(payload)
        receipts = [
            item
            for item in binding.state.get("result_receipts", [])
            if isinstance(item, Mapping)
            and item.get("assignment_id") == binding.record.get("assignment_id")
        ]
        if receipts:
            if len(receipts) != 1:
                raise RuntimeError("managed assignment has duplicate result receipts")
            if receipts[0].get("request_fingerprint") == request_fingerprint:
                self._resume_after_durable_ack(
                    binding.project_id, binding.sprint_id
                )
                return self._result_replay(receipts[0])
            raise ManagedContinuityError(RESULT_CONFLICT, 409, correlation_id)

        assignment = binding.record
        assignment_id = str(assignment["assignment_id"])
        if (
            assignment.get("status") != "active"
            or payload.get("from_commit") != assignment.get("initial_head_commit")
            or payload.get("status") not in assignment.get("allowed_outcomes", [])
            or self._has_unresolved_assignment_context(
                binding.state, assignment_id
            )
        ):
            raise ManagedContinuityError(RESULT_CONFLICT, 409, correlation_id)
        workspaces = _records_by(binding.state, "workspaces", "workspace_id")
        workspace = workspaces.get(str(assignment.get("workspace_id")))
        if not isinstance(workspace, dict):
            raise RuntimeError("managed assignment workspace is missing")
        result_key = managed_result_key(
            assignment_id,
            str(payload["status"]),
            str(payload["git_commit"]),
        )
        now = _timestamp(self.clock)
        if payload.get("git_branch") != workspace.get("assigned_branch"):
            self._stage_result_verification_failure(
                binding.project_id,
                binding.sprint_id,
                assignment_id=assignment_id,
                result_key=result_key,
                reason_code=RESULT_HEAD_MISMATCH,
                now=now,
                correlation_id=correlation_id,
            )
            raise ManagedContinuityError(RESULT_HEAD_MISMATCH, 409, correlation_id)

        try:
            secret_values = self._resolved_secret_values(binding.state)
        except (LookupError, RuntimeError, ValueError) as exc:
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation_id
            ) from exc
        redacted_summary, redaction_applied = _redact_summary(
            str(payload["result"]), secret_values
        )
        try:
            verification = self._verification_context(
                binding.state, assignment, workspace, payload
            )
            with verification:

                def mutate(state: dict[str, Any], connection: sqlite3.Connection) -> dict[str, Any]:
                    assignments = _records_by(state, "assignments", "assignment_id")
                    current = assignments.get(assignment_id)
                    if not isinstance(current, dict):
                        raise RuntimeError("managed result assignment disappeared")
                    prior = [
                        item
                        for item in state["result_receipts"]
                        if item.get("assignment_id") == assignment_id
                    ]
                    if prior:
                        if (
                            len(prior) == 1
                            and prior[0].get("request_fingerprint")
                            == request_fingerprint
                        ):
                            replay = deepcopy(dict(prior[0]["response"]))
                            replay.update(
                                {"status": "ALREADY_ACCEPTED", "deduplicated": True}
                            )
                            return replay
                        raise ManagedContinuityError(
                            RESULT_CONFLICT, 409, correlation_id
                        )
                    if (
                        current.get("status") != "active"
                        or current.get("agent_phone")
                        != binding.record.get("agent_phone")
                        or current.get("initial_head_commit")
                        != payload.get("from_commit")
                        or payload.get("status")
                        not in current.get("allowed_outcomes", [])
                        or self._has_unresolved_assignment_context(
                            state, assignment_id
                        )
                    ):
                        raise ManagedContinuityError(
                            RESULT_CONFLICT, 409, correlation_id
                        )
                    lease_id = current.get("branch_lease_id")
                    if isinstance(lease_id, str):
                        lease = connection.execute(
                            "SELECT * FROM branch_leases WHERE lease_id = ?",
                            (lease_id,),
                        ).fetchone()
                        runtime_lease = _records_by(
                            state, "branch_leases", "lease_id"
                        ).get(lease_id)
                        if (
                            lease is None
                            or not isinstance(runtime_lease, Mapping)
                            or lease["status"] != "active"
                            or lease["assignment_id"] != assignment_id
                            or lease["repository_id"]
                            != state["repository"]["repository_id"]
                            or lease["repository_key"]
                            != state["repository"]["repository_key"]
                            or lease["mirror_storage_key"]
                            != state["repository"]["mirror_storage_key"]
                            or lease["branch"] != workspace.get("assigned_branch")
                            or lease["source_commit"] != current.get("source_commit")
                            or lease["initial_head_commit"]
                            != current.get("initial_head_commit")
                            or runtime_lease.get("status") != "active"
                        ):
                            raise ManagedContinuityError(
                                RESULT_CONFLICT, 409, correlation_id
                            )

                    occurrence = _records_by(
                        state["workflow"], "occurrences", "occurrence_id"
                    ).get(str(current["occurrence_id"]))
                    if not isinstance(occurrence, dict) or occurrence.get("state") != "active":
                        raise RuntimeError("managed source occurrence is not active")
                    revision = _graph_revision(state, int(current["graph_revision"]))
                    definition = revision["definition"]
                    execution = definition.get("execution")
                    reviewers = execution.get("reviewers") if isinstance(execution, Mapping) else None
                    if not isinstance(reviewers, list) or len(reviewers) != 2:
                        raise RuntimeError("managed graph reviewer quorum is corrupt")

                    response = {
                        "assignment_id": assignment_id,
                        "outcome": str(payload["status"]),
                        "result_commit": str(payload["git_commit"]),
                        "result_key": result_key,
                        "status": "REVIEWS_PENDING",
                        "deduplicated": False,
                    }
                    state["result_receipts"].append(
                        {
                            "result_key": result_key,
                            "assignment_id": assignment_id,
                            "outcome": str(payload["status"]),
                            "result_commit": str(payload["git_commit"]),
                            "from_commit": str(payload["from_commit"]),
                            "git_branch": payload.get("git_branch"),
                            "result_summary_redacted": redacted_summary,
                            "redaction_applied": redaction_applied,
                            "request_fingerprint": request_fingerprint,
                            "response": deepcopy(response),
                            "accepted_at": now,
                        }
                    )
                    current.update(
                        {
                            "result_commit": str(payload["git_commit"]),
                            "outcome": str(payload["status"]),
                            "status": "reviews_pending",
                        }
                    )
                    occurrence["state"] = "reviews_pending"
                    state["transition_journal"].append(
                        {
                            "journal_id": _stable_id("journal", result_key),
                            "result_key": result_key,
                            "assignment_id": assignment_id,
                            "outcome": str(payload["status"]),
                            "result_commit": str(payload["git_commit"]),
                            "state": "REVIEWS_PENDING",
                            "disposition": "open",
                            "rework_id": None,
                            "transition_token_ids": [],
                            "target_occurrence_ids": [],
                            "outbox_event_ids": [],
                            "updated_at": now,
                        }
                    )
                    for index, reviewer in enumerate(reviewers, 1):
                        if not isinstance(reviewer, Mapping):
                            raise RuntimeError("managed reviewer definition is corrupt")
                        review_assignment_id = _stable_id(
                            "review", result_key, index, compact=True
                        )
                        state["review_assignments"].append(
                            {
                                "assignment_id": review_assignment_id,
                                "source_assignment_id": assignment_id,
                                "result_key": result_key,
                                "result_commit": str(payload["git_commit"]),
                                "result_outcome": str(payload["status"]),
                                "reviewer_id": str(reviewer["id"]),
                                "reviewer_phone": str(reviewer["phone"]),
                                "reviewer_index": index,
                                "status": "active",
                                "decision": None,
                                "request_fingerprint": None,
                                "response": None,
                                "created_at": now,
                                "activated_at": now,
                                "decided_at": None,
                            }
                        )
                        state["outbox"].append(
                            {
                                "event_id": _stable_id(
                                    "event-review", result_key, index
                                ),
                                "dedupe_key": (
                                    f"enqueue:review:{binding.sprint_id}:"
                                    f"{result_key}:{index}"
                                ),
                                "event_type": "REVIEW_ENQUEUE",
                                "payload": {
                                    "sprint_id": binding.sprint_id,
                                    "source_assignment_id": assignment_id,
                                    "review_assignment_id": review_assignment_id,
                                    "result_key": result_key,
                                    "reviewer_phone": str(reviewer["phone"]),
                                    "reviewer_index": index,
                                },
                                "status": "pending",
                                "created_at": now,
                                "delivered_at": None,
                                "queue_receipt_id": None,
                            }
                        )
                    return response

                _state, response = self.store.mutate_runtime_state(
                    binding.project_id, binding.sprint_id, mutate
                )
        except ManagedContinuityError as exc:
            if exc.correlation_id == "result-verification":
                self._stage_result_verification_failure(
                    binding.project_id,
                    binding.sprint_id,
                    assignment_id=assignment_id,
                    result_key=result_key,
                    reason_code=exc.code,
                    now=now,
                    correlation_id=correlation_id,
                )
                raise ManagedContinuityError(
                    exc.code, exc.http_status, correlation_id
                ) from exc
            raise
        self._fault("after_result_commit", result_key=result_key)
        self._resume_after_durable_ack(binding.project_id, binding.sprint_id)
        return ManagedContinuityResult(response)

    def _block_rework_limit_review(
        self,
        binding: _AssignmentBinding,
        *,
        review_assignment_id: str,
        source_assignment_id: str,
        result_key: str,
        feedback: str,
        request_fingerprint: str,
        maximum: int,
        next_cycle: int,
        now: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        """Persist a rejected review and freeze its occurrence at the cap."""

        try:
            self._stop_assignment_processes(
                binding.project_id, binding.sprint_id, source_assignment_id
            )
        except ManagedContinuityError as exc:
            return self._stage_rework_routing_failure(
                binding,
                review_assignment_id=review_assignment_id,
                source_assignment_id=source_assignment_id,
                result_key=result_key,
                feedback=feedback,
                request_fingerprint=request_fingerprint,
                failure_code=exc.code,
                now=now,
                correlation_id=correlation_id,
            )

        def mutate(
            state: dict[str, Any], connection: sqlite3.Connection
        ) -> dict[str, Any]:
            current = _records_by(
                state, "review_assignments", "assignment_id"
            ).get(review_assignment_id)
            journal = _records_by(
                state, "transition_journal", "result_key"
            ).get(result_key)
            source = _records_by(
                state, "assignments", "assignment_id"
            ).get(source_assignment_id)
            if (
                not isinstance(current, dict)
                or current.get("status") != "active"
                or not isinstance(journal, dict)
                or journal.get("state") != "REVIEWS_PENDING"
                or journal.get("disposition") != "open"
                or not isinstance(source, dict)
                or source.get("status") != "reviews_pending"
            ):
                raise ManagedContinuityError(
                    REVIEW_CONFLICT, 409, correlation_id
                )
            response = {
                "assignment_id": review_assignment_id,
                "source_assignment_id": source_assignment_id,
                "result_key": result_key,
                "decision": "REJECT",
                "status": "REWORK_ENQUEUED",
                "deduplicated": False,
            }
            current.update(
                {
                    "status": "decided",
                    "decision": "REJECT",
                    "request_fingerprint": request_fingerprint,
                    "response": deepcopy(response),
                    "decided_at": now,
                }
            )
            state["reviews"].append(
                {
                    "assignment_id": review_assignment_id,
                    "source_assignment_id": source_assignment_id,
                    "result_key": result_key,
                    "result_commit": current["result_commit"],
                    "result_outcome": current["result_outcome"],
                    "reviewer_id": current["reviewer_id"],
                    "reviewer_index": current["reviewer_index"],
                    "decision": "REJECT",
                    "feedback": feedback,
                    "request_fingerprint": request_fingerprint,
                    "response": deepcopy(response),
                    "decided_at": now,
                }
            )
            occurrence = self._settle_source_assignment(
                state,
                connection,
                source,
                now=now,
                complete_occurrence=False,
            )
            source.update({"status": "blocked", "completed_at": now})
            occurrence.update({"state": "blocked", "completed_at": now})
            self._append_assignment_failure_context(
                state,
                connection,
                source,
                result_key=result_key,
                reason_code="REWORK_LIMIT_EXCEEDED",
                normalized_error={
                    "code": "REWORK_LIMIT_EXCEEDED",
                    "max_rework_cycles": maximum,
                    "next_rework_cycle": next_cycle,
                },
                now=now,
                deliver_immediately=True,
            )
            terminal_status = self._terminal_status_when_quiescent(state)
            if terminal_status is not None:
                state["status"] = terminal_status
            return response

        _state, response = self.store.mutate_runtime_state(
            binding.project_id, binding.sprint_id, mutate
        )
        self._fault(
            "after_review_commit",
            review_assignment_id=review_assignment_id,
            decision="REJECT",
        )
        return ManagedContinuityResult(response)

    def _block_rework_limit_recovery(
        self,
        project_id: str,
        sprint_id: str,
        *,
        assignment_id: str,
        result_key: str,
        recovery_id: str,
        maximum: int,
        next_cycle: int,
        correlation_id: str,
    ) -> None:
        """Freeze a legacy/staged rework recovery that already exceeds its cap."""

        snapshot = self.store.runtime_state(project_id, sprint_id)
        if snapshot is None:
            raise RuntimeError("managed sprint runtime disappeared")
        snapshot_recovery = _records_by(
            snapshot, "recovery_records", "recovery_id"
        ).get(recovery_id)
        snapshot_context_id = (
            snapshot_recovery.get("context_id")
            if isinstance(snapshot_recovery, Mapping)
            else None
        )
        if not isinstance(snapshot_context_id, str):
            raise RuntimeError("managed recovery record disappeared")
        self._recovery_for_completion(
            snapshot,
            recovery_id,
            snapshot_context_id,
            correlation_id,
        )
        self._stop_assignment_processes(project_id, sprint_id, assignment_id)
        now = _timestamp(self.clock)

        def mutate(
            state: dict[str, Any], connection: sqlite3.Connection
        ) -> None:
            recovery_snapshot = _records_by(
                state, "recovery_records", "recovery_id"
            ).get(recovery_id)
            if not isinstance(recovery_snapshot, Mapping):
                raise RuntimeError("managed recovery record disappeared")
            recovery = self._recovery_for_completion(
                state,
                recovery_id,
                str(recovery_snapshot.get("context_id")),
                correlation_id,
                supersede_pending_at=now,
            )
            source = _records_by(
                state, "assignments", "assignment_id"
            ).get(assignment_id)
            journal = _records_by(
                state, "transition_journal", "result_key"
            ).get(result_key)
            if (
                not isinstance(recovery, dict)
                or recovery.get("status") != "pending"
                or not isinstance(source, dict)
                or source.get("status") != "reviews_pending"
                or not isinstance(journal, dict)
                or journal.get("state") != "REVIEWS_PENDING"
                or journal.get("disposition") != "open"
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            occurrence = self._settle_source_assignment(
                state,
                connection,
                source,
                now=now,
                complete_occurrence=False,
            )
            source.update({"status": "blocked", "completed_at": now})
            occurrence.update({"state": "blocked", "completed_at": now})
            self._append_assignment_failure_context(
                state,
                connection,
                source,
                result_key=result_key,
                reason_code="REWORK_LIMIT_EXCEEDED",
                normalized_error={
                    "code": "REWORK_LIMIT_EXCEEDED",
                    "max_rework_cycles": maximum,
                    "next_rework_cycle": next_cycle,
                },
                now=now,
                deliver_immediately=True,
            )
            recovery.update(
                {
                    "produced_record_ids": [],
                    "response": None,
                    "normalized_error": {
                        "code": "REWORK_LIMIT_EXCEEDED",
                        "http_status": 409,
                    },
                    "evidence": {},
                    "status": "failed",
                    "completed_at": now,
                }
            )
            terminal_status = self._terminal_status_when_quiescent(state)
            if terminal_status is not None:
                state["status"] = terminal_status

        self.store.mutate_runtime_state(project_id, sprint_id, mutate)
        raise ManagedContinuityError(
            "REWORK_LIMIT_EXCEEDED", 409, correlation_id
        )

    def drain_outbox(self, project_id: str, sprint_id: str) -> int:
        """Deliver pending records through the durable unique queue fence."""

        delivered = 0
        while True:
            state = self.store.runtime_state(project_id, sprint_id)
            if state is None:
                raise RuntimeError("managed sprint runtime disappeared")
            # A successor assignment and its outbox record commit before the
            # local Git publication receipt, because Git cannot participate in
            # the SQLite transaction.  Every drainage entry point (including
            # exact result/review/recovery replay) must close that crash gap;
            # otherwise an unrelated pending event could expose a write
            # assignment whose service-owned branch is not published yet.
            self._recover_assignment_publications(state)
            pending = next(
                (
                    event
                    for event in state.get("outbox", [])
                    if isinstance(event, Mapping) and event.get("status") == "pending"
                ),
                None,
            )
            if pending is None:
                break
            event_id = str(pending["event_id"])
            try:
                receipt_id = self.store.enqueue_managed_queue_item(
                    dedupe_key=str(pending["dedupe_key"]),
                    event_id=event_id,
                    event_type=str(pending["event_type"]),
                    payload=dict(pending["payload"]),
                    created_at=str(pending["created_at"]),
                )
            except Exception:
                self._record_outbox_delivery_failure(
                    project_id, sprint_id, event_id=event_id
                )
                raise
            self._fault(
                "after_queue_insert", event_id=event_id, receipt_id=receipt_id
            )
            delivered_at = _timestamp(self.clock)

            def mark(state: dict[str, Any], _connection: sqlite3.Connection) -> bool:
                event = _records_by(state, "outbox", "event_id").get(event_id)
                if not isinstance(event, dict):
                    raise RuntimeError("managed outbox event disappeared")
                if event.get("status") == "delivered":
                    if event.get("queue_receipt_id") != receipt_id:
                        raise RuntimeError("managed outbox receipt changed")
                    return False
                event.update(
                    {
                        "status": "delivered",
                        "delivered_at": delivered_at,
                        "queue_receipt_id": receipt_id,
                    }
                )
                return True

            _state, changed = self.store.mutate_runtime_state(
                project_id,
                sprint_id,
                mark,
                require_active=False,
            )
            delivered += int(bool(changed))
        return delivered

    def _record_outbox_delivery_failure(
        self, project_id: str, sprint_id: str, *, event_id: str
    ) -> None:
        """Persist Coordinator evidence without changing the pending event."""

        now = _timestamp(self.clock)

        def mutate(
            state: dict[str, Any], connection: sqlite3.Connection
        ) -> None:
            event = _records_by(state, "outbox", "event_id").get(event_id)
            if not isinstance(event, Mapping) or event.get("status") == "delivered":
                return
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise RuntimeError("managed outbox payload is corrupt")

            if event.get("event_type") == "COORDINATOR_ENQUEUE":
                # The source failure already owns the durable context and
                # blocker observation.  Retrying its Coordinator enqueue is
                # transport recovery, not a fresh observation of that source
                # blocker, and must not inflate the repeated-blocker count.
                return

            assignments = _records_by(state, "assignments", "assignment_id")
            receipts = _records_by(state, "result_receipts", "result_key")
            assignment: Mapping[str, Any] | None = None
            result_key: str | None = None
            if event.get("event_type") == "REVIEW_ENQUEUE":
                source_id = payload.get("source_assignment_id")
                candidate_key = payload.get("result_key")
                if isinstance(source_id, str):
                    assignment = assignments.get(source_id)
                if isinstance(candidate_key, str):
                    result_key = candidate_key
            elif event.get("event_type") == "ASSIGNMENT_ENQUEUE":
                target_id = payload.get("assignment_id")
                target = assignments.get(target_id) if isinstance(target_id, str) else None
                source_keys = (
                    target.get("source_result_keys")
                    if isinstance(target, Mapping)
                    and isinstance(target.get("source_result_keys"), list)
                    else []
                )
                if source_keys and isinstance(source_keys[0], str):
                    result_key = source_keys[0]
                    receipt = receipts.get(result_key)
                    source_id = (
                        receipt.get("assignment_id")
                        if isinstance(receipt, Mapping)
                        else None
                    )
                    if isinstance(source_id, str):
                        assignment = assignments.get(source_id)
                elif isinstance(target, Mapping):
                    assignment = target
                    result_key = _stable_id("handoff", event_id)
            else:
                return
            if not isinstance(assignment, Mapping) or not isinstance(result_key, str):
                raise RuntimeError("managed outbox owner is corrupt")
            self._append_assignment_failure_context(
                state,
                connection,
                assignment,
                result_key=result_key,
                reason_code="HANDOFF_DELIVERY_FAILED",
                normalized_error={
                    "code": "HANDOFF_DELIVERY_FAILED",
                    "phase": "OUTBOX_DELIVERY",
                },
                now=now,
                failure_scope="recovery",
                increment_existing_blocker=False,
            )

        self.store.mutate_runtime_state(
            project_id, sprint_id, mutate, require_active=False
        )

    @staticmethod
    def _review_replay(
        review_assignment: Mapping[str, Any],
    ) -> ManagedContinuityResult:
        stored = review_assignment.get("response")
        if not isinstance(stored, Mapping):
            raise RuntimeError("managed review response is corrupt")
        response = deepcopy(dict(stored))
        response.update({"status": "ALREADY_ACCEPTED", "deduplicated": True})
        return ManagedContinuityResult(response)

    def _stop_assignment_processes(
        self, project_id: str, sprint_id: str, assignment_id: str
    ) -> None:
        state = self.store.runtime_state(project_id, sprint_id)
        if state is None:
            raise RuntimeError("managed sprint runtime disappeared")
        live = [
            str(process["process_id"])
            for process in state.get("processes", [])
            if isinstance(process, Mapping)
            and process.get("assignment_id") == assignment_id
            and process.get("state") in {"PREPARED", "STARTING", "HEALTHY", "STOPPING"}
        ]
        if live and self.process_supervisor is None:
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED,
                503,
                "process-settlement",
            )
        for process_id in live:
            self.process_supervisor.stop(project_id, sprint_id, process_id)

    @staticmethod
    def _settle_source_assignment(
        state: dict[str, Any],
        connection: sqlite3.Connection,
        assignment: dict[str, Any],
        *,
        now: str,
        complete_occurrence: bool,
    ) -> dict[str, Any]:
        assignment_id = str(assignment["assignment_id"])
        process_records = [
            process
            for process in state["processes"]
            if process.get("assignment_id") == assignment_id
        ]
        if any(
            process.get("state") in {"PREPARED", "STARTING", "HEALTHY", "STOPPING"}
            for process in process_records
        ):
            raise RuntimeError("managed source processes are still live")
        if any(
            lease.get("assignment_id") == assignment_id
            and lease.get("status") in {"reserved", "bound"}
            for lease in state["port_leases"]
        ):
            raise RuntimeError("managed source port leases are still live")

        assignment.update({"status": "completed", "completed_at": now})
        occurrence = _records_by(
            state["workflow"], "occurrences", "occurrence_id"
        ).get(str(assignment["occurrence_id"]))
        if not isinstance(occurrence, dict):
            raise RuntimeError("managed source occurrence disappeared")
        if complete_occurrence:
            occurrence.update({"state": "completed", "completed_at": now})
        else:
            occurrence.update({"state": "active", "completed_at": None})

        active_ids = state["active_assignment_ids"]
        state["active_assignment_ids"] = [
            value for value in active_ids if value != assignment_id
        ]
        state["allowed_outcomes_by_assignment"].pop(assignment_id, None)

        lease_id = assignment.get("branch_lease_id")
        if isinstance(lease_id, str):
            released = BranchLeaseStore.release_in_transaction(
                connection,
                lease_id,
                assignment_id,
                assignment_settled=True,
                all_processes_stopped=True,
                released_at=now,
            )
            runtime_lease = _records_by(
                state, "branch_leases", "lease_id"
            ).get(lease_id)
            if not isinstance(runtime_lease, dict):
                raise RuntimeError("managed source branch lease disappeared")
            runtime_lease.update(
                {"status": released.status, "released_at": released.released_at}
            )
        return occurrence

    @classmethod
    def _terminal_live_work_exists(cls, state: Mapping[str, Any]) -> bool:
        """Return whether terminalizing one blocked branch would strand work."""

        workflow = state.get("workflow")
        occurrences = (
            workflow.get("occurrences", [])
            if isinstance(workflow, Mapping)
            else []
        )
        tokens = (
            workflow.get("transition_tokens", [])
            if isinstance(workflow, Mapping)
            else []
        )
        non_live_recovery_context_ids = {
            str(recovery["context_id"])
            for recovery in state.get("recovery_records", [])
            if isinstance(recovery, Mapping)
            and _recovery_settles_context(state, recovery)
            and isinstance(recovery.get("context_id"), str)
        }
        non_live_recovery_context_ids.update(
            str(context["context_id"])
            for context in state.get("coordinator_contexts", [])
            if isinstance(context, Mapping)
            and _is_rework_limit_context(context)
            and isinstance(context.get("context_id"), str)
        )
        stranded_token_ids = cls._stranded_rework_limit_token_ids(state)
        return bool(
            state.get("active_assignment_ids")
            or state.get("allowed_outcomes_by_assignment")
            or any(
                isinstance(assignment, Mapping)
                and assignment.get("status")
                in {"prepared", "active", "reviews_pending"}
                for assignment in state.get("assignments", [])
            )
            or any(
                isinstance(occurrence, Mapping)
                and occurrence.get("state")
                in {"prepared", "active", "reviews_pending"}
                for occurrence in occurrences
            )
            or any(
                isinstance(integration, Mapping)
                and integration.get("status") == "PREPARED"
                for integration in state.get("integrations", [])
            )
            or any(
                isinstance(lease, Mapping) and lease.get("status") == "active"
                for lease in state.get("branch_leases", [])
            )
            or any(
                isinstance(lease, Mapping)
                and lease.get("status") in {"reserved", "bound"}
                for lease in state.get("port_leases", [])
            )
            or any(
                isinstance(process, Mapping)
                and process.get("state")
                in {"PREPARED", "STARTING", "HEALTHY", "STOPPING"}
                for process in state.get("processes", [])
            )
            or (
                isinstance(workflow, Mapping)
                and any(
                    isinstance(recovery, Mapping)
                    and recovery.get("status") == "pending"
                    and recovery.get("action")
                    in {
                        "APPLY_REPAIR",
                        "CONTINUE_NODE",
                        "ROUTE_REWORK",
                        "BLOCK_EXTERNAL",
                    }
                    and recovery.get("context_id")
                    not in non_live_recovery_context_ids
                    for recovery in state.get("recovery_records", [])
                )
            )
            or any(
                isinstance(token, Mapping)
                and token.get("status") == "available"
                and token.get("token_id") not in stranded_token_ids
                for token in tokens
            )
            or any(
                isinstance(event, Mapping) and event.get("status") == "pending"
                for event in state.get("outbox", [])
            )
        )

    @staticmethod
    def _rework_limit_occurrence_ids(state: Mapping[str, Any]) -> set[str]:
        assignments = {
            str(assignment.get("assignment_id")): assignment
            for assignment in state.get("assignments", [])
            if isinstance(assignment, Mapping)
            and isinstance(assignment.get("assignment_id"), str)
        }
        result: set[str] = set()
        for context in state.get("coordinator_contexts", []):
            if (
                not isinstance(context, Mapping)
                or context.get("reason_code") != "REWORK_LIMIT_EXCEEDED"
            ):
                continue
            assignment = assignments.get(str(context.get("failed_assignment_id")))
            occurrence_id = (
                assignment.get("occurrence_id")
                if isinstance(assignment, Mapping)
                and assignment.get("status") == "blocked"
                else None
            )
            if isinstance(occurrence_id, str):
                result.add(occurrence_id)
        return result

    @classmethod
    def _stranded_rework_limit_token_ids(
        cls, state: Mapping[str, Any]
    ) -> set[str]:
        """Return available all-parent tokens made unschedulable by a cap.

        A capped occurrence emits no transition token.  If another parent has
        already emitted to the same ``all_parents`` target, that token can
        never form a complete cohort.  It remains immutable evidence, but is
        no longer live scheduler work.
        """

        workflow = state.get("workflow")
        if not isinstance(workflow, Mapping):
            return set()
        available = [
            token
            for token in workflow.get("transition_tokens", [])
            if isinstance(token, Mapping)
            and token.get("status") == "available"
            and isinstance(token.get("token_id"), str)
            and isinstance(token.get("target_node_id"), str)
        ]
        if not available:
            return set()

        assignments = {
            str(assignment.get("assignment_id")): assignment
            for assignment in state.get("assignments", [])
            if isinstance(assignment, Mapping)
            and isinstance(assignment.get("assignment_id"), str)
        }
        capped_parent_ids: set[str] = set()
        for context in state.get("coordinator_contexts", []):
            if (
                not isinstance(context, Mapping)
                or context.get("reason_code") != "REWORK_LIMIT_EXCEEDED"
            ):
                continue
            assignment = assignments.get(str(context.get("failed_assignment_id")))
            if (
                not isinstance(assignment, Mapping)
                or assignment.get("status") != "blocked"
                or not isinstance(assignment.get("node_id"), str)
            ):
                continue
            capped_parent_ids.add(str(assignment["node_id"]))

        try:
            current_revision = int(state["graph_revision"])
            definition = _graph_revision(state, current_revision)["definition"]
        except (KeyError, TypeError, ValueError):
            return set()
        stranded: set[str] = set()
        for target_node_id in sorted(
            {str(token["target_node_id"]) for token in available}
        ):
            try:
                target = _node(state, current_revision, target_node_id)
            except (KeyError, TypeError, ValueError):
                continue
            if target.get("activation_policy", "all_parents") != "all_parents":
                continue
            parent_order = target.get("join_parent_order")
            if not isinstance(parent_order, list):
                parent_order = cls._inbound_parent_ids(definition, target_node_id)
            available_parents = {
                str(token.get("source_node_id"))
                for token in available
                if token.get("target_node_id") == target_node_id
            }
            missing_parents = {
                str(parent_id) for parent_id in parent_order
            } - available_parents
            if not missing_parents.intersection(capped_parent_ids):
                continue
            stranded.update(
                str(token["token_id"])
                for token in available
                if token.get("target_node_id") == target_node_id
            )
        return stranded

    @classmethod
    def _terminal_status_when_quiescent(
        cls, state: Mapping[str, Any]
    ) -> str | None:
        """Combine terminal tokens with branch-scoped rework-cap evidence."""

        if cls._terminal_live_work_exists(state):
            return None
        workflow = state.get("workflow")
        tokens = (
            workflow.get("transition_tokens", [])
            if isinstance(workflow, Mapping)
            else []
        )
        terminal_tokens = [
            token
            for token in tokens
            if isinstance(token, Mapping) and token.get("status") == "terminal"
        ]
        capped_occurrences = cls._rework_limit_occurrence_ids(state)
        if not terminal_tokens and not capped_occurrences:
            return None
        terminal_statuses = {
            _node(
                state,
                int(token["target_graph_revision"]),
                str(token["target_node_id"]),
            ).get("status")
            for token in terminal_tokens
        }
        if "FAILED" in terminal_statuses:
            return "failed"
        if "BLOCKED_EXTERNAL" in terminal_statuses or capped_occurrences:
            return "blocked"
        if terminal_statuses == {"DONE"}:
            return "completed"
        return None

    @staticmethod
    def _terminal_failure_target(
        state: Mapping[str, Any], assignment: Mapping[str, Any]
    ) -> tuple[str, str] | None:
        source_revision = int(assignment["graph_revision"])
        source_definition = _graph_revision(state, source_revision)["definition"]
        target_node_id = _outcome_target(
            source_definition,
            str(assignment["node_id"]),
            str(assignment["outcome"]),
        )
        target = _node(state, int(state["graph_revision"]), target_node_id)
        terminal_status = target.get("status")
        if (
            target.get("type", "task") == "terminal"
            and terminal_status in {"FAILED", "BLOCKED_EXTERNAL"}
        ):
            return target_node_id, str(terminal_status)
        return None

    @staticmethod
    def _observe_blocker_in_state(
        state: dict[str, Any],
        assignment_id: str,
        reason_code: str,
        normalized_error: Mapping[str, Any],
        *,
        now: str,
    ) -> dict[str, Any]:
        """Create or increment one assignment-scoped blocker observation."""

        fingerprint = managed_blocker_fingerprint(
            assignment_id, reason_code, normalized_error
        )
        prior = _records_by(
            state, "blocker_observations", "fingerprint"
        ).get(fingerprint)
        if prior is None:
            prior = {
                "fingerprint": fingerprint,
                "assignment_id": assignment_id,
                "reason_code": reason_code,
                "normalized_error": deepcopy(dict(normalized_error)),
                "count": 1,
                "first_seen_at": now,
                "last_seen_at": now,
            }
            state["blocker_observations"].append(prior)
            return prior
        if (
            prior.get("assignment_id") != assignment_id
            or not (
                (
                    prior.get("reason_code") is None
                    and prior.get("normalized_error") is None
                )
                or (
                    prior.get("reason_code") == reason_code
                    and prior.get("normalized_error") == normalized_error
                )
            )
        ):
            raise RuntimeError("managed blocker fingerprint collision")
        prior["count"] = int(prior["count"]) + 1
        prior["last_seen_at"] = now
        return prior

    def _append_assignment_failure_context(
        self,
        state: dict[str, Any],
        connection: sqlite3.Connection,
        assignment: Mapping[str, Any],
        *,
        result_key: str,
        reason_code: str,
        normalized_error: Mapping[str, Any],
        now: str,
        failure_scope: str = "assignment",
        deliver_immediately: bool = False,
        identity_salt: str | None = None,
        increment_existing_blocker: bool = True,
    ) -> dict[str, Any]:
        assignment_id = str(assignment["assignment_id"])
        graph_revision = int(state["graph_revision"])
        context_identity: list[object] = [
            state["sprint_id"],
            graph_revision,
            assignment_id,
            result_key,
            reason_code,
        ]
        if identity_salt is not None:
            context_identity.append(identity_salt)
        context_id = _stable_id(
            "context-assignment", *context_identity, compact=True
        )
        workspace = _records_by(state, "workspaces", "workspace_id").get(
            str(assignment["workspace_id"])
        )
        if not isinstance(workspace, Mapping):
            raise RuntimeError("managed failed assignment workspace is missing")
        definition = _graph_revision(state, graph_revision)["definition"]
        coordinator = definition["coordinator"]
        coordinator_node = next(
            item
            for item in definition["nodes"]
            if item.get("id") == coordinator["node_id"]
        )
        reviewer_feedback = [
            {
                "reviewer_id": str(review["reviewer_id"]),
                "reviewer_index": int(review["reviewer_index"]),
                "decision": str(review["decision"]),
                "feedback": str(review.get("feedback") or ""),
            }
            for review in state.get("reviews", [])
            if isinstance(review, Mapping)
            and review.get("result_key") == result_key
        ]
        receipt = _records_by(state, "result_receipts", "result_key").get(
            result_key
        )
        context = {
            "context_id": context_id,
            "graph_revision": graph_revision,
            "failure_scope": failure_scope,
            "reason_code": reason_code,
            "import_attempt_id": None,
            "failed_assignment_id": assignment_id,
            "join_target": None,
            "assigned_branch": workspace.get("assigned_branch"),
            "source_commit": assignment.get("source_commit"),
            "result_commit": assignment.get("result_commit"),
            "diff_summary": {},
            "workspace_status": deepcopy(dict(workspace)),
            "process_records": [
                deepcopy(dict(process))
                for process in state.get("processes", [])
                if isinstance(process, Mapping)
                and process.get("assignment_id") == assignment_id
            ],
            "port_records": [
                deepcopy(dict(port))
                for port in state.get("port_leases", [])
                if isinstance(port, Mapping)
                and port.get("assignment_id") == assignment_id
            ],
            "test_evidence_summary": (
                {
                    "result_summary_redacted": receipt.get(
                        "result_summary_redacted"
                    )
                }
                if isinstance(receipt, Mapping)
                else {}
            ),
            "normalized_error": deepcopy(dict(normalized_error)),
            "reviewer_feedback": reviewer_feedback,
        }
        contexts = _records_by(state, "coordinator_contexts", "context_id")
        existing = contexts.get(context_id)
        if existing is not None:
            if (
                existing.get("failed_assignment_id") != assignment_id
                or existing.get("failure_scope") != failure_scope
                or existing.get("reason_code") != reason_code
                or existing.get("normalized_error") != normalized_error
            ):
                raise RuntimeError("managed assignment context identity conflict")
            if increment_existing_blocker:
                self._observe_blocker_in_state(
                    state,
                    assignment_id,
                    reason_code,
                    existing["normalized_error"],
                    now=now,
                )
            return existing
        event = {
            "event_id": _stable_id("event-coordinator", context_id),
            "dedupe_key": (
                f"enqueue:coordinator:{state['sprint_id']}:{context_id}"
            ),
            "event_type": "COORDINATOR_ENQUEUE",
            "payload": {
                "sprint_id": str(state["sprint_id"]),
                "context_id": context_id,
                "coordinator_phone": str(coordinator_node["agent"]["phone"]),
                "reason_code": reason_code,
            },
            "status": "pending",
            "created_at": now,
            "delivered_at": None,
            "queue_receipt_id": None,
        }
        if deliver_immediately:
            receipt_id = self.store.enqueue_managed_queue_item_in_transaction(
                connection,
                dedupe_key=str(event["dedupe_key"]),
                event_id=str(event["event_id"]),
                event_type=str(event["event_type"]),
                payload=dict(event["payload"]),
                created_at=now,
            )
            event.update(
                {
                    "status": "delivered",
                    "delivered_at": now,
                    "queue_receipt_id": receipt_id,
                }
            )
        self._observe_blocker_in_state(
            state,
            assignment_id,
            reason_code,
            normalized_error,
            now=now,
        )
        state["coordinator_contexts"].append(context)
        state["outbox"].append(event)
        return context

    def _stage_result_verification_failure(
        self,
        project_id: str,
        sprint_id: str,
        *,
        assignment_id: str,
        result_key: str,
        reason_code: str,
        now: str,
        correlation_id: str,
    ) -> None:
        """Durably route a non-destructive result verification failure."""

        def mutate(
            state: dict[str, Any], connection: sqlite3.Connection
        ) -> None:
            assignment = _records_by(
                state, "assignments", "assignment_id"
            ).get(assignment_id)
            if (
                not isinstance(assignment, dict)
                or assignment.get("status") != "active"
                or any(
                    item.get("assignment_id") == assignment_id
                    for item in state["result_receipts"]
                )
            ):
                raise ManagedContinuityError(
                    RESULT_CONFLICT, 409, correlation_id
                )
            self._append_assignment_failure_context(
                state,
                connection,
                assignment,
                result_key=result_key,
                reason_code=reason_code,
                normalized_error={
                    "code": reason_code,
                    "phase": "RESULT_VERIFICATION",
                },
                now=now,
            )

        self.store.mutate_runtime_state(project_id, sprint_id, mutate)
        self.drain_outbox(project_id, sprint_id)

    def _stage_rework_routing_failure(
        self,
        binding: _AssignmentBinding,
        *,
        review_assignment_id: str,
        source_assignment_id: str,
        result_key: str,
        feedback: str,
        request_fingerprint: str,
        failure_code: str,
        now: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        """Freeze the rejection and hand failed successor routing to Coordinator."""

        def mutate(
            state: dict[str, Any], connection: sqlite3.Connection
        ) -> dict[str, Any]:
            current = _records_by(
                state, "review_assignments", "assignment_id"
            ).get(review_assignment_id)
            journal = _records_by(
                state, "transition_journal", "result_key"
            ).get(result_key)
            source = _records_by(
                state, "assignments", "assignment_id"
            ).get(source_assignment_id)
            if not isinstance(current, dict) or not isinstance(source, dict):
                raise RuntimeError("managed rework routing binding disappeared")
            if current.get("status") == "decided":
                if current.get("request_fingerprint") == request_fingerprint:
                    replay = deepcopy(dict(current["response"]))
                    replay.update(
                        {"status": "ALREADY_ACCEPTED", "deduplicated": True}
                    )
                    return replay
                raise ManagedContinuityError(
                    REVIEW_CONFLICT, 409, correlation_id
                )
            if (
                current.get("status") != "active"
                or not isinstance(journal, dict)
                or journal.get("state") != "REVIEWS_PENDING"
                or journal.get("disposition") != "open"
                or source.get("status") != "reviews_pending"
            ):
                raise ManagedContinuityError(
                    REVIEW_CONFLICT, 409, correlation_id
                )
            response = {
                "assignment_id": review_assignment_id,
                "source_assignment_id": source_assignment_id,
                "result_key": result_key,
                "decision": "REJECT",
                "status": "REWORK_ENQUEUED",
                "deduplicated": False,
            }
            current.update(
                {
                    "status": "decided",
                    "decision": "REJECT",
                    "request_fingerprint": request_fingerprint,
                    "response": deepcopy(response),
                    "decided_at": now,
                }
            )
            state["reviews"].append(
                {
                    "assignment_id": review_assignment_id,
                    "source_assignment_id": source_assignment_id,
                    "result_key": result_key,
                    "result_commit": current["result_commit"],
                    "result_outcome": current["result_outcome"],
                    "reviewer_id": current["reviewer_id"],
                    "reviewer_index": current["reviewer_index"],
                    "decision": "REJECT",
                    "feedback": feedback,
                    "request_fingerprint": request_fingerprint,
                    "response": deepcopy(response),
                    "decided_at": now,
                }
            )
            self._append_assignment_failure_context(
                state,
                connection,
                source,
                result_key=result_key,
                reason_code="REWORK_ROUTING_FAILED",
                normalized_error={
                    "code": "REWORK_ROUTING_FAILED",
                    "failure_code": failure_code,
                },
                now=now,
            )
            return response

        _state, response = self.store.mutate_runtime_state(
            binding.project_id, binding.sprint_id, mutate
        )
        self._fault(
            "after_review_commit",
            review_assignment_id=review_assignment_id,
            decision="REJECT",
        )
        self._resume_after_durable_ack(binding.project_id, binding.sprint_id)
        return ManagedContinuityResult(response)

    @classmethod
    def _append_transition_token(
        cls,
        state: dict[str, Any],
        assignment: Mapping[str, Any],
        journal: dict[str, Any],
        *,
        now: str,
    ) -> dict[str, Any]:
        revision = int(assignment["graph_revision"])
        definition = _graph_revision(state, revision)["definition"]
        target_node_id = _outcome_target(
            definition,
            str(assignment["node_id"]),
            str(assignment["outcome"]),
        )
        current_revision = int(state["graph_revision"])
        target_node = _node(state, current_revision, target_node_id)
        terminal = target_node.get("type", "task") == "terminal"
        token_id = managed_transition_token_id(
            str(state["sprint_id"]),
            str(assignment["occurrence_id"]),
            str(journal["result_key"]),
            target_node_id,
        )
        token = {
            "token_id": token_id,
            "source_occurrence_id": str(assignment["occurrence_id"]),
            "source_node_id": str(assignment["node_id"]),
            "source_graph_revision": revision,
            "result_key": str(journal["result_key"]),
            "target_node_id": target_node_id,
            "target_graph_revision": current_revision if terminal else None,
            "status": "terminal" if terminal else "available",
            "consumed_by_occurrence_id": None,
            "created_at": now,
        }
        existing = _records_by(
            state["workflow"], "transition_tokens", "token_id"
        ).get(token_id)
        if existing is None:
            state["workflow"]["transition_tokens"].append(token)
        elif existing != token:
            raise RuntimeError("managed transition token identity conflict")
        journal.update(
            {
                "state": "TRANSITION_COMMITTED",
                "disposition": "accepted",
                "transition_token_ids": [token_id],
                "target_occurrence_ids": [],
                "outbox_event_ids": [],
                "updated_at": now,
            }
        )
        terminal_status = cls._terminal_status_when_quiescent(state)
        if terminal_status is not None:
            state["status"] = terminal_status
        return token

    @staticmethod
    def _recovery_replay(record: Mapping[str, Any]) -> ManagedContinuityResult:
        response = record.get("response")
        if not isinstance(response, Mapping):
            raise RuntimeError("managed recovery response is corrupt")
        replay = deepcopy(dict(response))
        replay["deduplicated"] = True
        return ManagedContinuityResult(replay)

    @staticmethod
    def _published_repair_for_pending_recovery(
        state: Mapping[str, Any], recovery: Mapping[str, Any]
    ) -> Mapping[str, Any] | None:
        """Find the irreversible repair effect owned by a pending recovery."""

        if (
            recovery.get("status") != "pending"
            or recovery.get("action") != "APPLY_REPAIR"
        ):
            return None
        parameters = recovery.get("parameters")
        request = (
            parameters.get("request")
            if isinstance(parameters, Mapping)
            else None
        )
        if not isinstance(request, Mapping):
            return None
        matches = [
            repair
            for repair in state.get("repairs", [])
            if isinstance(repair, Mapping)
            and repair.get("from_revision") == request.get("expected_revision")
            and repair.get("repair_source_commit")
            == request.get("repair_source_commit")
            and repair.get("idempotency_key") == request.get("idempotency_key")
            and repair.get("patch") == request.get("patch")
        ]
        return matches[0] if len(matches) == 1 else None

    @classmethod
    def _recovery_for_completion(
        cls,
        state: Mapping[str, Any],
        recovery_id: str,
        context_id: str,
        correlation_id: str,
        *,
        supersede_pending_at: str | None = None,
    ) -> dict[str, Any]:
        """Return one pending/completed recovery behind the context decision fence."""

        recovery = _records_by(
            state, "recovery_records", "recovery_id"
        ).get(recovery_id)
        if (
            not isinstance(recovery, dict)
            or recovery.get("context_id") != context_id
        ):
            raise RuntimeError("managed recovery binding disappeared")
        status = recovery.get("status")
        if status not in {"pending", "completed"}:
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )
        owns_published_repair = (
            cls._published_repair_for_pending_recovery(state, recovery)
            is not None
        )
        if status == "pending" and not owns_published_repair and any(
            item is not recovery
            and item.get("context_id") == context_id
            and _recovery_settles_context(state, item)
            for item in state.get("recovery_records", [])
            if isinstance(item, Mapping)
        ):
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )
        if status == "pending":
            pending_for_context = [
                item
                for item in state.get("recovery_records", [])
                if isinstance(item, dict)
                and item.get("context_id") == context_id
                and item.get("status") == "pending"
            ]
            effect_owners = [
                item
                for item in pending_for_context
                if cls._published_repair_for_pending_recovery(state, item)
                is not None
            ]
            winner = (
                effect_owners[0]
                if effect_owners
                else pending_for_context[0]
                if pending_for_context
                else None
            )
            if winner is not recovery:
                # Older runtimes could persist more than one in-flight choice.
                # Published repair effects own the decision in durable append
                # order; otherwise keep append order for effect-free racers.
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            if supersede_pending_at is not None:
                for loser in pending_for_context:
                    if (
                        loser is recovery
                        or cls._published_repair_for_pending_recovery(state, loser)
                        is not None
                    ):
                        continue
                    loser.update(
                        {
                            "produced_record_ids": [],
                            "response": None,
                            "normalized_error": {
                                "code": RECOVERY_CONFLICT,
                                "http_status": 409,
                            },
                            "evidence": {},
                            "status": "failed",
                            "completed_at": supersede_pending_at,
                        }
                    )
        return recovery

    def _submit_recovery(
        self,
        binding: _AssignmentBinding,
        payload: Mapping[str, Any],
        correlation_id: str,
        supplied_role_token: str = "",
    ) -> ManagedContinuityResult:
        with self._sprint_lock(binding.sprint_id):
            latest = self.store.runtime_state(binding.project_id, binding.sprint_id)
            if latest is None:
                raise RuntimeError("managed sprint runtime disappeared")
            context = _records_by(
                latest, "coordinator_contexts", "context_id"
            ).get(str(binding.record.get("context_id")))
            if not isinstance(context, dict):
                raise RuntimeError("managed Coordinator context disappeared")
            self._require_scope(latest, str(binding.record["assignment_id"]), payload, supplied_role_token, correlation_id)
            return self._submit_recovery_locked(
                binding.project_id,
                binding.sprint_id,
                latest,
                context,
                str(binding.record["coordinator_id"]),
                payload,
                correlation_id,
            )

    def _submit_recovery_locked(
        self,
        project_id: str,
        sprint_id: str,
        state: Mapping[str, Any],
        context: Mapping[str, Any],
        coordinator_id: str,
        payload: Mapping[str, Any],
        correlation_id: str,
    ) -> ManagedContinuityResult:
        if _is_rework_limit_context(context):
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )
        if set(payload) - {"scope_context"} != {
            "assignment_id",
            "idempotency_key",
            "action",
            "parameters",
        }:
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
            )
        idempotency_key = payload.get("idempotency_key")
        action = payload.get("action")
        parameters = payload.get("parameters")
        if (
            not isinstance(idempotency_key, str)
            or not 1 <= len(idempotency_key) <= 200
            or not isinstance(action, str)
            or not isinstance(parameters, Mapping)
        ):
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
            )
        if action == "RETRY_IMPORT":
            failed_attempt_id = parameters.get("failed_attempt_id")
            recovery_idempotency_key = parameters.get(
                "recovery_idempotency_key"
            )
            import_attempt = next(
                (
                    item
                    for item in state.get("import_attempts", [])
                    if isinstance(item, Mapping)
                    and item.get("attempt_id") == failed_attempt_id
                ),
                None,
            )
            if (
                set(parameters)
                != {"failed_attempt_id", "recovery_idempotency_key"}
                or not isinstance(failed_attempt_id, str)
                or not failed_attempt_id
                or not isinstance(recovery_idempotency_key, str)
                or not 1 <= len(recovery_idempotency_key) <= 200
                or context.get("import_attempt_id") != failed_attempt_id
                or context.get("failed_assignment_id") is not None
                or context.get("join_target") is not None
                or not isinstance(import_attempt, Mapping)
                or import_attempt.get("status") != "failed"
            ):
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
                )
        elif action == "BLOCK_EXTERNAL":
            if (
                set(parameters) != {"reason_code", "operator_action"}
                or not isinstance(parameters.get("reason_code"), str)
                or re.fullmatch(
                    r"[A-Z][A-Z0-9_]*", str(parameters.get("reason_code"))
                )
                is None
                or not isinstance(parameters.get("operator_action"), str)
                or not str(parameters.get("operator_action")).strip()
            ):
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
                )
            target_count = sum(
                isinstance(context.get(field), str)
                for field in ("import_attempt_id", "failed_assignment_id")
            )
            if (
                target_count != 1
                or context.get("join_target") is not None
                or parameters.get("reason_code") != context.get("reason_code")
            ):
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
                )
        elif action == "RETRY_HANDOFF":
            result_key = parameters.get("result_key")
            if (
                set(parameters) != {"result_key"}
                or not isinstance(result_key, str)
                or re.fullmatch(r"result-[0-9a-f]{64}", result_key) is None
                or not isinstance(context.get("failed_assignment_id"), str)
                or context.get("join_target") is not None
                or context.get("reason_code") != "HANDOFF_DELIVERY_FAILED"
                or context.get("failure_scope") != "recovery"
            ):
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
                )
        elif action == "ROUTE_REWORK":
            rejected_result_key = parameters.get("rejected_result_key")
            feedback = parameters.get("feedback")
            if (
                set(parameters) != {"rejected_result_key", "feedback"}
                or not isinstance(rejected_result_key, str)
                or re.fullmatch(r"result-[0-9a-f]{64}", rejected_result_key)
                is None
                or not isinstance(feedback, str)
                or not feedback.strip()
                or not isinstance(context.get("failed_assignment_id"), str)
                or context.get("join_target") is not None
            ):
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
                )
        elif action == "CONTINUE_NODE":
            node_id = parameters.get("node_id")
            source_commit = parameters.get("source_commit")
            if (
                set(parameters) != {"node_id", "source_commit"}
                or not isinstance(node_id, str)
                or re.fullmatch(
                    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9_-])?",
                    node_id,
                )
                is None
                or not isinstance(source_commit, str)
                or re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", source_commit)
                is None
                or not isinstance(context.get("failed_assignment_id"), str)
                or context.get("join_target") is not None
            ):
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
                )
        elif action == "APPLY_REPAIR":
            if (
                set(parameters) != {"request"}
                or not isinstance(parameters.get("request"), Mapping)
                or sum(
                    target is not None
                    for target in (
                        context.get("import_attempt_id"),
                        context.get("failed_assignment_id"),
                        context.get("join_target"),
                    )
                )
                != 1
            ):
                raise ManagedContinuityError(
                    INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
                )
            self._validate_schema(
                parameters["request"], _REPAIR_SCHEMA, correlation_id
            )
        else:
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST,
                400,
                correlation_id,
                field="action",
            )

        context_id = str(context["context_id"])
        graph_revision = int(context["graph_revision"])
        import_attempt_id = context.get("import_attempt_id")
        assignment_id = context.get("failed_assignment_id")
        join_target = context.get("join_target")
        existing = next(
            (
                item
                for item in state.get("recovery_records", [])
                if isinstance(item, Mapping)
                and item.get("coordinator_id") == coordinator_id
                and item.get("idempotency_key") == idempotency_key
            ),
            None,
        )
        if (
            existing is None
            and isinstance(state.get("workflow"), Mapping)
            and state.get("status") != "active"
            and action
            in {
                "APPLY_REPAIR",
                "CONTINUE_NODE",
                "ROUTE_REWORK",
                "BLOCK_EXTERNAL",
            }
        ):
            # These choices mutate live workflow state.  Terminal handoff
            # contexts remain addressable for RETRY_HANDOFF replay/recovery,
            # but must not stage a new workflow mutation behind a terminal
            # aggregate (native v2 also rejects that pending intermediate).
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )
        if existing is None and any(
            item.get("context_id") == context_id
            and (
                item.get("status") == "pending"
                or _recovery_settles_context(state, item)
            )
            for item in state.get("recovery_records", [])
            if isinstance(item, Mapping)
        ):
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )

        if action == "RETRY_IMPORT":
            incoming_parameters = deepcopy(dict(parameters))
        else:
            try:
                secret_values = self._resolved_secret_values(state)
            except (LookupError, RuntimeError, ValueError) as exc:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation_id
                ) from exc
            incoming_parameters = _redact_json_strings(parameters, secret_values)
        if not isinstance(incoming_parameters, dict):
            raise RuntimeError("managed recovery parameters are corrupt")
        fingerprint = recovery_request_fingerprint(
            context_id,
            graph_revision,
            action,
            str(import_attempt_id) if isinstance(import_attempt_id, str) else None,
            str(assignment_id) if isinstance(assignment_id, str) else None,
            incoming_parameters,
            join_target if isinstance(join_target, Mapping) else None,
        )

        if existing is not None:
            if existing.get("request_fingerprint") != fingerprint:
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            if existing.get("status") == "completed":
                if action == "RETRY_IMPORT":
                    try:
                        child_id = self._retry_import_child_id(
                            existing,
                            correlation_id=correlation_id,
                        )
                        child = self.store.begin_retry_import(
                            project_id,
                            sprint_id,
                            str(existing["recovery_id"]),
                            attempt_id_factory=self.importer.attempt_id_factory,
                            clock=self.clock,
                        )
                        if child.get("attempt_id") != child_id:
                            raise RuntimeError(
                                "managed retry import child identity changed"
                            )
                    except Exception:
                        # The completed recovery receipt is authoritative;
                        # startup reconciliation can resume its durable child.
                        pass
                else:
                    self._resume_after_durable_ack(
                        project_id, sprint_id, reconcile=True
                    )
                return self._recovery_replay(existing)
            if existing.get("status") == "failed":
                normalized_error = existing.get("normalized_error")
                if not isinstance(normalized_error, Mapping):
                    raise RuntimeError("managed recovery failure is corrupt")
                error_code = normalized_error.get("code")
                http_status = normalized_error.get("http_status")
                if (
                    not isinstance(error_code, str)
                    or not isinstance(http_status, int)
                    or isinstance(http_status, bool)
                    or not 400 <= http_status < 500
                ):
                    raise RuntimeError("managed recovery failure is corrupt")
                raise ManagedContinuityError(
                    error_code, http_status, correlation_id
                )
            if (
                self._published_repair_for_pending_recovery(state, existing)
                is None
                and any(
                    item is not existing
                    and item.get("context_id") == context_id
                    and _recovery_settles_context(state, item)
                    for item in state.get("recovery_records", [])
                    if isinstance(item, Mapping)
                )
            ):
                self._fail_pending_recovery(
                    project_id,
                    sprint_id,
                    str(existing["recovery_id"]),
                    error_code=RECOVERY_CONFLICT,
                    http_status=409,
                )
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            self._recovery_for_completion(
                state,
                str(existing["recovery_id"]),
                context_id,
                correlation_id,
            )
            durable_parameters = deepcopy(existing.get("parameters"))
            if not isinstance(durable_parameters, dict):
                raise RuntimeError("managed recovery parameters are corrupt")
        else:
            durable_parameters = incoming_parameters

        if existing is not None:
            recovery_id = existing.get("recovery_id")
            if not isinstance(recovery_id, str) or not recovery_id:
                raise RuntimeError("managed recovery identity is corrupt")
        else:
            recovery_id = _stable_id(
                "recovery",
                sprint_id,
                coordinator_id,
                idempotency_key,
                fingerprint,
                compact=True,
            )
        created_at = _timestamp(self.clock)
        if existing is None:

            def begin(
                candidate: dict[str, Any], _connection: sqlite3.Connection
            ) -> None:
                prior = next(
                    (
                        item
                        for item in candidate["recovery_records"]
                        if item.get("coordinator_id") == coordinator_id
                        and item.get("idempotency_key") == idempotency_key
                    ),
                    None,
                )
                if prior is not None:
                    if prior.get("request_fingerprint") != fingerprint:
                        raise ManagedContinuityError(
                            RECOVERY_CONFLICT, 409, correlation_id
                        )
                    return
                if any(
                    item.get("context_id") == context_id
                    and (
                        item.get("status") == "pending"
                        or _recovery_settles_context(candidate, item)
                    )
                    for item in candidate["recovery_records"]
                    if isinstance(item, Mapping)
                ):
                    # A context has one in-flight or decided recovery.  Only
                    # the exact idempotency-key replay above may resume it; a
                    # new key must target a newly emitted failure context.
                    raise ManagedContinuityError(
                        RECOVERY_CONFLICT, 409, correlation_id
                    )
                candidate["recovery_records"].append(
                    {
                        "recovery_id": recovery_id,
                        "coordinator_id": coordinator_id,
                        "context_id": context_id,
                        "graph_revision": graph_revision,
                        "idempotency_key": idempotency_key,
                        "request_fingerprint": fingerprint,
                        "action": action,
                        "parameters": deepcopy(durable_parameters),
                        "import_attempt_id": (
                            str(import_attempt_id)
                            if isinstance(import_attempt_id, str)
                            else None
                        ),
                        "assignment_id": (
                            str(assignment_id)
                            if isinstance(assignment_id, str)
                            else None
                        ),
                        "join_target": (
                            deepcopy(dict(join_target))
                            if isinstance(join_target, Mapping)
                            else None
                        ),
                        "produced_record_ids": [],
                        "response": None,
                        "normalized_error": None,
                        "evidence": {},
                        "status": "pending",
                        "created_at": created_at,
                        "completed_at": None,
                    }
                )

            self.store.mutate_runtime_state(
                project_id,
                sprint_id,
                begin,
                require_active=False,
            )
            self._fault("after_recovery_pending", recovery_id=recovery_id)

        try:
            if action == "RETRY_IMPORT":
                return self._complete_retry_import_recovery(
                    project_id,
                    sprint_id,
                    recovery_id=recovery_id,
                    correlation_id=correlation_id,
                )
            if action == "BLOCK_EXTERNAL":
                return self._complete_block_external_recovery(
                    project_id,
                    sprint_id,
                    context_id=context_id,
                    recovery_id=recovery_id,
                    correlation_id=correlation_id,
                )
            if action == "RETRY_HANDOFF":
                return self._complete_retry_handoff_recovery(
                    project_id,
                    sprint_id,
                    context_id=context_id,
                    recovery_id=recovery_id,
                    correlation_id=correlation_id,
                )
            if action == "ROUTE_REWORK":
                return self._complete_route_rework_recovery(
                    project_id,
                    sprint_id,
                    context_id=context_id,
                    recovery_id=recovery_id,
                    coordinator_id=coordinator_id,
                    correlation_id=correlation_id,
                )
            if action == "CONTINUE_NODE":
                return self._complete_continue_node_recovery(
                    project_id,
                    sprint_id,
                    context_id=context_id,
                    recovery_id=recovery_id,
                    correlation_id=correlation_id,
                )
            if action == "APPLY_REPAIR":
                return self._repair_locked(
                    sprint_id,
                    durable_parameters["request"],
                    correlation_id,
                    recovery_id=recovery_id,
                    recovery_context_id=context_id,
                )
        except ManagedContinuityError as exc:
            if exc.http_status < 500:
                self._fail_pending_recovery(
                    project_id,
                    sprint_id,
                    recovery_id,
                    error_code=exc.code,
                    http_status=exc.http_status,
                )
            raise
        raise AssertionError("unreachable managed recovery action")

    @staticmethod
    def _retry_import_child_id(
        recovery: Mapping[str, Any], *, correlation_id: str
    ) -> str:
        produced_ids = recovery.get("produced_record_ids")
        if (
            recovery.get("action") != "RETRY_IMPORT"
            or recovery.get("status") != "completed"
            or not isinstance(produced_ids, list)
            or len(produced_ids) != 1
            or not isinstance(produced_ids[0], str)
            or not produced_ids[0]
        ):
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED,
                503,
                correlation_id,
            )
        return produced_ids[0]

    def _resume_retry_import_child(
        self,
        project_id: str,
        child_attempt_id: str,
        *,
        correlation_id: str,
    ) -> None:
        """Resume a durable retry child, preserving its terminal failure."""

        try:
            self.importer.resume_import_attempt(project_id, child_attempt_id)
        except ManagedImportError:
            child = self.store.lookup_attempt(project_id, child_attempt_id)
            if not isinstance(child, Mapping) or child.get("status") not in {
                "VALIDATING",
                "PREPARING",
                "ACTIVATING",
                "FAILED",
                "SUCCEEDED",
            }:
                raise RuntimeError("managed retry import child is corrupt")
        child = self.store.lookup_attempt(project_id, child_attempt_id)
        if (
            isinstance(child, Mapping)
            and child.get("status") == "SUCCEEDED"
            and self.process_supervisor is not None
        ):
            try:
                self.process_supervisor.start_background()
            except Exception:
                # Import publication is already durable.  Process startup is
                # reconciled independently and must not rewrite its receipt.
                pass

    def _complete_retry_import_recovery(
        self,
        project_id: str,
        sprint_id: str,
        *,
        recovery_id: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        """Fence one child and immediately return its durable receipt.

        Git/workspace activation is intentionally left to ``reconcile_all``.
        It can exceed a public proxy timeout, while the immutable child and
        recovery decision are safe to acknowledge after their atomic commit.
        """

        try:
            child = self.store.begin_retry_import(
                project_id,
                sprint_id,
                recovery_id,
                attempt_id_factory=self.importer.attempt_id_factory,
                clock=self.clock,
            )
        except ManagedRetryImportConflict as exc:
            raise ManagedContinuityError(
                RECOVERY_CONFLICT,
                409,
                correlation_id,
            ) from exc
        child_attempt_id = child.get("attempt_id")
        if not isinstance(child_attempt_id, str) or not child_attempt_id:
            raise RuntimeError("managed retry import child identity is corrupt")
        # begin_retry_import commits this exact response atomically with the
        # child.  Re-reading after that commit would let a transient read or
        # validation failure replace an acknowledgement already owned by the
        # caller.
        result = {
            "recovery_id": recovery_id,
            "action": "RETRY_IMPORT",
            "status": "RECOVERY_COMPLETED",
            "produced_record_ids": [child_attempt_id],
            "deduplicated": False,
        }
        self._fault("after_recovery_commit", recovery_id=recovery_id)
        return ManagedContinuityResult(result)

    def _fail_pending_recovery(
        self,
        project_id: str,
        sprint_id: str,
        recovery_id: str,
        *,
        error_code: str,
        http_status: int,
    ) -> None:
        failed_at = _timestamp(self.clock)

        def fail(
            state: dict[str, Any], _connection: sqlite3.Connection
        ) -> None:
            recovery = _records_by(
                state, "recovery_records", "recovery_id"
            ).get(recovery_id)
            if not isinstance(recovery, dict):
                raise RuntimeError("managed recovery record disappeared")
            if recovery.get("status") != "pending":
                return
            recovery.update(
                {
                    "produced_record_ids": [],
                    "response": None,
                    "normalized_error": {
                        "code": error_code,
                        "http_status": http_status,
                    },
                    "evidence": {},
                    "status": "failed",
                    "completed_at": failed_at,
                }
            )
            terminal_status = self._terminal_status_when_quiescent(state)
            if terminal_status is not None:
                state["status"] = terminal_status

        self.store.mutate_runtime_state(
            project_id,
            sprint_id,
            fail,
            require_active=False,
        )

    def _complete_retry_handoff_recovery(
        self,
        project_id: str,
        sprint_id: str,
        *,
        context_id: str,
        recovery_id: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        """Retry every durable result-bound queue effect, then freeze a receipt."""

        snapshot = self.store.runtime_state(project_id, sprint_id)
        if snapshot is None:
            raise RuntimeError("managed sprint runtime disappeared")
        self._recovery_for_completion(
            snapshot, recovery_id, context_id, correlation_id
        )
        self.drain_outbox(project_id, sprint_id)
        completed_at = _timestamp(self.clock)

        def complete(
            state: dict[str, Any], _connection: sqlite3.Connection
        ) -> dict[str, Any]:
            recovery = self._recovery_for_completion(
                state,
                recovery_id,
                context_id,
                correlation_id,
                supersede_pending_at=completed_at,
            )
            context = _records_by(
                state, "coordinator_contexts", "context_id"
            ).get(context_id)
            if not isinstance(context, Mapping):
                raise RuntimeError("managed handoff recovery disappeared")
            if recovery.get("status") == "completed":
                return deepcopy(dict(recovery["response"]))
            parameters = recovery.get("parameters")
            assignment_id = context.get("failed_assignment_id")
            result_key = (
                parameters.get("result_key")
                if isinstance(parameters, Mapping)
                else None
            )
            receipt = (
                _records_by(state, "result_receipts", "result_key").get(
                    result_key
                )
                if isinstance(result_key, str)
                else None
            )
            journal = (
                _records_by(state, "transition_journal", "result_key").get(
                    result_key
                )
                if isinstance(result_key, str)
                else None
            )
            if (
                not isinstance(assignment_id, str)
                or not isinstance(receipt, Mapping)
                or receipt.get("assignment_id") != assignment_id
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            relevant_event_ids = [
                str(event["event_id"])
                for event in state["outbox"]
                if (
                    isinstance(event.get("payload"), Mapping)
                    and event["payload"].get("result_key") == result_key
                )
                or (
                    isinstance(journal, Mapping)
                    and event.get("event_id")
                    in journal.get("outbox_event_ids", [])
                )
            ]
            if not relevant_event_ids or any(
                _records_by(state, "outbox", "event_id")[event_id].get(
                    "status"
                )
                != "delivered"
                for event_id in relevant_event_ids
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            produced_ids = [result_key, *relevant_event_ids]
            response = {
                "recovery_id": recovery_id,
                "action": "RETRY_HANDOFF",
                "status": "RECOVERY_COMPLETED",
                "produced_record_ids": produced_ids,
                "deduplicated": False,
            }
            recovery.update(
                {
                    "produced_record_ids": produced_ids,
                    "response": deepcopy(response),
                    "normalized_error": None,
                    "evidence": {
                        "context_id": context_id,
                        "delivered_event_ids": relevant_event_ids,
                    },
                    "status": "completed",
                    "completed_at": completed_at,
                }
            )
            terminal_status = self._terminal_status_when_quiescent(state)
            if terminal_status is not None:
                state["status"] = terminal_status
            return response

        _state, response = self.store.mutate_runtime_state(
            project_id, sprint_id, complete, require_active=False
        )
        self._fault("after_recovery_commit", recovery_id=recovery_id)
        return ManagedContinuityResult(response)

    def _complete_continue_node_recovery(
        self,
        project_id: str,
        sprint_id: str,
        *,
        context_id: str,
        recovery_id: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        """Settle one failed assignment and continue its pinned occurrence."""

        state = self.store.runtime_state(project_id, sprint_id)
        if state is None:
            raise RuntimeError("managed sprint runtime disappeared")
        recovery = self._recovery_for_completion(
            state, recovery_id, context_id, correlation_id
        )
        self.drain_outbox(project_id, sprint_id)
        context = _records_by(state, "coordinator_contexts", "context_id").get(
            context_id
        )
        if not isinstance(recovery, Mapping) or not isinstance(context, Mapping):
            raise RuntimeError("managed continuation recovery disappeared")
        parameters = recovery.get("parameters")
        assignment_id = context.get("failed_assignment_id")
        assignment = (
            _records_by(state, "assignments", "assignment_id").get(assignment_id)
            if isinstance(assignment_id, str)
            else None
        )
        source_journals = [
            item
            for item in state.get("transition_journal", [])
            if isinstance(item, Mapping)
            and item.get("assignment_id") == assignment_id
        ]
        source_commit = (
            parameters.get("source_commit")
            if isinstance(parameters, Mapping)
            else None
        )
        if (
            recovery.get("status") != "pending"
            or not isinstance(assignment, Mapping)
            or assignment.get("status")
            not in {"active", "reviews_pending", "failed", "blocked"}
            or not isinstance(parameters, Mapping)
            or parameters.get("node_id") != assignment.get("node_id")
            or source_commit
            not in {assignment.get("source_commit"), assignment.get("result_commit")}
            or len(source_journals) > 1
        ):
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )
        source_journal = source_journals[0] if source_journals else None
        if source_journal is not None:
            approvals = [
                item
                for item in state.get("reviews", [])
                if isinstance(item, Mapping)
                and item.get("result_key") == source_journal.get("result_key")
                and item.get("decision") == "APPROVE"
            ]
            if (
                source_journal.get("state") != "REVIEWS_PENDING"
                or source_journal.get("disposition") != "open"
                or len(approvals) != 2
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
        elif assignment.get("result_commit") is not None:
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )

        prepared = self._prepare_assignment(
            project_id,
            state,
            occurrence_id=str(assignment["occurrence_id"]),
            node_id=str(assignment["node_id"]),
            graph_revision=int(assignment["graph_revision"]),
            source_kind=str(assignment["source_kind"]),
            source_result_keys=[
                str(value) for value in assignment.get("source_result_keys", [])
            ],
            source_commit=str(source_commit),
            rework_cycle=int(assignment.get("rework_cycle") or 0),
            integration_id=(
                str(assignment["integration_id"])
                if isinstance(assignment.get("integration_id"), str)
                else None
            ),
            identity_salt=recovery_id,
        )
        self._stop_assignment_processes(project_id, sprint_id, str(assignment_id))
        reservation_token: Any | None = None
        if prepared.port_leases:
            try:
                reservation_token = self.importer.port_reservations.acquire_many(
                    self.store.database_path, list(prepared.port_leases)
                )
            except ManagedPortReservationError as exc:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 409, correlation_id
                ) from exc
        completed_at = _timestamp(self.clock)
        try:

            def complete(
                candidate: dict[str, Any], connection: sqlite3.Connection
            ) -> dict[str, Any]:
                current_recovery = self._recovery_for_completion(
                    candidate,
                    recovery_id,
                    context_id,
                    correlation_id,
                    supersede_pending_at=completed_at,
                )
                current_context = _records_by(
                    candidate, "coordinator_contexts", "context_id"
                ).get(context_id)
                source = _records_by(
                    candidate, "assignments", "assignment_id"
                ).get(str(assignment_id))
                if current_recovery.get("status") == "completed":
                    return deepcopy(dict(current_recovery["response"]))
                if (
                    not isinstance(current_context, Mapping)
                    or not isinstance(source, dict)
                    or source.get("status")
                    not in {"active", "reviews_pending", "failed", "blocked"}
                    or current_recovery.get("parameters") != parameters
                ):
                    raise ManagedContinuityError(
                        RECOVERY_CONFLICT, 409, correlation_id
                    )
                current_journals = [
                    item
                    for item in candidate["transition_journal"]
                    if item.get("assignment_id") == assignment_id
                ]
                if len(current_journals) > 1:
                    raise ManagedContinuityError(
                        RECOVERY_CONFLICT, 409, correlation_id
                    )
                current_journal = (
                    current_journals[0] if current_journals else None
                )
                if current_journal is not None:
                    approvals = [
                        item
                        for item in candidate["reviews"]
                        if item.get("result_key")
                        == current_journal.get("result_key")
                        and item.get("decision") == "APPROVE"
                    ]
                    if (
                        current_journal.get("state") != "REVIEWS_PENDING"
                        or current_journal.get("disposition") != "open"
                        or len(approvals) != 2
                    ):
                        raise ManagedContinuityError(
                            RECOVERY_CONFLICT, 409, correlation_id
                        )
                prior_source_status = str(source["status"])
                occurrence = self._settle_source_assignment(
                    candidate,
                    connection,
                    source,
                    now=completed_at,
                    complete_occurrence=False,
                )
                if current_journal is not None:
                    current_journal.update(
                        {
                            "state": "REVIEWS_ACCEPTED",
                            "disposition": "accepted",
                            "updated_at": completed_at,
                        }
                    )
                else:
                    source["status"] = (
                        prior_source_status
                        if prior_source_status in {"failed", "blocked"}
                        else "failed"
                    )
                event_id = self._commit_prepared_assignment(
                    candidate,
                    connection,
                    prepared,
                    occurrence=occurrence,
                    now=completed_at,
                )
                candidate["status"] = "active"
                produced_ids = [
                    str(prepared.assignment["assignment_id"]),
                    event_id,
                ]
                response = {
                    "recovery_id": recovery_id,
                    "action": "CONTINUE_NODE",
                    "status": "RECOVERY_COMPLETED",
                    "produced_record_ids": produced_ids,
                    "deduplicated": False,
                }
                current_recovery.update(
                    {
                        "produced_record_ids": produced_ids,
                        "response": deepcopy(response),
                        "normalized_error": None,
                        "evidence": {
                            "context_id": context_id,
                            "source_assignment_id": str(assignment_id),
                        },
                        "status": "completed",
                        "completed_at": completed_at,
                    }
                )
                return response

            _state, response = self.store.mutate_runtime_state(
                project_id, sprint_id, complete
            )
        except BaseException:
            if reservation_token is not None:
                self.importer.port_reservations.rollback(reservation_token)
            raise
        self._fault("after_recovery_commit", recovery_id=recovery_id)
        if reservation_token is not None:
            try:
                self.importer.port_reservations.mark_durable_many(
                    self.store.database_path, list(prepared.port_leases)
                )
            except Exception:
                pass
        try:
            self._publish_prepared_assignment(state, prepared)
        except Exception:
            pass
        self._resume_after_durable_ack(project_id, sprint_id, reconcile=True)
        return ManagedContinuityResult(response)

    def _complete_route_rework_recovery(
        self,
        project_id: str,
        sprint_id: str,
        *,
        context_id: str,
        recovery_id: str,
        coordinator_id: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        """Publish the deterministic replacement for an already rejected result."""

        state = self.store.runtime_state(project_id, sprint_id)
        if state is None:
            raise RuntimeError("managed sprint runtime disappeared")
        recovery = self._recovery_for_completion(
            state, recovery_id, context_id, correlation_id
        )
        self.drain_outbox(project_id, sprint_id)
        context = _records_by(state, "coordinator_contexts", "context_id").get(
            context_id
        )
        if not isinstance(recovery, Mapping) or not isinstance(context, Mapping):
            raise RuntimeError("managed rework recovery disappeared")
        parameters = recovery.get("parameters")
        assignment_id = context.get("failed_assignment_id")
        result_key = (
            parameters.get("rejected_result_key")
            if isinstance(parameters, Mapping)
            else None
        )
        feedback = (
            parameters.get("feedback") if isinstance(parameters, Mapping) else None
        )
        assignment = (
            _records_by(state, "assignments", "assignment_id").get(assignment_id)
            if isinstance(assignment_id, str)
            else None
        )
        receipt = (
            _records_by(state, "result_receipts", "result_key").get(result_key)
            if isinstance(result_key, str)
            else None
        )
        journal = (
            _records_by(state, "transition_journal", "result_key").get(result_key)
            if isinstance(result_key, str)
            else None
        )
        rejecting_review = next(
            (
                item
                for item in state.get("reviews", [])
                if isinstance(item, Mapping)
                and item.get("result_key") == result_key
                and item.get("decision") == "REJECT"
                and item.get("feedback") == feedback
            ),
            None,
        )
        if (
            not isinstance(assignment, Mapping)
            or not isinstance(receipt, Mapping)
            or receipt.get("assignment_id") != assignment_id
            or not isinstance(journal, Mapping)
            or journal.get("state") != "REVIEWS_PENDING"
            or journal.get("disposition") != "open"
            or not isinstance(rejecting_review, Mapping)
        ):
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )
        definition = _graph_revision(state, int(assignment["graph_revision"]))[
            "definition"
        ]
        execution = definition.get("execution")
        maximum = (
            execution.get("max_rework_cycles")
            if isinstance(execution, Mapping)
            else None
        )
        next_cycle = int(assignment.get("rework_cycle") or 0) + 1
        if not isinstance(maximum, int) or next_cycle > maximum:
            self._block_rework_limit_recovery(
                project_id,
                sprint_id,
                assignment_id=str(assignment_id),
                result_key=str(result_key),
                recovery_id=recovery_id,
                maximum=maximum if isinstance(maximum, int) else -1,
                next_cycle=next_cycle,
                correlation_id=correlation_id,
            )
            raise AssertionError("rework-limit recovery must terminate")
        prepared = self._prepare_assignment(
            project_id,
            state,
            occurrence_id=str(assignment["occurrence_id"]),
            node_id=str(assignment["node_id"]),
            graph_revision=int(assignment["graph_revision"]),
            source_kind="rework_result",
            source_result_keys=[str(result_key)],
            source_commit=str(receipt["result_commit"]),
            rework_cycle=next_cycle,
        )
        self._stop_assignment_processes(
            project_id, sprint_id, str(assignment_id)
        )
        reservation_token: Any | None = None
        if prepared.port_leases:
            try:
                reservation_token = self.importer.port_reservations.acquire_many(
                    self.store.database_path, list(prepared.port_leases)
                )
            except ManagedPortReservationError as exc:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 409, correlation_id
                ) from exc
        completed_at = _timestamp(self.clock)
        rework_id = _stable_id("rework", result_key, recovery_id)
        try:

            def complete(
                candidate: dict[str, Any], connection: sqlite3.Connection
            ) -> dict[str, Any]:
                current_recovery = self._recovery_for_completion(
                    candidate,
                    recovery_id,
                    context_id,
                    correlation_id,
                    supersede_pending_at=completed_at,
                )
                source = _records_by(
                    candidate, "assignments", "assignment_id"
                ).get(str(assignment_id))
                current_journal = _records_by(
                    candidate, "transition_journal", "result_key"
                ).get(str(result_key))
                if current_recovery.get("status") == "completed":
                    return deepcopy(dict(current_recovery["response"]))
                if (
                    not isinstance(source, dict)
                    or not isinstance(current_journal, dict)
                    or current_journal.get("state") != "REVIEWS_PENDING"
                    or current_journal.get("disposition") != "open"
                ):
                    raise ManagedContinuityError(
                        RECOVERY_CONFLICT, 409, correlation_id
                    )
                occurrence = self._settle_source_assignment(
                    candidate,
                    connection,
                    source,
                    now=completed_at,
                    complete_occurrence=False,
                )
                event_id = self._commit_prepared_assignment(
                    candidate,
                    connection,
                    prepared,
                    occurrence=occurrence,
                    now=completed_at,
                )
                candidate["reworks"].append(
                    {
                        "rework_id": rework_id,
                        "rejected_result_key": str(result_key),
                        "reviewer_id": str(rejecting_review["reviewer_id"]),
                        "feedback": str(feedback),
                        "rework_cycle": next_cycle,
                        "new_assignment_id": prepared.assignment["assignment_id"],
                        "committed_at": completed_at,
                    }
                )
                current_journal.update(
                    {
                        "disposition": "reworked",
                        "rework_id": rework_id,
                        "updated_at": completed_at,
                    }
                )
                produced_ids = [
                    rework_id,
                    prepared.assignment["assignment_id"],
                    event_id,
                ]
                response = {
                    "recovery_id": recovery_id,
                    "action": "ROUTE_REWORK",
                    "status": "RECOVERY_COMPLETED",
                    "produced_record_ids": produced_ids,
                    "deduplicated": False,
                }
                current_recovery.update(
                    {
                        "produced_record_ids": produced_ids,
                        "response": deepcopy(response),
                        "normalized_error": None,
                        "evidence": {
                            "context_id": context_id,
                            "coordinator_id": coordinator_id,
                        },
                        "status": "completed",
                        "completed_at": completed_at,
                    }
                )
                return response

            _state, response = self.store.mutate_runtime_state(
                project_id, sprint_id, complete
            )
        except BaseException:
            if reservation_token is not None:
                self.importer.port_reservations.rollback(reservation_token)
            raise
        self._fault("after_recovery_commit", recovery_id=recovery_id)
        if reservation_token is not None:
            try:
                self.importer.port_reservations.mark_durable_many(
                    self.store.database_path, list(prepared.port_leases)
                )
            except Exception:
                pass
        try:
            self._publish_prepared_assignment(state, prepared)
        except Exception:
            pass
        self._resume_after_durable_ack(project_id, sprint_id, reconcile=True)
        return ManagedContinuityResult(response)

    def _complete_block_external_recovery(
        self,
        project_id: str,
        sprint_id: str,
        *,
        context_id: str,
        recovery_id: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        state = self.store.runtime_state(project_id, sprint_id)
        if state is None:
            raise RuntimeError("managed sprint runtime disappeared")
        self._recovery_for_completion(
            state, recovery_id, context_id, correlation_id
        )
        self.drain_outbox(project_id, sprint_id)
        context = _records_by(state, "coordinator_contexts", "context_id").get(
            context_id
        )
        if not isinstance(context, Mapping):
            raise RuntimeError("managed Coordinator context disappeared")
        import_attempt_id = context.get("import_attempt_id")
        assignment_id = context.get("failed_assignment_id")
        if (
            isinstance(import_attempt_id, str)
            and assignment_id is None
            and context.get("join_target") is None
        ):
            return self._complete_import_block_external_recovery(
                project_id,
                sprint_id,
                context_id=context_id,
                recovery_id=recovery_id,
                import_attempt_id=import_attempt_id,
                correlation_id=correlation_id,
            )
        if not isinstance(assignment_id, str) or context.get("join_target") is not None:
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST, 400, correlation_id
            )
        assignment_snapshot = _records_by(
            state, "assignments", "assignment_id"
        ).get(assignment_id)
        result_key_snapshot = next(
            (
                str(item["result_key"])
                for item in state["transition_journal"]
                if item.get("assignment_id") == assignment_id
                and item.get("state") == "REVIEWS_PENDING"
                and item.get("disposition") == "open"
            ),
            None,
        )
        approvals_snapshot = [
            item
            for item in state["reviews"]
            if item.get("result_key") == result_key_snapshot
            and item.get("decision") == "APPROVE"
        ]
        if (
            not isinstance(assignment_snapshot, Mapping)
            or result_key_snapshot is None
            or len(approvals_snapshot) != 2
            or self._terminal_failure_target(state, assignment_snapshot) is None
        ):
            # Process settlement is destructive relative to a live
            # assignment.  Prove this context represents an already reviewed
            # FAILED/BLOCKED terminal result before stopping anything; the
            # transaction below repeats the same check as the final CAS.
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation_id
            )
        self._stop_assignment_processes(project_id, sprint_id, assignment_id)
        completed_at = _timestamp(self.clock)

        def complete(
            candidate: dict[str, Any], connection: sqlite3.Connection
        ) -> dict[str, Any]:
            recovery = self._recovery_for_completion(
                candidate,
                recovery_id,
                context_id,
                correlation_id,
                supersede_pending_at=completed_at,
            )
            if recovery.get("status") == "completed":
                return deepcopy(dict(recovery["response"]))
            current_context = _records_by(
                candidate, "coordinator_contexts", "context_id"
            ).get(context_id)
            assignment = _records_by(
                candidate, "assignments", "assignment_id"
            ).get(assignment_id)
            if not isinstance(current_context, Mapping) or not isinstance(
                assignment, dict
            ):
                raise RuntimeError("managed recovery target disappeared")
            result_key = next(
                (
                    str(item["result_key"])
                    for item in candidate["transition_journal"]
                    if item.get("assignment_id") == assignment_id
                    and item.get("state") == "REVIEWS_PENDING"
                    and item.get("disposition") == "open"
                ),
                None,
            )
            if result_key is None:
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            journal = _records_by(
                candidate, "transition_journal", "result_key"
            )[result_key]
            approvals = [
                item
                for item in candidate["reviews"]
                if item.get("result_key") == result_key
                and item.get("decision") == "APPROVE"
            ]
            terminal_failure = self._terminal_failure_target(candidate, assignment)
            if len(approvals) != 2 or terminal_failure is None:
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            self._settle_source_assignment(
                candidate,
                connection,
                assignment,
                now=completed_at,
                complete_occurrence=True,
            )
            self._append_transition_token(
                candidate, assignment, journal, now=completed_at
            )
            normalized_error = current_context.get("normalized_error")
            reason_code = str(current_context["reason_code"])
            if not isinstance(normalized_error, Mapping):
                raise RuntimeError("managed recovery evidence is corrupt")
            blocker_fingerprint = managed_blocker_fingerprint(
                assignment_id, reason_code, normalized_error
            )
            observation = _records_by(
                candidate, "blocker_observations", "fingerprint"
            ).get(blocker_fingerprint)
            if observation is None:
                candidate["blocker_observations"].append(
                    {
                        "fingerprint": blocker_fingerprint,
                        "assignment_id": assignment_id,
                        "reason_code": reason_code,
                        "normalized_error": deepcopy(dict(normalized_error)),
                        "count": 1,
                        "first_seen_at": completed_at,
                        "last_seen_at": completed_at,
                    }
                )
            elif (
                observation.get("assignment_id") != assignment_id
                or not (
                    (
                        observation.get("reason_code") is None
                        and observation.get("normalized_error") is None
                    )
                    or (
                        observation.get("reason_code") == reason_code
                        and observation.get("normalized_error")
                        == normalized_error
                    )
                )
            ):
                raise RuntimeError("managed blocker evidence changed")
            produced_ids = [blocker_fingerprint]
            response = {
                "recovery_id": recovery_id,
                "action": "BLOCK_EXTERNAL",
                "status": "RECOVERY_COMPLETED",
                "produced_record_ids": produced_ids,
                "deduplicated": False,
            }
            recovery.update(
                {
                    "produced_record_ids": produced_ids,
                    "response": deepcopy(response),
                    "normalized_error": None,
                    "evidence": {
                        "context_id": context_id,
                        "terminal_status": terminal_failure[1],
                    },
                    "status": "completed",
                    "completed_at": completed_at,
                }
            )
            terminal_status = self._terminal_status_when_quiescent(candidate)
            if terminal_status is not None:
                candidate["status"] = terminal_status
            return response

        _state, response = self.store.mutate_runtime_state(
            project_id, sprint_id, complete
        )
        self._fault("after_recovery_commit", recovery_id=recovery_id)
        return ManagedContinuityResult(response)

    def _complete_import_block_external_recovery(
        self,
        project_id: str,
        sprint_id: str,
        *,
        context_id: str,
        recovery_id: str,
        import_attempt_id: str,
        correlation_id: str,
    ) -> ManagedContinuityResult:
        """Accept an operator-owned terminal outcome for a failed import."""

        completed_at = _timestamp(self.clock)

        def complete(
            state: dict[str, Any], _connection: sqlite3.Connection
        ) -> dict[str, Any]:
            recovery = self._recovery_for_completion(
                state,
                recovery_id,
                context_id,
                correlation_id,
                supersede_pending_at=completed_at,
            )
            if recovery.get("status") == "completed":
                response = recovery.get("response")
                if not isinstance(response, Mapping):
                    raise RuntimeError("managed recovery response is corrupt")
                return deepcopy(dict(response))
            context = _records_by(
                state, "coordinator_contexts", "context_id"
            ).get(context_id)
            attempt = _records_by(
                state, "import_attempts", "attempt_id"
            ).get(import_attempt_id)
            if (
                not isinstance(context, Mapping)
                or context.get("import_attempt_id") != import_attempt_id
                or context.get("failed_assignment_id") is not None
                or context.get("join_target") is not None
                or not isinstance(attempt, Mapping)
                or attempt.get("status") != "failed"
                or state.get("status") != "failed"
                or state.get("workflow") is not None
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation_id
                )
            response = {
                "recovery_id": recovery_id,
                "action": "BLOCK_EXTERNAL",
                "status": "RECOVERY_COMPLETED",
                "produced_record_ids": [],
                "deduplicated": False,
            }
            recovery.update(
                {
                    "produced_record_ids": [],
                    "response": deepcopy(response),
                    "normalized_error": None,
                    "evidence": {
                        "context_id": context_id,
                        "terminal_status": "failed",
                    },
                    "status": "completed",
                    "completed_at": completed_at,
                }
            )
            return response

        _state, response = self.store.mutate_runtime_state(
            project_id,
            sprint_id,
            complete,
            require_active=False,
        )
        self._fault("after_recovery_commit", recovery_id=recovery_id)
        return ManagedContinuityResult(response)

    def _submit_review(
        self,
        binding: _AssignmentBinding,
        payload: Mapping[str, Any],
        correlation_id: str,
        supplied_role_token: str = "",
    ) -> ManagedContinuityResult:
        with self._sprint_lock(binding.sprint_id):
            latest = self.store.runtime_state(binding.project_id, binding.sprint_id)
            if latest is None:
                raise RuntimeError("managed sprint runtime disappeared")
            current = _records_by(
                latest, "review_assignments", "assignment_id"
            ).get(str(binding.record.get("assignment_id")))
            if not isinstance(current, dict):
                raise RuntimeError("managed review assignment disappeared")
            refreshed = _AssignmentBinding(
                binding.project_id,
                binding.sprint_id,
                latest,
                binding.kind,
                current,
            )
            self._require_scope(latest, str(current["assignment_id"]), payload, supplied_role_token, correlation_id)
            return self._submit_review_locked(refreshed, payload, correlation_id)

    def _submit_review_locked(
        self,
        binding: _AssignmentBinding,
        payload: Mapping[str, Any],
        correlation_id: str,
    ) -> ManagedContinuityResult:
        self._validate_schema(payload, _REVIEW_SCHEMA, correlation_id)
        source_assignment_id = str(binding.record["source_assignment_id"])
        if binding.record.get("status") != "decided":
            source_snapshot = _records_by(
                binding.state, "assignments", "assignment_id"
            ).get(source_assignment_id)
            if not isinstance(source_snapshot, Mapping):
                raise RuntimeError("managed review source disappeared")
            if source_snapshot.get("status") != "reviews_pending":
                raise ManagedContinuityError(
                    REVIEW_CONFLICT, 409, correlation_id
                )
        # A direct retry may arrive after the durable review assignment but
        # before its outbox record was marked delivered.  Close that crash gap
        # before a first decision, whose terminal transition admits no pending
        # work.  A decided review is instead an immutable replay handle and
        # cannot make its acknowledgement depend on queue availability.
        if binding.record.get("status") != "decided":
            self.drain_outbox(binding.project_id, binding.sprint_id)
        decision = str(payload["status"])
        feedback = str(payload.get("feedback") or "").strip()
        if decision == "REJECT" and not feedback:
            raise ManagedContinuityError(
                INVALID_MANAGED_SPRINT_REQUEST,
                400,
                correlation_id,
                field="feedback",
            )
        review_assignment_id = str(binding.record["assignment_id"])
        request_fingerprint = review_request_fingerprint(
            review_assignment_id, decision, feedback
        )
        if binding.record.get("status") == "decided":
            if binding.record.get("request_fingerprint") == request_fingerprint:
                self._resume_after_durable_ack(
                    binding.project_id,
                    binding.sprint_id,
                    reconcile=True,
                )
                return self._review_replay(binding.record)
            raise ManagedContinuityError(REVIEW_CONFLICT, 409, correlation_id)
        if (
            binding.state.get("status") != "active"
            or binding.record.get("status") != "active"
        ):
            raise ManagedContinuityError(REVIEW_CONFLICT, 409, correlation_id)

        result_key = str(binding.record["result_key"])
        journals = _records_by(
            binding.state, "transition_journal", "result_key"
        )
        journal_snapshot = journals.get(result_key)
        if (
            not isinstance(journal_snapshot, Mapping)
            or journal_snapshot.get("disposition") != "open"
            or journal_snapshot.get("state") != "REVIEWS_PENDING"
        ):
            raise ManagedContinuityError(REVIEW_CONFLICT, 409, correlation_id)

        decided = [
            item
            for item in binding.state.get("reviews", [])
            if isinstance(item, Mapping) and item.get("result_key") == result_key
        ]
        approving_after = decision == "APPROVE" and (
            sum(item.get("decision") == "APPROVE" for item in decided) + 1 == 2
        )
        now = _timestamp(self.clock)
        join_plan: _JoinPlan | None = None
        join_preparation_failure_code: str | None = None
        join_preparation_failure_phase: str | None = None
        source_snapshot = _records_by(
            binding.state, "assignments", "assignment_id"
        ).get(source_assignment_id)
        if not isinstance(source_snapshot, Mapping):
            raise RuntimeError("managed review source disappeared")
        terminal_failure = (
            self._terminal_failure_target(binding.state, source_snapshot)
            if approving_after
            else None
        )
        if approving_after and terminal_failure is None:
            try:
                join_plan = self._prospective_join_plan(
                    binding.project_id,
                    binding.state,
                    source_assignment_id,
                    result_key,
                    now=now,
                )
            except (
                ManagedContinuityError,
                WorkspaceError,
                ManagedGitError,
                BranchLeaseError,
                OSError,
                RuntimeError,
            ) as exc:
                # A ready all-parent join must never exist without either its
                # prepared effect or durable Coordinator evidence.  Retain the
                # stable failure code so the review transaction can append the
                # token and its failure witness atomically.
                join_preparation_failure_code = str(
                    getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                )
                join_plan = None
        prepared_rework: _PreparedAssignment | None = None
        if decision == "REJECT":
            source = _records_by(
                binding.state, "assignments", "assignment_id"
            ).get(source_assignment_id)
            receipt = _records_by(
                binding.state, "result_receipts", "result_key"
            ).get(result_key)
            if not isinstance(source, dict) or not isinstance(receipt, dict):
                raise RuntimeError("managed rejected result binding is corrupt")
            definition = _graph_revision(
                binding.state, int(source["graph_revision"])
            )["definition"]
            execution = definition.get("execution")
            maximum = (
                execution.get("max_rework_cycles")
                if isinstance(execution, Mapping)
                else None
            )
            next_cycle = int(source.get("rework_cycle") or 0) + 1
            if not isinstance(maximum, int) or next_cycle > maximum:
                return self._block_rework_limit_review(
                    binding,
                    review_assignment_id=review_assignment_id,
                    source_assignment_id=source_assignment_id,
                    result_key=result_key,
                    feedback=feedback,
                    request_fingerprint=request_fingerprint,
                    maximum=maximum if isinstance(maximum, int) else -1,
                    next_cycle=next_cycle,
                    now=now,
                    correlation_id=correlation_id,
                )
            try:
                prepared_rework = self._prepare_assignment(
                    binding.project_id,
                    binding.state,
                    occurrence_id=str(source["occurrence_id"]),
                    node_id=str(source["node_id"]),
                    graph_revision=int(source["graph_revision"]),
                    source_kind="rework_result",
                    source_result_keys=[result_key],
                    source_commit=str(receipt["result_commit"]),
                    rework_cycle=next_cycle,
                )
            except (ManagedContinuityError, WorkspaceError, ManagedGitError) as exc:
                failure_code = (
                    exc.code
                    if isinstance(exc, ManagedContinuityError)
                    else getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                )
                return self._stage_rework_routing_failure(
                    binding,
                    review_assignment_id=review_assignment_id,
                    source_assignment_id=source_assignment_id,
                    result_key=result_key,
                    feedback=feedback,
                    request_fingerprint=request_fingerprint,
                    failure_code=str(failure_code),
                    now=now,
                    correlation_id=correlation_id,
                )

        if (
            approving_after and terminal_failure is None
        ) or decision == "REJECT":
            try:
                self._stop_assignment_processes(
                    binding.project_id, binding.sprint_id, source_assignment_id
                )
            except ManagedContinuityError as exc:
                if decision == "REJECT":
                    return self._stage_rework_routing_failure(
                        binding,
                        review_assignment_id=review_assignment_id,
                        source_assignment_id=source_assignment_id,
                        result_key=result_key,
                        feedback=feedback,
                        request_fingerprint=request_fingerprint,
                        failure_code=exc.code,
                        now=now,
                        correlation_id=correlation_id,
                    )
                if exc.correlation_id == "process-settlement":
                    raise ManagedContinuityError(
                        exc.code, exc.http_status, correlation_id
                    ) from exc
                raise

        prepared_successor = (
            prepared_rework
            if prepared_rework is not None
            else join_plan.prepared_assignment
            if join_plan is not None
            else None
        )
        reservation_token: Any | None = None
        if prepared_successor is not None and prepared_successor.port_leases:
            try:
                reservation_token = self.importer.port_reservations.acquire_many(
                    self.store.database_path,
                    list(prepared_successor.port_leases),
                )
            except ManagedPortReservationError as exc:
                if decision == "REJECT":
                    return self._stage_rework_routing_failure(
                        binding,
                        review_assignment_id=review_assignment_id,
                        source_assignment_id=source_assignment_id,
                        result_key=result_key,
                        feedback=feedback,
                        request_fingerprint=request_fingerprint,
                        failure_code=CONTINUITY_RUNTIME_FAILED,
                        now=now,
                        correlation_id=correlation_id,
                    )
                # As with prospective Git/workspace preparation, an approved
                # join must commit its failure witness with the review/token.
                join_preparation_failure_code = str(
                    getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                )
                join_preparation_failure_phase = "PORT_RESERVATION"
                join_plan = None
                prepared_successor = None

        try:

            def mutate(state: dict[str, Any], connection: sqlite3.Connection) -> dict[str, Any]:
                review_assignments = _records_by(
                    state, "review_assignments", "assignment_id"
                )
                current = review_assignments.get(review_assignment_id)
                if not isinstance(current, dict):
                    raise RuntimeError("managed review assignment disappeared")
                if current.get("status") == "decided":
                    if current.get("request_fingerprint") == request_fingerprint:
                        replay = deepcopy(dict(current["response"]))
                        replay.update(
                            {"status": "ALREADY_ACCEPTED", "deduplicated": True}
                        )
                        return replay
                    raise ManagedContinuityError(
                        REVIEW_CONFLICT, 409, correlation_id
                    )
                if current.get("status") != "active":
                    raise ManagedContinuityError(
                        REVIEW_CONFLICT, 409, correlation_id
                    )
                journal = _records_by(
                    state, "transition_journal", "result_key"
                ).get(result_key)
                if (
                    not isinstance(journal, dict)
                    or journal.get("state") != "REVIEWS_PENDING"
                    or journal.get("disposition") != "open"
                ):
                    raise ManagedContinuityError(
                        REVIEW_CONFLICT, 409, correlation_id
                    )
                source = _records_by(
                    state, "assignments", "assignment_id"
                ).get(source_assignment_id)
                if not isinstance(source, dict):
                    raise RuntimeError("managed review source disappeared")
                if source.get("status") != "reviews_pending":
                    raise ManagedContinuityError(
                        REVIEW_CONFLICT, 409, correlation_id
                    )
                response = {
                    "assignment_id": review_assignment_id,
                    "source_assignment_id": source_assignment_id,
                    "result_key": result_key,
                    "decision": decision,
                    "status": (
                        "REVIEW_ACCEPTED"
                        if decision == "APPROVE"
                        else "REWORK_ENQUEUED"
                    ),
                    "deduplicated": False,
                }
                current.update(
                    {
                        "status": "decided",
                        "decision": decision,
                        "request_fingerprint": request_fingerprint,
                        "response": deepcopy(response),
                        "decided_at": now,
                    }
                )
                state["reviews"].append(
                    {
                        "assignment_id": review_assignment_id,
                        "source_assignment_id": source_assignment_id,
                        "result_key": result_key,
                        "result_commit": current["result_commit"],
                        "result_outcome": current["result_outcome"],
                        "reviewer_id": current["reviewer_id"],
                        "reviewer_index": current["reviewer_index"],
                        "decision": decision,
                        "feedback": feedback,
                        "request_fingerprint": request_fingerprint,
                        "response": deepcopy(response),
                        "decided_at": now,
                    }
                )

                result_reviews = [
                    item
                    for item in state["reviews"]
                    if item.get("result_key") == result_key
                ]
                if decision == "APPROVE" and sum(
                    item.get("decision") == "APPROVE" for item in result_reviews
                ) == 2:
                    if terminal_failure is not None:
                        target_node_id, terminal_status = terminal_failure
                        reason_code = (
                            "ASSIGNMENT_FAILED"
                            if terminal_status == "FAILED"
                            else "BLOCKED_EXTERNAL"
                        )
                        self._append_assignment_failure_context(
                            state,
                            connection,
                            source,
                            result_key=result_key,
                            reason_code=reason_code,
                            normalized_error={
                                "code": reason_code,
                                "target_node_id": target_node_id,
                                "terminal_status": terminal_status,
                            },
                            now=now,
                        )
                    else:
                        self._settle_source_assignment(
                            state,
                            connection,
                            source,
                            now=now,
                            complete_occurrence=True,
                        )
                        transition_token = self._append_transition_token(
                            state, source, journal, now=now
                        )
                        if join_plan is not None:
                            self._commit_join_plan(
                                state, connection, join_plan, now=now
                            )
                        elif join_preparation_failure_code is not None:
                            target_node_id = str(
                                transition_token["target_node_id"]
                            )
                            selected_tokens = self._ready_tokens(
                                state, target_node_id
                            )
                            trigger_token_ids = [
                                str(item["token_id"])
                                for item in selected_tokens
                            ]
                            if (
                                len(selected_tokens) < 2
                                or transition_token["token_id"]
                                not in trigger_token_ids
                            ):
                                raise RuntimeError(
                                    "managed failed join selection changed"
                                )
                            selected_result_keys = [
                                str(item["result_key"])
                                for item in selected_tokens
                            ]
                            selected_receipts = _records_by(
                                state, "result_receipts", "result_key"
                            )
                            selected_commits = [
                                str(selected_receipts[key]["result_commit"])
                                for key in selected_result_keys
                            ]
                            failure_phase = join_preparation_failure_phase
                            if failure_phase is None:
                                failure_phase = (
                                    "ASSIGNMENT_PREPARE"
                                    if len(set(selected_commits)) == 1
                                    else "INTEGRATION_PREPARE"
                                )
                            self._append_successor_preparation_failure_in_state(
                                state,
                                connection,
                                target_node_id=target_node_id,
                                trigger_token_ids=trigger_token_ids,
                                result_keys=selected_result_keys,
                                phase=failure_phase,
                                failure_code=join_preparation_failure_code,
                                now=now,
                            )
                elif decision == "REJECT":
                    if prepared_rework is None:
                        raise RuntimeError("managed rework preparation is missing")
                    occurrence = self._settle_source_assignment(
                        state,
                        connection,
                        source,
                        now=now,
                        complete_occurrence=False,
                    )
                    self._commit_prepared_assignment(
                        state,
                        connection,
                        prepared_rework,
                        occurrence=occurrence,
                        now=now,
                    )
                    rework_id = _stable_id(
                        "rework", result_key, review_assignment_id
                    )
                    state["reworks"].append(
                        {
                            "rework_id": rework_id,
                            "rejected_result_key": result_key,
                            "reviewer_id": current["reviewer_id"],
                            "feedback": feedback,
                            "rework_cycle": prepared_rework.assignment["rework_cycle"],
                            "new_assignment_id": prepared_rework.assignment[
                                "assignment_id"
                            ],
                            "committed_at": now,
                        }
                    )
                    journal.update(
                        {
                            "disposition": "reworked",
                            "rework_id": rework_id,
                            "updated_at": now,
                        }
                    )
                return response

            _state, response = self.store.mutate_runtime_state(
                binding.project_id, binding.sprint_id, mutate
            )
        except Exception as exc:
            if reservation_token is not None:
                self.importer.port_reservations.rollback(reservation_token)
            if decision == "REJECT":
                failure_code = (
                    exc.code
                    if isinstance(exc, ManagedContinuityError)
                    else getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                )
                return self._stage_rework_routing_failure(
                    binding,
                    review_assignment_id=review_assignment_id,
                    source_assignment_id=source_assignment_id,
                    result_key=result_key,
                    feedback=feedback,
                    request_fingerprint=request_fingerprint,
                    failure_code=str(failure_code),
                    now=now,
                    correlation_id=correlation_id,
                )
            raise
        self._fault(
            "after_review_commit",
            review_assignment_id=review_assignment_id,
            decision=decision,
        )
        if reservation_token is not None:
            try:
                self.importer.port_reservations.mark_durable_many(
                    self.store.database_path,
                    list(
                        prepared_successor.port_leases
                        if prepared_successor
                        else ()
                    ),
                )
            except Exception:
                pass
        if prepared_successor is not None:
            try:
                self._publish_prepared_assignment(
                    binding.state, prepared_successor
                )
            except Exception:
                pass
        self._resume_after_durable_ack(
            binding.project_id,
            binding.sprint_id,
            reconcile=True,
        )
        return ManagedContinuityResult(response)

    def _prepare_assignment(
        self,
        project_id: str,
        state: Mapping[str, Any],
        *,
        occurrence_id: str,
        node_id: str,
        graph_revision: int,
        source_kind: str,
        source_result_keys: Sequence[str],
        source_commit: str,
        rework_cycle: int = 0,
        integration_id: str | None = None,
        identity_salt: str | None = None,
    ) -> _PreparedAssignment:
        sprint_id = str(state["sprint_id"])
        definition = _graph_revision(state, graph_revision)["definition"]
        node = _node(state, graph_revision, node_id)
        workspace_definition = node.get("workspace")
        if not isinstance(workspace_definition, Mapping):
            raise RuntimeError("managed assignment workspace definition is corrupt")
        identity_parts: list[object] = [
            sprint_id,
            occurrence_id,
            node_id,
            graph_revision,
            source_kind,
            rework_cycle,
            ",".join(source_result_keys),
        ]
        if identity_salt is not None:
            identity_parts.append(identity_salt)
        assignment_id = _stable_id(
            "assignment", *identity_parts, compact=True
        )
        workspace_id = _stable_id(
            "workspace", project_id, sprint_id, node_id, assignment_id
        )
        access = str(workspace_definition.get("access") or "")
        if access not in {"read", "write"}:
            raise RuntimeError("managed assignment workspace access is invalid")
        git_policy = workspace_definition.get("git")
        if not isinstance(git_policy, Mapping):
            git_policy = definition.get("git")
        if access == "write" and not isinstance(git_policy, Mapping):
            raise RuntimeError("managed write assignment Git policy is missing")
        assigned_branch = (
            str(git_policy["assigned_branch"]) if access == "write" else None
        )
        # A continuation/rework owns the same frozen logical branch but starts
        # from an immutable accepted commit.  Resume is the non-destructive
        # policy that permits that continuation while still requiring ancestry.
        workspace_request = WorkspaceRequest(
            project_id=project_id,
            sprint_id=sprint_id,
            node_id=node_id,
            assignment_id=assignment_id,
            repository_id=str(state["repository"]["repository_id"]),
            source_commit=source_commit,
            access="write" if access == "write" else "read",
            assigned_branch=assigned_branch,
            existing_branch_policy="resume" if access == "write" else None,
        )
        provider = self.importer.provider_for_durable_repository(state["repository"])
        repository = provider.ensure_mirror(
            str(state["repository"]["repository_id"]), fetch=False
        )
        provider.assert_commit(repository, source_commit)
        verified = self.importer.workspace_manager.prepare(
            workspace_request,
            repository=repository,
            publish_write_lease=access != "write",
            selected_initial_head=source_commit if access == "write" else None,
        )
        workspace = managed_workspace_record(verified)
        now = _timestamp(self.clock)
        branch_request: BranchLeaseRequest | None = None
        branch_record: dict[str, Any] | None = None
        if access == "write":
            assert assigned_branch is not None
            self.importer.branch_leases.ensure_initialized()
            lease_id = deterministic_branch_lease_id(
                repository.mirror_storage_key, assigned_branch, assignment_id
            )
            branch_request = BranchLeaseRequest(
                lease_id=lease_id,
                repository_id=repository.repository_id,
                repository_key=repository.canonical_remote,
                mirror_storage_key=repository.mirror_storage_key,
                branch=assigned_branch,
                assignment_id=assignment_id,
                source_commit=source_commit,
                initial_head_commit=source_commit,
            )
            branch_record = {
                "lease_id": lease_id,
                "repository_id": repository.repository_id,
                "repository_key": repository.canonical_remote,
                "mirror_storage_key": repository.mirror_storage_key,
                "branch": assigned_branch,
                "assignment_id": assignment_id,
                "source_commit": source_commit,
                "initial_head_commit": source_commit,
                "mode": "write",
                "status": "active",
                "acquired_at": now,
                "released_at": None,
            }

        process_plan, process_issues = self.importer._process_plan(
            definition,
            node,
            sprint_id=sprint_id,
            assignment_id=assignment_id,
            workspace_id=workspace_id,
        )
        if process_issues:
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED,
                409,
                "assignment-preparation",
                issues=process_issues,
            )
        plan: dict[str, Any] = {
            "node_id": node_id,
            "assignment_id": assignment_id,
            "workspace_id": workspace_id,
        }
        if process_plan is not None:
            occupied = {
                int(item["port"])
                for item in self.store.resource_snapshot().get("port_leases", [])
                if isinstance(item, Mapping)
                and item.get("status") in {"reserved", "bound"}
                and isinstance(item.get("port"), int)
            }
            selected_port = next(
                (
                    port
                    for port in range(
                        int(self.importer.runtime_config["child_port_start"]),
                        int(self.importer.runtime_config["child_port_end"]) + 1,
                    )
                    if port not in occupied
                    and self.importer.port_probe("127.0.0.1", port)
                ),
                None,
            )
            if selected_port is None:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 409, "assignment-preparation"
                )
            plan.update(
                {"process_plan": process_plan, "planned_port": selected_port}
            )
        port_leases, processes, resource_issues = (
            self.importer._planned_runtime_resources(
                [plan], [workspace], acquired_at=now
            )
        )
        if resource_issues:
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED,
                409,
                "assignment-preparation",
                issues=resource_issues,
            )
        assignment = {
            "assignment_id": assignment_id,
            "occurrence_id": occurrence_id,
            "node_id": node_id,
            "agent_id": str(node["agent"]["id"]),
            "agent_phone": str(node["agent"]["phone"]),
            "graph_revision": graph_revision,
            "source_kind": source_kind,
            "source_result_keys": list(source_result_keys),
            "source_commit": source_commit,
            "initial_head_commit": source_commit,
            "integration_id": integration_id,
            "rework_cycle": rework_cycle,
            "result_commit": None,
            "outcome": None,
            "status": "active",
            "workspace_id": workspace_id,
            "branch_lease_id": (
                branch_record["lease_id"] if branch_record is not None else None
            ),
            "allowed_outcomes": _allowed_outcomes(node),
            "created_at": now,
            "completed_at": None,
        }
        return _PreparedAssignment(
            assignment=assignment,
            workspace=workspace,
            workspace_request=workspace_request,
            verified_workspace=verified,
            branch_request=branch_request,
            branch_lease=branch_record,
            port_leases=tuple(port_leases),
            processes=tuple(processes),
        )

    def _commit_prepared_assignment(
        self,
        state: dict[str, Any],
        connection: sqlite3.Connection,
        prepared: _PreparedAssignment,
        *,
        occurrence: dict[str, Any],
        now: str,
    ) -> str:
        assignment = deepcopy(prepared.assignment)
        assignment_id = str(assignment["assignment_id"])
        if any(
            item.get("assignment_id") == assignment_id
            for item in state["assignments"]
        ):
            raise RuntimeError("managed assignment identity conflict")

        if prepared.branch_request is not None:
            acquired = self.importer.branch_leases.acquire_write_in_transaction(
                connection,
                prepared.branch_request,
                acquired_at=str(prepared.branch_lease["acquired_at"]),
            )
            expected = prepared.branch_lease
            if (
                expected is None
                or acquired.lease_id != expected["lease_id"]
                or acquired.assignment_id != assignment_id
                or acquired.status != "active"
            ):
                raise RuntimeError("managed assignment branch lease conflict")
            published = self.importer.workspace_manager.publish_write_workspace(
                prepared.workspace_request,
                prepared.verified_workspace,
                repository=self.importer.provider_for_durable_repository(
                    state["repository"]
                ).ensure_mirror(
                    str(state["repository"]["repository_id"]), fetch=False
                ),
                transactional_lease=acquired,
                publish_mirror_ref=False,
            )
            if published.branch_lease_id != acquired.lease_id:
                raise RuntimeError("managed write workspace publication failed")
            state["branch_leases"].append(deepcopy(expected))

        for lease in prepared.port_leases:
            if not self.importer.port_reservations.owns_endpoint(
                self.store.database_path,
                str(lease["host"]),
                int(lease["port"]),
            ):
                raise RuntimeError("managed port reservation was lost")
            connection.execute(
                """
                INSERT INTO managed_port_leases(
                    lease_id, instance_id, network_namespace_id,
                    assignment_id, process_id, host, port, status, lease_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease["lease_id"],
                    lease["instance_id"],
                    lease["network_namespace_id"],
                    lease["assignment_id"],
                    lease["process_id"],
                    lease["host"],
                    lease["port"],
                    lease["status"],
                    canonical_json_bytes(lease).decode("utf-8"),
                ),
            )
        for process in prepared.processes:
            connection.execute(
                """
                INSERT INTO managed_process_owners(
                    process_id, assignment_id, port_lease_id, pid,
                    state, process_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    process["process_id"],
                    process["assignment_id"],
                    process["port_lease_id"],
                    process["pid"],
                    process["state"],
                    canonical_json_bytes(process).decode("utf-8"),
                ),
            )

        state["workspaces"].append(deepcopy(prepared.workspace))
        state["port_leases"].extend(deepcopy(list(prepared.port_leases)))
        state["processes"].extend(deepcopy(list(prepared.processes)))
        state["assignments"].append(assignment)
        occurrence["assignment_ids"].append(assignment_id)
        occurrence.update({"state": "active", "completed_at": None})
        state["active_assignment_ids"].append(assignment_id)
        state["allowed_outcomes_by_assignment"][assignment_id] = list(
            assignment["allowed_outcomes"]
        )
        event_id = _stable_id("event-assignment", state["sprint_id"], assignment_id)
        state["outbox"].append(
            {
                "event_id": event_id,
                "dedupe_key": (
                    f"enqueue:assignment:{state['sprint_id']}:{assignment_id}"
                ),
                "event_type": "ASSIGNMENT_ENQUEUE",
                "payload": {
                    "sprint_id": state["sprint_id"],
                    "graph_revision": assignment["graph_revision"],
                    "assignment_id": assignment_id,
                    "node_id": assignment["node_id"],
                    "agent_phone": assignment["agent_phone"],
                },
                "status": "pending",
                "created_at": now,
                "delivered_at": None,
                "queue_receipt_id": None,
            }
        )
        return event_id

    def _publish_prepared_assignment(
        self,
        state: Mapping[str, Any],
        prepared: _PreparedAssignment,
    ) -> None:
        if prepared.branch_request is not None:
            provider = self.importer.provider_for_durable_repository(
                state["repository"]
            )
            repository = provider.ensure_mirror(
                str(state["repository"]["repository_id"]), fetch=False
            )
            provider.ensure_local_branch_publication(
                repository,
                str(prepared.branch_request.branch),
                selected_head=str(prepared.branch_request.initial_head_commit),
                publication_id=str(prepared.branch_request.lease_id),
                policy="resume",
                source_commit=str(prepared.branch_request.source_commit),
                expected_branch_head=None,
            )
        if prepared.processes and self.process_supervisor is not None:
            self.process_supervisor.start_background()

    def _recover_assignment_publications(
        self, state: Mapping[str, Any]
    ) -> int:
        """Publish durable write assignments before their queue events escape.

        Assignment/workspace/lease rows are committed together, while the local
        mirror publication receipt is necessarily a later Git side effect.  A
        crash in that gap must be recovered before the pending assignment is
        delivered to a worker.
        """

        repository_record = state.get("repository")
        if not isinstance(repository_record, Mapping):
            raise RuntimeError("managed runtime repository identity is corrupt")
        workspaces = _records_by(state, "workspaces", "workspace_id")
        pending_assignment_ids = {
            str(event.get("payload", {}).get("assignment_id"))
            for event in state.get("outbox", [])
            if isinstance(event, Mapping)
            and event.get("status") == "pending"
            and event.get("event_type") == "ASSIGNMENT_ENQUEUE"
            and isinstance(event.get("payload"), Mapping)
        }
        if not pending_assignment_ids:
            return 0
        pending_write_assignments = [
            assignment
            for assignment in state.get("assignments", [])
            if isinstance(assignment, Mapping)
            and assignment.get("status") in {"active", "reviews_pending"}
            and isinstance(assignment.get("branch_lease_id"), str)
            and assignment.get("assignment_id") in pending_assignment_ids
        ]
        if not pending_write_assignments:
            # Read workspaces have no non-transactional branch publication
            # gap.  Their queue delivery must not depend on Git availability.
            return 0
        provider = self.importer.provider_for_durable_repository(repository_record)
        repository = provider.ensure_mirror(
            str(repository_record["repository_id"]), fetch=False
        )
        recovered = 0
        for assignment in pending_write_assignments:
            workspace = workspaces.get(str(assignment.get("workspace_id")))
            if not isinstance(workspace, Mapping):
                raise RuntimeError("managed assignment workspace is missing")
            branch = workspace.get("assigned_branch")
            if not isinstance(branch, str):
                raise RuntimeError("managed write workspace branch is missing")
            lease_id = str(assignment["branch_lease_id"])
            lease = self.importer.branch_leases.get(lease_id)
            if lease is None or lease.status != "active":
                raise RuntimeError("managed write assignment lease is missing")
            request = WorkspaceRequest(
                project_id=str(workspace["project_id"]),
                sprint_id=str(workspace["sprint_id"]),
                node_id=str(assignment["node_id"]),
                assignment_id=str(assignment["assignment_id"]),
                repository_id=str(repository_record["repository_id"]),
                source_commit=str(assignment["source_commit"]),
                access="write",
                assigned_branch=branch,
                existing_branch_policy="resume",
            )
            durable_workspace = managed_workspace_from_record(
                workspace, repository.mirror_storage_key
            )
            self.importer.workspace_manager.publish_write_workspace(
                request,
                durable_workspace,
                repository=repository,
                transactional_lease=lease,
                publish_mirror_ref=False,
            )
            provider.ensure_local_branch_publication(
                repository,
                branch,
                selected_head=str(assignment["initial_head_commit"]),
                publication_id=lease_id,
                policy="resume",
                source_commit=str(assignment["source_commit"]),
                expected_branch_head=None,
            )
            recovered += 1
        return recovered

    @staticmethod
    def _inbound_parent_ids(
        definition: Mapping[str, Any], target_node_id: str
    ) -> list[str]:
        result: list[str] = []
        nodes = definition.get("nodes")
        nodes = nodes if isinstance(nodes, list) else []
        for candidate in nodes:
            transitions = (
                candidate.get("transitions")
                if isinstance(candidate, Mapping)
                else None
            )
            if (
                isinstance(candidate, Mapping)
                and candidate.get("type", "task") == "task"
                and isinstance(candidate.get("id"), str)
                and isinstance(transitions, Mapping)
                and target_node_id in transitions.values()
                and candidate["id"] not in result
            ):
                result.append(str(candidate["id"]))
        coordinator = definition.get("coordinator")
        if (
            isinstance(coordinator, Mapping)
            and coordinator.get("node_id") == target_node_id
        ):
            for candidate in nodes:
                if (
                    isinstance(candidate, Mapping)
                    and candidate.get("type", "task") == "task"
                    and isinstance(candidate.get("id"), str)
                    and candidate["id"] not in result
                ):
                    result.append(str(candidate["id"]))
        return result

    @staticmethod
    def _ready_tokens(
        state: Mapping[str, Any], target_node_id: str
    ) -> list[dict[str, Any]]:
        definition = _graph_revision(state, int(state["graph_revision"]))[
            "definition"
        ]
        target = _node(state, int(state["graph_revision"]), target_node_id)
        available = [
            token
            for token in state["workflow"]["transition_tokens"]
            if isinstance(token, dict)
            and token.get("status") == "available"
            and token.get("target_node_id") == target_node_id
        ]
        occurrences = _records_by(
            state["workflow"], "occurrences", "occurrence_id"
        )

        def ordering(token: Mapping[str, Any]) -> tuple[int, str, str]:
            occurrence = occurrences.get(str(token.get("source_occurrence_id")))
            generation = (
                int(occurrence.get("generation"))
                if isinstance(occurrence, Mapping)
                and isinstance(occurrence.get("generation"), int)
                else 2**31
            )
            return (
                generation,
                str(token.get("result_key") or ""),
                str(token.get("token_id") or ""),
            )

        policy = str(target.get("activation_policy") or "all_parents")
        if policy == "any_parent":
            return [min(available, key=ordering)] if available else []
        parent_order = target.get("join_parent_order")
        if not isinstance(parent_order, list):
            parent_order = ManagedContinuityRuntime._inbound_parent_ids(
                definition, target_node_id
            )
        selected: list[dict[str, Any]] = []
        for parent_id in parent_order:
            candidates = [
                token
                for token in available
                if token.get("source_node_id") == parent_id
            ]
            if not candidates:
                return []
            selected.append(min(candidates, key=ordering))
        return selected

    def _join_divergence_records(
        self,
        state: Mapping[str, Any],
        *,
        target_node_id: str,
        trigger_token_ids: Sequence[str],
        now: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        sprint_id = str(state["sprint_id"])
        graph_revision = int(state["graph_revision"])
        context_id = _stable_id(
            "context-join",
            sprint_id,
            graph_revision,
            target_node_id,
            *trigger_token_ids,
            compact=True,
        )
        definition = _graph_revision(state, graph_revision)["definition"]
        coordinator = definition["coordinator"]
        coordinator_node = next(
            item
            for item in definition["nodes"]
            if item.get("id") == coordinator["node_id"]
        )
        context = {
            "context_id": context_id,
            "graph_revision": graph_revision,
            "failure_scope": "integration",
            "reason_code": "JOIN_SOURCE_DIVERGED",
            "import_attempt_id": None,
            "failed_assignment_id": None,
            "join_target": {
                "target_graph_revision": graph_revision,
                "target_node_id": target_node_id,
                "trigger_token_ids": list(trigger_token_ids),
                "integration_id": None,
            },
            "assigned_branch": None,
            "source_commit": None,
            "result_commit": None,
            "diff_summary": {},
            "workspace_status": {},
            "process_records": [],
            "port_records": [],
            "test_evidence_summary": {},
            "normalized_error": {
                "code": "JOIN_SOURCE_DIVERGED",
                "message": "accepted parents have distinct commits",
            },
            "reviewer_feedback": [],
        }
        event = {
            "event_id": _stable_id("event-coordinator", context_id),
            "dedupe_key": f"enqueue:coordinator:{sprint_id}:{context_id}",
            "event_type": "COORDINATOR_ENQUEUE",
            "payload": {
                "sprint_id": sprint_id,
                "context_id": context_id,
                "coordinator_phone": str(coordinator_node["agent"]["phone"]),
                "reason_code": "JOIN_SOURCE_DIVERGED",
            },
            "status": "pending",
            "created_at": now,
            "delivered_at": None,
            "queue_receipt_id": None,
        }
        return context, event

    def _append_join_divergence_context(
        self,
        state: dict[str, Any],
        *,
        target_node_id: str,
        trigger_token_ids: Sequence[str],
        now: str,
    ) -> bool:
        context, event = self._join_divergence_records(
            state,
            target_node_id=target_node_id,
            trigger_token_ids=trigger_token_ids,
            now=now,
        )
        contexts = _records_by(state, "coordinator_contexts", "context_id")
        existing = contexts.get(str(context["context_id"]))
        if existing is not None:
            if existing != context:
                raise RuntimeError("managed join context identity conflict")
            return False
        state["coordinator_contexts"].append(context)
        state["outbox"].append(event)
        return True

    def _prepare_integration_pair(
        self,
        project_id: str,
        state: Mapping[str, Any],
        *,
        target_node_id: str,
        graph_revision: int,
        tokens: Sequence[Mapping[str, Any]],
        result_keys: Sequence[str],
        commits: Sequence[str],
        now: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        sprint_id = str(state["sprint_id"])
        trigger_ids = [str(token["token_id"]) for token in tokens]
        integration_id = managed_integration_id(
            sprint_id, graph_revision, target_node_id, trigger_ids
        )
        workspace_artifact_id = managed_integration_workspace_id(
            sprint_id, graph_revision, target_node_id, trigger_ids
        )
        repository_record = state.get("repository")
        if not isinstance(repository_record, Mapping):
            raise RuntimeError("managed runtime repository identity is corrupt")
        provider = self.importer.provider_for_durable_repository(repository_record)
        repository = provider.ensure_mirror(
            str(repository_record["repository_id"]), fetch=False
        )
        for commit in commits:
            provider.assert_commit(repository, commit)

        root = self.importer.workspace_manager.integration_workspace_root(
            workspace_artifact_id
        )
        lock_path = self.importer.workspace_manager.integration_workspace_lock_path(
            workspace_artifact_id
        )
        try:
            with ManagedFileLock(lock_path, timeout=60.0):
                if root.is_symlink() or (root.exists() and not root.is_dir()):
                    raise RuntimeError("managed integration workspace is redirected")
                if not root.exists():
                    root.parent.mkdir(parents=True, exist_ok=True)
                    root = self.importer.workspace_manager.integration_workspace_root(
                        workspace_artifact_id
                    )
                    preparation_root = root.parent / f".p-{uuid4().hex[:24]}"
                    provider.runner.run(
                        (
                            "-c",
                            "core.longpaths=true",
                            "init",
                            "--quiet",
                            f"--object-format={repository.object_format}",
                            "--initial-branch=nginx-qa-unborn",
                            "--",
                            str(preparation_root),
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    )
                    provider.runner.run(
                        (
                            "-C",
                            str(preparation_root),
                            "config",
                            "core.longpaths",
                            "true",
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    )
                    provider.runner.run(
                        (
                            "-C",
                            str(preparation_root),
                            "remote",
                            "add",
                            "origin",
                            repository.spec.transport_url,
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    )
                    parent_refs = [
                        provider.pin_commit(
                            repository,
                            commit,
                            owner_id=f"{integration_id}:parent:{index}",
                        )
                        for index, commit in enumerate(commits, 1)
                    ]
                    provider.runner.run(
                        (
                            "-C",
                            str(preparation_root),
                            "fetch",
                            "--quiet",
                            "--no-tags",
                            "--no-write-fetch-head",
                            str(repository.mirror_path),
                            *(
                                f"+{ref}:refs/nginx-qa/parents/{index}"
                                for index, ref in enumerate(parent_refs, 1)
                            ),
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    )
                    provider.runner.run(
                        (
                            "-C",
                            str(preparation_root),
                            "switch",
                            "--detach",
                            commits[0],
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    )
                    prepared_top = provider.runner.run(
                        (
                            "-C",
                            str(preparation_root),
                            "rev-parse",
                            "--show-toplevel",
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    ).stdout.strip()
                    prepared_status = provider.runner.run(
                        (
                            "-C",
                            str(preparation_root),
                            "status",
                            "--porcelain=v1",
                            "--untracked-files=all",
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    ).stdout
                    if (
                        Path(prepared_top).resolve(strict=True)
                        != preparation_root.resolve(strict=True)
                        or prepared_status
                        or root.exists()
                        or root.is_symlink()
                    ):
                        raise RuntimeError(
                            "managed integration workspace preparation changed"
                        )
                    preparation_root.rename(root)
                verified_root = (
                    self.importer.workspace_manager.verify_integration_workspace(
                        workspace_artifact_id,
                        repository,
                        tuple(commits),
                    )
                )
                if verified_root.resolve(strict=True) != root.resolve(strict=True):
                    raise RuntimeError("managed integration workspace changed")
                top = provider.runner.run(
                    ("-C", str(root), "rev-parse", "--show-toplevel"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip()
                git_dir_raw = provider.runner.run(
                    ("-C", str(root), "rev-parse", "--absolute-git-dir"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip()
                head = provider.runner.run(
                    ("-C", str(root), "rev-parse", "HEAD"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip().lower()
                status = provider.runner.run(
                    ("-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout
                object_format = provider.runner.run(
                    ("-C", str(root), "rev-parse", "--show-object-format"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip()
                origin = provider.runner.run(
                    ("-C", str(root), "remote", "get-url", "origin"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip()
                if (
                    Path(top).resolve(strict=True) != root.resolve(strict=True)
                    or not Path(git_dir_raw).resolve(strict=True).is_relative_to(
                        root.resolve(strict=True)
                    )
                    or head != commits[0]
                    or status
                    or object_format != repository.object_format
                    or origin != repository.spec.transport_url
                ):
                    raise RuntimeError("managed integration workspace changed")
        except OSError as exc:
            raise ManagedGitError(
                "INTEGRATION_WORKSPACE_FAILED",
                "integration workspace could not be prepared",
            ) from exc

        parents = [
            {
                "source_node_id": str(token["source_node_id"]),
                "result_key": result_key,
                "commit": commit,
            }
            for token, result_key, commit in zip(tokens, result_keys, commits)
        ]
        frozen_git_timestamp = _git_timestamp(now)
        integration = {
            "integration_id": integration_id,
            "target_node_id": target_node_id,
            "target_graph_revision": graph_revision,
            "trigger_token_ids": trigger_ids,
            "workspace_artifact_id": workspace_artifact_id,
            "strategy": "merge_no_ff",
            "status": "PREPARED",
            "parents": parents,
            "author_name": "nginx-qa managed integration",
            "author_email": "managed-integration@localhost",
            "author_timestamp": frozen_git_timestamp,
            "committer_name": "nginx-qa managed integration",
            "committer_email": "managed-integration@localhost",
            "committer_timestamp": frozen_git_timestamp,
            "commit_message": f"Managed integration for {target_node_id}",
            "integration_commit": None,
            "target_occurrence_id": None,
            "assignment_id": None,
            "normalized_error": None,
            "created_at": now,
            "updated_at": now,
        }
        artifact = {
            "workspace_artifact_id": workspace_artifact_id,
            "integration_id": integration_id,
            "project_id": project_id,
            "sprint_id": sprint_id,
            "repository_id": repository.repository_id,
            "repository_remote": repository.canonical_remote,
            "mirror_storage_key": repository.mirror_storage_key,
            "expected_root": str(root),
            "actual_git_toplevel": str(root.resolve(strict=True)),
            "actual_git_dir": str(Path(git_dir_raw).resolve(strict=True)),
            "base_commit": commits[0],
            "head_commit": commits[0],
            "artifact_status": "active",
            "working_tree_state": "clean",
            "created_at": now,
            "verified_at": now,
            "released_at": None,
        }
        return integration, artifact

    def _prospective_join_plan(
        self,
        project_id: str,
        state: Mapping[str, Any],
        source_assignment_id: str,
        result_key: str,
        *,
        now: str,
    ) -> _JoinPlan | None:
        prospective = deepcopy(dict(state))
        source = _records_by(prospective, "assignments", "assignment_id").get(
            source_assignment_id
        )
        journal = _records_by(
            prospective, "transition_journal", "result_key"
        ).get(result_key)
        if not isinstance(source, dict) or not isinstance(journal, dict):
            raise RuntimeError("managed prospective join source is missing")
        token = self._append_transition_token(prospective, source, journal, now=now)
        if token.get("status") != "available":
            return None
        graph_revision = int(prospective["graph_revision"])
        target_node_id = str(token["target_node_id"])
        target_node = _node(prospective, graph_revision, target_node_id)
        if target_node.get("activation_policy", "all_parents") != "all_parents":
            return None
        selected = self._ready_tokens(prospective, target_node_id)
        if len(selected) < 2 or token["token_id"] not in {
            item["token_id"] for item in selected
        }:
            return None
        trigger_ids = tuple(str(item["token_id"]) for item in selected)
        result_keys = tuple(str(item["result_key"]) for item in selected)
        receipts = _records_by(prospective, "result_receipts", "result_key")
        commits = tuple(str(receipts[key]["result_commit"]) for key in result_keys)
        if len(set(commits)) == 1:
            generations = [
                int(item["generation"])
                for item in prospective["workflow"]["occurrences"]
                if isinstance(item, Mapping)
                and item.get("node_id") == target_node_id
                and isinstance(item.get("generation"), int)
            ]
            generation = max(generations, default=0) + 1
            occurrence_id = managed_occurrence_id(
                str(prospective["sprint_id"]),
                graph_revision,
                target_node_id,
                generation,
                list(trigger_ids),
            )
            prepared = self._prepare_assignment(
                project_id,
                state,
                occurrence_id=occurrence_id,
                node_id=target_node_id,
                graph_revision=graph_revision,
                source_kind="accepted_result",
                source_result_keys=result_keys,
                source_commit=commits[0],
            )
            return _JoinPlan(
                "assignment",
                target_node_id,
                graph_revision,
                trigger_ids,
                result_keys,
                commits,
                generation,
                occurrence_id,
                prepared,
            )
        workspace = target_node.get("workspace")
        strategy = (
            workspace.get("join_strategy", "require_same_commit")
            if isinstance(workspace, Mapping)
            else "require_same_commit"
        )
        if strategy == "require_same_commit":
            return _JoinPlan(
                "divergence",
                target_node_id,
                graph_revision,
                trigger_ids,
                result_keys,
                commits,
            )
        integration, artifact = self._prepare_integration_pair(
            project_id,
            prospective,
            target_node_id=target_node_id,
            graph_revision=graph_revision,
            tokens=selected,
            result_keys=result_keys,
            commits=commits,
            now=now,
        )
        return _JoinPlan(
            "integration",
            target_node_id,
            graph_revision,
            trigger_ids,
            result_keys,
            commits,
            integration=integration,
            integration_workspace=artifact,
        )

    def _prospective_repaired_join_plan(
        self,
        project_id: str,
        state: Mapping[str, Any],
        candidate_definition: Mapping[str, Any],
        context: Mapping[str, Any],
        *,
        to_revision: int,
        now: str,
        repair_already_applied: bool = False,
    ) -> _JoinPlan:
        join_target = context.get("join_target")
        if not isinstance(join_target, Mapping):
            raise RuntimeError("managed join recovery target is missing")
        from_revision = int(context["graph_revision"])
        target_node_id = str(join_target["target_node_id"])
        trigger_ids = tuple(str(item) for item in join_target["trigger_token_ids"])
        old_definition = _graph_revision(state, from_revision)["definition"]
        old_target = next(
            (
                item
                for item in old_definition["nodes"]
                if item.get("id") == target_node_id
            ),
            None,
        )
        new_target = next(
            (
                item
                for item in candidate_definition["nodes"]
                if item.get("id") == target_node_id
            ),
            None,
        )
        old_order = (
            old_target.get("join_parent_order")
            if isinstance(old_target, Mapping)
            else None
        )
        new_order = (
            new_target.get("join_parent_order")
            if isinstance(new_target, Mapping)
            else None
        )
        if (
            join_target.get("target_graph_revision") != from_revision
            or not isinstance(old_target, Mapping)
            or not isinstance(new_target, Mapping)
            or old_target.get("activation_policy", "all_parents")
            != "all_parents"
            or new_target.get("activation_policy", "all_parents")
            != "all_parents"
            or not isinstance(old_order, list)
            or new_order != old_order
            or self._inbound_parent_ids(old_definition, target_node_id)
            != self._inbound_parent_ids(candidate_definition, target_node_id)
        ):
            raise ManagedContinuityError(
                REPAIR_PATCH_CONFLICT, 409, "join-recovery"
            )
        prospective = deepcopy(dict(state))
        if repair_already_applied:
            applied_revision = _graph_revision(prospective, to_revision)
            if (
                prospective.get("graph_revision") != to_revision
                or prospective.get("workflow", {}).get("graph_revision")
                != to_revision
                or canonical_json_sha256(applied_revision["definition"])
                != canonical_json_sha256(candidate_definition)
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, "join-recovery"
                )
        else:
            prospective["graph_revisions"].append(
                {
                    "revision": to_revision,
                    "definition_sha256": canonical_json_sha256(
                        candidate_definition
                    ),
                    "definition": deepcopy(dict(candidate_definition)),
                    "artifact_source_commit": None,
                    "created_at": now,
                    "source": "repair",
                    "repair_id": None,
                }
            )
            prospective["graph_revision"] = to_revision
            prospective["workflow"]["graph_revision"] = to_revision
        selected = self._ready_tokens(prospective, target_node_id)
        if tuple(str(item["token_id"]) for item in selected) != trigger_ids:
            raise ManagedContinuityError(
                REPAIR_PATCH_CONFLICT, 409, "join-recovery"
            )
        result_keys = tuple(str(item["result_key"]) for item in selected)
        receipts = _records_by(prospective, "result_receipts", "result_key")
        commits = tuple(str(receipts[key]["result_commit"]) for key in result_keys)
        if len(set(commits)) == 1:
            generations = [
                int(item["generation"])
                for item in prospective["workflow"]["occurrences"]
                if isinstance(item, Mapping)
                and item.get("node_id") == target_node_id
                and isinstance(item.get("generation"), int)
            ]
            generation = max(generations, default=0) + 1
            occurrence_id = managed_occurrence_id(
                str(prospective["sprint_id"]),
                to_revision,
                target_node_id,
                generation,
                list(trigger_ids),
            )
            prepared = self._prepare_assignment(
                project_id,
                prospective,
                occurrence_id=occurrence_id,
                node_id=target_node_id,
                graph_revision=to_revision,
                source_kind="accepted_result",
                source_result_keys=result_keys,
                source_commit=commits[0],
            )
            return _JoinPlan(
                "assignment",
                target_node_id,
                to_revision,
                trigger_ids,
                result_keys,
                commits,
                generation,
                occurrence_id,
                prepared,
            )
        workspace = new_target.get("workspace")
        strategy = (
            workspace.get("join_strategy", "require_same_commit")
            if isinstance(workspace, Mapping)
            else "require_same_commit"
        )
        if strategy == "require_same_commit":
            return _JoinPlan(
                "divergence",
                target_node_id,
                to_revision,
                trigger_ids,
                result_keys,
                commits,
            )
        integration, artifact = self._prepare_integration_pair(
            project_id,
            prospective,
            target_node_id=target_node_id,
            graph_revision=to_revision,
            tokens=selected,
            result_keys=result_keys,
            commits=commits,
            now=now,
        )
        return _JoinPlan(
            "integration",
            target_node_id,
            to_revision,
            trigger_ids,
            result_keys,
            commits,
            integration=integration,
            integration_workspace=artifact,
        )

    def _commit_repaired_join_effect(
        self,
        state: dict[str, Any],
        connection: sqlite3.Connection,
        plan: _JoinPlan,
        *,
        repair_id: str,
        now: str,
    ) -> list[str]:
        event_id = self._commit_join_plan(state, connection, plan, now=now)
        if plan.kind == "assignment":
            if (
                plan.occurrence_id is None
                or plan.prepared_assignment is None
                or event_id is None
            ):
                raise RuntimeError("managed repaired join assignment is incomplete")
            return [
                repair_id,
                plan.occurrence_id,
                plan.prepared_assignment.assignment["assignment_id"],
                event_id,
            ]
        if plan.kind == "divergence":
            context, event = self._join_divergence_records(
                state,
                target_node_id=plan.target_node_id,
                trigger_token_ids=plan.trigger_token_ids,
                now=now,
            )
            return [repair_id, context["context_id"], event["event_id"]]
        if plan.integration is None or plan.integration_workspace is None:
            raise RuntimeError("managed repaired join integration is incomplete")
        return [
            repair_id,
            plan.integration["integration_id"],
            plan.integration_workspace["workspace_artifact_id"],
        ]

    def _commit_join_plan(
        self,
        state: dict[str, Any],
        connection: sqlite3.Connection,
        plan: _JoinPlan,
        *,
        now: str,
    ) -> str | None:
        if state.get("graph_revision") != plan.graph_revision:
            raise RuntimeError("managed graph changed during join preparation")
        selected = self._ready_tokens(state, plan.target_node_id)
        if tuple(str(item["token_id"]) for item in selected) != plan.trigger_token_ids:
            raise RuntimeError("managed join selection changed")
        if plan.kind == "divergence":
            self._append_join_divergence_context(
                state,
                target_node_id=plan.target_node_id,
                trigger_token_ids=plan.trigger_token_ids,
                now=now,
            )
            return None
        if plan.kind == "integration":
            if plan.integration is None or plan.integration_workspace is None:
                raise RuntimeError("managed integration preparation is missing")
            state["integrations"].append(deepcopy(plan.integration))
            state["integration_workspaces"].append(
                deepcopy(plan.integration_workspace)
            )
            return None
        prepared = plan.prepared_assignment
        if (
            prepared is None
            or plan.occurrence_id is None
            or plan.generation is None
        ):
            raise RuntimeError("managed join assignment preparation is missing")
        target_node = _node(state, plan.graph_revision, plan.target_node_id)
        occurrence = {
            "occurrence_id": plan.occurrence_id,
            "node_id": plan.target_node_id,
            "graph_revision": plan.graph_revision,
            "generation": plan.generation,
            "activation_policy": str(
                target_node.get("activation_policy") or "all_parents"
            ),
            "trigger_token_ids": list(plan.trigger_token_ids),
            "state": "active",
            "assignment_ids": [],
            "created_at": now,
            "completed_at": None,
        }
        state["workflow"]["occurrences"].append(occurrence)
        event_id = self._commit_prepared_assignment(
            state, connection, prepared, occurrence=occurrence, now=now
        )
        tokens = _records_by(state["workflow"], "transition_tokens", "token_id")
        journals = _records_by(state, "transition_journal", "result_key")
        for token_id in plan.trigger_token_ids:
            current_token = tokens[token_id]
            current_token.update(
                {
                    "status": "consumed",
                    "target_graph_revision": plan.graph_revision,
                    "consumed_by_occurrence_id": plan.occurrence_id,
                }
            )
            journal = journals.get(str(current_token["result_key"]))
            if (
                not isinstance(journal, dict)
                or journal.get("state") != "TRANSITION_COMMITTED"
            ):
                raise RuntimeError("managed source journal is not schedulable")
            journal.update(
                {
                    "state": "NEXT_ASSIGNMENT_ENQUEUED",
                    "target_occurrence_ids": [plan.occurrence_id],
                    "outbox_event_ids": [event_id],
                    "updated_at": now,
                }
            )
        return event_id

    @staticmethod
    def _integration_workspace_projection(
        artifact: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            field: artifact.get(field)
            for field in (
                "workspace_artifact_id",
                "integration_id",
                "expected_root",
                "base_commit",
                "head_commit",
                "artifact_status",
                "working_tree_state",
                "verified_at",
            )
        }

    def _append_integration_failure_context(
        self,
        state: dict[str, Any],
        integration: Mapping[str, Any],
        artifact: Mapping[str, Any],
        *,
        reason_code: str,
        normalized_error: Mapping[str, Any],
        now: str,
    ) -> None:
        sprint_id = str(state["sprint_id"])
        integration_id = str(integration["integration_id"])
        context_id = _stable_id(
            "context-integration", integration_id, reason_code, compact=True
        )
        definition = _graph_revision(
            state, int(integration["target_graph_revision"])
        )["definition"]
        coordinator = definition["coordinator"]
        coordinator_node = next(
            item
            for item in definition["nodes"]
            if item.get("id") == coordinator["node_id"]
        )
        context = {
            "context_id": context_id,
            "graph_revision": int(integration["target_graph_revision"]),
            "failure_scope": "integration",
            "reason_code": reason_code,
            "import_attempt_id": None,
            "failed_assignment_id": None,
            "join_target": {
                "target_graph_revision": int(
                    integration["target_graph_revision"]
                ),
                "target_node_id": str(integration["target_node_id"]),
                "trigger_token_ids": list(integration["trigger_token_ids"]),
                "integration_id": integration_id,
            },
            "assigned_branch": None,
            "source_commit": None,
            "result_commit": None,
            "diff_summary": {},
            "workspace_status": self._integration_workspace_projection(artifact),
            "process_records": [],
            "port_records": [],
            "test_evidence_summary": {},
            "normalized_error": deepcopy(dict(normalized_error)),
            "reviewer_feedback": [],
        }
        event = {
            "event_id": _stable_id("event-coordinator", context_id),
            "dedupe_key": f"enqueue:coordinator:{sprint_id}:{context_id}",
            "event_type": "COORDINATOR_ENQUEUE",
            "payload": {
                "sprint_id": sprint_id,
                "context_id": context_id,
                "coordinator_phone": str(coordinator_node["agent"]["phone"]),
                "reason_code": reason_code,
            },
            "status": "pending",
            "created_at": now,
            "delivered_at": None,
            "queue_receipt_id": None,
        }
        state["coordinator_contexts"].append(context)
        state["outbox"].append(event)

    @staticmethod
    def _git_path_state(provider: Any, root: Path) -> tuple[str, bool]:
        unresolved = provider.runner.run(
            (
                "-C",
                str(root),
                "diff",
                "--name-only",
                "--diff-filter=U",
            ),
            error_code="INTEGRATION_WORKSPACE_FAILED",
        ).stdout
        merge_head = provider.runner.run(
            ("-C", str(root), "rev-parse", "--verify", "MERGE_HEAD"),
            allowed_returncodes=frozenset({0, 128}),
            error_code="INTEGRATION_WORKSPACE_FAILED",
        ).returncode == 0
        return ("conflicted" if unresolved else "merging" if merge_head else "clean"), merge_head

    @staticmethod
    def _verify_frozen_integration_commit(
        provider: Any,
        root: Path,
        integration: Mapping[str, Any],
        commit: str,
        *,
        expected_tree: str,
    ) -> None:
        parents = [str(item["commit"]) for item in integration["parents"]]
        parent_line = provider.runner.run(
            ("-C", str(root), "rev-list", "--parents", "-n", "1", commit),
            error_code="INTEGRATION_WORKSPACE_FAILED",
        ).stdout.strip().split()
        metadata = provider.runner.run(
            (
                "-C",
                str(root),
                "show",
                "-s",
                "--format=%an%x00%ae%x00%aI%x00%cn%x00%ce%x00%cI%x00%B",
                commit,
            ),
            error_code="INTEGRATION_WORKSPACE_FAILED",
        ).stdout
        fields = metadata.split("\0", 6)
        tree = provider.runner.run(
            ("-C", str(root), "rev-parse", f"{commit}^{{tree}}"),
            error_code="INTEGRATION_WORKSPACE_FAILED",
        ).stdout.strip().lower()
        if (
            parent_line != [commit, *parents]
            or tree != expected_tree
            or len(fields) != 7
            or fields[0] != integration["author_name"]
            or fields[1] != integration["author_email"]
            or fields[2] != integration["author_timestamp"]
            or fields[3] != integration["committer_name"]
            or fields[4] != integration["committer_email"]
            or fields[5] != integration["committer_timestamp"]
            or fields[6].strip() != integration["commit_message"]
        ):
            raise RuntimeError("managed integration commit changed")

    @staticmethod
    def _create_frozen_integration_commit(
        provider: Any,
        root: Path,
        integration: Mapping[str, Any],
        *,
        expected_tree: str,
    ) -> str:
        """Write the exact frozen integration commit without parent folding.

        ``git commit-tree`` silently removes duplicate ``-p`` arguments.  A
        partial duplicate is valid here: distinct accepted results may point
        at the same commit, while their ordered provenance must remain visible
        in the commit's parent tuple.  Ask Git to canonicalize the two frozen
        identities, then write the commit object itself so every parent line
        is preserved in declaration order.
        """

        def frozen_identity(
            variable: str,
            *,
            name: str,
            email: str,
            date_variable: str,
            timestamp: str,
        ) -> str:
            raw = provider.runner.run(
                (
                    "-C",
                    str(root),
                    "-c",
                    f"user.name={name}",
                    "-c",
                    f"user.email={email}",
                    "var",
                    variable,
                ),
                environment={date_variable: timestamp},
                error_code="INTEGRATION_MERGE_FAILED",
            ).stdout.rstrip("\r\n")
            if not raw or "\n" in raw or "\r" in raw or "\0" in raw:
                raise RuntimeError("managed integration identity changed")
            return raw

        author = frozen_identity(
            "GIT_AUTHOR_IDENT",
            name=str(integration["author_name"]),
            email=str(integration["author_email"]),
            date_variable="GIT_AUTHOR_DATE",
            timestamp=str(integration["author_timestamp"]),
        )
        committer = frozen_identity(
            "GIT_COMMITTER_IDENT",
            name=str(integration["committer_name"]),
            email=str(integration["committer_email"]),
            date_variable="GIT_COMMITTER_DATE",
            timestamp=str(integration["committer_timestamp"]),
        )
        parents = [str(item["commit"]) for item in integration["parents"]]
        headers = [
            f"tree {expected_tree}",
            *(f"parent {parent}" for parent in parents),
            f"author {author}",
            f"committer {committer}",
        ]
        payload = "\n".join(
            [*headers, "", str(integration["commit_message"])]
        ) + "\n"
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="nginx-qa-managed-commit-",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary.write(payload.encode("utf-8"))
                temporary_path = Path(temporary.name)
            candidate = provider.runner.run(
                (
                    "-C",
                    str(root),
                    "hash-object",
                    "-t",
                    "commit",
                    "-w",
                    str(temporary_path),
                ),
                error_code="INTEGRATION_MERGE_FAILED",
            ).stdout.strip().lower()
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", candidate) is None:
            raise RuntimeError("managed integration commit was not created")
        return candidate

    def _import_integration_commit(
        self,
        provider: Any,
        repository: ManagedRepository,
        root: Path,
        integration_id: str,
        commit: str,
    ) -> None:
        suffix = integration_id.removeprefix("integration-")
        target_ref = f"refs/nginx-qa/integrations/{suffix}"
        lock_path = provider.locks_root / f"{repository.mirror_storage_key}.lock"
        with ManagedFileLock(lock_path, timeout=provider.lock_timeout):
            current_result = provider.runner.run(
                (
                    "--git-dir",
                    str(repository.mirror_path),
                    "rev-parse",
                    "--verify",
                    f"{target_ref}^{{commit}}",
                ),
                allowed_returncodes=frozenset({0, 128}),
                error_code="INTEGRATION_WORKSPACE_FAILED",
            )
            current = (
                current_result.stdout.strip().lower()
                if current_result.returncode == 0
                else None
            )
            if current is not None and current != commit:
                raise RuntimeError("managed integration ref changed")
            if current is None:
                provider.runner.run(
                    (
                        "--git-dir",
                        str(repository.mirror_path),
                        "fetch",
                        "--quiet",
                        "--no-tags",
                        "--no-write-fetch-head",
                        str(root),
                        f"HEAD:{target_ref}",
                    ),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                )
        provider.assert_commit(repository, commit)
        provider.pin_commit(
            repository, commit, owner_id=f"{integration_id}:commit"
        )

    def _resume_one_prepared_integration(
        self, project_id: str, sprint_id: str, state: Mapping[str, Any]
    ) -> bool:
        integration = next(
            (
                item
                for item in state.get("integrations", [])
                if isinstance(item, Mapping)
                and item.get("status") == "PREPARED"
                and item.get("target_graph_revision")
                == state.get("graph_revision")
                and not self._has_successor_preparation_failure(
                    state,
                    target_node_id=str(item["target_node_id"]),
                    trigger_token_ids=tuple(
                        str(token_id)
                        for token_id in item["trigger_token_ids"]
                    ),
                )
            ),
            None,
        )
        if integration is None:
            return False
        trigger_ids = tuple(
            str(item) for item in integration["trigger_token_ids"]
        )
        result_keys = tuple(
            str(item["result_key"]) for item in integration["parents"]
        )
        graph_revision = int(integration["target_graph_revision"])
        target_node_id = str(integration["target_node_id"])
        artifacts = _records_by(
            state, "integration_workspaces", "workspace_artifact_id"
        )
        artifact = artifacts.get(str(integration.get("workspace_artifact_id")))
        if not isinstance(artifact, Mapping):
            raise RuntimeError("managed integration artifact disappeared")
        workspace_id = str(artifact["workspace_artifact_id"])
        repository_record = state.get("repository")
        if not isinstance(repository_record, Mapping):
            raise RuntimeError("managed runtime repository identity is corrupt")
        parents = [str(item["commit"]) for item in integration["parents"]]
        now = _timestamp(self.clock)
        failure: tuple[str, dict[str, Any], str] | None = None
        integration_commit: str | None = None
        root = Path(str(artifact["expected_root"]))
        provider: Any | None = None
        repository: ManagedRepository | None = None
        try:
            root = self.importer.workspace_manager.integration_workspace_root(
                workspace_id
            )
            if root.resolve(strict=True) != Path(
                str(artifact["expected_root"])
            ).resolve(strict=True):
                raise RuntimeError("managed integration workspace root changed")
            lock_path = (
                self.importer.workspace_manager.integration_workspace_lock_path(
                    workspace_id
                )
            )
            provider = self.importer.provider_for_durable_repository(
                repository_record
            )
            repository = provider.ensure_mirror(
                str(repository_record["repository_id"]), fetch=False
            )
            with ManagedFileLock(lock_path, timeout=60.0):
                verified_root = (
                    self.importer.workspace_manager.verify_integration_workspace(
                        workspace_id,
                        repository,
                        tuple(parents),
                    )
                )
                if verified_root.resolve(strict=True) != root.resolve(strict=True):
                    raise RuntimeError("managed integration workspace changed")
                top = provider.runner.run(
                    ("-C", str(root), "rev-parse", "--show-toplevel"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip()
                origin = provider.runner.run(
                    ("-C", str(root), "remote", "get-url", "origin"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip()
                head = provider.runner.run(
                    ("-C", str(root), "rev-parse", "HEAD"),
                    error_code="INTEGRATION_WORKSPACE_FAILED",
                ).stdout.strip().lower()
                if (
                    Path(top).resolve(strict=True) != root.resolve(strict=True)
                    or origin != repository.spec.transport_url
                ):
                    raise RuntimeError("managed integration workspace changed")
                tree_state, merge_head = self._git_path_state(provider, root)
                if head == parents[0] and not merge_head:
                    merge = provider.runner.run(
                        (
                            "-C",
                            str(root),
                            "-c",
                            "commit.gpgsign=false",
                            "merge",
                            "--no-ff",
                            "--no-commit",
                            "--no-edit",
                            *parents[1:],
                        ),
                        environment={"LC_ALL": "C", "LANG": "C"},
                        allowed_returncodes=frozenset({0, 1}),
                        error_code="INTEGRATION_MERGE_FAILED",
                    )
                    tree_state, merge_head = self._git_path_state(provider, root)
                    if merge.returncode != 0:
                        failure = (
                            "JOIN_MERGE_CONFLICT"
                            if tree_state == "conflicted"
                            else "JOIN_INTEGRATION_FAILED",
                            {
                                "code": (
                                    "MERGE_CONFLICT"
                                    if tree_state == "conflicted"
                                    else "INTEGRATION_FAILED"
                                )
                            },
                            tree_state,
                        )
                if failure is None:
                    tree_state, merge_head = self._git_path_state(provider, root)
                    if tree_state == "conflicted":
                        failure = (
                            "JOIN_MERGE_CONFLICT",
                            {"code": "MERGE_CONFLICT"},
                            tree_state,
                        )
                if failure is None:
                    if head == parents[0]:
                        # Porcelain merge is retained as the conflict-aware tree
                        # calculator, but it may legally omit an ancestor from
                        # MERGE_HEAD.  Freeze the exact declared parent tuple
                        # with commit-tree, verify the object while unreachable,
                        # and only then publish it through a compare-and-swap.
                        expected_tree = provider.runner.run(
                            ("-C", str(root), "write-tree"),
                            error_code="INTEGRATION_WORKSPACE_FAILED",
                        ).stdout.strip().lower()
                        candidate = self._create_frozen_integration_commit(
                            provider,
                            root,
                            integration,
                            expected_tree=expected_tree,
                        )
                        self._verify_frozen_integration_commit(
                            provider,
                            root,
                            integration,
                            candidate,
                            expected_tree=expected_tree,
                        )
                        provider.runner.run(
                            (
                                "-C", str(root), "update-ref", "HEAD",
                                candidate, parents[0],
                            ),
                            error_code="INTEGRATION_WORKSPACE_FAILED",
                        )
                        head = candidate
                        integration_commit = head
                    expected_tree = provider.runner.run(
                        ("-C", str(root), "write-tree"),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    ).stdout.strip().lower()
                    self._verify_frozen_integration_commit(
                        provider,
                        root,
                        integration,
                        head,
                        expected_tree=expected_tree,
                    )
                    integration_commit = head
                    _tree_state, merge_head = self._git_path_state(provider, root)
                    if merge_head:
                        provider.runner.run(
                            ("-C", str(root), "merge", "--quit"),
                            environment={"LC_ALL": "C", "LANG": "C"},
                            error_code="INTEGRATION_WORKSPACE_FAILED",
                        )
                    clean = provider.runner.run(
                        (
                            "-C",
                            str(root),
                            "status",
                            "--porcelain=v1",
                            "--untracked-files=all",
                        ),
                        error_code="INTEGRATION_WORKSPACE_FAILED",
                    ).stdout
                    if clean:
                        raise RuntimeError("managed integration workspace is dirty")
                    self._import_integration_commit(
                        provider,
                        repository,
                        root,
                        str(integration["integration_id"]),
                        integration_commit,
                    )
        except (
            ManagedGitError,
            WorkspaceError,
            RuntimeError,
            OSError,
            TimeoutError,
        ):
            # Once an exact frozen commit is verified and HEAD is advanced, a
            # mirror-import/pin or metadata-cleanup failure is retryable.  Do
            # not freeze contradictory FAILED evidence that still claims the
            # workspace is at the base commit.
            if integration_commit is not None:
                raise
            if failure is None:
                tree_state = str(artifact.get("working_tree_state") or "clean")
                if tree_state not in {"clean", "merging", "conflicted"}:
                    tree_state = "clean"
                if provider is not None and root.is_dir():
                    try:
                        tree_state, _merge_head = self._git_path_state(
                            provider, root
                        )
                    except (ManagedGitError, RuntimeError, OSError):
                        pass
                failure = (
                    "JOIN_INTEGRATION_FAILED",
                    {"code": "INTEGRATION_FAILED"},
                    tree_state,
                )

        if failure is not None:
            reason_code, normalized_error, tree_state = failure

            def fail(
                candidate: dict[str, Any], _connection: sqlite3.Connection
            ) -> bool:
                current = _records_by(
                    candidate, "integrations", "integration_id"
                ).get(str(integration["integration_id"]))
                current_artifact = _records_by(
                    candidate,
                    "integration_workspaces",
                    "workspace_artifact_id",
                ).get(workspace_id)
                if (
                    not isinstance(current, dict)
                    or current.get("status") != "PREPARED"
                    or not isinstance(current_artifact, dict)
                ):
                    return False
                current.update(
                    {
                        "status": (
                            "CONFLICT"
                            if reason_code == "JOIN_MERGE_CONFLICT"
                            else "FAILED"
                        ),
                        "normalized_error": deepcopy(normalized_error),
                        "updated_at": now,
                    }
                )
                current_artifact.update(
                    {
                        "artifact_status": "preserved",
                        "working_tree_state": tree_state,
                        "verified_at": now,
                    }
                )
                self._append_integration_failure_context(
                    candidate,
                    current,
                    current_artifact,
                    reason_code=reason_code,
                    normalized_error=normalized_error,
                    now=now,
                )
                return True

            _state, changed = self.store.mutate_runtime_state(
                project_id, sprint_id, fail
            )
            return bool(changed)

        assert integration_commit is not None
        generations = [
            int(item["generation"])
            for item in state["workflow"]["occurrences"]
            if isinstance(item, Mapping)
            and item.get("node_id") == target_node_id
            and isinstance(item.get("generation"), int)
        ]
        generation = max(generations, default=0) + 1
        occurrence_id = managed_occurrence_id(
            sprint_id,
            graph_revision,
            target_node_id,
            generation,
            list(trigger_ids),
        )
        try:
            prepared = self._prepare_assignment(
                project_id,
                state,
                occurrence_id=occurrence_id,
                node_id=target_node_id,
                graph_revision=graph_revision,
                source_kind="integration",
                source_result_keys=result_keys,
                source_commit=integration_commit,
                integration_id=str(integration["integration_id"]),
            )
        except (
            ManagedContinuityError,
            WorkspaceError,
            ManagedGitError,
            BranchLeaseError,
            ManagedPortReservationError,
            OSError,
            sqlite3.Error,
        ) as exc:
            return self._record_successor_preparation_failure(
                project_id,
                sprint_id,
                target_node_id=target_node_id,
                trigger_token_ids=trigger_ids,
                result_keys=result_keys,
                phase="ASSIGNMENT_PREPARE",
                failure_code=str(
                    getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                ),
            )
        reservation_token: Any | None = None
        if prepared.port_leases:
            try:
                reservation_token = self.importer.port_reservations.acquire_many(
                    self.store.database_path, list(prepared.port_leases)
                )
            except ManagedPortReservationError as exc:
                return self._record_successor_preparation_failure(
                    project_id,
                    sprint_id,
                    target_node_id=target_node_id,
                    trigger_token_ids=trigger_ids,
                    result_keys=result_keys,
                    phase="PORT_RESERVATION",
                    failure_code=str(
                        getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                    ),
                )
        try:

            def commit(
                candidate: dict[str, Any], connection: sqlite3.Connection
            ) -> str:
                if candidate.get("graph_revision") != graph_revision:
                    raise RuntimeError("managed graph changed during integration")
                current = _records_by(
                    candidate, "integrations", "integration_id"
                ).get(str(integration["integration_id"]))
                current_artifact = _records_by(
                    candidate,
                    "integration_workspaces",
                    "workspace_artifact_id",
                ).get(workspace_id)
                if (
                    not isinstance(current, dict)
                    or current.get("status") != "PREPARED"
                    or not isinstance(current_artifact, dict)
                ):
                    raise RuntimeError("managed integration state changed")
                selected = self._ready_tokens(candidate, target_node_id)
                if tuple(str(item["token_id"]) for item in selected) != trigger_ids:
                    raise RuntimeError("managed integration token selection changed")
                target_node = _node(candidate, graph_revision, target_node_id)
                occurrence = {
                    "occurrence_id": occurrence_id,
                    "node_id": target_node_id,
                    "graph_revision": graph_revision,
                    "generation": generation,
                    "activation_policy": str(
                        target_node.get("activation_policy") or "all_parents"
                    ),
                    "trigger_token_ids": list(trigger_ids),
                    "state": "active",
                    "assignment_ids": [],
                    "created_at": now,
                    "completed_at": None,
                }
                candidate["workflow"]["occurrences"].append(occurrence)
                event_id = self._commit_prepared_assignment(
                    candidate,
                    connection,
                    prepared,
                    occurrence=occurrence,
                    now=now,
                )
                tokens = _records_by(
                    candidate["workflow"], "transition_tokens", "token_id"
                )
                journals = _records_by(
                    candidate, "transition_journal", "result_key"
                )
                for token_id in trigger_ids:
                    token = tokens[token_id]
                    token.update(
                        {
                            "status": "consumed",
                            "target_graph_revision": graph_revision,
                            "consumed_by_occurrence_id": occurrence_id,
                        }
                    )
                    journal = journals[str(token["result_key"])]
                    journal.update(
                        {
                            "state": "NEXT_ASSIGNMENT_ENQUEUED",
                            "target_occurrence_ids": [occurrence_id],
                            "outbox_event_ids": [event_id],
                            "updated_at": now,
                        }
                    )
                current.update(
                    {
                        "status": "COMMITTED",
                        "integration_commit": integration_commit,
                        "target_occurrence_id": occurrence_id,
                        "assignment_id": prepared.assignment["assignment_id"],
                        "normalized_error": None,
                        "updated_at": now,
                    }
                )
                current_artifact.update(
                    {
                        "head_commit": integration_commit,
                        "artifact_status": "active",
                        "working_tree_state": "clean",
                        "verified_at": now,
                    }
                )
                return event_id

            self.store.mutate_runtime_state(project_id, sprint_id, commit)
        except BaseException as exc:
            if reservation_token is not None:
                self.importer.port_reservations.rollback(reservation_token)
            if not isinstance(
                exc,
                (
                    ManagedContinuityError,
                    WorkspaceError,
                    ManagedGitError,
                    BranchLeaseError,
                    ManagedPortReservationError,
                    OSError,
                    sqlite3.Error,
                ),
            ):
                raise
            return self._record_successor_preparation_failure(
                project_id,
                sprint_id,
                target_node_id=target_node_id,
                trigger_token_ids=trigger_ids,
                result_keys=result_keys,
                phase="ASSIGNMENT_COMMIT",
                failure_code=str(
                    getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                ),
            )
        if reservation_token is not None:
            self.importer.port_reservations.mark_durable_many(
                self.store.database_path, list(prepared.port_leases)
            )
        self._publish_prepared_assignment(state, prepared)
        return True

    def _record_prepared_integration(
        self,
        project_id: str,
        sprint_id: str,
        state: Mapping[str, Any],
        *,
        target_node_id: str,
        selected: Sequence[Mapping[str, Any]],
        result_keys: Sequence[str],
        commits: Sequence[str],
    ) -> bool:
        graph_revision = int(state["graph_revision"])
        trigger_ids = tuple(str(token["token_id"]) for token in selected)
        now = _timestamp(self.clock)
        integration, artifact = self._prepare_integration_pair(
            project_id,
            state,
            target_node_id=target_node_id,
            graph_revision=graph_revision,
            tokens=selected,
            result_keys=result_keys,
            commits=commits,
            now=now,
        )

        def mutate(
            candidate: dict[str, Any], _connection: sqlite3.Connection
        ) -> bool:
            if candidate.get("graph_revision") != graph_revision:
                raise RuntimeError("managed graph changed during integration preparation")
            current_selected = self._ready_tokens(candidate, target_node_id)
            if tuple(
                str(token["token_id"]) for token in current_selected
            ) != trigger_ids:
                raise RuntimeError("managed integration token selection changed")
            existing = _records_by(
                candidate, "integrations", "integration_id"
            ).get(str(integration["integration_id"]))
            if existing is not None:
                return False
            candidate["integrations"].append(deepcopy(integration))
            candidate["integration_workspaces"].append(deepcopy(artifact))
            return True

        _state, changed = self.store.mutate_runtime_state(
            project_id, sprint_id, mutate
        )
        return bool(changed)

    @staticmethod
    def _has_successor_preparation_failure(
        state: Mapping[str, Any],
        *,
        target_node_id: str,
        trigger_token_ids: Sequence[str],
    ) -> bool:
        graph_revision = int(state["graph_revision"])
        expected_tokens = [str(item) for item in trigger_token_ids]
        return any(
            context.get("reason_code") == "SUCCESSOR_PREPARATION_FAILED"
            and isinstance(context.get("normalized_error"), Mapping)
            and context["normalized_error"].get("target_graph_revision")
            == graph_revision
            and context["normalized_error"].get("target_node_id")
            == target_node_id
            and context["normalized_error"].get("trigger_token_ids")
            == expected_tokens
            for context in state.get("coordinator_contexts", [])
            if isinstance(context, Mapping)
        )

    def _append_successor_preparation_failure_in_state(
        self,
        state: dict[str, Any],
        connection: sqlite3.Connection,
        *,
        target_node_id: str,
        trigger_token_ids: Sequence[str],
        result_keys: Sequence[str],
        phase: str,
        failure_code: str,
        now: str,
    ) -> bool:
        """Append one atomic scheduler-failure witness to a mutable state."""

        if not result_keys or len(result_keys) != len(trigger_token_ids):
            raise RuntimeError("managed scheduler failure binding is incomplete")
        graph_revision = int(state["graph_revision"])
        stable_failure_code = (
            failure_code
            if re.fullmatch(r"[A-Z][A-Z0-9_]*", failure_code)
            else CONTINUITY_RUNTIME_FAILED
        )
        result_key = str(result_keys[0])
        receipt = _records_by(state, "result_receipts", "result_key").get(
            result_key
        )
        source_id = (
            receipt.get("assignment_id")
            if isinstance(receipt, Mapping)
            else None
        )
        source = (
            _records_by(state, "assignments", "assignment_id").get(source_id)
            if isinstance(source_id, str)
            else None
        )
        journal = _records_by(state, "transition_journal", "result_key").get(
            result_key
        )
        if (
            not isinstance(source, Mapping)
            or not isinstance(journal, Mapping)
            or journal.get("state") != "TRANSITION_COMMITTED"
        ):
            raise RuntimeError("managed scheduler failure owner changed")
        normalized_error = {
            "code": "SUCCESSOR_PREPARATION_FAILED",
            "failure_code": stable_failure_code,
            "phase": phase,
            "target_graph_revision": graph_revision,
            "target_node_id": target_node_id,
            "trigger_token_ids": [str(item) for item in trigger_token_ids],
        }
        if any(
            context.get("reason_code") == "SUCCESSOR_PREPARATION_FAILED"
            and context.get("failed_assignment_id") == source_id
            and context.get("failure_scope") == "prepare"
            and context.get("normalized_error") == normalized_error
            for context in state.get("coordinator_contexts", [])
            if isinstance(context, Mapping)
        ):
            return False
        identity_salt = _stable_id(
            "scheduler-boundary",
            graph_revision,
            target_node_id,
            *trigger_token_ids,
            phase,
            compact=True,
        )
        self._append_assignment_failure_context(
            state,
            connection,
            source,
            result_key=result_key,
            reason_code="SUCCESSOR_PREPARATION_FAILED",
            normalized_error=normalized_error,
            now=now,
            failure_scope="prepare",
            identity_salt=identity_salt,
            increment_existing_blocker=False,
        )
        return True

    def _record_successor_preparation_failure(
        self,
        project_id: str,
        sprint_id: str,
        *,
        target_node_id: str,
        trigger_token_ids: Sequence[str],
        result_keys: Sequence[str],
        phase: str,
        failure_code: str,
    ) -> bool:
        """Route deterministic successor preparation failure to Coordinator."""

        if not result_keys or len(result_keys) != len(trigger_token_ids):
            return False
        now = _timestamp(self.clock)

        def mutate(
            state: dict[str, Any], connection: sqlite3.Connection
        ) -> bool:
            graph_revision = int(state["graph_revision"])
            token_by_id = _records_by(
                state["workflow"], "transition_tokens", "token_id"
            )
            current_tokens = [
                token_by_id.get(str(token_id)) for token_id in trigger_token_ids
            ]
            if any(
                not isinstance(token, Mapping)
                or token.get("status") != "available"
                or token.get("target_node_id") != target_node_id
                or token.get("target_graph_revision") is not None
                or token.get("consumed_by_occurrence_id") is not None
                for token in current_tokens
            ):
                raise RuntimeError("managed scheduler failure tokens changed")
            current_result_keys = [
                str(token["result_key"])
                for token in current_tokens
                if isinstance(token, Mapping)
            ]
            if current_result_keys != [str(item) for item in result_keys]:
                raise RuntimeError("managed scheduler failure results changed")
            return self._append_successor_preparation_failure_in_state(
                state,
                connection,
                target_node_id=target_node_id,
                trigger_token_ids=trigger_token_ids,
                result_keys=result_keys,
                phase=phase,
                failure_code=failure_code,
                now=now,
            )

        _state, changed = self.store.mutate_runtime_state(
            project_id, sprint_id, mutate
        )
        return bool(changed)

    def _schedule_one(
        self, project_id: str, sprint_id: str, state: Mapping[str, Any]
    ) -> bool:
        available = [
            token
            for token in state["workflow"]["transition_tokens"]
            if isinstance(token, dict) and token.get("status") == "available"
        ]
        if not available:
            return False
        target_node_ids = sorted(
            {str(token["target_node_id"]) for token in available}
        )
        graph_revision = int(state["graph_revision"])
        receipts = _records_by(state, "result_receipts", "result_key")
        selected: list[dict[str, Any]] = []
        target_node_id = ""
        target_node: Mapping[str, Any] | None = None
        trigger_ids: list[str] = []
        result_keys: list[str] = []
        commits: list[str] = []
        for candidate in target_node_ids:
            candidate_tokens = self._ready_tokens(state, candidate)
            if not candidate_tokens:
                continue
            candidate_node = _node(state, graph_revision, candidate)
            candidate_trigger_ids = [
                str(token["token_id"]) for token in candidate_tokens
            ]
            candidate_result_keys = [
                str(token["result_key"]) for token in candidate_tokens
            ]
            candidate_commits = [
                str(receipts[key]["result_commit"])
                for key in candidate_result_keys
            ]
            if self._has_successor_preparation_failure(
                state,
                target_node_id=candidate,
                trigger_token_ids=candidate_trigger_ids,
            ):
                continue
            if len(set(candidate_commits)) != 1:
                workspace = candidate_node.get("workspace")
                strategy = (
                    workspace.get("join_strategy", "require_same_commit")
                    if isinstance(workspace, Mapping)
                    else "require_same_commit"
                )
                if strategy == "require_same_commit":
                    if self._record_join_divergence_context(
                        project_id,
                        sprint_id,
                        state,
                        target_node_id=candidate,
                        trigger_token_ids=candidate_trigger_ids,
                    ):
                        return True
                    continue
                integration_id = managed_integration_id(
                    sprint_id,
                    graph_revision,
                    candidate,
                    candidate_trigger_ids,
                )
                if any(
                    item.get("integration_id") == integration_id
                    for item in state.get("integrations", [])
                    if isinstance(item, Mapping)
                ):
                    continue
                try:
                    if self._record_prepared_integration(
                        project_id,
                        sprint_id,
                        state,
                        target_node_id=candidate,
                        selected=candidate_tokens,
                        result_keys=candidate_result_keys,
                        commits=candidate_commits,
                    ):
                        return True
                except (
                    ManagedContinuityError,
                    WorkspaceError,
                    ManagedGitError,
                    BranchLeaseError,
                    ManagedPortReservationError,
                    OSError,
                    sqlite3.Error,
                ) as exc:
                    return self._record_successor_preparation_failure(
                        project_id,
                        sprint_id,
                        target_node_id=candidate,
                        trigger_token_ids=candidate_trigger_ids,
                        result_keys=candidate_result_keys,
                        phase="INTEGRATION_PREPARE",
                        failure_code=str(
                            getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                        ),
                    )
                continue
            selected = candidate_tokens
            target_node_id = candidate
            target_node = candidate_node
            trigger_ids = candidate_trigger_ids
            result_keys = candidate_result_keys
            commits = candidate_commits
            break
        if not selected:
            return False
        assert target_node is not None

        existing_generations = [
            int(item["generation"])
            for item in state["workflow"]["occurrences"]
            if isinstance(item, Mapping)
            and item.get("node_id") == target_node_id
            and isinstance(item.get("generation"), int)
        ]
        generation = max(existing_generations, default=0) + 1
        occurrence_id = managed_occurrence_id(
            sprint_id,
            graph_revision,
            target_node_id,
            generation,
            trigger_ids,
        )
        try:
            prepared = self._prepare_assignment(
                project_id,
                state,
                occurrence_id=occurrence_id,
                node_id=target_node_id,
                graph_revision=graph_revision,
                source_kind="accepted_result",
                source_result_keys=result_keys,
                source_commit=commits[0],
            )
        except (
            ManagedContinuityError,
            WorkspaceError,
            ManagedGitError,
            BranchLeaseError,
            ManagedPortReservationError,
            OSError,
            sqlite3.Error,
        ) as exc:
            return self._record_successor_preparation_failure(
                project_id,
                sprint_id,
                target_node_id=target_node_id,
                trigger_token_ids=trigger_ids,
                result_keys=result_keys,
                phase="ASSIGNMENT_PREPARE",
                failure_code=str(getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)),
            )
        reservation_token: Any | None = None
        if prepared.port_leases:
            try:
                reservation_token = self.importer.port_reservations.acquire_many(
                    self.store.database_path, list(prepared.port_leases)
                )
            except ManagedPortReservationError as exc:
                return self._record_successor_preparation_failure(
                    project_id,
                    sprint_id,
                    target_node_id=target_node_id,
                    trigger_token_ids=trigger_ids,
                    result_keys=result_keys,
                    phase="PORT_RESERVATION",
                    failure_code=str(
                        getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                    ),
                )
        now = _timestamp(self.clock)
        try:

            def mutate(candidate: dict[str, Any], connection: sqlite3.Connection) -> str:
                if candidate.get("graph_revision") != graph_revision:
                    raise RuntimeError("managed graph changed during scheduling")
                tokens = _records_by(
                    candidate["workflow"], "transition_tokens", "token_id"
                )
                current_tokens = [tokens.get(token_id) for token_id in trigger_ids]
                if any(
                    not isinstance(token, dict) or token.get("status") != "available"
                    for token in current_tokens
                ):
                    raise RuntimeError("managed trigger tokens changed during scheduling")
                occurrence = {
                    "occurrence_id": occurrence_id,
                    "node_id": target_node_id,
                    "graph_revision": graph_revision,
                    "generation": generation,
                    "activation_policy": str(
                        target_node.get("activation_policy") or "all_parents"
                    ),
                    "trigger_token_ids": trigger_ids,
                    "state": "active",
                    "assignment_ids": [],
                    "created_at": now,
                    "completed_at": None,
                }
                candidate["workflow"]["occurrences"].append(occurrence)
                event_id = self._commit_prepared_assignment(
                    candidate,
                    connection,
                    prepared,
                    occurrence=occurrence,
                    now=now,
                )
                journals = _records_by(
                    candidate, "transition_journal", "result_key"
                )
                for token in current_tokens:
                    assert isinstance(token, dict)
                    token.update(
                        {
                            "status": "consumed",
                            "target_graph_revision": graph_revision,
                            "consumed_by_occurrence_id": occurrence_id,
                        }
                    )
                    journal = journals.get(str(token["result_key"]))
                    if (
                        not isinstance(journal, dict)
                        or journal.get("state") != "TRANSITION_COMMITTED"
                    ):
                        raise RuntimeError("managed source journal is not schedulable")
                    journal.update(
                        {
                            "state": "NEXT_ASSIGNMENT_ENQUEUED",
                            "target_occurrence_ids": [occurrence_id],
                            "outbox_event_ids": [event_id],
                            "updated_at": now,
                        }
                    )
                return event_id

            self.store.mutate_runtime_state(project_id, sprint_id, mutate)
        except BaseException as exc:
            if reservation_token is not None:
                self.importer.port_reservations.rollback(reservation_token)
            if not isinstance(
                exc,
                (
                    ManagedContinuityError,
                    WorkspaceError,
                    ManagedGitError,
                    BranchLeaseError,
                    ManagedPortReservationError,
                    OSError,
                    sqlite3.Error,
                ),
            ):
                raise
            return self._record_successor_preparation_failure(
                project_id,
                sprint_id,
                target_node_id=target_node_id,
                trigger_token_ids=trigger_ids,
                result_keys=result_keys,
                phase="ASSIGNMENT_COMMIT",
                failure_code=str(
                    getattr(exc, "code", CONTINUITY_RUNTIME_FAILED)
                ),
            )
        if reservation_token is not None:
            self.importer.port_reservations.mark_durable_many(
                self.store.database_path, list(prepared.port_leases)
            )
        self._publish_prepared_assignment(state, prepared)
        self._fault(
            "after_successor_commit",
            occurrence_id=occurrence_id,
            assignment_id=prepared.assignment["assignment_id"],
        )
        return True

    def _record_join_divergence_context(
        self,
        project_id: str,
        sprint_id: str,
        state: Mapping[str, Any],
        *,
        target_node_id: str,
        trigger_token_ids: Sequence[str],
    ) -> bool:
        graph_revision = int(state["graph_revision"])
        context_id = _stable_id(
            "context-join", sprint_id, graph_revision, target_node_id, *trigger_token_ids,
            compact=True,
        )
        if any(
            item.get("context_id") == context_id
            for item in state.get("coordinator_contexts", [])
            if isinstance(item, Mapping)
        ):
            return False
        definition = _graph_revision(state, graph_revision)["definition"]
        coordinator = definition["coordinator"]
        coordinator_node = next(
            item
            for item in definition["nodes"]
            if item.get("id") == coordinator["node_id"]
        )
        now = _timestamp(self.clock)

        def mutate(candidate: dict[str, Any], _connection: sqlite3.Connection) -> bool:
            if any(
                item.get("context_id") == context_id
                for item in candidate["coordinator_contexts"]
            ):
                return False
            tokens = _records_by(
                candidate["workflow"], "transition_tokens", "token_id"
            )
            if any(
                token_id not in tokens
                or tokens[token_id].get("status") != "available"
                for token_id in trigger_token_ids
            ):
                raise RuntimeError("managed join evidence changed")
            candidate["coordinator_contexts"].append(
                {
                    "context_id": context_id,
                    "graph_revision": graph_revision,
                    "failure_scope": "integration",
                    "reason_code": "JOIN_SOURCE_DIVERGED",
                    "import_attempt_id": None,
                    "failed_assignment_id": None,
                    "join_target": {
                        "target_graph_revision": graph_revision,
                        "target_node_id": target_node_id,
                        "trigger_token_ids": list(trigger_token_ids),
                        "integration_id": None,
                    },
                    "assigned_branch": None,
                    "source_commit": None,
                    "result_commit": None,
                    "diff_summary": {},
                    "workspace_status": {},
                    "process_records": [],
                    "port_records": [],
                    "test_evidence_summary": {},
                    "normalized_error": {
                        "code": "JOIN_SOURCE_DIVERGED",
                        "message": "accepted parents have distinct commits",
                    },
                    "reviewer_feedback": [],
                }
            )
            candidate["outbox"].append(
                {
                    "event_id": _stable_id("event-coordinator", context_id),
                    "dedupe_key": f"enqueue:coordinator:{sprint_id}:{context_id}",
                    "event_type": "COORDINATOR_ENQUEUE",
                    "payload": {
                        "sprint_id": sprint_id,
                        "context_id": context_id,
                        "coordinator_phone": str(coordinator_node["agent"]["phone"]),
                        "reason_code": "JOIN_SOURCE_DIVERGED",
                    },
                    "status": "pending",
                    "created_at": now,
                    "delivered_at": None,
                    "queue_receipt_id": None,
                }
            )

            return True

        _state, changed = self.store.mutate_runtime_state(
            project_id, sprint_id, mutate
        )
        return bool(changed)

    def _finalize_terminal(
        self, project_id: str, sprint_id: str, state: Mapping[str, Any]
    ) -> bool:
        if state.get("status") != "active":
            return False
        target_status = self._terminal_status_when_quiescent(state)
        if target_status is None:
            return False
        emitted = {
            token.get("source_occurrence_id")
            for token in state["workflow"]["transition_tokens"]
        }
        terminally_settled = emitted | self._rework_limit_occurrence_ids(state)
        if any(
            occurrence.get("occurrence_id") not in terminally_settled
            for occurrence in state["workflow"]["occurrences"]
        ):
            return False

        def mutate(candidate: dict[str, Any], _connection: sqlite3.Connection) -> bool:
            if candidate.get("status") != "active":
                return False
            candidate["status"] = target_status
            return True

        _state, changed = self.store.mutate_runtime_state(
            project_id, sprint_id, mutate
        )
        return bool(changed)

    def _settle_superseded_pending_recoveries(
        self, project_id: str, sprint_id: str, state: Mapping[str, Any]
    ) -> bool:
        """Fail legacy pending choices after another choice already completed."""

        settled_context_ids = {
            str(recovery["context_id"])
            for recovery in state.get("recovery_records", [])
            if isinstance(recovery, Mapping)
            and _recovery_settles_context(state, recovery)
            and isinstance(recovery.get("context_id"), str)
        }
        stale_ids = {
            str(recovery["recovery_id"])
            for recovery in state.get("recovery_records", [])
            if isinstance(recovery, Mapping)
            and recovery.get("status") == "pending"
            and recovery.get("context_id") in settled_context_ids
            and self._published_repair_for_pending_recovery(state, recovery)
            is None
            and isinstance(recovery.get("recovery_id"), str)
        }
        if not stale_ids:
            return False
        failed_at = _timestamp(self.clock)

        def mutate(
            candidate: dict[str, Any], _connection: sqlite3.Connection
        ) -> bool:
            settled = {
                str(recovery["context_id"])
                for recovery in candidate.get("recovery_records", [])
                if isinstance(recovery, Mapping)
                and _recovery_settles_context(candidate, recovery)
                and isinstance(recovery.get("context_id"), str)
            }
            changed = False
            for recovery in candidate.get("recovery_records", []):
                if (
                    isinstance(recovery, dict)
                    and recovery.get("recovery_id") in stale_ids
                    and recovery.get("status") == "pending"
                    and recovery.get("context_id") in settled
                    and self._published_repair_for_pending_recovery(
                        candidate, recovery
                    )
                    is None
                ):
                    recovery.update(
                        {
                            "produced_record_ids": [],
                            "response": None,
                            "normalized_error": {
                                "code": RECOVERY_CONFLICT,
                                "http_status": 409,
                            },
                            "evidence": {},
                            "status": "failed",
                            "completed_at": failed_at,
                        }
                    )
                    changed = True
            if changed:
                terminal_status = self._terminal_status_when_quiescent(candidate)
                if terminal_status is not None:
                    candidate["status"] = terminal_status
            return changed

        _state, changed = self.store.mutate_runtime_state(
            project_id, sprint_id, mutate, require_active=False
        )
        return bool(changed)

    def _record_exhausted_process_failure(
        self, project_id: str, sprint_id: str, state: Mapping[str, Any]
    ) -> bool:
        """Create one Coordinator context when a process chain is exhausted."""

        processes = [
            item for item in state.get("processes", []) if isinstance(item, Mapping)
        ]
        assignments = _records_by(state, "assignments", "assignment_id")
        ports = _records_by(state, "port_leases", "lease_id")
        covered_process_ids = {
            str(context["normalized_error"]["process_id"])
            for context in state.get("coordinator_contexts", [])
            if isinstance(context, Mapping)
            and context.get("reason_code") == "PROCESS_RESTART_EXHAUSTED"
            and isinstance(context.get("normalized_error"), Mapping)
            and isinstance(context["normalized_error"].get("process_id"), str)
        }
        parent_ids = {
            str(item["restart_of_process_id"])
            for item in processes
            if isinstance(item.get("restart_of_process_id"), str)
        }
        exhausted = next(
            (
                item
                for item in processes
                if item.get("state") == "FAILED"
                and item.get("terminal_reason") == "failure"
                and isinstance(item.get("restart_attempt"), int)
                and not isinstance(item.get("restart_attempt"), bool)
                and isinstance(item.get("max_restart_attempts"), int)
                and not isinstance(item.get("max_restart_attempts"), bool)
                and int(item["restart_attempt"])
                == int(item["max_restart_attempts"])
                and str(item.get("process_id")) not in parent_ids
                and str(item.get("process_id")) not in covered_process_ids
                and isinstance(assignments.get(item.get("assignment_id")), Mapping)
                and assignments[item["assignment_id"]].get("status")
                in {"active", "reviews_pending"}
                and isinstance(ports.get(item.get("port_lease_id")), Mapping)
                and ports[item["port_lease_id"]].get("status") == "released"
            ),
            None,
        )
        if not isinstance(exhausted, Mapping):
            return False
        assignment_id = exhausted.get("assignment_id")
        if not isinstance(assignment_id, str):
            raise RuntimeError("managed exhausted process owner is corrupt")
        process_id = exhausted.get("process_id")
        if not isinstance(process_id, str):
            raise RuntimeError("managed exhausted process identity is corrupt")
        result_key = _stable_id("process-failure", assignment_id, process_id)
        reason_code = "PROCESS_RESTART_EXHAUSTED"
        graph_revision = int(state["graph_revision"])
        context_id = _stable_id(
            "context-assignment",
            sprint_id,
            graph_revision,
            assignment_id,
            result_key,
            reason_code,
            compact=True,
        )
        if any(
            item.get("context_id") == context_id
            for item in state.get("coordinator_contexts", [])
            if isinstance(item, Mapping)
        ):
            return False
        now = _timestamp(self.clock)

        def mutate(
            candidate: dict[str, Any], connection: sqlite3.Connection
        ) -> bool:
            if any(
                item.get("context_id") == context_id
                for item in candidate["coordinator_contexts"]
            ):
                return False
            if any(
                item.get("reason_code") == "PROCESS_RESTART_EXHAUSTED"
                and isinstance(item.get("normalized_error"), Mapping)
                and item["normalized_error"].get("process_id") == process_id
                for item in candidate["coordinator_contexts"]
            ):
                return False
            assignment = _records_by(
                candidate, "assignments", "assignment_id"
            ).get(assignment_id)
            current_processes = _records_by(
                candidate, "processes", "process_id"
            )
            current_process = current_processes.get(process_id)
            current_parent_ids = {
                str(item["restart_of_process_id"])
                for item in current_processes.values()
                if isinstance(item.get("restart_of_process_id"), str)
            }
            current_port = _records_by(
                candidate, "port_leases", "lease_id"
            ).get(
                current_process.get("port_lease_id")
                if isinstance(current_process, Mapping)
                else ""
            )
            if (
                not isinstance(assignment, Mapping)
                or assignment.get("status") not in {"active", "reviews_pending"}
                or not isinstance(current_process, Mapping)
                or current_process.get("state") != "FAILED"
                or current_process.get("terminal_reason") != "failure"
                or current_process.get("restart_attempt")
                != current_process.get("max_restart_attempts")
                or process_id in current_parent_ids
                or not isinstance(current_port, Mapping)
                or current_port.get("status") != "released"
            ):
                return False
            self._append_assignment_failure_context(
                candidate,
                connection,
                assignment,
                result_key=result_key,
                reason_code=reason_code,
                normalized_error={
                    "code": reason_code,
                    "phase": "PROCESS_SUPERVISION",
                    "process_id": process_id,
                    "restart_attempt": int(current_process["restart_attempt"]),
                    "max_restart_attempts": int(
                        current_process["max_restart_attempts"]
                    ),
                },
                now=now,
                failure_scope="process",
                increment_existing_blocker=False,
            )
            return True

        _state, changed = self.store.mutate_runtime_state(
            project_id, sprint_id, mutate
        )
        return bool(changed)

    def reconcile(
        self, project_id: str, sprint_id: str, *, max_steps: int = 256
    ) -> int:
        """Resume pending outbox and deterministic scheduler work."""

        with self._sprint_lock(sprint_id):
            return self._reconcile_locked(
                project_id, sprint_id, max_steps=max_steps
            )

    def _reconcile_locked(
        self, project_id: str, sprint_id: str, *, max_steps: int = 256
    ) -> int:
        """Reconcile while the caller owns the per-sprint continuity lock."""

        progress = 0
        for _ in range(max_steps):
            state = self.store.runtime_state(project_id, sprint_id)
            if state is None or state.get("status") != "active":
                return progress
            self._recover_assignment_publications(state)
            progress += self.drain_outbox(project_id, sprint_id)
            state = self.store.runtime_state(project_id, sprint_id)
            if state is None or state.get("status") != "active":
                return progress
            if self._settle_superseded_pending_recoveries(
                project_id, sprint_id, state
            ):
                progress += 1
                continue
            if self._record_exhausted_process_failure(
                project_id, sprint_id, state
            ):
                progress += 1
                continue
            if self._resume_one_prepared_integration(
                project_id, sprint_id, state
            ):
                progress += 1
                continue
            if self._schedule_one(project_id, sprint_id, state):
                progress += 1
                continue
            state = self.store.runtime_state(project_id, sprint_id)
            if state is not None and self._finalize_terminal(
                project_id, sprint_id, state
            ):
                progress += 1
                continue
            return progress
        raise RuntimeError("managed continuity reconciliation did not converge")

    def reconcile_all(self) -> dict[str, int]:
        results: dict[str, int] = {}
        for project_id, sprint_id in (
            self.store.superseded_pending_recovery_states()
        ):
            try:
                with self._sprint_lock(sprint_id, timeout=0.05):
                    state = self.store.runtime_state(project_id, sprint_id)
                    if state is None:
                        continue
                    changed = self._settle_superseded_pending_recoveries(
                        project_id, sprint_id, state
                    )
                if changed:
                    results[sprint_id] = results.get(sprint_id, 0) + 1
            except Exception:
                # Legacy repair is isolated to its own aggregate; one corrupt
                # historical row must not starve unrelated startup work.
                results[sprint_id] = -1
        for project_id, sprint_id, attempt_id in (
            self.store.retryable_import_attempts()
        ):
            try:
                with self._sprint_lock(sprint_id, timeout=0.05):
                    self._resume_retry_import_child(
                        project_id,
                        attempt_id,
                        correlation_id=attempt_id,
                    )
                    progress = 1 + self.drain_outbox(project_id, sprint_id)
                    progress += self._reconcile_locked(project_id, sprint_id)
                results[sprint_id] = results.get(sprint_id, 0) + progress
            except Exception:
                # A retry child is fenced to its own project and sprint.  One
                # unavailable repository must not starve unrelated runtimes.
                results[sprint_id] = -1
        for state in self.store.active_runtime_states():
            identity = state.get("identity")
            if not isinstance(identity, Mapping):
                raise RuntimeError("managed runtime project identity is missing")
            project_id = str(identity["project_id"])
            sprint_id = str(state["sprint_id"])
            try:
                progress = self.reconcile(project_id, sprint_id)
                if results.get(sprint_id) != -1:
                    results[sprint_id] = results.get(sprint_id, 0) + progress
            except Exception:
                # One recoverable managed sprint must not starve reconciliation
                # of unrelated sprints or HTTP service readiness.
                results[sprint_id] = -1
        return results

    @staticmethod
    def _repair_immutable_issue(
        state: Mapping[str, Any], patch: Mapping[str, Any]
    ) -> str | None:
        historical_node_ids = {
            str(item["node_id"])
            for item in state.get("assignments", [])
            if isinstance(item, Mapping) and isinstance(item.get("node_id"), str)
        }
        addressed_node_ids = {
            str(item["id"])
            for item in patch.get("future_nodes", [])
            if isinstance(item, Mapping) and isinstance(item.get("id"), str)
        }
        addressed_node_ids.update(
            str(item)
            for item in patch.get("remove_future_node_ids", [])
            if isinstance(item, str)
        )
        prompts = patch.get("prompts")
        if isinstance(prompts, Mapping):
            addressed_node_ids.update(str(item) for item in prompts)
        if historical_node_ids.intersection(addressed_node_ids):
            return REPAIR_IMMUTABLE_HISTORY

        historical_actor_ids = {
            str(item["agent_id"])
            for item in state.get("assignments", [])
            if isinstance(item, Mapping) and isinstance(item.get("agent_id"), str)
        }
        historical_actor_ids.update(
            str(item["reviewer_id"])
            for item in state.get("review_assignments", [])
            if isinstance(item, Mapping) and isinstance(item.get("reviewer_id"), str)
        )
        profiles = patch.get("profiles")
        if isinstance(profiles, Mapping) and historical_actor_ids.intersection(
            str(item) for item in profiles
        ):
            return REPAIR_IMMUTABLE_HISTORY
        return None

    def repair(
        self,
        sprint_id: str,
        payload: Mapping[str, Any] | bytes,
        correlation_id: str | None = None,
    ) -> ManagedContinuityResult:
        correlation = correlation_id or f"repair-{uuid4().hex}"
        with self._sprint_lock(sprint_id):
            return self._repair_locked(sprint_id, payload, correlation)

    def _resume_existing_repair_recovery(
        self,
        project_id: str,
        sprint_id: str,
        state: Mapping[str, Any],
        request: Mapping[str, Any],
        repair: Mapping[str, Any],
        recovery_context: Mapping[str, Any],
        *,
        recovery_id: str,
        recovery_context_id: str,
        correlation: str,
    ) -> ManagedContinuityResult:
        repair_id = repair.get("repair_id")
        from_revision = repair.get("from_revision")
        to_revision = repair.get("to_revision")
        repair_fingerprint = repair.get("request_fingerprint")
        if (
            not isinstance(repair_id, str)
            or not isinstance(from_revision, int)
            or isinstance(from_revision, bool)
            or not isinstance(to_revision, int)
            or isinstance(to_revision, bool)
            or to_revision != from_revision + 1
            or from_revision != request.get("expected_revision")
            or repair.get("repair_source_commit")
            != request.get("repair_source_commit")
            or repair.get("idempotency_key") != request.get("idempotency_key")
            or repair.get("patch") != request.get("patch")
            or not isinstance(repair_fingerprint, str)
        ):
            raise ManagedContinuityError(RECOVERY_CONFLICT, 409, correlation)
        try:
            materialized = apply_managed_repair_patch(
                _graph_revision(state, from_revision)["definition"],
                request["patch"],
            )
            applied_revision = _graph_revision(state, to_revision)
        except (KeyError, TypeError, ValueError) as exc:
            raise ManagedContinuityError(
                RECOVERY_CONFLICT, 409, correlation
            ) from exc
        if (
            applied_revision.get("repair_id") != repair_id
            or applied_revision.get("source") != "repair"
            or applied_revision.get("artifact_source_commit")
            != repair.get("repair_source_commit")
            or canonical_json_sha256(applied_revision["definition"])
            != canonical_json_sha256(materialized)
        ):
            raise ManagedContinuityError(RECOVERY_CONFLICT, 409, correlation)

        now = _timestamp(self.clock)
        join_plan: _JoinPlan | None = None
        adopted_join_effect: tuple[str, ...] | None = None
        if isinstance(recovery_context.get("join_target"), Mapping):
            adopted_join_effect = self._existing_repaired_join_effect(
                project_id,
                sprint_id,
                state,
                recovery_context,
                to_revision=to_revision,
                correlation=correlation,
            )
            if adopted_join_effect is None:
                try:
                    join_plan = self._prospective_repaired_join_plan(
                        project_id,
                        state,
                        applied_revision["definition"],
                        recovery_context,
                        to_revision=to_revision,
                        now=now,
                        repair_already_applied=True,
                    )
                except ManagedContinuityError as exc:
                    raise ManagedContinuityError(
                        exc.code, exc.http_status, correlation
                    ) from exc

        prepared_successor = (
            join_plan.prepared_assignment if join_plan is not None else None
        )
        reservation_token: Any | None = None
        if prepared_successor is not None and prepared_successor.port_leases:
            try:
                reservation_token = self.importer.port_reservations.acquire_many(
                    self.store.database_path,
                    list(prepared_successor.port_leases),
                )
            except ManagedPortReservationError as exc:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 409, correlation
                ) from exc

        def complete(
            candidate: dict[str, Any], connection: sqlite3.Connection
        ) -> dict[str, Any]:
            current_recovery = self._recovery_for_completion(
                candidate,
                recovery_id,
                str(recovery_context_id),
                correlation,
                supersede_pending_at=now,
            )
            current_repair = _records_by(
                candidate, "repairs", "repair_id"
            ).get(repair_id)
            if (
                current_recovery.get("status") != "pending"
                or current_recovery.get("action") != "APPLY_REPAIR"
                or current_recovery.get("context_id") != recovery_context_id
                or current_recovery.get("parameters") != {"request": request}
                or not isinstance(current_repair, Mapping)
                or current_repair.get("request_fingerprint")
                != repair_fingerprint
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation
                )
            produced_ids = [repair_id]
            if adopted_join_effect is not None:
                current_effect = self._existing_repaired_join_effect(
                    project_id,
                    sprint_id,
                    candidate,
                    recovery_context,
                    to_revision=to_revision,
                    correlation=correlation,
                )
                if current_effect != adopted_join_effect:
                    raise ManagedContinuityError(
                        CONTINUITY_RUNTIME_FAILED, 503, correlation
                    )
                produced_ids.extend(adopted_join_effect)
            elif join_plan is not None:
                produced_ids = self._commit_repaired_join_effect(
                    candidate,
                    connection,
                    join_plan,
                    repair_id=repair_id,
                    now=now,
                )
            response = {
                "recovery_id": recovery_id,
                "action": "APPLY_REPAIR",
                "status": "RECOVERY_COMPLETED",
                "produced_record_ids": produced_ids,
                "deduplicated": False,
            }
            current_recovery.update(
                {
                    "produced_record_ids": produced_ids,
                    "response": deepcopy(response),
                    "normalized_error": None,
                    "evidence": {
                        "repair_id": repair_id,
                        "from_revision": from_revision,
                        "to_revision": to_revision,
                    },
                    "status": "completed",
                    "completed_at": now,
                }
            )
            return response

        try:
            _state, stored_response = self.store.mutate_runtime_state(
                project_id,
                sprint_id,
                complete,
                require_active=False,
            )
        except BaseException:
            if reservation_token is not None:
                self.importer.port_reservations.rollback(reservation_token)
            raise
        self._fault("after_recovery_commit", recovery_id=recovery_id)
        if reservation_token is not None:
            try:
                self.importer.port_reservations.mark_durable_many(
                    self.store.database_path,
                    list(
                        prepared_successor.port_leases
                        if prepared_successor
                        else ()
                    ),
                )
            except Exception:
                pass
        if prepared_successor is not None:
            try:
                self._publish_prepared_assignment(state, prepared_successor)
            except Exception:
                pass
        self._resume_after_durable_ack(project_id, sprint_id, reconcile=True)
        return ManagedContinuityResult(stored_response)

    def _existing_repaired_join_effect(
        self,
        project_id: str,
        sprint_id: str,
        state: Mapping[str, Any],
        recovery_context: Mapping[str, Any],
        *,
        to_revision: int,
        correlation: str,
    ) -> tuple[str, ...] | None:
        """Adopt an exact scheduler effect created after a direct repair.

        A pending APPLY_REPAIR receipt can coexist with the same repair already
        committed through the direct repair endpoint.  Direct repair then runs
        reconciliation, which may durably prepare or finish the repaired join
        before the pending recovery is replayed.  Treat that exact deterministic
        integration/workspace pair as the recovery's effect; never synthesize a
        duplicate from tokens that may already have been consumed.
        """

        join_target = recovery_context.get("join_target")
        if not isinstance(join_target, Mapping):
            return None
        target_node_id = join_target.get("target_node_id")
        trigger_token_ids = join_target.get("trigger_token_ids")
        if (
            not isinstance(target_node_id, str)
            or not isinstance(trigger_token_ids, list)
            or not trigger_token_ids
            or any(not isinstance(item, str) for item in trigger_token_ids)
            or join_target.get("target_graph_revision") != to_revision - 1
        ):
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )
        from_revision = to_revision - 1
        try:
            old_definition = _graph_revision(state, from_revision)["definition"]
            new_definition = _graph_revision(state, to_revision)["definition"]
        except (KeyError, TypeError, ValueError, RuntimeError) as exc:
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            ) from exc
        old_target = next(
            (
                item
                for item in old_definition.get("nodes", [])
                if isinstance(item, Mapping)
                and item.get("id") == target_node_id
            ),
            None,
        )
        new_target = next(
            (
                item
                for item in new_definition.get("nodes", [])
                if isinstance(item, Mapping)
                and item.get("id") == target_node_id
            ),
            None,
        )
        old_order = (
            old_target.get("join_parent_order")
            if isinstance(old_target, Mapping)
            else None
        )
        new_order = (
            new_target.get("join_parent_order")
            if isinstance(new_target, Mapping)
            else None
        )
        if (
            not isinstance(old_target, Mapping)
            or not isinstance(new_target, Mapping)
            or old_target.get("activation_policy", "all_parents")
            != "all_parents"
            or new_target.get("activation_policy", "all_parents")
            != "all_parents"
            or not isinstance(old_order, list)
            or new_order != old_order
            or self._inbound_parent_ids(old_definition, target_node_id)
            != self._inbound_parent_ids(new_definition, target_node_id)
        ):
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )

        tokens = _records_by(
            state.get("workflow", {}), "transition_tokens", "token_id"
        )
        receipts = _records_by(state, "result_receipts", "result_key")
        expected_parents: list[dict[str, str]] = []
        for index, token_id in enumerate(trigger_token_ids):
            token = tokens.get(token_id)
            result_key = (
                token.get("result_key") if isinstance(token, Mapping) else None
            )
            receipt = (
                receipts.get(result_key) if isinstance(result_key, str) else None
            )
            source_node_id = (
                token.get("source_node_id") if isinstance(token, Mapping) else None
            )
            result_commit = (
                receipt.get("result_commit")
                if isinstance(receipt, Mapping)
                else None
            )
            if (
                not isinstance(token, Mapping)
                or index >= len(new_order)
                or source_node_id != new_order[index]
                or token.get("target_node_id") != target_node_id
                or not isinstance(result_key, str)
                or not isinstance(result_commit, str)
            ):
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            expected_parents.append(
                {
                    "source_node_id": str(source_node_id),
                    "result_key": result_key,
                    "commit": result_commit,
                }
            )
        if len(expected_parents) != len(new_order):
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )
        result_keys = [item["result_key"] for item in expected_parents]
        commits = [item["commit"] for item in expected_parents]
        workspace_definition = new_target.get("workspace")
        join_strategy = (
            workspace_definition.get("join_strategy", "require_same_commit")
            if isinstance(workspace_definition, Mapping)
            else None
        )
        ready_ids = tuple(
            str(item["token_id"])
            for item in self._ready_tokens(state, target_node_id)
        )
        expected_trigger_ids = tuple(trigger_token_ids)

        if len(set(commits)) == 1:
            matching_occurrences = [
                item
                for item in state.get("workflow", {}).get("occurrences", [])
                if isinstance(item, Mapping)
                and item.get("graph_revision") == to_revision
                and item.get("node_id") == target_node_id
                and item.get("trigger_token_ids") == trigger_token_ids
            ]
            if not matching_occurrences:
                if ready_ids == expected_trigger_ids:
                    return None
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            if len(matching_occurrences) != 1:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            occurrence = matching_occurrences[0]
            occurrence_id = occurrence.get("occurrence_id")
            generation = occurrence.get("generation")
            assignment_ids = occurrence.get("assignment_ids")
            assignment_id = (
                assignment_ids[0]
                if isinstance(assignment_ids, list) and assignment_ids
                else None
            )
            expected_occurrence_id = (
                managed_occurrence_id(
                    sprint_id,
                    to_revision,
                    target_node_id,
                    generation,
                    trigger_token_ids,
                )
                if isinstance(generation, int) and not isinstance(generation, bool)
                else None
            )
            expected_assignment_id = (
                _stable_id(
                    "assignment",
                    sprint_id,
                    expected_occurrence_id,
                    target_node_id,
                    to_revision,
                    "accepted_result",
                    0,
                    ",".join(result_keys),
                    compact=True,
                )
                if isinstance(expected_occurrence_id, str)
                else None
            )
            assignment = (
                _records_by(state, "assignments", "assignment_id").get(
                    assignment_id
                )
                if isinstance(assignment_id, str)
                else None
            )
            expected_event_id = (
                _stable_id("event-assignment", sprint_id, assignment_id)
                if isinstance(assignment_id, str)
                else None
            )
            matching_events = [
                item
                for item in state.get("outbox", [])
                if isinstance(item, Mapping)
                and item.get("event_type") == "ASSIGNMENT_ENQUEUE"
                and isinstance(item.get("payload"), Mapping)
                and item["payload"].get("assignment_id") == assignment_id
            ]
            if (
                not isinstance(occurrence_id, str)
                or occurrence_id != expected_occurrence_id
                or occurrence.get("activation_policy") != "all_parents"
                or assignment_id != expected_assignment_id
                or not isinstance(assignment, Mapping)
                or assignment.get("occurrence_id") != occurrence_id
                or assignment.get("node_id") != target_node_id
                or assignment.get("graph_revision") != to_revision
                or assignment.get("source_kind") != "accepted_result"
                or assignment.get("source_result_keys") != result_keys
                or assignment.get("source_commit") != commits[0]
                or assignment.get("integration_id") is not None
                or assignment.get("rework_cycle") != 0
                or len(matching_events) != 1
                or matching_events[0].get("event_id") != expected_event_id
                or matching_events[0].get("dedupe_key")
                != f"enqueue:assignment:{sprint_id}:{assignment_id}"
                or matching_events[0]["payload"].get("sprint_id") != sprint_id
                or matching_events[0]["payload"].get("graph_revision")
                != to_revision
                or matching_events[0]["payload"].get("node_id")
                != target_node_id
                or matching_events[0]["payload"].get("agent_phone")
                != new_target.get("agent", {}).get("phone")
            ):
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            return occurrence_id, str(assignment_id), str(expected_event_id)

        if join_strategy == "require_same_commit":
            expected_context_id = _stable_id(
                "context-join",
                sprint_id,
                to_revision,
                target_node_id,
                *trigger_token_ids,
                compact=True,
            )
            expected_event_id = _stable_id(
                "event-coordinator", expected_context_id
            )
            matching_contexts = [
                item
                for item in state.get("coordinator_contexts", [])
                if isinstance(item, Mapping)
                and item.get("join_target")
                == {
                    "target_graph_revision": to_revision,
                    "target_node_id": target_node_id,
                    "trigger_token_ids": trigger_token_ids,
                    "integration_id": None,
                }
                and item.get("reason_code") == "JOIN_SOURCE_DIVERGED"
            ]
            matching_events = [
                item
                for item in state.get("outbox", [])
                if isinstance(item, Mapping)
                and item.get("event_type") == "COORDINATOR_ENQUEUE"
                and isinstance(item.get("payload"), Mapping)
                and item["payload"].get("context_id") == expected_context_id
            ]
            if not matching_contexts and not matching_events:
                if ready_ids == expected_trigger_ids:
                    return None
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            if (
                ready_ids != expected_trigger_ids
                or len(matching_contexts) != 1
                or matching_contexts[0].get("context_id") != expected_context_id
                or matching_contexts[0].get("graph_revision") != to_revision
                or matching_contexts[0].get("failure_scope") != "integration"
                or matching_contexts[0].get("normalized_error")
                != {
                    "code": "JOIN_SOURCE_DIVERGED",
                    "message": "accepted parents have distinct commits",
                }
                or len(matching_events) != 1
                or matching_events[0].get("event_id") != expected_event_id
                or matching_events[0].get("dedupe_key")
                != f"enqueue:coordinator:{sprint_id}:{expected_context_id}"
                or matching_events[0]["payload"].get("sprint_id") != sprint_id
                or matching_events[0]["payload"].get("reason_code")
                != "JOIN_SOURCE_DIVERGED"
            ):
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            return expected_context_id, expected_event_id

        if join_strategy != "merge_no_ff":
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )
        expected_integration_id = managed_integration_id(
            sprint_id,
            to_revision,
            target_node_id,
            trigger_token_ids,
        )
        expected_workspace_id = managed_integration_workspace_id(
            sprint_id,
            to_revision,
            target_node_id,
            trigger_token_ids,
        )
        integrations = [
            item
            for item in state.get("integrations", [])
            if isinstance(item, Mapping)
        ]
        workspaces = [
            item
            for item in state.get("integration_workspaces", [])
            if isinstance(item, Mapping)
        ]
        integration = next(
            (
                item
                for item in integrations
                if item.get("integration_id") == expected_integration_id
            ),
            None,
        )
        workspace = next(
            (
                item
                for item in workspaces
                if item.get("workspace_artifact_id") == expected_workspace_id
            ),
            None,
        )
        related_integrations = [
            item
            for item in integrations
            if (
            item.get("integration_id") == expected_integration_id
            or (
                item.get("target_graph_revision") == to_revision
                and item.get("target_node_id") == target_node_id
                and item.get("trigger_token_ids") == trigger_token_ids
            )
            )
        ]
        related_workspaces = [
            item
            for item in workspaces
            if (
            item.get("workspace_artifact_id") == expected_workspace_id
            or item.get("integration_id") == expected_integration_id
            )
        ]
        if integration is None and workspace is None:
            if related_integrations or related_workspaces:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            if ready_ids == expected_trigger_ids:
                return None
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )
        if (
            integration is None
            or workspace is None
            or len(related_integrations) != 1
            or len(related_workspaces) != 1
        ):
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )

        tokens = _records_by(
            state.get("workflow", {}), "transition_tokens", "token_id"
        )
        receipts = _records_by(state, "result_receipts", "result_key")
        expected_parents: list[dict[str, str]] = []
        for token_id in trigger_token_ids:
            token = tokens.get(token_id)
            if not isinstance(token, Mapping):
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            result_key = token.get("result_key")
            receipt = (
                receipts.get(result_key) if isinstance(result_key, str) else None
            )
            source_node_id = token.get("source_node_id")
            result_commit = (
                receipt.get("result_commit")
                if isinstance(receipt, Mapping)
                else None
            )
            if (
                token.get("target_node_id") != target_node_id
                or not isinstance(source_node_id, str)
                or not isinstance(result_key, str)
                or not isinstance(result_commit, str)
            ):
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 503, correlation
                )
            expected_parents.append(
                {
                    "source_node_id": source_node_id,
                    "result_key": result_key,
                    "commit": result_commit,
                }
            )

        repository = state.get("repository")
        if not isinstance(repository, Mapping):
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )
        valid = (
            join_strategy == "merge_no_ff"
            and integration.get("integration_id") == expected_integration_id
            and integration.get("target_node_id") == target_node_id
            and integration.get("target_graph_revision") == to_revision
            and integration.get("trigger_token_ids") == trigger_token_ids
            and integration.get("workspace_artifact_id") == expected_workspace_id
            and integration.get("strategy") == "merge_no_ff"
            and integration.get("status")
            in {"PREPARED", "COMMITTED", "CONFLICT", "FAILED"}
            and integration.get("parents") == expected_parents
            and workspace.get("workspace_artifact_id") == expected_workspace_id
            and workspace.get("integration_id") == expected_integration_id
            and workspace.get("project_id") == project_id
            and workspace.get("sprint_id") == sprint_id
            and workspace.get("repository_id") == repository.get("repository_id")
            and workspace.get("repository_remote")
            == repository.get("canonical_remote")
            and workspace.get("mirror_storage_key")
            == repository.get("mirror_storage_key")
            and workspace.get("base_commit") == expected_parents[0]["commit"]
        )
        if not valid:
            raise ManagedContinuityError(
                CONTINUITY_RUNTIME_FAILED, 503, correlation
            )
        return expected_integration_id, expected_workspace_id

    def _repair_locked(
        self,
        sprint_id: str,
        payload: Mapping[str, Any] | bytes,
        correlation: str,
        *,
        recovery_id: str | None = None,
        recovery_context_id: str | None = None,
    ) -> ManagedContinuityResult:
        request = (
            self.parse_body(payload, correlation)
            if isinstance(payload, bytes)
            else deepcopy(dict(payload))
        )
        self._validate_schema(request, _REPAIR_SCHEMA, correlation)
        resolved = self.store.runtime_state_by_sprint(sprint_id)
        if resolved is None:
            raise ManagedContinuityError(
                "MANAGED_SPRINT_NOT_FOUND", 404, correlation
            )
        project_id, state = resolved
        fingerprint = repair_request_fingerprint(
            sprint_id,
            int(request["expected_revision"]),
            str(request["repair_source_commit"]),
            request["patch"],
        )
        recovery_record: Mapping[str, Any] | None = None
        recovery_context: Mapping[str, Any] | None = None
        if recovery_id is not None:
            if recovery_context_id is None:
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation
                )
            recovery_record = self._recovery_for_completion(
                state,
                recovery_id,
                recovery_context_id,
                correlation,
            )
            recovery_context = _records_by(
                state, "coordinator_contexts", "context_id"
            ).get(str(recovery_context_id))
            if (
                not isinstance(recovery_record, Mapping)
                or recovery_record.get("status") != "pending"
                or recovery_record.get("action") != "APPLY_REPAIR"
                or recovery_record.get("parameters") != {"request": request}
                or not isinstance(recovery_context, Mapping)
                or recovery_record.get("context_id")
                != recovery_context.get("context_id")
            ):
                raise ManagedContinuityError(
                    RECOVERY_CONFLICT, 409, correlation
                )
        existing = next(
            (
                item
                for item in state.get("repairs", [])
                if isinstance(item, Mapping)
                and item.get("idempotency_key") == request["idempotency_key"]
            ),
            None,
        )
        if existing is not None:
            if existing.get("request_fingerprint") != fingerprint:
                raise ManagedContinuityError(
                    IDEMPOTENCY_KEY_CONFLICT, 409, correlation
                )
            if recovery_id is not None:
                assert isinstance(recovery_context, Mapping)
                return self._resume_existing_repair_recovery(
                    project_id,
                    sprint_id,
                    state,
                    request,
                    existing,
                    recovery_context,
                    recovery_id=recovery_id,
                    recovery_context_id=str(recovery_context_id),
                    correlation=correlation,
                )
            response = deepcopy(dict(existing["response"]))
            response["deduplicated"] = True
            self._resume_after_durable_ack(
                project_id, sprint_id, reconcile=True
            )
            return ManagedContinuityResult(response)
        if state.get("status") != "active":
            raise ManagedContinuityError(GRAPH_REVISION_CONFLICT, 409, correlation)
        if state.get("graph_revision") != request["expected_revision"]:
            raise ManagedContinuityError(GRAPH_REVISION_CONFLICT, 409, correlation)
        patch = request["patch"]
        immutable = self._repair_immutable_issue(state, patch)
        if immutable is not None:
            raise ManagedContinuityError(immutable, 409, correlation)
        current_definition = _graph_revision(
            state, int(request["expected_revision"])
        )["definition"]
        try:
            candidate_definition = apply_managed_repair_patch(
                current_definition, patch
            )
        except ValueError as exc:
            raise ManagedContinuityError(
                REPAIR_PATCH_CONFLICT, 409, correlation
            ) from exc
        schema_issues = managed_schema_errors(
            candidate_definition,
            "managed-workspace-sprint-v1.schema.json",
            issue_code="MANAGED_MANIFEST_SCHEMA_INVALID",
        )
        graph_issues = managed_graph_semantic_issues(candidate_definition)
        if schema_issues or graph_issues:
            issues = list(schema_issues) + [
                {
                    "code": code,
                    "path": "nodes",
                    "message": "Managed graph semantic validation failed",
                }
                for code in graph_issues
            ]
            raise ManagedContinuityError(
                REPAIR_PATCH_CONFLICT, 409, correlation, issues=issues
            )

        repository_record = state.get("repository")
        if not isinstance(repository_record, Mapping):
            raise RuntimeError("managed runtime repository identity is corrupt")
        provider = self.importer.provider_for_durable_repository(repository_record)
        try:
            repository = provider.ensure_mirror(
                str(repository_record["repository_id"]), fetch=True
            )
            repair_commit = provider.assert_commit(
                repository, str(request["repair_source_commit"])
            )
            for index, artifact in enumerate(candidate_definition.get("files", [])):
                content = provider.read_blob(
                    repository,
                    repair_commit,
                    str(artifact["path"]),
                    max_bytes=64 * 1024 * 1024,
                )
                if hashlib.sha256(content).hexdigest() != artifact.get("sha256"):
                    raise ManagedContinuityError(
                        REPAIR_PATCH_CONFLICT,
                        409,
                        correlation,
                        issues=[
                            {
                                "code": "MANIFEST_CHECKSUM_MISMATCH",
                                "path": f"files[{index}]",
                                "message": "Manifest file checksum does not match the repair commit",
                            }
                        ],
                    )
            provider.pin_commit(
                repository,
                repair_commit,
                owner_id=f"{sprint_id}:repair:{fingerprint}",
            )
        except ManagedContinuityError:
            raise
        except ManagedGitError as exc:
            raise ManagedContinuityError(
                REPAIR_PATCH_CONFLICT, 409, correlation
            ) from exc

        from_revision = int(request["expected_revision"])
        to_revision = from_revision + 1
        repair_id = _stable_id(
            "repair", sprint_id, request["idempotency_key"], fingerprint
        )
        now = _timestamp(self.clock)
        response = {
            "sprint_id": sprint_id,
            "from_revision": from_revision,
            "graph_revision": to_revision,
            "repair_source_commit": repair_commit,
            "deduplicated": False,
        }

        join_plan: _JoinPlan | None = None
        recovery_response: dict[str, Any] | None = None
        if recovery_id is not None:
            assert isinstance(recovery_context, Mapping)
            if isinstance(recovery_context.get("join_target"), Mapping):
                try:
                    join_plan = self._prospective_repaired_join_plan(
                        project_id,
                        state,
                        candidate_definition,
                        recovery_context,
                        to_revision=to_revision,
                        now=now,
                    )
                except ManagedContinuityError as exc:
                    raise ManagedContinuityError(
                        exc.code, exc.http_status, correlation
                    ) from exc

        prepared_successor = (
            join_plan.prepared_assignment if join_plan is not None else None
        )
        reservation_token: Any | None = None
        if prepared_successor is not None and prepared_successor.port_leases:
            try:
                reservation_token = self.importer.port_reservations.acquire_many(
                    self.store.database_path,
                    list(prepared_successor.port_leases),
                )
            except ManagedPortReservationError as exc:
                raise ManagedContinuityError(
                    CONTINUITY_RUNTIME_FAILED, 409, correlation
                ) from exc

        def mutate(candidate: dict[str, Any], connection: sqlite3.Connection) -> dict[str, Any]:
            prior = next(
                (
                    item
                    for item in candidate["repairs"]
                    if item.get("idempotency_key") == request["idempotency_key"]
                ),
                None,
            )
            if prior is not None:
                if prior.get("request_fingerprint") != fingerprint:
                    raise ManagedContinuityError(
                        IDEMPOTENCY_KEY_CONFLICT, 409, correlation
                    )
                replay = deepcopy(dict(prior["response"]))
                replay["deduplicated"] = True
                return replay
            if candidate.get("graph_revision") != from_revision:
                raise ManagedContinuityError(
                    GRAPH_REVISION_CONFLICT, 409, correlation
                )
            immutable_code = self._repair_immutable_issue(candidate, patch)
            if immutable_code is not None:
                raise ManagedContinuityError(immutable_code, 409, correlation)
            materialized = apply_managed_repair_patch(
                _graph_revision(candidate, from_revision)["definition"], patch
            )
            if canonical_json_sha256(materialized) != canonical_json_sha256(
                candidate_definition
            ):
                raise RuntimeError("managed repair materialization changed")
            candidate["repairs"].append(
                {
                    "repair_id": repair_id,
                    "from_revision": from_revision,
                    "to_revision": to_revision,
                    "repair_source_commit": repair_commit,
                    "idempotency_key": str(request["idempotency_key"]),
                    "request_fingerprint": fingerprint,
                    "patch": deepcopy(patch),
                    "response": deepcopy(response),
                    "created_at": now,
                }
            )
            candidate["graph_revisions"].append(
                {
                    "revision": to_revision,
                    "definition_sha256": canonical_json_sha256(materialized),
                    "definition": deepcopy(materialized),
                    "artifact_source_commit": repair_commit,
                    "created_at": now,
                    "source": "repair",
                    "repair_id": repair_id,
                }
            )
            candidate["graph_revision"] = to_revision
            candidate["workflow"]["graph_revision"] = to_revision
            current_nodes = {
                str(item["id"]): item
                for item in materialized["nodes"]
                if isinstance(item, Mapping)
            }
            for token in candidate["workflow"]["transition_tokens"]:
                if token.get("status") != "available":
                    continue
                target = current_nodes.get(str(token.get("target_node_id")))
                if isinstance(target, Mapping) and target.get(
                    "type", "task"
                ) == "terminal":
                    token.update(
                        {
                            "status": "terminal",
                            "target_graph_revision": to_revision,
                            "consumed_by_occurrence_id": None,
                        }
                    )
            stored_result: dict[str, Any] = deepcopy(response)
            if recovery_id is not None:
                current_recovery = self._recovery_for_completion(
                    candidate,
                    str(recovery_id),
                    str(recovery_context_id),
                    correlation,
                    supersede_pending_at=now,
                )
                if (
                    current_recovery.get("status") != "pending"
                    or current_recovery.get("context_id")
                    != recovery_context_id
                ):
                    raise ManagedContinuityError(
                        RECOVERY_CONFLICT, 409, correlation
                    )
                produced_ids = [repair_id]
                if join_plan is not None:
                    produced_ids = self._commit_repaired_join_effect(
                        candidate,
                        connection,
                        join_plan,
                        repair_id=repair_id,
                        now=now,
                    )
                recovery_response = {
                    "recovery_id": str(recovery_id),
                    "action": "APPLY_REPAIR",
                    "status": "RECOVERY_COMPLETED",
                    "produced_record_ids": produced_ids,
                    "deduplicated": False,
                }
                current_recovery.update(
                    {
                        "produced_record_ids": produced_ids,
                        "response": deepcopy(recovery_response),
                        "normalized_error": None,
                        "evidence": {
                            "repair_id": repair_id,
                            "from_revision": from_revision,
                            "to_revision": to_revision,
                        },
                        "status": "completed",
                        "completed_at": now,
                    }
                )
                stored_result = recovery_response
            terminal_status = self._terminal_status_when_quiescent(candidate)
            if terminal_status is not None:
                candidate["status"] = terminal_status
            preview_issues = managed_activation_invariant_issues(candidate)
            mapped = next(
                (
                    code
                    for code in preview_issues
                    if code
                    in {
                        REPAIR_IMMUTABLE_HISTORY,
                        REPAIR_LIVE_ASSIGNMENT_TARGET_INVALID,
                        REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE,
                    }
                ),
                None,
            )
            if mapped is not None:
                raise ManagedContinuityError(mapped, 409, correlation)
            if preview_issues:
                raise ManagedContinuityError(
                    REPAIR_PATCH_CONFLICT,
                    409,
                    correlation,
                    issues=[
                        {
                            "code": code,
                            "path": "patch",
                            "message": "Repair violates the managed runtime contract",
                        }
                        for code in preview_issues
                    ],
                )
            return deepcopy(stored_result)

        try:
            _state, stored_response = self.store.mutate_runtime_state(
                project_id, sprint_id, mutate
            )
        except BaseException:
            if reservation_token is not None:
                self.importer.port_reservations.rollback(reservation_token)
            raise
        self._fault("after_repair_commit", repair_id=repair_id)
        if reservation_token is not None:
            try:
                self.importer.port_reservations.mark_durable_many(
                    self.store.database_path,
                    list(
                        prepared_successor.port_leases
                        if prepared_successor
                        else ()
                    ),
                )
            except Exception:
                pass
        if prepared_successor is not None:
            try:
                self._publish_prepared_assignment(state, prepared_successor)
            except Exception:
                pass
        self._resume_after_durable_ack(project_id, sprint_id, reconcile=True)
        return ManagedContinuityResult(stored_response)

    def observe_blocker(
        self,
        project_id: str,
        sprint_id: str,
        assignment_id: str,
        reason_code: str,
        normalized_error: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Atomically count a stable blocker without auto-terminalizing work."""

        if (
            not re.fullmatch(r"[A-Z][A-Z0-9_]*", reason_code)
            or not normalized_error
        ):
            raise ValueError("blocker evidence is invalid")
        now = _timestamp(self.clock)

        def mutate(state: dict[str, Any], _connection: sqlite3.Connection) -> dict[str, Any]:
            if assignment_id not in _records_by(
                state, "assignments", "assignment_id"
            ):
                raise KeyError("managed blocker assignment was not found")
            return deepcopy(
                self._observe_blocker_in_state(
                    state,
                    assignment_id,
                    reason_code,
                    normalized_error,
                    now=now,
                )
            )

        _state, observation = self.store.mutate_runtime_state(
            project_id, sprint_id, mutate
        )
        return observation


__all__ = [
    "CONTINUITY_RUNTIME_FAILED",
    "GRAPH_REVISION_CONFLICT",
    "IDEMPOTENCY_KEY_CONFLICT",
    "INVALID_MANAGED_SPRINT_REQUEST",
    "MANAGED_ASSIGNMENT_NOT_FOUND",
    "MANAGED_IDENTITY_MISMATCH",
    "ManagedContinuityError",
    "ManagedContinuityResult",
    "ManagedContinuityRuntime",
    "REPAIR_IMMUTABLE_HISTORY",
    "REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE",
    "REPAIR_LIVE_ASSIGNMENT_TARGET_INVALID",
    "REPAIR_PATCH_CONFLICT",
    "RESULT_COMMIT_NOT_FOUND",
    "RESULT_CONFLICT",
    "RESULT_HEAD_MISMATCH",
    "RESULT_NOT_DESCENDANT",
    "REVIEW_CONFLICT",
]
