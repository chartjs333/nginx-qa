"""Pure helpers for assignment-preserving legacy scope amendments.

The legacy conditional-graph runtime stores the originally issued agent and
task snapshots as historical facts.  This module builds a separate effective
layer from two Git-bound sprint manifests and deliberately has no dependency
on FastAPI or :mod:`main`, so its invariants can be tested in isolation.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import hmac
import json
import os
from typing import Any, Mapping


SCOPE_CONTROL_SCHEMA_VERSION = 1
SCOPE_CONTROL_CAPABILITY = "legacy_scope_control_v1"
SCOPE_CONTROL_V2_SCHEMA_VERSION = 2
SCOPE_CONTROL_V2_CAPABILITY = "versioned_scope_control_v2"
SUPPORTED_SCOPE_CONTROL_VERSIONS = (1, 2)
SCOPE_CONTEXT_PRECEDENCE = "effective_scope_supersedes_conflicting_issued_scope"
ADMIN_TOKEN_ENV = "NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN"
ADMIN_TOKEN_HEADER = "X-Nginx-QA-Scope-Control-Token"
ROLE_TOKEN_ENV_PREFIX = "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_"
ROLE_TOKEN_HEADER = "X-Nginx-QA-Scope-Token"
REPOSITORY_MAP_ENV = "NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP"
MINIMUM_TOKEN_LENGTH = 32
EFFECTIVE_SCOPE_HASH_CONTRACT = {
    "algorithm": "SHA-256",
    "canonicalization": "json-sort-keys-utf8-no-whitespace",
    "input": "effective_core",
}


class LegacyScopeControlError(ValueError):
    """Stable, value-free validation error returned by the HTTP adapter."""

    def __init__(self, code: str, message: str, *, status_code: int = 409) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code

    def detail(self) -> dict[str, str]:
        return {"error": self.code, "message": self.message}


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_json_sha256(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def text_sha256(value: Any) -> str:
    return sha256_bytes(str(value or "").encode("utf-8"))


def role_token_environment_name(phone: Any) -> str:
    normalized = str(phone or "").strip()
    if not normalized or not normalized.isdigit():
        raise LegacyScopeControlError(
            "SCOPE_ROLE_PHONE_INVALID",
            "A governed role must have a numeric logical phone",
        )
    return f"{ROLE_TOKEN_ENV_PREFIX}{normalized}"


def configured_secret(environment_name: str) -> str | None:
    value = os.environ.get(environment_name)
    if value is None or len(value) < MINIMUM_TOKEN_LENGTH:
        return None
    return value


def require_configured_secret(environment_name: str) -> str:
    value = configured_secret(environment_name)
    if value is None:
        raise LegacyScopeControlError(
            "SCOPE_CONTROL_CREDENTIAL_UNAVAILABLE",
            "The required scope-control credential is not configured",
            status_code=503,
        )
    return value


def verify_secret(environment_name: str, supplied: Any) -> None:
    configured = require_configured_secret(environment_name)
    candidate = str(supplied or "")
    if not candidate or not hmac.compare_digest(configured, candidate):
        raise LegacyScopeControlError(
            "SCOPE_CONTROL_FORBIDDEN",
            "The supplied scope-control credential is invalid",
            status_code=403,
        )


def verify_admin_token(supplied: Any) -> None:
    verify_secret(ADMIN_TOKEN_ENV, supplied)


def verify_role_token(phone: Any, supplied: Any) -> None:
    verify_secret(role_token_environment_name(phone), supplied)


def require_role_tokens_configured(phones: list[str]) -> None:
    environment_names = [
        role_token_environment_name(phone) for phone in sorted(set(phones))
    ]
    configured = {
        environment_name: configured_secret(environment_name)
        for environment_name in environment_names
    }
    missing = [
        environment_name
        for environment_name, value in configured.items()
        if value is None
    ]
    if missing:
        # Do not include secret values. Environment variable names are safe and
        # make an operator preflight actionable.
        raise LegacyScopeControlError(
            "SCOPE_ROLE_CREDENTIALS_UNAVAILABLE",
            "Required role credentials are not configured: " + ", ".join(missing),
            status_code=503,
        )
    role_values = [str(configured[name]) for name in environment_names]
    admin_value = configured_secret(ADMIN_TOKEN_ENV)
    if len(set(role_values)) != len(role_values) or (
        admin_value is not None and admin_value in role_values
    ):
        raise LegacyScopeControlError(
            "SCOPE_CREDENTIALS_NOT_DISTINCT",
            "Admin and governed role credentials must all be distinct",
            status_code=503,
        )


def _manifest_reviewers(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    execution = manifest.get("execution")
    reviewers = execution.get("reviewers") if isinstance(execution, dict) else None
    if not isinstance(reviewers, list):
        raise LegacyScopeControlError(
            "SCOPE_MANIFEST_INVALID",
            "The source manifest does not contain execution.reviewers",
            status_code=400,
        )
    result: dict[str, dict[str, Any]] = {}
    for reviewer in reviewers:
        if not isinstance(reviewer, dict):
            raise LegacyScopeControlError(
                "SCOPE_MANIFEST_INVALID",
                "Every source-manifest reviewer must be an object",
                status_code=400,
            )
        reviewer_id = str(reviewer.get("id") or "").strip()
        if not reviewer_id or reviewer_id in result:
            raise LegacyScopeControlError(
                "SCOPE_MANIFEST_INVALID",
                "Source-manifest reviewer ids must be present and unique",
                status_code=400,
            )
        result[reviewer_id] = deepcopy(reviewer)
    return result


def _manifest_nodes(
    manifest: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    raw_nodes = manifest.get("nodes")
    if not isinstance(raw_nodes, list):
        raise LegacyScopeControlError(
            "SCOPE_MANIFEST_INVALID",
            "The source manifest does not contain nodes",
            status_code=400,
        )
    task_nodes: dict[str, dict[str, Any]] = {}
    terminal_nodes: dict[str, dict[str, Any]] = {}
    for node in raw_nodes:
        if not isinstance(node, dict):
            raise LegacyScopeControlError(
                "SCOPE_MANIFEST_INVALID",
                "Every source-manifest node must be an object",
                status_code=400,
            )
        node_id = str(node.get("id") or "").strip()
        if not node_id or node_id in task_nodes or node_id in terminal_nodes:
            raise LegacyScopeControlError(
                "SCOPE_MANIFEST_INVALID",
                "Source-manifest node ids must be present and unique",
                status_code=400,
            )
        if str(node.get("type") or "task").strip().lower() == "terminal":
            terminal_nodes[node_id] = deepcopy(node)
        else:
            task_nodes[node_id] = deepcopy(node)
    return task_nodes, terminal_nodes


def _replace_target_mutable_fields(
    manifest: Mapping[str, Any],
    *,
    node_ids: set[str],
    reviewer_ids: set[str],
    terminal_ids: set[str],
) -> dict[str, Any]:
    """Return an immutable-structure projection for allowlist comparison."""

    projected = deepcopy(dict(manifest))
    execution = projected.get("execution")
    reviewers = execution.get("reviewers") if isinstance(execution, dict) else []
    if isinstance(reviewers, list):
        for reviewer in reviewers:
            if (
                isinstance(reviewer, dict)
                and str(reviewer.get("id") or "").strip() in reviewer_ids
            ):
                reviewer["profile"] = "<scope-control-profile>"
    nodes = projected.get("nodes")
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id") or "").strip()
            if node_id in terminal_ids:
                node["message"] = "<scope-control-terminal-message>"
                continue
            if node_id not in node_ids:
                continue
            agent = node.get("agent")
            if isinstance(agent, dict):
                agent["profile"] = "<scope-control-profile>"
            raw_tasks = node.get("tasks")
            if raw_tasks is None and "task" in node:
                node["task"] = "<scope-control-task-message>"
            elif isinstance(raw_tasks, list):
                for index, task in enumerate(raw_tasks):
                    if isinstance(task, dict):
                        task["message"] = "<scope-control-task-message>"
                    elif isinstance(task, str):
                        raw_tasks[index] = "<scope-control-task-message>"
    return projected


def manifest_scope_delta(
    base_manifest: Mapping[str, Any],
    target_manifest: Mapping[str, Any],
    targets: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a profile/message-only manifest delta and return overlays.

    Any graph, identity, branch, phone, task id/queue/metadata, review policy,
    or non-target profile/message change makes the complete immutable
    projection differ and is rejected.
    """

    node_ids = {
        str(value).strip()
        for value in targets.get("future_node_ids", [])
        if str(value).strip()
    }
    reviewer_ids = {
        str(value).strip()
        for value in targets.get("reviewer_agent_ids", [])
        if str(value).strip()
    }
    terminal_ids = {
        str(value).strip()
        for value in targets.get("terminal_node_ids", [])
        if str(value).strip()
    }
    base_nodes, base_terminals = _manifest_nodes(base_manifest)
    target_nodes, target_terminals = _manifest_nodes(target_manifest)
    base_reviewers = _manifest_reviewers(base_manifest)
    target_reviewers = _manifest_reviewers(target_manifest)

    unknown_nodes = sorted(node_ids - set(base_nodes))
    unknown_reviewers = sorted(reviewer_ids - set(base_reviewers))
    unknown_terminals = sorted(terminal_ids - set(base_terminals))
    if unknown_nodes or unknown_reviewers or unknown_terminals:
        raise LegacyScopeControlError(
            "SCOPE_TARGET_NOT_FOUND",
            "Every scope target must exist in the base manifest",
            status_code=400,
        )
    if (
        set(base_nodes) != set(target_nodes)
        or set(base_reviewers) != set(target_reviewers)
        or set(base_terminals) != set(target_terminals)
    ):
        raise LegacyScopeControlError(
            "SCOPE_GRAPH_CHANGE_FORBIDDEN",
            "Scope amendments cannot add or remove graph identities",
        )

    base_projection = _replace_target_mutable_fields(
        base_manifest,
        node_ids=node_ids,
        reviewer_ids=reviewer_ids,
        terminal_ids=terminal_ids,
    )
    target_projection = _replace_target_mutable_fields(
        target_manifest,
        node_ids=node_ids,
        reviewer_ids=reviewer_ids,
        terminal_ids=terminal_ids,
    )
    if canonical_json_bytes(base_projection) != canonical_json_bytes(target_projection):
        raise LegacyScopeControlError(
            "SCOPE_GRAPH_CHANGE_FORBIDDEN",
            "The Git manifest delta contains changes outside the scope allowlist",
        )

    node_overrides: dict[str, dict[str, Any]] = {}
    for node_id in sorted(node_ids):
        target_node = target_nodes[node_id]
        target_agent = target_node.get("agent")
        if not isinstance(target_agent, dict):
            raise LegacyScopeControlError(
                "SCOPE_MANIFEST_INVALID",
                "Every targeted task node must contain an agent object",
                status_code=400,
            )
        tasks = target_node.get("tasks")
        if tasks is None and "task" in target_node:
            tasks = [target_node.get("task")]
        if not isinstance(tasks, list):
            raise LegacyScopeControlError(
                "SCOPE_MANIFEST_INVALID",
                "Every targeted task node must contain a tasks array",
                status_code=400,
            )
        node_overrides[node_id] = {
            "node_id": node_id,
            "agent_id": str(target_agent.get("id") or node_id).strip(),
            "agent_phone": str(target_agent.get("phone") or "").strip(),
            "git_branch": str(target_agent.get("git_branch") or "").strip(),
            "base_profile": str(
                (base_nodes[node_id].get("agent") or {}).get("profile") or ""
            ),
            "profile": str(target_agent.get("profile") or ""),
            "base_tasks": deepcopy(base_nodes[node_id].get("tasks") or []),
            "tasks": deepcopy(tasks),
        }

    reviewer_overrides: dict[str, dict[str, Any]] = {}
    for reviewer_id in sorted(reviewer_ids):
        reviewer = target_reviewers[reviewer_id]
        reviewer_overrides[reviewer_id] = {
            "agent_id": reviewer_id,
            "agent_phone": str(reviewer.get("phone") or "").strip(),
            "git_branch": str(reviewer.get("git_branch") or "").strip(),
            "base_profile": str(base_reviewers[reviewer_id].get("profile") or ""),
            "profile": str(reviewer.get("profile") or ""),
        }

    return {
        "node_overrides": node_overrides,
        "reviewer_overrides": reviewer_overrides,
        "terminal_overrides": {
            node_id: {
                "node_id": node_id,
                "base_message": str(base_terminals[node_id].get("message") or ""),
                "message": str(target_terminals[node_id].get("message") or ""),
            }
            for node_id in sorted(terminal_ids)
        },
    }


def replace_authored_profile(
    stored_profile: Any,
    base_authored_profile: Any,
    target_authored_profile: Any,
) -> str:
    """Replace only the imported authored prefix, preserving runtime suffixes."""

    stored = str(stored_profile or "")
    base = str(base_authored_profile or "")
    target = str(target_authored_profile or "")
    if base:
        if not stored.startswith(base):
            raise LegacyScopeControlError(
                "SCOPE_RUNTIME_BASE_MISMATCH",
                "The live authored profile does not match the Git base manifest",
            )
        return target + stored[len(base) :]
    if stored:
        raise LegacyScopeControlError(
            "SCOPE_RUNTIME_BASE_MISMATCH",
            "The live profile is not empty while the Git base profile is empty",
        )
    return target


def effective_agent_from_override(
    issued_agent: Mapping[str, Any],
    override: Mapping[str, Any],
) -> dict[str, Any]:
    effective = deepcopy(dict(issued_agent))
    if str(effective.get("id") or "").strip() != str(
        override.get("agent_id") or ""
    ).strip():
        raise LegacyScopeControlError(
            "SCOPE_RUNTIME_ACTOR_MISMATCH",
            "The live agent id does not match the Git-bound scope target",
        )
    expected_phone = str(override.get("agent_phone") or "").strip()
    live_phone = str(effective.get("phone") or "").strip()
    if expected_phone and expected_phone != live_phone:
        raise LegacyScopeControlError(
            "SCOPE_RUNTIME_ACTOR_MISMATCH",
            "The live agent phone does not match the Git-bound scope target",
        )
    expected_branch = str(override.get("git_branch") or "").strip()
    live_branch = str(
        effective.get("git_branch")
        or (effective.get("parameters") or {}).get("git_branch")
        or ""
    ).strip()
    if expected_branch and expected_branch != live_branch:
        raise LegacyScopeControlError(
            "SCOPE_RUNTIME_ACTOR_MISMATCH",
            "The live agent branch does not match the Git-bound scope target",
        )
    effective["profile"] = replace_authored_profile(
        effective.get("profile"),
        override.get("base_profile"),
        override.get("profile"),
    )
    if "tasks" in override:
        if canonical_json_bytes(effective.get("tasks") or []) != canonical_json_bytes(
            override.get("base_tasks") or []
        ):
            raise LegacyScopeControlError(
                "SCOPE_RUNTIME_BASE_MISMATCH",
                "The live tasks do not match the Git base manifest",
            )
        effective["tasks"] = deepcopy(override.get("tasks") or [])
    return effective


def active_amendment(scope_control: Mapping[str, Any]) -> dict[str, Any] | None:
    active_id = str(scope_control.get("active_amendment_id") or "").strip()
    for amendment in reversed(scope_control.get("amendments") or []):
        if (
            isinstance(amendment, dict)
            and str(amendment.get("amendment_id") or "").strip() == active_id
        ):
            return amendment
    return None


def amendment_by_id(
    scope_control: Mapping[str, Any], amendment_id: Any
) -> dict[str, Any] | None:
    normalized = str(amendment_id or "").strip()
    for amendment in reversed(scope_control.get("amendments") or []):
        if (
            isinstance(amendment, dict)
            and str(amendment.get("amendment_id") or "").strip() == normalized
        ):
            return amendment
    return None


def project_authored_active_task(
    issued_task: Mapping[str, Any],
    issued_agent: Mapping[str, Any],
    effective_agent: Mapping[str, Any],
) -> dict[str, Any]:
    """Deterministically project the authored text of an issued active task.

    The active-task projection is deliberately derived from the immutable
    issued snapshot plus the hash-bound effective profile/tasks.  Persisted
    projection bytes are therefore a cache that can be verified, never an
    independent instruction authority.
    """

    effective_task = deepcopy(dict(issued_task))
    message = str(effective_task.get("message") or "")
    replacements: list[tuple[str, str]] = [
        (
            str(issued_agent.get("profile") or ""),
            str(effective_agent.get("profile") or ""),
        )
    ]
    issued_tasks = [
        task for task in issued_agent.get("tasks", []) if isinstance(task, dict)
    ]
    effective_tasks = [
        task for task in effective_agent.get("tasks", []) if isinstance(task, dict)
    ]
    if len(issued_tasks) != len(effective_tasks):
        raise LegacyScopeControlError(
            "SCOPE_RUNTIME_BASE_MISMATCH",
            "Effective tasks cannot change the issued task count",
        )
    for issued, effective in zip(issued_tasks, effective_tasks):
        if str(issued.get("task_id") or "") != str(effective.get("task_id") or ""):
            raise LegacyScopeControlError(
                "SCOPE_GRAPH_CHANGE_FORBIDDEN",
                "Effective tasks cannot change task ids",
            )
        replacements.append(
            (str(issued.get("message") or ""), str(effective.get("message") or ""))
        )
    applied_replacements: dict[str, str] = {}
    for old, new in replacements:
        if old == new:
            continue
        if old in applied_replacements:
            if applied_replacements[old] != new:
                raise LegacyScopeControlError("SCOPE_ISSUED_TASK_MISMATCH", "Ambiguous authored text projection")
            continue
        if not old:
            # Managed roles may have no authored profile/task text at all.
            # There is no historical instruction to replace in that case.
            message = (new + "\n\n" + message).rstrip()
            applied_replacements[old] = new
            continue
        if old not in message:
            raise LegacyScopeControlError(
                "SCOPE_ISSUED_TASK_MISMATCH",
                "The issued active task cannot be projected from the authored snapshots",
            )
        message = message.replace(old, new)
        applied_replacements[old] = new
    effective_task["message"] = message
    return effective_task


def assignment_binding(
    state: Mapping[str, Any], assignment_id: Any
) -> dict[str, Any] | None:
    scope_control = state.get("scope_control")
    if not isinstance(scope_control, dict):
        return None
    if int(scope_control.get("schema_version") or 0) not in SUPPORTED_SCOPE_CONTROL_VERSIONS:
        raise LegacyScopeControlError(
            "SCOPE_CONTROL_VERSION_UNSUPPORTED",
            "The persisted scope-control version is not supported",
            status_code=503,
        )
    if int(scope_control.get("schema_version") or 0) == 2:
        validate_versioned_scope_history(scope_control)
    bindings = scope_control.get("assignment_bindings")
    binding = (
        bindings.get(str(assignment_id or "").strip())
        if isinstance(bindings, dict)
        else None
    )
    if not isinstance(binding, dict):
        return None
    normalized_assignment_id = str(assignment_id or "").strip()
    if str(binding.get("assignment_id") or "").strip() != normalized_assignment_id:
        raise LegacyScopeControlError(
            "SCOPE_BINDING_INVALID",
            "The assignment scope binding identity is inconsistent",
            status_code=503,
        )
    issued = binding.get("issued")
    effective = binding.get("effective")
    stored_core = binding.get("effective_core")
    context = binding.get("scope_context")
    if (
        not isinstance(issued, dict)
        or not isinstance(effective, dict)
        or not isinstance(stored_core, dict)
        or not isinstance(context, dict)
    ):
        raise LegacyScopeControlError(
            "SCOPE_BINDING_INVALID",
            "The assignment scope binding is incomplete",
            status_code=503,
        )
    try:
        computed_core = effective_scope_core(effective)
        computed_hash = canonical_json_sha256(computed_core)
        stored_contract = binding.get("effective_scope_hash_contract")
        issued_active_task = issued.get("active_task")
        stored_active_task = effective.get("active_task")
        delivery_projection = binding.get("delivery_projection")
        if issued_active_task is None and isinstance(delivery_projection, dict):
            issued_active_task = delivery_projection.get("task")
            projection_agent = delivery_projection.get("base_core")
        else:
            projection_agent = issued
        if issued_active_task is None and stored_active_task is None:
            active_task_projection_valid = True
        elif isinstance(issued_active_task, dict) and isinstance(
            stored_active_task, dict
        ):
            expected_active_task = project_authored_active_task(
                issued_active_task,
                projection_agent,
                effective,
            )
            active_task_projection_valid = canonical_json_bytes(
                expected_active_task
            ) == canonical_json_bytes(stored_active_task)
        else:
            active_task_projection_valid = False
        integrity_valid = (
            active_task_projection_valid
            and canonical_json_bytes(computed_core) == canonical_json_bytes(stored_core)
            and canonical_json_bytes(stored_contract)
            == canonical_json_bytes(EFFECTIVE_SCOPE_HASH_CONTRACT)
            and computed_hash == str(binding.get("effective_scope_sha256") or "")
            and computed_hash == str(context.get("effective_scope_sha256") or "")
            and canonical_json_sha256(context)
            == str(binding.get("scope_context_sha256") or "")
        )
    except (TypeError, ValueError, LegacyScopeControlError):
        integrity_valid = False
    if not integrity_valid:
        raise LegacyScopeControlError(
            "SCOPE_BINDING_INVALID",
            "The assignment scope binding integrity check failed",
            status_code=503,
        )
    return deepcopy(binding)


def store_assignment_binding(scope_control: dict[str, Any], binding: Mapping[str, Any]) -> None:
    """Append a revision snapshot; replace only the explicitly-current cache.

    Callers hold the runtime's write lock and persist the enclosing document
    atomically. The old snapshots are never amended in place, including during
    re-ACK or another amendment of the same still-active assignment.
    """
    assignment_id = str(binding.get("assignment_id") or "")
    if int(scope_control.get("schema_version") or 0) == 2:
        history = scope_control.setdefault("binding_history", {})
        records = history.setdefault(assignment_id, [])
        for existing in records:
            if existing.get("scope_context_sha256") == binding.get("scope_context_sha256"):
                if canonical_json_bytes(existing) != canonical_json_bytes(binding):
                    raise LegacyScopeControlError("SCOPE_HISTORY_CONFLICT", "A saved scope binding is immutable")
                return
        if records and canonical_json_bytes(records[0].get("issued")) != canonical_json_bytes(binding.get("issued")):
            raise LegacyScopeControlError("SCOPE_ISSUED_HISTORY_CONFLICT", "An assignment's originally issued instruction is immutable")
        records.append(deepcopy(dict(binding)))
    scope_control.setdefault("assignment_bindings", {})[assignment_id] = deepcopy(dict(binding))


def store_acknowledgement(scope_control: dict[str, Any], acknowledgement: Mapping[str, Any]) -> None:
    """Append exact-context ACK history without touching execution state."""
    if int(scope_control.get("schema_version") or 0) == 2:
        history = scope_control.setdefault("acknowledgement_history", {})
        ack_id = str(acknowledgement.get("ack_id") or "")
        existing = history.get(ack_id)
        if existing is not None and canonical_json_bytes(existing) != canonical_json_bytes(acknowledgement):
            raise LegacyScopeControlError("SCOPE_ACK_HISTORY_CONFLICT", "A saved acknowledgement is immutable")
        history[ack_id] = deepcopy(dict(acknowledgement))
    scope_control.setdefault("acknowledgements", {})[str(acknowledgement.get("assignment_id") or "")] = deepcopy(dict(acknowledgement))


def validate_versioned_scope_history(scope_control: Mapping[str, Any]) -> None:
    """Fail closed on missing lineage, edited snapshots, or invalid ACK links."""
    if int(scope_control.get("schema_version") or 0) != 2:
        return
    amendments = scope_control.get("amendments")
    if not isinstance(amendments, list) or not amendments:
        raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Versioned scope requires an amendment history", status_code=503)
    revision_map = {}
    for index, amendment in enumerate(amendments, 1):
        if not isinstance(amendment, dict) or amendment.get("effective_revision") != index:
            raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Scope revision lineage is incomplete", status_code=503)
        amendment_id = amendment.get("amendment_id")
        if not amendment_id or amendment_id in revision_map:
            raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Amendment identities must be unique", status_code=503)
        revision_map[amendment_id] = index
    if scope_control.get("effective_revision") != len(amendments) or scope_control.get("active_amendment_id") != amendments[-1].get("amendment_id"):
        raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "The active scope pointer is inconsistent", status_code=503)
    history = scope_control.get("binding_history")
    current = scope_control.get("assignment_bindings")
    ack_history = scope_control.get("acknowledgement_history")
    acks = scope_control.get("acknowledgements")
    if not all(isinstance(value, dict) for value in (history, current, ack_history, acks)) or set(history) != set(current):
        raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Versioned binding and ACK ledgers are incomplete", status_code=503)
    contexts = {}
    reviewed_sources = []
    for assignment_id, records in history.items():
        if not isinstance(records, list) or not records or canonical_json_bytes(records[-1]) != canonical_json_bytes(current[assignment_id]):
            raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Current binding does not match its history", status_code=503)
        previous_revision = 0
        for binding in records:
            validated = assignment_binding({"scope_control": {"schema_version": 1, "assignment_bindings": {assignment_id: binding}}}, assignment_id)
            revision = int(validated.get("effective_revision") or 0)
            if revision <= previous_revision or revision_map.get(validated.get("amendment_id")) != revision:
                raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Assignment revision lineage is inconsistent", status_code=503)
            context = binding["scope_context"]
            amendment = amendments[revision - 1]
            source = amendment.get("source") or {}
            if any(context.get(field) != binding.get(field) for field in ("assignment_id", "agent_id", "amendment_id", "effective_revision", "node_id", "phase", "occurrence")) or (context.get("source_assignment_id") or None) != (binding.get("source_assignment_id") or None) or context.get("source_commit") != source.get("target_commit") or context.get("source_path") != (source.get("amendment") or {}).get("path") or context.get("source_sha256") != (source.get("amendment") or {}).get("sha256"):
                raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Binding context is not linked to its exact revision source", status_code=503)
            if canonical_json_bytes(binding.get("issued")) != canonical_json_bytes(records[0].get("issued")):
                raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Originally issued snapshots differ", status_code=503)
            contexts[binding["scope_context_sha256"]] = binding
            reviewed = binding.get("reviewed_source_scope_context")
            if isinstance(reviewed, dict):
                reviewed_hash = canonical_json_sha256(reviewed)
                if context.get("reviewed_source_scope_context_sha256") != reviewed_hash or reviewed.get("assignment_id") != binding.get("source_assignment_id"):
                    raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Reviewed source scope linkage is inconsistent", status_code=503)
                reviewed_sources.append(reviewed_hash)
            elif context.get("reviewed_source_scope_context_sha256"):
                raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Reviewed source scope snapshot is missing", status_code=503)
            previous_revision = revision
    if any(key not in contexts for key in reviewed_sources):
        raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Reviewed source scope history is missing", status_code=503)
    for ack_id, ack in ack_history.items():
        if not isinstance(ack, dict):
            raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "ACK history contains an invalid entry", status_code=503)
        binding = contexts.get(canonical_json_sha256(ack.get("scope_context")))
        if binding is None or acknowledgement_id(binding) != ack_id or ack.get("ack_id") != ack_id or any(ack.get(field) != binding.get(field) for field in ("assignment_id", "agent_id", "agent_phone", "amendment_id", "effective_revision")):
            raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "An ACK does not reference its exact historical binding", status_code=503)
    for assignment_id, ack in acks.items():
        if not isinstance(ack, dict) or ack.get("assignment_id") != assignment_id or canonical_json_bytes(ack_history.get(ack.get("ack_id"))) != canonical_json_bytes(ack):
            raise LegacyScopeControlError("SCOPE_HISTORY_INVALID", "Current ACK cache is not backed by immutable history", status_code=503)


def effective_agent_for_binding(
    issued_agent: Mapping[str, Any], binding: Mapping[str, Any] | None
) -> dict[str, Any]:
    if not isinstance(binding, Mapping):
        return deepcopy(dict(issued_agent))
    if str(issued_agent.get("id") or "").strip() != str(
        binding.get("agent_id") or ""
    ).strip() or str(issued_agent.get("phone") or "").strip() != str(
        binding.get("agent_phone") or ""
    ).strip():
        raise LegacyScopeControlError(
            "SCOPE_RUNTIME_ACTOR_MISMATCH",
            "The live assignment actor does not match its scope binding",
            status_code=503,
        )
    issued = binding.get("issued")
    if not isinstance(issued, Mapping):
        raise LegacyScopeControlError(
            "SCOPE_BINDING_INVALID",
            "The assignment scope binding has no issued actor snapshot",
            status_code=503,
        )
    live_authored = {
        "profile": issued_agent.get("profile"),
        "tasks": issued_agent.get("tasks") or [],
    }
    bound_authored = {
        "profile": issued.get("profile"),
        "tasks": issued.get("tasks") or [],
    }
    if canonical_json_bytes(live_authored) != canonical_json_bytes(bound_authored):
        raise LegacyScopeControlError(
            "SCOPE_RUNTIME_BASE_MISMATCH",
            "The live issued actor changed after its scope binding was created",
            status_code=503,
        )
    effective = binding.get("effective")
    if not isinstance(effective, dict):
        raise LegacyScopeControlError(
            "SCOPE_BINDING_INVALID",
            "The assignment scope binding has no effective actor snapshot",
            status_code=503,
        )
    result = deepcopy(dict(issued_agent))
    result["profile"] = deepcopy(effective.get("profile"))
    result["tasks"] = deepcopy(effective.get("tasks") or [])
    return result


def project_effective_task(
    issued_task: Mapping[str, Any] | None,
    binding: Mapping[str, Any],
) -> dict[str, Any] | None:
    effective = binding.get("effective")
    if not isinstance(effective, dict):
        return deepcopy(dict(issued_task)) if isinstance(issued_task, Mapping) else None
    stored = effective.get("active_task")
    if isinstance(stored, dict):
        issued = binding.get("issued")
        historical_task = issued.get("active_task") if isinstance(issued, dict) else None
        if historical_task is None and isinstance(binding.get("delivery_projection"), dict):
            historical_task = binding["delivery_projection"].get("task")
            issued = binding["delivery_projection"].get("base_core")
        if not isinstance(historical_task, dict):
            raise LegacyScopeControlError(
                "SCOPE_BINDING_INVALID",
                "The effective task projection has no issued source",
                status_code=503,
            )
        task = project_authored_active_task(historical_task, issued, effective)
    elif isinstance(issued_task, Mapping):
        task = deepcopy(dict(issued_task))
        if binding.get("instruction_snapshot_only") is True:
            authored_messages = [str(effective.get("profile") or "")]
            authored_messages.extend(str(item.get("message") or "") for item in effective.get("tasks", []) if isinstance(item, dict))
            task["message"] = "\n\n".join(dict.fromkeys(text for text in authored_messages if text))
    else:
        return None
    metadata = dict(task.get("metadata") or {})
    effective_agent = effective
    nested_agent = metadata.get("agent")
    if isinstance(nested_agent, dict):
        nested_agent = deepcopy(nested_agent)
        nested_agent["profile"] = deepcopy(effective_agent.get("profile"))
        metadata["agent"] = nested_agent
    metadata["tasks"] = deepcopy(effective_agent.get("tasks") or [])
    metadata["task_ids"] = [
        str(item.get("task_id") or "")
        for item in effective_agent.get("tasks", [])
        if isinstance(item, dict) and str(item.get("task_id") or "")
    ]
    metadata["scope_context"] = deepcopy(binding.get("scope_context"))
    metadata["scope_controlled"] = True
    task["metadata"] = metadata
    banner = scope_banner(binding)
    message = str(task.get("message") or "")
    # Generated submission examples are operational instruction, not audit
    # evidence. Rebind their exact context too: a new banner above an old JSON
    # example would otherwise tell an executor to submit a guaranteed-stale ACK.
    projected_lines = []
    for line in message.splitlines():
        try:
            example = json.loads(line)
        except (ValueError, TypeError):
            projected_lines.append(line)
            continue
        if isinstance(example, dict) and example.get("assignment_id") == binding.get("assignment_id") and isinstance(example.get("scope_context"), dict):
            example["scope_context"] = deepcopy(binding.get("scope_context"))
            line = json.dumps(example, ensure_ascii=False, separators=(",", ":"))
        projected_lines.append(line)
    message = "\n".join(projected_lines)
    if message.startswith("=== SCOPE CONTROL:") and "=== END SCOPE CONTROL ===" in message:
        message = message.split("=== END SCOPE CONTROL ===", 1)[1].lstrip("\n")
    task["message"] = f"{banner}\n\n{message}".rstrip()
    return task


def scope_banner(binding: Mapping[str, Any]) -> str:
    context = binding.get("scope_context") if isinstance(binding, Mapping) else {}
    return "\n".join(
        [
            "=== SCOPE CONTROL: EFFECTIVE INSTRUCTION (AUTHORITATIVE) ===",
            (
                "The effective profile and tasks in this response supersede any "
                "conflicting scope text in the historical issued task."
            ),
            (
                "Graph node ids, transitions, required review policy, historical "
                "results and approvals are unchanged."
            ),
            "scope_context="
            + json.dumps(
                context,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "GET the effective-scope endpoint and ACK this exact context before work submission.",
            "=== END SCOPE CONTROL ===",
        ]
    )


def effective_scope_core(effective: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical, independently verifiable authored instruction."""

    return {
        "profile": effective.get("profile"),
        "tasks": effective.get("tasks") or [],
    }


def binding_effective_scope_hash(effective: Mapping[str, Any]) -> str:
    return canonical_json_sha256(effective_scope_core(effective))


def build_scope_context(
    *,
    amendment: Mapping[str, Any],
    assignment: Mapping[str, Any],
    node_id: str,
    phase: str,
    agent_id: str,
    occurrence: int,
    effective_scope_sha256: str,
    source_assignment_id: str = "",
) -> dict[str, Any]:
    source = amendment.get("source") if isinstance(amendment.get("source"), dict) else {}
    context = {
        "schema_version": 1,
        "amendment_id": str(amendment.get("amendment_id") or ""),
        "effective_revision": int(amendment.get("effective_revision") or 0),
        "assignment_id": str(assignment.get("assignment_id") or ""),
        "node_id": node_id,
        "phase": phase,
        "agent_id": agent_id,
        "occurrence": max(0, int(occurrence or 0)),
        "source_commit": str(source.get("target_commit") or ""),
        "source_path": str(
            (source.get("amendment") or {}).get("path")
            if isinstance(source.get("amendment"), dict)
            else ""
        ),
        "source_sha256": str(
            (source.get("amendment") or {}).get("sha256")
            if isinstance(source.get("amendment"), dict)
            else ""
        ),
        "effective_scope_sha256": effective_scope_sha256,
        "precedence": SCOPE_CONTEXT_PRECEDENCE,
        "ack_required": True,
    }
    if source_assignment_id:
        context["source_assignment_id"] = source_assignment_id
    return context


def make_assignment_binding(
    *,
    amendment: Mapping[str, Any],
    assignment: Mapping[str, Any],
    issued_agent: Mapping[str, Any],
    effective_agent: Mapping[str, Any],
    node_id: str,
    phase: str,
    occurrence: int,
    issued_active_task: Mapping[str, Any] | None = None,
    effective_active_task: Mapping[str, Any] | None = None,
    source_assignment_id: str = "",
    bound_at: str,
) -> dict[str, Any]:
    effective = {
        "profile": deepcopy(effective_agent.get("profile")),
        "tasks": deepcopy(effective_agent.get("tasks") or []),
        "active_task": (
            deepcopy(dict(effective_active_task))
            if isinstance(effective_active_task, Mapping)
            else None
        ),
    }
    effective_hash = binding_effective_scope_hash(effective)
    effective_core = effective_scope_core(effective)
    context = build_scope_context(
        amendment=amendment,
        assignment=assignment,
        node_id=node_id,
        phase=phase,
        agent_id=str(assignment.get("agent_id") or ""),
        occurrence=occurrence,
        effective_scope_sha256=effective_hash,
        source_assignment_id=source_assignment_id,
    )
    return {
        "assignment_id": str(assignment.get("assignment_id") or ""),
        "amendment_id": str(amendment.get("amendment_id") or ""),
        "effective_revision": int(amendment.get("effective_revision") or 0),
        "node_id": node_id,
        "phase": phase,
        "agent_id": str(assignment.get("agent_id") or ""),
        "agent_phone": str(assignment.get("agent_phone") or ""),
        "occurrence": max(0, int(occurrence or 0)),
        "source_assignment_id": source_assignment_id or None,
        "bound_at": bound_at,
        "issued": {
            "profile": deepcopy(issued_agent.get("profile")),
            "tasks": deepcopy(issued_agent.get("tasks") or []),
            "active_task": (
                deepcopy(dict(issued_active_task))
                if isinstance(issued_active_task, Mapping)
                else None
            ),
        },
        "effective": effective,
        "effective_core": effective_core,
        "effective_scope_sha256": effective_hash,
        "effective_scope_hash_contract": deepcopy(EFFECTIVE_SCOPE_HASH_CONTRACT),
        "scope_context": context,
        "scope_context_sha256": canonical_json_sha256(context),
    }


def rebind_active_assignment(
    state: Mapping[str, Any], *, amendment: Mapping[str, Any], assignment: Mapping[str, Any],
    issued_agent: Mapping[str, Any], effective_agent: Mapping[str, Any], node_id: str,
    phase: str, occurrence: int, active_task: Mapping[str, Any], bound_at: str,
    source_assignment_id: str = "",
) -> dict[str, Any]:
    """Bind a new revision even when the task was first issued under scope vN.

    Queue-time bindings legitimately have no active-task snapshot. Do not fill
    that historical null on a later delivery or amendment; retain a separate
    exact delivery projection instead.
    """
    previous = assignment_binding(state, assignment.get("assignment_id"))
    original_task = previous["issued"].get("active_task") if previous else active_task
    if original_task is not None:
        projection_task = original_task
        projection_agent = issued_agent
    elif previous and isinstance(previous.get("delivery_projection"), dict):
        projection_task = previous["delivery_projection"]["task"]
        projection_agent = previous["delivery_projection"]["base_core"]
    else:
        projection_task = active_task
        projection_agent = previous["effective"] if previous else issued_agent
    effective_task = project_authored_active_task(projection_task, projection_agent, effective_agent)
    binding = make_assignment_binding(amendment=amendment, assignment=assignment,
        issued_agent=issued_agent, effective_agent=effective_agent, node_id=node_id,
        phase=phase, occurrence=occurrence, issued_active_task=original_task,
        effective_active_task=effective_task, source_assignment_id=source_assignment_id,
        bound_at=bound_at)
    if previous:
        binding["issued"] = deepcopy(previous["issued"])
    if original_task is None:
        binding["delivery_projection"] = {"task": deepcopy(dict(projection_task)),
            "base_core": effective_scope_core(projection_agent)}
    return binding


def acknowledgement_id(binding: Mapping[str, Any]) -> str:
    return "scope-ack-" + canonical_json_sha256(
        {
            "assignment_id": binding.get("assignment_id"),
            "agent_id": binding.get("agent_id"),
            "scope_context": binding.get("scope_context"),
        }
    )[:32]


def attach_reviewed_source_context(binding: dict[str, Any], source_context: Any) -> None:
    """Keep the reviewed result's scope separate from this reviewer's scope."""
    binding["reviewed_source_scope_context"] = deepcopy(source_context) if isinstance(source_context, dict) else None
    if isinstance(source_context, dict):
        if source_context.get("assignment_id") != binding.get("source_assignment_id"):
            raise LegacyScopeControlError("SCOPE_REVIEW_SOURCE_MISMATCH", "Reviewer scope must retain its exact reviewed assignment")
        binding["scope_context"]["reviewed_source_scope_context_sha256"] = canonical_json_sha256(source_context)
        binding["scope_context_sha256"] = canonical_json_sha256(binding["scope_context"])


def acknowledgement_for_binding(
    scope_control: Mapping[str, Any], binding: Mapping[str, Any]
) -> dict[str, Any] | None:
    acknowledgements = scope_control.get("acknowledgements")
    ack = (
        acknowledgements.get(str(binding.get("assignment_id") or ""))
        if isinstance(acknowledgements, dict)
        else None
    )
    if not isinstance(ack, dict):
        return None
    if canonical_json_bytes(ack.get("scope_context")) != canonical_json_bytes(
        binding.get("scope_context")
    ):
        return None
    return deepcopy(ack)


def require_exact_scope_context(
    supplied: Any, binding: Mapping[str, Any]
) -> None:
    if not isinstance(supplied, dict):
        raise LegacyScopeControlError(
            "SCOPE_ACK_REQUIRED",
            "The governed assignment requires ACK and its exact scope_context",
            status_code=428,
        )
    if canonical_json_bytes(supplied) != canonical_json_bytes(
        binding.get("scope_context")
    ):
        raise LegacyScopeControlError(
            "SCOPE_CONTEXT_MISMATCH",
            "The submitted scope_context is stale or does not match this assignment",
        )


def apply_semantic_scope_revision(
    state: Mapping[str, Any],
    agents: list[dict[str, Any]],
    *,
    amendment_id: str,
    proposal: Mapping[str, Any],
    source: Mapping[str, Any],
    authorization: Mapping[str, Any],
    expected_scope_revision: int,
    applied_at: str,
    instruction_snapshot_only: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Prepare one atomic semantic revision without performing IO/execution.

    The human-decision adapter verifies authorization, source and decision CAS
    and persists this result and its decision together under its runtime lock.
    Adapters may normalize their immutable assignment/agent identities into
    this shape; only ``scope_control`` is changed here.
    """
    result = deepcopy(dict(state))
    control = deepcopy(result.get("scope_control") or {})
    if control and int(control.get("schema_version") or 0) != 2:
        raise LegacyScopeControlError("SCOPE_OFFLINE_MIGRATION_REQUIRED", "Existing v1 scope requires explicit offline migration before another revision")
    if int(control.get("effective_revision") or 0) != expected_scope_revision:
        raise LegacyScopeControlError("SCOPE_REVISION_CONFLICT", "The effective scope revision changed; validate the decision again")
    if control:
        validate_versioned_scope_history(control)
    if not amendment_id or amendment_by_id(control, amendment_id) is not None:
        raise LegacyScopeControlError("SCOPE_AMENDMENT_ID_CONFLICT", "The amendment identity must be new")
    if str(state.get("status") or "") != "active":
        raise LegacyScopeControlError("SCOPE_RUNTIME_NOT_APPLICABLE", "Scope revisions require active execution")
    assignment_id = str(state.get("current_assignment_id") or "")
    current_agent_id = str(state.get("current_agent_id") or "")
    node_id = str(state.get("current_node_id") or current_agent_id)
    phase = str(state.get("phase") or "node")
    assignment = next((item for item in state.get("assignments", []) if isinstance(item, dict) and item.get("assignment_id") == assignment_id), None)
    if not assignment or assignment.get("status") != "active":
        raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_ACTIVE", "There must be an active assignment to bind")
    active_task = state.get("active_task")
    if not instruction_snapshot_only and (not isinstance(active_task, dict) or str((active_task.get("metadata") or {}).get("assignment_id") or "") != assignment_id):
        raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_DELIVERED", "The active task must already be delivered")
    instructions = proposal.get("instructions")
    restrictions = proposal.get("retained_restrictions")
    if not isinstance(instructions, str) or not instructions.strip() or not isinstance(restrictions, list) or not all(isinstance(item, str) and item.strip() for item in restrictions):
        raise LegacyScopeControlError("SCOPE_PROPOSAL_INVALID", "Instructions and explicit retained restrictions are required", status_code=400)
    authored = instructions.strip() + "\n\nRetained restrictions:\n" + ("\n".join("- " + item.strip() for item in restrictions) or "None explicitly declared.")
    authored += "\n\nExisting execution, review and qualification gates remain unchanged. This effective instruction supersedes conflicting historical scope text."
    workflow = state.get("workflow") or {}
    node_agents = {str(node.get("id")): str(node.get("agent_id")) for node in workflow.get("nodes", []) if isinstance(node, dict)}
    if not node_agents:
        node_agents = {str(agent.get("id")): str(agent.get("id")) for agent in agents}
    requested_nodes = proposal.get("node_ids")
    requested_reviewers = proposal.get("reviewer_ids")
    if not isinstance(requested_nodes, list) or not isinstance(requested_reviewers, list) or len(set(requested_nodes)) != len(requested_nodes) or len(set(requested_reviewers)) != len(requested_reviewers) or not set(requested_nodes).issubset(node_agents):
        raise LegacyScopeControlError("SCOPE_TARGET_NOT_FOUND", "Scope targets must be unique existing identities", status_code=400)
    if set(requested_reviewers) != set(workflow.get("reviewer_agent_ids") or []):
        raise LegacyScopeControlError("SCOPE_REVIEW_POLICY_MISMATCH", "All existing reviewers must receive the same amendment")
    if (phase == "review" and current_agent_id not in requested_reviewers) or (phase != "review" and node_id not in requested_nodes):
        raise LegacyScopeControlError("SCOPE_ACTIVE_TARGET_MISSING", "The active assignment must be included in the approved scope")
    agents_by_id = {str(agent.get("id")): agent for agent in agents}
    previous = active_amendment(control) or {}
    node_overrides = deepcopy(previous.get("node_overrides") or {})
    reviewer_overrides = deepcopy(previous.get("reviewer_overrides") or {})
    for target_id, agent_id, is_review in [(target, node_agents[target], False) for target in requested_nodes] + [(target, target, True) for target in requested_reviewers]:
        agent = agents_by_id.get(agent_id)
        if not agent:
            raise LegacyScopeControlError("SCOPE_RUNTIME_ACTOR_MISMATCH", "A targeted actor is absent")
        tasks = deepcopy(agent.get("tasks") or [])
        for task in tasks:
            if isinstance(task, dict):
                task["message"] = authored
            else:
                raise LegacyScopeControlError("SCOPE_PROPOSAL_INVALID", "Normalized authored tasks must be objects", status_code=400)
        rule = {
            "agent_id": agent_id, "agent_phone": str(agent.get("phone") or ""),
            "issued": {"profile": deepcopy(agent.get("profile")), "tasks": deepcopy(agent.get("tasks") or [])},
            "effective": {"profile": authored, "tasks": tasks},
        }
        if is_review:
            reviewer_overrides[target_id] = rule
        else:
            rule.update(node_id=target_id, minimum_visit=int((state.get("visit_counts") or {}).get(target_id) or 0) + (0 if target_id == node_id else 1))
            node_overrides[target_id] = rule
    current_agent = agents_by_id.get(current_agent_id)
    if not current_agent:
        raise LegacyScopeControlError("SCOPE_RUNTIME_ACTOR_MISMATCH", "The active actor is absent")
    normalized_source = deepcopy(dict(source))
    normalized_source.setdefault("target_commit", source.get("commit"))
    normalized_source.setdefault("amendment", {"path": source.get("path"), "sha256": source.get("sha256")})
    revision = expected_scope_revision + 1
    amendment = {
        "amendment_id": amendment_id, "effective_revision": revision,
        "applied_at": applied_at, "source": normalized_source,
        "authorization": deepcopy(dict(authorization)), "proposal": deepcopy(dict(proposal)),
        "node_overrides": node_overrides, "reviewer_overrides": reviewer_overrides,
        "terminal_overrides": deepcopy(previous.get("terminal_overrides") or {}),
    }
    rule = reviewer_overrides[current_agent_id] if phase == "review" else node_overrides[node_id]
    effective_agent = {**deepcopy(current_agent), **deepcopy(rule["effective"])}
    pending = state.get("pending_transition") or {}
    source_assignment_id = str(assignment.get("source_assignment_id") or (pending.get("source_assignment_id") if phase == "review" else "") or "")
    binding_arguments = dict(amendment=amendment, assignment=assignment,
        issued_agent=current_agent, effective_agent=effective_agent, node_id=node_id,
        phase=phase, occurrence=int(assignment.get("occurrence") or (state.get("visit_counts") or {}).get(node_id) or 0),
        source_assignment_id=source_assignment_id, bound_at=applied_at)
    if instruction_snapshot_only:
        binding = make_assignment_binding(**binding_arguments)
        binding["instruction_snapshot_only"] = True
    else:
        binding = rebind_active_assignment(state, active_task=active_task, **binding_arguments)
    if phase == "review":
        source_binding = assignment_binding(state, source_assignment_id) if source_assignment_id else None
        attach_reviewed_source_context(binding, pending.get("scope_context") or (source_binding or {}).get("scope_context"))
    receipt = {"schema_version": 2, "amendment_id": amendment_id, "effective_revision": revision,
        "assignment_id": assignment_id, "scope_context": deepcopy(binding["scope_context"]),
        "effective_scope_sha256": binding["effective_scope_sha256"], "assignment_preserved": True,
        "graph_advanced": False, "queue_changed": False, "deduplicated": False}
    amendment["receipt"] = deepcopy(receipt)
    control.update(schema_version=2, minimum_runtime_capability=SCOPE_CONTROL_V2_CAPABILITY,
        effective_revision=revision, active_amendment_id=amendment_id)
    control.setdefault("amendments", []).append(amendment)
    control.setdefault("acknowledgements", {})
    control.setdefault("acknowledgement_history", {})
    store_assignment_binding(control, binding)
    validate_versioned_scope_history(control)
    result["scope_control"] = control
    return result, receipt


__all__ = [
    "ADMIN_TOKEN_ENV",
    "ADMIN_TOKEN_HEADER",
    "EFFECTIVE_SCOPE_HASH_CONTRACT",
    "LegacyScopeControlError",
    "MINIMUM_TOKEN_LENGTH",
    "ROLE_TOKEN_ENV_PREFIX",
    "ROLE_TOKEN_HEADER",
    "REPOSITORY_MAP_ENV",
    "SCOPE_CONTEXT_PRECEDENCE",
    "SCOPE_CONTROL_CAPABILITY",
    "SCOPE_CONTROL_SCHEMA_VERSION",
    "SCOPE_CONTROL_V2_SCHEMA_VERSION",
    "SCOPE_CONTROL_V2_CAPABILITY",
    "SUPPORTED_SCOPE_CONTROL_VERSIONS",
    "acknowledgement_for_binding",
    "acknowledgement_id",
    "active_amendment",
    "amendment_by_id",
    "assignment_binding",
    "attach_reviewed_source_context",
    "apply_semantic_scope_revision",
    "canonical_json_bytes",
    "canonical_json_sha256",
    "effective_agent_for_binding",
    "effective_agent_from_override",
    "effective_scope_core",
    "make_assignment_binding",
    "manifest_scope_delta",
    "project_effective_task",
    "project_authored_active_task",
    "require_exact_scope_context",
    "rebind_active_assignment",
    "require_role_tokens_configured",
    "role_token_environment_name",
    "scope_banner",
    "store_assignment_binding",
    "store_acknowledgement",
    "validate_versioned_scope_history",
    "sha256_bytes",
    "text_sha256",
    "verify_admin_token",
    "verify_role_token",
]
