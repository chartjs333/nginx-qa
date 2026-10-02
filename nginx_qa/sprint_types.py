"""Pure, versioned contracts for sprint-type dispatch.

This module deliberately does not call the legacy importer or the managed
workspace runtime.  It defines the value objects shared by those two paths and
the one read-only operation that may run before the mutation boundary.
"""

from __future__ import annotations

import hashlib
import json
import ntpath
import re
import urllib.parse
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


SPRINT_TYPE_FIELD = "sprint_type"
SPRINT_TYPE_CONTRACT_VERSION = 1
MANAGED_SPRINT_SCHEMA_VERSION = 1
SPRINT_TYPE_UNSUPPORTED = "SPRINT_TYPE_UNSUPPORTED"
MANAGED_MANIFEST_SCHEMA = "managed-workspace-sprint-v1.schema.json"

PROCESS_STATE_TRANSITIONS: dict[str, frozenset[str]] = {
    "PREPARED": frozenset({"STARTING", "FAILED"}),
    "STARTING": frozenset({"HEALTHY", "STOPPING", "FAILED"}),
    "HEALTHY": frozenset({"STOPPING", "FAILED"}),
    "STOPPING": frozenset({"STOPPED", "FAILED"}),
    "STOPPED": frozenset(),
    "FAILED": frozenset(),
}

ASSIGNMENT_STATE_TRANSITIONS: dict[str, frozenset[str]] = {
    "prepared": frozenset({"active", "blocked", "failed"}),
    "active": frozenset({"reviews_pending", "blocked", "failed"}),
    "reviews_pending": frozenset({"completed", "blocked", "failed"}),
    "completed": frozenset(),
    "blocked": frozenset(),
    "failed": frozenset(),
}

REVIEW_ASSIGNMENT_STATE_TRANSITIONS: dict[str, frozenset[str]] = {
    "prepared": frozenset({"active"}),
    "active": frozenset({"decided"}),
    "decided": frozenset(),
}

BRANCH_LEASE_STATE_TRANSITIONS: dict[str, frozenset[str]] = {
    "active": frozenset({"released"}),
    "released": frozenset(),
}

PORT_LEASE_STATE_TRANSITIONS: dict[str, frozenset[str]] = {
    "reserved": frozenset({"bound", "released"}),
    "bound": frozenset({"released"}),
    "released": frozenset(),
}

OUTBOX_STATE_TRANSITIONS: dict[str, frozenset[str]] = {
    "pending": frozenset({"delivered"}),
    "delivered": frozenset(),
}

TRANSITION_JOURNAL_STATES: tuple[str, ...] = (
    "RESULT_RECEIVED",
    "RESULT_VALIDATED",
    "REVIEWS_PENDING",
    "REVIEWS_ACCEPTED",
    "TRANSITION_COMMITTED",
    "NEXT_ASSIGNMENT_ENQUEUED",
)

_GIT_FORBIDDEN_CHARACTERS = frozenset(" ~^:?*[\\")
_WINDOWS_FORBIDDEN_CHARACTERS = frozenset('<>:"/\\|?*')
_WINDOWS_RESERVED_BASENAMES = frozenset(
    {"con", "prn", "aux", "nul", "conin$", "conout$"}
    | {f"com{number}" for number in range(1, 10)}
    | {f"lpt{number}" for number in range(1, 10)}
    | {f"com{number}" for number in "¹²³"}
    | {f"lpt{number}" for number in "¹²³"}
)
_CREDENTIAL_ENV_NAME = re.compile(
    r"(?:^|_)(?:AUTH|COOKIE|CREDENTIALS?|PASSWORD|PASSWD|PRIVATE_KEY|"
    r"SECRETS?|TOKENS?|API_KEY)(?:_|$)",
    re.IGNORECASE,
)
_SECRET_LITERAL = re.compile(
    r"(?:"
    r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}|"
    r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----|"
    r"\b(?:sk-(?:proj-)?|ghp_|github_pat_|glpat-)[A-Za-z0-9_-]{8,}|"
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
    r")",
    re.IGNORECASE,
)
_CREDENTIAL_REFERENCE = re.compile(
    r"(?:env|keyring|secret-manager|vault|windows-credential):"
    r"[A-Za-z0-9][A-Za-z0-9._/@+-]{0,254}\Z"
)


def _reference_value_valid(value: Any) -> bool:
    return (
        isinstance(value, str)
        and _CREDENTIAL_REFERENCE.fullmatch(value) is not None
        and _SECRET_LITERAL.search(value) is None
    )


def _is_secret_reference(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"secret_ref"}
        and _reference_value_valid(value.get("secret_ref"))
    )


def _environment_contains_secret_literal(environment: Any) -> bool:
    if not isinstance(environment, Mapping):
        return True
    for name, value in environment.items():
        if not isinstance(name, str):
            return True
        is_reference = _is_secret_reference(value)
        if _CREDENTIAL_ENV_NAME.search(name) and not is_reference:
            return True
        if isinstance(value, str) and _SECRET_LITERAL.search(value):
            return True
        if not isinstance(value, str) and not is_reference:
            return True
    return False


def _credential_reference_valid(value: Any) -> bool:
    """Accept only provider-qualified registry references, never token literals."""

    return value is None or _reference_value_valid(value)


def _repository_assertion_key(value: Any) -> str | None:
    """Normalize a manifest repository assertion like the project registry.

    This intentionally returns only the identity key; the submitted address is
    never a fetch target. Invalid or credential-bearing assertions return
    ``None`` so the relational checker reports a provenance mismatch.
    """

    if not isinstance(value, str):
        return None
    address = value.strip()
    if not address or len(address) > 2048 or any(
        ord(character) < 32 or ord(character) == 127 for character in address
    ):
        return None

    def repository_path(host: str, raw_path: str) -> str:
        path = raw_path.strip("/")
        if path.lower().endswith(".git"):
            path = path[:-4]
        return path.lower() if host.rstrip(".").lower() == "github.com" else path

    if re.match(r"^[A-Za-z]:[\\/]", address) or address.startswith(("/", "\\\\")):
        if "#" in address:
            return None
        return "local:" + ntpath.normcase(ntpath.normpath(address))

    if "://" not in address:
        scp_match = re.fullmatch(
            r"(?:[^@\s/:]+@)?(?P<host>[A-Za-z0-9.-]+):(?P<path>[^\s]+)",
            address,
        )
        if scp_match:
            host = scp_match.group("host").lower()
            path = repository_path(host, scp_match.group("path"))
            return f"{host}/{path}" if path else None

    parsed = urllib.parse.urlparse(address)
    if parsed.scheme:
        scheme = parsed.scheme.lower()
        if (
            scheme not in {"http", "https", "ssh", "git"}
            or not parsed.hostname
            or parsed.password is not None
            or (scheme in {"http", "https", "git"} and parsed.username is not None)
            or parsed.query
            or parsed.fragment
        ):
            return None
        try:
            port = parsed.port
        except ValueError:
            return None
        host = parsed.hostname.lower()
        default_port = {"http": 80, "https": 443, "ssh": 22, "git": 9418}[scheme]
        if port is not None and port != default_port:
            host = f"{host}:{port}"
        path = repository_path(parsed.hostname, parsed.path)
        return f"{host}/{path}" if path else None

    bare_match = re.fullmatch(
        r"(?P<host>(?:localhost|[A-Za-z0-9.-]+\.[A-Za-z0-9.-]+))/(?P<path>[^\s]+)",
        address,
        re.IGNORECASE,
    )
    if bare_match:
        host = bare_match.group("host").lower()
        path = repository_path(host, bare_match.group("path"))
        return f"{host}/{path}" if path else None
    return None


class SprintType(str, Enum):
    """Supported public values of the optional ``sprint_type`` field."""

    LEGACY_V1 = "legacy_v1"
    MANAGED_WORKSPACE_V1 = "managed_workspace_v1"


class SprintPipeline(str, Enum):
    """Internal dispatch target; it is not a replacement for execution.mode."""

    LEGACY = "legacy"
    MANAGED_WORKSPACE = "managed_workspace"


class SprintImportPhase(str, Enum):
    """Ordered phases of a managed import transaction."""

    VALIDATE = "VALIDATE"
    PREPARE = "PREPARE"
    ACTIVATE = "ACTIVATE"


class SprintTypeUnsupported(ValueError):
    """Raised before mutation when a manifest declares an unsupported type."""

    code = SPRINT_TYPE_UNSUPPORTED

    def __init__(self) -> None:
        super().__init__(f"{self.code}: unsupported sprint_type")

    def as_detail(self) -> dict[str, Any]:
        """Return the stable API error shape without prescribing HTTP routing."""

        return {
            "error": self.code,
            "field": SPRINT_TYPE_FIELD,
            "supported": [member.value for member in SprintType],
        }


@dataclass(frozen=True, slots=True)
class SprintTypeSelection:
    """Dispatch result that keeps declared and effective values distinct.

    ``declared`` stays ``None`` when the source JSON omitted the field.  That
    distinction is required so legacy payloads and archives can be serialized
    without injecting a new key.
    """

    declared: SprintType | None
    effective: SprintType
    pipeline: SprintPipeline

    @property
    def was_explicit(self) -> bool:
        return self.declared is not None

    @property
    def uses_legacy_pipeline(self) -> bool:
        return self.pipeline is SprintPipeline.LEGACY

    def serialized_field(self) -> dict[str, str]:
        """Return only the field originally declared by the source payload."""

        if self.declared is None:
            return {}
        return {SPRINT_TYPE_FIELD: self.declared.value}


@dataclass(frozen=True, slots=True)
class StartSprintFromGitRequest:
    """Transport-neutral request model for the start-from-git API."""

    repository_id: str
    ref: str
    manifest_path: str
    idempotency_key: str

    def request_fingerprint(self, canonical_project_id: str) -> str:
        return canonical_tuple_sha256(
            canonical_project_id,
            self.repository_id,
            self.ref,
            self.manifest_path,
        )


@dataclass(frozen=True, slots=True)
class SprintProvenance:
    """Immutable source identity persisted for every managed sprint."""

    project_id: str
    repository_id: str
    commit: str
    manifest_path: str
    manifest_sha256: str

    def identity_components(self) -> tuple[str, str, str, str, str]:
        """Return the exact ordered components of the public sprint identity."""

        return (
            self.project_id,
            self.repository_id,
            self.commit,
            self.manifest_path,
            self.manifest_sha256,
        )

    def canonical_identity_bytes(self) -> bytes:
        """Encode the identity as a canonical UTF-8 JSON array."""

        return canonical_tuple_bytes(*self.identity_components())

    def identity_sha256(self) -> str:
        """Return the reproducible fingerprint used in managed sprint IDs."""

        return hashlib.sha256(self.canonical_identity_bytes()).hexdigest()

    def stable_sprint_id(self) -> str:
        return f"msv1-{self.identity_sha256()}"


def canonical_tuple_bytes(*components: str) -> bytes:
    """Encode validated identity components without boundary ambiguity."""

    if not all(isinstance(component, str) for component in components):
        raise TypeError("canonical identity components must be strings")
    try:
        for component in components:
            component.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise ValueError("canonical identity components must be valid UTF-8") from None
    return json.dumps(
        components,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_tuple_sha256(*components: str) -> str:
    return hashlib.sha256(canonical_tuple_bytes(*components)).hexdigest()


def canonical_json_bytes(value: Any) -> bytes:
    """Encode a parsed JSON value for version-1 content fingerprints."""

    try:
        text = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return text.encode("utf-8", errors="strict")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("value must have a canonical UTF-8 JSON encoding") from None


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def repair_request_fingerprint(
    sprint_id: str,
    expected_revision: int,
    repair_source_commit: str,
    patch: Mapping[str, Any],
) -> str:
    """Bind repair idempotency to its route sprint, revision, and patch."""

    return canonical_json_sha256(
        {
            "expected_revision": expected_revision,
            "patch": patch,
            "repair_source_commit": repair_source_commit,
            "sprint_id": sprint_id,
        }
    )


def review_request_fingerprint(
    assignment_id: str,
    decision: str,
    feedback: str,
) -> str:
    """Hash the normalized managed review request."""

    return canonical_json_sha256(
        {
            "assignment_id": assignment_id,
            "feedback": feedback,
            "status": decision,
        }
    )


def recovery_request_fingerprint(
    context_id: str,
    graph_revision: int,
    action: str,
    import_attempt_id: str | None,
    assignment_id: str | None,
    parameters: Mapping[str, Any],
    join_target: Mapping[str, Any] | None = None,
) -> str:
    """Hash the normalized Coordinator recovery request."""

    return canonical_json_sha256(
        {
            "action": action,
            "assignment_id": assignment_id,
            "context_id": context_id,
            "graph_revision": graph_revision,
            "import_attempt_id": import_attempt_id,
            "join_target": join_target,
            "parameters": parameters,
        }
    )


def _apply_managed_repair_patch(
    definition: Mapping[str, Any],
    patch: Mapping[str, Any],
) -> dict[str, Any]:
    """Materialize the deterministic v1 repair result or reject ambiguity."""

    candidate = deepcopy(dict(definition))
    raw_nodes = candidate.get("nodes")
    if not isinstance(raw_nodes, list):
        raise ValueError("managed definition must contain nodes")
    nodes = deepcopy(raw_nodes)
    node_positions = {
        node.get("id"): index
        for index, node in enumerate(nodes)
        if isinstance(node, Mapping) and isinstance(node.get("id"), str)
    }

    future_nodes = patch.get("future_nodes", [])
    future_nodes = future_nodes if isinstance(future_nodes, list) else []
    future_by_id: dict[str, Mapping[str, Any]] = {}
    for node in future_nodes:
        node_id = node.get("id") if isinstance(node, Mapping) else None
        if not isinstance(node_id, str) or node_id in future_by_id:
            raise ValueError("future node IDs must be unique")
        future_by_id[node_id] = node

    removed_ids = patch.get("remove_future_node_ids", [])
    removed_ids = removed_ids if isinstance(removed_ids, list) else []
    if not all(isinstance(node_id, str) for node_id in removed_ids):
        raise ValueError("removed node IDs must be strings")
    if set(removed_ids).intersection(future_by_id):
        raise ValueError("a node cannot be removed and upserted")
    nodes = [node for node in nodes if node.get("id") not in set(removed_ids)]
    node_positions = {
        node.get("id"): index
        for index, node in enumerate(nodes)
        if isinstance(node, Mapping) and isinstance(node.get("id"), str)
    }
    for node_id, node in future_by_id.items():
        replacement = deepcopy(dict(node))
        if node_id in node_positions:
            nodes[node_positions[node_id]] = replacement
        else:
            node_positions[node_id] = len(nodes)
            nodes.append(replacement)
    candidate["nodes"] = nodes

    prompts = patch.get("prompts", {})
    prompts = prompts if isinstance(prompts, Mapping) else {}
    for node_id, replacements in prompts.items():
        position = node_positions.get(node_id)
        if position is None or not isinstance(replacements, Mapping):
            raise ValueError("prompt target must be a known task node")
        node = nodes[position]
        tasks = node.get("tasks") if isinstance(node, Mapping) else None
        if not isinstance(tasks, list):
            raise ValueError("prompt target must be a task node")
        tasks_by_id = {
            task.get("task_id"): task
            for task in tasks
            if isinstance(task, Mapping) and isinstance(task.get("task_id"), str)
        }
        explicit_future = future_by_id.get(node_id)
        explicit_tasks = (
            explicit_future.get("tasks")
            if isinstance(explicit_future, Mapping)
            else None
        )
        explicit_by_id = {
            task.get("task_id"): task
            for task in explicit_tasks
            if isinstance(task, Mapping) and isinstance(task.get("task_id"), str)
        } if isinstance(explicit_tasks, list) else {}
        for task_id, message in replacements.items():
            task = tasks_by_id.get(task_id)
            if not isinstance(task, dict) or not isinstance(message, str):
                raise ValueError("prompt target task must exist")
            explicit_task = explicit_by_id.get(task_id)
            if (
                isinstance(explicit_task, Mapping)
                and "message" in explicit_task
                and explicit_task.get("message") != message
            ):
                raise ValueError("repair patch has conflicting prompt values")
            task["message"] = message

    reviewer_metadata = patch.get("reviewer_metadata")
    if reviewer_metadata is not None:
        execution = candidate.get("execution")
        if not isinstance(execution, dict) or not isinstance(reviewer_metadata, list):
            raise ValueError("reviewer metadata target is invalid")
        execution["reviewers"] = deepcopy(reviewer_metadata)

    coordinator_routing = patch.get("coordinator_routing")
    if coordinator_routing is not None:
        if not isinstance(coordinator_routing, Mapping):
            raise ValueError("coordinator routing is invalid")
        candidate["coordinator"] = deepcopy(dict(coordinator_routing))

    checksum_metadata = patch.get("checksum_metadata")
    if checksum_metadata is not None:
        if not isinstance(checksum_metadata, list):
            raise ValueError("checksum metadata is invalid")
        candidate["files"] = deepcopy(checksum_metadata)

    profiles = patch.get("profiles", {})
    profiles = profiles if isinstance(profiles, Mapping) else {}
    profile_targets: dict[str, dict[str, Any]] = {}
    explicit_profile_ids: set[str] = set()
    for node_id, node in future_by_id.items():
        agent = node.get("agent") if isinstance(node, Mapping) else None
        if isinstance(agent, Mapping) and "profile" in agent:
            explicit_profile_ids.add(agent.get("id"))
    if isinstance(reviewer_metadata, list):
        explicit_profile_ids.update(
            reviewer.get("id")
            for reviewer in reviewer_metadata
            if isinstance(reviewer, Mapping) and "profile" in reviewer
        )
    for node in nodes:
        agent = node.get("agent") if isinstance(node, Mapping) else None
        if isinstance(agent, dict) and isinstance(agent.get("id"), str):
            profile_targets[agent["id"]] = agent
    execution = candidate.get("execution")
    reviewers = execution.get("reviewers") if isinstance(execution, Mapping) else None
    if isinstance(reviewers, list):
        for reviewer in reviewers:
            if isinstance(reviewer, dict) and isinstance(reviewer.get("id"), str):
                profile_targets[reviewer["id"]] = reviewer
    for agent_id, profile in profiles.items():
        target = profile_targets.get(agent_id)
        if not isinstance(target, dict) or not isinstance(profile, str):
            raise ValueError("profile target must be a known agent or reviewer")
        if (
            agent_id in explicit_profile_ids
            and target.get("profile") != profile
        ):
            raise ValueError("repair patch has conflicting profile values")
        target["profile"] = profile

    return candidate


def mirror_storage_key(canonical_remote: str) -> str:
    """Return a filesystem-safe key without redefining repository identity."""

    if not isinstance(canonical_remote, str) or not canonical_remote:
        raise ValueError("canonical remote must be a non-empty string")
    try:
        remote_bytes = canonical_remote.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise ValueError("canonical remote must be valid UTF-8") from None
    return f"mirror-{hashlib.sha256(remote_bytes).hexdigest()}"


def managed_result_key(
    assignment_id: str,
    outcome: str,
    result_commit: str,
) -> str:
    return f"result-{canonical_tuple_sha256(assignment_id, outcome, result_commit)}"


def managed_transition_token_id(
    sprint_id: str,
    source_occurrence_id: str,
    result_key: str,
    target_node_id: str,
) -> str:
    return "token-" + canonical_tuple_sha256(
        sprint_id,
        source_occurrence_id,
        result_key,
        target_node_id,
    )


def managed_integration_id(
    sprint_id: str,
    target_graph_revision: int,
    target_node_id: str,
    ordered_trigger_token_ids: list[str] | tuple[str, ...],
) -> str:
    return "integration-" + canonical_json_sha256(
        {
            "sprint_id": sprint_id,
            "target_graph_revision": target_graph_revision,
            "target_node_id": target_node_id,
            "trigger_token_ids": list(ordered_trigger_token_ids),
        }
    )


def managed_integration_workspace_id(
    sprint_id: str,
    target_graph_revision: int,
    target_node_id: str,
    ordered_trigger_token_ids: list[str] | tuple[str, ...],
) -> str:
    """Return the one durable merge-workspace ID for an integration signature."""

    integration_id = managed_integration_id(
        sprint_id,
        target_graph_revision,
        target_node_id,
        ordered_trigger_token_ids,
    )
    return "integration-workspace-" + integration_id.removeprefix("integration-")


def managed_occurrence_id(
    sprint_id: str,
    graph_revision: int,
    node_id: str,
    generation: int,
    ordered_trigger_token_ids: list[str] | tuple[str, ...],
) -> str:
    return "occ-" + canonical_json_sha256(
        {
            "generation": generation,
            "graph_revision": graph_revision,
            "node_id": node_id,
            "sprint_id": sprint_id,
            "trigger_token_ids": list(ordered_trigger_token_ids),
        }
    )


def git_ref_format_valid(value: str, *, branch: bool = False) -> bool:
    """Apply the non-mutating ``git check-ref-format`` v1 contract."""

    if not isinstance(value, str) or not value or value == "@":
        return False
    if branch:
        if value.startswith("-"):
            return False
        ref = f"refs/heads/{value}"
    else:
        ref = value
        if not (ref.startswith("refs/heads/") or ref.startswith("refs/tags/")):
            return False
    if (
        len(ref) > 512
        or ".." in ref
        or "@{" in ref
        or "//" in ref
        or ref.endswith(("/", "."))
        or any(ord(character) < 32 or ord(character) == 127 for character in ref)
        or any(character in _GIT_FORBIDDEN_CHARACTERS for character in ref)
    ):
        return False
    components = ref.split("/")
    return all(
        component
        and component not in {".", ".."}
        and not component.startswith(".")
        and not component.endswith(".lock")
        and windows_path_segment_valid(component)
        and len(component.encode("utf-16-le")) // 2 <= 255
        for component in components
    )


def relative_git_path_valid(value: str) -> bool:
    """Return whether a manifest/checksum path is a canonical Git-tree path."""

    if not isinstance(value, str) or not value or len(value) > 1024:
        return False
    if value.startswith("/") or value.endswith("/") or "\\" in value or "//" in value:
        return False
    return all(
        component not in {"", ".", ".."}
        and windows_path_segment_valid(component)
        and len(component.encode("utf-16-le")) // 2 <= 255
        for component in value.split("/")
    )


def windows_path_segment_valid(value: str) -> bool:
    """Reject Windows aliases/devices for any value used as a path segment."""

    if (
        not isinstance(value, str)
        or not value
        or value.startswith(" ")
        or value.endswith((".", " "))
        or " ." in value
        or "~" in value
    ):
        return False
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return False
    if any(
        ord(character) < 32
        or ord(character) == 127
        or character in _WINDOWS_FORBIDDEN_CHARACTERS
        for character in value
    ):
        return False
    basename = value.split(".", 1)[0].rstrip(" ").casefold()
    return basename not in _WINDOWS_RESERVED_BASENAMES


def managed_node_path_segment(node_id: str) -> str:
    """Map a logical node ID to a deterministic Windows-budgeted segment."""

    if not windows_path_segment_valid(node_id):
        raise ValueError("node_id is not a safe Windows path segment")
    if len(node_id.encode("utf-16-le")) // 2 <= 48:
        return node_id
    digest = hashlib.sha256(node_id.encode("utf-8")).hexdigest()
    return f"node-{digest[:24]}"


def managed_project_path_segment(project_id: str) -> str:
    """Map a logical project ID to a deterministic physical path segment."""

    if not windows_path_segment_valid(project_id):
        raise ValueError("project_id is not a safe Windows path segment")
    if len(project_id.encode("utf-16-le")) // 2 <= 48:
        return project_id
    digest = hashlib.sha256(project_id.encode("utf-8")).hexdigest()
    return f"project-{digest[:24]}"


def managed_sprint_path_segment(sprint_id: str) -> str:
    """Keep content-addressed sprint IDs within the Win32 Git path budget."""

    if not windows_path_segment_valid(sprint_id):
        raise ValueError("sprint_id is not a safe Windows path segment")
    if len(sprint_id.encode("utf-16-le")) // 2 <= 48:
        return sprint_id
    digest = hashlib.sha256(sprint_id.encode("utf-8")).hexdigest()
    return f"sprint-{digest[:24]}"


def windows_absolute_path_key(value: str) -> str | None:
    """Return one lexical Windows identity for an absolute durable path."""

    if not isinstance(value, str) or not value or "\x00" in value:
        return None
    normalized = ntpath.normpath(value)
    submitted = value.replace("\\", "/").rstrip("/").casefold()
    canonical = normalized.replace("\\", "/").rstrip("/").casefold()
    if submitted != canonical:
        return None
    if not ntpath.isabs(normalized):
        return None
    drive, tail = ntpath.splitdrive(normalized)
    if not re.fullmatch(r"[A-Za-z]:", drive) or not tail.startswith(("\\", "/")):
        return None
    components = tail.replace("/", "\\").strip("\\").split("\\")
    if components != [""] and not all(
        windows_path_segment_valid(component) for component in components
    ):
        return None
    key = ntpath.normcase(normalized)
    return key if tail in {"\\", "/"} else key.rstrip("\\/")


def windows_path_is_within(child: str, parent: str) -> bool:
    """Compare absolute Windows paths by components, not string prefixes."""

    child_key = windows_absolute_path_key(child)
    parent_key = windows_absolute_path_key(parent)
    if child_key is None or parent_key is None:
        return False
    try:
        return ntpath.commonpath([child_key, parent_key]) == parent_key
    except ValueError:
        return False


def windows_path_is_strictly_within(child: str, parent: str) -> bool:
    """Return whether child is below, but not equal to, an absolute parent."""

    child_key = windows_absolute_path_key(child)
    parent_key = windows_absolute_path_key(parent)
    return (
        child_key is not None
        and parent_key is not None
        and child_key != parent_key
        and windows_path_is_within(child, parent)
    )


def windows_paths_overlap(first: str, second: str) -> bool:
    """Return whether either absolute Windows path contains the other."""

    return windows_path_is_within(first, second) or windows_path_is_within(
        second, first
    )


def managed_runtime_config_invariant_issues(
    config: Mapping[str, Any],
) -> tuple[str, ...]:
    """Validate normalized managed-root and port relations beyond JSON Schema."""

    if not isinstance(config, Mapping):
        return ("RUNTIME_CONFIG_NOT_OBJECT",)
    issues: list[str] = []

    def add(code: str) -> None:
        if code not in issues:
            issues.append(code)

    path_fields = (
        "service_root",
        "runtime_root",
        "process_runtime_root",
        "log_root",
        "pid_root",
        "lease_root",
        "prompt_root",
        "managed_root",
    )
    path_keys = {
        field: windows_absolute_path_key(config.get(field)) for field in path_fields
    }
    protected_roots = config.get("protected_roots")
    protected_roots = protected_roots if isinstance(protected_roots, list) else []
    protected_keys = [windows_absolute_path_key(root) for root in protected_roots]
    if any(key is None for key in path_keys.values()) or any(
        key is None for key in protected_keys
    ):
        add("RUNTIME_CONFIG_PATH_INVALID")
    if len(protected_keys) != len(set(protected_keys)):
        add("RUNTIME_CONFIG_PATH_CONFLICT")

    runtime_root = config.get("runtime_root")
    for field, child in (
        ("process_runtime_root", "processes"),
        ("log_root", "logs"),
        ("pid_root", "pids"),
        ("lease_root", "leases"),
    ):
        expected = (
            windows_absolute_path_key(ntpath.join(runtime_root, child))
            if isinstance(runtime_root, str)
            else None
        )
        if path_keys.get(field) != expected:
            add("RUNTIME_CONFIG_DERIVED_ROOT_MISMATCH")

    isolated_fields = ("service_root", "runtime_root", "prompt_root", "managed_root")
    for index, first_field in enumerate(isolated_fields):
        for second_field in isolated_fields[index + 1 :]:
            first = config.get(first_field)
            second = config.get(second_field)
            if isinstance(first, str) and isinstance(second, str) and windows_paths_overlap(
                first, second
            ):
                add("RUNTIME_CONFIG_PATH_CONFLICT")
    for field in path_fields:
        path = config.get(field)
        for protected_root in protected_roots:
            if (
                isinstance(path, str)
                and isinstance(protected_root, str)
                and windows_paths_overlap(path, protected_root)
            ):
                add("RUNTIME_CONFIG_PROTECTED_ROOT_CONFLICT")

    http_port = config.get("http_port")
    child_start = config.get("child_port_start")
    child_end = config.get("child_port_end")
    if (
        config.get("http_host") != "127.0.0.1"
        or not isinstance(http_port, int)
        or isinstance(http_port, bool)
        or http_port in {8025, 8026}
    ):
        add("RUNTIME_CONFIG_HTTP_ENDPOINT_INVALID")
    if (
        not isinstance(child_start, int)
        or isinstance(child_start, bool)
        or not isinstance(child_end, int)
        or isinstance(child_end, bool)
        or child_start > child_end
        or any(child_start <= live_port <= child_end for live_port in (8025, 8026))
        or (
            isinstance(http_port, int)
            and not isinstance(http_port, bool)
            and child_start <= http_port <= child_end
        )
    ):
        add("RUNTIME_CONFIG_PORT_RANGE_INVALID")
    return tuple(issues)


def managed_manifest_schema(selection: SprintTypeSelection) -> str | None:
    """Return the managed schema only for the explicit managed pipeline."""

    if selection.pipeline is SprintPipeline.MANAGED_WORKSPACE:
        return MANAGED_MANIFEST_SCHEMA
    return None


def process_transition_allowed(current: str, target: str) -> bool:
    return target in PROCESS_STATE_TRANSITIONS.get(current, frozenset())


def lifecycle_transition_allowed(
    transitions: Mapping[str, frozenset[str]],
    current: str,
    target: str,
) -> bool:
    """Allow an idempotent replay or one declared durable lifecycle edge."""

    return current == target or target in transitions.get(current, frozenset())


def journal_advance_allowed(current: str, target: str) -> bool:
    """Allow an idempotent replay or exactly one monotonic journal step."""

    try:
        current_index = TRANSITION_JOURNAL_STATES.index(current)
        target_index = TRANSITION_JOURNAL_STATES.index(target)
    except ValueError:
        return False
    return target_index in {current_index, current_index + 1}


def managed_graph_semantic_issues(
    definition: Mapping[str, Any],
) -> tuple[str, ...]:
    """Validate graph relations after managed-manifest JSON Schema validation."""

    if not isinstance(definition, Mapping):
        return ("MANAGED_GRAPH_NOT_OBJECT",)

    issues: list[str] = []

    def add(code: str) -> None:
        if code not in issues:
            issues.append(code)

    raw_nodes = definition.get("nodes")
    raw_nodes = raw_nodes if isinstance(raw_nodes, list) else []
    node_by_id: dict[str, Mapping[str, Any]] = {}
    node_ids_casefolded: set[str] = set()
    ordered_task_ids: list[str] = []
    terminal_ids: set[str] = set()
    for node in raw_nodes:
        node_id = node.get("id") if isinstance(node, Mapping) else None
        if not isinstance(node_id, str) or not node_id:
            add("GRAPH_NODE_ID_INVALID")
            continue
        if not windows_path_segment_valid(node_id):
            add("PATH_SEGMENT_UNSAFE")
        if node_id in node_by_id or node_id.casefold() in node_ids_casefolded:
            add("DUPLICATE_NODE_ID")
            continue
        node_by_id[node_id] = node
        node_ids_casefolded.add(node_id.casefold())
        if node.get("type", "task") == "terminal":
            terminal_ids.add(node_id)
        else:
            ordered_task_ids.append(node_id)

    task_ids: set[str] = set()
    task_ids_casefolded: set[str] = set()
    agent_ids: set[str] = set()
    agent_ids_casefolded: set[str] = set()
    phones: set[str] = set()
    for node_id in ordered_task_ids:
        node = node_by_id[node_id]
        agent = node.get("agent")
        if isinstance(agent, Mapping):
            agent_id = agent.get("id")
            phone = agent.get("phone")
            if not isinstance(agent_id, str) or not windows_path_segment_valid(agent_id):
                add("PATH_SEGMENT_UNSAFE")
            if (
                not isinstance(agent_id, str)
                or agent_id in agent_ids
                or agent_id.casefold() in agent_ids_casefolded
            ):
                add("DUPLICATE_AGENT_ID")
            else:
                agent_ids.add(agent_id)
                agent_ids_casefolded.add(agent_id.casefold())
            if not isinstance(phone, str) or phone in phones:
                add("DUPLICATE_AGENT_PHONE")
            else:
                phones.add(phone)
        for task in node.get("tasks", []):
            task_id = task.get("task_id") if isinstance(task, Mapping) else None
            if not isinstance(task_id, str) or not windows_path_segment_valid(task_id):
                add("PATH_SEGMENT_UNSAFE")
            if (
                not isinstance(task_id, str)
                or task_id in task_ids
                or task_id.casefold() in task_ids_casefolded
            ):
                add("DUPLICATE_TASK_ID")
            else:
                task_ids.add(task_id)
                task_ids_casefolded.add(task_id.casefold())

    execution = definition.get("execution")
    reviewers = (
        execution.get("reviewers") if isinstance(execution, Mapping) else []
    )
    reviewers = reviewers if isinstance(reviewers, list) else []
    reviewer_ids: set[str] = set()
    reviewer_ids_casefolded: set[str] = set()
    reviewer_phones: set[str] = set()
    for reviewer in reviewers:
        reviewer_id = reviewer.get("id") if isinstance(reviewer, Mapping) else None
        phone = reviewer.get("phone") if isinstance(reviewer, Mapping) else None
        if not isinstance(reviewer_id, str) or not windows_path_segment_valid(
            reviewer_id
        ):
            add("PATH_SEGMENT_UNSAFE")
        if (
            not isinstance(reviewer_id, str)
            or reviewer_id in reviewer_ids
            or reviewer_id in agent_ids
            or reviewer_id.casefold() in reviewer_ids_casefolded
            or reviewer_id.casefold() in agent_ids_casefolded
        ):
            add("REVIEWER_CONFIGURATION_INVALID")
        else:
            reviewer_ids.add(reviewer_id)
            reviewer_ids_casefolded.add(reviewer_id.casefold())
        if (
            not isinstance(phone, str)
            or phone in reviewer_phones
            or phone in phones
        ):
            add("REVIEWER_CONFIGURATION_INVALID")
        else:
            reviewer_phones.add(phone)

    coordinator = definition.get("coordinator")
    coordinator_node_id = (
        coordinator.get("node_id") if isinstance(coordinator, Mapping) else None
    )
    coordinator_node = (
        node_by_id.get(coordinator_node_id)
        if isinstance(coordinator_node_id, str)
        else None
    )
    routes = coordinator.get("routes") if isinstance(coordinator, Mapping) else None
    if (
        not isinstance(coordinator_node, Mapping)
        or coordinator_node.get("type", "task") != "task"
        or coordinator_node.get("activation_policy") != "any_parent"
        or not isinstance(routes, Mapping)
        or routes.get("STOP") != coordinator_node_id
        or routes.get("NEED_DECISION") != coordinator_node_id
    ):
        add("COORDINATOR_ROUTE_MISSING")

    adjacency: dict[str, list[str]] = {node_id: [] for node_id in node_by_id}
    for node_id in ordered_task_ids:
        node = node_by_id[node_id]
        transitions = node.get("transitions")
        transitions = transitions if isinstance(transitions, Mapping) else {}
        for outcome, target_id in transitions.items():
            if target_id not in node_by_id:
                add("GRAPH_TRANSITION_TARGET_UNKNOWN")
                continue
            if outcome in {"STOP", "NEED_DECISION"} and target_id != coordinator_node_id:
                add("COORDINATOR_ROUTE_COLLISION")
            if target_id not in adjacency[node_id]:
                adjacency[node_id].append(target_id)
        if (
            isinstance(coordinator_node_id, str)
            and coordinator_node_id in node_by_id
            and coordinator_node_id not in adjacency[node_id]
        ):
            adjacency[node_id].append(coordinator_node_id)

    mode = execution.get("mode") if isinstance(execution, Mapping) else None
    if mode == "sequential":
        raw_entries = [execution.get("start_node")]
    elif mode == "parallel":
        start_nodes = execution.get("start_nodes")
        raw_entries = start_nodes if isinstance(start_nodes, list) else []
    else:
        raw_entries = []
    entry_ids: list[str] = []
    for entry_id in raw_entries:
        entry_node = node_by_id.get(entry_id) if isinstance(entry_id, str) else None
        if not isinstance(entry_node, Mapping) or entry_node.get("type", "task") != "task":
            add("GRAPH_START_NODE_UNKNOWN")
        else:
            entry_ids.append(entry_id)

    reachable: set[str] = set()
    pending = list(entry_ids)
    while pending:
        node_id = pending.pop(0)
        if node_id in reachable:
            continue
        reachable.add(node_id)
        pending.extend(adjacency.get(node_id, []))
    if set(node_by_id) != reachable:
        add("GRAPH_NODE_UNREACHABLE")
    if not terminal_ids.intersection(reachable):
        add("GRAPH_TERMINAL_UNREACHABLE")

    can_reach_terminal = set(terminal_ids)
    changed = True
    while changed:
        changed = False
        for source_id, target_ids in adjacency.items():
            if source_id not in can_reach_terminal and any(
                target_id in can_reach_terminal for target_id in target_ids
            ):
                can_reach_terminal.add(source_id)
                changed = True
    if any(
        node_id in reachable and node_id not in can_reach_terminal
        for node_id in ordered_task_ids
    ):
        add("GRAPH_TERMINAL_UNREACHABLE")

    inbound_by_node: dict[str, list[str]] = {
        node_id: [] for node_id in ordered_task_ids
    }
    for source_id in ordered_task_ids:
        for target_id in adjacency.get(source_id, []):
            if target_id in inbound_by_node and source_id not in inbound_by_node[target_id]:
                inbound_by_node[target_id].append(source_id)
    for node_id, inbound_ids in inbound_by_node.items():
        node = node_by_id[node_id]
        policy = node.get("activation_policy", "all_parents")
        order = node.get("join_parent_order")
        if (
            order is not None
            and (
                policy != "all_parents"
                or not isinstance(order, list)
                or len(order) != len(inbound_ids)
                or len(set(order)) != len(order)
                or set(order) != set(inbound_ids)
            )
        ) or (
            policy == "all_parents"
            and len(inbound_ids) > 1
            and order is None
        ):
            add("GRAPH_JOIN_PARENT_ORDER_INVALID")

    process_policy = definition.get("process_policy")
    process_policy = process_policy if isinstance(process_policy, Mapping) else {}
    policy_restart = process_policy.get("restart_policy", "never")
    policy_max_restarts = process_policy.get(
        "max_restart_attempts",
        3 if policy_restart == "on_failure" else 0,
    )
    if (
        policy_restart == "never" and policy_max_restarts != 0
    ) or (
        policy_restart == "on_failure"
        and (
            not isinstance(policy_max_restarts, int)
            or isinstance(policy_max_restarts, bool)
            or not 1 <= policy_max_restarts <= 20
        )
    ):
        add("PROCESS_POLICY_INVALID")
    for node_id in ordered_task_ids:
        workspace = node_by_id[node_id].get("workspace")
        process_launch = (
            workspace.get("process") if isinstance(workspace, Mapping) else None
        )
        if not isinstance(process_launch, Mapping):
            continue
        launch_cwd = process_launch.get("cwd")
        if launch_cwd != "." and not relative_git_path_valid(launch_cwd):
            add("PATH_SEGMENT_UNSAFE")
        if _environment_contains_secret_literal(process_launch.get("environment")):
            add("PROCESS_ENVIRONMENT_SECRET_LITERAL")
        command = process_launch.get("command")
        if isinstance(command, list) and any(
            isinstance(argument, str) and _SECRET_LITERAL.search(argument)
            for argument in command
        ):
            add("PROCESS_COMMAND_SECRET_LITERAL")
        effective_restart = process_launch.get("restart_policy", policy_restart)
        effective_max_restarts = (
            process_launch.get("max_restart_attempts")
            if "max_restart_attempts" in process_launch
            else process_policy.get("max_restart_attempts")
            if "max_restart_attempts" in process_policy
            else 3
            if effective_restart == "on_failure"
            else 0
        )
        if (
            effective_restart == "never" and effective_max_restarts != 0
        ) or (
            effective_restart == "on_failure"
            and (
                not isinstance(effective_max_restarts, int)
                or isinstance(effective_max_restarts, bool)
                or not 1 <= effective_max_restarts <= 20
            )
        ):
            add("PROCESS_POLICY_INVALID")

    files = definition.get("files")
    if isinstance(files, list):
        paths = [
            item.get("path")
            for item in files
            if isinstance(item, Mapping) and isinstance(item.get("path"), str)
        ]
        if len(paths) != len(set(paths)):
            add("MANIFEST_CHECKSUM_PATH_DUPLICATE")
        if len(paths) != len({path.casefold() for path in paths}):
            add("MANIFEST_CHECKSUM_PATH_DUPLICATE")
        if any(not relative_git_path_valid(path) for path in paths):
            add("PATH_SEGMENT_UNSAFE")

    git_policy = definition.get("git")
    effective_branch_names: list[str] = []
    if isinstance(git_policy, Mapping):
        if not git_ref_format_valid(git_policy.get("source_ref")):
            add("SOURCE_REF_INVALID")
        if not git_ref_format_valid(
            git_policy.get("assigned_branch"), branch=True
        ):
            add("PATH_SEGMENT_UNSAFE")
    for node_id in ordered_task_ids:
        workspace = node_by_id[node_id].get("workspace")
        node_git = workspace.get("git") if isinstance(workspace, Mapping) else None
        if isinstance(node_git, Mapping) and not git_ref_format_valid(
            node_git.get("assigned_branch"), branch=True
        ):
            add("PATH_SEGMENT_UNSAFE")
        if isinstance(workspace, Mapping) and workspace.get("access") == "write":
            branch_name = (
                node_git.get("assigned_branch")
                if isinstance(node_git, Mapping)
                else git_policy.get("assigned_branch")
                if isinstance(git_policy, Mapping)
                else None
            )
            if isinstance(branch_name, str):
                effective_branch_names.append(branch_name)
    for reviewer in reviewers:
        if isinstance(reviewer, Mapping) and not git_ref_format_valid(
            reviewer.get("git_branch"), branch=True
        ):
            add("PATH_SEGMENT_UNSAFE")
        if isinstance(reviewer, Mapping) and isinstance(
            reviewer.get("git_branch"), str
        ):
            effective_branch_names.append(reviewer["git_branch"])
    if len(effective_branch_names) != len(
        {branch.casefold() for branch in effective_branch_names}
    ):
        add("BRANCH_CASEFOLD_COLLISION")

    return tuple(issues)


def managed_activation_invariant_issues(state: Mapping[str, Any]) -> tuple[str, ...]:
    """Return relational failures for one managed durable-state snapshot.

    Shape validation by ``managed-runtime-state-v1`` is a required predecessor.
    This pure checker covers equality, ownership, ordering, and lifecycle
    relations that JSON Schema cannot express.
    """

    if not isinstance(state, Mapping):
        return ("STATE_NOT_OBJECT",)
    if state.get("sprint_type") != SprintType.MANAGED_WORKSPACE_V1.value:
        return ()

    issues: list[str] = []

    def add(code: str) -> None:
        if code not in issues:
            issues.append(code)

    def records_by(
        field: str,
        key_field: str,
        duplicate_code: str,
    ) -> dict[Any, Mapping[str, Any]]:
        result: dict[Any, Mapping[str, Any]] = {}
        records = state.get(field)
        if not isinstance(records, list):
            add(f"{field.upper()}_INVALID")
            return result
        for record in records:
            if not isinstance(record, Mapping):
                add(f"{field.upper()}_INVALID")
                continue
            key = record.get(key_field)
            if not isinstance(key, str) or not key:
                add(f"{field.upper()}_INVALID")
                continue
            if key in result:
                add(duplicate_code)
                continue
            result[key] = record
        return result

    status = state.get("status")
    if not isinstance(status, str) or status not in {
        "preparing",
        "active",
        "completed",
        "failed",
        "blocked",
    }:
        add("MANAGED_STATUS_INVALID")

    sprint_id = state.get("sprint_id")
    identity = state.get("identity")
    repository = state.get("repository")
    runtime_config = state.get("runtime_config")
    if isinstance(runtime_config, Mapping):
        for config_issue in managed_runtime_config_invariant_issues(runtime_config):
            add(config_issue)
    else:
        add("RUNTIME_CONFIG_NOT_OBJECT")
    if isinstance(identity, Mapping):
        try:
            derived_sprint_id = SprintProvenance(
                project_id=identity["project_id"],
                repository_id=identity["repository_id"],
                commit=identity["commit"],
                manifest_path=identity["manifest_path"],
                manifest_sha256=identity["manifest_sha256"],
            ).stable_sprint_id()
        except (KeyError, TypeError, ValueError):
            derived_sprint_id = None
        if derived_sprint_id != sprint_id:
            add("SPRINT_IDENTITY_MISMATCH")
        if (
            not isinstance(repository, Mapping)
            or repository.get("repository_id") != identity.get("repository_id")
        ):
            add("REPOSITORY_IDENTITY_MISMATCH")
        elif (
            repository.get("repository_key") != repository.get("canonical_remote")
            or repository.get("mirror_storage_key")
            != mirror_storage_key(repository.get("canonical_remote"))
        ):
            add("REPOSITORY_PROVENANCE_INVALID")
    else:
        add("SPRINT_IDENTITY_MISMATCH")
    if isinstance(repository, Mapping) and not _credential_reference_valid(
        repository.get("credential_reference")
    ):
        add("REPOSITORY_CREDENTIAL_REFERENCE_INVALID")

    assignment_by_id = records_by(
        "assignments", "assignment_id", "ASSIGNMENT_ID_DUPLICATE"
    )
    import_attempt_by_id = records_by(
        "import_attempts", "attempt_id", "IMPORT_ATTEMPT_ID_DUPLICATE"
    )
    workspace_by_id = records_by(
        "workspaces", "workspace_id", "WORKSPACE_ID_DUPLICATE"
    )
    branch_lease_by_id = records_by(
        "branch_leases", "lease_id", "BRANCH_LEASE_ID_DUPLICATE"
    )
    port_lease_by_id = records_by(
        "port_leases", "lease_id", "PORT_LEASE_ID_DUPLICATE"
    )
    process_by_id = records_by("processes", "process_id", "PROCESS_ID_DUPLICATE")
    receipt_by_key = records_by(
        "result_receipts", "result_key", "RESULT_KEY_DUPLICATE"
    )
    review_assignment_by_id = records_by(
        "review_assignments", "assignment_id", "REVIEW_ASSIGNMENT_ID_DUPLICATE"
    )
    review_by_assignment_id = records_by(
        "reviews", "assignment_id", "REVIEW_DECISION_DUPLICATE"
    )
    rework_by_id = records_by("reworks", "rework_id", "REWORK_ID_DUPLICATE")
    rework_by_assignment: dict[str, Mapping[str, Any]] = {}
    for rework in rework_by_id.values():
        new_assignment_id = rework.get("new_assignment_id")
        if not isinstance(new_assignment_id, str):
            add("REWORK_BINDING_INVALID")
        elif new_assignment_id in rework_by_assignment:
            add("REWORK_ASSIGNMENT_DUPLICATE")
        else:
            rework_by_assignment[new_assignment_id] = rework
    integration_by_id = records_by(
        "integrations", "integration_id", "INTEGRATION_ID_DUPLICATE"
    )
    integration_workspace_by_id = records_by(
        "integration_workspaces",
        "workspace_artifact_id",
        "INTEGRATION_WORKSPACE_ID_DUPLICATE",
    )
    repair_by_id = records_by("repairs", "repair_id", "REPAIR_ID_DUPLICATE")
    journal_by_result = records_by(
        "transition_journal", "result_key", "TRANSITION_JOURNAL_DUPLICATE"
    )
    outbox_by_id = records_by("outbox", "event_id", "OUTBOX_EVENT_ID_DUPLICATE")
    coordinator_context_by_id = records_by(
        "coordinator_contexts", "context_id", "COORDINATOR_CONTEXT_ID_DUPLICATE"
    )
    blocker_by_fingerprint = records_by(
        "blocker_observations", "fingerprint", "BLOCKER_OBSERVATION_DUPLICATE"
    )
    recovery_by_id = records_by(
        "recovery_records", "recovery_id", "RECOVERY_ID_DUPLICATE"
    )

    raw_revisions = state.get("graph_revisions")
    revision_by_number: dict[int, Mapping[str, Any]] = {}
    ordered_revision_numbers: list[int] = []
    if isinstance(raw_revisions, list):
        for revision in raw_revisions:
            if not isinstance(revision, Mapping):
                add("GRAPH_REVISION_HISTORY_INVALID")
                continue
            number = revision.get("revision")
            if not isinstance(number, int) or isinstance(number, bool):
                add("GRAPH_REVISION_HISTORY_INVALID")
                continue
            ordered_revision_numbers.append(number)
            if number in revision_by_number:
                add("GRAPH_REVISION_DUPLICATE")
            else:
                revision_by_number[number] = revision
    else:
        add("GRAPH_REVISION_HISTORY_INVALID")

    graph_revision = state.get("graph_revision")
    valid_graph_revision = (
        isinstance(graph_revision, int)
        and not isinstance(graph_revision, bool)
        and graph_revision >= 1
    )
    expected_revisions = (
        list(range(1, graph_revision + 1)) if valid_graph_revision else []
    )
    if (
        ordered_revision_numbers != expected_revisions
        or not valid_graph_revision
        or graph_revision not in revision_by_number
    ):
        add("GRAPH_REVISION_HISTORY_INVALID")

    nodes_by_revision: dict[int, dict[str, Mapping[str, Any]]] = {}
    for number, revision in revision_by_number.items():
        definition = revision.get("definition")
        try:
            digest = canonical_json_sha256(definition)
        except ValueError:
            digest = None
        if digest != revision.get("definition_sha256"):
            add("GRAPH_REVISION_DIGEST_MISMATCH")
        if isinstance(definition, Mapping):
            for semantic_issue in managed_graph_semantic_issues(definition):
                add(semantic_issue)
            if (
                "git_address" in definition
                and (
                    not isinstance(repository, Mapping)
                    or _repository_assertion_key(definition.get("git_address"))
                    != repository.get("canonical_remote")
                )
            ):
                add("REPOSITORY_IDENTITY_MISMATCH")
        revision_nodes: dict[str, Mapping[str, Any]] = {}
        if isinstance(definition, Mapping) and isinstance(definition.get("nodes"), list):
            for node in definition["nodes"]:
                if isinstance(node, Mapping) and isinstance(node.get("id"), str):
                    if node["id"] in revision_nodes:
                        add("GRAPH_NODE_ID_DUPLICATE")
                    else:
                        revision_nodes[node["id"]] = node
        nodes_by_revision[number] = revision_nodes
    if valid_graph_revision and not nodes_by_revision.get(graph_revision, {}):
        add("CURRENT_GRAPH_DEFINITION_MISSING")

    def definition_inbound_parent_ids(
        definition: Any,
        target_node_id: Any,
    ) -> list[str]:
        """Derive explicit and reserved Coordinator predecessors in node order."""

        result: list[str] = []
        if not isinstance(definition, Mapping) or not isinstance(
            definition.get("nodes"), list
        ):
            return result
        for candidate in definition["nodes"]:
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
                result.append(candidate["id"])
        coordinator = definition.get("coordinator")
        if (
            isinstance(coordinator, Mapping)
            and coordinator.get("node_id") == target_node_id
        ):
            for candidate in definition["nodes"]:
                if (
                    isinstance(candidate, Mapping)
                    and candidate.get("type", "task") == "task"
                    and isinstance(candidate.get("id"), str)
                    and candidate["id"] not in result
                ):
                    result.append(candidate["id"])
        return result

    def definition_outcome_target(
        definition: Any,
        source_node_id: Any,
        outcome: Any,
    ) -> Any:
        """Resolve an outcome using its pinned definition and reserved routes."""

        if not isinstance(definition, Mapping):
            return None
        if not isinstance(outcome, str):
            return None
        if outcome in {"STOP", "NEED_DECISION"}:
            coordinator = definition.get("coordinator")
            routes = (
                coordinator.get("routes")
                if isinstance(coordinator, Mapping)
                else None
            )
            return routes.get(outcome) if isinstance(routes, Mapping) else None
        raw_nodes = definition.get("nodes")
        nodes = raw_nodes if isinstance(raw_nodes, list) else []
        source_node = next(
            (
                node
                for node in nodes
                if isinstance(node, Mapping) and node.get("id") == source_node_id
            ),
            None,
        )
        transitions = (
            source_node.get("transitions")
            if isinstance(source_node, Mapping)
            else None
        )
        return transitions.get(outcome) if isinstance(transitions, Mapping) else None

    first_revision = revision_by_number.get(1)
    if isinstance(first_revision, Mapping) and (
        first_revision.get("source") != "activation"
        or first_revision.get("repair_id") is not None
        or first_revision.get("artifact_source_commit")
        != state.get("workspace_source_commit")
    ):
        add("GRAPH_REVISION_PROVENANCE_INVALID")
    for number, revision in revision_by_number.items():
        if number == 1:
            continue
        repair = repair_by_id.get(revision.get("repair_id"))
        response = repair.get("response") if isinstance(repair, Mapping) else None
        previous_definition = revision_by_number.get(number - 1, {}).get("definition")
        patch = repair.get("patch") if isinstance(repair, Mapping) else None
        try:
            if not isinstance(repair, Mapping) or not isinstance(patch, Mapping):
                raise ValueError("repair record is missing")
            expected_fingerprint = repair_request_fingerprint(
                sprint_id,
                number - 1,
                repair.get("repair_source_commit"),
                patch,
            )
            expected_definition = _apply_managed_repair_patch(
                previous_definition,
                patch,
            )
        except (TypeError, ValueError):
            expected_fingerprint = None
            expected_definition = None
        immutable_node_ids = {
            assignment.get("node_id")
            for assignment in assignment_by_id.values()
            if isinstance(assignment.get("graph_revision"), int)
            and not isinstance(assignment.get("graph_revision"), bool)
            and assignment.get("graph_revision") <= number - 1
            and isinstance(assignment.get("node_id"), str)
        }
        immutable_agent_ids: set[str] = set()
        if isinstance(previous_definition, Mapping) and isinstance(
            previous_definition.get("nodes"), list
        ):
            for previous_node in previous_definition["nodes"]:
                agent = (
                    previous_node.get("agent")
                    if isinstance(previous_node, Mapping)
                    and previous_node.get("id") in immutable_node_ids
                    else None
                )
                if isinstance(agent, Mapping) and isinstance(agent.get("id"), str):
                    immutable_agent_ids.add(agent["id"])
        future_node_ids = {
            node.get("id")
            for node in patch.get("future_nodes", [])
            if isinstance(patch, Mapping)
            and isinstance(patch.get("future_nodes"), list)
            and isinstance(node, Mapping)
            and isinstance(node.get("id"), str)
        } if isinstance(patch, Mapping) else set()
        removed_node_ids = (
            set(patch.get("remove_future_node_ids", []))
            if isinstance(patch, Mapping)
            and isinstance(patch.get("remove_future_node_ids"), list)
            else set()
        )
        prompt_node_ids = (
            set(patch.get("prompts", {}))
            if isinstance(patch, Mapping)
            and isinstance(patch.get("prompts"), Mapping)
            else set()
        )
        profile_agent_ids = (
            set(patch.get("profiles", {}))
            if isinstance(patch, Mapping)
            and isinstance(patch.get("profiles"), Mapping)
            else set()
        )
        if (
            immutable_node_ids.intersection(
                future_node_ids | removed_node_ids | prompt_node_ids
            )
            or immutable_agent_ids.intersection(profile_agent_ids)
        ):
            add("REPAIR_IMMUTABLE_HISTORY")
        if (
            revision.get("source") != "repair"
            or not isinstance(repair, Mapping)
            or repair.get("from_revision") != number - 1
            or repair.get("to_revision") != number
            or repair.get("repair_source_commit")
            != revision.get("artifact_source_commit")
            or repair.get("request_fingerprint") != expected_fingerprint
            or revision.get("definition") != expected_definition
            or not isinstance(response, Mapping)
            or response.get("sprint_id") != sprint_id
            or response.get("from_revision") != number - 1
            or response.get("graph_revision") != number
            or response.get("repair_source_commit")
            != repair.get("repair_source_commit")
            or response.get("deduplicated") is not False
        ):
            add("GRAPH_REPAIR_HISTORY_INVALID")
    repair_idempotency_keys: set[str] = set()
    for repair in repair_by_id.values():
        from_revision = repair.get("from_revision")
        to_revision = repair.get("to_revision")
        repair_key = repair.get("idempotency_key")
        if not isinstance(repair_key, str) or repair_key in repair_idempotency_keys:
            add("REPAIR_IDEMPOTENCY_KEY_DUPLICATE")
        else:
            repair_idempotency_keys.add(repair_key)
        if (
            not isinstance(from_revision, int)
            or isinstance(from_revision, bool)
            or to_revision != from_revision + 1
            or not isinstance(to_revision, int)
            or revision_by_number.get(to_revision, {}).get("repair_id")
            != repair.get("repair_id")
        ):
            add("GRAPH_REPAIR_HISTORY_INVALID")

    for assignment in assignment_by_id.values():
        assignment_revision = assignment.get("graph_revision")
        if (
            not isinstance(assignment_revision, int)
            or isinstance(assignment_revision, bool)
            or assignment_revision not in revision_by_number
        ):
            add("ASSIGNMENT_GRAPH_REVISION_UNKNOWN")

    workflow = state.get("workflow")
    occurrence_by_id: dict[str, Mapping[str, Any]] = {}
    token_by_id: dict[str, Mapping[str, Any]] = {}
    if isinstance(workflow, Mapping):
        if workflow.get("graph_revision") != graph_revision:
            add("WORKFLOW_REVISION_INVALID")
        raw_occurrences = workflow.get("occurrences")
        if isinstance(raw_occurrences, list):
            for occurrence in raw_occurrences:
                if not isinstance(occurrence, Mapping):
                    add("OCCURRENCE_RECORD_INVALID")
                    continue
                occurrence_id = occurrence.get("occurrence_id")
                if not isinstance(occurrence_id, str):
                    add("OCCURRENCE_RECORD_INVALID")
                    continue
                if occurrence_id in occurrence_by_id:
                    add("OCCURRENCE_ID_DUPLICATE")
                else:
                    occurrence_by_id[occurrence_id] = occurrence
        else:
            add("OCCURRENCE_RECORD_INVALID")
        raw_tokens = workflow.get("transition_tokens")
        if isinstance(raw_tokens, list):
            for token in raw_tokens:
                if not isinstance(token, Mapping):
                    add("TRANSITION_TOKEN_RECORD_INVALID")
                    continue
                token_id = token.get("token_id")
                if not isinstance(token_id, str):
                    add("TRANSITION_TOKEN_RECORD_INVALID")
                    continue
                if token_id in token_by_id:
                    add("TRANSITION_TOKEN_ID_DUPLICATE")
                else:
                    token_by_id[token_id] = token
        else:
            add("TRANSITION_TOKEN_RECORD_INVALID")
    elif status == "active":
        add("WORKFLOW_MISSING")

    current_revision_record = (
        revision_by_number.get(graph_revision) if valid_graph_revision else None
    )
    current_definition = (
        current_revision_record.get("definition")
        if isinstance(current_revision_record, Mapping)
        else None
    )
    current_nodes = (
        nodes_by_revision.get(graph_revision, {}) if valid_graph_revision else {}
    )
    for assignment in assignment_by_id.values():
        assignment_status = assignment.get("status")
        assignment_revision = assignment.get("graph_revision")
        if assignment_status not in {"prepared", "active", "reviews_pending"}:
            continue
        source_node_id = assignment.get("node_id")
        source_definition = (
            revision_by_number.get(assignment_revision, {}).get("definition")
            if isinstance(assignment_revision, int)
            and not isinstance(assignment_revision, bool)
            else None
        )
        possible_outcomes = (
            [assignment.get("outcome")]
            if assignment_status == "reviews_pending"
            else assignment.get("allowed_outcomes")
        )
        if not isinstance(possible_outcomes, list):
            possible_outcomes = []
        for outcome in possible_outcomes:
            target_node_id = definition_outcome_target(
                source_definition,
                source_node_id,
                outcome,
            )
            target_node = (
                current_nodes.get(target_node_id)
                if isinstance(target_node_id, str)
                else None
            )
            if not isinstance(target_node, Mapping):
                add("REPAIR_LIVE_ASSIGNMENT_TARGET_INVALID")
            elif (
                target_node.get("type", "task") == "task"
                and source_node_id
                not in definition_inbound_parent_ids(
                    current_definition,
                    target_node_id,
                )
            ):
                add("REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE")
    current_execution = (
        current_definition.get("execution")
        if isinstance(current_definition, Mapping)
        else None
    )
    if isinstance(workflow, Mapping) and isinstance(current_execution, Mapping):
        mode = current_execution.get("mode")
        expected_entries = (
            [current_execution.get("start_node")]
            if mode == "sequential"
            else current_execution.get("start_nodes")
        )
        entry_node_ids = workflow.get("entry_node_ids")
        entry_occurrence_ids = workflow.get("entry_occurrence_ids")
        if (
            workflow.get("execution_mode") != mode
            or not isinstance(expected_entries, list)
            or entry_node_ids != expected_entries
            or not isinstance(entry_occurrence_ids, list)
            or len(entry_occurrence_ids) != len(entry_node_ids)
        ):
            add("WORKFLOW_ENTRY_SET_INVALID")
        else:
            for node_id, occurrence_id in zip(entry_node_ids, entry_occurrence_ids):
                occurrence = occurrence_by_id.get(occurrence_id)
                if (
                    not isinstance(occurrence, Mapping)
                    or occurrence.get("node_id") != node_id
                    or occurrence.get("activation_policy") != "entry"
                    or occurrence.get("generation") != 1
                    or occurrence.get("trigger_token_ids") != []
                ):
                    add("WORKFLOW_ENTRY_SET_INVALID")

    for occurrence_id, occurrence in occurrence_by_id.items():
        occurrence_revision = occurrence.get("graph_revision")
        node_id = occurrence.get("node_id")
        generation = occurrence.get("generation")
        trigger_ids = occurrence.get("trigger_token_ids")
        try:
            expected_id = managed_occurrence_id(
                sprint_id,
                occurrence_revision,
                node_id,
                generation,
                trigger_ids,
            )
        except (TypeError, ValueError):
            expected_id = None
        if occurrence_id != expected_id:
            add("OCCURRENCE_ID_MISMATCH")
        node = (
            nodes_by_revision.get(occurrence_revision, {}).get(node_id)
            if isinstance(occurrence_revision, int)
            and not isinstance(occurrence_revision, bool)
            and isinstance(node_id, str)
            else None
        )
        if not isinstance(node, Mapping):
            add("OCCURRENCE_GRAPH_BINDING_INVALID")
        definition = revision_by_number.get(occurrence_revision, {}).get("definition")
        inbound_parent_ids = definition_inbound_parent_ids(definition, node_id)
        assignment_ids = occurrence.get("assignment_ids")
        if not isinstance(assignment_ids, list):
            add("OCCURRENCE_ASSIGNMENTS_INVALID")
            assignment_ids = []
        for assignment_id in assignment_ids:
            assignment = (
                assignment_by_id.get(assignment_id)
                if isinstance(assignment_id, str)
                else None
            )
            if (
                not isinstance(assignment, Mapping)
                or assignment.get("occurrence_id") != occurrence_id
                or assignment.get("node_id") != node_id
            ):
                add("OCCURRENCE_ASSIGNMENTS_INVALID")
        if not assignment_ids or len(assignment_ids) != len(set(assignment_ids)):
            add("OCCURRENCE_ASSIGNMENT_LINEAGE_INVALID")
        else:
            first_assignment = assignment_by_id.get(assignment_ids[0])
            if (
                not isinstance(first_assignment, Mapping)
                or first_assignment.get("source_kind") == "rework_result"
                or first_assignment.get("rework_cycle") != 0
                or assignment_ids[0] in rework_by_assignment
            ):
                add("OCCURRENCE_ASSIGNMENT_LINEAGE_INVALID")
            for cycle, replacement_assignment_id in enumerate(
                assignment_ids[1:], start=1
            ):
                replacement = assignment_by_id.get(replacement_assignment_id)
                rework = rework_by_assignment.get(replacement_assignment_id)
                if (
                    not isinstance(replacement, Mapping)
                    or replacement.get("source_kind") != "rework_result"
                    or replacement.get("rework_cycle") != cycle
                    or not isinstance(rework, Mapping)
                    or rework.get("rework_cycle") != cycle
                ):
                    add("OCCURRENCE_ASSIGNMENT_LINEAGE_INVALID")
        occurrence_state = occurrence.get("state")
        live_in_occurrence = [
            assignment_id
            for assignment_id in assignment_ids
            if isinstance(assignment_id, str)
            and assignment_by_id.get(assignment_id, {}).get("status")
            in {"active", "reviews_pending"}
        ]
        if isinstance(occurrence_state, str) and occurrence_state in {
            "active",
            "reviews_pending",
        }:
            if (
                len(live_in_occurrence) != 1
                or assignment_by_id[live_in_occurrence[0]].get("status")
                != occurrence_state
            ):
                add("OCCURRENCE_STATE_INVALID")
        elif live_in_occurrence:
            add("OCCURRENCE_STATE_INVALID")
        if occurrence.get("activation_policy") == "entry":
            if trigger_ids != [] or generation != 1:
                add("OCCURRENCE_TRIGGER_INVALID")
        else:
            expected_policy = (
                node.get("activation_policy", "all_parents")
                if isinstance(node, Mapping)
                else None
            )
            trigger_tokens = (
                [token_by_id.get(token_id) for token_id in trigger_ids]
                if isinstance(trigger_ids, list)
                and all(isinstance(token_id, str) for token_id in trigger_ids)
                else []
            )
            source_node_ids = [
                token.get("source_node_id")
                for token in trigger_tokens
                if isinstance(token, Mapping)
            ]
            join_parent_order = (
                node.get("join_parent_order") if isinstance(node, Mapping) else None
            )
            all_parent_order_valid = (
                (
                    len(inbound_parent_ids) <= 1
                    and (
                        join_parent_order is None
                        or join_parent_order == inbound_parent_ids
                    )
                )
                or (
                    len(inbound_parent_ids) > 1
                    and isinstance(join_parent_order, list)
                    and len(join_parent_order) == len(inbound_parent_ids)
                    and len(set(join_parent_order)) == len(join_parent_order)
                    and set(join_parent_order) == set(inbound_parent_ids)
                )
            )
            expected_parent_order = (
                join_parent_order if isinstance(join_parent_order, list) else inbound_parent_ids
            )
            deterministic_trigger_ids: list[str] = []
            if expected_policy == "any_parent":
                any_parent_candidates: list[tuple[Any, Any, str]] = []
                for candidate_token_id, candidate_token in token_by_id.items():
                    candidate_consumed_by = candidate_token.get(
                        "consumed_by_occurrence_id"
                    )
                    if (
                        candidate_token.get("source_node_id")
                        not in inbound_parent_ids
                        or candidate_token.get("target_node_id") != node_id
                        or (
                            candidate_token.get("status") != "available"
                            and candidate_consumed_by != occurrence_id
                        )
                    ):
                        continue
                    source_occurrence = occurrence_by_id.get(
                        candidate_token.get("source_occurrence_id")
                    )
                    if isinstance(source_occurrence, Mapping):
                        any_parent_candidates.append(
                            (
                                source_occurrence.get("generation"),
                                candidate_token.get("result_key"),
                                candidate_token_id,
                            )
                        )
                if any_parent_candidates:
                    deterministic_trigger_ids.append(
                        min(any_parent_candidates)[2]
                    )
            if expected_policy == "all_parents" and all_parent_order_valid:
                for parent_id in expected_parent_order:
                    candidates: list[tuple[Any, Any, str]] = []
                    for candidate_token_id, candidate_token in token_by_id.items():
                        candidate_consumed_by = candidate_token.get(
                            "consumed_by_occurrence_id"
                        )
                        if (
                            candidate_token.get("source_node_id") != parent_id
                            or candidate_token.get("target_node_id") != node_id
                            or (
                                candidate_token.get("status") != "available"
                                and candidate_consumed_by != occurrence_id
                            )
                        ):
                            continue
                        source_occurrence = occurrence_by_id.get(
                            candidate_token.get("source_occurrence_id")
                        )
                        if isinstance(source_occurrence, Mapping):
                            candidates.append(
                                (
                                    source_occurrence.get("generation"),
                                    candidate_token.get("result_key"),
                                    candidate_token_id,
                                )
                            )
                    if candidates:
                        deterministic_trigger_ids.append(min(candidates)[2])
            if (
                not trigger_ids
                or occurrence.get("activation_policy") != expected_policy
                or len(trigger_tokens) != len(trigger_ids)
                or (
                    expected_policy == "any_parent"
                    and (
                        len(source_node_ids) != 1
                        or source_node_ids[0] not in inbound_parent_ids
                        or trigger_ids != deterministic_trigger_ids
                    )
                )
                or (
                    expected_policy == "all_parents"
                    and (
                        not all_parent_order_valid
                        or source_node_ids != expected_parent_order
                        or trigger_ids != deterministic_trigger_ids
                    )
                )
            ):
                add("OCCURRENCE_TRIGGER_INVALID")

    generations_by_node: dict[Any, list[Any]] = {}
    for occurrence in occurrence_by_id.values():
        node_id = occurrence.get("node_id")
        if isinstance(node_id, str):
            generations_by_node.setdefault(node_id, []).append(
                occurrence.get("generation")
            )
    for generations in generations_by_node.values():
        if (
            not all(
                isinstance(generation, int) and not isinstance(generation, bool)
                for generation in generations
            )
            or sorted(generations) != list(range(1, len(generations) + 1))
        ):
            add("OCCURRENCE_GENERATION_INVALID")

    for result_key, journal in journal_by_result.items():
        receipt = receipt_by_key.get(result_key)
        token_ids = journal.get("transition_token_ids")
        target_occurrence_ids = journal.get("target_occurrence_ids")
        outbox_event_ids = journal.get("outbox_event_ids")
        token_ids = token_ids if isinstance(token_ids, list) else []
        target_occurrence_ids = (
            target_occurrence_ids if isinstance(target_occurrence_ids, list) else []
        )
        outbox_event_ids = outbox_event_ids if isinstance(outbox_event_ids, list) else []
        if (
            not isinstance(receipt, Mapping)
            or journal.get("assignment_id") != receipt.get("assignment_id")
            or journal.get("outcome") != receipt.get("outcome")
            or journal.get("result_commit") != receipt.get("result_commit")
        ):
            add("TRANSITION_JOURNAL_BINDING_INVALID")
        if any(
            token_by_id.get(token_id, {}).get("result_key") != result_key
            for token_id in token_ids
            if isinstance(token_id, str)
        ) or any(token_id not in token_by_id for token_id in token_ids):
            add("TRANSITION_JOURNAL_EFFECT_INVALID")
        if any(occurrence_id not in occurrence_by_id for occurrence_id in target_occurrence_ids):
            add("TRANSITION_JOURNAL_EFFECT_INVALID")
        if any(event_id not in outbox_by_id for event_id in outbox_event_ids):
            add("TRANSITION_JOURNAL_EFFECT_INVALID")
        if journal.get("state") == "NEXT_ASSIGNMENT_ENQUEUED":
            for occurrence_id in target_occurrence_ids:
                occurrence = occurrence_by_id.get(occurrence_id)
                if not isinstance(occurrence, Mapping) or not any(
                    token_id in occurrence.get("trigger_token_ids", [])
                    for token_id in token_ids
                ):
                    add("TRANSITION_JOURNAL_EFFECT_INVALID")
        source_assignment = assignment_by_id.get(journal.get("assignment_id"))
        source_occurrence = (
            occurrence_by_id.get(source_assignment.get("occurrence_id"))
            if isinstance(source_assignment, Mapping)
            else None
        )
        source_lease = (
            branch_lease_by_id.get(source_assignment.get("branch_lease_id"))
            if isinstance(source_assignment, Mapping)
            and isinstance(source_assignment.get("branch_lease_id"), str)
            else None
        )
        if journal.get("disposition") == "accepted" and journal.get("state") in {
            "REVIEWS_ACCEPTED",
            "TRANSITION_COMMITTED",
            "NEXT_ASSIGNMENT_ENQUEUED",
        }:
            if (
                not isinstance(source_assignment, Mapping)
                or source_assignment.get("status") != "completed"
                or not isinstance(source_occurrence, Mapping)
                or source_occurrence.get("state") != "completed"
                or (
                    isinstance(source_lease, Mapping)
                    and source_lease.get("status") != "released"
                )
            ):
                add("TRANSITION_SOURCE_NOT_SETTLED")
        elif journal.get("disposition") == "reworked" and (
            not isinstance(source_assignment, Mapping)
            or source_assignment.get("status") != "completed"
            or (
                isinstance(source_lease, Mapping)
                and source_lease.get("status") != "released"
            )
        ):
            add("TRANSITION_SOURCE_NOT_SETTLED")
    for assignment_id, assignment in assignment_by_id.items():
        assignment_occurrence_id = assignment.get("occurrence_id")
        occurrence = (
            occurrence_by_id.get(assignment_occurrence_id)
            if isinstance(assignment_occurrence_id, str)
            else None
        )
        if (
            not isinstance(occurrence, Mapping)
            or assignment_id not in occurrence.get("assignment_ids", [])
        ):
            add("ASSIGNMENT_OCCURRENCE_INVALID")

    accepted_result_keys = {
        result_key
        for result_key, journal in journal_by_result.items()
        if journal.get("disposition") == "accepted"
        and isinstance(journal.get("state"), str)
        and journal.get("state")
        in {"TRANSITION_COMMITTED", "NEXT_ASSIGNMENT_ENQUEUED"}
    }
    for token_id, token in token_by_id.items():
        source_occurrence_id = token.get("source_occurrence_id")
        source_occurrence = (
            occurrence_by_id.get(source_occurrence_id)
            if isinstance(source_occurrence_id, str)
            else None
        )
        result_key = token.get("result_key")
        target_node_id = token.get("target_node_id")
        try:
            expected_id = managed_transition_token_id(
                sprint_id,
                source_occurrence_id,
                result_key,
                target_node_id,
            )
        except (TypeError, ValueError):
            expected_id = None
        if token_id != expected_id:
            add("TRANSITION_TOKEN_ID_MISMATCH")
        receipt = receipt_by_key.get(result_key) if isinstance(result_key, str) else None
        source_assignment = (
            assignment_by_id.get(receipt.get("assignment_id"))
            if isinstance(receipt, Mapping)
            else None
        )
        source_revision = (
            source_occurrence.get("graph_revision")
            if isinstance(source_occurrence, Mapping)
            else None
        )
        source_node = (
            nodes_by_revision.get(source_revision, {}).get(
                source_occurrence.get("node_id")
            )
            if isinstance(source_revision, int)
            and not isinstance(source_revision, bool)
            and isinstance(source_occurrence, Mapping)
            else None
        )
        source_definition = revision_by_number.get(source_revision, {}).get("definition")
        outcome = receipt.get("outcome") if isinstance(receipt, Mapping) else None
        source_transition_target = definition_outcome_target(
            source_definition,
            source_occurrence.get("node_id")
            if isinstance(source_occurrence, Mapping)
            else None,
            outcome,
        )
        target_revision = token.get("target_graph_revision")
        target_node = (
            nodes_by_revision.get(target_revision, {}).get(target_node_id)
            if isinstance(target_revision, int)
            and not isinstance(target_revision, bool)
            and isinstance(target_node_id, str)
            else None
        )
        source_target_node = (
            nodes_by_revision.get(source_revision, {}).get(target_node_id)
            if isinstance(source_revision, int)
            and not isinstance(source_revision, bool)
            and isinstance(target_node_id, str)
            else None
        )
        current_target_node = (
            nodes_by_revision.get(graph_revision, {}).get(target_node_id)
            if valid_graph_revision and isinstance(target_node_id, str)
            else None
        )
        if (
            not isinstance(source_occurrence, Mapping)
            or token.get("source_node_id") != source_occurrence.get("node_id")
            or token.get("source_graph_revision") != source_revision
            or not isinstance(source_assignment, Mapping)
            or source_assignment.get("occurrence_id") != source_occurrence_id
            or result_key not in accepted_result_keys
            or source_transition_target != target_node_id
            or not isinstance(source_target_node, Mapping)
        ):
            add("TRANSITION_TOKEN_PROVENANCE_INVALID")
        consumed_by = token.get("consumed_by_occurrence_id")
        if token.get("status") == "consumed":
            target_occurrence = (
                occurrence_by_id.get(consumed_by)
                if isinstance(consumed_by, str)
                else None
            )
            if (
                not isinstance(target_occurrence, Mapping)
                or token_id not in target_occurrence.get("trigger_token_ids", [])
                or target_occurrence.get("node_id") != target_node_id
                or target_occurrence.get("graph_revision") != target_revision
                or not isinstance(target_node, Mapping)
            ):
                add("TRANSITION_TOKEN_CONSUMPTION_INVALID")
        elif token.get("status") == "available":
            if consumed_by is not None or target_revision is not None:
                add("TRANSITION_TOKEN_CONSUMPTION_INVALID")
        elif consumed_by is not None or not isinstance(target_node, Mapping):
            add("TRANSITION_TOKEN_CONSUMPTION_INVALID")
        token_status = token.get("status")
        if (
            token_status == "available"
            and (
                not isinstance(current_target_node, Mapping)
                or current_target_node.get("type", "task") == "terminal"
            )
        ) or (
            token_status in {"consumed", "terminal"}
            and isinstance(target_node, Mapping)
            and (
                (token_status == "terminal")
                != (target_node.get("type", "task") == "terminal")
            )
        ):
            add("TRANSITION_TOKEN_TARGET_INVALID")
        if (
            token_status == "available"
            and isinstance(current_target_node, Mapping)
            and token.get("source_node_id")
            not in definition_inbound_parent_ids(
                current_definition, target_node_id
            )
        ):
            add("TRANSITION_TOKEN_TARGET_INELIGIBLE")
        journal = journal_by_result.get(result_key)
        if token_status == "consumed":
            target_occurrence = (
                occurrence_by_id.get(consumed_by)
                if isinstance(consumed_by, str)
                else None
            )
            target_assignment_id = (
                target_occurrence.get("assignment_ids", [None])[0]
                if isinstance(target_occurrence, Mapping)
                and isinstance(target_occurrence.get("assignment_ids"), list)
                and target_occurrence.get("assignment_ids")
                else None
            )
            target_outbox_ids = [
                event_id
                for event_id, event in outbox_by_id.items()
                if event.get("event_type") == "ASSIGNMENT_ENQUEUE"
                and isinstance(event.get("payload"), Mapping)
                and event["payload"].get("assignment_id") == target_assignment_id
            ]
            if (
                not isinstance(journal, Mapping)
                or journal.get("state") != "NEXT_ASSIGNMENT_ENQUEUED"
                or token_id not in journal.get("transition_token_ids", [])
                or consumed_by not in journal.get("target_occurrence_ids", [])
                or len(target_outbox_ids) != 1
                or target_outbox_ids[0] not in journal.get("outbox_event_ids", [])
            ):
                add("TRANSITION_JOURNAL_EFFECT_INVALID")
        elif token_status in {"available", "terminal"} and (
            not isinstance(journal, Mapping)
            or journal.get("state") != "TRANSITION_COMMITTED"
            or token_id not in journal.get("transition_token_ids", [])
            or journal.get("target_occurrence_ids") != []
            or journal.get("outbox_event_ids") != []
        ):
            add("TRANSITION_JOURNAL_EFFECT_INVALID")

    for occurrence_id, occurrence in occurrence_by_id.items():
        for token_id in occurrence.get("trigger_token_ids", []):
            token = token_by_id.get(token_id) if isinstance(token_id, str) else None
            if (
                not isinstance(token, Mapping)
                or token.get("status") != "consumed"
                or token.get("consumed_by_occurrence_id") != occurrence_id
                or token.get("target_node_id") != occurrence.get("node_id")
            ):
                add("OCCURRENCE_TRIGGER_INVALID")

    raw_active_ids = state.get("active_assignment_ids")
    if (
        isinstance(raw_active_ids, list)
        and all(isinstance(item, str) and item for item in raw_active_ids)
        and len(raw_active_ids) == len(set(raw_active_ids))
    ):
        active_ids = raw_active_ids
    else:
        active_ids = []
        add("ACTIVE_ASSIGNMENTS_INVALID")
    active_id_set = set(active_ids)
    live_assignment_ids = {
        assignment_id
        for assignment_id, assignment in assignment_by_id.items()
        if assignment.get("status") in {"active", "reviews_pending"}
    }
    if live_assignment_ids != active_id_set:
        add("ACTIVE_ASSIGNMENT_SET_MISMATCH")

    raw_outcomes = state.get("allowed_outcomes_by_assignment")
    outcomes = raw_outcomes if isinstance(raw_outcomes, Mapping) else {}
    valid_outcomes = {
        assignment_id: values
        for assignment_id, values in outcomes.items()
        if isinstance(assignment_id, str)
        and isinstance(values, list)
        and values
        and all(isinstance(value, str) and value for value in values)
        and len(values) == len(set(values))
    }
    if set(outcomes) != active_id_set or len(valid_outcomes) != len(outcomes):
        add("ACTIVE_OUTCOMES_INVALID")
    project_id = identity.get("project_id") if isinstance(identity, Mapping) else None
    repository_id = (
        repository.get("repository_id") if isinstance(repository, Mapping) else None
    )
    repository_key = (
        repository.get("repository_key") if isinstance(repository, Mapping) else None
    )
    mirror_key = (
        repository.get("mirror_storage_key") if isinstance(repository, Mapping) else None
    )
    mirror_path = repository.get("mirror_path") if isinstance(repository, Mapping) else None
    mirror_path_key = windows_absolute_path_key(mirror_path)
    managed_root = (
        runtime_config.get("managed_root")
        if isinstance(runtime_config, Mapping)
        else None
    )
    managed_root_key = windows_absolute_path_key(managed_root)
    expected_mirror_path_key = (
        windows_absolute_path_key(
            ntpath.join(
                managed_root_key,
                "repositories",
                f"{mirror_key}.git",
            )
        )
        if isinstance(managed_root_key, str) and isinstance(mirror_key, str)
        else None
    )
    if mirror_path_key is None or mirror_path_key != expected_mirror_path_key:
        add("REPOSITORY_PROVENANCE_INVALID")

    def assignment_workspace_paths_valid(
        workspace: Mapping[str, Any],
        node_id: Any,
        assignment_id: Any,
    ) -> bool:
        if not all(
            isinstance(value, str) and value
            for value in (managed_root_key, project_id, sprint_id, node_id, assignment_id)
        ):
            return False
        try:
            node_path_segment = managed_node_path_segment(node_id)
            project_path_segment = managed_project_path_segment(project_id)
            sprint_path_segment = managed_sprint_path_segment(sprint_id)
        except ValueError:
            return False
        expected_key = windows_absolute_path_key(
            ntpath.join(
                managed_root_key,
                "projects",
                project_path_segment,
                "sprints",
                sprint_path_segment,
                "nodes",
                node_path_segment,
                assignment_id,
            )
        )
        recorded_key = windows_absolute_path_key(workspace.get("expected_root"))
        actual_key = windows_absolute_path_key(workspace.get("actual_git_toplevel"))
        git_dir = workspace.get("actual_git_dir")
        return (
            expected_key is not None
            and recorded_key == expected_key
            and actual_key == expected_key
            and isinstance(git_dir, str)
            and (
                windows_path_is_strictly_within(
                    git_dir, workspace.get("expected_root")
                )
                or (
                    isinstance(mirror_path, str)
                    and windows_path_is_strictly_within(git_dir, mirror_path)
                )
            )
        )

    active_branch_keys: set[tuple[Any, str]] = set()
    for lease in branch_lease_by_id.values():
        branch = lease.get("branch")
        lease_repository_key = lease.get("repository_key")
        if (
            lease.get("status") == "active"
            and isinstance(branch, str)
            and isinstance(lease_repository_key, str)
        ):
            ownership_key = (lease_repository_key, branch.casefold())
            if ownership_key in active_branch_keys:
                add("ACTIVE_BRANCH_OWNERSHIP_CONFLICT")
            active_branch_keys.add(ownership_key)
        if (
            lease.get("repository_id") != repository_id
            or lease.get("repository_key") != repository_key
            or lease.get("mirror_storage_key") != mirror_key
        ):
            add("BRANCH_LEASE_REPOSITORY_MISMATCH")

    def assignment_source_provenance_valid(
        assignment_id: str,
        assignment: Mapping[str, Any],
        occurrence: Mapping[str, Any] | None,
    ) -> bool:
        source_kind = assignment.get("source_kind")
        source_keys = assignment.get("source_result_keys")
        source_keys = source_keys if isinstance(source_keys, list) else []
        source_receipts = [
            receipt_by_key.get(key) if isinstance(key, str) else None
            for key in source_keys
        ]
        source_commits = [
            receipt.get("result_commit")
            for receipt in source_receipts
            if isinstance(receipt, Mapping)
        ]
        trigger_ids = (
            occurrence.get("trigger_token_ids", [])
            if isinstance(occurrence, Mapping)
            else []
        )
        trigger_keys = [
            token_by_id.get(token_id, {}).get("result_key")
            for token_id in trigger_ids
            if isinstance(token_id, str)
        ]
        if source_kind == "sprint_source":
            return (
                not source_keys
                and isinstance(occurrence, Mapping)
                and occurrence.get("activation_policy") == "entry"
                and assignment.get("source_commit")
                == state.get("workspace_source_commit")
            )
        if source_kind == "accepted_result":
            return (
                bool(source_keys)
                and source_keys == trigger_keys
                and all(key in accepted_result_keys for key in source_keys)
                and len(source_receipts) == len(source_commits)
                and len(set(source_commits)) == 1
                and assignment.get("source_commit") == source_commits[0]
            )
        if source_kind == "rework_result":
            rework = rework_by_assignment.get(assignment_id)
            return (
                len(source_keys) == 1
                and isinstance(rework, Mapping)
                and rework.get("rejected_result_key") == source_keys[0]
                and isinstance(source_receipts[0], Mapping)
                and assignment.get("source_commit")
                == source_receipts[0].get("result_commit")
                and assignment.get("rework_cycle") == rework.get("rework_cycle")
            )
        if source_kind == "integration":
            integration_id = assignment.get("integration_id")
            integration = (
                integration_by_id.get(integration_id)
                if isinstance(integration_id, str)
                else None
            )
            parents = integration.get("parents") if isinstance(integration, Mapping) else None
            expected_parents = []
            if source_keys == trigger_keys and len(source_receipts) == len(source_keys):
                for token_id, key, receipt in zip(
                    trigger_ids,
                    source_keys,
                    source_receipts,
                ):
                    token = token_by_id.get(token_id)
                    if isinstance(token, Mapping) and isinstance(receipt, Mapping):
                        expected_parents.append(
                            {
                                "source_node_id": token.get("source_node_id"),
                                "result_key": key,
                                "commit": receipt.get("result_commit"),
                            }
                        )
            return (
                len(source_keys) >= 2
                and all(key in accepted_result_keys for key in source_keys)
                and isinstance(integration, Mapping)
                and integration.get("status") == "COMMITTED"
                and integration.get("assignment_id") == assignment_id
                and integration.get("target_occurrence_id")
                == assignment.get("occurrence_id")
                and integration.get("target_node_id") == assignment.get("node_id")
                and integration.get("target_graph_revision")
                == assignment.get("graph_revision")
                and integration.get("trigger_token_ids") == trigger_ids
                and parents == expected_parents
                and len(expected_parents) == len(source_keys)
                and integration.get("integration_commit")
                == assignment.get("source_commit")
            )
        return False

    for assignment_id, assignment in assignment_by_id.items():
        assignment_revision = assignment.get("graph_revision")
        node_id = assignment.get("node_id")
        node = (
            nodes_by_revision.get(assignment_revision, {}).get(node_id)
            if isinstance(assignment_revision, int)
            and not isinstance(assignment_revision, bool)
            and isinstance(node_id, str)
            else None
        )
        occurrence_id = assignment.get("occurrence_id")
        occurrence = (
            occurrence_by_id.get(occurrence_id)
            if isinstance(occurrence_id, str)
            else None
        )
        agent = node.get("agent") if isinstance(node, Mapping) else None
        transitions = node.get("transitions") if isinstance(node, Mapping) else None
        expected_outcomes = (
            set(transitions) | {"STOP", "NEED_DECISION"}
            if isinstance(transitions, Mapping)
            else set()
        )
        allowed = assignment.get("allowed_outcomes")
        allowed_set = set(allowed) if isinstance(allowed, list) else set()
        if (
            not expected_outcomes
            or allowed_set != expected_outcomes
            or not isinstance(agent, Mapping)
            or assignment.get("agent_id") != agent.get("id")
            or assignment.get("agent_phone") != agent.get("phone")
            or not isinstance(occurrence, Mapping)
            or occurrence.get("graph_revision") != assignment_revision
        ):
            add("ASSIGNMENT_GRAPH_BINDING_INVALID")
        workspace_id = assignment.get("workspace_id")
        workspace = (
            workspace_by_id.get(workspace_id) if isinstance(workspace_id, str) else None
        )
        if (
            not isinstance(workspace, Mapping)
            or workspace.get("assignment_id") != assignment_id
            or workspace.get("node_id") != node_id
            or workspace.get("project_id") != project_id
            or workspace.get("sprint_id") != sprint_id
            or workspace.get("repository_id") != repository_id
            or workspace.get("repository_remote") != repository_key
            or workspace.get("source_commit") != assignment.get("source_commit")
            or workspace.get("initial_head_commit")
            != assignment.get("initial_head_commit")
            or not assignment_workspace_paths_valid(
                workspace, node_id, assignment_id
            )
        ):
            add("ASSIGNMENT_WORKSPACE_BINDING_INVALID")
        node_workspace = node.get("workspace") if isinstance(node, Mapping) else None
        access = node_workspace.get("access") if isinstance(node_workspace, Mapping) else None
        lease_id = assignment.get("branch_lease_id")
        lease = branch_lease_by_id.get(lease_id) if isinstance(lease_id, str) else None
        if access == "write":
            assignment_status = assignment.get("status")
            expected_lease_status = (
                "released"
                if isinstance(assignment_status, str)
                and assignment_status in {"completed", "failed", "blocked"}
                else "active"
            )
            if (
                not isinstance(lease, Mapping)
                or lease.get("assignment_id") != assignment_id
                or lease.get("mode") != "write"
                or lease.get("status") != expected_lease_status
                or lease.get("source_commit") != assignment.get("source_commit")
                or lease.get("initial_head_commit")
                != assignment.get("initial_head_commit")
                or not isinstance(workspace, Mapping)
                or workspace.get("assigned_branch") != lease.get("branch")
            ):
                add("ASSIGNMENT_BRANCH_LEASE_BINDING_INVALID")
        elif access == "read":
            if (
                lease_id is not None
                or assignment.get("initial_head_commit")
                != assignment.get("source_commit")
                or (
                    isinstance(workspace, Mapping)
                    and workspace.get("assigned_branch") is not None
                )
            ):
                add("READ_ASSIGNMENT_HAS_WRITE_LEASE")
        else:
            add("ASSIGNMENT_GRAPH_BINDING_INVALID")
        if not assignment_source_provenance_valid(
            assignment_id,
            assignment,
            occurrence if isinstance(occurrence, Mapping) else None,
        ):
            add("ASSIGNMENT_SOURCE_PROVENANCE_INVALID")

    for assignment_id in active_id_set.intersection(assignment_by_id):
        assignment = assignment_by_id[assignment_id]
        assignment_revision = assignment.get("graph_revision")
        node_id = assignment.get("node_id")
        node = nodes_by_revision.get(assignment_revision, {}).get(node_id)
        assignment_occurrence_id = assignment.get("occurrence_id")
        occurrence = (
            occurrence_by_id.get(assignment_occurrence_id)
            if isinstance(assignment_occurrence_id, str)
            else None
        )
        allowed = assignment.get("allowed_outcomes")
        allowed_set = set(allowed) if isinstance(allowed, list) else set()
        expected_outcomes = (
            set(node.get("transitions", {})) | {"STOP", "NEED_DECISION"}
            if isinstance(node, Mapping)
            and isinstance(node.get("transitions"), Mapping)
            else set()
        )
        agent = node.get("agent") if isinstance(node, Mapping) else None
        if (
            not isinstance(assignment.get("status"), str)
            or assignment.get("status") not in {"active", "reviews_pending"}
            or not isinstance(occurrence, Mapping)
            or occurrence.get("state") != assignment.get("status")
            or allowed_set != set(valid_outcomes.get(assignment_id, []))
        ):
            add("ACTIVE_ASSIGNMENT_INCONSISTENT")
        if (
            not expected_outcomes
            or not isinstance(allowed, list)
            or allowed_set != expected_outcomes
            or not isinstance(agent, Mapping)
            or assignment.get("agent_id") != agent.get("id")
            or assignment.get("agent_phone") != agent.get("phone")
        ):
            add("ASSIGNMENT_GRAPH_BINDING_INVALID")

        workspace = workspace_by_id.get(assignment.get("workspace_id"))
        if (
            not isinstance(workspace, Mapping)
            or workspace.get("assignment_id") != assignment_id
            or workspace.get("node_id") != node_id
            or workspace.get("project_id") != project_id
            or workspace.get("sprint_id") != sprint_id
            or workspace.get("repository_id") != repository_id
            or workspace.get("repository_remote") != repository_key
            or workspace.get("source_commit") != assignment.get("source_commit")
            or workspace.get("initial_head_commit")
            != assignment.get("initial_head_commit")
            or not assignment_workspace_paths_valid(
                workspace, node_id, assignment_id
            )
            or workspace.get("working_tree_state") != "clean"
        ):
            add("ACTIVE_WORKSPACE_INCONSISTENT")

        node_workspace = node.get("workspace") if isinstance(node, Mapping) else None
        access = node_workspace.get("access") if isinstance(node_workspace, Mapping) else None
        lease_id = assignment.get("branch_lease_id")
        if access == "write":
            lease = branch_lease_by_id.get(lease_id) if isinstance(lease_id, str) else None
            if (
                not isinstance(lease, Mapping)
                or lease.get("assignment_id") != assignment_id
                or lease.get("mode") != "write"
                or lease.get("status") != "active"
                or lease.get("source_commit") != assignment.get("source_commit")
                or lease.get("initial_head_commit")
                != assignment.get("initial_head_commit")
                or not isinstance(workspace, Mapping)
                or workspace.get("assigned_branch") != lease.get("branch")
            ):
                add("ACTIVE_BRANCH_LEASE_INCONSISTENT")
        elif access == "read":
            if (
                lease_id is not None
                or assignment.get("initial_head_commit")
                != assignment.get("source_commit")
                or (
                    isinstance(workspace, Mapping)
                    and workspace.get("assigned_branch") is not None
                )
            ):
                add("READ_ASSIGNMENT_HAS_WRITE_LEASE")
        else:
            add("ACTIVE_NODE_WORKSPACE_INVALID")

    for workspace_id, workspace in workspace_by_id.items():
        owner = assignment_by_id.get(workspace.get("assignment_id"))
        if (
            not isinstance(owner, Mapping)
            or owner.get("workspace_id") != workspace_id
            or owner.get("node_id") != workspace.get("node_id")
        ):
            add("WORKSPACE_OWNER_INVALID")

    for lease_id, lease in branch_lease_by_id.items():
        owner = assignment_by_id.get(lease.get("assignment_id"))
        if (
            not isinstance(owner, Mapping)
            or owner.get("branch_lease_id") != lease_id
        ):
            add("BRANCH_LEASE_OWNER_INVALID")

    for lease_id, lease in port_lease_by_id.items():
        owner = assignment_by_id.get(lease.get("assignment_id"))
        process_id = lease.get("process_id")
        process = (
            process_by_id.get(process_id) if isinstance(process_id, str) else None
        )
        if not isinstance(owner, Mapping) or (
            process_id is not None
            and (
                not isinstance(process, Mapping)
                or process.get("assignment_id") != owner.get("assignment_id")
                or process.get("port_lease_id") != lease_id
            )
        ):
            add("PORT_LEASE_OWNER_INVALID")

    owned_workspace_roots: set[str] = set()
    for workspace in workspace_by_id.values():
        root_key = windows_absolute_path_key(workspace.get("expected_root"))
        if root_key is None:
            add("ASSIGNMENT_WORKSPACE_BINDING_INVALID")
        elif root_key in owned_workspace_roots:
            add("WORKSPACE_ROOT_OWNERSHIP_CONFLICT")
        else:
            owned_workspace_roots.add(root_key)

    completed_join_recovery_by_context: dict[str, Mapping[str, Any]] = {}
    resolved_join_integration_ids: set[str] = set()
    for recovery in recovery_by_id.values():
        join_target = recovery.get("join_target")
        context_id = recovery.get("context_id")
        raw_produced_ids = recovery.get("produced_record_ids")
        produced_ids = raw_produced_ids if isinstance(raw_produced_ids, list) else []
        produced_repair_ids = [
            record_id for record_id in produced_ids if record_id in repair_by_id
        ]
        repair = (
            repair_by_id.get(produced_repair_ids[0])
            if len(produced_repair_ids) == 1
            else None
        )
        context = (
            coordinator_context_by_id.get(context_id)
            if isinstance(context_id, str)
            else None
        )
        response = recovery.get("response")
        if (
            recovery.get("action") == "APPLY_REPAIR"
            and recovery.get("status") == "completed"
            and isinstance(join_target, Mapping)
            and isinstance(context, Mapping)
            and context.get("join_target") == join_target
            and isinstance(repair, Mapping)
            and repair.get("from_revision") == recovery.get("graph_revision")
            and repair.get("to_revision") == repair.get("from_revision") + 1
            and isinstance(response, Mapping)
            and response.get("produced_record_ids") == produced_ids
        ):
            completed_join_recovery_by_context[context_id] = recovery
            integration_id = join_target.get("integration_id")
            if isinstance(integration_id, str):
                resolved_join_integration_ids.add(integration_id)

    integration_signatures: set[tuple[Any, Any, tuple[Any, ...]]] = set()
    referenced_integration_workspace_ids: set[str] = set()
    valid_in_flight_integration_ids: set[str] = set()
    for integration_id, integration in integration_by_id.items():
        trigger_ids = integration.get("trigger_token_ids")
        trigger_ids = trigger_ids if isinstance(trigger_ids, list) else []
        signature = (
            integration.get("target_graph_revision"),
            integration.get("target_node_id"),
            tuple(trigger_ids),
        )
        try:
            expected_integration_id = managed_integration_id(
                sprint_id,
                integration.get("target_graph_revision"),
                integration.get("target_node_id"),
                trigger_ids,
            )
            expected_workspace_artifact_id = managed_integration_workspace_id(
                sprint_id,
                integration.get("target_graph_revision"),
                integration.get("target_node_id"),
                trigger_ids,
            )
        except (TypeError, ValueError):
            expected_integration_id = None
            expected_workspace_artifact_id = None
        if integration_id != expected_integration_id or signature in integration_signatures:
            add("INTEGRATION_IDENTITY_INVALID")
        integration_signatures.add(signature)
        tokens = [
            token_by_id.get(token_id) if isinstance(token_id, str) else None
            for token_id in trigger_ids
        ]
        parents = integration.get("parents")
        expected_parents = []
        for token in tokens:
            receipt = (
                receipt_by_key.get(token.get("result_key"))
                if isinstance(token, Mapping)
                else None
            )
            if isinstance(token, Mapping) and isinstance(receipt, Mapping):
                expected_parents.append(
                    {
                        "source_node_id": token.get("source_node_id"),
                        "result_key": token.get("result_key"),
                        "commit": receipt.get("result_commit"),
                    }
                )
        target_revision = integration.get("target_graph_revision")
        target_node_id = integration.get("target_node_id")
        target_node = (
            nodes_by_revision.get(target_revision, {}).get(target_node_id)
            if isinstance(target_revision, int)
            and not isinstance(target_revision, bool)
            and isinstance(target_node_id, str)
            else None
        )
        target_workspace = (
            target_node.get("workspace") if isinstance(target_node, Mapping) else None
        )
        target_definition = revision_by_number.get(target_revision, {}).get(
            "definition"
        )
        inbound_parent_ids: list[str] = []
        if isinstance(target_definition, Mapping) and isinstance(
            target_definition.get("nodes"), list
        ):
            for candidate in target_definition["nodes"]:
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
                    and candidate["id"] not in inbound_parent_ids
                ):
                    inbound_parent_ids.append(candidate["id"])
            coordinator = target_definition.get("coordinator")
            if (
                isinstance(coordinator, Mapping)
                and coordinator.get("node_id") == target_node_id
            ):
                for candidate in target_definition["nodes"]:
                    if (
                        isinstance(candidate, Mapping)
                        and candidate.get("type", "task") == "task"
                        and isinstance(candidate.get("id"), str)
                        and candidate["id"] not in inbound_parent_ids
                    ):
                        inbound_parent_ids.append(candidate["id"])
        expected_join_order = (
            target_node.get("join_parent_order")
            if isinstance(target_node, Mapping)
            else None
        )
        integration_status = integration.get("status")
        target_occurrence_id = integration.get("target_occurrence_id")
        deterministic_trigger_ids: list[str] = []
        if isinstance(expected_join_order, list):
            for parent_id in expected_join_order:
                candidates: list[tuple[int, str, str]] = []
                for candidate_token_id, candidate_token in token_by_id.items():
                    source_occurrence = occurrence_by_id.get(
                        candidate_token.get("source_occurrence_id")
                    )
                    generation = (
                        source_occurrence.get("generation")
                        if isinstance(source_occurrence, Mapping)
                        else None
                    )
                    result_key = candidate_token.get("result_key")
                    token_is_eligible = candidate_token.get("status") == "available"
                    if integration_status == "COMMITTED":
                        token_is_eligible = token_is_eligible or (
                            candidate_token.get("status") == "consumed"
                            and candidate_token.get("consumed_by_occurrence_id")
                            == target_occurrence_id
                        )
                    elif integration_id in resolved_join_integration_ids:
                        token_is_eligible = token_is_eligible or (
                            candidate_token.get("status") == "consumed"
                        )
                    if (
                        candidate_token.get("source_node_id") == parent_id
                        and candidate_token.get("target_node_id") == target_node_id
                        and token_is_eligible
                        and isinstance(generation, int)
                        and not isinstance(generation, bool)
                        and isinstance(result_key, str)
                    ):
                        candidates.append(
                            (generation, result_key, candidate_token_id)
                        )
                if candidates:
                    deterministic_trigger_ids.append(min(candidates)[2])
        target_occurrence = (
            occurrence_by_id.get(target_occurrence_id)
            if isinstance(target_occurrence_id, str)
            else None
        )
        if integration_status == "COMMITTED":
            token_lifecycle_valid = (
                isinstance(target_occurrence, Mapping)
                and target_occurrence.get("node_id") == target_node_id
                and target_occurrence.get("graph_revision") == target_revision
                and target_occurrence.get("activation_policy") == "all_parents"
                and target_occurrence.get("trigger_token_ids") == trigger_ids
                and all(
                    isinstance(token, Mapping)
                    and token.get("status") == "consumed"
                    and token.get("target_graph_revision") == target_revision
                    and token.get("consumed_by_occurrence_id")
                    == target_occurrence_id
                    for token in tokens
                )
            )
        elif (
            integration_status in {"CONFLICT", "FAILED"}
            and integration_id in resolved_join_integration_ids
        ):
            resolved_tokens_available = all(
                isinstance(token, Mapping)
                and token.get("status") == "available"
                and token.get("target_graph_revision") is None
                and token.get("consumed_by_occurrence_id") is None
                for token in tokens
            )
            resolved_consumers = {
                token.get("consumed_by_occurrence_id")
                for token in tokens
                if isinstance(token, Mapping)
                and token.get("status") == "consumed"
            }
            resolved_tokens_consumed = (
                len(resolved_consumers) == 1
                and None not in resolved_consumers
                and all(
                    isinstance(token, Mapping)
                    and token.get("status") == "consumed"
                    and isinstance(token.get("target_graph_revision"), int)
                    and not isinstance(token.get("target_graph_revision"), bool)
                    for token in tokens
                )
            )
            token_lifecycle_valid = (
                target_occurrence_id is None
                and integration.get("assignment_id") is None
                and integration.get("integration_commit") is None
                and (resolved_tokens_available or resolved_tokens_consumed)
                and all(
                    isinstance(token, Mapping)
                    and token.get("target_node_id") == target_node_id
                    for token in tokens
                )
            )
        else:
            token_lifecycle_valid = (
                target_occurrence_id is None
                and integration.get("assignment_id") is None
                and integration.get("integration_commit") is None
                and all(
                    isinstance(token, Mapping)
                    and token.get("status") == "available"
                    and token.get("target_graph_revision") is None
                    and token.get("consumed_by_occurrence_id") is None
                    for token in tokens
                )
            )
        parent_commits = [
            parent.get("commit")
            for parent in expected_parents
            if isinstance(parent, Mapping)
        ]
        integration_provenance_valid = not (
            len(tokens) < 2
            or len(expected_parents) != len(tokens)
            or parents != expected_parents
            or not isinstance(target_node, Mapping)
            or target_node.get("type", "task") != "task"
            or target_node.get("activation_policy", "all_parents")
            != "all_parents"
            or not isinstance(target_workspace, Mapping)
            or target_workspace.get("join_strategy", "require_same_commit")
            != "merge_no_ff"
            or not isinstance(expected_join_order, list)
            or len(expected_join_order) < 2
            or len(expected_join_order) != len(inbound_parent_ids)
            or len(set(expected_join_order)) != len(expected_join_order)
            or set(expected_join_order) != set(inbound_parent_ids)
            or [
                token.get("source_node_id")
                for token in tokens
                if isinstance(token, Mapping)
            ]
            != expected_join_order
            or trigger_ids != deterministic_trigger_ids
            or not all(isinstance(commit, str) for commit in parent_commits)
            or len(set(parent_commits)) < 2
            or not token_lifecycle_valid
            or any(
                not isinstance(token, Mapping)
                or token.get("target_node_id") != integration.get("target_node_id")
                for token in tokens
            )
        )
        if not integration_provenance_valid:
            add("INTEGRATION_PROVENANCE_INVALID")
        workspace_artifact_id = integration.get("workspace_artifact_id")
        integration_workspace = (
            integration_workspace_by_id.get(workspace_artifact_id)
            if isinstance(workspace_artifact_id, str)
            else None
        )
        if isinstance(workspace_artifact_id, str):
            if workspace_artifact_id in referenced_integration_workspace_ids:
                add("INTEGRATION_WORKSPACE_BINDING_INVALID")
            referenced_integration_workspace_ids.add(workspace_artifact_id)
        base_commit = (
            parents[0].get("commit")
            if isinstance(parents, list)
            and parents
            and isinstance(parents[0], Mapping)
            else None
        )
        artifact_status = (
            integration_workspace.get("artifact_status")
            if isinstance(integration_workspace, Mapping)
            else None
        )
        tree_state = (
            integration_workspace.get("working_tree_state")
            if isinstance(integration_workspace, Mapping)
            else None
        )
        expected_integration_root_key = (
            windows_absolute_path_key(
                ntpath.join(
                    managed_root_key,
                    "integration-workspaces",
                    expected_workspace_artifact_id,
                )
            )
            if isinstance(managed_root_key, str)
            and isinstance(expected_workspace_artifact_id, str)
            else None
        )
        recorded_integration_root_key = (
            windows_absolute_path_key(integration_workspace.get("expected_root"))
            if isinstance(integration_workspace, Mapping)
            else None
        )
        actual_integration_root_key = (
            windows_absolute_path_key(
                integration_workspace.get("actual_git_toplevel")
            )
            if isinstance(integration_workspace, Mapping)
            else None
        )
        integration_git_dir = (
            integration_workspace.get("actual_git_dir")
            if isinstance(integration_workspace, Mapping)
            else None
        )
        integration_paths_valid = (
            expected_integration_root_key is not None
            and recorded_integration_root_key == expected_integration_root_key
            and actual_integration_root_key == expected_integration_root_key
            and isinstance(integration_git_dir, str)
            and (
                windows_path_is_strictly_within(
                    integration_git_dir,
                    integration_workspace.get("expected_root"),
                )
                or (
                    isinstance(mirror_path, str)
                    and windows_path_is_strictly_within(
                        integration_git_dir, mirror_path
                    )
                )
            )
        )
        status_binding_valid = (
            integration_status == "PREPARED"
            and artifact_status == "active"
            and tree_state in {"clean", "merging"}
            and integration.get("normalized_error") is None
            and isinstance(integration_workspace, Mapping)
            and integration_workspace.get("head_commit") == base_commit
        ) or (
            integration_status == "CONFLICT"
            and artifact_status == "preserved"
            and tree_state == "conflicted"
            and isinstance(integration_workspace, Mapping)
            and integration_workspace.get("head_commit") == base_commit
        ) or (
            integration_status == "FAILED"
            and artifact_status == "preserved"
            and tree_state in {"clean", "merging", "conflicted"}
            and isinstance(integration_workspace, Mapping)
            and integration_workspace.get("head_commit") == base_commit
        ) or (
            integration_status == "COMMITTED"
            and artifact_status in {"active", "released"}
            and (
                (artifact_status == "active" and tree_state == "clean")
                or (artifact_status == "released" and tree_state == "unavailable")
            )
            and isinstance(integration_workspace, Mapping)
            and integration_workspace.get("head_commit")
            == integration.get("integration_commit")
        )
        integration_workspace_binding_valid = not (
            workspace_artifact_id != expected_workspace_artifact_id
            or not isinstance(integration_workspace, Mapping)
            or integration_workspace.get("integration_id") != integration_id
            or integration_workspace.get("project_id") != project_id
            or integration_workspace.get("sprint_id") != sprint_id
            or integration_workspace.get("repository_id") != repository_id
            or integration_workspace.get("repository_remote") != repository_key
            or integration_workspace.get("mirror_storage_key") != mirror_key
            or not integration_paths_valid
            or integration_workspace.get("base_commit") != base_commit
            or not status_binding_valid
        )
        if not integration_workspace_binding_valid:
            add("INTEGRATION_WORKSPACE_BINDING_INVALID")
        if (
            integration_provenance_valid
            and integration_workspace_binding_valid
            and integration_status in {"PREPARED", "CONFLICT", "FAILED"}
        ):
            valid_in_flight_integration_ids.add(integration_id)
        if isinstance(integration_workspace, Mapping) and artifact_status in {
            "active",
            "preserved",
        }:
            root_key = windows_absolute_path_key(
                integration_workspace.get("expected_root")
            )
            if root_key is None:
                add("INTEGRATION_WORKSPACE_BINDING_INVALID")
            elif root_key in owned_workspace_roots:
                add("WORKSPACE_ROOT_OWNERSHIP_CONFLICT")
            else:
                owned_workspace_roots.add(root_key)
        if integration.get("status") == "COMMITTED":
            assignment = assignment_by_id.get(integration.get("assignment_id"))
            occurrence = occurrence_by_id.get(integration.get("target_occurrence_id"))
            matching_assignments = [
                candidate
                for candidate in assignment_by_id.values()
                if candidate.get("integration_id") == integration_id
            ]
            if (
                len(matching_assignments) != 1
                or not isinstance(assignment, Mapping)
                or assignment.get("integration_id") != integration.get("integration_id")
                or assignment.get("source_commit")
                != integration.get("integration_commit")
                or assignment.get("source_kind") != "integration"
                or assignment.get("occurrence_id")
                != integration.get("target_occurrence_id")
                or assignment.get("graph_revision")
                != integration.get("target_graph_revision")
                or assignment.get("source_result_keys")
                != [parent.get("result_key") for parent in expected_parents]
                or not isinstance(occurrence, Mapping)
                or occurrence.get("graph_revision")
                != integration.get("target_graph_revision")
                or occurrence.get("node_id") != integration.get("target_node_id")
                or occurrence.get("trigger_token_ids") != trigger_ids
                or not isinstance(occurrence.get("assignment_ids"), list)
                or not occurrence.get("assignment_ids")
                or occurrence.get("assignment_ids")[0]
                != integration.get("assignment_id")
            ):
                add("INTEGRATION_COMMIT_BINDING_INVALID")

    if any(
        artifact.get("integration_id") not in integration_by_id
        or integration_by_id[artifact.get("integration_id")].get(
            "workspace_artifact_id"
        )
        != workspace_artifact_id
        for workspace_artifact_id, artifact in integration_workspace_by_id.items()
    ) or set(integration_workspace_by_id) != referenced_integration_workspace_ids:
        add("INTEGRATION_WORKSPACE_BINDING_INVALID")

    for result_key, receipt in receipt_by_key.items():
        assignment = assignment_by_id.get(receipt.get("assignment_id"))
        try:
            expected_key = managed_result_key(
                receipt.get("assignment_id"),
                receipt.get("outcome"),
                receipt.get("result_commit"),
            )
        except (TypeError, ValueError):
            expected_key = None
        response = receipt.get("response")
        workspace = (
            workspace_by_id.get(assignment.get("workspace_id"))
            if isinstance(assignment, Mapping)
            else None
        )
        if (
            result_key != expected_key
            or not isinstance(assignment, Mapping)
            or receipt.get("from_commit") != assignment.get("initial_head_commit")
            or assignment.get("result_commit") != receipt.get("result_commit")
            or assignment.get("outcome") != receipt.get("outcome")
            or receipt.get("outcome") not in assignment.get("allowed_outcomes", [])
            or not isinstance(workspace, Mapping)
            or receipt.get("git_branch") != workspace.get("assigned_branch")
            or result_key not in journal_by_result
            or not isinstance(response, Mapping)
            or response.get("assignment_id") != receipt.get("assignment_id")
            or response.get("outcome") != receipt.get("outcome")
            or response.get("result_commit") != receipt.get("result_commit")
            or response.get("result_key") != result_key
        ):
            add("RESULT_RECEIPT_BINDING_INVALID")

    for assignment in assignment_by_id.values():
        if assignment.get("status") in {"reviews_pending", "completed"}:
            matching_receipts = [
                receipt
                for receipt in receipt_by_key.values()
                if receipt.get("assignment_id") == assignment.get("assignment_id")
            ]
            if len(matching_receipts) != 1:
                add("RESULT_RECEIPT_BINDING_INVALID")

    review_slots: set[tuple[Any, Any]] = set()
    review_assignments_by_result: dict[Any, list[Mapping[str, Any]]] = {}
    for review_assignment in review_assignment_by_id.values():
        slot = (
            review_assignment.get("result_key"),
            review_assignment.get("reviewer_index"),
        )
        if slot in review_slots:
            add("REVIEW_SLOT_DUPLICATE")
        review_slots.add(slot)
        result_key = review_assignment.get("result_key")
        review_assignments_by_result.setdefault(result_key, []).append(review_assignment)
        receipt = receipt_by_key.get(result_key)
        source_assignment = (
            assignment_by_id.get(receipt.get("assignment_id"))
            if isinstance(receipt, Mapping)
            else None
        )
        source_definition = (
            revision_by_number.get(source_assignment.get("graph_revision"), {}).get(
                "definition"
            )
            if isinstance(source_assignment, Mapping)
            else None
        )
        execution = (
            source_definition.get("execution")
            if isinstance(source_definition, Mapping)
            else None
        )
        reviewers = execution.get("reviewers") if isinstance(execution, Mapping) else None
        reviewer_index = review_assignment.get("reviewer_index")
        expected_reviewer = (
            reviewers[reviewer_index - 1]
            if isinstance(reviewers, list)
            and isinstance(reviewer_index, int)
            and not isinstance(reviewer_index, bool)
            and 1 <= reviewer_index <= len(reviewers)
            and isinstance(reviewers[reviewer_index - 1], Mapping)
            else None
        )
        if (
            not isinstance(receipt, Mapping)
            or review_assignment.get("source_assignment_id")
            != receipt.get("assignment_id")
            or review_assignment.get("result_commit") != receipt.get("result_commit")
            or review_assignment.get("result_outcome") != receipt.get("outcome")
            or not isinstance(expected_reviewer, Mapping)
            or review_assignment.get("reviewer_id") != expected_reviewer.get("id")
            or review_assignment.get("reviewer_phone")
            != expected_reviewer.get("phone")
        ):
            add("REVIEW_ASSIGNMENT_BINDING_INVALID")
        decision = review_by_assignment_id.get(review_assignment.get("assignment_id"))
        if review_assignment.get("status") == "decided":
            response = review_assignment.get("response")
            feedback = decision.get("feedback") if isinstance(decision, Mapping) else None
            try:
                expected_request_fingerprint = review_request_fingerprint(
                    review_assignment.get("assignment_id"),
                    review_assignment.get("decision"),
                    feedback,
                )
            except (TypeError, ValueError):
                expected_request_fingerprint = None
            if (
                not isinstance(decision, Mapping)
                or decision.get("source_assignment_id")
                != review_assignment.get("source_assignment_id")
                or decision.get("result_key") != result_key
                or decision.get("result_commit")
                != review_assignment.get("result_commit")
                or decision.get("result_outcome")
                != review_assignment.get("result_outcome")
                or decision.get("reviewer_id")
                != review_assignment.get("reviewer_id")
                or decision.get("reviewer_index") != reviewer_index
                or decision.get("decision") != review_assignment.get("decision")
                or decision.get("request_fingerprint")
                != expected_request_fingerprint
                or review_assignment.get("request_fingerprint")
                != expected_request_fingerprint
                or decision.get("response") != response
                or decision.get("decided_at")
                != review_assignment.get("decided_at")
                or not isinstance(response, Mapping)
                or response.get("assignment_id")
                != review_assignment.get("assignment_id")
                or response.get("source_assignment_id")
                != review_assignment.get("source_assignment_id")
                or response.get("result_key") != result_key
                or response.get("decision") != review_assignment.get("decision")
            ):
                add("REVIEW_DECISION_BINDING_INVALID")
        elif decision is not None:
            add("REVIEW_DECISION_BINDING_INVALID")

    for review_assignment_id, decision in review_by_assignment_id.items():
        review_assignment = review_assignment_by_id.get(review_assignment_id)
        if (
            not isinstance(review_assignment, Mapping)
            or review_assignment.get("status") != "decided"
            or decision.get("assignment_id") != review_assignment_id
        ):
            add("REVIEW_DECISION_BINDING_INVALID")

    for result_key, journal in journal_by_result.items():
        review_assignments = review_assignments_by_result.get(result_key, [])
        if journal.get("state") in {
            "REVIEWS_PENDING",
            "REVIEWS_ACCEPTED",
            "TRANSITION_COMMITTED",
            "NEXT_ASSIGNMENT_ENQUEUED",
        } and (
            len(review_assignments) != 2
            or {item.get("reviewer_index") for item in review_assignments} != {1, 2}
            or len({item.get("reviewer_id") for item in review_assignments}) != 2
        ):
            add("REVIEW_QUORUM_INVALID")
        decisions = [item.get("decision") for item in review_assignments]
        if (
            journal.get("disposition") == "accepted"
            and decisions.count("APPROVE") != 2
        ):
            add("REVIEW_QUORUM_INVALID")
        if journal.get("disposition") == "reworked" and "REJECT" not in decisions:
            add("REVIEW_QUORUM_INVALID")

    for rework_id, rework in rework_by_id.items():
        result_key = rework.get("rejected_result_key")
        receipt = receipt_by_key.get(result_key)
        journal = journal_by_result.get(result_key)
        source_assignment = (
            assignment_by_id.get(receipt.get("assignment_id"))
            if isinstance(receipt, Mapping)
            else None
        )
        new_assignment = assignment_by_id.get(rework.get("new_assignment_id"))
        occurrence = (
            occurrence_by_id.get(source_assignment.get("occurrence_id"))
            if isinstance(source_assignment, Mapping)
            else None
        )
        rejecting_reviews = [
            decision
            for review_assignment in review_assignments_by_result.get(result_key, [])
            for decision in [
                review_by_assignment_id.get(review_assignment.get("assignment_id"))
            ]
            if isinstance(decision, Mapping)
            and decision.get("decision") == "REJECT"
            and decision.get("reviewer_id") == rework.get("reviewer_id")
            and decision.get("feedback") == rework.get("feedback")
        ]
        assignment_lineage = (
            occurrence.get("assignment_ids")
            if isinstance(occurrence, Mapping)
            and isinstance(occurrence.get("assignment_ids"), list)
            else []
        )
        source_assignment_id = (
            source_assignment.get("assignment_id")
            if isinstance(source_assignment, Mapping)
            else None
        )
        new_assignment_id = rework.get("new_assignment_id")
        try:
            source_index = assignment_lineage.index(source_assignment_id)
        except ValueError:
            source_index = -1
        source_definition = (
            revision_by_number.get(source_assignment.get("graph_revision"), {}).get(
                "definition"
            )
            if isinstance(source_assignment, Mapping)
            else None
        )
        execution = (
            source_definition.get("execution")
            if isinstance(source_definition, Mapping)
            else None
        )
        max_rework_cycles = (
            execution.get("max_rework_cycles")
            if isinstance(execution, Mapping)
            else None
        )
        if (
            not isinstance(receipt, Mapping)
            or not isinstance(journal, Mapping)
            or journal.get("disposition") != "reworked"
            or journal.get("rework_id") != rework_id
            or len(rejecting_reviews) != 1
            or not isinstance(source_assignment, Mapping)
            or not isinstance(new_assignment, Mapping)
            or not isinstance(occurrence, Mapping)
            or source_index < 0
            or source_index + 1 >= len(assignment_lineage)
            or assignment_lineage[source_index + 1] != new_assignment_id
            or new_assignment.get("occurrence_id")
            != source_assignment.get("occurrence_id")
            or new_assignment.get("node_id") != source_assignment.get("node_id")
            or new_assignment.get("graph_revision")
            != source_assignment.get("graph_revision")
            or new_assignment.get("source_kind") != "rework_result"
            or new_assignment.get("source_result_keys") != [result_key]
            or new_assignment.get("source_commit") != receipt.get("result_commit")
            or new_assignment.get("rework_cycle") != rework.get("rework_cycle")
            or rework.get("rework_cycle")
            != source_assignment.get("rework_cycle", -1) + 1
            or not isinstance(max_rework_cycles, int)
            or isinstance(max_rework_cycles, bool)
            or rework.get("rework_cycle") > max_rework_cycles
        ):
            add("REWORK_BINDING_INVALID")

    for journal in journal_by_result.values():
        if (
            journal.get("disposition") == "reworked"
            and journal.get("rework_id") not in rework_by_id
        ):
            add("REWORK_BINDING_INVALID")

    terminal_evidence_context_ids: set[str] = set()
    join_context_ids_by_integration: dict[str, list[str]] = {}
    join_context_ids_by_signature: dict[
        tuple[Any, Any, tuple[Any, ...], Any], list[str]
    ] = {}
    valid_join_context_ids: set[str] = set()
    for context_id, context in coordinator_context_by_id.items():
        context_revision = context.get("graph_revision")
        import_attempt_id = context.get("import_attempt_id")
        failed_assignment_id = context.get("failed_assignment_id")
        join_target = context.get("join_target")
        import_attempt = (
            import_attempt_by_id.get(import_attempt_id)
            if isinstance(import_attempt_id, str)
            else None
        )
        failed_assignment = (
            assignment_by_id.get(failed_assignment_id)
            if isinstance(failed_assignment_id, str)
            else None
        )
        workspace = (
            workspace_by_id.get(failed_assignment.get("workspace_id"))
            if isinstance(failed_assignment, Mapping)
            else None
        )
        join_target_valid = join_target is None
        if isinstance(join_target, Mapping):
            target_revision = join_target.get("target_graph_revision")
            target_node_id = join_target.get("target_node_id")
            trigger_token_ids = join_target.get("trigger_token_ids")
            trigger_token_ids = (
                trigger_token_ids if isinstance(trigger_token_ids, list) else []
            )
            target_node = nodes_by_revision.get(target_revision, {}).get(
                target_node_id
            )
            target_workspace = (
                target_node.get("workspace")
                if isinstance(target_node, Mapping)
                else None
            )
            join_tokens = [token_by_id.get(token_id) for token_id in trigger_token_ids]
            join_source_ids = [
                token.get("source_node_id")
                for token in join_tokens
                if isinstance(token, Mapping)
            ]
            join_commits = [
                receipt_by_key.get(token.get("result_key"), {}).get("result_commit")
                for token in join_tokens
                if isinstance(token, Mapping)
            ]
            integration_id = join_target.get("integration_id")
            join_signature = (
                target_revision,
                target_node_id,
                tuple(trigger_token_ids),
                integration_id,
            )
            join_context_ids_by_signature.setdefault(join_signature, []).append(
                context_id
            )
            if isinstance(integration_id, str):
                join_context_ids_by_integration.setdefault(integration_id, []).append(
                    context_id
                )
            integration = (
                integration_by_id.get(integration_id)
                if isinstance(integration_id, str)
                else None
            )
            integration_workspace = (
                integration_workspace_by_id.get(
                    integration.get("workspace_artifact_id")
                )
                if isinstance(integration, Mapping)
                else None
            )
            expected_join_order = (
                target_node.get("join_parent_order")
                if isinstance(target_node, Mapping)
                else None
            )
            join_strategy = (
                target_workspace.get("join_strategy", "require_same_commit")
                if isinstance(target_workspace, Mapping)
                else None
            )
            deterministic_join_token_ids: list[str] = []
            if isinstance(expected_join_order, list):
                for parent_id in expected_join_order:
                    candidates: list[tuple[int, str, str]] = []
                    for candidate_token_id, candidate_token in token_by_id.items():
                        source_occurrence = occurrence_by_id.get(
                            candidate_token.get("source_occurrence_id")
                        )
                        generation = (
                            source_occurrence.get("generation")
                            if isinstance(source_occurrence, Mapping)
                            else None
                        )
                        result_key = candidate_token.get("result_key")
                        if (
                            candidate_token.get("source_node_id") == parent_id
                            and candidate_token.get("target_node_id")
                            == target_node_id
                            and (
                                candidate_token.get("status") == "available"
                                or (
                                    context_id
                                    in completed_join_recovery_by_context
                                    and candidate_token.get("status") == "consumed"
                                )
                            )
                            and isinstance(generation, int)
                            and not isinstance(generation, bool)
                            and isinstance(result_key, str)
                        ):
                            candidates.append(
                                (generation, result_key, candidate_token_id)
                            )
                    if candidates:
                        deterministic_join_token_ids.append(min(candidates)[2])
            workspace_projection = (
                {
                    field: integration_workspace.get(field)
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
                if isinstance(integration_workspace, Mapping)
                else None
            )
            join_tokens_lifecycle_valid = all(
                isinstance(token, Mapping)
                and token.get("status") == "available"
                and token.get("target_node_id") == target_node_id
                and token.get("target_graph_revision") is None
                and token.get("consumed_by_occurrence_id") is None
                for token in join_tokens
            )
            if context_id in completed_join_recovery_by_context:
                resolved_tokens_available = all(
                    isinstance(token, Mapping)
                    and token.get("status") == "available"
                    and token.get("target_graph_revision") is None
                    and token.get("consumed_by_occurrence_id") is None
                    for token in join_tokens
                )
                resolved_consumers = {
                    token.get("consumed_by_occurrence_id")
                    for token in join_tokens
                    if isinstance(token, Mapping)
                    and token.get("status") == "consumed"
                }
                resolved_tokens_consumed = (
                    len(resolved_consumers) == 1
                    and None not in resolved_consumers
                    and all(
                        isinstance(token, Mapping)
                        and token.get("status") == "consumed"
                        and isinstance(token.get("target_graph_revision"), int)
                        and not isinstance(token.get("target_graph_revision"), bool)
                        for token in join_tokens
                    )
                )
                join_tokens_lifecycle_valid = (
                    resolved_tokens_available or resolved_tokens_consumed
                ) and all(
                    isinstance(token, Mapping)
                    and token.get("target_node_id") == target_node_id
                    for token in join_tokens
                )
            common_join_valid = (
                target_revision == context_revision
                and isinstance(target_node, Mapping)
                and target_node.get("activation_policy", "all_parents")
                == "all_parents"
                and isinstance(expected_join_order, list)
                and len(expected_join_order) >= 2
                and join_source_ids == expected_join_order
                and len(join_tokens) == len(trigger_token_ids)
                and len(join_commits) == len(trigger_token_ids)
                and all(isinstance(commit, str) for commit in join_commits)
                and len(set(join_commits)) >= 2
                and trigger_token_ids == deterministic_join_token_ids
                and join_tokens_lifecycle_valid
                and context.get("failure_scope") == "integration"
                and context.get("assigned_branch") is None
                and context.get("source_commit") is None
                and context.get("result_commit") is None
                and context.get("process_records") == []
                and context.get("port_records") == []
                and context.get("reviewer_feedback") == []
            )
            if integration_id is None:
                join_target_valid = (
                    common_join_valid
                    and join_strategy == "require_same_commit"
                    and context.get("reason_code") == "JOIN_SOURCE_DIVERGED"
                    and context.get("workspace_status") == {}
                )
            else:
                integration_status = (
                    integration.get("status")
                    if isinstance(integration, Mapping)
                    else None
                )
                expected_reason_code = {
                    "CONFLICT": "JOIN_MERGE_CONFLICT",
                    "FAILED": "JOIN_INTEGRATION_FAILED",
                }.get(integration_status)
                join_target_valid = (
                    common_join_valid
                    and join_strategy == "merge_no_ff"
                    and isinstance(integration, Mapping)
                    and integration_status in {"CONFLICT", "FAILED"}
                    and integration.get("target_graph_revision") == target_revision
                    and integration.get("target_node_id") == target_node_id
                    and integration.get("trigger_token_ids") == trigger_token_ids
                    and isinstance(integration_workspace, Mapping)
                    and integration_workspace.get("artifact_status") == "preserved"
                    and context.get("reason_code") == expected_reason_code
                    and context.get("normalized_error")
                    == integration.get("normalized_error")
                    and context.get("workspace_status") == workspace_projection
                )
        target_count = sum(
            target is not None
            for target in (import_attempt_id, failed_assignment_id, join_target)
        )
        context_binding_valid = not (
            context_revision not in revision_by_number
            or target_count != 1
            or not join_target_valid
            or (
                import_attempt_id is not None
                and (
                    not isinstance(import_attempt, Mapping)
                    or import_attempt.get("status") != "failed"
                )
            )
            or (
                failed_assignment_id is not None
                and (
                    not isinstance(failed_assignment, Mapping)
                    or not isinstance(workspace, Mapping)
                    or context.get("assigned_branch")
                    != workspace.get("assigned_branch")
                    or context.get("source_commit")
                    != failed_assignment.get("source_commit")
                    or context.get("result_commit")
                    != failed_assignment.get("result_commit")
                )
            )
        )
        if not context_binding_valid:
            add("COORDINATOR_CONTEXT_BINDING_INVALID")
        elif isinstance(join_target, Mapping):
            valid_join_context_ids.add(context_id)
        if (
            isinstance(import_attempt, Mapping)
            and import_attempt.get("status") == "failed"
        ) or (
            isinstance(failed_assignment, Mapping)
            and failed_assignment.get("status") in {"failed", "blocked"}
        ):
            terminal_evidence_context_ids.add(context_id)

    if any(
        len(join_context_ids_by_integration.get(integration_id, []))
        != (1 if integration.get("status") in {"CONFLICT", "FAILED"} else 0)
        for integration_id, integration in integration_by_id.items()
    ) or any(
        integration_id not in integration_by_id
        for integration_id in join_context_ids_by_integration
    ):
        add("INTEGRATION_COORDINATOR_CONTEXT_INVALID")
    if any(
        len(context_ids) != 1
        for context_ids in join_context_ids_by_signature.values()
    ):
        add("JOIN_COORDINATOR_CONTEXT_INVALID")

    current_join_definition = (
        revision_by_number.get(graph_revision, {}).get("definition")
        if valid_graph_revision
        else None
    )
    current_join_nodes = (
        current_join_definition.get("nodes")
        if isinstance(current_join_definition, Mapping)
        and isinstance(current_join_definition.get("nodes"), list)
        else []
    )
    active_scheduler_witness = False
    for target_node_id, target_node in nodes_by_revision.get(
        graph_revision, {}
    ).items():
        if target_node.get("type", "task") != "task":
            continue
        policy = target_node.get("activation_policy", "all_parents")
        inbound_parent_ids = definition_inbound_parent_ids(
            current_join_definition, target_node_id
        )
        scheduler_candidates: list[tuple[int, str, str, str]] = []
        for token_id, token in token_by_id.items():
            source_occurrence = occurrence_by_id.get(token.get("source_occurrence_id"))
            generation = (
                source_occurrence.get("generation")
                if isinstance(source_occurrence, Mapping)
                else None
            )
            result_key = token.get("result_key")
            journal = (
                journal_by_result.get(result_key)
                if isinstance(result_key, str)
                else None
            )
            source_node_id = token.get("source_node_id")
            if (
                token.get("status") == "available"
                and token.get("target_node_id") == target_node_id
                and token.get("target_graph_revision") is None
                and token.get("consumed_by_occurrence_id") is None
                and source_node_id in inbound_parent_ids
                and isinstance(generation, int)
                and not isinstance(generation, bool)
                and isinstance(result_key, str)
                and isinstance(journal, Mapping)
                and journal.get("state") == "TRANSITION_COMMITTED"
            ):
                scheduler_candidates.append(
                    (generation, result_key, token_id, source_node_id)
                )
        if policy == "any_parent" and scheduler_candidates:
            active_scheduler_witness = True
        elif policy == "all_parents" and len(inbound_parent_ids) == 1 and any(
            candidate[3] == inbound_parent_ids[0]
            for candidate in scheduler_candidates
        ):
            active_scheduler_witness = True

    active_join_witness = False
    for target_node_id, target_node in nodes_by_revision.get(
        graph_revision, {}
    ).items():
        target_workspace = target_node.get("workspace")
        join_strategy = (
            target_workspace.get("join_strategy", "require_same_commit")
            if isinstance(target_workspace, Mapping)
            else None
        )
        if (
            target_node.get("type", "task") != "task"
            or target_node.get("activation_policy", "all_parents")
            != "all_parents"
            or not isinstance(target_workspace, Mapping)
            or join_strategy not in {"require_same_commit", "merge_no_ff"}
        ):
            continue
        inbound_parent_ids: list[str] = []
        for candidate in current_join_nodes:
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
                and candidate["id"] not in inbound_parent_ids
            ):
                inbound_parent_ids.append(candidate["id"])
        expected_order = target_node.get("join_parent_order")
        if (
            not isinstance(expected_order, list)
            or len(expected_order) < 2
            or len(expected_order) != len(inbound_parent_ids)
            or set(expected_order) != set(inbound_parent_ids)
        ):
            continue
        selected_token_ids: list[str] = []
        selected_commits: list[str] = []
        for parent_id in expected_order:
            candidates: list[tuple[int, str, str, str]] = []
            for candidate_token_id, candidate_token in token_by_id.items():
                source_occurrence = occurrence_by_id.get(
                    candidate_token.get("source_occurrence_id")
                )
                generation = (
                    source_occurrence.get("generation")
                    if isinstance(source_occurrence, Mapping)
                    else None
                )
                result_key = candidate_token.get("result_key")
                receipt = (
                    receipt_by_key.get(result_key)
                    if isinstance(result_key, str)
                    else None
                )
                commit = (
                    receipt.get("result_commit")
                    if isinstance(receipt, Mapping)
                    else None
                )
                if (
                    candidate_token.get("source_node_id") == parent_id
                    and candidate_token.get("target_node_id") == target_node_id
                    and candidate_token.get("status") == "available"
                    and candidate_token.get("target_graph_revision") is None
                    and candidate_token.get("consumed_by_occurrence_id") is None
                    and isinstance(generation, int)
                    and not isinstance(generation, bool)
                    and isinstance(result_key, str)
                    and isinstance(commit, str)
                ):
                    candidates.append(
                        (generation, result_key, candidate_token_id, commit)
                    )
            if candidates:
                selected = min(candidates)
                selected_token_ids.append(selected[2])
                selected_commits.append(selected[3])
        if len(selected_token_ids) != len(expected_order):
            continue
        if len(set(selected_commits)) < 2:
            add("JOIN_READY_NOT_COMMITTED")
            continue
        if join_strategy == "require_same_commit":
            signature = (
                graph_revision,
                target_node_id,
                tuple(selected_token_ids),
                None,
            )
            context_ids = join_context_ids_by_signature.get(signature, [])
            valid_context_ids = [
                context_id
                for context_id in context_ids
                if context_id in valid_join_context_ids
            ]
            if len(context_ids) != 1 or len(valid_context_ids) != 1:
                add("JOIN_COORDINATOR_CONTEXT_INVALID")
            else:
                active_join_witness = True
            continue

        matching_integration_ids = [
            integration_id
            for integration_id, integration in integration_by_id.items()
            if integration.get("target_graph_revision") == graph_revision
            and integration.get("target_node_id") == target_node_id
            and integration.get("trigger_token_ids") == selected_token_ids
        ]
        valid_matching_ids = [
            integration_id
            for integration_id in matching_integration_ids
            if integration_id in valid_in_flight_integration_ids
        ]
        if len(matching_integration_ids) != 1 or len(valid_matching_ids) != 1:
            add("JOIN_INTEGRATION_COVERAGE_INVALID")
            continue
        integration_id = valid_matching_ids[0]
        integration_status = integration_by_id[integration_id].get("status")
        if integration_status in {"CONFLICT", "FAILED"}:
            signature = (
                graph_revision,
                target_node_id,
                tuple(selected_token_ids),
                integration_id,
            )
            context_ids = join_context_ids_by_signature.get(signature, [])
            valid_context_ids = [
                context_id
                for context_id in context_ids
                if context_id in valid_join_context_ids
            ]
            if len(context_ids) != 1 or len(valid_context_ids) != 1:
                add("INTEGRATION_COORDINATOR_CONTEXT_INVALID")
                continue
        active_join_witness = True

    if (
        status == "active"
        and not active_id_set
        and not active_join_witness
        and not active_scheduler_witness
    ):
        add("ACTIVE_ASSIGNMENTS_INVALID")

    produced_record_ids: set[str] = set()
    for record_map in (
        import_attempt_by_id,
        assignment_by_id,
        review_assignment_by_id,
        receipt_by_key,
        rework_by_id,
        integration_by_id,
        integration_workspace_by_id,
        branch_lease_by_id,
        workspace_by_id,
        port_lease_by_id,
        process_by_id,
        outbox_by_id,
        repair_by_id,
        coordinator_context_by_id,
        blocker_by_fingerprint,
    ):
        produced_record_ids.update(record_map)
    produced_record_ids.update(
        journal.get("journal_id")
        for journal in journal_by_result.values()
        if isinstance(journal.get("journal_id"), str)
    )
    produced_record_ids.update(occurrence_by_id)
    produced_record_ids.update(token_by_id)

    def join_repair_shape(
        repair: Mapping[str, Any],
        original_join_target: Mapping[str, Any],
    ) -> tuple[int, str, list[str], Mapping[str, Any], list[str]] | None:
        """Return the frozen join cohort in the next revision when compatible."""

        from_revision = repair.get("from_revision")
        to_revision = repair.get("to_revision")
        target_node_id = original_join_target.get("target_node_id")
        trigger_token_ids = original_join_target.get("trigger_token_ids")
        old_node = (
            nodes_by_revision.get(from_revision, {}).get(target_node_id)
            if isinstance(from_revision, int)
            and not isinstance(from_revision, bool)
            and isinstance(target_node_id, str)
            else None
        )
        new_node = (
            nodes_by_revision.get(to_revision, {}).get(target_node_id)
            if isinstance(to_revision, int)
            and not isinstance(to_revision, bool)
            and isinstance(target_node_id, str)
            else None
        )
        old_definition = revision_by_number.get(from_revision, {}).get("definition")
        new_definition = revision_by_number.get(to_revision, {}).get("definition")
        old_order = (
            old_node.get("join_parent_order")
            if isinstance(old_node, Mapping)
            else None
        )
        new_order = (
            new_node.get("join_parent_order")
            if isinstance(new_node, Mapping)
            else None
        )
        old_inbound = definition_inbound_parent_ids(old_definition, target_node_id)
        new_inbound = definition_inbound_parent_ids(new_definition, target_node_id)
        if (
            original_join_target.get("target_graph_revision") != from_revision
            or not isinstance(to_revision, int)
            or isinstance(to_revision, bool)
            or not isinstance(target_node_id, str)
            or not isinstance(trigger_token_ids, list)
            or not all(isinstance(token_id, str) for token_id in trigger_token_ids)
            or not isinstance(old_node, Mapping)
            or not isinstance(new_node, Mapping)
            or old_node.get("activation_policy", "all_parents") != "all_parents"
            or new_node.get("activation_policy", "all_parents") != "all_parents"
            or not isinstance(old_order, list)
            or new_order != old_order
            or new_inbound != old_inbound
        ):
            return None
        return to_revision, target_node_id, trigger_token_ids, new_node, new_order

    def join_repair_effect_ids(
        repair_id: str,
        repair: Mapping[str, Any],
        original_join_target: Mapping[str, Any],
    ) -> set[str] | None:
        """Validate the immutable first durable recheck effects of a join repair."""

        shape = join_repair_shape(repair, original_join_target)
        if shape is None:
            return None
        to_revision, target_node_id, trigger_token_ids, new_node, new_order = shape
        tokens = [token_by_id.get(token_id) for token_id in trigger_token_ids]
        commits = [
            receipt_by_key.get(token.get("result_key"), {}).get("result_commit")
            for token in tokens
            if isinstance(token, Mapping)
        ]
        if (
            len(tokens) != len(trigger_token_ids)
            or len(tokens) != len(new_order)
            or [
                token.get("source_node_id")
                for token in tokens
                if isinstance(token, Mapping)
            ]
            != new_order
            or not all(isinstance(commit, str) for commit in commits)
        ):
            return None
        new_workspace = new_node.get("workspace")
        join_strategy = (
            new_workspace.get("join_strategy", "require_same_commit")
            if isinstance(new_workspace, Mapping)
            else None
        )
        expected_integration_id = managed_integration_id(
            sprint_id, to_revision, target_node_id, trigger_token_ids
        )
        integration = integration_by_id.get(expected_integration_id)
        if isinstance(integration, Mapping):
            workspace_artifact_id = integration.get("workspace_artifact_id")
            if (
                join_strategy != "merge_no_ff"
                or len(set(commits)) < 2
                or not isinstance(workspace_artifact_id, str)
                or workspace_artifact_id not in integration_workspace_by_id
                or integration.get("target_graph_revision") != to_revision
                or integration.get("target_node_id") != target_node_id
                or integration.get("trigger_token_ids") != trigger_token_ids
            ):
                return None
            return {repair_id, expected_integration_id, workspace_artifact_id}

        matching_context_ids = [
            context_id
            for context_id, context in coordinator_context_by_id.items()
            if context.get("join_target")
            == {
                "target_graph_revision": to_revision,
                "target_node_id": target_node_id,
                "trigger_token_ids": trigger_token_ids,
                "integration_id": None,
            }
            and context.get("reason_code") == "JOIN_SOURCE_DIVERGED"
        ]
        if matching_context_ids:
            if (
                join_strategy != "require_same_commit"
                or len(set(commits)) < 2
                or len(matching_context_ids) != 1
            ):
                return None
            context_id = matching_context_ids[0]
            event_ids = [
                event_id
                for event_id, event in outbox_by_id.items()
                if event.get("event_type") == "COORDINATOR_ENQUEUE"
                and isinstance(event.get("payload"), Mapping)
                and event["payload"].get("context_id") == context_id
            ]
            if len(event_ids) != 1:
                return None
            return {repair_id, context_id, event_ids[0]}

        matching_occurrences = [
            (occurrence_id, occurrence)
            for occurrence_id, occurrence in occurrence_by_id.items()
            if occurrence.get("graph_revision") == to_revision
            and occurrence.get("node_id") == target_node_id
            and occurrence.get("trigger_token_ids") == trigger_token_ids
        ]
        if len(matching_occurrences) != 1 or len(set(commits)) != 1:
            return None
        occurrence_id, occurrence = matching_occurrences[0]
        assignment_ids = occurrence.get("assignment_ids")
        assignment_id = (
            assignment_ids[0]
            if isinstance(assignment_ids, list) and assignment_ids
            else None
        )
        event_ids = [
            event_id
            for event_id, event in outbox_by_id.items()
            if event.get("event_type") == "ASSIGNMENT_ENQUEUE"
            and isinstance(event.get("payload"), Mapping)
            and event["payload"].get("assignment_id") == assignment_id
        ]
        if not isinstance(assignment_id, str) or len(event_ids) != 1:
            return None
        return {repair_id, occurrence_id, assignment_id, event_ids[0]}

    recovery_keys: set[tuple[str, str]] = set()
    for recovery in recovery_by_id.values():
        coordinator_id = recovery.get("coordinator_id")
        idempotency_key = recovery.get("idempotency_key")
        if isinstance(coordinator_id, str) and isinstance(idempotency_key, str):
            recovery_key = (coordinator_id, idempotency_key)
            if recovery_key in recovery_keys:
                add("RECOVERY_IDEMPOTENCY_KEY_DUPLICATE")
            recovery_keys.add(recovery_key)
        else:
            add("RECOVERY_BINDING_INVALID")
        context_id = recovery.get("context_id")
        context = (
            coordinator_context_by_id.get(context_id)
            if isinstance(context_id, str)
            else None
        )
        recovery_revision = recovery.get("graph_revision")
        recovery_definition = revision_by_number.get(recovery_revision, {}).get(
            "definition"
        )
        coordinator = (
            recovery_definition.get("coordinator")
            if isinstance(recovery_definition, Mapping)
            else None
        )
        coordinator_node = (
            nodes_by_revision.get(recovery_revision, {}).get(
                coordinator.get("node_id")
            )
            if isinstance(recovery_revision, int)
            and not isinstance(recovery_revision, bool)
            and isinstance(coordinator, Mapping)
            and isinstance(coordinator.get("node_id"), str)
            else None
        )
        coordinator_agent = (
            coordinator_node.get("agent")
            if isinstance(coordinator_node, Mapping)
            else None
        )
        if (
            not isinstance(context, Mapping)
            or context.get("graph_revision") != recovery.get("graph_revision")
            or not isinstance(coordinator_agent, Mapping)
            or coordinator_id != coordinator_agent.get("id")
        ):
            add("RECOVERY_BINDING_INVALID")
        action = recovery.get("action")
        parameters = recovery.get("parameters")
        parameters = parameters if isinstance(parameters, Mapping) else {}
        import_attempt_id = recovery.get("import_attempt_id")
        assignment_id = recovery.get("assignment_id")
        recovery_join_target = recovery.get("join_target")
        try:
            expected_fingerprint = recovery_request_fingerprint(
                context_id,
                recovery_revision,
                action,
                import_attempt_id,
                assignment_id,
                parameters,
                recovery_join_target,
            )
        except (TypeError, ValueError):
            expected_fingerprint = None
        raw_produced_ids = recovery.get("produced_record_ids")
        raw_produced_ids = (
            raw_produced_ids if isinstance(raw_produced_ids, list) else []
        )
        recovery_status = recovery.get("status")
        recovery_response = recovery.get("response")
        recovery_target_count = sum(
            target is not None
            for target in (
                import_attempt_id,
                assignment_id,
                recovery_join_target,
            )
        )
        if (
            recovery.get("request_fingerprint") != expected_fingerprint
            or recovery_target_count != 1
            or (
                isinstance(context, Mapping)
                and import_attempt_id is not None
                and context.get("import_attempt_id") != import_attempt_id
            )
            or (
                isinstance(context, Mapping)
                and assignment_id is not None
                and context.get("failed_assignment_id") != assignment_id
            )
            or (
                isinstance(context, Mapping)
                and recovery_join_target is not None
                and context.get("join_target") != recovery_join_target
            )
            or (
                recovery_join_target is not None
                and action != "APPLY_REPAIR"
            )
            or any(
                not isinstance(record_id, str)
                or record_id not in produced_record_ids
                for record_id in raw_produced_ids
            )
            or (
                recovery_status in {"pending", "failed"}
                and bool(raw_produced_ids)
            )
            or (
                recovery_status == "completed"
                and (
                    not isinstance(recovery_response, Mapping)
                    or recovery_response.get("recovery_id")
                    != recovery.get("recovery_id")
                    or recovery_response.get("action") != action
                    or recovery_response.get("status") != "RECOVERY_COMPLETED"
                    or recovery_response.get("produced_record_ids")
                    != raw_produced_ids
                )
            )
        ):
            add("RECOVERY_BINDING_INVALID")
        if action == "RETRY_IMPORT":
            attempt = (
                import_attempt_by_id.get(import_attempt_id)
                if isinstance(import_attempt_id, str)
                else None
            )
            if (
                assignment_id is not None
                or not isinstance(attempt, Mapping)
                or attempt.get("status") != "failed"
                or parameters.get("failed_attempt_id") != import_attempt_id
            ):
                add("RECOVERY_BINDING_INVALID")
            if recovery_status == "completed":
                produced_attempt_pairs = [
                    (record_id, import_attempt_by_id[record_id])
                    for record_id in raw_produced_ids
                    if record_id in import_attempt_by_id
                    and record_id != import_attempt_id
                ]
                if (
                    len(produced_attempt_pairs) != 1
                    or set(raw_produced_ids)
                    != {produced_attempt_pairs[0][0]}
                    or produced_attempt_pairs[0][1].get("identity")
                    != attempt.get("identity")
                    or produced_attempt_pairs[0][1].get("request_fingerprint")
                    != attempt.get("request_fingerprint")
                    or produced_attempt_pairs[0][1].get("idempotency_key")
                    != parameters.get("recovery_idempotency_key")
                ):
                    add("RECOVERY_EFFECT_BINDING_INVALID")
        elif action == "RETRY_HANDOFF":
            result_key = parameters.get("result_key")
            receipt = receipt_by_key.get(result_key) if isinstance(result_key, str) else None
            if (
                import_attempt_id is not None
                or not isinstance(receipt, Mapping)
                or receipt.get("assignment_id") != assignment_id
            ):
                add("RECOVERY_BINDING_INVALID")
            if recovery_status == "completed":
                journal = journal_by_result.get(result_key)
                relevant_event_ids = {
                    event_id
                    for event_id, event in outbox_by_id.items()
                    if (
                        isinstance(event.get("payload"), Mapping)
                        and event["payload"].get("result_key") == result_key
                    )
                    or (
                        isinstance(journal, Mapping)
                        and event_id in journal.get("outbox_event_ids", [])
                    )
                }
                expected_handoff_ids = {result_key} | relevant_event_ids
                if (
                    not relevant_event_ids
                    or set(raw_produced_ids) != expected_handoff_ids
                ):
                    add("RECOVERY_EFFECT_BINDING_INVALID")
        elif action == "CONTINUE_NODE":
            assignment = (
                assignment_by_id.get(assignment_id)
                if isinstance(assignment_id, str)
                else None
            )
            if (
                import_attempt_id is not None
                or not isinstance(assignment, Mapping)
                or parameters.get("node_id") != assignment.get("node_id")
                or parameters.get("source_commit")
                not in {assignment.get("source_commit"), assignment.get("result_commit")}
            ):
                add("RECOVERY_BINDING_INVALID")
            if recovery_status == "completed":
                produced_assignments = [
                    assignment_by_id[record_id]
                    for record_id in raw_produced_ids
                    if record_id in assignment_by_id and record_id != assignment_id
                ]
                produced_assignment_ids = {
                    produced.get("assignment_id") for produced in produced_assignments
                }
                produced_outbox_assignment_ids = {
                    event.get("payload", {}).get("assignment_id")
                    for event_id, event in outbox_by_id.items()
                    if event_id in raw_produced_ids
                    and event.get("event_type") == "ASSIGNMENT_ENQUEUE"
                    and isinstance(event.get("payload"), Mapping)
                }
                produced_outbox_event_ids = {
                    event_id
                    for event_id, event in outbox_by_id.items()
                    if event_id in raw_produced_ids
                    and event.get("event_type") == "ASSIGNMENT_ENQUEUE"
                    and isinstance(event.get("payload"), Mapping)
                }
                if (
                    len(produced_assignments) != 1
                    or produced_assignments[0].get("node_id")
                    != parameters.get("node_id")
                    or produced_assignments[0].get("source_commit")
                    != parameters.get("source_commit")
                    or produced_assignment_ids != produced_outbox_assignment_ids
                    or len(produced_outbox_event_ids) != 1
                    or set(raw_produced_ids)
                    != produced_assignment_ids | produced_outbox_event_ids
                ):
                    add("RECOVERY_EFFECT_BINDING_INVALID")
        elif action == "ROUTE_REWORK":
            result_key = parameters.get("rejected_result_key")
            receipt = receipt_by_key.get(result_key) if isinstance(result_key, str) else None
            if (
                import_attempt_id is not None
                or not isinstance(receipt, Mapping)
                or receipt.get("assignment_id") != assignment_id
            ):
                add("RECOVERY_BINDING_INVALID")
            if recovery_status == "completed":
                produced_reworks = [
                    rework_by_id[record_id]
                    for record_id in raw_produced_ids
                    if record_id in rework_by_id
                ]
                matching_reworks = [
                    rework
                    for rework in produced_reworks
                    if rework.get("rejected_result_key") == result_key
                    and rework.get("feedback") == parameters.get("feedback")
                ]
                produced_assignment_ids = {
                    record_id
                    for record_id in raw_produced_ids
                    if record_id in assignment_by_id
                }
                produced_outbox_assignment_ids = {
                    event.get("payload", {}).get("assignment_id")
                    for event_id, event in outbox_by_id.items()
                    if event_id in raw_produced_ids
                    and event.get("event_type") == "ASSIGNMENT_ENQUEUE"
                    and isinstance(event.get("payload"), Mapping)
                }
                produced_rework_ids = {
                    record_id for record_id in raw_produced_ids if record_id in rework_by_id
                }
                produced_outbox_event_ids = {
                    event_id
                    for event_id, event in outbox_by_id.items()
                    if event_id in raw_produced_ids
                    and event.get("event_type") == "ASSIGNMENT_ENQUEUE"
                    and isinstance(event.get("payload"), Mapping)
                }
                if (
                    len(matching_reworks) != 1
                    or matching_reworks[0].get("new_assignment_id")
                    not in produced_assignment_ids
                    or matching_reworks[0].get("new_assignment_id")
                    not in produced_outbox_assignment_ids
                    or len(produced_assignment_ids) != 1
                    or len(produced_rework_ids) != 1
                    or len(produced_outbox_event_ids) != 1
                    or set(raw_produced_ids)
                    != produced_rework_ids
                    | produced_assignment_ids
                    | produced_outbox_event_ids
                ):
                    add("RECOVERY_EFFECT_BINDING_INVALID")
        elif action == "APPLY_REPAIR":
            request = parameters.get("request")
            if not isinstance(request, Mapping):
                add("RECOVERY_BINDING_INVALID")
            published_repair_pairs = [
                (record_id, repair)
                for record_id, repair in repair_by_id.items()
                if isinstance(request, Mapping)
                and repair.get("from_revision") == request.get("expected_revision")
                and repair.get("repair_source_commit")
                == request.get("repair_source_commit")
                and repair.get("idempotency_key") == request.get("idempotency_key")
                and repair.get("patch") == request.get("patch")
            ]
            if isinstance(recovery_join_target, Mapping) and published_repair_pairs:
                if (
                    len(published_repair_pairs) != 1
                    or join_repair_shape(
                        published_repair_pairs[0][1], recovery_join_target
                    )
                    is None
                ):
                    add("REPAIR_JOIN_RECHECK_INVALID")
            if recovery_status == "failed" and published_repair_pairs:
                add("RECOVERY_EFFECT_BINDING_INVALID")
            if recovery_status == "completed":
                matching_repair_pairs = [
                    (record_id, repair)
                    for record_id, repair in published_repair_pairs
                    if record_id in raw_produced_ids
                ]
                expected_repair_effect_ids: set[str] | None = None
                if len(matching_repair_pairs) == 1:
                    matched_repair_id, matched_repair = matching_repair_pairs[0]
                    if isinstance(recovery_join_target, Mapping):
                        expected_repair_effect_ids = join_repair_effect_ids(
                            matched_repair_id,
                            matched_repair,
                            recovery_join_target,
                        )
                    else:
                        expected_repair_effect_ids = {matched_repair_id}
                if (
                    expected_repair_effect_ids is None
                    or set(raw_produced_ids) != expected_repair_effect_ids
                ):
                    add("RECOVERY_EFFECT_BINDING_INVALID")
        elif action == "BLOCK_EXTERNAL":
            block_external_valid = (
                (
                    import_attempt_id is not None
                    or assignment_id is not None
                )
                and isinstance(context, Mapping)
                and parameters.get("reason_code") == context.get("reason_code")
            )
            if not block_external_valid:
                add("RECOVERY_BINDING_INVALID")
            if recovery_status == "completed" and assignment_id is not None:
                matching_observations = [
                    (record_id, observation)
                    for record_id, observation in blocker_by_fingerprint.items()
                    if record_id in raw_produced_ids
                    and observation.get("assignment_id") == assignment_id
                ]
                if (
                    len(matching_observations) != 1
                    or set(raw_produced_ids) != {matching_observations[0][0]}
                ):
                    add("RECOVERY_EFFECT_BINDING_INVALID")
                    block_external_valid = False
            elif recovery_status == "completed" and raw_produced_ids:
                add("RECOVERY_EFFECT_BINDING_INVALID")
                block_external_valid = False
            if recovery_status == "completed" and block_external_valid:
                terminal_evidence_context_ids.add(context_id)
        else:
            add("RECOVERY_BINDING_INVALID")

    configured_child_start = (
        runtime_config.get("child_port_start")
        if isinstance(runtime_config, Mapping)
        and isinstance(runtime_config.get("child_port_start"), int)
        and not isinstance(runtime_config.get("child_port_start"), bool)
        else None
    )
    configured_child_end = (
        runtime_config.get("child_port_end")
        if isinstance(runtime_config, Mapping)
        and isinstance(runtime_config.get("child_port_end"), int)
        and not isinstance(runtime_config.get("child_port_end"), bool)
        else None
    )
    live_port_keys: set[tuple[Any, Any, Any]] = set()
    for lease in port_lease_by_id.values():
        if (
            not isinstance(runtime_config, Mapping)
            or lease.get("instance_id") != runtime_config.get("instance_id")
            or lease.get("host") != "127.0.0.1"
            or not isinstance(lease.get("port"), int)
            or isinstance(lease.get("port"), bool)
            or configured_child_start is None
            or configured_child_end is None
            or lease.get("port") < configured_child_start
            or lease.get("port") > configured_child_end
        ):
            add("PORT_RUNTIME_CONFIG_MISMATCH")
        if lease.get("status") in {"reserved", "bound"}:
            key = (
                lease.get("network_namespace_id"),
                lease.get("host"),
                lease.get("port"),
            )
            if key in live_port_keys:
                add("LIVE_PORT_OWNERSHIP_CONFLICT")
            live_port_keys.add(key)
        process_id = lease.get("process_id")
        if lease.get("status") == "bound" and (
            process_id not in process_by_id
            or process_by_id[process_id].get("port_lease_id") != lease.get("lease_id")
            or process_by_id[process_id].get("assignment_id")
            != lease.get("assignment_id")
        ):
            add("PORT_PROCESS_BINDING_INVALID")

    live_process_states = {"PREPARED", "STARTING", "HEALTHY", "STOPPING"}
    processes_by_assignment: dict[Any, list[Mapping[str, Any]]] = {}
    processes_by_port_lease: dict[Any, list[Mapping[str, Any]]] = {}
    children_by_process: dict[Any, int] = {}
    for process in process_by_id.values():
        processes_by_assignment.setdefault(process.get("assignment_id"), []).append(
            process
        )
        processes_by_port_lease.setdefault(process.get("port_lease_id"), []).append(
            process
        )
        parent_process_id = process.get("restart_of_process_id")
        if parent_process_id is not None:
            children_by_process[parent_process_id] = (
                children_by_process.get(parent_process_id, 0) + 1
            )
    for assignment_processes in processes_by_assignment.values():
        restart_attempts = [
            process.get("restart_attempt") for process in assignment_processes
        ]
        roots = [
            process
            for process in assignment_processes
            if process.get("restart_of_process_id") is None
        ]
        live_processes = [
            process
            for process in assignment_processes
            if process.get("state") in live_process_states
        ]
        if (
            len(roots) != 1
            or len(live_processes) > 1
            or not all(
                isinstance(attempt, int) and not isinstance(attempt, bool)
                for attempt in restart_attempts
            )
            or sorted(restart_attempts) != list(range(len(restart_attempts)))
        ):
            add("PROCESS_CHAIN_INVALID")
    if any(child_count > 1 for child_count in children_by_process.values()):
        add("PROCESS_CHAIN_INVALID")
    if any(
        len(lease_processes) != 1
        for lease_processes in processes_by_port_lease.values()
    ):
        add("PROCESS_PORT_LEASE_REUSE_INVALID")

    for process in process_by_id.values():
        process_id = process.get("process_id")
        process_assignment = assignment_by_id.get(process.get("assignment_id"))
        process_workspace = workspace_by_id.get(process.get("workspace_id"))
        process_port = port_lease_by_id.get(process.get("port_lease_id"))
        process_node = (
            nodes_by_revision.get(process_assignment.get("graph_revision"), {}).get(
                process_assignment.get("node_id")
            )
            if isinstance(process_assignment, Mapping)
            else None
        )
        node_workspace = (
            process_node.get("workspace")
            if isinstance(process_node, Mapping)
            else None
        )
        process_launch = (
            node_workspace.get("process")
            if isinstance(node_workspace, Mapping)
            else None
        )
        process_definition = (
            revision_by_number.get(process_assignment.get("graph_revision"), {}).get(
                "definition"
            )
            if isinstance(process_assignment, Mapping)
            else None
        )
        process_policy = (
            process_definition.get("process_policy", {})
            if isinstance(process_definition, Mapping)
            else {}
        )
        process_policy = process_policy if isinstance(process_policy, Mapping) else {}
        process_state = process.get("state")
        expected_port_status = (
            "reserved"
            if process_state == "PREPARED"
            else "bound"
            if process_state in {"STARTING", "HEALTHY", "STOPPING"}
            else "released"
        )
        expected_port_process_id = (
            None if process_state == "PREPARED" else process.get("process_id")
        )
        if (
            not isinstance(process_assignment, Mapping)
            or not isinstance(process_workspace, Mapping)
            or process_assignment.get("workspace_id")
            != process.get("workspace_id")
            or process_workspace.get("assignment_id")
            != process.get("assignment_id")
            or not isinstance(process_port, Mapping)
            or process_port.get("assignment_id") != process.get("assignment_id")
            or process_port.get("status") != expected_port_status
            or process_port.get("process_id") != expected_port_process_id
        ):
            add("PROCESS_OWNER_INVALID")

        default_limits = {
            "wall_time_seconds": 3600,
            "memory_bytes": 2147483648,
            "cpu_percent": 100,
            "process_count": 16,
        }
        effective_limits = dict(default_limits)
        policy_limits = process_policy.get("resource_limits")
        launch_limits = (
            process_launch.get("resource_limits")
            if isinstance(process_launch, Mapping)
            else None
        )
        if isinstance(launch_limits, Mapping):
            effective_limits.update(launch_limits)
        elif isinstance(policy_limits, Mapping):
            effective_limits.update(policy_limits)
        effective_restart_policy = (
            process_launch.get("restart_policy")
            if isinstance(process_launch, Mapping)
            and "restart_policy" in process_launch
            else process_policy.get("restart_policy", "never")
        )
        effective_max_restarts = (
            process_launch.get("max_restart_attempts")
            if isinstance(process_launch, Mapping)
            and "max_restart_attempts" in process_launch
            else process_policy.get("max_restart_attempts")
        )
        if effective_max_restarts is None:
            effective_max_restarts = (
                3 if effective_restart_policy == "on_failure" else 0
            )
        effective_backoff = (
            process_launch.get("restart_backoff_seconds")
            if isinstance(process_launch, Mapping)
            and "restart_backoff_seconds" in process_launch
            else process_policy.get("restart_backoff_seconds", 5)
        )
        effective_health_path = (
            process_launch.get("health_path")
            if isinstance(process_launch, Mapping)
            and "health_path" in process_launch
            else process_policy.get("health_path", "/health")
        )
        launch_cwd = (
            process_launch.get("cwd") if isinstance(process_launch, Mapping) else None
        )
        workspace_root = (
            process_workspace.get("actual_git_toplevel")
            if isinstance(process_workspace, Mapping)
            else None
        )
        process_runtime_root = (
            runtime_config.get("process_runtime_root")
            if isinstance(runtime_config, Mapping)
            else None
        )
        log_root = (
            runtime_config.get("log_root")
            if isinstance(runtime_config, Mapping)
            else None
        )
        expected_process_runtime_root = (
            windows_absolute_path_key(ntpath.join(process_runtime_root, process_id))
            if isinstance(process_runtime_root, str) and isinstance(process_id, str)
            else None
        )
        expected_stdout_log = (
            windows_absolute_path_key(
                ntpath.join(log_root, f"{process_id}.stdout.log")
            )
            if isinstance(log_root, str) and isinstance(process_id, str)
            else None
        )
        expected_stderr_log = (
            windows_absolute_path_key(
                ntpath.join(log_root, f"{process_id}.stderr.log")
            )
            if isinstance(log_root, str) and isinstance(process_id, str)
            else None
        )
        executable_path = process.get("executable_path")
        executable_key = windows_absolute_path_key(executable_path)
        configured_protected_roots = (
            runtime_config.get("protected_roots")
            if isinstance(runtime_config, Mapping)
            and isinstance(runtime_config.get("protected_roots"), list)
            else []
        )
        if (
            expected_process_runtime_root is None
            or windows_absolute_path_key(process.get("runtime_root"))
            != expected_process_runtime_root
            or windows_absolute_path_key(process.get("stdout_log"))
            != expected_stdout_log
            or windows_absolute_path_key(process.get("stderr_log"))
            != expected_stderr_log
            or expected_stdout_log == expected_stderr_log
            or executable_key is None
            or any(
                isinstance(protected_root, str)
                and windows_path_is_within(executable_path, protected_root)
                for protected_root in configured_protected_roots
            )
        ):
            add("PROCESS_PATH_CONFIGURATION_MISMATCH")
        expected_cwd = (
            workspace_root
            if launch_cwd == "."
            else ntpath.join(workspace_root, launch_cwd)
            if isinstance(workspace_root, str) and isinstance(launch_cwd, str)
            else None
        )
        health_endpoint = process.get("health_endpoint")
        if (
            not isinstance(process_launch, Mapping)
            or process.get("command_redacted") != process_launch.get("command")
            or process.get("environment_redacted")
            != process_launch.get("environment")
            or windows_absolute_path_key(process.get("cwd"))
            != windows_absolute_path_key(expected_cwd)
            or process.get("resource_limits") != effective_limits
            or process.get("restart_policy") != effective_restart_policy
            or process.get("max_restart_attempts") != effective_max_restarts
            or process.get("restart_backoff_seconds") != effective_backoff
            or not isinstance(health_endpoint, Mapping)
            or health_endpoint.get("host") != "127.0.0.1"
            or health_endpoint.get("path") != effective_health_path
            or (
                isinstance(process_port, Mapping)
                and health_endpoint.get("port") != process_port.get("port")
            )
        ):
            add("PROCESS_CONFIGURATION_MISMATCH")

        restart_attempt = process.get("restart_attempt")
        max_restart_attempts = process.get("max_restart_attempts")
        if (
            not isinstance(restart_attempt, int)
            or isinstance(restart_attempt, bool)
            or not isinstance(max_restart_attempts, int)
            or isinstance(max_restart_attempts, bool)
            or restart_attempt > max_restart_attempts
            or (
                process.get("restart_policy") == "never"
                and (restart_attempt != 0 or max_restart_attempts != 0)
            )
            or (
                process.get("restart_policy") == "on_failure"
                and max_restart_attempts < 1
            )
        ):
            add("PROCESS_RESTART_BUDGET_INVALID")
        restart_of_process_id = process.get("restart_of_process_id")
        if restart_attempt == 0:
            if restart_of_process_id is not None:
                add("PROCESS_RESTART_LINEAGE_INVALID")
        elif isinstance(restart_attempt, int) and not isinstance(restart_attempt, bool):
            parent = (
                process_by_id.get(restart_of_process_id)
                if isinstance(restart_of_process_id, str)
                else None
            )
            if (
                not isinstance(parent, Mapping)
                or parent.get("state") != "FAILED"
                or parent.get("assignment_id") != process.get("assignment_id")
                or parent.get("workspace_id") != process.get("workspace_id")
                or parent.get("restart_attempt") != restart_attempt - 1
                or parent.get("restart_policy") != process.get("restart_policy")
                or parent.get("max_restart_attempts") != max_restart_attempts
            ):
                add("PROCESS_RESTART_LINEAGE_INVALID")
        if isinstance(process_state, str) and process_state in live_process_states:
            assignment_id = process.get("assignment_id")
            workspace_id = process.get("workspace_id")
            port_lease_id = process.get("port_lease_id")
            assignment = (
                assignment_by_id.get(assignment_id)
                if isinstance(assignment_id, str)
                else None
            )
            workspace = (
                workspace_by_id.get(workspace_id)
                if isinstance(workspace_id, str)
                else None
            )
            port = (
                port_lease_by_id.get(port_lease_id)
                if isinstance(port_lease_id, str)
                else None
            )
            health_endpoint = process.get("health_endpoint")
            expected_port_status = "reserved" if process_state == "PREPARED" else "bound"
            if (
                not isinstance(assignment, Mapping)
                or not isinstance(assignment.get("status"), str)
                or assignment.get("status") not in {"active", "reviews_pending"}
                or not isinstance(workspace, Mapping)
                or workspace.get("assignment_id") != assignment.get("assignment_id")
                or not isinstance(port, Mapping)
                or port.get("assignment_id") != assignment.get("assignment_id")
                or port.get("status") != expected_port_status
                or (
                    expected_port_status == "reserved"
                    and port.get("process_id") is not None
                )
                or (
                    expected_port_status == "bound"
                    and port.get("process_id") != process.get("process_id")
                )
                or not isinstance(health_endpoint, Mapping)
                or health_endpoint.get("host") != port.get("host")
                or health_endpoint.get("port") != port.get("port")
            ):
                add("LIVE_PROCESS_OWNERSHIP_INVALID")

    assignment_enqueue_counts: dict[Any, int] = {}
    review_enqueue_counts: dict[Any, int] = {}
    coordinator_enqueue_counts: dict[Any, int] = {}
    dedupe_keys: set[Any] = set()
    queue_receipt_ids: set[Any] = set()
    for event in outbox_by_id.values():
        key = event.get("dedupe_key")
        if not isinstance(key, str) or not key:
            add("OUTBOX_DEDUPE_KEY_INVALID")
        elif key in dedupe_keys:
            add("OUTBOX_DEDUPE_KEY_DUPLICATE")
        else:
            dedupe_keys.add(key)
        payload = event.get("payload")
        event_type = event.get("event_type")
        binding_valid = isinstance(payload, Mapping) and payload.get("sprint_id") == sprint_id
        if binding_valid and event_type == "ASSIGNMENT_ENQUEUE":
            assignment_id = payload.get("assignment_id")
            assignment_enqueue_counts[assignment_id] = (
                assignment_enqueue_counts.get(assignment_id, 0) + 1
            )
            assignment = (
                assignment_by_id.get(assignment_id)
                if isinstance(assignment_id, str)
                else None
            )
            binding_valid = (
                isinstance(assignment, Mapping)
                and key == f"enqueue:assignment:{sprint_id}:{assignment_id}"
                and payload.get("graph_revision")
                == assignment.get("graph_revision")
                and payload.get("node_id") == assignment.get("node_id")
                and payload.get("agent_phone") == assignment.get("agent_phone")
            )
        elif binding_valid and event_type == "REVIEW_ENQUEUE":
            review_assignment_id = payload.get("review_assignment_id")
            review_enqueue_counts[review_assignment_id] = (
                review_enqueue_counts.get(review_assignment_id, 0) + 1
            )
            review_assignment = (
                review_assignment_by_id.get(review_assignment_id)
                if isinstance(review_assignment_id, str)
                else None
            )
            binding_valid = (
                isinstance(review_assignment, Mapping)
                and key
                == (
                    f"enqueue:review:{sprint_id}:"
                    f"{review_assignment.get('result_key')}:"
                    f"{review_assignment.get('reviewer_index')}"
                )
                and payload.get("source_assignment_id")
                == review_assignment.get("source_assignment_id")
                and payload.get("result_key") == review_assignment.get("result_key")
                and payload.get("reviewer_phone")
                == review_assignment.get("reviewer_phone")
                and payload.get("reviewer_index")
                == review_assignment.get("reviewer_index")
            )
        elif binding_valid and event_type == "COORDINATOR_ENQUEUE":
            context_id = payload.get("context_id")
            coordinator_enqueue_counts[context_id] = (
                coordinator_enqueue_counts.get(context_id, 0) + 1
            )
            context = (
                coordinator_context_by_id.get(context_id)
                if isinstance(context_id, str)
                else None
            )
            context_definition = (
                revision_by_number.get(context.get("graph_revision"), {}).get(
                    "definition"
                )
                if isinstance(context, Mapping)
                else None
            )
            coordinator = (
                context_definition.get("coordinator")
                if isinstance(context_definition, Mapping)
                else None
            )
            coordinator_node = (
                nodes_by_revision.get(context.get("graph_revision"), {}).get(
                    coordinator.get("node_id")
                )
                if isinstance(context, Mapping)
                and isinstance(coordinator, Mapping)
                and isinstance(context.get("graph_revision"), int)
                and isinstance(coordinator.get("node_id"), str)
                else None
            )
            coordinator_agent = (
                coordinator_node.get("agent")
                if isinstance(coordinator_node, Mapping)
                else None
            )
            binding_valid = (
                isinstance(context, Mapping)
                and key == f"enqueue:coordinator:{sprint_id}:{context_id}"
                and isinstance(coordinator_agent, Mapping)
                and payload.get("coordinator_phone")
                == coordinator_agent.get("phone")
                and payload.get("reason_code") == context.get("reason_code")
            )
        else:
            binding_valid = False
        if not binding_valid:
            add("OUTBOX_BINDING_INVALID")
        queue_receipt_id = event.get("queue_receipt_id")
        if event.get("status") == "delivered":
            if not isinstance(queue_receipt_id, str) or not queue_receipt_id:
                add("OUTBOX_QUEUE_RECEIPT_INVALID")
            elif queue_receipt_id in queue_receipt_ids:
                add("OUTBOX_QUEUE_RECEIPT_DUPLICATE")
            else:
                queue_receipt_ids.add(queue_receipt_id)

    if any(
        assignment_enqueue_counts.get(assignment_id) != 1
        for assignment_id in assignment_by_id
    ) or any(assignment_id not in assignment_by_id for assignment_id in assignment_enqueue_counts):
        add("ASSIGNMENT_OUTBOX_COVERAGE_INVALID")
    if any(
        review_enqueue_counts.get(assignment_id) != 1
        for assignment_id in review_assignment_by_id
    ) or any(
        assignment_id not in review_assignment_by_id
        for assignment_id in review_enqueue_counts
    ):
        add("REVIEW_OUTBOX_COVERAGE_INVALID")
    if any(
        coordinator_enqueue_counts.get(context_id) != 1
        for context_id in coordinator_context_by_id
    ) or any(
        context_id not in coordinator_context_by_id
        for context_id in coordinator_enqueue_counts
    ):
        add("COORDINATOR_OUTBOX_COVERAGE_INVALID")

    entry_occurrence_ids = (
        workflow.get("entry_occurrence_ids")
        if isinstance(workflow, Mapping)
        and isinstance(workflow.get("entry_occurrence_ids"), list)
        else []
    )
    expected_initial_assignment_ids: list[Any] = []
    for occurrence_id in entry_occurrence_ids:
        occurrence = occurrence_by_id.get(occurrence_id)
        assignment_ids = (
            occurrence.get("assignment_ids")
            if isinstance(occurrence, Mapping)
            and isinstance(occurrence.get("assignment_ids"), list)
            else []
        )
        expected_initial_assignment_ids.append(
            assignment_ids[0] if assignment_ids else None
        )
    expected_initial_workspace_ids = [
        assignment_by_id[assignment_id].get("workspace_id")
        if assignment_id in assignment_by_id
        else None
        for assignment_id in expected_initial_assignment_ids
    ]
    successful_activation_attempts = 0
    import_idempotency_keys: set[str] = set()
    for attempt in import_attempt_by_id.values():
        import_idempotency_key = attempt.get("idempotency_key")
        if (
            not isinstance(import_idempotency_key, str)
            or not import_idempotency_key
            or import_idempotency_key in import_idempotency_keys
        ):
            add("IMPORT_IDEMPOTENCY_KEY_DUPLICATE")
        else:
            import_idempotency_keys.add(import_idempotency_key)
        preflight = attempt.get("preflight")
        lease_snapshot = (
            preflight.get("lease_snapshot")
            if isinstance(preflight, Mapping)
            else None
        )
        try:
            expected_snapshot_sha256 = canonical_json_sha256(lease_snapshot)
            expected_request_fingerprint = canonical_tuple_sha256(
                identity.get("project_id"),
                repository.get("repository_id"),
                state.get("requested_ref"),
                identity.get("manifest_path"),
            )
        except (TypeError, ValueError):
            expected_snapshot_sha256 = None
            expected_request_fingerprint = None
        if (
            not isinstance(preflight, Mapping)
            or not isinstance(lease_snapshot, Mapping)
            or preflight.get("lease_snapshot_sha256")
            != expected_snapshot_sha256
        ):
            add("PREFLIGHT_SNAPSHOT_DIGEST_MISMATCH")
        if isinstance(lease_snapshot, Mapping):
            for collection, id_field in (
                ("branch_leases", "lease_id"),
                ("port_leases", "lease_id"),
                ("process_owners", "process_id"),
            ):
                records = lease_snapshot.get(collection)
                if isinstance(records, list) and records != sorted(
                    records,
                    key=lambda record: (
                        record.get(id_field, "")
                        if isinstance(record, Mapping)
                        else ""
                    ),
                ):
                    add("PREFLIGHT_SNAPSHOT_ORDER_INVALID")
        prepared_artifact_ids = attempt.get("prepared_artifact_ids")
        prepared_artifact_ids = (
            prepared_artifact_ids
            if isinstance(prepared_artifact_ids, list)
            else []
        )
        prepared_ids_valid = (
            len(prepared_artifact_ids)
            == len({item.casefold() for item in prepared_artifact_ids})
            and all(
                isinstance(item, str) and windows_path_segment_valid(item)
                for item in prepared_artifact_ids
            )
        ) if all(isinstance(item, str) for item in prepared_artifact_ids) else False
        attempt_status = attempt.get("status")
        attempt_phase = attempt.get("phase")
        activation_response = attempt.get("activation_response")
        if attempt_status == "succeeded" and attempt_phase == "ACTIVATE":
            successful_activation_attempts += 1
            response_valid = (
                isinstance(activation_response, Mapping)
                and isinstance(workflow, Mapping)
                and activation_response.get("sprint_id") == sprint_id
                and activation_response.get("identity") == identity
                and activation_response.get("workspace_source_commit")
                == state.get("workspace_source_commit")
                and activation_response.get("execution_mode")
                == workflow.get("execution_mode")
                and activation_response.get("initial_assignment_ids")
                == expected_initial_assignment_ids
                and activation_response.get("deduplicated") is False
            )
        else:
            response_valid = activation_response is None
        if (
            attempt.get("identity") != identity
            or attempt.get("contract_version") != state.get("contract_version")
            or attempt.get("manifest_schema_version")
            != state.get("manifest_schema_version")
            or attempt.get("request_fingerprint")
            != expected_request_fingerprint
            or not isinstance(preflight, Mapping)
            or preflight.get("sprint_id") != sprint_id
            or preflight.get("manifest_commit") != identity.get("commit")
            or preflight.get("workspace_source_commit")
            != state.get("workspace_source_commit")
            or preflight.get("manifest_sha256")
            != identity.get("manifest_sha256")
            or (
                attempt_status == "succeeded"
                and (attempt_phase != "ACTIVATE" or preflight.get("ok") is not True)
            )
            or not prepared_ids_valid
            or (
                attempt_status == "succeeded"
                and prepared_artifact_ids != expected_initial_workspace_ids
            )
            or not response_valid
        ):
            add("IMPORT_ATTEMPT_BINDING_INVALID")

    if status == "preparing" and (
        isinstance(workflow, Mapping)
        or active_id_set
        or outcomes
        or assignment_by_id
        or receipt_by_key
        or review_assignment_by_id
        or review_by_assignment_id
        or rework_by_id
        or integration_by_id
        or integration_workspace_by_id
        or branch_lease_by_id
        or workspace_by_id
        or port_lease_by_id
        or process_by_id
        or journal_by_result
        or outbox_by_id
        or successful_activation_attempts
    ):
        add("PREPARING_STATE_HAS_ACTIVATED_WORK")

    if status in {"completed", "failed", "blocked"}:
        terminal_tokens = [
            token for token in token_by_id.values() if token.get("status") == "terminal"
        ]
        terminal_manifest_statuses = {
            nodes_by_revision.get(token.get("target_graph_revision"), {})
            .get(token.get("target_node_id"), {})
            .get("status")
            for token in terminal_tokens
        }
        expected_terminal_status = (
            "failed"
            if "FAILED" in terminal_manifest_statuses
            else "blocked"
            if "BLOCKED_EXTERNAL" in terminal_manifest_statuses
            else "completed"
            if terminal_manifest_statuses == {"DONE"}
            else None
        )
        if isinstance(workflow, Mapping) and not terminal_tokens:
            add("TERMINAL_TOKEN_PROOF_MISSING")
        if terminal_tokens and status != expected_terminal_status:
            add("TERMINAL_STATUS_MISMATCH")
        emitted_by_occurrence = {
            token.get("source_occurrence_id") for token in token_by_id.values()
        }
        if isinstance(workflow, Mapping) and not set(occurrence_by_id).issubset(
            emitted_by_occurrence
        ):
            add("TERMINAL_BRANCH_PROOF_MISSING")
        live_occurrences = {
            occurrence_id
            for occurrence_id, occurrence in occurrence_by_id.items()
            if occurrence.get("state") in {"prepared", "active", "reviews_pending"}
        }
        if (
            active_id_set
            or outcomes
            or live_assignment_ids
            or live_occurrences
            or any(
                lease.get("status") == "active"
                for lease in branch_lease_by_id.values()
            )
            or any(
                lease.get("status") in {"reserved", "bound"}
                for lease in port_lease_by_id.values()
            )
            or any(
                process.get("state") in live_process_states
                for process in process_by_id.values()
            )
            or any(token.get("status") == "available" for token in token_by_id.values())
            or any(event.get("status") == "pending" for event in outbox_by_id.values())
        ):
            add("TERMINAL_STATE_HAS_LIVE_WORK")
        if status == "completed":
            completed_occurrence_ids = {
                occurrence_id
                for occurrence_id, occurrence in occurrence_by_id.items()
                if occurrence.get("state") == "completed"
            }
            if (
                not terminal_tokens
                or any(
                    occurrence.get("state") != "completed"
                    for occurrence in occurrence_by_id.values()
                )
                or any(
                    assignment.get("status") != "completed"
                    for assignment in assignment_by_id.values()
                )
                or not completed_occurrence_ids.issubset(emitted_by_occurrence)
            ):
                add("COMPLETED_STATE_TERMINAL_PROOF_MISSING")
        elif not terminal_evidence_context_ids:
            add("TERMINAL_FAILURE_EVIDENCE_MISSING")

    activated_state = isinstance(workflow, Mapping) or bool(assignment_by_id)
    if activated_state and successful_activation_attempts != 1:
        add("ACTIVATION_RECEIPT_MISSING")

    return tuple(issues)


def managed_project_control_invariant_issues(
    control: Mapping[str, Any],
    known_sprint_ids: set[str] | frozenset[str] | None = None,
) -> tuple[str, ...]:
    """Validate relations after project-control JSON Schema validation."""

    if not isinstance(control, Mapping):
        return ("PROJECT_CONTROL_NOT_OBJECT",)

    issues: list[str] = []
    project_id = control.get("project_id")
    counter = control.get("activation_fencing_counter")
    if not isinstance(counter, int) or isinstance(counter, bool) or counter < 0:
        issues.append("ACTIVATION_FENCING_COUNTER_INVALID")
        counter = -1

    records = control.get("start_idempotency_records")
    records = records if isinstance(records, list) else []
    keys: set[str] = set()
    attempts: set[str] = set()
    fencing_tokens: set[int] = set()
    successful_sprint_ids: set[str] = set()
    successful_sprints_by_token: list[tuple[int, str]] = []
    record_by_attempt: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            issues.append("START_IDEMPOTENCY_RECORD_INVALID")
            continue
        key = record.get("idempotency_key")
        attempt_id = record.get("attempt_id")
        recovery_of_attempt_id = record.get("recovery_of_attempt_id")
        recovery_generation = record.get("recovery_generation")
        if not isinstance(key, str) or not key or key in keys:
            issues.append("START_IDEMPOTENCY_KEY_DUPLICATE")
        else:
            keys.add(key)
        if not isinstance(attempt_id, str) or not attempt_id or attempt_id in attempts:
            issues.append("START_ATTEMPT_ID_DUPLICATE")
        else:
            attempts.add(attempt_id)
            record_by_attempt[attempt_id] = record
        if recovery_of_attempt_id is None:
            if recovery_generation != 0:
                issues.append("START_RECOVERY_LINEAGE_INVALID")
        elif (
            not isinstance(recovery_of_attempt_id, str)
            or not recovery_of_attempt_id
            or not isinstance(recovery_generation, int)
            or isinstance(recovery_generation, bool)
            or recovery_generation < 1
        ):
            issues.append("START_RECOVERY_LINEAGE_INVALID")

        token = record.get("fencing_token")
        if token is not None:
            if (
                not isinstance(token, int)
                or isinstance(token, bool)
                or token < 1
                or token > counter
                or token in fencing_tokens
            ):
                issues.append("START_FENCING_TOKEN_INVALID")
            else:
                fencing_tokens.add(token)

        pinned = record.get("pinned_identity")
        sprint_id = record.get("sprint_id")
        status = record.get("status")
        if status in {"PREPARING", "ACTIVATING", "SUCCEEDED"} and token is None:
            issues.append("START_FENCING_TOKEN_INVALID")
        if isinstance(pinned, Mapping):
            try:
                derived_sprint_id = SprintProvenance(
                    project_id=pinned["project_id"],
                    repository_id=pinned["repository_id"],
                    commit=pinned["commit"],
                    manifest_path=pinned["manifest_path"],
                    manifest_sha256=pinned["manifest_sha256"],
                ).stable_sprint_id()
            except (KeyError, TypeError, ValueError):
                derived_sprint_id = None
            if pinned.get("project_id") != project_id or (
                sprint_id is not None and sprint_id != derived_sprint_id
            ):
                issues.append("START_PINNED_IDENTITY_MISMATCH")
        elif isinstance(status, str) and status in {
            "PREPARING",
            "ACTIVATING",
            "SUCCEEDED",
        }:
            issues.append("START_PINNED_IDENTITY_MISSING")

        if isinstance(status, str) and status in {
            "PREPARING",
            "ACTIVATING",
            "SUCCEEDED",
        } and not isinstance(
            record.get("workspace_source_commit"), str
        ):
            issues.append("START_WORKSPACE_COMMIT_MISSING")

        if status == "SUCCEEDED":
            response = record.get("response")
            if (
                not isinstance(response, Mapping)
                or response.get("sprint_id") != sprint_id
                or response.get("identity") != pinned
                or response.get("workspace_source_commit")
                != record.get("workspace_source_commit")
                or response.get("deduplicated") is not False
            ):
                issues.append("START_SUCCESS_RESPONSE_MISMATCH")
            if isinstance(sprint_id, str):
                if sprint_id in successful_sprint_ids:
                    issues.append("START_SPRINT_SUCCESS_DUPLICATE")
                successful_sprint_ids.add(sprint_id)
                if isinstance(token, int) and not isinstance(token, bool):
                    successful_sprints_by_token.append((token, sprint_id))

    for record in records:
        if not isinstance(record, Mapping):
            continue
        parent_attempt_id = record.get("recovery_of_attempt_id")
        if parent_attempt_id is None:
            continue
        parent = record_by_attempt.get(parent_attempt_id)
        if (
            not isinstance(parent, Mapping)
            or parent.get("status") != "FAILED"
            or record.get("recovery_generation")
            != parent.get("recovery_generation", -1) + 1
            or record.get("request_fingerprint")
            != parent.get("request_fingerprint")
            or record.get("pinned_identity") != parent.get("pinned_identity")
            or record.get("workspace_source_commit")
            != parent.get("workspace_source_commit")
            or record.get("sprint_id") != parent.get("sprint_id")
        ):
            issues.append("START_RECOVERY_LINEAGE_INVALID")

    lease = control.get("activation_lease")
    if isinstance(lease, Mapping):
        attempt_id = lease.get("attempt_id")
        owner = record_by_attempt.get(attempt_id)
        if (
            lease.get("fencing_token") != counter
            or not isinstance(owner, Mapping)
            or owner.get("fencing_token") != counter
            or owner.get("status") not in {"VALIDATING", "PREPARING", "ACTIVATING"}
        ):
            issues.append("ACTIVATION_LEASE_OWNER_MISMATCH")

    active_sprint_id = control.get("active_sprint_id")
    latest_successful_sprint_id = (
        max(successful_sprints_by_token, key=lambda item: item[0])[1]
        if successful_sprints_by_token
        else None
    )
    if active_sprint_id != latest_successful_sprint_id:
        issues.append("ACTIVE_SPRINT_INDEX_MISMATCH")
    if known_sprint_ids is not None and active_sprint_id is not None:
        if active_sprint_id not in known_sprint_ids:
            issues.append("ACTIVE_SPRINT_STATE_MISSING")

    return tuple(dict.fromkeys(issues))


def resolve_sprint_type(payload: Mapping[str, Any]) -> SprintTypeSelection:
    """Resolve the optional type without mutating or normalizing ``payload``.

    Callers must invoke this before acquiring an activation lease, changing a
    pending record, writing history, replacing agents, enqueueing assignments,
    creating workspaces, or allocating ports/processes. A start-from-git caller
    may first update its isolated bare mirror solely to discover and read the
    pinned manifest. The returned pipeline does not inspect or alter
    ``execution.mode``.
    """

    if SPRINT_TYPE_FIELD not in payload:
        return SprintTypeSelection(
            declared=None,
            effective=SprintType.LEGACY_V1,
            pipeline=SprintPipeline.LEGACY,
        )

    raw_value = payload[SPRINT_TYPE_FIELD]
    try:
        declared = SprintType(raw_value)
    except (TypeError, ValueError):
        raise SprintTypeUnsupported() from None

    pipeline = (
        SprintPipeline.LEGACY
        if declared is SprintType.LEGACY_V1
        else SprintPipeline.MANAGED_WORKSPACE
    )
    return SprintTypeSelection(
        declared=declared,
        effective=declared,
        pipeline=pipeline,
    )


__all__ = [
    "MANAGED_SPRINT_SCHEMA_VERSION",
    "MANAGED_MANIFEST_SCHEMA",
    "ASSIGNMENT_STATE_TRANSITIONS",
    "BRANCH_LEASE_STATE_TRANSITIONS",
    "OUTBOX_STATE_TRANSITIONS",
    "PORT_LEASE_STATE_TRANSITIONS",
    "PROCESS_STATE_TRANSITIONS",
    "REVIEW_ASSIGNMENT_STATE_TRANSITIONS",
    "SPRINT_TYPE_CONTRACT_VERSION",
    "SPRINT_TYPE_FIELD",
    "SPRINT_TYPE_UNSUPPORTED",
    "TRANSITION_JOURNAL_STATES",
    "SprintImportPhase",
    "SprintPipeline",
    "SprintProvenance",
    "SprintType",
    "SprintTypeSelection",
    "SprintTypeUnsupported",
    "StartSprintFromGitRequest",
    "canonical_json_bytes",
    "canonical_json_sha256",
    "canonical_tuple_bytes",
    "canonical_tuple_sha256",
    "journal_advance_allowed",
    "lifecycle_transition_allowed",
    "git_ref_format_valid",
    "managed_manifest_schema",
    "managed_activation_invariant_issues",
    "managed_graph_semantic_issues",
    "managed_integration_id",
    "managed_integration_workspace_id",
    "managed_node_path_segment",
    "managed_project_path_segment",
    "managed_sprint_path_segment",
    "managed_occurrence_id",
    "managed_project_control_invariant_issues",
    "managed_runtime_config_invariant_issues",
    "managed_result_key",
    "managed_transition_token_id",
    "mirror_storage_key",
    "process_transition_allowed",
    "recovery_request_fingerprint",
    "relative_git_path_valid",
    "repair_request_fingerprint",
    "resolve_sprint_type",
    "review_request_fingerprint",
    "windows_path_segment_valid",
]
