"""Transactional ``start-from-git`` support for managed workspace sprints.

The importer is deliberately separate from the legacy actor/pending import
pipeline.  A fresh call may mutate only its isolated bare Git mirror until the
manifest has selected ``managed_workspace_v1``.  Project control and the
activated runtime then live in one SQLite database so publishing a sprint and
advancing its fenced idempotency record are one durable commit.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import socket
import sqlite3
import sys
import threading
from typing import Any, Callable, Iterator, Mapping, Sequence
from uuid import uuid4

from .branch_leases import (
    BranchHeads,
    BranchLease,
    BranchLeaseError,
    BranchLeaseRequest,
    BranchLeaseStore,
    deterministic_branch_lease_id,
    select_initial_head,
)
from .git_provider import (
    FileLockTimeout,
    ManagedGitError,
    ManagedFileLock,
    ManagedGitProvider,
    ManagedRepository,
    RepositorySpec,
    TRANSIENT_GIT_ERROR_CODES,
    canonical_remote_from_address,
)
from .port_leases import (
    ManagedPortReconciliationFence,
    ManagedPortReservationError,
    ManagedPortReservationRegistry,
    ManagedPortReservationToken,
)
from .sprint_types import (
    SprintPipeline,
    SprintProvenance,
    SprintTypeUnsupported,
    StartSprintFromGitRequest,
    canonical_json_bytes,
    canonical_json_sha256,
    git_ref_format_valid,
    managed_activation_invariant_issues,
    managed_graph_semantic_issues,
    managed_occurrence_id,
    managed_project_control_invariant_issues,
    managed_runtime_config_invariant_issues,
    relative_git_path_valid,
    resolve_sprint_type,
    windows_absolute_path_key,
    windows_path_is_within,
)
from .workspace_manager import (
    ManagedWorkspace,
    ManagedWorkspaceManager,
    WorkspaceError,
    WorkspaceRequest,
)


MANAGED_SPRINT_TYPE_REQUIRED = "MANAGED_SPRINT_TYPE_REQUIRED"
INVALID_MANAGED_SPRINT_REQUEST = "INVALID_MANAGED_SPRINT_REQUEST"
SPRINT_SCHEMA_UNSUPPORTED = "SPRINT_SCHEMA_UNSUPPORTED"
SPRINT_PREFLIGHT_FAILED = "SPRINT_PREFLIGHT_FAILED"
SPRINT_PREPARE_FAILED = "SPRINT_PREPARE_FAILED"
SPRINT_ACTIVATE_FAILED = "SPRINT_ACTIVATE_FAILED"
IDEMPOTENCY_KEY_CONFLICT = "IDEMPOTENCY_KEY_CONFLICT"
PROJECT_ACTIVATION_IN_PROGRESS = "PROJECT_ACTIVATION_IN_PROGRESS"
SPRINT_ALREADY_EXISTS = "SPRINT_ALREADY_EXISTS"
SPRINT_RECOVERY_REQUIRED = "SPRINT_RECOVERY_REQUIRED"
LEASE_SNAPSHOT_STALE = "LEASE_SNAPSHOT_STALE"

_SCHEMA_ROOT = Path(__file__).resolve().parents[1] / "schemas"
_REQUEST_SCHEMA = "start-sprint-from-git-v1.schema.json"
_MANIFEST_SCHEMA = "managed-workspace-sprint-v1.schema.json"
_LEASE_SECONDS = 300
_MANIFEST_MAX_BYTES = 4 * 1024 * 1024
_ARTIFACT_MAX_BYTES = 64 * 1024 * 1024


def _aggregate_process_limits_supported() -> bool:
    """Return whether this host has the v1 assignment-wide hard-limit backend."""

    # Windows Job Objects enforce memory, CPU rate, and process count over the
    # complete descendant set.  POSIX rlimits are per-process or per-UID and
    # therefore cannot truthfully implement the frozen per-assignment policy.
    return os.name == "nt"


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp(clock: Callable[[], datetime]) -> str:
    return _utc_now(clock).isoformat()


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _safe_identifier(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}"


def _assignment_id(sprint_id: str, node_id: str) -> str:
    # Sprint IDs already contribute 64 digest characters to the deterministic
    # workspace path.  A 96-bit assignment suffix leaves enough Win32 path
    # budget for Git's object fan-out below ordinary temporary roots while
    # retaining a collision-resistant per-sprint namespace.
    digest = hashlib.sha256("\0".join((sprint_id, node_id, "1")).encode("utf-8")).hexdigest()
    return f"assignment-{digest[:24]}"


def _workspace_id(request: WorkspaceRequest) -> str:
    return _safe_identifier(
        "workspace",
        request.project_id,
        request.sprint_id,
        request.node_id,
        request.assignment_id,
    )


def _strict_json_object(blob: bytes) -> dict[str, Any]:
    """Parse canonical-input JSON without accepting ambiguous object keys."""

    try:
        text = blob.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError("JSON is not strict UTF-8") from exc

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("JSON contains a duplicate object key")
            result[key] = value
        return result

    def reject_constant(_: str) -> Any:
        raise ValueError("JSON contains a non-finite number")

    try:
        value = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, RecursionError, MemoryError) as exc:
        raise ValueError("JSON is malformed") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON document must be an object")

    stack: list[Any] = [value]
    while stack:
        candidate = stack.pop()
        if isinstance(candidate, str):
            try:
                candidate.encode("utf-8", errors="strict")
            except UnicodeEncodeError as exc:
                raise ValueError("JSON contains a non-Unicode scalar value") from exc
        elif isinstance(candidate, float) and not math.isfinite(candidate):
            raise ValueError("JSON contains a non-finite number")
        elif isinstance(candidate, dict):
            for key, child in candidate.items():
                stack.append(key)
                stack.append(child)
        elif isinstance(candidate, list):
            stack.extend(candidate)
    return value


def parse_strict_json_object(blob: bytes) -> dict[str, Any]:
    """Public strict-JSON parser shared by managed runtime endpoints."""

    return _strict_json_object(blob)


def _schema_errors(
    instance: Any,
    filename: str,
    *,
    issue_code: str = "MANAGED_MANIFEST_SCHEMA_INVALID",
) -> list[dict[str, str]]:
    """Return stable, value-free JSON Schema diagnostics."""

    try:
        from jsonschema import Draft202012Validator, FormatChecker
        from referencing import Registry, Resource
    except ImportError as exc:  # pragma: no cover - deployment packaging guard
        raise RuntimeError("jsonschema is required for managed sprint imports") from exc
    schemas = {
        path.name: json.loads(path.read_text(encoding="utf-8"))
        for path in _SCHEMA_ROOT.glob("*.schema.json")
    }
    schema = schemas[filename]
    registry = Registry()
    for candidate in schemas.values():
        registry = registry.with_resource(
            candidate["$id"], Resource.from_contents(candidate)
        )
    validator = Draft202012Validator(
        schema,
        registry=registry,
        format_checker=FormatChecker(),
    )
    errors = sorted(
        validator.iter_errors(instance),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    result: list[dict[str, str]] = []
    for error in errors:
        path = ".".join(str(part) for part in error.absolute_path)
        result.append(
            {
                "code": issue_code,
                "path": path,
                "message": "Value does not satisfy the declared managed schema",
            }
        )
    return result


def managed_schema_errors(
    instance: Any,
    filename: str,
    *,
    issue_code: str = "MANAGED_REQUEST_SCHEMA_INVALID",
) -> list[dict[str, str]]:
    """Validate a managed public payload with stable, value-free issues."""

    return _schema_errors(instance, filename, issue_code=issue_code)


_RUNTIME_STATE_SCHEMA_BY_VERSION = {
    1: "managed-runtime-state-v1.schema.json",
    2: "managed-runtime-state-v2.schema.json",
}


def _runtime_state_schema_errors(
    instance: Any,
    *,
    issue_code: str,
) -> list[dict[str, str]]:
    """Validate a durable runtime snapshot with its declared exact version."""

    version = instance.get("schema_version") if isinstance(instance, Mapping) else None
    filename = (
        _RUNTIME_STATE_SCHEMA_BY_VERSION.get(version)
        if isinstance(version, int) and not isinstance(version, bool)
        else None
    )
    if filename is None:
        return [
            {
                "code": issue_code,
                "path": "schema_version",
                "message": "Unsupported managed runtime-state schema version",
            }
        ]
    return _schema_errors(instance, filename, issue_code=issue_code)


@dataclass(frozen=True, slots=True)
class ManagedStartResult:
    response: dict[str, Any]
    http_status: int


class ManagedImportError(RuntimeError):
    """Transport-neutral managed API error with a frozen response envelope."""

    def __init__(
        self,
        code: str,
        http_status: int,
        correlation_id: str,
        *,
        phase: str | None = None,
        issues: Sequence[Mapping[str, Any]] | None = None,
        field: str | None = None,
        supported: Sequence[str] | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(code)
        detail: dict[str, Any] = {
            "error": code,
            "correlation_id": correlation_id,
        }
        if phase is not None:
            detail["phase"] = phase
        if issues is not None:
            detail["issues"] = [dict(issue) for issue in issues]
        if field is not None:
            detail["field"] = field
        if supported is not None:
            detail["supported"] = list(supported)
        self.code = code
        self.http_status = http_status
        self.correlation_id = correlation_id
        self.envelope = {"detail": detail}
        self.evidence = deepcopy(dict(evidence or {}))

    @classmethod
    def from_stored(
        cls,
        envelope: Mapping[str, Any],
        http_status: int,
        evidence: Mapping[str, Any] | None = None,
    ) -> "ManagedImportError":
        detail = envelope.get("detail") if isinstance(envelope, Mapping) else None
        if not isinstance(detail, Mapping):
            raise RuntimeError("stored managed error envelope is corrupt")
        error = cls(
            str(detail.get("error") or SPRINT_ACTIVATE_FAILED),
            http_status,
            str(detail.get("correlation_id") or "corrupt-attempt"),
            phase=str(detail["phase"]) if detail.get("phase") is not None else None,
            issues=(
                detail.get("issues")
                if isinstance(detail.get("issues"), list)
                else None
            ),
            field=str(detail["field"]) if detail.get("field") is not None else None,
            supported=(
                detail.get("supported")
                if isinstance(detail.get("supported"), list)
                else None
            ),
            evidence=evidence,
        )
        error.envelope = deepcopy(dict(envelope))
        return error


class ManagedRetryImportConflict(RuntimeError):
    """Expected RETRY_IMPORT ineligibility, safe to expose as a 409 conflict."""

    code = "RECOVERY_CONFLICT"


_DEFAULT_MANAGED_PORT_RESERVATIONS = ManagedPortReservationRegistry()


def validate_start_request(payload: Any, correlation_id: str) -> StartSprintFromGitRequest:
    if not isinstance(payload, dict):
        raise ManagedImportError(
            INVALID_MANAGED_SPRINT_REQUEST,
            400,
            correlation_id,
            phase="VALIDATE",
        )
    errors = _schema_errors(payload, _REQUEST_SCHEMA)
    if errors:
        field = errors[0]["path"]
        raise ManagedImportError(
            INVALID_MANAGED_SPRINT_REQUEST,
            400,
            correlation_id,
            phase="VALIDATE",
            field=field,
        )
    if not git_ref_format_valid(payload["ref"]):
        raise ManagedImportError(
            INVALID_MANAGED_SPRINT_REQUEST,
            400,
            correlation_id,
            phase="VALIDATE",
            field="ref",
        )
    if not relative_git_path_valid(payload["manifest_path"]):
        raise ManagedImportError(
            INVALID_MANAGED_SPRINT_REQUEST,
            400,
            correlation_id,
            phase="VALIDATE",
            field="manifest_path",
        )
    return StartSprintFromGitRequest(
        repository_id=payload["repository_id"],
        ref=payload["ref"],
        manifest_path=payload["manifest_path"],
        idempotency_key=payload["idempotency_key"],
    )


def parse_start_request_bytes(
    body: bytes, correlation_id: str
) -> StartSprintFromGitRequest:
    if not isinstance(body, bytes) or len(body) > _MANIFEST_MAX_BYTES:
        raise ManagedImportError(
            INVALID_MANAGED_SPRINT_REQUEST,
            400,
            correlation_id,
            phase="VALIDATE",
        )
    try:
        payload = _strict_json_object(body)
    except ValueError:
        raise ManagedImportError(
            INVALID_MANAGED_SPRINT_REQUEST,
            400,
            correlation_id,
            phase="VALIDATE",
        ) from None
    return validate_start_request(payload, correlation_id)


def _required_environment_value(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _parse_bool(value: str) -> bool:
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("boolean environment value is invalid")


def _normalized_path(value: str) -> str:
    if windows_absolute_path_key(value) is None:
        raise ValueError("managed path must be a canonical absolute DOS path")
    if os.name == "nt":
        path = Path(value)
        resolved = Path(os.path.realpath(path.resolve(strict=False)))
        if os.path.normcase(os.path.normpath(str(path.absolute()))) != os.path.normcase(
            os.path.normpath(str(resolved))
        ):
            raise ValueError("managed path contains a filesystem alias")
        return str(resolved).replace("\\", "/")
    return value.replace("\\", "/")


def normalize_managed_runtime_config(
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build the explicit frozen managed configuration from environment."""

    source = os.environ if environment is None else environment
    service_root = _normalized_path(
        _required_environment_value(source, "NGINX_QA_SERVICE_ROOT")
    )
    runtime_root = _normalized_path(
        _required_environment_value(source, "NGINX_QA_RUNTIME_ROOT")
    )
    prompt_root = _normalized_path(
        _required_environment_value(source, "NGINX_QA_PROMPT_ROOT")
    )
    managed_root = _normalized_path(
        _required_environment_value(source, "NGINX_QA_MANAGED_ROOT")
    )
    protected_raw = _required_environment_value(source, "NGINX_QA_PROTECTED_ROOTS")
    try:
        decoded_protected = json.loads(protected_raw)
    except json.JSONDecodeError:
        decoded_protected = None
    if isinstance(decoded_protected, list):
        raw_protected = decoded_protected
    else:
        raw_protected = [item for item in protected_raw.split(os.pathsep) if item]
    protected_roots = [_normalized_path(str(item)) for item in raw_protected]
    if not protected_roots:
        raise ValueError("NGINX_QA_PROTECTED_ROOTS must not be empty")

    port_range = _required_environment_value(source, "NGINX_QA_CHILD_PORT_RANGE")
    if port_range.count("-") != 1:
        raise ValueError("NGINX_QA_CHILD_PORT_RANGE must use start-end")
    raw_start, raw_end = port_range.split("-", 1)
    config = {
        "http_host": _required_environment_value(source, "NGINX_QA_HTTP_HOST"),
        "http_port": int(_required_environment_value(source, "NGINX_QA_HTTP_PORT")),
        "service_root": service_root,
        "protected_roots": protected_roots,
        "runtime_root": runtime_root,
        "process_runtime_root": f"{runtime_root.rstrip('/')}/processes",
        "log_root": f"{runtime_root.rstrip('/')}/logs",
        "pid_root": f"{runtime_root.rstrip('/')}/pids",
        "lease_root": f"{runtime_root.rstrip('/')}/leases",
        "prompt_root": prompt_root,
        "managed_root": managed_root,
        "git_fetch_timeout_seconds": int(
            _required_environment_value(source, "NGINX_QA_GIT_FETCH_TIMEOUT_SECONDS")
        ),
        "child_port_start": int(raw_start),
        "child_port_end": int(raw_end),
        "instance_id": _required_environment_value(source, "NGINX_QA_INSTANCE_ID"),
        "disable_telegram": _parse_bool(
            _required_environment_value(source, "NGINX_QA_DISABLE_TELEGRAM")
        ),
        "disable_tunnel": _parse_bool(
            _required_environment_value(source, "NGINX_QA_DISABLE_TUNNEL")
        ),
    }
    return config


def empty_project_control(project_id: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "project_id": project_id,
        "active_sprint_id": None,
        "activation_fencing_counter": 0,
        "activation_lease": None,
        "start_idempotency_records": [],
    }


class ManagedImportStore:
    """SQLite persistence for project control and managed runtime snapshots."""

    def __init__(self, database_path: str | os.PathLike[str], *, timeout: float = 30.0):
        path = Path(database_path)
        if not path.is_absolute():
            raise ValueError("managed import database path must be absolute")
        self.database_path = path.resolve(strict=False)
        self.timeout = timeout
        self._initialization_lock = threading.Lock()
        self._initialized = False

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._initialization_lock:
            if not self._initialized:
                self._initialize()
                self._initialized = True

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=self.timeout,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(
        self,
        *,
        rollback_actions: list[Callable[[], None]] | None = None,
    ) -> Iterator[sqlite3.Connection]:
        self._ensure_initialized()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            # Undo process-local capabilities while BEGIN IMMEDIATE still
            # excludes a successor transaction.  Releasing SQLite first would
            # let that successor adopt the provisional socket before this
            # transaction's delayed rollback token runs.
            for action in reversed(rollback_actions or []):
                try:
                    action()
                except Exception:
                    # Preserve the transaction failure; a reservation holder
                    # is process-local and will also close on process exit.
                    pass
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        initialization_lock = (
            self.database_path.parent / f".{self.database_path.name}.init.lock"
        )
        with ManagedFileLock(initialization_lock, timeout=self.timeout):
            with closing(self._connect()) as connection:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS managed_projects (
                    project_id TEXT PRIMARY KEY,
                    control_json TEXT NOT NULL,
                    revision INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS managed_sprints (
                    project_id TEXT NOT NULL,
                    sprint_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    PRIMARY KEY (project_id, sprint_id),
                    FOREIGN KEY (project_id) REFERENCES managed_projects(project_id)
                );
                CREATE INDEX IF NOT EXISTS managed_sprints_project_status
                    ON managed_sprints(project_id, status);
                CREATE TABLE IF NOT EXISTS managed_port_leases (
                    lease_id TEXT PRIMARY KEY,
                    instance_id TEXT NOT NULL,
                    network_namespace_id TEXT NOT NULL,
                    assignment_id TEXT NOT NULL,
                    process_id TEXT,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    lease_json TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS managed_live_port_ownership
                    ON managed_port_leases(network_namespace_id, host, port)
                    WHERE status IN ('reserved', 'bound');
                CREATE TABLE IF NOT EXISTS managed_process_owners (
                    process_id TEXT PRIMARY KEY,
                    assignment_id TEXT NOT NULL,
                    port_lease_id TEXT NOT NULL UNIQUE,
                    pid INTEGER,
                    state TEXT NOT NULL,
                    process_json TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS managed_live_process_assignment
                    ON managed_process_owners(assignment_id)
                    WHERE state IN ('PREPARED', 'STARTING', 'HEALTHY', 'STOPPING');
                CREATE TABLE IF NOT EXISTS managed_queue_items (
                    dedupe_key TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    receipt_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                """
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    duplicate_active = connection.execute(
                        """
                        SELECT project_id FROM managed_sprints
                        WHERE status = 'active'
                        GROUP BY project_id HAVING COUNT(*) > 1
                        LIMIT 1
                        """
                    ).fetchone()
                    if duplicate_active is not None:
                        raise RuntimeError(
                            "managed sprint runtime invariant failed: "
                            "MULTIPLE_ACTIVE_SPRINTS"
                        )
                    connection.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS managed_single_active_sprint
                        ON managed_sprints(project_id) WHERE status = 'active'
                        """
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

    @staticmethod
    def _resource_snapshot_in_transaction(
        connection: sqlite3.Connection,
    ) -> dict[str, list[dict[str, Any]]]:
        port_leases: list[dict[str, Any]] = []
        for row in connection.execute(
            """
            SELECT lease_json FROM managed_port_leases
            WHERE status IN ('reserved', 'bound') ORDER BY lease_id
            """
        ):
            value = json.loads(row["lease_json"])
            if not isinstance(value, Mapping):
                raise RuntimeError("managed port lease is corrupt")
            port_leases.append(
                {
                    key: deepcopy(value.get(key))
                    for key in (
                        "lease_id",
                        "instance_id",
                        "network_namespace_id",
                        "assignment_id",
                        "process_id",
                        "host",
                        "port",
                        "status",
                    )
                }
            )
        process_owners: list[dict[str, Any]] = []
        for row in connection.execute(
            """
            SELECT process_json FROM managed_process_owners
            WHERE state IN ('PREPARED', 'STARTING', 'HEALTHY', 'STOPPING')
            ORDER BY process_id
            """
        ):
            value = json.loads(row["process_json"])
            if not isinstance(value, Mapping):
                raise RuntimeError("managed process owner is corrupt")
            process_owners.append(
                {
                    "process_id": deepcopy(value.get("process_id")),
                    "assignment_id": deepcopy(value.get("assignment_id")),
                    "pid": deepcopy(value.get("pid")),
                    "state": deepcopy(value.get("state")),
                }
            )
        return {
            "port_leases": port_leases,
            "process_owners": process_owners,
        }

    def resource_snapshot(self) -> dict[str, list[dict[str, Any]]]:
        if not self.database_path.exists():
            return {"port_leases": [], "process_owners": []}
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            return self._resource_snapshot_in_transaction(connection)

    @staticmethod
    def _record(control: Mapping[str, Any], key: str) -> dict[str, Any] | None:
        records = control.get("start_idempotency_records")
        if not isinstance(records, list):
            raise RuntimeError("managed project control is corrupt")
        for record in records:
            if isinstance(record, dict) and record.get("idempotency_key") == key:
                return record
        return None

    @staticmethod
    def _record_by_attempt(
        control: Mapping[str, Any], attempt_id: str
    ) -> dict[str, Any] | None:
        records = control.get("start_idempotency_records")
        if not isinstance(records, list):
            raise RuntimeError("managed project control is corrupt")
        for record in records:
            if isinstance(record, dict) and record.get("attempt_id") == attempt_id:
                return record
        return None

    @staticmethod
    def _load_control(
        connection: sqlite3.Connection, project_id: str
    ) -> tuple[dict[str, Any], int]:
        row = connection.execute(
            "SELECT control_json, revision FROM managed_projects WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        if row is None:
            return empty_project_control(project_id), 0
        try:
            control = json.loads(row["control_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("managed project control is corrupt") from exc
        if not isinstance(control, dict):
            raise RuntimeError("managed project control is corrupt")
        if control.get("project_id") != project_id:
            raise RuntimeError(
                "managed project control invariant failed: "
                "PROJECT_CONTROL_INDEX_MISMATCH"
            )
        return control, int(row["revision"])

    @staticmethod
    def _known_sprints(connection: sqlite3.Connection, project_id: str) -> set[str]:
        return {
            str(row[0])
            for row in connection.execute(
                "SELECT sprint_id FROM managed_sprints WHERE project_id = ?",
                (project_id,),
            )
        }

    def _write_control(
        self,
        connection: sqlite3.Connection,
        control: dict[str, Any],
        revision: int,
    ) -> None:
        schema_issues = _schema_errors(
            control,
            "managed-project-control-v1.schema.json",
            issue_code="PROJECT_CONTROL_SCHEMA_INVALID",
        )
        if schema_issues:
            raise RuntimeError("managed project control schema validation failed")
        known = self._known_sprints(connection, str(control.get("project_id") or ""))
        issues = managed_project_control_invariant_issues(control, known)
        if issues:
            raise RuntimeError(
                "managed project control invariant failed: " + ",".join(issues)
            )
        connection.execute(
            """
            INSERT INTO managed_projects(project_id, control_json, revision)
            VALUES (?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                control_json = excluded.control_json,
                revision = excluded.revision
            """,
            (control["project_id"], _json_text(control), revision + 1),
        )

    def project_control(self, project_id: str) -> dict[str, Any]:
        # The branch-lease store deliberately shares this database.  The file
        # may therefore exist before the managed-import tables do, so file
        # existence is not a schema-initialization signal.
        if not self.database_path.exists():
            return empty_project_control(project_id)
        self._ensure_initialized()
        migration_time = datetime.now(timezone.utc).isoformat()

        def validate(
            connection: sqlite3.Connection, control: dict[str, Any]
        ) -> dict[str, Any]:
            schema_issues = _schema_errors(
                control,
                "managed-project-control-v1.schema.json",
                issue_code="PROJECT_CONTROL_SCHEMA_INVALID",
            )
            if schema_issues:
                raise RuntimeError("managed project control schema validation failed")
            issues = managed_project_control_invariant_issues(
                control, self._known_sprints(connection, project_id)
            )
            if issues:
                raise RuntimeError(
                    "managed project control invariant failed: " + ",".join(issues)
                )
            return deepcopy(control)

        with closing(self._connect()) as connection:
            control, _ = self._load_control(connection, project_id)
            candidate = deepcopy(control)
            needs_migration = self._reconcile_superseded_attempts(
                candidate, updated_at=migration_time
            )
            if not needs_migration:
                return validate(connection, control)

        # A legacy repair is the uncommon write path. Reload under the writer
        # reservation so a concurrent activation cannot be overwritten.
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            if self._reconcile_superseded_attempts(
                control,
                updated_at=migration_time,
                connection=connection,
                project_id=project_id,
            ):
                self._write_control(connection, control, revision)
            return validate(connection, control)

    def _validate_control_snapshot(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        control: Mapping[str, Any],
    ) -> None:
        issues: list[str] = []
        if control.get("project_id") != project_id:
            issues.append("PROJECT_CONTROL_INDEX_MISMATCH")
        if _schema_errors(
            control,
            "managed-project-control-v1.schema.json",
            issue_code="PROJECT_CONTROL_SCHEMA_INVALID",
        ):
            issues.append("PROJECT_CONTROL_SCHEMA_INVALID")
        else:
            issues.extend(
                managed_project_control_invariant_issues(
                    control, self._known_sprints(connection, project_id)
                )
            )
        if issues:
            raise RuntimeError(
                "managed project control invariant failed: "
                + ",".join(dict.fromkeys(issues))
            )

    @staticmethod
    def _runtime_row_issues(
        control: Mapping[str, Any],
        row: sqlite3.Row,
        state: Any,
        *,
        require_indexed: bool,
    ) -> tuple[str, ...]:
        issues: list[str] = []
        if not isinstance(state, Mapping):
            return ("RUNTIME_STATE_NOT_OBJECT",)
        row_project_id = row["project_id"]
        row_sprint_id = row["sprint_id"]
        row_status = row["status"]
        row_fence = row["fencing_token"]
        identity = state.get("identity")
        if (
            not isinstance(identity, Mapping)
            or identity.get("project_id") != row_project_id
        ):
            issues.append("RUNTIME_PROJECT_INDEX_MISMATCH")
        if state.get("sprint_id") != row_sprint_id:
            issues.append("RUNTIME_SPRINT_INDEX_MISMATCH")
        if state.get("status") != row_status:
            issues.append("RUNTIME_STATUS_INDEX_MISMATCH")
        if _runtime_state_schema_errors(
            state,
            issue_code="RUNTIME_STATE_SCHEMA_INVALID",
        ):
            issues.append("RUNTIME_STATE_SCHEMA_INVALID")
        if managed_activation_invariant_issues(state):
            issues.append("RUNTIME_STATE_INVARIANT_INVALID")
        if state.get("scope_control") is not None:
            from .legacy_scope_control import LegacyScopeControlError, validate_versioned_scope_history
            try:
                if not isinstance(state["scope_control"], Mapping) or state["scope_control"].get("schema_version") != 2:
                    issues.append("SCOPE_CONTROL_VERSION_UNSUPPORTED")
                else:
                    validate_versioned_scope_history(state["scope_control"])
            except (LegacyScopeControlError, TypeError, ValueError):
                issues.append("SCOPE_HISTORY_INVALID")

        records = control.get("start_idempotency_records")
        all_records = (
            [record for record in records if isinstance(record, Mapping)]
            if isinstance(records, list)
            else []
        )
        successful = (
            [
                record
                for record in all_records
                if record.get("status") == "SUCCEEDED"
            ]
        )
        matching_fenced = [
            record
            for record in all_records
            if record.get("sprint_id") == row_sprint_id
            and record.get("fencing_token") == row_fence
        ]
        workflow = state.get("workflow")
        preactivation = workflow is None and row_status in {"preparing", "failed"}
        matching_status = (
            matching_fenced[0].get("status")
            if len(matching_fenced) == 1
            else None
        )
        fence_owner_valid = bool(
            len(matching_fenced) == 1
            and isinstance(row_fence, int)
            and not isinstance(row_fence, bool)
            and (
                (
                    preactivation
                    and row_status == "preparing"
                    and matching_status in {"PREPARING", "ACTIVATING"}
                )
                or (
                    preactivation
                    and row_status == "failed"
                    and matching_status
                    in {"VALIDATING", "PREPARING", "ACTIVATING", "FAILED"}
                )
                or (not preactivation and matching_status == "SUCCEEDED")
            )
        )
        if not fence_owner_valid:
            issues.append("RUNTIME_ACTIVATION_FENCE_MISMATCH")
        active_sprint_id = control.get("active_sprint_id")
        if row_status == "active" and active_sprint_id != row_sprint_id:
            issues.append("ACTIVE_SPRINT_INDEX_MISMATCH")
        if require_indexed or row_status == "active":
            if active_sprint_id != row_sprint_id:
                issues.append("ACTIVE_SPRINT_INDEX_MISMATCH")
            if row_status not in {"active", "completed", "failed", "blocked"}:
                issues.append("ACTIVE_SPRINT_STATUS_INVALID")
            successful_fences = [
                record.get("fencing_token")
                for record in successful
                if isinstance(record.get("fencing_token"), int)
                and not isinstance(record.get("fencing_token"), bool)
            ]
            if (
                len(matching_fenced) == 1
                and successful_fences
                and matching_fenced[0].get("fencing_token")
                != max(successful_fences)
            ):
                issues.append("ACTIVE_SPRINT_INDEX_MISMATCH")
        return tuple(dict.fromkeys(issues))

    def _indexed_runtime_or_error(
        self,
        connection: sqlite3.Connection,
        control: Mapping[str, Any],
        project_id: str,
        *,
        correlation_id: str,
        phase: str,
    ) -> tuple[sqlite3.Row, dict[str, Any]] | None:
        active_sprint_id = control.get("active_sprint_id")
        if active_sprint_id is None:
            return None
        if not isinstance(active_sprint_id, str):
            issues = ("ACTIVE_SPRINT_INDEX_MISMATCH",)
        else:
            row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, active_sprint_id),
            ).fetchone()
            if row is not None:
                try:
                    state = json.loads(row["state_json"])
                except (TypeError, json.JSONDecodeError):
                    state = None
                issues = self._runtime_row_issues(
                    control, row, state, require_indexed=True
                )
                if not issues and isinstance(state, dict):
                    return row, state
            else:
                issues = ("ACTIVE_SPRINT_STATE_MISSING",)
        raise ManagedImportError(
            SPRINT_RECOVERY_REQUIRED,
            409,
            correlation_id,
            phase=phase,
            issues=[
                {
                    "code": "ACTIVE_SPRINT_STATE_CONFLICT",
                    "path": "active_sprint_id",
                    "message": "Indexed managed sprint state is inconsistent",
                }
            ],
            evidence={"runtime_invariant_issues": list(issues)},
        )

    def runtime_state(self, project_id: str, sprint_id: str) -> dict[str, Any] | None:
        if not self.database_path.exists():
            return None
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, sprint_id),
            ).fetchone()
            if row is None:
                return None
            control, _ = self._load_control(connection, project_id)
            self._validate_control_snapshot(connection, project_id, control)
            try:
                value = json.loads(row["state_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("managed sprint runtime is corrupt") from exc
            issues = self._runtime_row_issues(
                control, row, value, require_indexed=False
            )
            if issues:
                raise RuntimeError(
                    "managed sprint runtime invariant failed: " + ",".join(issues)
                )
            return deepcopy(value)

    def runtime_state_by_sprint(
        self, sprint_id: str
    ) -> tuple[str, dict[str, Any]] | None:
        """Resolve one globally unique managed sprint without trusting a caller path.

        Stable sprint IDs include their project provenance, but the repair route
        intentionally omits ``project_id``.  Refuse duplicate/corrupt rows
        instead of choosing one nondeterministically.
        """

        if not self.database_path.exists():
            return None
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints WHERE sprint_id = ? ORDER BY project_id
                """,
                (sprint_id,),
            ).fetchall()
            if not rows:
                return None
            if len(rows) != 1:
                raise RuntimeError("managed sprint identity is not globally unique")
            row = rows[0]
            project_id = str(row["project_id"])
            control, _ = self._load_control(connection, project_id)
            self._validate_control_snapshot(connection, project_id, control)
            try:
                state = json.loads(row["state_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("managed sprint runtime is corrupt") from exc
            issues = self._runtime_row_issues(
                control, row, state, require_indexed=False
            )
            if issues or not isinstance(state, dict):
                raise RuntimeError(
                    "managed sprint runtime invariant failed: "
                    + ",".join(issues or ("RUNTIME_STATE_NOT_OBJECT",))
                )
            return project_id, deepcopy(state)

    def runtime_states_for_project(
        self, project_id: str
    ) -> tuple[dict[str, Any], ...]:
        """Return every validated runtime generation for one project.

        Managed result/review replay is deliberately resolved from durable
        assignment history instead of only the current active assignment.  A
        late exact replay must therefore remain on the managed path after the
        sprint becomes terminal, while an unrelated legacy phone must still
        be allowed to fall through to the legacy dispatcher.
        """

        if not self.database_path.exists():
            return ()
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            control, _ = self._load_control(connection, project_id)
            self._validate_control_snapshot(connection, project_id, control)
            rows = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ?
                ORDER BY fencing_token DESC, sprint_id
                """,
                (project_id,),
            ).fetchall()
            states: list[dict[str, Any]] = []
            for row in rows:
                try:
                    state = json.loads(row["state_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    raise RuntimeError("managed sprint runtime is corrupt") from exc
                issues = self._runtime_row_issues(
                    control, row, state, require_indexed=False
                )
                if issues or not isinstance(state, dict):
                    raise RuntimeError(
                        "managed sprint runtime invariant failed: "
                        + ",".join(issues or ("RUNTIME_STATE_NOT_OBJECT",))
                    )
                states.append(deepcopy(state))
            return tuple(states)

    def runtime_binding_snapshots_for_project(
        self, project_id: str
    ) -> tuple[dict[str, Any], ...]:
        """Return snapshots or safe ownership hints for binding discovery.

        Project-specific ``whoami`` is also a frozen legacy endpoint.  A
        corrupt *unrelated* managed row must therefore not claim or fail that
        request before an assignment/phone match exists.  A parseable corrupt
        row still exposes only its assignment identifiers, phones, and live
        flags as a tagged hint.  Continuity uses a matching hint to fail
        closed, while unrelated legacy phones can still fall through.  Valid
        snapshots are re-read through ``runtime_state`` after a concrete
        binding is selected.
        """

        if not self.database_path.exists():
            return ()
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            control, _ = self._load_control(connection, project_id)
            self._validate_control_snapshot(connection, project_id, control)
            rows = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ?
                ORDER BY fencing_token DESC, sprint_id
                """,
                (project_id,),
            ).fetchall()
            states: list[dict[str, Any]] = []
            for row in rows:
                corrupt_shell = {
                    "__managed_binding_corrupt__": True,
                    "sprint_id": str(row["sprint_id"]),
                    "project_wide": True,
                    "ownership": [],
                }
                try:
                    state = json.loads(row["state_json"])
                except (TypeError, json.JSONDecodeError):
                    # There is no safe way to prove that an opaque managed row
                    # does *not* own the requested phone or assignment.  Never
                    # let it fall through to the legacy mutator.
                    states.append(corrupt_shell)
                    continue
                if not isinstance(state, dict):
                    states.append(corrupt_shell)
                    continue
                try:
                    issues = self._runtime_row_issues(
                        control, row, state, require_indexed=False
                    )
                except (KeyError, TypeError, ValueError, RuntimeError):
                    issues = ("RUNTIME_STATE_CORRUPT",)
                if not issues:
                    states.append(deepcopy(state))
                    continue

                evidence: dict[tuple[str, str], dict[str, Any]] = {}
                opaque_ownership = False

                def binding(kind: str, assignment_id: Any) -> dict[str, Any] | None:
                    if not isinstance(assignment_id, str) or not assignment_id:
                        return None
                    return evidence.setdefault(
                        (kind, assignment_id),
                        {
                            "kind": kind,
                            "assignment_id": assignment_id,
                            "graph_phones": set(),
                            "outbox_phones": set(),
                            "record_phones": set(),
                            "live": False,
                            "terminal": False,
                        },
                    )

                def add_phone(
                    owner: dict[str, Any] | None, source: str, phone: Any
                ) -> None:
                    if (
                        owner is not None
                        and isinstance(phone, str)
                        and phone.strip()
                    ):
                        owner[f"{source}_phones"].add(phone.strip())

                def graph_definition(revision_number: Any) -> Mapping[str, Any] | None:
                    definition = revision_definitions.get(revision_number)
                    return definition if isinstance(definition, Mapping) else None

                revisions = state.get("graph_revisions")
                revisions = revisions if isinstance(revisions, list) else []
                revision_definitions: dict[int, Mapping[str, Any]] = {}
                for revision in revisions:
                    if not isinstance(revision, Mapping):
                        continue
                    revision_number = revision.get("revision")
                    definition = revision.get("definition")
                    expected_digest = revision.get("definition_sha256")
                    if (
                        not isinstance(revision_number, int)
                        or isinstance(revision_number, bool)
                        or not isinstance(definition, Mapping)
                        or not isinstance(expected_digest, str)
                    ):
                        continue
                    try:
                        digest_matches = (
                            canonical_json_sha256(definition) == expected_digest
                        )
                    except (TypeError, ValueError):
                        digest_matches = False
                    if digest_matches:
                        revision_definitions[revision_number] = definition

                assignment_records: dict[str, Mapping[str, Any]] = {}
                assignments = state.get("assignments")
                if not isinstance(assignments, list):
                    opaque_ownership = True
                    assignments = []
                for record in assignments:
                    if not isinstance(record, Mapping):
                        opaque_ownership = True
                        continue
                    assignment_id = record.get("assignment_id")
                    owner = binding("assignment", assignment_id)
                    if owner is None:
                        opaque_ownership = True
                        continue
                    assignment_records[str(assignment_id)] = record
                    add_phone(owner, "record", record.get("agent_phone"))
                    if record.get("status") in {
                        "completed",
                        "failed",
                        "blocked",
                    }:
                        owner["terminal"] = True
                    else:
                        owner["live"] = True
                    definition = graph_definition(record.get("graph_revision"))
                    nodes = (
                        definition.get("nodes")
                        if isinstance(definition, Mapping)
                        else None
                    )
                    node = next(
                        (
                            item
                            for item in (nodes if isinstance(nodes, list) else [])
                            if isinstance(item, Mapping)
                            and item.get("id") == record.get("node_id")
                        ),
                        None,
                    )
                    agent = node.get("agent") if isinstance(node, Mapping) else None
                    if isinstance(agent, Mapping):
                        add_phone(owner, "graph", agent.get("phone"))

                active_assignment_ids = state.get("active_assignment_ids")
                if not isinstance(active_assignment_ids, list):
                    opaque_ownership = True
                    active_assignment_ids = []
                for assignment_id in active_assignment_ids:
                    owner = binding("assignment", assignment_id)
                    if owner is not None:
                        owner["live"] = True
                    else:
                        opaque_ownership = True
                allowed = state.get("allowed_outcomes_by_assignment")
                if isinstance(allowed, Mapping):
                    for assignment_id in allowed:
                        owner = binding("assignment", assignment_id)
                        if owner is not None:
                            owner["live"] = True
                        else:
                            opaque_ownership = True
                else:
                    opaque_ownership = True
                workflow = state.get("workflow")
                occurrences = (
                    workflow.get("occurrences")
                    if isinstance(workflow, Mapping)
                    else None
                )
                if isinstance(workflow, Mapping) and not isinstance(
                    occurrences, list
                ):
                    opaque_ownership = True
                for occurrence in (
                    occurrences if isinstance(occurrences, list) else []
                ):
                    if not isinstance(occurrence, Mapping):
                        opaque_ownership = True
                        continue
                    occurrence_live = occurrence.get("state") in {
                        "active",
                        "reviews_pending",
                    }
                    definition = graph_definition(
                        occurrence.get("graph_revision")
                    )
                    nodes = (
                        definition.get("nodes")
                        if isinstance(definition, Mapping)
                        else None
                    )
                    node = next(
                        (
                            item
                            for item in (nodes if isinstance(nodes, list) else [])
                            if isinstance(item, Mapping)
                            and item.get("id") == occurrence.get("node_id")
                        ),
                        None,
                    )
                    agent = node.get("agent") if isinstance(node, Mapping) else None
                    graph_phone = (
                        agent.get("phone") if isinstance(agent, Mapping) else None
                    )
                    occurrence_assignment_ids = occurrence.get("assignment_ids")
                    for assignment_id in (
                        occurrence_assignment_ids
                        if isinstance(occurrence_assignment_ids, list)
                        else []
                    ):
                        owner = binding("assignment", assignment_id)
                        if owner is not None:
                            add_phone(owner, "graph", graph_phone)
                            owner["live"] = owner["live"] or occurrence_live
                        else:
                            opaque_ownership = True

                raw_journals = state.get("transition_journal")
                if not isinstance(raw_journals, list):
                    opaque_ownership = True
                    raw_journals = []
                elif any(not isinstance(item, Mapping) for item in raw_journals):
                    opaque_ownership = True
                open_result_keys = {
                    str(journal["result_key"])
                    for journal in raw_journals
                    if isinstance(journal, Mapping)
                    and journal.get("state") == "REVIEWS_PENDING"
                    and journal.get("disposition") == "open"
                    and isinstance(journal.get("result_key"), str)
                }
                raw_reviews = state.get("reviews")
                if not isinstance(raw_reviews, list):
                    opaque_ownership = True
                    raw_reviews = []
                elif any(not isinstance(item, Mapping) for item in raw_reviews):
                    opaque_ownership = True
                decided_review_ids = {
                    str(review["assignment_id"])
                    for review in raw_reviews
                    if isinstance(review, Mapping)
                    and isinstance(review.get("assignment_id"), str)
                }
                review_records = state.get("review_assignments")
                if not isinstance(review_records, list):
                    opaque_ownership = True
                    review_records = []
                for record in review_records:
                    if not isinstance(record, Mapping):
                        opaque_ownership = True
                        continue
                    review_id = record.get("assignment_id")
                    owner = binding("review", review_id)
                    if owner is None:
                        opaque_ownership = True
                        continue
                    add_phone(owner, "record", record.get("reviewer_phone"))
                    if record.get("status") == "decided":
                        owner["terminal"] = True
                    else:
                        owner["live"] = True
                    owner["live"] = owner["live"] or (
                        review_id not in decided_review_ids
                        and record.get("result_key") in open_result_keys
                    )
                    source = assignment_records.get(
                        str(record.get("source_assignment_id") or "")
                    )
                    definition = (
                        graph_definition(source.get("graph_revision"))
                        if isinstance(source, Mapping)
                        else None
                    )
                    execution = (
                        definition.get("execution")
                        if isinstance(definition, Mapping)
                        else None
                    )
                    reviewers = (
                        execution.get("reviewers")
                        if isinstance(execution, Mapping)
                        else None
                    )
                    reviewers = reviewers if isinstance(reviewers, list) else []
                    reviewer = next(
                        (
                            item
                            for item in reviewers
                            if isinstance(item, Mapping)
                            and item.get("id") == record.get("reviewer_id")
                        ),
                        None,
                    )
                    if reviewer is None:
                        reviewer_index = record.get("reviewer_index")
                        if (
                            isinstance(reviewer_index, int)
                            and not isinstance(reviewer_index, bool)
                            and 1 <= reviewer_index <= len(reviewers)
                            and isinstance(reviewers[reviewer_index - 1], Mapping)
                        ):
                            reviewer = reviewers[reviewer_index - 1]
                    if isinstance(reviewer, Mapping):
                        add_phone(owner, "graph", reviewer.get("phone"))

                completed_context_ids = {
                    str(record["context_id"])
                    for record in (
                        state.get("recovery_records")
                        if isinstance(state.get("recovery_records"), list)
                        else []
                    )
                    if isinstance(record, Mapping)
                    and record.get("status") == "completed"
                    and isinstance(record.get("context_id"), str)
                }
                contexts = state.get("coordinator_contexts")
                if not isinstance(contexts, list):
                    opaque_ownership = True
                    contexts = []
                for context in contexts:
                    if not isinstance(context, Mapping):
                        opaque_ownership = True
                        continue
                    context_id = context.get("context_id")
                    owner = binding("coordinator", context_id)
                    if owner is None:
                        opaque_ownership = True
                        continue
                    if context_id in completed_context_ids:
                        owner["terminal"] = True
                    else:
                        owner["live"] = True
                    definition = graph_definition(context.get("graph_revision"))
                    coordinator = (
                        definition.get("coordinator")
                        if isinstance(definition, Mapping)
                        else None
                    )
                    nodes = (
                        definition.get("nodes")
                        if isinstance(definition, Mapping)
                        else None
                    )
                    coordinator_node_id = (
                        coordinator.get("node_id")
                        if isinstance(coordinator, Mapping)
                        else None
                    )
                    coordinator_node = next(
                        (
                            node
                            for node in (nodes if isinstance(nodes, list) else [])
                            if isinstance(node, Mapping)
                            and node.get("id") == coordinator_node_id
                        ),
                        None,
                    )
                    agent = (
                        coordinator_node.get("agent")
                        if isinstance(coordinator_node, Mapping)
                        else None
                    )
                    if isinstance(agent, Mapping):
                        add_phone(owner, "graph", agent.get("phone"))

                outbox = state.get("outbox")
                if not isinstance(outbox, list):
                    opaque_ownership = True
                    outbox = []
                for event in outbox:
                    payload = (
                        event.get("payload")
                        if isinstance(event, Mapping)
                        else None
                    )
                    if not isinstance(event, Mapping) or not isinstance(
                        payload, Mapping
                    ):
                        opaque_ownership = True
                        continue
                    event_type = event.get("event_type")
                    if event_type == "ASSIGNMENT_ENQUEUE":
                        owner = binding("assignment", payload.get("assignment_id"))
                        if owner is None:
                            opaque_ownership = True
                        add_phone(owner, "outbox", payload.get("agent_phone"))
                        if owner is not None and event.get("status") != "delivered":
                            owner["live"] = True
                    elif event_type == "REVIEW_ENQUEUE":
                        owner = binding(
                            "review", payload.get("review_assignment_id")
                        )
                        if owner is None:
                            opaque_ownership = True
                        add_phone(owner, "outbox", payload.get("reviewer_phone"))
                        if (
                            owner is not None
                            and (
                                event.get("status") != "delivered"
                                or (
                                    payload.get("review_assignment_id")
                                    not in decided_review_ids
                                    and payload.get("result_key")
                                    in open_result_keys
                                )
                            )
                        ):
                            owner["live"] = True
                    elif event_type == "COORDINATOR_ENQUEUE":
                        context_id = payload.get("context_id")
                        owner = binding("coordinator", context_id)
                        if owner is None:
                            opaque_ownership = True
                        add_phone(owner, "outbox", payload.get("coordinator_phone"))
                        if owner is not None:
                            if event.get("status") != "delivered":
                                owner["live"] = True
                            elif context_id in completed_context_ids:
                                owner["terminal"] = True
                            else:
                                owner["live"] = True
                    else:
                        opaque_ownership = True

                ownership: list[dict[str, Any]] = []
                project_wide = opaque_ownership
                for owner in evidence.values():
                    # A discovered owner is conservatively live until a known
                    # terminal record proves otherwise.  This covers a
                    # delivered outbox whose reciprocal owner record was lost:
                    # delivery alone cannot prove that assignment/review has
                    # already settled.
                    owner["live"] = bool(
                        owner["live"] or not owner.pop("terminal")
                    )
                    graph_phones = owner.pop("graph_phones")
                    outbox_phones = owner.pop("outbox_phones")
                    record_phones = owner.pop("record_phones")
                    phone: str | None = None
                    sources = (graph_phones, outbox_phones, record_phones)
                    singleton_phones = [
                        next(iter(candidates))
                        for candidates in sources
                        if len(candidates) == 1
                    ]
                    phone_counts = {
                        candidate: singleton_phones.count(candidate)
                        for candidate in singleton_phones
                    }
                    agreed = [
                        candidate
                        for candidate, count in phone_counts.items()
                        if count >= 2
                    ]
                    ambiguous_source = any(len(candidates) > 1 for candidates in sources)
                    if len(agreed) == 1:
                        phone = agreed[0]
                    elif len(singleton_phones) == 1 and not ambiguous_source:
                        phone = singleton_phones[0]
                    elif singleton_phones or ambiguous_source:
                        # Two independent projections disagree and neither has
                        # a majority.  Selecting either phone could route a
                        # managed request into legacy state, so widen the
                        # corruption fence for this project.
                        project_wide = True
                    if owner["live"] and phone is None:
                        project_wide = True
                    ownership.append({**owner, "phone": phone})

                if not ownership or (
                    (row["status"] == "active" or state.get("status") == "active")
                    and not any(item["live"] for item in ownership)
                ):
                    project_wide = True
                states.append(
                    {
                        "__managed_binding_corrupt__": True,
                        "sprint_id": str(row["sprint_id"]),
                        "project_wide": project_wide,
                        "ownership": ownership,
                    }
                )
            return tuple(states)

    def retryable_import_attempts(self) -> tuple[tuple[str, str, str], ...]:
        """List validated preactivation rows that own one running import.

        This read model lets startup reconciliation close the crash window
        after ``begin_retry_import`` without treating active assignments as
        the only discoverable work.  Each tuple is ``(project_id, sprint_id,
        attempt_id)`` and is safe to pass to ``resume_import_attempt``.
        """

        if not self.database_path.exists():
            return ()
        self._ensure_initialized()
        migration_time = datetime.now(timezone.utc).isoformat()
        with self._transaction() as connection:
            project_ids = [
                str(row["project_id"])
                for row in connection.execute(
                    """
                    SELECT DISTINCT project_id FROM managed_sprints
                    WHERE status IN ('preparing', 'failed')
                    ORDER BY project_id
                    """
                ).fetchall()
            ]
            for project_id in project_ids:
                control, revision = self._load_control(connection, project_id)
                if self._reconcile_superseded_attempts(
                    control,
                    updated_at=migration_time,
                    connection=connection,
                    project_id=project_id,
                ):
                    self._write_control(connection, control, revision)
            rows = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE status IN ('preparing', 'failed')
                ORDER BY project_id, fencing_token, sprint_id
                """
            ).fetchall()
            controls: dict[str, dict[str, Any]] = {}
            result: list[tuple[str, str, str]] = []
            for row in rows:
                project_id = str(row["project_id"])
                control = controls.get(project_id)
                if control is None:
                    control, _ = self._load_control(connection, project_id)
                    self._validate_control_snapshot(
                        connection, project_id, control
                    )
                    controls[project_id] = control
                try:
                    state = json.loads(row["state_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    raise RuntimeError(
                        "managed sprint runtime is corrupt"
                    ) from exc
                issues = self._runtime_row_issues(
                    control, row, state, require_indexed=False
                )
                if issues or not isinstance(state, Mapping):
                    raise RuntimeError(
                        "managed sprint runtime invariant failed: "
                        + ",".join(issues or ("RUNTIME_STATE_NOT_OBJECT",))
                    )
                running_ids = [
                    str(item["attempt_id"])
                    for item in state.get("import_attempts", [])
                    if isinstance(item, Mapping)
                    and item.get("status") == "running"
                    and isinstance(item.get("attempt_id"), str)
                ]
                owner = next(
                    (
                        item
                        for item in control.get(
                            "start_idempotency_records", []
                        )
                        if isinstance(item, Mapping)
                        and item.get("sprint_id") == row["sprint_id"]
                        and item.get("fencing_token") == row["fencing_token"]
                        and item.get("status")
                        in {"VALIDATING", "PREPARING", "ACTIVATING"}
                    ),
                    None,
                )
                if owner is None:
                    if running_ids:
                        raise RuntimeError(
                            "managed running import has no fenced owner"
                        )
                    continue
                attempt_id = str(owner["attempt_id"])
                if running_ids != [attempt_id]:
                    raise RuntimeError(
                        "managed preactivation running attempt is ambiguous"
                    )
                recovery = next(
                    (
                        item
                        for item in state.get("recovery_records", [])
                        if isinstance(item, Mapping)
                        and item.get("action") == "RETRY_IMPORT"
                        and item.get("status") == "completed"
                        and item.get("produced_record_ids") == [attempt_id]
                    ),
                    None,
                )
                parameters = (
                    recovery.get("parameters")
                    if isinstance(recovery, Mapping)
                    else None
                )
                if (
                    not isinstance(owner.get("recovery_of_attempt_id"), str)
                    or not isinstance(recovery, Mapping)
                    or not isinstance(parameters, Mapping)
                    or parameters.get("recovery_idempotency_key")
                    != owner.get("idempotency_key")
                    or parameters.get("failed_attempt_id")
                    != owner.get("recovery_of_attempt_id")
                ):
                    # Ordinary initial PREPARING starts remain driven by their
                    # exact caller.  Only an atomically completed retry effect
                    # is safe for autonomous recovery.
                    continue
                result.append(
                    (project_id, str(row["sprint_id"]), attempt_id)
                )
            return tuple(result)

    def active_runtime_state(self, project_id: str) -> dict[str, Any] | None:
        """Return the validated currently indexed runtime, if one exists."""

        if not self.database_path.exists():
            return None
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            control, _ = self._load_control(connection, project_id)
            self._validate_control_snapshot(connection, project_id, control)
            active_sprint_id = control.get("active_sprint_id")
            if not isinstance(active_sprint_id, str):
                return None
            row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, active_sprint_id),
            ).fetchone()
            if row is None:
                raise RuntimeError("indexed managed sprint runtime is missing")
            try:
                state = json.loads(row["state_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("managed sprint runtime is corrupt") from exc
            issues = self._runtime_row_issues(
                control, row, state, require_indexed=True
            )
            if issues or not isinstance(state, dict):
                raise RuntimeError(
                    "managed sprint runtime invariant failed: "
                    + ",".join(issues or ("RUNTIME_STATE_NOT_OBJECT",))
                )
            return deepcopy(state)

    def mutate_runtime_state(
        self,
        project_id: str,
        sprint_id: str,
        mutator: Callable[[dict[str, Any], sqlite3.Connection], Any],
        *,
        require_active: bool = True,
    ) -> tuple[dict[str, Any], Any]:
        """Apply one validated continuity mutation under ``BEGIN IMMEDIATE``.

        The callback may update normalized ownership tables through the supplied
        connection.  Runtime JSON, its indexed status, and those normalized
        rows therefore commit or roll back together.  Every write revalidates
        both JSON Schema and the complete relational contract before it becomes
        visible.
        """

        with self._transaction() as connection:
            control, _ = self._load_control(connection, project_id)
            self._validate_control_snapshot(connection, project_id, control)
            row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, sprint_id),
            ).fetchone()
            if row is None:
                raise KeyError("managed sprint runtime was not found")
            try:
                current = json.loads(row["state_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("managed sprint runtime is corrupt") from exc
            current_issues = self._runtime_row_issues(
                control, row, current, require_indexed=False
            )
            if current_issues or not isinstance(current, dict):
                raise RuntimeError(
                    "managed sprint runtime invariant failed: "
                    + ",".join(current_issues or ("RUNTIME_STATE_NOT_OBJECT",))
                )
            if require_active and current.get("status") != "active":
                raise RuntimeError("managed sprint is not active")

            candidate = deepcopy(current)
            result = mutator(candidate, connection)
            if candidate != current:
                from .scope_runtime_adapters import bind_new_assignments, link_new_execution_records
                from .execution_observability import append_execution_checkpoint
                from .scope_workflow import request_list
                bind_new_assignments(candidate)
                link_new_execution_records(candidate, current)
                # This is an execution/audit cursor, never a graph-definition
                # revision. Scope callbacks may already have advanced it.
                before_execution = {k: v for k, v in current.items() if k not in {"scope_workflow", "execution_audit"}}
                after_execution = {k: v for k, v in candidate.items() if k not in {"scope_workflow", "execution_audit"}}
                if before_execution != after_execution:
                    candidate["execution_revision"] = max(int(current.get("execution_revision") or 0) + 1, int(candidate.get("execution_revision") or 0))
                append_execution_checkpoint(candidate.setdefault("execution_audit", {}),
                    {"project_id": project_id, "sprint_id": sprint_id, "execution": candidate},
                    recorded_at=str(candidate.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                    scope_requests=request_list(candidate)["requests"])
            schema_issues = _runtime_state_schema_errors(
                candidate,
                issue_code="RUNTIME_STATE_SCHEMA_INVALID",
            )
            invariant_issues = managed_activation_invariant_issues(candidate)
            if schema_issues or invariant_issues:
                issue_codes = [
                    str(issue.get("code") or "RUNTIME_STATE_SCHEMA_INVALID")
                    for issue in schema_issues
                ] + list(invariant_issues)
                raise RuntimeError(
                    "managed sprint runtime mutation is invalid: "
                    + ",".join(dict.fromkeys(issue_codes))
                )
            candidate_status = candidate.get("status")
            if candidate_status not in {"active", "completed", "failed", "blocked"}:
                raise RuntimeError("managed sprint runtime status is invalid")
            encoded = _json_text(candidate)
            cursor = connection.execute(
                """
                UPDATE managed_sprints
                SET status = ?, state_json = ?
                WHERE project_id = ? AND sprint_id = ?
                  AND fencing_token = ? AND state_json = ?
                """,
                (
                    candidate_status,
                    encoded,
                    project_id,
                    sprint_id,
                    row["fencing_token"],
                    row["state_json"],
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("managed sprint runtime changed concurrently")
            updated = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, sprint_id),
            ).fetchone()
            if updated is None:
                raise RuntimeError("managed sprint runtime disappeared")
            updated_issues = self._runtime_row_issues(
                control, updated, candidate, require_indexed=False
            )
            if updated_issues:
                raise RuntimeError(
                    "managed sprint runtime index mutation is invalid: "
                    + ",".join(updated_issues)
                )
            return deepcopy(candidate), deepcopy(result)

    def enqueue_managed_queue_item(
        self,
        *,
        dedupe_key: str,
        event_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        created_at: str,
    ) -> str:
        """Insert a durable queue item or return its immutable prior receipt."""

        if not all(
            isinstance(value, str) and value
            for value in (dedupe_key, event_id, event_type, created_at)
        ):
            raise ValueError("managed queue identity fields must be non-empty strings")
        with self._transaction() as connection:
            return self.enqueue_managed_queue_item_in_transaction(
                connection,
                dedupe_key=dedupe_key,
                event_id=event_id,
                event_type=event_type,
                payload=payload,
                created_at=created_at,
            )

    @staticmethod
    def enqueue_managed_queue_item_in_transaction(
        connection: sqlite3.Connection,
        *,
        dedupe_key: str,
        event_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        created_at: str,
    ) -> str:
        """Insert/verify a queue fence inside an aggregate transaction."""

        if not all(
            isinstance(value, str) and value
            for value in (dedupe_key, event_id, event_type, created_at)
        ):
            raise ValueError("managed queue identity fields must be non-empty strings")
        payload_text = _json_text(dict(payload))
        receipt_id = _safe_identifier("queue-receipt", dedupe_key)
        row = connection.execute(
            "SELECT * FROM managed_queue_items WHERE dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        if row is not None:
            if (
                row["event_id"] != event_id
                or row["event_type"] != event_type
                or row["payload_json"] != payload_text
                or row["receipt_id"] != receipt_id
            ):
                raise RuntimeError("managed queue dedupe key payload conflict")
            return str(row["receipt_id"])
        try:
            connection.execute(
                """
                INSERT INTO managed_queue_items(
                    dedupe_key, event_id, event_type, payload_json,
                    receipt_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    dedupe_key,
                    event_id,
                    event_type,
                    payload_text,
                    receipt_id,
                    created_at,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise RuntimeError("managed queue identity conflict") from exc
        return receipt_id

    def managed_queue_item(self, dedupe_key: str) -> dict[str, Any] | None:
        if not self.database_path.exists():
            return None
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM managed_queue_items WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "dedupe_key": str(row["dedupe_key"]),
            "event_id": str(row["event_id"]),
            "event_type": str(row["event_type"]),
            "payload": json.loads(row["payload_json"]),
            "receipt_id": str(row["receipt_id"]),
            "created_at": str(row["created_at"]),
        }

    def active_runtime_states(self) -> tuple[dict[str, Any], ...]:
        """Return every active managed runtime for startup reconciliation."""

        if not self.database_path.exists():
            return ()
        self._ensure_initialized()
        migration_time = datetime.now(timezone.utc).isoformat()
        with self._transaction() as connection:
            project_ids = [
                str(row["project_id"])
                for row in connection.execute(
                    """
                    SELECT DISTINCT project_id FROM managed_sprints
                    WHERE status = 'active' ORDER BY project_id
                    """
                )
            ]
            for project_id in project_ids:
                control, revision = self._load_control(connection, project_id)
                if self._reconcile_superseded_attempts(
                    control,
                    updated_at=migration_time,
                    connection=connection,
                    project_id=project_id,
                ):
                    self._write_control(connection, control, revision)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN")
            rows = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE status = 'active'
                ORDER BY project_id, sprint_id
                """
            ).fetchall()
            duplicate = connection.execute(
                """
                SELECT project_id FROM managed_sprints
                WHERE status = 'active'
                GROUP BY project_id HAVING COUNT(*) > 1
                LIMIT 1
                """
            ).fetchone()
            if duplicate is not None:
                raise RuntimeError(
                    "managed sprint runtime invariant failed: MULTIPLE_ACTIVE_SPRINTS"
                )
            states: list[dict[str, Any]] = []
            for row in rows:
                project_id = str(row["project_id"])
                control, _ = self._load_control(connection, project_id)
                self._validate_control_snapshot(connection, project_id, control)
                try:
                    state = json.loads(row["state_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    raise RuntimeError("managed sprint runtime is corrupt") from exc
                issues = self._runtime_row_issues(
                    control, row, state, require_indexed=True
                )
                if issues or not isinstance(state, dict):
                    raise RuntimeError(
                        "managed sprint runtime invariant failed: "
                        + ",".join(issues or ("RUNTIME_STATE_NOT_OBJECT",))
                    )
                states.append(deepcopy(state))
            connection.commit()
            return tuple(states)

    def superseded_pending_recovery_states(
        self,
    ) -> tuple[tuple[str, str], ...]:
        """List runtimes with a pending recovery behind a settled choice.

        Older writers could durably persist two choices for one Coordinator
        context.  Terminal and failed runtimes are not part of the ordinary
        active scheduler scan, so expose only this narrow repair work for
        startup reconciliation.  The later mutation path performs the full
        schema, relational, and row-fence validation before changing data.
        """

        if not self.database_path.exists():
            return ()
        self._ensure_initialized()
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT project_id, sprint_id, state_json
                FROM managed_sprints
                ORDER BY project_id, sprint_id
                """
            ).fetchall()
        candidates: list[tuple[str, str]] = []
        for row in rows:
            try:
                state = json.loads(row["state_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(state, Mapping):
                continue
            recoveries = [
                recovery
                for recovery in state.get("recovery_records", [])
                if isinstance(recovery, Mapping)
            ]
            rework_limit_assignment_ids = {
                str(context["failed_assignment_id"])
                for context in state.get("coordinator_contexts", [])
                if isinstance(context, Mapping)
                and context.get("reason_code") == "REWORK_LIMIT_EXCEEDED"
                and isinstance(context.get("failed_assignment_id"), str)
            }
            settled_context_ids = {
                str(recovery["context_id"])
                for recovery in recoveries
                if isinstance(recovery.get("context_id"), str)
                and (
                    recovery.get("status") == "completed"
                    or (
                        recovery.get("status") == "failed"
                        and recovery.get("action") == "ROUTE_REWORK"
                        and isinstance(
                            recovery.get("normalized_error"), Mapping
                        )
                        and recovery["normalized_error"].get("code")
                        == "REWORK_LIMIT_EXCEEDED"
                        and recovery.get("assignment_id")
                        in rework_limit_assignment_ids
                    )
                )
            }
            if any(
                recovery.get("status") == "pending"
                and recovery.get("context_id") in settled_context_ids
                for recovery in recoveries
            ):
                candidates.append(
                    (str(row["project_id"]), str(row["sprint_id"]))
                )
        return tuple(candidates)

    def lookup(self, project_id: str, key: str) -> dict[str, Any] | None:
        control = self.project_control(project_id)
        record = self._record(control, key)
        return deepcopy(record) if record is not None else None

    def lookup_attempt(
        self, project_id: str, attempt_id: str
    ) -> dict[str, Any] | None:
        """Return one immutable/control import attempt by its internal ID."""

        control = self.project_control(project_id)
        record = self._record_by_attempt(control, attempt_id)
        return deepcopy(record) if record is not None else None

    @staticmethod
    def _lease_live(lease: Any, now: datetime) -> bool:
        if not isinstance(lease, Mapping):
            return False
        expires_at = _parse_timestamp(lease.get("expires_at"))
        return expires_at is not None and expires_at > now

    def _reconcile_superseded_attempts(
        self,
        control: dict[str, Any],
        *,
        updated_at: str,
        connection: sqlite3.Connection | None = None,
        project_id: str | None = None,
    ) -> bool:
        """Migrate/fail stale pre-fix attempts before strict invariant reads."""

        records = control.get("start_idempotency_records")
        if not isinstance(records, list):
            raise RuntimeError("managed project control is corrupt")
        successful = [
            record
            for record in records
            if isinstance(record, Mapping)
            and record.get("status") == "SUCCEEDED"
            and isinstance(record.get("fencing_token"), int)
            and not isinstance(record.get("fencing_token"), bool)
        ]
        winner = (
            max(successful, key=lambda record: int(record["fencing_token"]))
            if successful
            else None
        )
        winner_token = int(winner["fencing_token"]) if winner is not None else None
        changed = False
        for record in records:
            if not isinstance(record, dict):
                continue
            if record.get("status") == "FAILED":
                evidence = record.get("evidence")
                if (
                    isinstance(evidence, Mapping)
                    and evidence.get("failure_code") == "ATTEMPT_SUPERSEDED"
                ):
                    if connection is None or project_id is None:
                        # A read pass cannot inspect the runtime projection;
                        # force the uncommon writer pass, which is idempotent.
                        changed = True
                    elif self._terminalize_superseded_runtime(
                        connection,
                        project_id,
                        control,
                        record,
                        updated_at=updated_at,
                    ):
                        changed = True
                continue
            if record.get("status") not in {
                "VALIDATING",
                "PREPARING",
                "ACTIVATING",
            }:
                continue
            created_token = record.get("created_fencing_token")
            current_token = record.get("fencing_token")
            if winner is None:
                if (
                    created_token is None
                    and isinstance(current_token, int)
                    and not isinstance(current_token, bool)
                ):
                    record["created_fencing_token"] = current_token
                    changed = True
                continue
            if (
                not isinstance(created_token, int)
                or isinstance(created_token, bool)
                or created_token <= int(winner_token)
            ):
                self._supersede_attempt(
                    record,
                    superseding_attempt_id=str(winner["attempt_id"]),
                    superseding_sprint_id=str(winner["sprint_id"]),
                    superseding_fencing_token=int(winner_token),
                    updated_at=updated_at,
                )
                if connection is not None and project_id is not None:
                    self._terminalize_superseded_runtime(
                        connection,
                        project_id,
                        control,
                        record,
                        updated_at=updated_at,
                    )
                lease = control.get("activation_lease")
                if (
                    isinstance(lease, Mapping)
                    and lease.get("attempt_id") == record.get("attempt_id")
                ):
                    control["activation_lease"] = None
                changed = True
        return changed

    @staticmethod
    def _supersede_attempt(
        record: dict[str, Any],
        *,
        superseding_attempt_id: str,
        superseding_sprint_id: str,
        superseding_fencing_token: int,
        updated_at: str,
    ) -> None:
        attempt_id = str(record.get("attempt_id") or "superseded-attempt")
        error = ManagedImportError(
            SPRINT_RECOVERY_REQUIRED,
            409,
            attempt_id,
            phase="ACTIVATE",
            issues=[
                {
                    "code": "ATTEMPT_SUPERSEDED",
                    "path": "activation.fencing_token",
                    "message": "A newer managed sprint activation superseded this attempt",
                }
            ],
        )
        evidence = (
            deepcopy(record.get("evidence"))
            if isinstance(record.get("evidence"), Mapping)
            else {}
        )
        evidence.update(
            {
                "phase": "ACTIVATE",
                "failure_code": "ATTEMPT_SUPERSEDED",
                "superseding_attempt_id": superseding_attempt_id,
                "superseding_sprint_id": superseding_sprint_id,
                "superseding_fencing_token": superseding_fencing_token,
            }
        )
        record.update(
            {
                "status": "FAILED",
                "response": None,
                "http_status": 409,
                "error": deepcopy(error.envelope),
                "evidence": evidence,
                "updated_at": updated_at,
            }
        )

    def _terminalize_superseded_runtime(
        self,
        connection: sqlite3.Connection,
        project_id: str,
        control: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        updated_at: str,
    ) -> bool:
        """Atomically mirror a superseded control child into its shell."""

        sprint_id = record.get("sprint_id")
        attempt_id = record.get("attempt_id")
        fencing_token = record.get("fencing_token")
        if not isinstance(sprint_id, str) or not isinstance(attempt_id, str):
            return False
        row = connection.execute(
            """
            SELECT project_id, sprint_id, status, fencing_token, state_json
            FROM managed_sprints
            WHERE project_id = ? AND sprint_id = ?
            """,
            (project_id, sprint_id),
        ).fetchone()
        if row is None:
            return False
        try:
            state = json.loads(row["state_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("managed sprint runtime is corrupt") from exc
        if not isinstance(state, dict):
            raise RuntimeError("managed superseded runtime is not an object")
        runtime_attempt = next(
            (
                item
                for item in state.get("import_attempts", [])
                if isinstance(item, dict)
                and item.get("attempt_id") == attempt_id
            ),
            None,
        )
        if runtime_attempt is None:
            raise RuntimeError("managed superseded runtime owner is missing")
        runtime_status = runtime_attempt.get("status")
        if runtime_status not in {"running", "failed"}:
            raise RuntimeError("managed superseded import attempt is corrupt")

        if state.get("workflow") is not None:
            if runtime_status != "failed":
                raise RuntimeError(
                    "managed superseded runtime has live work after activation"
                )
            issues = self._runtime_row_issues(
                control, row, state, require_indexed=False
            )
            if issues:
                raise RuntimeError(
                    "managed superseded runtime is inconsistent: "
                    + ",".join(issues)
                )
            return False

        row_status = row["status"]
        row_fence = row["fencing_token"]
        running_attempt_ids = [
            item.get("attempt_id")
            for item in state.get("import_attempts", [])
            if isinstance(item, Mapping) and item.get("status") == "running"
        ]
        control_repaired = False
        if row_fence != fencing_token:
            records = control.get("start_idempotency_records")
            row_fence_claimed_elsewhere = any(
                isinstance(item, Mapping)
                and item is not record
                and item.get("fencing_token") == row_fence
                for item in (records if isinstance(records, list) else [])
            )
            can_adopt_legacy_fence = bool(
                isinstance(record, dict)
                and record.get("created_fencing_token") is None
                and (
                    (
                        runtime_status == "running"
                        and running_attempt_ids == [attempt_id]
                    )
                    or (
                        runtime_status == "failed"
                        and not running_attempt_ids
                        and row_status == "failed"
                        and state.get("status") == "failed"
                    )
                )
                and isinstance(row_fence, int)
                and not isinstance(row_fence, bool)
                and 0 < row_fence
                <= int(control.get("activation_fencing_counter") or 0)
                and not row_fence_claimed_elsewhere
            )
            if not can_adopt_legacy_fence:
                raise RuntimeError("managed superseded runtime fence changed")
            # Pre-creation-fence records could be re-fenced in control while
            # their durable shell retained the original token.  Exact attempt
            # ownership proves this is the older shell, so migrate control to
            # its token rather than overwriting it with a newer token.
            record["created_fencing_token"] = row_fence
            record["fencing_token"] = row_fence
            fencing_token = row_fence
            control_repaired = True
        if (
            row_status not in {"preparing", "failed"}
            or state.get("status") != row_status
            or not isinstance(fencing_token, int)
            or isinstance(fencing_token, bool)
        ):
            raise RuntimeError("managed superseded runtime fence changed")
        if runtime_status == "running" and running_attempt_ids != [attempt_id]:
            raise RuntimeError("managed superseded runtime owner is ambiguous")
        if runtime_status == "failed" and running_attempt_ids:
            raise RuntimeError("managed superseded runtime still has a live owner")

        reason_code = SPRINT_RECOVERY_REQUIRED
        context_id = _safe_identifier(
            "context", sprint_id, attempt_id, reason_code
        )
        normalized_error = {
            "code": reason_code,
            "http_status": 409,
            "phase": "ACTIVATE",
            "issue_codes": ["ATTEMPT_SUPERSEDED"],
        }
        context = {
            "context_id": context_id,
            "graph_revision": int(state["graph_revision"]),
            "failure_scope": "activate",
            "reason_code": reason_code,
            "import_attempt_id": attempt_id,
            "failed_assignment_id": None,
            "join_target": None,
            "assigned_branch": None,
            "source_commit": None,
            "result_commit": None,
            "diff_summary": {},
            "workspace_status": {
                "prepared_artifact_ids": deepcopy(
                    runtime_attempt.get("prepared_artifact_ids", [])
                )
            },
            "process_records": [],
            "port_records": [],
            "test_evidence_summary": {},
            "normalized_error": normalized_error,
            "reviewer_feedback": [],
        }
        existing_context = next(
            (
                item
                for item in state.get("coordinator_contexts", [])
                if isinstance(item, Mapping)
                and item.get("context_id") == context_id
            ),
            None,
        )
        event_id = _safe_identifier("event", sprint_id, context_id)
        dedupe_key = f"enqueue:coordinator:{sprint_id}:{context_id}"
        payload = {
            "sprint_id": sprint_id,
            "context_id": context_id,
            "coordinator_phone": self._coordinator_phone(state),
            "reason_code": reason_code,
        }
        expected_receipt_id = _safe_identifier("queue-receipt", dedupe_key)
        existing_event = next(
            (
                item
                for item in state.get("outbox", [])
                if isinstance(item, Mapping)
                and item.get("event_id") == event_id
            ),
            None,
        )
        queue_row = connection.execute(
            "SELECT * FROM managed_queue_items WHERE dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        projection_complete = bool(
            runtime_status == "failed"
            and row_status == "failed"
            and isinstance(existing_context, Mapping)
            and dict(existing_context) == context
            and isinstance(existing_event, Mapping)
            and existing_event.get("dedupe_key") == dedupe_key
            and existing_event.get("event_type") == "COORDINATOR_ENQUEUE"
            and existing_event.get("payload") == payload
            and existing_event.get("status") == "delivered"
            and existing_event.get("queue_receipt_id") == expected_receipt_id
            and isinstance(existing_event.get("created_at"), str)
            and isinstance(existing_event.get("delivered_at"), str)
            and queue_row is not None
            and queue_row["event_id"] == event_id
            and queue_row["event_type"] == "COORDINATOR_ENQUEUE"
            and queue_row["payload_json"] == _json_text(payload)
            and queue_row["receipt_id"] == expected_receipt_id
        )
        if runtime_status == "failed":
            issues = self._runtime_row_issues(
                control, row, state, require_indexed=False
            )
            if projection_complete and not issues:
                return control_repaired
            raise RuntimeError(
                "managed superseded runtime terminal projection is incomplete"
            )

        runtime_attempt.update(
            {
                "phase": "ACTIVATE",
                "status": "failed",
                "activation_response": None,
                "updated_at": updated_at,
            }
        )
        if existing_context is None:
            state["coordinator_contexts"].append(context)
        elif dict(existing_context) != context:
            raise RuntimeError("managed supersession context identity conflict")

        receipt_id = self.enqueue_managed_queue_item_in_transaction(
            connection,
            dedupe_key=dedupe_key,
            event_id=event_id,
            event_type="COORDINATOR_ENQUEUE",
            payload=payload,
            created_at=updated_at,
        )
        event = {
            "event_id": event_id,
            "dedupe_key": dedupe_key,
            "event_type": "COORDINATOR_ENQUEUE",
            "payload": payload,
            "status": "delivered",
            "created_at": updated_at,
            "delivered_at": updated_at,
            "queue_receipt_id": receipt_id,
        }
        if existing_event is None:
            state["outbox"].append(event)
        elif dict(existing_event) != event:
            raise RuntimeError("managed supersession event identity conflict")

        state["status"] = "failed"
        schema_issues = _runtime_state_schema_errors(
            state,
            issue_code="IMPORT_SUPERSESSION_SCHEMA_INVALID",
        )
        invariant_issues = managed_activation_invariant_issues(state)
        if schema_issues or invariant_issues:
            issue_codes = [
                str(item.get("code")) for item in schema_issues
            ] + list(invariant_issues)
            raise RuntimeError(
                "managed superseded runtime is invalid: "
                + ",".join(dict.fromkeys(issue_codes))
            )
        cursor = connection.execute(
            """
            UPDATE managed_sprints
            SET status = 'failed', fencing_token = ?, state_json = ?
            WHERE project_id = ? AND sprint_id = ?
              AND status = ? AND fencing_token = ? AND state_json = ?
            """,
            (
                fencing_token,
                _json_text(state),
                project_id,
                sprint_id,
                row_status,
                row_fence,
                row["state_json"],
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("managed superseded runtime changed concurrently")
        updated = connection.execute(
            """
            SELECT project_id, sprint_id, status, fencing_token, state_json
            FROM managed_sprints
            WHERE project_id = ? AND sprint_id = ?
            """,
            (project_id, sprint_id),
        ).fetchone()
        if updated is None:
            raise RuntimeError("managed superseded runtime disappeared")
        updated_issues = self._runtime_row_issues(
            control, updated, state, require_indexed=False
        )
        if updated_issues:
            raise RuntimeError(
                "managed superseded runtime index mutation is invalid: "
                + ",".join(updated_issues)
            )
        return True

    def register_or_resume(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
        provenance: SprintProvenance | None,
        workspace_source_commit: str | None,
        *,
        repository_binding: Mapping[str, Any] | None = None,
        clock: Callable[[], datetime],
        attempt_id_factory: Callable[[], str],
    ) -> tuple[dict[str, Any], bool]:
        """Create the request intent or fence/resume the existing exact call."""

        fingerprint = request.request_fingerprint(project_id)
        now = _utc_now(clock)
        now_text = now.isoformat()
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            reconciled = self._reconcile_superseded_attempts(
                control,
                updated_at=now_text,
                connection=connection,
                project_id=project_id,
            )
            existing = self._record(control, request.idempotency_key)
            if existing is not None:
                if existing.get("request_fingerprint") != fingerprint:
                    raise ManagedImportError(
                        IDEMPOTENCY_KEY_CONFLICT,
                        409,
                        str(existing.get("attempt_id") or "idempotency-conflict"),
                        phase="VALIDATE",
                    )
                if existing.get("status") in {"SUCCEEDED", "FAILED"}:
                    if reconciled:
                        self._write_control(connection, control, revision)
                    return deepcopy(existing), True
                pinned = existing.get("pinned_identity")
                if (
                    provenance is not None
                    and isinstance(pinned, Mapping)
                    and dict(pinned) != asdict(provenance)
                ):
                    # The first caller to pin a mutable ref wins.  The caller
                    # resumes that attempt instead of rebinding the same key.
                    if reconciled:
                        self._write_control(connection, control, revision)
                    return deepcopy(existing), True
                lease = control.get("activation_lease")
                if (
                    self._lease_live(lease, now)
                    and isinstance(lease, Mapping)
                    and lease.get("attempt_id") != existing.get("attempt_id")
                ):
                    raise ManagedImportError(
                        PROJECT_ACTIVATION_IN_PROGRESS,
                        409,
                        str(existing.get("attempt_id") or "activation-busy"),
                        phase="VALIDATE",
                    )
                if (
                    self._lease_live(lease, now)
                    and isinstance(lease, Mapping)
                    and lease.get("attempt_id") == existing.get("attempt_id")
                ):
                    if reconciled:
                        self._write_control(connection, control, revision)
                    return deepcopy(existing), True
                counter = int(control.get("activation_fencing_counter") or 0) + 1
                control["activation_fencing_counter"] = counter
                self._rebind_preactivation_row_fence(
                    connection,
                    project_id,
                    existing.get("sprint_id"),
                    old_fencing_token=existing.get("fencing_token"),
                    new_fencing_token=counter,
                )
                existing["fencing_token"] = counter
                existing["updated_at"] = now_text
                control["activation_lease"] = {
                    "attempt_id": existing["attempt_id"],
                    "fencing_token": counter,
                    "acquired_at": now_text,
                    "expires_at": (now + timedelta(seconds=_LEASE_SECONDS)).isoformat(),
                }
                self._write_control(connection, control, revision)
                return deepcopy(existing), True

            lease = control.get("activation_lease")
            if self._lease_live(lease, now):
                correlation = (
                    str(lease.get("attempt_id"))
                    if isinstance(lease, Mapping)
                    else "activation-busy"
                )
                raise ManagedImportError(
                    PROJECT_ACTIVATION_IN_PROGRESS,
                    409,
                    correlation,
                    phase="VALIDATE",
                )

            attempt_id = attempt_id_factory()
            counter = int(control.get("activation_fencing_counter") or 0) + 1
            sprint_id = (
                provenance.stable_sprint_id() if provenance is not None else None
            )
            evidence: dict[str, Any] = {
                "phase": "VALIDATE",
                "request": {
                    "repository_id": request.repository_id,
                    "ref": request.ref,
                    "manifest_path": request.manifest_path,
                },
            }
            if repository_binding is not None:
                evidence["repository_binding"] = deepcopy(
                    dict(repository_binding)
                )
            record: dict[str, Any] = {
                "idempotency_key": request.idempotency_key,
                "request_fingerprint": fingerprint,
                "attempt_id": attempt_id,
                "recovery_of_attempt_id": None,
                "recovery_generation": 0,
                "created_fencing_token": counter,
                "fencing_token": counter,
                "pinned_identity": (
                    asdict(provenance) if provenance is not None else None
                ),
                "workspace_source_commit": workspace_source_commit,
                "sprint_id": sprint_id,
                "status": "VALIDATING",
                "response": None,
                "http_status": None,
                "error": None,
                "evidence": evidence,
                "created_at": now_text,
                "updated_at": now_text,
            }

            same_sprint = [
                candidate
                for candidate in control["start_idempotency_records"]
                if isinstance(candidate, Mapping)
                and sprint_id is not None
                and candidate.get("sprint_id") == sprint_id
            ]
            if same_sprint:
                prior_statuses = {candidate.get("status") for candidate in same_sprint}
                code = (
                    SPRINT_ALREADY_EXISTS
                    if "SUCCEEDED" in prior_statuses
                    else SPRINT_RECOVERY_REQUIRED
                )
                error = ManagedImportError(
                    code,
                    409,
                    attempt_id,
                    phase="VALIDATE",
                    evidence={"sprint_id": sprint_id},
                )
                record.update(
                    {
                        "status": "FAILED",
                        "http_status": 409,
                        "error": error.envelope,
                        "evidence": {"phase": "VALIDATE", "sprint_id": sprint_id},
                    }
                )
                control["activation_fencing_counter"] = counter
                control["activation_lease"] = None
                control["start_idempotency_records"].append(record)
                self._write_control(connection, control, revision)
                return deepcopy(record), False

            if provenance is not None:
                indexed = self._indexed_runtime_or_error(
                    connection,
                    control,
                    project_id,
                    correlation_id=attempt_id,
                    phase="VALIDATE",
                )
                if indexed is not None and indexed[0]["status"] == "active":
                    raise ManagedImportError(
                        PROJECT_ACTIVATION_IN_PROGRESS,
                        409,
                        str(indexed[0]["sprint_id"]),
                        phase="VALIDATE",
                    )

            control["activation_fencing_counter"] = counter
            control["activation_lease"] = {
                "attempt_id": attempt_id,
                "fencing_token": counter,
                "acquired_at": now_text,
                "expires_at": (now + timedelta(seconds=_LEASE_SECONDS)).isoformat(),
            }
            control["start_idempotency_records"].append(record)
            self._write_control(connection, control, revision)
            return deepcopy(record), False

    def record_terminal_failure(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
        provenance: SprintProvenance,
        workspace_source_commit: str | None,
        error: ManagedImportError,
        *,
        attempt_id: str,
        clock: Callable[[], datetime],
    ) -> dict[str, Any]:
        """Atomically bind immutable identity to a normalized VALIDATE failure."""

        fingerprint = request.request_fingerprint(project_id)
        now_text = _timestamp(clock)
        sprint_id = provenance.stable_sprint_id()
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            existing = self._record(control, request.idempotency_key)
            if existing is not None:
                if existing.get("request_fingerprint") != fingerprint:
                    raise ManagedImportError(
                        IDEMPOTENCY_KEY_CONFLICT,
                        409,
                        str(existing.get("attempt_id") or "idempotency-conflict"),
                        phase="VALIDATE",
                    )
                return deepcopy(existing)
            prior_same_sprint = next(
                (
                    candidate
                    for candidate in control["start_idempotency_records"]
                    if isinstance(candidate, Mapping)
                    and candidate.get("sprint_id") == sprint_id
                ),
                None,
            )
            final_error = error
            evidence = deepcopy(error.evidence) or {
                "phase": "VALIDATE",
                "failure_code": error.code,
            }
            if prior_same_sprint is not None:
                code = (
                    SPRINT_ALREADY_EXISTS
                    if prior_same_sprint.get("status") == "SUCCEEDED"
                    else SPRINT_RECOVERY_REQUIRED
                )
                final_error = ManagedImportError(
                    code,
                    409,
                    attempt_id,
                    phase="VALIDATE",
                    evidence={"sprint_id": sprint_id},
                )
                evidence = deepcopy(final_error.evidence)
            record = {
                "idempotency_key": request.idempotency_key,
                "request_fingerprint": fingerprint,
                "attempt_id": attempt_id,
                "recovery_of_attempt_id": None,
                "recovery_generation": 0,
                "fencing_token": None,
                "pinned_identity": asdict(provenance),
                "workspace_source_commit": workspace_source_commit,
                "sprint_id": sprint_id,
                "status": "FAILED",
                "response": None,
                "http_status": final_error.http_status,
                "error": deepcopy(final_error.envelope),
                "evidence": evidence,
                "created_at": now_text,
                "updated_at": now_text,
            }
            control["start_idempotency_records"].append(record)
            self._write_control(connection, control, revision)
            return deepcopy(record)

    def resume_existing(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
        *,
        clock: Callable[[], datetime],
    ) -> dict[str, Any]:
        fingerprint = request.request_fingerprint(project_id)
        now = _utc_now(clock)
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            reconciled = self._reconcile_superseded_attempts(
                control,
                updated_at=now.isoformat(),
                connection=connection,
                project_id=project_id,
            )
            record = self._record(control, request.idempotency_key)
            if record is None:
                raise RuntimeError("managed start record disappeared")
            if record.get("request_fingerprint") != fingerprint:
                raise ManagedImportError(
                    IDEMPOTENCY_KEY_CONFLICT,
                    409,
                    str(record.get("attempt_id") or "idempotency-conflict"),
                    phase="VALIDATE",
                )
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                if reconciled:
                    self._write_control(connection, control, revision)
                return deepcopy(record)
            lease = control.get("activation_lease")
            if (
                self._lease_live(lease, now)
                and isinstance(lease, Mapping)
                and lease.get("attempt_id") != record.get("attempt_id")
            ):
                raise ManagedImportError(
                    PROJECT_ACTIVATION_IN_PROGRESS,
                    409,
                    str(record.get("attempt_id") or "activation-busy"),
                    phase="VALIDATE",
                )
            if (
                self._lease_live(lease, now)
                and isinstance(lease, Mapping)
                and lease.get("attempt_id") == record.get("attempt_id")
            ):
                if reconciled:
                    self._write_control(connection, control, revision)
                return deepcopy(record)
            counter = int(control.get("activation_fencing_counter") or 0) + 1
            now_text = now.isoformat()
            self._rebind_preactivation_row_fence(
                connection,
                project_id,
                record.get("sprint_id"),
                old_fencing_token=record.get("fencing_token"),
                new_fencing_token=counter,
            )
            record["fencing_token"] = counter
            record["updated_at"] = now_text
            control["activation_fencing_counter"] = counter
            control["activation_lease"] = {
                "attempt_id": record["attempt_id"],
                "fencing_token": counter,
                "acquired_at": now_text,
                "expires_at": (now + timedelta(seconds=_LEASE_SECONDS)).isoformat(),
            }
            self._write_control(connection, control, revision)
            return deepcopy(record)

    @staticmethod
    def _assert_fence(
        control: Mapping[str, Any],
        record: Mapping[str, Any],
        expected_fencing_token: int,
    ) -> None:
        lease = control.get("activation_lease")
        if (
            not isinstance(lease, Mapping)
            or lease.get("attempt_id") != record.get("attempt_id")
            or record.get("fencing_token") != expected_fencing_token
            or lease.get("fencing_token") != expected_fencing_token
            or control.get("activation_fencing_counter") != expected_fencing_token
        ):
            raise ManagedImportError(
                PROJECT_ACTIVATION_IN_PROGRESS,
                409,
                str(record.get("attempt_id") or "stale-fence"),
                phase="ACTIVATE",
            )

    @staticmethod
    def _rebind_preactivation_row_fence(
        connection: sqlite3.Connection,
        project_id: str,
        sprint_id: Any,
        *,
        old_fencing_token: Any,
        new_fencing_token: int,
    ) -> None:
        """Move a resumable shell with the renewed project-control fence."""

        if not isinstance(sprint_id, str):
            return
        row = connection.execute(
            """
            SELECT status, fencing_token, state_json FROM managed_sprints
            WHERE project_id = ? AND sprint_id = ?
            """,
            (project_id, sprint_id),
        ).fetchone()
        if row is None:
            return
        try:
            state = json.loads(row["state_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("managed sprint runtime is corrupt") from exc
        if (
            not isinstance(state, Mapping)
            or state.get("workflow") is not None
            or row["status"] not in {"preparing", "failed"}
            or row["fencing_token"] != old_fencing_token
        ):
            raise RuntimeError("managed preactivation fence cannot be renewed")
        connection.execute(
            """
            UPDATE managed_sprints SET fencing_token = ?
            WHERE project_id = ? AND sprint_id = ? AND fencing_token = ?
            """,
            (
                new_fencing_token,
                project_id,
                sprint_id,
                old_fencing_token,
            ),
        )

    def update_attempt(
        self,
        project_id: str,
        attempt_id: str,
        *,
        status: str | None = None,
        evidence_update: Mapping[str, Any] | None = None,
        fencing_token: int,
        clock: Callable[[], datetime],
    ) -> dict[str, Any]:
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            record = self._record_by_attempt(control, attempt_id)
            if record is None:
                raise RuntimeError("managed start attempt is missing")
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                return deepcopy(record)
            self._assert_fence(control, record, fencing_token)
            if status is not None:
                allowed = {
                    "VALIDATING": {"VALIDATING", "PREPARING", "FAILED"},
                    "PREPARING": {"PREPARING", "ACTIVATING", "FAILED"},
                    "ACTIVATING": {"ACTIVATING", "FAILED"},
                }
                if status not in allowed.get(str(record.get("status")), set()):
                    raise RuntimeError("managed import phase transition is invalid")
                record["status"] = status
            if evidence_update:
                evidence = record.get("evidence")
                if not isinstance(evidence, dict):
                    evidence = {}
                    record["evidence"] = evidence
                evidence.update(deepcopy(dict(evidence_update)))
            updated_at = _timestamp(clock)
            record["updated_at"] = updated_at
            sprint_id = record.get("sprint_id")
            if isinstance(sprint_id, str):
                runtime_row = connection.execute(
                    """
                    SELECT project_id, sprint_id, status, fencing_token, state_json
                    FROM managed_sprints
                    WHERE project_id = ? AND sprint_id = ?
                    """,
                    (project_id, sprint_id),
                ).fetchone()
                if runtime_row is not None:
                    try:
                        runtime_state = json.loads(runtime_row["state_json"])
                    except (TypeError, json.JSONDecodeError) as exc:
                        raise RuntimeError(
                            "managed sprint runtime is corrupt"
                        ) from exc
                    runtime_issues = self._runtime_row_issues(
                        control,
                        runtime_row,
                        runtime_state,
                        require_indexed=False,
                    )
                    if runtime_issues or not isinstance(runtime_state, dict):
                        raise RuntimeError(
                            "managed sprint runtime invariant failed: "
                            + ",".join(
                                runtime_issues or ("RUNTIME_STATE_NOT_OBJECT",)
                            )
                        )
                    if runtime_state.get("workflow") is None:
                        import_attempt = next(
                            (
                                item
                                for item in runtime_state.get(
                                    "import_attempts", []
                                )
                                if isinstance(item, dict)
                                and item.get("attempt_id") == attempt_id
                            ),
                            None,
                        )
                        if not isinstance(import_attempt, dict):
                            raise RuntimeError(
                                "preactivation runtime import attempt is missing"
                            )
                        if status == "ACTIVATING":
                            prepared_workspaces = (
                                evidence_update.get("prepared_workspaces")
                                if isinstance(evidence_update, Mapping)
                                else None
                            )
                            prepared_ids = [
                                str(item["workspace_id"])
                                for item in prepared_workspaces or []
                                if isinstance(item, Mapping)
                                and isinstance(item.get("workspace_id"), str)
                            ]
                            import_attempt.update(
                                {
                                    "phase": "ACTIVATE",
                                    "status": "running",
                                    "prepared_artifact_ids": prepared_ids,
                                    "activation_response": None,
                                    "updated_at": updated_at,
                                }
                            )
                        connection.execute(
                            """
                            UPDATE managed_sprints
                            SET fencing_token = ?, state_json = ?
                            WHERE project_id = ? AND sprint_id = ?
                            """,
                            (
                                fencing_token,
                                _json_text(runtime_state),
                                project_id,
                                sprint_id,
                            ),
                        )
            self._write_control(connection, control, revision)
            return deepcopy(record)

    def stage_preparing_runtime(
        self,
        project_id: str,
        attempt_id: str,
        runtime_state: Mapping[str, Any],
        *,
        evidence_update: Mapping[str, Any],
        fencing_token: int,
        clock: Callable[[], datetime],
    ) -> dict[str, Any]:
        """Publish a no-work preactivation shell with the PREPARING fence.

        The shell is the durable home for import attempts and Coordinator
        recovery before any assignment, lease, workspace, port, or process is
        activated.  Re-entry is idempotent; a failed shell may already contain
        the running child produced by ``begin_retry_import``.
        """

        proposed = deepcopy(dict(runtime_state))
        if proposed.get("status") != "preparing":
            raise ValueError("preactivation runtime must be preparing")
        schema_issues = _runtime_state_schema_errors(
            proposed,
            issue_code="PREPARATION_SCHEMA_INVALID",
        )
        invariant_issues = managed_activation_invariant_issues(proposed)
        if schema_issues or invariant_issues:
            raise RuntimeError("preactivation runtime state is invalid")

        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            record = self._record_by_attempt(control, attempt_id)
            if record is None:
                raise RuntimeError("managed start attempt is missing")
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                return deepcopy(record)
            self._assert_fence(control, record, fencing_token)
            current_status = str(record.get("status"))
            if current_status not in {
                "VALIDATING",
                "PREPARING",
                "ACTIVATING",
            }:
                raise RuntimeError("managed start attempt is not preparing")
            if record.get("sprint_id") != proposed.get("sprint_id"):
                raise RuntimeError("preactivation sprint identity changed")

            if current_status != "ACTIVATING":
                record["status"] = "PREPARING"
            evidence = record.get("evidence")
            if not isinstance(evidence, dict):
                evidence = {}
                record["evidence"] = evidence
            durable_update = deepcopy(dict(evidence_update))
            if current_status == "ACTIVATING":
                durable_update.pop("phase", None)
            evidence.update(durable_update)
            record["updated_at"] = _timestamp(clock)

            sprint_id = str(record["sprint_id"])
            existing = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, sprint_id),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO managed_sprints(
                        project_id, sprint_id, status, fencing_token, state_json
                    ) VALUES (?, ?, 'preparing', ?, ?)
                    """,
                    (
                        project_id,
                        sprint_id,
                        fencing_token,
                        _json_text(proposed),
                    ),
                )
            else:
                try:
                    current = json.loads(existing["state_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    raise RuntimeError("managed sprint runtime is corrupt") from exc
                current_issues = self._runtime_row_issues(
                    control, existing, current, require_indexed=False
                )
                if current_issues or not isinstance(current, dict):
                    raise RuntimeError(
                        "managed sprint runtime invariant failed: "
                        + ",".join(
                            current_issues or ("RUNTIME_STATE_NOT_OBJECT",)
                        )
                    )
                current_attempt = next(
                    (
                        item
                        for item in current.get("import_attempts", [])
                        if isinstance(item, dict)
                        and item.get("attempt_id") == attempt_id
                        and item.get("status") == "running"
                    ),
                    None,
                )
                proposed_attempt = next(
                    (
                        item
                        for item in proposed.get("import_attempts", [])
                        if isinstance(item, Mapping)
                        and item.get("attempt_id") == attempt_id
                        and item.get("status") == "running"
                    ),
                    None,
                )
                if not isinstance(current_attempt, dict) or not isinstance(
                    proposed_attempt, Mapping
                ):
                    raise RuntimeError(
                        "preactivation runtime does not contain the fenced attempt"
                    )
                if current_status != "ACTIVATING":
                    current_attempt.update(
                        {
                            "phase": "PREPARE",
                            "status": "running",
                            "preflight": deepcopy(
                                dict(proposed_attempt["preflight"])
                            ),
                            "prepared_artifact_ids": [],
                            "activation_response": None,
                            "updated_at": record["updated_at"],
                        }
                    )
                current_schema_issues = _runtime_state_schema_errors(
                    current,
                    issue_code="PREPARATION_SCHEMA_INVALID",
                )
                current_invariant_issues = managed_activation_invariant_issues(
                    current
                )
                if current_schema_issues or current_invariant_issues:
                    issue_codes = [
                        str(item.get("code"))
                        for item in current_schema_issues
                    ] + list(current_invariant_issues)
                    raise RuntimeError(
                        "managed preactivation refresh is invalid: "
                        + ",".join(dict.fromkeys(issue_codes))
                    )
                connection.execute(
                    """
                    UPDATE managed_sprints
                    SET fencing_token = ?, state_json = ?
                    WHERE project_id = ? AND sprint_id = ?
                    """,
                    (
                        fencing_token,
                        _json_text(current),
                        project_id,
                        sprint_id,
                    ),
                )
            self._write_control(connection, control, revision)
            return deepcopy(record)

    def set_pinned_identity(
        self,
        project_id: str,
        attempt_id: str,
        provenance: SprintProvenance,
        *,
        fencing_token: int,
        clock: Callable[[], datetime],
    ) -> dict[str, Any]:
        """Atomically fill immutable provenance in a pre-Git request intent."""

        pinned_identity = asdict(provenance)
        sprint_id = provenance.stable_sprint_id()
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            record = self._record_by_attempt(control, attempt_id)
            if record is None:
                raise RuntimeError("managed start attempt is missing")
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                return deepcopy(record)
            self._assert_fence(control, record, fencing_token)
            existing_identity = record.get("pinned_identity")
            existing_sprint_id = record.get("sprint_id")
            if existing_identity is not None and existing_identity != pinned_identity:
                raise RuntimeError("managed start identity is already pinned")
            if existing_sprint_id is not None and existing_sprint_id != sprint_id:
                raise RuntimeError("managed sprint id is already pinned")
            if record.get("status") != "VALIDATING":
                raise RuntimeError("managed identity must be pinned during VALIDATE")
            prior_same_sprint = next(
                (
                    candidate
                    for candidate in control["start_idempotency_records"]
                    if isinstance(candidate, Mapping)
                    and candidate is not record
                    and candidate.get("sprint_id") == sprint_id
                ),
                None,
            )
            if prior_same_sprint is not None:
                code = (
                    SPRINT_ALREADY_EXISTS
                    if prior_same_sprint.get("status") == "SUCCEEDED"
                    else SPRINT_RECOVERY_REQUIRED
                )
                raise ManagedImportError(
                    code,
                    409,
                    attempt_id,
                    phase="VALIDATE",
                    evidence={"sprint_id": sprint_id},
                )
            indexed = self._indexed_runtime_or_error(
                connection,
                control,
                project_id,
                correlation_id=attempt_id,
                phase="VALIDATE",
            )
            if indexed is not None and indexed[0]["status"] == "active":
                raise ManagedImportError(
                    PROJECT_ACTIVATION_IN_PROGRESS,
                    409,
                    str(indexed[0]["sprint_id"]),
                    phase="VALIDATE",
                )
            record["pinned_identity"] = pinned_identity
            record["sprint_id"] = sprint_id
            record["updated_at"] = _timestamp(clock)
            self._write_control(connection, control, revision)
            return deepcopy(record)

    def set_workspace_source(
        self,
        project_id: str,
        attempt_id: str,
        workspace_source_commit: str,
        *,
        fencing_token: int,
        clock: Callable[[], datetime],
    ) -> dict[str, Any]:
        """Bind the immutable workspace commit while the attempt is validating."""

        if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", workspace_source_commit) is None:
            raise ValueError("workspace source commit must be a full object ID")
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            record = self._record_by_attempt(control, attempt_id)
            if record is None:
                raise RuntimeError("managed start attempt is missing")
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                return deepcopy(record)
            self._assert_fence(control, record, fencing_token)
            existing = record.get("workspace_source_commit")
            if existing is not None and existing != workspace_source_commit:
                raise RuntimeError("workspace source commit is already pinned")
            if record.get("status") != "VALIDATING":
                raise RuntimeError("workspace source must be pinned during VALIDATE")
            record["workspace_source_commit"] = workspace_source_commit
            record["updated_at"] = _timestamp(clock)
            self._write_control(connection, control, revision)
            return deepcopy(record)

    def fail_attempt(
        self,
        project_id: str,
        attempt_id: str,
        error: ManagedImportError,
        *,
        evidence: Mapping[str, Any],
        fencing_token: int,
        clock: Callable[[], datetime],
    ) -> dict[str, Any] | None:
        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            record = self._record_by_attempt(control, attempt_id)
            if record is None:
                return None
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                return deepcopy(record)
            self._assert_fence(control, record, fencing_token)
            accumulated_evidence = (
                deepcopy(record.get("evidence"))
                if isinstance(record.get("evidence"), Mapping)
                else {}
            )
            accumulated_evidence.update(deepcopy(dict(evidence)))
            failed_at = _timestamp(clock)
            record.update(
                {
                    "status": "FAILED",
                    "response": None,
                    "http_status": error.http_status,
                    "error": deepcopy(error.envelope),
                    "evidence": accumulated_evidence or {"phase": "VALIDATE"},
                    "updated_at": failed_at,
                }
            )

            control["activation_lease"] = None

            sprint_id = record.get("sprint_id")
            runtime_row = (
                connection.execute(
                    """
                    SELECT project_id, sprint_id, status, fencing_token, state_json
                    FROM managed_sprints
                    WHERE project_id = ? AND sprint_id = ?
                    """,
                    (project_id, sprint_id),
                ).fetchone()
                if isinstance(sprint_id, str)
                else None
            )
            if runtime_row is not None:
                try:
                    runtime_state = json.loads(runtime_row["state_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    raise RuntimeError("managed sprint runtime is corrupt") from exc
                runtime_issues = self._runtime_row_issues(
                    control,
                    runtime_row,
                    runtime_state,
                    require_indexed=False,
                )
                # The control record was just terminalized in memory, so a
                # preparing row may report only the expected owner-status
                # mismatch until the state below becomes failed.
                unexpected_issues = tuple(
                    issue
                    for issue in runtime_issues
                    if issue != "RUNTIME_ACTIVATION_FENCE_MISMATCH"
                )
                if unexpected_issues or not isinstance(runtime_state, dict):
                    raise RuntimeError(
                        "managed sprint runtime invariant failed: "
                        + ",".join(
                            unexpected_issues or ("RUNTIME_STATE_NOT_OBJECT",)
                        )
                    )
                if runtime_state.get("workflow") is None:
                    import_attempt = next(
                        (
                            item
                            for item in runtime_state.get("import_attempts", [])
                            if isinstance(item, dict)
                            and item.get("attempt_id") == attempt_id
                        ),
                        None,
                    )
                    if not isinstance(import_attempt, dict):
                        raise RuntimeError(
                            "preactivation runtime import attempt is missing"
                        )
                    phase = str(
                        error.envelope.get("detail", {}).get("phase")
                        or accumulated_evidence.get("phase")
                        or import_attempt.get("phase")
                        or "PREPARE"
                    ).upper()
                    if phase not in {"VALIDATE", "PREPARE", "ACTIVATE"}:
                        phase = "PREPARE"
                    import_attempt.update(
                        {
                            "phase": phase,
                            "status": "failed",
                            "activation_response": None,
                            "updated_at": failed_at,
                        }
                    )
                    context_id = _safe_identifier(
                        "context", str(sprint_id), attempt_id, error.code
                    )
                    existing_context = next(
                        (
                            item
                            for item in runtime_state.get(
                                "coordinator_contexts", []
                            )
                            if isinstance(item, Mapping)
                            and item.get("context_id") == context_id
                        ),
                        None,
                    )
                    issue_codes = [
                        str(item.get("code"))
                        for item in error.envelope.get("detail", {}).get(
                            "issues", []
                        )
                        if isinstance(item, Mapping)
                        and isinstance(item.get("code"), str)
                    ]
                    normalized_error = {
                        "code": error.code,
                        "http_status": error.http_status,
                        "phase": phase,
                        "issue_codes": issue_codes,
                    }
                    if existing_context is None:
                        runtime_state["coordinator_contexts"].append(
                            {
                                "context_id": context_id,
                                "graph_revision": int(
                                    runtime_state["graph_revision"]
                                ),
                                "failure_scope": phase.lower(),
                                "reason_code": error.code,
                                "import_attempt_id": attempt_id,
                                "failed_assignment_id": None,
                                "join_target": None,
                                "assigned_branch": None,
                                "source_commit": None,
                                "result_commit": None,
                                "diff_summary": {},
                                "workspace_status": {
                                    "prepared_artifact_ids": deepcopy(
                                        import_attempt.get(
                                            "prepared_artifact_ids", []
                                        )
                                    )
                                },
                                "process_records": [],
                                "port_records": [],
                                "test_evidence_summary": {},
                                "normalized_error": normalized_error,
                                "reviewer_feedback": [],
                            }
                        )
                    event_id = _safe_identifier(
                        "event", str(sprint_id), context_id
                    )
                    dedupe_key = (
                        f"enqueue:coordinator:{sprint_id}:{context_id}"
                    )
                    coordinator_phone = self._coordinator_phone(runtime_state)
                    payload = {
                        "sprint_id": str(sprint_id),
                        "context_id": context_id,
                        "coordinator_phone": coordinator_phone,
                        "reason_code": error.code,
                    }
                    queue_receipt_id = (
                        self.enqueue_managed_queue_item_in_transaction(
                            connection,
                            dedupe_key=dedupe_key,
                            event_id=event_id,
                            event_type="COORDINATOR_ENQUEUE",
                            payload=payload,
                            created_at=failed_at,
                        )
                    )
                    existing_event = next(
                        (
                            item
                            for item in runtime_state.get("outbox", [])
                            if isinstance(item, Mapping)
                            and item.get("event_id") == event_id
                        ),
                        None,
                    )
                    delivered_event = {
                        "event_id": event_id,
                        "dedupe_key": dedupe_key,
                        "event_type": "COORDINATOR_ENQUEUE",
                        "payload": payload,
                        "status": "delivered",
                        "created_at": failed_at,
                        "delivered_at": failed_at,
                        "queue_receipt_id": queue_receipt_id,
                    }
                    if existing_event is None:
                        runtime_state["outbox"].append(delivered_event)
                    elif dict(existing_event) != delivered_event:
                        raise RuntimeError(
                            "managed Coordinator enqueue identity conflict"
                        )
                    runtime_state["status"] = "failed"
                    schema_issues = _runtime_state_schema_errors(
                        runtime_state,
                        issue_code="IMPORT_FAILURE_SCHEMA_INVALID",
                    )
                    invariant_issues = managed_activation_invariant_issues(
                        runtime_state
                    )
                    if schema_issues or invariant_issues:
                        issue_codes = [
                            str(item.get("code")) for item in schema_issues
                        ] + list(invariant_issues)
                        raise RuntimeError(
                            "managed import failure runtime is invalid: "
                            + ",".join(dict.fromkeys(issue_codes))
                        )
                    connection.execute(
                        """
                        UPDATE managed_sprints
                        SET status = 'failed', fencing_token = ?, state_json = ?
                        WHERE project_id = ? AND sprint_id = ?
                        """,
                        (
                            fencing_token,
                            _json_text(runtime_state),
                            project_id,
                            sprint_id,
                        ),
                    )
            self._write_control(connection, control, revision)
            return deepcopy(record)

    @staticmethod
    def _coordinator_phone(runtime_state: Mapping[str, Any]) -> str:
        revision = runtime_state.get("graph_revision")
        graph_revision = next(
            (
                item
                for item in runtime_state.get("graph_revisions", [])
                if isinstance(item, Mapping)
                and item.get("revision") == revision
            ),
            None,
        )
        definition = (
            graph_revision.get("definition")
            if isinstance(graph_revision, Mapping)
            else None
        )
        coordinator = (
            definition.get("coordinator")
            if isinstance(definition, Mapping)
            else None
        )
        node_id = (
            coordinator.get("node_id")
            if isinstance(coordinator, Mapping)
            else None
        )
        node = next(
            (
                item
                for item in definition.get("nodes", [])
                if isinstance(item, Mapping) and item.get("id") == node_id
            ),
            None,
        ) if isinstance(definition, Mapping) else None
        agent = node.get("agent") if isinstance(node, Mapping) else None
        phone = agent.get("phone") if isinstance(agent, Mapping) else None
        if not isinstance(phone, str) or re.fullmatch(r"[0-9]{4}", phone) is None:
            raise RuntimeError("managed Coordinator phone is invalid")
        return phone

    def begin_retry_import(
        self,
        project_id: str,
        sprint_id: str,
        recovery_id: str,
        *,
        attempt_id_factory: Callable[[], str],
        clock: Callable[[], datetime],
    ) -> dict[str, Any]:
        """Create exactly one immutable-generation import recovery child.

        The control child, running runtime receipt, row fence, activation
        lease, and completed Coordinator recovery are one SQLite commit.  No
        repository registry lookup or mutable ref resolution occurs here.
        """

        with self._transaction() as connection:
            control, revision = self._load_control(connection, project_id)
            self._validate_control_snapshot(connection, project_id, control)
            row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, sprint_id),
            ).fetchone()
            if row is None:
                raise RuntimeError("managed retry runtime was not found")
            try:
                state = json.loads(row["state_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError("managed sprint runtime is corrupt") from exc
            row_issues = self._runtime_row_issues(
                control, row, state, require_indexed=False
            )
            if row_issues or not isinstance(state, dict):
                raise RuntimeError(
                    "managed sprint runtime invariant failed: "
                    + ",".join(row_issues or ("RUNTIME_STATE_NOT_OBJECT",))
                )
            recovery = next(
                (
                    item
                    for item in state.get("recovery_records", [])
                    if isinstance(item, dict)
                    and item.get("recovery_id") == recovery_id
                ),
                None,
            )
            if not isinstance(recovery, dict) or recovery.get("action") != "RETRY_IMPORT":
                raise RuntimeError("managed retry recovery was not found")
            if recovery.get("status") == "completed":
                produced_ids = recovery.get("produced_record_ids")
                child_id = (
                    produced_ids[0]
                    if isinstance(produced_ids, list) and len(produced_ids) == 1
                    else None
                )
                child = (
                    self._record_by_attempt(control, child_id)
                    if isinstance(child_id, str)
                    else None
                )
                if not isinstance(child, dict):
                    raise RuntimeError("managed retry child is missing")
                return deepcopy(child)
            if recovery.get("status") != "pending":
                raise ManagedRetryImportConflict(
                    "managed retry recovery is not pending"
                )
            context_id = recovery.get("context_id")
            if state.get("status") != "failed" or state.get("workflow") is not None:
                raise ManagedRetryImportConflict(
                    "managed retry target is not a failed import"
                )

            parameters = recovery.get("parameters")
            parent_attempt_id = recovery.get("import_attempt_id")
            if (
                not isinstance(parameters, Mapping)
                or set(parameters) != {
                    "failed_attempt_id",
                    "recovery_idempotency_key",
                }
                or parameters.get("failed_attempt_id") != parent_attempt_id
                or not isinstance(parent_attempt_id, str)
            ):
                raise RuntimeError("managed retry recovery parameters are invalid")
            recovery_key = parameters.get("recovery_idempotency_key")
            if not isinstance(recovery_key, str) or not 1 <= len(recovery_key) <= 200:
                raise RuntimeError("managed retry idempotency key is invalid")
            if self._record(control, recovery_key) is not None:
                raise ManagedRetryImportConflict(
                    "managed retry idempotency key already exists"
                )

            parent_control = self._record_by_attempt(control, parent_attempt_id)
            parent_runtime = next(
                (
                    item
                    for item in state.get("import_attempts", [])
                    if isinstance(item, Mapping)
                    and item.get("attempt_id") == parent_attempt_id
                ),
                None,
            )
            if (
                not isinstance(parent_control, Mapping)
                or parent_control.get("status") != "FAILED"
                or not isinstance(parent_runtime, Mapping)
                or parent_runtime.get("status") != "failed"
            ):
                raise ManagedRetryImportConflict(
                    "managed retry parent is not failed"
                )
            pinned_identity = parent_control.get("pinned_identity")
            workspace_source_commit = parent_control.get(
                "workspace_source_commit"
            )
            if (
                not isinstance(pinned_identity, Mapping)
                or parent_control.get("sprint_id") != sprint_id
                or not isinstance(workspace_source_commit, str)
            ):
                raise ManagedRetryImportConflict(
                    "managed retry parent is not fully pinned"
                )
            parent_evidence = parent_control.get("evidence")
            binding = (
                parent_evidence.get("repository_binding")
                if isinstance(parent_evidence, Mapping)
                else None
            )
            request_record = (
                parent_evidence.get("request")
                if isinstance(parent_evidence, Mapping)
                else None
            )
            if (
                not isinstance(binding, Mapping)
                or not isinstance(request_record, Mapping)
            ):
                raise ManagedRetryImportConflict(
                    "managed retry provenance is incomplete"
                )
            try:
                durable_request = StartSprintFromGitRequest(
                    repository_id=str(request_record["repository_id"]),
                    ref=str(request_record["ref"]),
                    manifest_path=str(request_record["manifest_path"]),
                    idempotency_key=recovery_key,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ManagedRetryImportConflict(
                    "managed retry request is invalid"
                ) from exc
            if (
                durable_request.repository_id
                != pinned_identity.get("repository_id")
                or durable_request.ref != state.get("requested_ref")
                or durable_request.manifest_path
                != pinned_identity.get("manifest_path")
                or durable_request.request_fingerprint(project_id)
                != parent_control.get("request_fingerprint")
            ):
                raise ManagedRetryImportConflict(
                    "managed retry request provenance changed"
                )
            generation = parent_control.get("recovery_generation")
            if (
                not isinstance(generation, int)
                or isinstance(generation, bool)
                or generation >= 100
            ):
                raise ManagedRetryImportConflict(
                    "managed retry generation is exhausted"
                )
            sibling_children = [
                item
                for item in control.get("start_idempotency_records", [])
                if isinstance(item, Mapping)
                and item.get("recovery_of_attempt_id") == parent_attempt_id
            ]
            if sibling_children:
                raise ManagedRetryImportConflict(
                    "managed retry parent already has a child"
                )
            if any(
                item is not recovery
                and item.get("context_id") == context_id
                and item.get("status") == "completed"
                for item in state.get("recovery_records", [])
                if isinstance(item, Mapping)
            ):
                # The import child and the recovery receipt commit together,
                # so enforce the same one-decision-per-context fence inside
                # this transaction before allocating any durable child state.
                raise ManagedRetryImportConflict(
                    "managed retry context already has a completed recovery"
                )
            pending_for_context = [
                item
                for item in state.get("recovery_records", [])
                if isinstance(item, dict)
                and item.get("context_id") == context_id
                and item.get("status") == "pending"
            ]
            if pending_for_context and pending_for_context[0] is not recovery:
                raise ManagedRetryImportConflict(
                    "managed retry context has an earlier pending recovery"
                )
            nonterminal = [
                item
                for item in control.get("start_idempotency_records", [])
                if isinstance(item, Mapping)
                and item.get("status")
                in {"VALIDATING", "PREPARING", "ACTIVATING"}
            ]
            if nonterminal:
                raise ManagedRetryImportConflict(
                    "another managed import is already running"
                )
            now = _utc_now(clock)
            now_text = now.isoformat()
            for loser in pending_for_context[1:]:
                loser.update(
                    {
                        "produced_record_ids": [],
                        "response": None,
                        "normalized_error": {
                            "code": "RECOVERY_CONFLICT",
                            "http_status": 409,
                        },
                        "evidence": {},
                        "status": "failed",
                        "completed_at": now_text,
                    }
                )
            counter = int(control.get("activation_fencing_counter") or 0) + 1
            child_attempt_id = attempt_id_factory()
            if (
                not isinstance(child_attempt_id, str)
                or not child_attempt_id
                or self._record_by_attempt(control, child_attempt_id) is not None
            ):
                raise RuntimeError("managed retry attempt identity is invalid")
            child_evidence = {
                "phase": "VALIDATE",
                "repository_binding": deepcopy(dict(binding)),
                "request": deepcopy(dict(request_record)),
            }
            child = {
                "idempotency_key": recovery_key,
                "request_fingerprint": parent_control["request_fingerprint"],
                "attempt_id": child_attempt_id,
                "recovery_of_attempt_id": parent_attempt_id,
                "recovery_generation": generation + 1,
                "created_fencing_token": counter,
                "fencing_token": counter,
                "pinned_identity": deepcopy(dict(pinned_identity)),
                "workspace_source_commit": workspace_source_commit,
                "sprint_id": sprint_id,
                "status": "VALIDATING",
                "response": None,
                "http_status": None,
                "error": None,
                "evidence": child_evidence,
                "created_at": now_text,
                "updated_at": now_text,
            }
            control["activation_fencing_counter"] = counter
            control["activation_lease"] = {
                "attempt_id": child_attempt_id,
                "fencing_token": counter,
                "acquired_at": now_text,
                "expires_at": (
                    now + timedelta(seconds=_LEASE_SECONDS)
                ).isoformat(),
            }
            control["start_idempotency_records"].append(child)

            state["import_attempts"].append(
                {
                    "attempt_id": child_attempt_id,
                    "idempotency_key": recovery_key,
                    "request_fingerprint": parent_control[
                        "request_fingerprint"
                    ],
                    "identity": deepcopy(dict(pinned_identity)),
                    "contract_version": 1,
                    "manifest_schema_version": 1,
                    "phase": "VALIDATE",
                    "status": "running",
                    "preflight": deepcopy(dict(parent_runtime["preflight"])),
                    "prepared_artifact_ids": [],
                    "activation_response": None,
                    "created_at": now_text,
                    "updated_at": now_text,
                }
            )
            produced_ids = [child_attempt_id]
            recovery.update(
                {
                    "produced_record_ids": produced_ids,
                    "response": {
                        "recovery_id": recovery_id,
                        "action": "RETRY_IMPORT",
                        "status": "RECOVERY_COMPLETED",
                        "produced_record_ids": produced_ids,
                        "deduplicated": False,
                    },
                    "normalized_error": None,
                    "evidence": {},
                    "status": "completed",
                    "completed_at": now_text,
                }
            )
            schema_issues = _runtime_state_schema_errors(
                state,
                issue_code="RETRY_IMPORT_SCHEMA_INVALID",
            )
            invariant_issues = managed_activation_invariant_issues(state)
            if schema_issues or invariant_issues:
                issue_codes = [
                    str(item.get("code")) for item in schema_issues
                ] + list(invariant_issues)
                raise RuntimeError(
                    "managed retry runtime is invalid: "
                    + ",".join(dict.fromkeys(issue_codes))
                )
            connection.execute(
                """
                UPDATE managed_sprints
                SET fencing_token = ?, state_json = ?
                WHERE project_id = ? AND sprint_id = ?
                """,
                (counter, _json_text(state), project_id, sprint_id),
            )
            self._write_control(connection, control, revision)
            return deepcopy(child)

    @staticmethod
    def _merge_preactivation_history(
        current: Mapping[str, Any],
        activated: Mapping[str, Any],
        *,
        attempt_id: str,
    ) -> dict[str, Any]:
        """Replace a shell with activated work without erasing recovery history."""

        if current.get("workflow") is not None or current.get("status") not in {
            "preparing",
            "failed",
        }:
            raise RuntimeError("managed sprint identity already has other state")
        if (
            current.get("sprint_id") != activated.get("sprint_id")
            or current.get("identity") != activated.get("identity")
            or current.get("repository") != activated.get("repository")
            or current.get("workspace_source_commit")
            != activated.get("workspace_source_commit")
            or current.get("requested_ref") != activated.get("requested_ref")
            or [
                (
                    item.get("revision"),
                    item.get("definition_sha256"),
                    item.get("definition"),
                    item.get("artifact_source_commit"),
                )
                for item in current.get("graph_revisions", [])
                if isinstance(item, Mapping)
            ]
            != [
                (
                    item.get("revision"),
                    item.get("definition_sha256"),
                    item.get("definition"),
                    item.get("artifact_source_commit"),
                )
                for item in activated.get("graph_revisions", [])
                if isinstance(item, Mapping)
            ]
        ):
            raise RuntimeError("managed preactivation provenance changed")
        activated_attempt = next(
            (
                item
                for item in activated.get("import_attempts", [])
                if isinstance(item, Mapping)
                and item.get("attempt_id") == attempt_id
            ),
            None,
        )
        current_attempt = next(
            (
                item
                for item in current.get("import_attempts", [])
                if isinstance(item, Mapping)
                and item.get("attempt_id") == attempt_id
            ),
            None,
        )
        if (
            not isinstance(activated_attempt, Mapping)
            or activated_attempt.get("status") != "succeeded"
            or not isinstance(current_attempt, Mapping)
            or current_attempt.get("status") != "running"
        ):
            raise RuntimeError("managed activation attempt history is inconsistent")
        merged = deepcopy(dict(activated))
        merged["graph_revisions"] = deepcopy(current["graph_revisions"])
        merged["import_attempts"] = [
            deepcopy(dict(item))
            for item in current.get("import_attempts", [])
            if isinstance(item, Mapping) and item.get("attempt_id") != attempt_id
        ] + [deepcopy(dict(activated_attempt))]
        for collection in (
            "coordinator_contexts",
            "blocker_observations",
            "recovery_records",
        ):
            merged[collection] = [
                deepcopy(dict(item))
                for item in current.get(collection, [])
                if isinstance(item, Mapping)
            ]
        historical_outbox = [
            deepcopy(dict(item))
            for item in current.get("outbox", [])
            if isinstance(item, Mapping)
        ]
        activated_outbox = [
            deepcopy(dict(item))
            for item in activated.get("outbox", [])
            if isinstance(item, Mapping)
        ]
        if {item.get("event_id") for item in historical_outbox} & {
            item.get("event_id") for item in activated_outbox
        }:
            raise RuntimeError("managed activation outbox identity conflict")
        merged["outbox"] = historical_outbox + activated_outbox
        return merged

    def activate(
        self,
        project_id: str,
        attempt_id: str,
        runtime_state: Mapping[str, Any],
        response: Mapping[str, Any],
        *,
        fencing_token: int,
        branch_lease_store: BranchLeaseStore,
        branch_lease_requests: Sequence[BranchLeaseRequest],
        expected_lease_snapshot: Mapping[str, Any],
        external_snapshot_provider: Callable[[], Mapping[str, Any]] | None,
        port_reservations: ManagedPortReservationRegistry,
        workspace_publisher: Callable[
            [Sequence[BranchLease]], Sequence[Mapping[str, Any]]
        ],
        clock: Callable[[], datetime],
    ) -> bool:
        state = deepcopy(dict(runtime_state))
        runtime_schema_issues = _runtime_state_schema_errors(
            state,
            issue_code="ACTIVATION_SCHEMA_INVALID",
        )
        response_schema_issues = _schema_errors(
            response,
            "start-sprint-from-git-response-v1.schema.json",
            issue_code="ACTIVATION_RESPONSE_SCHEMA_INVALID",
        )
        if runtime_schema_issues or response_schema_issues:
            raise ManagedImportError(
                SPRINT_ACTIVATE_FAILED,
                500,
                attempt_id,
                phase="ACTIVATE",
                issues=runtime_schema_issues + response_schema_issues,
                evidence={
                    "schema_issue_codes": [
                        issue["code"]
                        for issue in runtime_schema_issues + response_schema_issues
                    ]
                },
            )
        invariant_issues = managed_activation_invariant_issues(state)
        if invariant_issues:
            raise ManagedImportError(
                SPRINT_ACTIVATE_FAILED,
                500,
                attempt_id,
                phase="ACTIVATE",
                issues=[
                    {
                        "code": "ACTIVATION_INVARIANT_FAILED",
                        "path": "runtime_state",
                        "message": "Managed activation invariant failed",
                    }
                ],
                evidence={"invariant_issues": list(invariant_issues)},
            )
        if (
            branch_lease_store.database_path.resolve(strict=False)
            != self.database_path.resolve(strict=False)
        ):
            raise RuntimeError(
                "branch leases and activation state do not share one database"
            )
        branch_lease_store.ensure_initialized()
        rollback_actions: list[Callable[[], None]] = []
        with self._transaction(rollback_actions=rollback_actions) as connection:
            control, revision = self._load_control(connection, project_id)
            record = self._record_by_attempt(control, attempt_id)
            if record is None:
                raise RuntimeError("managed start attempt is missing")
            if record.get("status") == "SUCCEEDED":
                return False
            if record.get("status") == "FAILED":
                stored_error = record.get("error")
                stored_status = record.get("http_status")
                if isinstance(stored_error, Mapping) and isinstance(
                    stored_status, int
                ):
                    raise ManagedImportError.from_stored(
                        stored_error,
                        stored_status,
                        record.get("evidence")
                        if isinstance(record.get("evidence"), Mapping)
                        else {},
                    )
                raise RuntimeError("managed failed attempt is corrupt")
            self._assert_fence(control, record, fencing_token)
            if record.get("status") != "ACTIVATING":
                raise RuntimeError("managed start attempt is not ready to activate")

            sprint_id = str(record.get("sprint_id") or "")
            created_fencing_token = record.get("created_fencing_token")
            successful_fences = [
                int(candidate["fencing_token"])
                for candidate in control["start_idempotency_records"]
                if isinstance(candidate, Mapping)
                and candidate.get("status") == "SUCCEEDED"
                and isinstance(candidate.get("fencing_token"), int)
                and not isinstance(candidate.get("fencing_token"), bool)
            ]
            if (
                not isinstance(created_fencing_token, int)
                or isinstance(created_fencing_token, bool)
                or created_fencing_token > fencing_token
                or (
                    successful_fences
                    and created_fencing_token <= max(successful_fences)
                )
            ):
                raise ManagedImportError(
                    SPRINT_RECOVERY_REQUIRED,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    issues=[
                        {
                            "code": "ATTEMPT_SUPERSEDED",
                            "path": "activation.created_fencing_token",
                            "message": "Managed attempt predates a successful activation",
                        }
                    ],
                )
            active_rows = connection.execute(
                """
                SELECT sprint_id
                FROM managed_sprints
                WHERE project_id = ? AND status = 'active'
                ORDER BY sprint_id
                """,
                (project_id,),
            ).fetchall()
            if len(active_rows) > 1:
                raise ManagedImportError(
                    SPRINT_RECOVERY_REQUIRED,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    issues=[
                        {
                            "code": "ACTIVE_SPRINT_STATE_CONFLICT",
                            "path": "active_sprint_id",
                            "message": "Multiple active managed sprint states exist",
                        }
                    ],
                )
            indexed = self._indexed_runtime_or_error(
                connection,
                control,
                project_id,
                correlation_id=attempt_id,
                phase="ACTIVATE",
            )
            if indexed is None:
                indexed_conflict = bool(active_rows)
            else:
                indexed_row, _ = indexed
                indexed_conflict = (
                    bool(active_rows)
                    and active_rows[0]["sprint_id"] != indexed_row["sprint_id"]
                )
                if indexed_row["status"] == "active" and not indexed_conflict:
                    raise ManagedImportError(
                        PROJECT_ACTIVATION_IN_PROGRESS,
                        409,
                        str(indexed_row["sprint_id"]),
                        phase="ACTIVATE",
                    )
                if (
                    indexed_row["status"] != "active"
                    and indexed_row["fencing_token"] >= created_fencing_token
                ):
                    indexed_conflict = True
            if indexed_conflict:
                raise ManagedImportError(
                    SPRINT_RECOVERY_REQUIRED,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    issues=[
                        {
                            "code": "ACTIVE_SPRINT_STATE_CONFLICT",
                            "path": "active_sprint_id",
                            "message": "Indexed managed sprint state cannot be replaced safely",
                        }
                    ],
                )

            updated_at = _timestamp(clock)
            for candidate in control["start_idempotency_records"]:
                if (
                    not isinstance(candidate, dict)
                    or candidate is record
                    or candidate.get("status")
                    not in {"VALIDATING", "PREPARING", "ACTIVATING"}
                ):
                    continue
                candidate_token = candidate.get("fencing_token")
                if (
                    not isinstance(candidate_token, int)
                    or isinstance(candidate_token, bool)
                    or candidate_token >= fencing_token
                ):
                    raise ManagedImportError(
                        SPRINT_RECOVERY_REQUIRED,
                        409,
                        attempt_id,
                        phase="ACTIVATE",
                        issues=[
                            {
                                "code": "ACTIVE_SPRINT_STATE_CONFLICT",
                                "path": "activation.fencing_token",
                                "message": "Concurrent managed attempt ordering is corrupt",
                            }
                        ],
                    )
                self._supersede_attempt(
                    candidate,
                    superseding_attempt_id=attempt_id,
                    superseding_sprint_id=sprint_id,
                    superseding_fencing_token=fencing_token,
                    updated_at=updated_at,
                )
                self._terminalize_superseded_runtime(
                    connection,
                    project_id,
                    control,
                    candidate,
                    updated_at=updated_at,
                )
            current_branch_snapshot = sorted(
                [
                    _branch_snapshot(lease)
                    for lease in branch_lease_store.active_writers_in_transaction(
                        connection
                    )
                ],
                key=lambda item: item["lease_id"],
            )
            current_resources = self._resource_snapshot_in_transaction(connection)
            external = (
                deepcopy(dict(external_snapshot_provider()))
                if external_snapshot_provider is not None
                else {}
            )
            external_observation = {
                "branch_leases": sorted(
                    deepcopy(external.get("branch_leases") or []),
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "port_leases": sorted(
                    deepcopy(external.get("port_leases") or []),
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "process_owners": sorted(
                    deepcopy(external.get("process_owners") or []),
                    key=lambda item: str(item.get("process_id") or ""),
                ),
            }
            current_snapshot = {
                "branch_leases": sorted(
                    [
                        *current_branch_snapshot,
                        *deepcopy(external.get("branch_leases") or []),
                    ],
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "port_leases": sorted(
                    [
                        *current_resources["port_leases"],
                        *deepcopy(external.get("port_leases") or []),
                    ],
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "process_owners": sorted(
                    [
                        *current_resources["process_owners"],
                        *deepcopy(external.get("process_owners") or []),
                    ],
                    key=lambda item: str(item.get("process_id") or ""),
                ),
            }
            frozen_snapshot = {
                "branch_leases": sorted(
                    deepcopy(expected_lease_snapshot.get("branch_leases") or []),
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "port_leases": sorted(
                    deepcopy(expected_lease_snapshot.get("port_leases") or []),
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "process_owners": sorted(
                    deepcopy(expected_lease_snapshot.get("process_owners") or []),
                    key=lambda item: str(item.get("process_id") or ""),
                ),
            }
            if canonical_json_sha256(current_snapshot) != canonical_json_sha256(
                frozen_snapshot
            ):
                raise ManagedImportError(
                    LEASE_SNAPSHOT_STALE,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    evidence={
                        "expected_lease_snapshot_sha256": canonical_json_sha256(
                            frozen_snapshot
                        ),
                        "actual_lease_snapshot_sha256": canonical_json_sha256(
                            current_snapshot
                        ),
                    },
                )
            state_branch_records = state.get("branch_leases")
            if not isinstance(state_branch_records, list):
                raise RuntimeError("managed runtime branch leases are invalid")
            planned_by_id = {
                str(item.get("lease_id")): item
                for item in state_branch_records
                if isinstance(item, Mapping)
            }
            acquired: list[BranchLease] = []
            for request in branch_lease_requests:
                planned = planned_by_id.get(request.lease_id)
                if not isinstance(planned, Mapping):
                    raise RuntimeError("managed runtime omitted a planned branch lease")
                acquired_at = planned.get("acquired_at")
                if not isinstance(acquired_at, str):
                    raise RuntimeError("planned branch lease has no acquisition time")
                try:
                    lease = branch_lease_store.acquire_write_in_transaction(
                        connection,
                        request,
                        acquired_at=acquired_at,
                    )
                except BranchLeaseError as exc:
                    raise ManagedImportError(
                        LEASE_SNAPSHOT_STALE,
                        409,
                        attempt_id,
                        phase="ACTIVATE",
                        issues=[
                            {
                                "code": exc.code,
                                "path": "lease_snapshot.branch_leases",
                                "message": "Branch ownership changed after preflight",
                            }
                        ],
                    ) from exc
                if _branch_runtime_record(lease) != dict(planned):
                    raise RuntimeError("acquired branch lease differs from activation plan")
                acquired.append(lease)

            state_port_records = state.get("port_leases")
            state_process_records = state.get("processes")
            if not isinstance(state_port_records, list) or not isinstance(
                state_process_records, list
            ):
                raise RuntimeError("managed runtime resource ownership is invalid")
            normalized_port_records: list[dict[str, Any]] = []
            for raw_lease in state_port_records:
                if not isinstance(raw_lease, Mapping):
                    raise RuntimeError("managed runtime port lease is invalid")
                lease = deepcopy(dict(raw_lease))
                host = lease.get("host")
                port = lease.get("port")
                if (
                    lease.get("status") != "reserved"
                    or not isinstance(host, str)
                    or not isinstance(port, int)
                    or isinstance(port, bool)
                ):
                    raise RuntimeError("managed runtime port lease is invalid")
                normalized_port_records.append(lease)
            try:
                reservation_token = port_reservations.acquire_many(
                    self.database_path,
                    normalized_port_records,
                )
            except ManagedPortReservationError as exc:
                raise ManagedImportError(
                    LEASE_SNAPSHOT_STALE,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    issues=[
                        {
                            "code": "LIVE_PORT_CONFLICT",
                            "path": "lease_snapshot.port_leases",
                            "message": "Planned child port is no longer available",
                        }
                    ],
                ) from exc
            rollback_actions.append(
                lambda token=reservation_token: port_reservations.rollback(token)
            )
            try:
                for lease in normalized_port_records:
                    host = lease.get("host")
                    port = lease.get("port")
                    if not port_reservations.owns_endpoint(
                        self.database_path, str(host), int(port)
                    ):
                        raise RuntimeError("managed port reservation was lost")
                    connection.execute(
                        """
                        INSERT INTO managed_port_leases(
                            lease_id, instance_id, network_namespace_id,
                            assignment_id, process_id, host, port, status,
                            lease_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            lease["lease_id"],
                            lease["instance_id"],
                            lease["network_namespace_id"],
                            lease["assignment_id"],
                            lease["process_id"],
                            host,
                            port,
                            lease["status"],
                            _json_text(lease),
                        ),
                    )
                for raw_process in state_process_records:
                    if not isinstance(raw_process, Mapping):
                        raise RuntimeError("managed runtime process owner is invalid")
                    process = deepcopy(dict(raw_process))
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
                            _json_text(process),
                        ),
                    )
            except sqlite3.IntegrityError as exc:
                raise ManagedImportError(
                    LEASE_SNAPSHOT_STALE,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    issues=[
                        {
                            "code": "PORT_LEASE_CONFLICT",
                            "path": "lease_snapshot.port_leases",
                            "message": "Runtime ownership changed after preflight",
                        }
                    ],
                ) from exc
            published_workspaces = [
                deepcopy(dict(item)) for item in workspace_publisher(acquired)
            ]
            if published_workspaces != state.get("workspaces"):
                raise RuntimeError(
                    "published workspaces differ from the activation plan"
                )
            # Compatibility observations which have not yet migrated into the
            # shared SQLite ownership tables are checked again at the final
            # mutation boundary.  Authoritative managed owners are acquired
            # above through SQLite uniqueness constraints; a volatile external
            # observer may only remain unchanged for this commit.
            latest_external = (
                deepcopy(dict(external_snapshot_provider()))
                if external_snapshot_provider is not None
                else {}
            )
            normalized_latest_external = {
                "branch_leases": sorted(
                    deepcopy(latest_external.get("branch_leases") or []),
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "port_leases": sorted(
                    deepcopy(latest_external.get("port_leases") or []),
                    key=lambda item: str(item.get("lease_id") or ""),
                ),
                "process_owners": sorted(
                    deepcopy(latest_external.get("process_owners") or []),
                    key=lambda item: str(item.get("process_id") or ""),
                ),
            }
            if canonical_json_sha256(
                normalized_latest_external
            ) != canonical_json_sha256(external_observation):
                raise ManagedImportError(
                    LEASE_SNAPSHOT_STALE,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    evidence={
                        "expected_external_snapshot_sha256": canonical_json_sha256(
                            external_observation
                        ),
                        "actual_external_snapshot_sha256": canonical_json_sha256(
                            normalized_latest_external
                        ),
                    },
                )
            existing = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, sprint_id),
            ).fetchone()
            if existing is not None:
                try:
                    existing_state = json.loads(existing["state_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    raise RuntimeError("managed sprint runtime is corrupt") from exc
                existing_issues = self._runtime_row_issues(
                    control,
                    existing,
                    existing_state,
                    require_indexed=False,
                )
                if existing_issues or not isinstance(existing_state, dict):
                    raise RuntimeError(
                        "managed sprint runtime invariant failed: "
                        + ",".join(
                            existing_issues or ("RUNTIME_STATE_NOT_OBJECT",)
                        )
                    )
                state = self._merge_preactivation_history(
                    existing_state,
                    state,
                    attempt_id=attempt_id,
                )
                merged_schema_issues = _runtime_state_schema_errors(
                    state,
                    issue_code="ACTIVATION_SCHEMA_INVALID",
                )
                merged_invariant_issues = managed_activation_invariant_issues(
                    state
                )
                if merged_schema_issues or merged_invariant_issues:
                    issue_codes = [
                        str(item.get("code"))
                        for item in merged_schema_issues
                    ] + list(merged_invariant_issues)
                    raise RuntimeError(
                        "managed activation history merge is invalid: "
                        + ",".join(dict.fromkeys(issue_codes))
                    )
                connection.execute(
                    """
                    UPDATE managed_sprints
                    SET status = ?, fencing_token = ?, state_json = ?
                    WHERE project_id = ? AND sprint_id = ?
                    """,
                    (
                        state["status"],
                        record["fencing_token"],
                        _json_text(state),
                        project_id,
                        sprint_id,
                    ),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO managed_sprints(
                        project_id, sprint_id, status, fencing_token, state_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        project_id,
                        sprint_id,
                        state["status"],
                        record["fencing_token"],
                        _json_text(state),
                    ),
                )
            record.update(
                {
                    "status": "SUCCEEDED",
                    "response": deepcopy(dict(response)),
                    "http_status": 201,
                    "error": None,
                    "updated_at": _timestamp(clock),
                }
            )
            control["active_sprint_id"] = sprint_id
            control["activation_lease"] = None
            self._write_control(connection, control, revision)
        # The sockets were provisional while SQLite could still roll back.
        # Publish their durable generation only after the commit succeeds, so
        # a reconciliation based on an older DB snapshot cannot discard them.
        port_reservations.mark_durable_many(
            self.database_path,
            normalized_port_records,
        )
        return True


def _repository_assertion_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    remote = canonical_remote_from_address(value)
    if remote is not None:
        return remote
    if re.match(r"^[A-Za-z]:[\\/]", value):
        return "local:" + os.path.normcase(os.path.normpath(value))
    return None


def _workspace_record(workspace: ManagedWorkspace) -> dict[str, Any]:
    return {
        "workspace_id": workspace.workspace_id,
        "project_id": workspace.project_id,
        "sprint_id": workspace.sprint_id,
        "node_id": workspace.node_id,
        "assignment_id": workspace.assignment_id,
        "expected_root": str(workspace.expected_root),
        "actual_git_toplevel": str(workspace.actual_git_toplevel),
        "actual_git_dir": str(workspace.actual_git_dir),
        "repository_id": workspace.repository_id,
        "repository_remote": workspace.repository_remote,
        "source_commit": workspace.source_commit,
        "initial_head_commit": workspace.initial_head_commit,
        "assigned_branch": workspace.assigned_branch,
        "working_tree_state": workspace.working_tree_state,
    }


def _managed_workspace(record: Mapping[str, Any], mirror_key: str) -> ManagedWorkspace:
    return ManagedWorkspace(
        workspace_id=str(record["workspace_id"]),
        project_id=str(record["project_id"]),
        sprint_id=str(record["sprint_id"]),
        node_id=str(record["node_id"]),
        assignment_id=str(record["assignment_id"]),
        repository_id=str(record["repository_id"]),
        repository_remote=str(record["repository_remote"]),
        mirror_storage_key=mirror_key,
        expected_root=Path(str(record["expected_root"])),
        actual_git_toplevel=Path(str(record["actual_git_toplevel"])),
        actual_git_dir=Path(str(record["actual_git_dir"])),
        source_commit=str(record["source_commit"]),
        initial_head_commit=str(record["initial_head_commit"]),
        head_commit=str(record["initial_head_commit"]),
        assigned_branch=(
            str(record["assigned_branch"])
            if record.get("assigned_branch") is not None
            else None
        ),
        access="write" if record.get("assigned_branch") is not None else "read",
        working_tree_state=str(record["working_tree_state"]),
        branch_lease_id=(
            str(record["branch_lease_id"])
            if record.get("branch_lease_id") is not None
            else None
        ),
    )


def managed_workspace_record(workspace: ManagedWorkspace) -> dict[str, Any]:
    """Serialize a verified workspace into the frozen runtime projection."""

    return _workspace_record(workspace)


def managed_workspace_from_record(
    record: Mapping[str, Any], mirror_key: str
) -> ManagedWorkspace:
    """Rebuild a verified workspace value from its durable projection."""

    return _managed_workspace(record, mirror_key)


def _branch_snapshot(lease: BranchLease) -> dict[str, Any]:
    return {
        "lease_id": lease.lease_id,
        "repository_id": lease.repository_id,
        "repository_key": lease.repository_key,
        "mirror_storage_key": lease.mirror_storage_key,
        "branch": lease.branch,
        "assignment_id": lease.assignment_id,
        "mode": lease.mode,
        "status": lease.status,
    }


def _branch_runtime_record(lease: BranchLease) -> dict[str, Any]:
    return {
        "lease_id": lease.lease_id,
        "repository_id": lease.repository_id,
        "repository_key": lease.repository_key,
        "mirror_storage_key": lease.mirror_storage_key,
        "branch": lease.branch,
        "assignment_id": lease.assignment_id,
        "source_commit": lease.source_commit,
        "initial_head_commit": lease.initial_head_commit,
        "mode": lease.mode,
        "status": lease.status,
        "acquired_at": lease.acquired_at,
        "released_at": lease.released_at,
    }


class TransactionalSprintImporter:
    """Execute the frozen VALIDATE -> PREPARE -> ACTIVATE protocol."""

    def __init__(
        self,
        runtime_config: Mapping[str, Any],
        repository_registry: Mapping[str, RepositorySpec | Mapping[str, object]],
        *,
        store: ManagedImportStore | None = None,
        git_provider: ManagedGitProvider | None = None,
        branch_leases: BranchLeaseStore | None = None,
        workspace_manager: ManagedWorkspaceManager | None = None,
        lease_snapshot_provider: Callable[[], Mapping[str, Any]] | None = None,
        port_probe: Callable[[str, int], bool] | None = None,
        port_reservations: ManagedPortReservationRegistry | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        attempt_id_factory: Callable[[], str] = lambda: f"attempt-{uuid4().hex}",
        fault_injector: Callable[[str, Mapping[str, Any]], None] | None = None,
        credential_resolver: Callable[[str], Mapping[str, str]] | None = None,
        allow_local_transport: bool = False,
    ) -> None:
        self.runtime_config = deepcopy(dict(runtime_config))
        self.clock = clock
        self.attempt_id_factory = attempt_id_factory
        self.fault_injector = fault_injector
        managed_root = self.runtime_config.get("managed_root")
        lease_root = self.runtime_config.get("lease_root")
        if not isinstance(managed_root, str) or not isinstance(lease_root, str):
            raise ValueError("runtime_config must contain managed_root and lease_root")
        self.store = store or ManagedImportStore(
            Path(lease_root) / "managed-import.sqlite3"
        )
        self.git_provider = git_provider or ManagedGitProvider(
            managed_root,
            repository_registry,
            credential_resolver=credential_resolver,
            command_timeout=float(
                self.runtime_config.get("git_fetch_timeout_seconds") or 120
            ),
            allow_local_transport=allow_local_transport,
        )
        self._branch_leases = branch_leases
        self._workspace_manager = workspace_manager
        if workspace_manager is not None and branch_leases is None:
            self._branch_leases = workspace_manager.branch_leases
        self._lease_root = lease_root
        self._managed_root = managed_root
        self.lease_snapshot_provider = lease_snapshot_provider
        self.port_probe = port_probe or self._port_available
        self.port_reservations = (
            port_reservations or _DEFAULT_MANAGED_PORT_RESERVATIONS
        )
        if (
            self._branch_leases is not None
            and self._branch_leases.database_path.resolve(strict=False)
            != self.store.database_path.resolve(strict=False)
        ):
            raise ValueError(
                "managed branch leases must share the activation database"
            )

    @property
    def branch_leases(self) -> BranchLeaseStore:
        if self._branch_leases is None:
            self._branch_leases = BranchLeaseStore(
                self.store.database_path.parent,
                database_name=self.store.database_path.name,
                timeout=self.store.timeout,
            )
        return self._branch_leases

    @property
    def workspace_manager(self) -> ManagedWorkspaceManager:
        if self._workspace_manager is None:
            self._workspace_manager = ManagedWorkspaceManager(
                self._managed_root,
                self.git_provider,
                self.branch_leases,
                protected_roots=tuple(
                    self.runtime_config.get("protected_roots") or ()
                ),
                install_roots=tuple(
                    dict.fromkeys(
                        (
                            self.runtime_config["service_root"],
                            str(Path(sys.prefix).resolve(strict=False)),
                            str(Path(sys.base_prefix).resolve(strict=False)),
                        )
                    )
                ),
            )
        return self._workspace_manager

    @staticmethod
    def _port_available(host: str, port: int) -> bool:
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        with socket.socket(family, socket.SOCK_STREAM) as candidate:
            candidate.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            try:
                candidate.bind((host, port))
            except OSError:
                return False
        return True

    @staticmethod
    def _branch_ref_paths_fit_windows_budget(
        repository: ManagedRepository,
        expected_root: Path,
        branch: str,
    ) -> bool:
        """Guard Git-for-Windows loose-ref/reflog paths before PREPARE."""

        components = PurePosixPath(branch).parts
        preparation_root = expected_root.parent / (".p-" + "0" * 24)
        candidates: list[Path] = []
        for git_directory in (preparation_root / ".git", repository.mirror_path):
            ref_path = git_directory.joinpath("refs", "heads", *components)
            candidates.extend(
                (
                    Path(str(ref_path) + ".lock"),
                    git_directory.joinpath("logs", "refs", "heads", *components),
                )
            )
        return all(
            len(str(candidate).encode("utf-16-le")) // 2 < 260
            for candidate in candidates
        )

    def _fault(self, point: str, **context: Any) -> None:
        if self.fault_injector is not None:
            self.fault_injector(point, context)

    def _common_forbidden_roots(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    *tuple(self.runtime_config.get("protected_roots") or ()),
                    self.runtime_config["service_root"],
                    str(Path(sys.prefix).resolve(strict=False)),
                    str(Path(sys.base_prefix).resolve(strict=False)),
                )
            )
        )

    def _assert_control_store_root_safe(self) -> None:
        """Validate the SQLite mutation path before any schema initialization."""

        common_forbidden = self._common_forbidden_roots()
        runtime_root = ManagedWorkspaceManager.validate_isolated_root(
            self.runtime_config["runtime_root"],
            protected_roots=common_forbidden,
        )
        lease_root = ManagedWorkspaceManager.validate_isolated_root(
            self.runtime_config["lease_root"],
            protected_roots=common_forbidden,
        )
        expected_lease_root = (runtime_root / "leases").resolve(strict=False)
        if os.path.normcase(str(lease_root)) != os.path.normcase(
            str(expected_lease_root)
        ):
            raise WorkspaceError(
                "RUNTIME_ROOT_NOT_ISOLATED",
                "lease root is not the runtime lease directory",
            )
        if os.path.normcase(str(self.store.database_path.parent)) != os.path.normcase(
            str(lease_root)
        ):
            raise WorkspaceError(
                "RUNTIME_ROOT_NOT_ISOLATED",
                "managed control database escaped the lease root",
            )

    def _assert_mutation_roots_safe(self) -> None:
        # These checks are filesystem-read-only.  The narrower control-store
        # check runs before lookup; all remaining roots are required before a
        # new or resumable attempt may touch Git or prepared artifacts.
        self._assert_control_store_root_safe()
        path_issues = [
            code
            for code in managed_runtime_config_invariant_issues(self.runtime_config)
            if "PORT" not in code and "ENDPOINT" not in code
        ]
        if path_issues:
            raise WorkspaceError(
                "RUNTIME_ROOT_NOT_ISOLATED",
                "managed runtime path relations are invalid",
            )
        common_forbidden = tuple(
            dict.fromkeys(
                (
                    *self._common_forbidden_roots(),
                    self.runtime_config["prompt_root"],
                    self.runtime_config["managed_root"],
                )
            )
        )
        for name in (
            "runtime_root",
            "process_runtime_root",
            "log_root",
            "pid_root",
            "lease_root",
        ):
            ManagedWorkspaceManager.validate_isolated_root(
                self.runtime_config[name],
                protected_roots=common_forbidden,
            )
        ManagedWorkspaceManager.validate_isolated_root(
            self.runtime_config["prompt_root"],
            protected_roots=(
                *self._common_forbidden_roots(),
                self.runtime_config["runtime_root"],
                self.runtime_config["managed_root"],
            ),
        )
        ManagedWorkspaceManager.validate_isolated_root(
            self.runtime_config["managed_root"],
            protected_roots=(
                *self._common_forbidden_roots(),
                self.runtime_config["runtime_root"],
                self.runtime_config["prompt_root"],
            ),
        )
        self.workspace_manager

    def _snapshot(self) -> dict[str, Any]:
        extra = (
            deepcopy(dict(self.lease_snapshot_provider()))
            if self.lease_snapshot_provider is not None
            else {}
        )
        durable = self.store.resource_snapshot()
        branch = [_branch_snapshot(lease) for lease in self.branch_leases.active_writers()]
        if extra.get("branch_leases"):
            branch.extend(deepcopy(extra["branch_leases"]))
        snapshot = {
            "branch_leases": sorted(branch, key=lambda item: item["lease_id"]),
            "port_leases": sorted(
                [
                    *deepcopy(durable.get("port_leases") or []),
                    *deepcopy(extra.get("port_leases") or []),
                ],
                key=lambda item: item.get("lease_id", ""),
            ),
            "process_owners": sorted(
                [
                    *deepcopy(durable.get("process_owners") or []),
                    *deepcopy(extra.get("process_owners") or []),
                ],
                key=lambda item: item.get("process_id", ""),
            ),
        }
        return snapshot

    @staticmethod
    def _entry_nodes(manifest: Mapping[str, Any]) -> list[str]:
        execution = manifest["execution"]
        if execution["mode"] == "sequential":
            return [execution["start_node"]]
        return list(execution["start_nodes"])

    @staticmethod
    def _task_nodes(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
        return {
            str(node["id"]): node
            for node in manifest["nodes"]
            if isinstance(node, Mapping) and node.get("type", "task") != "terminal"
        }

    def _process_plan(
        self,
        manifest: Mapping[str, Any],
        node: Mapping[str, Any],
        *,
        sprint_id: str,
        assignment_id: str,
        workspace_id: str,
        frozen: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
        workspace = node.get("workspace")
        launch = workspace.get("process") if isinstance(workspace, Mapping) else None
        if not isinstance(launch, Mapping):
            return None, []
        issues: list[dict[str, str]] = []
        frozen_process = (
            frozen.get("process_plan") if isinstance(frozen, Mapping) else None
        )
        policy = manifest.get("process_policy")
        policy = policy if isinstance(policy, Mapping) else {}
        command = launch.get("command")
        executable = (
            str(frozen_process.get("executable_path"))
            if isinstance(frozen_process, Mapping)
            and isinstance(frozen_process.get("executable_path"), str)
            else None
        )
        if (
            executable is None
            and isinstance(command, list)
            and command
            and isinstance(command[0], str)
        ):
            raw_executable = command[0]
            if Path(raw_executable).is_absolute():
                executable = str(Path(raw_executable).resolve(strict=False))
            else:
                found = shutil.which(raw_executable)
                if found:
                    executable = str(Path(found).resolve(strict=False))
        if executable is None or windows_absolute_path_key(executable) is None:
            issues.append(
                {
                    "code": "RESOURCE_LIMIT_UNSUPPORTED",
                    "path": f"nodes[{node.get('id')}].workspace.process.command[0]",
                    "message": "Process executable cannot be resolved safely",
                }
            )
            executable = str(Path(sys.executable).resolve(strict=False))
        if any(
            isinstance(root, str) and windows_path_is_within(executable, root)
            for root in self.runtime_config.get("protected_roots", [])
        ):
            issues.append(
                {
                    "code": "WORKSPACE_ROOT_FORBIDDEN",
                    "path": f"nodes[{node.get('id')}].workspace.process.command[0]",
                    "message": "Process executable overlaps a protected root",
                }
            )

        restart_policy = launch.get(
            "restart_policy", policy.get("restart_policy", "never")
        )
        max_restarts = launch.get(
            "max_restart_attempts", policy.get("max_restart_attempts")
        )
        if max_restarts is None:
            max_restarts = 3 if restart_policy == "on_failure" else 0
        backoff = launch.get(
            "restart_backoff_seconds",
            policy.get("restart_backoff_seconds", 5),
        )
        health_path = launch.get("health_path", policy.get("health_path", "/health"))
        limits = {
            "wall_time_seconds": 3600,
            "memory_bytes": 2147483648,
            "cpu_percent": 100,
            "process_count": 16,
        }
        policy_limits = policy.get("resource_limits")
        launch_limits = launch.get("resource_limits")
        if isinstance(launch_limits, Mapping):
            limits.update(launch_limits)
        elif isinstance(policy_limits, Mapping):
            limits.update(policy_limits)
        if not _aggregate_process_limits_supported():
            issues.append(
                {
                    "code": "RESOURCE_LIMIT_UNSUPPORTED",
                    "path": (
                        f"nodes[{node.get('id')}].workspace.process.resource_limits"
                    ),
                    "message": (
                        "This platform has no assignment-wide hard-limit backend"
                    ),
                }
            )
        if (
            os.name == "nt"
            and isinstance(limits.get("cpu_percent"), int)
            and not isinstance(limits.get("cpu_percent"), bool)
            and int(limits["cpu_percent"]) > 100
        ):
            issues.append(
                {
                    "code": "RESOURCE_LIMIT_UNSUPPORTED",
                    "path": (
                        f"nodes[{node.get('id')}].workspace.process."
                        "resource_limits.cpu_percent"
                    ),
                    "message": (
                        "Windows Job CPU hard-cap supports cpu_percent "
                        "values from 1 to 100"
                    ),
                }
            )
        if (
            isinstance(limits.get("memory_bytes"), int)
            and not isinstance(limits.get("memory_bytes"), bool)
            and int(limits["memory_bytes"]) > sys.maxsize
        ):
            issues.append(
                {
                    "code": "RESOURCE_LIMIT_UNSUPPORTED",
                    "path": (
                        f"nodes[{node.get('id')}].workspace.process."
                        "resource_limits.memory_bytes"
                    ),
                    "message": (
                        "Process memory limit exceeds this platform's "
                        "representable resource-limit range"
                    ),
                }
            )
        process_id = _safe_identifier("process", sprint_id, assignment_id, "0")
        port_lease_id = _safe_identifier("port-lease", process_id)
        launch_nonce = (
            frozen_process.get("launch_nonce")
            if isinstance(frozen_process, Mapping)
            and isinstance(frozen_process.get("launch_nonce"), str)
            else uuid4().hex
        )
        calculated = {
                "process_id": process_id,
                "assignment_id": assignment_id,
                "workspace_id": workspace_id,
                "port_lease_id": port_lease_id,
                "launch_nonce": launch_nonce,
                "executable_path": executable,
                "command_redacted": deepcopy(command),
                "environment_redacted": deepcopy(launch.get("environment")),
                "cwd_relative": launch.get("cwd"),
                "health_path": health_path,
                "resource_limits": limits,
                "restart_policy": restart_policy,
                "max_restart_attempts": max_restarts,
                "restart_backoff_seconds": backoff,
            }
        if isinstance(frozen_process, Mapping) and dict(frozen_process) != calculated:
            issues.append(
                {
                    "code": "ACTIVATION_INVARIANT_FAILED",
                    "path": f"nodes[{node.get('id')}].workspace.process",
                    "message": "Frozen process activation plan changed after restart",
                }
            )
            return deepcopy(dict(frozen_process)), issues
        return calculated, issues

    def _activation_plan(
        self,
        project_id: str,
        provenance: SprintProvenance,
        manifest: Mapping[str, Any],
        repository: ManagedRepository,
        workspace_source_commit: str,
        snapshot: Mapping[str, Any],
        *,
        frozen_plans: Sequence[Mapping[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        issues: list[dict[str, str]] = []
        task_nodes = self._task_nodes(manifest)
        plans: list[dict[str, Any]] = []
        frozen_by_node = {
            str(item.get("node_id")): item
            for item in (frozen_plans or ())
            if isinstance(item, Mapping)
        }
        active_writers = {
            (str(item.get("mirror_storage_key")), str(item.get("branch")).casefold()): item
            for item in snapshot.get("branch_leases", [])
            if isinstance(item, Mapping) and item.get("status") == "active"
        }
        required_ports = 0
        for index, node_id in enumerate(self._entry_nodes(manifest)):
            node = task_nodes[node_id]
            frozen = frozen_by_node.get(node_id)
            occurrence_id = managed_occurrence_id(
                provenance.stable_sprint_id(), 1, node_id, 1, []
            )
            assignment_id = _assignment_id(provenance.stable_sprint_id(), node_id)
            workspace = node["workspace"]
            access = workspace["access"]
            node_git = workspace.get("git") if isinstance(workspace, Mapping) else None
            git_policy = node_git if isinstance(node_git, Mapping) else manifest["git"]
            assigned_branch = git_policy["assigned_branch"] if access == "write" else None
            policy = git_policy["existing_branch_policy"] if access == "write" else None
            expected_head = git_policy.get("expected_branch_head") if access == "write" else None
            initial_head = workspace_source_commit
            if access == "write":
                if frozen is not None:
                    initial_head = str(
                        frozen.get("initial_head_commit") or workspace_source_commit
                    )
                else:
                    key = (repository.mirror_storage_key, assigned_branch.casefold())
                    if key in active_writers:
                        issues.append(
                            {
                                "code": "BRANCH_ALREADY_LEASED",
                                "path": f"nodes[{node_id}].workspace.git.assigned_branch",
                                "message": "Assigned branch already has an active writer",
                            }
                        )
                    try:
                        raw_heads = self.git_provider.branch_heads(
                            repository, assigned_branch
                        )
                        initial_head = select_initial_head(
                            policy=policy,
                            source_commit=workspace_source_commit,
                            expected_branch_head=expected_head,
                            heads=BranchHeads(
                                local_head=raw_heads.get(
                                    f"refs/heads/{assigned_branch}"
                                ),
                                remote_head=raw_heads.get(
                                    f"refs/remotes/origin/{assigned_branch}"
                                ),
                            ),
                            is_ancestor=lambda ancestor, descendant: self.git_provider.is_ancestor(
                                repository, ancestor, descendant
                            ),
                        )
                    except (ManagedGitError, RuntimeError) as exc:
                        if (
                            isinstance(exc, ManagedGitError)
                            and exc.code in TRANSIENT_GIT_ERROR_CODES
                        ):
                            raise
                        code = getattr(exc, "code", "BRANCH_DIVERGED")
                        issues.append(
                            {
                                "code": code,
                                "path": f"nodes[{node_id}].workspace.git",
                                "message": "Assigned branch policy could not be satisfied",
                            }
                        )
            request = WorkspaceRequest(
                project_id=project_id,
                sprint_id=provenance.stable_sprint_id(),
                node_id=node_id,
                assignment_id=assignment_id,
                repository_id=provenance.repository_id,
                source_commit=workspace_source_commit,
                access=access,
                assigned_branch=assigned_branch,
                existing_branch_policy=policy,
                expected_branch_head=expected_head,
            )
            try:
                expected_root = self.workspace_manager.expected_root(request)
            except WorkspaceError as exc:
                issues.append(
                    {
                        "code": exc.code,
                        "path": f"nodes[{node_id}].workspace",
                        "message": "Candidate workspace root is not isolated",
                    }
                )
                expected_root = Path(self.runtime_config["managed_root"]) / "invalid"
            if (
                access == "write"
                and isinstance(assigned_branch, str)
                and not self._branch_ref_paths_fit_windows_budget(
                    repository, expected_root, assigned_branch
                )
            ):
                issues.append(
                    {
                        "code": "GIT_BRANCH_INVALID",
                        "path": f"nodes[{node_id}].workspace.git.assigned_branch",
                        "message": "Assigned branch exceeds the Windows ref-path budget",
                    }
                )
            transitions = list(node["transitions"])
            allowed = transitions + [
                outcome
                for outcome in ("STOP", "NEED_DECISION")
                if outcome not in transitions
            ]
            plan = {
                "index": index,
                "node_id": node_id,
                "node": deepcopy(dict(node)),
                "occurrence_id": occurrence_id,
                "assignment_id": assignment_id,
                "workspace_id": _workspace_id(request),
                "workspace_request": request,
                "expected_root": str(expected_root),
                "initial_head_commit": initial_head,
                "allowed_outcomes": allowed,
            }
            process_plan, process_issues = self._process_plan(
                manifest,
                node,
                sprint_id=provenance.stable_sprint_id(),
                assignment_id=assignment_id,
                workspace_id=plan["workspace_id"],
                frozen=frozen,
            )
            issues.extend(process_issues)
            if process_plan is not None:
                required_ports += 1
                plan["process_plan"] = process_plan
            plans.append(plan)

        if frozen_plans is not None:
            for plan in plans:
                frozen = frozen_by_node.get(str(plan["node_id"]))
                if isinstance(frozen, Mapping) and isinstance(
                    frozen.get("planned_port"), int
                ):
                    plan["planned_port"] = frozen["planned_port"]
            return plans, issues

        leased_ports = {
            int(item["port"])
            for item in snapshot.get("port_leases", [])
            if isinstance(item, Mapping)
            and item.get("status") in {"reserved", "bound"}
            and isinstance(item.get("port"), int)
        }
        available_ports: list[int] = []
        for port in range(
            int(self.runtime_config.get("child_port_start") or 0),
            int(self.runtime_config.get("child_port_end") or -1) + 1,
        ):
            if port in leased_ports:
                continue
            if self.port_probe("127.0.0.1", port):
                available_ports.append(port)
                if len(available_ports) >= required_ports:
                    break
        if len(available_ports) < required_ports:
            issues.append(
                {
                    "code": "LIVE_PORT_CONFLICT",
                    "path": "runtime_config.child_port_start",
                    "message": "The child port range has insufficient free ports",
                }
            )
        for plan, port in zip(
            (plan for plan in plans if plan.get("process_plan") is not None),
            available_ports,
        ):
            plan["planned_port"] = port
        return plans, issues

    def _preflight(
        self,
        project_id: str,
        provenance: SprintProvenance,
        manifest: Mapping[str, Any],
        repository: ManagedRepository,
        workspace_source_commit: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        issues: list[dict[str, str]] = []
        for issue in _schema_errors(
            self.runtime_config,
            "managed-runtime-config-v1.schema.json",
            issue_code="RUNTIME_CONFIG_SCHEMA_INVALID",
        ):
            issues.append(
                {
                    "code": "RUNTIME_ROOT_NOT_ISOLATED",
                    "path": issue["path"] or "runtime_config",
                    "message": "Managed runtime configuration is invalid",
                }
            )
        config_issues = managed_runtime_config_invariant_issues(self.runtime_config)
        for code in config_issues:
            normalized = (
                "PORT_RANGE_INVALID"
                if "PORT" in code
                else "RUNTIME_ROOT_NOT_ISOLATED"
            )
            issues.append(
                {
                    "code": normalized,
                    "path": "runtime_config",
                    "message": "Managed runtime configuration is not isolated",
                }
            )
        assertion = manifest.get("git_address")
        if assertion is not None and _repository_assertion_key(assertion) != repository.canonical_remote:
            issues.append(
                {
                    "code": "REPOSITORY_IDENTITY_MISMATCH",
                    "path": "git_address",
                    "message": "Manifest repository assertion does not match the registry",
                }
            )
        for index, artifact in enumerate(manifest.get("files", [])):
            try:
                content = self.git_provider.read_blob(
                    repository,
                    workspace_source_commit,
                    artifact["path"],
                    max_bytes=_ARTIFACT_MAX_BYTES,
                )
                digest = hashlib.sha256(content).hexdigest()
            except ManagedGitError as exc:
                if exc.code != "GIT_BLOB_NOT_FOUND":
                    raise
                digest = None
            if digest != artifact.get("sha256"):
                issues.append(
                    {
                        "code": "MANIFEST_CHECKSUM_MISMATCH",
                        "path": f"files[{index}]",
                        "message": "Manifest file checksum does not match the pinned commit",
                    }
                )
        graph_issue_codes = managed_graph_semantic_issues(manifest)
        for code in graph_issue_codes:
            issues.append(
                {
                    "code": code,
                    "path": "nodes",
                    "message": "Managed graph semantic validation failed",
                }
            )
        snapshot = self._snapshot()
        if graph_issue_codes:
            plans, plan_issues = [], []
        else:
            plans, plan_issues = self._activation_plan(
                project_id,
                provenance,
                manifest,
                repository,
                workspace_source_commit,
                snapshot,
            )
        issues.extend(plan_issues)
        report = {
            "schema_version": 1,
            "contract_version": 1,
            "manifest_schema_version": 1,
            "sprint_id": provenance.stable_sprint_id(),
            "manifest_commit": provenance.commit,
            "workspace_source_commit": workspace_source_commit,
            "manifest_sha256": provenance.manifest_sha256,
            "lease_snapshot": snapshot,
            "lease_snapshot_sha256": canonical_json_sha256(snapshot),
            "ok": not issues,
            "phase": "VALIDATE",
            "issues": issues,
            "checked_at": _timestamp(self.clock),
        }
        report_schema_issues = _schema_errors(
            report,
            "sprint-preflight-report-v1.schema.json",
            issue_code="PREFLIGHT_REPORT_SCHEMA_INVALID",
        )
        if report_schema_issues:
            raise RuntimeError("generated preflight report is invalid")
        return report, plans

    def _prepare(
        self,
        project_id: str,
        attempt_id: str,
        plans: Sequence[Mapping[str, Any]],
        repository: ManagedRepository,
        record: Mapping[str, Any],
        fencing_token: int,
    ) -> list[dict[str, Any]]:
        evidence = record.get("evidence")
        prepared = (
            deepcopy(evidence.get("prepared_workspaces"))
            if isinstance(evidence, Mapping)
            and isinstance(evidence.get("prepared_workspaces"), list)
            else []
        )
        prepared_by_id = {
            item.get("workspace_id"): item for item in prepared if isinstance(item, dict)
        }
        results: list[dict[str, Any]] = []
        for plan in plans:
            request = plan["workspace_request"]
            frozen = prepared_by_id.get(plan["workspace_id"])
            expected = (
                _managed_workspace(frozen, repository.mirror_storage_key)
                if isinstance(frozen, Mapping)
                else None
            )
            workspace = self.workspace_manager.prepare(
                request,
                repository=repository,
                expected_record=expected,
                publish_write_lease=request.access != "write",
                selected_initial_head=(
                    str(plan["initial_head_commit"])
                    if request.access == "write"
                    else None
                ),
            )
            serialized = _workspace_record(workspace)
            results.append(serialized)
            if frozen is None:
                durable_prepared = list(prepared_by_id.values()) + [serialized]
                record = self.store.update_attempt(
                    project_id,
                    attempt_id,
                    evidence_update={"prepared_workspaces": durable_prepared},
                    fencing_token=fencing_token,
                    clock=self.clock,
                )
                prepared_by_id[workspace.workspace_id] = serialized
            self._fault(
                "after_workspace",
                attempt_id=attempt_id,
                workspace_id=workspace.workspace_id,
            )
        return results

    def _publish_workspaces(
        self,
        plans: Sequence[Mapping[str, Any]],
        workspaces: Sequence[Mapping[str, Any]],
        repository: ManagedRepository,
        attempt_id: str,
        leases: Sequence[BranchLease],
    ) -> list[dict[str, Any]]:
        workspace_by_id = {
            str(item["workspace_id"]): item for item in workspaces
        }
        lease_by_assignment = {lease.assignment_id: lease for lease in leases}
        published: list[dict[str, Any]] = []
        for plan in plans:
            raw = workspace_by_id[str(plan["workspace_id"])]
            request = plan["workspace_request"]
            if request.access == "write":
                assert request.assigned_branch is not None
                workspace = _managed_workspace(raw, repository.mirror_storage_key)
                lease = lease_by_assignment.get(request.assignment_id)
                if lease is None:
                    raise RuntimeError("activation omitted a write-workspace lease")
                workspace = self.workspace_manager.publish_write_workspace(
                    request,
                    workspace,
                    repository=repository,
                    transactional_lease=lease,
                    publish_mirror_ref=False,
                )
                serialized = _workspace_record(workspace)
                if workspace.branch_lease_id is None:
                    raise RuntimeError("published write workspace has no branch lease")
                serialized["branch_lease_id"] = workspace.branch_lease_id
            else:
                serialized = deepcopy(dict(raw))
            published.append(serialized)
            self._fault(
                "after_workspace_publish",
                attempt_id=attempt_id,
                workspace_id=serialized["workspace_id"],
            )
        return published

    @staticmethod
    def _planned_branch_leases(
        plans: Sequence[Mapping[str, Any]],
        repository: ManagedRepository,
        *,
        acquired_at: str,
    ) -> tuple[list[BranchLeaseRequest], list[BranchLease]]:
        requests: list[BranchLeaseRequest] = []
        leases: list[BranchLease] = []
        for plan in plans:
            workspace_request = plan.get("workspace_request")
            if (
                not isinstance(workspace_request, WorkspaceRequest)
                or workspace_request.access != "write"
            ):
                continue
            branch = workspace_request.assigned_branch
            if not isinstance(branch, str):
                raise RuntimeError("write workspace omitted its assigned branch")
            lease_id = deterministic_branch_lease_id(
                repository.mirror_storage_key,
                branch,
                workspace_request.assignment_id,
            )
            request = BranchLeaseRequest(
                lease_id=lease_id,
                repository_id=workspace_request.repository_id,
                repository_key=repository.canonical_remote,
                mirror_storage_key=repository.mirror_storage_key,
                branch=branch,
                assignment_id=workspace_request.assignment_id,
                source_commit=workspace_request.source_commit,
                initial_head_commit=str(plan["initial_head_commit"]),
            )
            requests.append(request)
            leases.append(
                BranchLease(
                    lease_id=lease_id,
                    repository_id=request.repository_id,
                    repository_key=request.repository_key,
                    mirror_storage_key=request.mirror_storage_key,
                    branch=request.branch,
                    branch_key=request.branch.casefold(),
                    assignment_id=request.assignment_id,
                    source_commit=request.source_commit,
                    initial_head_commit=request.initial_head_commit,
                    mode="write",
                    status="active",
                    acquired_at=acquired_at,
                    released_at=None,
                )
            )
        return requests, leases

    @staticmethod
    def _activated_workspace_records(
        plans: Sequence[Mapping[str, Any]],
        workspaces: Sequence[Mapping[str, Any]],
        leases: Sequence[BranchLease],
    ) -> list[dict[str, Any]]:
        plan_by_workspace = {
            str(plan["workspace_id"]): plan for plan in plans
        }
        lease_by_assignment = {lease.assignment_id: lease for lease in leases}
        activated: list[dict[str, Any]] = []
        for raw in workspaces:
            record = deepcopy(dict(raw))
            plan = plan_by_workspace[str(record["workspace_id"])]
            request = plan["workspace_request"]
            if request.access == "write":
                lease = lease_by_assignment.get(request.assignment_id)
                if lease is None:
                    raise RuntimeError("write workspace omitted its planned lease")
                record["branch_lease_id"] = lease.lease_id
            activated.append(record)
        return activated

    def _planned_runtime_resources(
        self,
        plans: Sequence[Mapping[str, Any]],
        workspaces: Sequence[Mapping[str, Any]],
        *,
        acquired_at: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
        workspace_by_id = {
            str(item["workspace_id"]): item for item in workspaces
        }
        port_leases: list[dict[str, Any]] = []
        processes: list[dict[str, Any]] = []
        issues: list[dict[str, str]] = []
        for plan in plans:
            process_plan = plan.get("process_plan")
            if not isinstance(process_plan, Mapping):
                continue
            workspace = workspace_by_id.get(str(plan["workspace_id"]))
            port = plan.get("planned_port")
            if not isinstance(workspace, Mapping) or not isinstance(port, int):
                issues.append(
                    {
                        "code": "PORT_LEASE_CONFLICT",
                        "path": f"nodes[{plan.get('node_id')}].workspace.process",
                        "message": "Process activation plan has no reserved port",
                    }
                )
                continue
            workspace_root = Path(str(workspace["actual_git_toplevel"]))
            relative_cwd = process_plan.get("cwd_relative")
            if relative_cwd == ".":
                candidate_cwd = workspace_root
            elif isinstance(relative_cwd, str):
                candidate_cwd = workspace_root.joinpath(*PurePosixPath(relative_cwd).parts)
            else:
                candidate_cwd = workspace_root.parent / "invalid"
            resolved_root = Path(os.path.realpath(workspace_root.resolve(strict=False)))
            resolved_cwd = Path(os.path.realpath(candidate_cwd.resolve(strict=False)))
            try:
                contained = (
                    os.path.commonpath((str(resolved_root), str(resolved_cwd)))
                    == str(resolved_root)
                )
            except ValueError:
                contained = False
            if not contained or not resolved_cwd.is_dir():
                issues.append(
                    {
                        "code": "WORKSPACE_ROOT_MISMATCH",
                        "path": f"nodes[{plan.get('node_id')}].workspace.process.cwd",
                        "message": "Process cwd escapes or is missing from the workspace",
                    }
                )
                continue
            process_id = str(process_plan["process_id"])
            port_lease_id = str(process_plan["port_lease_id"])
            assignment_id = str(plan["assignment_id"])
            port_leases.append(
                {
                    "lease_id": port_lease_id,
                    "instance_id": self.runtime_config["instance_id"],
                    "network_namespace_id": "host",
                    "assignment_id": assignment_id,
                    "process_id": None,
                    "host": "127.0.0.1",
                    "port": port,
                    "status": "reserved",
                    "bind_verified": False,
                    "acquired_at": acquired_at,
                    "released_at": None,
                }
            )
            processes.append(
                {
                    "process_id": process_id,
                    "assignment_id": assignment_id,
                    "workspace_id": str(plan["workspace_id"]),
                    "runtime_root": str(
                        Path(self.runtime_config["process_runtime_root"]) / process_id
                    ),
                    "state": "PREPARED",
                    "launch_nonce": str(process_plan["launch_nonce"]),
                    "os_process_created_at": None,
                    "os_process_birth_token": None,
                    "executable_path": str(process_plan["executable_path"]),
                    "job_object_id": None,
                    "pid": None,
                    "process_group_id": None,
                    "command_redacted": deepcopy(process_plan["command_redacted"]),
                    "environment_redacted": deepcopy(
                        process_plan["environment_redacted"]
                    ),
                    "cwd": str(resolved_cwd),
                    "stdout_log": str(
                        Path(self.runtime_config["log_root"])
                        / f"{process_id}.stdout.log"
                    ),
                    "stderr_log": str(
                        Path(self.runtime_config["log_root"])
                        / f"{process_id}.stderr.log"
                    ),
                    "health_endpoint": {
                        "host": "127.0.0.1",
                        "port": port,
                        "path": process_plan["health_path"],
                    },
                    "resource_limits": deepcopy(process_plan["resource_limits"]),
                    "restart_policy": process_plan["restart_policy"],
                    "restart_attempt": 0,
                    "restart_of_process_id": None,
                    "max_restart_attempts": process_plan["max_restart_attempts"],
                    "restart_backoff_seconds": process_plan[
                        "restart_backoff_seconds"
                    ],
                    "port_lease_id": port_lease_id,
                    "started_at": None,
                    "startup_deadline_at": None,
                    "stopped_at": None,
                    "failed_at": None,
                    "terminal_reason": None,
                }
            )
        return port_leases, processes, issues

    def _preparing_runtime_state(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
        provenance: SprintProvenance,
        manifest: Mapping[str, Any],
        repository: ManagedRepository,
        workspace_source_commit: str,
        attempt: Mapping[str, Any],
        preflight: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Build the frozen no-work runtime shell published after preflight."""

        now = _timestamp(self.clock)
        identity = asdict(provenance)
        return {
            "schema_version": 2,
            "contract_version": 1,
            "manifest_schema_version": 1,
            "sprint_type": "managed_workspace_v1",
            "sprint_id": provenance.stable_sprint_id(),
            "identity": identity,
            "workspace_source_commit": workspace_source_commit,
            "requested_ref": request.ref,
            "repository": {
                "repository_id": request.repository_id,
                "repository_key": repository.canonical_remote,
                "canonical_remote": repository.canonical_remote,
                "mirror_storage_key": repository.mirror_storage_key,
                "mirror_path": str(repository.mirror_path),
                "credential_reference": repository.spec.credential_reference,
            },
            "runtime_config": deepcopy(self.runtime_config),
            "status": "preparing",
            "graph_revision": 1,
            "graph_revisions": [
                {
                    "revision": 1,
                    "definition_sha256": canonical_json_sha256(manifest),
                    "definition": deepcopy(dict(manifest)),
                    "artifact_source_commit": workspace_source_commit,
                    "created_at": now,
                    "source": "activation",
                    "repair_id": None,
                }
            ],
            "workflow": None,
            "active_assignment_ids": [],
            "allowed_outcomes_by_assignment": {},
            "import_attempts": [
                {
                    "attempt_id": attempt["attempt_id"],
                    "idempotency_key": request.idempotency_key,
                    "request_fingerprint": request.request_fingerprint(project_id),
                    "identity": identity,
                    "contract_version": 1,
                    "manifest_schema_version": 1,
                    "phase": "PREPARE",
                    "status": "running",
                    "preflight": deepcopy(dict(preflight)),
                    "prepared_artifact_ids": [],
                    "activation_response": None,
                    "created_at": attempt["created_at"],
                    "updated_at": now,
                }
            ],
            "assignments": [],
            "result_receipts": [],
            "review_assignments": [],
            "reviews": [],
            "reworks": [],
            "integrations": [],
            "integration_workspaces": [],
            "branch_leases": [],
            "workspaces": [],
            "port_leases": [],
            "processes": [],
            "transition_journal": [],
            "outbox": [],
            "repairs": [],
            "coordinator_contexts": [],
            "blocker_observations": [],
            "recovery_records": [],
        }

    def _runtime_state(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
        provenance: SprintProvenance,
        manifest: Mapping[str, Any],
        repository: ManagedRepository,
        workspace_source_commit: str,
        attempt: Mapping[str, Any],
        preflight: Mapping[str, Any],
        plans: Sequence[Mapping[str, Any]],
        workspaces: Sequence[Mapping[str, Any]],
        branch_leases: Sequence[Mapping[str, Any]],
        port_leases: Sequence[Mapping[str, Any]],
        processes: Sequence[Mapping[str, Any]],
        response: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = _timestamp(self.clock)
        assignments: list[dict[str, Any]] = []
        occurrences: list[dict[str, Any]] = []
        outbox: list[dict[str, Any]] = []
        active_ids: list[str] = []
        outcomes: dict[str, list[str]] = {}
        workspace_by_id = {item["workspace_id"]: item for item in workspaces}
        for plan in plans:
            assignment_id = str(plan["assignment_id"])
            active_ids.append(assignment_id)
            outcomes[assignment_id] = list(plan["allowed_outcomes"])
            workspace = workspace_by_id[plan["workspace_id"]]
            occurrences.append(
                {
                    "occurrence_id": plan["occurrence_id"],
                    "node_id": plan["node_id"],
                    "graph_revision": 1,
                    "generation": 1,
                    "activation_policy": "entry",
                    "trigger_token_ids": [],
                    "state": "active",
                    "assignment_ids": [assignment_id],
                    "created_at": now,
                    "completed_at": None,
                }
            )
            assignments.append(
                {
                    "assignment_id": assignment_id,
                    "occurrence_id": plan["occurrence_id"],
                    "node_id": plan["node_id"],
                    "agent_id": plan["node"]["agent"]["id"],
                    "agent_phone": plan["node"]["agent"]["phone"],
                    "graph_revision": 1,
                    "source_kind": "sprint_source",
                    "source_result_keys": [],
                    "source_commit": workspace_source_commit,
                    "initial_head_commit": workspace["initial_head_commit"],
                    "integration_id": None,
                    "rework_cycle": 0,
                    "result_commit": None,
                    "outcome": None,
                    "status": "active",
                    "workspace_id": plan["workspace_id"],
                    "branch_lease_id": workspace.get("branch_lease_id"),
                    "allowed_outcomes": list(plan["allowed_outcomes"]),
                    "created_at": now,
                    "completed_at": None,
                }
            )
            outbox.append(
                {
                    "event_id": _safe_identifier(
                        "event", provenance.stable_sprint_id(), assignment_id
                    ),
                    "dedupe_key": (
                        f"enqueue:assignment:{provenance.stable_sprint_id()}:"
                        f"{assignment_id}"
                    ),
                    "event_type": "ASSIGNMENT_ENQUEUE",
                    "payload": {
                        "sprint_id": provenance.stable_sprint_id(),
                        "graph_revision": 1,
                        "assignment_id": assignment_id,
                        "node_id": plan["node_id"],
                        "agent_phone": plan["node"]["agent"]["phone"],
                    },
                    "status": "pending",
                    "created_at": now,
                    "delivered_at": None,
                    "queue_receipt_id": None,
                }
            )
        identity = asdict(provenance)
        return {
            "schema_version": 2,
            "contract_version": 1,
            "manifest_schema_version": 1,
            "sprint_type": "managed_workspace_v1",
            "sprint_id": provenance.stable_sprint_id(),
            "identity": identity,
            "workspace_source_commit": workspace_source_commit,
            "requested_ref": request.ref,
            "repository": {
                "repository_id": request.repository_id,
                "repository_key": repository.canonical_remote,
                "canonical_remote": repository.canonical_remote,
                "mirror_storage_key": repository.mirror_storage_key,
                "mirror_path": str(repository.mirror_path),
                "credential_reference": repository.spec.credential_reference,
            },
            "runtime_config": deepcopy(self.runtime_config),
            "status": "active",
            "graph_revision": 1,
            "graph_revisions": [
                {
                    "revision": 1,
                    "definition_sha256": canonical_json_sha256(manifest),
                    "definition": deepcopy(dict(manifest)),
                    "artifact_source_commit": workspace_source_commit,
                    "created_at": now,
                    "source": "activation",
                    "repair_id": None,
                }
            ],
            "workflow": {
                "graph_revision": 1,
                "execution_mode": manifest["execution"]["mode"],
                "entry_node_ids": [plan["node_id"] for plan in plans],
                "entry_occurrence_ids": [plan["occurrence_id"] for plan in plans],
                "occurrences": occurrences,
                "transition_tokens": [],
            },
            "active_assignment_ids": active_ids,
            "allowed_outcomes_by_assignment": outcomes,
            "import_attempts": [
                {
                    "attempt_id": attempt["attempt_id"],
                    "idempotency_key": request.idempotency_key,
                    "request_fingerprint": request.request_fingerprint(project_id),
                    "identity": identity,
                    "contract_version": 1,
                    "manifest_schema_version": 1,
                    "phase": "ACTIVATE",
                    "status": "succeeded",
                    "preflight": deepcopy(dict(preflight)),
                    "prepared_artifact_ids": [plan["workspace_id"] for plan in plans],
                    "activation_response": deepcopy(dict(response)),
                    "created_at": attempt["created_at"],
                    "updated_at": now,
                }
            ],
            "assignments": assignments,
            "result_receipts": [],
            "review_assignments": [],
            "reviews": [],
            "reworks": [],
            "integrations": [],
            "integration_workspaces": [],
            "branch_leases": [deepcopy(dict(item)) for item in branch_leases],
            "workspaces": [deepcopy(dict(item)) for item in workspaces],
            "port_leases": [deepcopy(dict(item)) for item in port_leases],
            "processes": [deepcopy(dict(item)) for item in processes],
            "transition_journal": [],
            "outbox": outbox,
            "repairs": [],
            "coordinator_contexts": [],
            "blocker_observations": [],
            "recovery_records": [],
        }

    @staticmethod
    def _replay(record: Mapping[str, Any]) -> ManagedStartResult:
        status = record.get("status")
        if status == "SUCCEEDED":
            response = deepcopy(record.get("response"))
            if not isinstance(response, dict):
                raise RuntimeError("stored managed success response is corrupt")
            response["deduplicated"] = True
            return ManagedStartResult(response, 200)
        if status == "FAILED":
            error = record.get("error")
            http_status = record.get("http_status")
            if not isinstance(error, Mapping) or not isinstance(http_status, int):
                raise RuntimeError("stored managed failure response is corrupt")
            raise ManagedImportError.from_stored(
                error,
                http_status,
                record.get("evidence") if isinstance(record.get("evidence"), Mapping) else {},
            )
        raise RuntimeError("managed import is still in progress")

    def _restore_reserved_ports(
        self,
        leases: Sequence[Mapping[str, Any]],
        *,
        correlation_id: str,
    ) -> None:
        reserved = [
            deepcopy(dict(item))
            for item in leases
            if isinstance(item, Mapping) and item.get("status") == "reserved"
        ]
        try:
            self.port_reservations.acquire_durable_many(
                self.store.database_path,
                reserved,
            )
        except ManagedPortReservationError as exc:
            raise ManagedImportError(
                SPRINT_RECOVERY_REQUIRED,
                409,
                correlation_id,
                phase="ACTIVATE",
                issues=[
                    {
                        "code": "LIVE_PORT_CONFLICT",
                        "path": "runtime_state.port_leases",
                        "message": "Durable child-port ownership could not be restored",
                    }
                ],
            ) from exc

    @staticmethod
    def _matching_launch_receipt(
        state: Mapping[str, Any],
        process: Mapping[str, Any],
        pid_root: Path,
    ) -> bool:
        """Accept only structurally complete evidence for this exact attempt."""

        process_id = process.get("process_id")
        if not isinstance(process_id, str):
            return False
        path = pid_root / f"{process_id}.json"
        try:
            with path.open("rb") as stream:
                raw = stream.read((64 * 1024) + 1)
            if len(raw) > 64 * 1024:
                return False
            receipt = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False
        if not isinstance(receipt, Mapping):
            return False
        required_keys = {
            "schema_version",
            "phase",
            "project_id",
            "sprint_id",
            "process_id",
            "assignment_id",
            "launch_nonce",
            "pid",
            "os_process_created_at",
            "os_process_birth_token",
            "executable_path",
            "cwd",
            "process_group_id",
            "job_object_id",
            "port_lease_id",
            "created_at",
        }
        if not required_keys.issubset(receipt):
            return False
        schema_version = receipt.get("schema_version")
        if (
            not isinstance(schema_version, int)
            or isinstance(schema_version, bool)
            or schema_version != 1
        ):
            return False
        identity = state.get("identity")
        expected = {
            "project_id": (
                identity.get("project_id")
                if isinstance(identity, Mapping)
                else None
            ),
            "sprint_id": state.get("sprint_id"),
            "process_id": process_id,
            "assignment_id": process.get("assignment_id"),
            "launch_nonce": process.get("launch_nonce"),
            "executable_path": process.get("executable_path"),
            "cwd": process.get("cwd"),
            "port_lease_id": process.get("port_lease_id"),
        }
        if any(receipt.get(key) != value for key, value in expected.items()):
            return False
        phase = receipt.get("phase")
        if phase not in {"intent", "launch_gated", "launched"} or _parse_timestamp(
            receipt.get("created_at")
        ) is None:
            return False
        if phase in {"intent", "launch_gated"}:
            if (
                "startup_deadline_at" not in receipt
                or _parse_timestamp(receipt.get("startup_deadline_at")) is None
            ):
                return False
        elif "startup_deadline_at" in receipt and _parse_timestamp(
            receipt.get("startup_deadline_at")
        ) is None:
            # Legacy launched v1 receipts may omit the deadline.  A present
            # but malformed value is never legacy evidence.
            return False
        expected_job = (
            "Global\\nginx-qa-managed-"
            + hashlib.sha256(
                (
                    process_id
                    + "\0"
                    + str(process.get("launch_nonce") or "")
                ).encode("utf-8")
            ).hexdigest()[:40]
            if os.name == "nt"
            else None
        )
        if phase == "intent":
            return (
                receipt.get("pid") is None
                and receipt.get("os_process_created_at") is None
                and receipt.get("os_process_birth_token") is None
                and receipt.get("process_group_id") == expected_job
                and receipt.get("job_object_id") == expected_job
            )
        pid = receipt.get("pid")
        group_id = receipt.get("process_group_id")
        job_id = receipt.get("job_object_id")
        return (
            isinstance(pid, int)
            and not isinstance(pid, bool)
            and pid > 0
            and _parse_timestamp(receipt.get("os_process_created_at")) is not None
            and isinstance(receipt.get("os_process_birth_token"), str)
            and bool(receipt.get("os_process_birth_token"))
            and (
                (group_id == expected_job and job_id == expected_job)
                if os.name == "nt"
                else (
                    isinstance(group_id, int)
                    and not isinstance(group_id, bool)
                    and group_id == pid
                    and job_id == f"posix-session:{pid}"
                )
            )
        )

    @staticmethod
    def _restorable_reserved_ports(
        state: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Return PREPARED leases without exact structural launch evidence.

        A service crash can happen after spawn/receipt publication but before
        PREPARED -> STARTING is committed.  Rebinding that exact port would
        collide with the legitimate child and prevent the supervisor from
        validating or adopting it.  Mere file presence is never evidence: a
        truncated, foreign, or corrupt receipt leaves the lease restorable.
        The supervisor still performs OS identity and socket-owner validation.
        """

        processes = state.get("processes")
        runtime_config = state.get("runtime_config")
        pid_root = (
            Path(str(runtime_config.get("pid_root")))
            if isinstance(runtime_config, Mapping)
            and isinstance(runtime_config.get("pid_root"), str)
            else None
        )
        prepared_process_by_lease = {
            str(process.get("port_lease_id")): process
            for process in processes or []
            if isinstance(process, Mapping)
            and process.get("state") == "PREPARED"
            and isinstance(process.get("port_lease_id"), str)
            and isinstance(process.get("process_id"), str)
        }
        restorable: list[dict[str, Any]] = []
        for raw_lease in state.get("port_leases", []):
            if (
                not isinstance(raw_lease, Mapping)
                or raw_lease.get("status") != "reserved"
            ):
                continue
            lease = deepcopy(dict(raw_lease))
            process = prepared_process_by_lease.get(str(lease.get("lease_id")))
            receipt_matches = (
                pid_root is not None
                and isinstance(process, Mapping)
                and TransactionalSprintImporter._matching_launch_receipt(
                    state,
                    process,
                    pid_root,
                )
            )
            if not receipt_matches:
                restorable.append(lease)
        return restorable

    def _reconcile_local_port_claims(
        self,
        states: Sequence[Mapping[str, Any]],
        *,
        correlation_id: str,
        fence: ManagedPortReconciliationFence,
    ) -> None:
        """Drop process-local holders made stale by another service instance."""

        live_leases = [
            deepcopy(dict(lease))
            for state in states
            if isinstance(state, Mapping)
            for lease in state.get("port_leases", [])
            if isinstance(lease, Mapping)
            and lease.get("status") in {"reserved", "bound"}
        ]
        try:
            self.port_reservations.reconcile_durable(
                self.store.database_path,
                live_leases,
                fence=fence,
            )
        except ManagedPortReservationError as exc:
            raise ManagedImportError(
                SPRINT_RECOVERY_REQUIRED,
                409,
                correlation_id,
                phase="ACTIVATE",
                issues=[
                    {
                        "code": "LIVE_PORT_CONFLICT",
                        "path": "runtime_state.port_leases",
                        "message": "Local child-port ownership differs from durable state",
                    }
                ],
            ) from exc

    def restore_committed_activations(
        self,
        *,
        isolate_process_failures: bool = False,
    ) -> None:
        """Reconcile durable ports and Git publications before serving traffic.

        The application startup path isolates each PREPARED port recovery so
        one conflicted assignment cannot prevent unrelated children (or the
        HTTP service itself) from being restored.  Explicit recovery callers
        retain the original all-or-nothing error behavior by default.
        """

        self._assert_mutation_roots_safe()
        port_fence = self.port_reservations.reconciliation_fence(
            self.store.database_path
        )
        states = self.store.active_runtime_states()
        self._reconcile_local_port_claims(
            states,
            correlation_id="startup-port-reconciliation",
            fence=port_fence,
        )
        leases_without_launch_receipts = [
            lease
            for state in states
            for lease in self._restorable_reserved_ports(state)
        ]
        if isolate_process_failures:
            for lease in leases_without_launch_receipts:
                try:
                    self._restore_reserved_ports(
                        [lease],
                        correlation_id="managed-port-startup-recovery",
                    )
                except ManagedImportError:
                    # The process supervisor reconciles this exact assignment
                    # independently.  Other durable port holders and Git
                    # publications must still be restored first.
                    continue
        else:
            self._restore_reserved_ports(
                leases_without_launch_receipts,
                correlation_id="managed-port-startup-recovery",
            )
        for state in states:
            repository_record = state.get("repository")
            identity = state.get("identity")
            if not isinstance(repository_record, Mapping) or not isinstance(
                identity, Mapping
            ):
                raise RuntimeError("active runtime has no repository identity")
            self._ensure_succeeded_publication(
                {
                    "status": "SUCCEEDED",
                    "sprint_id": state.get("sprint_id"),
                    "pinned_identity": deepcopy(dict(identity)),
                    "attempt_id": "managed-startup-recovery",
                },
                restore_ports=False,
            )

    def _provider_for_durable_repository(
        self, repository_record: Mapping[str, Any]
    ) -> ManagedGitProvider:
        """Rebuild a project-scoped provider from committed repository identity."""

        repository_id = str(repository_record.get("repository_id") or "")
        canonical_remote = str(repository_record.get("canonical_remote") or "")
        if not repository_id or not canonical_remote:
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "durable repository identity is incomplete",
            )
        expected_mirror = self.git_provider.mirror_path_for(
            canonical_remote
        ).resolve(strict=False)
        recorded_mirror = Path(
            str(repository_record.get("mirror_path") or "")
        ).resolve(strict=False)
        if os.path.normcase(str(expected_mirror)) != os.path.normcase(
            str(recorded_mirror)
        ):
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "durable repository mirror path is not canonical",
            )
        transport_result = self.git_provider.runner.run(
            (
                "--git-dir",
                str(expected_mirror),
                "config",
                "--get",
                "remote.origin.url",
            ),
            error_code="REPOSITORY_MIRROR_INVALID",
        )
        transport_url = transport_result.stdout.strip()
        if not transport_url or "\n" in transport_url:
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "durable repository mirror remote is invalid",
            )
        repository_spec = RepositorySpec(
            repository_id=repository_id,
            canonical_remote=canonical_remote,
            transport_url=transport_url,
            credential_reference=(
                str(repository_record["credential_reference"])
                if repository_record.get("credential_reference") is not None
                else None
            ),
        )
        return self._provider_for_repository_spec(repository_spec)

    def provider_for_durable_repository(
        self, repository_record: Mapping[str, Any]
    ) -> ManagedGitProvider:
        """Return a provider bound only to committed repository identity."""

        return self._provider_for_durable_repository(repository_record)

    def _provider_for_repository_spec(
        self, repository_spec: RepositorySpec
    ) -> ManagedGitProvider:
        provider = ManagedGitProvider(
            self._managed_root,
            {repository_spec.repository_id: repository_spec},
            credential_resolver=self.git_provider.credential_resolver,
            git_executable=self.git_provider.runner.executable,
            command_timeout=self.git_provider.runner.timeout,
            lock_timeout=self.git_provider.lock_timeout,
            allow_local_transport=self.git_provider.allow_local_transport,
        )
        # The registry is deliberately frozen, but the command runner and any
        # instance-level test/observability hooks remain shared.
        provider.runner = self.git_provider.runner
        for name, value in vars(self.git_provider).items():
            if callable(value) and name not in {"registry", "credential_resolver"}:
                setattr(provider, name, value)
        return provider

    def _repository_binding(self, repository_spec: RepositorySpec) -> dict[str, Any]:
        return {
            "repository_id": repository_spec.repository_id,
            "canonical_remote": repository_spec.canonical_remote,
            "transport_url": repository_spec.transport_url,
            "credential_reference": repository_spec.credential_reference,
            "mirror_path": str(
                self.git_provider.mirror_path_for(
                    repository_spec.canonical_remote
                ).resolve(strict=False)
            ),
        }

    def _provider_for_request_record(
        self,
        record: Mapping[str, Any],
        request: StartSprintFromGitRequest,
    ) -> ManagedGitProvider:
        evidence = record.get("evidence")
        binding = (
            evidence.get("repository_binding")
            if isinstance(evidence, Mapping)
            else None
        )
        if not isinstance(binding, Mapping) or set(binding) != {
            "repository_id",
            "canonical_remote",
            "transport_url",
            "credential_reference",
            "mirror_path",
        }:
            raise ManagedImportError(
                SPRINT_RECOVERY_REQUIRED,
                409,
                str(record.get("attempt_id") or "repository-binding"),
                phase="VALIDATE",
                issues=[
                    {
                        "code": "REPOSITORY_BINDING_INVALID",
                        "path": "repository",
                        "message": "Durable request repository binding is invalid",
                    }
                ],
            )
        repository_spec = RepositorySpec(
            repository_id=str(binding.get("repository_id") or ""),
            canonical_remote=str(binding.get("canonical_remote") or ""),
            transport_url=str(binding.get("transport_url") or ""),
            credential_reference=(
                str(binding["credential_reference"])
                if binding.get("credential_reference") is not None
                else None
            ),
        )
        if repository_spec.repository_id != request.repository_id:
            raise ManagedImportError(
                SPRINT_RECOVERY_REQUIRED,
                409,
                str(record.get("attempt_id") or "repository-binding"),
                phase="VALIDATE",
                issues=[
                    {
                        "code": "REPOSITORY_BINDING_INVALID",
                        "path": "repository",
                        "message": "Durable repository alias changed",
                    }
                ],
            )
        expected_mirror = self.git_provider.mirror_path_for(
            repository_spec.canonical_remote
        ).resolve(strict=False)
        recorded_mirror = Path(str(binding.get("mirror_path") or "")).resolve(
            strict=False
        )
        if os.path.normcase(str(expected_mirror)) != os.path.normcase(
            str(recorded_mirror)
        ):
            raise ManagedImportError(
                SPRINT_RECOVERY_REQUIRED,
                409,
                str(record.get("attempt_id") or "repository-binding"),
                phase="VALIDATE",
                issues=[
                    {
                        "code": "RUNTIME_ROOT_NOT_ISOLATED",
                        "path": "runtime_config.managed_root",
                        "message": "Managed repository root changed after request intent",
                    }
                ],
            )
        provider = self._provider_for_repository_spec(repository_spec)
        provider.resolve_repository(request.repository_id)
        return provider

    def _pre_dispatch_repository_provider(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
        fingerprint: str,
    ) -> tuple[ManagedGitProvider, bool]:
        binding_key = self._request_binding_key(project_id, request.idempotency_key)
        try:
            repository_spec, binding_created = (
                self.git_provider.bind_request_repository(
                    binding_key,
                    fingerprint,
                    request.repository_id,
                )
            )
        except ManagedGitError as exc:
            if exc.code == "IDEMPOTENCY_KEY_CONFLICT":
                raise ManagedImportError(
                    IDEMPOTENCY_KEY_CONFLICT,
                    409,
                    f"binding-{binding_key}",
                    phase="VALIDATE",
                ) from exc
            raise
        return self._provider_for_repository_spec(repository_spec), binding_created

    @staticmethod
    def _request_binding_key(project_id: str, idempotency_key: str) -> str:
        return hashlib.sha256(
            canonical_json_bytes([project_id, idempotency_key])
        ).hexdigest()

    def _require_bound_mirror(
        self,
        git_provider: ManagedGitProvider,
        project_id: str,
        request: StartSprintFromGitRequest,
    ) -> None:
        repository_spec = git_provider.resolve_repository(request.repository_id)
        mirror_path = git_provider.mirror_path_for(repository_spec.canonical_remote)
        if mirror_path.is_dir() and not mirror_path.is_symlink():
            return
        binding_key = self._request_binding_key(
            project_id, request.idempotency_key
        )
        raise ManagedImportError(
            SPRINT_RECOVERY_REQUIRED,
            409,
            f"binding-{binding_key}",
            phase="VALIDATE",
            issues=[
                {
                    "code": "REPOSITORY_MIRROR_INVALID",
                    "path": "repository",
                    "message": "Durably bound repository mirror is missing",
                }
            ],
        )

    def _fail_validating_attempt(
        self,
        project_id: str,
        record: Mapping[str, Any],
        repository: ManagedRepository,
        error: ManagedImportError,
    ) -> ManagedStartResult:
        fetch_result = repository.fetch_result
        error.evidence.setdefault(
            "git_fetch",
            {
                "returncode": fetch_result.returncode,
                "elapsed_seconds": fetch_result.elapsed_seconds,
                "stdout": fetch_result.stdout,
                "stderr": fetch_result.stderr,
            },
        )
        error.evidence.setdefault("phase", "VALIDATE")
        error.evidence.setdefault("failure_code", error.code)
        failed = self.store.fail_attempt(
            project_id,
            str(record["attempt_id"]),
            error,
            evidence=error.evidence,
            fencing_token=int(record["fencing_token"]),
            clock=self.clock,
        )
        if failed is None:
            raise RuntimeError("managed start intent disappeared")
        return self._replay(failed)

    def restore_reserved_port_reservations(self) -> None:
        """Backward-compatible alias for complete startup reconciliation."""

        self.restore_committed_activations()

    def _ensure_succeeded_publication(
        self,
        record: Mapping[str, Any],
        *,
        git_provider: ManagedGitProvider | None = None,
        restore_ports: bool = True,
    ) -> None:
        """Restore process-local resources and Git refs for a committed activation."""

        if record.get("status") != "SUCCEEDED":
            return
        sprint_id = record.get("sprint_id")
        pinned = record.get("pinned_identity")
        if not isinstance(sprint_id, str) or not isinstance(pinned, Mapping):
            raise RuntimeError("managed success record has no immutable identity")
        state = self.store.runtime_state(str(pinned["project_id"]), sprint_id)
        if not isinstance(state, Mapping):
            raise RuntimeError("managed success runtime is missing")
        if restore_ports:
            self._restore_reserved_ports(
                self._restorable_reserved_ports(state),
                correlation_id=str(
                    record.get("attempt_id") or "managed-port-recovery"
                ),
            )
        repository_record = state.get("repository")
        if not isinstance(repository_record, Mapping):
            raise RuntimeError("managed success repository is missing")
        repository_id = str(repository_record["repository_id"])
        mirror_path = Path(str(repository_record.get("mirror_path") or ""))
        try:
            if not mirror_path.is_dir() or mirror_path.is_symlink():
                raise ManagedGitError(
                    "REPOSITORY_MIRROR_INVALID",
                    "durable managed mirror is missing or redirected",
                )
            provider = git_provider or self._provider_for_durable_repository(
                repository_record
            )
            repository = provider.ensure_mirror(repository_id, fetch=False)
        except ManagedGitError as exc:
            raise ManagedImportError(
                SPRINT_RECOVERY_REQUIRED,
                409,
                str(record.get("attempt_id") or "managed-repository-recovery"),
                phase="ACTIVATE",
                issues=[
                    {
                        "code": exc.code,
                        "path": "runtime_state.repository",
                        "message": "Durable repository mirror could not be restored",
                    }
                ],
            ) from exc
        revisions = state.get("graph_revisions")
        revision = next(
            (
                item
                for item in revisions
                if isinstance(item, Mapping)
                and item.get("revision") == state.get("graph_revision")
            ),
            None,
        ) if isinstance(revisions, list) else None
        definition = revision.get("definition") if isinstance(revision, Mapping) else None
        if not isinstance(definition, Mapping):
            raise RuntimeError("managed success graph definition is missing")
        nodes = {
            str(node.get("id")): node
            for node in definition.get("nodes", [])
            if isinstance(node, Mapping)
        }
        assignments = {
            str(item.get("assignment_id")): item
            for item in state.get("assignments", [])
            if isinstance(item, Mapping)
        }
        try:
            for raw_lease in state.get("branch_leases", []):
                if (
                    not isinstance(raw_lease, Mapping)
                    or raw_lease.get("status") != "active"
                ):
                    continue
                assignment = assignments.get(str(raw_lease["assignment_id"]))
                if not isinstance(assignment, Mapping):
                    raise RuntimeError("branch lease assignment is missing")
                node = nodes.get(str(assignment.get("node_id")))
                workspace_definition = (
                    node.get("workspace") if isinstance(node, Mapping) else None
                )
                if not isinstance(workspace_definition, Mapping):
                    raise RuntimeError("branch lease node workspace is missing")
                node_git = workspace_definition.get("git")
                git_policy = (
                    node_git if isinstance(node_git, Mapping) else definition["git"]
                )
                provider.ensure_local_branch_publication(
                    repository,
                    str(raw_lease["branch"]),
                    selected_head=str(raw_lease["initial_head_commit"]),
                    publication_id=str(raw_lease["lease_id"]),
                    assignment_id=str(raw_lease["assignment_id"]),
                    policy=str(git_policy["existing_branch_policy"]),
                    source_commit=str(raw_lease["source_commit"]),
                    expected_branch_head=(
                        str(git_policy["expected_branch_head"])
                        if git_policy.get("expected_branch_head") is not None
                        else None
                    ),
                )
        except ManagedGitError as exc:
            raise ManagedImportError(
                SPRINT_RECOVERY_REQUIRED,
                409,
                str(record.get("attempt_id") or "managed-branch-recovery"),
                phase="ACTIVATE",
                issues=[
                    {
                        "code": exc.code,
                        "path": "runtime_state.branch_leases",
                        "message": "Durable branch publication could not be restored",
                    }
                ],
            ) from exc

    def start(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
        *,
        request_lock_timeout: float | None = None,
    ) -> ManagedStartResult:
        try:
            # Serialize the pre-record Git-pin window as well as the normal
            # activation.  Without this lock, an identical concurrent caller
            # could observe the repository binding before the winner's pins or
            # idempotency record become durable.
            try:
                self._assert_control_store_root_safe()
                existing = self.store.lookup(project_id, request.idempotency_key)
                terminal = isinstance(existing, Mapping) and existing.get(
                    "status"
                ) in {"SUCCEEDED", "FAILED"}
                if not terminal:
                    self._assert_mutation_roots_safe()
            except (WorkspaceError, KeyError, TypeError, ValueError):
                # Let the transaction path preserve its phase-specific public
                # normalization without creating a lock below an unsafe root.
                return self._start_transaction(project_id, request)
            if terminal:
                return self._start_transaction(project_id, request)
            binding_key = self._request_binding_key(
                project_id, request.idempotency_key
            )
            request_lock = self.git_provider.request_operation_lock(
                binding_key,
                timeout=(
                    max(float(_LEASE_SECONDS), self.store.timeout)
                    if request_lock_timeout is None
                    else request_lock_timeout
                ),
            )
            try:
                with request_lock:
                    return self._start_transaction(project_id, request)
            except FileLockTimeout:
                raise ManagedImportError(
                    PROJECT_ACTIVATION_IN_PROGRESS,
                    409,
                    f"binding-{binding_key}",
                    phase="VALIDATE",
                ) from None
        except ManagedImportError:
            raise
        except ManagedGitError as exc:
            if exc.code in TRANSIENT_GIT_ERROR_CODES:
                code, http_status = SPRINT_PREFLIGHT_FAILED, 503
            elif exc.code == "REPOSITORY_NOT_FOUND":
                code, http_status = "REPOSITORY_NOT_FOUND", 404
            elif exc.code in {
                "SOURCE_COMMIT_NOT_FOUND",
                "GIT_BLOB_NOT_FOUND",
            }:
                code, http_status = "SOURCE_REF_NOT_FOUND", 404
            elif exc.code in {
                "GIT_REF_INVALID",
                "GIT_PATH_INVALID",
                "REPOSITORY_ID_INVALID",
            }:
                code, http_status = INVALID_MANAGED_SPRINT_REQUEST, 400
            else:
                code, http_status = SPRINT_PREFLIGHT_FAILED, 409
            raise ManagedImportError(
                code,
                http_status,
                f"request-{uuid4().hex}",
                phase="VALIDATE",
                issues=[
                    {
                        "code": exc.code,
                        "path": "repository",
                        "message": "Managed repository validation failed",
                    }
                ],
            ) from exc

    def resume_import_attempt(
        self,
        project_id: str,
        attempt_id: str,
    ) -> ManagedStartResult:
        """Resume a fenced attempt solely from its durable request binding."""

        record = self.store.lookup_attempt(project_id, attempt_id)
        if not isinstance(record, Mapping):
            raise RuntimeError("managed import attempt was not found")
        evidence = record.get("evidence")
        request_record = (
            evidence.get("request") if isinstance(evidence, Mapping) else None
        )
        if not isinstance(request_record, Mapping):
            raise RuntimeError("managed import durable request is missing")
        try:
            request = StartSprintFromGitRequest(
                repository_id=str(request_record["repository_id"]),
                ref=str(request_record["ref"]),
                manifest_path=str(request_record["manifest_path"]),
                idempotency_key=str(record["idempotency_key"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("managed import durable request is invalid") from exc
        pinned = record.get("pinned_identity")
        if (
            not isinstance(pinned, Mapping)
            or request.repository_id != pinned.get("repository_id")
            or request.manifest_path != pinned.get("manifest_path")
            or request.request_fingerprint(project_id)
            != record.get("request_fingerprint")
        ):
            raise RuntimeError("managed import durable request changed")
        # Reconciliation must never wait behind an import already executing in
        # this or another process.  A lost-owner crash leaves the file lock
        # free even while its durable activation lease is still live, so a
        # short nonblocking probe distinguishes those cases safely.
        return self.start(
            project_id,
            request,
            request_lock_timeout=0.01,
        )

    def _start_transaction(
        self,
        project_id: str,
        request: StartSprintFromGitRequest,
    ) -> ManagedStartResult:
        try:
            self._assert_control_store_root_safe()
        except (WorkspaceError, KeyError, TypeError, ValueError) as exc:
            raise ManagedImportError(
                SPRINT_PREFLIGHT_FAILED,
                409,
                f"request-{uuid4().hex}",
                phase="VALIDATE",
                issues=[
                    {
                        "code": getattr(exc, "code", "RUNTIME_ROOT_NOT_ISOLATED"),
                        "path": "runtime_config",
                        "message": "Managed mutation roots are not isolated",
                    }
                ],
            ) from None
        fingerprint = request.request_fingerprint(project_id)
        existing = self.store.lookup(project_id, request.idempotency_key)
        if existing is not None:
            if existing.get("request_fingerprint") != fingerprint:
                raise ManagedImportError(
                    IDEMPOTENCY_KEY_CONFLICT,
                    409,
                    str(existing.get("attempt_id") or "idempotency-conflict"),
                    phase="VALIDATE",
                )
            if existing.get("status") == "FAILED":
                # FAILED is an immutable receipt.  Resource reconciliation for
                # a newer active sprint must never replace the original
                # caller's frozen error envelope on exact replay.
                return self._replay(existing)
        port_fence = self.port_reservations.reconciliation_fence(
            self.store.database_path
        )
        try:
            active_states = self.store.active_runtime_states()
        except RuntimeError:
            # The normal fenced prepare path below converts a corrupt active
            # predecessor into SPRINT_RECOVERY_REQUIRED.  Do not let this
            # best-effort local socket reconciliation bypass that stable API,
            # and especially do not close reservations whose durable owner
            # cannot be decoded safely.
            active_states = None
        if active_states is not None:
            self._reconcile_local_port_claims(
                active_states,
                correlation_id=(
                    str(existing.get("attempt_id"))
                    if isinstance(existing, Mapping)
                    and isinstance(existing.get("attempt_id"), str)
                    else f"request-{uuid4().hex}"
                ),
                fence=port_fence,
            )
        if existing is not None:
            if existing.get("request_fingerprint") != fingerprint:
                raise ManagedImportError(
                    IDEMPOTENCY_KEY_CONFLICT,
                    409,
                    str(existing.get("attempt_id") or "idempotency-conflict"),
                    phase="VALIDATE",
                )
            if existing.get("status") == "FAILED":
                return self._replay(existing)
            if existing.get("status") == "SUCCEEDED":
                try:
                    self._assert_mutation_roots_safe()
                except (WorkspaceError, KeyError, TypeError, ValueError) as exc:
                    raise ManagedImportError(
                        SPRINT_RECOVERY_REQUIRED,
                        409,
                        str(existing.get("attempt_id") or "managed-recovery"),
                        phase="ACTIVATE",
                        issues=[
                            {
                                "code": getattr(
                                    exc, "code", "RUNTIME_ROOT_NOT_ISOLATED"
                                ),
                                "path": "runtime_config",
                                "message": "Managed runtime roots changed after activation",
                            }
                        ],
                    ) from None
                self._ensure_succeeded_publication(existing)
                return self._replay(existing)
        try:
            self._assert_mutation_roots_safe()
        except (WorkspaceError, KeyError, TypeError, ValueError) as exc:
            raise ManagedImportError(
                SPRINT_PREFLIGHT_FAILED,
                409,
                str(
                    existing.get("attempt_id")
                    if isinstance(existing, Mapping)
                    else f"request-{uuid4().hex}"
                ),
                phase="VALIDATE",
                issues=[
                    {
                        "code": getattr(exc, "code", "RUNTIME_ROOT_NOT_ISOLATED"),
                        "path": "runtime_config",
                        "message": "Managed mutation roots are not isolated",
                    }
                ],
            ) from None
        self.port_reservations.prune()
        config_schema_issues = _schema_errors(
            self.runtime_config,
            "managed-runtime-config-v1.schema.json",
            issue_code="RUNTIME_CONFIG_SCHEMA_INVALID",
        )
        config_invariant_issues = managed_runtime_config_invariant_issues(
            self.runtime_config
        )
        if config_schema_issues or config_invariant_issues:
            normalized_issues = [
                {
                    "code": (
                        "PORT_RANGE_INVALID"
                        if "PORT" in code or "ENDPOINT" in code
                        else "RUNTIME_ROOT_NOT_ISOLATED"
                    ),
                    "path": "runtime_config",
                    "message": "Managed runtime configuration is not isolated",
                }
                for code in config_invariant_issues
            ]
            if config_schema_issues and not normalized_issues:
                normalized_issues.append(
                    {
                        "code": "RUNTIME_ROOT_NOT_ISOLATED",
                        "path": "runtime_config",
                        "message": "Managed runtime configuration is invalid",
                    }
                )
            raise ManagedImportError(
                SPRINT_PREFLIGHT_FAILED,
                409,
                f"request-{uuid4().hex}",
                phase="VALIDATE",
                issues=normalized_issues,
            )
        # Re-read after safety validation: another process may have won the
        # idempotency race while this caller was checking local configuration.
        existing = self.store.lookup(project_id, request.idempotency_key)
        if existing is not None:
            if existing.get("request_fingerprint") != fingerprint:
                raise ManagedImportError(
                    IDEMPOTENCY_KEY_CONFLICT,
                    409,
                    str(existing.get("attempt_id") or "idempotency-conflict"),
                    phase="VALIDATE",
                )
            if existing.get("status") in {"SUCCEEDED", "FAILED"}:
                self._ensure_succeeded_publication(existing)
                return self._replay(existing)
            # Do not renew or steal the fence until this caller owns the
            # cross-process attempt lock.  Otherwise a waiter could fence a
            # healthy long-running owner and then time out on that same lock.
            record = existing
            git_provider = self._provider_for_request_record(record, request)
            pinned = record.get("pinned_identity")
            self._require_bound_mirror(
                git_provider, project_id, request
            )
            repository = git_provider.ensure_mirror(request.repository_id, fetch=False)
            if isinstance(pinned, Mapping):
                provenance = SprintProvenance(**dict(pinned))
            else:
                request_pin_owner = (
                    f"start:{project_id}:{request.idempotency_key}:{fingerprint}"
                )
                manifest_commit = git_provider.pinned_commit(
                    repository,
                    owner_id=f"{request_pin_owner}:manifest",
                )
                if manifest_commit is None:
                    raise ManagedImportError(
                        SPRINT_RECOVERY_REQUIRED,
                        409,
                        str(record.get("attempt_id") or "managed-recovery"),
                        phase="VALIDATE",
                        issues=[
                            {
                                "code": "GIT_PIN_INVALID",
                                "path": "repository",
                                "message": "Managed request manifest pin is missing",
                            }
                        ],
                    )
                pinned_manifest_blob = git_provider.read_blob(
                    repository,
                    manifest_commit,
                    request.manifest_path,
                    max_bytes=_MANIFEST_MAX_BYTES,
                )
                provenance = SprintProvenance(
                    project_id=project_id,
                    repository_id=request.repository_id,
                    commit=manifest_commit,
                    manifest_path=request.manifest_path,
                    manifest_sha256=hashlib.sha256(
                        pinned_manifest_blob
                    ).hexdigest(),
                )
                try:
                    record = self.store.set_pinned_identity(
                        project_id,
                        str(record["attempt_id"]),
                        provenance,
                        fencing_token=int(record["fencing_token"]),
                        clock=self.clock,
                    )
                except ManagedImportError as exc:
                    failed = self.store.fail_attempt(
                        project_id,
                        str(record["attempt_id"]),
                        exc,
                        evidence=exc.evidence,
                        fencing_token=int(record["fencing_token"]),
                        clock=self.clock,
                    )
                    if failed is None:
                        raise RuntimeError("managed start intent disappeared")
                    return self._replay(failed)
            manifest_blob = git_provider.read_blob(
                repository,
                provenance.commit,
                provenance.manifest_path,
                max_bytes=_MANIFEST_MAX_BYTES,
            )
            if hashlib.sha256(manifest_blob).hexdigest() != provenance.manifest_sha256:
                raise RuntimeError("pinned manifest content changed")
            manifest = _strict_json_object(manifest_blob)
            correlation_id = str(record["attempt_id"])
            if manifest.get("schema_version") != 1:
                return self._fail_validating_attempt(
                    project_id,
                    record,
                    repository,
                    ManagedImportError(
                        SPRINT_SCHEMA_UNSUPPORTED,
                        400,
                        correlation_id,
                        phase="VALIDATE",
                        supported=["1"],
                    ),
                )
            schema_issues = _schema_errors(manifest, _MANIFEST_SCHEMA)
            if schema_issues:
                return self._fail_validating_attempt(
                    project_id,
                    record,
                    repository,
                    ManagedImportError(
                        INVALID_MANAGED_SPRINT_REQUEST,
                        400,
                        correlation_id,
                        phase="VALIDATE",
                        issues=schema_issues,
                    ),
                )
            raw_git = manifest.get("git")
            source_ref = (
                raw_git.get("source_ref")
                if isinstance(raw_git, Mapping)
                else None
            )
            if not isinstance(source_ref, str) or not git_ref_format_valid(source_ref):
                return self._fail_validating_attempt(
                    project_id,
                    record,
                    repository,
                    ManagedImportError(
                        SPRINT_PREFLIGHT_FAILED,
                        409,
                        correlation_id,
                        phase="VALIDATE",
                        issues=[
                            {
                                "code": "SOURCE_REF_INVALID",
                                "path": "git.source_ref",
                                "message": "Workspace source ref is invalid",
                            }
                        ],
                    ),
                )
            workspace_source_commit = str(record.get("workspace_source_commit") or "")
            if not workspace_source_commit:
                request_pin_owner = (
                    f"start:{project_id}:{request.idempotency_key}:{fingerprint}"
                )
                workspace_source_commit = str(
                    git_provider.pinned_commit(
                        repository,
                        owner_id=f"{request_pin_owner}:workspace-source",
                    )
                    or ""
                )
                if not workspace_source_commit:
                    try:
                        workspace_source_commit = (
                            git_provider.resolve_and_pin_commit(
                                repository,
                                source_ref,
                                owner_id=f"{request_pin_owner}:workspace-source",
                            )
                        )
                    except ManagedGitError as exc:
                        if exc.code not in {
                            "GIT_REF_INVALID",
                            "SOURCE_COMMIT_NOT_FOUND",
                        }:
                            raise
                        return self._fail_validating_attempt(
                            project_id,
                            record,
                            repository,
                            ManagedImportError(
                                SPRINT_PREFLIGHT_FAILED,
                                409,
                                correlation_id,
                                phase="VALIDATE",
                                issues=[
                                    {
                                        "code": "SOURCE_REF_INVALID",
                                        "path": "git.source_ref",
                                        "message": "Workspace source ref could not be resolved",
                                    }
                                ],
                            ),
                        )
            sprint_id = provenance.stable_sprint_id()
            git_provider.pin_commit(
                repository,
                provenance.commit,
                owner_id=f"{sprint_id}:manifest",
            )
            git_provider.pin_commit(
                repository,
                workspace_source_commit,
                owner_id=f"{sprint_id}:workspace-source",
            )
        else:
            git_provider, binding_created = self._pre_dispatch_repository_provider(
                project_id,
                request,
                fingerprint,
            )
            request_pin_owner = (
                f"start:{project_id}:{request.idempotency_key}:{fingerprint}"
            )
            if not binding_created:
                self._require_bound_mirror(
                    git_provider, project_id, request
                )
            repository = git_provider.ensure_mirror(
                request.repository_id, fetch=False
            )
            manifest_commit = git_provider.pinned_commit(
                repository, owner_id=f"{request_pin_owner}:manifest"
            )
            if manifest_commit is None:
                # A newly created mirror has already fetched once.  Existing
                # mirrors fetch only when this request has no durable Git pin.
                if not repository.fetch_result.argv:
                    repository = git_provider.ensure_mirror(
                        request.repository_id, fetch=True
                    )
                manifest_commit = git_provider.resolve_and_pin_commit(
                    repository,
                    request.ref,
                    owner_id=f"{request_pin_owner}:manifest",
                )
            manifest_blob = git_provider.read_blob(
                repository,
                manifest_commit,
                request.manifest_path,
                max_bytes=_MANIFEST_MAX_BYTES,
            )
            try:
                manifest = _strict_json_object(manifest_blob)
            except ValueError:
                raise ManagedImportError(
                    INVALID_MANAGED_SPRINT_REQUEST,
                    400,
                    f"request-{uuid4().hex}",
                    phase="VALIDATE",
                ) from None
            try:
                selection = resolve_sprint_type(manifest)
            except SprintTypeUnsupported as exc:
                unsupported = exc.as_detail()
                raise ManagedImportError(
                    exc.code,
                    400,
                    f"request-{uuid4().hex}",
                    phase="VALIDATE",
                    field=str(unsupported["field"]),
                    supported=list(unsupported["supported"]),
                ) from None
            if selection.pipeline is not SprintPipeline.MANAGED_WORKSPACE:
                raise ManagedImportError(
                    MANAGED_SPRINT_TYPE_REQUIRED,
                    400,
                    f"request-{uuid4().hex}",
                    phase="VALIDATE",
                    supported=["managed_workspace_v1"],
                )
            record, raced = self.store.register_or_resume(
                project_id,
                request,
                None,
                None,
                repository_binding=self._repository_binding(repository.spec),
                clock=self.clock,
                attempt_id_factory=self.attempt_id_factory,
            )
            if raced:
                # Another root/process won the durable intent.  Re-enter using
                # its frozen repository binding; never continue with this
                # caller's pre-dispatch provider.
                return self._start_transaction(project_id, request)
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                self._ensure_succeeded_publication(record)
                return self._replay(record)
            manifest_sha = hashlib.sha256(manifest_blob).hexdigest()
            provenance = SprintProvenance(
                project_id=project_id,
                repository_id=request.repository_id,
                commit=manifest_commit,
                manifest_path=request.manifest_path,
                manifest_sha256=manifest_sha,
            )
            sprint_id = provenance.stable_sprint_id()
            try:
                record = self.store.set_pinned_identity(
                    project_id,
                    str(record["attempt_id"]),
                    provenance,
                    fencing_token=int(record["fencing_token"]),
                    clock=self.clock,
                )
            except ManagedImportError as exc:
                failed = self.store.fail_attempt(
                    project_id,
                    str(record["attempt_id"]),
                    exc,
                    evidence=exc.evidence,
                    fencing_token=int(record["fencing_token"]),
                    clock=self.clock,
                )
                if failed is None:
                    raise RuntimeError("managed start intent disappeared")
                return self._replay(failed)
            git_provider.pin_commit(
                repository,
                manifest_commit,
                owner_id=f"{sprint_id}:manifest",
            )

            def terminal_validation_failure(
                error: ManagedImportError,
                *,
                source_commit: str | None = None,
            ) -> ManagedStartResult:
                if source_commit is not None:
                    self.store.set_workspace_source(
                        project_id,
                        str(record["attempt_id"]),
                        source_commit,
                        fencing_token=int(record["fencing_token"]),
                        clock=self.clock,
                    )
                fetch_result = repository.fetch_result
                error.evidence.setdefault(
                    "git_fetch",
                    {
                        "returncode": fetch_result.returncode,
                        "elapsed_seconds": fetch_result.elapsed_seconds,
                        "stdout": fetch_result.stdout,
                        "stderr": fetch_result.stderr,
                    },
                )
                error.evidence.setdefault("phase", "VALIDATE")
                error.evidence.setdefault("failure_code", error.code)
                failed = self.store.fail_attempt(
                    project_id,
                    str(record["attempt_id"]),
                    error,
                    evidence=error.evidence,
                    fencing_token=int(record["fencing_token"]),
                    clock=self.clock,
                )
                if failed is None:
                    raise RuntimeError("managed start intent disappeared")
                return self._replay(failed)

            if manifest.get("schema_version") != 1:
                failure_id = str(record["attempt_id"])
                return terminal_validation_failure(
                    ManagedImportError(
                        SPRINT_SCHEMA_UNSUPPORTED,
                        400,
                        failure_id,
                        phase="VALIDATE",
                        supported=["1"],
                    )
                )
            schema_issues = _schema_errors(manifest, _MANIFEST_SCHEMA)
            if schema_issues:
                failure_id = str(record["attempt_id"])
                return terminal_validation_failure(
                    ManagedImportError(
                        INVALID_MANAGED_SPRINT_REQUEST,
                        400,
                        failure_id,
                        phase="VALIDATE",
                        issues=schema_issues,
                    )
                )
            raw_git = manifest.get("git")
            source_ref = (
                raw_git.get("source_ref")
                if isinstance(raw_git, Mapping)
                else None
            )
            if not isinstance(source_ref, str) or not git_ref_format_valid(source_ref):
                failure_id = str(record["attempt_id"])
                return terminal_validation_failure(
                    ManagedImportError(
                        SPRINT_PREFLIGHT_FAILED,
                        409,
                        failure_id,
                        phase="VALIDATE",
                        issues=[
                            {
                                "code": "SOURCE_REF_INVALID",
                                "path": "git.source_ref",
                                "message": "Workspace source ref is invalid",
                            }
                        ],
                    )
                )
            try:
                workspace_source_commit = git_provider.resolve_and_pin_commit(
                    repository,
                    source_ref,
                    owner_id=f"{request_pin_owner}:workspace-source",
                )
            except ManagedGitError as exc:
                if exc.code not in {"GIT_REF_INVALID", "SOURCE_COMMIT_NOT_FOUND"}:
                    raise
                failure_id = str(record["attempt_id"])
                failure = ManagedImportError(
                    SPRINT_PREFLIGHT_FAILED,
                    409,
                    failure_id,
                    phase="VALIDATE",
                    issues=[
                        {
                            "code": "SOURCE_REF_INVALID",
                            "path": "git.source_ref",
                            "message": "Workspace source ref could not be resolved",
                        }
                    ],
                )
                return terminal_validation_failure(failure)
            git_provider.pin_commit(
                repository,
                workspace_source_commit,
                owner_id=f"{sprint_id}:workspace-source",
            )
            expected_source = raw_git.get("expected_source_commit")
            if (
                expected_source is not None
                and workspace_source_commit != expected_source
            ):
                failure_id = str(record["attempt_id"])
                return terminal_validation_failure(
                    ManagedImportError(
                        SPRINT_PREFLIGHT_FAILED,
                        409,
                        failure_id,
                        phase="VALIDATE",
                        issues=[
                            {
                                "code": "SOURCE_COMMIT_MISMATCH",
                                "path": "git.expected_source_commit",
                                "message": "Workspace source commit does not match",
                            }
                        ],
                    ),
                    source_commit=workspace_source_commit,
                )
            if expected_source is None and workspace_source_commit != manifest_commit:
                failure_id = str(record["attempt_id"])
                return terminal_validation_failure(
                    ManagedImportError(
                        SPRINT_PREFLIGHT_FAILED,
                        409,
                        failure_id,
                        phase="VALIDATE",
                        issues=[
                            {
                                "code": "SOURCE_COMMIT_REQUIRED",
                                "path": "git.expected_source_commit",
                                "message": "A distinct workspace source needs an expected commit",
                            }
                        ],
                    ),
                    source_commit=workspace_source_commit,
                )
            record = self.store.set_workspace_source(
                project_id,
                str(record["attempt_id"]),
                workspace_source_commit,
                fencing_token=int(record["fencing_token"]),
                clock=self.clock,
            )
            # This fault point is intentionally after the single durable write
            # which binds both provenance commits.  A restart must never
            # resolve either mutable ref again.
            self._fault("after_manifest_read", commit=manifest_commit)

        attempt_id = str(record["attempt_id"])
        fencing_token = int(record["fencing_token"])
        plans: list[dict[str, Any]] = []
        activation_committed = False
        lock_digest = hashlib.sha256(
            f"{project_id}\0{attempt_id}".encode("utf-8")
        ).hexdigest()
        attempt_lock = ManagedFileLock(
            Path(self._managed_root)
            / "locks"
            / "import-attempts"
            / f"{lock_digest}.lock",
            timeout=max(30.0, self.store.timeout),
        )
        try:
            attempt_lock.__enter__()
        except FileLockTimeout:
            raise ManagedImportError(
                PROJECT_ACTIVATION_IN_PROGRESS,
                409,
                attempt_id,
                phase="VALIDATE",
            ) from None
        try:
            latest = self.store.lookup(project_id, request.idempotency_key)
            if latest is None:
                raise RuntimeError("managed start attempt disappeared")
            if latest.get("status") in {"SUCCEEDED", "FAILED"}:
                self._ensure_succeeded_publication(latest)
                return self._replay(latest)
            record = self.store.resume_existing(project_id, request, clock=self.clock)
            if record.get("status") in {"SUCCEEDED", "FAILED"}:
                self._ensure_succeeded_publication(record)
                return self._replay(record)
            fencing_token = int(record["fencing_token"])
            stored_workspace_source = record.get("workspace_source_commit")
            if stored_workspace_source is None:
                record = self.store.set_workspace_source(
                    project_id,
                    attempt_id,
                    workspace_source_commit,
                    fencing_token=fencing_token,
                    clock=self.clock,
                )
            elif stored_workspace_source != workspace_source_commit:
                raise RuntimeError("durable workspace source commit changed")
            current_evidence = record.get("evidence")
            if not isinstance(current_evidence, Mapping) or not isinstance(
                current_evidence.get("git_fetch"), Mapping
            ):
                fetch_result = repository.fetch_result
                record = self.store.update_attempt(
                    project_id,
                    attempt_id,
                    evidence_update={
                        "git_fetch": {
                            "returncode": fetch_result.returncode,
                            "elapsed_seconds": fetch_result.elapsed_seconds,
                            "stdout": fetch_result.stdout,
                            "stderr": fetch_result.stderr,
                        }
                    },
                    fencing_token=fencing_token,
                    clock=self.clock,
                )
            if manifest.get("schema_version") != 1:
                raise ManagedImportError(
                    SPRINT_SCHEMA_UNSUPPORTED,
                    400,
                    attempt_id,
                    phase="VALIDATE",
                    supported=["1"],
                )
            schema_issues = _schema_errors(manifest, _MANIFEST_SCHEMA)
            if schema_issues:
                raise ManagedImportError(
                    INVALID_MANAGED_SPRINT_REQUEST,
                    400,
                    attempt_id,
                    phase="VALIDATE",
                    issues=schema_issues,
                )
            expected = manifest["git"].get("expected_source_commit")
            if expected is not None and workspace_source_commit != expected:
                raise ManagedImportError(
                    SPRINT_PREFLIGHT_FAILED,
                    409,
                    attempt_id,
                    phase="VALIDATE",
                    issues=[
                        {
                            "code": "SOURCE_COMMIT_MISMATCH",
                            "path": "git.expected_source_commit",
                            "message": "Workspace source commit does not match",
                        }
                    ],
                )
            if expected is None and workspace_source_commit != provenance.commit:
                raise ManagedImportError(
                    SPRINT_PREFLIGHT_FAILED,
                    409,
                    attempt_id,
                    phase="VALIDATE",
                    issues=[
                        {
                            "code": "SOURCE_COMMIT_REQUIRED",
                            "path": "git.expected_source_commit",
                            "message": "A distinct workspace source needs an expected commit",
                        }
                    ],
                )
            self._fault("after_pin", attempt_id=attempt_id)
            evidence = record.get("evidence")
            stored_preflight = (
                evidence.get("preflight")
                if isinstance(evidence, Mapping)
                and isinstance(evidence.get("preflight"), Mapping)
                else None
            )
            stored_plans = (
                evidence.get("plans")
                if isinstance(evidence, Mapping)
                and isinstance(evidence.get("plans"), list)
                else None
            )
            if stored_preflight is None or stored_plans is None:
                preflight, plans = self._preflight(
                    project_id,
                    provenance,
                    manifest,
                    repository,
                    workspace_source_commit,
                )
                if not preflight["ok"]:
                    raise ManagedImportError(
                        SPRINT_PREFLIGHT_FAILED,
                        409,
                        attempt_id,
                        phase="VALIDATE",
                        issues=preflight["issues"],
                        evidence={"preflight": preflight},
                    )
                serializable_plans = [
                    {
                        key: deepcopy(value)
                        for key, value in plan.items()
                        if key not in {"workspace_request", "node"}
                    }
                    for plan in plans
                ]
                preparing_state = self._preparing_runtime_state(
                    project_id,
                    request,
                    provenance,
                    manifest,
                    repository,
                    workspace_source_commit,
                    record,
                    preflight,
                )
                record = self.store.stage_preparing_runtime(
                    project_id,
                    attempt_id,
                    preparing_state,
                    evidence_update={
                        "phase": "PREPARE",
                        "preflight": preflight,
                        "plans": serializable_plans,
                    },
                    fencing_token=fencing_token,
                    clock=self.clock,
                )
            else:
                preflight = deepcopy(dict(stored_preflight))
                # Reconstruct all executable plan members deterministically;
                # stored plans are only tamper-evident recovery evidence.
                snapshot = preflight["lease_snapshot"]
                plans, plan_issues = self._activation_plan(
                    project_id,
                    provenance,
                    manifest,
                    repository,
                    workspace_source_commit,
                    snapshot,
                    frozen_plans=stored_plans,
                )
                if plan_issues:
                    raise ManagedImportError(
                        SPRINT_PREFLIGHT_FAILED,
                        409,
                        attempt_id,
                        phase="VALIDATE",
                        issues=plan_issues,
                        evidence={"preflight": preflight},
                    )
                preparing_state = self._preparing_runtime_state(
                    project_id,
                    request,
                    provenance,
                    manifest,
                    repository,
                    workspace_source_commit,
                    record,
                    preflight,
                )
                record = self.store.stage_preparing_runtime(
                    project_id,
                    attempt_id,
                    preparing_state,
                    evidence_update={
                        "phase": "PREPARE",
                        "preflight": preflight,
                        "plans": deepcopy(stored_plans),
                    },
                    fencing_token=fencing_token,
                    clock=self.clock,
                )
            self._fault("after_validate", attempt_id=attempt_id)
            workspaces = self._prepare(
                project_id,
                attempt_id,
                plans,
                repository,
                record,
                fencing_token,
            )
            port_leases, process_records, resource_issues = (
                self._planned_runtime_resources(
                    plans,
                    workspaces,
                    acquired_at=_timestamp(self.clock),
                )
            )
            if resource_issues:
                raise ManagedImportError(
                    SPRINT_PREPARE_FAILED,
                    500,
                    attempt_id,
                    phase="PREPARE",
                    issues=resource_issues,
                )
            record = self.store.update_attempt(
                project_id,
                attempt_id,
                status="ACTIVATING",
                evidence_update={
                    "phase": "ACTIVATE",
                    "prepared_workspaces": workspaces,
                },
                fencing_token=fencing_token,
                clock=self.clock,
            )
            self._fault("after_prepare", attempt_id=attempt_id)
            current_snapshot = self._snapshot()
            expected_snapshot = preflight["lease_snapshot"]
            expected_external = {
                "port_leases": deepcopy(expected_snapshot.get("port_leases") or []),
                "process_owners": deepcopy(
                    expected_snapshot.get("process_owners") or []
                ),
            }
            current_external = {
                "port_leases": deepcopy(current_snapshot.get("port_leases") or []),
                "process_owners": deepcopy(
                    current_snapshot.get("process_owners") or []
                ),
            }
            if canonical_json_sha256(current_external) != canonical_json_sha256(
                expected_external
            ):
                raise ManagedImportError(
                    LEASE_SNAPSHOT_STALE,
                    409,
                    attempt_id,
                    phase="ACTIVATE",
                    evidence={
                        "expected_external_snapshot_sha256": canonical_json_sha256(
                            expected_external
                        ),
                        "actual_external_snapshot_sha256": canonical_json_sha256(
                            current_external
                        ),
                    },
                )
            lease_requests, planned_leases = self._planned_branch_leases(
                plans,
                repository,
                acquired_at=_timestamp(self.clock),
            )
            activated_workspaces = self._activated_workspace_records(
                plans, workspaces, planned_leases
            )
            response = {
                "sprint_id": provenance.stable_sprint_id(),
                "status": "active",
                "phase": "ACTIVATE",
                "deduplicated": False,
                "execution_mode": manifest["execution"]["mode"],
                "identity": asdict(provenance),
                "workspace_source_commit": workspace_source_commit,
                "initial_assignment_ids": [
                    str(plan["assignment_id"]) for plan in plans
                ],
            }
            runtime_state = self._runtime_state(
                project_id,
                request,
                provenance,
                manifest,
                repository,
                workspace_source_commit,
                record,
                preflight,
                plans,
                activated_workspaces,
                [_branch_runtime_record(lease) for lease in planned_leases],
                port_leases,
                process_records,
                response,
            )
            self._fault("before_activate", attempt_id=attempt_id)
            committed = self.store.activate(
                project_id,
                attempt_id,
                runtime_state,
                response,
                fencing_token=fencing_token,
                branch_lease_store=self.branch_leases,
                branch_lease_requests=lease_requests,
                expected_lease_snapshot=preflight["lease_snapshot"],
                external_snapshot_provider=self.lease_snapshot_provider,
                port_reservations=self.port_reservations,
                workspace_publisher=lambda acquired: self._publish_workspaces(
                    plans,
                    workspaces,
                    repository,
                    attempt_id,
                    acquired,
                ),
                clock=self.clock,
            )
            if not committed:
                completed = self.store.lookup(project_id, request.idempotency_key)
                if completed is None:
                    raise RuntimeError("committed managed attempt disappeared")
                self._ensure_succeeded_publication(completed)
                return self._replay(completed)
            port_fence = self.port_reservations.reconciliation_fence(
                self.store.database_path
            )
            self._reconcile_local_port_claims(
                self.store.active_runtime_states(),
                correlation_id=attempt_id,
                fence=port_fence,
            )
            self._fault("after_activation_commit", attempt_id=attempt_id)
            settled = self.store.lookup(project_id, request.idempotency_key)
            if settled is None or settled.get("status") != "SUCCEEDED":
                raise RuntimeError("committed managed attempt is not replayable")
            self._ensure_succeeded_publication(settled)
            activation_committed = True
            self._fault("after_activate", attempt_id=attempt_id)
            return ManagedStartResult(response, 201)
        except ManagedImportError as exc:
            if activation_committed:
                return ManagedStartResult(response, 201)
            settled = self.store.lookup(project_id, request.idempotency_key)
            if settled is not None and settled.get("status") == "SUCCEEDED":
                self._ensure_succeeded_publication(settled)
                stored = deepcopy(settled.get("response"))
                if isinstance(stored, dict):
                    return ManagedStartResult(stored, 201)
            self.store.fail_attempt(
                project_id,
                attempt_id,
                exc,
                evidence=exc.evidence
                or {
                    "phase": exc.envelope["detail"].get("phase", "VALIDATE"),
                    "error": exc.code,
                },
                fencing_token=fencing_token,
                clock=self.clock,
            )
            raise
        except ManagedGitError as exc:
            if exc.code in TRANSIENT_GIT_ERROR_CODES:
                phase = str(record.get("status") or "VALIDATING")
                stable_phase = (
                    "VALIDATE"
                    if phase == "VALIDATING"
                    else "PREPARE"
                    if phase == "PREPARING"
                    else "ACTIVATE"
                )
                raise ManagedImportError(
                    SPRINT_PREFLIGHT_FAILED,
                    503,
                    attempt_id,
                    phase=stable_phase,
                    issues=[
                        {
                            "code": exc.code,
                            "path": stable_phase.lower(),
                            "message": "Managed Git infrastructure is temporarily unavailable",
                        }
                    ],
                    evidence={
                        "phase": stable_phase,
                        "failure_code": exc.code,
                        "retryable": True,
                    },
                ) from exc
            phase = str(record.get("status") or "VALIDATING")
            stable_phase = (
                "VALIDATE"
                if phase == "VALIDATING"
                else "PREPARE"
                if phase == "PREPARING"
                else "ACTIVATE"
            )
            normalized = ManagedImportError(
                SPRINT_PREFLIGHT_FAILED,
                409,
                attempt_id,
                phase=stable_phase,
                issues=[
                    {
                        "code": exc.code,
                        "path": stable_phase.lower(),
                        "message": "Managed Git validation failed",
                    }
                ],
                evidence={
                    "phase": stable_phase,
                    "failure_code": exc.code,
                },
            )
            self.store.fail_attempt(
                project_id,
                attempt_id,
                normalized,
                evidence=normalized.evidence,
                fencing_token=fencing_token,
                clock=self.clock,
            )
            raise normalized from exc
        except Exception as exc:
            if activation_committed:
                return ManagedStartResult(response, 201)
            settled = self.store.lookup(project_id, request.idempotency_key)
            if settled is not None and settled.get("status") == "SUCCEEDED":
                self._ensure_succeeded_publication(settled)
                stored = deepcopy(settled.get("response"))
                if isinstance(stored, dict):
                    return ManagedStartResult(stored, 201)
            phase = str(record.get("status") or "VALIDATING")
            if phase == "VALIDATING":
                stable_phase = "VALIDATE"
                code = SPRINT_PREFLIGHT_FAILED
                http_status = 409
            elif phase == "PREPARING":
                stable_phase = "PREPARE"
                code = SPRINT_PREPARE_FAILED
                http_status = 500
            else:
                stable_phase = "ACTIVATE"
                code = SPRINT_ACTIVATE_FAILED
                http_status = 500
            normalized = ManagedImportError(
                code,
                http_status,
                attempt_id,
                phase=stable_phase,
                issues=[
                    {
                        "code": getattr(exc, "code", code),
                        "path": stable_phase.lower(),
                        "message": "Managed import phase failed",
                    }
                ],
                evidence={
                    "phase": stable_phase,
                    "failure_code": getattr(exc, "code", code),
                },
            )
            self.store.fail_attempt(
                project_id,
                attempt_id,
                normalized,
                evidence=normalized.evidence,
                fencing_token=fencing_token,
                clock=self.clock,
            )
            raise normalized from exc
        finally:
            attempt_lock.__exit__(None, None, None)


__all__ = [
    "IDEMPOTENCY_KEY_CONFLICT",
    "INVALID_MANAGED_SPRINT_REQUEST",
    "LEASE_SNAPSHOT_STALE",
    "MANAGED_SPRINT_TYPE_REQUIRED",
    "ManagedImportError",
    "ManagedImportStore",
    "ManagedRetryImportConflict",
    "ManagedPortReservationRegistry",
    "ManagedPortReservationToken",
    "ManagedStartResult",
    "PROJECT_ACTIVATION_IN_PROGRESS",
    "SPRINT_ACTIVATE_FAILED",
    "SPRINT_ALREADY_EXISTS",
    "SPRINT_PREFLIGHT_FAILED",
    "SPRINT_PREPARE_FAILED",
    "SPRINT_RECOVERY_REQUIRED",
    "SPRINT_SCHEMA_UNSUPPORTED",
    "TransactionalSprintImporter",
    "empty_project_control",
    "normalize_managed_runtime_config",
    "managed_schema_errors",
    "managed_workspace_from_record",
    "managed_workspace_record",
    "parse_strict_json_object",
    "parse_start_request_bytes",
    "validate_start_request",
]
