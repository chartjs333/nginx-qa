"""Setup and Diagnostics Wizard v1 — deterministic diagnostics engine and contracts.

Strictly passive read-only state inspection, bound networking and secrets protection.
No background schedulers, no hidden mutations, no external network calls on GET.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Mapping, Sequence

import jsonschema


# Initial Server-Owned Action and Status Catalogs (contracts.schema.json)
CHECK_STATUSES = ("PASS", "WARN", "FAIL", "UNKNOWN", "NOT_CONFIGURED", "BLOCKED")
CHECK_SCOPES = ("passive", "connection", "execution")
CHECK_FRESHNESS = ("FRESH", "STALE", "UNOBSERVED")
EVIDENCE_KINDS = ("CODE", "CONFIG_METADATA", "LIVE_PROBE", "MANAGED_RESULT", "MOCK", "NONE")
ACTION_IDS = (
    "diagnostics.refresh",
    "worker.probe",
    "worker.bind",
    "provider.probe",
    "telegram.probe",
    "setup.open",
    "selftest.start",
    "selftest.stop",
    "recovery.open",
    "git.probe",
)
ERROR_CODES = (
    "TIMEOUT",
    "DNS_ERROR",
    "CONNECTION_ERROR",
    "AUTH_REJECTED",
    "ACCESS_DENIED",
    "NOT_CONFIGURED",
    "PROTOCOL_INVALID",
    "PROBE_UNSUPPORTED",
    "SOURCE_STALE",
    "PROJECT_BUSY",
    "OWNERSHIP_MISMATCH",
    "STORE_UNAVAILABLE",
    "STATE_CORRUPT",
    "UNSUPPORTED_OPERATION",
)

_PROJECT_ID_PATTERN = re.compile(r"^9[0-9]{3}$")
_CHECK_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,79}$")
_OPAQUE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_BOT_TOKEN_PATTERN = re.compile(r"\b\d{8,11}:[A-Za-z0-9_-]{35}\b")

_COMPILED_SCHEMA: dict[str, Any] | None = None
_SNAPSHOT_VALIDATOR: jsonschema.Draft202012Validator | None = None


def get_contracts_schema() -> dict[str, Any]:
    global _COMPILED_SCHEMA
    if _COMPILED_SCHEMA is None:
        schema_path = Path(__file__).resolve().parents[1] / "docs" / "setup-diagnostics-wizard-v1" / "contracts.schema.json"
        if not schema_path.exists():
            raise FileNotFoundError(f"Contracts schema not found at {schema_path}")
        with open(schema_path, "r", encoding="utf-8") as f:
            _COMPILED_SCHEMA = json.load(f)
    return _COMPILED_SCHEMA


def get_snapshot_validator() -> jsonschema.Draft202012Validator:
    global _SNAPSHOT_VALIDATOR
    if _SNAPSHOT_VALIDATOR is None:
        schema = get_contracts_schema()
        subschema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": "#/$defs/DiagnosticSnapshot",
        }
        _SNAPSHOT_VALIDATOR = jsonschema.Draft202012Validator(
            subschema, format_checker=jsonschema.FormatChecker()
        )
    return _SNAPSHOT_VALIDATOR


def canonical_json_bytes(obj: Any) -> bytes:
    """Deterministic UTF-8 canonical JSON bytes with sorted keys and no space."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_digest(data: bytes | str) -> str:
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return hashlib.sha256(raw).hexdigest()


def compute_snapshot_id(snapshot: Mapping[str, Any]) -> str:
    """Compute snapshot_id as SHA-256 digest of canonical JSON without snapshot_id."""
    cleaned = {k: v for k, v in snapshot.items() if k != "snapshot_id"}
    return sha256_digest(canonical_json_bytes(cleaned))


def compute_authority_fingerprint(
    project_id: str,
    *,
    instance_id: str | None = None,
    git_head: str | None = None,
    control_revision: int = 0,
    policy_generation: int = 1,
    active_sprint_id: str | None = None,
    active_assignment_id: str | None = None,
) -> str:
    payload = {
        "active_assignment_id": str(active_assignment_id or ""),
        "active_sprint_id": str(active_sprint_id or ""),
        "control_revision": int(control_revision),
        "git_head": str(git_head or ""),
        "instance_id": str(instance_id or ""),
        "policy_generation": int(policy_generation),
        "project_id": str(project_id),
    }
    return sha256_digest(canonical_json_bytes(payload))


def parse_strict_json_with_limits(
    data: bytes | str,
    *,
    max_bytes: int = 64 * 1024,
    max_depth: int = 16,
) -> Any:
    """Strict JSON parser rejecting duplicates, non-finites, excess depth and body size."""
    raw_bytes = data.encode("utf-8") if isinstance(data, str) else data
    if len(raw_bytes) > max_bytes:
        raise ValueError(f"JSON payload exceeded maximum size {max_bytes} bytes")

    def _object_pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings")
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    parsed = json.loads(
        raw_bytes.decode("utf-8"),
        object_pairs_hook=_object_pairs_hook,
        parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Invalid JSON constant: {c}")),
    )

    def _check_depth(node: Any, current_depth: int = 1) -> None:
        if current_depth > max_depth:
            raise ValueError(f"JSON nesting depth exceeded {max_depth}")
        if isinstance(node, dict):
            for v in node.values():
                _check_depth(v, current_depth + 1)
        elif isinstance(node, list):
            for v in node:
                _check_depth(v, current_depth + 1)

    _check_depth(parsed)
    return parsed


def redact_secrets(text: str, extra_canaries: Sequence[str] = ()) -> str:
    """Remove telegram bot tokens, secret env references and known canary secrets."""
    if not isinstance(text, str):
        return text
    result = _BOT_TOKEN_PATTERN.sub("[REDACTED_BOT_TOKEN]", text)
    for canary in extra_canaries:
        if canary and canary in result:
            result = result.replace(canary, "[REDACTED_SECRET]")
    # Redact protected control env names if their values appear in text
    for env_name, env_val in os.environ.items():
        folded = env_name.casefold()
        if (folded.startswith(("nginx_qa_scope_control_", "nginx_qa_launcher_control_"))
                or folded in {"telegram_bot_token", "telegram_webhook_secret", "gemini_api_key"}):
            if env_val and len(env_val) >= 4 and env_val in result:
                result = result.replace(env_val, "[REDACTED_CREDENTIAL]")
    return result



def _bounded_bytes(path: Path, limit: int) -> bytes:
    """Bound before parsing, including files that grow between stat and read."""
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError("SOURCE_LIMIT_EXCEEDED")
    return data


def _read_json_file(path: Path, limit: int = 65536) -> dict[str, Any]:
    value = parse_strict_json_with_limits(_bounded_bytes(path, limit), max_bytes=limit, max_depth=32)
    if not isinstance(value, dict):
        raise ValueError("STATE_CORRUPT")
    return value


def _diagnostic_environment(environment: Mapping[str, Any]) -> dict[str, Any]:
    # Never retain a copy of the process environment or raw credentials in the
    # issued-snapshot cache. These checks only need configuration presence.
    result: dict[str, Any] = {}
    for name in ("NGINX_QA_GIT_CONFIG_PATH", "NGINX_QA_LAUNCHER_ORIGIN_STATE",
                 "NGINX_QA_STAGING_STATE_BASE", "NGINX_QA_INSTANCE_ID", "NGINX_QA_GATEWAY_PORT"):
        value = environment.get(name)
        if isinstance(value, str) and 0 < len(value) <= 4096:
            result[name] = value
    for name in ("NGINX_QA_LAUNCHER_EXECUTOR_URL", "NGINX_QA_LAUNCHER_BOT_SECRET_REF",
                 "NGINX_QA_LAUNCHER_WEBHOOK_SECRET_REF", "NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF",
                 "NGINX_QA_LAUNCHER_ADVISOR_SECRET_REF", "NGINX_QA_LAUNCHER_CONTROL_ADVISOR_KEY",
                 "TELEGRAM_BOT_TOKEN"):
        result[name] = bool(environment.get(name))
    return result


class DiagnosticSourceUnavailable(ValueError):
    def __init__(self, error_code: str = "SOURCE_STALE") -> None:
        super().__init__(error_code)
        self.error_code = error_code


_DB_IMAGE_ADMISSION = threading.BoundedSemaphore(1)


class _SnapshotConnection(sqlite3.Connection):
    _owns_admission = False

    def close(self):
        try:
            super().close()
        finally:
            if self._owns_admission:
                self._owns_admission = False
                _DB_IMAGE_ADMISSION.release()

    def __del__(self):
        self.close()


def open_readonly_snapshot(path: Path, *, max_bytes: int = 64 * 1024 * 1024) -> sqlite3.Connection:
    # Shared by GET, plan/apply authority checks, status and recovery readers.
    # Admission lives until SQLite releases the image, not until HTTP cancels.
    if not _DB_IMAGE_ADMISSION.acquire(blocking=False):
        raise DiagnosticSourceUnavailable("SOURCE_BUSY")
    try:
        connection = _open_readonly_snapshot(path, max_bytes=max_bytes)
    except BaseException:
        _DB_IMAGE_ADMISSION.release()
        raise
    connection._owns_admission = True
    return connection


def _open_readonly_snapshot(path: Path, *, max_bytes: int) -> sqlite3.Connection:
    """Open a bounded stable database image in RAM, never the live SQLite files.

    SQLite mode=ro can create or update WAL shared-memory files; immutable=1
    ignores uncheckpointed WAL. Therefore a nonempty WAL or active rollback
    journal is explicitly unavailable, never silently read as an older DB.
    No checkpoint, recovery, lock file, temporary file, or journal is created.
    Caller must close the returned connection (contextlib.closing). At the
    default limit the transient image and SQLite copy require about 128 MiB;
    all callers share process-wide image admission until connection.close().
    """
    path = Path(path)
    wal, journal = Path(str(path) + "-wal"), Path(str(path) + "-journal")
    def signature() -> tuple:
        values = []
        for source in (path, wal, journal):
            try:
                info = source.stat()
                values.append((info.st_size, info.st_mtime_ns, info.st_ctime_ns))
            except FileNotFoundError:
                values.append(None)
        if values[0] is None:
            raise DiagnosticSourceUnavailable("STORE_UNAVAILABLE")
        if values[1] is not None and values[1][0] > 0:
            raise DiagnosticSourceUnavailable()
        if values[2] is not None and values[2][0] > 0:
            with journal.open("rb") as handle:
                if any(handle.read(8)):
                    raise DiagnosticSourceUnavailable()
        return tuple(values)
    before = signature()
    image = bytearray(_bounded_bytes(path, max_bytes))
    # Checkpoint writes and sources changed during the copy must not appear valid.
    if before != signature() or image != _bounded_bytes(path, max_bytes) or before != signature():
        raise DiagnosticSourceUnavailable()
    if len(image) < 100 or image[:16] != b"SQLite format 3\x00" or image[18] not in (1, 2) or image[19] not in (1, 2):
        raise DiagnosticSourceUnavailable("STATE_CORRUPT")
    # This copy has no WAL and is never persisted. Deserialize otherwise attempts
    # to find a WAL beside its in-memory database when the original header is WAL.
    image[18:20] = b"\x01\x01"
    connection = sqlite3.connect(":memory:", factory=_SnapshotConnection)
    try:
        connection.deserialize(image)
        connection.execute("PRAGMA temp_store=MEMORY")
        connection.execute("PRAGMA query_only=ON")
        return connection
    except BaseException:
        connection.close()
        raise


def _git_metadata(root: Path | None) -> tuple[str, str, bool, tuple[Path, ...]]:
    """Read only local HEAD metadata, including worktree/common-directory indirection."""
    if root is None:
        return "0" * 40, "unknown", False, ()
    paths: list[Path] = [root / ".git"]
    try:
        git_dir = root / ".git"
        if git_dir.is_file():
            pointer = _bounded_bytes(git_dir, 4096).decode("utf-8").strip()
            if not pointer.startswith("gitdir: "):
                raise ValueError
            git_dir = (root / pointer[8:]).resolve()
        # Explicit bare repository pointers are also supported.
        elif not git_dir.is_dir() and (root / "HEAD").is_file() and (root / "objects").is_dir():
            git_dir = root
        paths.extend((git_dir / "HEAD", git_dir / "commondir"))
        common = git_dir
        if paths[-1].is_file():
            common = (git_dir / _bounded_bytes(paths[-1], 4096).decode("utf-8").strip()).resolve()
        head = _bounded_bytes(git_dir / "HEAD", 4096).decode("utf-8").strip()
        branch = "detached"
        if head.startswith("ref: "):
            ref = head[5:]
            if not re.fullmatch(r"refs/[A-Za-z0-9_./-]{1,240}", ref) or ".." in ref or "//" in ref:
                raise ValueError
            branch = ref.removeprefix("refs/heads/")
            paths.extend((git_dir / ref, common / ref, common / "packed-refs"))
            head = ""
            for ref_path in paths[-3:-1]:
                if ref_path.is_file():
                    head = _bounded_bytes(ref_path, 4096).decode("ascii").strip()
                    break
            if not head and (common / "packed-refs").is_file():
                for line in _bounded_bytes(common / "packed-refs", 1048576).decode("ascii").splitlines():
                    if line.startswith(("#", "^")):
                        continue
                    fields = line.split(" ")
                    if len(fields) == 2 and fields[1] == ref:
                        head = fields[0]
                        break
        if not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", head) or set(head) == {"0"}:
            raise ValueError
        return head, branch, True, tuple(paths)
    except (OSError, ValueError, UnicodeError):
        return "0" * 40, "unknown", False, tuple(paths)


class DiagnosticSnapshotCollector:
    """Bounded filesystem/SQLite projections; no constructors that initialize state.

    All optional metadata comes from the trusted host, never a request body. A
    repository_root is the selected project's verified repository pointer, not
    the application checkout. Missing evidence remains UNKNOWN.
    """

    def __init__(
        self, *, code_root: Path | None = None, git_config_path: Path | None = None,
        state_base: Path | None = None, store_path: Path | None = None,
        repository_root: Path | None = None, project_metadata: Mapping[str, Any] | None = None,
        binding_metadata: Mapping[str, Any] | None = None,
        environment: Mapping[str, Any] | None = None, now: datetime | None = None,
    ) -> None:
        self.code_root = Path(code_root) if code_root else Path(__file__).resolve().parents[1]
        self.environment = _diagnostic_environment(os.environ if environment is None else environment)
        self.git_config_path = Path(git_config_path) if git_config_path else (
            Path(self.environment["NGINX_QA_GIT_CONFIG_PATH"]) if self.environment.get("NGINX_QA_GIT_CONFIG_PATH") else None
        )
        self.state_base = Path(state_base) if state_base else None
        # Never guess another installation's database or instantiate its store.
        self.store_path = Path(store_path) if store_path else None
        self.repository_root = Path(repository_root) if repository_root else None
        self.project_metadata = dict(project_metadata) if project_metadata is not None else None
        self.binding_metadata = dict(binding_metadata) if binding_metadata is not None else None
        self.now = now or datetime.now(timezone.utc)

    def _resolve_git_head_and_branch(self) -> tuple[str, str, bool]:
        return _git_metadata(self.code_root)[:3]

    def _read_git_config(self) -> tuple[dict[str, Any] | None, str | None]:
        if self.git_config_path is None or not self.git_config_path.is_file():
            return None, "NOT_CONFIGURED"
        try:
            return _read_json_file(self.git_config_path), None
        except (OSError, ValueError, UnicodeError, RecursionError):
            return None, "STATE_CORRUPT"

    def _project_entry(self, project_id: str) -> tuple[dict[str, Any] | None, str | None]:
        if self.project_metadata is not None:
            entry = self.project_metadata
            if str(entry.get("project_phone") or entry.get("project_id")) != project_id:
                return None, "OWNERSHIP_MISMATCH"
            return entry, None
        config, error = self._read_git_config()
        if config is None:
            return None, error
        projects = config.get("projects")
        if not isinstance(projects, dict):
            return None, "STATE_CORRUPT"
        entries = [dict(v, git_context_key=v.get("git_context_key", k))
                   for k, v in projects.items() if isinstance(v, dict) and v.get("project_phone") == project_id]
        # Phone-map entries are routing aliases, never evidence of registration.
        if len(entries) != 1:
            return None, "OWNERSHIP_MISMATCH"
        entry = entries[0]
        from nginx_qa.git_provider import canonical_remote_from_address
        remote = canonical_remote_from_address(entry.get("git_address"))
        context = entry.get("git_context_key")
        if not remote or not isinstance(context, str) or context.split("#", 1)[0] != remote:
            return None, "STATE_CORRUPT"
        return entry, None

    def source_version(self) -> str:
        """Local invalidation signature: metadata only, without network or state writes."""
        files: list[Path] = []
        if self.git_config_path:
            files.append(self.git_config_path)
        if self.store_path:
            files.extend((self.store_path, Path(str(self.store_path) + "-wal"), Path(str(self.store_path) + "-journal")))
        files.extend(_git_metadata(self.code_root)[3])
        files.extend(_git_metadata(self.repository_root)[3])
        origin = self.environment.get("NGINX_QA_LAUNCHER_ORIGIN_STATE")
        state = self.state_base or self.environment.get("NGINX_QA_STAGING_STATE_BASE")
        if origin:
            files.append(Path(str(origin)))
        if state:
            files.append(Path(state) / ".nginx-qa-staging-owner.json")
        stamps = []
        for path in files:
            try:
                st = path.stat()
                stamps.append((str(path), st.st_size, st.st_mtime_ns, st.st_ctime_ns))
            except OSError:
                stamps.append((str(path), None))
        return sha256_digest(canonical_json_bytes(stamps))

    def _read_sqlite_store(self, project_id: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "available": False, "error_code": "STORE_UNAVAILABLE", "control_revision": 0,
            "active_sprint_id": None, "active_assignment_id": None, "sprint_status": None,
            "graph_revision": None, "slot_busy": None, "port_leases_count": None,
            "branch_leases_count": None, "queue_items_count": None, "pending_reviews_count": None,
            "has_pending_recovery": None, "resources_available": False, "delivery_available": False,
            "launcher_slot_observed": False,
        }
        if self.store_path is None or not self.store_path.is_file():
            return result
        connection = None
        try:
            connection = open_readonly_snapshot(self.store_path)
            deadline = time.monotonic() + 1.0
            steps = 0
            def bounded_query() -> int:
                nonlocal steps
                steps += 1000
                return int(steps > 250000 or time.monotonic() > deadline)
            connection.set_progress_handler(bounded_query, 1000)
            connection.row_factory = sqlite3.Row
            # One consistent read transaction. No migrations, schema healing, or PRAGMA writes.
            connection.execute("BEGIN")
            row = connection.execute(
                "SELECT CASE WHEN length(CAST(control_json AS BLOB)) <= 2097152 THEN control_json END AS data, revision "
                "FROM managed_projects WHERE project_id = ?", (project_id,),
            ).fetchone()
            if row is None:
                return result
            control = parse_strict_json_with_limits(row["data"], max_bytes=2097152, max_depth=32)
            required = {"project_id", "active_sprint_id", "activation_lease", "start_idempotency_records"}
            if (not isinstance(control, dict) or not required.issubset(control)
                    or control["project_id"] != project_id or type(row["revision"]) is not int
                    or row["revision"] < 0 or not isinstance(control["start_idempotency_records"], list)):
                raise ValueError
            result["control_revision"] = row["revision"]
            # Canonical index lookup is bounded independently of large terminal
            # histories. Historical resource health is explicitly unobserved.
            index_rows = connection.execute(
                "SELECT sprint_id, status FROM managed_sprints WHERE project_id = ? LIMIT 1025", (project_id,),
            ).fetchall()
            if len(index_rows) > 1024:
                raise ValueError
            known_sprints = {r["sprint_id"] for r in index_rows}
            states = []
            total_bytes = 0
            cursor = connection.execute(
                "SELECT project_id, sprint_id, status, fencing_token, CASE WHEN length(CAST(state_json AS BLOB)) <= 2097152 "
                "THEN state_json END AS data FROM managed_sprints WHERE project_id = ? "
                "AND (sprint_id = ? OR status IN ('active','preparing')) LIMIT 129",
                (project_id, control["active_sprint_id"]),
            )
            for row in cursor:
                if len(states) >= 128 or row["data"] is None:
                    raise ValueError
                total_bytes += len(row["data"].encode("utf-8"))
                if total_bytes > 2097152:
                    raise ValueError
                state = parse_strict_json_with_limits(row["data"], max_bytes=2097152, max_depth=32)
                if (not isinstance(state, dict) or state.get("sprint_id") != row["sprint_id"]
                        or state.get("status") != row["status"]
                        or state.get("identity", {}).get("project_id") != project_id):
                    raise ValueError
                # These existing validators are pure functions; no store is constructed.
                from nginx_qa.managed_import import ManagedImportStore
                if ManagedImportStore._runtime_row_issues(
                        control, row, state, require_indexed=control["active_sprint_id"] == row["sprint_id"]):
                    raise ValueError
                if time.monotonic() > deadline:
                    raise ValueError
                states.append(state)
            active = [s for s in states if s["status"] == "active"]
            from nginx_qa.managed_import import _schema_errors
            from nginx_qa.sprint_types import managed_project_control_invariant_issues
            if (_schema_errors(control, "managed-project-control-v1.schema.json", issue_code="STATE_CORRUPT")
                    or managed_project_control_invariant_issues(control, known_sprints)
                    or len(active) > 1
                    or (active and control["active_sprint_id"] != active[0]["sprint_id"])):
                raise ValueError
            try:
                reservations = connection.execute(
                    "SELECT operation_id FROM telegram_launcher_operations WHERE project_id = ? AND holds_slot = 1 LIMIT 2",
                    (project_id,),
                ).fetchall()
                result["launcher_slot_observed"] = True
            except sqlite3.OperationalError:
                # Older stores may lack the launcher extension. Preserve native
                # runtime evidence but never infer a free slot from absent data.
                reservations = []
            if len(reservations) > 1:
                raise ValueError
            records = control["start_idempotency_records"]
            if any(not isinstance(r, dict) for r in records):
                raise ValueError
            preparing = any(r.get("status") in {"VALIDATING", "PREPARING", "PREPARED", "ACTIVATING"} for r in records)
            result["slot_busy"] = bool(active or reservations or control["activation_lease"] is not None or preparing
                                       or any(s["status"] == "preparing" for s in states)
                                       or any(s.get("active_assignment_ids") for s in states))
            if not result["slot_busy"] and not result["launcher_slot_observed"]:
                result["slot_busy"] = None
            result["has_pending_recovery"] = any(s["status"] == "blocked" for s in states)
            assignment_ids: set[str] = set()
            port_lease_ids: set[str] = set()
            branch_lease_ids: set[str] = set()
            event_ids: set[str] = set()
            pending_reviews = 0
            for state in states:
                result["has_pending_recovery"] |= any(r["status"] == "pending" for r in state["recovery_records"])
                assignments = state.get("assignments")
                outbox = state.get("outbox")
                if not isinstance(assignments, list) or not isinstance(outbox, list):
                    raise ValueError
                for assignment in assignments:
                    aid = assignment.get("assignment_id") if isinstance(assignment, dict) else None
                    if not isinstance(aid, str) or not _OPAQUE_ID_PATTERN.fullmatch(aid):
                        raise ValueError
                    assignment_ids.add(aid)
                for event in outbox:
                    if not isinstance(event, dict) or not isinstance(event.get("event_id"), str):
                        raise ValueError
                    event_ids.add(event["event_id"])
                for collection, identifiers in (("port_leases", port_lease_ids), ("branch_leases", branch_lease_ids)):
                    for lease in state[collection]:
                        identifiers.add(lease["lease_id"])
                if any(len(ids) > 1024 for ids in (assignment_ids, event_ids, port_lease_ids, branch_lease_ids)):
                    raise ValueError
                if state["status"] == "active":
                    result.update(active_sprint_id=state["sprint_id"], sprint_status=state["status"],
                                  graph_revision=state.get("graph_revision"))
                    current = [a for a in assignments if a.get("status") == "active"]
                    result["active_assignment_id"] = current[0]["assignment_id"] if current else None
                    result["has_pending_recovery"] |= bool(state.get("pending_recovery") or state.get("operator_stop_requested"))
                    # Native review assignments carry source assignment and exact result identity.
                    reviews = state.get("review_assignments")
                    if not isinstance(reviews, list):
                        raise ValueError
                    pending_reviews = sum(1 for r in reviews if isinstance(r, dict) and r.get("status") != "decided")
            result["pending_reviews_count"] = pending_reviews
            def scoped_count(table: str, key: str, ids: set[str], predicate: str = "") -> int:
                # An empty ID set cannot prove a missing projection table exists.
                connection.execute(f"SELECT {key} FROM {table} WHERE 0")
                if not ids:
                    return 0
                count = 0
                ordered = sorted(ids)
                for offset in range(0, len(ordered), 100):
                    chunk = ordered[offset:offset + 100]
                    sql = f"SELECT COUNT(*) FROM {table} WHERE {key} IN ({','.join('?' for _ in chunk)})"
                    if predicate:
                        sql += " AND " + predicate
                    count += int(connection.execute(sql, chunk).fetchone()[0])
                return count
            # No global scans/aggregate values are exposed. Native ownership comes from this project's states.
            try:
                result["port_leases_count"] = scoped_count(
                    "managed_port_leases", "lease_id", port_lease_ids, "status IN ('reserved','bound')")
                result["branch_leases_count"] = scoped_count(
                    "branch_leases", "lease_id", branch_lease_ids, "status = 'active'")
                result["resources_available"] = True
            except sqlite3.DatabaseError:
                pass
            try:
                result["queue_items_count"] = scoped_count("managed_queue_items", "event_id", event_ids)
                result["delivery_available"] = True
            except sqlite3.DatabaseError:
                pass
            result.update(available=True, error_code=None)
            return result
        except DiagnosticSourceUnavailable as exc:
            result["error_code"] = exc.error_code
            return result
        except (sqlite3.DatabaseError, OSError):
            result["error_code"] = "STORE_UNAVAILABLE"
            return result
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, AttributeError):
            result["error_code"] = "STATE_CORRUPT"
            return result
        finally:
            if connection is not None:
                connection.close()

    def _tunnel_state(self, instance_id: str | None) -> tuple[str, str, bool]:
        setting = self.environment.get("NGINX_QA_LAUNCHER_ORIGIN_STATE")
        if not setting:
            return "NOT_CONFIGURED", "NOT_CONFIGURED", False
        try:
            # Existing ownership path validator rejects symlinks, junctions and aliases.
            from nginx_qa.launcher_host import _path, _same_path
            state_setting = self.state_base or self.environment.get("NGINX_QA_STAGING_STATE_BASE")
            if not state_setting:
                raise ValueError
            state = _path(state_setting)
            expected = state / "legacy" / "runtime_state" / "launcher-origin.json"
            if not _same_path(setting, expected):
                raise ValueError
            marker = _read_json_file(_path(state / ".nginx-qa-staging-owner.json"), 32768)
            owner = marker["ownership"]
            receipt = _read_json_file(_path(expected), 32768)
            configured_instance = self.environment.get("NGINX_QA_INSTANCE_ID")
            if (marker.get("schema_version") != 1 or receipt.get("schema_version") != 1
                    or not configured_instance or owner["instance_id"] != configured_instance
                    or (instance_id is not None and instance_id != configured_instance)
                    or type(owner["gateway_port"]) is not int
                    or owner["gateway_port"] != int(self.environment.get("NGINX_QA_GATEWAY_PORT", ""))
                    or not _same_path(owner["state_base"], state)
                    or not _same_path(owner["gateway_origin_state"], expected)
                    or receipt["initialization_id"] != marker["initialization_id"]
                    or receipt["instance_id"] != owner["instance_id"]
                    or receipt["upstream"] != f"http://127.0.0.1:{owner['gateway_port']}"
                    or not re.fullmatch(r"https://[a-z0-9-]+\.trycloudflare\.com", receipt.get("public_origin", ""))):
                raise ValueError
            heartbeat = receipt.get("heartbeat")
            fresh = (receipt.get("phase") == "READY" and type(heartbeat) in (float, int)
                     and 0 <= self.now.timestamp() - heartbeat <= 15)
            return "PASS", "PASS" if fresh else "WARN", fresh
        except (OSError, ValueError, TypeError, KeyError):
            return "BLOCKED", "UNKNOWN", False

    def collect_snapshot(
        self, project_id: str, *, instance_id: str | None = None, actor_id: str = "",
        policy_generation: int = 1, authority_version: str = "",
        authorized_action_ids: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        if not _PROJECT_ID_PATTERN.fullmatch(project_id):
            raise ValueError("Invalid canonical project ID")
        if instance_id is not None and not _OPAQUE_ID_PATTERN.fullmatch(instance_id):
            raise ValueError("Invalid instance ID")
        app_commit, _, app_git_ok = self._resolve_git_head_and_branch()
        project, project_error = self._project_entry(project_id)
        store = self._read_sqlite_store(project_id)
        head, branch, git_ok, _ = _git_metadata(self.repository_root if project is not None else None)
        now_iso = self.now.isoformat()
        expires_iso = datetime.fromtimestamp(self.now.timestamp() + 120, timezone.utc).isoformat()
        config_version = sha256_digest(canonical_json_bytes({
            "source": self.source_version(), "authority": authority_version,
            "policy": policy_generation, "project": project, "binding": self.binding_metadata,
        }))
        source = {"adapter_id": "diagnostic-core-adapter", "adapter_version": "1.1.0",
                  "code_commit": app_commit, "configuration_version": config_version}
        allowed = set(ACTION_IDS if authorized_action_ids is None else authorized_action_ids) & set(ACTION_IDS)
        checks: list[dict[str, Any]] = []
        def add(check_id: str, status: str, facts: Mapping[str, Any] | None = None, *,
                error: str | None = None, observed: bool = True, stale: bool = False,
                evidence_kind: str = "CONFIG_METADATA", actions: Sequence[str] = ()) -> None:
            facts = facts or {}
            evidence = []
            for key, value in facts.items():
                if isinstance(value, str):
                    value = redact_secrets(value)[:256]
                if type(value) is int and not -(2**31) <= value < 2**31:
                    value = None
                if value is not None and type(value) not in (bool, int, str):
                    raise ValueError("Unsafe diagnostic fact")
                evidence.append({"fact_id": key, "value": value})
            checks.append({
                "check_id": check_id, "status": status, "scope": "passive",
                "observed_at": now_iso if observed else None, "expires_at": expires_iso if observed else None,
                "freshness": ("STALE" if stale else "FRESH") if observed else "UNOBSERVED",
                "source": source, "evidence_kind": evidence_kind, "evidence": evidence,
                "error_code": error, "capability_ids": [],
                "next_action_ids": [a for a in actions if a in allowed],
            })
        add("project.identity", "PASS" if project is not None else (
            "NOT_CONFIGURED" if project_error == "NOT_CONFIGURED" else "BLOCKED"),
            {"project.canonical_phone": project_id} if project is not None else {},
            error=project_error, observed=project is not None, actions=("setup.open",))
        available = store["available"]
        slot_observed = available and store["slot_busy"] is not None
        add("project.slot", ("WARN" if store["slot_busy"] else "PASS") if slot_observed else "UNKNOWN",
            {"project.slot_busy": store["slot_busy"], "project.launcher_slot_observed": store["launcher_slot_observed"]} if available else {},
            error=("PROJECT_BUSY" if store["slot_busy"] else None) if slot_observed else (store["error_code"] or "STORE_UNAVAILABLE"),
            observed=slot_observed, actions=("diagnostics.refresh",))
        add("engine.runtime", "PASS" if available else "UNKNOWN",
            {"engine.active_sprint_id": store["active_sprint_id"], "engine.control_revision": store["control_revision"],
             "engine.collector_revision_observed": app_git_ok} if available else {"engine.collector_revision_observed": app_git_ok},
            error=None if available else store["error_code"], observed=available)
        resources = available and store["resources_available"]
        add("engine.resources", "PASS" if resources else "UNKNOWN",
            {"engine.port_leases_count": store["port_leases_count"], "engine.branch_leases_count": store["branch_leases_count"],
             "engine.resource_scope": "indexed_and_preparing_sprints", "engine.historical_resources_observed": False} if resources else {},
            error=None if resources else "STORE_UNAVAILABLE", observed=resources)
        delivery = available and store["delivery_available"]
        add("engine.delivery", "PASS" if delivery else "UNKNOWN",
            {"engine.queue_items_count": store["queue_items_count"], "engine.queue_scope": "indexed_and_preparing_sprints"} if delivery else {},
            error=None if delivery else "STORE_UNAVAILABLE", observed=delivery)
        add("engine.result_reviews", ("WARN" if store["pending_reviews_count"] else "PASS") if available else "UNKNOWN",
            {"engine.pending_review_assignments": store["pending_reviews_count"]} if available else {},
            error=None if available else store["error_code"], observed=available)
        add("engine.recovery", ("WARN" if store["has_pending_recovery"] else "PASS") if available else "UNKNOWN",
            {"engine.pending_recovery": store["has_pending_recovery"]} if available else {},
            error=None if available else store["error_code"], observed=available, actions=("recovery.open",))
        executor = bool(self.environment.get("NGINX_QA_LAUNCHER_EXECUTOR_URL"))
        add("worker.configuration", "PASS" if executor else "NOT_CONFIGURED",
            {"worker.endpoint_configured": executor, "worker.live_connection_observed": False},
            error=None if executor else "NOT_CONFIGURED", observed=executor, actions=("worker.probe", "setup.open"))
        binding = self.binding_metadata
        binding_owned = bool(binding and binding.get("project_id") == project_id
                             and binding.get("instance_id") == instance_id
                             and isinstance(binding.get("binding_id"), str)
                             and _OPAQUE_ID_PATTERN.fullmatch(binding["binding_id"]))
        add("worker.binding", "PASS" if binding_owned else ("BLOCKED" if binding else "UNKNOWN"),
            {"worker.binding_id": binding["binding_id"], "worker.live_connection_observed": False} if binding_owned else {},
            error=None if binding_owned else ("OWNERSHIP_MISMATCH" if binding else "NOT_CONFIGURED"),
            observed=binding_owned, actions=("worker.bind", "worker.probe"))
        add("git.source", "PASS" if git_ok else "UNKNOWN",
            {"git.head": head, "git.branch": branch, "git.remote_freshness_observed": False} if git_ok else {},
            error=None if git_ok else "SOURCE_STALE", observed=git_ok, evidence_kind="CODE", actions=("git.probe",))
        add("git.publication_authority", "UNKNOWN", {"git.publication_authority_observed": False},
            error="PROBE_UNSUPPORTED", observed=False, evidence_kind="NONE", actions=("git.probe",))
        refs = {name: bool(self.environment.get("NGINX_QA_LAUNCHER_" + env + "_SECRET_REF"))
                for name, env in (("bot", "BOT"), ("webhook", "WEBHOOK"), ("executor", "EXECUTOR"))}
        add("secrets.references", "PASS" if any(refs.values()) else "NOT_CONFIGURED",
            {"secrets." + k + "_reference_configured": v for k, v in refs.items()},
            error=None if any(refs.values()) else "NOT_CONFIGURED", observed=any(refs.values()), actions=("setup.open",))
        provider = bool(self.environment.get("NGINX_QA_LAUNCHER_ADVISOR_SECRET_REF")
                        or self.environment.get("NGINX_QA_LAUNCHER_CONTROL_ADVISOR_KEY"))
        add("provider.configuration", "PASS" if provider else "NOT_CONFIGURED",
            {"provider.credential_configured": provider, "provider.live_auth_observed": False},
            error=None if provider else "NOT_CONFIGURED", observed=provider, actions=("provider.probe",))
        telegram = bool(refs["bot"] or self.environment.get("TELEGRAM_BOT_TOKEN"))
        add("telegram.configuration", "PASS" if telegram else "NOT_CONFIGURED",
            {"telegram.credential_configured": telegram, "telegram.live_auth_observed": False},
            error=None if telegram else "NOT_CONFIGURED", observed=telegram, actions=("telegram.probe",))
        owned, heartbeat, fresh = self._tunnel_state(instance_id)
        add("tunnel.ownership", owned, {"tunnel.ownership_verified": owned == "PASS"},
            error=None if owned == "PASS" else ("NOT_CONFIGURED" if owned == "NOT_CONFIGURED" else "OWNERSHIP_MISMATCH"),
            observed=owned == "PASS", actions=("setup.open",))
        add("tunnel.heartbeat", heartbeat, {"tunnel.receipt_fresh": fresh} if owned == "PASS" else {},
            error=None if fresh else ("SOURCE_STALE" if owned == "PASS" else "NOT_CONFIGURED"),
            observed=owned == "PASS", stale=owned == "PASS" and not fresh,
            evidence_kind="CONFIG_METADATA", actions=("diagnostics.refresh",))
        candidates = ["diagnostics.refresh", "setup.open"]
        if executor:
            candidates.extend(("worker.probe", "worker.bind"))
        if provider:
            candidates.append("provider.probe")
        if telegram:
            candidates.append("telegram.probe")
        if project is not None:
            candidates.append("git.probe")
        if project is not None and binding_owned:
            candidates.append("selftest.start")
        # Stopping still needs the operation manager's exact native ownership check.
        candidates.append("selftest.stop")
        if available and store["has_pending_recovery"]:
            candidates.append("recovery.open")
        # Policy supplies capability grants, but this collector never asserts execution readiness.
        fingerprint = compute_authority_fingerprint(
            project_id, instance_id=instance_id, git_head=head if git_ok else None,
            control_revision=store["control_revision"], policy_generation=policy_generation,
            active_sprint_id=store["active_sprint_id"], active_assignment_id=store["active_assignment_id"])
        fingerprint = sha256_digest(canonical_json_bytes({
            "native": fingerprint, "actor_id": actor_id, "authority_version": authority_version,
            "source_version": config_version, "allowed": sorted(allowed),
        }))
        snapshot = {
            "kind": "setup_diagnostic_snapshot", "schema_version": 1, "snapshot_id": "0" * 64,
            "project_id": project_id, "instance_id": instance_id, "generated_at": now_iso,
            "source": source, "authority_fingerprint": fingerprint, "checks": checks,
            "available_action_ids": [a for a in candidates if a in allowed],
        }
        snapshot["snapshot_id"] = compute_snapshot_id(snapshot)
        get_snapshot_validator().validate(snapshot)
        if len(canonical_json_bytes(snapshot)) > 65536:
            raise ValueError("Snapshot exceeds contract limit")
        return snapshot


class DiagnosticReadGate:
    """One real capture, no waiting queue, owner-scoped bounded cache/issued IDs.

    Request cancellation never cancels the to_thread task or frees admission.
    Its done callback releases admission only after the blocking capture exits.
    """

    def __init__(self, *, max_cache_entries: int = 32, default_ttl: float = 5.0,
                 issued_ttl: float = 120.0, max_cache_bytes: int = 2 * 1024 * 1024) -> None:
        if max_cache_entries < 1 or default_ttl < 0 or not 0 < issued_ttl <= 120 or max_cache_bytes < 65536:
            raise ValueError("Invalid diagnostic limits")
        self.max_cache_entries = max_cache_entries
        self.default_ttl = default_ttl
        self.issued_ttl = issued_ttl
        self.max_cache_bytes = max_cache_bytes
        self._cache: dict[tuple[Any, ...], tuple[float, str]] = {}
        self._issued: dict[tuple[Any, ...], tuple[float, dict[str, Any], DiagnosticSnapshotCollector, str, int]] = {}
        self._semaphore = asyncio.Semaphore(1)
        self._tasks: set[asyncio.Task] = set()
        self._epoch = 0
        self._closed = False

    @staticmethod
    def _owner(project_id: str, actor_id: str, policy_generation: int,
               authority_version: str, instance_id: str | None) -> tuple[Any, ...]:
        return project_id, actor_id, policy_generation, authority_version, instance_id

    def _prune(self) -> None:
        now = time.monotonic()
        for key, item in list(self._issued.items()):
            if item[0] <= now:
                del self._issued[key]
        for key, (expiry, snapshot_id) in list(self._cache.items()):
            if expiry <= now or key[:5] + (snapshot_id,) not in self._issued:
                del self._cache[key]
        while (len(self._issued) > self.max_cache_entries
               or sum(v[4] for v in self._issued.values()) > self.max_cache_bytes):
            oldest = min(self._issued, key=lambda key: self._issued[key][0])
            del self._issued[oldest]

    def get_issued_snapshot(
        self, snapshot_id: str, project_id: str, *, actor_id: str = "",
        policy_generation: int = 1, authority_version: str = "", instance_id: str | None = None,
    ) -> dict[str, Any]:
        from copy import deepcopy
        self._prune()
        key = self._owner(project_id, actor_id, policy_generation, authority_version, instance_id) + (snapshot_id,)
        item = self._issued.get(key)
        if item is None:
            raise LookupError("SNAPSHOT_STALE_OR_NOT_ISSUED")
        _, snapshot, collector, version, _ = item
        if collector.source_version() != version:
            del self._issued[key]
            raise LookupError("SNAPSHOT_SOURCE_CHANGED")
        return deepcopy(snapshot)

    def invalidate(self, project_id: str, *, actor_id: str | None = None) -> None:
        for key in list(self._issued):
            if key[0] == project_id and (actor_id is None or key[1] == actor_id):
                del self._issued[key]
        for key in list(self._cache):
            if key[0] == project_id and (actor_id is None or key[1] == actor_id):
                del self._cache[key]
        # Also covers the interval between thread completion and cache publication.
        # With a single capture, conservatively discarding it is bounded and safe.
        self._epoch += 1

    async def drain(self, *, timeout: float = 5.0) -> bool:
        self._closed = True
        if not self._tasks:
            return True
        _, pending = await asyncio.wait(self._tasks, timeout=timeout)
        return not pending

    async def get_snapshot(
        self, project_id: str, *, instance_id: str | None = None, actor_id: str = "",
        policy_generation: int = 1, authority_version: str = "",
        authorized_action_ids: Sequence[str] | None = None,
        code_root: Path | None = None, state_base: Path | None = None,
        git_config_path: Path | None = None, store_path: Path | None = None,
        repository_root: Path | None = None, project_metadata: Mapping[str, Any] | None = None,
        binding_metadata: Mapping[str, Any] | None = None, environment: Mapping[str, Any] | None = None,
        timeout: float = 4.0, force_refresh: bool = False,
    ) -> dict[str, Any]:
        from copy import deepcopy
        if self._closed:
            raise RuntimeError("DIAGNOSTIC_GATE_CLOSED")
        if not 0 < timeout <= 30:
            raise ValueError("Invalid capture timeout")
        collector = DiagnosticSnapshotCollector(
            code_root=code_root, state_base=state_base, git_config_path=git_config_path,
            store_path=store_path, repository_root=repository_root, project_metadata=project_metadata,
            binding_metadata=binding_metadata, environment=environment)
        source_version = collector.source_version()
        # Only safe presence bits and trusted source paths are retained here.
        context_version = sha256_digest(canonical_json_bytes({
            "source": source_version, "environment": collector.environment,
            "project": project_metadata, "binding": binding_metadata,
            "grants": sorted(ACTION_IDS if authorized_action_ids is None else authorized_action_ids),
        }))
        owner = self._owner(project_id, actor_id, policy_generation, authority_version, instance_id)
        key = owner + (context_version,)
        self._prune()
        cached = self._cache.get(key)
        if cached and not force_refresh:
            try:
                return self.get_issued_snapshot(cached[1], project_id, actor_id=actor_id,
                    policy_generation=policy_generation, authority_version=authority_version, instance_id=instance_id)
            except LookupError:
                self._cache.pop(key, None)
        if self._semaphore.locked():
            raise RuntimeError("DIAGNOSTIC_CAPTURE_BUSY")
        await self._semaphore.acquire()
        epoch = self._epoch
        task = asyncio.create_task(asyncio.to_thread(
            collector.collect_snapshot, project_id, instance_id=instance_id, actor_id=actor_id,
            policy_generation=policy_generation, authority_version=authority_version,
            authorized_action_ids=authorized_action_ids))
        self._tasks.add(task)
        def completed(done: asyncio.Task) -> None:
            self._tasks.discard(done)
            self._semaphore.release()
            if not done.cancelled():
                done.exception()  # Consume failures after a caller cancels or times out.
        task.add_done_callback(completed)
        try:
            snapshot = await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError("DIAGNOSTIC_CAPTURE_TIMEOUT") from None
        if (epoch != self._epoch
                or source_version != collector.source_version() or self._closed):
            raise RuntimeError("DIAGNOSTIC_SOURCE_CHANGED")
        size = len(canonical_json_bytes(snapshot))
        issued_key = owner + (snapshot["snapshot_id"],)
        self._issued[issued_key] = (time.monotonic() + self.issued_ttl, deepcopy(snapshot), collector, source_version, size)
        self._cache[key] = (time.monotonic() + self.default_ttl, snapshot["snapshot_id"])
        self._prune()
        return deepcopy(snapshot)
