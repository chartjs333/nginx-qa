"""Durable managed-child process supervision.

The transactional importer publishes only ``PREPARED`` process records and
``reserved`` port leases.  This module owns the post-commit side effects: it
claims one immutable process attempt, hands its reserved socket to the child,
persists the exact operating-system identity, verifies socket and HTTP health,
and advances the durable process/lease records together.

The supervisor never discovers children by name, command line, or port.  A
candidate must come from the managed runtime database and must match its PID
receipt, OS birth identity, executable, cwd, and process group/job.  The
current service process and its ancestors are always protected.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence, TYPE_CHECKING
from uuid import uuid4

from .port_leases import (
    ManagedPortHandoffToken,
    ManagedPortReservationError,
    ManagedPortReservationRegistry,
)
from .sprint_types import (
    managed_activation_invariant_issues,
    managed_health_path_valid,
    process_transition_allowed,
)

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from .managed_import import ManagedImportStore


ORPHAN_PROCESS = "ORPHAN_PROCESS"
RESOURCE_LIMIT_UNSUPPORTED = "RESOURCE_LIMIT_UNSUPPORTED"
PROCESS_LAUNCH_FAILED = "PROCESS_LAUNCH_FAILED"
PROCESS_HEALTH_FAILED = "PROCESS_HEALTH_FAILED"
PROCESS_STATE_CONFLICT = "PROCESS_STATE_CONFLICT"
RUNTIME_CONFIG_DRIFT = "RUNTIME_CONFIG_DRIFT"

_LIVE_STATES = frozenset({"PREPARED", "STARTING", "HEALTHY", "STOPPING"})
_RUNNING_STATES = frozenset({"STARTING", "HEALTHY", "STOPPING"})
_MINIMAL_ENVIRONMENT = (
    "PATH",
    "TEMP",
    "TMP",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "SystemRoot",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
)
_HEALTH_FAILURE_THRESHOLD = 3
_MAX_PID_RECEIPT_BYTES = 64 * 1024


_PROCESS_ACTION_LOCKS_GUARD = threading.Lock()
_PROCESS_ACTION_LOCKS: dict[str, threading.RLock] = {}
_WINDOWS_SPAWN_LOCK = threading.Lock()


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


def _json_text(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _safe_identifier(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest}"


def _path_key(path: str | os.PathLike[str]) -> str:
    return os.path.normcase(os.path.realpath(os.fspath(path)))


def _lexical_path_key(path: str | os.PathLike[str]) -> str:
    """Return an absolute path key without following filesystem aliases."""

    return os.path.normcase(os.path.abspath(os.path.normpath(os.fspath(path))))


def _is_within(child: Path, parent: Path) -> bool:
    try:
        return os.path.commonpath((_path_key(child), _path_key(parent))) == _path_key(
            parent
        )
    except ValueError:
        return False


def _is_lexically_within(child: Path, parent: Path) -> bool:
    try:
        return os.path.commonpath(
            (_lexical_path_key(child), _lexical_path_key(parent))
        ) == _lexical_path_key(parent)
    except ValueError:
        return False


def _path_entry_is_redirected(path: Path) -> bool:
    """Detect symlinks and Windows junction/reparse points without following."""

    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(metadata.st_mode):
        return True
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(getattr(metadata, "st_file_attributes", 0) & reparse_flag)


def _atomic_write_json(
    path: Path,
    value: Mapping[str, Any],
    *,
    directory_fd: int | None = None,
) -> None:
    if directory_fd is None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary_name = f".{path.name}.{uuid4().hex}.tmp"
    temporary: Path | str = (
        path.parent / temporary_name if directory_fd is None else temporary_name
    )
    payload = (_json_text(value) + "\n").encode("utf-8")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
        dir_fd=directory_fd,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if directory_fd is None:
            os.replace(temporary, path)
        else:
            os.replace(
                temporary_name,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
        if os.name != "nt":
            if directory_fd is not None:
                os.fsync(directory_fd)
            else:
                directory_descriptor = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_descriptor)
                finally:
                    os.close(directory_descriptor)
    except BaseException:
        try:
            if directory_fd is None:
                Path(temporary).unlink()
            else:
                os.unlink(temporary_name, dir_fd=directory_fd)
        except OSError:
            pass
        raise


class ManagedProcessSupervisorError(RuntimeError):
    """Stable fail-closed error raised by managed process supervision."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        process_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.process_id = process_id


class _RestartPortUnavailable(RuntimeError):
    """Roll back one restart candidate whose endpoint cannot be reserved."""


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    pid: int
    created_at: str
    birth_token: str
    executable_path: str
    cwd: str | None
    process_group_id: int | str | None


@dataclass(frozen=True, slots=True)
class SupervisionResult:
    project_id: str
    sprint_id: str
    process_id: str
    state: str
    action: str


@dataclass(slots=True)
class _ProcessContext:
    project_id: str
    sprint_id: str
    runtime_state: dict[str, Any]
    process: dict[str, Any]
    lease: dict[str, Any]
    workspace: dict[str, Any]
    supervisor_fence: int = 0


@dataclass(slots=True)
class _PinnedPath:
    descriptor: int
    path: Path
    purpose: str
    directory: bool
    device: int
    inode: int


@dataclass(slots=True)
class _OwnedProcess:
    process: Any
    stdout_stream: Any
    stderr_stream: Any
    job: "_WindowsJob | None"
    launch_gate: int | None = None
    path_pins: tuple[_PinnedPath, ...] = ()


class _WindowsChildProcess:
    """The small Popen-compatible surface used by the Windows supervisor."""

    def __init__(self, handle: int, pid: int, args: Sequence[str], kernel32: Any) -> None:
        self._handle = subprocess.Handle(handle)
        self.pid = pid
        self.args = list(args)
        self.returncode: int | None = None
        self._kernel32 = kernel32

    def _exit_code(self) -> int:
        import ctypes
        from ctypes import wintypes

        code = wintypes.DWORD()
        if not self._kernel32.GetExitCodeProcess(
            self._handle, ctypes.byref(code)
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        self.returncode = int(code.value)
        return self.returncode

    def poll(self) -> int | None:
        import ctypes

        if self.returncode is not None:
            return self.returncode
        result = int(self._kernel32.WaitForSingleObject(self._handle, 0))
        if result == 258:  # WAIT_TIMEOUT
            return None
        if result != 0:  # WAIT_OBJECT_0
            raise ctypes.WinError(ctypes.get_last_error())
        return self._exit_code()

    def wait(self, timeout: float | None = None) -> int:
        import ctypes

        if self.returncode is not None:
            return self.returncode
        milliseconds = (
            0xFFFFFFFF
            if timeout is None
            else min(max(0, math.ceil(timeout * 1000)), 0xFFFFFFFE)
        )
        result = int(
            self._kernel32.WaitForSingleObject(self._handle, milliseconds)
        )
        if result == 258:
            raise subprocess.TimeoutExpired(self.args, timeout)
        if result != 0:
            raise ctypes.WinError(ctypes.get_last_error())
        return self._exit_code()

    def kill(self) -> None:
        import ctypes

        if self.returncode is not None:
            return
        if self._kernel32.TerminateProcess(self._handle, 1):
            return
        error = ctypes.get_last_error()
        if error == 5 and self.poll() is not None:
            return
        raise ctypes.WinError(error)

    terminate = kill

    def send_signal(self, selected_signal: int) -> None:
        if selected_signal == signal.SIGTERM:
            self.kill()
        elif selected_signal in {signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT}:
            os.kill(self.pid, selected_signal)
        else:
            raise ValueError(f"unsupported Windows signal: {selected_signal}")

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.Close()


class _SupervisorRepository:
    """CAS mutations that keep JSON and normalized resource rows identical."""

    def __init__(
        self,
        store: "ManagedImportStore",
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self.store = store
        self.clock = clock
        self._ensure_claim_columns()
        self._upgrade_prepared_runtime_v1()

    def _ensure_claim_columns(self) -> None:
        self.store._ensure_initialized()  # type: ignore[attr-defined]
        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(managed_process_owners)"
                )
            }
            additions = {
                "supervisor_fence": (
                    "ALTER TABLE managed_process_owners "
                    "ADD COLUMN supervisor_fence INTEGER NOT NULL DEFAULT 0"
                ),
                "claim_owner": (
                    "ALTER TABLE managed_process_owners ADD COLUMN claim_owner TEXT"
                ),
                "claim_expires_at": (
                    "ALTER TABLE managed_process_owners "
                    "ADD COLUMN claim_expires_at TEXT"
                ),
            }
            for name, statement in additions.items():
                if name not in columns:
                    connection.execute(statement)

    @staticmethod
    def _assert_runtime_shape(
        state: Mapping[str, Any], *, process_id: str = ""
    ) -> None:
        # Local import avoids the managed_import -> process_supervisor cycle at
        # module load while keeping every supervisor write version-routed.
        from .managed_import import _runtime_state_schema_errors

        issues = _runtime_state_schema_errors(
            state,
            issue_code="RUNTIME_STATE_SCHEMA_INVALID",
        )
        if issues:
            paths = ", ".join(
                sorted(
                    {
                        str(issue.get("path") or "<root>")
                        for issue in issues
                        if isinstance(issue, Mapping)
                    }
                )
            )
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "process transition violates the declared runtime schema at "
                + paths,
                process_id=process_id,
            )

    def _upgrade_prepared_runtime_v1(self) -> None:
        """Lazily upgrade unlaunched v1 process records before supervision.

        The predecessor importer could only publish PREPARED process attempts;
        no older supervisor existed to produce a live v1 identity.  Restricting
        migration to that unambiguous state preserves v1 readability and fails
        closed if a foreign writer claims otherwise.
        """

        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            rows = connection.execute(
                """
                SELECT project_id, sprint_id, state_json
                FROM managed_sprints WHERE status = 'active'
                """
            ).fetchall()
            for row in rows:
                state = json.loads(row["state_json"])
                version = state.get("schema_version")
                if type(version) is not int or version not in {1, 2}:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "runtime declares an unsupported schema version",
                    )
                self._assert_runtime_shape(state)
                if version != 1:
                    continue
                processes = state.get("processes")
                if not isinstance(processes, list):
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "v1 runtime process collection is corrupt",
                    )
                if not processes:
                    continue
                upgraded = deepcopy(state)
                upgraded["schema_version"] = 2
                for process in upgraded["processes"]:
                    if (
                        not isinstance(process, dict)
                        or process.get("state") != "PREPARED"
                        or any(
                            key in process
                            for key in (
                                "os_process_birth_token",
                                "startup_deadline_at",
                                "terminal_reason",
                            )
                        )
                    ):
                        raise ManagedProcessSupervisorError(
                            PROCESS_STATE_CONFLICT,
                            "v1 runtime cannot be upgraded without launch evidence",
                            process_id=(
                                str(process.get("process_id"))
                                if isinstance(process, Mapping)
                                else ""
                            ),
                        )
                    process.update(
                        {
                            "os_process_birth_token": None,
                            "startup_deadline_at": None,
                            "terminal_reason": None,
                        }
                    )
                self._assert_runtime_shape(upgraded)
                invariant_issues = managed_activation_invariant_issues(upgraded)
                if invariant_issues:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "v1 runtime upgrade violates relational invariants",
                    )
                for old_process, new_process in zip(
                    state["processes"], upgraded["processes"], strict=True
                ):
                    process_id = str(new_process["process_id"])
                    owner = connection.execute(
                        """
                        SELECT state, process_json FROM managed_process_owners
                        WHERE process_id = ?
                        """,
                        (process_id,),
                    ).fetchone()
                    if (
                        owner is None
                        or owner["state"] != "PREPARED"
                        or json.loads(owner["process_json"]) != old_process
                    ):
                        raise ManagedProcessSupervisorError(
                            PROCESS_STATE_CONFLICT,
                            "normalized v1 process differs from runtime JSON",
                            process_id=process_id,
                        )
                    connection.execute(
                        """
                        UPDATE managed_process_owners SET process_json = ?
                        WHERE process_id = ? AND state = 'PREPARED'
                        """,
                        (_json_text(new_process), process_id),
                    )
                cursor = connection.execute(
                    """
                    UPDATE managed_sprints SET state_json = ?
                    WHERE project_id = ? AND sprint_id = ?
                      AND status = 'active' AND state_json = ?
                    """,
                    (
                        _json_text(upgraded),
                        row["project_id"],
                        row["sprint_id"],
                        row["state_json"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "v1 runtime changed during schema upgrade",
                    )

    @staticmethod
    def _find_record(
        state: Mapping[str, Any], key: str, identity_key: str, identity: str
    ) -> dict[str, Any]:
        records = state.get(key)
        if not isinstance(records, list):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT, f"runtime {key} are corrupt"
            )
        matches = [
            item
            for item in records
            if isinstance(item, Mapping) and item.get(identity_key) == identity
        ]
        if len(matches) != 1:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"runtime {key} ownership is missing or ambiguous",
            )
        return deepcopy(dict(matches[0]))

    @staticmethod
    def _replace_record(
        state: dict[str, Any],
        key: str,
        identity_key: str,
        replacement: Mapping[str, Any],
    ) -> None:
        identity = replacement.get(identity_key)
        records = state.get(key)
        if not isinstance(records, list):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT, f"runtime {key} are corrupt"
            )
        positions = [
            index
            for index, item in enumerate(records)
            if isinstance(item, Mapping) and item.get(identity_key) == identity
        ]
        if len(positions) != 1:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"runtime {key} ownership is missing or ambiguous",
            )
        records[positions[0]] = deepcopy(dict(replacement))

    @staticmethod
    def _context_from_state(
        project_id: str,
        sprint_id: str,
        state: Mapping[str, Any],
        process_id: str,
        *,
        supervisor_fence: int = 0,
    ) -> _ProcessContext:
        runtime = deepcopy(dict(state))
        process = _SupervisorRepository._find_record(
            runtime, "processes", "process_id", process_id
        )
        lease_id = process.get("port_lease_id")
        workspace_id = process.get("workspace_id")
        if not isinstance(lease_id, str) or not isinstance(workspace_id, str):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "process ownership references are invalid",
                process_id=process_id,
            )
        lease = _SupervisorRepository._find_record(
            runtime, "port_leases", "lease_id", lease_id
        )
        workspace = _SupervisorRepository._find_record(
            runtime, "workspaces", "workspace_id", workspace_id
        )
        return _ProcessContext(
            project_id=project_id,
            sprint_id=sprint_id,
            runtime_state=runtime,
            process=process,
            lease=lease,
            workspace=workspace,
            supervisor_fence=supervisor_fence,
        )

    @staticmethod
    def _normalized_projection_matches(
        context: _ProcessContext,
        owner_row: Any,
        lease_row: Any,
    ) -> bool:
        """Compare every normalized ownership column with its JSON source."""

        process = context.process
        lease = context.lease
        return bool(
            owner_row is not None
            and lease_row is not None
            and owner_row["assignment_id"] == process.get("assignment_id")
            and owner_row["port_lease_id"] == process.get("port_lease_id")
            and owner_row["pid"] == process.get("pid")
            and owner_row["state"] == process.get("state")
            and json.loads(owner_row["process_json"]) == process
            and lease_row["instance_id"] == lease.get("instance_id")
            and lease_row["network_namespace_id"]
            == lease.get("network_namespace_id")
            and lease_row["assignment_id"] == lease.get("assignment_id")
            and lease_row["process_id"] == lease.get("process_id")
            and lease_row["host"] == lease.get("host")
            and lease_row["port"] == lease.get("port")
            and lease_row["status"] == lease.get("status")
            and json.loads(lease_row["lease_json"]) == lease
        )

    def _validated_runtime_state_from_row(
        self,
        connection: Any,
        row: Any,
        *,
        process_id: str = "",
    ) -> dict[str, Any]:
        """Preserve the store's full row/control validation in one snapshot."""

        project_id = str(row["project_id"])
        try:
            control, _revision = self.store._load_control(  # type: ignore[attr-defined]
                connection, project_id
            )
            self.store._validate_control_snapshot(  # type: ignore[attr-defined]
                connection, project_id, control
            )
            state = json.loads(row["state_json"])
            issues = self.store._runtime_row_issues(  # type: ignore[attr-defined]
                control,
                row,
                state,
                require_indexed=True,
            )
            if issues or not isinstance(state, dict):
                raise ValueError(
                    "managed runtime row is invalid: "
                    + ",".join(issues or ("RUNTIME_STATE_NOT_OBJECT",))
                )
            return state
        except ManagedProcessSupervisorError:
            raise
        except (KeyError, TypeError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed runtime row or project control is corrupt",
                process_id=process_id,
            ) from exc

    def active_states(self) -> tuple[dict[str, Any], ...]:
        """Load active states only after atomically validating projections."""

        # Preserve the import store's startup repair of superseded attempts;
        # the authoritative snapshot below is then re-read and validated.
        try:
            self.store.active_runtime_states()
        except RuntimeError as exc:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "active managed runtime is corrupt",
            ) from exc
        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            sprint_rows = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints WHERE status = 'active'
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
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "multiple active managed runtimes exist for one project",
                )
            owner_rows = {
                str(row["process_id"]): row
                for row in connection.execute(
                    """
                    SELECT process_id, assignment_id, port_lease_id, pid, state,
                           process_json, supervisor_fence
                    FROM managed_process_owners
                    """
                ).fetchall()
            }
            lease_rows = {
                str(row["lease_id"]): row
                for row in connection.execute(
                    """
                    SELECT lease_id, instance_id, network_namespace_id,
                           assignment_id, process_id, host, port, status,
                           lease_json
                    FROM managed_port_leases
                    """
                ).fetchall()
            }
            states: list[dict[str, Any]] = []
            try:
                for row in sprint_rows:
                    state = self._validated_runtime_state_from_row(connection, row)
                    processes = state.get("processes")
                    if not isinstance(processes, list):
                        raise TypeError("active runtime processes are invalid")
                    for raw_process in processes:
                        if not isinstance(raw_process, Mapping):
                            raise TypeError("active runtime process is invalid")
                        process_id = raw_process.get("process_id")
                        if not isinstance(process_id, str):
                            raise TypeError("active runtime process ID is invalid")
                        context = self._context_from_state(
                            str(row["project_id"]),
                            str(row["sprint_id"]),
                            state,
                            process_id,
                            supervisor_fence=int(
                                owner_rows[process_id]["supervisor_fence"]
                            ),
                        )
                        lease_id = str(context.lease["lease_id"])
                        if not self._normalized_projection_matches(
                            context,
                            owner_rows.get(process_id),
                            lease_rows.get(lease_id),
                        ):
                            raise ValueError(
                                "active normalized projection differs from JSON"
                            )
                    states.append(deepcopy(state))
            except ManagedProcessSupervisorError:
                raise
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "active runtime normalized ownership is corrupt",
                ) from exc
            return tuple(states)

    def contexts(
        self,
        *,
        project_id: str | None = None,
        sprint_id: str | None = None,
    ) -> tuple[_ProcessContext, ...]:
        contexts: list[_ProcessContext] = []
        for raw_state in self.active_states():
            state = deepcopy(dict(raw_state))
            identity = state.get("identity")
            state_project = (
                identity.get("project_id")
                if isinstance(identity, Mapping)
                else None
            )
            state_sprint = state.get("sprint_id")
            if not isinstance(state_project, str) or not isinstance(state_sprint, str):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT, "active runtime identity is corrupt"
                )
            if project_id is not None and state_project != project_id:
                continue
            if sprint_id is not None and state_sprint != sprint_id:
                continue
            processes = state.get("processes")
            if not isinstance(processes, list):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT, "active runtime processes are corrupt"
                )
            for raw_process in processes:
                if not isinstance(raw_process, Mapping):
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT, "active runtime process is corrupt"
                    )
                process_id_value = raw_process.get("process_id")
                if not isinstance(process_id_value, str):
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT, "active runtime process ID is corrupt"
                    )
                contexts.append(
                    self._context_from_state(
                        state_project,
                        state_sprint,
                        state,
                        process_id_value,
                    )
                )
        return tuple(contexts)

    def load(
        self, project_id: str, sprint_id: str, process_id: str
    ) -> _ProcessContext:
        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            sprint_row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ? AND status = 'active'
                """,
                (project_id, sprint_id),
            ).fetchone()
            owner_row = connection.execute(
                """
                SELECT assignment_id, port_lease_id, pid, process_json, state,
                       supervisor_fence
                FROM managed_process_owners WHERE process_id = ?
                """,
                (process_id,),
            ).fetchone()
            if sprint_row is None or owner_row is None:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process ownership disappeared",
                    process_id=process_id,
                )
            try:
                state = self._validated_runtime_state_from_row(
                    connection,
                    sprint_row,
                    process_id=process_id,
                )
                context = self._context_from_state(
                    project_id,
                    sprint_id,
                    state,
                    process_id,
                    supervisor_fence=int(owner_row["supervisor_fence"]),
                )
                lease_row = connection.execute(
                    """
                    SELECT instance_id, network_namespace_id, assignment_id,
                           process_id, host, port, lease_json, status
                    FROM managed_port_leases
                    WHERE lease_id = ?
                    """,
                    (str(context.lease["lease_id"]),),
                ).fetchone()
                projections_match = self._normalized_projection_matches(
                    context,
                    owner_row,
                    lease_row,
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "normalized process or port owner is corrupt",
                    process_id=process_id,
                ) from exc
            if not projections_match:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "normalized process or port owner differs from runtime JSON",
                    process_id=process_id,
                )
            return context

    def claim_prepared(
        self,
        project_id: str,
        sprint_id: str,
        process_id: str,
        *,
        owner: str,
        ttl_seconds: float,
    ) -> _ProcessContext | None:
        now = _utc_now(self.clock)
        expires = (now + timedelta(seconds=max(ttl_seconds, 1.0))).isoformat()
        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            sprint_row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ? AND status = 'active'
                """,
                (project_id, sprint_id),
            ).fetchone()
            owner_row = connection.execute(
                """
                SELECT assignment_id, port_lease_id, pid, process_json, state,
                       supervisor_fence, claim_owner, claim_expires_at
                FROM managed_process_owners WHERE process_id = ?
                """,
                (process_id,),
            ).fetchone()
            if sprint_row is None or owner_row is None:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process ownership disappeared",
                    process_id=process_id,
                )
            try:
                state = self._validated_runtime_state_from_row(
                    connection,
                    sprint_row,
                    process_id=process_id,
                )
                context = self._context_from_state(
                    project_id,
                    sprint_id,
                    state,
                    process_id,
                    supervisor_fence=int(owner_row["supervisor_fence"]),
                )
                lease_row = connection.execute(
                    """
                    SELECT instance_id, network_namespace_id, assignment_id,
                           process_id, host, port, lease_json, status
                    FROM managed_port_leases
                    WHERE lease_id = ?
                    """,
                    (str(context.lease["lease_id"]),),
                ).fetchone()
                projections_match = self._normalized_projection_matches(
                    context,
                    owner_row,
                    lease_row,
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "normalized process or port owner is corrupt",
                    process_id=process_id,
                ) from exc
            if not projections_match:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "normalized process or port owner differs from runtime JSON",
                    process_id=process_id,
                )
            if context.process.get("state") != "PREPARED":
                return None
            current_owner = owner_row["claim_owner"]
            current_expiry = _parse_timestamp(owner_row["claim_expires_at"])
            if (
                isinstance(current_owner, str)
                and current_expiry is not None
                and current_expiry > now
            ):
                # A claim is exclusive even when two callers belong to the
                # same supervisor instance.  API and monitor reconciliation
                # may overlap; sharing ``owner`` must never authorize a second
                # irreversible spawn side effect.  We deliberately wait for
                # expiry instead of treating an unreadable owner PID as dead.
                return None
            old_fence = int(owner_row["supervisor_fence"])
            new_fence = old_fence + 1
            cursor = connection.execute(
                """
                UPDATE managed_process_owners
                SET supervisor_fence = ?, claim_owner = ?, claim_expires_at = ?
                WHERE process_id = ? AND state = 'PREPARED'
                  AND supervisor_fence = ?
                """,
                (new_fence, owner, expires, process_id, old_fence),
            )
            if cursor.rowcount != 1:
                return None
            context.supervisor_fence = new_fence
            return context

    def release_claim(
        self,
        process_id: str,
        *,
        owner: str,
        supervisor_fence: int,
    ) -> None:
        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            connection.execute(
                """
                UPDATE managed_process_owners
                SET claim_owner = NULL, claim_expires_at = NULL
                WHERE process_id = ? AND claim_owner = ? AND supervisor_fence = ?
                """,
                (process_id, owner, supervisor_fence),
            )

    def release_owner_claims(self, owner: str) -> None:
        """Release this quiescent supervisor's PREPARED claims on clean close."""

        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            connection.execute(
                """
                UPDATE managed_process_owners
                SET claim_owner = NULL, claim_expires_at = NULL
                WHERE claim_owner = ? AND state = 'PREPARED'
                """,
                (owner,),
            )

    def transition(
        self,
        context: _ProcessContext,
        process: Mapping[str, Any],
        lease: Mapping[str, Any],
        *,
        expected_states: Iterable[str],
        claim_owner: str | None = None,
        supervisor_fence: int | None = None,
    ) -> _ProcessContext:
        expected = frozenset(expected_states)
        process_record = deepcopy(dict(process))
        lease_record = deepcopy(dict(lease))
        process_id = str(context.process["process_id"])
        lease_id = str(context.lease["lease_id"])
        with self.store._transaction() as connection:  # type: ignore[attr-defined]
            sprint_row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ? AND status = 'active'
                """,
                (context.project_id, context.sprint_id),
            ).fetchone()
            owner_row = connection.execute(
                """
                SELECT assignment_id, port_lease_id, pid, process_json, state,
                       supervisor_fence, claim_owner
                FROM managed_process_owners WHERE process_id = ?
                """,
                (process_id,),
            ).fetchone()
            lease_row = connection.execute(
                """
                SELECT instance_id, network_namespace_id, assignment_id,
                       process_id, host, port, lease_json, status
                FROM managed_port_leases
                WHERE lease_id = ?
                """,
                (lease_id,),
            ).fetchone()
            if sprint_row is None or owner_row is None or lease_row is None:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed runtime ownership disappeared",
                    process_id=process_id,
                )
            state = self._validated_runtime_state_from_row(
                connection,
                sprint_row,
                process_id=process_id,
            )
            current = self._context_from_state(
                context.project_id,
                context.sprint_id,
                state,
                process_id,
                supervisor_fence=int(owner_row["supervisor_fence"]),
            )
            current_state = current.process.get("state")
            try:
                projections_match = self._normalized_projection_matches(
                    current,
                    owner_row,
                    lease_row,
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "normalized process or port owner is corrupt",
                    process_id=process_id,
                ) from exc
            if (
                current_state not in expected
                or not projections_match
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed runtime changed during process transition",
                    process_id=process_id,
                )
            if claim_owner is not None and owner_row["claim_owner"] != claim_owner:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process claim was lost",
                    process_id=process_id,
                )
            if (
                supervisor_fence is not None
                and int(owner_row["supervisor_fence"]) != supervisor_fence
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process fence is stale",
                    process_id=process_id,
                )
            target_state = process_record.get("state")
            if current_state != target_state and not (
                isinstance(current_state, str)
                and isinstance(target_state, str)
                and process_transition_allowed(current_state, target_state)
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    f"invalid process transition {current_state!r} -> {target_state!r}",
                    process_id=process_id,
                )
            immutable_process_keys = (
                "process_id",
                "assignment_id",
                "workspace_id",
                "runtime_root",
                "launch_nonce",
                "executable_path",
                "command_redacted",
                "environment_redacted",
                "cwd",
                "stdout_log",
                "stderr_log",
                "health_endpoint",
                "resource_limits",
                "restart_policy",
                "restart_attempt",
                "restart_of_process_id",
                "max_restart_attempts",
                "restart_backoff_seconds",
                "port_lease_id",
            )
            if any(
                process_record.get(key) != current.process.get(key)
                for key in immutable_process_keys
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "immutable process attempt identity changed",
                    process_id=process_id,
                )
            immutable_lease_keys = (
                "lease_id",
                "instance_id",
                "network_namespace_id",
                "assignment_id",
                "host",
                "port",
                "acquired_at",
            )
            if any(
                lease_record.get(key) != current.lease.get(key)
                for key in immutable_lease_keys
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "immutable port lease identity changed",
                    process_id=process_id,
                )
            self._replace_record(state, "processes", "process_id", process_record)
            self._replace_record(state, "port_leases", "lease_id", lease_record)
            self._assert_runtime_shape(state, process_id=process_id)
            issues = managed_activation_invariant_issues(state)
            if issues:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "process transition violates runtime invariants: "
                    + ", ".join(issues),
                    process_id=process_id,
                )
            connection.execute(
                """
                UPDATE managed_sprints SET state_json = ?
                WHERE project_id = ? AND sprint_id = ? AND status = 'active'
                """,
                (_json_text(state), context.project_id, context.sprint_id),
            )
            cursor = connection.execute(
                """
                UPDATE managed_process_owners
                SET pid = ?, state = ?, process_json = ?,
                    claim_owner = NULL, claim_expires_at = NULL
                WHERE process_id = ?
                """,
                (
                    process_record.get("pid"),
                    process_record["state"],
                    _json_text(process_record),
                    process_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "normalized process transition was lost",
                    process_id=process_id,
                )
            cursor = connection.execute(
                """
                UPDATE managed_port_leases
                SET process_id = ?, status = ?, lease_json = ?
                WHERE lease_id = ?
                """,
                (
                    lease_record.get("process_id"),
                    lease_record["status"],
                    _json_text(lease_record),
                    lease_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "normalized port transition was lost",
                    process_id=process_id,
                )
        return self._context_from_state(
            context.project_id,
            context.sprint_id,
            state,
            process_id,
            supervisor_fence=context.supervisor_fence,
        )

    def append_restart(
        self,
        context: _ProcessContext,
        process: Mapping[str, Any],
        lease: Mapping[str, Any],
        port_reservations: ManagedPortReservationRegistry,
    ) -> _ProcessContext | None:
        process_record = deepcopy(dict(process))
        lease_record = deepcopy(dict(lease))
        process_id = str(process_record["process_id"])
        rollback_actions: list[Callable[[], None]] = []
        try:
            with self.store._transaction(  # type: ignore[attr-defined]
                rollback_actions=rollback_actions
            ) as connection:
                sprint_row = connection.execute(
                    """
                    SELECT state_json FROM managed_sprints
                    WHERE project_id = ? AND sprint_id = ? AND status = 'active'
                    """,
                    (context.project_id, context.sprint_id),
                ).fetchone()
                if sprint_row is None:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT, "managed runtime disappeared"
                    )
                state = json.loads(sprint_row["state_json"])
                parent = self._find_record(
                    state,
                    "processes",
                    "process_id",
                    str(context.process["process_id"]),
                )
                if parent.get("state") != "FAILED":
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "restart parent is not failed",
                        process_id=process_id,
                    )
                if connection.execute(
                    "SELECT 1 FROM managed_process_owners WHERE process_id = ?",
                    (process_id,),
                ).fetchone() is not None:
                    existing = self._find_record(
                        state, "processes", "process_id", process_id
                    )
                    if existing != process_record:
                        raise ManagedProcessSupervisorError(
                            PROCESS_STATE_CONFLICT,
                            "restart process identity changed",
                            process_id=process_id,
                        )
                    return self._context_from_state(
                        context.project_id,
                        context.sprint_id,
                        state,
                        process_id,
                    )

                # BEGIN IMMEDIATE serializes every service instance before any
                # process-local bind.  A durable lease remains authoritative
                # while its reservation socket is in the child handoff gap.
                endpoint_owner = connection.execute(
                    """
                    SELECT lease_id FROM managed_port_leases
                    WHERE network_namespace_id = ? AND host = ? AND port = ?
                      AND status IN ('reserved', 'bound')
                    """,
                    (
                        lease_record["network_namespace_id"],
                        lease_record["host"],
                        lease_record["port"],
                    ),
                ).fetchone()
                if endpoint_owner is not None:
                    return None

                state["processes"].append(process_record)
                state["port_leases"].append(lease_record)
                self._assert_runtime_shape(state, process_id=process_id)
                issues = managed_activation_invariant_issues(state)
                if issues:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "restart creation violates runtime invariants: "
                        + ", ".join(issues),
                        process_id=process_id,
                    )
                try:
                    connection.execute(
                        """
                        INSERT INTO managed_port_leases(
                            lease_id, instance_id, network_namespace_id,
                            assignment_id, process_id, host, port, status, lease_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            lease_record["lease_id"],
                            lease_record["instance_id"],
                            lease_record["network_namespace_id"],
                            lease_record["assignment_id"],
                            lease_record["process_id"],
                            lease_record["host"],
                            lease_record["port"],
                            lease_record["status"],
                            _json_text(lease_record),
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "restart ownership conflicts with a live resource",
                        process_id=process_id,
                    ) from exc

                try:
                    reservation_token = port_reservations.acquire_many(
                        self.store.database_path, [lease_record]
                    )
                except ManagedPortReservationError as exc:
                    raise _RestartPortUnavailable from exc
                rollback_actions.append(
                    lambda token=reservation_token: port_reservations.rollback(token)
                )

                try:
                    connection.execute(
                        """
                        INSERT INTO managed_process_owners(
                            process_id, assignment_id, port_lease_id, pid,
                            state, process_json, supervisor_fence,
                            claim_owner, claim_expires_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL)
                        """,
                        (
                            process_record["process_id"],
                            process_record["assignment_id"],
                            process_record["port_lease_id"],
                            process_record["pid"],
                            process_record["state"],
                            _json_text(process_record),
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "restart ownership conflicts with a live resource",
                        process_id=process_id,
                    ) from exc
                connection.execute(
                    """
                    UPDATE managed_sprints SET state_json = ?
                    WHERE project_id = ? AND sprint_id = ? AND status = 'active'
                    """,
                    (
                        _json_text(state),
                        context.project_id,
                        context.sprint_id,
                    ),
                )
        except _RestartPortUnavailable:
            return None
        port_reservations.mark_durable_many(
            self.store.database_path,
            [lease_record],
        )
        return self._context_from_state(
            context.project_id,
            context.sprint_id,
            state,
            process_id,
        )


def _linux_process_identity(pid: int) -> ProcessIdentity | None:
    proc = Path("/proc") / str(pid)
    try:
        stat_text = (proc / "stat").read_text(encoding="ascii")
        close = stat_text.rfind(")")
        if close < 0:
            return None
        fields = stat_text[close + 2 :].split()
        process_group = int(fields[2])
        start_ticks = int(fields[19])
        ticks_per_second = int(os.sysconf("SC_CLK_TCK"))
        boot_seconds = None
        for line in Path("/proc/stat").read_text(encoding="ascii").splitlines():
            if line.startswith("btime "):
                boot_seconds = int(line.split()[1])
                break
        if boot_seconds is None:
            return None
        created = datetime.fromtimestamp(
            boot_seconds + start_ticks / ticks_per_second, timezone.utc
        ).isoformat()
        executable = os.path.realpath(os.readlink(proc / "exe"))
        try:
            cwd = os.path.realpath(os.readlink(proc / "cwd"))
        except OSError:
            cwd = None
        try:
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(
                encoding="ascii"
            ).strip()
        except OSError:
            return None
        if not boot_id:
            return None
        return ProcessIdentity(
            pid,
            created,
            f"linux:{boot_id}:{start_ticks}",
            executable,
            cwd,
            process_group,
        )
    except (OSError, ValueError, IndexError):
        return None


def _windows_process_identity(pid: int) -> ProcessIdentity | None:
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetProcessTimes.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
        ]
        kernel32.GetProcessTimes.restype = wintypes.BOOL
        kernel32.QueryFullProcessImageNameW.argtypes = [
            wintypes.HANDLE,
            wintypes.DWORD,
            wintypes.LPWSTR,
            ctypes.POINTER(wintypes.DWORD),
        ]
        kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(0x1000 | 0x00100000, False, pid)
        if not handle:
            return None
        try:
            creation = wintypes.FILETIME()
            exit_time = wintypes.FILETIME()
            kernel_time = wintypes.FILETIME()
            user_time = wintypes.FILETIME()
            if not kernel32.GetProcessTimes(
                handle,
                ctypes.byref(creation),
                ctypes.byref(exit_time),
                ctypes.byref(kernel_time),
                ctypes.byref(user_time),
            ):
                return None
            ticks = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
            unix_seconds = ticks / 10_000_000 - 11_644_473_600
            created = datetime.fromtimestamp(unix_seconds, timezone.utc).isoformat()
            size = wintypes.DWORD(32768)
            buffer = ctypes.create_unicode_buffer(size.value)
            if not kernel32.QueryFullProcessImageNameW(
                handle, 0, buffer, ctypes.byref(size)
            ):
                return None
            return ProcessIdentity(
                pid,
                created,
                f"windows-filetime:{ticks}",
                os.path.realpath(buffer.value),
                None,
                None,
            )
        finally:
            kernel32.CloseHandle(handle)
    except (OSError, OverflowError, ValueError):
        return None


def process_identity(pid: int) -> ProcessIdentity | None:
    """Return a stable birth/executable identity for one live process."""

    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return None
    if os.name == "nt":
        return _windows_process_identity(pid)
    return _linux_process_identity(pid)


def _linux_parent_pid(pid: int) -> int | None:
    try:
        text = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
        close = text.rfind(")")
        fields = text[close + 2 :].split()
        return int(fields[1])
    except (OSError, ValueError, IndexError):
        return None


def _windows_parent_map() -> dict[int, int]:
    try:
        import ctypes
        from ctypes import wintypes

        class PROCESSENTRY32W(ctypes.Structure):
            _fields_ = [
                ("dwSize", wintypes.DWORD),
                ("cntUsage", wintypes.DWORD),
                ("th32ProcessID", wintypes.DWORD),
                ("th32DefaultHeapID", ctypes.c_size_t),
                ("th32ModuleID", wintypes.DWORD),
                ("cntThreads", wintypes.DWORD),
                ("th32ParentProcessID", wintypes.DWORD),
                ("pcPriClassBase", wintypes.LONG),
                ("dwFlags", wintypes.DWORD),
                ("szExeFile", wintypes.WCHAR * 260),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
        kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel32.Process32FirstW.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(PROCESSENTRY32W),
        ]
        kernel32.Process32FirstW.restype = wintypes.BOOL
        kernel32.Process32NextW.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(PROCESSENTRY32W),
        ]
        kernel32.Process32NextW.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        snapshot = kernel32.CreateToolhelp32Snapshot(0x00000002, 0)
        invalid = ctypes.c_void_p(-1).value
        if not snapshot or int(snapshot) == invalid:
            raise OSError(
                ctypes.get_last_error(),
                "CreateToolhelp32Snapshot failed",
            )
        parents: dict[int, int] = {}
        try:
            entry = PROCESSENTRY32W()
            entry.dwSize = ctypes.sizeof(entry)
            ctypes.set_last_error(0)
            if not kernel32.Process32FirstW(snapshot, ctypes.byref(entry)):
                raise OSError(
                    ctypes.get_last_error(),
                    "Process32FirstW failed",
                )
            while True:
                parents[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
                ctypes.set_last_error(0)
                if kernel32.Process32NextW(snapshot, ctypes.byref(entry)):
                    continue
                error = ctypes.get_last_error()
                if error not in {0, 18}:  # ERROR_NO_MORE_FILES
                    raise OSError(error, "Process32NextW failed")
                if error in {0, 18}:
                    break
        finally:
            kernel32.CloseHandle(snapshot)
        return parents
    except (OSError, TypeError, ValueError) as exc:
        raise ManagedProcessSupervisorError(
            PROCESS_STATE_CONFLICT,
            "service process ancestry cannot be enumerated safely",
        ) from exc


def _pid_definitely_dead(pid: int) -> bool:
    """Distinguish a vanished ancestor from an uninspectable live process."""

    if os.name != "nt":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        return False
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE
        if not handle:
            error = ctypes.get_last_error()
            if error in {87, 1168}:  # INVALID_PARAMETER / NOT_FOUND
                return True
            raise OSError(error, "OpenProcess liveness probe failed")
        try:
            result = int(kernel32.WaitForSingleObject(handle, 0))
            if result == 0:  # WAIT_OBJECT_0
                return True
            if result == 258:  # WAIT_TIMEOUT
                return False
            raise OSError(ctypes.get_last_error(), "process liveness wait failed")
        finally:
            kernel32.CloseHandle(handle)
    except (OSError, TypeError, ValueError) as exc:
        raise ManagedProcessSupervisorError(
            PROCESS_STATE_CONFLICT,
            "service ancestor liveness cannot be proven",
            process_id=str(pid),
        ) from exc


def service_protected_pids() -> frozenset[int]:
    """Return the complete service ancestry or fail closed.

    Missing one ancestor would make it possible for a forged managed scope to
    select and signal that live process.  Enumeration errors therefore abort
    supervisor construction instead of silently shortening the protected set.
    """

    protected: set[int] = set()
    current = os.getpid()
    parents = _windows_parent_map() if os.name == "nt" else None
    while current > 0 and current not in protected:
        protected.add(current)
        if parents is not None:
            parent = parents.get(current)
        else:
            parent = _linux_parent_pid(current)
        if parent is None:
            # A parent recorded by the previous process entry may have exited
            # before this snapshot/read.  It is no longer signalable, so the
            # live ancestry ends here.  A still-live process whose parent
            # relation cannot be established remains a hard failure.
            if _pid_definitely_dead(current):
                current = 0
                break
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "service process ancestry is incomplete",
                process_id=str(current),
            )
        current = parent
    if current > 0:
        raise ManagedProcessSupervisorError(
            PROCESS_STATE_CONFLICT,
            "service process ancestry contains a cycle",
            process_id=str(current),
        )
    return frozenset(protected)


def _linux_port_owner_pids(host: str, port: int) -> set[int]:
    inodes: set[str] = set()
    expected_address = socket.inet_aton(host)[::-1].hex().upper()
    for table in (Path("/proc/net/tcp"),):
        try:
            lines = table.read_text(encoding="ascii").splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            if len(fields) < 10 or fields[3] != "0A":
                continue
            try:
                local_address, local_port_text = fields[1].rsplit(":", 1)
                local_port = int(local_port_text, 16)
            except (ValueError, IndexError):
                continue
            if local_port == port and local_address.upper() == expected_address:
                inodes.add(fields[9])
    if not inodes:
        return set()
    owners: set[int] = set()
    proc_root = Path("/proc")
    try:
        candidates = tuple(proc_root.iterdir())
    except OSError:
        return owners
    for candidate in candidates:
        if not candidate.name.isdigit():
            continue
        try:
            descriptors = tuple((candidate / "fd").iterdir())
        except OSError:
            continue
        for descriptor in descriptors:
            try:
                target = os.readlink(descriptor)
            except OSError:
                continue
            if target.startswith("socket:[") and target[8:-1] in inodes:
                owners.add(int(candidate.name))
                break
    return owners


def _windows_port_owner_pids(host: str, port: int) -> set[int]:
    try:
        import ctypes
        from ctypes import wintypes

        iphlpapi = ctypes.WinDLL("iphlpapi", use_last_error=True)
        iphlpapi.GetExtendedTcpTable.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(wintypes.ULONG),
            wintypes.BOOL,
            wintypes.ULONG,
            wintypes.ULONG,
            wintypes.ULONG,
        ]
        iphlpapi.GetExtendedTcpTable.restype = wintypes.DWORD
        size = wintypes.ULONG(0)
        result = iphlpapi.GetExtendedTcpTable(None, ctypes.byref(size), False, 2, 5, 0)
        if result not in {0, 122}:
            raise OSError(int(result), "GetExtendedTcpTable size probe failed")
        buffer = None
        for _attempt in range(4):
            buffer = ctypes.create_string_buffer(max(int(size.value), 4))
            result = iphlpapi.GetExtendedTcpTable(
                buffer, ctypes.byref(size), False, 2, 5, 0
            )
            if result == 0:
                break
            if result != 122:  # ERROR_INSUFFICIENT_BUFFER
                raise OSError(int(result), "GetExtendedTcpTable failed")
        else:
            raise OSError(122, "GetExtendedTcpTable kept growing")
        assert buffer is not None
        raw = buffer.raw
        count = struct.unpack_from("<I", raw, 0)[0]
        expected_address = struct.unpack("<I", socket.inet_aton(host))[0]
        owners: set[int] = set()
        offset = 4
        for _ in range(count):
            state, _address, raw_port, _remote_address, _remote_port, pid = (
                struct.unpack_from("<6I", raw, offset)
            )
            offset += 24
            local_port = socket.ntohs(raw_port & 0xFFFF)
            if state == 2 and local_port == port and _address == expected_address:
                owners.add(int(pid))
        return owners
    except (OSError, struct.error, ValueError) as exc:
        raise ManagedProcessSupervisorError(
            PROCESS_STATE_CONFLICT,
            "Windows TCP ownership cannot be enumerated safely",
        ) from exc


def port_owner_pids(host: str, port: int) -> frozenset[int]:
    """Return listening owner PIDs for the managed IPv4 loopback endpoint."""

    if host != "127.0.0.1" or not 1 <= port <= 65535:
        return frozenset()
    owners = (
        _windows_port_owner_pids(host, port)
        if os.name == "nt"
        else _linux_port_owner_pids(host, port)
    )
    return frozenset(owners)


class _WindowsJob:
    """Named Job Object with finite memory/process/CPU limits."""

    def __init__(self, handle: Any, name: str, kernel32: Any) -> None:
        self.handle = handle
        self.name = name
        self.kernel32 = kernel32

    @classmethod
    def create(cls, name: str, limits: Mapping[str, Any]) -> "_WindowsJob":
        if os.name != "nt":
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED, "Windows Job Objects are unavailable"
            )
        try:
            import ctypes
            from ctypes import wintypes

            class IO_COUNTERS(ctypes.Structure):
                _fields_ = [(field, ctypes.c_uint64) for field in (
                    "ReadOperationCount",
                    "WriteOperationCount",
                    "OtherOperationCount",
                    "ReadTransferCount",
                    "WriteTransferCount",
                    "OtherTransferCount",
                )]

            class BASIC_LIMITS(ctypes.Structure):
                _fields_ = [
                    ("PerProcessUserTimeLimit", ctypes.c_int64),
                    ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD),
                ]

            class EXTENDED_LIMITS(ctypes.Structure):
                _fields_ = [
                    ("BasicLimitInformation", BASIC_LIMITS),
                    ("IoInfo", IO_COUNTERS),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t),
                ]

            class CPU_RATE(ctypes.Structure):
                _fields_ = [
                    ("ControlFlags", wintypes.DWORD),
                    ("CpuRate", wintypes.DWORD),
                ]

            process_count = int(limits["process_count"])
            memory_bytes = int(limits["memory_bytes"])
            cpu_percent = int(limits["cpu_percent"])
            if (
                process_count <= 0
                or memory_bytes <= 0
                or memory_bytes > int(ctypes.c_size_t(-1).value)
            ):
                raise ValueError("Windows Job limits must be positive")
            if not 1 <= cpu_percent <= 100:
                raise ValueError(
                    "Windows Job CPU hard-cap supports percentages from 1 to 100"
                )

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
            kernel32.CreateJobObjectW.restype = wintypes.HANDLE
            kernel32.SetInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
            ]
            kernel32.SetInformationJobObject.restype = wintypes.BOOL
            kernel32.AssignProcessToJobObject.argtypes = [
                wintypes.HANDLE,
                wintypes.HANDLE,
            ]
            kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
            kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            kernel32.TerminateJobObject.restype = wintypes.BOOL
            kernel32.IsProcessInJob.argtypes = [
                wintypes.HANDLE,
                wintypes.HANDLE,
                ctypes.POINTER(wintypes.BOOL),
            ]
            kernel32.IsProcessInJob.restype = wintypes.BOOL
            kernel32.QueryInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
                ctypes.POINTER(wintypes.DWORD),
            ]
            kernel32.QueryInformationJobObject.restype = wintypes.BOOL
            kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL

            ctypes.set_last_error(0)
            handle = kernel32.CreateJobObjectW(None, name)
            if not handle:
                raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
            if ctypes.get_last_error() == 183:  # ERROR_ALREADY_EXISTS
                kernel32.CloseHandle(handle)
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed Windows Job name already exists without launch recovery",
                )
            job = cls(handle, name, kernel32)
            information = EXTENDED_LIMITS()
            information.BasicLimitInformation.LimitFlags = (
                0x00000008  # JOB_OBJECT_LIMIT_ACTIVE_PROCESS
                | 0x00000200  # JOB_OBJECT_LIMIT_JOB_MEMORY
                | 0x00002000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            )
            information.BasicLimitInformation.ActiveProcessLimit = process_count
            information.JobMemoryLimit = memory_bytes
            if not kernel32.SetInformationJobObject(
                handle,
                9,
                ctypes.byref(information),
                ctypes.sizeof(information),
            ):
                job.close()
                raise OSError(ctypes.get_last_error(), "Job limits failed")
            cpu = CPU_RATE()
            cpu.ControlFlags = 0x1 | 0x4
            cpu.CpuRate = cpu_percent * 100
            if not kernel32.SetInformationJobObject(
                handle, 15, ctypes.byref(cpu), ctypes.sizeof(cpu)
            ):
                job.close()
                raise OSError(ctypes.get_last_error(), "Job CPU limit failed")
            return job
        except (KeyError, OSError, TypeError, ValueError) as exc:
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED,
                "finite Windows process limits could not be installed",
            ) from exc

    @classmethod
    def open(cls, name: str) -> "_WindowsJob | None":
        if os.name != "nt":
            return None
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenJobObjectW.argtypes = [
                wintypes.DWORD,
                wintypes.BOOL,
                wintypes.LPCWSTR,
            ]
            kernel32.OpenJobObjectW.restype = wintypes.HANDLE
            kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            kernel32.TerminateJobObject.restype = wintypes.BOOL
            kernel32.IsProcessInJob.argtypes = [
                wintypes.HANDLE,
                wintypes.HANDLE,
                ctypes.POINTER(wintypes.BOOL),
            ]
            kernel32.IsProcessInJob.restype = wintypes.BOOL
            kernel32.QueryInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
                ctypes.POINTER(wintypes.DWORD),
            ]
            kernel32.QueryInformationJobObject.restype = wintypes.BOOL
            kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL
            handle = kernel32.OpenJobObjectW(0x1F001F, False, name)
            if not handle:
                error = ctypes.get_last_error()
                if error in {2, 3}:  # ERROR_FILE_NOT_FOUND / PATH_NOT_FOUND
                    return None
                raise OSError(error, "OpenJobObjectW failed")
            return cls(handle, name, kernel32)
        except ManagedProcessSupervisorError:
            raise
        except OSError as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed Windows Job Object cannot be opened safely",
            ) from exc

    def assign(self, process: subprocess.Popen[bytes]) -> None:
        try:
            from ctypes import wintypes

            assigned = self.kernel32.AssignProcessToJobObject(
                self.handle,
                wintypes.HANDLE(int(process._handle)),  # type: ignore[attr-defined]
            )
        except (AttributeError, OSError, TypeError, ValueError) as exc:
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED,
                "child could not be assigned to its Windows Job Object",
            ) from exc
        if not assigned:
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED,
                "child could not be assigned to its Windows Job Object",
            )

    def contains(self, pid: int) -> bool:
        try:
            import ctypes
            from ctypes import wintypes

            process_handle = self.kernel32.OpenProcess(0x1000, False, pid)
            if not process_handle:
                return False
            try:
                result = wintypes.BOOL()
                return bool(
                    self.kernel32.IsProcessInJob(
                        process_handle, self.handle, ctypes.byref(result)
                    )
                    and result.value
                )
            finally:
                self.kernel32.CloseHandle(process_handle)
        except OSError:
            return False

    def active_process_ids(self) -> frozenset[int]:
        """Return every live member of this Job, or fail closed.

        Port ownership is not a death proof: a non-listening descendant can
        remain in a Job after its leader exits.  Query the Job membership
        itself before signalling or releasing the associated lease.
        """

        try:
            import ctypes
            from ctypes import wintypes

            class BASIC_PROCESS_ID_LIST(ctypes.Structure):
                _fields_ = [
                    ("NumberOfAssignedProcesses", wintypes.DWORD),
                    ("NumberOfProcessIdsInList", wintypes.DWORD),
                    ("ProcessIdList", ctypes.c_size_t * 1),
                ]

            size = max(ctypes.sizeof(BASIC_PROCESS_ID_LIST), 4096)
            for _attempt in range(4):
                buffer = ctypes.create_string_buffer(size)
                returned = wintypes.DWORD()
                if self.kernel32.QueryInformationJobObject(
                    self.handle,
                    3,  # JobObjectBasicProcessIdList
                    buffer,
                    size,
                    ctypes.byref(returned),
                ):
                    header = ctypes.cast(
                        buffer, ctypes.POINTER(BASIC_PROCESS_ID_LIST)
                    ).contents
                    assigned = int(header.NumberOfAssignedProcesses)
                    count = int(header.NumberOfProcessIdsInList)
                    offset = BASIC_PROCESS_ID_LIST.ProcessIdList.offset
                    required_count = max(assigned, count)
                    required = offset + required_count * ctypes.sizeof(
                        ctypes.c_size_t
                    )
                    if assigned > count or required > size:
                        size = required
                        continue
                    values = (ctypes.c_size_t * count).from_buffer(buffer, offset)
                    return frozenset(int(value) for value in values if int(value) > 0)
                error = ctypes.get_last_error()
                if error == 234:  # ERROR_MORE_DATA
                    size = max(size * 2, int(returned.value) or 0)
                    continue
                raise OSError(error, "QueryInformationJobObject failed")
            raise OSError("Job membership exceeded the query buffer")
        except (OSError, TypeError, ValueError) as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed Windows Job membership could not be verified",
            ) from exc

    def terminate(self) -> None:
        if self.handle and not self.kernel32.TerminateJobObject(self.handle, 1):
            raise OSError("TerminateJobObject failed")

    @staticmethod
    def resume(process: subprocess.Popen[bytes]) -> None:
        """Resume a CREATE_SUSPENDED process after Job assignment/receipt."""

        try:
            import ctypes
            from ctypes import wintypes

            ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
            ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]
            ntdll.NtResumeProcess.restype = ctypes.c_long
            status = int(
                ntdll.NtResumeProcess(
                    wintypes.HANDLE(int(process._handle))  # type: ignore[attr-defined]
                )
            )
            if status != 0:
                raise OSError(status, "NtResumeProcess failed")
        except (AttributeError, OSError, TypeError, ValueError) as exc:
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED,
                "suspended managed child could not be resumed",
            ) from exc

    @staticmethod
    def resume_pid(pid: int) -> None:
        """Resume a recovered suspended process by its exact verified PID."""

        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = [
                wintypes.DWORD,
                wintypes.BOOL,
                wintypes.DWORD,
            ]
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL
            handle = kernel32.OpenProcess(0x0800 | 0x1000, False, pid)
            if not handle:
                raise OSError(ctypes.get_last_error(), "OpenProcess failed")
            try:
                ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
                ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]
                ntdll.NtResumeProcess.restype = ctypes.c_long
                status = int(ntdll.NtResumeProcess(handle))
                if status != 0:
                    raise OSError(status, "NtResumeProcess failed")
            finally:
                kernel32.CloseHandle(handle)
        except (OSError, TypeError, ValueError) as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "recovered suspended child could not be resumed",
                process_id=str(pid),
            ) from exc

    def close(self) -> None:
        if self.handle:
            self.kernel32.CloseHandle(self.handle)
            self.handle = None


def _spawn_suspended_in_windows_job(
    job: _WindowsJob,
    argv: Sequence[str],
    *,
    cwd: str,
    environment: Mapping[str, str],
    stdout_stream: Any,
    stderr_stream: Any,
) -> _WindowsChildProcess:
    """Create a suspended process already associated with ``job``.

    ``PROC_THREAD_ATTRIBUTE_JOB_LIST`` closes the Popen-to-Assign crash gap:
    there is no observable child outside the exact named Job Object.
    """

    if os.name != "nt":
        raise ManagedProcessSupervisorError(
            RESOURCE_LIMIT_UNSUPPORTED,
            "atomic Windows Job launch is unavailable",
        )
    try:
        import ctypes
        from ctypes import wintypes
        import msvcrt

        class STARTUPINFOW(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("lpReserved", wintypes.LPWSTR),
                ("lpDesktop", wintypes.LPWSTR),
                ("lpTitle", wintypes.LPWSTR),
                ("dwX", wintypes.DWORD),
                ("dwY", wintypes.DWORD),
                ("dwXSize", wintypes.DWORD),
                ("dwYSize", wintypes.DWORD),
                ("dwXCountChars", wintypes.DWORD),
                ("dwYCountChars", wintypes.DWORD),
                ("dwFillAttribute", wintypes.DWORD),
                ("dwFlags", wintypes.DWORD),
                ("wShowWindow", wintypes.WORD),
                ("cbReserved2", wintypes.WORD),
                ("lpReserved2", wintypes.LPBYTE),
                ("hStdInput", wintypes.HANDLE),
                ("hStdOutput", wintypes.HANDLE),
                ("hStdError", wintypes.HANDLE),
            ]

        class STARTUPINFOEXW(ctypes.Structure):
            _fields_ = [
                ("StartupInfo", STARTUPINFOW),
                ("lpAttributeList", wintypes.LPVOID),
            ]

        class PROCESS_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("hProcess", wintypes.HANDLE),
                ("hThread", wintypes.HANDLE),
                ("dwProcessId", wintypes.DWORD),
                ("dwThreadId", wintypes.DWORD),
            ]

        kernel32 = job.kernel32
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.DuplicateHandle.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.HANDLE),
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        ]
        kernel32.DuplicateHandle.restype = wintypes.BOOL
        kernel32.InitializeProcThreadAttributeList.argtypes = [
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        kernel32.InitializeProcThreadAttributeList.restype = wintypes.BOOL
        kernel32.UpdateProcThreadAttribute.argtypes = [
            wintypes.LPVOID,
            wintypes.DWORD,
            ctypes.c_size_t,
            wintypes.LPVOID,
            ctypes.c_size_t,
            wintypes.LPVOID,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        kernel32.UpdateProcThreadAttribute.restype = wintypes.BOOL
        kernel32.DeleteProcThreadAttributeList.argtypes = [wintypes.LPVOID]
        kernel32.DeleteProcThreadAttributeList.restype = None
        kernel32.CreateProcessW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.LPWSTR,
            wintypes.LPVOID,
            wintypes.LPVOID,
            wintypes.BOOL,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.LPCWSTR,
            wintypes.LPVOID,
            ctypes.POINTER(PROCESS_INFORMATION),
        ]
        kernel32.CreateProcessW.restype = wintypes.BOOL
        kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.GetExitCodeProcess.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.DWORD),
        ]
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.TerminateProcess.restype = wintypes.BOOL

        for key, value in environment.items():
            if (
                not key
                or "\0" in key
                or "=" in key
                or "\0" in value
            ):
                raise ValueError("invalid Windows child environment")
        if not argv or any("\0" in value for value in (*argv, cwd)):
            raise ValueError("invalid Windows child command")
        command_line = ctypes.create_unicode_buffer(
            subprocess.list2cmdline(list(argv))
        )
        environment_entries = sorted(
            environment.items(), key=lambda item: item[0].upper()
        )
        environment_block = ctypes.create_unicode_buffer(
            "\0".join(f"{key}={value}" for key, value in environment_entries)
            + "\0"
        )

        def duplicate(source: int, *, desired_access: int | None = None) -> int:
            target = wintypes.HANDLE()
            current = kernel32.GetCurrentProcess()
            if not kernel32.DuplicateHandle(
                current,
                wintypes.HANDLE(source),
                current,
                ctypes.byref(target),
                0 if desired_access is None else desired_access,
                True,
                0x00000002 if desired_access is None else 0,
                # DUPLICATE_SAME_ACCESS for stdio; an explicitly restricted
                # Job handle below only keeps the kernel object alive.
            ):
                raise ctypes.WinError(ctypes.get_last_error())
            return int(target.value)

        with _WINDOWS_SPAWN_LOCK:
            inherited: list[int] = []
            attribute_pointer: Any = None
            attribute_initialized = False
            information = PROCESS_INFORMATION()
            try:
                with open(os.devnull, "rb", buffering=0) as null_stream:
                    inherited = [
                        duplicate(msvcrt.get_osfhandle(null_stream.fileno())),
                        duplicate(msvcrt.get_osfhandle(stdout_stream.fileno())),
                        duplicate(msvcrt.get_osfhandle(stderr_stream.fileno())),
                        # Keep the named Job reachable if the launcher dies.
                        # KILL_ON_JOB_CLOSE then also guarantees that a dead
                        # leader cannot strand descendants after the final
                        # inherited/recovery handle closes.  The child only
                        # receives SYNCHRONIZE and cannot mutate its own hard
                        # Job limits through this inherited capability.
                        duplicate(int(job.handle), desired_access=0x00100000),
                    ]
                handle_values = (wintypes.HANDLE * len(inherited))(*inherited)
                job_values = (wintypes.HANDLE * 1)(job.handle)
                attribute_size = ctypes.c_size_t()
                kernel32.InitializeProcThreadAttributeList(
                    None, 2, 0, ctypes.byref(attribute_size)
                )
                if not attribute_size.value:
                    raise ctypes.WinError(ctypes.get_last_error())
                attribute_storage = ctypes.create_string_buffer(
                    attribute_size.value
                )
                attribute_pointer = ctypes.cast(
                    attribute_storage, wintypes.LPVOID
                )
                if not kernel32.InitializeProcThreadAttributeList(
                    attribute_pointer,
                    2,
                    0,
                    ctypes.byref(attribute_size),
                ):
                    raise ctypes.WinError(ctypes.get_last_error())
                attribute_initialized = True
                for attribute, values in (
                    (0x00020002, handle_values),  # HANDLE_LIST
                    (0x0002000D, job_values),  # JOB_LIST
                ):
                    if not kernel32.UpdateProcThreadAttribute(
                        attribute_pointer,
                        0,
                        attribute,
                        ctypes.cast(values, wintypes.LPVOID),
                        ctypes.sizeof(values),
                        None,
                        None,
                    ):
                        raise ctypes.WinError(ctypes.get_last_error())
                startup = STARTUPINFOEXW()
                startup.StartupInfo.cb = ctypes.sizeof(startup)
                startup.StartupInfo.dwFlags = 0x00000100  # STARTF_USESTDHANDLES
                startup.StartupInfo.hStdInput = inherited[0]
                startup.StartupInfo.hStdOutput = inherited[1]
                startup.StartupInfo.hStdError = inherited[2]
                startup.lpAttributeList = attribute_pointer
                flags = (
                    0x00000004  # CREATE_SUSPENDED
                    | 0x00000400  # CREATE_UNICODE_ENVIRONMENT
                    | 0x00000200  # CREATE_NEW_PROCESS_GROUP
                    | 0x00080000  # EXTENDED_STARTUPINFO_PRESENT
                    | 0x08000000  # CREATE_NO_WINDOW
                )
                if not kernel32.CreateProcessW(
                    str(argv[0]),
                    command_line,
                    None,
                    None,
                    True,
                    flags,
                    ctypes.cast(environment_block, wintypes.LPVOID),
                    cwd,
                    ctypes.cast(ctypes.byref(startup), wintypes.LPVOID),
                    ctypes.byref(information),
                ):
                    raise ctypes.WinError(ctypes.get_last_error())
            finally:
                if attribute_initialized:
                    kernel32.DeleteProcThreadAttributeList(attribute_pointer)
                if information.hThread:
                    kernel32.CloseHandle(information.hThread)
                for inherited_handle in inherited:
                    kernel32.CloseHandle(wintypes.HANDLE(inherited_handle))
        if not information.hProcess or not information.dwProcessId:
            raise OSError("CreateProcessW returned an incomplete process identity")
        return _WindowsChildProcess(
            int(information.hProcess),
            int(information.dwProcessId),
            argv,
            kernel32,
        )
    except ManagedProcessSupervisorError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise ManagedProcessSupervisorError(
            RESOURCE_LIMIT_UNSUPPORTED,
            "managed child could not be created atomically inside its Windows Job",
        ) from exc


class ManagedProcessSupervisor:
    """Launch, monitor, stop, and recover managed child process attempts."""

    def __init__(
        self,
        runtime_config: Mapping[str, Any],
        store: "ManagedImportStore",
        port_reservations: ManagedPortReservationRegistry,
        *,
        secret_resolver: Callable[[str], str] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        health_timeout_seconds: float = 15.0,
        stop_timeout_seconds: float = 10.0,
        poll_interval_seconds: float = 1.0,
        protected_pids: Iterable[int] = (),
        fault_injector: Callable[[str, Mapping[str, Any]], None] | None = None,
    ) -> None:
        self.runtime_config = deepcopy(dict(runtime_config))
        self.store = store
        self.port_reservations = port_reservations
        self.secret_resolver = secret_resolver
        self.clock = clock
        self.health_timeout_seconds = max(float(health_timeout_seconds), 0.1)
        self.stop_timeout_seconds = max(float(stop_timeout_seconds), 0.1)
        self.poll_interval_seconds = max(float(poll_interval_seconds), 0.05)
        self.claim_owner = f"supervisor-{os.getpid()}-{uuid4().hex}"
        self.fault_injector = fault_injector
        self.protected_pids = frozenset(
            set(service_protected_pids())
            | {
                pid
                for pid in protected_pids
                if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0
            }
        )
        self._repository = _SupervisorRepository(store, clock=clock)
        self._owned: dict[str, _OwnedProcess] = {}
        self._owned_lock = threading.RLock()
        self._monitor_stop = threading.Event()
        self._monitor_wake = threading.Event()
        self._monitor_thread: threading.Thread | None = None
        self._last_errors: dict[str, ManagedProcessSupervisorError] = {}
        self._health_failures: dict[str, int] = {}
        self._lifecycle = threading.Condition(threading.RLock())
        self._operation_depth = threading.local()
        self._active_operations = 0
        self._closing = False
        self._closed = False
        self._process_guard_depth = threading.local()

    def _fault(self, point: str, context: _ProcessContext) -> None:
        if self.fault_injector is not None:
            self.fault_injector(
                point,
                {
                    "project_id": context.project_id,
                    "sprint_id": context.sprint_id,
                    "process_id": context.process.get("process_id"),
                    "port_lease_id": context.process.get("port_lease_id"),
                },
            )

    @property
    def last_errors(self) -> Mapping[str, ManagedProcessSupervisorError]:
        return dict(self._last_errors)

    @contextmanager
    def _operation(self) -> Iterator[None]:
        """Track every public operation so shutdown can prove quiescence."""

        depth = int(getattr(self._operation_depth, "value", 0))
        if depth == 0:
            with self._lifecycle:
                if self._closing or self._closed:
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        "managed supervisor is closing",
                    )
                self._active_operations += 1
        self._operation_depth.value = depth + 1
        try:
            yield
        finally:
            remaining = int(getattr(self._operation_depth, "value", 1)) - 1
            self._operation_depth.value = max(remaining, 0)
            if depth == 0:
                with self._lifecycle:
                    self._active_operations -= 1
                    self._lifecycle.notify_all()

    @contextmanager
    def _process_action_guard(self, process_id: str) -> Iterator[None]:
        """Serialize claims and their OS side effects across service instances.

        The durable fence is checked while this per-attempt lock is held.  A
        contender therefore cannot steal an expired claim while the previous
        owner is paused between a fence check and spawn/signal/release.
        """

        if os.name == "nt":
            mutex_name = self._windows_action_mutex_name(process_id)
            key = os.path.normcase(mutex_name)
            with _PROCESS_ACTION_LOCKS_GUARD:
                local_lock = _PROCESS_ACTION_LOCKS.setdefault(
                    key, threading.RLock()
                )
            depths = getattr(self._process_guard_depth, "values", None)
            if depths is None:
                depths = {}
                self._process_guard_depth.values = depths
            with local_lock:
                nested = int(depths.get(key, 0))
                depths[key] = nested + 1
                mutex: tuple[Any, Any] | None = None
                try:
                    if nested == 0:
                        mutex = self._acquire_windows_action_mutex(
                            mutex_name,
                            process_id,
                            timeout_seconds=max(
                                (self.health_timeout_seconds * 2)
                                + (self.stop_timeout_seconds * 2)
                                + 10.0,
                                30.0,
                            ),
                        )
                    yield
                finally:
                    depths[key] = int(depths.get(key, 1)) - 1
                    if depths[key] <= 0:
                        depths.pop(key, None)
                    if mutex is not None:
                        handle, kernel32 = mutex
                        active_exception = sys.exc_info()[0] is not None
                        released = False
                        try:
                            released = bool(kernel32.ReleaseMutex(handle))
                        finally:
                            kernel32.CloseHandle(handle)
                        if not released and not active_exception:
                            raise ManagedProcessSupervisorError(
                                PROCESS_STATE_CONFLICT,
                                "managed process action mutex release failed",
                                process_id=process_id,
                            )
            return

        pid_root = self._validated_configured_root("pid_root", process_id)
        lock_root = pid_root / ".action-locks"
        directory_descriptors: list[int] = []
        try:
            pid_descriptor = self._open_posix_directory_chain(
                pid_root, create=True
            )
            directory_descriptors.append(pid_descriptor)
            try:
                os.mkdir(".action-locks", 0o700, dir_fd=pid_descriptor)
            except FileExistsError:
                pass
            directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            directory_flags |= getattr(os, "O_CLOEXEC", 0) | getattr(
                os, "O_NOFOLLOW", 0
            )
            directory_descriptors.append(
                os.open(
                    ".action-locks",
                    directory_flags,
                    dir_fd=pid_descriptor,
                )
            )
            self._validate_frozen_root(
                lock_root,
                pid_root / ".action-locks",
                label="managed process action lock root",
                process_id=process_id,
                forbidden_roots=self._physical_forbidden_roots(workspace=False),
            )
        except BaseException:
            for directory_descriptor in reversed(directory_descriptors):
                os.close(directory_descriptor)
            raise
        name = hashlib.sha256(process_id.encode("utf-8")).hexdigest() + ".lock"
        path = lock_root / name
        key = _lexical_path_key(path)
        with _PROCESS_ACTION_LOCKS_GUARD:
            local_lock = _PROCESS_ACTION_LOCKS.setdefault(key, threading.RLock())
        depths = getattr(self._process_guard_depth, "values", None)
        if depths is None:
            depths = {}
            self._process_guard_depth.values = depths
        with local_lock:
            nested = int(depths.get(key, 0))
            depths[key] = nested + 1
            descriptor: int | None = None
            try:
                if nested == 0:
                    try:
                        descriptor = self._open_action_lock_file(
                            path,
                            process_id,
                            directory_fd=(
                                directory_descriptors[-1]
                                if os.name != "nt"
                                else None
                            ),
                        )
                        import fcntl

                        fcntl.flock(descriptor, fcntl.LOCK_EX)
                    except OSError as exc:
                        if descriptor is not None:
                            os.close(descriptor)
                            descriptor = None
                        raise ManagedProcessSupervisorError(
                            PROCESS_STATE_CONFLICT,
                            "managed process action lock is unavailable",
                            process_id=process_id,
                        ) from exc
                yield
            finally:
                depths[key] = int(depths.get(key, 1)) - 1
                if depths[key] <= 0:
                    depths.pop(key, None)
                if descriptor is not None:
                    try:
                        os.lseek(descriptor, 0, os.SEEK_SET)
                        import fcntl

                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    finally:
                        os.close(descriptor)
                for directory_descriptor in reversed(directory_descriptors):
                    os.close(directory_descriptor)

    def _windows_action_mutex_name(self, process_id: str) -> str:
        try:
            database_path = Path(self.store.database_path)
            database_identity = database_path.stat()
        except OSError as exc:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed runtime database identity is unavailable",
                process_id=process_id,
            ) from exc
        file_identity = (
            f"{database_identity.st_dev}:{database_identity.st_ino}"
            if database_identity.st_ino
            else _path_key(database_path)
        )
        digest = hashlib.sha256(
            "\0".join((file_identity, process_id)).encode("utf-8")
        ).hexdigest()
        return f"Global\\nginx-qa-process-action-{digest}"

    @staticmethod
    def _acquire_windows_action_mutex(
        name: str,
        process_id: str,
        *,
        timeout_seconds: float,
    ) -> tuple[Any, Any]:
        """Acquire a crash-releasing, cross-process Windows action lock."""

        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.argtypes = [
            ctypes.c_void_p,
            wintypes.BOOL,
            wintypes.LPCWSTR,
        ]
        kernel32.CreateMutexW.restype = wintypes.HANDLE
        kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.ReleaseMutex.argtypes = [wintypes.HANDLE]
        kernel32.ReleaseMutex.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.CreateMutexW(None, False, name)
        if not handle:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed process action mutex is unavailable",
                process_id=process_id,
            ) from ctypes.WinError(ctypes.get_last_error())
        milliseconds = max(1, min(int(timeout_seconds * 1000), 0xFFFFFFFE))
        result = int(kernel32.WaitForSingleObject(handle, milliseconds))
        if result in {0x00000000, 0x00000080}:  # acquired / abandoned owner
            return handle, kernel32
        error = ctypes.get_last_error()
        kernel32.CloseHandle(handle)
        detail = "timed out" if result == 0x00000102 else "failed"
        cause = OSError(error, f"WaitForSingleObject {detail}")
        raise ManagedProcessSupervisorError(
            PROCESS_STATE_CONFLICT,
            f"managed process action mutex {detail}",
            process_id=process_id,
        ) from cause

    def _validate_config(self, context: _ProcessContext) -> None:
        if context.runtime_state.get("runtime_config") != self.runtime_config:
            raise ManagedProcessSupervisorError(
                RUNTIME_CONFIG_DRIFT,
                "current managed runtime config differs from the frozen sprint config",
                process_id=str(context.process.get("process_id") or ""),
            )

    def _physical_forbidden_roots(self, *, workspace: bool) -> tuple[str, ...]:
        values = [
            root
            for root in self.runtime_config.get("protected_roots", [])
            if isinstance(root, str)
        ]
        keys = (
            ("service_root", "runtime_root", "prompt_root")
            if workspace
            else ("service_root", "managed_root", "prompt_root")
        )
        values.extend(
            value
            for key in keys
            if isinstance((value := self.runtime_config.get(key)), str)
        )
        return tuple(dict.fromkeys(values))

    def _validate_frozen_root(
        self,
        path: Path,
        expected: Path,
        *,
        label: str,
        process_id: str,
        forbidden_roots: Iterable[str],
    ) -> Path:
        """Pin a mutation/execution root to its durable lexical identity.

        The expected side deliberately is *not* resolved again.  Resolving
        both sides would let a replaced Windows junction make an unsafe target
        appear equal to the frozen path.
        """

        if (
            not path.is_absolute()
            or not expected.is_absolute()
            or "\x00" in str(path)
            or "\x00" in str(expected)
            or _lexical_path_key(path) != _lexical_path_key(expected)
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"{label} differs from its frozen absolute path",
                process_id=process_id,
            )
        try:
            exists = os.path.lexists(path)
            if exists and (
                _path_entry_is_redirected(path) or not path.is_dir()
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    f"{label} is redirected or is not a directory",
                    process_id=process_id,
                )
            resolved = Path(os.path.realpath(path))
        except OSError as exc:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"{label} identity cannot be verified",
                process_id=process_id,
            ) from exc
        if _lexical_path_key(resolved) != _lexical_path_key(expected):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"{label} resolves away from its frozen path",
                process_id=process_id,
            )
        for raw_forbidden in forbidden_roots:
            forbidden = Path(raw_forbidden)
            if not forbidden.is_absolute() or "\x00" in raw_forbidden:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed protected root is not an absolute path",
                    process_id=process_id,
                )
            physical_forbidden = Path(os.path.realpath(forbidden))
            if _is_within(resolved, physical_forbidden) or _is_within(
                physical_forbidden, resolved
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    f"{label} overlaps a protected physical root",
                    process_id=process_id,
                )
        return Path(os.path.abspath(os.path.normpath(path)))

    def _validated_configured_root(self, key: str, process_id: str) -> Path:
        raw = self.runtime_config.get(key)
        if not isinstance(raw, str):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"managed {key} is missing",
                process_id=process_id,
            )
        root = Path(raw)
        return self._validate_frozen_root(
            root,
            root,
            label=f"managed {key}",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=False),
        )

    @staticmethod
    def _open_posix_directory_chain(path: Path, *, create: bool) -> int:
        if not path.is_absolute():
            raise OSError("managed directory must be absolute")
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path.anchor or "/", flags)
        try:
            for component in path.parts[1:]:
                try:
                    child = os.open(component, flags, dir_fd=descriptor)
                except FileNotFoundError:
                    if not create:
                        raise
                    try:
                        os.mkdir(component, 0o700, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                    child = os.open(component, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _ensure_windows_directory(
        self, path: Path, *, process_id: str, purpose: str
    ) -> None:
        missing: list[str] = []
        cursor = path
        while not os.path.lexists(cursor):
            if cursor.parent == cursor:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    f"managed {purpose} has no existing safe ancestor",
                    process_id=process_id,
                )
            missing.append(cursor.name)
            cursor = cursor.parent
        descriptor = self._open_windows_verified_path(
            cursor,
            process_id,
            purpose=f"{purpose} ancestor",
            writable=False,
            create=False,
            directory=True,
            unique=False,
            block_rename=True,
            share_write=True,
            retry_sharing_violation=True,
        )
        try:
            for component in reversed(missing):
                child_path = cursor / component
                child_descriptor = self._open_or_create_windows_directory_child(
                    descriptor,
                    component,
                    child_path,
                    process_id,
                    purpose=purpose,
                )
                os.close(descriptor)
                descriptor = child_descriptor
                cursor = child_path
        finally:
            os.close(descriptor)

    def _ensure_managed_directory(
        self, path: Path, *, process_id: str, purpose: str
    ) -> None:
        if os.name == "nt":
            self._ensure_windows_directory(
                path,
                process_id=process_id,
                purpose=purpose,
            )
            return
        descriptor = self._open_posix_directory_chain(path, create=True)
        os.close(descriptor)

    @staticmethod
    def _verified_windows_descriptor(
        handle: int,
        path: Path,
        process_id: str,
        *,
        purpose: str,
        directory: bool,
        unique: bool,
        descriptor_flags: int,
    ) -> int:
        """Verify one already-open Windows object and transfer its handle."""

        import ctypes
        import msvcrt
        from ctypes import wintypes

        class BY_HANDLE_FILE_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("dwFileAttributes", wintypes.DWORD),
                ("ftCreationTime", wintypes.FILETIME),
                ("ftLastAccessTime", wintypes.FILETIME),
                ("ftLastWriteTime", wintypes.FILETIME),
                ("dwVolumeSerialNumber", wintypes.DWORD),
                ("nFileSizeHigh", wintypes.DWORD),
                ("nFileSizeLow", wintypes.DWORD),
                ("nNumberOfLinks", wintypes.DWORD),
                ("nFileIndexHigh", wintypes.DWORD),
                ("nFileIndexLow", wintypes.DWORD),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetFileInformationByHandle.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(BY_HANDLE_FILE_INFORMATION),
        ]
        kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
        kernel32.GetFinalPathNameByHandleW.argtypes = [
            wintypes.HANDLE,
            wintypes.LPWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
        ]
        kernel32.GetFinalPathNameByHandleW.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        transferred = False
        try:
            information = BY_HANDLE_FILE_INFORMATION()
            if not kernel32.GetFileInformationByHandle(
                wintypes.HANDLE(handle), ctypes.byref(information)
            ):
                raise ctypes.WinError(ctypes.get_last_error())
            if (
                information.dwFileAttributes
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
                or (unique and information.nNumberOfLinks != 1)
                or bool(information.dwFileAttributes & 0x00000010) != directory
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    f"managed {purpose} path is redirected or multiply linked",
                    process_id=process_id,
                )
            final_buffer = ctypes.create_unicode_buffer(32768)
            final_length = kernel32.GetFinalPathNameByHandleW(
                wintypes.HANDLE(handle), final_buffer, len(final_buffer), 0
            )
            if not final_length or final_length >= len(final_buffer):
                raise ctypes.WinError(ctypes.get_last_error())
            final_path = final_buffer.value
            if final_path.startswith("\\\\?\\UNC\\"):
                final_path = "\\\\" + final_path[8:]
            elif final_path.startswith("\\\\?\\"):
                final_path = final_path[4:]
            if _lexical_path_key(final_path) != _lexical_path_key(path):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    f"managed {purpose} path changed identity",
                    process_id=process_id,
                )
            descriptor = msvcrt.open_osfhandle(handle, descriptor_flags)
            transferred = True
            return descriptor
        finally:
            if not transferred:
                kernel32.CloseHandle(wintypes.HANDLE(handle))

    @staticmethod
    def _open_or_create_windows_directory_child(
        parent_descriptor: int,
        component: str,
        child_path: Path,
        process_id: str,
        *,
        purpose: str,
    ) -> int:
        """Atomically open-or-create one directory relative to a pinned parent."""

        import ctypes
        import msvcrt
        from ctypes import wintypes

        if (
            not component
            or component in {".", ".."}
            or any(character in component for character in "\\/:\x00")
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"managed {purpose} has an unsafe path component",
                process_id=process_id,
            )

        class UNICODE_STRING(ctypes.Structure):
            _fields_ = [
                ("Length", wintypes.USHORT),
                ("MaximumLength", wintypes.USHORT),
                ("Buffer", wintypes.LPWSTR),
            ]

        class OBJECT_ATTRIBUTES(ctypes.Structure):
            _fields_ = [
                ("Length", wintypes.ULONG),
                ("RootDirectory", wintypes.HANDLE),
                ("ObjectName", ctypes.POINTER(UNICODE_STRING)),
                ("Attributes", wintypes.ULONG),
                ("SecurityDescriptor", ctypes.c_void_p),
                ("SecurityQualityOfService", ctypes.c_void_p),
            ]

        class IO_STATUS_BLOCK(ctypes.Structure):
            _fields_ = [
                ("Status", ctypes.c_void_p),
                ("Information", ctypes.c_size_t),
            ]

        encoded_length = len(component.encode("utf-16-le"))
        if encoded_length > 65532:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                f"managed {purpose} path component is too long",
                process_id=process_id,
            )
        name_buffer = ctypes.create_unicode_buffer(component)
        object_name = UNICODE_STRING(
            encoded_length,
            encoded_length + 2,
            ctypes.cast(name_buffer, wintypes.LPWSTR),
        )
        object_attributes = OBJECT_ATTRIBUTES(
            ctypes.sizeof(OBJECT_ATTRIBUTES),
            wintypes.HANDLE(msvcrt.get_osfhandle(parent_descriptor)),
            ctypes.pointer(object_name),
            0x00000040,  # OBJ_CASE_INSENSITIVE
            None,
            None,
        )
        io_status = IO_STATUS_BLOCK()
        handle = wintypes.HANDLE()
        ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
        ntdll.NtCreateFile.argtypes = [
            ctypes.POINTER(wintypes.HANDLE),
            wintypes.DWORD,
            ctypes.POINTER(OBJECT_ATTRIBUTES),
            ctypes.POINTER(IO_STATUS_BLOCK),
            ctypes.c_void_p,
            wintypes.ULONG,
            wintypes.ULONG,
            wintypes.ULONG,
            wintypes.ULONG,
            ctypes.c_void_p,
            wintypes.ULONG,
        ]
        ntdll.NtCreateFile.restype = ctypes.c_long
        ntdll.RtlNtStatusToDosError.argtypes = [ctypes.c_long]
        ntdll.RtlNtStatusToDosError.restype = wintypes.ULONG
        status = int(
            ntdll.NtCreateFile(
                ctypes.byref(handle),
                0x00100081,  # SYNCHRONIZE | READ_ATTRIBUTES | LIST_DIRECTORY
                ctypes.byref(object_attributes),
                ctypes.byref(io_status),
                None,
                0x00000080,  # FILE_ATTRIBUTE_NORMAL
                0x00000003,  # FILE_SHARE_READ | FILE_SHARE_WRITE
                3,  # FILE_OPEN_IF
                0x00200021,  # DIRECTORY | SYNCHRONOUS_NONALERT | OPEN_REPARSE
                None,
                0,
            )
        )
        if status < 0:
            error = int(ntdll.RtlNtStatusToDosError(status))
            raise ctypes.WinError(error)
        return ManagedProcessSupervisor._verified_windows_descriptor(
            int(handle.value),
            child_path,
            process_id,
            purpose=purpose,
            directory=True,
            unique=False,
            descriptor_flags=os.O_RDONLY,
        )

    @staticmethod
    def _open_windows_verified_path(
        path: Path,
        process_id: str,
        *,
        purpose: str,
        writable: bool,
        create: bool,
        exclusive: bool = False,
        directory: bool = False,
        unique: bool = True,
        block_rename: bool = False,
        share_write: bool = False,
        share_delete: bool = False,
        retry_sharing_violation: bool = False,
    ) -> int:
        """Open a Windows leaf itself and prove its post-open path identity."""

        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.c_void_p,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        kernel32.CreateFileW.restype = wintypes.HANDLE

        desired_access = 0x00000080 if directory else 0x80000000
        share_mode = 0x00000001  # FILE_SHARE_READ; deliberately no DELETE
        if directory and block_rename:
            if share_delete:
                raise ValueError("rename blocker cannot share delete access")
            share_mode |= 0x00000002  # FILE_SHARE_WRITE
        if directory and block_rename:
            # FILE_LIST_DIRECTORY plus no FILE_SHARE_DELETE prevents the
            # directory (or a pinned ancestor) from being renamed without
            # requesting DELETE ourselves.  Multiple pins can therefore
            # coexist, and children may still be created, atomically replaced,
            # renamed, or deleted while their parent stays fixed.
            desired_access |= 0x00000001  # FILE_LIST_DIRECTORY
        if share_write:
            share_mode |= 0x00000002  # FILE_SHARE_WRITE
        if share_delete:
            share_mode |= 0x00000004  # FILE_SHARE_DELETE
        descriptor_flags = os.O_RDONLY
        if writable:
            desired_access |= 0x40000000  # GENERIC_WRITE
            share_mode |= 0x00000002  # FILE_SHARE_WRITE
            descriptor_flags = os.O_RDWR
        invalid_handle = wintypes.HANDLE(-1).value
        retry_deadline = time.monotonic() + 10.0
        while True:
            handle = kernel32.CreateFileW(
                str(path),
                desired_access,
                share_mode,
                None,
                1 if exclusive else (4 if create else 3),
                # CREATE_NEW / OPEN_ALWAYS / OPEN_EXISTING
                0x00000080
                | 0x00200000
                | (0x02000000 if directory else 0),  # NORMAL | REPARSE | BACKUP
                None,
            )
            if handle != invalid_handle:
                break
            error = ctypes.get_last_error()
            if (
                not retry_sharing_violation
                or error != 32  # ERROR_SHARING_VIOLATION
                or time.monotonic() >= retry_deadline
            ):
                raise ctypes.WinError(error)
            time.sleep(0.01)
        return ManagedProcessSupervisor._verified_windows_descriptor(
            int(handle),
            path,
            process_id,
            purpose=purpose,
            directory=directory,
            unique=unique,
            descriptor_flags=descriptor_flags,
        )

    @staticmethod
    def _open_action_lock_file(
        path: Path, process_id: str, *, directory_fd: int | None = None
    ) -> int:
        """Open one stable lock inode without following a hostile leaf alias."""

        descriptor: int | None = None
        try:
            if os.name == "nt":
                descriptor = ManagedProcessSupervisor._open_windows_verified_path(
                    path,
                    process_id,
                    purpose="process action lock",
                    writable=True,
                    create=True,
                )
            else:
                flags = os.O_RDWR | os.O_CREAT
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                descriptor = os.open(
                    path.name if directory_fd is not None else path,
                    flags,
                    0o600,
                    dir_fd=directory_fd,
                )

            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process action lock is not a unique regular file",
                    process_id=process_id,
                )
            if metadata.st_size == 0:
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            return descriptor
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            raise

    def _validate_frozen_executable(
        self, executable: Path, *, process_id: str
    ) -> Path:
        if (
            not executable.is_absolute()
            or "\x00" in str(executable)
            or not os.path.lexists(executable)
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed executable is missing or is not an absolute path",
                process_id=process_id,
            )
        try:
            if _path_entry_is_redirected(executable) or not executable.is_file():
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed executable is redirected or is not a regular file",
                    process_id=process_id,
                )
            resolved = Path(os.path.realpath(executable))
        except OSError as exc:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed executable identity cannot be verified",
                process_id=process_id,
            ) from exc
        if _lexical_path_key(resolved) != _lexical_path_key(executable):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed executable resolves away from its frozen path",
                process_id=process_id,
            )
        if any(
            isinstance(root, str)
            and (
                _is_within(resolved, Path(root))
                or _is_within(Path(root), resolved)
            )
            for root in self.runtime_config.get("protected_roots", [])
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed executable overlaps a protected live root",
                process_id=process_id,
            )
        return resolved

    def _pid_receipt_path(self, process_id: str) -> Path:
        return Path(str(self.runtime_config["pid_root"])) / f"{process_id}.json"

    @contextmanager
    def _pinned_pid_receipt_parent(
        self, process_id: str
    ) -> Iterator[tuple[Path, int]]:
        """Hold the verified receipt parent stable for one filesystem mutation."""

        allowed = frozenset(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
        )
        alphanumeric = frozenset(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        )
        if (
            not process_id
            or len(process_id) > 128
            or process_id[0] not in alphanumeric
            or process_id[-1] not in alphanumeric | frozenset("_-")
            or any(character not in allowed for character in process_id)
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed process ID is not a safe PID receipt path segment",
                process_id=process_id,
            )
        root = self._validated_configured_root("pid_root", process_id)
        path = root / f"{process_id}.json"
        if os.name == "nt":
            descriptor = self._open_windows_verified_path(
                root,
                process_id,
                purpose="PID receipt root",
                writable=False,
                create=False,
                directory=True,
                unique=False,
                block_rename=True,
                share_write=True,
                retry_sharing_violation=True,
            )
            try:
                yield path, descriptor
            finally:
                os.close(descriptor)
            return
        descriptor = self._open_posix_directory_chain(root, create=False)
        try:
            yield path, descriptor
        finally:
            os.close(descriptor)

    def _write_pid_receipt(
        self, process_id: str, receipt: Mapping[str, Any]
    ) -> None:
        with self._pinned_pid_receipt_parent(process_id) as (path, descriptor):
            _atomic_write_json(
                path,
                receipt,
                directory_fd=descriptor if os.name != "nt" else None,
            )

    def _remove_pid_receipt(self, process_id: str) -> None:
        with self._pinned_pid_receipt_parent(process_id) as (path, descriptor):
            if os.name == "nt":
                path.unlink()
            else:
                os.unlink(path.name, dir_fd=descriptor)
                os.fsync(descriptor)

    def _prepare_paths(
        self,
        context: _ProcessContext,
        *,
        before_logs: Callable[[], None] | None = None,
    ) -> tuple[Any, Any]:
        process_id = str(context.process["process_id"])
        runtime_root = Path(str(context.process["runtime_root"]))
        configured_runtime_root = self._validated_configured_root(
            "process_runtime_root", process_id
        )
        expected_runtime = configured_runtime_root / process_id
        stdout_path = Path(str(context.process["stdout_log"]))
        stderr_path = Path(str(context.process["stderr_log"]))
        configured_log_root = self._validated_configured_root("log_root", process_id)
        configured_pid_root = self._validated_configured_root("pid_root", process_id)
        if (
            _lexical_path_key(runtime_root) != _lexical_path_key(expected_runtime)
            or not _is_lexically_within(runtime_root, configured_runtime_root)
            or not _is_lexically_within(stdout_path, configured_log_root)
            or not _is_lexically_within(stderr_path, configured_log_root)
            or not _is_lexically_within(
                self._pid_receipt_path(process_id), configured_pid_root
            )
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed process paths escape their frozen roots",
                process_id=process_id,
            )
        for purpose, root in (
            ("process runtime parent", configured_runtime_root),
            ("log root", configured_log_root),
            ("PID root", configured_pid_root),
        ):
            self._ensure_managed_directory(
                root,
                process_id=process_id,
                purpose=purpose,
            )
        configured_runtime_root = self._validated_configured_root(
            "process_runtime_root", process_id
        )
        configured_log_root = self._validated_configured_root("log_root", process_id)
        self._validated_configured_root("pid_root", process_id)
        runtime_root = self._validate_frozen_root(
            runtime_root,
            configured_runtime_root / process_id,
            label="managed process runtime root",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=False),
        )
        self._ensure_managed_directory(
            runtime_root,
            process_id=process_id,
            purpose="process runtime root",
        )
        self._validate_frozen_root(
            runtime_root,
            configured_runtime_root / process_id,
            label="managed process runtime root",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=False),
        )
        if before_logs is not None:
            before_logs()
        configured_log_root = self._validated_configured_root("log_root", process_id)
        self._validate_frozen_root(
            runtime_root,
            configured_runtime_root / process_id,
            label="managed process runtime root",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=False),
        )
        log_root_pin: int | None = None
        try:
            if os.name == "nt":
                log_root_pin = self._open_windows_verified_path(
                    configured_log_root,
                    process_id,
                    purpose="log root",
                    writable=False,
                    create=False,
                    directory=True,
                    unique=False,
                    block_rename=True,
                    share_write=True,
                    retry_sharing_violation=True,
                )
                stdout_descriptor = self._open_windows_verified_path(
                    stdout_path,
                    process_id,
                    purpose="stdout log",
                    writable=True,
                    create=True,
                    exclusive=True,
                )
                try:
                    stdout_stream = os.fdopen(
                        stdout_descriptor, "wb", buffering=0, closefd=True
                    )
                except BaseException:
                    os.close(stdout_descriptor)
                    raise
            else:
                stdout_stream = stdout_path.open("xb", buffering=0)
            try:
                if os.name == "nt":
                    stderr_descriptor = self._open_windows_verified_path(
                        stderr_path,
                        process_id,
                        purpose="stderr log",
                        writable=True,
                        create=True,
                        exclusive=True,
                    )
                    try:
                        stderr_stream = os.fdopen(
                            stderr_descriptor, "wb", buffering=0, closefd=True
                        )
                    except BaseException:
                        os.close(stderr_descriptor)
                        raise
                else:
                    stderr_stream = stderr_path.open("xb", buffering=0)
            except BaseException:
                stdout_stream.close()
                raise
        except FileExistsError as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "immutable process logs already exist without a recoverable PID receipt",
                process_id=process_id,
            ) from exc
        finally:
            if log_root_pin is not None:
                os.close(log_root_pin)
        return stdout_stream, stderr_stream

    def _validate_workspace_and_executable(self, context: _ProcessContext) -> None:
        process_id = str(context.process["process_id"])
        expected_root = Path(str(context.workspace.get("expected_root") or ""))
        workspace_root = Path(str(context.workspace["actual_git_toplevel"]))
        workspace_root = self._validate_frozen_root(
            workspace_root,
            expected_root,
            label="managed workspace root",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=True),
        )
        expected_git_dir = expected_root / ".git"
        self._validate_frozen_root(
            Path(str(context.workspace.get("actual_git_dir") or "")),
            expected_git_dir,
            label="managed workspace Git directory",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=True),
        )
        cwd = Path(str(context.process["cwd"]))
        cwd = self._validate_frozen_root(
            cwd,
            cwd,
            label="managed process cwd",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=True),
        )
        executable = Path(str(context.process["executable_path"]))
        self._validate_frozen_executable(executable, process_id=process_id)
        try:
            resolved_workspace = workspace_root.resolve(strict=True)
            resolved_cwd = cwd.resolve(strict=True)
        except OSError as exc:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed executable or cwd disappeared before launch",
                process_id=process_id,
            ) from exc
        if not resolved_cwd.is_dir() or not _is_within(resolved_cwd, resolved_workspace):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed process cwd escapes its owned workspace",
                process_id=process_id,
            )

    def _pin_windows_launch_paths(
        self, context: _ProcessContext
    ) -> tuple[_PinnedPath, ...]:
        """Capture stable Windows file identities across suspended spawn."""

        if os.name != "nt":
            return ()
        process_id = str(context.process["process_id"])
        # Revalidate after intent/log publication so a fault or concurrent
        # rename cannot exploit the earlier preflight-to-spawn interval.
        self._validate_workspace_and_executable(context)
        configured_runtime_root = self._validated_configured_root(
            "process_runtime_root", process_id
        )
        process_runtime_root = self._validate_frozen_root(
            Path(str(context.process["runtime_root"])),
            configured_runtime_root / process_id,
            label="managed process runtime root",
            process_id=process_id,
            forbidden_roots=self._physical_forbidden_roots(workspace=False),
        )
        executable = Path(str(context.process["executable_path"]))
        candidates = (
            (
                "workspace root",
                Path(str(context.workspace["actual_git_toplevel"])),
                True,
                True,
            ),
            ("process cwd", Path(str(context.process["cwd"])), True, True),
            # The executable leaf handle itself withholds delete sharing and
            # pins its ancestor chain without serializing unrelated children
            # that use the same interpreter directory.
            ("executable", executable, False, False),
        )
        pins: list[_PinnedPath] = []
        seen: set[tuple[str, bool]] = set()
        try:
            runtime_guard = self._open_windows_verified_path(
                process_runtime_root,
                process_id,
                purpose="process runtime root",
                writable=False,
                create=False,
                directory=True,
                unique=False,
                block_rename=True,
            )
            try:
                runtime_pin_path = process_runtime_root / ".nginx-qa-path-pin"
                created_runtime_pin = self._open_windows_verified_path(
                    runtime_pin_path,
                    process_id,
                    purpose="process runtime pin",
                    writable=True,
                    create=True,
                )
                os.close(created_runtime_pin)
                runtime_pin_descriptor = self._open_windows_verified_path(
                    runtime_pin_path,
                    process_id,
                    purpose="process runtime pin",
                    writable=False,
                    create=False,
                )
            except BaseException:
                os.close(runtime_guard)
                raise
            try:
                runtime_root_metadata = os.fstat(runtime_guard)
            except BaseException:
                os.close(runtime_pin_descriptor)
                os.close(runtime_guard)
                raise
            pins.append(
                _PinnedPath(
                    descriptor=runtime_guard,
                    path=process_runtime_root,
                    purpose="process runtime root",
                    directory=True,
                    device=runtime_root_metadata.st_dev,
                    inode=runtime_root_metadata.st_ino,
                )
            )
            seen.add((_lexical_path_key(process_runtime_root), True))
            try:
                runtime_metadata = os.fstat(runtime_pin_descriptor)
            except BaseException:
                os.close(runtime_pin_descriptor)
                raise
            pins.append(
                _PinnedPath(
                    descriptor=runtime_pin_descriptor,
                    path=runtime_pin_path,
                    purpose="process runtime pin",
                    directory=False,
                    device=runtime_metadata.st_dev,
                    inode=runtime_metadata.st_ino,
                )
            )
            seen.add((_lexical_path_key(runtime_pin_path), False))
            for purpose, path, directory, block_rename in candidates:
                key = (_lexical_path_key(path), directory)
                if key in seen:
                    continue
                seen.add(key)
                descriptor: int | None = None
                try:
                    descriptor = self._open_windows_verified_path(
                        path,
                        process_id,
                        purpose=purpose,
                        writable=False,
                        create=False,
                        directory=directory,
                        unique=not directory,
                        block_rename=block_rename,
                    )
                    metadata = os.fstat(descriptor)
                    expected_kind = (
                        stat.S_ISDIR(metadata.st_mode)
                        if directory
                        else stat.S_ISREG(metadata.st_mode)
                    )
                    if not expected_kind:
                        raise ManagedProcessSupervisorError(
                            PROCESS_STATE_CONFLICT,
                            f"managed {purpose} has the wrong filesystem type",
                            process_id=process_id,
                        )
                    pins.append(
                        _PinnedPath(
                            descriptor=descriptor,
                            path=path,
                            purpose=purpose,
                            directory=directory,
                            device=metadata.st_dev,
                            inode=metadata.st_ino,
                        )
                    )
                    descriptor = None
                finally:
                    if descriptor is not None:
                        os.close(descriptor)
            return tuple(pins)
        except BaseException:
            for pin in reversed(pins):
                os.close(pin.descriptor)
            raise

    def _revalidate_windows_launch_pins(
        self,
        pins: Sequence[_PinnedPath],
        *,
        process_id: str,
    ) -> None:
        """Prove every lexical path still names its pre-spawn file object."""

        if os.name != "nt":
            return
        for pin in pins:
            descriptor: int | None = None
            try:
                descriptor = self._open_windows_verified_path(
                    pin.path,
                    process_id,
                    purpose=pin.purpose,
                    writable=False,
                    create=False,
                    directory=pin.directory,
                    unique=not pin.directory,
                    share_delete=True,
                )
                metadata = os.fstat(descriptor)
                if (metadata.st_dev, metadata.st_ino) != (pin.device, pin.inode):
                    raise ManagedProcessSupervisorError(
                        PROCESS_STATE_CONFLICT,
                        f"managed {pin.purpose} changed during suspended launch",
                        process_id=process_id,
                    )
            finally:
                if descriptor is not None:
                    os.close(descriptor)

    @staticmethod
    def _close_launch_pins(pins: Sequence[_PinnedPath]) -> None:
        for pin in reversed(pins):
            try:
                os.close(pin.descriptor)
            except OSError:
                pass

    @classmethod
    def _release_launch_pins(cls, owned: _OwnedProcess) -> None:
        cls._close_launch_pins(owned.path_pins)
        owned.path_pins = ()

    def _resume_windows_with_pins(
        self,
        context: _ProcessContext,
        pid: int,
        *,
        startup_deadline_at: str,
    ) -> tuple[_PinnedPath, ...]:
        pins = self._pin_windows_launch_paths(context)
        try:
            self._revalidate_windows_launch_pins(
                pins,
                process_id=str(context.process["process_id"]),
            )
            deadline = _parse_timestamp(startup_deadline_at)
            if deadline is None or _utc_now(self.clock) >= deadline:
                raise ManagedProcessSupervisorError(
                    PROCESS_HEALTH_FAILED,
                    "managed gated launch expired before resume",
                    process_id=str(context.process["process_id"]),
            )
            _WindowsJob.resume_pid(pid)
            # Launch identity pins are needed only through the resume gate;
            # release them before ordinary child execution begins.
            return ()
        finally:
            self._close_launch_pins(pins)

    def _terminalize_expired_gated_launch(
        self,
        context: _ProcessContext,
        *,
        startup_deadline_at: str,
        create_restart: bool,
        action: str,
    ) -> SupervisionResult | None:
        deadline = _parse_timestamp(startup_deadline_at)
        if deadline is None or _utc_now(self.clock) < deadline:
            return None
        self._terminate_context(
            context,
            allow_posix_launch_helper=True,
        )
        terminal = self._terminalize(
            context,
            failed=True,
            create_restart=create_restart,
            terminal_reason=(
                "failure" if create_restart else "operator_cancelled"
            ),
            claim_owner=self.claim_owner,
            supervisor_fence=context.supervisor_fence,
        )
        return SupervisionResult(
            context.project_id,
            context.sprint_id,
            str(context.process["process_id"]),
            str(terminal.process["state"]),
            action,
        )

    @staticmethod
    def _wall_time_seconds(context: _ProcessContext) -> int:
        limits = context.process.get("resource_limits")
        wall = (
            limits.get("wall_time_seconds")
            if isinstance(limits, Mapping)
            else None
        )
        if not isinstance(wall, int) or isinstance(wall, bool) or wall <= 0:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed process has no finite wall-clock limit",
                process_id=str(context.process["process_id"]),
            )
        return wall

    @staticmethod
    def _effective_started_at(process: Mapping[str, Any]) -> datetime | None:
        candidates = tuple(
            parsed
            for key in ("os_process_created_at", "started_at")
            if (parsed := _parse_timestamp(process.get(key))) is not None
        )
        return min(candidates) if candidates else None

    def _bounded_startup_deadline(
        self,
        context: _ProcessContext,
        startup_deadline_at: str,
        *,
        identity: ProcessIdentity,
        intent_created_at: Any = None,
    ) -> str:
        """Cap startup by one immutable process-lifetime wall budget."""

        deadline = _parse_timestamp(startup_deadline_at)
        created_at = _parse_timestamp(identity.created_at)
        if deadline is None or created_at is None:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed launch has invalid durable timing evidence",
                process_id=str(context.process["process_id"]),
            )
        wall = timedelta(seconds=self._wall_time_seconds(context))
        deadline = min(deadline, created_at + wall)
        intent_at = _parse_timestamp(intent_created_at)
        if intent_created_at is not None and intent_at is None:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed launch intent has invalid creation time",
                process_id=str(context.process["process_id"]),
            )
        if intent_at is not None:
            deadline = min(deadline, intent_at + wall)
        return deadline.isoformat()

    def _child_environment(self, context: _ProcessContext) -> dict[str, str]:
        environment = {
            key: value
            for key in _MINIMAL_ENVIRONMENT
            if isinstance((value := os.environ.get(key)), str)
        }
        explicit = context.process.get("environment_redacted")
        if not isinstance(explicit, Mapping):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed process environment snapshot is invalid",
                process_id=str(context.process["process_id"]),
            )
        explicit_names: set[str] = set()
        for key, raw_value in explicit.items():
            if (
                not isinstance(key, str)
                or not key
                or "=" in key
                or "\0" in key
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process environment contains an invalid name",
                    process_id=str(context.process["process_id"]),
                )
            folded_key = key.casefold()
            if folded_key in explicit_names:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process environment contains colliding names",
                    process_id=str(context.process["process_id"]),
                )
            explicit_names.add(folded_key)
            if folded_key.startswith("nginx_qa_managed_"):
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process environment contains a reserved key",
                    process_id=str(context.process["process_id"]),
                )
            if isinstance(raw_value, str):
                resolved_value = raw_value
            elif (
                isinstance(raw_value, Mapping)
                and set(raw_value) == {"secret_ref"}
                and isinstance(raw_value.get("secret_ref"), str)
            ):
                if self.secret_resolver is None:
                    raise ManagedProcessSupervisorError(
                        PROCESS_LAUNCH_FAILED,
                        "managed process secret provider is unavailable",
                        process_id=str(context.process["process_id"]),
                    )
                resolved_value = self.secret_resolver(
                    str(raw_value["secret_ref"])
                )
            else:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process environment snapshot is invalid",
                    process_id=str(context.process["process_id"]),
                )
            if not isinstance(resolved_value, str) or "\0" in resolved_value:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed process environment contains an invalid value",
                    process_id=str(context.process["process_id"]),
                )
            for inherited_key in tuple(environment):
                if inherited_key.casefold() == folded_key:
                    del environment[inherited_key]
            environment[key] = resolved_value
        environment.update(
            {
                "NGINX_QA_MANAGED_HOST": str(context.lease["host"]),
                "NGINX_QA_MANAGED_PORT": str(context.lease["port"]),
                "NGINX_QA_MANAGED_PROCESS_ID": str(context.process["process_id"]),
                "NGINX_QA_MANAGED_RUNTIME_ROOT": str(context.process["runtime_root"]),
                "NGINX_QA_MANAGED_LAUNCH_NONCE": str(context.process["launch_nonce"]),
            }
        )
        return environment

    @staticmethod
    def _windows_job_name(context: _ProcessContext) -> str:
        # A global name is required for recovery by a service instance running
        # in another Terminal Services session.  The digest remains scoped to
        # the immutable process attempt, and create() rejects pre-existence.
        return "Global\\nginx-qa-managed-" + hashlib.sha256(
            (
                str(context.process["process_id"])
                + "\0"
                + str(context.process["launch_nonce"])
            ).encode("utf-8")
        ).hexdigest()[:40]

    def _spawn(
        self,
        context: _ProcessContext,
        stdout_stream: Any,
        stderr_stream: Any,
    ) -> tuple[Any, _WindowsJob | None, int | str, bool, int | None]:
        command = context.process.get("command_redacted")
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(item, str) for item in command)
            or any("\0" in item for item in command)
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed process command snapshot is invalid",
                process_id=str(context.process["process_id"]),
            )
        argv = [str(context.process["executable_path"]), *command[1:]]
        limits = context.process.get("resource_limits")
        if not isinstance(limits, Mapping):
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED,
                "managed process has no finite resource limits",
                process_id=str(context.process["process_id"]),
            )
        if os.name != "nt":
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED,
                "assignment-wide hard resource limits are unavailable",
                process_id=str(context.process["process_id"]),
            )
        job: _WindowsJob | None = None
        gate_read: int | None = None
        gate_write: int | None = None
        options: dict[str, Any] = {}
        if os.name == "nt":
            job = _WindowsJob.create(self._windows_job_name(context), limits)
        else:
            options["start_new_session"] = True
            gate_read, gate_write = os.pipe()
            os.set_inheritable(gate_read, True)
            os.set_inheritable(gate_write, False)
            options["pass_fds"] = (gate_read,)
            helper = Path(__file__).with_name("process_exec_helper.py").resolve(
                strict=True
            )
            argv = [
                sys.executable,
                str(helper),
                json.dumps(dict(limits), sort_keys=True, separators=(",", ":")),
                str(gate_read),
                *argv,
            ]
        try:
            if os.name == "nt":
                assert job is not None
                process = _spawn_suspended_in_windows_job(
                    job,
                    argv,
                    cwd=str(context.process["cwd"]),
                    environment=self._child_environment(context),
                    stdout_stream=stdout_stream,
                    stderr_stream=stderr_stream,
                )
                return process, job, job.name, True, None
            process = subprocess.Popen(
                argv,
                cwd=str(context.process["cwd"]),
                env=self._child_environment(context),
                stdin=subprocess.DEVNULL,
                stdout=stdout_stream,
                stderr=stderr_stream,
                shell=False,
                close_fds=True,
                **options,
            )
            if gate_read is not None:
                os.close(gate_read)
                gate_read = None
            group_id: int | str = process.pid
            return process, job, group_id, False, gate_write
        except ManagedProcessSupervisorError:
            for descriptor in (gate_read, gate_write):
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
            if "process" in locals() and process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            if job is not None:
                job.close()
            raise
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            for descriptor in (gate_read, gate_write):
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
            if job is not None:
                job.close()
            raise ManagedProcessSupervisorError(
                PROCESS_LAUNCH_FAILED,
                "managed child could not be created",
                process_id=str(context.process["process_id"]),
            ) from exc

    def _identity_matches(
        self,
        context: _ProcessContext,
        identity: ProcessIdentity,
        receipt: Mapping[str, Any],
        *,
        job: _WindowsJob | None = None,
        allow_posix_launch_helper: bool = False,
    ) -> bool:
        pid = context.process.get("pid")
        expected_created_at = context.process.get("os_process_created_at")
        expected_birth_token = context.process.get("os_process_birth_token")
        expected_group = context.process.get("process_group_id")
        expected_job = (
            self._windows_job_name(context)
            if os.name == "nt"
            else f"posix-session:{identity.pid}"
        )
        if identity.pid in self.protected_pids:
            return False
        if pid != identity.pid or receipt.get("pid") != identity.pid:
            return False
        if os.name == "nt":
            if expected_group != expected_job:
                return False
        elif expected_group != identity.pid:
            return False
        actual_time = _parse_timestamp(identity.created_at)
        stored_time = _parse_timestamp(expected_created_at)
        receipt_time = _parse_timestamp(receipt.get("os_process_created_at"))
        if actual_time is None or stored_time is None or receipt_time is None:
            return False
        if abs((actual_time - stored_time).total_seconds()) > 0.05 or abs(
            (actual_time - receipt_time).total_seconds()
        ) > 0.05:
            return False
        if (
            not isinstance(expected_birth_token, str)
            or expected_birth_token != identity.birth_token
            or receipt.get("os_process_birth_token") != identity.birth_token
        ):
            return False
        executable_matches = _path_key(identity.executable_path) == _path_key(
            str(context.process.get("executable_path") or "")
        )
        if (
            allow_posix_launch_helper
            and os.name != "nt"
            and _path_key(identity.executable_path) == _path_key(sys.executable)
        ):
            executable_matches = True
        if (
            not executable_matches
            or receipt.get("process_id") != context.process.get("process_id")
            or receipt.get("launch_nonce") != context.process.get("launch_nonce")
            or receipt.get("executable_path") != context.process.get("executable_path")
            or receipt.get("cwd") != context.process.get("cwd")
            or receipt.get("process_group_id") != expected_group
            or context.process.get("job_object_id") != expected_job
            or receipt.get("job_object_id") != expected_job
        ):
            return False
        if identity.cwd is not None and _path_key(identity.cwd) != _path_key(
            str(context.process["cwd"])
        ):
            return False
        if os.name == "nt":
            actual_job = job or (
                _WindowsJob.open(str(expected_job))
                if isinstance(expected_job, str)
                else None
            )
            if actual_job is None:
                return False
            try:
                return actual_job.contains(identity.pid)
            finally:
                if job is None:
                    actual_job.close()
        return (
            identity.process_group_id == expected_group
            and self._linux_managed_environment_matches(context, identity.pid)
        )

    @staticmethod
    def _linux_managed_environment_matches(
        context: _ProcessContext,
        pid: int,
    ) -> bool:
        """Authenticate a POSIX leader with its unguessable launch identity."""

        environ_path = Path("/proc") / str(pid) / "environ"
        try:
            environment = set(environ_path.read_bytes().split(b"\0"))
        except FileNotFoundError:
            return False
        except (PermissionError, OSError) as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed POSIX process environment cannot be verified",
                process_id=str(context.process.get("process_id") or pid),
            ) from exc
        expected = {
            f"NGINX_QA_MANAGED_HOST={context.lease['host']}".encode("utf-8"),
            f"NGINX_QA_MANAGED_PORT={context.lease['port']}".encode("utf-8"),
            (
                "NGINX_QA_MANAGED_PROCESS_ID="
                + str(context.process["process_id"])
            ).encode("utf-8"),
            (
                "NGINX_QA_MANAGED_RUNTIME_ROOT="
                + str(context.process["runtime_root"])
            ).encode("utf-8"),
            (
                "NGINX_QA_MANAGED_LAUNCH_NONCE="
                + str(context.process["launch_nonce"])
            ).encode("utf-8"),
        }
        return expected.issubset(environment)

    def _receipt(self, process_id: str) -> dict[str, Any]:
        path = self._pid_receipt_path(process_id)
        descriptor: int | None = None
        try:
            if os.name == "nt":
                descriptor = self._open_windows_verified_path(
                    path,
                    process_id,
                    purpose="PID receipt",
                    writable=False,
                    create=False,
                )
            else:
                flags = os.O_RDONLY
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                descriptor = os.open(path, flags)
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_size > _MAX_PID_RECEIPT_BYTES
            ):
                raise ValueError("managed PID receipt is not a bounded unique file")
            raw = os.read(descriptor, _MAX_PID_RECEIPT_BYTES + 1)
            if len(raw) > _MAX_PID_RECEIPT_BYTES:
                raise ValueError("managed PID receipt exceeds its size limit")
            value = json.loads(raw.decode("utf-8"))
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValueError,
            ManagedProcessSupervisorError,
        ) as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed PID receipt is missing or corrupt",
                process_id=process_id,
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if not isinstance(value, dict):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed PID receipt is invalid",
                process_id=process_id,
            )
        self._assert_receipt_envelope(value, process_id)
        return value

    @staticmethod
    def _assert_receipt_envelope(
        receipt: Mapping[str, Any], process_id: str
    ) -> None:
        required = {
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
        version = receipt.get("schema_version")
        phase = receipt.get("phase")
        deadline = receipt.get("startup_deadline_at")
        valid = (
            required.issubset(receipt)
            and type(version) is int
            and version == 1
            and phase in {"intent", "launch_gated", "launched"}
            and _parse_timestamp(receipt.get("created_at")) is not None
        )
        if phase in {"intent", "launch_gated"}:
            valid = valid and _parse_timestamp(deadline) is not None
        elif phase == "launched" and "startup_deadline_at" in receipt:
            valid = valid and _parse_timestamp(deadline) is not None
        if not valid:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed PID receipt has an unsupported or malformed envelope",
                process_id=process_id,
            )

    def _context_from_receipt(
        self,
        context: _ProcessContext,
        receipt: Mapping[str, Any] | None = None,
        *,
        allowed_phases: Iterable[str] = ("launched",),
        allow_posix_launch_helper: bool = False,
    ) -> tuple[_ProcessContext, ProcessIdentity | None, dict[str, Any]]:
        """Bind a durable PREPARED record to its exact launch receipt.

        A receipt is launch evidence even while the database row is still
        PREPARED.  Dynamic OS fields may therefore be absent from the row but
        must be copied only after all immutable receipt fields match it.
        """

        process_id = str(context.process["process_id"])
        receipt_value = dict(receipt or self._receipt(process_id))
        self._assert_receipt_envelope(receipt_value, process_id)
        if receipt_value.get("phase") not in frozenset(allowed_phases):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed PID receipt is not in launched phase",
                process_id=process_id,
            )
        expected = {
            "project_id": context.project_id,
            "sprint_id": context.sprint_id,
            "process_id": process_id,
            "assignment_id": context.process.get("assignment_id"),
            "launch_nonce": context.process.get("launch_nonce"),
            "executable_path": context.process.get("executable_path"),
            "cwd": context.process.get("cwd"),
            "port_lease_id": context.process.get("port_lease_id"),
        }
        if any(receipt_value.get(key) != value for key, value in expected.items()):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed PID receipt does not match its durable process attempt",
                process_id=process_id,
            )
        pid = receipt_value.get("pid")
        group_id = receipt_value.get("process_group_id")
        job_id = receipt_value.get("job_object_id")
        created_at = receipt_value.get("os_process_created_at")
        birth_token = receipt_value.get("os_process_birth_token")
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not isinstance(created_at, str)
            or not created_at
            or not isinstance(birth_token, str)
            or not birth_token
            or not isinstance(group_id, (int, str))
            or isinstance(group_id, bool)
            or not isinstance(job_id, str)
            or not job_id
        ):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed PID receipt has incomplete launch identity",
                process_id=process_id,
            )
        expected_job = (
            self._windows_job_name(context)
            if os.name == "nt"
            else f"posix-session:{pid}"
        )
        expected_group: int | str = expected_job if os.name == "nt" else pid
        if group_id != expected_group or job_id != expected_job:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed PID receipt names a non-deterministic process scope",
                process_id=process_id,
            )
        process = deepcopy(context.process)
        process.update(
            {
                "pid": pid,
                "os_process_created_at": created_at,
                "os_process_birth_token": birth_token,
                "process_group_id": group_id,
                "job_object_id": job_id,
            }
        )
        candidate = deepcopy(context)
        candidate.process = process
        identity = process_identity(pid)
        if identity is not None and not self._identity_matches(
            candidate,
            identity,
            receipt_value,
            allow_posix_launch_helper=allow_posix_launch_helper,
        ):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "PID reuse or PREPARED launch identity mismatch detected",
                process_id=process_id,
            )
        if identity is not None:
            self._assert_scope_safe(candidate)
        return candidate, identity, receipt_value

    def _linux_intent_candidates(
        self, context: _ProcessContext
    ) -> tuple[ProcessIdentity, ...]:
        if os.name == "nt":
            return ()
        proc_root = Path("/proc")
        try:
            entries = tuple(proc_root.iterdir())
        except OSError as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed launch intent cannot inspect /proc",
                process_id=str(context.process["process_id"]),
            ) from exc
        expected_environment = {
            os.fsencode(f"NGINX_QA_MANAGED_HOST={context.lease['host']}"),
            os.fsencode(f"NGINX_QA_MANAGED_PORT={context.lease['port']}"),
            os.fsencode(
                f"NGINX_QA_MANAGED_PROCESS_ID={context.process['process_id']}"
            ),
            os.fsencode(
                "NGINX_QA_MANAGED_RUNTIME_ROOT="
                f"{context.process['runtime_root']}"
            ),
            os.fsencode(
                "NGINX_QA_MANAGED_LAUNCH_NONCE="
                f"{context.process['launch_nonce']}"
            ),
        }
        command = context.process.get("command_redacted")
        limits = context.process.get("resource_limits")
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(item, str) for item in command)
            or not isinstance(limits, Mapping)
        ):
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed launch intent has invalid command evidence",
                process_id=str(context.process["process_id"]),
            )
        helper = Path(__file__).with_name("process_exec_helper.py").resolve(
            strict=True
        )
        expected_limits = os.fsencode(
            json.dumps(dict(limits), sort_keys=True, separators=(",", ":"))
        )
        expected_target = tuple(
            os.fsencode(value)
            for value in [
                str(context.process["executable_path"]),
                *command[1:],
            ]
        )
        candidates: list[ProcessIdentity] = []
        own_uid = os.geteuid() if hasattr(os, "geteuid") else None
        for entry in entries:
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            same_uid = True
            if own_uid is not None:
                try:
                    uid_line = next(
                        line
                        for line in (entry / "status").read_text(
                            encoding="ascii", errors="replace"
                        ).splitlines()
                        if line.startswith("Uid:")
                    )
                    same_uid = int(uid_line.split()[1]) == own_uid
                except (OSError, StopIteration, ValueError):
                    continue
            if not same_uid:
                continue
            try:
                environment = set((entry / "environ").read_bytes().split(b"\0"))
            except FileNotFoundError:
                continue
            except PermissionError as exc:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "same-user launch environment cannot be inspected",
                    process_id=str(context.process["process_id"]),
                ) from exc
            except OSError:
                continue
            if not expected_environment.issubset(environment):
                continue
            try:
                argv = (entry / "cmdline").read_bytes().split(b"\0")
                if argv and argv[-1] == b"":
                    argv.pop()
                if len(argv) != 4 + len(expected_target):
                    continue
                gate_fd_raw = argv[3]
                if not gate_fd_raw.isdigit() or int(gate_fd_raw) <= 0:
                    continue
                gate_target = os.readlink(entry / "fd" / os.fsdecode(gate_fd_raw))
            except FileNotFoundError:
                continue
            except PermissionError as exc:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "same-user launch command cannot be inspected",
                    process_id=str(context.process["process_id"]),
                ) from exc
            except (OSError, ValueError):
                continue
            if (
                _path_key(os.fsdecode(argv[0])) != _path_key(sys.executable)
                or _path_key(os.fsdecode(argv[1])) != _path_key(helper)
                or argv[2] != expected_limits
                or tuple(argv[4:]) != expected_target
                or not gate_target.startswith("pipe:[")
            ):
                continue
            identity = process_identity(pid)
            if (
                identity is not None
                and identity.pid not in self.protected_pids
                and identity.process_group_id == identity.pid
                and _path_key(identity.executable_path)
                == _path_key(sys.executable)
                and identity.cwd is not None
                and _path_key(identity.cwd) == _path_key(str(context.process["cwd"]))
            ):
                candidates.append(identity)
        return tuple(candidates)

    def _recover_launch_intent(
        self,
        context: _ProcessContext,
        receipt: Mapping[str, Any],
        *,
        create_restart: bool = True,
    ) -> SupervisionResult:
        """Recover the crash window after intent publication and before receipt."""

        process_id = str(context.process["process_id"])
        self._assert_receipt_envelope(receipt, process_id)
        expected = {
            "phase": "intent",
            "project_id": context.project_id,
            "sprint_id": context.sprint_id,
            "process_id": process_id,
            "assignment_id": context.process.get("assignment_id"),
            "launch_nonce": context.process.get("launch_nonce"),
            "executable_path": context.process.get("executable_path"),
            "cwd": context.process.get("cwd"),
            "port_lease_id": context.process.get("port_lease_id"),
        }
        if any(receipt.get(key) != value for key, value in expected.items()):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed launch intent does not match its process attempt",
                process_id=process_id,
            )
        startup_deadline_at = receipt.get("startup_deadline_at")
        if _parse_timestamp(startup_deadline_at) is None:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed launch intent has no durable startup deadline",
                process_id=process_id,
            )

        job: _WindowsJob | None = None
        try:
            if os.name == "nt":
                job_name = self._windows_job_name(context)
                if (
                    receipt.get("job_object_id") != job_name
                    or receipt.get("process_group_id") != job_name
                ):
                    raise ManagedProcessSupervisorError(
                        ORPHAN_PROCESS,
                        "managed launch intent has an invalid Windows Job identity",
                        process_id=process_id,
                    )
                job = _WindowsJob.open(job_name)
                member_pids = job.active_process_ids() if job is not None else frozenset()
                if len(member_pids) > 1:
                    raise ManagedProcessSupervisorError(
                        ORPHAN_PROCESS,
                        "managed pre-receipt Job contains multiple processes",
                        process_id=process_id,
                    )
                identities = tuple(
                    identity
                    for pid in member_pids
                    if (identity := process_identity(pid)) is not None
                )
                if member_pids and len(identities) != len(member_pids):
                    raise ManagedProcessSupervisorError(
                        ORPHAN_PROCESS,
                        "managed pre-receipt process identity is unavailable",
                        process_id=process_id,
                    )
            else:
                identities = self._linux_intent_candidates(context)

            if not identities:
                # No irreversible spawn side effect exists.  Removing the
                # intent turns PREPARED back into a provably never-launched
                # attempt; its immutable logs (if any) remain as evidence.
                try:
                    self._remove_pid_receipt(process_id)
                except FileNotFoundError:
                    pass
                terminal = self._terminalize(
                    context,
                    failed=True,
                    create_restart=create_restart,
                    terminal_reason=(
                        "failure" if create_restart else "operator_cancelled"
                    ),
                    claim_owner=self.claim_owner,
                    supervisor_fence=context.supervisor_fence,
                )
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    str(terminal.process["state"]),
                    "empty-launch-intent-reconciled",
                )
            if len(identities) != 1:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed launch intent matches multiple live processes",
                    process_id=process_id,
                )
            identity = identities[0]
            if (
                identity.pid in self.protected_pids
                or (
                    os.name == "nt"
                    and _path_key(identity.executable_path)
                    != _path_key(str(context.process["executable_path"]))
                )
                or (
                    os.name != "nt"
                    and _path_key(identity.executable_path)
                    != _path_key(sys.executable)
                )
                or (
                    identity.cwd is not None
                    and _path_key(identity.cwd)
                    != _path_key(str(context.process["cwd"]))
                )
            ):
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed launch intent discovered an invalid child identity",
                    process_id=process_id,
                )
            startup_deadline_at = self._bounded_startup_deadline(
                context,
                str(startup_deadline_at),
                identity=identity,
                intent_created_at=receipt.get("created_at"),
            )
            group_id: int | str = job.name if job is not None else identity.pid
            job_id = job.name if job is not None else f"posix-session:{identity.pid}"
            self._write_receipt(
                context,
                identity,
                group_id,
                job_id,
                phase="launch_gated",
                startup_deadline_at=str(startup_deadline_at),
            )
            candidate = deepcopy(context)
            candidate.process.update(
                {
                    "pid": identity.pid,
                    "process_group_id": group_id,
                    "job_object_id": job_id,
                    "os_process_created_at": identity.created_at,
                    "os_process_birth_token": identity.birth_token,
                }
            )
            if os.name != "nt":
                # An intent-only POSIX launch is still the exec helper behind
                # its one-way pipe gate.  The crashed launcher was the sole
                # writer, so a replacement supervisor cannot safely release
                # that gate and must not wait until the health deadline for a
                # listener that can never appear.  Stop the exact authenticated
                # session, prove it empty, and let terminalization create the
                # normal immutable restart attempt when policy permits.
                self._terminate_context(
                    candidate,
                    allow_posix_launch_helper=True,
                )
                terminal = self._terminalize(
                    candidate,
                    failed=True,
                    create_restart=create_restart,
                    terminal_reason=(
                        "failure" if create_restart else "operator_cancelled"
                    ),
                    claim_owner=self.claim_owner,
                    supervisor_fence=context.supervisor_fence,
                )
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    str(terminal.process["state"]),
                    (
                        "dead-launch-intent-reconciled"
                        if create_restart
                        else "cancelled-recovered-intent"
                    ),
                )
            if not create_restart:
                # Operator cancellation must never start a process merely to
                # make it stoppable.  The gated receipt supplies the exact
                # Job/group identity needed to kill the still-suspended child
                # (or the POSIX exec helper) and prove the whole scope dead.
                self._terminate_context(
                    candidate,
                    allow_posix_launch_helper=True,
                )
                terminal = self._terminalize(
                    candidate,
                    failed=True,
                    create_restart=False,
                    terminal_reason="operator_cancelled",
                    claim_owner=self.claim_owner,
                    supervisor_fence=context.supervisor_fence,
                )
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    str(terminal.process["state"]),
                    "cancelled-recovered-intent",
                )
            expired = self._terminalize_expired_gated_launch(
                candidate,
                startup_deadline_at=str(startup_deadline_at),
                create_restart=create_restart,
                action="expired-launch-intent-reconciled",
            )
            if expired is not None:
                return expired
            recovery_pins: tuple[_PinnedPath, ...] = ()
            try:
                if job is not None:
                    self._assert_scope_safe(candidate, job=job)
                    try:
                        recovery_pins = self._resume_windows_with_pins(
                            candidate,
                            identity.pid,
                            startup_deadline_at=str(startup_deadline_at),
                        )
                    except ManagedProcessSupervisorError as exc:
                        if exc.code != PROCESS_HEALTH_FAILED:
                            raise
                        expired = self._terminalize_expired_gated_launch(
                            candidate,
                            startup_deadline_at=str(startup_deadline_at),
                            create_restart=create_restart,
                            action="expired-launch-intent-reconciled",
                        )
                        if expired is None:
                            raise
                        return expired
                self._write_receipt(
                    context,
                    identity,
                    group_id,
                    job_id,
                    startup_deadline_at=str(startup_deadline_at),
                )
                return self._recover_prepared(
                    context, create_restart=create_restart
                )
            finally:
                self._close_launch_pins(recovery_pins)
        finally:
            if job is not None:
                job.close()

    def _recover_gated_receipt(
        self,
        context: _ProcessContext,
        *,
        create_restart: bool = True,
    ) -> SupervisionResult:
        """Idempotently resume a durably identified contained child."""

        process_id = str(context.process["process_id"])
        candidate, identity, receipt = self._context_from_receipt(
            context,
            allowed_phases=("launch_gated",),
            allow_posix_launch_helper=True,
        )
        startup_deadline_at = receipt.get("startup_deadline_at")
        if _parse_timestamp(startup_deadline_at) is None:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "gated managed launch has no durable startup deadline",
                process_id=process_id,
        )
        if identity is None:
            if not self._confirmed_dead(candidate):
                # The exact leader may have exited after creating descendants
                # in its immutable Job/session.  Eliminate that whole verified
                # scope before releasing the lease or creating a restart.
                self._terminate_context(
                    candidate,
                    allow_posix_launch_helper=True,
                )
            if not self._confirmed_dead(candidate):
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "gated managed child scope could not be proven dead",
                    process_id=process_id,
                )
            terminal = self._terminalize(
                candidate,
                failed=True,
                create_restart=create_restart,
                terminal_reason=(
                    "failure" if create_restart else "operator_cancelled"
                ),
                claim_owner=self.claim_owner,
                supervisor_fence=context.supervisor_fence,
            )
            return SupervisionResult(
                context.project_id,
                context.sprint_id,
                process_id,
                str(terminal.process["state"]),
                "dead-gated-launch-reconciled",
            )
        bounded_deadline = self._bounded_startup_deadline(
            candidate,
            str(startup_deadline_at),
            identity=identity,
        )
        if bounded_deadline != startup_deadline_at:
            self._write_receipt(
                context,
                identity,
                receipt["process_group_id"],
                str(receipt["job_object_id"]),
                phase="launch_gated",
                startup_deadline_at=bounded_deadline,
            )
        startup_deadline_at = bounded_deadline
        expired = self._terminalize_expired_gated_launch(
            candidate,
            startup_deadline_at=str(startup_deadline_at),
            create_restart=create_restart,
            action="expired-gated-launch-reconciled",
        )
        if expired is not None:
            return expired
        if os.name == "nt":
            self._assert_scope_safe(candidate)
            try:
                recovery_pins = self._resume_windows_with_pins(
                    candidate,
                    identity.pid,
                    startup_deadline_at=str(startup_deadline_at),
                )
            except ManagedProcessSupervisorError as exc:
                if exc.code != PROCESS_HEALTH_FAILED:
                    raise
                expired = self._terminalize_expired_gated_launch(
                    candidate,
                    startup_deadline_at=str(startup_deadline_at),
                    create_restart=create_restart,
                    action="expired-gated-launch-reconciled",
                )
                if expired is None:
                    raise
                return expired
            try:
                self._write_receipt(
                    context,
                    identity,
                    receipt["process_group_id"],
                    str(receipt["job_object_id"]),
                    startup_deadline_at=str(startup_deadline_at),
                )
                return self._recover_prepared(
                    context,
                    create_restart=create_restart,
                )
            finally:
                self._close_launch_pins(recovery_pins)
        else:
            deadline = _parse_timestamp(startup_deadline_at)
            target = _path_key(str(context.process["executable_path"]))
            while (
                deadline is not None
                and _utc_now(self.clock) < deadline
                and _path_key(identity.executable_path) != target
            ):
                time.sleep(0.025)
                refreshed = process_identity(identity.pid)
                if refreshed is None or refreshed.birth_token != identity.birth_token:
                    identity = None
                    break
                identity = refreshed
            if identity is None:
                if not self._confirmed_dead(candidate):
                    self._terminate_context(
                        candidate,
                        allow_posix_launch_helper=True,
                    )
                terminal = self._terminalize(
                    candidate,
                    failed=True,
                    create_restart=create_restart,
                    terminal_reason=(
                        "failure" if create_restart else "operator_cancelled"
                    ),
                    claim_owner=self.claim_owner,
                    supervisor_fence=context.supervisor_fence,
                )
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    str(terminal.process["state"]),
                    "dead-gated-launch-reconciled",
                )
            if _path_key(identity.executable_path) != target:
                self._terminate_context(
                    candidate,
                    allow_posix_launch_helper=True,
                )
                terminal = self._terminalize(
                    candidate,
                    failed=True,
                    create_restart=create_restart,
                    terminal_reason=(
                        "failure" if create_restart else "operator_cancelled"
                    ),
                    claim_owner=self.claim_owner,
                    supervisor_fence=context.supervisor_fence,
                )
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    str(terminal.process["state"]),
                    "gated-launch-timeout",
                )
        self._write_receipt(
            context,
            identity,
            receipt["process_group_id"],
            str(receipt["job_object_id"]),
            startup_deadline_at=str(startup_deadline_at),
        )
        return self._recover_prepared(
            context,
            create_restart=create_restart,
        )

    def _assert_scope_safe(
        self,
        context: _ProcessContext,
        *,
        owned: _OwnedProcess | None = None,
        job: _WindowsJob | None = None,
    ) -> None:
        """Reject any process scope containing nginx-qa or one of its ancestors."""

        process_id = str(context.process["process_id"])
        if os.name == "nt":
            actual_job = job or (owned.job if owned is not None else None)
            close_job = False
            if actual_job is None:
                job_id = context.process.get("job_object_id")
                actual_job = (
                    _WindowsJob.open(str(job_id)) if isinstance(job_id, str) else None
                )
                close_job = actual_job is not None
            if actual_job is None:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed Windows Job scope cannot be verified",
                    process_id=process_id,
                )
            try:
                members = actual_job.active_process_ids()
                protected = members.intersection(self.protected_pids)
                if protected:
                    raise ManagedProcessSupervisorError(
                        ORPHAN_PROCESS,
                        "managed Windows Job contains a protected service PID",
                        process_id=process_id,
                    )
            finally:
                if close_job:
                    actual_job.close()
            return

        group = context.process.get("process_group_id")
        if not isinstance(group, int) and owned is not None:
            group = owned.process.pid
        if not isinstance(group, int) or group <= 0:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed POSIX process group cannot be verified",
                process_id=process_id,
            )
        for protected_pid in self.protected_pids:
            try:
                if os.getpgid(protected_pid) == group:
                    raise ManagedProcessSupervisorError(
                        ORPHAN_PROCESS,
                        "managed POSIX process group contains a protected service PID",
                        process_id=process_id,
                    )
            except ProcessLookupError:
                continue
            except PermissionError as exc:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "protected process-group membership cannot be verified",
                    process_id=process_id,
                ) from exc

    def _scope_contains_pid(
        self, context: _ProcessContext, pid: int, *, job: _WindowsJob | None = None
    ) -> bool:
        if pid in self.protected_pids:
            return False
        if os.name == "nt":
            actual_job = job or (
                _WindowsJob.open(str(context.process.get("job_object_id")))
                if isinstance(context.process.get("job_object_id"), str)
                else None
            )
            if actual_job is None:
                return False
            try:
                return actual_job.contains(pid)
            finally:
                if job is None:
                    actual_job.close()
        expected_group = context.process.get("process_group_id")
        try:
            return isinstance(expected_group, int) and os.getpgid(pid) == expected_group
        except OSError:
            return False

    def _port_owned(self, context: _ProcessContext, *, job: _WindowsJob | None = None) -> bool:
        owners = port_owner_pids(str(context.lease["host"]), int(context.lease["port"]))
        return bool(owners) and all(
            self._scope_contains_pid(context, owner, job=job) for owner in owners
        )

    def _wait_for_port_owner(
        self,
        context: _ProcessContext,
        process: subprocess.Popen[bytes],
        *,
        job: _WindowsJob | None,
        startup_deadline_at: str,
    ) -> bool:
        deadline = _parse_timestamp(startup_deadline_at)
        if deadline is None:
            return False
        while _utc_now(self.clock) < deadline:
            if process.poll() is not None:
                return False
            provisional = deepcopy(context.process)
            provisional.update(
                {
                    "pid": process.pid,
                    "process_group_id": job.name if job is not None else process.pid,
                    "job_object_id": job.name if job is not None else f"posix-session:{process.pid}",
                }
            )
            candidate = deepcopy(context)
            candidate.process = provisional
            if self._port_owned(candidate, job=job):
                return True
            time.sleep(0.025)
        return False

    def _wait_for_recovered_port_owner(
        self, context: _ProcessContext, *, startup_deadline_at: str
    ) -> bool:
        deadline = _parse_timestamp(startup_deadline_at)
        if deadline is None:
            return False
        pid = context.process.get("pid")
        while _utc_now(self.clock) < deadline:
            if not isinstance(pid, int) or process_identity(pid) is None:
                return False
            if self._port_owned(context):
                return True
            time.sleep(0.025)
        return False

    def _health_ok(self, context: _ProcessContext, *, timeout: float = 1.0) -> bool:
        if not self._port_owned(context):
            return False
        endpoint = context.process.get("health_endpoint")
        if not isinstance(endpoint, Mapping):
            return False
        host = endpoint.get("host")
        port = endpoint.get("port")
        path = endpoint.get("path")
        if (
            host != "127.0.0.1"
            or not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
            or not managed_health_path_valid(path)
        ):
            return False
        deadline = time.monotonic() + max(timeout, 0.1)
        try:
            # Parse only the bounded response header.  Recomputing the socket
            # timeout before every blocking operation makes ``timeout`` an
            # absolute probe deadline even for a child that drip-feeds bytes.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                probe.settimeout(remaining)
                probe.connect((host, port))
                request = (
                    f"GET {path} HTTP/1.1\r\n"
                    f"Host: {host}:{port}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("ascii")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                probe.settimeout(remaining)
                probe.sendall(request)
                header = bytearray()
                while b"\r\n\r\n" not in header:
                    if len(header) >= 16 * 1024:
                        return False
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    probe.settimeout(remaining)
                    chunk = probe.recv(min(4096, (16 * 1024) - len(header)))
                    if not chunk:
                        return False
                    header.extend(chunk)
            status_line = bytes(header).split(b"\r\n", 1)[0]
            fields = status_line.split(b" ", 2)
            return (
                len(fields) >= 2
                and fields[0] in {b"HTTP/1.0", b"HTTP/1.1"}
                and fields[1] == b"200"
                and self._port_owned(context)
            )
        except (OSError, TimeoutError, UnicodeEncodeError, ValueError):
            return False

    def _wait_for_health(self, context: _ProcessContext) -> bool:
        durable_deadline = _parse_timestamp(
            context.process.get("startup_deadline_at")
        )
        if durable_deadline is None:
            durable_deadline = _utc_now(self.clock) + timedelta(
                seconds=self.health_timeout_seconds
            )
        started_at = self._effective_started_at(context.process)
        limits = context.process.get("resource_limits")
        if started_at is not None and isinstance(limits, Mapping):
            wall = limits.get("wall_time_seconds")
            if isinstance(wall, int):
                durable_deadline = min(
                    durable_deadline,
                    started_at + timedelta(seconds=wall),
                )
        while _utc_now(self.clock) < durable_deadline:
            identity = process_identity(int(context.process["pid"]))
            if identity is None:
                return False
            if self._health_ok(context, timeout=min(1.0, self.health_timeout_seconds)):
                return True
            time.sleep(0.05)
        return False

    def _write_launch_intent(self, context: _ProcessContext) -> dict[str, Any]:
        """Publish immutable intent before logs, port handoff, or spawn."""

        job_id = self._windows_job_name(context) if os.name == "nt" else None
        now = _utc_now(self.clock)
        startup_budget_seconds = min(
            self.health_timeout_seconds,
            float(self._wall_time_seconds(context)),
        )
        receipt = {
            "schema_version": 1,
            "phase": "intent",
            "project_id": context.project_id,
            "sprint_id": context.sprint_id,
            "process_id": context.process["process_id"],
            "assignment_id": context.process["assignment_id"],
            "launch_nonce": context.process["launch_nonce"],
            "pid": None,
            "os_process_created_at": None,
            "os_process_birth_token": None,
            "executable_path": context.process["executable_path"],
            "cwd": context.process["cwd"],
            "process_group_id": job_id,
            "job_object_id": job_id,
            "port_lease_id": context.process["port_lease_id"],
            "created_at": now.isoformat(),
            "startup_deadline_at": (
                now + timedelta(seconds=startup_budget_seconds)
            ).isoformat(),
        }
        self._write_pid_receipt(str(context.process["process_id"]), receipt)
        return receipt

    def _write_receipt(
        self,
        context: _ProcessContext,
        identity: ProcessIdentity,
        group_id: int | str,
        job_id: str,
        *,
        phase: str = "launched",
        startup_deadline_at: str,
    ) -> dict[str, Any]:
        receipt = {
            "schema_version": 1,
            "phase": phase,
            "project_id": context.project_id,
            "sprint_id": context.sprint_id,
            "process_id": context.process["process_id"],
            "assignment_id": context.process["assignment_id"],
            "launch_nonce": context.process["launch_nonce"],
            "pid": identity.pid,
            "os_process_created_at": identity.created_at,
            "os_process_birth_token": identity.birth_token,
            "executable_path": context.process["executable_path"],
            "cwd": context.process["cwd"],
            "process_group_id": group_id,
            "job_object_id": job_id,
            "port_lease_id": context.process["port_lease_id"],
            "created_at": _timestamp(self.clock),
            "startup_deadline_at": startup_deadline_at,
        }
        self._write_pid_receipt(str(context.process["process_id"]), receipt)
        return receipt

    def _starting_records(
        self,
        context: _ProcessContext,
        identity: ProcessIdentity,
        group_id: int | str,
        job_id: str,
        startup_deadline_at: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        process = deepcopy(context.process)
        process.update(
            {
                "state": "STARTING",
                "pid": identity.pid,
                "process_group_id": group_id,
                "job_object_id": job_id,
                "os_process_created_at": identity.created_at,
                "os_process_birth_token": identity.birth_token,
                # The wall-clock budget begins when the OS created the
                # contained process, including time spent suspended/gated.
                "started_at": identity.created_at,
                "startup_deadline_at": startup_deadline_at,
                "stopped_at": None,
                "failed_at": None,
            }
        )
        lease = deepcopy(context.lease)
        lease.update(
            {
                "status": "bound",
                "process_id": process["process_id"],
                "bind_verified": True,
                "released_at": None,
            }
        )
        return process, lease

    def _mark_healthy(self, context: _ProcessContext) -> _ProcessContext:
        process = deepcopy(context.process)
        process["state"] = "HEALTHY"
        return self._repository.transition(
            context,
            process,
            context.lease,
            expected_states={"STARTING"},
        )

    def launch_prepared(
        self, project_id: str, sprint_id: str, process_id: str
    ) -> SupervisionResult:
        with self._operation(), self._process_action_guard(process_id):
            return self._launch_prepared_untracked(
                project_id, sprint_id, process_id
            )

    def _launch_prepared_untracked(
        self, project_id: str, sprint_id: str, process_id: str
    ) -> SupervisionResult:
        receipt_path = self._pid_receipt_path(process_id)
        if not receipt_path.exists():
            preclaim = self._repository.load(project_id, sprint_id, process_id)
            self._validate_config(preclaim)
            if preclaim.process.get("state") == "PREPARED":
                reservation = self.port_reservations.reservation(
                    self.store.database_path,
                    str(preclaim.lease["lease_id"]),
                )
                if reservation is None:
                    try:
                        self.port_reservations.acquire_durable_many(
                            self.store.database_path,
                            [preclaim.lease],
                        )
                    except ManagedPortReservationError:
                        # Another service instance may own the reservation
                        # socket.  It must be the only instance allowed to
                        # claim and hand off this PREPARED attempt.
                        return SupervisionResult(
                            project_id,
                            sprint_id,
                            process_id,
                            "PREPARED",
                            "reservation-not-owned",
                        )
        claim = self._repository.claim_prepared(
            project_id,
            sprint_id,
            process_id,
            owner=self.claim_owner,
            ttl_seconds=max(
                (self.health_timeout_seconds * 2)
                + (self.stop_timeout_seconds * 2)
                + 10.0,
                30.0,
            ),
        )
        if claim is None:
            current = self._repository.load(project_id, sprint_id, process_id)
            return SupervisionResult(
                project_id,
                sprint_id,
                process_id,
                str(current.process["state"]),
                "not-claimed",
            )
        self._validate_config(claim)
        if receipt_path.exists():
            receipt = self._receipt(process_id)
            if receipt.get("phase") == "intent":
                return self._recover_launch_intent(claim, receipt)
            if receipt.get("phase") == "launch_gated":
                return self._recover_gated_receipt(claim)
            return self._recover_prepared(claim)
        if os.name != "nt":
            # Reject a fresh unsupported launch before publishing intent,
            # creating logs, or yielding its durable port reservation.  Old
            # receipts are still reconciled by the branches above.
            self._repository.release_claim(
                process_id,
                owner=self.claim_owner,
                supervisor_fence=claim.supervisor_fence,
            )
            raise ManagedProcessSupervisorError(
                RESOURCE_LIMIT_UNSUPPORTED,
                "assignment-wide hard resource limits are unavailable",
                process_id=process_id,
            )
        self._validate_workspace_and_executable(claim)
        stdout_stream = None
        stderr_stream = None
        handoff: ManagedPortHandoffToken | None = None
        owned: _OwnedProcess | None = None
        launch_intent: dict[str, Any] | None = None
        path_pins: tuple[_PinnedPath, ...] = ()
        try:
            def publish_intent() -> None:
                nonlocal launch_intent
                launch_intent = self._write_launch_intent(claim)
                self._fault("after_launch_intent", claim)

            stdout_stream, stderr_stream = self._prepare_paths(
                claim,
                before_logs=publish_intent,
            )
            path_pins = self._pin_windows_launch_paths(claim)
            reservation = self.port_reservations.reservation(
                self.store.database_path, str(claim.lease["lease_id"])
            )
            if reservation is None:
                # This is a single missing handoff holder, not whole-database
                # restart recovery.  Other assignments may already have
                # reservations in the shared registry.
                self.port_reservations.acquire_durable_many(
                    self.store.database_path, [claim.lease]
                )
            handoff = self.port_reservations.begin_handoff(
                self.store.database_path, str(claim.lease["lease_id"])
            )
            process, job, group_id, suspended, launch_gate = self._spawn(
                claim, stdout_stream, stderr_stream
            )
            owned = _OwnedProcess(
                process,
                stdout_stream,
                stderr_stream,
                job,
                launch_gate,
                path_pins,
            )
            path_pins = ()
            with self._owned_lock:
                self._owned[process_id] = owned
            self._fault("after_spawn", claim)
            identity = None
            deadline = time.monotonic() + min(self.health_timeout_seconds, 2.0)
            while identity is None and time.monotonic() < deadline:
                candidate_identity = process_identity(process.pid)
                if (
                    candidate_identity is not None
                    and candidate_identity.pid not in self.protected_pids
                    and (
                        launch_gate is not None
                        or _path_key(candidate_identity.executable_path)
                        == _path_key(str(claim.process["executable_path"]))
                    )
                    and (
                        candidate_identity.cwd is None
                        or _path_key(candidate_identity.cwd)
                        == _path_key(str(claim.process["cwd"]))
                    )
                    and (
                        os.name == "nt"
                        or candidate_identity.process_group_id == process.pid
                    )
                ):
                    identity = candidate_identity
                    break
                if process.poll() is not None:
                    break
                time.sleep(0.01)
            if identity is None or identity.pid in self.protected_pids:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "spawned child identity could not be verified",
                    process_id=process_id,
                )
            job_id = job.name if job is not None else f"posix-session:{group_id}"
            startup_deadline_at = (
                launch_intent.get("startup_deadline_at")
                if launch_intent is not None
                else None
            )
            if _parse_timestamp(startup_deadline_at) is None:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed launch lost its durable startup deadline",
                    process_id=process_id,
                )
            assert launch_intent is not None
            startup_deadline_at = self._bounded_startup_deadline(
                claim,
                str(startup_deadline_at),
                identity=identity,
                intent_created_at=launch_intent.get("created_at"),
            )
            durable_deadline = _parse_timestamp(startup_deadline_at)
            assert durable_deadline is not None
            self._write_receipt(
                claim,
                identity,
                group_id,
                job_id,
                phase="launch_gated",
                startup_deadline_at=str(startup_deadline_at),
            )
            self._fault("after_gated_receipt", claim)
            self._revalidate_windows_launch_pins(
                owned.path_pins,
                process_id=process_id,
            )
            if launch_gate is not None:
                if _utc_now(self.clock) >= durable_deadline:
                    raise ManagedProcessSupervisorError(
                        PROCESS_HEALTH_FAILED,
                        "managed launch expired before gate release",
                        process_id=process_id,
                    )
                try:
                    if os.write(launch_gate, b"G") != 1:
                        raise OSError("short managed launch-gate write")
                finally:
                    os.close(launch_gate)
                    launch_gate = None
                    owned.launch_gate = None
            elif suspended:
                if _utc_now(self.clock) >= durable_deadline:
                    raise ManagedProcessSupervisorError(
                        PROCESS_HEALTH_FAILED,
                        "managed launch expired before resume",
                        process_id=process_id,
                    )
                _WindowsJob.resume(process)
            # All launch paths were identity-checked immediately before the
            # gate opened.  Their shared no-delete pins are no longer needed.
            self._release_launch_pins(owned)
            self._fault("after_gate_release", claim)
            launched_identity = None
            identity_deadline = time.monotonic() + self.health_timeout_seconds
            while (
                time.monotonic() < identity_deadline
                and _utc_now(self.clock) < durable_deadline
            ):
                current_identity = process_identity(process.pid)
                if (
                    current_identity is not None
                    and current_identity.birth_token == identity.birth_token
                    and _path_key(current_identity.executable_path)
                    == _path_key(str(claim.process["executable_path"]))
                ):
                    launched_identity = current_identity
                    break
                if process.poll() is not None:
                    break
                time.sleep(0.01)
            if launched_identity is None:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "gated child did not enter its exact managed executable",
                    process_id=process_id,
                )
            identity = launched_identity
            self._write_receipt(
                claim,
                identity,
                group_id,
                job_id,
                startup_deadline_at=str(startup_deadline_at),
            )
            self._fault("after_launch_receipt", claim)
            if not self._wait_for_port_owner(
                claim,
                process,
                job=job,
                startup_deadline_at=str(startup_deadline_at),
            ):
                raise ManagedProcessSupervisorError(
                    PROCESS_HEALTH_FAILED,
                    "managed child did not bind its owned port",
                    process_id=process_id,
                )
            self._fault("after_bind_verification", claim)
            starting_process, bound_lease = self._starting_records(
                claim,
                identity,
                group_id,
                job_id,
                str(startup_deadline_at),
            )
            starting = self._repository.transition(
                claim,
                starting_process,
                bound_lease,
                expected_states={"PREPARED"},
                claim_owner=self.claim_owner,
                supervisor_fence=claim.supervisor_fence,
            )
            self._fault("after_starting_commit", starting)
            self.port_reservations.complete_handoff(handoff)
            handoff = None
            if not self._wait_for_health(starting):
                raise ManagedProcessSupervisorError(
                    PROCESS_HEALTH_FAILED,
                    "managed child health check did not succeed",
                    process_id=process_id,
                )
            healthy = self._mark_healthy(starting)
            return SupervisionResult(
                project_id, sprint_id, process_id, "HEALTHY", "launched"
            )
        except Exception as exc:
            if owned is not None:
                self._terminate_owned(process_id, owned, context=claim)
            else:
                for pin in reversed(path_pins):
                    os.close(pin.descriptor)
                path_pins = ()
            if handoff is not None:
                try:
                    self.port_reservations.reacquire_handoff(handoff)
                except ManagedPortReservationError:
                    pass
            try:
                current = self._repository.load(project_id, sprint_id, process_id)
                if current.process.get("state") in {"PREPARED", "STARTING", "HEALTHY"}:
                    death_context = current
                    if current.process.get("state") == "PREPARED" and receipt_path.exists():
                        try:
                            phase = self._receipt(process_id).get("phase")
                            if phase in {"launch_gated", "launched"}:
                                death_context, _identity, _receipt = (
                                    self._context_from_receipt(
                                        current,
                                        allowed_phases=("launch_gated", "launched"),
                                    )
                                )
                        except ManagedProcessSupervisorError:
                            death_context = current
                    if self._confirmed_dead(death_context, owned=owned):
                        self._terminalize(
                            death_context,
                            failed=True,
                            claim_owner=(
                                self.claim_owner
                                if current.process.get("state") == "PREPARED"
                                else None
                            ),
                            supervisor_fence=(
                                claim.supervisor_fence
                                if current.process.get("state") == "PREPARED"
                                else None
                            ),
                        )
                    else:
                        self._repository.release_claim(
                            process_id,
                            owner=self.claim_owner,
                            supervisor_fence=claim.supervisor_fence,
                        )
                        raise ManagedProcessSupervisorError(
                            ORPHAN_PROCESS,
                            "failed launch left an unverified live process group",
                            process_id=process_id,
                        ) from exc
            finally:
                if owned is None:
                    for stream in (stdout_stream, stderr_stream):
                        if stream is not None:
                            stream.close()
            if isinstance(exc, ManagedProcessSupervisorError):
                raise
            raise ManagedProcessSupervisorError(
                PROCESS_LAUNCH_FAILED,
                "managed child launch failed",
                process_id=process_id,
            ) from exc

    def _recover_prepared(
        self, context: _ProcessContext, *, create_restart: bool = True
    ) -> SupervisionResult:
        process_id = str(context.process["process_id"])
        candidate, identity, receipt = self._context_from_receipt(context)
        startup_deadline_at = receipt.get("startup_deadline_at")
        receipt_identity = identity or ProcessIdentity(
            pid=int(candidate.process["pid"]),
            created_at=str(candidate.process["os_process_created_at"]),
            birth_token=str(candidate.process["os_process_birth_token"]),
            executable_path=str(candidate.process["executable_path"]),
            cwd=str(candidate.process["cwd"]),
            process_group_id=candidate.process.get("process_group_id"),
        )
        if _parse_timestamp(startup_deadline_at) is None:
            # Upgrade a pre-deadline v1 receipt once.  The replacement is
            # durable, so subsequent recoveries can never extend the window.
            startup_deadline_at = (
                _utc_now(self.clock)
                + timedelta(seconds=self.health_timeout_seconds)
            ).isoformat()
        bounded_deadline = self._bounded_startup_deadline(
            candidate,
            str(startup_deadline_at),
            identity=receipt_identity,
        )
        if bounded_deadline != receipt.get("startup_deadline_at"):
            self._write_receipt(
                context,
                receipt_identity,
                receipt["process_group_id"],
                str(receipt["job_object_id"]),
                startup_deadline_at=bounded_deadline,
            )
        startup_deadline_at = bounded_deadline
        if identity is None:
            if not self._confirmed_dead(candidate):
                self._terminate_context(candidate)
            if not self._confirmed_dead(candidate):
                self._repository.release_claim(
                    process_id,
                    owner=self.claim_owner,
                    supervisor_fence=context.supervisor_fence,
                )
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "PREPARED process scope could not be proven dead",
                    process_id=process_id,
                )
            terminal = self._terminalize(
                candidate,
                failed=True,
                create_restart=create_restart,
                terminal_reason=(
                    "failure" if create_restart else "operator_cancelled"
                ),
                claim_owner=self.claim_owner,
                supervisor_fence=context.supervisor_fence,
            )
            return SupervisionResult(
                context.project_id,
                context.sprint_id,
                process_id,
                str(terminal.process["state"]),
                "dead-prepared-reconciled",
            )
        if not self._wait_for_recovered_port_owner(
            candidate,
            startup_deadline_at=str(startup_deadline_at),
        ):
            self._terminate_context(candidate)
            terminal = self._terminalize(
                candidate,
                failed=True,
                create_restart=create_restart,
                terminal_reason=(
                    "failure" if create_restart else "operator_cancelled"
                ),
                claim_owner=self.claim_owner,
                supervisor_fence=context.supervisor_fence,
            )
            return SupervisionResult(
                context.project_id,
                context.sprint_id,
                process_id,
                str(terminal.process["state"]),
                "recovered-bind-timeout",
            )
        starting_process, bound_lease = self._starting_records(
            candidate,
            identity,
            receipt["process_group_id"],
            str(receipt["job_object_id"]),
            str(startup_deadline_at),
        )
        starting = self._repository.transition(
            context,
            starting_process,
            bound_lease,
            expected_states={"PREPARED"},
            claim_owner=self.claim_owner,
            supervisor_fence=context.supervisor_fence,
        )
        reservation = self.port_reservations.reservation(
            self.store.database_path, str(context.lease["lease_id"])
        )
        if reservation is not None and reservation.state == "reserved":
            # A listener and a held reservation cannot coexist.  This catches a
            # corrupt in-process recovery registry before it can be marked bound.
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "recovered child conflicts with a held reservation socket",
                process_id=process_id,
            )
        if self._wait_for_health(starting):
            self._mark_healthy(starting)
            return SupervisionResult(
                context.project_id,
                context.sprint_id,
                process_id,
                "HEALTHY",
                "recovered-prepared",
            )
        self._terminate_context(starting)
        terminal = self._terminalize(
            starting, failed=True, create_restart=create_restart
        )
        return SupervisionResult(
            context.project_id,
            context.sprint_id,
            process_id,
            str(terminal.process["state"]),
            "recovered-health-failed",
        )

    def _record_identity(
        self,
        context: _ProcessContext,
        *,
        allow_posix_launch_helper: bool = False,
    ) -> tuple[ProcessIdentity, dict[str, Any]]:
        process_id = str(context.process["process_id"])
        pid = context.process.get("pid")
        if not isinstance(pid, int) or isinstance(pid, bool):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "live managed process has no PID",
                process_id=process_id,
            )
        if pid in self.protected_pids:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "live service PID or ancestor cannot be selected as a managed child",
                process_id=process_id,
            )
        identity = process_identity(pid)
        if identity is None:
            raise ProcessLookupError(pid)
        receipt = self._receipt(process_id)
        with self._owned_lock:
            owned = self._owned.get(process_id)
        if not self._identity_matches(
            context,
            identity,
            receipt,
            job=owned.job if owned is not None else None,
            allow_posix_launch_helper=allow_posix_launch_helper,
        ):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "PID reuse or managed process identity mismatch detected",
                process_id=process_id,
            )
        self._assert_scope_safe(context, owned=owned)
        return identity, receipt

    def _confirmed_dead(
        self, context: _ProcessContext, *, owned: _OwnedProcess | None = None
    ) -> bool:
        process_id = str(context.process["process_id"])
        if owned is None:
            with self._owned_lock:
                owned = self._owned.get(process_id)
        if owned is not None:
            # poll()/wait() reaps an exited owned leader on POSIX and removes
            # the false-live zombie that killpg(group, 0) alone cannot detect.
            if owned.process.poll() is not None:
                try:
                    owned.process.wait(timeout=0)
                except (subprocess.SubprocessError, OSError):
                    return False
        pid = context.process.get("pid")
        if not isinstance(pid, int):
            pid = owned.process.pid if owned is not None else None
        if not isinstance(pid, int):
            return not self._pid_receipt_path(process_id).exists()
        if pid in self.protected_pids:
            return False
        if process_identity(pid) is not None:
            return False
        group = context.process.get("process_group_id")
        if os.name == "nt":
            expected_job = self._windows_job_name(context)
            if owned is not None and owned.job is not None:
                # Before the launch receipt is durable, PREPARED legitimately
                # has no PID/Job fields.  The in-memory Job is nevertheless
                # exact: _spawn created it under the deterministic name while
                # this process action lock and claim fence were held.
                if owned.job.name != expected_job:
                    return False
                if context.process.get("job_object_id") not in {
                    None,
                    expected_job,
                } or group not in {None, expected_job}:
                    return False
                job = owned.job
            else:
                if (
                    context.process.get("job_object_id") != expected_job
                    or group != expected_job
                ):
                    return False
                job = _WindowsJob.open(expected_job)
            if job is None:
                # A Job persists after its last handle closes while any
                # associated process remains.  With a globally visible,
                # deterministic name and an already-dead exact leader,
                # absence therefore proves that the whole scope is gone.
                return True
            try:
                members = job.active_process_ids()
                return not members
            except ManagedProcessSupervisorError:
                return False
            finally:
                if owned is None or owned.job is None:
                    job.close()
        if not isinstance(group, int):
            return False
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        return False

    def _terminate_owned(
        self,
        process_id: str,
        owned: _OwnedProcess,
        *,
        context: _ProcessContext,
    ) -> None:
        self._assert_scope_safe(context, owned=owned)
        try:
            if owned.job is not None:
                owned.job.terminate()
            elif os.name != "nt":
                try:
                    os.killpg(owned.process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            elif owned.process.poll() is None:
                owned.process.send_signal(getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM))
            try:
                owned.process.wait(timeout=self.stop_timeout_seconds)
            except subprocess.TimeoutExpired:
                if owned.job is not None:
                    owned.job.terminate()
                elif os.name != "nt":
                    try:
                        os.killpg(owned.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                else:
                    owned.process.kill()
                owned.process.wait(timeout=self.stop_timeout_seconds)
        except (OSError, subprocess.SubprocessError) as exc:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed process scope could not be terminated exactly",
                process_id=process_id,
            ) from exc

    def _close_owned(self, process_id: str) -> None:
        with self._owned_lock:
            owned = self._owned.pop(process_id, None)
        if owned is None:
            return
        if owned.launch_gate is not None:
            try:
                os.close(owned.launch_gate)
            except OSError:
                pass
            owned.launch_gate = None
        for stream in (owned.stdout_stream, owned.stderr_stream):
            try:
                stream.close()
            except OSError:
                pass
        self._release_launch_pins(owned)
        if owned.job is not None:
            owned.job.close()
        close_process = getattr(owned.process, "close", None)
        if callable(close_process):
            close_process()

    def _terminalize(
        self,
        context: _ProcessContext,
        *,
        failed: bool,
        create_restart: bool = True,
        terminal_reason: str | None = None,
        claim_owner: str | None = None,
        supervisor_fence: int | None = None,
    ) -> _ProcessContext:
        process_id = str(context.process["process_id"])
        self._health_failures.pop(process_id, None)
        with self._owned_lock:
            owned = self._owned.get(process_id)
        if not self._confirmed_dead(context, owned=owned):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed process scope exit was not proven; lease remains owned",
                process_id=process_id,
            )
        process = deepcopy(context.process)
        target = "FAILED" if failed else "STOPPED"
        now = _timestamp(self.clock)
        process["state"] = target
        process["terminal_reason"] = terminal_reason or (
            "failure" if failed else "operator_stopped"
        )
        process["failed_at"] = now if failed else None
        process["stopped_at"] = None if failed else now
        lease = deepcopy(context.lease)
        lease.update(
            {
                "status": "released",
                "process_id": process["process_id"],
                "released_at": now,
            }
        )
        terminal = self._repository.transition(
            context,
            process,
            lease,
            expected_states={"PREPARED", "STARTING", "HEALTHY", "STOPPING"},
            claim_owner=claim_owner,
            supervisor_fence=supervisor_fence,
        )
        self.port_reservations.release(
            self.store.database_path, str(lease["lease_id"])
        )
        self._close_owned(str(process["process_id"]))
        self._fault("after_terminal_commit", terminal)
        if failed and create_restart:
            self._create_restart_if_allowed(terminal)
        return terminal

    def _create_restart_if_allowed(
        self, context: _ProcessContext
    ) -> _ProcessContext | None:
        process = context.process
        if (
            process.get("state") != "FAILED"
            or process.get("terminal_reason") == "operator_cancelled"
            or process.get("restart_policy") != "on_failure"
            or not isinstance(process.get("restart_attempt"), int)
            or not isinstance(process.get("max_restart_attempts"), int)
            or process["restart_attempt"] >= process["max_restart_attempts"]
        ):
            return None
        attempt = int(process["restart_attempt"]) + 1
        new_process_id = _safe_identifier(
            "process",
            context.sprint_id,
            str(process["assignment_id"]),
            str(attempt),
        )
        new_lease_id = _safe_identifier("port-lease", new_process_id)
        acquired_at = _timestamp(self.clock)
        config = context.runtime_state["runtime_config"]
        for port in range(int(config["child_port_start"]), int(config["child_port_end"]) + 1):
            lease = {
                "lease_id": new_lease_id,
                "instance_id": config["instance_id"],
                "network_namespace_id": "host",
                "assignment_id": process["assignment_id"],
                "process_id": None,
                "host": "127.0.0.1",
                "port": port,
                "status": "reserved",
                "bind_verified": False,
                "acquired_at": acquired_at,
                "released_at": None,
            }
            restart = deepcopy(process)
            restart.update(
                {
                    "process_id": new_process_id,
                    "runtime_root": str(
                        Path(str(config["process_runtime_root"])) / new_process_id
                    ),
                    "state": "PREPARED",
                    "launch_nonce": uuid4().hex,
                    "os_process_created_at": None,
                    "os_process_birth_token": None,
                    "job_object_id": None,
                    "pid": None,
                    "process_group_id": None,
                    "stdout_log": str(
                        Path(str(config["log_root"])) / f"{new_process_id}.stdout.log"
                    ),
                    "stderr_log": str(
                        Path(str(config["log_root"])) / f"{new_process_id}.stderr.log"
                    ),
                    "health_endpoint": {
                        "host": "127.0.0.1",
                        "port": port,
                        "path": process["health_endpoint"]["path"],
                    },
                    "restart_attempt": attempt,
                    "restart_of_process_id": process["process_id"],
                    "port_lease_id": new_lease_id,
                    "started_at": None,
                    "startup_deadline_at": None,
                    "stopped_at": None,
                    "failed_at": None,
                    "terminal_reason": None,
                }
            )
            created = self._repository.append_restart(
                context,
                restart,
                lease,
                self.port_reservations,
            )
            if created is not None:
                return created
        raise ManagedProcessSupervisorError(
            PROCESS_LAUNCH_FAILED,
            "restart budget remains but no managed child port is available",
            process_id=str(process["process_id"]),
        )

    def _reconcile_running(self, context: _ProcessContext) -> SupervisionResult:
        process_id = str(context.process["process_id"])
        try:
            self._record_identity(context)
        except ProcessLookupError:
            if not self._confirmed_dead(context):
                # The leader may exit while a non-listening descendant remains
                # in the exact recorded Job/process group.  Terminate that
                # scope before releasing the lease.
                self._terminate_context(context)
            if not self._confirmed_dead(context):
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed leader exited but its group cannot be confirmed dead",
                    process_id=process_id,
                )
            terminal = self._terminalize(
                context, failed=context.process.get("state") != "STOPPING"
            )
            return SupervisionResult(
                context.project_id,
                context.sprint_id,
                process_id,
                str(terminal.process["state"]),
                "dead-reconciled",
            )
        state = context.process.get("state")
        if state == "STOPPING":
            return self.stop(context.project_id, context.sprint_id, process_id)
        owners = port_owner_pids(
            str(context.lease["host"]), int(context.lease["port"])
        )
        port_owned = bool(owners) and all(
            self._scope_contains_pid(context, owner) for owner in owners
        )
        if owners and not port_owned:
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "durable endpoint is owned outside the exact managed scope",
                process_id=process_id,
            )
        if state == "STARTING":
            if self._wait_for_health(context):
                self._mark_healthy(context)
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    "HEALTHY",
                    "health-reconciled",
                )
            self._terminate_context(context)
            terminal = self._terminalize(context, failed=True)
            return SupervisionResult(
                context.project_id,
                context.sprint_id,
                process_id,
                str(terminal.process["state"]),
                "startup-health-failed",
            )
        started_at = self._effective_started_at(context.process)
        limits = context.process.get("resource_limits")
        if started_at is not None and isinstance(limits, Mapping):
            wall = limits.get("wall_time_seconds")
            if isinstance(wall, int) and _utc_now(self.clock) >= started_at + timedelta(
                seconds=wall
            ):
                self._terminate_context(context)
                terminal = self._terminalize(context, failed=True)
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    str(terminal.process["state"]),
                    "wall-limit",
                )
        if state == "HEALTHY":
            if port_owned and self._health_ok(context):
                self._health_failures.pop(process_id, None)
            else:
                failures = self._health_failures.get(process_id, 0) + 1
                self._health_failures[process_id] = failures
                if failures < _HEALTH_FAILURE_THRESHOLD:
                    return SupervisionResult(
                        context.project_id,
                        context.sprint_id,
                        process_id,
                        "HEALTHY",
                        "health-degraded",
                    )
                self._health_failures.pop(process_id, None)
                self._terminate_context(context)
                terminal = self._terminalize(context, failed=True)
                return SupervisionResult(
                    context.project_id,
                    context.sprint_id,
                    process_id,
                    str(terminal.process["state"]),
                    "runtime-health-failed",
                )
        return SupervisionResult(
            context.project_id,
            context.sprint_id,
            process_id,
            str(context.process["state"]),
            "verified",
        )

    def _terminate_context(
        self,
        context: _ProcessContext,
        *,
        allow_posix_launch_helper: bool = False,
    ) -> None:
        process_id = str(context.process["process_id"])
        self._health_failures.pop(process_id, None)
        with self._owned_lock:
            owned = self._owned.get(process_id)
        if self._confirmed_dead(context, owned=owned):
            return
        signal_context = context
        try:
            self._record_identity(
                context,
                allow_posix_launch_helper=allow_posix_launch_helper,
            )
        except ProcessLookupError:
            # A dead leader is not sufficient death proof.  Re-bind the exact
            # receipt so its surviving Job/group can be inspected and stopped.
            signal_context, _identity, _receipt = self._context_from_receipt(
                context,
                allowed_phases=(
                    ("launch_gated", "launched")
                    if allow_posix_launch_helper
                    else ("launched",)
                ),
                allow_posix_launch_helper=allow_posix_launch_helper,
            )
        if owned is not None:
            self._terminate_owned(process_id, owned, context=signal_context)
        elif os.name == "nt":
            job_id = signal_context.process.get("job_object_id")
            job = _WindowsJob.open(str(job_id)) if isinstance(job_id, str) else None
            if job is None:
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed Windows Job Object cannot be reopened",
                    process_id=process_id,
                )
            try:
                self._assert_scope_safe(signal_context, job=job)
                job.terminate()
            finally:
                job.close()
        else:
            group = signal_context.process.get("process_group_id")
            if not isinstance(group, int):
                raise ManagedProcessSupervisorError(
                    ORPHAN_PROCESS,
                    "managed process group identity is unsafe",
                    process_id=process_id,
                )
            self._assert_scope_safe(signal_context)
            try:
                os.killpg(group, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + self.stop_timeout_seconds
        while time.monotonic() < deadline:
            if self._confirmed_dead(signal_context, owned=owned):
                return
            time.sleep(0.05)
        if os.name != "nt" and isinstance(
            signal_context.process.get("process_group_id"), int
        ):
            self._assert_scope_safe(signal_context)
            try:
                os.killpg(
                    int(signal_context.process["process_group_id"]), signal.SIGKILL
                )
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + self.stop_timeout_seconds
        while time.monotonic() < deadline:
            if self._confirmed_dead(signal_context, owned=owned):
                return
            time.sleep(0.05)
        raise ManagedProcessSupervisorError(
            ORPHAN_PROCESS,
            "managed process group did not exit; lease remains bound",
            process_id=process_id,
        )

    def stop(
        self, project_id: str, sprint_id: str, process_id: str
    ) -> SupervisionResult:
        with self._operation(), self._process_action_guard(process_id):
            return self._stop_untracked(project_id, sprint_id, process_id)

    def _stop_untracked(
        self, project_id: str, sprint_id: str, process_id: str
    ) -> SupervisionResult:
        context = self._repository.load(project_id, sprint_id, process_id)
        self._validate_config(context)
        state = context.process.get("state")
        if state in {"STOPPED", "FAILED"}:
            return SupervisionResult(
                project_id, sprint_id, process_id, str(state), "already-terminal"
            )
        if state == "PREPARED":
            claim = self._repository.claim_prepared(
                project_id,
                sprint_id,
                process_id,
                owner=self.claim_owner,
                ttl_seconds=max(
                    self.health_timeout_seconds
                    + (self.stop_timeout_seconds * 2)
                    + 10.0,
                    10.0,
                ),
            )
            if claim is None:
                current = self._repository.load(project_id, sprint_id, process_id)
                if current.process.get("state") != "PREPARED":
                    return self.stop(project_id, sprint_id, process_id)
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "PREPARED process has an active launch claim",
                    process_id=process_id,
                )
            context = claim
            receipt_path = self._pid_receipt_path(process_id)
            action = "cancelled-before-launch"
            if receipt_path.exists():
                receipt = self._receipt(process_id)
                if receipt.get("phase") == "intent":
                    recovered = self._recover_launch_intent(
                        context, receipt, create_restart=False
                    )
                    if recovered.state in {"STOPPED", "FAILED"}:
                        return recovered
                    return self.stop(project_id, sprint_id, process_id)
                context, _identity, receipt = self._context_from_receipt(
                    context,
                    receipt,
                    allowed_phases=("launch_gated", "launched"),
                    allow_posix_launch_helper=(
                        receipt.get("phase") == "launch_gated"
                    ),
                )
                if not self._confirmed_dead(context):
                    self._terminate_context(
                        context,
                        allow_posix_launch_helper=(
                            receipt.get("phase") == "launch_gated"
                        ),
                    )
                action = "cancelled-recovered-launch"
            # The v1 lifecycle has no PREPARED -> STOPPED edge.  A cancelled
            # attempt therefore takes the declared FAILED sink, while
            # explicitly suppressing restart creation.  A launch receipt is
            # first treated as live launch evidence and its whole scope is
            # proven dead before the lease can be released.
            terminal = self._terminalize(
                context,
                failed=True,
                create_restart=False,
                terminal_reason="operator_cancelled",
                claim_owner=self.claim_owner,
                supervisor_fence=claim.supervisor_fence,
            )
            return SupervisionResult(
                project_id,
                sprint_id,
                process_id,
                str(terminal.process["state"]),
                action,
            )
        if state in {"STARTING", "HEALTHY"}:
            stopping_process = deepcopy(context.process)
            stopping_process["state"] = "STOPPING"
            context = self._repository.transition(
                context,
                stopping_process,
                context.lease,
                expected_states={str(state)},
            )
        self._terminate_context(context)
        if not self._confirmed_dead(context):
            raise ManagedProcessSupervisorError(
                ORPHAN_PROCESS,
                "managed process group exit was not confirmed",
                process_id=process_id,
            )
        terminal = self._terminalize(context, failed=False)
        return SupervisionResult(
            project_id, sprint_id, process_id, str(terminal.process["state"]), "stopped"
        )

    def reconcile(
        self,
        *,
        project_id: str | None = None,
        sprint_id: str | None = None,
        start_prepared: bool = True,
    ) -> tuple[SupervisionResult, ...]:
        with self._operation():
            return self._reconcile_untracked(
                project_id=project_id,
                sprint_id=sprint_id,
                start_prepared=start_prepared,
            )

    def _reconcile_untracked(
        self,
        *,
        project_id: str | None = None,
        sprint_id: str | None = None,
        start_prepared: bool = True,
    ) -> tuple[SupervisionResult, ...]:
        port_fence = self.port_reservations.reconciliation_fence(
            self.store.database_path
        )
        durable_states = self._repository.active_states()
        live_leases = [
            deepcopy(dict(lease))
            for state in durable_states
            if isinstance(state, Mapping)
            for lease in state.get("port_leases", [])
            if isinstance(lease, Mapping)
            and lease.get("status") in {"reserved", "bound"}
        ]
        try:
            self.port_reservations.reconcile_durable(
                self.store.database_path,
                live_leases,
                fence=port_fence,
            )
        except ManagedPortReservationError as exc:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "local child-port ownership differs from durable state",
            ) from exc
        if os.name == "nt":
            live_process_ids = {
                str(process.get("process_id"))
                for state in durable_states
                if isinstance(state, Mapping)
                for process in state.get("processes", [])
                if isinstance(process, Mapping)
                and process.get("state") in _LIVE_STATES
                and isinstance(process.get("process_id"), str)
            }
            with self._owned_lock:
                stale_owned_ids = tuple(
                    process_id
                    for process_id in self._owned
                    if process_id not in live_process_ids
                )
            for stale_process_id in stale_owned_ids:
                # The first durable snapshot can predate a concurrent
                # activation/launch.  Serialize with that launch and reload
                # authoritative state before closing its freshly installed
                # local handles.
                with self._process_action_guard(stale_process_id):
                    still_live = any(
                        context.process.get("process_id") == stale_process_id
                        and context.process.get("state") in _LIVE_STATES
                        for context in self._repository.contexts()
                    )
                    if not still_live:
                        # Closing the supervisor's Job handle is non-signalling
                        # while the managed child still owns its inherited
                        # handle.  The Job keeps KILL_ON_JOB_CLOSE so the final
                        # handle disappearing cannot strand descendants.
                        self._close_owned(stale_process_id)
        contexts = self._repository.contexts(
            project_id=project_id, sprint_id=sprint_id
        )
        results: list[SupervisionResult] = []
        prepared: list[_ProcessContext] = []
        failures: list[ManagedProcessSupervisorError] = []
        child_process_ids = {
            str(context.process.get("restart_of_process_id"))
            for context in contexts
            if isinstance(context.process.get("restart_of_process_id"), str)
        }
        for context in contexts:
            try:
                self._validate_config(context)
                state = context.process.get("state")
                if state == "PREPARED":
                    if start_prepared:
                        parent_id = context.process.get("restart_of_process_id")
                        if isinstance(parent_id, str):
                            parent = next(
                                (
                                    candidate
                                    for candidate in contexts
                                    if candidate.process.get("process_id") == parent_id
                                ),
                                None,
                            )
                            failed_at = (
                                _parse_timestamp(parent.process.get("failed_at"))
                                if parent is not None
                                else None
                            )
                            backoff = context.process.get("restart_backoff_seconds")
                            if (
                                failed_at is not None
                                and isinstance(backoff, int)
                                and _utc_now(self.clock)
                                < failed_at + timedelta(seconds=backoff)
                            ):
                                results.append(
                                    SupervisionResult(
                                        context.project_id,
                                        context.sprint_id,
                                        str(context.process["process_id"]),
                                        "PREPARED",
                                        "restart-backoff",
                                    )
                                )
                                continue
                        prepared.append(context)
                    continue
                if state in _RUNNING_STATES:
                    with self._process_action_guard(
                        str(context.process["process_id"])
                    ):
                        current = self._repository.load(
                            context.project_id,
                            context.sprint_id,
                            str(context.process["process_id"]),
                        )
                        if current.process.get("state") in _RUNNING_STATES:
                            results.append(self._reconcile_running(current))
                    continue
                if (
                    state == "FAILED"
                    and str(context.process.get("process_id")) not in child_process_ids
                ):
                    with self._process_action_guard(
                        str(context.process["process_id"])
                    ):
                        current = self._repository.load(
                            context.project_id,
                            context.sprint_id,
                            str(context.process["process_id"]),
                        )
                        latest_contexts = self._repository.contexts(
                            project_id=context.project_id,
                            sprint_id=context.sprint_id,
                        )
                        already_has_child = any(
                            candidate.process.get("restart_of_process_id")
                            == current.process.get("process_id")
                            for candidate in latest_contexts
                        )
                        restart = (
                            None
                            if already_has_child
                            else self._create_restart_if_allowed(current)
                        )
                    if restart is not None:
                        results.append(
                            SupervisionResult(
                                context.project_id,
                                context.sprint_id,
                                str(context.process["process_id"]),
                                "FAILED",
                                "restart-reconciled",
                            )
                        )
            except ManagedProcessSupervisorError as exc:
                failures.append(exc)
                if exc.process_id:
                    self._last_errors[exc.process_id] = exc
        if prepared:
            if os.name == "nt":
                with ThreadPoolExecutor(max_workers=min(len(prepared), 16)) as executor:
                    futures = {
                        executor.submit(
                            self.launch_prepared,
                            context.project_id,
                            context.sprint_id,
                            str(context.process["process_id"]),
                        ): context
                        for context in prepared
                    }
                    for future in as_completed(futures):
                        try:
                            results.append(future.result())
                        except ManagedProcessSupervisorError as exc:
                            failures.append(exc)
                            if exc.process_id:
                                self._last_errors[exc.process_id] = exc
            else:
                # preexec_fn is not safe in a fork from a multithreaded
                # executor.  POSIX launches are serialized; the children still
                # run concurrently once started.
                for context in prepared:
                    try:
                        results.append(
                            self.launch_prepared(
                                context.project_id,
                                context.sprint_id,
                                str(context.process["process_id"]),
                            )
                        )
                    except ManagedProcessSupervisorError as exc:
                        failures.append(exc)
                        if exc.process_id:
                            self._last_errors[exc.process_id] = exc
        if failures:
            first = failures[0]
            raise ManagedProcessSupervisorError(
                first.code,
                f"{len(failures)} managed child reconciliation action(s) failed: {first}",
                process_id=first.process_id,
            ) from first
        return tuple(
            sorted(results, key=lambda item: (item.project_id, item.sprint_id, item.process_id))
        )

    def start_background(self) -> None:
        def monitor() -> None:
            while not self._monitor_stop.is_set():
                self._monitor_wake.wait(self.poll_interval_seconds)
                self._monitor_wake.clear()
                if self._monitor_stop.is_set():
                    break
                try:
                    self.reconcile(start_prepared=True)
                except ManagedProcessSupervisorError as exc:
                    if exc.process_id:
                        self._last_errors[exc.process_id] = exc
                except Exception:
                    # An unexpected failure is isolated to this pass.  The
                    # periodic monitor must remain alive so later durable work
                    # can still be reconciled.
                    continue

        with self._lifecycle:
            if self._closing or self._closed:
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed supervisor is closing",
                )
            if self._monitor_thread is not None and self._monitor_thread.is_alive():
                self._monitor_wake.set()
                return
            self._monitor_stop.clear()
            self._monitor_wake.clear()
            self._monitor_thread = threading.Thread(
                target=monitor,
                name="nginx-qa-managed-process-supervisor",
                daemon=True,
            )
            self._monitor_thread.start()
            self._monitor_wake.set()

    def close(self) -> None:
        """Stop monitoring without signalling managed children.

        Children deliberately survive a service restart so the next supervisor
        can verify and reattach using durable PID receipts.  Explicit ``stop``
        remains the only normal termination path.
        """

        if int(getattr(self._operation_depth, "value", 0)) > 0:
            raise ManagedProcessSupervisorError(
                PROCESS_STATE_CONFLICT,
                "managed supervisor cannot close from an active operation",
            )
        timeout = max(
            self.health_timeout_seconds
            + (self.stop_timeout_seconds * 2)
            + self.poll_interval_seconds
            + 5.0,
            5.0,
        )
        deadline = time.monotonic() + timeout
        with self._lifecycle:
            if self._closed:
                return
            self._closing = True
            self._monitor_stop.set()
            self._monitor_wake.set()
            thread = self._monitor_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(deadline - time.monotonic(), 0.0))
        with self._lifecycle:
            while self._active_operations and time.monotonic() < deadline:
                self._lifecycle.wait(timeout=max(deadline - time.monotonic(), 0.0))
            monitor_alive = thread is not None and thread.is_alive()
            if monitor_alive or self._active_operations:
                # Keep ``_closing`` set.  No new operation may enter, and the
                # caller must not close shared reservations until close() is
                # retried after the in-flight work has actually quiesced.
                raise ManagedProcessSupervisorError(
                    PROCESS_STATE_CONFLICT,
                    "managed supervisor operations did not quiesce before shutdown",
                )
            self._repository.release_owner_claims(self.claim_owner)
            self._monitor_thread = None
            self._closed = True
        # A hard launcher crash closes these descriptors.  Closing only a
        # still-pending POSIX gate here gives in-process crash simulations the
        # same property without signalling already launched children.
        with self._owned_lock:
            owned_snapshot = list(self._owned.items())
        for process_id, owned in owned_snapshot:
            descriptor = owned.launch_gate
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                owned.launch_gate = None
            try:
                exited = owned.process.poll() is not None
                if exited:
                    owned.process.wait(timeout=0)
            except (OSError, subprocess.SubprocessError):
                exited = False
            if os.name == "nt" or exited:
                self._close_owned(process_id)


__all__ = [
    "ManagedProcessSupervisor",
    "ManagedProcessSupervisorError",
    "ORPHAN_PROCESS",
    "PROCESS_HEALTH_FAILED",
    "PROCESS_LAUNCH_FAILED",
    "PROCESS_STATE_CONFLICT",
    "ProcessIdentity",
    "RESOURCE_LIMIT_UNSUPPORTED",
    "RUNTIME_CONFIG_DRIFT",
    "SupervisionResult",
    "port_owner_pids",
    "process_identity",
    "service_protected_pids",
]
