"""Durable write-branch ownership for managed workspaces."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sqlite3
import threading
from typing import Callable

from .git_provider import ManagedFileLock, deterministic_branch_publication_id
from .sprint_types import git_ref_format_valid, mirror_storage_key


_COMMIT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_POLICIES = frozenset(
    {"create", "resume", "reject_if_exists", "require_exact_head"}
)


class BranchLeaseError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BranchHeads:
    local_head: str | None = None
    remote_head: str | None = None

    @property
    def agreed_head(self) -> str | None:
        values = {value for value in (self.local_head, self.remote_head) if value}
        if len(values) > 1:
            raise BranchLeaseError(
                "BRANCH_DIVERGED", "local and fetched remote branch heads disagree"
            )
        return next(iter(values), None)


@dataclass(frozen=True)
class BranchLeaseRequest:
    lease_id: str
    repository_id: str
    repository_key: str
    mirror_storage_key: str
    branch: str
    assignment_id: str
    source_commit: str
    initial_head_commit: str


@dataclass(frozen=True)
class BranchLease:
    lease_id: str
    repository_id: str
    repository_key: str
    mirror_storage_key: str
    branch: str
    branch_key: str
    assignment_id: str
    source_commit: str
    initial_head_commit: str
    mode: str
    status: str
    acquired_at: str
    released_at: str | None


def deterministic_branch_lease_id(
    mirror_key: str, branch: str, assignment_id: str
) -> str:
    return deterministic_branch_publication_id(mirror_key, branch, assignment_id)


def select_initial_head(
    *,
    policy: str,
    source_commit: str,
    expected_branch_head: str | None,
    heads: BranchHeads,
    is_ancestor: Callable[[str, str], bool],
) -> str:
    """Apply the frozen v1 existing-branch policy without mutating Git."""

    if policy not in _POLICIES:
        raise BranchLeaseError("BRANCH_POLICY_INVALID", "branch policy is invalid")
    if _COMMIT_ID.fullmatch(source_commit) is None:
        raise BranchLeaseError("SOURCE_COMMIT_NOT_FOUND", "source commit is invalid")
    existing = heads.agreed_head

    if policy in {"create", "reject_if_exists"}:
        if existing is not None:
            raise BranchLeaseError(
                "BRANCH_ALREADY_EXISTS", "assigned branch already exists"
            )
        return source_commit

    if policy == "resume":
        if existing is None:
            return source_commit
        if not is_ancestor(source_commit, existing):
            raise BranchLeaseError(
                "BRANCH_DIVERGED",
                "existing branch head does not descend from the pinned source",
            )
        return existing

    if (
        expected_branch_head is None
        or _COMMIT_ID.fullmatch(expected_branch_head) is None
        or existing is None
        or existing != expected_branch_head
    ):
        raise BranchLeaseError(
            "BRANCH_DIVERGED", "assigned branch does not have the exact required head"
        )
    return expected_branch_head


class BranchLeaseStore:
    """SQLite-backed lease store with process-safe unique write ownership."""

    def __init__(
        self,
        lease_root: str | os.PathLike[str],
        *,
        database_name: str = "branch-leases.sqlite3",
        timeout: float = 30.0,
    ) -> None:
        root = Path(lease_root)
        if not root.is_absolute():
            raise ValueError("lease_root must be absolute")
        if timeout <= 0:
            raise ValueError("lease timeout must be positive")
        self.lease_root = root.resolve(strict=False)
        self.database_path = self.lease_root / database_name
        self.timeout = timeout
        self._initialization_lock = threading.Lock()
        self._initialized = False

    def ensure_initialized(self) -> None:
        """Create the shared lease schema once per store instance."""

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
        return connection

    @contextmanager
    def _connection(self):  # type: ignore[no-untyped-def]
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        self.lease_root.mkdir(parents=True, exist_ok=True)
        initialization_lock = self.lease_root / f".{self.database_path.name}.init.lock"
        with ManagedFileLock(initialization_lock, timeout=self.timeout):
            with self._connection() as connection:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = FULL")
                self.initialize_connection(connection)

    @staticmethod
    def initialize_connection(connection: sqlite3.Connection) -> None:
        """Install the lease tables on an existing SQLite connection."""

        connection.executescript(
            """
                CREATE TABLE IF NOT EXISTS branch_leases (
                    lease_id TEXT PRIMARY KEY,
                    repository_id TEXT NOT NULL,
                    repository_key TEXT NOT NULL,
                    mirror_storage_key TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    branch_key TEXT NOT NULL,
                    assignment_id TEXT NOT NULL,
                    source_commit TEXT NOT NULL,
                    initial_head_commit TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK (mode = 'write'),
                    status TEXT NOT NULL CHECK (status IN ('active', 'released')),
                    acquired_at TEXT NOT NULL,
                    released_at TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS branch_leases_one_active_writer
                    ON branch_leases (mirror_storage_key, branch_key)
                    WHERE status = 'active' AND mode = 'write';
                CREATE INDEX IF NOT EXISTS branch_leases_assignment
                    ON branch_leases (assignment_id, status);
            """
        )

    @staticmethod
    def _validate_request(request: BranchLeaseRequest) -> None:
        if not request.lease_id or len(request.lease_id) > 200:
            raise BranchLeaseError("BRANCH_LEASE_INVALID", "lease ID is invalid")
        if not request.repository_id or not request.assignment_id:
            raise BranchLeaseError("BRANCH_LEASE_INVALID", "lease owner is invalid")
        if not request.repository_key:
            raise BranchLeaseError(
                "REPOSITORY_IDENTITY_MISMATCH", "repository identity is empty"
            )
        if request.mirror_storage_key != mirror_storage_key(request.repository_key):
            raise BranchLeaseError(
                "REPOSITORY_IDENTITY_MISMATCH", "mirror key is not canonical"
            )
        if not git_ref_format_valid(request.branch, branch=True) or any(
            len(component.encode("utf-16-le")) // 2 > 255
            for component in request.branch.split("/")
        ):
            raise BranchLeaseError("GIT_BRANCH_INVALID", "assigned branch is invalid")
        if _COMMIT_ID.fullmatch(request.source_commit) is None or _COMMIT_ID.fullmatch(
            request.initial_head_commit
        ) is None:
            raise BranchLeaseError("BRANCH_LEASE_INVALID", "lease commit is invalid")

    def acquire_write(self, request: BranchLeaseRequest) -> BranchLease:
        self._validate_request(request)
        self.ensure_initialized()
        now = datetime.now(timezone.utc).isoformat()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                lease = self.acquire_write_in_transaction(
                    connection, request, acquired_at=now
                )
                connection.commit()
        except sqlite3.IntegrityError:
            raise BranchLeaseError(
                "BRANCH_ALREADY_LEASED", "assigned branch already has an active writer"
            ) from None
        except sqlite3.OperationalError as exc:
            raise BranchLeaseError(
                "BRANCH_LEASE_STORE_UNAVAILABLE",
                f"branch lease transaction failed: {exc.__class__.__name__}",
            ) from None
        return lease

    def acquire_write_in_transaction(
        self,
        connection: sqlite3.Connection,
        request: BranchLeaseRequest,
        *,
        acquired_at: str,
    ) -> BranchLease:
        """Acquire a writer using the caller's already-open transaction.

        The caller must use this store's database and hold a write transaction.
        This seam lets sprint ACTIVATE publish branch ownership in the same
        durable commit as runtime/control state.
        """

        self._validate_request(request)
        same_id = connection.execute(
            "SELECT * FROM branch_leases WHERE lease_id = ?",
            (request.lease_id,),
        ).fetchone()
        if same_id is not None:
            lease = _row_to_lease(same_id)
            if lease.status == "released":
                raise BranchLeaseError(
                    "BRANCH_LEASE_RELEASED",
                    "a released branch lease cannot be reactivated",
                )
            if not _request_matches_lease(request, lease):
                raise BranchLeaseError(
                    "BRANCH_LEASE_CONFLICT",
                    "lease ID is already bound to different immutable facts",
                )
            return lease

        active = connection.execute(
            """
            SELECT * FROM branch_leases
            WHERE mirror_storage_key = ? AND branch_key = ?
              AND status = 'active' AND mode = 'write'
            """,
            (request.mirror_storage_key, request.branch.casefold()),
        ).fetchone()
        if active is not None:
            lease = _row_to_lease(active)
            if _request_matches_lease(request, lease):
                return lease
            raise BranchLeaseError(
                "BRANCH_ALREADY_LEASED",
                "assigned branch already has an active writer",
            )

        connection.execute(
            """
            INSERT INTO branch_leases (
                lease_id, repository_id, repository_key,
                mirror_storage_key, branch, branch_key, assignment_id,
                source_commit, initial_head_commit, mode, status,
                acquired_at, released_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'write', 'active', ?, NULL)
            """,
            (
                request.lease_id,
                request.repository_id,
                request.repository_key,
                request.mirror_storage_key,
                request.branch,
                request.branch.casefold(),
                request.assignment_id,
                request.source_commit,
                request.initial_head_commit,
                acquired_at,
            ),
        )
        row = connection.execute(
            "SELECT * FROM branch_leases WHERE lease_id = ?",
            (request.lease_id,),
        ).fetchone()
        assert row is not None
        return _row_to_lease(row)

    @staticmethod
    def active_writers_in_transaction(
        connection: sqlite3.Connection,
    ) -> tuple[BranchLease, ...]:
        rows = connection.execute(
            """
            SELECT * FROM branch_leases
            WHERE status = 'active' AND mode = 'write'
            ORDER BY mirror_storage_key, branch_key, lease_id
            """
        ).fetchall()
        return tuple(_row_to_lease(row) for row in rows)

    def release(
        self,
        lease_id: str,
        assignment_id: str,
        *,
        assignment_settled: bool = False,
        all_processes_stopped: bool = False,
    ) -> BranchLease:
        if not self.database_path.exists():
            raise BranchLeaseError(
                "BRANCH_LEASE_NOT_FOUND", "branch lease does not exist"
            )
        self.ensure_initialized()
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                released = self.release_in_transaction(
                    connection,
                    lease_id,
                    assignment_id,
                    assignment_settled=assignment_settled,
                    all_processes_stopped=all_processes_stopped,
                    released_at=datetime.now(timezone.utc).isoformat(),
                )
                connection.commit()
        except sqlite3.OperationalError as exc:
            raise BranchLeaseError(
                "BRANCH_LEASE_STORE_UNAVAILABLE",
                f"branch lease transaction failed: {exc.__class__.__name__}",
            ) from None
        return released

    @staticmethod
    def release_in_transaction(
        connection: sqlite3.Connection,
        lease_id: str,
        assignment_id: str,
        *,
        assignment_settled: bool,
        all_processes_stopped: bool,
        released_at: str,
    ) -> BranchLease:
        """Release a writer inside the caller's existing SQLite transaction.

        Continuity settlement must change the normalized branch owner and the
        matching runtime snapshot atomically.  The standalone :meth:`release`
        method delegates here so both paths retain identical ownership and
        lifecycle checks.
        """

        row = connection.execute(
            "SELECT * FROM branch_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if row is None:
            raise BranchLeaseError(
                "BRANCH_LEASE_NOT_FOUND", "branch lease does not exist"
            )
        lease = _row_to_lease(row)
        if lease.assignment_id != assignment_id:
            raise BranchLeaseError(
                "BRANCH_LEASE_OWNER_MISMATCH",
                "only the owning assignment may release a branch lease",
            )
        if lease.status == "released":
            return lease
        if not assignment_settled or not all_processes_stopped:
            raise BranchLeaseError(
                "BRANCH_LEASE_RELEASE_BLOCKED",
                "assignment must be settled and every owned process stopped",
            )
        connection.execute(
            """
            UPDATE branch_leases
            SET status = 'released', released_at = ?
            WHERE lease_id = ? AND status = 'active'
            """,
            (released_at, lease_id),
        )
        updated = connection.execute(
            "SELECT * FROM branch_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if updated is None:
            raise BranchLeaseError(
                "BRANCH_LEASE_NOT_FOUND", "branch lease disappeared during release"
            )
        return _row_to_lease(updated)

    def get(self, lease_id: str) -> BranchLease | None:
        if not self.database_path.exists():
            return None
        self.ensure_initialized()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM branch_leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
        return _row_to_lease(row) if row is not None else None

    def active_writers(self) -> tuple[BranchLease, ...]:
        if not self.database_path.exists():
            return ()
        self.ensure_initialized()
        with self._connection() as connection:
            return self.active_writers_in_transaction(connection)


def _request_matches_lease(request: BranchLeaseRequest, lease: BranchLease) -> bool:
    return (
        lease.lease_id == request.lease_id
        and lease.repository_id == request.repository_id
        and lease.repository_key == request.repository_key
        and lease.mirror_storage_key == request.mirror_storage_key
        and lease.branch == request.branch
        and lease.branch_key == request.branch.casefold()
        and lease.assignment_id == request.assignment_id
        and lease.source_commit == request.source_commit
        and lease.initial_head_commit == request.initial_head_commit
        and lease.mode == "write"
        and lease.status == "active"
    )


def _row_to_lease(row: sqlite3.Row) -> BranchLease:
    return BranchLease(
        lease_id=row["lease_id"],
        repository_id=row["repository_id"],
        repository_key=row["repository_key"],
        mirror_storage_key=row["mirror_storage_key"],
        branch=row["branch"],
        branch_key=row["branch_key"],
        assignment_id=row["assignment_id"],
        source_commit=row["source_commit"],
        initial_head_commit=row["initial_head_commit"],
        mode=row["mode"],
        status=row["status"],
        acquired_at=row["acquired_at"],
        released_at=row["released_at"],
    )


__all__ = [
    "BranchHeads",
    "BranchLease",
    "BranchLeaseError",
    "BranchLeaseRequest",
    "BranchLeaseStore",
    "deterministic_branch_lease_id",
    "select_initial_head",
]
