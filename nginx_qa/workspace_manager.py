"""Verified, per-assignment workspaces for ``managed_workspace_v1``."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import stat
import sys
from typing import Iterator, Literal
import uuid

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
    ManagedFileLock,
    ManagedGitError,
    ManagedGitProvider,
    ManagedRepository,
    RepositorySpec,
    TRANSIENT_GIT_ERROR_CODES,
)
from .sprint_types import (
    managed_node_path_segment,
    managed_project_path_segment,
    managed_sprint_path_segment,
    windows_absolute_path_key,
    windows_path_segment_valid,
)


_COMMIT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_INTEGRATION_WORKSPACE_ID = re.compile(
    r"integration-workspace-[0-9a-f]{64}\Z"
)
_ACCESS_VALUES = frozenset({"read", "write"})
_POLICIES = frozenset(
    {"create", "resume", "reject_if_exists", "require_exact_head"}
)


class WorkspaceError(RuntimeError):
    """Stable workspace failure which never triggers implicit repair."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class WorkspaceRequest:
    project_id: str
    sprint_id: str
    node_id: str
    assignment_id: str
    repository_id: str
    source_commit: str
    access: Literal["read", "write"]
    assigned_branch: str | None = None
    existing_branch_policy: str | None = None
    expected_branch_head: str | None = None


@dataclass(frozen=True)
class ManagedWorkspace:
    workspace_id: str
    project_id: str
    sprint_id: str
    node_id: str
    assignment_id: str
    repository_id: str
    repository_remote: str
    mirror_storage_key: str
    expected_root: Path
    actual_git_toplevel: Path
    actual_git_dir: Path
    source_commit: str
    initial_head_commit: str
    head_commit: str
    assigned_branch: str | None
    access: str
    working_tree_state: str
    branch_lease_id: str | None


class ManagedWorkspaceManager:
    """Create or verify deterministic workspaces without destructive recovery."""

    def __init__(
        self,
        managed_root: str | os.PathLike[str],
        git_provider: ManagedGitProvider,
        branch_leases: BranchLeaseStore,
        *,
        protected_roots: tuple[str | os.PathLike[str], ...] = (),
        install_roots: tuple[str | os.PathLike[str], ...] | None = None,
        workspace_lock_timeout: float = 60.0,
    ) -> None:
        if workspace_lock_timeout <= 0:
            raise ValueError("workspace lock timeout must be positive")
        self.managed_root = self._canonical_configured_root(managed_root)
        if not _same_non_strict_path(self.managed_root, git_provider.managed_root):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace manager and Git provider use different managed roots",
            )
        self.git_provider = git_provider
        self.branch_leases = branch_leases
        self.workspace_lock_timeout = workspace_lock_timeout
        self.workspaces_root = self.managed_root / "projects"
        self.workspace_locks_root = self.managed_root / "locks" / "workspaces"

        explicit_protected = tuple(
            self._resolve_configured_path(value) for value in protected_roots
        )
        effective_install_roots = install_roots
        if effective_install_roots is None:
            effective_install_roots = tuple(
                dict.fromkeys((Path(sys.prefix), Path(sys.base_prefix)))
            )
        resolved_install = tuple(
            self._resolve_configured_path(value) for value in effective_install_roots
        )
        self.protected_roots = explicit_protected
        self.install_roots = resolved_install
        self._assert_root_is_isolated(self.managed_root)

    @staticmethod
    def _resolve_configured_path(value: str | os.PathLike[str]) -> Path:
        path = Path(value)
        if not path.is_absolute():
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN", "configured safety root must be absolute"
            )
        return Path(os.path.realpath(path.resolve(strict=False)))

    @classmethod
    def _canonical_configured_root(cls, value: str | os.PathLike[str]) -> Path:
        raw = Path(value)
        if not raw.is_absolute():
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN", "managed root must be absolute"
            )
        raw_text = str(value)
        if "\x00" in raw_text:
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN", "managed root contains NUL"
            )
        if os.name == "nt":
            namespace = raw_text.replace("/", "\\")
            if namespace.startswith(("\\\\", "\\\\?\\", "\\\\.\\", "\\??\\")):
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN",
                    "UNC and device namespace roots are not permitted",
                )
            if windows_absolute_path_key(raw_text) is None:
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN", "managed root is not canonical"
                )
        resolved = Path(os.path.realpath(raw.resolve(strict=False)))
        if _path_key(raw.absolute()) != _path_key(resolved):
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN",
                "managed root contains a symlink or junction alias",
            )
        if _is_filesystem_root(resolved):
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN", "a filesystem root cannot be managed"
            )
        return resolved

    def _assert_root_is_isolated(self, root: Path) -> None:
        home = Path(os.path.realpath(Path.home().resolve(strict=False)))
        if _same_non_strict_path(root, home):
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN", "the user home cannot be a managed root"
            )
        for forbidden in (*self.install_roots, *self.protected_roots):
            if _paths_overlap(root, forbidden):
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN",
                    "managed root overlaps an install, service, or protected root",
                )
        self._assert_no_parent_repository(root, include_candidate=True)

    @classmethod
    def validate_isolated_root(
        cls,
        value: str | os.PathLike[str],
        *,
        protected_roots: tuple[str | os.PathLike[str], ...] = (),
        install_roots: tuple[str | os.PathLike[str], ...] = (),
    ) -> Path:
        """Validate a configured mutation root without creating it.

        Runtime, lease, prompt, and workspace roots share the same containment
        hazards: filesystem aliases, parent Git repositories, user/install
        roots, and protected/live trees.  Import preflight uses this helper
        before opening SQLite or creating a mirror.
        """

        root = cls._canonical_configured_root(value)
        home = Path(os.path.realpath(Path.home().resolve(strict=False)))
        if _same_non_strict_path(root, home):
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN", "the user home cannot be a mutation root"
            )
        forbidden = tuple(
            cls._resolve_configured_path(item)
            for item in (*protected_roots, *install_roots)
        )
        for candidate in forbidden:
            if _paths_overlap(root, candidate):
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN",
                    "mutation root overlaps an install, service, or protected root",
                )
        cls._assert_no_parent_repository(root, include_candidate=True)
        return root

    @staticmethod
    def _validate_request(request: WorkspaceRequest) -> None:
        for name, value in (
            ("project_id", request.project_id),
            ("sprint_id", request.sprint_id),
            ("node_id", request.node_id),
            ("assignment_id", request.assignment_id),
        ):
            if (
                not isinstance(value, str)
                or len(value) > 200
                or not windows_path_segment_valid(value)
                or len(value.encode("utf-16-le")) // 2 > 200
            ):
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN", f"{name} is not a safe path segment"
                )
        if not isinstance(request.repository_id, str) or not request.repository_id:
            raise WorkspaceError(
                "REPOSITORY_ID_INVALID", "repository_id must be a non-empty string"
            )
        if not isinstance(request.access, str) or request.access not in _ACCESS_VALUES:
            raise WorkspaceError("WORKSPACE_ACCESS_INVALID", "workspace access is invalid")
        if (
            not isinstance(request.source_commit, str)
            or _COMMIT_ID.fullmatch(request.source_commit) is None
        ):
            raise WorkspaceError(
                "SOURCE_COMMIT_NOT_FOUND", "source commit must be a full object ID"
            )
        if request.access == "read":
            if any(
                value is not None
                for value in (
                    request.assigned_branch,
                    request.existing_branch_policy,
                    request.expected_branch_head,
                )
            ):
                raise WorkspaceError(
                    "WORKSPACE_ACCESS_INVALID",
                    "read workspaces cannot declare write-branch policy",
                )
        else:
            if not request.assigned_branch:
                raise WorkspaceError("GIT_BRANCH_INVALID", "write workspace needs a branch")
            if request.existing_branch_policy not in _POLICIES:
                raise WorkspaceError(
                    "BRANCH_POLICY_INVALID", "write workspace policy is invalid"
                )
            if request.existing_branch_policy == "require_exact_head":
                if (
                    not isinstance(request.expected_branch_head, str)
                    or _COMMIT_ID.fullmatch(request.expected_branch_head) is None
                ):
                    raise WorkspaceError(
                        "BRANCH_DIVERGED", "exact-head policy requires a full commit ID"
                    )
            elif request.expected_branch_head is not None:
                raise WorkspaceError(
                    "BRANCH_POLICY_INVALID",
                    "expected branch head is exclusive to require_exact_head",
                )

    def expected_root(self, request: WorkspaceRequest) -> Path:
        self._validate_request(request)
        root = (
            self.workspaces_root
            / managed_project_path_segment(request.project_id)
            / "sprints"
            / managed_sprint_path_segment(request.sprint_id)
            / "nodes"
            / managed_node_path_segment(request.node_id)
            / request.assignment_id
        )
        resolved_candidate = _resolve_from_nearest_existing(root)
        if _path_key(root.absolute()) != _path_key(resolved_candidate):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace path contains a symlink or junction alias",
            )
        if not _is_strictly_within(resolved_candidate, self.managed_root):
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN", "workspace root escaped the managed root"
            )
        for forbidden in (*self.protected_roots, *self.install_roots):
            if _paths_overlap(resolved_candidate, forbidden):
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN", "workspace overlaps a protected root"
                )
        self._assert_no_parent_repository(root)
        return root

    def integration_workspace_root(self, workspace_artifact_id: str) -> Path:
        """Return one isolated, non-redirected integration workspace root."""

        if _INTEGRATION_WORKSPACE_ID.fullmatch(workspace_artifact_id) is None:
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "integration workspace identity is invalid",
            )
        root = self.managed_root / "integration-workspaces" / workspace_artifact_id
        resolved_candidate = _resolve_from_nearest_existing(root)
        if _path_key(root.absolute()) != _path_key(resolved_candidate):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "integration workspace path contains a symlink or junction alias",
            )
        if not _is_strictly_within(resolved_candidate, self.managed_root):
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN",
                "integration workspace escaped the managed root",
            )
        for forbidden in (*self.protected_roots, *self.install_roots):
            if _paths_overlap(resolved_candidate, forbidden):
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN",
                    "integration workspace overlaps a protected root",
                )
        self._assert_no_parent_repository(root)
        return root

    def integration_workspace_lock_path(self, workspace_artifact_id: str) -> Path:
        return self._workspace_lock_path(
            self.integration_workspace_root(workspace_artifact_id)
        )

    def verify_integration_workspace(
        self,
        workspace_artifact_id: str,
        repository: ManagedRepository,
        parent_commits: tuple[str, ...],
    ) -> Path:
        """Verify an integration checkout owns all Git storage and frozen refs."""

        if len(parent_commits) < 2 or any(
            _COMMIT_ID.fullmatch(commit) is None for commit in parent_commits
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "integration parent identity is invalid",
            )
        root = self.integration_workspace_root(workspace_artifact_id)
        if not root.is_dir() or root.is_symlink():
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "integration workspace root is missing or redirected",
            )
        resolved = Path(os.path.realpath(root.resolve(strict=True)))
        if _path_key(root.absolute()) != _path_key(resolved):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "integration workspace root resolves to another path",
            )
        self._verify_git_root_only(
            root, expected_object_format=repository.object_format
        )
        self._verify_remote(root, repository.spec)
        for index, expected_commit in enumerate(parent_commits, 1):
            actual = self.git_provider.runner.run(
                (
                    "-C",
                    str(root),
                    "rev-parse",
                    "--verify",
                    f"refs/nginx-qa/parents/{index}^{{commit}}",
                ),
                error_code="WORKSPACE_VERIFY_FAILED",
            ).stdout.strip().lower()
            if actual != expected_commit:
                raise WorkspaceError(
                    "WORKSPACE_HEAD_MISMATCH",
                    "integration parent ref changed",
                )
        return root

    def _workspace_lock_path(self, expected_root: Path) -> Path:
        lock_digest = hashlib.sha256(
            os.path.normcase(str(expected_root)).encode("utf-8")
        ).hexdigest()
        lock_path = self.workspace_locks_root / f"workspace-{lock_digest}.lock"
        lock_root = _resolve_from_nearest_existing(self.workspace_locks_root)
        if (
            _path_key(self.workspace_locks_root.absolute()) != _path_key(lock_root)
            or not _is_strictly_within(lock_root, self.managed_root)
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace lock root contains a symlink or junction alias",
            )
        resolved_lock_path = _resolve_from_nearest_existing(lock_path)
        if (
            _path_key(lock_path.absolute()) != _path_key(resolved_lock_path)
            or not _is_strictly_within(resolved_lock_path, self.managed_root)
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "workspace lock path is redirected"
            )
        return lock_path

    @contextmanager
    def verify_result(
        self,
        request: WorkspaceRequest,
        repository: ManagedRepository,
        *,
        expected_root: Path,
        result_commit: str,
        branch_lease_id: str | None,
    ) -> Iterator[ManagedWorkspace]:
        """Hold the workspace fence through result verification and commit.

        The caller performs its durable SQLite mutation inside this context.
        Keeping the lock held closes the otherwise exploitable gap between a
        clean/HEAD observation and publication of the immutable result receipt.
        """

        canonical_root = self.expected_root(request)
        if os.path.normcase(str(canonical_root)) != os.path.normcase(
            str(expected_root.resolve(strict=False))
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "durable workspace root differs from its canonical assignment root",
            )
        lock_path = self._workspace_lock_path(canonical_root)
        try:
            with ManagedFileLock(lock_path, timeout=self.workspace_lock_timeout):
                yield self.verify(
                    request,
                    repository,
                    expected_root=canonical_root,
                    expected_initial_head=result_commit,
                    branch_lease_id=branch_lease_id,
                )
        except FileLockTimeout:
            raise WorkspaceError(
                "WORKSPACE_LOCK_TIMEOUT", "timed out waiting for the workspace lock"
            ) from None

    @staticmethod
    def _assert_no_parent_repository(
        candidate: Path,
        *,
        include_candidate: bool = False,
    ) -> None:
        current = candidate if include_candidate else candidate.parent
        while True:
            dot_git = current / ".git"
            bare_marker = (
                (current / "HEAD").is_file()
                and (current / "objects").is_dir()
                and (current / "config").is_file()
            )
            if dot_git.exists() or dot_git.is_symlink() or bare_marker:
                raise WorkspaceError(
                    "WORKSPACE_ROOT_MISMATCH",
                    "candidate workspace is nested below another Git repository",
                )
            if current.parent == current:
                break
            current = current.parent

    def prepare(
        self,
        request: WorkspaceRequest,
        *,
        repository: ManagedRepository | None = None,
        expected_record: ManagedWorkspace | None = None,
        publish_write_lease: bool = True,
        selected_initial_head: str | None = None,
    ) -> ManagedWorkspace:
        """Safely create a workspace or verify an exact frozen replay."""

        self._validate_request(request)
        if request.access == "read" and selected_initial_head is not None:
            raise WorkspaceError(
                "WORKSPACE_ACCESS_INVALID",
                "read workspaces cannot override their pinned source head",
            )
        if publish_write_lease and selected_initial_head is not None:
            raise WorkspaceError(
                "BRANCH_POLICY_INVALID",
                "selected_initial_head is reserved for unpublished preparation",
            )
        expected_root = self.expected_root(request)
        if expected_record is not None and not (
            expected_root.exists() or expected_root.is_symlink()
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "a frozen workspace record points to a missing checkout",
            )
        if repository is None:
            repository = self.git_provider.ensure_mirror(request.repository_id)
        elif (
            repository.repository_id != request.repository_id
            or repository.mirror_path.resolve(strict=False)
            != self.git_provider.mirror_path_for(
                repository.canonical_remote
            ).resolve(strict=False)
        ):
            raise WorkspaceError(
                "REPOSITORY_IDENTITY_MISMATCH",
                "bound repository does not match the workspace request",
            )
        spec = repository.spec
        self.git_provider.assert_commit(repository, request.source_commit)

        lease: BranchLease | None = None
        initial_head = request.source_commit
        lease_id: str | None = None
        if request.access == "write":
            assert request.assigned_branch is not None
            assert request.existing_branch_policy is not None
            self.git_provider.assert_branch_name(request.assigned_branch)
            lease_id = deterministic_branch_lease_id(
                repository.mirror_storage_key,
                request.assigned_branch,
                request.assignment_id,
            )
            if not publish_write_lease:
                if expected_record is not None:
                    self._assert_record_matches_request(
                        expected_record,
                        request,
                        repository,
                        expected_root,
                        allow_unpublished_write=True,
                    )
                    initial_head = expected_record.initial_head_commit
                    if (
                        selected_initial_head is not None
                        and selected_initial_head != initial_head
                    ):
                        raise WorkspaceError(
                            "BRANCH_DIVERGED",
                            "unpublished workspace head differs from frozen preflight",
                        )
                elif selected_initial_head is None:
                    raise WorkspaceError(
                        "BRANCH_POLICY_INVALID",
                        "unpublished write preparation needs a frozen initial head",
                    )
                else:
                    initial_head = selected_initial_head
                self.git_provider.assert_commit(repository, initial_head)
                lease_id = None
            else:
                frozen_lease = self.branch_leases.get(lease_id)
                if expected_record is not None:
                    self._assert_record_matches_request(
                        expected_record, request, repository, expected_root
                    )
                    if frozen_lease is None:
                        raise BranchLeaseError(
                            "BRANCH_LEASE_NOT_FOUND",
                            "frozen write workspace has no durable branch lease",
                        )
                    initial_head = expected_record.initial_head_commit
                elif frozen_lease is not None:
                    if frozen_lease.status != "active":
                        raise BranchLeaseError(
                            "BRANCH_LEASE_RELEASED",
                            "released assignment lease cannot prepare another workspace",
                        )
                    initial_head = frozen_lease.initial_head_commit
                else:
                    raw_heads = self.git_provider.branch_heads(
                        repository, request.assigned_branch
                    )
                    heads = BranchHeads(
                        local_head=raw_heads.get(
                            f"refs/heads/{request.assigned_branch}"
                        ),
                        remote_head=raw_heads.get(
                            f"refs/remotes/origin/{request.assigned_branch}"
                        ),
                    )
                    initial_head = select_initial_head(
                        policy=request.existing_branch_policy,
                        source_commit=request.source_commit,
                        expected_branch_head=request.expected_branch_head,
                        heads=heads,
                        is_ancestor=lambda ancestor, descendant: self.git_provider.is_ancestor(
                            repository, ancestor, descendant
                        ),
                    )
                self.git_provider.assert_commit(repository, initial_head)
                lease = self.branch_leases.acquire_write(
                    BranchLeaseRequest(
                        lease_id=lease_id,
                        repository_id=request.repository_id,
                        repository_key=repository.canonical_remote,
                        mirror_storage_key=repository.mirror_storage_key,
                        branch=request.assigned_branch,
                        assignment_id=request.assignment_id,
                        source_commit=request.source_commit,
                        initial_head_commit=initial_head,
                    )
                )
        elif expected_record is not None:
            self._assert_record_matches_request(
                expected_record, request, repository, expected_root
            )
            initial_head = expected_record.initial_head_commit

        lock_digest = hashlib.sha256(
            os.path.normcase(str(expected_root)).encode("utf-8")
        ).hexdigest()
        lock_path = self.workspace_locks_root / f"workspace-{lock_digest}.lock"
        lock_root = _resolve_from_nearest_existing(self.workspace_locks_root)
        if (
            _path_key(self.workspace_locks_root.absolute()) != _path_key(lock_root)
            or not _is_strictly_within(lock_root, self.managed_root)
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace lock root contains a symlink or junction alias",
            )
        resolved_lock_path = _resolve_from_nearest_existing(lock_path)
        if (
            _path_key(lock_path.absolute()) != _path_key(resolved_lock_path)
            or not _is_strictly_within(resolved_lock_path, self.managed_root)
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "workspace lock path is redirected"
            )
        try:
            with ManagedFileLock(lock_path, timeout=self.workspace_lock_timeout):
                if expected_root.exists() or expected_root.is_symlink():
                    verified = self.verify(
                        request,
                        repository,
                        expected_root=expected_root,
                        expected_initial_head=initial_head,
                        branch_lease_id=lease.lease_id if lease else None,
                    )
                    self.git_provider.pin_commit(
                        repository,
                        request.source_commit,
                        owner_id=f"{request.assignment_id}:source",
                    )
                    self.git_provider.pin_commit(
                        repository,
                        initial_head,
                        owner_id=f"{request.assignment_id}:initial",
                    )
                    if request.access == "write" and publish_write_lease:
                        assert request.assigned_branch is not None
                        self.git_provider.reserve_local_branch(
                            repository,
                            request.assigned_branch,
                            policy=request.existing_branch_policy or "",
                            source_commit=request.source_commit,
                            expected_branch_head=request.expected_branch_head,
                            selected_head=initial_head,
                        )
                    return verified
                source_pin = self.git_provider.pin_commit(
                    repository,
                    request.source_commit,
                    owner_id=f"{request.assignment_id}:source",
                )
                initial_pin = self.git_provider.pin_commit(
                    repository,
                    initial_head,
                    owner_id=f"{request.assignment_id}:initial",
                )
                if request.access == "write" and publish_write_lease:
                    assert request.assigned_branch is not None
                    self.git_provider.reserve_local_branch(
                        repository,
                        request.assigned_branch,
                        policy=request.existing_branch_policy or "",
                        source_commit=request.source_commit,
                        expected_branch_head=request.expected_branch_head,
                        selected_head=initial_head,
                    )
                return self._materialize(
                    request,
                    repository,
                    spec,
                    expected_root,
                    initial_head,
                    source_pin,
                    initial_pin,
                    branch_lease_id=lease.lease_id if lease else None,
                )
        except FileLockTimeout:
            raise WorkspaceError(
                "WORKSPACE_LOCK_TIMEOUT", "timed out waiting for the workspace lock"
            ) from None

    def publish_write_workspace(
        self,
        request: WorkspaceRequest,
        workspace: ManagedWorkspace,
        *,
        repository: ManagedRepository | None = None,
        transactional_lease: BranchLease | None = None,
        publish_mirror_ref: bool = True,
    ) -> ManagedWorkspace:
        """Fence and publish one already prepared write workspace.

        PREPARE uses ``publish_write_lease=False`` so the checkout remains an
        unreachable artifact.  ACTIVATE calls this method immediately before
        its durable state commit.  Replays are exact and never reset either the
        mirror branch or the workspace.
        """

        self._validate_request(request)
        if request.access != "write" or not request.assigned_branch:
            raise WorkspaceError(
                "WORKSPACE_ACCESS_INVALID", "only write workspaces can be published"
            )
        if repository is None:
            repository = self.git_provider.ensure_mirror(
                request.repository_id, fetch=False
            )
        elif (
            repository.repository_id != request.repository_id
            or repository.mirror_path.resolve(strict=False)
            != self.git_provider.mirror_path_for(
                repository.canonical_remote
            ).resolve(strict=False)
        ):
            raise WorkspaceError(
                "REPOSITORY_IDENTITY_MISMATCH",
                "bound repository does not match the workspace request",
            )
        expected_root = self.expected_root(request)
        self._assert_record_matches_request(
            workspace,
            request,
            repository,
            expected_root,
            allow_unpublished_write=workspace.branch_lease_id is None,
        )
        lease_id = deterministic_branch_lease_id(
            repository.mirror_storage_key,
            request.assigned_branch,
            request.assignment_id,
        )
        if transactional_lease is not None:
            expected_request = BranchLeaseRequest(
                lease_id=lease_id,
                repository_id=request.repository_id,
                repository_key=repository.canonical_remote,
                mirror_storage_key=repository.mirror_storage_key,
                branch=request.assigned_branch,
                assignment_id=request.assignment_id,
                source_commit=request.source_commit,
                initial_head_commit=workspace.initial_head_commit,
            )
            if (
                transactional_lease.status != "active"
                or transactional_lease.mode != "write"
                or transactional_lease.lease_id != expected_request.lease_id
                or transactional_lease.repository_id != expected_request.repository_id
                or transactional_lease.repository_key != expected_request.repository_key
                or transactional_lease.mirror_storage_key
                != expected_request.mirror_storage_key
                or transactional_lease.branch != expected_request.branch
                or transactional_lease.assignment_id != expected_request.assignment_id
                or transactional_lease.source_commit != expected_request.source_commit
                or transactional_lease.initial_head_commit
                != expected_request.initial_head_commit
            ):
                raise WorkspaceError(
                    "BRANCH_LEASE_CONFLICT",
                    "transactional branch lease does not match the workspace",
                )
            prior_lease = transactional_lease
        else:
            prior_lease = self.branch_leases.get(lease_id)
        if prior_lease is None:
            raw_heads = self.git_provider.branch_heads(
                repository, request.assigned_branch
            )
            selected = select_initial_head(
                policy=request.existing_branch_policy or "",
                source_commit=request.source_commit,
                expected_branch_head=request.expected_branch_head,
                heads=BranchHeads(
                    local_head=raw_heads.get(f"refs/heads/{request.assigned_branch}"),
                    remote_head=raw_heads.get(
                        f"refs/remotes/origin/{request.assigned_branch}"
                    ),
                ),
                is_ancestor=lambda ancestor, descendant: self.git_provider.is_ancestor(
                    repository, ancestor, descendant
                ),
            )
            if selected != workspace.initial_head_commit:
                raise WorkspaceError(
                    "BRANCH_DIVERGED",
                    "branch changed after the frozen preflight observation",
                )
        lease = (
            transactional_lease
            if transactional_lease is not None
            else self.branch_leases.acquire_write(
                BranchLeaseRequest(
                    lease_id=lease_id,
                    repository_id=request.repository_id,
                    repository_key=repository.canonical_remote,
                    mirror_storage_key=repository.mirror_storage_key,
                    branch=request.assigned_branch,
                    assignment_id=request.assignment_id,
                    source_commit=request.source_commit,
                    initial_head_commit=workspace.initial_head_commit,
                )
            )
        )
        try:
            if publish_mirror_ref:
                self.git_provider.reserve_local_branch(
                    repository,
                    request.assigned_branch,
                    policy=request.existing_branch_policy or "",
                    source_commit=request.source_commit,
                    expected_branch_head=request.expected_branch_head,
                    selected_head=workspace.initial_head_commit,
                )
            return self.verify(
                request,
                repository,
                expected_root=expected_root,
                expected_initial_head=workspace.initial_head_commit,
                branch_lease_id=lease.lease_id,
            )
        except Exception:
            if prior_lease is None and transactional_lease is None:
                self.branch_leases.release(
                    lease.lease_id,
                    request.assignment_id,
                    assignment_settled=True,
                    all_processes_stopped=True,
                )
            raise

    def _materialize(
        self,
        request: WorkspaceRequest,
        repository: ManagedRepository,
        spec: RepositorySpec,
        expected_root: Path,
        initial_head: str,
        source_pin: str,
        initial_pin: str,
        *,
        branch_lease_id: str | None,
    ) -> ManagedWorkspace:
        self._assert_no_parent_repository(expected_root)
        expected_root.parent.mkdir(parents=True, exist_ok=True)
        resolved_candidate = _resolve_from_nearest_existing(expected_root)
        if _path_key(expected_root.absolute()) != _path_key(resolved_candidate):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace parent changed through a symlink or junction",
            )
        if not _is_strictly_within(resolved_candidate, self.managed_root):
            raise WorkspaceError(
                "WORKSPACE_ROOT_FORBIDDEN",
                "workspace parent changed outside the managed root",
            )
        for forbidden in (*self.protected_roots, *self.install_roots):
            if _paths_overlap(resolved_candidate, forbidden):
                raise WorkspaceError(
                    "WORKSPACE_ROOT_FORBIDDEN",
                    "workspace parent changed into a protected root",
                )
        self._assert_no_parent_repository(expected_root)
        # Keep the unpublished name opaque but compact: the final managed path
        # already contains a full sprint digest and Git appends ~50 characters
        # for loose objects on Windows.
        preparation_root = expected_root.parent / f".p-{uuid.uuid4().hex[:24]}"
        if preparation_root.exists() or preparation_root.is_symlink():
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "workspace preparation root already exists"
            )

        self.git_provider.runner.run(
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
            error_code="WORKSPACE_CREATE_FAILED",
        )
        # The frozen managed path topology is intentionally descriptive and
        # can exceed the legacy Win32 260-character limit once Git appends
        # object paths.  Enable Git's native long-path handling before the
        # first fetch writes any objects into the prepared checkout.
        self.git_provider.runner.run(
            ("-C", str(preparation_root), "config", "core.longpaths", "true"),
            error_code="WORKSPACE_CREATE_FAILED",
        )
        self._verify_git_root_only(
            preparation_root, expected_object_format=repository.object_format
        )
        self.git_provider.runner.run(
            (
                "-C",
                str(preparation_root),
                "remote",
                "add",
                "origin",
                spec.transport_url,
            ),
            error_code="WORKSPACE_CREATE_FAILED",
        )
        self._verify_remote(preparation_root, spec)

        refspecs = [
            f"+{source_pin}:refs/nginx-qa/source",
            f"+{initial_pin}:refs/nginx-qa/initial",
        ]
        self.git_provider.runner.run(
            (
                "-C",
                str(preparation_root),
                "fetch",
                "--quiet",
                "--no-tags",
                "--no-write-fetch-head",
                str(repository.mirror_path),
                *refspecs,
            ),
            error_code="WORKSPACE_CREATE_FAILED",
        )
        if request.access == "read":
            self.git_provider.runner.run(
                ("-C", str(preparation_root), "switch", "--detach", initial_head),
                error_code="WORKSPACE_CREATE_FAILED",
            )
        else:
            assert request.assigned_branch is not None
            self.git_provider.runner.run(
                (
                    "-C",
                    str(preparation_root),
                    "switch",
                    "--create",
                    request.assigned_branch,
                    initial_head,
                ),
                error_code="WORKSPACE_CREATE_FAILED",
            )
        self.verify(
            request,
            repository,
            expected_root=preparation_root,
            expected_initial_head=initial_head,
            branch_lease_id=branch_lease_id,
            expected_workspace_id_root=expected_root,
        )
        if expected_root.exists() or expected_root.is_symlink():
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "canonical workspace root appeared during preparation",
            )
        preparation_root.rename(expected_root)
        return self.verify(
            request,
            repository,
            expected_root=expected_root,
            expected_initial_head=initial_head,
            branch_lease_id=branch_lease_id,
        )

    def verify(
        self,
        request: WorkspaceRequest,
        repository: ManagedRepository,
        *,
        expected_root: Path,
        expected_initial_head: str,
        branch_lease_id: str | None,
        expected_workspace_id_root: Path | None = None,
    ) -> ManagedWorkspace:
        """Verify all ownership facts; never modifies an existing workspace."""

        if not expected_root.is_dir() or expected_root.is_symlink():
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "workspace root is missing or redirected"
            )
        lexical_root = expected_root.absolute()
        resolved_root = Path(os.path.realpath(expected_root.resolve(strict=True)))
        if _path_key(lexical_root) != _path_key(resolved_root):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "workspace root resolves to another path"
            )
        try:
            toplevel_result = self.git_provider.runner.run(
                ("-C", str(expected_root), "rev-parse", "--show-toplevel"),
                error_code="WORKSPACE_ROOT_MISMATCH",
            )
        except ManagedGitError as exc:
            if exc.code in TRANSIENT_GIT_ERROR_CODES:
                raise
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "workspace is not an owned Git root"
            ) from exc
        actual_toplevel = Path(toplevel_result.stdout.strip())
        if not _same_existing_path(actual_toplevel, resolved_root):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "actual Git toplevel differs from the expected workspace root",
            )

        git_dir_result = self.git_provider.runner.run(
            ("-C", str(expected_root), "rev-parse", "--absolute-git-dir"),
            error_code="WORKSPACE_ROOT_MISMATCH",
        )
        actual_git_dir = Path(
            os.path.realpath(Path(git_dir_result.stdout.strip()).resolve(strict=True))
        )
        actual_object_format = self.git_provider.runner.run(
            ("-C", str(expected_root), "rev-parse", "--show-object-format"),
            error_code="WORKSPACE_ROOT_MISMATCH",
        ).stdout.strip()
        if actual_object_format != repository.object_format:
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace and managed mirror use different object formats",
            )
        expected_git_dir = resolved_root / ".git"
        if (
            not expected_git_dir.is_dir()
            or expected_git_dir.is_symlink()
            or not _path_resolves_to_itself(expected_git_dir)
            or not _same_existing_path(actual_git_dir, expected_git_dir)
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace must own its exact non-linked .git directory",
            )
        common_dir = self.git_provider.runner.run(
            (
                "-C",
                str(expected_root),
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
            ),
            error_code="WORKSPACE_ROOT_MISMATCH",
        ).stdout.strip()
        object_dir = self.git_provider.runner.run(
            (
                "-C",
                str(expected_root),
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                "objects",
            ),
            error_code="WORKSPACE_ROOT_MISMATCH",
        ).stdout.strip()
        refs_dir = self.git_provider.runner.run(
            (
                "-C",
                str(expected_root),
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                "refs",
            ),
            error_code="WORKSPACE_ROOT_MISMATCH",
        ).stdout.strip()
        expected_objects = expected_git_dir / "objects"
        expected_refs = expected_git_dir / "refs"
        if (
            not _workspace_git_storage_safe(expected_git_dir, require_index=True)
            or not _path_resolves_to_itself(expected_objects)
            or not _same_existing_path(Path(common_dir), expected_git_dir)
            or not _same_existing_path(Path(object_dir), expected_objects)
            or not _same_existing_path(Path(refs_dir), expected_refs)
            or not all(
                _is_strictly_within(Path(value), resolved_root)
                for value in (common_dir, object_dir, refs_dir)
            )
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace depends on external common or object storage",
            )
        alternates = expected_git_dir / "objects" / "info" / "alternates"
        if alternates.exists() or alternates.is_symlink():
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "workspace may not use an alternate object store",
            )

        spec = repository.spec
        if (
            repository.repository_id != request.repository_id
            or spec.repository_id != request.repository_id
            or repository.canonical_remote != spec.canonical_remote
            or repository.mirror_storage_key != spec.storage_key
        ):
            raise WorkspaceError(
                "REPOSITORY_IDENTITY_MISMATCH",
                "managed repository snapshot is internally inconsistent",
            )
        self._verify_remote(expected_root, spec)
        source_check = self.git_provider.runner.run(
            (
                "-C",
                str(expected_root),
                "cat-file",
                "-e",
                f"{request.source_commit}^{{commit}}",
            ),
            allowed_returncodes=frozenset({0, 1, 128}),
            error_code="SOURCE_COMMIT_NOT_FOUND",
        )
        if source_check.returncode != 0:
            raise WorkspaceError(
                "SOURCE_COMMIT_NOT_FOUND", "pinned source is absent from the workspace"
            )

        head_commit = self.git_provider.runner.run(
            ("-C", str(expected_root), "rev-parse", "--verify", "HEAD^{commit}"),
            error_code="BRANCH_DIVERGED",
        ).stdout.strip().lower()
        branch_result = self.git_provider.runner.run(
            ("-C", str(expected_root), "symbolic-ref", "--quiet", "--short", "HEAD"),
            allowed_returncodes=frozenset({0, 1}),
            error_code="BRANCH_DIVERGED",
        )
        actual_branch = branch_result.stdout.strip() if branch_result.returncode == 0 else None
        if request.access == "read":
            if actual_branch is not None or branch_lease_id is not None:
                raise WorkspaceError(
                    "BRANCH_DIVERGED", "read workspace must have detached HEAD and no lease"
                )
        elif actual_branch != request.assigned_branch:
            raise WorkspaceError(
                "BRANCH_DIVERGED", "workspace is not on its assigned branch"
            )
        if head_commit != expected_initial_head:
            raise WorkspaceError(
                "BRANCH_DIVERGED", "workspace HEAD differs from its frozen initial head"
            )

        status_result = self.git_provider.runner.run(
            (
                "-C",
                str(expected_root),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--ignore-submodules=none",
            ),
            error_code="WORKSPACE_ROOT_MISMATCH",
        )
        if status_result.stdout:
            raise WorkspaceError(
                "WORKSPACE_DIRTY",
                "workspace has tracked, staged, untracked, or submodule changes",
            )

        identity_root = expected_workspace_id_root or expected_root
        workspace_id = _workspace_id(request)
        return ManagedWorkspace(
            workspace_id=workspace_id,
            project_id=request.project_id,
            sprint_id=request.sprint_id,
            node_id=request.node_id,
            assignment_id=request.assignment_id,
            repository_id=request.repository_id,
            repository_remote=repository.canonical_remote,
            mirror_storage_key=repository.mirror_storage_key,
            expected_root=identity_root,
            actual_git_toplevel=resolved_root,
            actual_git_dir=actual_git_dir,
            source_commit=request.source_commit,
            initial_head_commit=expected_initial_head,
            head_commit=head_commit,
            assigned_branch=request.assigned_branch,
            access=request.access,
            working_tree_state="clean",
            branch_lease_id=branch_lease_id,
        )

    def _verify_git_root_only(
        self, root: Path, *, expected_object_format: str
    ) -> None:
        try:
            actual = self.git_provider.runner.run(
                ("-C", str(root), "rev-parse", "--show-toplevel"),
                error_code="WORKSPACE_ROOT_MISMATCH",
            ).stdout.strip()
            git_dir = self.git_provider.runner.run(
                ("-C", str(root), "rev-parse", "--absolute-git-dir"),
                error_code="WORKSPACE_ROOT_MISMATCH",
            ).stdout.strip()
            common_dir = self.git_provider.runner.run(
                (
                    "-C",
                    str(root),
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-common-dir",
                ),
                error_code="WORKSPACE_ROOT_MISMATCH",
            ).stdout.strip()
            object_dir = self.git_provider.runner.run(
                (
                    "-C",
                    str(root),
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-path",
                    "objects",
                ),
                error_code="WORKSPACE_ROOT_MISMATCH",
            ).stdout.strip()
            refs_dir = self.git_provider.runner.run(
                (
                    "-C",
                    str(root),
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-path",
                    "refs",
                ),
                error_code="WORKSPACE_ROOT_MISMATCH",
            ).stdout.strip()
            object_format = self.git_provider.runner.run(
                ("-C", str(root), "rev-parse", "--show-object-format"),
                error_code="WORKSPACE_ROOT_MISMATCH",
            ).stdout.strip()
        except ManagedGitError as exc:
            if exc.code in TRANSIENT_GIT_ERROR_CODES:
                raise
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "prepared repository root is invalid"
            ) from exc
        resolved = Path(os.path.realpath(root.resolve(strict=True)))
        expected_git_dir = resolved / ".git"
        if (
            not _same_existing_path(Path(actual), resolved)
            or not expected_git_dir.is_dir()
            or expected_git_dir.is_symlink()
            or not _path_resolves_to_itself(expected_git_dir)
            or not _workspace_git_storage_safe(expected_git_dir, require_index=False)
            or not _same_existing_path(
                Path(os.path.realpath(Path(git_dir).resolve(strict=True))),
                expected_git_dir,
            )
            or not _same_existing_path(Path(common_dir), expected_git_dir)
            or not _path_resolves_to_itself(expected_git_dir / "objects")
            or not _same_existing_path(Path(object_dir), expected_git_dir / "objects")
            or not _same_existing_path(Path(refs_dir), expected_git_dir / "refs")
            or object_format != expected_object_format
            or not all(
                _is_strictly_within(Path(value), resolved)
                for value in (common_dir, object_dir, refs_dir)
            )
            or (expected_git_dir / "objects" / "info" / "alternates").exists()
            or (expected_git_dir / "objects" / "info" / "alternates").is_symlink()
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH", "prepared repository root is mismatched"
            )

    def _verify_remote(self, root: Path, spec: RepositorySpec) -> None:
        try:
            fetch_urls = self.git_provider.runner.run(
                ("-C", str(root), "remote", "get-url", "--all", "origin"),
                error_code="REPOSITORY_IDENTITY_MISMATCH",
            ).stdout.splitlines()
            push_urls = self.git_provider.runner.run(
                (
                    "-C",
                    str(root),
                    "remote",
                    "get-url",
                    "--push",
                    "--all",
                    "origin",
                ),
                error_code="REPOSITORY_IDENTITY_MISMATCH",
            ).stdout.splitlines()
        except ManagedGitError as exc:
            if exc.code in TRANSIENT_GIT_ERROR_CODES:
                raise
            raise WorkspaceError(
                "REPOSITORY_IDENTITY_MISMATCH", "workspace origin is missing"
            ) from exc
        if not self.git_provider.remote_configuration_matches(
            spec, fetch_urls, push_urls
        ):
            raise WorkspaceError(
                "REPOSITORY_IDENTITY_MISMATCH",
                "workspace origin does not match the repository registry",
            )

    @staticmethod
    def _assert_record_matches_request(
        record: ManagedWorkspace,
        request: WorkspaceRequest,
        repository: ManagedRepository,
        expected_root: Path,
        *,
        allow_unpublished_write: bool = False,
    ) -> None:
        if (
            record.workspace_id != _workspace_id(request)
            or record.project_id != request.project_id
            or record.sprint_id != request.sprint_id
            or record.node_id != request.node_id
            or record.assignment_id != request.assignment_id
            or record.repository_id != request.repository_id
            or record.repository_remote != repository.canonical_remote
            or record.mirror_storage_key != repository.mirror_storage_key
            or _path_key(record.expected_root.absolute())
            != _path_key(expected_root.absolute())
            or not _same_existing_path(record.expected_root, expected_root)
            or not _same_existing_path(record.actual_git_toplevel, expected_root)
            or not _same_existing_path(
                record.actual_git_dir, expected_root / ".git"
            )
            or record.source_commit != request.source_commit
            or record.head_commit != record.initial_head_commit
            or record.assigned_branch != request.assigned_branch
            or record.access != request.access
            or record.working_tree_state != "clean"
            or (
                request.access == "read" and record.branch_lease_id is not None
            )
            or (
                request.access == "read"
                and record.initial_head_commit != request.source_commit
            )
            or (
                request.access == "write"
                and (
                    (
                        allow_unpublished_write
                        and record.branch_lease_id is not None
                    )
                    or (
                        not allow_unpublished_write
                        and record.branch_lease_id
                        != deterministic_branch_lease_id(
                            repository.mirror_storage_key,
                            request.assigned_branch or "",
                            request.assignment_id,
                        )
                    )
                )
            )
        ):
            raise WorkspaceError(
                "WORKSPACE_ROOT_MISMATCH",
                "frozen workspace record does not match the prepare request",
            )

    def release_write_lease(
        self,
        workspace: ManagedWorkspace,
        *,
        assignment_settled: bool,
        all_processes_stopped: bool,
    ) -> BranchLease:
        if workspace.access != "write" or workspace.branch_lease_id is None:
            raise WorkspaceError(
                "BRANCH_LEASE_NOT_FOUND", "workspace does not own a write lease"
            )
        return self.branch_leases.release(
            workspace.branch_lease_id,
            workspace.assignment_id,
            assignment_settled=assignment_settled,
            all_processes_stopped=all_processes_stopped,
        )


def _workspace_id(request: WorkspaceRequest) -> str:
    digest = hashlib.sha256(
        "\0".join(
            (
                request.project_id,
                request.sprint_id,
                request.node_id,
                request.assignment_id,
            )
        ).encode("utf-8")
    ).hexdigest()
    return f"workspace-{digest}"


def _is_filesystem_root(path: Path) -> bool:
    return path.parent == path


def _path_key(path: Path) -> str:
    return os.path.normcase(os.path.normpath(str(path)))


def _same_non_strict_path(first: Path, second: Path) -> bool:
    return _path_key(first.resolve(strict=False)) == _path_key(second.resolve(strict=False))


def _same_existing_path(first: Path, second: Path) -> bool:
    try:
        first_real = Path(os.path.realpath(first.resolve(strict=True)))
        second_real = Path(os.path.realpath(second.resolve(strict=True)))
    except OSError:
        return False
    return _path_key(first_real) == _path_key(second_real)


def _path_resolves_to_itself(path: Path) -> bool:
    if path.is_symlink() or _path_is_junction(path):
        return False
    try:
        resolved = Path(os.path.realpath(path.resolve(strict=True)))
    except OSError:
        return False
    return _path_key(path.absolute()) == _path_key(resolved)


def _path_is_junction(path: Path) -> bool:
    checker = getattr(path, "is_junction", None)
    try:
        return bool(checker()) if checker is not None else False
    except OSError:
        return True


def _workspace_git_storage_safe(git_dir: Path, *, require_index: bool) -> bool:
    for directory in (git_dir / "objects", git_dir / "refs"):
        if (
            not directory.is_dir()
            or not _path_resolves_to_itself(directory)
            or _tree_contains_redirect(directory)
        ):
            return False
    for required_file in (git_dir / "HEAD", git_dir / "config"):
        if not required_file.is_file() or not _path_resolves_to_itself(required_file):
            return False
    index = git_dir / "index"
    if require_index and (
        not index.is_file() or not _path_resolves_to_itself(index)
    ):
        return False
    if not require_index and (index.exists() or index.is_symlink()) and (
        not index.is_file() or not _path_resolves_to_itself(index)
    ):
        return False
    logs = git_dir / "logs"
    if (logs.exists() or logs.is_symlink() or _path_is_junction(logs)) and (
        not logs.is_dir() or not _path_resolves_to_itself(logs)
    ):
        return False
    if logs.is_dir() and _tree_contains_redirect(logs):
        return False
    packed_refs = git_dir / "packed-refs"
    if (packed_refs.exists() or packed_refs.is_symlink()) and (
        not packed_refs.is_file() or not _path_resolves_to_itself(packed_refs)
    ):
        return False
    return not any(
        path.exists() or path.is_symlink()
        for path in (
            git_dir / "commondir",
            git_dir / "objects" / "info" / "alternates",
            git_dir / "index.lock",
        )
    )


def _tree_contains_redirect(root: Path) -> bool:
    pending = [root]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    if entry.is_symlink() or _path_is_junction(path):
                        return True
                    stat_result = entry.stat(follow_symlinks=False)
                    reparse_flag = getattr(stat_result, "st_file_attributes", 0) & getattr(
                        stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
                    )
                    if reparse_flag:
                        return True
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(path)
        except OSError:
            return True
    return False


def _is_within(child: Path, parent: Path) -> bool:
    child_key = _path_key(child.resolve(strict=False))
    parent_key = _path_key(parent.resolve(strict=False))
    try:
        return os.path.commonpath((child_key, parent_key)) == parent_key
    except ValueError:
        return False


def _is_strictly_within(child: Path, parent: Path) -> bool:
    return not _same_non_strict_path(child, parent) and _is_within(child, parent)


def _paths_overlap(first: Path, second: Path) -> bool:
    return _is_within(first, second) or _is_within(second, first)


def _resolve_from_nearest_existing(path: Path) -> Path:
    missing: list[str] = []
    current = path
    while not current.exists() and not current.is_symlink():
        if current.parent == current:
            break
        missing.append(current.name)
        current = current.parent
    try:
        resolved = Path(os.path.realpath(current.resolve(strict=True)))
    except OSError:
        resolved = Path(os.path.realpath(current.resolve(strict=False)))
    for component in reversed(missing):
        resolved /= component
    return resolved


__all__ = [
    "ManagedWorkspace",
    "ManagedWorkspaceManager",
    "WorkspaceError",
    "WorkspaceRequest",
]
