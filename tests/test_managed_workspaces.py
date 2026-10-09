from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from nginx_qa.branch_leases import (
    BranchLeaseError,
    BranchLeaseStore,
    deterministic_branch_lease_id,
)
from nginx_qa.git_provider import ManagedGitError, ManagedGitProvider, RepositorySpec
from nginx_qa.sprint_types import mirror_storage_key
from nginx_qa.workspace_manager import (
    ManagedWorkspaceManager,
    WorkspaceError,
    WorkspaceRequest,
)
from tests.managed_workspace_support import ManagedWorkspaceFixture


class SimulatedPublicationCrash(BaseException):
    pass


class ManagedRepositoryAndWorkspaceTests(ManagedWorkspaceFixture, unittest.TestCase):
    @staticmethod
    def publication_refs(repository, publication_id: str) -> tuple[str, str]:
        digest = hashlib.sha256(
            f"{repository.canonical_remote}\0{publication_id}".encode("utf-8")
        ).hexdigest()
        return (
            f"refs/nginx-qa/intents/{digest}",
            f"refs/nginx-qa/publications/{digest}",
        )

    @staticmethod
    def update_ref_mentions(
        arguments: tuple[str, ...] | list[str], kwargs: dict[str, object], ref: str
    ) -> bool:
        if "update-ref" not in arguments:
            return False
        material = "\0".join(str(argument) for argument in arguments)
        stdin_text = kwargs.get("stdin_text")
        if isinstance(stdin_text, str):
            material = f"{material}\0{stdin_text}"
        return ref in material

    def read_request(self, assignment_id: str, *, repository_id: str = "primary"):
        return WorkspaceRequest(
            project_id="project-1",
            sprint_id="sprint-1",
            node_id="reader",
            assignment_id=assignment_id,
            repository_id=repository_id,
            source_commit=self.source_commit,
            access="read",
        )

    def write_request(
        self,
        assignment_id: str,
        *,
        branch: str = "agent/work",
        policy: str = "create",
        expected_head: str | None = None,
    ):
        return WorkspaceRequest(
            project_id="project-1",
            sprint_id="sprint-1",
            node_id="writer",
            assignment_id=assignment_id,
            repository_id="primary",
            source_commit=self.source_commit,
            access="write",
            assigned_branch=branch,
            existing_branch_policy=policy,
            expected_branch_head=expected_head,
        )

    def test_publication_intent_lock_path_fits_receipt_lock_path_budget(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key,
            "agent/publication-path-budget",
            "assignment-publication-path-budget",
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        intent_lock = mirror.mirror_path / f"{intent_ref}.lock"
        receipt_lock = mirror.mirror_path / f"{receipt_ref}.lock"

        self.assertTrue(intent_ref.startswith("refs/nginx-qa/intents/"))
        self.assertLessEqual(len(str(intent_lock)), len(str(receipt_lock)))

    def test_aliases_share_one_bare_mirror_with_separate_remote_namespace(self) -> None:
        primary = self.provider.ensure_mirror("primary")
        alias = self.provider.ensure_mirror("alias")
        self.assertEqual(primary.mirror_path, alias.mirror_path)
        self.assertEqual(
            self.git(
                "--git-dir",
                str(primary.mirror_path),
                "rev-parse",
                "--is-bare-repository",
            ),
            "true",
        )
        self.assertEqual(
            self.git(
                "--git-dir",
                str(primary.mirror_path),
                "rev-parse",
                "refs/remotes/origin/main",
            ),
            self.source_commit,
        )
        local_head = self.git(
            "--git-dir",
            str(primary.mirror_path),
            "show-ref",
            "--verify",
            "refs/heads/main",
            check=False,
        )
        self.assertEqual(local_head, "")
        self.assertEqual(primary.fetch_result.returncode, 0)

    def test_sha256_remote_materializes_sha256_mirror_and_workspace(self) -> None:
        source = self.base / "sha256-source"
        source.mkdir()
        self.git(
            "init",
            "--object-format=sha256",
            "--initial-branch=main",
            cwd=source,
        )
        self.git("config", "user.name", "Managed Test", cwd=source)
        self.git("config", "user.email", "managed@example.invalid", cwd=source)
        (source / "tracked.txt").write_text("sha256\n", encoding="utf-8")
        self.git("add", "tracked.txt", cwd=source)
        self.git("commit", "-m", "sha256 initial", cwd=source)
        source_commit = self.git("rev-parse", "HEAD", cwd=source)
        self.assertEqual(len(source_commit), 64)

        remote = self.base / "sha256-upstream.git"
        self.git("clone", "--bare", str(source), str(remote))
        managed_root = self.base / "sha256-managed"
        spec = RepositorySpec(
            "sha256",
            "example.invalid/acme/sha256-repository",
            str(remote),
        )
        provider = ManagedGitProvider(
            managed_root,
            {"sha256": spec},
            allow_local_transport=True,
        )
        manager = ManagedWorkspaceManager(
            managed_root,
            provider,
            BranchLeaseStore(managed_root / "leases"),
            install_roots=(),
        )
        workspace = manager.prepare(
            WorkspaceRequest(
                project_id="project-1",
                sprint_id="sprint-1",
                node_id="sha256-reader",
                assignment_id="sha256-assignment",
                repository_id="sha256",
                source_commit=source_commit,
                access="read",
            )
        )
        mirror = provider.ensure_mirror("sha256", fetch=False)
        self.assertEqual(mirror.object_format, "sha256")
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                "--show-object-format",
            ),
            "sha256",
        )
        self.assertEqual(
            self.git("rev-parse", "--show-object-format", cwd=workspace.expected_root),
            "sha256",
        )
        self.assertEqual(workspace.head_commit, source_commit)

    def test_sha256_publication_creates_and_replays_intent_and_receipt(self) -> None:
        source = self.base / "sha256-publication-source"
        source.mkdir()
        self.git(
            "init",
            "--object-format=sha256",
            "--initial-branch=main",
            cwd=source,
        )
        self.git("config", "user.name", "Managed Test", cwd=source)
        self.git("config", "user.email", "managed@example.invalid", cwd=source)
        (source / "tracked.txt").write_text("sha256 publication\n", encoding="utf-8")
        self.git("add", "tracked.txt", cwd=source)
        self.git("commit", "-m", "sha256 publication initial", cwd=source)
        source_commit = self.git("rev-parse", "HEAD", cwd=source)

        remote = self.base / "sha256-publication-upstream.git"
        self.git("clone", "--bare", str(source), str(remote))
        managed_root = self.base / "sha256-publication-managed"
        spec = RepositorySpec(
            "sha256-publication",
            "example.invalid/acme/sha256-publication-repository",
            str(remote),
        )
        provider = ManagedGitProvider(
            managed_root,
            {"sha256-publication": spec},
            allow_local_transport=True,
        )
        mirror = provider.ensure_mirror("sha256-publication")
        branch = "agent/sha256-publication"
        branch_ref = f"refs/heads/{branch}"
        assignment_id = "assignment-sha256-publication"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)

        first_result = provider.ensure_local_branch_publication(
            mirror,
            branch,
            selected_head=source_commit,
            publication_id=publication_id,
            assignment_id=assignment_id,
            policy="create",
            source_commit=source_commit,
            expected_branch_head=None,
        )
        intent_object = provider._read_ref(mirror, intent_ref)

        self.assertEqual(mirror.object_format, "sha256")
        self.assertEqual(len(source_commit), 64)
        self.assertEqual(first_result, receipt_ref)
        self.assertIsNotNone(intent_object)
        self.assertEqual(len(str(intent_object)), 64)
        self.assertEqual(provider._read_ref(mirror, branch_ref), source_commit)
        self.assertEqual(provider._read_ref(mirror, receipt_ref), source_commit)
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "cat-file",
                "-t",
                str(intent_object),
            ),
            "blob",
        )

        replay_result = provider.ensure_local_branch_publication(
            mirror,
            branch,
            selected_head=source_commit,
            publication_id=publication_id,
            assignment_id=assignment_id,
            policy="create",
            source_commit=source_commit,
            expected_branch_head=None,
        )

        self.assertEqual(replay_result, receipt_ref)
        self.assertEqual(provider._read_ref(mirror, intent_ref), intent_object)
        self.assertEqual(provider._read_ref(mirror, branch_ref), source_commit)
        self.assertEqual(provider._read_ref(mirror, receipt_ref), source_commit)

    def test_prepare_uses_one_frozen_registry_snapshot(self) -> None:
        calls = 0
        spec = RepositorySpec(
            "snapshot",
            "example.invalid/acme/snapshot-repository",
            str(self.remote),
        )

        def registry(repository_id: str) -> RepositorySpec:
            nonlocal calls
            calls += 1
            self.assertEqual(repository_id, "snapshot")
            if calls != 1:
                raise AssertionError("repository registry was resolved more than once")
            return spec

        managed_root = self.base / "snapshot-managed"
        provider = ManagedGitProvider(
            managed_root,
            registry,
            allow_local_transport=True,
        )
        manager = ManagedWorkspaceManager(
            managed_root,
            provider,
            BranchLeaseStore(managed_root / "leases"),
            install_roots=(),
        )
        workspace = manager.prepare(
            WorkspaceRequest(
                project_id="project-1",
                sprint_id="sprint-1",
                node_id="snapshot-reader",
                assignment_id="snapshot-assignment",
                repository_id="snapshot",
                source_commit=self.source_commit,
                access="read",
            )
        )
        self.assertEqual(calls, 1)
        self.assertEqual(workspace.repository_remote, spec.canonical_remote)

    def test_legacy_mirror_refspec_is_rejected_before_fetch(self) -> None:
        managed = self.base / "legacy-managed"
        provider = ManagedGitProvider(
            managed,
            {
                "primary": RepositorySpec(
                    "primary", self.canonical_remote, str(self.remote)
                )
            },
            allow_local_transport=True,
        )
        mirror = (
            managed
            / "repositories"
            / f"{mirror_storage_key(self.canonical_remote)}.git"
        )
        mirror.parent.mkdir(parents=True)
        self.git("clone", "--mirror", str(self.remote), str(mirror))
        self.git(
            "--git-dir",
            str(mirror),
            "update-ref",
            "refs/nginx-qa/pins/evidence",
            self.source_commit,
        )
        with self.assertRaises(ManagedGitError) as raised:
            provider.ensure_mirror("primary")
        self.assertEqual(raised.exception.code, "REPOSITORY_MIRROR_INVALID")
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror),
                "rev-parse",
                "refs/nginx-qa/pins/evidence",
            ),
            self.source_commit,
        )

    def test_mirror_alternate_object_store_is_rejected_before_fetch(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        alternates = mirror.mirror_path / "objects" / "info" / "alternates"
        alternates.write_text(
            str(self.remote / "objects") + "\n", encoding="utf-8"
        )
        before = alternates.read_bytes()
        with self.assertRaises(ManagedGitError) as raised:
            self.provider.ensure_mirror("primary")
        self.assertEqual(raised.exception.code, "REPOSITORY_MIRROR_INVALID")
        self.assertEqual(alternates.read_bytes(), before)

    def test_assignment_pin_survives_force_move_and_aggressive_gc(self) -> None:
        workspace = self.manager.prepare(self.read_request("reader-pin"))
        mirror = self.provider.ensure_mirror("primary", fetch=False)
        pin_lines = self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "for-each-ref",
            "--format=%(objectname)",
            "refs/nginx-qa/pins",
        ).splitlines()
        self.assertIn(self.source_commit, pin_lines)

        self.git("checkout", "--orphan", "replacement", cwd=self.source)
        self.git("rm", "-rf", ".", cwd=self.source)
        (self.source / "replacement.txt").write_text("replacement\n", encoding="utf-8")
        self.git("add", "replacement.txt", cwd=self.source)
        self.git("commit", "-m", "unrelated replacement", cwd=self.source)
        self.git(
            "push",
            "--force",
            "upstream",
            "HEAD:refs/heads/main",
            cwd=self.source,
        )
        self.provider.ensure_mirror("primary")
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "reflog",
            "expire",
            "--expire=now",
            "--all",
        )
        self.git("--git-dir", str(mirror.mirror_path), "gc", "--prune=now")
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "cat-file",
                "-t",
                workspace.source_commit,
            ),
            "commit",
        )

    def test_moved_and_deleted_tags_are_fetched_exactly(self) -> None:
        self.git("tag", "release", self.source_commit, cwd=self.source)
        self.git("push", "upstream", "refs/tags/release", cwd=self.source)
        mirror = self.provider.ensure_mirror("primary")
        self.assertEqual(
            self.provider.resolve_commit(mirror, "refs/tags/release"),
            self.source_commit,
        )

        moved = self.commit("tag moved\n", "move tag target")
        self.git("tag", "--force", "release", moved, cwd=self.source)
        self.git("push", "--force", "upstream", "refs/tags/release", cwd=self.source)
        self.provider.ensure_mirror("primary")
        self.assertEqual(
            self.provider.resolve_commit(mirror, "refs/tags/release"), moved
        )

        self.git("push", "upstream", ":refs/tags/release", cwd=self.source)
        self.provider.ensure_mirror("primary")
        with self.assertRaises(ManagedGitError) as raised:
            self.provider.resolve_commit(mirror, "refs/tags/release")
        self.assertEqual(raised.exception.code, "SOURCE_COMMIT_NOT_FOUND")

    def test_concurrent_readers_get_distinct_detached_clean_workspaces(self) -> None:
        requests = [self.read_request(f"reader-{index}") for index in range(5)]
        with ThreadPoolExecutor(max_workers=5) as pool:
            workspaces = list(pool.map(self.manager.prepare, requests))
        self.assertEqual(len({item.expected_root for item in workspaces}), 5)
        self.assertEqual(self.leases.active_writers(), ())
        for workspace in workspaces:
            self.assertEqual(workspace.head_commit, self.source_commit)
            self.assertIsNone(workspace.assigned_branch)
            self.assertIsNone(workspace.branch_lease_id)
            self.assertEqual(
                self.git(
                    "-C",
                    str(workspace.expected_root),
                    "symbolic-ref",
                    "--short",
                    "-q",
                    "HEAD",
                    check=False,
                ),
                "",
            )
            self.assertEqual(
                self.git("-C", str(workspace.expected_root), "status", "--porcelain"),
                "",
            )

    def test_write_workspace_is_exact_and_replay_uses_frozen_record(self) -> None:
        request = self.write_request("writer-1")
        first = self.manager.prepare(request)
        replay = self.manager.prepare(request, expected_record=first)
        self.assertEqual(replay.expected_root, first.expected_root)
        self.assertEqual(replay.initial_head_commit, self.source_commit)
        self.assertEqual(replay.assigned_branch, "agent/work")
        self.assertTrue(str(first.expected_root).endswith(
            str(Path("projects/project-1/sprints/sprint-1/nodes/writer/writer-1"))
        ))
        self.assertEqual(len(self.leases.active_writers()), 1)
        mirror = self.provider.ensure_mirror("primary", fetch=False)
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                "refs/heads/agent/work",
            ),
            self.source_commit,
        )

    def test_branch_lookup_is_exact_not_a_ref_prefix(self) -> None:
        self.git(
            "push",
            "upstream",
            f"{self.source_commit}:refs/heads/agent/child",
            cwd=self.source,
        )
        workspace = self.manager.prepare(
            self.write_request("writer-prefix", branch="agent", policy="create")
        )
        self.assertEqual(workspace.assigned_branch, "agent")

    def test_case_only_branch_alias_fails_closed(self) -> None:
        self.git(
            "push",
            "upstream",
            f"{self.source_commit}:refs/heads/Agent/Case",
            cwd=self.source,
        )
        with self.assertRaises(ManagedGitError) as raised:
            self.manager.prepare(
                self.write_request(
                    "writer-case", branch="agent/case", policy="create"
                )
            )
        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")

    def test_unicode_casefold_branch_alias_fails_closed(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            "refs/heads/ß",
            self.source_commit,
        )
        with self.assertRaises(ManagedGitError) as raised:
            self.manager.prepare(
                self.write_request("writer-unicode-case", branch="SS", policy="create")
            )
        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")

    def test_large_ref_namespace_is_streamed_without_truncating_identity(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        updates = "".join(
            f"update refs/heads/noise/{index:04d}-{'x' * 40} {self.source_commit}\n"
            for index in range(700)
        )
        completed = subprocess.run(
            ("git", "--git-dir", str(mirror.mirror_path), "update-ref", "--stdin"),
            input=updates.encode("ascii"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(
            completed.returncode, 0, completed.stderr.decode("utf-8", errors="replace")
        )
        self.assertEqual(self.provider.branch_heads(mirror, "agent/target"), {})

    def test_local_ref_directory_file_collisions_fail_before_lease(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            "refs/heads/agent",
            self.source_commit,
        )
        with self.assertRaises(ManagedGitError) as ancestor:
            self.manager.prepare(
                self.write_request(
                    "writer-df-child", branch="agent/child", policy="create"
                )
            )
        self.assertEqual(ancestor.exception.code, "BRANCH_ALREADY_EXISTS")
        self.assertEqual(self.leases.active_writers(), ())

        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            "-d",
            "refs/heads/agent",
        )
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            "refs/heads/agent/child",
            self.source_commit,
        )
        with self.assertRaises(ManagedGitError) as descendant:
            self.manager.prepare(
                self.write_request(
                    "writer-df-parent", branch="agent", policy="create"
                )
            )
        self.assertEqual(descendant.exception.code, "BRANCH_ALREADY_EXISTS")
        self.assertEqual(self.leases.active_writers(), ())

    def test_resume_selects_existing_descendant(self) -> None:
        descendant = self.commit("two\n", "descendant")
        self.git(
            "push",
            "upstream",
            "HEAD:refs/heads/agent/resume",
            cwd=self.source,
        )
        workspace = self.manager.prepare(
            self.write_request(
                "writer-resume", branch="agent/resume", policy="resume"
            )
        )
        self.assertEqual(workspace.source_commit, self.source_commit)
        self.assertEqual(workspace.initial_head_commit, descendant)
        self.assertEqual(workspace.head_commit, descendant)

    def test_resume_publication_fast_forwards_reused_branch_with_receipt(self) -> None:
        branch = "agent/reused"
        assignment_id = "assignment-reused-generation-2"
        self.git(
            "push",
            "upstream",
            f"{self.source_commit}:refs/heads/{branch}",
            cwd=self.source,
        )
        mirror = self.provider.ensure_mirror("primary")
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            f"refs/heads/{branch}",
            self.source_commit,
        )
        descendant = self.commit("two\n", "accepted continuation")
        self.git("push", "upstream", "HEAD:refs/heads/main", cwd=self.source)
        mirror = self.provider.ensure_mirror("primary")
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )

        publication_ref = self.provider.ensure_local_branch_publication(
            mirror,
            branch,
            selected_head=descendant,
            publication_id=publication_id,
            assignment_id=assignment_id,
            policy="resume",
            source_commit=descendant,
            expected_branch_head=None,
        )

        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                f"refs/heads/{branch}",
            ),
            descendant,
        )
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                f"refs/remotes/origin/{branch}",
            ),
            self.source_commit,
        )
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                publication_ref,
            ),
            descendant,
        )
        self.assertEqual(
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=descendant,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="resume",
                source_commit=descendant,
                expected_branch_head=None,
            ),
            publication_ref,
        )

    def test_resume_publication_still_rejects_divergent_reused_branch(self) -> None:
        descendant = self.commit("two\n", "accepted continuation")
        self.git("push", "upstream", "HEAD:refs/heads/main", cwd=self.source)
        self.git("checkout", "--orphan", "unrelated", cwd=self.source)
        self.git("rm", "-rf", ".", cwd=self.source)
        (self.source / "unrelated.txt").write_text("unrelated\n", encoding="utf-8")
        self.git("add", "unrelated.txt", cwd=self.source)
        self.git("commit", "-m", "unrelated publication", cwd=self.source)
        unrelated = self.git("rev-parse", "HEAD", cwd=self.source)
        self.git(
            "push",
            "upstream",
            "HEAD:refs/heads/unrelated-publication",
            cwd=self.source,
        )
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/diverged-publication"
        assignment_id = "assignment-diverged-generation-2"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            f"refs/heads/{branch}",
            unrelated,
        )

        with self.assertRaises(ManagedGitError) as raised:
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=descendant,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="resume",
                source_commit=descendant,
                expected_branch_head=None,
            )

        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                f"refs/heads/{branch}",
            ),
            unrelated,
        )

    def test_publication_retry_completes_after_branch_write_without_receipt(
        self,
    ) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/publication-retry"
        branch_ref = f"refs/heads/{branch}"
        assignment_id = "assignment-publication-retry"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        original_run = self.provider.runner.run
        injected = False

        def fail_after_branch(arguments, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal injected
            result = original_run(arguments, **kwargs)
            if not injected and self.update_ref_mentions(arguments, kwargs, branch_ref):
                injected = True
                raise SimulatedPublicationCrash(
                    "injected crash after branch write and before receipt"
                )
            return result

        with patch.object(self.provider.runner, "run", side_effect=fail_after_branch):
            with self.assertRaises(SimulatedPublicationCrash):
                self.provider.ensure_local_branch_publication(
                    mirror,
                    branch,
                    selected_head=self.source_commit,
                    publication_id=publication_id,
                    assignment_id=assignment_id,
                    policy="create",
                    source_commit=self.source_commit,
                    expected_branch_head=None,
                )
        self.assertTrue(injected)
        intent_object = self.provider._read_ref(mirror, intent_ref)
        self.assertIsNotNone(intent_object)
        self.assertEqual(
            self.provider._read_ref(mirror, branch_ref), self.source_commit
        )
        self.assertIsNone(self.provider._read_ref(mirror, receipt_ref))

        replay_ref = self.provider.ensure_local_branch_publication(
            mirror,
            branch,
            selected_head=self.source_commit,
            publication_id=publication_id,
            assignment_id=assignment_id,
            policy="create",
            source_commit=self.source_commit,
            expected_branch_head=None,
        )

        self.assertEqual(replay_ref, receipt_ref)
        self.assertEqual(self.provider._read_ref(mirror, intent_ref), intent_object)
        self.assertEqual(
            self.provider._read_ref(mirror, branch_ref), self.source_commit
        )
        self.assertEqual(
            self.provider._read_ref(mirror, receipt_ref), self.source_commit
        )

    def test_publication_retry_reclaims_only_matching_receipt_lock(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/publication-lock-retry"
        branch_ref = f"refs/heads/{branch}"
        assignment_id = "assignment-publication-lock-retry"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        original_run = self.provider.runner.run

        def fail_after_branch(arguments, **kwargs):  # type: ignore[no-untyped-def]
            result = original_run(arguments, **kwargs)
            if self.update_ref_mentions(arguments, kwargs, branch_ref):
                raise SimulatedPublicationCrash("injected crash after branch write")
            return result

        with patch.object(self.provider.runner, "run", side_effect=fail_after_branch):
            with self.assertRaises(SimulatedPublicationCrash):
                self.provider.ensure_local_branch_publication(
                    mirror,
                    branch,
                    selected_head=self.source_commit,
                    publication_id=publication_id,
                    assignment_id=assignment_id,
                    policy="create",
                    source_commit=self.source_commit,
                    expected_branch_head=None,
                )
        self.assertIsNotNone(self.provider._read_ref(mirror, intent_ref))
        self.assertEqual(
            self.provider._read_ref(mirror, branch_ref), self.source_commit
        )
        self.assertIsNone(self.provider._read_ref(mirror, receipt_ref))

        receipt_lock = mirror.mirror_path / f"{receipt_ref}.lock"
        receipt_lock.parent.mkdir(parents=True, exist_ok=True)
        receipt_lock.write_bytes(f"{self.source_commit}\n".encode("ascii"))
        unrelated_lock = (
            mirror.mirror_path
            / "refs/nginx-qa/publications"
            / f"{'f' * 64}.lock"
        )
        unrelated_lock.write_bytes(b"unrelated lock must survive")

        self.assertEqual(
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=self.source_commit,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="create",
                source_commit=self.source_commit,
                expected_branch_head=None,
            ),
            receipt_ref,
        )
        self.assertFalse(receipt_lock.exists())
        self.assertEqual(unrelated_lock.read_bytes(), b"unrelated lock must survive")
        self.assertEqual(
            self.provider._read_ref(mirror, receipt_ref), self.source_commit
        )

    def test_publication_retry_rejects_mismatched_receipt_lock_unchanged(
        self,
    ) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/publication-mismatched-receipt-lock"
        branch_ref = f"refs/heads/{branch}"
        assignment_id = "assignment-publication-mismatched-receipt-lock"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        original_run = self.provider.runner.run

        def fail_after_branch(arguments, **kwargs):  # type: ignore[no-untyped-def]
            result = original_run(arguments, **kwargs)
            if self.update_ref_mentions(arguments, kwargs, branch_ref):
                raise SimulatedPublicationCrash("injected crash after branch write")
            return result

        with patch.object(self.provider.runner, "run", side_effect=fail_after_branch):
            with self.assertRaises(SimulatedPublicationCrash):
                self.provider.ensure_local_branch_publication(
                    mirror,
                    branch,
                    selected_head=self.source_commit,
                    publication_id=publication_id,
                    assignment_id=assignment_id,
                    policy="create",
                    source_commit=self.source_commit,
                    expected_branch_head=None,
                )
        intent_object = self.provider._read_ref(mirror, intent_ref)
        self.assertIsNotNone(intent_object)
        self.assertEqual(
            self.provider._read_ref(mirror, branch_ref), self.source_commit
        )
        self.assertIsNone(self.provider._read_ref(mirror, receipt_ref))

        different_valid_oid = self.commit(
            "different receipt lock owner\n", "different receipt lock owner"
        )
        self.assertNotEqual(different_valid_oid, self.source_commit)
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "fetch",
            str(self.source),
            different_valid_oid,
        )
        receipt_lock = mirror.mirror_path / f"{receipt_ref}.lock"
        receipt_lock.parent.mkdir(parents=True, exist_ok=True)
        lock_contents = f"{different_valid_oid}\n".encode("ascii")
        receipt_lock.write_bytes(lock_contents)

        with self.assertRaises(ManagedGitError) as raised:
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=self.source_commit,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="create",
                source_commit=self.source_commit,
                expected_branch_head=None,
            )

        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")
        self.assertTrue(receipt_lock.is_file())
        self.assertEqual(receipt_lock.read_bytes(), lock_contents)
        self.assertEqual(self.provider._read_ref(mirror, intent_ref), intent_object)
        self.assertEqual(
            self.provider._read_ref(mirror, branch_ref), self.source_commit
        )
        self.assertIsNone(self.provider._read_ref(mirror, receipt_ref))

    def test_same_sha_branch_without_publication_intent_fails_closed(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/unowned-same-sha"
        assignment_id = "assignment-unowned-same-sha"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            f"refs/heads/{branch}",
            self.source_commit,
        )

        with self.assertRaises(ManagedGitError) as raised:
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=self.source_commit,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="create",
                source_commit=self.source_commit,
                expected_branch_head=None,
            )

        self.assertEqual(raised.exception.code, "BRANCH_ALREADY_EXISTS")
        self.assertIsNone(self.provider._read_ref(mirror, intent_ref))
        self.assertIsNone(self.provider._read_ref(mirror, receipt_ref))

    def test_publication_intent_from_another_assignment_fails_closed(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/mismatched-intent"
        branch_ref = f"refs/heads/{branch}"
        first_assignment = "assignment-intent-owner"
        first_publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, first_assignment
        )
        first_intent_ref, _ = self.publication_refs(mirror, first_publication_id)
        second_assignment = "assignment-intent-impostor"
        second_publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, second_assignment
        )
        second_intent_ref, second_receipt_ref = self.publication_refs(
            mirror, second_publication_id
        )
        original_run = self.provider.runner.run

        def fail_before_branch(arguments, **kwargs):  # type: ignore[no-untyped-def]
            if self.update_ref_mentions(arguments, kwargs, branch_ref):
                raise ManagedGitError(
                    "GIT_COMMAND_TIMEOUT", "injected failure before branch write"
                )
            return original_run(arguments, **kwargs)

        with patch.object(self.provider.runner, "run", side_effect=fail_before_branch):
            with self.assertRaises(ManagedGitError):
                self.provider.ensure_local_branch_publication(
                    mirror,
                    branch,
                    selected_head=self.source_commit,
                    publication_id=first_publication_id,
                    assignment_id=first_assignment,
                    policy="create",
                    source_commit=self.source_commit,
                    expected_branch_head=None,
                )
        intent_object = self.provider._read_ref(mirror, first_intent_ref)
        self.assertIsNotNone(intent_object)
        self.assertIsNone(self.provider._read_ref(mirror, branch_ref))
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            second_intent_ref,
            str(intent_object),
        )

        with self.assertRaises(ManagedGitError) as raised:
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=self.source_commit,
                publication_id=second_publication_id,
                assignment_id=second_assignment,
                policy="create",
                source_commit=self.source_commit,
                expected_branch_head=None,
            )

        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")
        self.assertIsNone(self.provider._read_ref(mirror, branch_ref))
        self.assertIsNone(self.provider._read_ref(mirror, second_receipt_ref))

    def test_publication_intent_without_branch_resumes(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/intent-only-retry"
        branch_ref = f"refs/heads/{branch}"
        assignment_id = "assignment-intent-only-retry"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        original_run = self.provider.runner.run

        def fail_before_branch(arguments, **kwargs):  # type: ignore[no-untyped-def]
            if self.update_ref_mentions(arguments, kwargs, branch_ref):
                raise ManagedGitError(
                    "GIT_COMMAND_TIMEOUT", "injected failure before branch write"
                )
            return original_run(arguments, **kwargs)

        with patch.object(self.provider.runner, "run", side_effect=fail_before_branch):
            with self.assertRaises(ManagedGitError):
                self.provider.ensure_local_branch_publication(
                    mirror,
                    branch,
                    selected_head=self.source_commit,
                    publication_id=publication_id,
                    assignment_id=assignment_id,
                    policy="create",
                    source_commit=self.source_commit,
                    expected_branch_head=None,
                )
        self.assertIsNotNone(self.provider._read_ref(mirror, intent_ref))
        self.assertIsNone(self.provider._read_ref(mirror, branch_ref))
        self.assertIsNone(self.provider._read_ref(mirror, receipt_ref))

        self.assertEqual(
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=self.source_commit,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="create",
                source_commit=self.source_commit,
                expected_branch_head=None,
            ),
            receipt_ref,
        )
        self.assertEqual(
            self.provider._read_ref(mirror, branch_ref), self.source_commit
        )
        self.assertEqual(
            self.provider._read_ref(mirror, receipt_ref), self.source_commit
        )

    def test_publication_intent_retry_reclaims_matching_branch_lock(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/intent-branch-lock-retry"
        branch_ref = f"refs/heads/{branch}"
        assignment_id = "assignment-intent-branch-lock-retry"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        original_run = self.provider.runner.run

        def fail_before_branch(arguments, **kwargs):  # type: ignore[no-untyped-def]
            if self.update_ref_mentions(arguments, kwargs, branch_ref):
                raise ManagedGitError(
                    "GIT_COMMAND_TIMEOUT", "injected failure before branch write"
                )
            return original_run(arguments, **kwargs)

        with patch.object(self.provider.runner, "run", side_effect=fail_before_branch):
            with self.assertRaises(ManagedGitError):
                self.provider.ensure_local_branch_publication(
                    mirror,
                    branch,
                    selected_head=self.source_commit,
                    publication_id=publication_id,
                    assignment_id=assignment_id,
                    policy="create",
                    source_commit=self.source_commit,
                    expected_branch_head=None,
                )
        intent_object = self.provider._read_ref(mirror, intent_ref)
        self.assertIsNotNone(intent_object)
        self.assertIsNone(self.provider._read_ref(mirror, branch_ref))
        self.assertIsNone(self.provider._read_ref(mirror, receipt_ref))

        branch_lock = mirror.mirror_path / f"{branch_ref}.lock"
        branch_lock.parent.mkdir(parents=True, exist_ok=True)
        branch_lock.write_bytes(f"{self.source_commit}\n".encode("ascii"))

        self.assertEqual(
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=self.source_commit,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="create",
                source_commit=self.source_commit,
                expected_branch_head=None,
            ),
            receipt_ref,
        )
        self.assertFalse(branch_lock.exists())
        self.assertEqual(self.provider._read_ref(mirror, intent_ref), intent_object)
        self.assertEqual(
            self.provider._read_ref(mirror, branch_ref), self.source_commit
        )
        self.assertEqual(
            self.provider._read_ref(mirror, receipt_ref), self.source_commit
        )

    def test_legacy_publication_receipt_replay_accepts_descendant_branch(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        branch = "agent/publication-descendant-replay"
        branch_ref = f"refs/heads/{branch}"
        assignment_id = "assignment-publication-descendant-replay"
        publication_id = deterministic_branch_lease_id(
            mirror.mirror_storage_key, branch, assignment_id
        )
        intent_ref, receipt_ref = self.publication_refs(mirror, publication_id)
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            branch_ref,
            self.source_commit,
        )
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            receipt_ref,
            self.source_commit,
        )
        self.assertIsNone(self.provider._read_ref(mirror, intent_ref))
        descendant = self.commit("two\n", "publication descendant")
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "fetch",
            str(self.source),
            descendant,
        )
        self.git(
            "--git-dir",
            str(mirror.mirror_path),
            "update-ref",
            branch_ref,
            descendant,
            self.source_commit,
        )

        self.assertEqual(
            self.provider.ensure_local_branch_publication(
                mirror,
                branch,
                selected_head=self.source_commit,
                publication_id=publication_id,
                assignment_id=assignment_id,
                policy="create",
                source_commit=self.source_commit,
                expected_branch_head=None,
            ),
            receipt_ref,
        )
        self.assertIsNone(self.provider._read_ref(mirror, intent_ref))
        self.assertEqual(self.provider._read_ref(mirror, branch_ref), descendant)

    def test_create_rejects_an_existing_remote_branch(self) -> None:
        self.git(
            "push",
            "upstream",
            f"{self.source_commit}:refs/heads/agent/existing",
            cwd=self.source,
        )
        with self.assertRaises(BranchLeaseError) as raised:
            self.manager.prepare(
                self.write_request(
                    "writer-existing", branch="agent/existing", policy="create"
                )
            )
        self.assertEqual(raised.exception.code, "BRANCH_ALREADY_EXISTS")

    def test_branch_is_revalidated_under_fetch_lock_before_reservation(self) -> None:
        request = self.write_request(
            "writer-race", branch="agent/race", policy="create"
        )
        original_acquire = self.leases.acquire_write

        def acquire_then_publish(lease_request):
            lease = original_acquire(lease_request)
            self.git(
                "push",
                "upstream",
                f"{self.source_commit}:refs/heads/agent/race",
                cwd=self.source,
            )
            self.provider.ensure_mirror("primary")
            return lease

        self.leases.acquire_write = acquire_then_publish
        with self.assertRaises(ManagedGitError) as raised:
            self.manager.prepare(request)
        self.assertEqual(raised.exception.code, "BRANCH_ALREADY_EXISTS")
        mirror = self.provider.ensure_mirror("primary", fetch=False)
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                "--verify",
                "--quiet",
                "refs/heads/agent/race",
                check=False,
            ),
            "",
        )
        self.assertFalse(self.manager.expected_root(request).exists())

    def test_resume_preserves_an_unrelated_branch_and_reports_divergence(self) -> None:
        self.git("checkout", "--orphan", "unrelated", cwd=self.source)
        self.git("rm", "-rf", ".", cwd=self.source)
        (self.source / "other.txt").write_text("other\n", encoding="utf-8")
        self.git("add", "other.txt", cwd=self.source)
        self.git("commit", "-m", "unrelated", cwd=self.source)
        unrelated = self.git("rev-parse", "HEAD", cwd=self.source)
        self.git(
            "push",
            "upstream",
            "HEAD:refs/heads/agent/diverged",
            cwd=self.source,
        )
        request = self.write_request(
            "writer-diverged", branch="agent/diverged", policy="resume"
        )
        expected_root = self.manager.expected_root(request)
        with self.assertRaises(BranchLeaseError) as raised:
            self.manager.prepare(request)
        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")
        self.assertFalse(expected_root.exists())
        self.assertEqual(
            self.git("--git-dir", str(self.remote), "rev-parse", "agent/diverged"),
            unrelated,
        )

    def test_dirty_workspace_is_preserved_byte_for_byte(self) -> None:
        request = self.write_request("writer-dirty", branch="agent/dirty")
        workspace = self.manager.prepare(request)
        untracked = workspace.expected_root / "do-not-delete.txt"
        untracked.write_bytes(b"preserve me\r\n")
        before_stat = untracked.stat()
        before_status = self.git(
            "-C", str(workspace.expected_root), "status", "--porcelain=v1"
        )
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "WORKSPACE_DIRTY")
        self.assertEqual(untracked.read_bytes(), b"preserve me\r\n")
        self.assertEqual(untracked.stat().st_mtime_ns, before_stat.st_mtime_ns)
        self.assertEqual(
            self.git("-C", str(workspace.expected_root), "status", "--porcelain=v1"),
            before_status,
        )

    def test_changed_workspace_head_is_not_reset_or_checked_out(self) -> None:
        request = self.write_request("writer-head", branch="agent/head")
        workspace = self.manager.prepare(request)
        self.git("config", "user.name", "Worker", cwd=workspace.expected_root)
        self.git("config", "user.email", "worker@example.invalid", cwd=workspace.expected_root)
        (workspace.expected_root / "worker.txt").write_text("work\n", encoding="utf-8")
        self.git("add", "worker.txt", cwd=workspace.expected_root)
        self.git("commit", "-m", "worker commit", cwd=workspace.expected_root)
        changed_head = self.git("rev-parse", "HEAD", cwd=workspace.expected_root)

        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")
        self.assertEqual(
            self.git("rev-parse", "HEAD", cwd=workspace.expected_root), changed_head
        )
        self.assertTrue((workspace.expected_root / "worker.txt").is_file())


if __name__ == "__main__":
    unittest.main()
