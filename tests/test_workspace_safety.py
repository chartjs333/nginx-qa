from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import unittest

from nginx_qa.branch_leases import BranchLeaseStore
from nginx_qa.git_provider import ManagedGitError, ManagedGitProvider, RepositorySpec
from nginx_qa.workspace_manager import (
    ManagedWorkspaceManager,
    WorkspaceError,
    WorkspaceRequest,
)
from tests.managed_workspace_support import ManagedWorkspaceFixture


class WorkspaceSafetyTests(ManagedWorkspaceFixture, unittest.TestCase):
    def redirect_directory(self, path: Path, target: Path) -> None:
        self.assertTrue(path.is_dir())
        self.assertEqual(tuple(path.iterdir()), ())
        path.rmdir()
        target.mkdir(parents=True)
        if os.name == "nt":
            completed = subprocess.run(
                ("cmd.exe", "/d", "/c", "mklink", "/J", str(path), str(target)),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            if completed.returncode != 0:
                self.fail(f"could not create test junction: {completed.stderr}")
        else:
            path.symlink_to(target, target_is_directory=True)

    def read_request(self, assignment_id: str, **overrides) -> WorkspaceRequest:
        values = {
            "project_id": "project-safe",
            "sprint_id": "sprint-safe",
            "node_id": "reader",
            "assignment_id": assignment_id,
            "repository_id": "primary",
            "source_commit": self.source_commit,
            "access": "read",
        }
        values.update(overrides)
        return WorkspaceRequest(**values)

    def test_parent_git_repository_is_rejected_before_workspace_creation(self) -> None:
        foreign = self.base / "foreign-parent"
        foreign.mkdir()
        self.git("init", "--initial-branch=main", cwd=foreign)
        candidate = foreign / "managed"
        provider = ManagedGitProvider(
            candidate,
            {
                "primary": RepositorySpec(
                    "primary", self.canonical_remote, str(self.remote)
                )
            },
            allow_local_transport=True,
        )
        outside_leases = BranchLeaseStore(self.base / "outside-leases")
        with self.assertRaises(WorkspaceError) as raised:
            ManagedWorkspaceManager(
                candidate, provider, outside_leases, install_roots=()
            )
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        self.assertFalse(candidate.exists())

    def test_parent_repository_appearing_after_start_is_rejected(self) -> None:
        request = self.read_request("parent-appeared")
        expected = self.manager.expected_root(request)
        expected.parent.mkdir(parents=True)
        self.git("init", "--initial-branch=foreign", cwd=expected.parent)
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request)
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        self.assertFalse(expected.exists())

    def test_foreign_target_directory_is_preserved(self) -> None:
        request = self.read_request("foreign-directory")
        expected = self.manager.expected_root(request)
        expected.mkdir(parents=True)
        marker = expected / "keep.bin"
        marker.write_bytes(b"foreign workspace evidence")
        before = marker.stat().st_mtime_ns
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request)
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        self.assertEqual(marker.read_bytes(), b"foreign workspace evidence")
        self.assertEqual(marker.stat().st_mtime_ns, before)

    def test_foreign_write_target_does_not_create_a_mirror_branch_or_pin(self) -> None:
        request = WorkspaceRequest(
            project_id="project-safe",
            sprint_id="sprint-safe",
            node_id="writer",
            assignment_id="foreign-write",
            repository_id="primary",
            source_commit=self.source_commit,
            access="write",
            assigned_branch="agent/foreign",
            existing_branch_policy="create",
        )
        expected = self.manager.expected_root(request)
        expected.mkdir(parents=True)
        (expected / "evidence.txt").write_text("keep\n", encoding="utf-8")
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request)
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        mirror = self.provider.ensure_mirror("primary", fetch=False)
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "rev-parse",
                "--verify",
                "--quiet",
                "refs/heads/agent/foreign",
                check=False,
            ),
            "",
        )
        self.assertEqual(
            self.git(
                "--git-dir",
                str(mirror.mirror_path),
                "for-each-ref",
                "--format=%(refname)",
                "refs/nginx-qa/pins",
            ),
            "",
        )

    def test_git_dir_indirection_outside_workspace_is_rejected(self) -> None:
        request = self.read_request("external-git-dir")
        expected = self.manager.expected_root(request)
        expected.parent.mkdir(parents=True)
        external_git_dir = self.base / "external-git-dir"
        self.git(
            "init",
            "--separate-git-dir",
            str(external_git_dir),
            str(expected),
        )
        git_pointer = (expected / ".git").read_bytes()
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request)
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        self.assertEqual((expected / ".git").read_bytes(), git_pointer)
        self.assertTrue(external_git_dir.is_dir())

    def test_workspace_alternate_object_store_is_rejected_and_preserved(self) -> None:
        request = self.read_request("alternate-objects")
        workspace = self.manager.prepare(request)
        alternates = workspace.actual_git_dir / "objects" / "info" / "alternates"
        alternates.write_text(
            str(self.remote / "objects") + "\n", encoding="utf-8"
        )
        before = alternates.read_bytes()
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        self.assertEqual(alternates.read_bytes(), before)

    def test_nested_mirror_ref_redirect_is_rejected_before_ref_write(self) -> None:
        mirror = self.provider.ensure_mirror("primary")
        external_refs = self.base / "external-mirror-refs"
        self.redirect_directory(mirror.mirror_path / "refs" / "heads", external_refs)
        with self.assertRaises(ManagedGitError) as raised:
            self.provider.ensure_mirror("primary", fetch=False)
        self.assertEqual(raised.exception.code, "REPOSITORY_MIRROR_INVALID")
        self.assertEqual(tuple(external_refs.iterdir()), ())

    def test_nested_workspace_ref_redirect_is_rejected_and_preserved(self) -> None:
        request = self.read_request("nested-ref-redirect")
        workspace = self.manager.prepare(request)
        external_refs = self.base / "external-workspace-refs"
        self.redirect_directory(workspace.actual_git_dir / "refs" / "heads", external_refs)
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        self.assertEqual(tuple(external_refs.iterdir()), ())

    def test_workspace_remote_mismatch_is_preserved(self) -> None:
        request = self.read_request("wrong-remote")
        workspace = self.manager.prepare(request)
        other = self.base / "other.git"
        self.git("init", "--bare", str(other))
        self.git(
            "remote",
            "set-url",
            "origin",
            str(other),
            cwd=workspace.expected_root,
        )
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "REPOSITORY_IDENTITY_MISMATCH")
        self.assertEqual(
            self.git("remote", "get-url", "origin", cwd=workspace.expected_root),
            str(other),
        )

    def test_extra_push_url_is_rejected_and_preserved(self) -> None:
        request = self.read_request("extra-push-url")
        workspace = self.manager.prepare(request)
        other = self.base / "push-target.git"
        self.git("init", "--bare", str(other))
        self.git(
            "remote",
            "set-url",
            "--add",
            "--push",
            "origin",
            str(other),
            cwd=workspace.expected_root,
        )
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "REPOSITORY_IDENTITY_MISMATCH")
        self.assertEqual(
            self.git(
                "remote",
                "get-url",
                "--push",
                "--all",
                "origin",
                cwd=workspace.expected_root,
            ),
            str(other),
        )

    def test_dirty_verification_does_not_refresh_the_git_index(self) -> None:
        request = WorkspaceRequest(
            project_id="project-safe",
            sprint_id="sprint-safe",
            node_id="writer",
            assignment_id="dirty-index",
            repository_id="primary",
            source_commit=self.source_commit,
            access="write",
            assigned_branch="agent/dirty-index",
            existing_branch_policy="create",
        )
        workspace = self.manager.prepare(request)
        index = workspace.actual_git_dir / "index"
        (workspace.expected_root / "tracked.txt").write_text(
            "dirty tracked bytes\n", encoding="utf-8"
        )
        before_bytes = index.read_bytes()
        before_mtime = index.stat().st_mtime_ns
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "WORKSPACE_DIRTY")
        self.assertEqual(index.read_bytes(), before_bytes)
        self.assertEqual(index.stat().st_mtime_ns, before_mtime)

    def test_missing_frozen_workspace_is_not_recreated(self) -> None:
        request = self.read_request("missing-frozen")
        workspace = self.manager.prepare(request)
        original_root = workspace.expected_root
        renamed_root = original_root.with_name(original_root.name + "-evidence")
        original_root.rename(renamed_root)
        with self.assertRaises(WorkspaceError) as raised:
            self.manager.prepare(request, expected_record=workspace)
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")
        self.assertFalse(original_root.exists())
        self.assertTrue(renamed_root.is_dir())

    def test_tampered_frozen_workspace_facts_are_rejected(self) -> None:
        request = self.read_request("tampered-record")
        workspace = self.manager.prepare(request)
        tampered_records = (
            replace(
                workspace,
                actual_git_toplevel=self.base / "foreign-toplevel",
            ),
            replace(
                workspace,
                actual_git_dir=self.base / "foreign-git-dir",
            ),
            replace(workspace, head_commit="0" * len(workspace.head_commit)),
            replace(
                workspace,
                initial_head_commit="0" * len(workspace.initial_head_commit),
                head_commit="0" * len(workspace.head_commit),
            ),
        )
        for record in tampered_records:
            with self.subTest(record=record):
                with self.assertRaises(WorkspaceError) as raised:
                    self.manager.prepare(request, expected_record=record)
                self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_MISMATCH")

    def test_unsafe_external_path_segments_are_rejected(self) -> None:
        bad_values = (
            ".",
            "..",
            "CON",
            "NUL.txt",
            "name.",
            "name ",
            "bad:name",
            "PROGRA~1",
            "with\\separator",
            "with/slash",
            "😀" * 101,
        )
        for index, value in enumerate(bad_values):
            with self.subTest(value=value):
                request = self.read_request(f"safe-{index}", project_id=value)
                with self.assertRaises(WorkspaceError) as raised:
                    self.manager.expected_root(request)
                self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_FORBIDDEN")

    def test_drive_root_and_user_home_are_forbidden(self) -> None:
        if os.name == "nt":
            drive_root = Path(self.managed_root.anchor)
            with self.assertRaises(WorkspaceError) as raised:
                ManagedWorkspaceManager(
                    drive_root, self.provider, self.leases, install_roots=()
                )
            self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_FORBIDDEN")

        home = Path.home().resolve()
        home_provider = ManagedGitProvider(
            home,
            {
                "primary": RepositorySpec(
                    "primary", self.canonical_remote, str(self.remote)
                )
            },
            allow_local_transport=True,
        )
        with self.assertRaises(WorkspaceError) as raised:
            ManagedWorkspaceManager(
                home, home_provider, self.leases, install_roots=()
            )
        self.assertEqual(raised.exception.code, "WORKSPACE_ROOT_FORBIDDEN")

    def test_install_and_explicit_protected_roots_are_forbidden(self) -> None:
        install = Path(sys.prefix).resolve()
        install_provider = ManagedGitProvider(
            install,
            {
                "primary": RepositorySpec(
                    "primary", self.canonical_remote, str(self.remote)
                )
            },
            allow_local_transport=True,
        )
        with self.assertRaises(WorkspaceError) as install_error:
            ManagedWorkspaceManager(install, install_provider, self.leases)
        self.assertEqual(install_error.exception.code, "WORKSPACE_ROOT_FORBIDDEN")

        protected = self.base / "protected-service"
        protected_provider = ManagedGitProvider(
            protected,
            {
                "primary": RepositorySpec(
                    "primary", self.canonical_remote, str(self.remote)
                )
            },
            allow_local_transport=True,
        )
        with self.assertRaises(WorkspaceError) as protected_error:
            ManagedWorkspaceManager(
                protected,
                protected_provider,
                self.leases,
                protected_roots=(protected,),
                install_roots=(),
            )
        self.assertEqual(
            protected_error.exception.code, "WORKSPACE_ROOT_FORBIDDEN"
        )

    def test_no_destructive_git_recovery_commands_are_present(self) -> None:
        source = (
            Path(__file__).parents[1] / "nginx_qa" / "workspace_manager.py"
        ).read_text(encoding="utf-8")
        for forbidden in (
            "reset --hard",
            "clean -fd",
            "checkout -f",
            "restore .",
            "force push",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
