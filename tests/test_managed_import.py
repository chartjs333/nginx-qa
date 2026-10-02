import hashlib
import json
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.parse
from unittest.mock import AsyncMock, patch

import main

from nginx_qa.branch_leases import BranchLeaseRequest, deterministic_branch_lease_id
from nginx_qa.git_provider import ManagedGitError, ManagedGitProvider, RepositorySpec
from nginx_qa.managed_import import (
    ManagedImportError,
    ManagedImportStore,
    ManagedPortReservationRegistry,
    ManagedStartResult,
    TransactionalSprintImporter,
    parse_start_request_bytes,
)
from nginx_qa.sprint_types import (
    StartSprintFromGitRequest,
    managed_activation_invariant_issues,
    managed_node_path_segment,
    managed_project_path_segment,
    managed_project_control_invariant_issues,
)


class SimulatedCrash(BaseException):
    pass


async def asgi_raw_request(
    target: str,
    body: bytes,
    *,
    content_type: bytes | None = b"application/json",
) -> tuple[int, object]:
    parsed = urllib.parse.urlsplit(target)
    delivered = False
    messages: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    headers = [(b"host", b"testserver")]
    if content_type is not None:
        headers.append((b"content-type", content_type))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": parsed.path,
        "raw_path": parsed.path.encode("ascii"),
        "query_string": parsed.query.encode("ascii"),
        "root_path": "",
        "headers": headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    await main.app(scope, receive, send)
    response_start = next(
        message for message in messages if message["type"] == "http.response.start"
    )
    response_body = b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
    return int(response_start["status"]), json.loads(response_body)


class ManagedImportFixture(unittest.TestCase):
    project_id = "9002"
    repository_id = "main"
    canonical_remote = "example.invalid/acme/repository"

    def setUp(self) -> None:
        super().setUp()
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.source = self.base / "source"
        self.source.mkdir()
        self.git("init", "--initial-branch=main", cwd=self.source)
        self.git("config", "user.name", "Managed Import Test", cwd=self.source)
        self.git("config", "user.email", "managed@example.invalid", cwd=self.source)
        tracked = b"print('managed')\n"
        (self.source / "service.py").write_bytes(tracked)
        orchestration = self.source / "orchestration"
        orchestration.mkdir()
        self.manifest_path = "orchestration/sprint.json"
        self.manifest = self.manifest_fixture(hashlib.sha256(tracked).hexdigest())
        (self.source / self.manifest_path).write_text(
            json.dumps(self.manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.git("add", ".", cwd=self.source)
        self.git("commit", "-m", "managed manifest", cwd=self.source)
        self.commit = self.git("rev-parse", "HEAD", cwd=self.source)
        self.remote = self.base / "upstream.git"
        self.git("clone", "--bare", str(self.source), str(self.remote))
        self.runtime_config = self.runtime_config_fixture()
        self.port_reservations = ManagedPortReservationRegistry()
        self.registry = {
            self.repository_id: RepositorySpec(
                repository_id=self.repository_id,
                canonical_remote=self.canonical_remote,
                transport_url=str(self.remote),
            )
        }
        self.request = StartSprintFromGitRequest(
            repository_id=self.repository_id,
            ref="refs/heads/main",
            manifest_path=self.manifest_path,
            idempotency_key="managed-import-key",
        )

    def tearDown(self) -> None:
        self.port_reservations.close_all()
        self.temporary.cleanup()
        super().tearDown()

    @staticmethod
    def git(*arguments: str, cwd: Path | None = None) -> str:
        completed = subprocess.run(
            ("git", *arguments),
            cwd=str(cwd) if cwd is not None else None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.returncode:
            raise AssertionError(
                f"git {' '.join(arguments)} failed: {completed.stderr}"
            )
        return completed.stdout.strip()

    @classmethod
    def manifest_fixture(cls, sha256: str) -> dict:
        return {
            "schema_version": 1,
            "sprint_type": "managed_workspace_v1",
            "git_address": "https://example.invalid/acme/repository.git",
            "git": {
                "source_ref": "refs/heads/main",
                "expected_source_commit": None,
                "assigned_branch": "agent/managed-build",
                "existing_branch_policy": "create",
            },
            "execution": {
                "mode": "sequential",
                "start_node": "build",
                "required_approvals": 2,
                "max_rework_cycles": 5,
                "reviewers": [
                    {
                        "id": "reviewer-a",
                        "name": "Reviewer A",
                        "phone": "2891",
                        "git_branch": "review/a",
                    },
                    {
                        "id": "reviewer-b",
                        "name": "Reviewer B",
                        "phone": "2892",
                        "git_branch": "review/b",
                    },
                ],
            },
            "nodes": [
                {
                    "id": "build",
                    "agent": {
                        "id": "builder",
                        "name": "Builder",
                        "phone": "2864",
                    },
                    "tasks": [
                        {
                            "task_id": "BUILD-1",
                            "queue": "worker-all",
                            "message": "Build",
                        }
                    ],
                    "workspace": {"access": "write"},
                    "transitions": {"DONE": "completed"},
                },
                {
                    "id": "continuity",
                    "agent": {
                        "id": "coordinator",
                        "name": "Coordinator",
                        "phone": "2860",
                    },
                    "tasks": [
                        {
                            "task_id": "COORD-1",
                            "queue": "consultant-all",
                            "message": "Recover",
                        }
                    ],
                    "workspace": {"access": "read"},
                    "activation_policy": "any_parent",
                    "transitions": {
                        "RESUME": "build",
                        "BLOCKED_EXTERNAL": "completed",
                    },
                },
                {
                    "id": "completed",
                    "type": "terminal",
                    "status": "DONE",
                    "message": "Completed",
                },
            ],
            "coordinator": {
                "node_id": "continuity",
                "routes": {
                    "STOP": "continuity",
                    "NEED_DECISION": "continuity",
                },
            },
            "files": [{"path": "service.py", "sha256": sha256}],
        }

    def runtime_config_fixture(self) -> dict:
        def path(name: str) -> str:
            return (self.base / name).resolve().as_posix()

        runtime = path("runtime")
        return {
            "http_host": "127.0.0.1",
            "http_port": 18025,
            "service_root": path("service"),
            "protected_roots": [path("protected")],
            "runtime_root": runtime,
            "process_runtime_root": f"{runtime}/processes",
            "log_root": f"{runtime}/logs",
            "pid_root": f"{runtime}/pids",
            "lease_root": f"{runtime}/leases",
            "prompt_root": path("prompts"),
            "managed_root": path("managed"),
            "git_fetch_timeout_seconds": 30,
            "child_port_start": 18100,
            "child_port_end": 18109,
            "instance_id": "managed-import-test",
            "disable_telegram": True,
            "disable_tunnel": True,
        }

    def importer(self, **kwargs) -> TransactionalSprintImporter:
        kwargs.setdefault("port_reservations", self.port_reservations)
        return TransactionalSprintImporter(
            self.runtime_config,
            self.registry,
            allow_local_transport=True,
            **kwargs,
        )


class TransactionalSprintImporterTests(ManagedImportFixture):
    def push_manifest(self, manifest: dict, message: str) -> str:
        (self.source / self.manifest_path).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self.git("add", ".", cwd=self.source)
        self.git("commit", "-m", message, cwd=self.source)
        self.git("push", str(self.remote), "main", cwd=self.source)
        return self.git("rev-parse", "HEAD", cwd=self.source)

    def create_managed_remote(
        self, name: str, canonical_remote: str
    ) -> tuple[Path, str]:
        source = self.base / f"{name}-source"
        source.mkdir()
        self.git("init", "--initial-branch=main", cwd=source)
        self.git("config", "user.name", "Managed Import Test", cwd=source)
        self.git(
            "config",
            "user.email",
            "managed@example.invalid",
            cwd=source,
        )
        tracked = f"print('{name}')\n".encode("utf-8")
        (source / "service.py").write_bytes(tracked)
        (source / "orchestration").mkdir()
        manifest = self.manifest_fixture(hashlib.sha256(tracked).hexdigest())
        manifest["git_address"] = f"https://{canonical_remote}.git"
        (source / self.manifest_path).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.git("add", ".", cwd=source)
        self.git("commit", "-m", f"{name} managed manifest", cwd=source)
        commit = self.git("rev-parse", "HEAD", cwd=source)
        remote = self.base / f"{name}-upstream.git"
        self.git("clone", "--bare", str(source), str(remote))
        return remote, commit

    def test_first_activation_and_success_replay_are_complete(self) -> None:
        importer = self.importer()
        first = importer.start(self.project_id, self.request)
        self.assertEqual(first.http_status, 201)
        self.assertFalse(first.response["deduplicated"])

        state = importer.store.runtime_state(
            self.project_id, first.response["sprint_id"]
        )
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.assertIsNotNone(state["workflow"])
        self.assertEqual(state["active_assignment_ids"], list(state["allowed_outcomes_by_assignment"]))

        replay = importer.start(self.project_id, self.request)
        self.assertEqual(replay.http_status, 200)
        self.assertTrue(replay.response["deduplicated"])
        stored = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert stored is not None
        self.assertFalse(stored["response"]["deduplicated"])
        control = importer.store.project_control(self.project_id)
        self.assertEqual(
            managed_project_control_invariant_issues(
                control, {first.response["sprint_id"]}
            ),
            (),
        )

    def test_second_idempotency_key_scans_existing_mirror(self) -> None:
        self.importer().start(self.project_id, self.request)
        second_request = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="second-managed-import-key",
        )

        with self.assertRaises(ManagedImportError) as raised:
            self.importer().start(self.project_id, second_request)

        self.assertEqual(raised.exception.code, "SPRINT_ALREADY_EXISTS")
        record = self.importer().store.lookup(
            self.project_id, second_request.idempotency_key
        )
        assert record is not None
        self.assertEqual(record["status"], "FAILED")

    def test_legacy_manifest_stops_before_control_or_lease_storage(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["sprint_type"] = "legacy_v1"
        self.push_manifest(changed, "legacy dispatch")
        importer = self.importer()
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "MANAGED_SPRINT_TYPE_REQUIRED")
        lease_root = Path(self.runtime_config["lease_root"])
        self.assertFalse((lease_root / "managed-import.sqlite3").exists())
        self.assertFalse((lease_root / "branch-leases.sqlite3").exists())

    def test_checksum_failure_is_durable_and_replays_without_fetch(self) -> None:
        manifest_path = self.source / self.manifest_path
        invalid = json.loads(manifest_path.read_text(encoding="utf-8"))
        invalid["files"][0]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(invalid), encoding="utf-8")
        self.git("add", self.manifest_path, cwd=self.source)
        self.git("commit", "-m", "bad checksum", cwd=self.source)
        self.git("push", str(self.remote), "main", cwd=self.source)

        importer = self.importer()
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")

        importer.git_provider.ensure_mirror = lambda *args, **kwargs: self.fail(
            "failed replay fetched Git"
        )
        with self.assertRaises(ManagedImportError) as replayed:
            importer.start(self.project_id, self.request)
        self.assertEqual(replayed.exception.envelope, raised.exception.envelope)

    def test_crash_before_activate_resumes_same_complete_activation(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "before_activate" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        control = importer.store.project_control(self.project_id)
        self.assertIsNone(control["active_sprint_id"])
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "ACTIVATING")

        restarted = self.importer()
        result = restarted.start(self.project_id, self.request)
        self.assertEqual(result.http_status, 201)
        state = restarted.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_crash_after_prepare_publishes_no_branch_lease(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_prepare" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        self.assertEqual(importer.branch_leases.active_writers(), ())
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "ACTIVATING")
        self.assertTrue(record["evidence"]["prepared_workspaces"])
        self.assertNotIn(
            "branch_lease_id", record["evidence"]["prepared_workspaces"][0]
        )

        restarted = self.importer()
        result = restarted.start(self.project_id, self.request)
        self.assertEqual(result.http_status, 201)

    def test_activation_failure_releases_uncommitted_branch_lease(self) -> None:
        def fault(point: str, _: dict) -> None:
            if point == "after_workspace_publish":
                raise RuntimeError("injected activation failure")

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_ACTIVATE_FAILED")
        self.assertEqual(importer.branch_leases.active_writers(), ())
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertIsNone(
            importer.store.runtime_state(self.project_id, record["sprint_id"])
        )
        self.assertIn("preflight", record["evidence"])
        self.assertIn("plans", record["evidence"])
        self.assertTrue(record["evidence"]["prepared_workspaces"])

    def test_crash_during_atomic_publication_leaves_no_visible_lease(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_workspace_publish" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "ACTIVATING")
        self.assertIsNone(
            importer.store.runtime_state(self.project_id, record["sprint_id"])
        )
        self.assertEqual(importer.branch_leases.active_writers(), ())
        repository = importer.git_provider.ensure_mirror(
            self.repository_id, fetch=False
        )
        self.assertIsNone(
            importer.git_provider.branch_heads(
                repository, "agent/managed-build"
            ).get("refs/heads/agent/managed-build")
        )

        restarted = self.importer()
        result = restarted.start(self.project_id, self.request)
        self.assertEqual(result.http_status, 201)
        self.assertEqual(len(restarted.branch_leases.active_writers()), 1)

    def test_crash_after_database_commit_recovers_branch_publication(self) -> None:
        def fault(point: str, _: dict) -> None:
            if point == "after_activation_commit":
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "SUCCEEDED")
        repository = importer.git_provider.ensure_mirror(
            self.repository_id, fetch=False
        )
        self.assertIsNone(
            importer.git_provider.branch_heads(
                repository, "agent/managed-build"
            ).get("refs/heads/agent/managed-build")
        )

        restarted = self.importer()
        restarted.restore_committed_activations()
        repository = restarted.git_provider.ensure_mirror(
            self.repository_id, fetch=False
        )
        self.assertEqual(
            restarted.git_provider.branch_heads(
                repository, "agent/managed-build"
            )["refs/heads/agent/managed-build"],
            record["workspace_source_commit"],
        )
        replay = restarted.start(self.project_id, self.request)
        self.assertEqual(replay.http_status, 200)
        repository = restarted.git_provider.ensure_mirror(
            self.repository_id, fetch=False
        )
        self.assertEqual(
            restarted.git_provider.branch_heads(
                repository, "agent/managed-build"
            )["refs/heads/agent/managed-build"],
            record["workspace_source_commit"],
        )

    def test_startup_recovery_keeps_repository_aliases_project_scoped(self) -> None:
        first_importer = self.importer()
        first_result = first_importer.start(self.project_id, self.request)

        second_source = self.base / "second-source"
        second_source.mkdir()
        self.git("init", "--initial-branch=main", cwd=second_source)
        self.git("config", "user.name", "Managed Import Test", cwd=second_source)
        self.git(
            "config",
            "user.email",
            "managed@example.invalid",
            cwd=second_source,
        )
        tracked = b"print('second managed project')\n"
        (second_source / "service.py").write_bytes(tracked)
        second_orchestration = second_source / "orchestration"
        second_orchestration.mkdir()
        second_manifest = self.manifest_fixture(hashlib.sha256(tracked).hexdigest())
        second_manifest["git_address"] = (
            "https://example.invalid/acme/second-repository.git"
        )
        (second_source / self.manifest_path).write_text(
            json.dumps(second_manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.git("add", ".", cwd=second_source)
        self.git("commit", "-m", "second managed manifest", cwd=second_source)
        second_remote = self.base / "second-upstream.git"
        self.git("clone", "--bare", str(second_source), str(second_remote))
        second_registry = {
            self.repository_id: RepositorySpec(
                repository_id=self.repository_id,
                canonical_remote="example.invalid/acme/second-repository",
                transport_url=str(second_remote),
            )
        }
        second_importer = TransactionalSprintImporter(
            self.runtime_config,
            second_registry,
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        second_result = second_importer.start("9003", self.request)

        bootstrap = TransactionalSprintImporter(
            self.runtime_config,
            {},
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        bootstrap.restore_committed_activations()

        first_state = bootstrap.store.runtime_state(
            self.project_id, first_result.response["sprint_id"]
        )
        second_state = bootstrap.store.runtime_state(
            "9003", second_result.response["sprint_id"]
        )
        assert first_state is not None
        assert second_state is not None
        self.assertNotEqual(
            first_state["repository"]["mirror_path"],
            second_state["repository"]["mirror_path"],
        )
        self.assertEqual(
            first_state["repository"]["repository_id"],
            second_state["repository"]["repository_id"],
        )

    def test_success_replay_uses_durable_repository_after_alias_retarget(self) -> None:
        first = self.importer().start(self.project_id, self.request)

        unrelated_source = self.base / "unrelated-source"
        unrelated_source.mkdir()
        self.git("init", "--initial-branch=main", cwd=unrelated_source)
        self.git("config", "user.name", "Managed Import Test", cwd=unrelated_source)
        self.git(
            "config",
            "user.email",
            "managed@example.invalid",
            cwd=unrelated_source,
        )
        (unrelated_source / "unrelated.txt").write_text(
            "unrelated\n", encoding="utf-8"
        )
        self.git("add", ".", cwd=unrelated_source)
        self.git("commit", "-m", "unrelated repository", cwd=unrelated_source)
        unrelated_remote = self.base / "unrelated-upstream.git"
        self.git("clone", "--bare", str(unrelated_source), str(unrelated_remote))
        unrelated_canonical = "example.invalid/acme/unrelated-repository"
        retargeted = TransactionalSprintImporter(
            self.runtime_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=unrelated_canonical,
                    transport_url=str(unrelated_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        unrelated_mirror = retargeted.git_provider.mirror_path_for(
            unrelated_canonical
        )
        self.assertFalse(unrelated_mirror.exists())

        replay = retargeted.start(self.project_id, self.request)

        self.assertEqual(replay.http_status, 200)
        self.assertTrue(replay.response["deduplicated"])
        self.assertEqual(replay.response["sprint_id"], first.response["sprint_id"])
        self.assertFalse(unrelated_mirror.exists())

    def test_unreceipted_divergent_branch_requires_recovery(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["tasks"][0]["message"] = "new source head"
        self.push_manifest(changed, "new source for publication race")
        importer = self.importer()
        original_activate = importer.store.activate

        def commit_then_diverge(*args, **kwargs):
            committed = original_activate(*args, **kwargs)
            repository = importer.git_provider.ensure_mirror(
                self.repository_id, fetch=False
            )
            importer.git_provider.runner.run(
                (
                    "--git-dir",
                    str(repository.mirror_path),
                    "update-ref",
                    "refs/heads/agent/managed-build",
                    self.commit,
                    "0" * len(self.commit),
                ),
                error_code="BRANCH_DIVERGED",
            )
            return committed

        importer.store.activate = commit_then_diverge
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "BRANCH_ALREADY_EXISTS",
        )
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "SUCCEEDED")

    def test_unreceipted_same_head_branch_rechecks_create_policy(self) -> None:
        importer = self.importer()
        original_activate = importer.store.activate

        def commit_then_publish_local(*args, **kwargs):
            committed = original_activate(*args, **kwargs)
            repository = importer.git_provider.ensure_mirror(
                self.repository_id, fetch=False
            )
            importer.git_provider.runner.run(
                (
                    "--git-dir",
                    str(repository.mirror_path),
                    "update-ref",
                    "refs/heads/agent/managed-build",
                    self.commit,
                    "0" * len(self.commit),
                ),
                error_code="BRANCH_DIVERGED",
            )
            return committed

        importer.store.activate = commit_then_publish_local
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "BRANCH_ALREADY_EXISTS",
        )

    def test_unreceipted_remote_branch_rechecks_create_policy(self) -> None:
        importer = self.importer()
        original_activate = importer.store.activate

        def commit_then_publish_remote(*args, **kwargs):
            committed = original_activate(*args, **kwargs)
            repository = importer.git_provider.ensure_mirror(
                self.repository_id, fetch=False
            )
            importer.git_provider.runner.run(
                (
                    "--git-dir",
                    str(repository.mirror_path),
                    "update-ref",
                    "refs/remotes/origin/agent/managed-build",
                    self.commit,
                    "0" * len(self.commit),
                ),
                error_code="BRANCH_DIVERGED",
            )
            return committed

        importer.store.activate = commit_then_publish_remote
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "BRANCH_ALREADY_EXISTS",
        )

    def test_branch_exceeding_windows_ref_budget_fails_preflight(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["git"]["assigned_branch"] = "a" * 240
        self.push_manifest(changed, "oversized physical branch ref")

        with self.assertRaises(ManagedImportError) as raised:
            self.importer().start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertIn(
            "GIT_BRANCH_INVALID",
            {
                issue["code"]
                for issue in raised.exception.envelope["detail"]["issues"]
            },
        )

    def test_maximum_node_id_uses_a_budgeted_physical_path(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        long_node_id = "n" * 96
        changed["execution"]["start_node"] = long_node_id
        changed["nodes"][0]["id"] = long_node_id
        changed["nodes"][1]["transitions"]["RESUME"] = long_node_id
        self.push_manifest(changed, "maximum node id")

        importer = self.importer()
        result = importer.start(self.project_id, self.request)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        workspace = state["workspaces"][0]
        self.assertIn(managed_node_path_segment(long_node_id), workspace["expected_root"])
        self.assertNotIn(long_node_id, workspace["expected_root"])
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_maximum_project_id_uses_a_budgeted_physical_path(self) -> None:
        project_id = "p" * 128

        importer = self.importer()
        result = importer.start(project_id, self.request)
        state = importer.store.runtime_state(project_id, result.response["sprint_id"])
        assert state is not None
        workspace = state["workspaces"][0]
        self.assertIn(managed_project_path_segment(project_id), workspace["expected_root"])
        self.assertNotIn(project_id, workspace["expected_root"])
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_process_activation_atomically_reserves_port_and_owner(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["workspace"]["process"] = {
            "command": [sys.executable, "service.py"],
            "cwd": ".",
            "environment": {"APP_MODE": "test"},
            "health_path": "/health",
            "restart_policy": "never",
            "max_restart_attempts": 0,
            "resource_limits": {},
        }
        self.push_manifest(changed, "process activation")

        importer = self.importer(port_probe=lambda _host, _port: True)
        result = importer.start(self.project_id, self.request)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(len(state["port_leases"]), 1)
        self.assertEqual(state["port_leases"][0]["status"], "reserved")
        self.assertEqual(len(state["processes"]), 1)
        self.assertEqual(state["processes"][0]["state"], "PREPARED")
        self.assertEqual(managed_activation_invariant_issues(state), ())
        snapshot = importer.store.resource_snapshot()
        self.assertEqual(len(snapshot["port_leases"]), 1)
        self.assertEqual(len(snapshot["process_owners"]), 1)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as competitor:
            with self.assertRaises(OSError):
                competitor.bind(
                    (state["port_leases"][0]["host"], state["port_leases"][0]["port"])
                )
        self.port_reservations.close_all()
        restarted_reservations = ManagedPortReservationRegistry()
        try:
            restarted = TransactionalSprintImporter(
                self.runtime_config,
                self.registry,
                port_reservations=restarted_reservations,
                allow_local_transport=True,
            )
            restarted.restore_committed_activations()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as competitor:
                with self.assertRaises(OSError):
                    competitor.bind(
                        (
                            state["port_leases"][0]["host"],
                            state["port_leases"][0]["port"],
                        )
                    )
        finally:
            restarted_reservations.close_all()

    def test_activation_holds_port_before_workspace_publication_boundary(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["workspace"]["process"] = {
            "command": [sys.executable, "service.py"],
            "cwd": ".",
            "environment": {},
        }
        self.push_manifest(changed, "atomic OS port acquisition")
        competitor_was_blocked = False

        def fault(point: str, _: dict) -> None:
            nonlocal competitor_was_blocked
            if point != "after_workspace_publish":
                return
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as competitor:
                try:
                    competitor.bind(
                        ("127.0.0.1", self.runtime_config["child_port_start"])
                    )
                except OSError:
                    competitor_was_blocked = True

        result = self.importer(fault_injector=fault).start(
            self.project_id, self.request
        )
        self.assertEqual(result.http_status, 201)
        self.assertTrue(competitor_was_blocked)

    def test_failed_activation_releases_os_port_reservation(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["workspace"]["process"] = {
            "command": [sys.executable, "service.py"],
            "cwd": ".",
            "environment": {},
        }
        self.push_manifest(changed, "rolled back OS port acquisition")

        def fault(point: str, _: dict) -> None:
            if point == "after_workspace_publish":
                raise RuntimeError("injected activation failure")

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(ManagedImportError):
            importer.start(self.project_id, self.request)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
            candidate.bind(("127.0.0.1", self.runtime_config["child_port_start"]))

    def test_process_plan_does_not_reresolve_path_after_restart(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["workspace"]["process"] = {
            "command": ["python", "service.py"],
            "cwd": ".",
            "environment": {},
        }
        self.push_manifest(changed, "frozen process executable")
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_validate" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(
            port_probe=lambda _host, _port: True,
            fault_injector=fault,
        )
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        frozen_executable = record["evidence"]["plans"][0]["process_plan"][
            "executable_path"
        ]

        with patch(
            "nginx_qa.managed_import.shutil.which",
            side_effect=AssertionError("frozen replay resolved PATH"),
        ):
            result = self.importer(port_probe=lambda _host, _port: True).start(
                self.project_id, self.request
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(state["processes"][0]["executable_path"], frozen_executable)

    def test_port_snapshot_change_at_activate_has_no_partial_ownership(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["workspace"]["process"] = {
            "command": [sys.executable, "service.py"],
            "cwd": ".",
            "environment": {},
        }
        self.push_manifest(changed, "port race")
        external = {"port_leases": [], "process_owners": []}

        def snapshot() -> dict:
            return json.loads(json.dumps(external))

        def fault(point: str, _: dict) -> None:
            if point == "before_activate" and not external["port_leases"]:
                external["port_leases"].append(
                    {
                        "lease_id": "competing-port",
                        "instance_id": "competitor",
                        "network_namespace_id": "host",
                        "assignment_id": "competitor-assignment",
                        "process_id": None,
                        "host": "127.0.0.1",
                        "port": self.runtime_config["child_port_start"],
                        "status": "reserved",
                    }
                )

        importer = self.importer(
            lease_snapshot_provider=snapshot,
            port_probe=lambda _host, _port: True,
            fault_injector=fault,
        )
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "LEASE_SNAPSHOT_STALE")
        self.assertEqual(importer.branch_leases.active_writers(), ())
        self.assertEqual(
            importer.store.resource_snapshot(),
            {"port_leases": [], "process_owners": []},
        )

    def test_external_branch_change_at_final_boundary_rolls_back(self) -> None:
        external = {"branch_leases": []}

        def snapshot() -> dict:
            return json.loads(json.dumps(external))

        def fault(point: str, _: dict) -> None:
            if point == "after_workspace_publish" and not external["branch_leases"]:
                external["branch_leases"].append(
                    {
                        "lease_id": "external-writer",
                        "repository_id": self.repository_id,
                        "repository_key": self.canonical_remote,
                        "mirror_storage_key": "mirror-" + "0" * 64,
                        "branch": "agent/managed-build",
                        "assignment_id": "external-assignment",
                        "mode": "write",
                        "status": "active",
                    }
                )

        importer = self.importer(
            lease_snapshot_provider=snapshot,
            fault_injector=fault,
        )
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "LEASE_SNAPSHOT_STALE")
        self.assertEqual(importer.branch_leases.active_writers(), ())
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertIsNone(
            importer.store.runtime_state(self.project_id, record["sprint_id"])
        )

    def test_unknown_start_node_is_a_durable_preflight_failure(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["execution"]["start_node"] = "ghost"
        self.push_manifest(changed, "unknown start node")

        importer = self.importer()
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertIn(
            "GRAPH_START_NODE_UNKNOWN",
            {item["code"] for item in raised.exception.envelope["detail"]["issues"]},
        )
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertIsNone(importer.store.project_control(self.project_id)["activation_lease"])
        self.assertEqual(importer.branch_leases.active_writers(), ())

    def test_schema_failure_is_pinned_and_replayed_after_ref_moves(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        reviewers = changed["execution"].pop("reviewers")
        self.push_manifest(changed, "invalid managed schema")

        importer = self.importer()
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertIsNotNone(record["pinned_identity"])

        changed["execution"]["reviewers"] = reviewers
        self.push_manifest(changed, "repair moved manifest ref")
        importer.git_provider.ensure_mirror = lambda *args, **kwargs: self.fail(
            "stored schema failure fetched Git"
        )
        with self.assertRaises(ManagedImportError) as replayed:
            importer.start(self.project_id, self.request)
        self.assertEqual(replayed.exception.envelope, raised.exception.envelope)

    def test_source_commit_mismatch_is_a_durable_preflight_failure(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["git"]["expected_source_commit"] = "0" * 40
        mismatch_commit = self.push_manifest(changed, "source mismatch")

        importer = self.importer()
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertIn(
            "SOURCE_COMMIT_MISMATCH",
            {item["code"] for item in raised.exception.envelope["detail"]["issues"]},
        )
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertEqual(record["workspace_source_commit"], mismatch_commit)

        importer.git_provider.ensure_mirror = lambda *args, **kwargs: self.fail(
            "stored source mismatch fetched Git"
        )
        with self.assertRaises(ManagedImportError) as replayed:
            importer.start(self.project_id, self.request)
        self.assertEqual(replayed.exception.envelope, raised.exception.envelope)

    def test_distinct_source_without_expected_commit_is_durable_failure(self) -> None:
        source_commit = self.commit
        self.git("branch", "workspace-source", cwd=self.source)
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["git"]["source_ref"] = "refs/heads/workspace-source"
        self.push_manifest(changed, "distinct source without expectation")
        self.git(
            "push",
            str(self.remote),
            "workspace-source:workspace-source",
            cwd=self.source,
        )

        importer = self.importer()
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertIn(
            "SOURCE_COMMIT_REQUIRED",
            {
                item["code"]
                for item in raised.exception.envelope["detail"]["issues"]
            },
        )
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertEqual(record["workspace_source_commit"], source_commit)

        importer.git_provider.ensure_mirror = lambda *args, **kwargs: self.fail(
            "stored distinct-source failure fetched Git"
        )
        with self.assertRaises(ManagedImportError) as replayed:
            importer.start(self.project_id, self.request)
        self.assertEqual(replayed.exception.envelope, raised.exception.envelope)

    def test_missing_source_ref_failure_is_durable_after_branch_appears(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["git"]["source_ref"] = "refs/heads/not-yet-created"
        self.push_manifest(changed, "missing source ref")

        importer = self.importer()
        with self.assertRaises(ManagedImportError) as first:
            importer.start(self.project_id, self.request)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertIsNone(record["workspace_source_commit"])
        self.assertIn("git_fetch", record["evidence"])

        self.git("branch", "not-yet-created", cwd=self.source)
        self.git(
            "push",
            str(self.remote),
            "not-yet-created:not-yet-created",
            cwd=self.source,
        )
        with self.assertRaises(ManagedImportError) as replay:
            self.importer().start(self.project_id, self.request)
        self.assertEqual(replay.exception.envelope, first.exception.envelope)

    def test_pinned_commit_survives_force_move_and_aggressive_gc(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_pin" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        old_commit = self.commit

        replacement = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        service_bytes = (self.source / "service.py").read_bytes()
        self.git("switch", "--orphan", "replacement", cwd=self.source)
        replacement["nodes"][0]["tasks"][0]["message"] = "unrelated history"
        (self.source / "orchestration").mkdir(exist_ok=True)
        (self.source / "service.py").write_bytes(service_bytes)
        (self.source / self.manifest_path).write_text(
            json.dumps(replacement), encoding="utf-8"
        )
        self.git("add", "-A", cwd=self.source)
        self.git("commit", "-m", "unrelated replacement root", cwd=self.source)
        self.git(
            "push",
            "--force",
            str(self.remote),
            "replacement:main",
            cwd=self.source,
        )

        repository = importer.git_provider.ensure_mirror(self.repository_id)
        self.git(
            "--git-dir",
            str(repository.mirror_path),
            "reflog",
            "expire",
            "--expire=now",
            "--all",
        )
        self.git(
            "--git-dir", str(repository.mirror_path), "gc", "--prune=now"
        )
        self.assertEqual(
            self.git(
                "--git-dir",
                str(repository.mirror_path),
                "rev-parse",
                f"{old_commit}^{{commit}}",
            ),
            old_commit,
        )

        result = self.importer().start(self.project_id, self.request)
        self.assertEqual(result.http_status, 201)
        self.assertEqual(result.response["identity"]["commit"], old_commit)

    def test_git_pin_recovers_crash_before_identity_record(self) -> None:
        importer = self.importer()
        original = importer.git_provider.resolve_and_pin_commit

        def crash_after_source_pin(repository, ref, *, owner_id):
            commit = original(repository, ref, owner_id=owner_id)
            if owner_id.endswith(":workspace-source"):
                raise SimulatedCrash()
            return commit

        importer.git_provider.resolve_and_pin_commit = crash_after_source_pin
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        intent = importer.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert intent is not None
        self.assertEqual(intent["status"], "VALIDATING")
        self.assertIsNone(intent["workspace_source_commit"])
        original_commit = self.commit

        moved = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        moved["nodes"][0]["tasks"][0]["message"] = "moved mutable ref"
        self.push_manifest(moved, "move ref after durable Git pins")

        result = self.importer().start(self.project_id, self.request)
        self.assertEqual(result.http_status, 201)
        self.assertEqual(result.response["identity"]["commit"], original_commit)
        self.assertEqual(result.response["workspace_source_commit"], original_commit)

    def test_crash_before_identity_record_does_not_follow_retargeted_alias(self) -> None:
        importer = self.importer()
        original = importer.git_provider.resolve_and_pin_commit

        def crash_after_source_pin(repository, ref, *, owner_id):
            commit = original(repository, ref, owner_id=owner_id)
            if owner_id.endswith(":workspace-source"):
                raise SimulatedCrash()
            return commit

        importer.git_provider.resolve_and_pin_commit = crash_after_source_pin
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        intent = importer.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert intent is not None
        self.assertEqual(intent["status"], "VALIDATING")
        self.assertIsNone(intent["workspace_source_commit"])

        other_canonical = "example.invalid/acme/retargeted-repository"
        other_remote, other_commit = self.create_managed_remote(
            "retargeted", other_canonical
        )
        retargeted = TransactionalSprintImporter(
            self.runtime_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        other_mirror = retargeted.git_provider.mirror_path_for(other_canonical)

        result = retargeted.start(self.project_id, self.request)

        self.assertEqual(result.http_status, 201)
        self.assertEqual(result.response["identity"]["commit"], self.commit)
        self.assertNotEqual(result.response["identity"]["commit"], other_commit)
        self.assertFalse(other_mirror.exists())

    def test_crash_before_identity_record_keeps_idempotency_fingerprint(self) -> None:
        importer = self.importer()
        original = importer.git_provider.resolve_and_pin_commit

        def crash_after_source_pin(repository, ref, *, owner_id):
            commit = original(repository, ref, owner_id=owner_id)
            if owner_id.endswith(":workspace-source"):
                raise SimulatedCrash()
            return commit

        importer.git_provider.resolve_and_pin_commit = crash_after_source_pin
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        changed_request = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path="orchestration/other.json",
            idempotency_key=self.request.idempotency_key,
        )

        with self.assertRaises(ManagedImportError) as raised:
            self.importer().start(self.project_id, changed_request)

        self.assertEqual(raised.exception.code, "IDEMPOTENCY_KEY_CONFLICT")
        intent = importer.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert intent is not None
        self.assertEqual(intent["status"], "VALIDATING")

    def test_in_progress_retry_does_not_follow_retargeted_alias(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_manifest_read" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "VALIDATING")

        other_canonical = "example.invalid/acme/in-progress-retarget"
        other_remote, _ = self.create_managed_remote(
            "in-progress-retarget", other_canonical
        )
        retargeted = TransactionalSprintImporter(
            self.runtime_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        other_mirror = retargeted.git_provider.mirror_path_for(other_canonical)

        result = retargeted.start(self.project_id, self.request)

        self.assertEqual(result.http_status, 201)
        self.assertEqual(result.response["identity"]["commit"], self.commit)
        self.assertFalse(other_mirror.exists())

    def test_crash_after_mirror_creation_keeps_first_repository_binding(self) -> None:
        other_canonical = "example.invalid/acme/pre-dispatch-retarget"
        other_remote, other_commit = self.create_managed_remote(
            "pre-dispatch-retarget", other_canonical
        )
        importer = self.importer()
        original_ensure_mirror = ManagedGitProvider.ensure_mirror
        crashed = False

        def crash_after_mirror(provider, repository_id, *, fetch=True):
            nonlocal crashed
            repository = original_ensure_mirror(
                provider, repository_id, fetch=fetch
            )
            if (
                fetch
                and repository.canonical_remote == self.canonical_remote
                and not crashed
            ):
                crashed = True
                raise SimulatedCrash()
            return repository

        with patch.object(
            ManagedGitProvider,
            "ensure_mirror",
            crash_after_mirror,
        ):
            with self.assertRaises(SimulatedCrash):
                importer.start(self.project_id, self.request)

        self.assertTrue(crashed)
        self.assertIsNone(
            importer.store.lookup(self.project_id, self.request.idempotency_key)
        )
        first_mirror = importer.git_provider.mirror_path_for(self.canonical_remote)
        self.assertTrue(first_mirror.is_dir())

        retargeted = TransactionalSprintImporter(
            self.runtime_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        other_mirror = retargeted.git_provider.mirror_path_for(other_canonical)

        result = retargeted.start(self.project_id, self.request)

        self.assertEqual(result.http_status, 201)
        self.assertEqual(result.response["identity"]["commit"], self.commit)
        self.assertNotEqual(result.response["identity"]["commit"], other_commit)
        self.assertFalse(other_mirror.exists())

    def test_crash_before_mirror_creation_recovers_from_root_binding(self) -> None:
        other_canonical = "example.invalid/acme/root-only-retarget"
        other_remote, other_commit = self.create_managed_remote(
            "root-only-retarget", other_canonical
        )
        importer = self.importer()
        original_ensure_mirror = ManagedGitProvider.ensure_mirror
        crashed = False

        def crash_at_mirror_entry(provider, repository_id, *, fetch=True):
            nonlocal crashed
            if fetch and not crashed:
                crashed = True
                raise SimulatedCrash()
            return original_ensure_mirror(provider, repository_id, fetch=fetch)

        with patch.object(
            ManagedGitProvider,
            "ensure_mirror",
            crash_at_mirror_entry,
        ):
            with self.assertRaises(SimulatedCrash):
                importer.start(self.project_id, self.request)

        binding_key = importer._request_binding_key(
            self.project_id, self.request.idempotency_key
        )
        binding_path = (
            Path(self.runtime_config["managed_root"])
            / "repository-bindings"
            / f"{binding_key}.json"
        )
        self.assertTrue(binding_path.is_file())
        self.assertFalse(
            importer.git_provider.mirror_path_for(self.canonical_remote).exists()
        )

        retargeted = TransactionalSprintImporter(
            self.runtime_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        result = retargeted.start(self.project_id, self.request)

        self.assertEqual(result.response["identity"]["commit"], self.commit)
        self.assertNotEqual(result.response["identity"]["commit"], other_commit)
        self.assertFalse(
            retargeted.git_provider.mirror_path_for(other_canonical).exists()
        )

    def test_legacy_git_binding_is_migrated_before_alias_resolution(self) -> None:
        importer = self.importer()
        binding_key = "a" * 64
        fingerprint = "b" * 64
        bound, created = importer.git_provider.bind_request_repository(
            binding_key, fingerprint, self.repository_id
        )
        self.assertTrue(created)
        binding_path = (
            Path(self.runtime_config["managed_root"])
            / "repository-bindings"
            / f"{binding_key}.json"
        )
        binding_path.unlink()

        other_canonical = "example.invalid/acme/legacy-binding-retarget"
        other_remote, _ = self.create_managed_remote(
            "legacy-binding-retarget", other_canonical
        )
        retargeted = ManagedGitProvider(
            Path(self.runtime_config["managed_root"]),
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            allow_local_transport=True,
        )
        migrated, recreated = retargeted.bind_request_repository(
            binding_key, fingerprint, self.repository_id
        )

        self.assertFalse(recreated)
        self.assertEqual(migrated, bound)
        self.assertTrue(binding_path.is_file())
        self.assertFalse(retargeted.mirror_path_for(other_canonical).exists())

    def test_legacy_binding_with_invalid_utf8_is_not_migrated(self) -> None:
        canonical_remote = "example.invalid/acme/\ufffd-binding"
        provider = ManagedGitProvider(
            Path(self.runtime_config["managed_root"]),
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=canonical_remote,
                    transport_url=str(self.remote),
                )
            },
            allow_local_transport=True,
        )
        binding_key = "e" * 64
        fingerprint = "f" * 64
        bound, _ = provider.bind_request_repository(
            binding_key, fingerprint, self.repository_id
        )
        binding_path = (
            Path(self.runtime_config["managed_root"])
            / "repository-bindings"
            / f"{binding_key}.json"
        )
        serialized = binding_path.read_bytes()
        self.assertIn(b"\xef\xbf\xbd", serialized)
        binding_path.unlink()
        mirror_path = provider.mirror_path_for(bound.canonical_remote)
        invalid = serialized.replace(b"\xef\xbf\xbd", b"\xff", 1)
        hashed = subprocess.run(
            (
                "git",
                "--git-dir",
                str(mirror_path),
                "hash-object",
                "-w",
                "--stdin",
            ),
            input=invalid,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        object_id = hashed.stdout.decode("ascii").strip()
        self.git(
            "--git-dir",
            str(mirror_path),
            "update-ref",
            f"refs/nginx-qa/request-bindings/{binding_key}",
            object_id,
        )

        with self.assertRaises(ManagedGitError) as raised:
            provider.bind_request_repository(
                binding_key, fingerprint, self.repository_id
            )

        self.assertEqual(raised.exception.code, "REPOSITORY_BINDING_INVALID")
        self.assertFalse(binding_path.exists())

    def test_secret_looking_repository_alias_replays_without_redaction(self) -> None:
        repository_id = "ghp_abcdefgh"
        provider = ManagedGitProvider(
            Path(self.runtime_config["managed_root"]),
            {
                repository_id: RepositorySpec(
                    repository_id=repository_id,
                    canonical_remote=self.canonical_remote,
                    transport_url=str(self.remote),
                )
            },
            allow_local_transport=True,
        )
        binding_key = "1" * 64
        fingerprint = "2" * 64

        first, created = provider.bind_request_repository(
            binding_key, fingerprint, repository_id
        )
        replay, recreated = provider.bind_request_repository(
            binding_key, fingerprint, repository_id
        )

        self.assertTrue(created)
        self.assertFalse(recreated)
        self.assertEqual(replay, first)

    def test_corrupt_root_binding_does_not_fall_back_to_valid_git_ref(self) -> None:
        importer = self.importer()
        binding_key = "c" * 64
        fingerprint = "d" * 64
        importer.git_provider.bind_request_repository(
            binding_key, fingerprint, self.repository_id
        )
        binding_path = (
            Path(self.runtime_config["managed_root"])
            / "repository-bindings"
            / f"{binding_key}.json"
        )
        malformed_receipts = (
            b"not-json",
            b'\xff',
            b'{"value":"\\ud800"}',
            b'{"value":' + (b"1" * 5000) + b"}",
            b'{"value":NaN}',
            (b"[" * 2000) + b"0" + (b"]" * 2000),
            b'{"repository":{},"request_fingerprint":"'
            + fingerprint.encode("ascii")
            + b'","schema_version":true}',
        )
        for raw in malformed_receipts:
            with self.subTest(raw=raw[:32]):
                binding_path.write_bytes(raw)
                with self.assertRaises(ManagedGitError) as raised:
                    importer.git_provider.bind_request_repository(
                        binding_key, fingerprint, self.repository_id
                    )
                self.assertEqual(
                    raised.exception.code, "REPOSITORY_BINDING_INVALID"
                )

    def test_binding_directory_file_fails_before_mirror_or_control_mutation(self) -> None:
        binding_root = (
            Path(self.runtime_config["managed_root"]) / "repository-bindings"
        )
        binding_root.parent.mkdir(parents=True, exist_ok=True)
        binding_root.write_bytes(b"not-a-directory")
        importer = self.importer()

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(raised.exception.envelope["detail"]["phase"], "VALIDATE")
        self.assertFalse(
            importer.git_provider.mirror_path_for(self.canonical_remote).exists()
        )
        self.assertFalse(importer.store.database_path.exists())

    def test_request_lock_directory_file_is_a_stable_validate_failure(self) -> None:
        lock_root = (
            Path(self.runtime_config["managed_root"])
            / "locks"
            / "import-requests"
        )
        lock_root.parent.mkdir(parents=True, exist_ok=True)
        lock_root.write_bytes(b"not-a-directory")
        importer = self.importer()

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(raised.exception.envelope["detail"]["phase"], "VALIDATE")
        self.assertFalse(
            importer.git_provider.mirror_path_for(self.canonical_remote).exists()
        )
        self.assertFalse(importer.store.database_path.exists())

    def test_request_lock_path_directory_is_a_stable_validate_failure(self) -> None:
        importer = self.importer()
        binding_key = importer._request_binding_key(
            self.project_id, self.request.idempotency_key
        )
        lock_path = (
            Path(self.runtime_config["managed_root"])
            / "locks"
            / "import-requests"
            / f"{binding_key}.lock"
        )
        lock_path.mkdir(parents=True)

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(raised.exception.envelope["detail"]["phase"], "VALIDATE")
        self.assertFalse(importer.store.database_path.exists())

    def test_binding_lock_path_directory_is_a_stable_validate_failure(self) -> None:
        importer = self.importer()
        binding_key = importer._request_binding_key(
            self.project_id, self.request.idempotency_key
        )
        lock_path = (
            Path(self.runtime_config["managed_root"])
            / "locks"
            / "repository-bindings"
            / f"{binding_key}.lock"
        )
        lock_path.mkdir(parents=True)

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(raised.exception.envelope["detail"]["phase"], "VALIDATE")
        self.assertFalse(importer.store.database_path.exists())

    def test_mirror_directory_file_is_a_stable_validate_failure(self) -> None:
        importer = self.importer()
        repositories_root = Path(self.runtime_config["managed_root"]) / "repositories"
        repositories_root.parent.mkdir(parents=True, exist_ok=True)
        repositories_root.write_bytes(b"not-a-directory")

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(raised.exception.envelope["detail"]["phase"], "VALIDATE")
        self.assertFalse(importer.store.database_path.exists())

    def test_mirror_lock_directory_file_is_a_stable_validate_failure(self) -> None:
        importer = self.importer()
        locks_root = (
            Path(self.runtime_config["managed_root"])
            / "locks"
            / "repositories"
        )
        locks_root.parent.mkdir(parents=True, exist_ok=True)
        locks_root.write_bytes(b"not-a-directory")

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(raised.exception.envelope["detail"]["phase"], "VALIDATE")
        self.assertFalse(importer.store.database_path.exists())

    def test_registry_surrogate_is_rejected_before_binding_publication(self) -> None:
        invalid = TransactionalSprintImporter(
            self.runtime_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote="example.invalid/acme/\ud800",
                    transport_url="https://example.invalid/acme/\ud800.git",
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )

        with self.assertRaises(ManagedImportError) as raised:
            invalid.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "REPOSITORY_REGISTRY_INVALID",
        )
        binding_root = (
            Path(self.runtime_config["managed_root"]) / "repository-bindings"
        )
        self.assertFalse(binding_root.exists())
        self.assertFalse(invalid.store.database_path.exists())

    def test_lost_binding_link_ack_retries_from_published_receipt(self) -> None:
        other_canonical = "example.invalid/acme/lost-binding-ack-retarget"
        other_remote, other_commit = self.create_managed_remote(
            "lost-binding-ack-retarget", other_canonical
        )
        importer = self.importer()
        original_link = os.link
        injected = False

        def link_then_lose_ack(source, destination, *args, **kwargs):
            nonlocal injected
            original_link(source, destination, *args, **kwargs)
            if not injected:
                injected = True
                raise OSError("simulated lost hardlink acknowledgement")

        with patch("nginx_qa.git_provider.os.link", link_then_lose_ack):
            with self.assertRaises(ManagedImportError) as raised:
                importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")

        retargeted = TransactionalSprintImporter(
            self.runtime_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        result = retargeted.start(self.project_id, self.request)

        self.assertEqual(result.response["identity"]["commit"], self.commit)
        self.assertNotEqual(result.response["identity"]["commit"], other_commit)
        self.assertFalse(
            retargeted.git_provider.mirror_path_for(other_canonical).exists()
        )

    def test_retry_durably_fails_schema_invalid_null_intent(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["git"]["source_ref"] = "not-a-full-ref"
        self.push_manifest(changed, "invalid source ref for null intent")
        importer = self.importer()
        original_set_identity = importer.store.set_pinned_identity
        crashed = False

        def commit_identity_then_crash(*args, **kwargs):
            nonlocal crashed
            record = original_set_identity(*args, **kwargs)
            if not crashed:
                crashed = True
                raise SimulatedCrash()
            return record

        importer.store.set_pinned_identity = commit_identity_then_crash
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)

        with self.assertRaises(ManagedImportError) as raised:
            self.importer().start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "INVALID_MANAGED_SPRINT_REQUEST")
        record = importer.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert record is not None
        self.assertEqual(record["status"], "FAILED")

    def test_post_dispatch_crash_fails_closed_after_managed_root_change(self) -> None:
        importer = self.importer()
        original_pin = importer.git_provider.pin_commit

        def crash_after_sprint_manifest_pin(repository, commit, *, owner_id):
            pinned = original_pin(repository, commit, owner_id=owner_id)
            if owner_id.startswith("msv1-") and owner_id.endswith(":manifest"):
                raise SimulatedCrash()
            return pinned

        importer.git_provider.pin_commit = crash_after_sprint_manifest_pin
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        intent = importer.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert intent is not None
        self.assertEqual(intent["status"], "VALIDATING")
        self.assertIsNotNone(intent["pinned_identity"])

        other_canonical = "example.invalid/acme/root-drift-repository"
        other_remote, _ = self.create_managed_remote(
            "root-drift", other_canonical
        )
        drifted_config = dict(self.runtime_config)
        drifted_config["managed_root"] = (
            self.base / "different-managed-root"
        ).resolve().as_posix()
        retargeted = TransactionalSprintImporter(
            drifted_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        other_mirror = retargeted.git_provider.mirror_path_for(other_canonical)

        with self.assertRaises(ManagedImportError) as raised:
            retargeted.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertFalse(other_mirror.exists())

    def test_cross_root_registration_race_reenters_winners_binding(self) -> None:
        other_canonical = "example.invalid/acme/cross-root-racer"
        other_remote, other_commit = self.create_managed_remote(
            "cross-root-racer", other_canonical
        )
        drifted_config = dict(self.runtime_config)
        drifted_config["managed_root"] = (
            self.base / "different-managed-root"
        ).resolve().as_posix()
        winner = self.importer()
        racer = TransactionalSprintImporter(
            drifted_config,
            {
                self.repository_id: RepositorySpec(
                    repository_id=self.repository_id,
                    canonical_remote=other_canonical,
                    transport_url=str(other_remote),
                )
            },
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        both_registering = threading.Barrier(2)
        winner_registered = threading.Event()
        original_winner_register = winner.store.register_or_resume
        original_racer_register = racer.store.register_or_resume

        def register_winner(*args, **kwargs):
            both_registering.wait(timeout=30)
            result = original_winner_register(*args, **kwargs)
            winner_registered.set()
            return result

        def register_racer(*args, **kwargs):
            both_registering.wait(timeout=30)
            self.assertTrue(winner_registered.wait(timeout=30))
            return original_racer_register(*args, **kwargs)

        winner.store.register_or_resume = register_winner
        racer.store.register_or_resume = register_racer

        def start(importer):
            try:
                return importer.start(self.project_id, self.request)
            except ManagedImportError as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as pool:
            winner_future = pool.submit(start, winner)
            racer_future = pool.submit(start, racer)
            winner_result = winner_future.result(timeout=90)
            racer_result = racer_future.result(timeout=90)

        self.assertIsInstance(winner_result, ManagedStartResult)
        self.assertEqual(winner_result.http_status, 201)
        self.assertEqual(winner_result.response["identity"]["commit"], self.commit)
        self.assertIsInstance(racer_result, ManagedImportError)
        self.assertEqual(racer_result.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertIn(
            "RUNTIME_ROOT_NOT_ISOLATED",
            {
                item["code"]
                for item in racer_result.envelope["detail"]["issues"]
            },
        )
        record = winner.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "SUCCEEDED")
        self.assertEqual(record["pinned_identity"]["commit"], self.commit)
        self.assertNotEqual(record["pinned_identity"]["commit"], other_commit)
        self.assertEqual(
            record["evidence"]["repository_binding"]["canonical_remote"],
            self.canonical_remote,
        )

    def test_callable_registry_is_frozen_after_first_resolution(self) -> None:
        other_canonical = "example.invalid/acme/callable-registry-repository"
        other_remote, other_commit = self.create_managed_remote(
            "callable-registry", other_canonical
        )
        calls = 0

        def registry(repository_id: str) -> RepositorySpec:
            nonlocal calls
            self.assertEqual(repository_id, self.repository_id)
            calls += 1
            if calls == 1:
                return self.registry[self.repository_id]
            return RepositorySpec(
                repository_id=self.repository_id,
                canonical_remote=other_canonical,
                transport_url=str(other_remote),
            )

        importer = TransactionalSprintImporter(
            self.runtime_config,
            registry,
            port_reservations=self.port_reservations,
            allow_local_transport=True,
        )
        other_mirror = importer.git_provider.mirror_path_for(other_canonical)

        result = importer.start(self.project_id, self.request)

        self.assertEqual(result.http_status, 201)
        self.assertEqual(result.response["identity"]["commit"], self.commit)
        self.assertNotEqual(result.response["identity"]["commit"], other_commit)
        self.assertEqual(calls, 1)
        self.assertFalse(other_mirror.exists())

    def test_transient_checksum_read_failure_is_retryable(self) -> None:
        importer = self.importer()
        original_read_blob = importer.git_provider.read_blob
        failed_once = False

        def transient_read(repository, commit, path, *, max_bytes):
            nonlocal failed_once
            if path == "service.py" and not failed_once:
                failed_once = True
                raise ManagedGitError(
                    "GIT_COMMAND_TIMEOUT", "injected transient Git timeout"
                )
            return original_read_blob(
                repository, commit, path, max_bytes=max_bytes
            )

        importer.git_provider.read_blob = transient_read
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.http_status, 503)
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "GIT_COMMAND_TIMEOUT",
        )
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertNotEqual(record["status"], "FAILED")

        recovered = self.importer().start(self.project_id, self.request)
        self.assertEqual(recovered.http_status, 201)

    def test_commit_then_lost_ack_returns_committed_success(self) -> None:
        importer = self.importer()
        original_activate = importer.store.activate

        def commit_then_raise(*args, **kwargs):
            original_activate(*args, **kwargs)
            raise RuntimeError("lost activation acknowledgement")

        importer.store.activate = commit_then_raise
        result = importer.start(self.project_id, self.request)
        self.assertEqual(result.http_status, 201)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "SUCCEEDED")
        self.assertEqual(len(importer.branch_leases.active_writers()), 1)

        def late_fault(point: str, _: dict) -> None:
            if point == "after_workspace_publish":
                raise RuntimeError("late duplicate must not execute")

        replay = self.importer(fault_injector=late_fault).start(
            self.project_id, self.request
        )
        self.assertEqual(replay.http_status, 200)
        self.assertEqual(len(importer.branch_leases.active_writers()), 1)

    def test_unsafe_runtime_roots_fail_before_any_filesystem_mutation(self) -> None:
        for field in ("managed_root", "lease_root"):
            with self.subTest(field=field):
                protected = self.base / f"protected-{field}"
                config = dict(self.runtime_config)
                config["protected_roots"] = [protected.as_posix()]
                config[field] = (protected / "nested").as_posix()
                importer = TransactionalSprintImporter(
                    config,
                    self.registry,
                    allow_local_transport=True,
                )
                with self.assertRaises(ManagedImportError) as raised:
                    importer.start(self.project_id, self.request)
                self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
                self.assertFalse(protected.exists())

    def test_mutation_root_cannot_be_a_git_repository_itself(self) -> None:
        cases = (
            ("runtime_root", self.source, self.source / "leases" / "managed-import.sqlite3"),
            ("managed_root", self.source, self.source / "repositories"),
            ("managed_root", self.remote, self.remote / "repositories"),
        )
        for field, unsafe_root, forbidden_artifact in cases:
            with self.subTest(field=field, root=unsafe_root.name):
                config = dict(self.runtime_config)
                if field == "runtime_root":
                    config.update(
                        {
                            "runtime_root": unsafe_root.as_posix(),
                            "process_runtime_root": (unsafe_root / "processes").as_posix(),
                            "log_root": (unsafe_root / "logs").as_posix(),
                            "pid_root": (unsafe_root / "pids").as_posix(),
                            "lease_root": (unsafe_root / "leases").as_posix(),
                        }
                    )
                else:
                    config[field] = unsafe_root.as_posix()
                importer = TransactionalSprintImporter(
                    config,
                    self.registry,
                    port_reservations=self.port_reservations,
                    allow_local_transport=True,
                )
                with self.assertRaises(ManagedImportError) as raised:
                    importer.start(self.project_id, self.request)
                self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
                self.assertFalse(forbidden_artifact.exists())

    def test_unsafe_preexisting_database_is_not_schema_initialized(self) -> None:
        runtime = self.source / "runtime-state"
        lease_root = runtime / "leases"
        lease_root.mkdir(parents=True)
        database = lease_root / "managed-import.sqlite3"
        database.write_bytes(b"")
        config = dict(self.runtime_config)
        config.update(
            {
                "runtime_root": runtime.as_posix(),
                "process_runtime_root": (runtime / "processes").as_posix(),
                "log_root": (runtime / "logs").as_posix(),
                "pid_root": (runtime / "pids").as_posix(),
                "lease_root": lease_root.as_posix(),
            }
        )
        importer = TransactionalSprintImporter(
            config, self.registry, allow_local_transport=True
        )
        with self.assertRaises(ManagedImportError):
            importer.start(self.project_id, self.request)
        self.assertEqual(database.read_bytes(), b"")

    def test_terminal_replay_precedes_non_path_config_drift(self) -> None:
        first = self.importer().start(self.project_id, self.request)
        drifted = dict(self.runtime_config)
        drifted["http_port"] = 8025
        replay = TransactionalSprintImporter(
            drifted, self.registry, allow_local_transport=True
        ).start(self.project_id, self.request)
        self.assertEqual(first.response["sprint_id"], replay.response["sprint_id"])
        self.assertEqual(replay.http_status, 200)
        self.assertTrue(replay.response["deduplicated"])

    def test_parallel_activation_creates_every_initial_assignment(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["execution"].pop("start_node")
        changed["execution"]["mode"] = "parallel"
        changed["execution"]["start_nodes"] = ["build", "continuity"]
        self.push_manifest(changed, "parallel managed activation")

        importer = self.importer()
        result = importer.start(self.project_id, self.request)
        self.assertEqual(result.http_status, 201)
        self.assertEqual(len(result.response["initial_assignment_ids"]), 2)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(len(state["workspaces"]), 2)
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_second_sprint_cannot_replace_an_active_runtime(self) -> None:
        importer = self.importer()
        first = importer.start(self.project_id, self.request)
        active_leases = importer.branch_leases.active_writers()

        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["git"]["assigned_branch"] = "agent/second-build"
        changed["nodes"][0]["tasks"][0]["message"] = "second sprint"
        self.push_manifest(changed, "second active sprint")
        second_request = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="second-managed-import-key",
        )
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, second_request)
        self.assertEqual(raised.exception.code, "PROJECT_ACTIVATION_IN_PROGRESS")
        self.assertEqual(
            importer.store.project_control(self.project_id)["active_sprint_id"],
            first.response["sprint_id"],
        )
        self.assertEqual(importer.branch_leases.active_writers(), active_leases)

    def test_expired_preparing_attempt_cannot_replace_new_active_sprint(self) -> None:
        current_time = [datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)]
        crashed = False

        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "resource-free expired attempt")

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_validate" and not crashed:
                crashed = True
                raise SimulatedCrash()

        first = self.importer(
            clock=lambda: current_time[0],
            fault_injector=fault,
        )
        with self.assertRaises(SimulatedCrash):
            first.start(self.project_id, self.request)
        first_record = first.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert first_record is not None
        self.assertEqual(first_record["status"], "PREPARING")

        current_time[0] += timedelta(minutes=10)
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["tasks"][0]["message"] = "new active sprint"
        self.push_manifest(changed, "activate newer sprint after expired attempt")
        second_request = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="new-active-after-expired-attempt",
        )
        second = self.importer(clock=lambda: current_time[0])
        second_result = second.start(self.project_id, second_request)

        with self.assertRaises(ManagedImportError) as raised:
            first.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "ATTEMPT_SUPERSEDED",
        )
        control = second.store.project_control(self.project_id)
        self.assertEqual(
            control["active_sprint_id"], second_result.response["sprint_id"]
        )
        active = second.store.active_runtime_states()
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["sprint_id"], second_result.response["sprint_id"])
        preserved = second.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert preserved is not None
        self.assertEqual(preserved["status"], "FAILED")
        self.assertEqual(preserved["evidence"]["failure_code"], "ATTEMPT_SUPERSEDED")

    def test_legacy_refenced_attempt_is_failed_by_immutable_creation_fence(self) -> None:
        current_time = [datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)]
        crashed = False
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "legacy refence setup")

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_validate" and not crashed:
                crashed = True
                raise SimulatedCrash()

        first = self.importer(clock=lambda: current_time[0], fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            first.start(self.project_id, self.request)
        current_time[0] += timedelta(minutes=10)
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["tasks"][0]["message"] = "legacy winner"
        self.push_manifest(changed, "legacy winner activation")
        winner_request = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="legacy-refence-winner",
        )
        winner = self.importer(clock=lambda: current_time[0])
        winner_result = winner.start(self.project_id, winner_request)

        database = winner.store.database_path
        with closing(sqlite3.connect(database)) as connection:
            raw_control = connection.execute(
                "SELECT control_json FROM managed_projects WHERE project_id = ?",
                (self.project_id,),
            ).fetchone()[0]
            control = json.loads(raw_control)
            old_record = next(
                record
                for record in control["start_idempotency_records"]
                if record["idempotency_key"] == self.request.idempotency_key
            )
            winner_record = next(
                record
                for record in control["start_idempotency_records"]
                if record["idempotency_key"] == winner_request.idempotency_key
            )
            old_record.pop("created_fencing_token", None)
            old_record.update(
                {
                    "status": "PREPARING",
                    "fencing_token": int(winner_record["fencing_token"]) + 1,
                    "response": None,
                    "http_status": None,
                    "error": None,
                }
            )
            control["activation_fencing_counter"] = old_record["fencing_token"]
            control["activation_lease"] = None
            connection.execute(
                "UPDATE managed_projects SET control_json = ? WHERE project_id = ?",
                (
                    json.dumps(
                        control,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    self.project_id,
                ),
            )
            connection.commit()

        restarted = ManagedImportStore(database)
        restored = restarted.active_runtime_states()
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0]["sprint_id"], winner_result.response["sprint_id"])
        startup_control = restarted.project_control(self.project_id)
        startup_old = next(
            record
            for record in startup_control["start_idempotency_records"]
            if record["idempotency_key"] == self.request.idempotency_key
        )
        self.assertEqual(startup_old["status"], "FAILED")
        self.assertEqual(
            startup_old["evidence"]["failure_code"], "ATTEMPT_SUPERSEDED"
        )

        with self.assertRaises(ManagedImportError) as raised:
            first.start(self.project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "ATTEMPT_SUPERSEDED",
        )
        repaired = winner.store.project_control(self.project_id)
        repaired_old = next(
            record
            for record in repaired["start_idempotency_records"]
            if record["idempotency_key"] == self.request.idempotency_key
        )
        self.assertEqual(repaired_old["status"], "FAILED")
        self.assertEqual(
            repaired["active_sprint_id"], winner_result.response["sprint_id"]
        )

    def test_current_fence_winner_supersedes_later_abandoned_attempt(self) -> None:
        current_time = [datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)]
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "current fence winner setup")

        first_crashed = False

        def first_fault(point: str, _: dict) -> None:
            nonlocal first_crashed
            if point == "after_validate" and not first_crashed:
                first_crashed = True
                raise SimulatedCrash()

        first = self.importer(
            clock=lambda: current_time[0], fault_injector=first_fault
        )
        with self.assertRaises(SimulatedCrash):
            first.start(self.project_id, self.request)

        current_time[0] += timedelta(minutes=10)
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["tasks"][0]["message"] = "later abandoned attempt"
        self.push_manifest(changed, "later abandoned attempt")
        second_request = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="later-abandoned-attempt",
        )
        second_crashed = False

        def second_fault(point: str, _: dict) -> None:
            nonlocal second_crashed
            if point == "after_validate" and not second_crashed:
                second_crashed = True
                raise SimulatedCrash()

        second = self.importer(
            clock=lambda: current_time[0], fault_injector=second_fault
        )
        with self.assertRaises(SimulatedCrash):
            second.start(self.project_id, second_request)

        current_time[0] += timedelta(minutes=10)
        result = first.start(self.project_id, self.request)

        self.assertEqual(result.http_status, 201)
        abandoned = first.store.lookup(
            self.project_id, second_request.idempotency_key
        )
        assert abandoned is not None
        self.assertEqual(abandoned["status"], "FAILED")
        self.assertEqual(
            abandoned["evidence"]["failure_code"], "ATTEMPT_SUPERSEDED"
        )

    def test_inconsistent_terminal_runtime_fails_closed_at_final_boundary(self) -> None:
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "inconsistent terminal setup")
        importer = self.importer()
        first = importer.start(self.project_id, self.request)

        database = importer.store.database_path
        with closing(sqlite3.connect(database)) as connection:
            raw_state = connection.execute(
                """
                SELECT state_json FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (self.project_id, first.response["sprint_id"]),
            ).fetchone()[0]
            state = json.loads(raw_state)
            state["status"] = "completed"
            connection.execute(
                """
                UPDATE managed_sprints SET status = 'completed', state_json = ?
                WHERE project_id = ? AND sprint_id = ?
                """,
                (
                    json.dumps(
                        state,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    self.project_id,
                    first.response["sprint_id"],
                ),
            )
            connection.commit()

        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["tasks"][0]["message"] = "must not replace corrupt terminal"
        self.push_manifest(changed, "inconsistent terminal contender")
        contender = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="inconsistent-terminal-contender",
        )

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, contender)

        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "ACTIVE_SPRINT_STATE_CONFLICT",
        )

    def test_corrupt_active_runtime_is_recovery_not_transient_conflict(self) -> None:
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "corrupt active setup")
        importer = self.importer()
        first = importer.start(self.project_id, self.request)

        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            raw_state = connection.execute(
                """
                SELECT state_json FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (self.project_id, first.response["sprint_id"]),
            ).fetchone()[0]
            state = json.loads(raw_state)
            state.pop("schema_version")
            connection.execute(
                """
                UPDATE managed_sprints SET state_json = ?
                WHERE project_id = ? AND sprint_id = ?
                """,
                (
                    json.dumps(state, sort_keys=True, separators=(",", ":")),
                    self.project_id,
                    first.response["sprint_id"],
                ),
            )
            connection.commit()

        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["tasks"][0]["message"] = "corrupt active contender"
        self.push_manifest(changed, "corrupt active contender")
        contender = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="corrupt-active-contender",
        )

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, contender)

        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "ACTIVE_SPRINT_STATE_CONFLICT",
        )

    def test_valid_terminal_predecessor_with_exact_fence_can_be_replaced(self) -> None:
        from tests.test_sprint_type_contract import (
            completed_runtime_fixture,
            project_control_fixture,
        )

        importer = self.importer()
        importer.store._ensure_initialized()
        control = project_control_fixture()
        terminal = completed_runtime_fixture()
        prior_project_id = str(control["project_id"])
        prior_sprint_id = str(control["active_sprint_id"])
        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            connection.execute(
                """
                INSERT INTO managed_projects(project_id, control_json, revision)
                VALUES (?, ?, 1)
                """,
                (
                    prior_project_id,
                    json.dumps(control, sort_keys=True, separators=(",", ":")),
                ),
            )
            connection.execute(
                """
                INSERT INTO managed_sprints(
                    project_id, sprint_id, status, fencing_token, state_json
                ) VALUES (?, ?, 'completed', 1, ?)
                """,
                (
                    prior_project_id,
                    prior_sprint_id,
                    json.dumps(terminal, sort_keys=True, separators=(",", ":")),
                ),
            )
            connection.commit()

        result = importer.start(prior_project_id, self.request)

        self.assertEqual(result.http_status, 201)
        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            rows = connection.execute(
                """
                SELECT sprint_id, status FROM managed_sprints
                WHERE project_id = ? ORDER BY sprint_id
                """,
                (prior_project_id,),
            ).fetchall()
        self.assertEqual(sum(status == "active" for _, status in rows), 1)
        self.assertIn((prior_sprint_id, "completed"), rows)
        self.assertEqual(
            importer.store.project_control(prior_project_id)["active_sprint_id"],
            result.response["sprint_id"],
        )

    def test_terminal_predecessor_fence_mismatch_fails_closed(self) -> None:
        from tests.test_sprint_type_contract import (
            completed_runtime_fixture,
            project_control_fixture,
        )

        importer = self.importer()
        importer.store._ensure_initialized()
        control = project_control_fixture()
        terminal = completed_runtime_fixture()
        prior_project_id = str(control["project_id"])
        prior_sprint_id = str(control["active_sprint_id"])
        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            connection.execute(
                """
                INSERT INTO managed_projects(project_id, control_json, revision)
                VALUES (?, ?, 1)
                """,
                (
                    prior_project_id,
                    json.dumps(control, sort_keys=True, separators=(",", ":")),
                ),
            )
            connection.execute(
                """
                INSERT INTO managed_sprints(
                    project_id, sprint_id, status, fencing_token, state_json
                ) VALUES (?, ?, 'completed', 0, ?)
                """,
                (
                    prior_project_id,
                    prior_sprint_id,
                    json.dumps(terminal, sort_keys=True, separators=(",", ":")),
                ),
            )
            connection.commit()

        with self.assertRaises(ManagedImportError) as raised:
            importer.start(prior_project_id, self.request)

        self.assertEqual(raised.exception.code, "SPRINT_RECOVERY_REQUIRED")
        self.assertEqual(
            raised.exception.envelope["detail"]["issues"][0]["code"],
            "ACTIVE_SPRINT_STATE_CONFLICT",
        )

    def test_legacy_duplicate_active_rows_fail_startup_reconciliation(self) -> None:
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "duplicate active setup")
        importer = self.importer()
        first = importer.start(self.project_id, self.request)
        duplicate_sprint_id = "msv1-" + "0" * 64

        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            connection.execute("DROP INDEX managed_single_active_sprint")
            raw_state, fencing_token = connection.execute(
                """
                SELECT state_json, fencing_token FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (self.project_id, first.response["sprint_id"]),
            ).fetchone()
            connection.execute(
                """
                INSERT INTO managed_sprints(
                    project_id, sprint_id, status, fencing_token, state_json
                ) VALUES (?, ?, 'active', ?, ?)
                """,
                (
                    self.project_id,
                    duplicate_sprint_id,
                    fencing_token,
                    raw_state,
                ),
            )
            connection.commit()

        restarted = ManagedImportStore(importer.store.database_path)
        with self.assertRaisesRegex(RuntimeError, "MULTIPLE_ACTIVE_SPRINTS"):
            restarted.active_runtime_states()

        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            connection.execute(
                "DELETE FROM managed_sprints WHERE project_id = ? AND sprint_id = ?",
                (self.project_id, duplicate_sprint_id),
            )
            connection.commit()
        restarted._ensure_initialized()
        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            index = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'index' AND name = 'managed_single_active_sprint'
                """
            ).fetchone()
            self.assertIsNotNone(index)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO managed_sprints(
                        project_id, sprint_id, status, fencing_token, state_json
                    ) VALUES (?, ?, 'active', ?, ?)
                    """,
                    (
                        self.project_id,
                        "msv1-" + "1" * 64,
                        fencing_token,
                        raw_state,
                    ),
                )
            connection.rollback()
        self.assertEqual(len(restarted.active_runtime_states()), 1)

    def test_corrupt_project_control_fails_startup_reconciliation(self) -> None:
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "corrupt project control setup")
        importer = self.importer()
        importer.start(self.project_id, self.request)

        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            raw_control = connection.execute(
                "SELECT control_json FROM managed_projects WHERE project_id = ?",
                (self.project_id,),
            ).fetchone()[0]
            cases = (
                ("indexed project", "PROJECT_CONTROL_INDEX_MISMATCH"),
                ("lease schema", "PROJECT_CONTROL_SCHEMA_INVALID"),
                ("success response", "START_SUCCESS_RESPONSE_MISMATCH"),
            )
            for corruption, expected_issue in cases:
                with self.subTest(corruption=corruption):
                    control = json.loads(raw_control)
                    if corruption == "indexed project":
                        control["project_id"] = "wrong-project"
                    elif corruption == "lease schema":
                        control["activation_lease"] = {}
                    else:
                        control["start_idempotency_records"][0]["response"][
                            "workspace_source_commit"
                        ] = "a" * 40
                    connection.execute(
                        """
                        UPDATE managed_projects SET control_json = ?
                        WHERE project_id = ?
                        """,
                        (
                            json.dumps(
                                control, sort_keys=True, separators=(",", ":")
                            ),
                            self.project_id,
                        ),
                    )
                    connection.commit()
                    with self.assertRaisesRegex(RuntimeError, expected_issue):
                        ManagedImportStore(
                            importer.store.database_path
                        ).active_runtime_states()

    def test_stale_active_pointer_fails_startup_reconciliation(self) -> None:
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "stale active pointer setup")
        importer = self.importer()
        first = importer.start(self.project_id, self.request)

        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            raw_control = connection.execute(
                "SELECT control_json FROM managed_projects WHERE project_id = ?",
                (self.project_id,),
            ).fetchone()[0]
            control = json.loads(raw_control)
            later = json.loads(
                json.dumps(control["start_idempotency_records"][0])
            )
            later_sprint_id = "msv1-" + "b" * 64
            later.update(
                {
                    "idempotency_key": "later-success",
                    "request_fingerprint": "3" * 64,
                    "attempt_id": "later-attempt",
                    "created_fencing_token": 2,
                    "fencing_token": 2,
                    "sprint_id": later_sprint_id,
                }
            )
            later["response"]["sprint_id"] = later_sprint_id
            control["activation_fencing_counter"] = 2
            control["start_idempotency_records"].append(later)
            self.assertEqual(
                control["active_sprint_id"], first.response["sprint_id"]
            )
            connection.execute(
                "UPDATE managed_projects SET control_json = ? WHERE project_id = ?",
                (
                    json.dumps(control, sort_keys=True, separators=(",", ":")),
                    self.project_id,
                ),
            )
            connection.commit()

        restarted = ManagedImportStore(importer.store.database_path)
        with self.assertRaisesRegex(RuntimeError, "ACTIVE_SPRINT_INDEX_MISMATCH"):
            restarted.active_runtime_states()

    def test_non_integer_control_fence_fails_startup_reconciliation(self) -> None:
        resource_free = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        resource_free["nodes"][0]["workspace"] = {"access": "read"}
        self.push_manifest(resource_free, "non-integer control fence setup")
        importer = self.importer()
        importer.start(self.project_id, self.request)

        with closing(sqlite3.connect(importer.store.database_path)) as connection:
            raw_control = connection.execute(
                "SELECT control_json FROM managed_projects WHERE project_id = ?",
                (self.project_id,),
            ).fetchone()[0]
            cases = (
                (True, "PROJECT_CONTROL_SCHEMA_INVALID"),
                (1.0, "START_FENCING_TOKEN_INVALID"),
            )
            for bad_fence, expected_issue in cases:
                with self.subTest(bad_fence=bad_fence):
                    control = json.loads(raw_control)
                    control["start_idempotency_records"][0][
                        "fencing_token"
                    ] = bad_fence
                    connection.execute(
                        """
                        UPDATE managed_projects SET control_json = ?
                        WHERE project_id = ?
                        """,
                        (
                            json.dumps(
                                control, sort_keys=True, separators=(",", ":")
                            ),
                            self.project_id,
                        ),
                    )
                    connection.commit()
                    with self.assertRaisesRegex(
                        RuntimeError, expected_issue
                    ):
                        ManagedImportStore(
                            importer.store.database_path
                        ).active_runtime_states()

    def test_branch_race_returns_stale_snapshot_without_partial_activation(self) -> None:
        injected = False
        importer = None

        def fault(point: str, _: dict) -> None:
            nonlocal injected
            if point != "after_prepare" or injected:
                return
            injected = True
            assert importer is not None
            repository = importer.git_provider.ensure_mirror(
                self.repository_id, fetch=False
            )
            branch = self.manifest["git"]["assigned_branch"]
            importer.branch_leases.acquire_write(
                BranchLeaseRequest(
                    lease_id=deterministic_branch_lease_id(
                        repository.mirror_storage_key,
                        branch,
                        "competing-assignment",
                    ),
                    repository_id=self.repository_id,
                    repository_key=repository.canonical_remote,
                    mirror_storage_key=repository.mirror_storage_key,
                    branch=branch,
                    assignment_id="competing-assignment",
                    source_commit=self.commit,
                    initial_head_commit=self.commit,
                )
            )

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "LEASE_SNAPSHOT_STALE")
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertIsNone(
            importer.store.runtime_state(self.project_id, record["sprint_id"])
        )
        writers = importer.branch_leases.active_writers()
        self.assertEqual(len(writers), 1)
        self.assertEqual(writers[0].assignment_id, "competing-assignment")

    def test_crash_after_pin_never_reresolves_a_moved_ref(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_pin" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        pinned_commit = self.commit

        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["tasks"][0]["message"] = "Moved definition"
        moved_commit = self.push_manifest(changed, "move managed ref")
        self.assertNotEqual(moved_commit, pinned_commit)

        restarted = self.importer()
        result = restarted.start(self.project_id, self.request)
        self.assertEqual(result.response["identity"]["commit"], pinned_commit)
        self.assertNotEqual(result.response["identity"]["commit"], moved_commit)

    def test_changed_idempotency_request_conflicts_before_git(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_pin" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        importer.git_provider.ensure_mirror = lambda *args, **kwargs: self.fail(
            "idempotency conflict fetched Git"
        )
        changed = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref="refs/heads/other",
            manifest_path=self.request.manifest_path,
            idempotency_key=self.request.idempotency_key,
        )
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, changed)
        self.assertEqual(raised.exception.code, "IDEMPOTENCY_KEY_CONFLICT")

    def test_expired_owner_cannot_write_with_a_successor_fence(self) -> None:
        started_at = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)

        def crash_after_pin(point: str, _: dict) -> None:
            if point == "after_pin":
                raise SimulatedCrash()

        first = self.importer(
            clock=lambda: started_at,
            fault_injector=crash_after_pin,
        )
        with self.assertRaises(SimulatedCrash):
            first.start(self.project_id, self.request)
        old_record = first.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert old_record is not None
        old_token = old_record["fencing_token"]

        successor = self.importer(
            clock=lambda: started_at + timedelta(minutes=10),
            fault_injector=crash_after_pin,
        )
        with self.assertRaises(SimulatedCrash):
            successor.start(self.project_id, self.request)
        new_record = successor.store.lookup(
            self.project_id, self.request.idempotency_key
        )
        assert new_record is not None
        self.assertGreater(new_record["fencing_token"], old_token)

        with self.assertRaises(ManagedImportError) as stale:
            first.store.update_attempt(
                self.project_id,
                old_record["attempt_id"],
                status="PREPARING",
                evidence_update={"stale": True},
                fencing_token=old_token,
                clock=lambda: started_at + timedelta(minutes=11),
            )
        self.assertEqual(stale.exception.code, "PROJECT_ACTIVATION_IN_PROGRESS")

    def test_crash_after_commit_replays_the_same_activation(self) -> None:
        crashed = False

        def fault(point: str, _: dict) -> None:
            nonlocal crashed
            if point == "after_activate" and not crashed:
                crashed = True
                raise SimulatedCrash()

        importer = self.importer(fault_injector=fault)
        with self.assertRaises(SimulatedCrash):
            importer.start(self.project_id, self.request)
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "SUCCEEDED")

        replay = self.importer().start(self.project_id, self.request)
        self.assertEqual(replay.http_status, 200)
        self.assertTrue(replay.response["deduplicated"])

    def test_process_manifest_requires_an_available_child_port(self) -> None:
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["workspace"]["process"] = {
            "command": ["python", "service.py"],
            "cwd": ".",
            "environment": {},
        }
        self.push_manifest(changed, "process manifest")
        importer = self.importer(port_probe=lambda _host, _port: False)
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertIn(
            "LIVE_PORT_CONFLICT",
            {issue["code"] for issue in raised.exception.envelope["detail"]["issues"]},
        )
        self.assertEqual(importer.branch_leases.active_writers(), ())

    def test_four_exact_callers_publish_one_runtime(self) -> None:
        def invoke(_: int):
            return self.importer().start(self.project_id, self.request)

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(invoke, range(4)))
        self.assertTrue(all(result.http_status in {200, 201} for result in results))
        self.assertEqual(
            [result.http_status for result in results].count(201), 1
        )
        sprint_ids = {result.response["sprint_id"] for result in results}
        self.assertEqual(len(sprint_ids), 1)
        store = ManagedImportStore(
            Path(self.runtime_config["lease_root"]) / "managed-import.sqlite3"
        )
        control = store.project_control(self.project_id)
        self.assertEqual(len(control["start_idempotency_records"]), 1)
        self.assertEqual(control["start_idempotency_records"][0]["status"], "SUCCEEDED")

    def test_active_null_workflow_is_rejected_before_commit(self) -> None:
        importer = self.importer()
        result = importer.start(self.project_id, self.request)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        state["workflow"] = None
        state["active_assignment_ids"] = []
        state["allowed_outcomes_by_assignment"] = {}
        issues = managed_activation_invariant_issues(state)
        self.assertIn("WORKFLOW_MISSING", issues)

    def test_store_refuses_active_null_workflow_at_commit_boundary(self) -> None:
        importer = self.importer()
        original_activate = importer.store.activate

        def corrupt_activate(project_id, attempt_id, runtime_state, response, **kwargs):
            corrupt = json.loads(json.dumps(runtime_state))
            corrupt["workflow"] = None
            corrupt["active_assignment_ids"] = []
            corrupt["allowed_outcomes_by_assignment"] = {}
            return original_activate(
                project_id, attempt_id, corrupt, response, **kwargs
            )

        importer.store.activate = corrupt_activate
        with self.assertRaises(ManagedImportError) as raised:
            importer.start(self.project_id, self.request)
        self.assertEqual(raised.exception.code, "SPRINT_ACTIVATE_FAILED")
        record = importer.store.lookup(self.project_id, self.request.idempotency_key)
        assert record is not None
        self.assertEqual(record["status"], "FAILED")
        self.assertIsNone(
            importer.store.runtime_state(self.project_id, record["sprint_id"])
        )


class ManagedImportStoreTests(ManagedImportFixture):
    def test_store_path_must_be_absolute(self) -> None:
        with self.assertRaises(ValueError):
            ManagedImportStore("relative.sqlite3")

    def test_shared_database_is_safe_when_branch_schema_is_first(self) -> None:
        importer = self.importer()
        importer.branch_leases.ensure_initialized()
        self.assertEqual(
            importer.store.project_control(self.project_id)["project_id"],
            self.project_id,
        )
        self.assertIsNone(
            importer.store.runtime_state(self.project_id, "msv1-" + "0" * 64)
        )

    def test_shared_database_initializers_can_race(self) -> None:
        importer = self.importer()
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(importer.branch_leases.ensure_initialized),
                executor.submit(importer.store._ensure_initialized),
            ]
            for future in futures:
                future.result()
        self.assertEqual(importer.branch_leases.active_writers(), ())
        self.assertEqual(
            importer.store.resource_snapshot(),
            {"port_leases": [], "process_owners": []},
        )


class ManagedCredentialResolverTests(unittest.TestCase):
    def test_env_reference_resolves_only_ephemeral_git_environment(self) -> None:
        encoded = json.dumps(
            {
                "GIT_ASKPASS": "C:/managed/askpass.exe",
                "SSH_AUTH_SOCK": "managed-agent",
            }
        )
        with patch.dict(
            main.os.environ,
            {"NGINX_QA_TEST_GIT_CREDENTIAL": encoded},
            clear=False,
        ):
            resolved = main.managed_git_credential_resolver(
                "env:NGINX_QA_TEST_GIT_CREDENTIAL"
            )
        self.assertEqual(json.loads(encoded), resolved)
        with self.assertRaises(LookupError):
            main.managed_git_credential_resolver("vault:production/repository")


class ManagedRequestParserTests(unittest.TestCase):
    def test_deep_json_and_float_overflow_are_normalized(self) -> None:
        deeply_nested = b'{"x":' * 1100 + b"0" + b"}" * 1100
        with self.assertRaises(ManagedImportError) as deep_error:
            parse_start_request_bytes(deeply_nested, "deep-correlation")
        self.assertEqual(deep_error.exception.http_status, 400)

        overflow = (
            b'{"repository_id":"main","ref":"refs/heads/main",'
            b'"manifest_path":"orchestration/sprint.json",'
            b'"idempotency_key":"key","extra":1e1000000}'
        )
        with self.assertRaises(ManagedImportError) as overflow_error:
            parse_start_request_bytes(overflow, "overflow-correlation")
        self.assertEqual(overflow_error.exception.http_status, 400)


class ManagedStartRouteTests(unittest.IsolatedAsyncioTestCase):
    payload = {
        "repository_id": "main",
        "ref": "refs/heads/managed",
        "manifest_path": "orchestration/sprint.json",
        "idempotency_key": "route-key",
    }

    async def test_route_returns_dynamic_success_status(self) -> None:
        response = {
            "sprint_id": "msv1-" + "a" * 64,
            "status": "active",
            "phase": "ACTIVATE",
            "deduplicated": False,
            "execution_mode": "sequential",
            "identity": {
                "project_id": "9002",
                "repository_id": "main",
                "commit": "b" * 40,
                "manifest_path": "orchestration/sprint.json",
                "manifest_sha256": "c" * 64,
            },
            "workspace_source_commit": "b" * 40,
            "initial_assignment_ids": ["assignment-a"],
        }

        class Importer:
            def start(self, project_id, request):
                self.project_id = project_id
                self.request = request
                return ManagedStartResult(response, 201)

        importer = Importer()
        body = json.dumps(self.payload).encode("utf-8")
        project_entry = {
            "project_phone": "9002",
            "git_address": "https://github.com/example/repository.git",
        }
        with (
            patch.object(main, "read_git_config", AsyncMock(return_value={})),
            patch.object(
                main,
                "project_for_group_api",
                return_value=("key", "context", project_entry, project_entry),
            ),
            patch.object(main, "managed_repository_registry_for_project", return_value={}),
            patch.object(main, "load_managed_runtime_config", return_value={}),
            patch.object(main, "managed_sprint_importer_factory", return_value=importer),
        ):
            http_status, decoded = await asgi_raw_request(
                "/api/v1/projects/9002/sprints/start-from-git", body
            )
        self.assertEqual(http_status, 201)
        self.assertEqual(decoded, response)
        self.assertEqual(importer.project_id, "9002")
        self.assertEqual(importer.request.idempotency_key, "route-key")

    async def test_malformed_request_stops_before_project_lookup(self) -> None:
        read_config = AsyncMock(return_value={})
        with patch.object(main, "read_git_config", read_config):
            http_status, decoded = await asgi_raw_request(
                "/api/v1/projects/9002/sprints/start-from-git",
                b'{"repository_id":"main","repository_id":"other"}',
            )
        self.assertEqual(http_status, 400)
        self.assertEqual(
            decoded["detail"]["error"], "INVALID_MANAGED_SPRINT_REQUEST"
        )
        self.assertTrue(decoded["detail"]["correlation_id"])
        read_config.assert_not_awaited()

    async def test_non_scalar_unicode_and_oversized_body_fail_before_lookup(self) -> None:
        invalid_unicode = (
            b'{"repository_id":"main","ref":"refs/heads/managed",'
            b'"manifest_path":"orchestration/sprint.json",'
            b'"idempotency_key":"\\ud800"}'
        )
        read_config = AsyncMock(return_value={})
        with patch.object(main, "read_git_config", read_config):
            status_unicode, unicode_body = await asgi_raw_request(
                "/api/v1/projects/9002/sprints/start-from-git",
                invalid_unicode,
            )
            status_large, large_body = await asgi_raw_request(
                "/api/v1/projects/9002/sprints/start-from-git",
                b"{" + b"x" * (4 * 1024 * 1024) + b"}",
            )
        self.assertEqual(status_unicode, 400)
        self.assertEqual(status_large, 400)
        self.assertEqual(
            unicode_body["detail"]["error"], "INVALID_MANAGED_SPRINT_REQUEST"
        )
        self.assertEqual(
            large_body["detail"]["error"], "INVALID_MANAGED_SPRINT_REQUEST"
        )
        read_config.assert_not_awaited()

    async def test_config_read_failure_uses_managed_error_envelope(self) -> None:
        with patch.object(
            main,
            "read_git_config",
            AsyncMock(side_effect=OSError("injected config failure")),
        ):
            http_status, decoded = await asgi_raw_request(
                "/api/v1/projects/9002/sprints/start-from-git",
                json.dumps(self.payload).encode("utf-8"),
            )
        self.assertEqual(http_status, 500)
        self.assertEqual(decoded["detail"]["error"], "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(decoded["detail"]["phase"], "VALIDATE")
        self.assertTrue(decoded["detail"]["correlation_id"])


if __name__ == "__main__":
    unittest.main()
