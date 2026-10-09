from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import socket
import stat
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import nginx_qa.process_supervisor as process_supervisor_module
from nginx_qa.process_supervisor import (
    ManagedProcessSupervisor,
    ManagedProcessSupervisorError,
    ORPHAN_PROCESS,
    PROCESS_LAUNCH_FAILED,
    PROCESS_STATE_CONFLICT,
    port_owner_pids,
    process_identity,
    service_protected_pids,
)
from nginx_qa.managed_import import TransactionalSprintImporter
from nginx_qa.port_leases import ManagedPortReservationRegistry
from nginx_qa.sprint_types import (
    canonical_json_sha256,
    managed_activation_invariant_issues,
)
from tests.test_managed_import import ManagedImportFixture


FIXTURE = Path(__file__).parent / "fixtures" / "managed_child_service.py"


class SimulatedServiceCrash(BaseException):
    pass


class ProtectedProcessDiscoveryTests(unittest.TestCase):
    def test_service_and_every_discovered_ancestor_are_protected(self) -> None:
        protected = service_protected_pids()
        current = os.getpid()
        expected: set[int] = set()
        parent_map = (
            process_supervisor_module._windows_parent_map()
            if os.name == "nt"
            else None
        )
        while current > 0 and current not in expected:
            expected.add(current)
            parent = (
                parent_map.get(current)
                if parent_map is not None
                else process_supervisor_module._linux_parent_pid(current)
            )
            if parent is None:
                self.assertTrue(
                    process_supervisor_module._pid_definitely_dead(current)
                )
                current = 0
                break
            current = parent
        self.assertEqual(current, 0)
        self.assertEqual(protected, frozenset(expected))

    def test_incomplete_ancestry_fails_closed(self) -> None:
        helper = (
            "_windows_parent_map" if os.name == "nt" else "_linux_parent_pid"
        )
        incomplete = {} if os.name == "nt" else None
        with patch.object(
            process_supervisor_module,
            helper,
            return_value=incomplete,
        ):
            with self.assertRaises(ManagedProcessSupervisorError):
                service_protected_pids()

    def test_ancestry_cycle_fails_closed(self) -> None:
        if os.name == "nt":
            replacement = patch.object(
                process_supervisor_module,
                "_windows_parent_map",
                return_value={os.getpid(): os.getpid()},
            )
        else:
            replacement = patch.object(
                process_supervisor_module,
                "_linux_parent_pid",
                return_value=os.getpid(),
            )
        with replacement:
            with self.assertRaises(ManagedProcessSupervisorError):
                service_protected_pids()


@unittest.skipUnless(
    os.name == "nt",
    "managed launches require assignment-wide Windows Job limits",
)
class ManagedProcessSupervisorTests(ManagedImportFixture):
    def setUp(self) -> None:
        super().setUp()
        self.supervisors: list[ManagedProcessSupervisor] = []
        start, end = self.available_port_range(8)
        self.runtime_config["child_port_start"] = start
        self.runtime_config["child_port_end"] = end

    def tearDown(self) -> None:
        for supervisor in reversed(self.supervisors):
            try:
                try:
                    contexts = supervisor._repository.contexts()
                except ManagedProcessSupervisorError:
                    contexts = ()
                for context in contexts:
                    if context.process.get("state") in {
                        "PREPARED",
                        "STARTING",
                        "HEALTHY",
                        "STOPPING",
                    }:
                        try:
                            supervisor.stop(
                                context.project_id,
                                context.sprint_id,
                                str(context.process["process_id"]),
                            )
                        except ManagedProcessSupervisorError:
                            pass
            finally:
                supervisor.close()
        super().tearDown()

    @staticmethod
    def available_port_range(size: int) -> tuple[int, int]:
        for start in range(20000, 59000 - size):
            sockets: list[socket.socket] = []
            try:
                for port in range(start, start + size):
                    candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    try:
                        if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                            candidate.setsockopt(
                                socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1
                            )
                        candidate.bind(("127.0.0.1", port))
                    except BaseException:
                        candidate.close()
                        raise
                    sockets.append(candidate)
                return start, start + size - 1
            except OSError:
                continue
            finally:
                for candidate in sockets:
                    candidate.close()
        raise AssertionError("no contiguous localhost test port range is available")

    def install_service(self) -> str:
        # Git stores text blobs with LF even when a Windows checkout uses
        # core.autocrlf=true.  Build the manifest checksum from those same
        # canonical bytes so a fresh checkout exercises the supervisor too.
        payload = FIXTURE.read_bytes().replace(b"\r\n", b"\n")
        (self.source / "service.py").write_bytes(payload)
        return hashlib.sha256(payload).hexdigest()

    def push_manifest(self, manifest: dict, message: str) -> str:
        (self.source / self.manifest_path).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self.git("add", ".", cwd=self.source)
        self.git("commit", "-m", message, cwd=self.source)
        self.git("push", str(self.remote), "main", cwd=self.source)
        return self.git("rev-parse", "HEAD", cwd=self.source)

    @staticmethod
    def process_spec(*arguments: str, restart: bool = False) -> dict[str, object]:
        return {
            "command": [sys.executable, "service.py", *arguments],
            "cwd": ".",
            "environment": {},
            "health_path": "/health",
            "restart_policy": "on_failure" if restart else "never",
            "max_restart_attempts": 1 if restart else 0,
            "restart_backoff_seconds": 0,
            "resource_limits": {
                "wall_time_seconds": 120,
                "memory_bytes": 268435456,
                "cpu_percent": 100,
                "process_count": 8,
            },
        }

    def activate_manifest(self, manifest: dict) -> tuple[object, object]:
        self.push_manifest(manifest, "managed process supervisor fixture")
        importer = self.importer()
        result = importer.start(self.project_id, self.request)
        return importer, result

    def supervisor(self, importer: object) -> ManagedProcessSupervisor:
        supervisor = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,  # type: ignore[attr-defined]
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            poll_interval_seconds=0.05,
        )
        self.supervisors.append(supervisor)
        return supervisor

    @staticmethod
    def create_directory_junction(link: Path, target: Path) -> None:
        result = subprocess.run(
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise AssertionError(
                f"could not create test junction: {result.stdout} {result.stderr}"
            )
        if not link.is_junction():
            raise AssertionError("mklink did not create a directory junction")

    @staticmethod
    def remove_directory_junction(link: Path) -> None:
        if os.path.lexists(link):
            os.rmdir(link)

    def single_process_manifest(
        self, *arguments: str, restart: bool = False
    ) -> dict:
        checksum = self.install_service()
        manifest = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        manifest["files"] = [{"path": "service.py", "sha256": checksum}]
        manifest["nodes"][0]["workspace"]["process"] = self.process_spec(
            *arguments, restart=restart
        )
        return manifest

    def four_process_manifest(self) -> dict:
        checksum = self.install_service()
        manifest = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        children = []
        for index in range(4):
            node_id = f"child-{index + 1}"
            children.append(
                {
                    "id": node_id,
                    "agent": {
                        "id": f"child-agent-{index + 1}",
                        "name": f"Child Agent {index + 1}",
                        "phone": f"28{70 + index}",
                    },
                    "tasks": [
                        {
                            "task_id": f"CHILD-{index + 1}",
                            "queue": "worker-all",
                            "message": f"Run child {index + 1}",
                        }
                    ],
                    "workspace": {
                        "access": "read",
                        "process": self.process_spec("--mode", "serve"),
                    },
                    "transitions": {"DONE": "completed"},
                }
            )
        continuity = manifest["nodes"][1]
        continuity["transitions"]["RESUME"] = "child-1"
        terminal = manifest["nodes"][2]
        manifest["nodes"] = [*children, continuity, terminal]
        manifest["execution"].pop("start_node")
        manifest["execution"].update(
            {
                "mode": "parallel",
                "start_nodes": [node["id"] for node in children],
            }
        )
        manifest["files"] = [{"path": "service.py", "sha256": checksum}]
        return manifest

    def test_prepared_runtime_v1_is_atomically_upgraded_before_supervision(
        self,
    ) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        state["schema_version"] = 1
        process = state["processes"][0]
        for key in (
            "os_process_birth_token",
            "startup_deadline_at",
            "terminal_reason",
        ):
            process.pop(key)
        definition = state["graph_revisions"][0]["definition"]
        launch = definition["nodes"][0]["workspace"]["process"]
        legacy_command = [*process["command_redacted"], "legacy\0argument"]
        legacy_environment = {
            "": "legacy-empty-name",
            "APP_MODE": "one",
            "app_mode": "two",
            "LEGACY_NUL": "bad\0value",
        }
        launch["command"] = legacy_command
        launch["environment"] = legacy_environment
        launch["health_path"] = "/ready now"
        process["command_redacted"] = legacy_command
        process["environment_redacted"] = legacy_environment
        process["health_endpoint"]["path"] = "/ready now"
        state["graph_revisions"][0]["definition_sha256"] = canonical_json_sha256(
            definition
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        encoded_state = json.dumps(
            state, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        encoded_process = json.dumps(
            process, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        with importer.store._transaction() as connection:
            connection.execute(
                """
                UPDATE managed_sprints SET state_json = ?
                WHERE project_id = ? AND sprint_id = ?
                """,
                (encoded_state, self.project_id, result.response["sprint_id"]),
            )
            connection.execute(
                """
                UPDATE managed_process_owners SET process_json = ?
                WHERE process_id = ?
                """,
                (encoded_process, process["process_id"]),
            )

        supervisor = self.supervisor(importer)
        upgraded = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert upgraded is not None
        self.assertEqual(upgraded["schema_version"], 2)
        self.assertEqual(upgraded["migrated_from_runtime_schema_version"], 1)
        self.assertEqual(
            upgraded["processes"][0]["migrated_from_runtime_schema_version"], 1
        )
        self.assertEqual(
            upgraded["processes"][0]["command_redacted"], legacy_command
        )
        self.assertEqual(
            upgraded["processes"][0]["environment_redacted"], legacy_environment
        )
        self.assertEqual(
            upgraded["processes"][0]["health_endpoint"]["path"], "/ready now"
        )
        self.assertIsNone(upgraded["processes"][0]["os_process_birth_token"])
        self.assertIsNone(upgraded["processes"][0]["startup_deadline_at"])
        self.assertIsNone(upgraded["processes"][0]["terminal_reason"])
        self.assertEqual(managed_activation_invariant_issues(upgraded), ())
        stopped = supervisor.stop(
            self.project_id,
            result.response["sprint_id"],
            process["process_id"],
        )
        self.assertEqual(stopped.state, "FAILED")
        self.assertEqual(stopped.action, "cancelled-before-launch")

    def test_runtime_v1_upgrade_rolls_back_on_normalized_owner_mismatch(
        self,
    ) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        state["schema_version"] = 1
        process = state["processes"][0]
        for key in (
            "os_process_birth_token",
            "startup_deadline_at",
            "terminal_reason",
        ):
            process.pop(key)
        encoded_state = json.dumps(
            state, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        with importer.store._transaction() as connection:
            owner_before = connection.execute(
                """
                SELECT process_json FROM managed_process_owners
                WHERE process_id = ?
                """,
                (process["process_id"],),
            ).fetchone()["process_json"]
            connection.execute(
                """
                UPDATE managed_sprints SET state_json = ?
                WHERE project_id = ? AND sprint_id = ?
                """,
                (encoded_state, self.project_id, result.response["sprint_id"]),
            )

        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            self.supervisor(importer)
        self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)

        with importer.store._transaction() as connection:
            stored_state = connection.execute(
                """
                SELECT state_json FROM managed_sprints
                WHERE project_id = ? AND sprint_id = ?
                """,
                (self.project_id, result.response["sprint_id"]),
            ).fetchone()["state_json"]
            stored_owner = connection.execute(
                """
                SELECT process_json FROM managed_process_owners
                WHERE process_id = ?
                """,
                (process["process_id"],),
            ).fetchone()["process_json"]
        self.assertEqual(stored_state, encoded_state)
        self.assertEqual(stored_owner, owner_before)

    def test_runtime_schema_version_bool_and_float_fail_closed(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        original = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert original is not None
        try:
            for invalid_version in (True, 1.0):
                with self.subTest(schema_version=invalid_version):
                    corrupt = dict(original)
                    corrupt["schema_version"] = invalid_version
                    encoded = json.dumps(
                        corrupt,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    with importer.store._transaction() as connection:
                        connection.execute(
                            """
                            UPDATE managed_sprints SET state_json = ?
                            WHERE project_id = ? AND sprint_id = ?
                            """,
                            (
                                encoded,
                                self.project_id,
                                result.response["sprint_id"],
                            ),
                        )
                    with self.assertRaises(ManagedProcessSupervisorError) as raised:
                        ManagedProcessSupervisor(
                            self.runtime_config,
                            importer.store,
                            self.port_reservations,
                        )
                    self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
        finally:
            encoded = json.dumps(
                original,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_sprints SET state_json = ?
                    WHERE project_id = ? AND sprint_id = ?
                    """,
                    (encoded, self.project_id, result.response["sprint_id"]),
                )

    def test_launch_prepared_to_healthy_and_stop_releases_lease(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)

        outcomes = supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual([item.state for item in outcomes], ["HEALTHY"])
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        lease = state["port_leases"][0]
        self.assertEqual(process["state"], "HEALTHY")
        self.assertIsNotNone(process_identity(process["pid"]))
        self.assertEqual(lease["status"], "bound")
        self.assertTrue(lease["bind_verified"])
        self.assertEqual(lease["process_id"], process["process_id"])
        self.assertTrue(Path(process["stdout_log"]).is_file())
        self.assertTrue(Path(process["stderr_log"]).is_file())
        self.assertTrue(
            (Path(process["runtime_root"]) / "managed-child-marker.json").is_file()
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())

        stopped = supervisor.stop(
            self.project_id, result.response["sprint_id"], process["process_id"]
        )
        self.assertEqual(stopped.state, "STOPPED")
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(state["processes"][0]["state"], "STOPPED")
        self.assertEqual(state["port_leases"][0]["status"], "released")
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_normalized_lease_drift_is_rejected_before_spawn(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        lease = state["port_leases"][0]
        encoded_lease = json.dumps(
            lease, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        corrupt_lease = dict(lease)
        corrupt_lease["port"] = int(lease["port"]) + 1
        encoded_corrupt_lease = json.dumps(
            corrupt_lease,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        try:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_port_leases SET lease_json = ?
                    WHERE lease_id = ?
                    """,
                    (encoded_corrupt_lease, lease["lease_id"]),
                )
            with patch.object(supervisor, "_spawn") as spawn:
                with self.assertRaises(ManagedProcessSupervisorError) as raised:
                    supervisor.launch_prepared(
                        self.project_id,
                        result.response["sprint_id"],
                        process["process_id"],
                    )
                self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
                spawn.assert_not_called()
        finally:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_port_leases SET lease_json = ?
                    WHERE lease_id = ?
                    """,
                    (encoded_lease, lease["lease_id"]),
                )

    def test_corrupt_workspace_snapshot_is_rejected_before_spawn(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process_id = state["processes"][0]["process_id"]
        original_state = json.dumps(
            state, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        corrupt_state = json.loads(original_state)
        corrupt_state["workspaces"][0]["actual_git_toplevel"] = 7
        encoded_corrupt_state = json.dumps(
            corrupt_state,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        try:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_sprints SET state_json = ?
                    WHERE project_id = ? AND sprint_id = ?
                    """,
                    (
                        encoded_corrupt_state,
                        self.project_id,
                        result.response["sprint_id"],
                    ),
                )
            with patch.object(supervisor, "_spawn") as spawn:
                with self.assertRaises(ManagedProcessSupervisorError) as raised:
                    supervisor.launch_prepared(
                        self.project_id,
                        result.response["sprint_id"],
                        process_id,
                    )
                self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
                spawn.assert_not_called()
        finally:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_sprints SET state_json = ?
                    WHERE project_id = ? AND sprint_id = ?
                    """,
                    (
                        original_state,
                        self.project_id,
                        result.response["sprint_id"],
                    ),
                )

    def test_active_projection_drift_fails_before_port_reconciliation(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        lease = state["port_leases"][0]
        corrupt_port = int(lease["port"]) + 1000
        try:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_port_leases SET port = ?
                    WHERE lease_id = ?
                    """,
                    (corrupt_port, lease["lease_id"]),
                )
            with patch.object(
                self.port_reservations,
                "reconcile_durable",
            ) as reconcile_ports:
                with self.assertRaises(ManagedProcessSupervisorError) as raised:
                    supervisor.reconcile(
                        project_id=self.project_id,
                        sprint_id=result.response["sprint_id"],
                        start_prepared=False,
                    )
                self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
                reconcile_ports.assert_not_called()
        finally:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_port_leases SET port = ?
                    WHERE lease_id = ?
                    """,
                    (lease["port"], lease["lease_id"]),
                )

    def test_normalized_running_drift_is_rejected_before_termination(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        launched = supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual([outcome.state for outcome in launched], ["HEALTHY"])
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        lease = state["port_leases"][0]
        encoded_lease = json.dumps(
            lease, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        corrupt_lease = dict(lease)
        corrupt_lease["bind_verified"] = False
        encoded_corrupt_lease = json.dumps(
            corrupt_lease,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        try:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_port_leases SET lease_json = ?
                    WHERE lease_id = ?
                    """,
                    (encoded_corrupt_lease, lease["lease_id"]),
                )
            with patch.object(supervisor, "_terminate_context") as terminate:
                with self.assertRaises(ManagedProcessSupervisorError) as raised:
                    supervisor.reconcile(
                        project_id=self.project_id,
                        sprint_id=result.response["sprint_id"],
                    )
                self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
                terminate.assert_not_called()
        finally:
            with importer.store._transaction() as connection:
                connection.execute(
                    """
                    UPDATE managed_port_leases SET lease_json = ?
                    WHERE lease_id = ?
                    """,
                    (encoded_lease, lease["lease_id"]),
                )

    def test_workspace_junction_drift_is_rejected_before_spawn(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        workspace_root = Path(str(context.workspace["actual_git_toplevel"]))
        backup = workspace_root.with_name(workspace_root.name + "-original")
        workspace_root.rename(backup)
        try:
            self.create_directory_junction(workspace_root, self.source)
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                supervisor._validate_workspace_and_executable(context)
            self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
        finally:
            self.remove_directory_junction(workspace_root)
            backup.rename(workspace_root)

    def test_corrupt_environment_and_command_snapshot_fail_before_spawn(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]

        for environment in (
            {"": "value"},
            {"BAD=NAME": "value"},
            {"BAD\0NAME": "value"},
            {"APP_MODE": "one", "app_mode": "two"},
            {"GOOD_NAME": "bad\0value"},
            {"nGiNx_Qa_MaNaGeD_Port": "bad"},
        ):
            with self.subTest(environment=environment):
                context.process["environment_redacted"] = environment
                with self.assertRaises(ManagedProcessSupervisorError) as raised:
                    supervisor._child_environment(context)
                self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)

        context.process["environment_redacted"] = {"path": "explicit-path"}
        child_environment = supervisor._child_environment(context)
        self.assertEqual(child_environment["path"], "explicit-path")
        self.assertEqual(
            [key for key in child_environment if key.casefold() == "path"],
            ["path"],
        )

        context.process["environment_redacted"] = {}
        context.process["command_redacted"] = [
            sys.executable,
            "service.py\0redirected",
        ]
        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor._spawn(context, None, None)
        self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)

        context.process["health_endpoint"]["path"] = "/готов"
        with patch.object(supervisor, "_port_owned", return_value=True):
            self.assertFalse(supervisor._health_ok(context, timeout=0.2))

    def test_executable_parent_junction_drift_is_rejected_before_spawn(self) -> None:
        frozen_parent = self.base / "frozen-bin"
        frozen_parent.mkdir()
        frozen_executable = frozen_parent / "python.exe"
        frozen_executable.write_bytes(Path(sys.executable).read_bytes())
        replacement_parent = self.base / "replacement-bin"
        replacement_parent.mkdir()
        (replacement_parent / "python.exe").write_bytes(b"not-the-frozen-executable")
        manifest = self.single_process_manifest("--mode", "serve")
        manifest["nodes"][0]["workspace"]["process"]["command"][0] = str(
            frozen_executable
        )
        importer, result = self.activate_manifest(manifest)
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        backup = frozen_parent.with_name(frozen_parent.name + "-original")
        frozen_parent.rename(backup)
        try:
            self.create_directory_junction(frozen_parent, replacement_parent)
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                supervisor._validate_workspace_and_executable(context)
            self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
        finally:
            self.remove_directory_junction(frozen_parent)
            backup.rename(frozen_parent)

    def test_cwd_junction_drift_within_workspace_is_rejected_before_spawn(
        self,
    ) -> None:
        (self.source / "run").mkdir()
        (self.source / "run" / "marker.txt").write_text("frozen", encoding="utf-8")
        (self.source / "other").mkdir()
        (self.source / "other" / "marker.txt").write_text(
            "replacement", encoding="utf-8"
        )
        manifest = self.single_process_manifest("--mode", "serve")
        manifest["nodes"][0]["workspace"]["process"]["cwd"] = "run"
        importer, result = self.activate_manifest(manifest)
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        cwd = Path(str(context.process["cwd"]))
        replacement = cwd.parent / "other"
        backup = cwd.with_name(cwd.name + "-original")
        cwd.rename(backup)
        try:
            self.create_directory_junction(cwd, replacement)
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                supervisor._validate_workspace_and_executable(context)
            self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
        finally:
            self.remove_directory_junction(cwd)
            backup.rename(cwd)

    def test_runtime_roots_reject_windows_junction_redirection(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        process_id = str(context.process["process_id"])
        protected = Path(self.runtime_config["protected_roots"][0])
        protected.mkdir(parents=True, exist_ok=True)

        for key in ("process_runtime_root", "log_root", "pid_root"):
            with self.subTest(root=key):
                root = Path(self.runtime_config[key])
                root.parent.mkdir(parents=True, exist_ok=True)
                backup = root.with_name(root.name + "-original")
                had_root = root.exists()
                if had_root:
                    root.rename(backup)
                try:
                    self.create_directory_junction(root, protected)
                    with self.assertRaises(ManagedProcessSupervisorError) as raised:
                        supervisor._validated_configured_root(key, process_id)
                    self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
                finally:
                    self.remove_directory_junction(root)
                    if had_root:
                        backup.rename(root)

    def test_log_root_swap_after_intent_is_rejected_before_log_create(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        log_root = Path(self.runtime_config["log_root"])
        backup = log_root.with_name(log_root.name + "-original")
        protected = Path(self.runtime_config["protected_roots"][0])
        protected.mkdir(parents=True, exist_ok=True)

        def swap_log_root() -> None:
            log_root.rename(backup)
            self.create_directory_junction(log_root, protected)

        try:
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                supervisor._prepare_paths(context, before_logs=swap_log_root)
            self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
            self.assertEqual(tuple(protected.iterdir()), ())
        finally:
            self.remove_directory_junction(log_root)
            if backup.exists():
                backup.rename(log_root)

    def test_pid_receipt_write_rejects_root_swap_before_parent_pin(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        process_id = str(context.process["process_id"])
        pid_root = Path(self.runtime_config["pid_root"])
        pid_root.mkdir(parents=True, exist_ok=True)
        backup = pid_root.with_name(pid_root.name + "-original")
        protected = Path(self.runtime_config["protected_roots"][0])
        protected.mkdir(parents=True, exist_ok=True)

        pid_root.rename(backup)
        try:
            self.create_directory_junction(pid_root, protected)
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                supervisor._write_launch_intent(context)
            self.assertEqual(raised.exception.code, PROCESS_STATE_CONFLICT)
            self.assertEqual(tuple(protected.iterdir()), ())
        finally:
            if pid_root.is_junction():
                self.remove_directory_junction(pid_root)
            if backup.exists():
                backup.rename(pid_root)
            (protected / f"{process_id}.json").unlink(missing_ok=True)
            for temporary in protected.glob(f".{process_id}.json.*.tmp"):
                temporary.unlink(missing_ok=True)

    def test_pid_receipt_parent_pin_blocks_swap_during_write_and_remove(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        process_id = str(context.process["process_id"])
        pid_root = Path(self.runtime_config["pid_root"])
        pid_root.mkdir(parents=True, exist_ok=True)
        backup = pid_root.with_name(pid_root.name + "-original")
        protected = Path(self.runtime_config["protected_roots"][0])
        protected.mkdir(parents=True, exist_ok=True)
        intent = supervisor._write_launch_intent(context)
        identity = process_identity(os.getpid())
        self.assertIsNotNone(identity)
        assert identity is not None
        job_id = supervisor._windows_job_name(context)
        write_swap_succeeded: list[bool] = []
        remove_swap_succeeded: list[bool] = []
        original_atomic_write = process_supervisor_module._atomic_write_json
        original_unlink = process_supervisor_module.os.unlink

        def atomic_write_after_swap_attempt(
            path: Path,
            value: dict,
            *,
            directory_fd: int | None = None,
        ) -> None:
            try:
                pid_root.rename(backup)
            except OSError:
                write_swap_succeeded.append(False)
            else:
                write_swap_succeeded.append(True)
                self.create_directory_junction(pid_root, protected)
            original_atomic_write(path, value, directory_fd=directory_fd)

        def unlink_after_swap_attempt(path: object, *args: object, **kwargs: object) -> None:
            try:
                pid_root.rename(backup)
            except OSError:
                remove_swap_succeeded.append(False)
            else:
                remove_swap_succeeded.append(True)
                self.create_directory_junction(pid_root, protected)
            original_unlink(path, *args, **kwargs)

        try:
            with patch.object(
                process_supervisor_module,
                "_atomic_write_json",
                side_effect=atomic_write_after_swap_attempt,
            ):
                receipt = supervisor._write_receipt(
                    context,
                    identity,
                    job_id,
                    job_id,
                    phase="launch_gated",
                    startup_deadline_at=str(intent["startup_deadline_at"]),
                )
            self.assertEqual(receipt["phase"], "launch_gated")
            self.assertEqual(write_swap_succeeded, [False])

            with patch.object(
                process_supervisor_module.os,
                "unlink",
                side_effect=unlink_after_swap_attempt,
            ):
                supervisor._remove_pid_receipt(process_id)
            self.assertEqual(remove_swap_succeeded, [False])
            self.assertFalse((pid_root / f"{process_id}.json").exists())
            self.assertEqual(tuple(protected.iterdir()), ())
        finally:
            (protected / f"{process_id}.json").unlink(missing_ok=True)
            for temporary in protected.glob(f".{process_id}.json.*.tmp"):
                temporary.unlink(missing_ok=True)
            if pid_root.is_junction():
                self.remove_directory_junction(pid_root)
            if backup.exists():
                backup.rename(pid_root)

    def test_missing_windows_directory_chain_is_created_by_pinned_native_open(
        self,
    ) -> None:
        importer = self.importer()
        supervisor = self.supervisor(importer)
        target = self.base / "native-directory-parent" / "nested-child"

        with patch.object(
            Path,
            "mkdir",
            side_effect=AssertionError("un-pinned mkdir must not be used"),
        ):
            supervisor._ensure_windows_directory(
                target,
                process_id="native-directory-test",
                purpose="native directory test",
            )

        self.assertTrue(target.is_dir())
        descriptor = supervisor._open_windows_verified_path(
            target,
            "native-directory-test",
            purpose="native directory test",
            writable=False,
            create=False,
            directory=True,
            unique=False,
            block_rename=True,
            share_write=True,
        )
        try:
            with self.assertRaises(OSError):
                target.rename(target.with_name("retargeted-child"))
        finally:
            os.close(descriptor)

    def test_windows_action_mutex_does_not_touch_legacy_lock_root(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        process_id = str(context.process["process_id"])
        pid_root = Path(self.runtime_config["pid_root"])
        pid_root.mkdir(parents=True, exist_ok=True)
        lock_root = pid_root / ".action-locks"
        target = self.base / "redirected-action-locks"
        target.mkdir(parents=True, exist_ok=True)
        self.create_directory_junction(lock_root, target)
        try:
            with supervisor._process_action_guard(process_id):
                pass
            self.assertEqual(tuple(target.iterdir()), ())
        finally:
            self.remove_directory_junction(lock_root)

    def test_windows_action_mutex_does_not_touch_legacy_lock_leaf(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        process_id = str(context.process["process_id"])
        lock_root = Path(self.runtime_config["pid_root"]) / ".action-locks"
        lock_root.mkdir(parents=True, exist_ok=True)
        lock_path = lock_root / (
            hashlib.sha256(process_id.encode("utf-8")).hexdigest() + ".lock"
        )
        target = self.base / "must-not-be-written-by-action-lock"
        target.write_bytes(b"")
        os.link(target, lock_path)
        try:
            with supervisor._process_action_guard(process_id):
                pass
            self.assertEqual(target.read_bytes(), b"")
        finally:
            lock_path.unlink(missing_ok=True)

    def test_windows_action_mutex_key_survives_instance_config_drift(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        first = self.supervisor(importer)
        context = first._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        drifted_config = dict(self.runtime_config)
        drifted_config["instance_id"] = "drifted-service-instance"
        second = ManagedProcessSupervisor(
            drifted_config,
            importer.store,
            self.port_reservations,
        )
        self.supervisors.append(second)
        process_id = str(context.process["process_id"])
        self.assertEqual(
            first._windows_action_mutex_name(process_id),
            second._windows_action_mutex_name(process_id),
        )

    def test_receipt_recovery_rejects_config_drift_before_claim(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        owner = self.supervisor(importer)
        context = owner._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        process_id = str(context.process["process_id"])
        receipt_path = owner._pid_receipt_path(process_id)
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_text("{}", encoding="utf-8")

        drifted_config = dict(self.runtime_config)
        drifted_config["instance_id"] = "drifted-service-instance"
        drifted = ManagedProcessSupervisor(
            drifted_config,
            importer.store,
            self.port_reservations,
        )
        self.supervisors.append(drifted)
        with (
            patch.object(
                drifted._repository,
                "claim_prepared",
                wraps=drifted._repository.claim_prepared,
            ) as claim_prepared,
            self.assertRaises(ManagedProcessSupervisorError) as raised,
        ):
            drifted.launch_prepared(
                self.project_id,
                result.response["sprint_id"],
                process_id,
            )
        self.assertEqual(
            raised.exception.code,
            process_supervisor_module.RUNTIME_CONFIG_DRIFT,
        )
        claim_prepared.assert_not_called()

        claim = owner._repository.claim_prepared(
            self.project_id,
            result.response["sprint_id"],
            process_id,
            owner="claim-verifier",
            ttl_seconds=30,
        )
        self.assertIsNotNone(claim)
        assert claim is not None
        owner._repository.release_claim(
            process_id,
            owner="claim-verifier",
            supervisor_fence=claim.supervisor_fence,
        )

    def test_windows_launch_path_pins_block_retarget_before_resume(self) -> None:
        frozen_parent = self.base / "pinned-bin"
        frozen_parent.mkdir()
        frozen_executable = frozen_parent / "python.exe"
        frozen_executable.write_bytes(Path(sys.executable).read_bytes())
        (self.source / "run").mkdir()
        (self.source / "run" / "marker.txt").write_text("cwd", encoding="utf-8")
        (self.source / "other").mkdir()
        (self.source / "other" / "marker.txt").write_text("other", encoding="utf-8")
        manifest = self.single_process_manifest("--mode", "serve")
        process_spec = manifest["nodes"][0]["workspace"]["process"]
        process_spec["command"][0] = str(frozen_executable)
        process_spec["cwd"] = "run"
        importer, result = self.activate_manifest(manifest)
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        runtime_root = Path(str(context.process["runtime_root"]))
        runtime_root.parent.mkdir(parents=True, exist_ok=True)
        runtime_root.mkdir()
        pins = supervisor._pin_windows_launch_paths(context)
        moved: list[tuple[Path, Path]] = []
        try:
            candidates = (
                Path(str(context.process["cwd"])),
                runtime_root,
                frozen_executable,
                Path(str(context.workspace["actual_git_toplevel"])),
            )
            for candidate in candidates:
                with self.subTest(path=candidate.name):
                    try:
                        replacement = candidate.with_name(candidate.name + "-moved")
                        candidate.rename(replacement)
                    except OSError:
                        continue
                    moved.append((candidate, replacement))
            self.assertEqual(moved, [])
            supervisor._revalidate_windows_launch_pins(
                pins,
                process_id=str(context.process["process_id"]),
            )
        finally:
            for pin in reversed(pins):
                os.close(pin.descriptor)
            for original, replacement in reversed(moved):
                replacement.rename(original)

    def test_launch_path_pin_closes_unpublished_descriptor_on_fstat_failure(
        self,
    ) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        directory_metadata = SimpleNamespace(
            st_mode=stat.S_IFDIR,
            st_dev=1,
            st_ino=101,
        )
        file_metadata = SimpleNamespace(
            st_mode=stat.S_IFREG,
            st_dev=1,
            st_ino=102,
        )

        with (
            patch.object(
                supervisor,
                "_open_windows_verified_path",
                side_effect=(10, 11, 12, 13),
            ),
            patch.object(
                process_supervisor_module.os,
                "fstat",
                side_effect=(
                    directory_metadata,
                    file_metadata,
                    OSError("injected fstat failure"),
                ),
            ),
            patch.object(process_supervisor_module.os, "close") as close,
        ):
            with self.assertRaisesRegex(OSError, "injected fstat failure"):
                supervisor._pin_windows_launch_paths(context)

        self.assertEqual(
            [call.args[0] for call in close.call_args_list],
            [11, 13, 12, 10],
        )

    def test_pid_receipt_envelope_size_and_leaf_identity_fail_closed(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        process_id = str(context.process["process_id"])
        receipt_path = Path(self.runtime_config["pid_root"]) / f"{process_id}.json"
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        original = supervisor._write_launch_intent(context)
        self.assertEqual(supervisor._receipt(process_id), original)

        for field, invalid in (("schema_version", 2), ("created_at", "invalid")):
            with self.subTest(field=field):
                corrupt = dict(original)
                corrupt[field] = invalid
                receipt_path.write_text(json.dumps(corrupt), encoding="utf-8")
                with self.assertRaises(ManagedProcessSupervisorError) as raised:
                    supervisor._receipt(process_id)
                self.assertEqual(raised.exception.code, ORPHAN_PROCESS)

        receipt_path.write_bytes(b"x" * ((64 * 1024) + 1))
        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor._receipt(process_id)
        self.assertEqual(raised.exception.code, ORPHAN_PROCESS)

        receipt_path.unlink()
        target = self.base / "receipt-hardlink-target.json"
        target.write_text(json.dumps(original), encoding="utf-8")
        os.link(target, receipt_path)
        try:
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                supervisor._receipt(process_id)
            self.assertEqual(raised.exception.code, ORPHAN_PROCESS)
        finally:
            receipt_path.unlink(missing_ok=True)

    def test_persistent_runtime_health_failure_is_terminalized(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]

        with patch.object(supervisor, "_health_ok", return_value=False):
            first = supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            second = supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        with patch.object(
            process_supervisor_module,
            "port_owner_pids",
            return_value=frozenset(),
        ):
            third = supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )

        self.assertEqual(first[0].action, "health-degraded")
        self.assertEqual(second[0].action, "health-degraded")
        self.assertEqual(third[0].action, "runtime-health-failed")
        self.assertIsNone(process_identity(process["pid"]))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(after["port_leases"][0]["status"], "released")

    def test_health_probe_rejects_redirect_without_contacting_target(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        target_hit = threading.Event()

        class TargetHandler(BaseHTTPRequestHandler):
            def do_GET(inner_self) -> None:
                target_hit.set()
                inner_self.send_response(200)
                inner_self.end_headers()

            def log_message(inner_self, format: str, *args: object) -> None:
                return

        target = ThreadingHTTPServer(("127.0.0.1", 0), TargetHandler)

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(inner_self) -> None:
                inner_self.send_response(302)
                inner_self.send_header(
                    "Location",
                    f"http://127.0.0.1:{target.server_port}/healthy-elsewhere",
                )
                inner_self.end_headers()

            def log_message(inner_self, format: str, *args: object) -> None:
                return

        redirect = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        threads = [
            threading.Thread(target=target.serve_forever, daemon=True),
            threading.Thread(target=redirect.serve_forever, daemon=True),
        ]
        for thread in threads:
            thread.start()
        try:
            context.process["health_endpoint"] = {
                "host": "127.0.0.1",
                "port": redirect.server_port,
                "path": "/health",
            }
            with patch.object(supervisor, "_port_owned", return_value=True):
                self.assertFalse(supervisor._health_ok(context, timeout=1))
            self.assertFalse(target_hit.wait(timeout=0.2))
        finally:
            redirect.shutdown()
            target.shutdown()
            redirect.server_close()
            target.server_close()
            for thread in threads:
                thread.join(timeout=2)

    def test_health_probe_enforces_one_absolute_header_deadline(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]

        class DripServer(ThreadingHTTPServer):
            daemon_threads = True
            block_on_close = False

        class DripHandler(BaseHTTPRequestHandler):
            def do_GET(inner_self) -> None:
                try:
                    for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n":
                        inner_self.connection.sendall(bytes((byte,)))
                        time.sleep(0.05)
                except OSError:
                    pass

            def log_message(inner_self, format: str, *args: object) -> None:
                return

        server = DripServer(("127.0.0.1", 0), DripHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            context.process["health_endpoint"] = {
                "host": "127.0.0.1",
                "port": server.server_port,
                "path": "/health",
            }
            started = time.monotonic()
            with patch.object(supervisor, "_port_owned", return_value=True):
                self.assertFalse(supervisor._health_ok(context, timeout=0.2))
            self.assertLess(time.monotonic() - started, 0.8)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_four_simultaneous_children_are_fully_isolated(self) -> None:
        importer, result = self.activate_manifest(self.four_process_manifest())
        supervisor = self.supervisor(importer)

        outcomes = supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(len(outcomes), 4)
        self.assertEqual({item.state for item in outcomes}, {"HEALTHY"})
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        processes = state["processes"]
        leases = state["port_leases"]
        self.assertEqual(len(processes), 4)
        self.assertEqual(len(leases), 4)
        self.assertEqual({process["state"] for process in processes}, {"HEALTHY"})
        self.assertEqual({lease["status"] for lease in leases}, {"bound"})
        self.assertEqual(len({process["pid"] for process in processes}), 4)
        self.assertNotIn(os.getpid(), {process["pid"] for process in processes})
        self.assertEqual(len({lease["port"] for lease in leases}), 4)
        self.assertEqual(len({process["runtime_root"] for process in processes}), 4)
        self.assertEqual(len({process["stdout_log"] for process in processes}), 4)
        self.assertEqual(len({process["stderr_log"] for process in processes}), 4)
        self.assertEqual(
            len({workspace["actual_git_toplevel"] for workspace in state["workspaces"]}),
            4,
        )
        contexts_by_process_id = {
            str(context.process["process_id"]): context
            for context in supervisor._repository.contexts()
        }
        for process in processes:
            marker = Path(process["runtime_root"]) / "managed-child-marker.json"
            self.assertTrue(marker.is_file())
            marker_value = json.loads(marker.read_text(encoding="utf-8"))
            self.assertEqual(marker_value["process_id"], process["process_id"])
            self.assertEqual(marker_value["port"], process["health_endpoint"]["port"])
            # Windows may keep a venv redirector as the authenticated Job
            # leader while the base interpreter descendant owns the health
            # socket.  Both PIDs are valid only when the listener is inside
            # the exact durable process scope; equality would make this test
            # depend on which Python launcher ran unittest.
            marker_pid = int(marker_value["pid"])
            self.assertIn(
                marker_pid,
                port_owner_pids(
                    str(process["health_endpoint"]["host"]),
                    int(process["health_endpoint"]["port"]),
                ),
            )
            self.assertTrue(
                supervisor._scope_contains_pid(
                    contexts_by_process_id[str(process["process_id"])],
                    marker_pid,
                )
            )
            self.assertEqual(
                os.path.normcase(marker_value["cwd"]),
                os.path.normcase(process["cwd"]),
            )
            self.assertEqual(
                os.path.normcase(marker_value["runtime_root"]),
                os.path.normcase(process["runtime_root"]),
            )
            self.assertEqual(marker_value["launch_nonce"], process["launch_nonce"])
            stdout_text = Path(process["stdout_log"]).read_text(encoding="utf-8")
            stderr_text = Path(process["stderr_log"]).read_text(encoding="utf-8")
            self.assertIn(process["process_id"], stdout_text)
            self.assertIn(process["process_id"], stderr_text)
            for other in processes:
                if other["process_id"] != process["process_id"]:
                    self.assertNotIn(other["process_id"], stdout_text)
                    self.assertNotIn(other["process_id"], stderr_text)
            self.assertEqual(
                port_owner_pids(
                    process["health_endpoint"]["host"],
                    process["health_endpoint"]["port"],
                ),
                frozenset({marker_pid}),
            )
            receipt = json.loads(
                (
                    Path(self.runtime_config["pid_root"])
                    / f"{process['process_id']}.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(receipt["pid"], process["pid"])
            self.assertEqual(receipt["launch_nonce"], process["launch_nonce"])
            self.assertEqual(
                receipt["os_process_birth_token"],
                process["os_process_birth_token"],
            )
        self.assertEqual(managed_activation_invariant_issues(state), ())

        for process in processes:
            supervisor.stop(
                self.project_id, result.response["sprint_id"], process["process_id"]
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual({process["state"] for process in state["processes"]}, {"STOPPED"})
        self.assertEqual({lease["status"] for lease in state["port_leases"]}, {"released"})

    def test_restart_reconciliation_adopts_exact_live_child_without_respawn(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        first = self.supervisor(importer)
        first.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        pid = before["processes"][0]["pid"]

        restarted = self.supervisor(importer)
        outcomes = restarted.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0].action, "verified")
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["pid"], pid)
        self.assertEqual(len(after["processes"]), 1)
        self.assertEqual(len(after["port_leases"]), 1)

        first.stop(
            self.project_id,
            result.response["sprint_id"],
            before["processes"][0]["process_id"],
        )

    def test_restart_recovers_crash_after_launch_receipt_before_starting_commit(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_launch_receipt":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        self.assertEqual(before["processes"][0]["state"], "PREPARED")
        receipt_path = (
            Path(self.runtime_config["pid_root"])
            / f"{before['processes'][0]['process_id']}.json"
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertIsNotNone(process_identity(receipt["pid"]))

        crashed.close()
        self.port_reservations.close_all()
        restarted_ports = ManagedPortReservationRegistry()
        restarted_importer = TransactionalSprintImporter(
            self.runtime_config,
            self.registry,
            port_reservations=restarted_ports,
            allow_local_transport=True,
        )
        restarted_importer.restore_committed_activations()
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            restarted_importer.store,
            restarted_ports,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(restarted)
        try:
            outcomes = restarted.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(len(outcomes), 1)
            self.assertEqual(outcomes[0].state, "HEALTHY")
            after = restarted_importer.store.runtime_state(
                self.project_id, result.response["sprint_id"]
            )
            assert after is not None
            self.assertEqual(after["processes"][0]["pid"], receipt["pid"])
            self.assertEqual(len(after["processes"]), 1)
            restarted.stop(
                self.project_id,
                result.response["sprint_id"],
                after["processes"][0]["process_id"],
            )
            owned = crashed._owned.get(after["processes"][0]["process_id"])
            if owned is not None:
                owned.process.wait(timeout=5)
                crashed._close_owned(after["processes"][0]["process_id"])
        finally:
            restarted_ports.close_all()

    def test_prepared_dead_leader_descendants_are_killed_before_release(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest(
                "--mode",
                "crash-after-health",
                "--spawn-descendant",
                "--crash-after-health",
                "0.5",
            )
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_launch_receipt":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        marker_path = Path(process["runtime_root"]) / "managed-child-marker.json"
        deadline = time.monotonic() + 5
        marker: dict[str, object] = {}
        while time.monotonic() < deadline:
            if marker_path.is_file():
                try:
                    marker = json.loads(marker_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    marker = {}
                if isinstance(marker.get("descendant_pid"), int):
                    break
            time.sleep(0.05)
        descendant_pid = marker.get("descendant_pid")
        self.assertIsInstance(descendant_pid, int)
        assert isinstance(descendant_pid, int)
        endpoint = process["health_endpoint"]
        with socket.create_connection(
            (endpoint["host"], endpoint["port"]), timeout=2
        ) as health_socket:
            health_socket.sendall(
                (
                    f"GET {endpoint['path']} HTTP/1.1\r\n"
                    f"Host: {endpoint['host']}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("ascii")
            )
            while health_socket.recv(4096):
                pass
        receipt = json.loads(
            (
                Path(self.runtime_config["pid_root"])
                / f"{process['process_id']}.json"
            ).read_text(encoding="utf-8")
        )
        while (
            process_identity(receipt["pid"]) is not None
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        self.assertIsNone(process_identity(receipt["pid"]))
        self.assertIsNotNone(process_identity(descendant_pid))

        crashed.close()
        self.port_reservations.close_all()
        restarted_ports = ManagedPortReservationRegistry()
        restarted_importer = TransactionalSprintImporter(
            self.runtime_config,
            self.registry,
            port_reservations=restarted_ports,
            allow_local_transport=True,
        )
        restarted_importer.restore_committed_activations()
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            restarted_importer.store,
            restarted_ports,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(restarted)
        try:
            outcomes = restarted.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(len(outcomes), 1)
            self.assertEqual(outcomes[0].action, "dead-prepared-reconciled")
            deadline = time.monotonic() + 5
            while (
                process_identity(descendant_pid) is not None
                and time.monotonic() < deadline
            ):
                time.sleep(0.05)
            self.assertIsNone(process_identity(descendant_pid))
            after = restarted_importer.store.runtime_state(
                self.project_id, result.response["sprint_id"]
            )
            assert after is not None
            self.assertEqual(after["processes"][0]["state"], "FAILED")
            self.assertEqual(after["port_leases"][0]["status"], "released")
        finally:
            restarted_ports.close_all()

    @unittest.skipUnless(os.name == "nt", "Windows suspended launch recovery")
    def test_restart_resumes_durable_gated_windows_child(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_gated_receipt":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        receipt_path = (
            Path(self.runtime_config["pid_root"])
            / f"{process['process_id']}.json"
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["phase"], "launch_gated")
        self.assertIsNotNone(process_identity(receipt["pid"]))

        crashed.close()
        self.port_reservations.close_all()
        restarted_ports = ManagedPortReservationRegistry()
        restarted_importer = TransactionalSprintImporter(
            self.runtime_config,
            self.registry,
            port_reservations=restarted_ports,
            allow_local_transport=True,
        )
        restarted_importer.restore_committed_activations()
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            restarted_importer.store,
            restarted_ports,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(restarted)
        try:
            outcomes = restarted.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(outcomes[0].state, "HEALTHY")
            after = restarted_importer.store.runtime_state(
                self.project_id, result.response["sprint_id"]
            )
            assert after is not None
            self.assertEqual(after["processes"][0]["pid"], receipt["pid"])
            self.assertEqual(
                json.loads(receipt_path.read_text(encoding="utf-8"))["phase"],
                "launched",
            )
            restarted.stop(
                self.project_id,
                result.response["sprint_id"],
                process["process_id"],
            )
            owned = crashed._owned.get(process["process_id"])
            if owned is not None:
                owned.process.wait(timeout=5)
                crashed._close_owned(process["process_id"])
        finally:
            restarted_ports.close_all()

    def test_expired_gated_windows_child_is_killed_without_execution(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_gated_receipt":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        marker = Path(process["runtime_root"]) / "managed-child-marker.json"
        receipt = json.loads(
            (
                Path(self.runtime_config["pid_root"])
                / f"{process['process_id']}.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["phase"], "launch_gated")
        self.assertFalse(marker.exists())

        crashed.close()
        recovered = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(recovered)
        outcomes = recovered.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(outcomes[0].action, "expired-gated-launch-reconciled")
        self.assertFalse(marker.exists())
        self.assertIsNone(process_identity(receipt["pid"]))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(after["port_leases"][0]["status"], "released")
        owned = crashed._owned.get(process["process_id"])
        if owned is not None:
            owned.process.wait(timeout=5)
            crashed._close_owned(process["process_id"])

    def test_restart_recovers_after_gate_release_before_full_receipt(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_gate_release":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        receipt_path = (
            Path(self.runtime_config["pid_root"])
            / f"{process['process_id']}.json"
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["phase"], "launch_gated")
        original_pid = receipt["pid"]

        # Emulate abrupt launcher loss: no in-process Popen or Job HANDLE may
        # be available to the recovering supervisor.
        crashed._close_owned(process["process_id"])
        crashed.close()
        self.port_reservations.close_all()
        restarted_ports = ManagedPortReservationRegistry()
        restarted_importer = TransactionalSprintImporter(
            self.runtime_config,
            self.registry,
            port_reservations=restarted_ports,
            allow_local_transport=True,
        )
        restarted_importer.restore_committed_activations()
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            restarted_importer.store,
            restarted_ports,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(restarted)
        try:
            outcomes = restarted.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(len(outcomes), 1)
            self.assertEqual(outcomes[0].state, "HEALTHY")
            after = restarted_importer.store.runtime_state(
                self.project_id, result.response["sprint_id"]
            )
            assert after is not None
            self.assertEqual(after["processes"][0]["pid"], original_pid)
            self.assertEqual(
                json.loads(receipt_path.read_text(encoding="utf-8"))["phase"],
                "launched",
            )
            restarted.stop(
                self.project_id,
                result.response["sprint_id"],
                process["process_id"],
            )
            owned = crashed._owned.get(process["process_id"])
            if owned is not None:
                owned.process.wait(timeout=5)
                crashed._close_owned(process["process_id"])
        finally:
            restarted_ports.close_all()

    def test_restart_recovers_crash_after_spawn_before_full_receipt(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_spawn":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        owned = crashed._owned[process["process_id"]]
        original_pid = owned.process.pid
        intent_path = (
            Path(self.runtime_config["pid_root"])
            / f"{process['process_id']}.json"
        )
        self.assertEqual(
            json.loads(intent_path.read_text(encoding="utf-8"))["phase"],
            "intent",
        )

        crashed.close()
        if os.name != "nt":
            # Closing the POSIX gate makes the helper exit with no target
            # exec.  Reap it before recovery scans /proc so the assertion is
            # deterministic even when the target executable is Python too.
            owned.process.wait(timeout=5)
        self.port_reservations.close_all()
        restarted_ports = ManagedPortReservationRegistry()
        restarted_importer = TransactionalSprintImporter(
            self.runtime_config,
            self.registry,
            port_reservations=restarted_ports,
            allow_local_transport=True,
        )
        restarted_importer.restore_committed_activations()
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            restarted_importer.store,
            restarted_ports,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(restarted)
        try:
            outcomes = restarted.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(len(outcomes), 1)
            after = restarted_importer.store.runtime_state(
                self.project_id, result.response["sprint_id"]
            )
            assert after is not None
            if os.name == "nt":
                self.assertEqual(outcomes[0].state, "HEALTHY")
                self.assertEqual(after["processes"][0]["pid"], original_pid)
                self.assertEqual(
                    json.loads(intent_path.read_text(encoding="utf-8"))["phase"],
                    "launched",
                )
                restarted.stop(
                    self.project_id,
                    result.response["sprint_id"],
                    process["process_id"],
                )
            else:
                # The POSIX helper is still held behind its exec gate at this
                # crash point.  Closing the crashed supervisor closes the gate,
                # so the helper exits without executing the target and recovery
                # must terminalize the empty intent rather than adopt it.
                self.assertEqual(outcomes[0].state, "FAILED")
                self.assertEqual(
                    outcomes[0].action, "empty-launch-intent-reconciled"
                )
                self.assertEqual(after["processes"][0]["state"], "FAILED")
                self.assertIsNone(process_identity(original_pid))
                self.assertFalse(intent_path.exists())
            if os.name != "nt":
                owned.process.wait(timeout=5)
                crashed._close_owned(process["process_id"])
        finally:
            restarted_ports.close_all()

    def test_expired_launch_intent_is_killed_without_execution(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_spawn":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        marker = Path(process["runtime_root"]) / "managed-child-marker.json"
        receipt_path = (
            Path(self.runtime_config["pid_root"])
            / f"{process['process_id']}.json"
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["phase"], "intent")
        self.assertFalse(marker.exists())

        crashed.close()
        recovered = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(recovered)
        outcomes = recovered.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(
            outcomes[0].action,
            "expired-launch-intent-reconciled",
        )
        self.assertFalse(marker.exists())
        self.assertIsNone(process_identity(receipt["pid"]))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(after["port_leases"][0]["status"], "released")
        owned = crashed._owned.get(process["process_id"])
        if owned is not None:
            owned.process.wait(timeout=5)
            crashed._close_owned(process["process_id"])

    def test_fresh_launch_expiry_before_resume_never_executes_child(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        current_time = [datetime.now(timezone.utc)]

        def expire(point: str, _context: object) -> None:
            if point == "after_gated_receipt":
                current_time[0] += timedelta(minutes=5)

        supervisor = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            clock=lambda: current_time[0],
            fault_injector=expire,
        )
        self.supervisors.append(supervisor)
        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        self.assertEqual(
            raised.exception.code,
            "PROCESS_HEALTH_FAILED",
            repr(raised.exception.__cause__),
        )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        self.assertEqual(process["state"], "FAILED")
        self.assertEqual(state["port_leases"][0]["status"], "released")
        self.assertFalse(
            (Path(process["runtime_root"]) / "managed-child-marker.json").exists()
        )

    def test_fresh_startup_is_capped_by_wall_time_from_os_creation(self) -> None:
        manifest = self.single_process_manifest(
            "--mode",
            "delayed-start",
            "--delay-start",
            "30",
        )
        manifest["nodes"][0]["workspace"]["process"]["resource_limits"][
            "wall_time_seconds"
        ] = 1
        importer, result = self.activate_manifest(manifest)
        supervisor = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(supervisor)

        started = time.monotonic()
        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        self.assertEqual(
            raised.exception.code,
            "PROCESS_HEALTH_FAILED",
            repr(raised.exception.__cause__),
        )
        self.assertLess(time.monotonic() - started, 5)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        receipt = json.loads(
            (
                Path(self.runtime_config["pid_root"])
                / f"{process['process_id']}.json"
            ).read_text(encoding="utf-8")
        )
        created_at = datetime.fromisoformat(receipt["os_process_created_at"])
        deadline = datetime.fromisoformat(receipt["startup_deadline_at"])
        self.assertLessEqual(deadline, created_at + timedelta(seconds=1))
        self.assertEqual(process["state"], "FAILED")
        self.assertEqual(state["port_leases"][0]["status"], "released")

    def test_recovery_reclamps_legacy_deadline_to_os_wall_budget(self) -> None:
        manifest = self.single_process_manifest(
            "--mode",
            "delayed-start",
            "--delay-start",
            "30",
        )
        manifest["nodes"][0]["workspace"]["process"]["resource_limits"][
            "wall_time_seconds"
        ] = 1
        importer, result = self.activate_manifest(manifest)

        def crash(point: str, _context: object) -> None:
            if point == "after_launch_receipt":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        receipt_path = (
            Path(self.runtime_config["pid_root"])
            / f"{process['process_id']}.json"
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["startup_deadline_at"] = (
            datetime.now(timezone.utc) + timedelta(minutes=10)
        ).isoformat()
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        time.sleep(1.1)

        crashed.close()
        recovered = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(recovered)
        started = time.monotonic()
        outcomes = recovered.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(outcomes[0].action, "recovered-bind-timeout")
        bounded = json.loads(receipt_path.read_text(encoding="utf-8"))
        created_at = datetime.fromisoformat(bounded["os_process_created_at"])
        deadline = datetime.fromisoformat(bounded["startup_deadline_at"])
        self.assertLessEqual(deadline, created_at + timedelta(seconds=1))
        self.assertIsNone(process_identity(receipt["pid"]))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(after["port_leases"][0]["status"], "released")
        owned = crashed._owned.get(process["process_id"])
        if owned is not None:
            owned.process.wait(timeout=5)
            crashed._close_owned(process["process_id"])

    def test_ordinary_failure_after_spawn_terminalizes_without_restart(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        launched_pids: list[int] = []
        supervisor: ManagedProcessSupervisor

        def fail(point: str, context: object) -> None:
            if point != "after_spawn":
                return
            assert isinstance(context, dict)
            process_id = str(context["process_id"])
            launched_pids.append(supervisor._owned[process_id].process.pid)
            raise RuntimeError("ordinary failure after spawn")

        supervisor = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=fail,
        )
        self.supervisors.append(supervisor)
        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        self.assertEqual(raised.exception.code, PROCESS_LAUNCH_FAILED)
        self.assertEqual(len(launched_pids), 1)
        self.assertIsNone(process_identity(launched_pids[0]))
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        self.assertEqual(process["state"], "FAILED")
        self.assertEqual(process["terminal_reason"], "failure")
        self.assertEqual(state["port_leases"][0]["status"], "released")
        self.assertNotIn(process["process_id"], supervisor._owned)
        self.assertEqual(
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
                start_prepared=False,
            ),
            (),
        )

    def test_restart_terminalizes_empty_launch_intent_without_orphan(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_launch_intent":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        intent_path = (
            Path(self.runtime_config["pid_root"])
            / f"{process['process_id']}.json"
        )
        self.assertEqual(
            json.loads(intent_path.read_text(encoding="utf-8"))["phase"],
            "intent",
        )
        self.assertNotIn(process["process_id"], crashed._owned)

        crashed.close()
        self.port_reservations.close_all()
        restarted_ports = ManagedPortReservationRegistry()
        restarted_importer = TransactionalSprintImporter(
            self.runtime_config,
            self.registry,
            port_reservations=restarted_ports,
            allow_local_transport=True,
        )
        restarted_importer.restore_committed_activations()
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            restarted_importer.store,
            restarted_ports,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(restarted)
        try:
            outcomes = restarted.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(outcomes[0].action, "empty-launch-intent-reconciled")
            after = restarted_importer.store.runtime_state(
                self.project_id, result.response["sprint_id"]
            )
            assert after is not None
            self.assertEqual(after["processes"][0]["state"], "FAILED")
            self.assertEqual(after["port_leases"][0]["status"], "released")
            self.assertFalse(intent_path.exists())
        finally:
            restarted_ports.close_all()

    def test_starting_deadline_terminalizes_recovered_unhealthy_child(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_starting_commit":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        self.assertEqual(process["state"], "STARTING")
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=0.2,
            stop_timeout_seconds=8,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(restarted)
        outcomes = restarted.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(outcomes[0].action, "startup-health-failed")
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(after["port_leases"][0]["status"], "released")
        owned = crashed._owned.get(process["process_id"])
        if owned is not None:
            owned.process.wait(timeout=5)
            crashed._close_owned(process["process_id"])

    def test_prepared_receipt_deadline_terminates_child_that_never_bound(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest(
                "--mode",
                "delayed-start",
                "--delay-start",
                "30",
            )
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_launch_receipt":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=1,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        receipt = json.loads(
            (
                Path(self.runtime_config["pid_root"])
                / f"{process['process_id']}.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["phase"], "launched")
        self.assertIsNotNone(receipt["startup_deadline_at"])
        crashed.close()

        recovered = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=30,
            stop_timeout_seconds=8,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(recovered)
        outcomes = recovered.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(outcomes[0].action, "recovered-bind-timeout")
        self.assertIsNone(process_identity(receipt["pid"]))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(after["port_leases"][0]["status"], "released")
        owned = crashed._owned.get(process["process_id"])
        if owned is not None:
            owned.process.wait(timeout=5)
            crashed._close_owned(process["process_id"])

    def test_stop_prepared_with_receipt_kills_scope_before_releasing_lease(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest(
                "--mode", "spawn-descendant", restart=True
            )
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_launch_receipt":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        receipt = json.loads(
            (
                Path(self.runtime_config["pid_root"])
                / f"{process['process_id']}.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(process["state"], "PREPARED")
        self.assertIsNotNone(process_identity(receipt["pid"]))

        future_clock = lambda: datetime.now(timezone.utc) + timedelta(minutes=5)
        restarted = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            clock=future_clock,
        )
        self.supervisors.append(restarted)
        stopped = restarted.stop(
            self.project_id,
            result.response["sprint_id"],
            process["process_id"],
        )
        self.assertEqual(stopped.action, "cancelled-recovered-launch")
        self.assertIsNone(process_identity(receipt["pid"]))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(
            after["processes"][0]["terminal_reason"], "operator_cancelled"
        )
        self.assertEqual(after["port_leases"][0]["status"], "released")
        restarted.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
            start_prepared=False,
        )
        reconciled = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert reconciled is not None
        self.assertEqual(len(reconciled["processes"]), 1)
        owned = crashed._owned.get(process["process_id"])
        if owned is not None:
            owned.process.wait(timeout=5)
            crashed._close_owned(process["process_id"])

    def test_stop_launch_intent_kills_gated_child_without_starting_it(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest(
                "--mode", "spawn-descendant", restart=True
            )
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_spawn":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        before = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert before is not None
        process = before["processes"][0]
        owned = crashed._owned[process["process_id"]]
        launched_pid = owned.process.pid
        receipt_path = (
            Path(self.runtime_config["pid_root"])
            / f"{process['process_id']}.json"
        )
        self.assertEqual(
            json.loads(receipt_path.read_text(encoding="utf-8"))["phase"],
            "intent",
        )

        canceller = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(canceller)
        stopped = canceller.stop(
            self.project_id,
            result.response["sprint_id"],
            process["process_id"],
        )
        self.assertEqual(stopped.action, "cancelled-recovered-intent")
        self.assertIsNone(process_identity(launched_pid))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(
            after["processes"][0]["terminal_reason"], "operator_cancelled"
        )
        self.assertEqual(after["port_leases"][0]["status"], "released")
        canceller.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
            start_prepared=False,
        )
        reconciled = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert reconciled is not None
        self.assertEqual(len(reconciled["processes"]), 1)
        owned.process.wait(timeout=5)
        crashed._close_owned(process["process_id"])

    def test_concurrent_reconciliation_spawns_exactly_one_child(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        spawned = threading.Event()
        release_spawn = threading.Event()

        def pause(point: str, _context: object) -> None:
            if point == "after_spawn":
                spawned.set()
                if not release_spawn.wait(timeout=10):
                    raise AssertionError("concurrent launch test barrier timed out")

        supervisor = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=pause,
        )
        self.supervisors.append(supervisor)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        contender = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(contender)
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(
                supervisor.launch_prepared,
                self.project_id,
                result.response["sprint_id"],
                str(context.process["process_id"]),
            )
            self.assertTrue(spawned.wait(timeout=10))
            second = executor.submit(
                contender.launch_prepared,
                self.project_id,
                result.response["sprint_id"],
                str(context.process["process_id"]),
            )
            time.sleep(0.2)
            self.assertFalse(second.done())
            release_spawn.set()
            first_result = first.result(timeout=20)
            second_result = second.result(timeout=20)
            self.assertEqual(second_result.action, "not-claimed")
        self.assertEqual(first_result.state, "HEALTHY")
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(len(state["processes"]), 1)
        self.assertEqual(len(state["port_leases"]), 1)
        self.assertEqual(state["processes"][0]["state"], "HEALTHY")
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_stale_owned_cleanup_rechecks_after_concurrent_launch_guard(self) -> None:
        importer = self.importer()
        supervisor = self.supervisor(importer)
        process_id = "process-raced-into-owned"
        supervisor._owned[process_id] = object()  # type: ignore[assignment]
        launch_guard_held = threading.Event()
        stale_snapshot_taken = threading.Event()
        live_state_published = threading.Event()
        release_launch_guard = threading.Event()

        def concurrent_launch() -> None:
            with supervisor._process_action_guard(process_id):
                launch_guard_held.set()
                if not stale_snapshot_taken.wait(timeout=10):
                    raise AssertionError("stale owned snapshot barrier timed out")
                live_state_published.set()
                if not release_launch_guard.wait(timeout=10):
                    raise AssertionError("launch guard release barrier timed out")

        def stale_snapshot() -> tuple[object, ...]:
            stale_snapshot_taken.set()
            return ()

        def authoritative_contexts(
            *, project_id: str | None = None, sprint_id: str | None = None
        ) -> tuple[object, ...]:
            if project_id is None and sprint_id is None and live_state_published.is_set():
                return (
                    SimpleNamespace(
                        process={"process_id": process_id, "state": "HEALTHY"}
                    ),
                )
            return ()

        try:
            with (
                patch.object(
                    importer.store,
                    "active_runtime_states",
                    side_effect=stale_snapshot,
                ),
                patch.object(
                    supervisor._repository,
                    "contexts",
                    side_effect=authoritative_contexts,
                ),
                patch.object(supervisor, "_close_owned") as close_owned,
                ThreadPoolExecutor(max_workers=2) as executor,
            ):
                launch = executor.submit(concurrent_launch)
                self.assertTrue(launch_guard_held.wait(timeout=10))
                reconcile = executor.submit(
                    supervisor.reconcile,
                    project_id="unrelated-project-filter",
                    start_prepared=False,
                )
                self.assertTrue(stale_snapshot_taken.wait(timeout=10))
                self.assertTrue(live_state_published.wait(timeout=10))
                time.sleep(0.1)
                self.assertFalse(reconcile.done())
                release_launch_guard.set()
                launch.result(timeout=10)
                self.assertEqual(reconcile.result(timeout=10), ())
                close_owned.assert_not_called()
        finally:
            release_launch_guard.set()
            supervisor._owned.pop(process_id, None)

    def test_independent_registry_cannot_steal_or_strand_port_claim(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        owner = self.supervisor(importer)
        other_registry = ManagedPortReservationRegistry()
        contender = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            other_registry,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            poll_interval_seconds=0.05,
        )
        self.supervisors.append(contender)
        try:
            skipped = contender.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(len(skipped), 1)
            self.assertEqual(skipped[0].action, "reservation-not-owned")

            launched = owner.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(launched[0].state, "HEALTHY")
            state = importer.store.runtime_state(
                self.project_id, result.response["sprint_id"]
            )
            assert state is not None
            process = state["processes"][0]
            lease = state["port_leases"][0]

            contender.stop(
                self.project_id,
                result.response["sprint_id"],
                process["process_id"],
            )
            self.assertTrue(
                self.port_reservations.owns_endpoint(
                    importer.store.database_path,
                    lease["host"],
                    lease["port"],
                )
            )

            owner.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
                start_prepared=False,
            )
            self.assertFalse(
                self.port_reservations.owns_endpoint(
                    importer.store.database_path,
                    lease["host"],
                    lease["port"],
                )
            )
            replacement = {
                **lease,
                "lease_id": "cross-instance-replacement",
                "assignment_id": "cross-instance-replacement",
                "process_id": None,
                "status": "reserved",
                "bind_verified": False,
                "released_at": None,
            }
            token = self.port_reservations.acquire_many(
                importer.store.database_path,
                [replacement],
            )
            self.port_reservations.rollback(token)
        finally:
            contender.close()
            self.supervisors.remove(contender)
            other_registry.close_all()

    def test_recovered_holder_survives_stale_durable_claim_window(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        crashed = self.supervisor(importer)
        context = crashed._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        stale = crashed._repository.claim_prepared(
            self.project_id,
            result.response["sprint_id"],
            str(context.process["process_id"]),
            owner="simulated-crashed-service",
            ttl_seconds=30,
        )
        assert stale is not None
        self.port_reservations.close_all()

        recovered_registry = ManagedPortReservationRegistry()
        recovered = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            recovered_registry,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
        )
        self.supervisors.append(recovered)
        try:
            skipped = recovered.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(skipped[0].action, "not-claimed")
            lease = context.lease
            self.assertTrue(
                recovered_registry.holds_endpoint(
                    importer.store.database_path,
                    lease["host"],
                    lease["port"],
                )
            )
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as competitor:
                with self.assertRaises(OSError):
                    competitor.bind((lease["host"], lease["port"]))

            recovered._repository.release_claim(
                str(context.process["process_id"]),
                owner="simulated-crashed-service",
                supervisor_fence=stale.supervisor_fence,
            )
            launched = recovered.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
            self.assertEqual(launched[0].state, "HEALTHY")
            recovered.stop(
                self.project_id,
                result.response["sprint_id"],
                str(context.process["process_id"]),
            )
        finally:
            recovered.close()
            self.supervisors.remove(recovered)
            recovered_registry.close_all()

    def test_close_waits_for_direct_launch_and_rejects_new_work(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        paused = threading.Event()
        release = threading.Event()

        def pause(point: str, _context: object) -> None:
            if point == "after_spawn":
                paused.set()
                if not release.wait(timeout=10):
                    raise AssertionError("close quiescence barrier timed out")

        supervisor = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=pause,
        )
        self.supervisors.append(supervisor)
        process_id = str(
            supervisor._repository.contexts(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )[0].process["process_id"]
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            launch = executor.submit(
                supervisor.launch_prepared,
                self.project_id,
                result.response["sprint_id"],
                process_id,
            )
            self.assertTrue(paused.wait(timeout=10))
            closing = executor.submit(supervisor.close)
            time.sleep(0.2)
            self.assertFalse(closing.done())
            release.set()
            self.assertEqual(launch.result(timeout=20).state, "HEALTHY")
            closing.result(timeout=20)
        with self.assertRaises(ManagedProcessSupervisorError):
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )

        cleaner = self.supervisor(importer)
        cleaner.stop(
            self.project_id,
            result.response["sprint_id"],
            process_id,
        )
        owned = supervisor._owned.get(process_id)
        if owned is not None:
            owned.process.wait(timeout=5)
            supervisor._close_owned(process_id)

    def test_stale_claimant_cannot_terminalize_newer_fence(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        first = self.supervisor(importer)
        context = first._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        stale = first._repository.claim_prepared(
            self.project_id,
            result.response["sprint_id"],
            str(context.process["process_id"]),
            owner=first.claim_owner,
            ttl_seconds=1,
        )
        assert stale is not None
        second = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            clock=lambda: datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.supervisors.append(second)
        current = second._repository.claim_prepared(
            self.project_id,
            result.response["sprint_id"],
            str(context.process["process_id"]),
            owner=second.claim_owner,
            ttl_seconds=30,
        )
        assert current is not None
        with self.assertRaises(ManagedProcessSupervisorError):
            first._terminalize(
                stale,
                failed=True,
                claim_owner=first.claim_owner,
                supervisor_fence=stale.supervisor_fence,
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(state["processes"][0]["state"], "PREPARED")
        self.assertEqual(state["port_leases"][0]["status"], "reserved")
        second._repository.release_claim(
            str(context.process["process_id"]),
            owner=second.claim_owner,
            supervisor_fence=current.supervisor_fence,
        )
        second.stop(
            self.project_id,
            result.response["sprint_id"],
            str(context.process["process_id"]),
        )

    def test_child_crash_is_failed_logs_preserved_and_restart_is_new_attempt(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "immediate-crash", restart=True)
        )
        supervisor = self.supervisor(importer)
        with self.assertRaises(ManagedProcessSupervisorError):
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(len(state["processes"]), 2)
        failed, restart = sorted(
            state["processes"], key=lambda process: process["restart_attempt"]
        )
        self.assertEqual(failed["state"], "FAILED")
        self.assertEqual(restart["state"], "PREPARED")
        self.assertEqual(restart["restart_of_process_id"], failed["process_id"])
        self.assertNotEqual(restart["process_id"], failed["process_id"])
        self.assertNotEqual(restart["port_lease_id"], failed["port_lease_id"])
        failed_log = Path(failed["stderr_log"])
        self.assertTrue(failed_log.is_file())
        original = failed_log.read_bytes()
        self.assertIn(b"immediate_crash", original)
        supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
            start_prepared=False,
        )
        self.assertEqual(failed_log.read_bytes(), original)
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_restart_skips_durable_port_during_other_registry_handoff_gap(
        self,
    ) -> None:
        manifest = self.four_process_manifest()
        for node in manifest["nodes"][:4]:
            node["workspace"]["process"] = self.process_spec(
                "--mode", "serve", restart=True
            )
        importer, result = self.activate_manifest(manifest)
        sprint_id = result.response["sprint_id"]
        first = self.supervisor(importer)
        contexts = sorted(
            first._repository.contexts(
                project_id=self.project_id,
                sprint_id=sprint_id,
            ),
            key=lambda context: int(context.lease["port"]),
        )
        handoff_context, restart_parent = contexts[:2]
        handoff_port = int(handoff_context.lease["port"])
        restart_port = int(restart_parent.lease["port"])
        self.assertEqual(restart_port, handoff_port + 1)

        failed_parent = first._terminalize(
            restart_parent,
            failed=True,
            create_restart=False,
        )
        handoff = self.port_reservations.begin_handoff(
            importer.store.database_path,
            str(handoff_context.lease["lease_id"]),
        )
        self.assertFalse(
            self.port_reservations.holds_endpoint(
                importer.store.database_path,
                "127.0.0.1",
                handoff_port,
            )
        )
        self.assertTrue(
            self.port_reservations.owns_endpoint(
                importer.store.database_path,
                "127.0.0.1",
                handoff_port,
            )
        )

        second_registry = ManagedPortReservationRegistry()
        second = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            second_registry,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            poll_interval_seconds=0.05,
        )
        attempted_ports: list[int] = []
        acquire_many = second_registry.acquire_many
        restart = None

        def record_acquire(
            database_path: str | os.PathLike[str], leases: list[dict]
        ) -> object:
            attempted_ports.extend(int(lease["port"]) for lease in leases)
            return acquire_many(database_path, leases)

        try:
            with patch.object(
                second_registry,
                "acquire_many",
                side_effect=record_acquire,
            ):
                restart = second._create_restart_if_allowed(failed_parent)
            assert restart is not None
            self.assertEqual(int(restart.lease["port"]), restart_port)
            self.assertEqual(attempted_ports, [restart_port])
            self.assertFalse(
                second_registry.owns_endpoint(
                    importer.store.database_path,
                    "127.0.0.1",
                    handoff_port,
                )
            )
            self.assertTrue(
                second_registry.holds_endpoint(
                    importer.store.database_path,
                    "127.0.0.1",
                    restart_port,
                )
            )
        finally:
            try:
                try:
                    if restart is not None:
                        second.stop(
                            self.project_id,
                            sprint_id,
                            str(restart.process["process_id"]),
                        )
                finally:
                    self.port_reservations.reacquire_handoff(handoff)
            finally:
                try:
                    second.close()
                finally:
                    second_registry.close_all()

    def test_failed_leaf_recreates_missing_restart_after_service_crash(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest(
                "--mode",
                "crash-after-health",
                "--crash-after-health",
                "0.5",
                restart=True,
            )
        )

        def crash(point: str, _context: object) -> None:
            if point == "after_terminal_commit":
                raise SimulatedServiceCrash()

        crashed = ManagedProcessSupervisor(
            self.runtime_config,
            importer.store,
            self.port_reservations,
            health_timeout_seconds=10,
            stop_timeout_seconds=8,
            fault_injector=crash,
        )
        self.supervisors.append(crashed)
        crashed.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        initial = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert initial is not None
        first_process = initial["processes"][0]
        deadline = time.monotonic() + 5
        while (
            process_identity(first_process["pid"]) is not None
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        with self.assertRaises(SimulatedServiceCrash):
            crashed.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        failed = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert failed is not None
        self.assertEqual(len(failed["processes"]), 1)
        self.assertEqual(failed["processes"][0]["state"], "FAILED")

        contenders = [self.supervisor(importer), self.supervisor(importer)]
        barrier = threading.Barrier(2)

        def synchronize_first_snapshot(original: object) -> object:
            first_call_lock = threading.Lock()
            first_call = True

            def synchronized_contexts(
                *, project_id: str | None = None, sprint_id: str | None = None
            ) -> tuple[object, ...]:
                nonlocal first_call
                contexts = original(  # type: ignore[operator]
                    project_id=project_id, sprint_id=sprint_id
                )
                with first_call_lock:
                    wait_here = first_call
                    first_call = False
                if wait_here:
                    barrier.wait(timeout=10)
                return contexts

            return synchronized_contexts

        for contender in contenders:
            contender._repository.contexts = synchronize_first_snapshot(  # type: ignore[method-assign,assignment]
                contender._repository.contexts
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    contender.reconcile,
                    project_id=self.project_id,
                    sprint_id=result.response["sprint_id"],
                    start_prepared=False,
                )
                for contender in contenders
            ]
            outcome_batches = [future.result(timeout=20) for future in futures]
        self.assertEqual(
            sum(
                outcome.action == "restart-reconciled"
                for outcomes in outcome_batches
                for outcome in outcomes
            ),
            1,
        )
        repaired = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert repaired is not None
        self.assertEqual(len(repaired["processes"]), 2)
        restart = max(
            repaired["processes"], key=lambda item: item["restart_attempt"]
        )
        self.assertEqual(restart["state"], "PREPARED")
        self.assertEqual(
            restart["restart_of_process_id"], first_process["process_id"]
        )
        self.assertEqual(managed_activation_invariant_issues(repaired), ())

    def test_stop_terminates_descendant_before_port_release(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "spawn-descendant")
        )
        supervisor = self.supervisor(importer)
        supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        marker = json.loads(
            (Path(process["runtime_root"]) / "managed-child-marker.json").read_text(
                encoding="utf-8"
            )
        )
        descendant_pid = marker["descendant_pid"]
        self.assertIsNotNone(process_identity(descendant_pid))

        supervisor.stop(
            self.project_id, result.response["sprint_id"], process["process_id"]
        )
        deadline = time.monotonic() + 5
        while process_identity(descendant_pid) is not None and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertIsNone(process_identity(descendant_pid))
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(state["processes"][0]["state"], "STOPPED")
        self.assertEqual(state["port_leases"][0]["status"], "released")

    def test_dead_leader_descendant_is_killed_before_lease_release(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest(
                "--mode",
                "spawn-descendant",
                "--crash-after-health",
                "0.5",
            )
        )
        supervisor = self.supervisor(importer)
        supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        marker = json.loads(
            (Path(process["runtime_root"]) / "managed-child-marker.json").read_text(
                encoding="utf-8"
            )
        )
        descendant_pid = marker["descendant_pid"]
        deadline = time.monotonic() + 5
        while (
            process_identity(process["pid"]) is not None
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        self.assertIsNone(process_identity(process["pid"]))
        self.assertIsNotNone(process_identity(descendant_pid))

        outcomes = supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(outcomes[0].action, "dead-reconciled")
        self.assertIsNone(process_identity(descendant_pid))
        after = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert after is not None
        self.assertEqual(after["processes"][0]["state"], "FAILED")
        self.assertEqual(after["port_leases"][0]["status"], "released")

    @unittest.skipUnless(os.name == "nt", "Windows Job membership guard")
    def test_protected_pid_inside_job_scope_is_rejected_before_signal(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]

        class FakeJob:
            def active_process_ids(inner_self: object) -> frozenset[int]:
                return frozenset({os.getpid()})

            def close(inner_self: object) -> None:
                raise AssertionError("borrowed fake Job must not be closed")

        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor._assert_scope_safe(context, job=FakeJob())  # type: ignore[arg-type]
        self.assertEqual(raised.exception.code, ORPHAN_PROCESS)
        self.assertIsNotNone(process_identity(os.getpid()))

    @unittest.skipUnless(os.name == "nt", "Windows Job termination handle")
    def test_recovered_termination_anchors_one_job_through_signal(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        expected_job = supervisor._windows_job_name(context)
        context.process.update(
            {
                "pid": 424242,
                "os_process_created_at": datetime_now(),
                "os_process_birth_token": "windows-filetime:424242",
                "process_group_id": expected_job,
                "job_object_id": expected_job,
            }
        )

        class FakeJob:
            name = expected_job
            terminated = False
            closed = False

            def active_process_ids(inner_self: object) -> frozenset[int]:
                return frozenset()

            def terminate(inner_self: object) -> None:
                inner_self.terminated = True  # type: ignore[attr-defined]

            def close(inner_self: object) -> None:
                inner_self.closed = True  # type: ignore[attr-defined]

        job = FakeJob()
        death_checks = 0

        def confirmed_dead(
            _context: object,
            *,
            owned: object = None,
            job: object = None,
        ) -> bool:
            nonlocal death_checks
            self.assertIsNone(owned)
            self.assertIs(job, anchored_job)
            death_checks += 1
            return death_checks > 1

        def record_identity(
            _context: object,
            *,
            allow_posix_launch_helper: bool = False,
            job: object = None,
        ) -> tuple[object, dict[str, object]]:
            self.assertFalse(allow_posix_launch_helper)
            self.assertIs(job, anchored_job)
            return object(), {}

        anchored_job = job
        with (
            patch.object(
                process_supervisor_module._WindowsJob,
                "open",
                return_value=anchored_job,
            ) as open_job,
            patch.object(supervisor, "_confirmed_dead", side_effect=confirmed_dead),
            patch.object(supervisor, "_record_identity", side_effect=record_identity),
        ):
            supervisor._terminate_context(context)

        open_job.assert_called_once_with(expected_job)
        self.assertTrue(job.terminated)
        self.assertTrue(job.closed)
        self.assertEqual(death_checks, 2)

    def test_resume_exact_rejects_reused_pid_on_the_open_handle(self) -> None:
        class FakeKernel32:
            opened: list[tuple[int, bool, int]] = []
            closed: list[int] = []

            def OpenProcess(
                inner_self: object,
                access: int,
                inherit: bool,
                pid: int,
            ) -> int:
                inner_self.opened.append((access, inherit, pid))  # type: ignore[attr-defined]
                return 12345

            def CloseHandle(inner_self: object, handle: int) -> bool:
                inner_self.closed.append(handle)  # type: ignore[attr-defined]
                return True

            def IsProcessInJob(inner_self: object, *_arguments: object) -> bool:
                raise AssertionError("changed process must be rejected before membership")

        kernel32 = FakeKernel32()
        job = process_supervisor_module._WindowsJob(
            67890,
            "Global\\nginx-qa-resume-exact-test",
            kernel32,
        )
        expected = process_supervisor_module.ProcessIdentity(
            pid=1234,
            created_at="2026-01-01T00:00:00+00:00",
            birth_token="windows-filetime:1",
            executable_path=sys.executable,
            cwd=None,
            process_group_id=None,
        )
        replacement = process_supervisor_module.ProcessIdentity(
            pid=1234,
            created_at="2026-01-01T00:00:01+00:00",
            birth_token="windows-filetime:2",
            executable_path=sys.executable,
            cwd=None,
            process_group_id=None,
        )
        with patch.object(
            process_supervisor_module,
            "_windows_process_identity_from_handle",
            return_value=replacement,
        ):
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                job.resume_exact(expected, process_id="managed-process")
        self.assertEqual(raised.exception.code, ORPHAN_PROCESS)
        self.assertEqual(kernel32.opened[0][1:], (False, expected.pid))
        self.assertEqual(kernel32.closed, [12345])

    @unittest.skipUnless(os.name == "nt", "Windows Job resource limits")
    def test_fresh_launch_rejects_a_preexisting_named_job(self) -> None:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        name = f"Local\\nginx-qa-preexisting-test-{os.getpid()}-{time.time_ns()}"
        handle = kernel32.CreateJobObjectW(None, name)
        self.assertTrue(handle)
        try:
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                process_supervisor_module._WindowsJob.create(
                    name,
                    {
                        "process_count": 1,
                        "memory_bytes": 64 * 1024 * 1024,
                        "cpu_percent": 100,
                    },
                )
            self.assertEqual(raised.exception.code, ORPHAN_PROCESS)
        finally:
            kernel32.CloseHandle(handle)

    @unittest.skipUnless(os.name == "nt", "Windows Job inherited capability")
    def test_child_job_lifetime_handle_has_only_synchronize_access(self) -> None:
        import ctypes
        from ctypes import wintypes

        handle_path = self.base / "inherited-job-handle.txt"
        child_code = (
            "import ctypes,sys; "
            "from ctypes import wintypes; "
            "from pathlib import Path; "
            "handle=int(Path(sys.argv[1]).read_text()); "
            "kernel32=ctypes.WinDLL('kernel32',use_last_error=True); "
            "kernel32.TerminateJobObject.argtypes="
            "[wintypes.HANDLE,wintypes.UINT]; "
            "kernel32.TerminateJobObject.restype=wintypes.BOOL; "
            "changed=bool(kernel32.TerminateJobObject("
            "wintypes.HANDLE(handle),77)); "
            "raise SystemExit(42 if changed else 0)"
        )
        job = process_supervisor_module._WindowsJob.create(
            f"Global\\nginx-qa-restricted-job-test-{os.getpid()}-{time.time_ns()}",
            {
                "process_count": 2,
                "memory_bytes": 256 * 1024 * 1024,
                "cpu_percent": 100,
            },
        )
        native_duplicate = job.kernel32.DuplicateHandle
        native_duplicate.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.HANDLE),
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        ]
        native_duplicate.restype = wintypes.BOOL
        duplicate_calls: list[tuple[int, int, int]] = []

        def record_duplicate(*arguments: object) -> object:
            result = native_duplicate(*arguments)
            target = ctypes.cast(
                arguments[3], ctypes.POINTER(wintypes.HANDLE)
            ).contents.value
            desired_access = int(arguments[4])
            options = int(arguments[6])
            duplicate_calls.append(
                (desired_access, options, int(target) if target else 0)
            )
            return result

        job.kernel32.DuplicateHandle = record_duplicate
        process = None
        try:
            with (
                (self.base / "restricted-job.stdout").open("wb") as stdout_stream,
                (self.base / "restricted-job.stderr").open("wb") as stderr_stream,
            ):
                process = process_supervisor_module._spawn_suspended_in_windows_job(
                    job,
                    [sys.executable, "-c", child_code, str(handle_path)],
                    cwd=str(self.base),
                    environment=dict(os.environ),
                    stdout_stream=stdout_stream,
                    stderr_stream=stderr_stream,
                )
                job.kernel32.DuplicateHandle = native_duplicate
                self.assertEqual(len(duplicate_calls), 4)
                self.assertEqual(
                    duplicate_calls[:3],
                    [(0, 0x00000002, value) for _, _, value in duplicate_calls[:3]],
                )
                self.assertEqual(duplicate_calls[3][0:2], (0x00100000, 0))
                self.assertGreater(duplicate_calls[3][2], 0)
                handle_path.write_text(
                    str(duplicate_calls[3][2]), encoding="ascii"
                )

                # The restricted inherited handle must keep KILL_ON_JOB_CLOSE
                # from firing when the launcher drops its full-control handle.
                # This is a capability-minimization check, not a same-token
                # security boundary: repository code is explicitly trusted.
                job.close()
                process_supervisor_module._WindowsJob.resume(process)
                self.assertEqual(process.wait(timeout=20), 0)
        finally:
            job.kernel32.DuplicateHandle = native_duplicate
            if process is not None:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                process.close()
            job.close()

    @unittest.skipUnless(os.name == "nt", "Windows Job reboot recovery")
    def test_dead_exact_pid_and_absent_global_job_proves_scope_empty(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        probe = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(5)"],
            cwd=str(context.process["cwd"]),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 2
            identity = process_identity(probe.pid)
            while identity is None and time.monotonic() < deadline:
                time.sleep(0.01)
                identity = process_identity(probe.pid)
            self.assertIsNotNone(identity)
            assert identity is not None
        finally:
            probe.terminate()
            probe.wait(timeout=5)
        self.assertIsNone(process_identity(identity.pid))
        job_name = supervisor._windows_job_name(context)
        Path(self.runtime_config["pid_root"]).mkdir(parents=True, exist_ok=True)
        supervisor._write_receipt(
            context,
            identity,
            job_name,
            job_name,
            startup_deadline_at=(
                datetime.now(timezone.utc) + timedelta(seconds=10)
            ).isoformat(),
        )

        outcomes = supervisor.reconcile(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0].action, "dead-prepared-reconciled")
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(state["processes"][0]["state"], "FAILED")
        self.assertEqual(state["port_leases"][0]["status"], "released")

    @unittest.skipUnless(os.name == "nt", "Windows Job resource limits")
    def test_windows_job_enforces_assignment_process_limit(self) -> None:
        manifest = self.single_process_manifest("--mode", "spawn-descendant")
        manifest["nodes"][0]["workspace"]["process"]["resource_limits"][  # type: ignore[index]
            "process_count"
        ] = 1
        importer, result = self.activate_manifest(manifest)
        supervisor = self.supervisor(importer)
        with self.assertRaises(ManagedProcessSupervisorError):
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        process = state["processes"][0]
        self.assertEqual(process["state"], "FAILED")
        self.assertEqual(state["port_leases"][0]["status"], "released")
        self.assertIn(
            "Traceback",
            Path(process["stderr_log"]).read_text(
                encoding="utf-8", errors="replace"
            ),
        )

    def test_one_orphan_does_not_block_other_prepared_children(self) -> None:
        importer, result = self.activate_manifest(self.four_process_manifest())
        supervisor = self.supervisor(importer)
        contexts = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )
        forged = contexts[0]
        identity = process_identity(os.getpid())
        assert identity is not None
        forged_process = dict(forged.process)
        now = datetime_now()
        forged_process.update(
            {
                "state": "STARTING",
                "pid": os.getpid(),
                "process_group_id": identity.process_group_id or os.getpid(),
                "job_object_id": "forged-live-service-scope",
                "os_process_created_at": identity.created_at,
                "os_process_birth_token": identity.birth_token,
                "started_at": now,
                "startup_deadline_at": now,
            }
        )
        forged_lease = dict(forged.lease)
        forged_lease.update(
            {
                "status": "bound",
                "process_id": forged_process["process_id"],
                "bind_verified": True,
            }
        )
        supervisor._repository.transition(
            forged,
            forged_process,
            forged_lease,
            expected_states={"PREPARED"},
        )

        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        self.assertEqual(raised.exception.code, ORPHAN_PROCESS)
        state = importer.store.runtime_state(
            self.project_id, result.response["sprint_id"]
        )
        assert state is not None
        self.assertEqual(
            [process["state"] for process in state["processes"]].count("HEALTHY"),
            3,
        )
        self.assertEqual(
            [process["state"] for process in state["processes"]].count("STARTING"),
            1,
        )

        # Remove only the synthetic forged ownership record without signalling
        # the protected service process.  The assertion above verifies the
        # production supervisor itself failed closed.
        current = supervisor._repository.load(
            self.project_id,
            result.response["sprint_id"],
            str(forged.process["process_id"]),
        )
        failed_process = dict(current.process)
        failed_process.update(
            {
                "state": "FAILED",
                "failed_at": datetime_now(),
                "terminal_reason": "failure",
            }
        )
        released_lease = dict(current.lease)
        released_lease.update(
            {
                "status": "released",
                "process_id": failed_process["process_id"],
                "released_at": datetime_now(),
            }
        )
        supervisor._repository.transition(
            current,
            failed_process,
            released_lease,
            expected_states={"STARTING"},
        )
        self.port_reservations.release(
            importer.store.database_path, str(released_lease["lease_id"])
        )
        self.assertIsNotNone(process_identity(os.getpid()))

    def test_live_service_pid_is_rejected_without_signal(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        identity = process_identity(os.getpid())
        self.assertIsNotNone(identity)
        assert identity is not None
        forged_process = dict(context.process)
        forged_process.update(
            {
                "state": "STARTING",
                "pid": os.getpid(),
                "process_group_id": identity.process_group_id or os.getpid(),
                "job_object_id": "forged-live-service",
                "os_process_created_at": identity.created_at,
                "os_process_birth_token": identity.birth_token,
                "started_at": datetime_now(),
                "startup_deadline_at": datetime_now(),
            }
        )
        forged_lease = dict(context.lease)
        forged_lease.update(
            {
                "status": "bound",
                "process_id": forged_process["process_id"],
                "bind_verified": True,
            }
        )
        supervisor._repository.transition(
            context,
            forged_process,
            forged_lease,
            expected_states={"PREPARED"},
        )
        with self.assertRaises(ManagedProcessSupervisorError) as raised:
            supervisor.reconcile(
                project_id=self.project_id,
                sprint_id=result.response["sprint_id"],
            )
        self.assertEqual(raised.exception.code, ORPHAN_PROCESS)
        self.assertIsNotNone(process_identity(os.getpid()))

    def test_forged_receipt_scope_is_rejected_before_process_inspection(self) -> None:
        importer, result = self.activate_manifest(
            self.single_process_manifest("--mode", "serve")
        )
        supervisor = self.supervisor(importer)
        context = supervisor._repository.contexts(
            project_id=self.project_id,
            sprint_id=result.response["sprint_id"],
        )[0]
        identity = process_identity(os.getpid())
        assert identity is not None
        forged_group: int | str = (
            "Local\\nginx-qa-managed-forged"
            if os.name == "nt"
            else os.getpid() + 1
        )
        receipt = {
            "schema_version": 1,
            "phase": "launched",
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
            "process_group_id": forged_group,
            "job_object_id": (
                forged_group
                if os.name == "nt"
                else f"posix-session:{forged_group}"
            ),
            "port_lease_id": context.process["port_lease_id"],
            "created_at": datetime_now(),
            "startup_deadline_at": datetime_now(),
        }
        with patch.object(
            process_supervisor_module,
            "process_identity",
            side_effect=AssertionError(
                "a non-deterministic scope must be rejected before PID inspection"
            ),
        ):
            with self.assertRaises(ManagedProcessSupervisorError) as raised:
                supervisor._context_from_receipt(context, receipt)
        self.assertEqual(raised.exception.code, ORPHAN_PROCESS)
        self.assertIsNotNone(process_identity(os.getpid()))


def datetime_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    unittest.main()
