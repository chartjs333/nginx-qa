from __future__ import annotations

from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


RUNNER_PATH = (
    Path(__file__).resolve().parents[1]
    / "orchestration"
    / "sprints"
    / "universal-managed-sprint-engine"
    / "staging_qualification_e2e.py"
)
SPEC = importlib.util.spec_from_file_location("staging_qualification_e2e", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


SHA = "a" * 40
REF = "refs/heads/agent/umse-07-staging-qualification"
PROJECT_ID = "9010"
SPRINT_ID = "msv1-" + "b" * 64
MANIFEST_PATH = "orchestration/staging.json"
MANIFEST_SHA256 = "c" * 64
SERVICE_ROOT = Path("C:/staging/source")
STATE_BASE = Path("C:/staging-state")
CHILD_PYTHON = STATE_BASE / ".venv" / "Scripts" / "python.exe"
RUNTIME_ROOT = STATE_BASE / "runtime_state"
MANAGED_ROOT = STATE_BASE / "managed"
PROTECTED_ROOTS = (Path("D:/live"), Path("D:/Prompt"))


def config_fixture(*, evidence_path: Path = Path("C:/staging-state/evidence/run.json")):
    return runner.PrepareConfig(
        repo_root=SERVICE_ROOT,
        expected_sha=SHA,
        ref=REF,
        manifest_path=MANIFEST_PATH,
        git_address="https://github.com/example/repo.git",
        expected_child_python=CHILD_PYTHON,
        expected_runtime_root=RUNTIME_ROOT,
        expected_managed_root=MANAGED_ROOT,
        ownership_marker=STATE_BASE / ".nginx-qa-staging-owner.json",
        run_id="run-1",
        staging_url="http://127.0.0.1:18025",
        managed_db=RUNTIME_ROOT / "leases" / "managed-import.sqlite3",
        evidence_path=evidence_path,
        protected_roots=PROTECTED_ROOTS,
        timeout_seconds=30.0,
        live_baseline_path=STATE_BASE / "evidence" / "live-before.json",
    )


def _process_owner(process: dict) -> dict:
    return {
        "json": deepcopy(process),
        "relational": {
            key: deepcopy(process[key])
            for key in (
                "process_id",
                "assignment_id",
                "port_lease_id",
                "pid",
                "state",
            )
        },
    }


def _lease_owner(lease: dict) -> dict:
    return {
        "json": deepcopy(lease),
        "relational": {
            key: deepcopy(lease[key])
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
        },
    }


def runtime_fixture() -> tuple[dict, dict]:
    assignments: list[dict] = []
    workspaces: list[dict] = []
    processes: list[dict] = []
    leases: list[dict] = []
    for index in range(4):
        assignment_id = f"assignment-{index}"
        workspace_id = f"workspace-{index}"
        process_id = f"process-{index}"
        lease_id = f"lease-{index}"
        workspace_root = str(MANAGED_ROOT / f"workspace-{index}")
        process_runtime_root = str(RUNTIME_ROOT / "processes" / process_id)
        launch_nonce = f"nonce-{index}"
        job_id = "Global\\nginx-qa-managed-" + hashlib.sha256(
            (process_id + "\0" + launch_nonce).encode("utf-8")
        ).hexdigest()[:40]
        assignments.append(
            {
                "assignment_id": assignment_id,
                "source_commit": SHA,
                "initial_head_commit": SHA,
            }
        )
        workspaces.append(
            {
                "workspace_id": workspace_id,
                "assignment_id": assignment_id,
                "expected_root": workspace_root,
                "actual_git_toplevel": workspace_root,
                "source_commit": SHA,
                "initial_head_commit": SHA,
                "working_tree_state": "clean",
                "repository_remote": "github.com/example/repo",
            }
        )
        processes.append(
            {
                "process_id": process_id,
                "assignment_id": assignment_id,
                "workspace_id": workspace_id,
                "port_lease_id": lease_id,
                "pid": 4100 + index,
                "state": "HEALTHY",
                "restart_attempt": 0,
                "cwd": workspace_root,
                "runtime_root": process_runtime_root,
                "command_redacted": [
                    str(CHILD_PYTHON),
                    runner.CHILD_FIXTURE_PATH,
                    "--mode",
                    "serve",
                ],
                "environment_redacted": {},
                "restart_policy": "on_failure",
                "max_restart_attempts": 1,
                "restart_backoff_seconds": 0,
                "resource_limits": deepcopy(runner.EXPECTED_RESOURCE_LIMITS),
                "launch_nonce": launch_nonce,
                "health_endpoint": {
                    "host": "127.0.0.1",
                    "port": 18100 + index,
                    "path": "/health",
                },
                "executable_path": str(CHILD_PYTHON),
                "os_process_birth_token": f"birth-{index}",
                "job_object_id": job_id,
                "process_group_id": job_id,
            }
        )
        leases.append(
            {
                "lease_id": lease_id,
                "assignment_id": assignment_id,
                "process_id": process_id,
                "host": "127.0.0.1",
                "port": 18100 + index,
                "status": "bound",
                "bind_verified": True,
                "instance_id": runner.STAGING_INSTANCE_ID,
                "network_namespace_id": "host",
            }
        )
    identity = {
        "project_id": PROJECT_ID,
        "repository_id": "main",
        "commit": SHA,
        "manifest_path": MANIFEST_PATH,
        "manifest_sha256": MANIFEST_SHA256,
    }
    state = {
        "sprint_id": SPRINT_ID,
        "status": "active",
        "identity": deepcopy(identity),
        "workspace_source_commit": SHA,
        "requested_ref": REF,
        "repository": {"canonical_remote": "github.com/example/repo"},
        "runtime_config": {
            "service_root": str(SERVICE_ROOT),
            "protected_roots": [str(item) for item in PROTECTED_ROOTS],
            "runtime_root": str(RUNTIME_ROOT),
            "process_runtime_root": str(RUNTIME_ROOT / "processes"),
            "log_root": str(RUNTIME_ROOT / "logs"),
            "pid_root": str(RUNTIME_ROOT / "pids"),
            "lease_root": str(RUNTIME_ROOT / "leases"),
            "prompt_root": str(STATE_BASE / "prompt"),
            "managed_root": str(MANAGED_ROOT),
            "http_host": "127.0.0.1",
            "http_port": runner.STAGING_HTTP_PORT,
            "child_port_start": runner.CHILD_PORT_START,
            "child_port_end": runner.CHILD_PORT_END,
            "instance_id": runner.STAGING_INSTANCE_ID,
            "disable_telegram": True,
            "disable_tunnel": True,
        },
        "assignments": assignments,
        "workspaces": workspaces,
        "processes": processes,
        "port_leases": leases,
        "import_attempts": [],
        "outbox": [],
        "result_receipts": [],
        "review_assignments": [],
        "reviews": [],
        "reworks": [],
        "transition_journal": [],
    }
    process_owners = [_process_owner(item) for item in processes]
    lease_owners = [_lease_owner(item) for item in leases]
    bundle = {
        "row": {
            "project_id": PROJECT_ID,
            "sprint_id": SPRINT_ID,
            "status": "active",
            "fencing_token": 3,
        },
        "state": state,
        "owner_processes": process_owners,
        "owner_leases": lease_owners,
        "live_assignment_processes": deepcopy(process_owners),
        "live_assignment_leases": deepcopy(lease_owners),
        "queue_items": [],
    }
    response = {
        "sprint_id": SPRINT_ID,
        "status": "active",
        "phase": "ACTIVATE",
        "deduplicated": False,
        "execution_mode": "parallel",
        "identity": deepcopy(identity),
        "workspace_source_commit": SHA,
        "initial_assignment_ids": [f"assignment-{index}" for index in range(4)],
    }
    return bundle, response


def sync_process_owner(bundle: dict, index: int = 0) -> None:
    process = bundle["state"]["processes"][index]
    for collection in ("owner_processes", "live_assignment_processes"):
        wrapper = bundle[collection][index]
        wrapper["json"] = deepcopy(process)
        for key in tuple(wrapper["relational"]):
            wrapper["relational"][key] = deepcopy(process.get(key))


def sync_lease_owner(bundle: dict, index: int = 0) -> None:
    lease = bundle["state"]["port_leases"][index]
    for collection in ("owner_leases", "live_assignment_leases"):
        wrapper = bundle[collection][index]
        wrapper["json"] = deepcopy(lease)
        for key in tuple(wrapper["relational"]):
            wrapper["relational"][key] = deepcopy(lease.get(key))


def validate_runtime(bundle: dict, response: dict, **overrides):
    arguments = {
        "expected_sha": SHA,
        "expected_ref": REF,
        "project_id": PROJECT_ID,
        "start_response": response,
        "expected_child_python": CHILD_PYTHON,
        "expected_runtime_root": RUNTIME_ROOT,
        "expected_managed_root": MANAGED_ROOT,
        "expected_service_root": SERVICE_ROOT,
        "expected_protected_roots": PROTECTED_ROOTS,
        "expected_canonical_remote": "github.com/example/repo",
    }
    arguments.update(overrides)
    return runner.validate_runtime_snapshot(bundle, **arguments)


def live_snapshot(captured_at: str) -> dict:
    return {
        "schema_version": 1,
        "captured_at": captured_at,
        "listener": {
            "host": "0.0.0.0",
            "port": runner.LIVE_HTTP_PORT,
            "pid": 7000,
            "started_at": "2026-10-03T09:00:00+00:00",
        },
        "files": {
            "root": "D:/nginx-qa",
            "scope": runner.LIVE_DURABLE_SCOPE,
            "algorithm": runner.LIVE_DURABLE_ALGORITHM,
            "count": 12,
            "total_bytes": 12345,
            "metadata_sha256": "f" * 64,
            "newest_write_utc": "2026-10-03T09:30:00+00:00",
        },
        "git": {"head": SHA, "branch": "main", "status": "clean"},
    }


class StagingQualificationRunnerTests(unittest.TestCase):
    def test_api_url_and_child_health_can_never_target_live(self) -> None:
        self.assertEqual(
            runner.validate_loopback_url("http://127.0.0.1:18025"),
            ("http://127.0.0.1:18025", 18025),
        )
        for invalid in (
            "http://127.0.0.1:8025",
            "http://localhost:18025",
            "http://127.0.0.2:18025",
            "https://127.0.0.1:18025",
            "http://127.0.0.1:18025/path",
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(runner.QualificationError):
                    runner.validate_loopback_url(invalid)
        with patch.object(runner.urllib.request, "build_opener") as opener:
            for port in (8025, 18025, 18099, 18200):
                with self.assertRaisesRegex(
                    runner.QualificationError, "non-exact child health endpoint"
                ):
                    runner._get_child_health("127.0.0.1", port, "/health")
            opener.assert_not_called()

    def test_http_client_explicitly_disables_environment_proxies(self) -> None:
        real_builder = runner.urllib.request.build_opener
        with patch.object(
            runner.urllib.request, "build_opener", side_effect=real_builder
        ) as builder:
            runner.LoopbackJsonClient("http://127.0.0.1:18025")
        proxy_handler = builder.call_args.args[0]
        self.assertIsInstance(proxy_handler, runner.urllib.request.ProxyHandler)
        self.assertEqual(proxy_handler.proxies, {})

    def test_http_client_forwards_the_exact_configured_timeout(self) -> None:
        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @staticmethod
            def geturl() -> str:
                return "http://127.0.0.1:18025/health"

            @staticmethod
            def read(_limit: int) -> bytes:
                return b"{}"

        client = runner.LoopbackJsonClient(
            "http://127.0.0.1:18025",
            timeout=37.5,
        )
        with patch.object(
            client._opener, "open", return_value=FakeResponse()
        ) as open_request:
            self.assertEqual(client.request("GET", "/health"), (200, {}))
        self.assertEqual(open_request.call_args.kwargs["timeout"], 37.5)

    def test_context_key_is_unique_and_repository_bound(self) -> None:
        self.assertEqual(
            runner.build_context_key(
                "https://github.com/Example/Repo.git", "run-20261003"
            ),
            "github.com/example/repo#staging-qualification-run-20261003",
        )
        with self.assertRaises(runner.QualificationError):
            runner.build_context_key("https://github.com/Example/Repo.git", "bad id")

    def test_git_proof_binds_full_manifest_and_fixture_git_blob(self) -> None:
        fixture_bytes = b"source-bound fixture\n"
        fixture_sha = hashlib.sha256(fixture_bytes).hexdigest()
        process_spec = {
            "command": [str(CHILD_PYTHON), runner.CHILD_FIXTURE_PATH, "--mode", "serve"],
            "cwd": ".",
            "environment": {},
            "health_path": "/health",
            "restart_policy": "on_failure",
            "max_restart_attempts": 1,
            "restart_backoff_seconds": 0,
            "resource_limits": deepcopy(runner.EXPECTED_RESOURCE_LIMITS),
        }
        manifest = {
            "git_address": "https://github.com/example/repo.git",
            "git": {"source_ref": REF, "expected_source_commit": SHA},
            "execution": {
                "mode": "parallel",
                "start_nodes": [f"child-{index}" for index in range(4)],
            },
            "nodes": [
                {
                    "id": f"child-{index}",
                    "workspace": {"access": "read", "process": deepcopy(process_spec)},
                }
                for index in range(4)
            ],
            "files": [{"path": runner.CHILD_FIXTURE_PATH, "sha256": fixture_sha}],
        }
        config = config_fixture()
        approved_manifest_hash = hashlib.sha256(
            json.dumps(manifest).encode()
        ).hexdigest()

        def fake_git(_root: Path, *arguments: str, timeout: float = 120.0) -> bytes:
            del timeout
            if arguments == ("rev-parse", "HEAD"):
                return (SHA + "\n").encode()
            if arguments[:1] == ("status",):
                return b""
            if arguments == ("rev-parse", f"{REF}^{{commit}}"):
                return (SHA + "\n").encode()
            if arguments == ("remote", "get-url", "origin"):
                return b"https://github.com/example/repo.git\n"
            if arguments[:3] == ("ls-remote", "--exit-code", "origin"):
                return f"{SHA}\t{REF}\n".encode()
            if arguments == ("show", f"{SHA}:{MANIFEST_PATH}"):
                return json.dumps(manifest).encode()
            if arguments == ("show", f"{SHA}:{runner.CHILD_FIXTURE_PATH}"):
                return fixture_bytes
            raise AssertionError(arguments)

        with (
            patch.object(runner, "_git", side_effect=fake_git),
            patch.object(
                runner,
                "EXPECTED_MANIFEST_SHA256",
                approved_manifest_hash,
            ),
        ):
            proof = runner.validate_git_source(config)
        self.assertEqual(proof["fixture_sha256"], fixture_sha)
        self.assertEqual(proof["expected_child_python"], str(CHILD_PYTHON))

        manifest["nodes"][0]["workspace"]["process"]["health_path"] = "/ready"
        with (
            patch.object(runner, "_git", side_effect=fake_git),
            patch.object(
                runner,
                "EXPECTED_MANIFEST_SHA256",
                approved_manifest_hash,
            ),
        ):
            with self.assertRaisesRegex(runner.QualificationError, "exact staging fixture"):
                runner.validate_git_source(config)

    def test_start_response_requires_local_manifest_blob_hash(self) -> None:
        _, response = runtime_fixture()
        runner._assert_start_response(
            201,
            response,
            config=config_fixture(),
            project_id=PROJECT_ID,
            allow_deduplicated=False,
            expected_manifest_sha256=MANIFEST_SHA256,
        )
        response["identity"]["manifest_sha256"] = "d" * 64
        with self.assertRaises(runner.QualificationError):
            runner._assert_start_response(
                201,
                response,
                config=config_fixture(),
                project_id=PROJECT_ID,
                allow_deduplicated=False,
                expected_manifest_sha256=MANIFEST_SHA256,
            )

    def test_runtime_snapshot_proves_four_exact_owned_children(self) -> None:
        bundle, response = runtime_fixture()
        summary = validate_runtime(bundle, response)
        self.assertEqual(len(summary["processes"]), 4)
        self.assertEqual(len(summary["leases"]), 4)
        self.assertEqual(
            {item["port"] for item in summary["leases"]},
            {18100, 18101, 18102, 18103},
        )
        self.assertIn("queue_items", summary["side_effects"])

    def test_runtime_snapshot_rejects_wrong_roots_or_protected_roots(self) -> None:
        bundle, response = runtime_fixture()
        bundle["state"]["runtime_config"]["managed_root"] = "D:/live/managed"
        with self.assertRaisesRegex(runner.QualificationError, "exact staging boundary"):
            validate_runtime(bundle, response)

        bundle, response = runtime_fixture()
        bundle["state"]["runtime_config"]["protected_roots"].reverse()
        with self.assertRaisesRegex(runner.QualificationError, "protected roots"):
            validate_runtime(bundle, response)

    def test_runtime_snapshot_rejects_relational_owner_column_drift(self) -> None:
        bundle, response = runtime_fixture()
        bundle["owner_processes"][0]["relational"]["pid"] += 1
        with self.assertRaisesRegex(runner.QualificationError, "owner differs"):
            validate_runtime(bundle, response)

    def test_runtime_snapshot_rejects_duplicate_or_extra_port_ownership(self) -> None:
        bundle, response = runtime_fixture()
        bundle["state"]["port_leases"][1]["port"] = 18100
        bundle["state"]["processes"][1]["health_endpoint"]["port"] = 18100
        with self.assertRaisesRegex(runner.QualificationError, "ports are not unique"):
            validate_runtime(bundle, response)

        bundle, response = runtime_fixture()
        extra = deepcopy(bundle["live_assignment_leases"][0])
        extra["json"]["lease_id"] = "lease-extra"
        extra["relational"]["lease_id"] = "lease-extra"
        extra["json"]["port"] = 18150
        extra["relational"]["port"] = 18150
        bundle["live_assignment_leases"].append(extra)
        with self.assertRaisesRegex(runner.QualificationError, "duplicate or unexpected"):
            validate_runtime(bundle, response)

    def test_runtime_snapshot_rejects_wrong_interpreter_or_health_path(self) -> None:
        bundle, response = runtime_fixture()
        with self.assertRaisesRegex(runner.QualificationError, "expected staging interpreter"):
            validate_runtime(
                bundle,
                response,
                expected_child_python=Path("C:/other/.venv/Scripts/python.exe"),
            )
        bundle, response = runtime_fixture()
        bundle["state"]["processes"][0]["executable_path"] = (
            "C:/Windows/System32/notepad.exe"
        )
        sync_process_owner(bundle)
        with self.assertRaisesRegex(runner.QualificationError, "staging interpreter"):
            validate_runtime(bundle, response)
        bundle, response = runtime_fixture()
        bundle["state"]["processes"][0]["health_endpoint"]["path"] = "/other"
        with self.assertRaisesRegex(runner.QualificationError, "endpoint"):
            validate_runtime(bundle, response)

    def test_runtime_snapshot_rejects_process_runtime_escape(self) -> None:
        bundle, response = runtime_fixture()
        escaped = "D:/nginx-qa"
        process = bundle["state"]["processes"][0]
        process["process_id"] = escaped
        process["runtime_root"] = escaped
        bundle["state"]["port_leases"][0]["process_id"] = escaped
        sync_process_owner(bundle)
        sync_lease_owner(bundle)
        with self.assertRaisesRegex(runner.QualificationError, "path-safe"):
            validate_runtime(bundle, response)

    def test_runtime_snapshot_requires_exact_numeric_types(self) -> None:
        for field, value in (
            ("restart_attempt", 0.0),
            ("max_restart_attempts", True),
            ("restart_backoff_seconds", False),
        ):
            with self.subTest(field=field, value=value):
                bundle, response = runtime_fixture()
                bundle["state"]["processes"][0][field] = value
                sync_process_owner(bundle)
                with self.assertRaisesRegex(
                    runner.QualificationError, "staging interpreter"
                ):
                    validate_runtime(bundle, response)
        bundle, response = runtime_fixture()
        bundle["state"]["processes"][0]["resource_limits"][
            "wall_time_seconds"
        ] = 3600.0
        sync_process_owner(bundle)
        with self.assertRaisesRegex(runner.QualificationError, "staging interpreter"):
            validate_runtime(bundle, response)
        for invalid_fence in (True, 3.0):
            with self.subTest(fencing_token=invalid_fence):
                bundle, response = runtime_fixture()
                bundle["row"]["fencing_token"] = invalid_fence
                with self.assertRaisesRegex(runner.QualificationError, "identity/status"):
                    validate_runtime(bundle, response)
        bundle, response = runtime_fixture()
        bundle["state"]["runtime_config"]["http_port"] = 18025.0
        with self.assertRaisesRegex(runner.QualificationError, "staging boundary"):
            validate_runtime(bundle, response)
        bundle, response = runtime_fixture()
        bundle["state"]["processes"][0]["health_endpoint"]["port"] = 18100.0
        sync_process_owner(bundle)
        with self.assertRaisesRegex(runner.QualificationError, "endpoint"):
            validate_runtime(bundle, response)

    def test_timeout_requires_a_finite_positive_number(self) -> None:
        self.assertEqual(runner._validate_timeout_seconds(1), 1.0)
        self.assertEqual(runner._validate_timeout_seconds(0.5), 0.5)
        for invalid in (True, 0, -1, float("nan"), float("inf"), float("-inf")):
            with self.subTest(timeout=invalid):
                with self.assertRaisesRegex(runner.QualificationError, "finite positive"):
                    runner._validate_timeout_seconds(invalid)

    def test_runtime_snapshot_uses_type_sensitive_relational_parity(self) -> None:
        bundle, response = runtime_fixture()
        bundle["owner_processes"][0]["json"]["pid"] = 4100.0
        with self.assertRaisesRegex(runner.QualificationError, "owner differs"):
            validate_runtime(bundle, response)

    def test_cleanup_snapshot_uses_type_sensitive_relational_parity(self) -> None:
        bundle, _ = runtime_fixture()
        for index, process in enumerate(bundle["state"]["processes"]):
            process["state"] = "STOPPED"
            sync_process_owner(bundle, index)
        for index, lease in enumerate(bundle["state"]["port_leases"]):
            lease["status"] = "released"
            sync_lease_owner(bundle, index)
        bundle["live_assignment_processes"] = []
        bundle["live_assignment_leases"] = []
        process_ids = {
            item["process_id"] for item in bundle["state"]["processes"]
        }
        lease_ids = {item["lease_id"] for item in bundle["state"]["port_leases"]}
        runner._validate_cleanup_snapshot(
            bundle, process_ids=process_ids, lease_ids=lease_ids
        )
        bundle["owner_processes"][0]["relational"]["pid"] = 4100.0
        with self.assertRaisesRegex(runner.QualificationError, "relational parity"):
            runner._validate_cleanup_snapshot(
                bundle, process_ids=process_ids, lease_ids=lease_ids
            )
        bundle, response = runtime_fixture()
        bundle["owner_processes"][0]["relational"]["pid"] = 4100.0
        with self.assertRaisesRegex(runner.QualificationError, "owner differs"):
            validate_runtime(bundle, response)

    def test_health_payload_binds_worker_to_process_nonce_and_roots(self) -> None:
        bundle, response = runtime_fixture()
        summary = validate_runtime(bundle, response)

        def getter(_host: str, port: int, _path: str) -> dict:
            index = port - 18100
            process = bundle["state"]["processes"][index]
            workspace = bundle["state"]["workspaces"][index]
            return {
                "status": "ok",
                "identity": process["process_id"],
                "process_id": process["process_id"],
                "runtime_root": process["runtime_root"],
                "cwd": workspace["actual_git_toplevel"],
                "host": "127.0.0.1",
                "port": port,
                "pid": 5100 + index,
                "launch_nonce": process["launch_nonce"],
            }

        health = runner.verify_child_health(summary, getter=getter)
        self.assertEqual({item["worker_pid"] for item in health}, set(range(5100, 5104)))

        def wrong_getter(host: str, port: int, path: str) -> dict:
            payload = getter(host, port, path)
            payload["launch_nonce"] = "wrong"
            return payload

        with self.assertRaisesRegex(runner.QualificationError, "ownership proof"):
            runner.verify_child_health(summary, getter=wrong_getter)

        def float_port_getter(host: str, port: int, path: str) -> dict:
            payload = getter(host, port, path)
            payload["port"] = float(port)
            return payload

        with self.assertRaisesRegex(runner.QualificationError, "ownership proof"):
            runner.verify_child_health(summary, getter=float_port_getter)

    def test_os_ownership_enumerates_entire_child_range_and_jobs(self) -> None:
        bundle, response = runtime_fixture()
        summary = validate_runtime(bundle, response)
        health = [
            {
                "assignment_id": f"assignment-{index}",
                "process_id": f"process-{index}",
                "leader_pid": 4100 + index,
                "worker_pid": 5100 + index,
                "port": 18100 + index,
            }
            for index in range(4)
        ]
        inspected_ports: list[int] = []
        all_inspected_ports: list[int] = []

        def port_owners(host: str, port: int):
            self.assertEqual(host, "127.0.0.1")
            inspected_ports.append(port)
            return [5100 + port - 18100] if 18100 <= port <= 18103 else []

        def all_listeners(port: int):
            all_inspected_ports.append(port)
            owners = [5100 + port - 18100] if 18100 <= port <= 18103 else []
            return {"127.0.0.1": owners}

        def identity(pid: int):
            index = pid - 4100
            return SimpleNamespace(
                pid=pid,
                birth_token=f"birth-{index}",
                executable_path=str(CHILD_PYTHON),
                cwd=None,
            )

        class FakeJob:
            def __init__(self, name: str) -> None:
                self.name = name
                self.closed = False

            def contains_exact(self, _identity) -> bool:
                return True

            def contains(self, pid: int) -> bool:
                return 5100 <= pid <= 5103

            def close(self) -> None:
                self.closed = True

        proof = runner.verify_child_os_ownership(
            summary,
            health,
            identity_fn=identity,
            port_owner_fn=port_owners,
            job_open_fn=FakeJob,
            all_listener_fn=all_listeners,
            os_name="nt",
        )
        self.assertEqual(len(proof), 4)
        self.assertEqual(inspected_ports, list(range(18100, 18200)))
        self.assertEqual(all_inspected_ports, list(range(18100, 18200)))
        self.assertNotIn(8025, inspected_ports)

        def orphan_owners(_host: str, port: int):
            if port == 18150:
                return [9999]
            return [5100 + port - 18100] if 18100 <= port <= 18103 else []

        def orphan_listeners(port: int):
            return {"127.0.0.1": orphan_owners("127.0.0.1", port)}

        with self.assertRaisesRegex(runner.QualificationError, "orphan listener"):
            runner.verify_child_os_ownership(
                summary,
                health,
                identity_fn=identity,
                port_owner_fn=orphan_owners,
                job_open_fn=FakeJob,
                all_listener_fn=orphan_listeners,
                os_name="nt",
            )

    def test_windows_listener_snapshot_fails_closed_without_filtered_errors(self) -> None:
        completed = SimpleNamespace(returncode=0, stdout="[]", stderr="")
        with (
            patch.object(runner.os, "name", "nt"),
            patch.object(runner.subprocess, "run", return_value=completed) as run,
        ):
            self.assertEqual(runner._windows_listener_snapshot(), {})
        script = run.call_args.args[0][-1]
        self.assertIn("Get-NetTCPConnection -State Listen -ErrorAction Stop", script)
        self.assertNotIn("SilentlyContinue", script)
        self.assertNotIn("-LocalPort", script)
        failed = SimpleNamespace(returncode=1, stdout="", stderr="failure")
        with (
            patch.object(runner.os, "name", "nt"),
            patch.object(runner.subprocess, "run", return_value=failed),
            self.assertRaisesRegex(runner.QualificationError, "cannot enumerate"),
        ):
            runner._windows_listener_snapshot()

    def test_host_listener_proof_binds_pid_command_ancestry_and_cwd(self) -> None:
        config = config_fixture()
        base_python = "C:/Python312/python.exe"
        marker = {"base_python": base_python}
        identity = SimpleNamespace(
            pid=9000,
            executable_path=base_python,
            cwd=None,
            birth_token="host-birth",
        )
        command_line = "ignored by injected parser"
        chain = [
            {
                "pid": 9000,
                "parent_pid": 8000,
                "executable_path": base_python,
                "command_line": command_line,
            },
            {
                "pid": 8000,
                "parent_pid": 0,
                "executable_path": "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
                "command_line": "powershell.exe",
            },
        ]
        argv = [
            str(CHILD_PYTHON),
            "-E",
            "-s",
            "-B",
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "18025",
        ]
        proof = runner.validate_host_listener_ownership(
            config,
            marker,
            listener_pid_fn=lambda _port: 9000,
            identity_fn=lambda _pid: identity,
            chain_fn=lambda _pid: chain,
            argv_fn=lambda _line: argv,
            cwd_fn=lambda _pid: str(SERVICE_ROOT),
        )
        self.assertEqual(proof["pid"], 9000)
        self.assertEqual(runner._path_key(proof["cwd"]), runner._path_key(str(SERVICE_ROOT)))
        with self.assertRaisesRegex(runner.QualificationError, "executable/cwd"):
            runner.validate_host_listener_ownership(
                config,
                marker,
                listener_pid_fn=lambda _port: 9000,
                identity_fn=lambda _pid: identity,
                chain_fn=lambda _pid: chain,
                argv_fn=lambda _line: argv,
                cwd_fn=lambda _pid: "C:/wrong",
            )

    def test_host_listener_ancestry_allows_only_proven_chain_termination(self) -> None:
        config = config_fixture()
        base_python = "C:/Python312/python.exe"
        marker = {"base_python": base_python}
        identity = SimpleNamespace(
            pid=9000,
            executable_path=base_python,
            cwd=None,
            birth_token="host-birth",
        )
        argv = [
            str(CHILD_PYTHON),
            "-E",
            "-s",
            "-B",
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "18025",
        ]

        def prove(chain: list[dict[str, object]]) -> dict[str, object]:
            return runner.validate_host_listener_ownership(
                config,
                marker,
                listener_pid_fn=lambda _port: 9000,
                identity_fn=lambda _pid: identity,
                chain_fn=lambda _pid: chain,
                argv_fn=lambda _line: argv,
                cwd_fn=lambda _pid: str(SERVICE_ROOT),
            )

        terminated_chain = [
            {
                "pid": 9000,
                "parent_pid": 8000,
                "executable_path": base_python,
                "command_line": "listener",
            },
            {
                "pid": 8000,
                "parent_pid": 3924,
                "executable_path": "C:/Program Files/PowerShell/7/pwsh.exe",
                "command_line": "launcher",
            },
        ]
        self.assertEqual(prove(terminated_chain)["pid"], 9000)

        invalid_chains = {
            "internal gap": [
                {**terminated_chain[0]},
                {**terminated_chain[1], "pid": 7000, "parent_pid": 0},
            ],
            "terminal cycle": [
                {**terminated_chain[0]},
                {**terminated_chain[1], "parent_pid": 9000},
            ],
            "depth truncation": [
                {
                    "pid": 9000 - index,
                    "parent_pid": 8999 - index if index < 15 else 7000,
                    "executable_path": base_python if index == 0 else "ancestor.exe",
                    "command_line": "listener" if index == 0 else "ancestor",
                }
                for index in range(runner.MAX_PROCESS_CHAIN_DEPTH)
            ],
        }
        for label, chain in invalid_chains.items():
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    runner.QualificationError, "ancestry is inconsistent"
                ):
                    prove(chain)

        for invalid_parent in (-1, True, 3924.0, "3924"):
            with self.subTest(terminal_parent=invalid_parent):
                chain = deepcopy(terminated_chain)
                chain[-1]["parent_pid"] = invalid_parent
                with self.assertRaisesRegex(
                    runner.QualificationError, "ancestry is inconsistent"
                ):
                    prove(chain)

    @unittest.skipUnless(os.name == "nt", "Windows PEB CWD proof")
    def test_windows_process_cwd_reads_current_process(self) -> None:
        self.assertEqual(
            runner._path_key(runner._windows_process_cwd(os.getpid())),
            runner._path_key(str(Path.cwd())),
        )

    def test_adoption_requires_changed_host_and_stable_side_effects(self) -> None:
        bundle, response = runtime_fixture()
        summary = runner._evidence_summary(validate_runtime(bundle, response))
        runner.validate_adoption(
            summary,
            deepcopy(summary),
            host_pid_before=6000,
            host_pid_after=6001,
        )
        with self.assertRaisesRegex(runner.QualificationError, "did not change"):
            runner.validate_adoption(
                summary,
                deepcopy(summary),
                host_pid_before=6000,
                host_pid_after=6000,
            )
        with self.assertRaisesRegex(runner.QualificationError, "did not change"):
            runner.validate_adoption(
                summary,
                deepcopy(summary),
                host_pid_before=True,
                host_pid_after=6001,
            )
        changed = deepcopy(summary)
        changed["side_effects"]["queue_items"]["count"] += 1
        with self.assertRaisesRegex(runner.QualificationError, "changed side_effects"):
            runner.validate_adoption(
                summary,
                changed,
                host_pid_before=6000,
                host_pid_after=6001,
            )
        changed = deepcopy(summary)
        changed["processes"][0]["pid"] = float(changed["processes"][0]["pid"])
        with self.assertRaisesRegex(runner.QualificationError, "changed processes"):
            runner.validate_adoption(
                summary,
                changed,
                host_pid_before=6000,
                host_pid_after=6001,
            )

    def test_external_live_snapshot_schema_and_comparison_are_strict(self) -> None:
        before = live_snapshot("2026-10-03T10:00:00+00:00")
        after = live_snapshot("2026-10-03T10:01:00+00:00")
        runner.compare_external_live_snapshots(before, after)
        after["files"]["count"] += 1
        with self.assertRaisesRegex(runner.QualificationError, "snapshot changed"):
            runner.compare_external_live_snapshots(before, after)
        invalid = live_snapshot("not-a-time")
        with self.assertRaisesRegex(runner.QualificationError, "time is invalid"):
            runner.validate_external_live_snapshot(invalid)
        invalid = live_snapshot("2026-10-03T10:00:00+00:00")
        invalid["listener"]["host"] = "127.0.0.1"
        with self.assertRaisesRegex(runner.QualificationError, "listener identity"):
            runner.validate_external_live_snapshot(invalid)
        invalid = live_snapshot("2026-10-03T10:00:00+00:00")
        invalid["files"]["scope"] = "whole-tree"
        with self.assertRaisesRegex(runner.QualificationError, "file identity"):
            runner.validate_external_live_snapshot(invalid)
        invalid = live_snapshot("2026-10-03T10:00:00+00:00")
        del invalid["files"]["scope"]
        with self.assertRaisesRegex(runner.QualificationError, "file snapshot"):
            runner.validate_external_live_snapshot(invalid)
        invalid = live_snapshot("2026-10-03T10:00:00+00:00")
        invalid["schema_version"] = True
        with self.assertRaisesRegex(runner.QualificationError, "schema"):
            runner.validate_external_live_snapshot(invalid)
        invalid = live_snapshot("2026-10-03T10:00:00+00:00")
        invalid["listener"]["port"] = float(runner.LIVE_HTTP_PORT)
        with self.assertRaisesRegex(runner.QualificationError, "listener identity"):
            runner.validate_external_live_snapshot(invalid)
        for field in ("started_at",):
            with self.subTest(field=field):
                invalid = live_snapshot("2026-10-03T10:00:00+00:00")
                invalid["listener"][field] = "2026-10-03T10:00:01+00:00"
                with self.assertRaisesRegex(runner.QualificationError, "not causal"):
                    runner.validate_external_live_snapshot(invalid)
        invalid = live_snapshot("2026-10-03T10:00:00+00:00")
        invalid["files"]["newest_write_utc"] = "2026-10-03T10:00:01+00:00"
        with self.assertRaisesRegex(runner.QualificationError, "not causal"):
            runner.validate_external_live_snapshot(invalid)

    def test_live_after_snapshot_path_is_directly_owned_and_distinct(self) -> None:
        config = config_fixture()
        owned = STATE_BASE / "evidence" / "live-after.json"
        self.assertEqual(
            runner.validate_live_after_path(config, owned), owned.resolve(strict=False)
        )
        for invalid in (
            STATE_BASE / "live-after.json",
            STATE_BASE / "evidence" / "nested" / "live-after.json",
            STATE_BASE / "evidence" / "live-after.txt",
            STATE_BASE / "evidence" / "evidence.json:after.json",
        ):
            with self.subTest(path=invalid):
                with self.assertRaisesRegex(runner.QualificationError, "owned evidence root"):
                    runner.validate_live_after_path(config, invalid)
        with self.assertRaisesRegex(runner.QualificationError, "must not replace evidence"):
            runner.validate_live_after_path(config, config.evidence_path)

    def test_evidence_allows_fencing_token_but_rejects_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "evidence.json"
            value = {
                "schema_version": runner.EVIDENCE_SCHEMA_VERSION,
                "qualification": runner.QUALIFICATION_ID,
                "runtime": {"fencing_token": 7},
            }
            path.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(runner._read_evidence(path), value)
            value["schema_version"] = True
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(runner.QualificationError, "schema"):
                runner._read_evidence(path)
            value["schema_version"] = runner.EVIDENCE_SCHEMA_VERSION
            value["runtime"]["api_token"] = "forbidden"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(runner.QualificationError, "sensitive key"):
                runner._read_evidence(path)

    def test_config_rejects_bidirectional_protected_root_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            service = base / "service"
            state = base / "state"
            runtime_root = state / "runtime_state"
            managed_root = state / "managed"
            scripts = state / ".venv" / "Scripts"
            for directory in (
                service / ".git",
                runtime_root / "leases",
                managed_root,
                scripts,
                state / "evidence",
            ):
                directory.mkdir(parents=True, exist_ok=True)
            child_python = scripts / "python.exe"
            child_python.write_bytes(b"python")
            database = runtime_root / "leases" / "managed-import.sqlite3"
            database.write_bytes(b"sqlite")
            marker = state / ".nginx-qa-staging-owner.json"
            marker.write_text("{}", encoding="utf-8")
            baseline = state / "evidence" / "live.json"
            baseline.write_text("{}", encoding="utf-8")
            config = runner.PrepareConfig(
                repo_root=service,
                expected_sha=SHA,
                ref=REF,
                manifest_path=MANIFEST_PATH,
                git_address="https://github.com/example/repo.git",
                expected_child_python=child_python,
                expected_runtime_root=runtime_root,
                expected_managed_root=managed_root,
                ownership_marker=marker,
                run_id="run-1",
                staging_url=runner.DEFAULT_STAGING_URL,
                managed_db=database,
                evidence_path=state / "evidence" / "run.json",
                protected_roots=(managed_root / "protected-child",),
                timeout_seconds=1.0,
                live_baseline_path=baseline,
            )
            with patch.object(
                runner,
                "EXPECTED_PROTECTED_ROOTS",
                (managed_root / "protected-child", base / "another-required-root"),
            ):
                with self.assertRaisesRegex(
                    runner.QualificationError, "exact protected roots"
                ):
                    runner.validate_config(config)
            with (
                patch.object(runner, "EXPECTED_SERVICE_ROOT", service),
                patch.object(runner, "EXPECTED_STATE_BASE", state),
                patch.object(runner, "EXPECTED_VENV_ROOT", scripts.parent),
                patch.object(runner, "EXPECTED_CHILD_PYTHON", child_python),
                patch.object(runner, "EXPECTED_RUNTIME_ROOT", runtime_root),
                patch.object(runner, "EXPECTED_PROMPT_ROOT", state / "prompt"),
                patch.object(runner, "EXPECTED_MANAGED_ROOT", managed_root),
                patch.object(runner, "EXPECTED_OWNERSHIP_MARKER", marker),
                patch.object(
                    runner, "EXPECTED_LIVE_ROOT", managed_root / "protected-child"
                ),
                patch.object(
                    runner,
                    "EXPECTED_PROTECTED_ROOTS",
                    (managed_root / "protected-child",),
                ),
            ):
                with self.assertRaisesRegex(runner.QualificationError, "overlaps"):
                    runner.validate_config(config)

    def test_ownership_marker_requires_exact_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            service = base / "service"
            state = base / "state"
            venv = state / ".venv"
            runtime_root = state / "runtime_state"
            managed_root = state / "managed"
            prompt_root = state / "prompt"
            protected = base / "live"
            for directory in (
                service,
                venv / "Scripts",
                runtime_root,
                managed_root,
                prompt_root,
                protected,
            ):
                directory.mkdir(parents=True, exist_ok=True)
            child_python = venv / "Scripts" / "python.exe"
            child_python.write_bytes(b"venv")
            marker_path = state / ".nginx-qa-staging-owner.json"
            ownership = {
                "instance_id": runner.STAGING_INSTANCE_ID,
                "approved_commit": SHA,
                "service_root": str(service),
                "origin": "https://github.com/example/repo.git",
                "branch": REF.removeprefix("refs/heads/"),
                "state_base": str(state),
                "venv_root": str(venv),
                "base_python": sys.executable,
                "base_python_prefix": sys.base_prefix,
                "base_python_version": runner.platform.python_version(),
                "base_python_sha256": runner._hash_file(Path(sys.executable)),
                "runtime_root": str(runtime_root),
                "prompt_root": str(prompt_root),
                "managed_root": str(managed_root),
                "legacy_runtime_root": str(service / "runtime_state"),
                "http_host": "127.0.0.1",
                "http_port": runner.STAGING_HTTP_PORT,
                "child_port_range": "18100-18199",
                "protected_roots": [str(protected)],
            }
            marker = {
                "schema_version": 1,
                "initialization_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                "ownership": ownership,
            }
            marker_path.write_text(json.dumps(marker), encoding="utf-8")
            config = runner.PrepareConfig(
                repo_root=service,
                expected_sha=SHA,
                ref=REF,
                manifest_path=MANIFEST_PATH,
                git_address="https://github.com/example/repo.git",
                expected_child_python=child_python,
                expected_runtime_root=runtime_root,
                expected_managed_root=managed_root,
                ownership_marker=marker_path,
                run_id="run-1",
                staging_url=runner.DEFAULT_STAGING_URL,
                managed_db=runtime_root / "leases" / "managed-import.sqlite3",
                evidence_path=state / "evidence.json",
                protected_roots=(protected,),
                timeout_seconds=1.0,
                live_baseline_path=state / "live.json",
            )
            with patch.object(runner, "EXPECTED_PROMPT_ROOT", prompt_root):
                proof = runner.validate_ownership_marker(config)
            self.assertEqual(proof["approved_commit"], SHA)
            for invalid_initialization_id in (
                "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE",
                "{aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee}",
                "aaaaaaaabbbb4ccc8dddeeeeeeeeeeee",
                1,
            ):
                with self.subTest(initialization_id=invalid_initialization_id):
                    invalid_marker = deepcopy(marker)
                    invalid_marker["initialization_id"] = invalid_initialization_id
                    marker_path.write_text(
                        json.dumps(invalid_marker), encoding="utf-8"
                    )
                    with patch.object(runner, "EXPECTED_PROMPT_ROOT", prompt_root):
                        with self.assertRaisesRegex(
                            runner.QualificationError, "initialization|identity"
                        ):
                            runner.validate_ownership_marker(config)
            invalid_marker = deepcopy(marker)
            invalid_marker["schema_version"] = True
            marker_path.write_text(json.dumps(invalid_marker), encoding="utf-8")
            with patch.object(runner, "EXPECTED_PROMPT_ROOT", prompt_root):
                with self.assertRaisesRegex(runner.QualificationError, "identity"):
                    runner.validate_ownership_marker(config)
            invalid_marker = deepcopy(marker)
            invalid_marker["ownership"]["http_port"] = float(
                runner.STAGING_HTTP_PORT
            )
            marker_path.write_text(json.dumps(invalid_marker), encoding="utf-8")
            with patch.object(runner, "EXPECTED_PROMPT_ROOT", prompt_root):
                with self.assertRaisesRegex(runner.QualificationError, "does not match"):
                    runner.validate_ownership_marker(config)
            marker_path.write_text(json.dumps(marker), encoding="utf-8")
            marker["ownership"]["origin"] = "https://github.com/example/repo"
            marker_path.write_text(json.dumps(marker), encoding="utf-8")
            with patch.object(runner, "EXPECTED_PROMPT_ROOT", prompt_root):
                with self.assertRaisesRegex(runner.QualificationError, "does not match"):
                    runner.validate_ownership_marker(config)

    def test_managed_state_reader_selects_relational_columns_read_only(self) -> None:
        bundle, _ = runtime_fixture()
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "managed.sqlite3"
            connection = sqlite3.connect(database)
            connection.executescript(
                """
                CREATE TABLE managed_sprints (
                    project_id TEXT, sprint_id TEXT, status TEXT,
                    fencing_token INTEGER, state_json TEXT
                );
                CREATE TABLE managed_process_owners (
                    process_id TEXT, assignment_id TEXT, port_lease_id TEXT,
                    pid INTEGER, state TEXT, process_json TEXT
                );
                CREATE TABLE managed_port_leases (
                    lease_id TEXT, instance_id TEXT, network_namespace_id TEXT,
                    assignment_id TEXT, process_id TEXT, host TEXT, port INTEGER,
                    status TEXT, lease_json TEXT
                );
                CREATE TABLE managed_queue_items (
                    dedupe_key TEXT, event_id TEXT, event_type TEXT,
                    payload_json TEXT, receipt_id TEXT, created_at TEXT
                );
                """
            )
            connection.execute(
                "INSERT INTO managed_sprints VALUES (?, ?, ?, ?, ?)",
                (PROJECT_ID, SPRINT_ID, "active", 3, json.dumps(bundle["state"])),
            )
            for wrapper in bundle["owner_processes"]:
                process = wrapper["json"]
                connection.execute(
                    "INSERT INTO managed_process_owners VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        process["process_id"],
                        process["assignment_id"],
                        process["port_lease_id"],
                        process["pid"],
                        process["state"],
                        json.dumps(process),
                    ),
                )
            for wrapper in bundle["owner_leases"]:
                lease = wrapper["json"]
                connection.execute(
                    "INSERT INTO managed_port_leases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        lease["lease_id"],
                        lease["instance_id"],
                        lease["network_namespace_id"],
                        lease["assignment_id"],
                        lease["process_id"],
                        lease["host"],
                        lease["port"],
                        lease["status"],
                        json.dumps(lease),
                    ),
                )
            connection.execute(
                "INSERT INTO managed_queue_items VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "dedupe",
                    "event",
                    "TEST",
                    json.dumps({"sprint_id": SPRINT_ID}),
                    "receipt",
                    "2026-10-03T10:00:00+00:00",
                ),
            )
            connection.commit()
            connection.close()

            snapshot = runner.ManagedStateReader(database).read(PROJECT_ID, SPRINT_ID)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(len(snapshot["owner_processes"]), 4)
            self.assertEqual(len(snapshot["owner_leases"]), 4)
            self.assertEqual(snapshot["owner_processes"][0]["relational"]["pid"], 4100)
            self.assertEqual(len(snapshot["queue_items"]), 1)
            verification = sqlite3.connect(database)
            try:
                self.assertEqual(
                    verification.execute("SELECT COUNT(*) FROM managed_sprints").fetchone()[0],
                    1,
                )
            finally:
                verification.close()

    def test_prepare_resumes_checkpoint_and_reproves_owner_before_posts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evidence_path = Path(temporary) / "evidence.json"
            config = config_fixture(evidence_path=evidence_path)
            inputs = runner._inputs(config)
            git_proof = {"manifest_sha256": MANIFEST_SHA256, "head": SHA}
            marker_proof = {"marker_sha256": "e" * 64}
            host_proof = {
                "pid": 9000,
                "birth_token_sha256": "f" * 64,
                "executable_path": "C:/Python312/python.exe",
                "cwd": str(SERVICE_ROOT),
                "venv_ancestry": True,
                "command_sha256": "1" * 64,
            }
            evidence = {
                "schema_version": runner.EVIDENCE_SCHEMA_VERSION,
                "qualification": runner.QUALIFICATION_ID,
                "status": "project_provisioned",
                "created_at": "2026-10-03T10:00:00+00:00",
                "updated_at": "2026-10-03T10:00:00+00:00",
                "inputs": inputs,
                "git": git_proof,
                "ownership_marker": marker_proof,
                "pre_restart_host": host_proof,
                "pre_restart_listener_pid": 9000,
                "external_live_baseline": live_snapshot(
                    "2026-10-03T10:00:00+00:00"
                ),
                "checks": [],
            }
            runner.atomic_write_json(evidence_path, evidence)
            _, start_response = runtime_fixture()
            start_response["deduplicated"] = True
            calls: list[str] = []
            client_configs: list[tuple[str, float]] = []

            class FakeClient:
                def __init__(self, base_url: str, *, timeout: float) -> None:
                    self.base_url = base_url
                    client_configs.append((base_url, timeout))

                def request(self, method: str, path: str, payload=None):
                    self.assert_post(method)
                    calls.append(path)
                    if path == "/project-manager/0001":
                        return 200, {
                            "project": {"git_context_key": inputs["git_context_key"]},
                            "project_phone": PROJECT_ID,
                            "created": False,
                        }
                    return 200, deepcopy(start_response)

                @staticmethod
                def assert_post(method: str) -> None:
                    if method != "POST":
                        raise AssertionError(method)

            health = [
                {"leader_pid": 4100 + index, "worker_pid": 5100 + index}
                for index in range(4)
            ]
            summary = {"processes": [{"pid": 4100 + index} for index in range(4)]}
            with (
                patch.object(runner, "validate_config", return_value=config),
                patch.object(runner, "validate_git_source", return_value=git_proof),
                patch.object(
                    runner, "validate_ownership_marker", return_value=marker_proof
                ) as marker_mock,
                patch.object(
                    runner, "validate_host_listener_ownership", return_value=host_proof
                ) as host_mock,
                patch.object(runner, "LoopbackJsonClient", FakeClient),
                patch.object(runner, "ManagedStateReader"),
                patch.object(
                    runner,
                    "wait_for_healthy",
                    return_value=(summary, health, [{"job": "stable"}]),
                ),
            ):
                result = runner.prepare(config)
            self.assertEqual(result["status"], "awaiting_external_restart")
            self.assertEqual(len(calls), 2)
            self.assertEqual(
                client_configs,
                [(config.staging_url, config.timeout_seconds)],
            )
            self.assertEqual(marker_mock.call_count, 3)
            self.assertEqual(host_mock.call_count, 4)


if __name__ == "__main__":
    unittest.main()
