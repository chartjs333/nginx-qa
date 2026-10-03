from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
import tempfile
from types import ModuleType
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests.test_staging_qualification_e2e_runner import (
    PROJECT_ID,
    config_fixture,
    runner,
    runtime_fixture,
    sync_lease_owner,
    sync_process_owner,
    validate_runtime,
)


def _health_proof(summary: dict) -> list[dict]:
    return [
        {
            "assignment_id": item["assignment_id"],
            "process_id": item["process_id"],
            "leader_pid": item["pid"],
            "worker_pid": 5100 + index,
            "port": item["health_endpoint"]["port"],
            "runtime_root": item["runtime_root"],
            "workspace_root": summary["workspaces"][index]["root"],
        }
        for index, item in enumerate(summary["processes"])
    ]


def _os_proof(summary: dict, health: list[dict]) -> list[dict]:
    return [
        {
            "assignment_id": item["assignment_id"],
            "process_id": item["process_id"],
            "leader_pid": item["leader_pid"],
            "leader_birth_token_sha256": f"birth-sha-{index}",
            "leader_executable": summary["_expected_child_python"],
            "leader_cwd": summary["workspaces"][index]["root"],
            "job_object_id": f"job-{index}",
            "port": item["port"],
            "port_owner_pids": [item["worker_pid"]],
        }
        for index, item in enumerate(health)
    ]


class CleanupCheckpointReproofTests(unittest.TestCase):
    def _assert_cleanup_rejects_before_supervisor(
        self,
        bundle: dict,
        response: dict,
        expected_runtime: dict,
        expected_health: list[dict],
        expected_os: list[dict],
        *,
        current_health: list[dict] | None = None,
        current_os: list[dict] | None = None,
        listener_snapshots: list[dict] | None = None,
        bundle_after_reproof: dict | None = None,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            evidence_path = (Path(temp_dir) / "evidence.json").resolve()
            config = config_fixture(evidence_path=evidence_path)
            evidence = {
                "status": "verified_awaiting_cleanup",
                "git": {"commit": "expected"},
                "ownership_marker": {"instance_id": "expected"},
                "start_response": deepcopy(response),
                "post_restart": {
                    "runtime": deepcopy(expected_runtime),
                    "child_health": deepcopy(expected_health),
                    "child_os_ownership": deepcopy(expected_os),
                },
            }
            reader = Mock()
            if bundle_after_reproof is None:
                reader.read.return_value = bundle
            else:
                reader.read.side_effect = [bundle, bundle_after_reproof]
            supervisor_instance = Mock()
            supervisor_class = Mock(return_value=supervisor_instance)
            managed_module = ModuleType("nginx_qa.managed_import")
            managed_module.ManagedImportStore = Mock()
            managed_module.ManagedPortReservationRegistry = Mock()
            supervisor_module = ModuleType("nginx_qa.process_supervisor")
            supervisor_module.ManagedProcessSupervisor = supervisor_class
            supervisor_module.ManagedProcessSupervisorError = RuntimeError
            evidence_writer = Mock()
            listener_snapshot = (
                Mock(side_effect=listener_snapshots)
                if listener_snapshots is not None
                else Mock(return_value={})
            )

            with (
                patch.object(runner, "_read_evidence", return_value=evidence),
                patch.object(runner, "_config_from_evidence", return_value=config),
                patch.object(
                    runner, "validate_git_source", return_value=evidence["git"]
                ),
                patch.object(
                    runner,
                    "validate_ownership_marker",
                    return_value=evidence["ownership_marker"],
                ),
                patch.object(
                    runner,
                    "_cleanup_resources",
                    return_value=(
                        PROJECT_ID,
                        response["sprint_id"],
                        {f"process-{index}": {} for index in range(4)},
                        {f"lease-{index}": {} for index in range(4)},
                    ),
                ),
                patch.object(runner, "_windows_listener_snapshot", listener_snapshot),
                patch.object(runner, "ManagedStateReader", return_value=reader),
                patch.object(
                    runner,
                    "verify_child_health",
                    return_value=deepcopy(
                        expected_health if current_health is None else current_health
                    ),
                ),
                patch.object(
                    runner,
                    "verify_child_os_ownership",
                    return_value=deepcopy(expected_os if current_os is None else current_os),
                ),
                patch.object(runner, "atomic_write_json", evidence_writer),
                patch.dict(
                    sys.modules,
                    {
                        "nginx_qa.managed_import": managed_module,
                        "nginx_qa.process_supervisor": supervisor_module,
                    },
                ),
            ):
                with self.assertRaises(runner.QualificationError):
                    runner.cleanup(evidence_path)

            managed_module.ManagedImportStore.assert_not_called()
            managed_module.ManagedPortReservationRegistry.assert_not_called()
            supervisor_class.assert_not_called()
            supervisor_instance.stop.assert_not_called()
            evidence_writer.assert_not_called()

    def test_cleanup_rejects_pid_lease_fence_and_job_drift_before_stop(self) -> None:
        pristine, response = runtime_fixture()
        summary = validate_runtime(pristine, response)
        expected_runtime = runner._evidence_summary(summary)
        expected_health = _health_proof(summary)
        expected_os = _os_proof(summary, expected_health)

        drift_cases: list[
            tuple[str, dict, list[dict] | None, list[dict] | None]
        ] = []

        pid_drift = deepcopy(pristine)
        pid_drift["state"]["processes"][0]["pid"] += 100
        sync_process_owner(pid_drift, 0)
        drift_cases.append(("pid", pid_drift, None, None))

        lease_drift = deepcopy(pristine)
        lease_drift["state"]["port_leases"][0]["port"] += 10
        lease_drift["state"]["processes"][0]["health_endpoint"]["port"] += 10
        sync_lease_owner(lease_drift, 0)
        sync_process_owner(lease_drift, 0)
        drift_cases.append(("lease", lease_drift, None, None))

        fence_drift = deepcopy(pristine)
        fence_drift["row"]["fencing_token"] += 1
        drift_cases.append(("fencing_token", fence_drift, None, None))

        extra_resource = deepcopy(pristine)
        extra_process = deepcopy(extra_resource["state"]["processes"][0])
        extra_process["process_id"] = "unexpected-process"
        extra_resource["state"]["processes"].append(extra_process)
        drift_cases.append(("extra_resource", extra_resource, None, None))

        health_drift = deepcopy(expected_health)
        health_drift[0]["worker_pid"] += 100
        drift_cases.append(("health", deepcopy(pristine), health_drift, None))

        job_drift = deepcopy(expected_os)
        job_drift[0]["job_object_id"] = "replacement-job"
        drift_cases.append(("job", deepcopy(pristine), None, job_drift))

        for label, bundle, current_health, current_os in drift_cases:
            with self.subTest(drift=label):
                self._assert_cleanup_rejects_before_supervisor(
                    bundle,
                    response,
                    expected_runtime,
                    expected_health,
                    expected_os,
                    current_health=current_health,
                    current_os=current_os,
                )

    def test_cleanup_rechecks_host_listener_after_slow_reproof_before_write(self) -> None:
        active, response = runtime_fixture()
        summary = validate_runtime(active, response)
        expected_runtime = runner._evidence_summary(summary)
        expected_health = _health_proof(summary)
        expected_os = _os_proof(summary, expected_health)

        self._assert_cleanup_rejects_before_supervisor(
            active,
            response,
            expected_runtime,
            expected_health,
            expected_os,
            listener_snapshots=[
                {},
                {
                    runner.STAGING_HTTP_PORT: {
                        "127.0.0.1": (91234,),
                    }
                },
            ],
        )

    def test_cleanup_rechecks_database_after_slow_reproof_before_write(self) -> None:
        active, response = runtime_fixture()
        summary = validate_runtime(active, response)
        expected_runtime = runner._evidence_summary(summary)
        expected_health = _health_proof(summary)
        expected_os = _os_proof(summary, expected_health)
        changed_during_reproof = deepcopy(active)
        changed_during_reproof["row"]["fencing_token"] += 1

        self._assert_cleanup_rejects_before_supervisor(
            active,
            response,
            expected_runtime,
            expected_health,
            expected_os,
            bundle_after_reproof=changed_during_reproof,
        )

    def test_cleanup_exact_checkpoint_stops_each_authenticated_process(self) -> None:
        active, response = runtime_fixture()
        summary = validate_runtime(active, response)
        expected_runtime = runner._evidence_summary(summary)
        expected_health = _health_proof(summary)
        expected_os = _os_proof(summary, expected_health)
        terminal = deepcopy(active)
        for index, process in enumerate(terminal["state"]["processes"]):
            process["state"] = "STOPPED"
            sync_process_owner(terminal, index)
        for index, lease in enumerate(terminal["state"]["port_leases"]):
            lease["status"] = "released"
            sync_lease_owner(terminal, index)
        terminal["live_assignment_processes"] = []
        terminal["live_assignment_leases"] = []

        with tempfile.TemporaryDirectory() as temp_dir:
            evidence_path = (Path(temp_dir) / "evidence.json").resolve()
            config = config_fixture(evidence_path=evidence_path)
            evidence = {
                "status": "verified_awaiting_cleanup",
                "git": {"commit": "expected"},
                "ownership_marker": {"instance_id": "expected"},
                "project": {"project_phone": PROJECT_ID},
                "start_response": deepcopy(response),
                "post_restart": {
                    "runtime": deepcopy(expected_runtime),
                    "child_health": deepcopy(expected_health),
                    "child_os_ownership": deepcopy(expected_os),
                },
                "checks": [],
            }
            reader = Mock()
            reader.read.side_effect = [active, deepcopy(active), terminal]
            events: list[str] = []
            store = Mock()
            store_class = Mock(
                side_effect=lambda _database: (events.append("store"), store)[1]
            )
            reservations = Mock()
            reservations_class = Mock(
                side_effect=lambda: (events.append("reservations"), reservations)[1]
            )
            supervisor = Mock()

            def stop_process(
                _project: str, _sprint: str, process_id: str
            ) -> SimpleNamespace:
                events.append(f"stop:{process_id}")
                return SimpleNamespace(
                    process_id=process_id,
                    state="STOPPED",
                    action="stopped",
                )

            supervisor.stop.side_effect = stop_process
            supervisor_class = Mock(
                side_effect=lambda *_args, **_kwargs: (
                    events.append("supervisor"),
                    supervisor,
                )[1]
            )
            managed_module = ModuleType("nginx_qa.managed_import")
            managed_module.ManagedImportStore = store_class
            managed_module.ManagedPortReservationRegistry = reservations_class
            supervisor_module = ModuleType("nginx_qa.process_supervisor")
            supervisor_module.ManagedProcessSupervisor = supervisor_class
            supervisor_module.ManagedProcessSupervisorError = RuntimeError

            def verify_os_after_service_import(
                _summary: dict, _health: list[dict]
            ) -> list[dict]:
                self.assertIn(str(config.repo_root), sys.path)
                return deepcopy(expected_os)

            def record_evidence_write(_path: Path, payload: dict) -> None:
                if payload.get("status") == "verified_awaiting_cleanup":
                    self.assertIn("pre_cleanup", payload)
                    events.append("pre_cleanup_write")
                else:
                    events.append("final_write")

            with (
                patch.object(runner, "_read_evidence", return_value=evidence),
                patch.object(runner, "_config_from_evidence", return_value=config),
                patch.object(
                    runner, "validate_git_source", return_value=evidence["git"]
                ),
                patch.object(
                    runner,
                    "validate_ownership_marker",
                    return_value=evidence["ownership_marker"],
                ),
                patch.object(runner, "_windows_listener_snapshot", return_value={}),
                patch.object(runner, "ManagedStateReader", return_value=reader),
                patch.object(
                    runner,
                    "verify_child_health",
                    return_value=deepcopy(expected_health),
                ),
                patch.object(
                    runner,
                    "verify_child_os_ownership",
                    side_effect=verify_os_after_service_import,
                ),
                patch.object(
                    runner,
                    "atomic_write_json",
                    side_effect=record_evidence_write,
                ) as evidence_writer,
                patch.dict(
                    sys.modules,
                    {
                        "nginx_qa.managed_import": managed_module,
                        "nginx_qa.process_supervisor": supervisor_module,
                    },
                ),
                patch.object(sys, "path", list(sys.path)),
            ):
                result = runner.cleanup(evidence_path)

            self.assertEqual(result["status"], "cleaned_awaiting_live_after")
            self.assertEqual(
                result["pre_cleanup"]["runtime"],
                expected_runtime,
            )
            self.assertEqual(
                result["pre_cleanup"]["child_health"],
                expected_health,
            )
            self.assertEqual(
                result["pre_cleanup"]["child_os_ownership"],
                expected_os,
            )
            self.assertEqual(
                result["pre_cleanup"]["staging_host_listener"],
                {"port": runner.STAGING_HTTP_PORT, "owners": {}},
            )
            self.assertEqual(events[0], "pre_cleanup_write")
            self.assertLess(events.index("pre_cleanup_write"), events.index("store"))
            self.assertLess(events.index("pre_cleanup_write"), events.index("reservations"))
            self.assertLess(events.index("pre_cleanup_write"), events.index("supervisor"))
            self.assertLess(
                events.index("pre_cleanup_write"),
                min(index for index, event in enumerate(events) if event.startswith("stop:")),
            )
            self.assertEqual(evidence_writer.call_count, 2)
            self.assertEqual(supervisor.stop.call_count, 4)
            self.assertEqual(
                {call.args[2] for call in supervisor.stop.call_args_list},
                {f"process-{index}" for index in range(4)},
            )
            store_class.assert_called_once_with(config.managed_db)
            reservations_class.assert_called_once_with()
            supervisor.close.assert_called_once_with()
            reservations.close_all.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
