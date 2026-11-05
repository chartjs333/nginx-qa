"""Operator self-test ingress into the existing Managed Sprint engine.

Uses the same reservation, preparation, activation, client ACK and cancellation
transactions as Telegram. It creates no scheduler and no synthetic result.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import closing
import hashlib
import json
import sqlite3
from typing import Any

from .telegram_launcher_store import LauncherOperations
from .telegram_launcher_execution import launch_sync, stop_sync
from .telegram_launcher_intake import repository_binding
from .sprint_types import canonical_json_sha256
from .setup_policy import SetupPolicy
from .setup_diagnostics import open_readonly_snapshot


@dataclass(frozen=True)
class OperatorPrincipal:
    actor_id: str
    project_id: str
    actions: frozenset[str] = frozenset({"prepare", "start", "stop"})


class OperatorOperations(LauncherOperations):
    @staticmethod
    def _principal_fields(principal):
        if not isinstance(principal, OperatorPrincipal):
            raise PermissionError("OPERATOR_REQUIRED")
        return {"owner_kind": "setup_operator", "actor_id": principal.actor_id}

    @staticmethod
    def _owner(record, principal):
        if (not isinstance(principal, OperatorPrincipal)
                or record.get("owner_kind") != "setup_operator"
                or record.get("actor_id") != principal.actor_id
                or record.get("project_id") != principal.project_id):
            raise PermissionError("OPERATION_OWNER_MISMATCH")


def reconcile_operator_receipts(runtime):
    """Project terminal native receipts in the existing host maintenance tick.

    No activation, worker dispatch or user impersonation. Cleanup projection must
    survive user revocation; its authority is the engine's already-final state.
    """
    operations = object.__new__(OperatorOperations)
    operations.store = runtime.store
    changed, failed = 0, 0
    with runtime.store._transaction() as connection:
        rows = connection.execute("SELECT * FROM telegram_launcher_operations WHERE holds_slot=1 "
            "AND CASE WHEN json_valid(record_json) THEN json_extract(record_json,'$.owner_kind') ELSE NULL END='setup_operator' LIMIT 100").fetchall()
        for row in rows:
            connection.execute('SAVEPOINT setup_receipt')
            try:
                record = operations._decode(row)
                if not isinstance(record.get("actor_id"), str) or record.get("transport", {}).get("kind") not in {"setup_selftest", "setup_task"}:
                    raise ValueError("PRIVATE_OPERATION_CORRUPT")
                control, _ = runtime.store._load_control(connection, record["project_id"])
                runtime.store._validate_control_snapshot(connection, record["project_id"], control)
                current = operations._refresh(connection, row, control)
                changed += int(current != record or current["lifecycle"] in {"Completed", "Stopped", "Failed"})
            except Exception:
                connection.execute('ROLLBACK TO setup_receipt')
                failed += 1
            finally:
                connection.execute('RELEASE setup_receipt')
    return {"updated": changed, "failed": failed}


class NativeSelfTest:
    def __init__(self, host, policy: SetupPolicy, registry_reader, worker_adapter, store_path, binding_authority=None):
        self.host, self.policy, self.registry_reader = host, policy, registry_reader
        self.worker_adapter, self.store_path = worker_adapter, store_path
        self.binding_authority = binding_authority

    def _project(self, project_id):
        _, _, entry, context = self.host.project_for_group_api(self.registry_reader(), project_id)
        return entry, context, self.host.managed_repository_registry_for_project(entry, context)

    def validate_profile(self, actor, source_project, profile_id, *, policy_data=None):
        policy_data = self.policy.read() if policy_data is None else policy_data
        profile = self.policy.profile(profile_id, data=policy_data)
        target = profile["target_project_id"]
        self.policy.grant(actor, source_project, data=policy_data).require("selftest.start")
        self.policy.grant(actor, target, data=policy_data).require("selftest.start")
        _, _, source_repos = self._project(source_project)
        _, _, target_repos = self._project(target)
        spec = target_repos.get(profile["repository_id"])
        if (target == source_project or spec is None
                or any(x.canonical_remote == spec.canonical_remote for x in source_repos.values())):
            raise ValueError("SELFTEST_REPOSITORY_NOT_ISOLATED")
        return profile, target_repos

    def _authorize(self, actor, source_project, target, action):
        for project in (source_project, target):
            self.policy.grant(actor, project).require("selftest." + action)
        return OperatorPrincipal(actor, target)

    def start(self, *, actor_id, project_id, arguments, operation_id, authority_fingerprint):
        captured_policy = self.policy.read()
        profile, repositories = self.validate_profile(actor_id, project_id, arguments["test_profile_id"], policy_data=captured_policy)
        _, _, captured_source = self._project(project_id)
        target = profile["target_project_id"]
        principal = self._authorize(actor_id, project_id, target, "start")
        def start_version():
            current_profile, current_repositories = self.validate_profile(actor_id, project_id, arguments["test_profile_id"])
            _, _, source_repositories = self._project(project_id)
            return canonical_json_sha256({"policy": self.policy.grant(actor_id, project_id).version,
                "profile": current_profile,
                "source": {key: repository_binding(value) for key, value in source_repositories.items()},
                "target": {key: repository_binding(value) for key, value in current_repositories.items()}})
        starting_version = canonical_json_sha256({"policy": self.policy.grant(actor_id, project_id, data=captured_policy).version,
            "profile": profile, "source": {key: repository_binding(value) for key, value in captured_source.items()},
            "target": {key: repository_binding(value) for key, value in repositories.items()}})
        def authorize_start(p, action, unused):
            current = self._authorize(actor_id, project_id, target, action)
            if (start_version() != starting_version or (self.binding_authority is not None
                    and self.binding_authority(actor_id, project_id) != authority_fingerprint)):
                raise ValueError("START_AUTHORITY_CHANGED")
            return current
        # Probe/bind belongs to the source project and authenticated operator.
        executor = self.worker_adapter.native_executor(actor_id=actor_id, project_id=project_id,
                                                       authority_fingerprint=authority_fingerprint)
        binding = self.worker_adapter.active_binding(actor_id=actor_id, project_id=project_id,
                                                     authority_fingerprint=authority_fingerprint)
        if not binding:
            raise ValueError("WORKER_BINDING_NOT_CURRENT")
        importer = self.host.managed_sprint_importer_factory(self.host.load_managed_runtime_config(), repositories)
        operations = OperatorOperations(importer)
        envelope = {"project_id": target, "autonomous_mode": True, "manifest": profile["manifest"]}
        key = hashlib.sha256(("setup:" + actor_id + ":" + project_id + ":" + operation_id).encode()).hexdigest()
        record = operations.reserve(principal, (key, key), envelope,
            {"kind": "setup_selftest", "source_project_id": project_id, "setup_operation_id": operation_id,
             "executor_binding_id": binding["binding_id"]},
            repository_binding(repositories[profile["repository_id"]]))
        native_id = record["operation_id"]
        intent = {"ref": profile["ref"], "commit": profile["commit"], "manifest_path": profile["manifest_path"],
                  "manifest_sha256": canonical_json_sha256(profile["manifest"])}
        operations.update(native_id, principal, provision_intent=intent, published=True, blocked_reason=None)
        authorize_start(principal, "start", None)
        operations.prepare(native_id, principal)
        return launch_sync(operations, native_id, principal, None, lambda: executor,
                           authorize=authorize_start)

    def _read_record(self, actor, source_project, operation_id):
        if self.store_path is None or not self.store_path.is_file():
            raise LookupError("SELFTEST_NOT_FOUND")
        with closing(open_readonly_snapshot(self.store_path)) as connection:
            connection.row_factory = sqlite3.Row
            if operation_id.startswith("op-"):
                key = hashlib.sha256(("setup:" + actor + ":" + source_project + ":" + operation_id).encode()).hexdigest()
                row = connection.execute("SELECT * FROM telegram_launcher_operations WHERE dedupe_key = ?", (key,)).fetchone()
            else:
                row = connection.execute("SELECT * FROM telegram_launcher_operations WHERE operation_id = ?",
                                         (operation_id,)).fetchone()
            if row is None:
                raise LookupError("SELFTEST_NOT_FOUND")
            record = LauncherOperations._decode(row)
            OperatorOperations._owner(record, OperatorPrincipal(actor, record["project_id"]))
            if record.get("transport", {}).get("source_project_id") != source_project:
                raise PermissionError("OPERATION_OWNER_MISMATCH")
            sprint_id = record.get("sprint_id")
            state = None
            if sprint_id:
                row = connection.execute("SELECT * FROM managed_sprints WHERE project_id = ? AND sprint_id = ?",
                                         (record["project_id"], sprint_id)).fetchone()
                state = json.loads(row["state_json"]) if row else None
                if row:
                    from .managed_import import ManagedImportStore
                    control_row = connection.execute("SELECT control_json FROM managed_projects WHERE project_id = ?",
                                                     (record["project_id"],)).fetchone()
                    if not control_row:
                        raise ValueError("STATE_CORRUPT")
                    control = json.loads(control_row[0])
                    if ManagedImportStore._runtime_row_issues(control, row, state,
                            require_indexed=control.get("active_sprint_id") == sprint_id):
                        raise ValueError("STATE_CORRUPT")
                    LauncherOperations._validate_client_receipts(record, ManagedImportStore.launcher_attempt(control, record["operation_id"]))
        return record, state

    def status(self, actor_id, project_id, operation_id):
        record, state = self._read_record(actor_id, project_id, operation_id)
        self.policy.grant(actor_id, record["project_id"]).require("read")
        return self.receipt_fields(record, state)

    @staticmethod
    def receipt_fields(record, state=None):
        status = state.get("status") if isinstance(state, dict) else None
        result = {"native_operation_id": record["operation_id"], "native_sprint_id": record.get("sprint_id"),
                  "state": "APPLYING", "error_code": record.get("blocked_reason"), "retryable": False,
                  "evidence_ids": [record["operation_id"]]}
        if status == "completed" or record.get("lifecycle") == "Completed":
            result.update(state="SUCCEEDED", error_code=None)
        elif status in {"failed", "blocked"} or record.get("lifecycle") == "Failed":
            result.update(state="FAILED", error_code="ENGINE_TERMINAL_FAILURE")
        elif status == "stopped" or record.get("lifecycle") == "Stopped":
            result.update(state="FAILED", error_code="SELFTEST_STOPPED")
        elif record.get("blocked_reason"):
            result.update(state="BLOCKED", retryable=True)
        return result

    def stop(self, *, actor_id, project_id, arguments, operation_id, authority_fingerprint):
        native_id = arguments["self_test_id"]
        record, _ = self._read_record(actor_id, project_id, native_id)
        target = record["project_id"]
        principal = self._authorize(actor_id, project_id, target, "stop")
        _, _, repositories = self._project(target)
        importer = self.host.managed_sprint_importer_factory(self.host.load_managed_runtime_config(), repositories)
        operations = OperatorOperations(importer)
        from .managed_continuity import ManagedContinuityRuntime
        runtime = ManagedContinuityRuntime(importer, process_supervisor=getattr(self.host, "managed_process_supervisor", None))
        # Original executor/session are verified again by the existing stop protocol.
        def executor():
            return self.worker_adapter.original_executor(record["transport"].get("executor_binding_id"),
                                                          actor_id=actor_id, project_id=project_id)
        record = stop_sync(operations, native_id, principal, None, executor, runtime,
            authorize=lambda p, action, _: self._authorize(actor_id, project_id, target, action))
        fields = self.receipt_fields(record)
        if record["lifecycle"] in {"Stopped", "Completed"}:
            fields.update(state="SUCCEEDED", error_code=None)
        return fields
