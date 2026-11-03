"""Lossless, server-owned cancellation of an existing managed runtime.

Assignments/results/reviews/outbox remain historical facts. Cancellation does
not manufacture task outcomes or review decisions. The small receipt permits
reconstruction and full validation of the exact pre-cancellation snapshot.
"""
from copy import deepcopy

from nginx_qa.sprint_types import canonical_json_sha256


def cancellation_issues(state, validate):
    from nginx_qa.managed_import import _runtime_state_schema_errors
    receipt = state.get("operator_cancellation")
    try:
        if (state["status"] != "stopped" or not isinstance(receipt, dict)
            or set(receipt) != {"operation_id", "stopped_at", "before_sha256", "active_assignment_ids",
                               "allowed_outcomes_by_assignment", "released_branch_lease_ids"}
            or state.get("operator_stop_requested", {}).get("operation_id") != receipt["operation_id"]
            or state["active_assignment_ids"] or state["allowed_outcomes_by_assignment"]
            or any(p["state"] in {"PREPARED", "STARTING", "HEALTHY", "STOPPING"} for p in state["processes"])
            or any(p["status"] in {"reserved", "bound"} for p in state["port_leases"])
            or any(b["status"] != "released" for b in state["branch_leases"])):
            return ("OPERATOR_CANCELLATION_INVALID",)
        before = deepcopy(state)
        before.pop("operator_cancellation")
        before["status"] = "active"
        before["active_assignment_ids"] = receipt["active_assignment_ids"]
        before["allowed_outcomes_by_assignment"] = receipt["allowed_outcomes_by_assignment"]
        released = set(receipt["released_branch_lease_ids"])
        if len(released) != len(receipt["released_branch_lease_ids"]):
            return ("OPERATOR_CANCELLATION_INVALID",)
        for lease in before["branch_leases"]:
            if lease["lease_id"] in released:
                if lease["released_at"] != receipt["stopped_at"]:
                    return ("OPERATOR_CANCELLATION_INVALID",)
                lease.update(status="active", released_at=None)
                released.remove(lease["lease_id"])
        if (released or canonical_json_sha256(before) != receipt["before_sha256"]
            or _runtime_state_schema_errors(before, issue_code="CANCEL_SOURCE_INVALID") or validate(before)):
            return ("OPERATOR_CANCELLATION_SOURCE_INVALID",)
    except (KeyError, TypeError, ValueError):
        return ("OPERATOR_CANCELLATION_INVALID",)
    return ()


def assert_no_live_resources(connection, state, *, include_branches=True):
    """Check normalized ownership too; a JSON-only proof is insufficient."""
    owners = {item["assignment_id"] for item in state.get("assignments", [])}
    owners.update(item["assignment_id"] for item in state.get("review_assignments", []))
    owners.update(item["assignment_id"] for item in state.get("processes", []))
    owners.update(item["assignment_id"] for item in state.get("branch_leases", []))
    for assignment_id in owners:
        queries = [("managed_process_owners", "state IN ('PREPARED','STARTING','HEALTHY','STOPPING')"),
                   ("managed_port_leases", "status IN ('reserved','bound')")]
        if include_branches:
            queries.append(("branch_leases", "status = 'active'"))
        for table, condition in queries:
            if connection.execute(f"SELECT 1 FROM {table} WHERE assignment_id = ? AND {condition}", (assignment_id,)).fetchone():
                raise RuntimeError("owned live resource prevents retirement")


def begin_cancel(store, project_id, sprint_id, operation_id, *, requested_at):
    """Durable fence before the external client and native process stop calls."""
    import json
    with store._transaction() as connection:
        control, revision = store._load_control(connection, project_id)
        store._validate_control_snapshot(connection, project_id, control)
        row = connection.execute("SELECT * FROM managed_sprints WHERE project_id = ? AND sprint_id = ?", (project_id, sprint_id)).fetchone()
        state = json.loads(row["state_json"])
        if store._runtime_row_issues(control, row, state, require_indexed=False):
            raise RuntimeError("invalid cancellation source")
        owner = store.launcher_attempt(control, operation_id)
        if not owner or owner["status"] != "SUCCEEDED" or owner["sprint_id"] != sprint_id:
            raise RuntimeError("only the owning launcher operation may stop this sprint")
        store.assert_launcher_slot_available(connection, project_id, operation_id=operation_id)
        if state["status"] != "active" or state.get("operator_stop_requested"):
            return state
        marker = {"operation_id": operation_id, "requested_at": requested_at}
        state["operator_stop_requested"] = marker
        owner["evidence"]["operator_stop_requested"] = deepcopy(marker)
        store._write_control(connection, control, revision)
        connection.execute("UPDATE managed_sprints SET state_json = ? WHERE project_id = ? AND sprint_id = ?",
                           (json.dumps(state), project_id, sprint_id))
        return state


def finalize_cancel(store, project_id, sprint_id, operation_id, *, stopped_at):
    """Commit cancellation, lease retirement and launcher receipt atomically."""
    import json
    from nginx_qa.branch_leases import BranchLeaseStore
    from nginx_qa.managed_import import _runtime_state_schema_errors
    from nginx_qa.sprint_types import managed_activation_invariant_issues
    with store._transaction() as connection:
        control, revision = store._load_control(connection, project_id)
        store._validate_control_snapshot(connection, project_id, control)
        row = connection.execute("SELECT * FROM managed_sprints WHERE project_id = ? AND sprint_id = ?", (project_id, sprint_id)).fetchone()
        state = json.loads(row["state_json"])
        if store._runtime_row_issues(control, row, state, require_indexed=False):
            raise RuntimeError("invalid cancellation source")
        if state["status"] == "stopped":
            return state
        request = state.get("operator_stop_requested", {})
        owner = store.launcher_attempt(control, operation_id)
        operation_row = connection.execute("SELECT * FROM telegram_launcher_operations WHERE operation_id = ? AND project_id = ?",
                                           (operation_id, project_id)).fetchone()
        operation = json.loads(operation_row["record_json"])
        from nginx_qa.telegram_launcher_store import LauncherOperations
        LauncherOperations._validate_client_receipts(operation, owner)
        if (state["status"] != "active" or request.get("operation_id") != operation_id
            or not owner or owner["status"] != "SUCCEEDED" or owner["sprint_id"] != sprint_id
            or not operation_row["holds_slot"] or operation.get("executor_stop_receipt", {}).get("status") != "STOPPED"):
            raise RuntimeError("cancellation requires owned activation and stopped client receipt")
        if (any(p["state"] in {"PREPARED", "STARTING", "HEALTHY", "STOPPING"} for p in state["processes"])
            or any(p["status"] in {"reserved", "bound"} for p in state["port_leases"])):
            raise RuntimeError("native processes have not stopped")
        assert_no_live_resources(connection, state, include_branches=False)
        receipt = {"operation_id": operation_id, "stopped_at": stopped_at,
                   "before_sha256": canonical_json_sha256(state),
                   "active_assignment_ids": deepcopy(state["active_assignment_ids"]),
                   "allowed_outcomes_by_assignment": deepcopy(state["allowed_outcomes_by_assignment"]),
                   "released_branch_lease_ids": []}
        for lease in state["branch_leases"]:
            if lease["status"] == "active":
                released = BranchLeaseStore.release_in_transaction(connection, lease["lease_id"], lease["assignment_id"],
                    assignment_settled=True, all_processes_stopped=True, released_at=stopped_at)
                receipt["released_branch_lease_ids"].append(lease["lease_id"])
                lease.update(status=released.status, released_at=released.released_at)
        state.update(status="stopped", active_assignment_ids=[], allowed_outcomes_by_assignment={}, operator_cancellation=receipt)
        if _runtime_state_schema_errors(state, issue_code="CANCEL_SCHEMA_INVALID") or managed_activation_invariant_issues(state):
            raise RuntimeError("invalid cancellation result")
        assert_no_live_resources(connection, state)
        owner["evidence"]["operator_cancellation"] = deepcopy(receipt)
        operation.update(lifecycle="Stopped", blocked_reason=None, updated_at=stopped_at)
        store._write_control(connection, control, revision)
        connection.execute("UPDATE managed_sprints SET status = 'stopped', state_json = ? WHERE project_id = ? AND sprint_id = ?",
                           (json.dumps(state), project_id, sprint_id))
        connection.execute("UPDATE telegram_launcher_operations SET holds_slot = 0, record_json = ? WHERE operation_id = ?",
                           (json.dumps(operation), operation_id))
        return state
