import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
import urllib.parse
from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import main

from nginx_qa.branch_leases import BranchLeaseRequest, BranchLeaseStore
from nginx_qa.managed_continuity import (
    ManagedContinuityError,
    ManagedContinuityRuntime,
    RECOVERY_CONFLICT,
    RESULT_CONFLICT,
    REVIEW_CONFLICT,
    _PreparedAssignment,
    _recovery_settles_context,
    _stable_id,
)
from nginx_qa.managed_import import ManagedImportStore, TransactionalSprintImporter
from nginx_qa.sprint_types import (
    canonical_json_bytes,
    managed_activation_invariant_issues,
    managed_blocker_fingerprint,
    managed_occurrence_id,
    recovery_request_fingerprint,
    review_request_fingerprint,
)
from nginx_qa.workspace_manager import ManagedWorkspace, WorkspaceRequest
from tests.test_sprint_type_contract import (
    COMMIT,
    SPRINT_ID,
    TIMESTAMP,
    active_runtime_fixture,
    completed_runtime_fixture,
    prepared_integration_runtime_fixture,
    process_fixture,
    project_control_fixture,
)


class InjectedCrash(RuntimeError):
    pass


class ManagedContinuityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self.temporary.name)
        database = root / "managed.sqlite3"
        self.store = ManagedImportStore(database)
        self.store._ensure_initialized()
        self.branch_leases = BranchLeaseStore(root, database_name=database.name)
        self.branch_leases.ensure_initialized()
        self.initial_state = active_runtime_fixture()
        with sqlite3.connect(database) as connection:
            connection.execute(
                "INSERT INTO managed_projects(project_id,control_json,revision) VALUES(?,?,?)",
                (
                    "project-id",
                    canonical_json_bytes(project_control_fixture()).decode("utf-8"),
                    1,
                ),
            )
            connection.execute(
                "INSERT INTO managed_sprints(project_id,sprint_id,status,fencing_token,state_json) VALUES(?,?,?,?,?)",
                (
                    "project-id",
                    SPRINT_ID,
                    "active",
                    1,
                    canonical_json_bytes(self.initial_state).decode("utf-8"),
                ),
            )
            lease = self.initial_state["branch_leases"][0]
            connection.execute(
                """
                INSERT INTO branch_leases(
                    lease_id,repository_id,repository_key,mirror_storage_key,
                    branch,branch_key,assignment_id,source_commit,
                    initial_head_commit,mode,status,acquired_at,released_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    lease["lease_id"], lease["repository_id"],
                    lease["repository_key"], lease["mirror_storage_key"],
                    lease["branch"], lease["branch"].casefold(),
                    lease["assignment_id"], lease["source_commit"],
                    lease["initial_head_commit"], lease["mode"],
                    lease["status"], lease["acquired_at"], lease["released_at"],
                ),
            )
            connection.commit()
        self.importer = TransactionalSprintImporter(
            self.initial_state["runtime_config"],
            {},
            store=self.store,
            branch_leases=self.branch_leases,
        )
        self.runtime = ManagedContinuityRuntime(
            self.importer,
            result_verifier=lambda *_: nullcontext(),
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def result_request(*, result: str = "Implemented and tested") -> dict:
        return {
            "assignment_id": "assignment-1",
            "status": "DONE",
            "result": result,
            "from_commit": COMMIT,
            "git_commit": COMMIT,
            "git_branch": "agent/example",
        }

    def submit_result(self, **changes: object):
        payload = self.result_request()
        payload.update(changes)
        return self.runtime.submit_if_managed(
            "project-id", "2861", canonical_json_bytes(payload), "result-correlation"
        )

    def replace_state(self, state: dict) -> None:
        with sqlite3.connect(self.store.database_path) as connection:
            connection.execute("DELETE FROM branch_leases")
            for lease in state["branch_leases"]:
                connection.execute(
                    """
                    INSERT INTO branch_leases(
                        lease_id,repository_id,repository_key,mirror_storage_key,
                        branch,branch_key,assignment_id,source_commit,
                        initial_head_commit,mode,status,acquired_at,released_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        lease["lease_id"], lease["repository_id"],
                        lease["repository_key"], lease["mirror_storage_key"],
                        lease["branch"], lease["branch"].casefold(),
                        lease["assignment_id"], lease["source_commit"],
                        lease["initial_head_commit"], lease["mode"],
                        lease["status"], lease["acquired_at"], lease["released_at"],
                    ),
                )
            connection.execute(
                "UPDATE managed_sprints SET status=?, state_json=? "
                "WHERE project_id=? AND sprint_id=?",
                (
                    state["status"],
                    canonical_json_bytes(state).decode("utf-8"),
                    "project-id",
                    SPRINT_ID,
                ),
            )
            connection.commit()

    def configure_parallel_active_sibling(
        self, *, maximum: int = 0
    ) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        definition = state["graph_revisions"][0]["definition"]
        build = definition["nodes"][0]
        lint = deepcopy(build)
        lint.update(
            {
                "id": "lint",
                "agent": {
                    "id": "linter",
                    "name": "Linter",
                    "phone": "2862",
                },
                "tasks": [
                    {
                        "task_id": "LINT-1",
                        "queue": "worker-all",
                        "message": "Lint",
                    }
                ],
                "workspace": {"access": "read"},
            }
        )
        definition["nodes"].insert(1, lint)
        definition["execution"].pop("start_node")
        definition["execution"].update(
            {
                "mode": "parallel",
                "start_nodes": ["build", "lint"],
                "max_rework_cycles": maximum,
            }
        )
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()

        sibling_assignment_id = "assignment-parallel"
        sibling_workspace_id = "workspace-parallel"
        sibling_occurrence_id = managed_occurrence_id(
            SPRINT_ID, 1, "lint", 1, []
        )
        source_assignment = state["assignments"][0]
        sibling_assignment = deepcopy(source_assignment)
        sibling_assignment.update(
            {
                "assignment_id": sibling_assignment_id,
                "occurrence_id": sibling_occurrence_id,
                "node_id": "lint",
                "agent_id": "linter",
                "agent_phone": "2862",
                "workspace_id": sibling_workspace_id,
                "branch_lease_id": None,
            }
        )
        source_workspace = state["workspaces"][0]
        workspace_root = (
            source_workspace["expected_root"].split("/nodes/", 1)[0]
            + f"/nodes/lint/{sibling_assignment_id}"
        )
        sibling_workspace = deepcopy(source_workspace)
        sibling_workspace.update(
            {
                "workspace_id": sibling_workspace_id,
                "node_id": "lint",
                "assignment_id": sibling_assignment_id,
                "expected_root": workspace_root,
                "actual_git_toplevel": workspace_root,
                "actual_git_dir": workspace_root + "/.git",
                "assigned_branch": None,
            }
        )
        state["workflow"].update(
            {
                "execution_mode": "parallel",
                "entry_node_ids": ["build", "lint"],
                "entry_occurrence_ids": [
                    state["workflow"]["entry_occurrence_ids"][0],
                    sibling_occurrence_id,
                ],
            }
        )
        state["workflow"]["occurrences"].append(
            {
                "occurrence_id": sibling_occurrence_id,
                "node_id": "lint",
                "graph_revision": 1,
                "generation": 1,
                "activation_policy": "entry",
                "trigger_token_ids": [],
                "state": "active",
                "assignment_ids": [sibling_assignment_id],
                "created_at": TIMESTAMP,
                "completed_at": None,
            }
        )
        state["assignments"].append(sibling_assignment)
        state["workspaces"].append(sibling_workspace)
        state["active_assignment_ids"].append(sibling_assignment_id)
        state["allowed_outcomes_by_assignment"][sibling_assignment_id] = list(
            sibling_assignment["allowed_outcomes"]
        )
        state["outbox"].append(
            {
                "event_id": "event-assignment-parallel",
                "dedupe_key": (
                    f"enqueue:assignment:{SPRINT_ID}:{sibling_assignment_id}"
                ),
                "event_type": "ASSIGNMENT_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "graph_revision": 1,
                    "assignment_id": sibling_assignment_id,
                    "node_id": "lint",
                    "agent_phone": "2862",
                },
                "status": "delivered",
                "created_at": TIMESTAMP,
                "delivered_at": TIMESTAMP,
                "queue_receipt_id": "queue-receipt-assignment-parallel",
            }
        )
        attempt = state["import_attempts"][0]
        attempt["prepared_artifact_ids"].append(sibling_workspace_id)
        attempt["activation_response"].update(
            {
                "execution_mode": "parallel",
                "initial_assignment_ids": [
                    "assignment-1",
                    sibling_assignment_id,
                ],
            }
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

    def assert_all_parent_cap_settles(self, capped_outcome: str) -> None:
        self.configure_parallel_active_sibling(maximum=0)
        state = self.store.runtime_state("project-id", SPRINT_ID)
        definition = state["graph_revisions"][0]["definition"]
        build = next(node for node in definition["nodes"] if node["id"] == "build")
        lint = next(node for node in definition["nodes"] if node["id"] == "lint")
        join = deepcopy(lint)
        join.update(
            {
                "id": "join",
                "agent": {
                    "id": "joiner",
                    "name": "Joiner",
                    "phone": "2863",
                },
                "tasks": [
                    {
                        "task_id": "JOIN-1",
                        "queue": "worker-all",
                        "message": "Join",
                    }
                ],
                "activation_policy": "all_parents",
                "join_parent_order": ["build", "lint"],
                "workspace": {
                    "access": "read",
                    "join_strategy": "require_same_commit",
                },
                "transitions": {"DONE": "completed"},
            }
        )
        build["transitions"]["DONE"] = "join"
        lint["transitions"]["DONE"] = "join"
        definition["nodes"].insert(2, join)
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        self.submit_result(status=capped_outcome)
        pending = self.store.runtime_state("project-id", SPRINT_ID)
        review = next(
            item
            for item in pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-1"
        )
        self.runtime.submit_if_managed(
            "project-id",
            review["reviewer_phone"],
            canonical_json_bytes(
                {
                    "assignment_id": review["assignment_id"],
                    "status": "REJECT",
                    "feedback": "The all-parent branch exhausted its budget",
                }
            ),
            f"all-parent-cap-{capped_outcome.lower()}",
        )
        self.runtime.submit_if_managed(
            "project-id",
            "2862",
            canonical_json_bytes(
                {
                    "assignment_id": "assignment-parallel",
                    "status": "DONE",
                    "result": "The sibling reached the all-parent join",
                    "from_commit": COMMIT,
                    "git_commit": COMMIT,
                    "git_branch": None,
                }
            ),
            f"all-parent-sibling-{capped_outcome.lower()}",
        )
        sibling_pending = self.store.runtime_state("project-id", SPRINT_ID)
        for sibling_review in [
            item
            for item in sibling_pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-parallel"
        ]:
            self.runtime.submit_if_managed(
                "project-id",
                sibling_review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": sibling_review["assignment_id"],
                        "status": "APPROVE",
                    }
                ),
                f"all-parent-review-{capped_outcome.lower()}",
            )

        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "blocked")
        self.assertEqual(terminal["active_assignment_ids"], [])
        available = [
            token
            for token in terminal["workflow"]["transition_tokens"]
            if token["status"] == "available"
        ]
        self.assertEqual(len(available), 1)
        self.assertEqual(available[0]["target_node_id"], "join")
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

    def configure_terminal_result(self, terminal_status: str) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        definition = state["graph_revisions"][0]["definition"]
        definition["nodes"][0]["transitions"]["DONE"] = "failure-terminal"
        definition["nodes"].append(
            {
                "id": "failure-terminal",
                "type": "terminal",
                "status": terminal_status,
                "message": "Explicit Coordinator settlement required",
            }
        )
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()
        self.replace_state(state)

    @staticmethod
    def add_assignment_coordinator_context(
        state: dict,
        *,
        context_id: str,
        reason_code: str,
    ) -> dict:
        assignment = state["assignments"][0]
        workspace = next(
            item
            for item in state["workspaces"]
            if item["workspace_id"] == assignment["workspace_id"]
        )
        definition = next(
            item["definition"]
            for item in state["graph_revisions"]
            if item["revision"] == assignment["graph_revision"]
        )
        coordinator_node = next(
            item
            for item in definition["nodes"]
            if item["id"] == definition["coordinator"]["node_id"]
        )
        context = {
            "context_id": context_id,
            "graph_revision": assignment["graph_revision"],
            "failure_scope": "recovery",
            "reason_code": reason_code,
            "import_attempt_id": None,
            "failed_assignment_id": assignment["assignment_id"],
            "join_target": None,
            "assigned_branch": workspace["assigned_branch"],
            "source_commit": assignment["source_commit"],
            "result_commit": assignment["result_commit"],
            "diff_summary": {},
            "workspace_status": {},
            "process_records": [],
            "port_records": [],
            "test_evidence_summary": {},
            "normalized_error": {"code": reason_code},
            "reviewer_feedback": [],
        }
        state["coordinator_contexts"].append(context)
        state["outbox"].append(
            {
                "event_id": f"event-{context_id}",
                "dedupe_key": f"enqueue:coordinator:{SPRINT_ID}:{context_id}",
                "event_type": "COORDINATOR_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "context_id": context_id,
                    "coordinator_phone": coordinator_node["agent"]["phone"],
                    "reason_code": reason_code,
                },
                "status": "delivered",
                "created_at": assignment["created_at"],
                "delivered_at": assignment["created_at"],
                "queue_receipt_id": f"queue-receipt-{context_id}",
            }
        )
        return context

    @staticmethod
    def prepared_continuation(
        state: dict,
        *,
        assignment_id: str,
        source_commit: str,
    ) -> _PreparedAssignment:
        source = state["assignments"][0]
        source_workspace = state["workspaces"][0]
        source_lease = state["branch_leases"][0]
        workspace_id = f"workspace-{assignment_id}"
        lease_id = f"branch-lease-{assignment_id}"
        expected_root = (
            source_workspace["expected_root"].rsplit("/", 1)[0]
            + f"/{assignment_id}"
        )
        assignment = {
            **source,
            "assignment_id": assignment_id,
            "source_commit": source_commit,
            "initial_head_commit": source_commit,
            "result_commit": None,
            "outcome": None,
            "status": "active",
            "workspace_id": workspace_id,
            "branch_lease_id": lease_id,
            "created_at": source["created_at"],
            "completed_at": None,
        }
        workspace = {
            **source_workspace,
            "workspace_id": workspace_id,
            "assignment_id": assignment_id,
            "expected_root": expected_root,
            "actual_git_toplevel": expected_root,
            "actual_git_dir": f"{expected_root}/.git",
            "source_commit": source_commit,
            "initial_head_commit": source_commit,
        }
        branch_request = BranchLeaseRequest(
            lease_id=lease_id,
            repository_id=source_lease["repository_id"],
            repository_key=source_lease["repository_key"],
            mirror_storage_key=source_lease["mirror_storage_key"],
            branch=source_lease["branch"],
            assignment_id=assignment_id,
            source_commit=source_commit,
            initial_head_commit=source_commit,
        )
        workspace_request = WorkspaceRequest(
            project_id="project-id",
            sprint_id=SPRINT_ID,
            node_id=source["node_id"],
            assignment_id=assignment_id,
            repository_id=source_lease["repository_id"],
            source_commit=source_commit,
            access="write",
            assigned_branch=source_lease["branch"],
            existing_branch_policy="resume",
        )
        verified_workspace = ManagedWorkspace(
            workspace_id=workspace_id,
            project_id="project-id",
            sprint_id=SPRINT_ID,
            node_id=source["node_id"],
            assignment_id=assignment_id,
            repository_id=source_lease["repository_id"],
            repository_remote=source_lease["repository_key"],
            mirror_storage_key=source_lease["mirror_storage_key"],
            expected_root=Path(expected_root),
            actual_git_toplevel=Path(expected_root),
            actual_git_dir=Path(f"{expected_root}/.git"),
            source_commit=source_commit,
            initial_head_commit=source_commit,
            head_commit=source_commit,
            assigned_branch=source_lease["branch"],
            access="write",
            working_tree_state="clean",
            branch_lease_id=lease_id,
        )
        branch_lease = {
            **source_lease,
            "lease_id": lease_id,
            "assignment_id": assignment_id,
            "source_commit": source_commit,
            "initial_head_commit": source_commit,
            "status": "active",
            "released_at": None,
        }
        return _PreparedAssignment(
            assignment=assignment,
            workspace=workspace,
            workspace_request=workspace_request,
            verified_workspace=verified_workspace,
            branch_request=branch_request,
            branch_lease=branch_lease,
            port_leases=(),
            processes=(),
        )

    def configure_join_conflict_repair(
        self, *, repair_key: str
    ) -> tuple[str, str, dict, Mock]:
        state, trigger_ids = prepared_integration_runtime_fixture()
        integration = state["integrations"][0]
        artifact = state["integration_workspaces"][0]
        integration.update(
            {
                "status": "CONFLICT",
                "normalized_error": {"code": "MERGE_CONFLICT"},
            }
        )
        artifact.update(
            {"artifact_status": "preserved", "working_tree_state": "conflicted"}
        )
        context_id = f"context-join-conflict-{repair_key}"
        workspace_projection = {
            key: artifact[key]
            for key in (
                "workspace_artifact_id",
                "integration_id",
                "expected_root",
                "base_commit",
                "head_commit",
                "artifact_status",
                "working_tree_state",
                "verified_at",
            )
        }
        state["coordinator_contexts"] = [
            {
                "context_id": context_id,
                "graph_revision": 1,
                "failure_scope": "integration",
                "reason_code": "JOIN_MERGE_CONFLICT",
                "import_attempt_id": None,
                "failed_assignment_id": None,
                "join_target": {
                    "target_graph_revision": 1,
                    "target_node_id": "join",
                    "trigger_token_ids": trigger_ids,
                    "integration_id": integration["integration_id"],
                },
                "assigned_branch": None,
                "source_commit": None,
                "result_commit": None,
                "diff_summary": {},
                "workspace_status": workspace_projection,
                "process_records": [],
                "port_records": [],
                "test_evidence_summary": {},
                "normalized_error": {"code": "MERGE_CONFLICT"},
                "reviewer_feedback": [],
            }
        ]
        state["outbox"].append(
            {
                "event_id": f"event-coordinator-{repair_key}",
                "dedupe_key": f"enqueue:coordinator:{SPRINT_ID}:{context_id}",
                "event_type": "COORDINATOR_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "context_id": context_id,
                    "coordinator_phone": "2860",
                    "reason_code": "JOIN_MERGE_CONFLICT",
                },
                "status": "delivered",
                "created_at": integration["created_at"],
                "delivered_at": integration["created_at"],
                "queue_receipt_id": f"queue-receipt-{repair_key}",
            }
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        join = next(
            node
            for node in state["graph_revisions"][0]["definition"]["nodes"]
            if node["id"] == "join"
        )
        repaired_join = json.loads(json.dumps(join))
        repaired_join["workspace"]["join_strategy"] = "require_same_commit"
        content = b"repair artifact\n"
        digest = hashlib.sha256(content).hexdigest()
        repair_request = {
            "expected_revision": 1,
            "repair_source_commit": "e" * 40,
            "idempotency_key": repair_key,
            "patch": {
                "future_nodes": [repaired_join],
                "checksum_metadata": [
                    {"path": "orchestration/README.md", "sha256": digest},
                    {"path": "service.py", "sha256": digest},
                ],
            },
        }
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        provider.assert_commit.return_value = "e" * 40
        provider.read_blob.return_value = content
        return context_id, integration["integration_id"], repair_request, provider

    def test_result_two_approvals_and_terminal_replay_are_idempotent(self) -> None:
        accepted = self.submit_result()
        self.assertEqual(accepted.response["status"], "REVIEWS_PENDING")
        state = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.assertEqual(len(state["review_assignments"]), 2)
        for review in state["review_assignments"]:
            response = self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                "review-correlation",
            )
            self.assertEqual(response.response["status"], "REVIEW_ACCEPTED")
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "completed")
        self.assertEqual(
            terminal["transition_journal"][0]["state"], "TRANSITION_COMMITTED"
        )
        self.assertEqual(managed_activation_invariant_issues(terminal), ())
        self.assertEqual(self.submit_result().response["status"], "ALREADY_ACCEPTED")
        review = terminal["review_assignments"][0]
        replay = self.runtime.submit_if_managed(
            "project-id",
            review["reviewer_phone"],
            canonical_json_bytes(
                {"assignment_id": review["assignment_id"], "status": "APPROVE"}
            ),
            "review-replay",
        )
        self.assertEqual(replay.response["status"], "ALREADY_ACCEPTED")

    def test_changed_result_and_review_replays_conflict(self) -> None:
        self.submit_result()
        with self.assertRaises(ManagedContinuityError) as caught:
            self.submit_result(result="different")
        self.assertEqual(caught.exception.code, RESULT_CONFLICT)
        review = self.store.runtime_state("project-id", SPRINT_ID)[
            "review_assignments"
        ][0]
        self.runtime.submit_if_managed(
            "project-id",
            review["reviewer_phone"],
            canonical_json_bytes(
                {"assignment_id": review["assignment_id"], "status": "APPROVE"}
            ),
            "review-first",
        )
        with self.assertRaises(ManagedContinuityError) as caught:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": review["assignment_id"],
                        "status": "REJECT",
                        "feedback": "Change it",
                    }
                ),
                "review-changed",
            )
        self.assertEqual(caught.exception.code, REVIEW_CONFLICT)

    def test_queue_insert_crash_is_resumed_by_exact_result_replay(self) -> None:
        crashed = False

        def fault(point: str, _context: object) -> None:
            nonlocal crashed
            if point == "after_queue_insert" and not crashed:
                crashed = True
                raise InjectedCrash("after queue insert")

        self.runtime.fault_injector = fault
        with self.assertRaises(InjectedCrash):
            self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertTrue(any(item["status"] == "pending" for item in state["outbox"]))
        self.assertEqual(self.submit_result().response["status"], "ALREADY_ACCEPTED")
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertTrue(all(item["status"] == "delivered" for item in recovered["outbox"]))
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_current_identity_recovers_publication_before_enqueue_and_exposure(
        self,
    ) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        event = state["outbox"][0]
        event.update(
            {
                "status": "pending",
                "delivered_at": None,
                "queue_receipt_id": None,
            }
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)
        order: list[str] = []

        def recover(_state: dict) -> None:
            order.append("publish")

        def enqueue(**_kwargs: object) -> str:
            order.append("enqueue")
            return "queue-receipt-publication-order"

        with (
            patch.object(
                self.runtime,
                "_recover_assignment_publications",
                side_effect=recover,
            ),
            patch.object(
                self.store,
                "enqueue_managed_queue_item",
                side_effect=enqueue,
            ),
        ):
            identity = self.runtime.current_identity("project-id", "2861")
            order.append("expose")

        self.assertEqual(identity["assignment"]["assignment_id"], "assignment-1")
        self.assertLess(order.index("publish"), order.index("enqueue"))
        self.assertLess(order.index("enqueue"), order.index("expose"))

    def test_legacy_identity_message_body_returns_managed_assignment(self) -> None:
        for payload in ({"message": "Кто я?"}, {}):
            with self.subTest(payload=payload):
                result = self.runtime.submit_if_managed(
                    "project-id",
                    "2861",
                    canonical_json_bytes(payload),
                    "managed-identity-message",
                )

                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(result.http_status, 200)
                self.assertTrue(result.response["managed"])
                self.assertEqual(
                    result.response["assignment"]["assignment_id"],
                    "assignment-1",
                )

    def test_any_parent_parallel_occurrences_share_phone_identity_deterministically(
        self,
    ) -> None:
        state, _trigger_ids = prepared_integration_runtime_fixture()
        definition = state["graph_revisions"][0]["definition"]
        join = next(node for node in definition["nodes"] if node["id"] == "join")
        join["activation_policy"] = "any_parent"
        join.pop("join_parent_order")
        join["workspace"].pop("join_strategy")
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()
        state["integrations"] = []
        state["integration_workspaces"] = []
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        def prepare_read_assignment(
            project_id: str,
            candidate: dict,
            *,
            occurrence_id: str,
            node_id: str,
            graph_revision: int,
            source_kind: str,
            source_result_keys: list[str],
            source_commit: str,
            rework_cycle: int = 0,
            integration_id: str | None = None,
            identity_salt: str | None = None,
        ) -> _PreparedAssignment:
            identity_parts: list[object] = [
                SPRINT_ID,
                occurrence_id,
                node_id,
                graph_revision,
                source_kind,
                rework_cycle,
                ",".join(source_result_keys),
            ]
            if identity_salt is not None:
                identity_parts.append(identity_salt)
            assignment_id = _stable_id(
                "assignment", *identity_parts, compact=True
            )
            workspace_id = _stable_id(
                "workspace", project_id, SPRINT_ID, node_id, assignment_id
            )
            workspace_base = candidate["workspaces"][0]["expected_root"].split(
                "/nodes/", 1
            )[0]
            expected_root = (
                f"{workspace_base}/nodes/{node_id}/{assignment_id}"
            )
            repository = candidate["repository"]
            assignment = {
                "assignment_id": assignment_id,
                "occurrence_id": occurrence_id,
                "node_id": node_id,
                "agent_id": "joiner",
                "agent_phone": "2863",
                "graph_revision": graph_revision,
                "source_kind": source_kind,
                "source_result_keys": list(source_result_keys),
                "source_commit": source_commit,
                "initial_head_commit": source_commit,
                "integration_id": integration_id,
                "rework_cycle": rework_cycle,
                "result_commit": None,
                "outcome": None,
                "status": "active",
                "workspace_id": workspace_id,
                "branch_lease_id": None,
                "allowed_outcomes": ["DONE", "STOP", "NEED_DECISION"],
                "created_at": TIMESTAMP,
                "completed_at": None,
            }
            workspace = {
                "workspace_id": workspace_id,
                "project_id": project_id,
                "sprint_id": SPRINT_ID,
                "node_id": node_id,
                "assignment_id": assignment_id,
                "expected_root": expected_root,
                "actual_git_toplevel": expected_root,
                "actual_git_dir": f"{expected_root}/.git",
                "repository_id": repository["repository_id"],
                "repository_remote": repository["canonical_remote"],
                "source_commit": source_commit,
                "initial_head_commit": source_commit,
                "assigned_branch": None,
                "working_tree_state": "clean",
            }
            request = WorkspaceRequest(
                project_id=project_id,
                sprint_id=SPRINT_ID,
                node_id=node_id,
                assignment_id=assignment_id,
                repository_id=repository["repository_id"],
                source_commit=source_commit,
                access="read",
                assigned_branch=None,
                existing_branch_policy=None,
            )
            verified = ManagedWorkspace(
                workspace_id=workspace_id,
                project_id=project_id,
                sprint_id=SPRINT_ID,
                node_id=node_id,
                assignment_id=assignment_id,
                repository_id=repository["repository_id"],
                repository_remote=repository["canonical_remote"],
                mirror_storage_key=repository["mirror_storage_key"],
                expected_root=Path(expected_root),
                actual_git_toplevel=Path(expected_root),
                actual_git_dir=Path(f"{expected_root}/.git"),
                source_commit=source_commit,
                initial_head_commit=source_commit,
                head_commit=source_commit,
                assigned_branch=None,
                access="read",
                working_tree_state="clean",
                branch_lease_id=None,
            )
            return _PreparedAssignment(
                assignment=assignment,
                workspace=workspace,
                workspace_request=request,
                verified_workspace=verified,
                branch_request=None,
                branch_lease=None,
                port_leases=(),
                processes=(),
            )

        with (
            patch.object(
                self.runtime,
                "_prepare_assignment",
                side_effect=prepare_read_assignment,
            ),
            patch.object(self.runtime, "_publish_prepared_assignment"),
        ):
            first = self.store.runtime_state("project-id", SPRINT_ID)
            self.assertTrue(
                self.runtime._schedule_one("project-id", SPRINT_ID, first)
            )
            second = self.store.runtime_state("project-id", SPRINT_ID)
            self.assertTrue(
                self.runtime._schedule_one("project-id", SPRINT_ID, second)
            )

        scheduled = self.store.runtime_state("project-id", SPRINT_ID)
        live = [
            assignment
            for assignment in scheduled["assignments"]
            if assignment["node_id"] == "join"
            and assignment["status"] == "active"
        ]
        self.assertEqual(len(live), 2)
        self.assertEqual({item["agent_phone"] for item in live}, {"2863"})
        self.assertEqual(managed_activation_invariant_issues(scheduled), ())

        identity = self.runtime.submit_if_managed(
            "project-id",
            "2863",
            canonical_json_bytes({"message": "Кто я?"}),
            "parallel-identity",
        )
        self.assertIsNotNone(identity)
        assert identity is not None
        self.assertEqual(
            identity.response["assignment"]["assignment_id"],
            min(item["assignment_id"] for item in live),
        )

    def test_identity_rediscovery_selects_remaining_same_phone_binding(
        self,
    ) -> None:
        bindings, corrupt, project_wide = self.runtime._project_binding_discovery(
            "project-id"
        )
        self.assertEqual(corrupt, ())
        self.assertFalse(project_wide)
        first = next(
            binding
            for binding in bindings
            if binding.record.get("assignment_id") == "assignment-1"
        )
        second_record = deepcopy(first.record)
        second_record.update(
            {
                "assignment_id": "assignment-2",
                "created_at": "2999-01-01T00:00:00+00:00",
            }
        )
        second = type(first)(
            first.project_id,
            first.sprint_id,
            deepcopy(first.state),
            first.kind,
            second_record,
        )
        settled_record = deepcopy(first.record)
        settled_record["status"] = "completed"
        settled = type(first)(
            first.project_id,
            first.sprint_id,
            deepcopy(first.state),
            first.kind,
            settled_record,
        )

        with (
            patch.object(
                self.runtime,
                "_project_binding_discovery",
                side_effect=[
                    ((first, second), (), False),
                    ((second,), (), False),
                ],
            ),
            patch.object(
                self.runtime,
                "_publish_binding",
                side_effect=[settled, second],
            ) as publish,
        ):
            identity = self.runtime.current_identity("project-id", "2861")

        self.assertIsNotNone(identity)
        assert identity is not None
        self.assertEqual(identity["assignment"]["assignment_id"], "assignment-2")
        self.assertEqual(publish.call_count, 2)

    def test_unmatched_legacy_identity_does_not_drain_managed_outbox(self) -> None:
        with patch.object(
            self.runtime,
            "drain_outbox",
            side_effect=AssertionError(
                "unrelated legacy lookup must have no managed side effects"
            ),
        ):
            self.assertIsNone(
                self.runtime.current_identity("project-id", "9999")
            )
            self.assertIsNone(
                self.runtime.submit_if_managed(
                    "project-id",
                    "9999",
                    canonical_json_bytes(
                        {"assignment_id": "legacy-assignment"}
                    ),
                    "legacy-fallback",
                )
            )

    def test_corrupt_target_binding_fails_closed_but_unrelated_phone_falls_through(
        self,
    ) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        state["outbox"] = [
            event
            for event in state["outbox"]
            if event.get("payload", {}).get("assignment_id") != "assignment-1"
        ]
        self.assertIn(
            "ASSIGNMENT_OUTBOX_COVERAGE_INVALID",
            managed_activation_invariant_issues(state),
        )
        self.replace_state(state)

        for body in (
            canonical_json_bytes({"message": "Кто я?"}),
            canonical_json_bytes(self.result_request()),
        ):
            with self.subTest(body=body), self.assertRaises(RuntimeError):
                self.runtime.submit_if_managed(
                    "project-id", "2861", body, "corrupt-managed-binding"
                )

        self.assertIsNone(
            self.runtime.submit_if_managed(
                "project-id",
                "9999",
                canonical_json_bytes({"message": "Кто я?"}),
                "unrelated-legacy-identity",
            )
        )
        self.assertIsNone(
            self.runtime.submit_if_managed(
                "project-id",
                "9999",
                canonical_json_bytes(
                    {"assignment_id": "legacy-assignment"}
                ),
                "unrelated-legacy-result",
            )
        )

    def test_corrupt_binding_is_recovered_from_independent_witnesses(self) -> None:
        cases = (
            (
                "assignment-record-missing",
                lambda state: state.update({"assignments": []}),
            ),
            (
                "assignment-phone-forged",
                lambda state: state["assignments"][0].update(
                    {"agent_phone": "9999"}
                ),
            ),
            (
                "assignment-status-forged",
                lambda state: state["assignments"][0].update(
                    {"status": "completed"}
                ),
            ),
        )
        for label, corrupt in cases:
            with self.subTest(label=label):
                state = deepcopy(self.initial_state)
                corrupt(state)
                self.assertTrue(managed_activation_invariant_issues(state))
                self.replace_state(state)

                for body in (
                    canonical_json_bytes({"message": "Кто я?"}),
                    canonical_json_bytes(self.result_request()),
                ):
                    with self.assertRaises(RuntimeError):
                        self.runtime.submit_if_managed(
                            "project-id",
                            "2861",
                            body,
                            f"corrupt-independent-{label}",
                        )

                # The forged record phone is not authoritative when the
                # frozen graph and enqueue record agree on the real owner.
                self.assertIsNone(
                    self.runtime.submit_if_managed(
                        "project-id",
                        "9999",
                        canonical_json_bytes({"message": "Кто я?"}),
                        f"unrelated-independent-{label}",
                    )
                )

    def test_unknown_or_missing_binding_status_is_conservatively_live(self) -> None:
        cases = (
            (
                "assignment-status-missing",
                "assignments",
                "agent_phone",
                lambda record: record.pop("status"),
            ),
            (
                "review-status-unknown",
                "review_assignments",
                "reviewer_phone",
                lambda record: record.update({"status": "unknown"}),
            ),
        )
        for label, collection, phone_key, corrupt in cases:
            with self.subTest(label=label):
                state = completed_runtime_fixture()
                record = state[collection][0]
                phone = record[phone_key]
                corrupt(record)
                self.assertTrue(managed_activation_invariant_issues(state))
                self.replace_state(state)

                with self.assertRaises(RuntimeError):
                    self.runtime.submit_if_managed(
                        "project-id",
                        phone,
                        canonical_json_bytes({"message": "Кто я?"}),
                        f"corrupt-unknown-status-{label}",
                    )

                self.assertIsNone(
                    self.runtime.submit_if_managed(
                        "project-id",
                        "9999",
                        canonical_json_bytes({"message": "Кто я?"}),
                        f"unrelated-unknown-status-{label}",
                    )
                )

    def test_pending_outbox_without_owner_record_is_live_binding_evidence(
        self,
    ) -> None:
        cases = (
            (
                "assignment",
                "ASSIGNMENT_ENQUEUE",
                "2877",
                {
                    "sprint_id": SPRINT_ID,
                    "graph_revision": 1,
                    "assignment_id": "missing-assignment",
                    "node_id": "build",
                    "agent_phone": "2877",
                },
            ),
            (
                "review",
                "REVIEW_ENQUEUE",
                "2878",
                {
                    "sprint_id": SPRINT_ID,
                    "source_assignment_id": "assignment-1",
                    "review_assignment_id": "missing-review",
                    "result_key": "result-" + "0" * 64,
                    "reviewer_phone": "2878",
                    "reviewer_index": 1,
                },
            ),
        )
        for label, event_type, phone, payload in cases:
            with self.subTest(label=label):
                state = deepcopy(self.initial_state)
                state["outbox"].append(
                    {
                        "event_id": f"event-missing-{label}",
                        "dedupe_key": f"enqueue:{label}:{SPRINT_ID}:missing",
                        "event_type": event_type,
                        "payload": payload,
                        "status": "pending",
                        "created_at": TIMESTAMP,
                        "delivered_at": None,
                        "queue_receipt_id": None,
                    }
                )
                self.assertTrue(managed_activation_invariant_issues(state))
                self.replace_state(state)

                with self.assertRaises(RuntimeError):
                    self.runtime.current_identity("project-id", phone)
                self.assertIsNone(
                    self.runtime.current_identity("project-id", "9999")
                )

    def test_unknown_or_missing_outbox_status_is_conservatively_live(self) -> None:
        cases = (
            ("assignment-status-missing", "ASSIGNMENT_ENQUEUE", None),
            ("review-status-unknown", "REVIEW_ENQUEUE", "unknown"),
        )
        for label, event_type, corrupt_status in cases:
            with self.subTest(label=label):
                state = completed_runtime_fixture()
                event = next(
                    item
                    for item in state["outbox"]
                    if item["event_type"] == event_type
                )
                phone = str(
                    event["payload"].get("agent_phone")
                    or event["payload"].get("reviewer_phone")
                )
                if corrupt_status is None:
                    event.pop("status")
                else:
                    event["status"] = corrupt_status
                self.replace_state(state)

                with self.assertRaises(RuntimeError):
                    self.runtime.current_identity("project-id", phone)
                self.assertIsNone(
                    self.runtime.current_identity("project-id", "9999")
                )

    def test_unparseable_corrupt_runtime_fails_closed_project_wide(self) -> None:
        with sqlite3.connect(self.store.database_path) as connection:
            connection.execute(
                "UPDATE managed_sprints SET state_json = ? "
                "WHERE project_id = ? AND sprint_id = ?",
                ("{", "project-id", SPRINT_ID),
            )
            connection.commit()

        for phone, body in (
            ("2861", canonical_json_bytes({"message": "Кто я?"})),
            ("2861", canonical_json_bytes(self.result_request())),
            ("9999", canonical_json_bytes({"message": "Кто я?"})),
        ):
            with self.subTest(phone=phone, body=body), self.assertRaises(
                RuntimeError
            ):
                self.runtime.submit_if_managed(
                    "project-id", phone, body, "opaque-managed-runtime"
                )

    def test_opaque_ownership_record_fails_closed_project_wide(self) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        state["assignments"].append("opaque-owner")
        self.assertTrue(managed_activation_invariant_issues(state))
        self.replace_state(state)

        with self.assertRaises(RuntimeError):
            self.runtime.current_identity("project-id", "9999")

    def test_corrupt_review_binding_is_recovered_from_outbox(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        review = deepcopy(state["review_assignments"][0])
        state["review_assignments"] = [
            item
            for item in state["review_assignments"]
            if item["assignment_id"] != review["assignment_id"]
        ]
        self.assertTrue(managed_activation_invariant_issues(state))
        self.replace_state(state)

        for body in (
            canonical_json_bytes({"message": "Кто я?"}),
            canonical_json_bytes(
                {"assignment_id": review["assignment_id"], "status": "APPROVE"}
            ),
        ):
            with self.assertRaises(RuntimeError):
                self.runtime.submit_if_managed(
                    "project-id",
                    review["reviewer_phone"],
                    body,
                    "corrupt-review-binding",
                )
        self.assertIsNone(
            self.runtime.submit_if_managed(
                "project-id",
                "9999",
                canonical_json_bytes({"message": "Кто я?"}),
                "unrelated-corrupt-review",
            )
        )

    def test_corrupt_coordinator_binding_is_recovered_from_outbox(self) -> None:
        self.configure_terminal_result("BLOCKED_EXTERNAL")
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        for review in state["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                "coordinator-corrupt-review",
            )

        state = self.store.runtime_state("project-id", SPRINT_ID)
        context_id = state["coordinator_contexts"][0]["context_id"]
        state["coordinator_contexts"] = []
        self.assertTrue(managed_activation_invariant_issues(state))
        self.replace_state(state)

        for body in (
            canonical_json_bytes({"message": "Кто я?"}),
            canonical_json_bytes({"assignment_id": context_id}),
        ):
            with self.assertRaises(RuntimeError):
                self.runtime.submit_if_managed(
                    "project-id", "2860", body, "corrupt-coordinator-binding"
                )
        self.assertIsNone(
            self.runtime.submit_if_managed(
                "project-id",
                "9999",
                canonical_json_bytes({"message": "Кто я?"}),
                "unrelated-corrupt-coordinator",
            )
        )

    def test_retry_handoff_freezes_exact_effect_ids_and_replays(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        result_key = state["result_receipts"][0]["result_key"]
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-handoff",
            reason_code="HANDOFF_DELIVERY_FAILED",
        )
        relevant_event_ids = [
            event["event_id"]
            for event in state["outbox"]
            if event.get("payload", {}).get("result_key") == result_key
            or event["event_id"]
            in state["transition_journal"][0]["outbox_event_ids"]
        ]
        self.assertTrue(relevant_event_ids)
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "retry-handoff-1",
            "action": "RETRY_HANDOFF",
            "parameters": {"result_key": result_key},
        }

        completed = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "retry-handoff-first",
        )
        expected_ids = [result_key, *relevant_event_ids]
        self.assertEqual(completed.response["produced_record_ids"], expected_ids)
        self.assertFalse(completed.response["deduplicated"])
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(
            recovered["recovery_records"][0]["produced_record_ids"],
            expected_ids,
        )
        self.assertEqual(managed_activation_invariant_issues(recovered), ())
        incompatible = deepcopy(recovered)
        incompatible["coordinator_contexts"][0]["reason_code"] = (
            "SUCCESSOR_PREPARATION_FAILED"
        )
        self.assertIn(
            "RECOVERY_BINDING_INVALID",
            managed_activation_invariant_issues(incompatible),
        )

        replay = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "retry-handoff-replay",
        )
        self.assertEqual(replay.response["produced_record_ids"], expected_ids)
        self.assertTrue(replay.response["deduplicated"])

    def test_pending_recovery_excludes_competing_key_and_action(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        result_key = state["result_receipts"][0]["result_key"]
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-recovery-decision-fence",
            reason_code="HANDOFF_DELIVERY_FAILED",
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)
        first_request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "recovery-decision-first",
            "action": "RETRY_HANDOFF",
            "parameters": {"result_key": result_key},
        }

        def fault(point: str, _context: dict) -> None:
            if point == "after_recovery_pending":
                raise InjectedCrash("after recovery pending")

        self.runtime.fault_injector = fault
        with self.assertRaises(InjectedCrash):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(first_request),
                "recovery-decision-first-crash",
            )
        self.runtime.fault_injector = None
        pending = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(len(pending["recovery_records"]), 1)
        self.assertEqual(pending["recovery_records"][0]["status"], "pending")

        competing = {
            "assignment_id": context["context_id"],
            "idempotency_key": "recovery-decision-second",
            "action": "BLOCK_EXTERNAL",
            "parameters": {
                "reason_code": "HANDOFF_DELIVERY_FAILED",
                "operator_action": "Choose a different recovery effect",
            },
        }
        with self.assertRaises(ManagedContinuityError) as conflict:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(competing),
                "recovery-decision-second-conflict",
            )
        self.assertEqual(conflict.exception.code, RECOVERY_CONFLICT)
        unchanged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(len(unchanged["recovery_records"]), 1)
        self.assertEqual(unchanged["graph_revision"], 1)
        self.assertEqual(unchanged["repairs"], [])

        completed = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(first_request),
            "recovery-decision-first-resume",
        )
        self.assertEqual(completed.response["status"], "RECOVERY_COMPLETED")
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(recovered["recovery_records"][0]["status"], "completed")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_unrelated_failed_recovery_code_does_not_settle_context(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        result_key = state["result_receipts"][0]["result_key"]
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-unrelated-cap-error",
            reason_code="HANDOFF_DELIVERY_FAILED",
        )
        self.replace_state(state)
        first_request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "unrelated-cap-error-first",
            "action": "RETRY_HANDOFF",
            "parameters": {"result_key": result_key},
        }

        def fault(point: str, _context: dict) -> None:
            if point == "after_recovery_pending":
                raise InjectedCrash("after recovery pending")

        self.runtime.fault_injector = fault
        with self.assertRaises(InjectedCrash):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(first_request),
                "unrelated-cap-error-first",
            )
        self.runtime.fault_injector = None
        failed = self.store.runtime_state("project-id", SPRINT_ID)
        failed["recovery_records"][0].update(
            {
                "normalized_error": {
                    "code": "REWORK_LIMIT_EXCEEDED",
                    "http_status": 409,
                },
                "status": "failed",
                "completed_at": TIMESTAMP,
            }
        )
        self.assertEqual(managed_activation_invariant_issues(failed), ())
        self.replace_state(failed)
        identity = self.runtime.current_identity("project-id", "2860")
        self.assertEqual(
            identity["assignment"]["context_id"], context["context_id"]
        )

        retry = deepcopy(first_request)
        retry["idempotency_key"] = "unrelated-cap-error-second"
        completed = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(retry),
            "unrelated-cap-error-second",
        )
        self.assertEqual(completed.response["status"], "RECOVERY_COMPLETED")
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(
            [item["status"] for item in recovered["recovery_records"]],
            ["failed", "completed"],
        )
        self.assertNotIn(
            "RECOVERY_CONTEXT_COMPLETION_DUPLICATE",
            managed_activation_invariant_issues(recovered),
        )
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_orphan_route_rework_cap_claim_is_not_a_settled_choice(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        result_key = state["result_receipts"][0]["result_key"]
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-orphan-route-cap",
            reason_code="REWORK_ROUTING_FAILED",
        )
        parameters = {
            "rejected_result_key": result_key,
            "feedback": "This failure has no durable cap evidence",
        }
        fingerprint = recovery_request_fingerprint(
            context["context_id"],
            1,
            "ROUTE_REWORK",
            None,
            "assignment-1",
            parameters,
        )
        orphan = {
            "recovery_id": "recovery-orphan-route-cap",
            "coordinator_id": "coordinator",
            "context_id": context["context_id"],
            "graph_revision": 1,
            "idempotency_key": "orphan-route-cap",
            "request_fingerprint": fingerprint,
            "action": "ROUTE_REWORK",
            "parameters": parameters,
            "import_attempt_id": None,
            "assignment_id": "assignment-1",
            "join_target": None,
            "produced_record_ids": [],
            "response": None,
            "normalized_error": {
                "code": "REWORK_LIMIT_EXCEEDED",
                "http_status": 409,
            },
            "evidence": {},
            "status": "failed",
            "created_at": TIMESTAMP,
            "completed_at": TIMESTAMP,
        }
        state["recovery_records"].append(orphan)
        self.assertFalse(_recovery_settles_context(state, orphan))
        state["schema_version"] = 2
        self.assertIn(
            "RECOVERY_BINDING_INVALID",
            managed_activation_invariant_issues(state),
        )

    def test_handoff_context_remains_resolvable_after_terminal_completion(
        self,
    ) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        result_key = state["result_receipts"][0]["result_key"]
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-terminal-handoff",
            reason_code="HANDOFF_DELIVERY_FAILED",
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        for review in state["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": review["assignment_id"],
                        "status": "APPROVE",
                    }
                ),
                f"terminal-handoff-review-{review['reviewer_index']}",
            )

        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "completed")
        terminal["schema_version"] = 2
        self.replace_state(terminal)
        identity = self.runtime.current_identity("project-id", "2860")
        self.assertIsNotNone(identity)
        assert identity is not None
        self.assertEqual(
            identity["assignment"]["assignment_id"], context["context_id"]
        )

        with self.assertRaises(ManagedContinuityError) as terminal_mutation:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(
                    {
                        "assignment_id": context["context_id"],
                        "idempotency_key": "terminal-handoff-block",
                        "action": "BLOCK_EXTERNAL",
                        "parameters": {
                            "reason_code": "HANDOFF_DELIVERY_FAILED",
                            "operator_action": "Do not mutate terminal workflow",
                        },
                    }
                ),
                "terminal-handoff-block",
            )
        self.assertEqual(terminal_mutation.exception.code, RECOVERY_CONFLICT)
        unchanged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(unchanged["recovery_records"], [])

        completed = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(
                {
                    "assignment_id": context["context_id"],
                    "idempotency_key": "terminal-handoff-retry",
                    "action": "RETRY_HANDOFF",
                    "parameters": {"result_key": result_key},
                }
            ),
            "terminal-handoff-retry",
        )
        self.assertEqual(completed.response["status"], "RECOVERY_COMPLETED")
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(recovered["status"], "completed")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())
        self.assertIsNone(self.runtime.current_identity("project-id", "2860"))

    def test_failed_recovery_replays_exact_error_code_and_http_status(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        result_key = state["result_receipts"][0]["result_key"]
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-failed-handoff",
            reason_code="HANDOFF_DELIVERY_FAILED",
        )
        result_events = [
            event
            for event in state["outbox"]
            if event.get("payload", {}).get("result_key") == result_key
        ]
        self.assertTrue(result_events)
        result_events[0].update(
            {
                "status": "pending",
                "delivered_at": None,
                "queue_receipt_id": None,
            }
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "failed-handoff-1",
            "action": "RETRY_HANDOFF",
            "parameters": {"result_key": result_key},
        }

        with patch.object(self.runtime, "drain_outbox", return_value=0):
            with self.assertRaises(ManagedContinuityError) as first:
                self.runtime.submit_if_managed(
                    "project-id",
                    "2860",
                    canonical_json_bytes(request),
                    "failed-handoff-first",
                )
            self.assertEqual(first.exception.code, RECOVERY_CONFLICT)
            self.assertEqual(first.exception.http_status, 409)
            failed = self.store.runtime_state("project-id", SPRINT_ID)
            self.assertEqual(
                failed["recovery_records"][0]["normalized_error"],
                {"code": RECOVERY_CONFLICT, "http_status": 409},
            )
            self.assertEqual(managed_activation_invariant_issues(failed), ())

        with patch.object(
            self.runtime,
            "drain_outbox",
            side_effect=RuntimeError("queue unavailable during failed replay"),
        ):
            with self.assertRaises(ManagedContinuityError) as replay:
                self.runtime.submit_if_managed(
                    "project-id",
                    "2860",
                    canonical_json_bytes(request),
                    "failed-handoff-replay",
                )
        self.assertEqual(replay.exception.code, RECOVERY_CONFLICT)
        self.assertEqual(replay.exception.http_status, 409)

    def test_route_rework_freezes_exact_rework_assignment_and_enqueue_ids(
        self,
    ) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        source = state["assignments"][0]
        source_workspace = state["workspaces"][0]
        source_lease = state["branch_leases"][0]
        result_key = state["result_receipts"][0]["result_key"]
        feedback = "Route the rejected result through a replacement assignment"
        review_assignment = state["review_assignments"][0]
        decided_at = review_assignment["activated_at"]
        request_fingerprint = review_request_fingerprint(
            review_assignment["assignment_id"], "REJECT", feedback
        )
        review_response = {
            "assignment_id": review_assignment["assignment_id"],
            "source_assignment_id": source["assignment_id"],
            "result_key": result_key,
            "decision": "REJECT",
            "status": "REWORK_ENQUEUED",
            "deduplicated": False,
        }
        review_assignment.update(
            {
                "status": "decided",
                "decision": "REJECT",
                "request_fingerprint": request_fingerprint,
                "response": json.loads(json.dumps(review_response)),
                "decided_at": decided_at,
            }
        )
        state["reviews"].append(
            {
                "assignment_id": review_assignment["assignment_id"],
                "source_assignment_id": source["assignment_id"],
                "result_key": result_key,
                "result_commit": review_assignment["result_commit"],
                "result_outcome": review_assignment["result_outcome"],
                "reviewer_id": review_assignment["reviewer_id"],
                "reviewer_index": review_assignment["reviewer_index"],
                "decision": "REJECT",
                "feedback": feedback,
                "request_fingerprint": request_fingerprint,
                "response": json.loads(json.dumps(review_response)),
                "decided_at": decided_at,
            }
        )
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-route-rework",
            reason_code="REWORK_ROUTING_FAILED",
        )
        context["failure_scope"] = "review"
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        assignment_id = "assignment-route-rework"
        workspace_id = "workspace-route-rework"
        lease_id = "branch-lease-route-rework"
        expected_root = (
            source_workspace["expected_root"].rsplit("/", 1)[0]
            + f"/{assignment_id}"
        )
        assignment = {
            **source,
            "assignment_id": assignment_id,
            "source_kind": "rework_result",
            "source_result_keys": [result_key],
            "source_commit": COMMIT,
            "initial_head_commit": COMMIT,
            "rework_cycle": 1,
            "result_commit": None,
            "outcome": None,
            "status": "active",
            "workspace_id": workspace_id,
            "branch_lease_id": lease_id,
            "completed_at": None,
        }
        workspace = {
            **source_workspace,
            "workspace_id": workspace_id,
            "assignment_id": assignment_id,
            "expected_root": expected_root,
            "actual_git_toplevel": expected_root,
            "actual_git_dir": f"{expected_root}/.git",
            "source_commit": COMMIT,
            "initial_head_commit": COMMIT,
        }
        branch_request = BranchLeaseRequest(
            lease_id=lease_id,
            repository_id=source_lease["repository_id"],
            repository_key=source_lease["repository_key"],
            mirror_storage_key=source_lease["mirror_storage_key"],
            branch=source_lease["branch"],
            assignment_id=assignment_id,
            source_commit=COMMIT,
            initial_head_commit=COMMIT,
        )
        workspace_request = WorkspaceRequest(
            project_id="project-id",
            sprint_id=SPRINT_ID,
            node_id=source["node_id"],
            assignment_id=assignment_id,
            repository_id=source_lease["repository_id"],
            source_commit=COMMIT,
            access="write",
            assigned_branch=source_lease["branch"],
            existing_branch_policy="resume",
        )
        verified_workspace = ManagedWorkspace(
            workspace_id=workspace_id,
            project_id="project-id",
            sprint_id=SPRINT_ID,
            node_id=source["node_id"],
            assignment_id=assignment_id,
            repository_id=source_lease["repository_id"],
            repository_remote=source_lease["repository_key"],
            mirror_storage_key=source_lease["mirror_storage_key"],
            expected_root=Path(expected_root),
            actual_git_toplevel=Path(expected_root),
            actual_git_dir=Path(f"{expected_root}/.git"),
            source_commit=COMMIT,
            initial_head_commit=COMMIT,
            head_commit=COMMIT,
            assigned_branch=source_lease["branch"],
            access="write",
            working_tree_state="clean",
            branch_lease_id=lease_id,
        )
        branch_lease = {
            **source_lease,
            "lease_id": lease_id,
            "assignment_id": assignment_id,
            "source_commit": COMMIT,
            "initial_head_commit": COMMIT,
            "status": "active",
            "released_at": None,
        }
        prepared = _PreparedAssignment(
            assignment=assignment,
            workspace=workspace,
            workspace_request=workspace_request,
            verified_workspace=verified_workspace,
            branch_request=branch_request,
            branch_lease=branch_lease,
            port_leases=(),
            processes=(),
        )
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        workspace_manager = Mock(unsafe=True)
        workspace_manager.publish_write_workspace.return_value = verified_workspace
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "route-rework-1",
            "action": "ROUTE_REWORK",
            "parameters": {
                "rejected_result_key": result_key,
                "feedback": feedback,
            },
        }
        with (
            patch.object(self.runtime, "_prepare_assignment", return_value=prepared),
            patch.object(self.runtime, "_publish_prepared_assignment"),
            patch.object(
                self.runtime, "_recover_assignment_publications", return_value=0
            ),
            patch.object(
                self.importer,
                "provider_for_durable_repository",
                return_value=provider,
            ),
            patch.object(
                self.importer,
                "_workspace_manager",
                workspace_manager,
            ),
        ):
            completed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "route-rework-first",
            )

        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        rework = recovered["reworks"][0]
        successor = next(
            item
            for item in recovered["assignments"]
            if item["assignment_id"] == assignment_id
        )
        enqueue = next(
            item
            for item in recovered["outbox"]
            if item.get("payload", {}).get("assignment_id") == assignment_id
        )
        self.assertEqual(
            completed.response["produced_record_ids"],
            [rework["rework_id"], successor["assignment_id"], enqueue["event_id"]],
        )
        self.assertEqual(rework["feedback"], feedback)
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_continue_node_settles_source_and_enqueues_exact_successor(
        self,
    ) -> None:
        self.configure_terminal_result("BLOCKED_EXTERNAL")
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        for review in state["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                "review-continue-node",
            )

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        source = staged["assignments"][0]
        context = staged["coordinator_contexts"][0]
        prepared = self.prepared_continuation(
            staged,
            assignment_id="assignment-continue-node",
            source_commit=COMMIT,
        )
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        workspace_manager = Mock(unsafe=True)
        workspace_manager.publish_write_workspace.return_value = (
            prepared.verified_workspace
        )
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "continue-node-1",
            "action": "CONTINUE_NODE",
            "parameters": {
                "node_id": source["node_id"],
                "source_commit": COMMIT,
            },
        }
        with (
            patch.object(self.runtime, "_prepare_assignment", return_value=prepared),
            patch.object(self.runtime, "_publish_prepared_assignment"),
            patch.object(
                self.runtime, "_recover_assignment_publications", return_value=0
            ),
            patch.object(
                self.importer,
                "provider_for_durable_repository",
                return_value=provider,
            ),
            patch.object(
                self.importer,
                "_workspace_manager",
                workspace_manager,
            ),
        ):
            completed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "continue-node-first",
            )
            replay = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "continue-node-replay",
            )

        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        successor = next(
            item
            for item in recovered["assignments"]
            if item["assignment_id"] == prepared.assignment["assignment_id"]
        )
        enqueue = next(
            item
            for item in recovered["outbox"]
            if item.get("payload", {}).get("assignment_id")
            == successor["assignment_id"]
        )
        source_after = next(
            item
            for item in recovered["assignments"]
            if item["assignment_id"] == source["assignment_id"]
        )
        occurrence = next(
            item
            for item in recovered["workflow"]["occurrences"]
            if item["occurrence_id"] == source["occurrence_id"]
        )
        journal = recovered["transition_journal"][0]
        self.assertEqual(
            completed.response["produced_record_ids"],
            [successor["assignment_id"], enqueue["event_id"]],
        )
        self.assertFalse(completed.response["deduplicated"])
        self.assertTrue(replay.response["deduplicated"])
        self.assertEqual(source_after["status"], "completed")
        self.assertEqual(successor["status"], "active")
        self.assertEqual(occurrence["state"], "active")
        self.assertEqual(journal["state"], "REVIEWS_ACCEPTED")
        self.assertEqual(journal["disposition"], "accepted")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_dirty_result_routes_to_coordinator_and_continues_without_receipt(
        self,
    ) -> None:
        class DirtyVerification:
            def __enter__(self):
                raise ManagedContinuityError(
                    "WORKSPACE_DIRTY", 409, "result-verification"
                )

            def __exit__(self, *_args: object) -> None:
                return None

        self.runtime.result_verifier = lambda *_args: DirtyVerification()
        with self.assertRaises(ManagedContinuityError) as caught:
            self.submit_result()
        self.assertEqual(caught.exception.code, "WORKSPACE_DIRTY")
        self.runtime.result_verifier = lambda *_args: nullcontext()
        with self.assertRaises(ManagedContinuityError) as fenced:
            self.submit_result()
        self.assertEqual(fenced.exception.code, RESULT_CONFLICT)

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(staged["result_receipts"], [])
        self.assertEqual(len(staged["coordinator_contexts"]), 1)
        self.assertEqual(len(staged["blocker_observations"]), 1)
        self.assertEqual(staged["blocker_observations"][0]["count"], 1)
        context = staged["coordinator_contexts"][0]
        self.assertEqual(context["reason_code"], "WORKSPACE_DIRTY")
        identity = self.runtime.current_identity("project-id", "2860")
        self.assertEqual(identity["assignment"]["assignment_id"], context["context_id"])
        self.assertEqual(managed_activation_invariant_issues(staged), ())
        prepared = self.prepared_continuation(
            staged,
            assignment_id="assignment-dirty-continue",
            source_commit=COMMIT,
        )
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        workspace_manager = Mock(unsafe=True)
        workspace_manager.publish_write_workspace.return_value = (
            prepared.verified_workspace
        )
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "continue-dirty-1",
            "action": "CONTINUE_NODE",
            "parameters": {"node_id": "build", "source_commit": COMMIT},
        }
        with (
            patch.object(self.runtime, "_prepare_assignment", return_value=prepared),
            patch.object(self.runtime, "_publish_prepared_assignment"),
            patch.object(
                self.runtime, "_recover_assignment_publications", return_value=0
            ),
            patch.object(
                self.importer,
                "provider_for_durable_repository",
                return_value=provider,
            ),
            patch.object(
                self.importer,
                "_workspace_manager",
                workspace_manager,
            ),
        ):
            completed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "continue-dirty",
            )
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(completed.response["status"], "RECOVERY_COMPLETED")
        self.assertEqual(recovered["assignments"][0]["status"], "failed")
        self.assertEqual(recovered["assignments"][1]["status"], "active")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_invalid_block_external_does_not_stop_live_assignment_processes(
        self,
    ) -> None:
        class DirtyVerification:
            def __enter__(self):
                raise ManagedContinuityError(
                    "WORKSPACE_DIRTY", 409, "result-verification"
                )

            def __exit__(self, *_args: object) -> None:
                return None

        self.runtime.result_verifier = lambda *_args: DirtyVerification()
        with self.assertRaises(ManagedContinuityError):
            self.submit_result()
        staged = self.store.runtime_state("project-id", SPRINT_ID)
        context = staged["coordinator_contexts"][0]
        process_snapshot = deepcopy(staged["processes"])
        port_snapshot = deepcopy(staged["port_leases"])
        assignment_status = staged["assignments"][0]["status"]
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "invalid-block-external",
            "action": "BLOCK_EXTERNAL",
            "parameters": {
                "reason_code": "WORKSPACE_DIRTY",
                "operator_action": "Do not stop a still-live assignment",
            },
        }

        with patch.object(self.runtime, "_stop_assignment_processes") as stop:
            with self.assertRaises(ManagedContinuityError) as caught:
                self.runtime.submit_if_managed(
                    "project-id",
                    "2860",
                    canonical_json_bytes(request),
                    "invalid-block-external",
                )
        self.assertEqual(caught.exception.code, RECOVERY_CONFLICT)
        stop.assert_not_called()
        after = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(after["processes"], process_snapshot)
        self.assertEqual(after["port_leases"], port_snapshot)
        self.assertEqual(after["assignments"][0]["status"], assignment_status)

    def test_outbox_failure_routes_once_without_retry_count_inflation(self) -> None:
        with patch.object(
            self.store,
            "enqueue_managed_queue_item",
            side_effect=RuntimeError("queue unavailable"),
        ):
            accepted = self.submit_result()
            replay = self.submit_result()

        self.assertEqual(accepted.response["status"], "REVIEWS_PENDING")
        self.assertFalse(accepted.response["deduplicated"])
        self.assertEqual(replay.response["status"], "ALREADY_ACCEPTED")
        self.assertTrue(replay.response["deduplicated"])

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        contexts = [
            item
            for item in staged["coordinator_contexts"]
            if item["reason_code"] == "HANDOFF_DELIVERY_FAILED"
        ]
        self.assertEqual(len(contexts), 1)
        coordinator_events = [
            item
            for item in staged["outbox"]
            if item["event_type"] == "COORDINATOR_ENQUEUE"
        ]
        self.assertEqual(len(coordinator_events), 1)
        self.assertEqual(coordinator_events[0]["status"], "pending")
        self.assertEqual(len(staged["blocker_observations"]), 1)
        self.assertEqual(staged["blocker_observations"][0]["count"], 1)
        self.assertEqual(contexts[0]["failure_scope"], "recovery")
        self.assertEqual(managed_activation_invariant_issues(staged), ())

    def test_decided_review_ack_and_replay_ignore_post_commit_drain_failure(
        self,
    ) -> None:
        self.submit_result()
        review = self.store.runtime_state("project-id", SPRINT_ID)[
            "review_assignments"
        ][0]
        payload = canonical_json_bytes(
            {"assignment_id": review["assignment_id"], "status": "APPROVE"}
        )
        original_drain = self.runtime.drain_outbox
        calls = 0

        def fail_after_decision(project_id: str, sprint_id: str) -> int:
            nonlocal calls
            calls += 1
            if calls >= 3:
                raise RuntimeError("queue unavailable after durable review")
            return original_drain(project_id, sprint_id)

        with patch.object(
            self.runtime, "drain_outbox", side_effect=fail_after_decision
        ):
            accepted = self.runtime.submit_if_managed(
                "project-id", review["reviewer_phone"], payload, "review-first"
            )
            replay = self.runtime.submit_if_managed(
                "project-id", review["reviewer_phone"], payload, "review-replay"
            )

        self.assertEqual(accepted.response["status"], "REVIEW_ACCEPTED")
        self.assertFalse(accepted.response["deduplicated"])
        self.assertEqual(replay.response["status"], "ALREADY_ACCEPTED")
        self.assertTrue(replay.response["deduplicated"])

    def test_successor_preparation_failure_routes_to_coordinator(self) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        definition = state["graph_revisions"][0]["definition"]
        definition["nodes"][0]["transitions"]["DONE"] = "deploy"
        definition["nodes"].insert(
            1,
            {
                "id": "deploy",
                "agent": {
                    "id": "deployer",
                    "name": "Deployer",
                    "phone": "2862",
                },
                "tasks": [
                    {
                        "task_id": "DEPLOY-1",
                        "queue": "worker-all",
                        "message": "Deploy",
                    }
                ],
                "workspace": {"access": "read"},
                "transitions": {"DONE": "completed"},
            },
        )
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()
        self.replace_state(state)
        self.submit_result()
        reviews = self.store.runtime_state("project-id", SPRINT_ID)[
            "review_assignments"
        ]
        self.runtime.submit_if_managed(
            "project-id",
            reviews[0]["reviewer_phone"],
            canonical_json_bytes(
                {"assignment_id": reviews[0]["assignment_id"], "status": "APPROVE"}
            ),
            "review-one",
        )
        with patch.object(
            self.runtime,
            "_prepare_assignment",
            side_effect=ManagedContinuityError(
                "CONTINUITY_RUNTIME_FAILED", 409, "assignment-preparation"
            ),
        ):
            response = self.runtime.submit_if_managed(
                "project-id",
                reviews[1]["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": reviews[1]["assignment_id"],
                        "status": "APPROVE",
                    }
                ),
                "review-two",
            )

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(response.response["status"], "REVIEW_ACCEPTED")
        self.assertEqual(
            staged["transition_journal"][0]["state"], "TRANSITION_COMMITTED"
        )
        context = next(
            item
            for item in staged["coordinator_contexts"]
            if item["reason_code"] == "SUCCESSOR_PREPARATION_FAILED"
        )
        self.assertEqual(context["failed_assignment_id"], "assignment-1")
        self.assertEqual(context["failure_scope"], "prepare")
        self.assertEqual(len(staged["blocker_observations"]), 1)
        self.assertEqual(staged["blocker_observations"][0]["count"], 1)
        self.assertEqual(managed_activation_invariant_issues(staged), ())

        result_key = staged["transition_journal"][0]["result_key"]
        retry_handoff = {
            "assignment_id": context["context_id"],
            "idempotency_key": "invalid-successor-handoff-retry",
            "action": "RETRY_HANDOFF",
            "parameters": {"result_key": result_key},
        }
        with self.assertRaises(ManagedContinuityError) as rejected:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(retry_handoff),
                "invalid-successor-handoff-retry",
            )
        self.assertEqual(rejected.exception.code, "INVALID_MANAGED_SPRINT_REQUEST")
        unchanged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(unchanged["recovery_records"], [])
        self.assertTrue(
            any(
                token["status"] == "available"
                for token in unchanged["workflow"]["transition_tokens"]
            )
        )
        identity = self.runtime.current_identity("project-id", "2860")
        self.assertEqual(identity["assignment"]["assignment_id"], context["context_id"])

    def test_exhausted_process_routes_once_after_port_release(self) -> None:
        state = active_runtime_fixture(include_process_definition=True)
        state["schema_version"] = 2
        process = process_fixture()
        process.update(
            {
                "state": "FAILED",
                "restart_policy": "never",
                "max_restart_attempts": 0,
                "failed_at": TIMESTAMP,
                "terminal_reason": "failure",
            }
        )
        state["processes"] = [process]
        state["port_leases"] = [
            {
                "lease_id": "port-lease-1",
                "instance_id": "umse-staging",
                "network_namespace_id": "host",
                "assignment_id": "assignment-1",
                "process_id": "process-1",
                "host": "127.0.0.1",
                "port": 18100,
                "status": "released",
                "bind_verified": True,
                "acquired_at": TIMESTAMP,
                "released_at": TIMESTAMP,
            }
        ]
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        with patch.object(
            self.runtime, "_recover_assignment_publications", return_value=0
        ):
            progressed = self.runtime.reconcile("project-id", SPRINT_ID)
            replayed = self.runtime.reconcile("project-id", SPRINT_ID)

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        contexts = [
            item
            for item in staged["coordinator_contexts"]
            if item["reason_code"] == "PROCESS_RESTART_EXHAUSTED"
        ]
        self.assertGreaterEqual(progressed, 1)
        self.assertEqual(replayed, 0)
        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["failure_scope"], "process")
        self.assertEqual(
            contexts[0]["normalized_error"]["process_id"], "process-1"
        )
        self.assertEqual(staged["assignments"][0]["status"], "active")
        self.assertEqual(staged["processes"], [process])
        self.assertEqual(staged["port_leases"], state["port_leases"])
        self.assertEqual(len(staged["blocker_observations"]), 1)
        self.assertEqual(staged["blocker_observations"][0]["count"], 1)
        self.assertEqual(managed_activation_invariant_issues(staged), ())
        repaired_view = deepcopy(staged)
        repaired_view["graph_revision"] = 2
        self.assertFalse(
            self.runtime._record_exhausted_process_failure(
                "project-id", SPRINT_ID, repaired_view
            )
        )

    def test_process_failure_below_restart_budget_is_not_routed(self) -> None:
        state = active_runtime_fixture(include_process_definition=True)
        state["schema_version"] = 2
        definition = state["graph_revisions"][0]["definition"]
        definition["nodes"][0]["workspace"]["process"].update(
            {"restart_policy": "on_failure", "max_restart_attempts": 3}
        )
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()
        state["processes"] = []
        state["port_leases"] = []
        for attempt in range(3):
            process = process_fixture()
            process_id = f"process-{attempt}"
            lease_id = f"port-lease-{attempt}"
            process.update(
                {
                    "process_id": process_id,
                    "runtime_root": (
                        "D:/nginx-qa-staging/runtime/processes/" + process_id
                    ),
                    "state": "FAILED",
                    "launch_nonce": f"nonce-0123456789a{attempt}",
                    "stdout_log": (
                        "D:/nginx-qa-staging/runtime/logs/"
                        + process_id
                        + ".stdout.log"
                    ),
                    "stderr_log": (
                        "D:/nginx-qa-staging/runtime/logs/"
                        + process_id
                        + ".stderr.log"
                    ),
                    "health_endpoint": {
                        "host": "127.0.0.1",
                        "port": 18100 + attempt,
                        "path": "/health",
                    },
                    "restart_attempt": attempt,
                    "restart_of_process_id": (
                        None if attempt == 0 else f"process-{attempt - 1}"
                    ),
                    "port_lease_id": lease_id,
                    "failed_at": TIMESTAMP,
                    "terminal_reason": "failure",
                }
            )
            state["processes"].append(process)
            state["port_leases"].append(
                {
                    "lease_id": lease_id,
                    "instance_id": "umse-staging",
                    "network_namespace_id": "host",
                    "assignment_id": "assignment-1",
                    "process_id": process_id,
                    "host": "127.0.0.1",
                    "port": 18100 + attempt,
                    "status": "released",
                    "bind_verified": True,
                    "acquired_at": TIMESTAMP,
                    "released_at": TIMESTAMP,
                }
            )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        with patch.object(
            self.runtime, "_recover_assignment_publications", return_value=0
        ):
            progressed = self.runtime.reconcile("project-id", SPRINT_ID)

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(progressed, 0)
        self.assertEqual(staged["coordinator_contexts"], [])
        self.assertEqual(staged["blocker_observations"], [])

    def test_join_integration_preparation_failure_routes_once(self) -> None:
        state, trigger_ids = prepared_integration_runtime_fixture()
        state["integrations"] = []
        state["integration_workspaces"] = []
        lint = next(
            item
            for item in state["assignments"]
            if item["assignment_id"] == "lint-assignment"
        )
        lint.update(
            {
                "result_commit": None,
                "outcome": None,
                "status": "active",
                "completed_at": None,
            }
        )
        lint_occurrence = next(
            item
            for item in state["workflow"]["occurrences"]
            if item["occurrence_id"] == lint["occurrence_id"]
        )
        lint_occurrence.update({"state": "active", "completed_at": None})
        state["active_assignment_ids"] = ["lint-assignment"]
        state["allowed_outcomes_by_assignment"] = {
            "lint-assignment": list(lint["allowed_outcomes"])
        }
        lint_result_keys = {
            item["result_key"]
            for item in state["result_receipts"]
            if item["assignment_id"] == "lint-assignment"
        }
        for collection in (
            "result_receipts",
            "transition_journal",
            "review_assignments",
            "reviews",
        ):
            state[collection] = [
                item
                for item in state[collection]
                if item.get("result_key") not in lint_result_keys
            ]
        state["workflow"]["transition_tokens"] = [
            item
            for item in state["workflow"]["transition_tokens"]
            if item.get("result_key") not in lint_result_keys
        ]
        state["outbox"] = [
            item
            for item in state["outbox"]
            if item.get("payload", {}).get("result_key") not in lint_result_keys
        ]
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        accepted = self.runtime.submit_if_managed(
            "project-id",
            "2862",
            canonical_json_bytes(
                {
                    "assignment_id": "lint-assignment",
                    "status": "DONE",
                    "result": "Lint complete",
                    "from_commit": COMMIT,
                    "git_commit": "d" * 40,
                    "git_branch": None,
                }
            ),
            "lint-result",
        )
        self.assertEqual(accepted.response["status"], "REVIEWS_PENDING")
        reviews = [
            item
            for item in self.store.runtime_state("project-id", SPRINT_ID)[
                "review_assignments"
            ]
            if item["source_assignment_id"] == "lint-assignment"
        ]
        self.runtime.submit_if_managed(
            "project-id",
            reviews[0]["reviewer_phone"],
            canonical_json_bytes(
                {"assignment_id": reviews[0]["assignment_id"], "status": "APPROVE"}
            ),
            "lint-review-one",
        )

        with patch.object(
            self.runtime,
            "_prepare_integration_pair",
            side_effect=ManagedContinuityError(
                "CONTINUITY_RUNTIME_FAILED", 503, "integration-preparation"
            ),
        ):
            approved = self.runtime.submit_if_managed(
                "project-id",
                reviews[1]["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": reviews[1]["assignment_id"],
                        "status": "APPROVE",
                    }
                ),
                "lint-review-two",
            )
        replayed = self.runtime.reconcile("project-id", SPRINT_ID)

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        contexts = [
            item
            for item in staged["coordinator_contexts"]
            if item["reason_code"] == "SUCCESSOR_PREPARATION_FAILED"
        ]
        self.assertEqual(approved.response["status"], "REVIEW_ACCEPTED")
        self.assertEqual(replayed, 0)
        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["failed_assignment_id"], "lint-assignment")
        self.assertEqual(contexts[0]["failure_scope"], "prepare")
        self.assertEqual(
            contexts[0]["normalized_error"]["phase"], "INTEGRATION_PREPARE"
        )
        self.assertEqual(
            contexts[0]["normalized_error"]["trigger_token_ids"], trigger_ids
        )
        self.assertEqual(len(staged["blocker_observations"]), 1)
        self.assertEqual(managed_activation_invariant_issues(staged), ())

    def test_rework_preparation_failure_is_durably_routed(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        review = state["review_assignments"][0]
        with patch.object(
            self.runtime,
            "_prepare_assignment",
            side_effect=ManagedContinuityError(
                "CONTINUITY_RUNTIME_FAILED", 409, "assignment-preparation"
            ),
        ):
            response = self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": review["assignment_id"],
                        "status": "REJECT",
                        "feedback": "Retry successor preparation safely",
                    }
                ),
                "review-route-failure",
            )

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(response.response["status"], "REWORK_ENQUEUED")
        self.assertEqual(staged["reworks"], [])
        self.assertEqual(staged["assignments"][0]["status"], "reviews_pending")
        self.assertEqual(staged["transition_journal"][0]["disposition"], "open")
        self.assertEqual(len(staged["reviews"]), 1)
        self.assertEqual(len(staged["coordinator_contexts"]), 1)
        self.assertEqual(
            staged["coordinator_contexts"][0]["reason_code"],
            "REWORK_ROUTING_FAILED",
        )
        self.assertEqual(managed_activation_invariant_issues(staged), ())

    def test_rework_commit_failure_is_durably_routed(self) -> None:
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        review = state["review_assignments"][0]
        prepared = self.prepared_continuation(
            state,
            assignment_id="assignment-rework-commit-failure",
            source_commit=COMMIT,
        )
        prepared.assignment.update(
            {
                "source_kind": "rework_result",
                "source_result_keys": [state["result_receipts"][0]["result_key"]],
                "rework_cycle": 1,
            }
        )

        with (
            patch.object(self.runtime, "_prepare_assignment", return_value=prepared),
            patch.object(
                self.runtime,
                "_commit_prepared_assignment",
                side_effect=RuntimeError("injected branch lease commit failure"),
            ),
        ):
            response = self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": review["assignment_id"],
                        "status": "REJECT",
                        "feedback": "Route after the durable commit fence fails",
                    }
                ),
                "review-commit-route-failure",
            )

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(response.response["status"], "REWORK_ENQUEUED")
        self.assertEqual(len(staged["reviews"]), 1)
        self.assertEqual(staged["reworks"], [])
        self.assertEqual(len(staged["assignments"]), 1)
        self.assertEqual(
            staged["coordinator_contexts"][0]["reason_code"],
            "REWORK_ROUTING_FAILED",
        )
        self.assertEqual(managed_activation_invariant_issues(staged), ())

    def test_rework_limit_blocks_occurrence_and_sprint_with_evidence(self) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        definition = state["graph_revisions"][0]["definition"]
        definition["execution"]["max_rework_cycles"] = 0
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()
        self.replace_state(state)
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        review = state["review_assignments"][0]
        response = self.runtime.submit_if_managed(
            "project-id",
            review["reviewer_phone"],
            canonical_json_bytes(
                {
                    "assignment_id": review["assignment_id"],
                    "status": "REJECT",
                    "feedback": "The cycle limit is exhausted",
                }
            ),
            "review-limit",
        )

        blocked = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(response.response["status"], "REWORK_ENQUEUED")
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(blocked["assignments"][0]["status"], "blocked")
        self.assertEqual(blocked["workflow"]["occurrences"][0]["state"], "blocked")
        self.assertEqual(blocked["reworks"], [])
        self.assertEqual(len(blocked["assignments"]), 1)
        self.assertEqual(
            blocked["coordinator_contexts"][0]["reason_code"],
            "REWORK_LIMIT_EXCEEDED",
        )
        coordinator_event = next(
            item
            for item in blocked["outbox"]
            if item["event_type"] == "COORDINATOR_ENQUEUE"
        )
        self.assertEqual(coordinator_event["status"], "delivered")
        self.assertEqual(managed_activation_invariant_issues(blocked), ())

        invalid_states: list[tuple[str, dict]] = []

        cap_not_exceeded = deepcopy(blocked)
        cap_definition = cap_not_exceeded["graph_revisions"][0]["definition"]
        cap_definition["execution"]["max_rework_cycles"] = 5
        cap_not_exceeded["graph_revisions"][0]["definition_sha256"] = (
            hashlib.sha256(canonical_json_bytes(cap_definition)).hexdigest()
        )
        invalid_states.append(("cap-not-exceeded", cap_not_exceeded))

        wrong_maximum = deepcopy(blocked)
        wrong_maximum["coordinator_contexts"][0]["normalized_error"][
            "max_rework_cycles"
        ] = 999
        invalid_states.append(("wrong-normalized-maximum", wrong_maximum))

        wrong_next_cycle = deepcopy(blocked)
        wrong_next_cycle["coordinator_contexts"][0]["normalized_error"][
            "next_rework_cycle"
        ] = 2
        invalid_states.append(("wrong-normalized-next-cycle", wrong_next_cycle))

        wrong_journal_state = deepcopy(blocked)
        wrong_journal_state["transition_journal"][0]["state"] = "REVIEWS_ACCEPTED"
        invalid_states.append(("journal-not-pending", wrong_journal_state))

        wrong_journal_disposition = deepcopy(blocked)
        wrong_journal_disposition["transition_journal"][0][
            "disposition"
        ] = "accepted"
        invalid_states.append(("journal-not-open", wrong_journal_disposition))

        missing_reject = deepcopy(blocked)
        missing_reject["reviews"] = []
        invalid_states.append(("reject-missing", missing_reject))

        wrong_feedback = deepcopy(blocked)
        wrong_feedback["coordinator_contexts"][0]["reviewer_feedback"][0][
            "feedback"
        ] = "forged feedback"
        invalid_states.append(("feedback-mismatch", wrong_feedback))

        fake_rework = deepcopy(blocked)
        fake_rework["reworks"].append(
            {
                "rework_id": "forged-rework",
                "rejected_result_key": fake_rework["transition_journal"][0][
                    "result_key"
                ],
            }
        )
        invalid_states.append(("rework-exists", fake_rework))

        successor_lineage = deepcopy(blocked)
        successor_lineage["workflow"]["occurrences"][0]["assignment_ids"].append(
            "forged-successor"
        )
        invalid_states.append(("successor-exists", successor_lineage))

        for label, candidate in invalid_states:
            with self.subTest(label=label):
                issues = managed_activation_invariant_issues(candidate)
                self.assertIn("REWORK_LIMIT_EVIDENCE_INVALID", issues)
                self.assertIn("TERMINAL_TOKEN_PROOF_MISSING", issues)
                self.assertIn("TERMINAL_BRANCH_PROOF_MISSING", issues)

    def test_parallel_direct_review_cap_preserves_and_settles_live_sibling(
        self,
    ) -> None:
        self.configure_parallel_active_sibling(maximum=0)
        self.submit_result()
        pending = self.store.runtime_state("project-id", SPRINT_ID)
        review = next(
            item
            for item in pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-1"
        )
        request = {
            "assignment_id": review["assignment_id"],
            "status": "REJECT",
            "feedback": "The parallel branch exhausted its rework budget",
        }
        response = self.runtime.submit_if_managed(
            "project-id",
            review["reviewer_phone"],
            canonical_json_bytes(request),
            "parallel-review-cap",
        )

        branch_scoped = self.store.runtime_state("project-id", SPRINT_ID)
        assignments = {
            item["assignment_id"]: item for item in branch_scoped["assignments"]
        }
        occurrences = {
            item["occurrence_id"]: item
            for item in branch_scoped["workflow"]["occurrences"]
        }
        self.assertEqual(response.response["status"], "REWORK_ENQUEUED")
        self.assertEqual(branch_scoped["status"], "active")
        self.assertEqual(assignments["assignment-1"]["status"], "blocked")
        self.assertEqual(assignments["assignment-parallel"]["status"], "active")
        self.assertEqual(
            occurrences[assignments["assignment-1"]["occurrence_id"]]["state"],
            "blocked",
        )
        self.assertEqual(
            occurrences[assignments["assignment-parallel"]["occurrence_id"]][
                "state"
            ],
            "active",
        )
        self.assertEqual(
            branch_scoped["active_assignment_ids"], ["assignment-parallel"]
        )
        self.assertEqual(
            set(branch_scoped["allowed_outcomes_by_assignment"]),
            {"assignment-parallel"},
        )
        self.assertEqual(branch_scoped["reworks"], [])
        self.assertEqual(
            [
                context["reason_code"]
                for context in branch_scoped["coordinator_contexts"]
            ],
            ["REWORK_LIMIT_EXCEEDED"],
        )
        self.assertEqual(managed_activation_invariant_issues(branch_scoped), ())
        sibling_identity = self.runtime.current_identity("project-id", "2862")
        self.assertEqual(
            sibling_identity["assignment"]["assignment_id"],
            "assignment-parallel",
        )

        late_review = next(
            item
            for item in branch_scoped["review_assignments"]
            if item["source_assignment_id"] == "assignment-1"
            and item["assignment_id"] != review["assignment_id"]
        )
        before_late_review = canonical_json_bytes(branch_scoped)
        with self.assertRaises(ManagedContinuityError) as late:
            self.runtime.submit_if_managed(
                "project-id",
                late_review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": late_review["assignment_id"],
                        "status": "APPROVE",
                    }
                ),
                "parallel-review-after-cap",
            )
        self.assertEqual(late.exception.code, REVIEW_CONFLICT)
        self.assertEqual(late.exception.http_status, 409)
        after_late_review = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(
            canonical_json_bytes(after_late_review), before_late_review
        )
        self.assertEqual(len(after_late_review["reviews"]), 1)
        self.assertEqual(
            next(
                item
                for item in after_late_review["review_assignments"]
                if item["assignment_id"] == late_review["assignment_id"]
            )["status"],
            "active",
        )
        self.assertEqual(
            managed_activation_invariant_issues(after_late_review), ()
        )

        replay = self.runtime.submit_if_managed(
            "project-id",
            review["reviewer_phone"],
            canonical_json_bytes(request),
            "parallel-review-cap-replay",
        )
        self.assertEqual(replay.response["status"], "ALREADY_ACCEPTED")
        self.assertTrue(replay.response["deduplicated"])

        legacy_pending = self.store.runtime_state("project-id", SPRINT_ID)
        cap_context = legacy_pending["coordinator_contexts"][0]
        source_assignment = assignments["assignment-1"]
        definition = legacy_pending["graph_revisions"][-1]["definition"]
        coordinator_node = next(
            item
            for item in definition["nodes"]
            if item["id"] == definition["coordinator"]["node_id"]
        )
        pending_parameters = {
            "node_id": source_assignment["node_id"],
            "source_commit": source_assignment["source_commit"],
        }
        legacy_pending["recovery_records"].append(
            {
                "recovery_id": "recovery-legacy-pending-at-rework-cap",
                "coordinator_id": coordinator_node["agent"]["id"],
                "context_id": cap_context["context_id"],
                "graph_revision": cap_context["graph_revision"],
                "idempotency_key": "legacy-pending-at-rework-cap",
                "request_fingerprint": recovery_request_fingerprint(
                    cap_context["context_id"],
                    cap_context["graph_revision"],
                    "CONTINUE_NODE",
                    None,
                    cap_context["failed_assignment_id"],
                    pending_parameters,
                    None,
                ),
                "action": "CONTINUE_NODE",
                "parameters": pending_parameters,
                "import_attempt_id": None,
                "assignment_id": cap_context["failed_assignment_id"],
                "join_target": None,
                "produced_record_ids": [],
                "response": None,
                "normalized_error": None,
                "evidence": {},
                "status": "pending",
                "created_at": TIMESTAMP,
                "completed_at": None,
            }
        )
        self.assertEqual(managed_activation_invariant_issues(legacy_pending), ())
        self.replace_state(legacy_pending)
        before_legacy_replay = canonical_json_bytes(legacy_pending)
        with self.assertRaises(ManagedContinuityError) as capped_replay:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(
                    {
                        "assignment_id": cap_context["context_id"],
                        "idempotency_key": "legacy-pending-at-rework-cap",
                        "action": "CONTINUE_NODE",
                        "parameters": pending_parameters,
                    }
                ),
                "legacy-pending-at-rework-cap",
            )
        self.assertEqual(capped_replay.exception.code, RECOVERY_CONFLICT)
        self.assertEqual(capped_replay.exception.http_status, 409)
        self.assertEqual(
            canonical_json_bytes(
                self.store.runtime_state("project-id", SPRINT_ID)
            ),
            before_legacy_replay,
        )
        self.assertIsNone(self.runtime.current_identity("project-id", "2860"))

        sibling_result = self.runtime.submit_if_managed(
            "project-id",
            "2862",
            canonical_json_bytes(
                {
                    "assignment_id": "assignment-parallel",
                    "status": "DONE",
                    "result": "The sibling completed independently",
                    "from_commit": COMMIT,
                    "git_commit": COMMIT,
                    "git_branch": None,
                }
            ),
            "parallel-sibling-result",
        )
        self.assertEqual(sibling_result.response["status"], "REVIEWS_PENDING")
        sibling_pending = self.store.runtime_state("project-id", SPRINT_ID)
        sibling_reviews = [
            item
            for item in sibling_pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-parallel"
        ]
        for sibling_review in sibling_reviews:
            self.runtime.submit_if_managed(
                "project-id",
                sibling_review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": sibling_review["assignment_id"],
                        "status": "APPROVE",
                    }
                ),
                "parallel-sibling-review",
            )
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "blocked")
        self.assertEqual(terminal["active_assignment_ids"], [])
        self.assertEqual(
            [
                item["status"]
                for item in terminal["workflow"]["transition_tokens"]
            ],
            ["terminal"],
        )
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

    def test_parallel_route_rework_cap_is_atomic_and_replayable(self) -> None:
        self.configure_parallel_active_sibling(maximum=0)
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        source = state["assignments"][0]
        result_key = state["result_receipts"][0]["result_key"]
        feedback = "Recover the rejected branch without stopping its sibling"
        review = state["review_assignments"][0]
        request_fingerprint = review_request_fingerprint(
            review["assignment_id"], "REJECT", feedback
        )
        review_response = {
            "assignment_id": review["assignment_id"],
            "source_assignment_id": source["assignment_id"],
            "result_key": result_key,
            "decision": "REJECT",
            "status": "REWORK_ENQUEUED",
            "deduplicated": False,
        }
        review.update(
            {
                "status": "decided",
                "decision": "REJECT",
                "request_fingerprint": request_fingerprint,
                "response": deepcopy(review_response),
                "decided_at": review["activated_at"],
            }
        )
        state["reviews"].append(
            {
                "assignment_id": review["assignment_id"],
                "source_assignment_id": source["assignment_id"],
                "result_key": result_key,
                "result_commit": review["result_commit"],
                "result_outcome": review["result_outcome"],
                "reviewer_id": review["reviewer_id"],
                "reviewer_index": review["reviewer_index"],
                "decision": "REJECT",
                "feedback": feedback,
                "request_fingerprint": request_fingerprint,
                "response": deepcopy(review_response),
                "decided_at": review["activated_at"],
            }
        )
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-parallel-route-rework-cap",
            reason_code="REWORK_ROUTING_FAILED",
        )
        context["failure_scope"] = "review"
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "parallel-route-rework-cap",
            "action": "ROUTE_REWORK",
            "parameters": {
                "rejected_result_key": result_key,
                "feedback": feedback,
            },
        }

        with (
            patch.object(
                self.runtime,
                "_fail_pending_recovery",
                side_effect=InjectedCrash("after atomic cap settlement"),
            ),
            self.assertRaises(InjectedCrash),
        ):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "parallel-route-rework-cap-first",
            )

        branch_scoped = self.store.runtime_state("project-id", SPRINT_ID)
        assignments = {
            item["assignment_id"]: item for item in branch_scoped["assignments"]
        }
        recovery = branch_scoped["recovery_records"][0]
        self.assertEqual(branch_scoped["status"], "active")
        self.assertEqual(assignments["assignment-1"]["status"], "blocked")
        self.assertEqual(assignments["assignment-parallel"]["status"], "active")
        self.assertEqual(
            branch_scoped["active_assignment_ids"], ["assignment-parallel"]
        )
        self.assertEqual(branch_scoped["reworks"], [])
        self.assertEqual(recovery["status"], "failed")
        self.assertEqual(recovery["produced_record_ids"], [])
        self.assertIsNone(recovery["response"])
        self.assertEqual(
            recovery["normalized_error"],
            {"code": "REWORK_LIMIT_EXCEEDED", "http_status": 409},
        )
        self.assertEqual(
            [
                item["reason_code"]
                for item in branch_scoped["coordinator_contexts"]
            ],
            ["REWORK_ROUTING_FAILED", "REWORK_LIMIT_EXCEEDED"],
        )
        self.assertEqual(managed_activation_invariant_issues(branch_scoped), ())
        self.assertIsNone(self.runtime.current_identity("project-id", "2860"))

        cap_context = next(
            item
            for item in branch_scoped["coordinator_contexts"]
            if item["reason_code"] == "REWORK_LIMIT_EXCEEDED"
        )
        strict_cap_state = deepcopy(branch_scoped)
        strict_cap_state["schema_version"] = 2
        forged_cap_recovery = deepcopy(recovery)
        forged_cap_recovery.update(
            {
                "recovery_id": "recovery-forged-cap-context",
                "context_id": cap_context["context_id"],
                "graph_revision": cap_context["graph_revision"],
                "idempotency_key": "forged-cap-context-recovery",
                "request_fingerprint": recovery_request_fingerprint(
                    cap_context["context_id"],
                    cap_context["graph_revision"],
                    recovery["action"],
                    recovery["import_attempt_id"],
                    recovery["assignment_id"],
                    recovery["parameters"],
                    recovery["join_target"],
                ),
            }
        )
        strict_cap_state["recovery_records"].append(forged_cap_recovery)
        self.assertIn(
            "RECOVERY_BINDING_INVALID",
            managed_activation_invariant_issues(strict_cap_state),
        )
        capped_continue = {
            "assignment_id": cap_context["context_id"],
            "idempotency_key": "parallel-cap-must-not-continue",
            "action": "CONTINUE_NODE",
            "parameters": {
                "node_id": source["node_id"],
                "source_commit": source["source_commit"],
            },
        }
        before_capped_continue = canonical_json_bytes(branch_scoped)
        with self.assertRaises(ManagedContinuityError) as capped:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(capped_continue),
                "parallel-cap-continue-conflict",
            )
        self.assertEqual(capped.exception.code, RECOVERY_CONFLICT)
        self.assertEqual(capped.exception.http_status, 409)
        self.assertEqual(
            canonical_json_bytes(
                self.store.runtime_state("project-id", SPRINT_ID)
            ),
            before_capped_continue,
        )

        with self.assertRaises(ManagedContinuityError) as replay:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "parallel-route-rework-cap-replay",
            )
        self.assertEqual(replay.exception.code, "REWORK_LIMIT_EXCEEDED")
        self.assertEqual(replay.exception.http_status, 409)
        replayed = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(len(replayed["recovery_records"]), 1)
        self.assertEqual(managed_activation_invariant_issues(replayed), ())

        content = b"must not be read\n"
        digest = hashlib.sha256(content).hexdigest()
        repair_commit = "d" * 40
        competing_request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "parallel-route-rework-cap-competing-repair",
            "action": "APPLY_REPAIR",
            "parameters": {
                "request": {
                    "expected_revision": 1,
                    "repair_source_commit": repair_commit,
                    "idempotency_key": "parallel-route-rework-cap-repair",
                    "patch": {
                        "checksum_metadata": [
                            {
                                "path": "orchestration/README.md",
                                "sha256": digest,
                            },
                            {"path": "service.py", "sha256": digest},
                        ]
                    },
                }
            },
        }
        provider = Mock(unsafe=True)
        with (
            patch.object(
                self.importer,
                "provider_for_durable_repository",
                return_value=provider,
            ),
            self.assertRaises(ManagedContinuityError) as competing,
        ):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(competing_request),
                "parallel-route-rework-cap-competing-repair",
            )
        self.assertEqual(competing.exception.code, RECOVERY_CONFLICT)
        self.assertEqual(competing.exception.http_status, 409)
        provider.ensure_mirror.assert_not_called()
        fenced = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(fenced["graph_revision"], 1)
        self.assertEqual(fenced["repairs"], [])
        self.assertEqual(len(fenced["recovery_records"]), 1)
        self.assertEqual(
            next(
                item
                for item in fenced["assignments"]
                if item["assignment_id"] == "assignment-parallel"
            )["status"],
            "active",
        )
        self.assertEqual(managed_activation_invariant_issues(fenced), ())

        legacy_cap = deepcopy(fenced)
        legacy_cap["recovery_records"][0]["normalized_error"].pop(
            "http_status"
        )
        self.assertEqual(managed_activation_invariant_issues(legacy_cap), ())
        self.replace_state(legacy_cap)
        legacy_request = deepcopy(competing_request)
        legacy_request["idempotency_key"] = (
            "parallel-route-rework-cap-legacy-competing-repair"
        )
        legacy_provider = Mock(unsafe=True)
        with (
            patch.object(
                self.importer,
                "provider_for_durable_repository",
                return_value=legacy_provider,
            ),
            self.assertRaises(ManagedContinuityError) as legacy_conflict,
        ):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(legacy_request),
                "parallel-route-rework-cap-legacy-conflict",
            )
        self.assertEqual(legacy_conflict.exception.code, RECOVERY_CONFLICT)
        legacy_provider.ensure_mirror.assert_not_called()
        legacy_fenced = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(len(legacy_fenced["recovery_records"]), 1)
        self.assertEqual(legacy_fenced["graph_revision"], 1)

    def test_parallel_rework_cap_combines_prior_terminal_sibling(self) -> None:
        self.configure_parallel_active_sibling(maximum=0)
        sibling_result = self.runtime.submit_if_managed(
            "project-id",
            "2862",
            canonical_json_bytes(
                {
                    "assignment_id": "assignment-parallel",
                    "status": "DONE",
                    "result": "The sibling reached its terminal first",
                    "from_commit": COMMIT,
                    "git_commit": COMMIT,
                    "git_branch": None,
                }
            ),
            "parallel-terminal-first-result",
        )
        self.assertEqual(sibling_result.response["status"], "REVIEWS_PENDING")
        pending = self.store.runtime_state("project-id", SPRINT_ID)
        for review in [
            item
            for item in pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-parallel"
        ]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {
                        "assignment_id": review["assignment_id"],
                        "status": "APPROVE",
                    }
                ),
                "parallel-terminal-first-review",
            )
        sibling_terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(sibling_terminal["status"], "active")
        self.assertEqual(
            [
                item["status"]
                for item in sibling_terminal["workflow"]["transition_tokens"]
            ],
            ["terminal"],
        )
        self.assertEqual(managed_activation_invariant_issues(sibling_terminal), ())

        self.submit_result()
        source_pending = self.store.runtime_state("project-id", SPRINT_ID)
        source_review = next(
            item
            for item in source_pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-1"
        )
        response = self.runtime.submit_if_managed(
            "project-id",
            source_review["reviewer_phone"],
            canonical_json_bytes(
                {
                    "assignment_id": source_review["assignment_id"],
                    "status": "REJECT",
                    "feedback": "The remaining branch exhausted its budget",
                }
            ),
            "parallel-terminal-first-cap",
        )
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(response.response["status"], "REWORK_ENQUEUED")
        self.assertEqual(terminal["status"], "blocked")
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

        failed_priority = deepcopy(terminal)
        definition = failed_priority["graph_revisions"][0]["definition"]
        terminal_target_id = failed_priority["workflow"]["transition_tokens"][0][
            "target_node_id"
        ]
        terminal_target = next(
            node for node in definition["nodes"] if node["id"] == terminal_target_id
        )
        terminal_target["status"] = "FAILED"
        failed_priority["graph_revisions"][0]["definition_sha256"] = (
            hashlib.sha256(canonical_json_bytes(definition)).hexdigest()
        )
        failed_priority["status"] = "failed"
        self.assertEqual(
            self.runtime._terminal_status_when_quiescent(failed_priority), "failed"
        )
        self.assertEqual(managed_activation_invariant_issues(failed_priority), ())

    def test_parallel_rework_cap_accepts_multiple_blocked_branches(self) -> None:
        self.configure_parallel_active_sibling(maximum=0)
        self.submit_result()
        first_pending = self.store.runtime_state("project-id", SPRINT_ID)
        first_review = next(
            item
            for item in first_pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-1"
        )
        self.runtime.submit_if_managed(
            "project-id",
            first_review["reviewer_phone"],
            canonical_json_bytes(
                {
                    "assignment_id": first_review["assignment_id"],
                    "status": "REJECT",
                    "feedback": "The build branch exhausted its budget",
                }
            ),
            "parallel-first-cap",
        )
        self.runtime.submit_if_managed(
            "project-id",
            "2862",
            canonical_json_bytes(
                {
                    "assignment_id": "assignment-parallel",
                    "status": "DONE",
                    "result": "The lint branch also needs rework",
                    "from_commit": COMMIT,
                    "git_commit": COMMIT,
                    "git_branch": None,
                }
            ),
            "parallel-second-cap-result",
        )
        second_pending = self.store.runtime_state("project-id", SPRINT_ID)
        second_review = next(
            item
            for item in second_pending["review_assignments"]
            if item["source_assignment_id"] == "assignment-parallel"
        )
        self.runtime.submit_if_managed(
            "project-id",
            second_review["reviewer_phone"],
            canonical_json_bytes(
                {
                    "assignment_id": second_review["assignment_id"],
                    "status": "REJECT",
                    "feedback": "The lint branch exhausted its budget",
                }
            ),
            "parallel-second-cap",
        )

        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "blocked")
        self.assertEqual(terminal["active_assignment_ids"], [])
        self.assertEqual(
            [
                item["status"]
                for item in terminal["assignments"]
                if item["assignment_id"]
                in {"assignment-1", "assignment-parallel"}
            ],
            ["blocked", "blocked"],
        )
        self.assertEqual(
            [
                item["reason_code"]
                for item in terminal["coordinator_contexts"]
            ],
            ["REWORK_LIMIT_EXCEEDED", "REWORK_LIMIT_EXCEEDED"],
        )
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

    def test_all_parent_join_blocks_when_done_parent_is_capped(self) -> None:
        self.assert_all_parent_cap_settles("DONE")

    def test_all_parent_join_blocks_when_stopped_parent_is_capped(self) -> None:
        self.assert_all_parent_cap_settles("STOP")

    def test_rework_limit_evidence_allows_an_approval_before_rejection(self) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        definition = state["graph_revisions"][0]["definition"]
        definition["execution"]["max_rework_cycles"] = 0
        state["graph_revisions"][0]["definition_sha256"] = hashlib.sha256(
            canonical_json_bytes(definition)
        ).hexdigest()
        self.replace_state(state)
        self.submit_result()
        pending = self.store.runtime_state("project-id", SPRINT_ID)
        first, second = pending["review_assignments"]
        self.runtime.submit_if_managed(
            "project-id",
            first["reviewer_phone"],
            canonical_json_bytes(
                {
                    "assignment_id": first["assignment_id"],
                    "status": "APPROVE",
                    "feedback": "First review approved",
                }
            ),
            "review-limit-approval",
        )
        response = self.runtime.submit_if_managed(
            "project-id",
            second["reviewer_phone"],
            canonical_json_bytes(
                {
                    "assignment_id": second["assignment_id"],
                    "status": "REJECT",
                    "feedback": "Second review rejects at the cycle limit",
                }
            ),
            "review-limit-rejection",
        )

        blocked = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(response.response["status"], "REWORK_ENQUEUED")
        self.assertEqual(
            [
                item["decision"]
                for item in blocked["coordinator_contexts"][0]["reviewer_feedback"]
            ],
            ["APPROVE", "REJECT"],
        )
        self.assertEqual(managed_activation_invariant_issues(blocked), ())

    def test_blocker_observation_uses_one_stable_counter(self) -> None:
        first = self.runtime.observe_blocker(
            "project-id", SPRINT_ID, "assignment-1", "UPSTREAM_UNAVAILABLE",
            {"code": "CONNECTION_REFUSED"},
        )
        second = self.runtime.observe_blocker(
            "project-id", SPRINT_ID, "assignment-1", "UPSTREAM_UNAVAILABLE",
            {"code": "CONNECTION_REFUSED"},
        )
        self.assertEqual(first["fingerprint"], second["fingerprint"])
        self.assertEqual(second["count"], 2)
        state = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(len(state["blocker_observations"]), 1)
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_blocker_observation_increments_frozen_legacy_record(self) -> None:
        reason_code = "UPSTREAM_UNAVAILABLE"
        normalized_error = {"code": "CONNECTION_REFUSED"}
        fingerprint = managed_blocker_fingerprint(
            "assignment-1", reason_code, normalized_error
        )
        state = self.store.runtime_state("project-id", SPRINT_ID)
        state["blocker_observations"] = [
            {
                "fingerprint": fingerprint,
                "assignment_id": "assignment-1",
                "count": 3,
                "first_seen_at": TIMESTAMP,
                "last_seen_at": TIMESTAMP,
            }
        ]
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        observed = self.runtime.observe_blocker(
            "project-id",
            SPRINT_ID,
            "assignment-1",
            reason_code,
            normalized_error,
        )

        self.assertEqual(observed["fingerprint"], fingerprint)
        self.assertEqual(observed["count"], 4)
        self.assertNotIn("reason_code", observed)
        self.assertNotIn("normalized_error", observed)
        updated = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(managed_activation_invariant_issues(updated), ())

    def test_repair_replay_is_checked_before_stale_revision(self) -> None:
        content = b"repair artifact\n"
        digest = hashlib.sha256(content).hexdigest()
        repair_commit = "b" * 40
        request = {
            "expected_revision": 1,
            "repair_source_commit": repair_commit,
            "idempotency_key": "repair-1",
            "patch": {
                "checksum_metadata": [
                    {"path": "orchestration/README.md", "sha256": digest},
                    {"path": "service.py", "sha256": digest},
                ]
            },
        }
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        provider.assert_commit.return_value = repair_commit
        provider.read_blob.return_value = content
        with (
            patch.object(
                self.importer,
                "provider_for_durable_repository",
                return_value=provider,
            ),
            patch.object(
                self.runtime,
                "_reconcile_locked",
                side_effect=RuntimeError("reconciler temporarily unavailable"),
            ),
        ):
            first = self.runtime.repair(SPRINT_ID, request, "repair-first")
            replay = self.runtime.repair(SPRINT_ID, request, "repair-replay")
        self.assertFalse(first.response["deduplicated"])
        self.assertTrue(replay.response["deduplicated"])
        state = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(state["graph_revision"], 2)
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_assignment_apply_repair_produces_only_the_repair_id(self) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-assignment-repair",
            reason_code="CONFIGURATION_INVALID",
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)
        content = b"repair artifact\n"
        digest = hashlib.sha256(content).hexdigest()
        repair_commit = "a" * 40
        repair_request = {
            "expected_revision": 1,
            "repair_source_commit": repair_commit,
            "idempotency_key": "assignment-repair-1",
            "patch": {
                "checksum_metadata": [
                    {"path": "orchestration/README.md", "sha256": digest},
                    {"path": "service.py", "sha256": digest},
                ]
            },
        }
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "assignment-recovery-1",
            "action": "APPLY_REPAIR",
            "parameters": {"request": repair_request},
        }
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        provider.assert_commit.return_value = repair_commit
        provider.read_blob.return_value = content

        with patch.object(
            self.importer, "provider_for_durable_repository", return_value=provider
        ):
            completed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "assignment-repair-first",
            )

        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        repair_id = recovered["repairs"][0]["repair_id"]
        self.assertEqual(completed.response["produced_record_ids"], [repair_id])
        self.assertEqual(
            recovered["recovery_records"][0]["produced_record_ids"],
            [repair_id],
        )
        self.assertEqual(recovered["recovery_records"][0]["status"], "completed")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

        replay = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "assignment-repair-replay",
        )
        self.assertEqual(replay.response["produced_record_ids"], [repair_id])
        self.assertTrue(replay.response["deduplicated"])

    def test_pending_assignment_repair_resumes_existing_durable_repair(self) -> None:
        state = self.store.runtime_state("project-id", SPRINT_ID)
        context = self.add_assignment_coordinator_context(
            state,
            context_id="context-assignment-repair-crash",
            reason_code="CONFIGURATION_INVALID",
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)
        content = b"repair artifact\n"
        digest = hashlib.sha256(content).hexdigest()
        repair_commit = "b" * 40
        repair_request = {
            "expected_revision": 1,
            "repair_source_commit": repair_commit,
            "idempotency_key": "assignment-repair-crash-1",
            "patch": {
                "checksum_metadata": [
                    {"path": "orchestration/README.md", "sha256": digest},
                    {"path": "service.py", "sha256": digest},
                ]
            },
        }
        request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "assignment-recovery-crash-1",
            "action": "APPLY_REPAIR",
            "parameters": {"request": repair_request},
        }

        def fault(point: str, _context: dict) -> None:
            if point == "after_recovery_pending":
                raise InjectedCrash("after recovery pending")

        self.runtime.fault_injector = fault
        with self.assertRaises(InjectedCrash):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "assignment-repair-crash",
            )
        self.runtime.fault_injector = None
        pending = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(pending["recovery_records"][0]["status"], "pending")
        owner = pending["recovery_records"][0]
        owner_recovery_id = owner["recovery_id"]
        loser = deepcopy(owner)
        loser_repair_request = deepcopy(repair_request)
        loser_repair_request["idempotency_key"] = (
            "assignment-repair-legacy-loser"
        )
        loser_parameters = {"request": loser_repair_request}
        loser.update(
            {
                "recovery_id": "recovery-legacy-earlier-repair-choice",
                "idempotency_key": "assignment-recovery-legacy-loser",
                "request_fingerprint": recovery_request_fingerprint(
                    loser["context_id"],
                    loser["graph_revision"],
                    loser["action"],
                    loser["import_attempt_id"],
                    loser["assignment_id"],
                    loser_parameters,
                    loser["join_target"],
                ),
                "parameters": loser_parameters,
            }
        )
        pending["recovery_records"].insert(0, loser)
        self.assertEqual(managed_activation_invariant_issues(pending), ())
        self.replace_state(pending)

        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        provider.assert_commit.return_value = repair_commit
        provider.read_blob.return_value = content
        with patch.object(
            self.importer, "provider_for_durable_repository", return_value=provider
        ):
            self.runtime.repair(
                SPRINT_ID, repair_request, "assignment-repair-durable"
            )
        interrupted = self.store.runtime_state("project-id", SPRINT_ID)
        repair_id = interrupted["repairs"][0]["repair_id"]
        self.assertEqual(
            [item["status"] for item in interrupted["recovery_records"]],
            ["pending", "pending"],
        )
        self.assertEqual(managed_activation_invariant_issues(interrupted), ())

        multiple_owners = deepcopy(interrupted)
        second_owner = deepcopy(
            next(
                item
                for item in multiple_owners["recovery_records"]
                if item["recovery_id"] == owner_recovery_id
            )
        )
        second_owner.update(
            {
                "recovery_id": "recovery-legacy-second-repair-owner",
                "idempotency_key": "assignment-recovery-second-owner",
            }
        )
        multiple_owners["recovery_records"].append(second_owner)
        self.assertEqual(managed_activation_invariant_issues(multiple_owners), ())
        with self.assertRaises(ManagedContinuityError) as effect_free:
            self.runtime._recovery_for_completion(
                multiple_owners,
                "recovery-legacy-earlier-repair-choice",
                context["context_id"],
                "multiple-effect-owners-loser",
            )
        self.assertEqual(effect_free.exception.code, RECOVERY_CONFLICT)
        selected_owner = self.runtime._recovery_for_completion(
            multiple_owners,
            owner_recovery_id,
            context["context_id"],
            "multiple-effect-owners-winner",
            supersede_pending_at=TIMESTAMP,
        )
        self.assertEqual(selected_owner["recovery_id"], owner_recovery_id)
        selection_records = {
            item["recovery_id"]: item
            for item in multiple_owners["recovery_records"]
        }
        self.assertEqual(
            selection_records["recovery-legacy-earlier-repair-choice"]["status"],
            "failed",
        )
        self.assertEqual(second_owner["status"], "pending")
        self.assertEqual(managed_activation_invariant_issues(multiple_owners), ())

        with patch.object(
            self.importer,
            "provider_for_durable_repository",
            side_effect=AssertionError("durable repair must not be fetched again"),
        ):
            resumed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "assignment-repair-resume",
            )
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(resumed.response["produced_record_ids"], [repair_id])
        self.assertFalse(resumed.response["deduplicated"])
        self.assertEqual(len(recovered["repairs"]), 1)
        recoveries = {
            item["recovery_id"]: item for item in recovered["recovery_records"]
        }
        self.assertEqual(recoveries[owner_recovery_id]["status"], "completed")
        self.assertEqual(
            recoveries[owner_recovery_id]["produced_record_ids"], [repair_id]
        )
        self.assertEqual(
            recoveries["recovery-legacy-earlier-repair-choice"]["status"],
            "failed",
        )
        self.assertEqual(
            recoveries["recovery-legacy-earlier-repair-choice"][
                "normalized_error"
            ],
            {"code": RECOVERY_CONFLICT, "http_status": 409},
        )
        self.assertEqual(
            recoveries["recovery-legacy-earlier-repair-choice"][
                "produced_record_ids"
            ],
            [],
        )
        self.assertIsNone(
            recoveries["recovery-legacy-earlier-repair-choice"]["response"]
        )
        self.assertEqual(recovered["graph_revision"], 2)
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

        replay = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "assignment-repair-replay",
        )
        self.assertTrue(replay.response["deduplicated"])
        self.assertEqual(replay.response["produced_record_ids"], [repair_id])

    def test_published_repair_owner_completes_after_context_terminalizes(
        self,
    ) -> None:
        self.configure_terminal_result("BLOCKED_EXTERNAL")
        self.submit_result()
        pending = self.store.runtime_state("project-id", SPRINT_ID)
        for review in pending["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                f"terminal-repair-review-{review['reviewer_index']}",
            )

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        context = staged["coordinator_contexts"][0]
        content = b"terminal repair artifact\n"
        digest = hashlib.sha256(content).hexdigest()
        repair_commit = "e" * 40
        repair_request = {
            "expected_revision": 1,
            "repair_source_commit": repair_commit,
            "idempotency_key": "terminal-published-repair",
            "patch": {
                "checksum_metadata": [
                    {"path": "orchestration/README.md", "sha256": digest},
                    {"path": "service.py", "sha256": digest},
                ]
            },
        }
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        provider.assert_commit.return_value = repair_commit
        provider.read_blob.return_value = content
        with patch.object(
            self.importer, "provider_for_durable_repository", return_value=provider
        ):
            self.runtime.repair(
                SPRINT_ID, repair_request, "terminal-published-repair"
            )
        repaired = self.store.runtime_state("project-id", SPRINT_ID)
        repair_id = repaired["repairs"][0]["repair_id"]

        block_request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "terminal-settled-choice",
            "action": "BLOCK_EXTERNAL",
            "parameters": {
                "reason_code": context["reason_code"],
                "operator_action": "Acknowledge the terminal branch",
            },
        }
        self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(block_request),
            "terminal-settled-choice",
        )
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "blocked")
        settled = terminal["recovery_records"][0]

        owner_id = "recovery-terminal-published-repair-owner"
        owner_parameters = {"request": repair_request}
        owner_request = {
            "assignment_id": context["context_id"],
            "idempotency_key": "terminal-published-repair-owner",
            "action": "APPLY_REPAIR",
            "parameters": owner_parameters,
        }
        terminal["schema_version"] = 2
        terminal["recovery_records"].append(
            {
                "recovery_id": owner_id,
                "coordinator_id": settled["coordinator_id"],
                "context_id": context["context_id"],
                "graph_revision": context["graph_revision"],
                "idempotency_key": owner_request["idempotency_key"],
                "request_fingerprint": recovery_request_fingerprint(
                    context["context_id"],
                    context["graph_revision"],
                    owner_request["action"],
                    None,
                    context["failed_assignment_id"],
                    owner_parameters,
                    None,
                ),
                "action": owner_request["action"],
                "parameters": owner_parameters,
                "import_attempt_id": None,
                "assignment_id": context["failed_assignment_id"],
                "join_target": None,
                "produced_record_ids": [],
                "response": None,
                "normalized_error": None,
                "evidence": {},
                "status": "pending",
                "created_at": settled["created_at"],
                "completed_at": None,
            }
        )
        self.assertEqual(managed_activation_invariant_issues(terminal), ())
        self.replace_state(terminal)

        before_startup_reconcile = canonical_json_bytes(terminal)
        startup_progress = self.runtime.reconcile_all()
        self.assertNotEqual(startup_progress.get(SPRINT_ID), -1)
        after_startup_reconcile = self.store.runtime_state("project-id", SPRINT_ID)
        startup_owner = next(
            item
            for item in after_startup_reconcile["recovery_records"]
            if item["recovery_id"] == owner_id
        )
        self.assertEqual(startup_owner["status"], "pending")
        self.assertEqual(
            canonical_json_bytes(after_startup_reconcile), before_startup_reconcile
        )

        with patch.object(
            self.importer,
            "provider_for_durable_repository",
            side_effect=AssertionError("durable repair must not be fetched again"),
        ):
            resumed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(owner_request),
                "terminal-published-repair-owner",
            )
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        owner = next(
            item
            for item in recovered["recovery_records"]
            if item["recovery_id"] == owner_id
        )
        self.assertEqual(resumed.response["produced_record_ids"], [repair_id])
        self.assertEqual(owner["status"], "completed")
        self.assertEqual(owner["produced_record_ids"], [repair_id])
        self.assertEqual(len(recovered["repairs"]), 1)
        self.assertEqual(recovered["status"], "blocked")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_terminal_block_is_staged_then_completed_by_coordinator(self) -> None:
        self.configure_terminal_result("BLOCKED_EXTERNAL")
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        for review in state["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                "review-terminal",
            )

        staged = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(staged["status"], "active")
        self.assertEqual(staged["assignments"][0]["status"], "reviews_pending")
        self.assertEqual(len(staged["coordinator_contexts"]), 1)
        self.assertEqual(managed_activation_invariant_issues(staged), ())
        identity = self.runtime.current_identity("project-id", "2860")
        self.assertEqual(identity["assignment_kind"], "coordinator")
        context_id = identity["assignment"]["context_id"]
        request = {
            "assignment_id": context_id,
            "idempotency_key": "terminal-block-1",
            "action": "BLOCK_EXTERNAL",
            "parameters": {
                "reason_code": "BLOCKED_EXTERNAL",
                "operator_action": "Restore the external dependency",
            },
        }
        completed = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "recovery-terminal",
        )
        self.assertEqual(completed.response["status"], "RECOVERY_COMPLETED")
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "blocked")
        self.assertEqual(len(terminal["recovery_records"]), 1)
        self.assertEqual(len(terminal["blocker_observations"]), 1)
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

        with patch.object(
            self.runtime,
            "drain_outbox",
            side_effect=RuntimeError("queue unavailable after recovery"),
        ):
            replay = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "recovery-replay",
            )
        self.assertTrue(replay.response["deduplicated"])
        changed = json.loads(json.dumps(request))
        changed["parameters"]["operator_action"] = "Different action"
        with self.assertRaises(ManagedContinuityError) as caught:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(changed),
                "recovery-conflict",
            )
        self.assertEqual(caught.exception.code, RECOVERY_CONFLICT)

        legacy_race = self.store.runtime_state("project-id", SPRINT_ID)
        stale = deepcopy(legacy_race["recovery_records"][0])
        stale.update(
            {
                "recovery_id": "recovery-stale-before-completed-winner",
                "idempotency_key": "terminal-block-stale-choice",
                "produced_record_ids": [],
                "response": None,
                "normalized_error": None,
                "evidence": {},
                "status": "pending",
                "completed_at": None,
            }
        )
        legacy_race["recovery_records"].insert(0, stale)
        legacy_race["status"] = "active"
        self.assertEqual(managed_activation_invariant_issues(legacy_race), ())
        self.replace_state(legacy_race)

        restarted = ManagedContinuityRuntime(
            self.importer,
            result_verifier=lambda *_: nullcontext(),
        )
        progress = restarted.reconcile_all()
        self.assertGreater(progress[SPRINT_ID], 0)
        reconciled = self.store.runtime_state("project-id", SPRINT_ID)
        recoveries = {
            recovery["recovery_id"]: recovery
            for recovery in reconciled["recovery_records"]
        }
        self.assertEqual(reconciled["status"], "blocked")
        self.assertEqual(
            recoveries["recovery-stale-before-completed-winner"]["status"],
            "failed",
        )
        self.assertEqual(
            recoveries["recovery-stale-before-completed-winner"][
                "normalized_error"
            ],
            {"code": RECOVERY_CONFLICT, "http_status": 409},
        )
        self.assertEqual(
            recoveries[terminal["recovery_records"][0]["recovery_id"]]["status"],
            "completed",
        )
        self.assertEqual(managed_activation_invariant_issues(reconciled), ())

    def test_terminal_failed_is_settled_only_after_explicit_recovery(self) -> None:
        self.configure_terminal_result("FAILED")
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        for review in state["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                "review-failed",
            )
        identity = self.runtime.current_identity("project-id", "2860")
        context_id = identity["assignment"]["context_id"]
        self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(
                {
                    "assignment_id": context_id,
                    "idempotency_key": "terminal-failed-1",
                    "action": "BLOCK_EXTERNAL",
                    "parameters": {
                        "reason_code": "ASSIGNMENT_FAILED",
                        "operator_action": "Acknowledge the failed branch",
                    },
                }
            ),
            "recovery-failed",
        )
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "failed")
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

    def test_block_external_redacts_operator_secret_and_exact_replay_matches(
        self,
    ) -> None:
        secret = "ultra-private-recovery-token"
        alternate_secret = "alternate-private-recovery-token"
        self.runtime.secret_values = (secret, alternate_secret)
        self.configure_terminal_result("BLOCKED_EXTERNAL")
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        for review in state["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                "review-secret-block",
            )
        identity = self.runtime.current_identity("project-id", "2860")
        request = {
            "assignment_id": identity["assignment"]["context_id"],
            "idempotency_key": "terminal-secret-block-1",
            "action": "BLOCK_EXTERNAL",
            "parameters": {
                "reason_code": "BLOCKED_EXTERNAL",
                "operator_action": f"Restore {secret} and retry",
            },
        }

        completed = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "recovery-secret-first",
        )
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        recovery = terminal["recovery_records"][0]
        self.assertEqual(
            recovery["parameters"]["operator_action"],
            "Restore [REDACTED] and retry",
        )
        redacted_fingerprint = recovery_request_fingerprint(
            recovery["context_id"],
            recovery["graph_revision"],
            recovery["action"],
            recovery["import_attempt_id"],
            recovery["assignment_id"],
            recovery["parameters"],
            recovery["join_target"],
        )
        raw_fingerprint = recovery_request_fingerprint(
            recovery["context_id"],
            recovery["graph_revision"],
            recovery["action"],
            recovery["import_attempt_id"],
            recovery["assignment_id"],
            request["parameters"],
            recovery["join_target"],
        )
        self.assertEqual(recovery["request_fingerprint"], redacted_fingerprint)
        self.assertNotEqual(
            recovery["request_fingerprint"],
            raw_fingerprint,
        )
        self.assertNotIn("durable_request_fingerprint", recovery)
        serialized = canonical_json_bytes(terminal).decode("utf-8")
        self.assertNotIn(secret, serialized)
        self.assertNotIn(raw_fingerprint, serialized)
        self.assertNotIn(
            alternate_secret, serialized
        )
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

        replay = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "recovery-secret-replay",
        )
        self.assertEqual(
            replay.response["produced_record_ids"],
            completed.response["produced_record_ids"],
        )
        self.assertTrue(replay.response["deduplicated"])

        equivalent = deepcopy(request)
        equivalent["parameters"]["operator_action"] = (
            f"Restore {alternate_secret} and retry"
        )
        alternate_replay = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(equivalent),
            "recovery-secret-equivalent-replay",
        )
        self.assertTrue(alternate_replay.response["deduplicated"])

        changed = deepcopy(request)
        changed["parameters"]["operator_action"] = "Use a different recovery plan"
        with self.assertRaises(ManagedContinuityError) as conflict:
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(changed),
                "recovery-secret-conflict",
            )
        self.assertEqual(conflict.exception.code, RECOVERY_CONFLICT)

    def test_unknown_recovery_fingerprint_extension_is_ignored_on_replay(
        self,
    ) -> None:
        secret = "legacy-private-recovery-token"
        self.runtime.secret_values = (secret,)
        self.configure_terminal_result("BLOCKED_EXTERNAL")
        self.submit_result()
        state = self.store.runtime_state("project-id", SPRINT_ID)
        for review in state["review_assignments"]:
            self.runtime.submit_if_managed(
                "project-id",
                review["reviewer_phone"],
                canonical_json_bytes(
                    {"assignment_id": review["assignment_id"], "status": "APPROVE"}
                ),
                "legacy-secret-review",
            )
        identity = self.runtime.current_identity("project-id", "2860")
        request = {
            "assignment_id": identity["assignment"]["context_id"],
            "idempotency_key": "legacy-secret-recovery",
            "action": "BLOCK_EXTERNAL",
            "parameters": {
                "reason_code": "BLOCKED_EXTERNAL",
                "operator_action": f"Restore {secret} and retry",
            },
        }

        def fault(point: str, _context: dict) -> None:
            if point == "after_recovery_pending":
                raise InjectedCrash("after recovery pending")

        self.runtime.fault_injector = fault
        with self.assertRaises(InjectedCrash):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "legacy-secret-pending",
            )
        self.runtime.fault_injector = None
        legacy = self.store.runtime_state("project-id", SPRINT_ID)
        recovery = legacy["recovery_records"][0]
        recovery_id = recovery["recovery_id"]
        recovery["durable_request_fingerprint"] = "0" * 64
        self.assertEqual(managed_activation_invariant_issues(legacy), ())
        self.replace_state(legacy)

        completed = self.runtime.submit_if_managed(
            "project-id",
            "2860",
            canonical_json_bytes(request),
            "legacy-secret-resume",
        )
        self.assertEqual(completed.response["recovery_id"], recovery_id)
        terminal = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(terminal["status"], "blocked")
        self.assertEqual(terminal["recovery_records"][0]["status"], "completed")
        self.assertNotIn(secret, canonical_json_bytes(terminal).decode("utf-8"))
        self.assertEqual(managed_activation_invariant_issues(terminal), ())

    def test_join_apply_repair_records_exact_recheck_boundary(self) -> None:
        state, trigger_ids = prepared_integration_runtime_fixture()
        integration = state["integrations"][0]
        artifact = state["integration_workspaces"][0]
        integration.update(
            {
                "status": "CONFLICT",
                "normalized_error": {"code": "MERGE_CONFLICT"},
            }
        )
        artifact.update(
            {"artifact_status": "preserved", "working_tree_state": "conflicted"}
        )
        context_id = "context-join-conflict"
        workspace_projection = {
            key: artifact[key]
            for key in (
                "workspace_artifact_id", "integration_id", "expected_root",
                "base_commit", "head_commit", "artifact_status",
                "working_tree_state", "verified_at",
            )
        }
        state["coordinator_contexts"] = [
            {
                "context_id": context_id,
                "graph_revision": 1,
                "failure_scope": "integration",
                "reason_code": "JOIN_MERGE_CONFLICT",
                "import_attempt_id": None,
                "failed_assignment_id": None,
                "join_target": {
                    "target_graph_revision": 1,
                    "target_node_id": "join",
                    "trigger_token_ids": trigger_ids,
                    "integration_id": integration["integration_id"],
                },
                "assigned_branch": None,
                "source_commit": None,
                "result_commit": None,
                "diff_summary": {},
                "workspace_status": workspace_projection,
                "process_records": [],
                "port_records": [],
                "test_evidence_summary": {},
                "normalized_error": {"code": "MERGE_CONFLICT"},
                "reviewer_feedback": [],
            }
        ]
        state["outbox"].append(
            {
                "event_id": "event-coordinator-conflict",
                "dedupe_key": f"enqueue:coordinator:{SPRINT_ID}:{context_id}",
                "event_type": "COORDINATOR_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "context_id": context_id,
                    "coordinator_phone": "2860",
                    "reason_code": "JOIN_MERGE_CONFLICT",
                },
                "status": "delivered",
                "created_at": state["integrations"][0]["created_at"],
                "delivered_at": state["integrations"][0]["created_at"],
                "queue_receipt_id": "queue-receipt-coordinator-conflict",
            }
        )
        self.assertEqual(managed_activation_invariant_issues(state), ())
        self.replace_state(state)

        join = next(
            node
            for node in state["graph_revisions"][0]["definition"]["nodes"]
            if node["id"] == "join"
        )
        repaired_join = json.loads(json.dumps(join))
        repaired_join["workspace"]["join_strategy"] = "require_same_commit"
        content = b"repair artifact\n"
        digest = hashlib.sha256(content).hexdigest()
        repair_request = {
            "expected_revision": 1,
            "repair_source_commit": "e" * 40,
            "idempotency_key": "join-repair-1",
            "patch": {
                "future_nodes": [repaired_join],
                "checksum_metadata": [
                    {"path": "orchestration/README.md", "sha256": digest},
                    {"path": "service.py", "sha256": digest},
                ],
            },
        }
        provider = Mock(unsafe=True)
        provider.ensure_mirror.return_value = object()
        provider.assert_commit.return_value = "e" * 40
        provider.read_blob.return_value = content
        identity = self.runtime.current_identity("project-id", "2860")
        request = {
            "assignment_id": identity["assignment"]["context_id"],
            "idempotency_key": "join-recovery-1",
            "action": "APPLY_REPAIR",
            "parameters": {"request": repair_request},
        }
        with patch.object(
            self.importer, "provider_for_durable_repository", return_value=provider
        ):
            result = self.runtime.submit_if_managed(
                "project-id", "2860", canonical_json_bytes(request), "join-recovery"
            )
        self.assertEqual(result.response["status"], "RECOVERY_COMPLETED")
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(recovered["graph_revision"], 2)
        self.assertEqual(recovered["recovery_records"][0]["status"], "completed")
        self.assertEqual(recovered["integrations"][0]["status"], "CONFLICT")
        self.assertEqual(len(recovered["coordinator_contexts"]), 2)
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_pending_join_repair_resumes_and_commits_missing_join_effect(
        self,
    ) -> None:
        context_id, integration_id, repair_request, provider = (
            self.configure_join_conflict_repair(
                repair_key="join-repair-crash-1"
            )
        )
        request = {
            "assignment_id": context_id,
            "idempotency_key": "join-recovery-crash-1",
            "action": "APPLY_REPAIR",
            "parameters": {"request": repair_request},
        }

        def pending_fault(point: str, _context: dict) -> None:
            if point == "after_recovery_pending":
                raise InjectedCrash("after recovery pending")

        self.runtime.fault_injector = pending_fault
        with self.assertRaises(InjectedCrash):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "join-recovery-pending-crash",
            )

        def repair_fault(point: str, _context: dict) -> None:
            if point == "after_repair_commit":
                raise InjectedCrash("after durable repair commit")

        self.runtime.fault_injector = repair_fault
        with (
            patch.object(
                self.importer,
                "provider_for_durable_repository",
                return_value=provider,
            ),
            self.assertRaises(InjectedCrash),
        ):
            self.runtime.repair(
                SPRINT_ID, repair_request, "join-repair-durable-crash"
            )
        self.runtime.fault_injector = None

        interrupted = self.store.runtime_state("project-id", SPRINT_ID)
        repair_id = interrupted["repairs"][0]["repair_id"]
        self.assertEqual(interrupted["recovery_records"][0]["status"], "pending")
        self.assertEqual(len(interrupted["coordinator_contexts"]), 1)
        self.assertEqual(interrupted["integrations"][0]["integration_id"], integration_id)
        self.assertEqual(interrupted["integrations"][0]["status"], "CONFLICT")
        self.assertEqual(managed_activation_invariant_issues(interrupted), ())

        with patch.object(
            self.importer,
            "provider_for_durable_repository",
            side_effect=AssertionError("durable repair must not be fetched again"),
        ):
            resumed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "join-recovery-resume",
            )
        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        produced_ids = resumed.response["produced_record_ids"]
        self.assertEqual(produced_ids[0], repair_id)
        self.assertEqual(len(produced_ids), 3)
        self.assertEqual(recovered["recovery_records"][0]["status"], "completed")
        self.assertEqual(
            recovered["recovery_records"][0]["produced_record_ids"],
            produced_ids,
        )
        self.assertEqual(len(recovered["repairs"]), 1)
        self.assertEqual(len(recovered["coordinator_contexts"]), 2)
        self.assertEqual(recovered["integrations"][0]["status"], "CONFLICT")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_pending_join_repair_adopts_direct_divergence_effect(self) -> None:
        context_id, integration_id, repair_request, provider = (
            self.configure_join_conflict_repair(
                repair_key="join-repair-adopt-divergence-1"
            )
        )
        request = {
            "assignment_id": context_id,
            "idempotency_key": "join-recovery-adopt-divergence-1",
            "action": "APPLY_REPAIR",
            "parameters": {"request": repair_request},
        }

        def pending_fault(point: str, _context: dict) -> None:
            if point == "after_recovery_pending":
                raise InjectedCrash("after recovery pending")

        self.runtime.fault_injector = pending_fault
        with self.assertRaises(InjectedCrash):
            self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "join-recovery-adopt-divergence-pending",
            )
        self.runtime.fault_injector = None

        with patch.object(
            self.importer,
            "provider_for_durable_repository",
            return_value=provider,
        ):
            self.runtime.repair(
                SPRINT_ID,
                repair_request,
                "join-repair-adopt-divergence-direct",
            )

        repaired = self.store.runtime_state("project-id", SPRINT_ID)
        repair_id = repaired["repairs"][0]["repair_id"]
        divergence_context = next(
            item
            for item in repaired["coordinator_contexts"]
            if item.get("reason_code") == "JOIN_SOURCE_DIVERGED"
        )
        divergence_event = next(
            item
            for item in repaired["outbox"]
            if item.get("event_type") == "COORDINATOR_ENQUEUE"
            and item.get("payload", {}).get("context_id")
            == divergence_context["context_id"]
        )
        self.assertEqual(repaired["recovery_records"][0]["status"], "pending")
        self.assertEqual(repaired["integrations"][0]["integration_id"], integration_id)
        self.assertEqual(managed_activation_invariant_issues(repaired), ())

        with patch.object(
            self.importer,
            "provider_for_durable_repository",
            side_effect=AssertionError("durable repair must not be fetched again"),
        ):
            resumed = self.runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(request),
                "join-recovery-adopt-divergence-resume",
            )

        recovered = self.store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(
            resumed.response["produced_record_ids"],
            [
                repair_id,
                divergence_context["context_id"],
                divergence_event["event_id"],
            ],
        )
        self.assertEqual(recovered["recovery_records"][0]["status"], "completed")
        self.assertEqual(len(recovered["repairs"]), 1)
        self.assertEqual(len(recovered["coordinator_contexts"]), 2)
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_same_commit_repair_effect_adoption_requires_exact_assignment_id(
        self,
    ) -> None:
        state, trigger_ids = prepared_integration_runtime_fixture()
        repaired_definition = deepcopy(
            state["graph_revisions"][0]["definition"]
        )
        repaired_join = next(
            item
            for item in repaired_definition["nodes"]
            if item["id"] == "join"
        )
        repaired_join["workspace"]["join_strategy"] = "require_same_commit"
        state["graph_revisions"].append(
            {
                "revision": 2,
                "definition_sha256": hashlib.sha256(
                    canonical_json_bytes(repaired_definition)
                ).hexdigest(),
                "definition": repaired_definition,
                "artifact_source_commit": "e" * 40,
                "created_at": TIMESTAMP,
                "source": "repair",
                "repair_id": "repair-synthetic",
            }
        )
        state["graph_revision"] = 2
        state["workflow"]["graph_revision"] = 2

        common_commit = "c" * 40
        for receipt in state["result_receipts"]:
            receipt["result_commit"] = common_commit
        tokens = {
            item["token_id"]: item
            for item in state["workflow"]["transition_tokens"]
        }
        result_keys = [tokens[token_id]["result_key"] for token_id in trigger_ids]
        occurrence_id = managed_occurrence_id(
            SPRINT_ID, 2, "join", 1, trigger_ids
        )
        assignment_id = _stable_id(
            "assignment",
            SPRINT_ID,
            occurrence_id,
            "join",
            2,
            "accepted_result",
            0,
            ",".join(result_keys),
            compact=True,
        )
        for token_id in trigger_ids:
            tokens[token_id].update(
                {
                    "status": "consumed",
                    "target_graph_revision": 2,
                    "consumed_by_occurrence_id": occurrence_id,
                }
            )
        state["workflow"]["occurrences"].append(
            {
                "occurrence_id": occurrence_id,
                "node_id": "join",
                "graph_revision": 2,
                "generation": 1,
                "activation_policy": "all_parents",
                "trigger_token_ids": trigger_ids,
                "state": "active",
                "assignment_ids": [assignment_id],
                "created_at": TIMESTAMP,
                "completed_at": None,
            }
        )
        state["assignments"].append(
            {
                "assignment_id": assignment_id,
                "occurrence_id": occurrence_id,
                "node_id": "join",
                "graph_revision": 2,
                "source_kind": "accepted_result",
                "source_result_keys": result_keys,
                "source_commit": common_commit,
                "integration_id": None,
                "rework_cycle": 0,
            }
        )
        event_id = _stable_id("event-assignment", SPRINT_ID, assignment_id)
        state["outbox"].append(
            {
                "event_id": event_id,
                "dedupe_key": f"enqueue:assignment:{SPRINT_ID}:{assignment_id}",
                "event_type": "ASSIGNMENT_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "graph_revision": 2,
                    "assignment_id": assignment_id,
                    "node_id": "join",
                    "agent_phone": "2863",
                },
            }
        )
        recovery_context = {
            "join_target": {
                "target_graph_revision": 1,
                "target_node_id": "join",
                "trigger_token_ids": trigger_ids,
            }
        }

        adopted = self.runtime._existing_repaired_join_effect(
            "project-id",
            SPRINT_ID,
            state,
            recovery_context,
            to_revision=2,
            correlation="same-commit-adoption",
        )
        self.assertEqual(adopted, (occurrence_id, assignment_id, event_id))

        inconsistent = deepcopy(state)
        wrong_assignment_id = "assignment-self-consistent-but-not-deterministic"
        inconsistent["workflow"]["occurrences"][-1]["assignment_ids"] = [
            wrong_assignment_id
        ]
        inconsistent["assignments"][-1]["assignment_id"] = wrong_assignment_id
        wrong_event_id = _stable_id(
            "event-assignment", SPRINT_ID, wrong_assignment_id
        )
        inconsistent["outbox"][-1].update(
            {
                "event_id": wrong_event_id,
                "dedupe_key": (
                    f"enqueue:assignment:{SPRINT_ID}:{wrong_assignment_id}"
                ),
            }
        )
        inconsistent["outbox"][-1]["payload"][
            "assignment_id"
        ] = wrong_assignment_id
        with self.assertRaises(ManagedContinuityError) as raised:
            self.runtime._existing_repaired_join_effect(
                "project-id",
                SPRINT_ID,
                inconsistent,
                recovery_context,
                to_revision=2,
                correlation="same-commit-adoption-invalid",
            )
        self.assertEqual(raised.exception.code, "CONTINUITY_RUNTIME_FAILED")
        self.assertEqual(raised.exception.http_status, 503)


async def asgi_request(
    target: str,
    body: bytes,
    *,
    extra_headers: tuple[tuple[bytes, bytes], ...] = (),
    chunks: tuple[bytes, ...] | None = None,
) -> tuple[int, object, bytes]:
    parsed = urllib.parse.urlsplit(target)
    pending_chunks = list(chunks if chunks is not None else (body,))
    messages: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        if not pending_chunks:
            return {"type": "http.disconnect"}
        chunk = pending_chunks.pop(0)
        return {
            "type": "http.request",
            "body": chunk,
            "more_body": bool(pending_chunks),
        }

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "POST", "scheme": "http", "path": parsed.path,
        "raw_path": parsed.path.encode("ascii"),
        "query_string": parsed.query.encode("ascii"), "root_path": "",
        "headers": [
            (b"host", b"testserver"),
            (b"content-type", b"application/json"),
            *extra_headers,
        ],
        "client": ("127.0.0.1", 12345), "server": ("testserver", 80),
    }
    await main.app(scope, receive, send)
    start = next(item for item in messages if item["type"] == "http.response.start")
    raw = b"".join(
        item.get("body", b"")
        for item in messages
        if item["type"] == "http.response.body"
    )
    return int(start["status"]), json.loads(raw), raw


class ManagedContinuityRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_managed_whoami_bypasses_legacy_snapshot(self) -> None:
        runtime = Mock()
        runtime.submit_if_managed.return_value = Mock(
            http_status=200,
            response={"status": "REVIEWS_PENDING", "deduplicated": False},
        )
        with (
            patch.dict(os.environ, {"NGINX_QA_MANAGED_ROOT": "C:/managed-test-root"}),
            patch.object(main, "read_git_config", AsyncMock(return_value={})),
            patch.object(main, "managed_continuity_runtime_for_requests", return_value=runtime),
            patch.object(
                main, "run_group_write_transaction",
                side_effect=AssertionError("legacy state must not be touched"),
            ),
        ):
            status_code, payload, _ = await asgi_request(
                "/api/v1/projects/project-id/agents/2861/whoami",
                b'{"assignment_id":"assignment-1"}',
            )
        self.assertEqual(status_code, 200)
        self.assertEqual(payload["status"], "REVIEWS_PENDING")

    async def test_managed_whoami_resolves_route_to_canonical_project_phone(
        self,
    ) -> None:
        runtime = Mock()
        runtime.submit_if_managed.return_value = Mock(
            http_status=200,
            response={"status": "REVIEWS_PENDING", "deduplicated": False},
        )
        project_entry = {"project_phone": "9002"}
        with (
            patch.dict(os.environ, {"NGINX_QA_MANAGED_ROOT": "C:/managed-test-root"}),
            patch.object(main, "read_git_config", AsyncMock(return_value={})),
            patch.object(
                main,
                "project_for_group_api",
                return_value=("alias", "context", project_entry, {}),
            ),
            patch.object(main, "managed_continuity_runtime_for_requests", return_value=runtime),
            patch.object(
                main, "run_group_write_transaction",
                side_effect=AssertionError("legacy state must not be touched"),
            ),
        ):
            status_code, payload, _ = await asgi_request(
                "/api/v1/projects/alias/agents/2861/whoami",
                b'{"assignment_id":"assignment-1"}',
            )
        self.assertEqual(status_code, 200)
        self.assertEqual(payload["status"], "REVIEWS_PENDING")
        self.assertEqual(runtime.submit_if_managed.call_args.args[0], "9002")

    async def test_unexpected_managed_failure_is_always_json(self) -> None:
        runtime = Mock()
        runtime.submit_if_managed.side_effect = RuntimeError("injected")
        with (
            patch.dict(os.environ, {"NGINX_QA_MANAGED_ROOT": "C:/managed-test-root"}),
            patch.object(main, "read_git_config", AsyncMock(return_value={})),
            patch.object(main, "managed_continuity_runtime_for_requests", return_value=runtime),
        ):
            status_code, payload, raw = await asgi_request(
                "/api/v1/projects/project-id/agents/2861/whoami",
                b'{"assignment_id":"assignment-1"}',
            )
        self.assertEqual(status_code, 500)
        self.assertEqual(payload["detail"]["error"], "CONTINUITY_RUNTIME_FAILED")
        self.assertFalse(raw.lstrip().startswith(b"<"))

    async def test_repair_rejects_declared_and_streamed_oversize_bodies(
        self,
    ) -> None:
        runtime = Mock()
        with patch.object(
            main, "managed_continuity_runtime_for_requests", return_value=runtime
        ):
            declared_status, declared_payload, _ = await asgi_request(
                "/api/v1/sprints/sprint-1/repair",
                b"{}",
                extra_headers=((b"content-length", b"4194305"),),
            )
            streamed_status, streamed_payload, _ = await asgi_request(
                "/api/v1/sprints/sprint-1/repair",
                b"",
                chunks=(b"a" * (4 * 1024 * 1024), b"b"),
            )
        self.assertEqual(declared_status, 400)
        self.assertEqual(
            declared_payload["detail"]["error"], "INVALID_MANAGED_SPRINT_REQUEST"
        )
        self.assertEqual(streamed_status, 400)
        self.assertEqual(
            streamed_payload["detail"]["error"], "INVALID_MANAGED_SPRINT_REQUEST"
        )
        runtime.repair.assert_not_called()


if __name__ == "__main__":
    unittest.main()
