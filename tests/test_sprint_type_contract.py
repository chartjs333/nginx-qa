import copy
import json
import unittest
from pathlib import Path
from urllib.parse import urldefrag, urljoin

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from referencing import Registry, Resource

from nginx_qa.sprint_types import (
    ASSIGNMENT_STATE_TRANSITIONS,
    BRANCH_LEASE_STATE_TRANSITIONS,
    MANAGED_MANIFEST_SCHEMA,
    OUTBOX_STATE_TRANSITIONS,
    PORT_LEASE_STATE_TRANSITIONS,
    PROCESS_STATE_TRANSITIONS,
    REVIEW_ASSIGNMENT_STATE_TRANSITIONS,
    SPRINT_TYPE_UNSUPPORTED,
    TRANSITION_JOURNAL_STATES,
    SprintPipeline,
    SprintProvenance,
    SprintType,
    SprintTypeUnsupported,
    StartSprintFromGitRequest,
    canonical_json_bytes,
    canonical_json_sha256,
    canonical_tuple_sha256,
    git_ref_format_valid,
    journal_advance_allowed,
    lifecycle_transition_allowed,
    managed_activation_invariant_issues,
    managed_graph_semantic_issues,
    managed_integration_id,
    managed_integration_workspace_id,
    managed_manifest_schema,
    managed_occurrence_id,
    managed_project_control_invariant_issues,
    managed_runtime_config_invariant_issues,
    managed_sprint_path_segment,
    managed_result_key,
    managed_transition_token_id,
    mirror_storage_key,
    process_transition_allowed,
    recovery_request_fingerprint,
    relative_git_path_valid,
    repair_request_fingerprint,
    resolve_sprint_type,
    review_request_fingerprint,
    windows_absolute_path_key,
    windows_path_is_within,
    windows_path_segment_valid,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "schemas"
COMMIT = "a" * 40
MANIFEST_SHA = "b" * 64
SPRINT_ID = "msv1-5f55f42e54a6ffccd983edbe38830972dc3b8e57ad82323cdb41f3008faa37f6"
SPRINT_PATH_SEGMENT = managed_sprint_path_segment(SPRINT_ID)
TIMESTAMP = "2026-10-01T12:00:00+00:00"
REPOSITORY_KEY = "github.com/owner/repository"
MIRROR_KEY = mirror_storage_key(REPOSITORY_KEY)
WORKSPACE_1_ROOT = (
    "D:/nginx-qa-staging/managed/projects/project-id/sprints/"
    f"{SPRINT_PATH_SEGMENT}/nodes/build/assignment-1"
)
EXPECTED_SCHEMA_FILES = {
    "managed-api-error-v1.schema.json",
    "managed-assignment-result-response-v1.schema.json",
    "managed-assignment-result-v1.schema.json",
    "managed-project-control-v1.schema.json",
    "managed-review-decision-response-v1.schema.json",
    "managed-review-decision-v1.schema.json",
    "managed-runtime-config-v1.schema.json",
    "managed-runtime-state-v1.schema.json",
    "managed-runtime-state-v2.schema.json",
    "managed-workspace-sprint-v1.schema.json",
    "repair-sprint-response-v1.schema.json",
    "repair-sprint-v1.schema.json",
    "sprint-dispatch-v1.schema.json",
    "sprint-preflight-report-v1.schema.json",
    "start-sprint-from-git-response-v1.schema.json",
    "start-sprint-from-git-v1.schema.json",
}


def managed_manifest_fixture() -> dict:
    return {
        "schema_version": 1,
        "sprint_type": "managed_workspace_v1",
        "git_address": "https://github.com/owner/repository.git",
        "git": {
            "source_ref": "refs/heads/main",
            "expected_source_commit": None,
            "assigned_branch": "agent/example",
            "existing_branch_policy": "resume",
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
                "agent": {"id": "builder", "name": "Builder", "phone": "2861"},
                "tasks": [
                    {
                        "task_id": "BUILD-1",
                        "queue": "worker-all",
                        "message": "Build",
                    }
                ],
                "workspace": {
                    "access": "write",
                    "process": {
                        "command": ["python", "service.py"],
                        "cwd": ".",
                        "environment": {"APP_MODE": "test"},
                        "health_path": "/health",
                        "restart_policy": "never",
                        "resource_limits": {},
                    },
                },
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
            "routes": {"STOP": "continuity", "NEED_DECISION": "continuity"},
        },
        "files": [
            {"path": "orchestration/README.md", "sha256": "c" * 64},
            {"path": "service.py", "sha256": "d" * 64},
        ],
    }


def identity_fixture() -> dict:
    return {
        "project_id": "project-id",
        "repository_id": "main",
        "commit": COMMIT,
        "manifest_path": "orchestration/sprint.json",
        "manifest_sha256": MANIFEST_SHA,
    }


def start_response_fixture() -> dict:
    return {
        "sprint_id": SPRINT_ID,
        "status": "active",
        "phase": "ACTIVATE",
        "deduplicated": False,
        "execution_mode": "sequential",
        "identity": identity_fixture(),
        "workspace_source_commit": COMMIT,
        "initial_assignment_ids": ["assignment-1"],
    }


def preflight_fixture() -> dict:
    lease_snapshot = {
        "branch_leases": [],
        "port_leases": [],
        "process_owners": [],
    }
    return {
        "schema_version": 1,
        "contract_version": 1,
        "manifest_schema_version": 1,
        "sprint_id": SPRINT_ID,
        "manifest_commit": COMMIT,
        "workspace_source_commit": COMMIT,
        "manifest_sha256": MANIFEST_SHA,
        "lease_snapshot": lease_snapshot,
        "lease_snapshot_sha256": canonical_json_sha256(lease_snapshot),
        "ok": True,
        "phase": "VALIDATE",
        "issues": [],
        "checked_at": TIMESTAMP,
    }


def active_runtime_fixture(*, include_process_definition: bool = False) -> dict:
    manifest = managed_manifest_fixture()
    if not include_process_definition:
        manifest["nodes"][0]["workspace"].pop("process", None)
    occurrence_id = managed_occurrence_id(SPRINT_ID, 1, "build", 1, [])
    return {
        "schema_version": 1,
        "contract_version": 1,
        "manifest_schema_version": 1,
        "sprint_type": "managed_workspace_v1",
        "sprint_id": SPRINT_ID,
        "identity": identity_fixture(),
        "workspace_source_commit": COMMIT,
        "requested_ref": "refs/heads/sprint-definition",
        "repository": {
            "repository_id": "main",
            "repository_key": REPOSITORY_KEY,
            "canonical_remote": REPOSITORY_KEY,
            "mirror_storage_key": MIRROR_KEY,
            "mirror_path": (
                f"D:/nginx-qa-staging/managed/repositories/{MIRROR_KEY}.git"
            ),
            "credential_reference": None,
        },
        "runtime_config": runtime_config_fixture(),
        "status": "active",
        "graph_revision": 1,
        "graph_revisions": [
            {
                "revision": 1,
                "definition_sha256": canonical_json_sha256(manifest),
                "definition": manifest,
                "artifact_source_commit": COMMIT,
                "created_at": TIMESTAMP,
                "source": "activation",
                "repair_id": None,
            }
        ],
        "workflow": {
            "graph_revision": 1,
            "execution_mode": "sequential",
            "entry_node_ids": ["build"],
            "entry_occurrence_ids": [occurrence_id],
            "occurrences": [
                {
                    "occurrence_id": occurrence_id,
                    "node_id": "build",
                    "graph_revision": 1,
                    "generation": 1,
                    "activation_policy": "entry",
                    "trigger_token_ids": [],
                    "state": "active",
                    "assignment_ids": ["assignment-1"],
                    "created_at": TIMESTAMP,
                    "completed_at": None,
                }
            ],
            "transition_tokens": [],
        },
        "active_assignment_ids": ["assignment-1"],
        "allowed_outcomes_by_assignment": {
            "assignment-1": ["DONE", "STOP", "NEED_DECISION"]
        },
        "import_attempts": [
            {
                "attempt_id": "attempt-1",
                "idempotency_key": "key-1",
                "request_fingerprint": canonical_tuple_sha256(
                    "project-id",
                    "main",
                    "refs/heads/sprint-definition",
                    "orchestration/sprint.json",
                ),
                "identity": identity_fixture(),
                "contract_version": 1,
                "manifest_schema_version": 1,
                "phase": "ACTIVATE",
                "status": "succeeded",
                "preflight": preflight_fixture(),
                "prepared_artifact_ids": ["workspace-1"],
                "activation_response": start_response_fixture(),
                "created_at": TIMESTAMP,
                "updated_at": TIMESTAMP,
            }
        ],
        "assignments": [
            {
                "assignment_id": "assignment-1",
                "occurrence_id": occurrence_id,
                "node_id": "build",
                "agent_id": "builder",
                "agent_phone": "2861",
                "graph_revision": 1,
                "source_kind": "sprint_source",
                "source_result_keys": [],
                "source_commit": COMMIT,
                "initial_head_commit": COMMIT,
                "integration_id": None,
                "rework_cycle": 0,
                "result_commit": None,
                "outcome": None,
                "status": "active",
                "workspace_id": "workspace-1",
                "branch_lease_id": "branch-lease-1",
                "allowed_outcomes": ["DONE", "STOP", "NEED_DECISION"],
                "created_at": TIMESTAMP,
                "completed_at": None,
            }
        ],
        "result_receipts": [],
        "review_assignments": [],
        "reviews": [],
        "reworks": [],
        "integrations": [],
        "integration_workspaces": [],
        "branch_leases": [
            {
                "lease_id": "branch-lease-1",
                "repository_id": "main",
                "repository_key": REPOSITORY_KEY,
                "mirror_storage_key": MIRROR_KEY,
                "branch": "agent/example",
                "assignment_id": "assignment-1",
                "source_commit": COMMIT,
                "initial_head_commit": COMMIT,
                "mode": "write",
                "status": "active",
                "acquired_at": TIMESTAMP,
                "released_at": None,
            }
        ],
        "workspaces": [
            {
                "workspace_id": "workspace-1",
                "project_id": "project-id",
                "sprint_id": SPRINT_ID,
                "node_id": "build",
                "assignment_id": "assignment-1",
                "expected_root": WORKSPACE_1_ROOT,
                "actual_git_toplevel": WORKSPACE_1_ROOT,
                "actual_git_dir": WORKSPACE_1_ROOT + "/.git",
                "repository_id": "main",
                "repository_remote": REPOSITORY_KEY,
                "source_commit": COMMIT,
                "initial_head_commit": COMMIT,
                "assigned_branch": "agent/example",
                "working_tree_state": "clean",
            }
        ],
        "port_leases": [],
        "processes": [],
        "transition_journal": [],
        "outbox": [
            {
                "event_id": "event-1",
                "dedupe_key": f"enqueue:assignment:{SPRINT_ID}:assignment-1",
                "event_type": "ASSIGNMENT_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "graph_revision": 1,
                    "assignment_id": "assignment-1",
                    "node_id": "build",
                    "agent_phone": "2861",
                },
                "status": "delivered",
                "created_at": TIMESTAMP,
                "delivered_at": TIMESTAMP,
                "queue_receipt_id": "queue-receipt-1",
            }
        ],
        "repairs": [],
        "coordinator_contexts": [],
        "blocker_observations": [],
        "recovery_records": [],
    }


def completed_runtime_fixture() -> dict:
    state = active_runtime_fixture()
    assignment = state["assignments"][0]
    occurrence = state["workflow"]["occurrences"][0]
    result_key = managed_result_key("assignment-1", "DONE", COMMIT)
    token_id = managed_transition_token_id(
        SPRINT_ID,
        occurrence["occurrence_id"],
        result_key,
        "completed",
    )
    state["status"] = "completed"
    state["active_assignment_ids"] = []
    state["allowed_outcomes_by_assignment"] = {}
    assignment.update(
        {
            "result_commit": COMMIT,
            "outcome": "DONE",
            "status": "completed",
            "completed_at": TIMESTAMP,
        }
    )
    occurrence.update({"state": "completed", "completed_at": TIMESTAMP})
    state["branch_leases"][0].update(
        {"status": "released", "released_at": TIMESTAMP}
    )
    result_response = {
        "assignment_id": "assignment-1",
        "outcome": "DONE",
        "result_commit": COMMIT,
        "result_key": result_key,
        "status": "REVIEWS_PENDING",
        "deduplicated": False,
    }
    state["result_receipts"] = [
        {
            "result_key": result_key,
            "assignment_id": "assignment-1",
            "outcome": "DONE",
            "result_commit": COMMIT,
            "from_commit": COMMIT,
            "git_branch": "agent/example",
            "result_summary_redacted": "Implemented and tested",
            "redaction_applied": False,
            "request_fingerprint": "1" * 64,
            "response": result_response,
            "accepted_at": TIMESTAMP,
        }
    ]
    review_assignments = []
    reviews = []
    for index, reviewer in enumerate(
        managed_manifest_fixture()["execution"]["reviewers"], start=1
    ):
        review_assignment_id = f"review-assignment-{index}"
        request_fingerprint = review_request_fingerprint(
            review_assignment_id,
            "APPROVE",
            "",
        )
        review_response = {
            "assignment_id": review_assignment_id,
            "source_assignment_id": "assignment-1",
            "result_key": result_key,
            "decision": "APPROVE",
            "status": "REVIEW_ACCEPTED",
            "deduplicated": False,
        }
        review_assignments.append(
            {
                "assignment_id": review_assignment_id,
                "source_assignment_id": "assignment-1",
                "result_key": result_key,
                "result_commit": COMMIT,
                "result_outcome": "DONE",
                "reviewer_id": reviewer["id"],
                "reviewer_phone": reviewer["phone"],
                "reviewer_index": index,
                "status": "decided",
                "decision": "APPROVE",
                "request_fingerprint": request_fingerprint,
                "response": review_response,
                "created_at": TIMESTAMP,
                "activated_at": TIMESTAMP,
                "decided_at": TIMESTAMP,
            }
        )
        reviews.append(
            {
                "assignment_id": review_assignment_id,
                "source_assignment_id": "assignment-1",
                "result_key": result_key,
                "result_commit": COMMIT,
                "result_outcome": "DONE",
                "reviewer_id": reviewer["id"],
                "reviewer_index": index,
                "decision": "APPROVE",
                "feedback": "",
                "request_fingerprint": request_fingerprint,
                "response": review_response,
                "decided_at": TIMESTAMP,
            }
        )
    state["review_assignments"] = review_assignments
    state["reviews"] = reviews
    state["outbox"].extend(
        {
            "event_id": f"review-event-{review_assignment['reviewer_index']}",
            "dedupe_key": (
                f"enqueue:review:{SPRINT_ID}:{result_key}:"
                f"{review_assignment['reviewer_index']}"
            ),
            "event_type": "REVIEW_ENQUEUE",
            "payload": {
                "sprint_id": SPRINT_ID,
                "source_assignment_id": "assignment-1",
                "review_assignment_id": review_assignment["assignment_id"],
                "result_key": result_key,
                "reviewer_phone": review_assignment["reviewer_phone"],
                "reviewer_index": review_assignment["reviewer_index"],
            },
            "status": "delivered",
            "created_at": TIMESTAMP,
            "delivered_at": TIMESTAMP,
            "queue_receipt_id": (
                f"review-queue-receipt-{review_assignment['reviewer_index']}"
            ),
        }
        for review_assignment in review_assignments
    )
    state["transition_journal"] = [
        {
            "journal_id": "journal-1",
            "result_key": result_key,
            "assignment_id": "assignment-1",
            "outcome": "DONE",
            "result_commit": COMMIT,
            "state": "TRANSITION_COMMITTED",
            "disposition": "accepted",
            "rework_id": None,
            "transition_token_ids": [token_id],
            "target_occurrence_ids": [],
            "outbox_event_ids": [],
            "updated_at": TIMESTAMP,
        }
    ]
    state["workflow"]["transition_tokens"] = [
        {
            "token_id": token_id,
            "source_occurrence_id": occurrence["occurrence_id"],
            "source_node_id": "build",
            "source_graph_revision": 1,
            "result_key": result_key,
            "target_node_id": "completed",
            "target_graph_revision": 1,
            "status": "terminal",
            "consumed_by_occurrence_id": None,
            "created_at": TIMESTAMP,
        }
    ]
    return state


def pending_coordinator_runtime_fixture() -> dict:
    state = completed_runtime_fixture()
    assignment = state["assignments"][0]
    occurrence = state["workflow"]["occurrences"][0]
    result_key = managed_result_key("assignment-1", "STOP", COMMIT)
    token_id = managed_transition_token_id(
        SPRINT_ID,
        occurrence["occurrence_id"],
        result_key,
        "continuity",
    )
    state["status"] = "active"
    assignment["outcome"] = "STOP"
    receipt = state["result_receipts"][0]
    receipt.update({"result_key": result_key, "outcome": "STOP"})
    receipt["response"].update({"result_key": result_key, "outcome": "STOP"})
    for review_assignment in state["review_assignments"]:
        review_assignment.update(
            {"result_key": result_key, "result_outcome": "STOP"}
        )
        review_assignment["response"]["result_key"] = result_key
    for review in state["reviews"]:
        review.update({"result_key": result_key, "result_outcome": "STOP"})
        review["response"]["result_key"] = result_key
    for event in state["outbox"]:
        if event["event_type"] == "REVIEW_ENQUEUE":
            reviewer_index = event["payload"]["reviewer_index"]
            event["dedupe_key"] = (
                f"enqueue:review:{SPRINT_ID}:{result_key}:{reviewer_index}"
            )
            event["payload"]["result_key"] = result_key
    state["transition_journal"][0].update(
        {
            "result_key": result_key,
            "outcome": "STOP",
            "transition_token_ids": [token_id],
        }
    )
    state["workflow"]["transition_tokens"][0].update(
        {
            "token_id": token_id,
            "result_key": result_key,
            "target_node_id": "continuity",
            "target_graph_revision": None,
            "status": "available",
        }
    )
    return state


def apply_moved_coordinator_repair(state: dict) -> None:
    """Move reserved routes while retaining the old Coordinator as a future node."""

    previous_definition = state["graph_revisions"][0]["definition"]
    retired_coordinator = copy.deepcopy(previous_definition["nodes"][1])
    retired_coordinator.update(
        {
            "agent": {
                "id": "retired-coordinator",
                "name": "Retired Coordinator",
                "phone": "2864",
            },
            "tasks": [
                {
                    "task_id": "COORD-OLD",
                    "queue": "consultant-all",
                    "message": "Recover old",
                }
            ],
        }
    )
    new_coordinator = {
        "id": "continuity2",
        "agent": {
            "id": "coordinator",
            "name": "Coordinator",
            "phone": "2860",
        },
        "tasks": [
            {
                "task_id": "COORD-2",
                "queue": "consultant-all",
                "message": "Recover new",
            }
        ],
        "workspace": {"access": "read"},
        "activation_policy": "any_parent",
        "transitions": {
            "RESUME": "continuity",
            "BLOCKED_EXTERNAL": "completed",
        },
    }
    coordinator_routing = {
        "node_id": "continuity2",
        "routes": {
            "STOP": "continuity2",
            "NEED_DECISION": "continuity2",
        },
    }
    patch = {
        "future_nodes": [retired_coordinator, new_coordinator],
        "coordinator_routing": coordinator_routing,
    }
    repaired_definition = copy.deepcopy(previous_definition)
    repaired_definition["nodes"][1] = retired_coordinator
    repaired_definition["nodes"].append(new_coordinator)
    repaired_definition["coordinator"] = coordinator_routing
    repair_id = "repair-coordinator"
    repair_commit = "f" * 40
    state["repairs"] = [
        {
            "repair_id": repair_id,
            "from_revision": 1,
            "to_revision": 2,
            "repair_source_commit": repair_commit,
            "idempotency_key": "repair-coordinator-key",
            "request_fingerprint": repair_request_fingerprint(
                SPRINT_ID, 1, repair_commit, patch
            ),
            "patch": patch,
            "response": {
                "sprint_id": SPRINT_ID,
                "from_revision": 1,
                "graph_revision": 2,
                "repair_source_commit": repair_commit,
                "deduplicated": False,
            },
            "created_at": TIMESTAMP,
        }
    ]
    state["graph_revisions"].append(
        {
            "revision": 2,
            "definition_sha256": canonical_json_sha256(repaired_definition),
            "definition": repaired_definition,
            "artifact_source_commit": repair_commit,
            "created_at": TIMESTAMP,
            "source": "repair",
            "repair_id": repair_id,
        }
    )
    state["graph_revision"] = 2
    state["workflow"]["graph_revision"] = 2


def repaired_runtime_fixture(
    *,
    agent_id: str = "coordinator",
    profile: str = "repaired-profile",
) -> dict:
    state = active_runtime_fixture()
    previous_definition = state["graph_revisions"][0]["definition"]
    definition = copy.deepcopy(previous_definition)
    for node in definition["nodes"]:
        agent = node.get("agent")
        if isinstance(agent, dict) and agent.get("id") == agent_id:
            agent["profile"] = profile
            break
    patch = {"profiles": {agent_id: profile}}
    repair_commit = "f" * 40
    repair_id = "repair-1"
    state["graph_revision"] = 2
    state["workflow"]["graph_revision"] = 2
    state["repairs"] = [
        {
            "repair_id": repair_id,
            "from_revision": 1,
            "to_revision": 2,
            "repair_source_commit": repair_commit,
            "idempotency_key": "repair-key",
            "request_fingerprint": repair_request_fingerprint(
                SPRINT_ID,
                1,
                repair_commit,
                patch,
            ),
            "patch": patch,
            "response": {
                "sprint_id": SPRINT_ID,
                "from_revision": 1,
                "graph_revision": 2,
                "repair_source_commit": repair_commit,
                "deduplicated": False,
            },
            "created_at": TIMESTAMP,
        }
    ]
    state["graph_revisions"].append(
        {
            "revision": 2,
            "definition_sha256": canonical_json_sha256(definition),
            "definition": definition,
            "artifact_source_commit": repair_commit,
            "created_at": TIMESTAMP,
            "source": "repair",
            "repair_id": repair_id,
        }
    )
    return state


def successor_runtime_fixture() -> dict:
    state = completed_runtime_fixture()
    definition = state["graph_revisions"][0]["definition"]
    definition["nodes"][0]["transitions"] = {"DONE": "continuity"}
    state["graph_revisions"][0]["definition_sha256"] = canonical_json_sha256(
        definition
    )
    source_occurrence = state["workflow"]["occurrences"][0]
    result_key = state["result_receipts"][0]["result_key"]
    token_id = managed_transition_token_id(
        SPRINT_ID,
        source_occurrence["occurrence_id"],
        result_key,
        "continuity",
    )
    target_occurrence_id = managed_occurrence_id(
        SPRINT_ID,
        1,
        "continuity",
        1,
        [token_id],
    )
    target_occurrence = {
        "occurrence_id": target_occurrence_id,
        "node_id": "continuity",
        "graph_revision": 1,
        "generation": 1,
        "activation_policy": "any_parent",
        "trigger_token_ids": [token_id],
        "state": "active",
        "assignment_ids": ["assignment-2"],
        "created_at": TIMESTAMP,
        "completed_at": None,
    }
    state["status"] = "active"
    state["active_assignment_ids"] = ["assignment-2"]
    state["allowed_outcomes_by_assignment"] = {
        "assignment-2": ["RESUME", "BLOCKED_EXTERNAL", "STOP", "NEED_DECISION"]
    }
    state["workflow"]["occurrences"].append(target_occurrence)
    state["workflow"]["transition_tokens"] = [
        {
            "token_id": token_id,
            "source_occurrence_id": source_occurrence["occurrence_id"],
            "source_node_id": "build",
            "source_graph_revision": 1,
            "result_key": result_key,
            "target_node_id": "continuity",
            "target_graph_revision": 1,
            "status": "consumed",
            "consumed_by_occurrence_id": target_occurrence_id,
            "created_at": TIMESTAMP,
        }
    ]
    state["assignments"].append(
        {
            "assignment_id": "assignment-2",
            "occurrence_id": target_occurrence_id,
            "node_id": "continuity",
            "agent_id": "coordinator",
            "agent_phone": "2860",
            "graph_revision": 1,
            "source_kind": "accepted_result",
            "source_result_keys": [result_key],
            "source_commit": COMMIT,
            "initial_head_commit": COMMIT,
            "integration_id": None,
            "rework_cycle": 0,
            "result_commit": None,
            "outcome": None,
            "status": "active",
            "workspace_id": "workspace-2",
            "branch_lease_id": None,
            "allowed_outcomes": [
                "RESUME",
                "BLOCKED_EXTERNAL",
                "STOP",
                "NEED_DECISION",
            ],
            "created_at": TIMESTAMP,
            "completed_at": None,
        }
    )
    workspace_root = (
        "D:/nginx-qa-staging/managed/projects/project-id/sprints/"
        f"{SPRINT_PATH_SEGMENT}/"
        "nodes/continuity/assignment-2"
    )
    state["workspaces"].append(
        {
            "workspace_id": "workspace-2",
            "project_id": "project-id",
            "sprint_id": SPRINT_ID,
            "node_id": "continuity",
            "assignment_id": "assignment-2",
            "expected_root": workspace_root,
            "actual_git_toplevel": workspace_root,
            "actual_git_dir": f"{workspace_root}/.git",
            "repository_id": "main",
            "repository_remote": REPOSITORY_KEY,
            "source_commit": COMMIT,
            "initial_head_commit": COMMIT,
            "assigned_branch": None,
            "working_tree_state": "clean",
        }
    )
    assignment_event_id = "event-2"
    state["outbox"].append(
        {
            "event_id": assignment_event_id,
            "dedupe_key": f"enqueue:assignment:{SPRINT_ID}:assignment-2",
            "event_type": "ASSIGNMENT_ENQUEUE",
            "payload": {
                "sprint_id": SPRINT_ID,
                "graph_revision": 1,
                "assignment_id": "assignment-2",
                "node_id": "continuity",
                "agent_phone": "2860",
            },
            "status": "delivered",
            "created_at": TIMESTAMP,
            "delivered_at": TIMESTAMP,
            "queue_receipt_id": "queue-receipt-2",
        }
    )
    state["transition_journal"][0].update(
        {
            "state": "NEXT_ASSIGNMENT_ENQUEUED",
            "transition_token_ids": [token_id],
            "target_occurrence_ids": [target_occurrence_id],
            "outbox_event_ids": [assignment_event_id],
        }
    )
    return state


def pending_successor_runtime_fixture() -> dict:
    state = successor_runtime_fixture()
    state["workflow"]["occurrences"] = state["workflow"]["occurrences"][:1]
    state["workflow"]["transition_tokens"][0].update(
        {
            "status": "available",
            "target_graph_revision": None,
            "consumed_by_occurrence_id": None,
        }
    )
    state["assignments"] = state["assignments"][:1]
    state["workspaces"] = state["workspaces"][:1]
    state["outbox"] = [
        event for event in state["outbox"] if event["event_id"] != "event-2"
    ]
    state["active_assignment_ids"] = []
    state["allowed_outcomes_by_assignment"] = {}
    state["transition_journal"][0].update(
        {
            "state": "TRANSITION_COMMITTED",
            "target_occurrence_ids": [],
            "outbox_event_ids": [],
        }
    )
    return state


def project_control_fixture() -> dict:
    return {
        "schema_version": 1,
        "project_id": "project-id",
        "active_sprint_id": SPRINT_ID,
        "activation_fencing_counter": 1,
        "activation_lease": None,
        "start_idempotency_records": [
            {
                "idempotency_key": "start-key",
                "request_fingerprint": "e" * 64,
                "attempt_id": "attempt-1",
                "recovery_of_attempt_id": None,
                "recovery_generation": 0,
                "created_fencing_token": 1,
                "fencing_token": 1,
                "pinned_identity": identity_fixture(),
                "workspace_source_commit": COMMIT,
                "sprint_id": SPRINT_ID,
                "status": "SUCCEEDED",
                "response": start_response_fixture(),
                "http_status": 201,
                "error": None,
                "evidence": {},
                "created_at": TIMESTAMP,
                "updated_at": TIMESTAMP,
            }
        ],
    }


def runtime_config_fixture() -> dict:
    return {
        "http_host": "127.0.0.1",
        "http_port": 18025,
        "service_root": "D:/nginx-qa-staging/universal-managed-sprint-engine",
        "protected_roots": ["D:/nginx-qa"],
        "runtime_root": "D:/nginx-qa-staging/runtime",
        "process_runtime_root": "D:/nginx-qa-staging/runtime/processes",
        "log_root": "D:/nginx-qa-staging/runtime/logs",
        "pid_root": "D:/nginx-qa-staging/runtime/pids",
        "lease_root": "D:/nginx-qa-staging/runtime/leases",
        "prompt_root": "D:/nginx-qa-staging/prompts",
        "managed_root": "D:/nginx-qa-staging/managed",
        "git_fetch_timeout_seconds": 120,
        "child_port_start": 18100,
        "child_port_end": 18199,
        "instance_id": "umse-staging",
        "disable_telegram": True,
        "disable_tunnel": True,
    }


def process_fixture() -> dict:
    return {
        "process_id": "process-1",
        "assignment_id": "assignment-1",
        "workspace_id": "workspace-1",
        "runtime_root": "D:/nginx-qa-staging/runtime/processes/process-1",
        "state": "PREPARED",
        "launch_nonce": "nonce-0123456789ab",
        "os_process_created_at": None,
        "os_process_birth_token": None,
        "executable_path": "C:/Python/python.exe",
        "job_object_id": None,
        "pid": None,
        "process_group_id": None,
        "command_redacted": ["python", "service.py"],
        "environment_redacted": {"APP_MODE": "test"},
        "cwd": WORKSPACE_1_ROOT,
        "stdout_log": "D:/nginx-qa-staging/runtime/logs/process-1.stdout.log",
        "stderr_log": "D:/nginx-qa-staging/runtime/logs/process-1.stderr.log",
        "health_endpoint": {"host": "127.0.0.1", "port": 18100, "path": "/health"},
        "resource_limits": {
            "wall_time_seconds": 3600,
            "memory_bytes": 2147483648,
            "cpu_percent": 100,
            "process_count": 16,
        },
        "restart_policy": "on_failure",
        "restart_attempt": 0,
        "restart_of_process_id": None,
        "max_restart_attempts": 3,
        "restart_backoff_seconds": 5,
        "port_lease_id": "port-lease-1",
        "started_at": None,
        "startup_deadline_at": None,
        "stopped_at": None,
        "failed_at": None,
        "terminal_reason": None,
    }


def prepared_integration_runtime_fixture() -> tuple[dict, list[str]]:
    state = active_runtime_fixture()
    definition = state["graph_revisions"][0]["definition"]
    definition["nodes"][0]["transitions"] = {"DONE": "join"}
    definition["nodes"].insert(
        1,
        {
            "id": "lint",
            "agent": {"id": "linter", "name": "Linter", "phone": "2862"},
            "tasks": [
                {"task_id": "LINT-1", "queue": "worker-all", "message": "Lint"}
            ],
            "workspace": {"access": "read"},
            "transitions": {"DONE": "join"},
        },
    )
    definition["nodes"].insert(
        2,
        {
            "id": "join",
            "agent": {"id": "joiner", "name": "Joiner", "phone": "2863"},
            "tasks": [
                {"task_id": "JOIN-1", "queue": "worker-all", "message": "Join"}
            ],
            "workspace": {"access": "read", "join_strategy": "merge_no_ff"},
            "activation_policy": "all_parents",
            "join_parent_order": ["lint", "build"],
            "transitions": {"DONE": "completed"},
        },
    )
    definition["execution"].pop("start_node")
    definition["execution"].update(
        {"mode": "parallel", "start_nodes": ["build", "lint"]}
    )
    state["graph_revisions"][0]["definition_sha256"] = canonical_json_sha256(
        definition
    )

    build_occurrence = state["workflow"]["occurrences"][0]
    build_occurrence.update({"state": "completed", "completed_at": TIMESTAMP})
    lint_occurrence_id = managed_occurrence_id(SPRINT_ID, 1, "lint", 1, [])
    state["workflow"]["occurrences"].append(
        {
            "occurrence_id": lint_occurrence_id,
            "node_id": "lint",
            "graph_revision": 1,
            "generation": 1,
            "activation_policy": "entry",
            "trigger_token_ids": [],
            "state": "completed",
            "assignment_ids": ["lint-assignment"],
            "created_at": TIMESTAMP,
            "completed_at": TIMESTAMP,
        }
    )
    state["workflow"].update(
        {
            "execution_mode": "parallel",
            "entry_node_ids": ["build", "lint"],
            "entry_occurrence_ids": [
                build_occurrence["occurrence_id"],
                lint_occurrence_id,
            ],
        }
    )
    build_commit = "c" * 40
    lint_commit = "d" * 40
    build_result_key = managed_result_key("assignment-1", "DONE", build_commit)
    lint_result_key = managed_result_key("lint-assignment", "DONE", lint_commit)

    state["assignments"][0].update(
        {
            "result_commit": build_commit,
            "outcome": "DONE",
            "status": "completed",
            "completed_at": TIMESTAMP,
        }
    )
    state["assignments"].append(
        {
            "assignment_id": "lint-assignment",
            "occurrence_id": lint_occurrence_id,
            "node_id": "lint",
            "agent_id": "linter",
            "agent_phone": "2862",
            "graph_revision": 1,
            "source_kind": "sprint_source",
            "source_result_keys": [],
            "source_commit": COMMIT,
            "initial_head_commit": COMMIT,
            "integration_id": None,
            "rework_cycle": 0,
            "result_commit": lint_commit,
            "outcome": "DONE",
            "status": "completed",
            "workspace_id": "workspace-lint",
            "branch_lease_id": None,
            "allowed_outcomes": ["DONE", "STOP", "NEED_DECISION"],
            "created_at": TIMESTAMP,
            "completed_at": TIMESTAMP,
        }
    )
    lint_workspace_root = (
        "D:/nginx-qa-staging/managed/projects/project-id/sprints/"
        f"{SPRINT_PATH_SEGMENT}/nodes/lint/lint-assignment"
    )
    state["workspaces"].append(
        {
            "workspace_id": "workspace-lint",
            "project_id": "project-id",
            "sprint_id": SPRINT_ID,
            "node_id": "lint",
            "assignment_id": "lint-assignment",
            "expected_root": lint_workspace_root,
            "actual_git_toplevel": lint_workspace_root,
            "actual_git_dir": lint_workspace_root + "/.git",
            "repository_id": "main",
            "repository_remote": REPOSITORY_KEY,
            "source_commit": COMMIT,
            "initial_head_commit": COMMIT,
            "assigned_branch": None,
            "working_tree_state": "clean",
        }
    )
    state["branch_leases"][0].update(
        {"status": "released", "released_at": TIMESTAMP}
    )
    state["active_assignment_ids"] = []
    state["allowed_outcomes_by_assignment"] = {}
    state["outbox"].append(
        {
            "event_id": "event-lint-assignment",
            "dedupe_key": f"enqueue:assignment:{SPRINT_ID}:lint-assignment",
            "event_type": "ASSIGNMENT_ENQUEUE",
            "payload": {
                "sprint_id": SPRINT_ID,
                "graph_revision": 1,
                "assignment_id": "lint-assignment",
                "node_id": "lint",
                "agent_phone": "2862",
            },
            "status": "delivered",
            "created_at": TIMESTAMP,
            "delivered_at": TIMESTAMP,
            "queue_receipt_id": "queue-receipt-lint-assignment",
        }
    )
    activation_response = state["import_attempts"][0]["activation_response"]
    activation_response.update(
        {
            "execution_mode": "parallel",
            "initial_assignment_ids": ["assignment-1", "lint-assignment"],
        }
    )
    state["import_attempts"][0]["prepared_artifact_ids"] = [
        "workspace-1",
        "workspace-lint",
    ]

    def receipt(
        assignment_id: str,
        result_key: str,
        result_commit: str,
        git_branch: str | None,
    ) -> dict:
        return {
            "result_key": result_key,
            "assignment_id": assignment_id,
            "outcome": "DONE",
            "result_commit": result_commit,
            "from_commit": COMMIT,
            "git_branch": git_branch,
            "result_summary_redacted": "Completed",
            "redaction_applied": False,
            "request_fingerprint": "9" * 64,
            "response": {
                "assignment_id": assignment_id,
                "outcome": "DONE",
                "result_commit": result_commit,
                "result_key": result_key,
                "status": "REVIEWS_PENDING",
                "deduplicated": False,
            },
            "accepted_at": TIMESTAMP,
        }

    state["result_receipts"] = [
        receipt("assignment-1", build_result_key, build_commit, "agent/example"),
        receipt("lint-assignment", lint_result_key, lint_commit, None),
    ]
    build_token_id = managed_transition_token_id(
        SPRINT_ID,
        build_occurrence["occurrence_id"],
        build_result_key,
        "join",
    )
    lint_token_id = managed_transition_token_id(
        SPRINT_ID,
        lint_occurrence_id,
        lint_result_key,
        "join",
    )
    trigger_ids = [lint_token_id, build_token_id]
    state["workflow"]["transition_tokens"] = [
        {
            "token_id": build_token_id,
            "source_occurrence_id": build_occurrence["occurrence_id"],
            "source_node_id": "build",
            "source_graph_revision": 1,
            "result_key": build_result_key,
            "target_node_id": "join",
            "target_graph_revision": None,
            "status": "available",
            "consumed_by_occurrence_id": None,
            "created_at": TIMESTAMP,
        },
        {
            "token_id": lint_token_id,
            "source_occurrence_id": lint_occurrence_id,
            "source_node_id": "lint",
            "source_graph_revision": 1,
            "result_key": lint_result_key,
            "target_node_id": "join",
            "target_graph_revision": None,
            "status": "available",
            "consumed_by_occurrence_id": None,
            "created_at": TIMESTAMP,
        },
    ]
    state["transition_journal"] = [
        {
            "journal_id": "journal-build",
            "result_key": build_result_key,
            "assignment_id": "assignment-1",
            "outcome": "DONE",
            "result_commit": build_commit,
            "state": "TRANSITION_COMMITTED",
            "disposition": "accepted",
            "rework_id": None,
            "transition_token_ids": [build_token_id],
            "target_occurrence_ids": [],
            "outbox_event_ids": [],
            "updated_at": TIMESTAMP,
        },
        {
            "journal_id": "journal-lint",
            "result_key": lint_result_key,
            "assignment_id": "lint-assignment",
            "outcome": "DONE",
            "result_commit": lint_commit,
            "state": "TRANSITION_COMMITTED",
            "disposition": "accepted",
            "rework_id": None,
            "transition_token_ids": [lint_token_id],
            "target_occurrence_ids": [],
            "outbox_event_ids": [],
            "updated_at": TIMESTAMP,
        },
    ]
    state["review_assignments"] = []
    state["reviews"] = []
    for source_assignment_id, result_key, result_commit, label in (
        ("assignment-1", build_result_key, build_commit, "build"),
        ("lint-assignment", lint_result_key, lint_commit, "lint"),
    ):
        for index, reviewer in enumerate(definition["execution"]["reviewers"], 1):
            review_assignment_id = f"review-{label}-{index}"
            request_fingerprint = review_request_fingerprint(
                review_assignment_id, "APPROVE", ""
            )
            response = {
                "assignment_id": review_assignment_id,
                "source_assignment_id": source_assignment_id,
                "result_key": result_key,
                "decision": "APPROVE",
                "status": "REVIEW_ACCEPTED",
                "deduplicated": False,
            }
            state["review_assignments"].append(
                {
                    "assignment_id": review_assignment_id,
                    "source_assignment_id": source_assignment_id,
                    "result_key": result_key,
                    "result_commit": result_commit,
                    "result_outcome": "DONE",
                    "reviewer_id": reviewer["id"],
                    "reviewer_phone": reviewer["phone"],
                    "reviewer_index": index,
                    "status": "decided",
                    "decision": "APPROVE",
                    "request_fingerprint": request_fingerprint,
                    "response": response,
                    "created_at": TIMESTAMP,
                    "activated_at": TIMESTAMP,
                    "decided_at": TIMESTAMP,
                }
            )
            state["reviews"].append(
                {
                    "assignment_id": review_assignment_id,
                    "source_assignment_id": source_assignment_id,
                    "result_key": result_key,
                    "result_commit": result_commit,
                    "result_outcome": "DONE",
                    "reviewer_id": reviewer["id"],
                    "reviewer_index": index,
                    "decision": "APPROVE",
                    "feedback": "",
                    "request_fingerprint": request_fingerprint,
                    "response": response,
                    "decided_at": TIMESTAMP,
                }
            )
            state["outbox"].append(
                {
                    "event_id": f"event-review-{label}-{index}",
                    "dedupe_key": f"enqueue:review:{SPRINT_ID}:{result_key}:{index}",
                    "event_type": "REVIEW_ENQUEUE",
                    "payload": {
                        "sprint_id": SPRINT_ID,
                        "source_assignment_id": source_assignment_id,
                        "review_assignment_id": review_assignment_id,
                        "result_key": result_key,
                        "reviewer_phone": reviewer["phone"],
                        "reviewer_index": index,
                    },
                    "status": "delivered",
                    "created_at": TIMESTAMP,
                    "delivered_at": TIMESTAMP,
                    "queue_receipt_id": f"queue-receipt-review-{label}-{index}",
                }
            )
    integration_id = managed_integration_id(SPRINT_ID, 1, "join", trigger_ids)
    workspace_artifact_id = managed_integration_workspace_id(
        SPRINT_ID, 1, "join", trigger_ids
    )
    state["integrations"] = [
        {
            "integration_id": integration_id,
            "target_node_id": "join",
            "target_graph_revision": 1,
            "trigger_token_ids": trigger_ids,
            "workspace_artifact_id": workspace_artifact_id,
            "strategy": "merge_no_ff",
            "status": "PREPARED",
            "parents": [
                {
                    "source_node_id": "lint",
                    "result_key": lint_result_key,
                    "commit": lint_commit,
                },
                {
                    "source_node_id": "build",
                    "result_key": build_result_key,
                    "commit": build_commit,
                },
            ],
            "author_name": "nginx-qa managed integration",
            "author_email": "managed-integration@localhost",
            "author_timestamp": TIMESTAMP,
            "committer_name": "nginx-qa managed integration",
            "committer_email": "managed-integration@localhost",
            "committer_timestamp": TIMESTAMP,
            "commit_message": "Managed integration",
            "integration_commit": None,
            "target_occurrence_id": None,
            "assignment_id": None,
            "normalized_error": None,
            "created_at": TIMESTAMP,
            "updated_at": TIMESTAMP,
        }
    ]
    root = (
        "D:/nginx-qa-staging/managed/integration-workspaces/"
        + workspace_artifact_id
    )
    state["integration_workspaces"] = [
        {
            "workspace_artifact_id": workspace_artifact_id,
            "integration_id": integration_id,
            "project_id": "project-id",
            "sprint_id": SPRINT_ID,
            "repository_id": "main",
            "repository_remote": REPOSITORY_KEY,
            "mirror_storage_key": MIRROR_KEY,
            "expected_root": root,
            "actual_git_toplevel": root,
            "actual_git_dir": root + "/.git",
            "base_commit": lint_commit,
            "head_commit": lint_commit,
            "artifact_status": "active",
            "working_tree_state": "clean",
            "created_at": TIMESTAMP,
            "verified_at": TIMESTAMP,
            "released_at": None,
        }
    ]
    return state, trigger_ids


class SprintTypeResolutionContractTests(unittest.TestCase):
    def test_absent_type_preserves_absence_and_selects_legacy(self) -> None:
        payload = {"execution": {"mode": "sequential"}, "nodes": [{"id": "a"}]}
        before = copy.deepcopy(payload)
        selection = resolve_sprint_type(payload)
        self.assertIsNone(selection.declared)
        self.assertIs(selection.effective, SprintType.LEGACY_V1)
        self.assertIs(selection.pipeline, SprintPipeline.LEGACY)
        self.assertEqual(selection.serialized_field(), {})
        self.assertEqual(payload, before)
        self.assertIsNone(managed_manifest_schema(selection))

    def test_explicit_legacy_and_old_legacy_flag_never_select_managed_schema(self) -> None:
        for payload in (
            {"sprint_type": "legacy_v1", "actors": []},
            {"legacy": True, "actors": []},
            {"legacy": False, "actors": []},
            {"execution": {"sprint_type": "managed_workspace_v1"}},
        ):
            with self.subTest(payload=payload):
                before = copy.deepcopy(payload)
                selection = resolve_sprint_type(payload)
                self.assertIs(selection.pipeline, SprintPipeline.LEGACY)
                self.assertIsNone(managed_manifest_schema(selection))
                self.assertEqual(payload, before)

    def test_explicit_legacy_round_trips_only_declared_provenance(self) -> None:
        selection = resolve_sprint_type({"sprint_type": "legacy_v1"})
        self.assertIs(selection.effective, SprintType.LEGACY_V1)
        self.assertEqual(selection.serialized_field(), {"sprint_type": "legacy_v1"})

    def test_managed_type_is_strict_opt_in(self) -> None:
        selection = resolve_sprint_type({"sprint_type": "managed_workspace_v1"})
        self.assertIs(selection.pipeline, SprintPipeline.MANAGED_WORKSPACE)
        self.assertEqual(managed_manifest_schema(selection), MANAGED_MANIFEST_SCHEMA)

    def test_unknown_values_fail_safely_without_mutation_or_value_echo(self) -> None:
        unsupported_values = [
            None,
            "",
            "LEGACY_V1",
            " legacy_v1 ",
            "UNIQUE-SECRET-SENTINEL-future_v9",
            True,
            1,
            [],
            {},
        ]
        expected_detail = {
            "error": SPRINT_TYPE_UNSUPPORTED,
            "field": "sprint_type",
            "supported": ["legacy_v1", "managed_workspace_v1"],
        }
        for raw_value in unsupported_values:
            with self.subTest(raw_value=raw_value):
                payload = {"sprint_type": raw_value, "sentinel": {"value": 1}}
                before = copy.deepcopy(payload)
                with self.assertRaises(SprintTypeUnsupported) as raised:
                    resolve_sprint_type(payload)
                self.assertEqual(payload, before)
                self.assertEqual(raised.exception.as_detail(), expected_detail)
                serialized = json.dumps(expected_detail, sort_keys=True)
                if isinstance(raw_value, str) and raw_value:
                    self.assertNotIn(raw_value, str(raised.exception))
                    self.assertNotIn(raw_value, serialized)

    def test_execution_mode_is_orthogonal(self) -> None:
        for mode in ("sequential", "parallel", "future-mode", None):
            self.assertIs(
                resolve_sprint_type({"execution": {"mode": mode}}).pipeline,
                SprintPipeline.LEGACY,
            )
            self.assertIs(
                resolve_sprint_type(
                    {"sprint_type": "managed_workspace_v1", "execution": {"mode": mode}}
                ).pipeline,
                SprintPipeline.MANAGED_WORKSPACE,
            )


class PureIdentityAndStateMachineContractTests(unittest.TestCase):
    def test_provenance_identity_matches_documented_golden_value(self) -> None:
        provenance = SprintProvenance(**identity_fixture())
        self.assertEqual(provenance.stable_sprint_id(), SPRINT_ID)

    def test_identity_encoding_has_no_component_boundary_collision(self) -> None:
        self.assertNotEqual(
            canonical_tuple_sha256("ab", "c"),
            canonical_tuple_sha256("a", "bc"),
        )
        with self.assertRaises(ValueError):
            canonical_tuple_sha256("\ud800")

    def test_fingerprints_and_graph_ids_have_golden_values(self) -> None:
        request = StartSprintFromGitRequest(
            repository_id="main",
            ref="refs/heads/sprint-definition",
            manifest_path="orchestration/sprint.json",
            idempotency_key="key",
        )
        self.assertEqual(
            request.request_fingerprint("project-id"),
            "878db6b16908cc7a3e8eddd06306772ba7707fce552d4a136b4c92b63a1bdcad",
        )
        self.assertEqual(
            mirror_storage_key("github.com/пример/репо"),
            "mirror-20fda9aef6904ef6c74f7f79255935a5c8037d272e1b61159094b0dd79b0df99",
        )
        self.assertEqual(
            managed_result_key("assignment-1", "DONE", COMMIT),
            "result-97e762594471032b0464e91af0abe602fc3c24bd84486cce89289a8e68c062cd",
        )
        self.assertEqual(
            canonical_json_sha256({"b": 2, "a": "пример"}),
            "a7ffc8a841eb0177fb7509a116a7fc692293bc7ce9ad5752c6101f7f9d648acb",
        )
        self.assertEqual(
            repair_request_fingerprint(
                SPRINT_ID,
                1,
                COMMIT,
                {"remove_future_node_ids": ["later"]},
            ),
            "6e6ed2df6bca83cbfcc026325f8da783d1436fdb49b30ffd168220cba9dae719",
        )
        join_target = {
            "target_graph_revision": 2,
            "target_node_id": "join",
            "trigger_token_ids": ["token-" + "1" * 64, "token-" + "2" * 64],
            "integration_id": "integration-" + "3" * 64,
        }
        join_recovery_fingerprint = recovery_request_fingerprint(
            "context-join",
            2,
            "APPLY_REPAIR",
            None,
            None,
            {"request": {"expected_revision": 2}},
            join_target,
        )
        self.assertEqual(
            join_recovery_fingerprint,
            canonical_json_sha256(
                {
                    "action": "APPLY_REPAIR",
                    "assignment_id": None,
                    "context_id": "context-join",
                    "graph_revision": 2,
                    "import_attempt_id": None,
                    "join_target": join_target,
                    "parameters": {"request": {"expected_revision": 2}},
                }
            ),
        )
        self.assertNotEqual(
            join_recovery_fingerprint,
            recovery_request_fingerprint(
                "context-join",
                2,
                "APPLY_REPAIR",
                None,
                None,
                {"request": {"expected_revision": 2}},
            ),
        )
        occurrence_id = managed_occurrence_id(SPRINT_ID, 1, "build", 1, [])
        self.assertEqual(
            occurrence_id,
            "occ-09fc21a5da1feb77d8f63adeee72b7a7965967d5e9172d1a294a53e4638e06e6",
        )
        self.assertEqual(
            managed_transition_token_id(
                SPRINT_ID,
                occurrence_id,
                "result-" + "c" * 64,
                "completed",
            ),
            "token-a40ef4d7cdf94b37a3fa0ca4ec375033a4c3e51a55cdd6c9d31950d6f141223c",
        )
        integration_id = managed_integration_id(
            SPRINT_ID,
            2,
            "join",
            ["token-" + "1" * 64, "token-" + "2" * 64],
        )
        self.assertRegex(integration_id, r"^integration-[0-9a-f]{64}$")
        self.assertEqual(
            managed_integration_workspace_id(
                SPRINT_ID,
                2,
                "join",
                ["token-" + "1" * 64, "token-" + "2" * 64],
            ),
            "integration-workspace-" + integration_id.removeprefix("integration-"),
        )
        self.assertNotEqual(
            integration_id,
            managed_integration_id(
                SPRINT_ID,
                2,
                "join",
                ["token-" + "2" * 64, "token-" + "1" * 64],
            ),
        )
        self.assertEqual(
            canonical_json_bytes({"b": 2, "a": 1}),
            b'{"a":1,"b":2}',
        )

    def test_git_ref_and_relative_path_contracts_reject_windows_aliases(self) -> None:
        self.assertTrue(git_ref_format_valid("refs/heads/topic"))
        self.assertTrue(git_ref_format_valid("topic/subtopic", branch=True))
        for value in (
            "refs/heads/a..b",
            "refs/heads/name.lock",
            "refs/heads/a@{b",
            "refs/heads/trailing/",
            "refs/heads/has space",
            "refs/heads/.hidden",
            "refs/heads/CON",
            "refs/heads/topic.",
        ):
            self.assertFalse(git_ref_format_valid(value), value)
        for value in (
            "/absolute.json",
            "../up.json",
            "a/../b",
            "a//b",
            "a\\b",
            "a/",
            "AUX.txt",
            "dir/name:stream",
            "dir/trailing.",
        ):
            self.assertFalse(relative_git_path_valid(value), value)
        for value in (
            "CON",
            "con.txt",
            "Lpt9.log",
            "COM¹.txt",
            "lpt³.tar.gz",
            "NUL .txt",
            "CONIN$",
            "CONOUT$.txt",
            "PROGRA~1",
            "normal .txt",
            " leading",
            "bad ",
            "bad.",
        ):
            self.assertFalse(windows_path_segment_valid(value), value)
        self.assertTrue(relative_git_path_valid("orchestration/sprint.json"))
        self.assertTrue(relative_git_path_valid("x" * 255))
        self.assertFalse(relative_git_path_valid("x" * 256))
        self.assertTrue(git_ref_format_valid("x" * 255, branch=True))
        self.assertFalse(git_ref_format_valid("x" * 256, branch=True))

    def test_all_lifecycle_edges_are_frozen(self) -> None:
        lifecycle_maps = (
            ASSIGNMENT_STATE_TRANSITIONS,
            REVIEW_ASSIGNMENT_STATE_TRANSITIONS,
            BRANCH_LEASE_STATE_TRANSITIONS,
            PORT_LEASE_STATE_TRANSITIONS,
            OUTBOX_STATE_TRANSITIONS,
        )
        for transitions in lifecycle_maps:
            for current in transitions:
                for target in transitions:
                    self.assertEqual(
                        lifecycle_transition_allowed(transitions, current, target),
                        target == current or target in transitions[current],
                        (current, target),
                    )
        for current in PROCESS_STATE_TRANSITIONS:
            for target in PROCESS_STATE_TRANSITIONS:
                self.assertEqual(
                    process_transition_allowed(current, target),
                    target in PROCESS_STATE_TRANSITIONS[current],
                    (current, target),
                )
        for index, state in enumerate(TRANSITION_JOURNAL_STATES):
            for target_index, target in enumerate(TRANSITION_JOURNAL_STATES):
                self.assertEqual(
                    journal_advance_allowed(state, target),
                    target_index in {index, index + 1},
                    (state, target),
                )


class SprintSchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas = {
            path.name: json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(SCHEMA_ROOT.glob("*.schema.json"))
        }
        registry = Registry()
        for schema in cls.schemas.values():
            registry = registry.with_resource(
                schema["$id"],
                Resource.from_contents(schema),
            )
        cls.registry = registry
        cls.schema_by_id = {schema["$id"]: schema for schema in cls.schemas.values()}

    def validator(self, filename: str) -> Draft202012Validator:
        return Draft202012Validator(
            self.schemas[filename],
            registry=self.registry,
            format_checker=FormatChecker(),
        )

    def def_validator(self, filename: str, definition: str) -> Draft202012Validator:
        return Draft202012Validator(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$ref": f"{self.schemas[filename]['$id']}#/$defs/{definition}",
            },
            registry=self.registry,
            format_checker=FormatChecker(),
        )

    def assert_invalid(self, filename: str, instance: dict) -> None:
        with self.assertRaises(ValidationError):
            self.validator(filename).validate(instance)

    def test_exact_schema_set_meta_validates_and_refs_resolve_offline(self) -> None:
        self.assertEqual(set(self.schemas), EXPECTED_SCHEMA_FILES)
        for name, schema in self.schemas.items():
            with self.subTest(schema=name):
                Draft202012Validator.check_schema(schema)
                for node in self._walk(schema):
                    if not isinstance(node, dict) or "$ref" not in node:
                        continue
                    absolute_ref = urljoin(schema["$id"], node["$ref"])
                    resource_uri, fragment = urldefrag(absolute_ref)
                    self.assertIn(resource_uri, self.schema_by_id)
                    target = self.schema_by_id[resource_uri]
                    if fragment:
                        self._resolve_pointer(target, fragment)

    @staticmethod
    def _walk(value):
        yield value
        if isinstance(value, dict):
            for child in value.values():
                yield from SprintSchemaContractTests._walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from SprintSchemaContractTests._walk(child)

    @staticmethod
    def _resolve_pointer(document, fragment: str):
        self_value = document
        for raw_part in fragment.lstrip("/").split("/"):
            part = raw_part.replace("~1", "/").replace("~0", "~")
            self_value = self_value[int(part)] if isinstance(self_value, list) else self_value[part]
        return self_value

    def test_dispatch_schema_is_optional_and_legacy_open(self) -> None:
        schema = self.schemas["sprint-dispatch-v1.schema.json"]
        self.assertNotIn("required", schema)
        self.assertTrue(schema["additionalProperties"])
        self.assertEqual(
            schema["properties"]["sprint_type"]["enum"],
            ["legacy_v1", "managed_workspace_v1"],
        )

    def test_documented_managed_manifest_and_parallel_variant_validate(self) -> None:
        validator = self.validator("managed-workspace-sprint-v1.schema.json")
        manifest = managed_manifest_fixture()
        validator.validate(manifest)
        parallel = copy.deepcopy(manifest)
        parallel["execution"].pop("start_node")
        parallel["execution"]["mode"] = "parallel"
        parallel["execution"]["start_nodes"] = ["build", "continuity"]
        validator.validate(parallel)

    def test_process_environment_and_argv_semantics_reject_unsafe_values(self) -> None:
        invalid_environments = (
            ({"": "value"}, "PROCESS_ENVIRONMENT_NAME_INVALID"),
            ({"BAD=NAME": "value"}, "PROCESS_ENVIRONMENT_NAME_INVALID"),
            ({"BAD\0NAME": "value"}, "PROCESS_ENVIRONMENT_NAME_INVALID"),
            ({"GOOD_NAME": "bad\0value"}, "PROCESS_ENVIRONMENT_VALUE_INVALID"),
        )
        for environment, issue_code in invalid_environments:
            with self.subTest(environment=environment):
                manifest = managed_manifest_fixture()
                manifest["nodes"][0]["workspace"]["process"][
                    "environment"
                ] = environment
                self.validator(
                    "managed-workspace-sprint-v1.schema.json"
                ).validate(
                    manifest
                )
                self.assertIn(
                    issue_code,
                    managed_graph_semantic_issues(manifest),
                )

        colliding = managed_manifest_fixture()
        colliding["nodes"][0]["workspace"]["process"]["environment"] = {
            "APP_MODE": "one",
            "app_mode": "two",
        }
        self.validator("managed-workspace-sprint-v1.schema.json").validate(
            colliding
        )
        self.assertIn(
            "PROCESS_ENVIRONMENT_NAME_COLLISION",
            managed_graph_semantic_issues(colliding),
        )

        nul_argument = managed_manifest_fixture()
        nul_argument["nodes"][0]["workspace"]["process"]["command"] = [
            "python",
            "service.py\0redirected",
        ]
        self.validator(
            "managed-workspace-sprint-v1.schema.json"
        ).validate(
            nul_argument
        )
        self.assertIn(
            "PROCESS_COMMAND_INVALID",
            managed_graph_semantic_issues(nul_argument),
        )

    def test_runtime_v2_process_rejects_invalid_environment_and_argv(self) -> None:
        validator = self.def_validator(
            "managed-runtime-state-v2.schema.json", "process"
        )
        invalid_processes = []
        for environment in (
            {"": "value"},
            {"BAD=NAME": "value"},
            {"BAD\0NAME": "value"},
            {"GOOD_NAME": "bad\0value"},
        ):
            process = process_fixture()
            process["environment_redacted"] = environment
            invalid_processes.append(process)
        command = process_fixture()
        command["command_redacted"] = ["python", "service.py\0redirected"]
        invalid_processes.append(command)
        for process in invalid_processes:
            with self.subTest(
                environment=process["environment_redacted"],
                command=process["command_redacted"],
            ):
                with self.assertRaises(ValidationError):
                    validator.validate(process)

    def test_process_health_path_requires_ascii_http_origin_form(self) -> None:
        for path in ("/ready now", "/готов", "/bad%ZZ", "/back\\slash"):
            with self.subTest(path=path):
                manifest = managed_manifest_fixture()
                manifest["nodes"][0]["workspace"]["process"][
                    "health_path"
                ] = path
                # Manifest v1 remains frozen; semantic preflight owns the
                # portable request-target restriction.
                self.validator("managed-workspace-sprint-v1.schema.json").validate(
                    manifest
                )
                self.assertIn(
                    "PROCESS_HEALTH_PATH_INVALID",
                    managed_graph_semantic_issues(manifest),
                )

                process = process_fixture()
                process["health_endpoint"]["path"] = path
                with self.assertRaises(ValidationError):
                    self.def_validator(
                        "managed-runtime-state-v2.schema.json", "process"
                    ).validate(process)

        valid = managed_manifest_fixture()
        valid["nodes"][0]["workspace"]["process"][
            "health_path"
        ] = "/v1/ready%20now;mode=full"
        self.assertNotIn(
            "PROCESS_HEALTH_PATH_INVALID",
            managed_graph_semantic_issues(valid),
        )

    def test_manifest_rejects_partial_override_and_unsafe_values(self) -> None:
        partial = managed_manifest_fixture()
        partial["nodes"][0]["workspace"]["git"] = {}
        self.assert_invalid("managed-workspace-sprint-v1.schema.json", partial)
        traversal = managed_manifest_fixture()
        traversal["files"][0]["path"] = "../secret"
        self.assert_invalid("managed-workspace-sprint-v1.schema.json", traversal)
        reserved_env = managed_manifest_fixture()
        reserved_env["nodes"][0]["workspace"]["process"]["environment"] = {
            "NgInX_Qa_MaNaGeD_PoRt": "bad"
        }
        self.assert_invalid("managed-workspace-sprint-v1.schema.json", reserved_env)
        secret_literal = managed_manifest_fixture()
        secret_literal["nodes"][0]["workspace"]["process"]["environment"] = {
            "DATABASE_PASSWORD": "not-for-durable-state"
        }
        self.validator("managed-workspace-sprint-v1.schema.json").validate(
            secret_literal
        )
        self.assertIn(
            "PROCESS_ENVIRONMENT_SECRET_LITERAL",
            managed_graph_semantic_issues(secret_literal),
        )
        secret_reference = managed_manifest_fixture()
        secret_reference["nodes"][0]["workspace"]["process"]["environment"] = {
            "DATABASE_PASSWORD": {"secret_ref": "vault:database/password"}
        }
        self.validator("managed-workspace-sprint-v1.schema.json").validate(
            secret_reference
        )
        self.assertNotIn(
            "PROCESS_ENVIRONMENT_SECRET_LITERAL",
            managed_graph_semantic_issues(secret_reference),
        )
        raw_secret_reference = managed_manifest_fixture()
        raw_secret_reference["nodes"][0]["workspace"]["process"][
            "environment"
        ] = {
            "DATABASE_PASSWORD": {
                "secret_ref": "ghp_1234567890ABCDEFGHIJK"
            }
        }
        self.assert_invalid(
            "managed-workspace-sprint-v1.schema.json", raw_secret_reference
        )
        disguised_secret_reference = managed_manifest_fixture()
        disguised_secret_reference["nodes"][0]["workspace"]["process"][
            "environment"
        ] = {
            "DATABASE_PASSWORD": {
                "secret_ref": "vault:ghp_1234567890ABCDEFGHIJK"
            }
        }
        self.validator("managed-workspace-sprint-v1.schema.json").validate(
            disguised_secret_reference
        )
        self.assertIn(
            "PROCESS_ENVIRONMENT_SECRET_LITERAL",
            managed_graph_semantic_issues(disguised_secret_reference),
        )
        durable_disguised_reference = active_runtime_fixture(
            include_process_definition=True
        )
        durable_definition = durable_disguised_reference["graph_revisions"][0][
            "definition"
        ]
        durable_definition["nodes"][0]["workspace"]["process"][
            "environment"
        ] = disguised_secret_reference["nodes"][0]["workspace"]["process"][
            "environment"
        ]
        durable_disguised_reference["graph_revisions"][0][
            "definition_sha256"
        ] = canonical_json_sha256(durable_definition)
        self.validator("managed-runtime-state-v1.schema.json").validate(
            durable_disguised_reference
        )
        self.assertIn(
            "PROCESS_ENVIRONMENT_SECRET_LITERAL",
            managed_activation_invariant_issues(durable_disguised_reference),
        )
        secret_in_command = managed_manifest_fixture()
        secret_in_command["nodes"][0]["workspace"]["process"]["command"] = [
            "python",
            "service.py",
            "--authorization=Bearer abcdefghijklmnop",
        ]
        self.assertIn(
            "PROCESS_COMMAND_SECRET_LITERAL",
            managed_graph_semantic_issues(secret_in_command),
        )
        invalid_restart = managed_manifest_fixture()
        invalid_restart["nodes"][0]["workspace"]["process"].update(
            {"restart_policy": "on_failure", "max_restart_attempts": 0}
        )
        self.assert_invalid("managed-workspace-sprint-v1.schema.json", invalid_restart)
        empty_executable = managed_manifest_fixture()
        empty_executable["nodes"][0]["workspace"]["process"]["command"] = [""]
        self.assert_invalid(
            "managed-workspace-sprint-v1.schema.json", empty_executable
        )
        windows_device = managed_manifest_fixture()
        windows_device["nodes"][0]["id"] = "CON"
        self.assertIn(
            "PATH_SEGMENT_UNSAFE",
            managed_graph_semantic_issues(windows_device),
        )

        raw_process_environment = process_fixture()
        raw_process_environment["environment_raw"] = {"PASSWORD": "secret"}
        with self.assertRaises(ValidationError):
            self.def_validator(
                "managed-runtime-state-v2.schema.json", "process"
            ).validate(raw_process_environment)
        referenced_process_environment = process_fixture()
        referenced_process_environment["environment_redacted"] = {
            "DATABASE_PASSWORD": {"secret_ref": "vault:database/password"}
        }
        self.def_validator(
            "managed-runtime-state-v2.schema.json", "process"
        ).validate(referenced_process_environment)

    def test_schema_v1_accepts_process_records_from_before_supervisor_hardening(
        self,
    ) -> None:
        legacy_process = process_fixture()
        legacy_process.pop("os_process_birth_token")
        legacy_process.pop("startup_deadline_at")
        legacy_process.pop("terminal_reason")
        self.def_validator(
            "managed-runtime-state-v1.schema.json", "process"
        ).validate(legacy_process)
        with self.assertRaises(ValidationError):
            self.def_validator(
                "managed-runtime-state-v1.schema.json", "process"
            ).validate(process_fixture())

    def test_runtime_v1_keeps_legacy_process_semantics_but_new_inputs_are_strict(
        self,
    ) -> None:
        cases = (
            (
                "invalid environment name",
                "PROCESS_ENVIRONMENT_NAME_INVALID",
                "environment",
                {"": "value"},
            ),
            (
                "case-folded environment collision",
                "PROCESS_ENVIRONMENT_NAME_COLLISION",
                "environment",
                {"APP_MODE": "one", "app_mode": "two"},
            ),
            (
                "invalid environment value",
                "PROCESS_ENVIRONMENT_VALUE_INVALID",
                "environment",
                {"APP_MODE": "bad\0value"},
            ),
            (
                "invalid command argument",
                "PROCESS_COMMAND_INVALID",
                "command",
                ["python", "service.py\0redirected"],
            ),
            (
                "non-origin health path",
                "PROCESS_HEALTH_PATH_INVALID",
                "health_path",
                "/ready now",
            ),
        )
        for label, issue_code, field, value in cases:
            with self.subTest(case=label):
                legacy = active_runtime_fixture(include_process_definition=True)
                process = process_fixture()
                process.update(
                    {
                        "restart_policy": "never",
                        "max_restart_attempts": 0,
                    }
                )
                for version_two_field in (
                    "os_process_birth_token",
                    "startup_deadline_at",
                    "terminal_reason",
                ):
                    process.pop(version_two_field)
                definition = legacy["graph_revisions"][0]["definition"]
                launch = definition["nodes"][0]["workspace"]["process"]
                launch[field] = copy.deepcopy(value)
                if field == "environment":
                    process["environment_redacted"] = copy.deepcopy(value)
                elif field == "command":
                    process["command_redacted"] = copy.deepcopy(value)
                else:
                    process["health_endpoint"]["path"] = value
                legacy["graph_revisions"][0]["definition_sha256"] = (
                    canonical_json_sha256(definition)
                )
                legacy["processes"] = [process]
                legacy["port_leases"] = [
                    {
                        "lease_id": "port-lease-1",
                        "instance_id": "umse-staging",
                        "network_namespace_id": "host",
                        "assignment_id": "assignment-1",
                        "process_id": None,
                        "host": "127.0.0.1",
                        "port": 18100,
                        "status": "reserved",
                        "bind_verified": False,
                        "acquired_at": TIMESTAMP,
                        "released_at": None,
                    }
                ]

                self.validator("managed-runtime-state-v1.schema.json").validate(
                    legacy
                )
                self.assertEqual(managed_activation_invariant_issues(legacy), ())
                self.assertIn(
                    issue_code,
                    managed_graph_semantic_issues(definition),
                )

                strict = copy.deepcopy(legacy)
                strict["schema_version"] = 2
                strict["processes"][0].update(
                    {
                        "os_process_birth_token": None,
                        "startup_deadline_at": None,
                        "terminal_reason": None,
                    }
                )
                self.assertIn(
                    issue_code,
                    managed_activation_invariant_issues(strict),
                )

                migrated = copy.deepcopy(strict)
                migrated["migrated_from_runtime_schema_version"] = 1
                migrated["processes"][0][
                    "migrated_from_runtime_schema_version"
                ] = 1
                self.validator("managed-runtime-state-v2.schema.json").validate(
                    migrated
                )
                self.assertEqual(
                    managed_activation_invariant_issues(migrated), ()
                )

    def test_runtime_v1_migration_provenance_is_complete_and_consistent(
        self,
    ) -> None:
        migrated = active_runtime_fixture(include_process_definition=True)
        migrated["schema_version"] = 2
        migrated["migrated_from_runtime_schema_version"] = 1
        migrated_process = process_fixture()
        migrated_process.update(
            {
                "migrated_from_runtime_schema_version": 1,
                "restart_policy": "never",
                "max_restart_attempts": 0,
            }
        )
        migrated["processes"] = [migrated_process]
        migrated["port_leases"] = [
            {
                "lease_id": "port-lease-1",
                "instance_id": "umse-staging",
                "network_namespace_id": "host",
                "assignment_id": "assignment-1",
                "process_id": None,
                "host": "127.0.0.1",
                "port": 18100,
                "status": "reserved",
                "bind_verified": False,
                "acquired_at": TIMESTAMP,
                "released_at": None,
            }
        ]
        self.validator("managed-runtime-state-v2.schema.json").validate(migrated)
        self.assertEqual(managed_activation_invariant_issues(migrated), ())

        missing_process_marker = copy.deepcopy(migrated)
        missing_process_marker["processes"][0].pop(
            "migrated_from_runtime_schema_version"
        )
        with self.assertRaises(ValidationError):
            self.validator("managed-runtime-state-v2.schema.json").validate(
                missing_process_marker
            )
        self.assertIn(
            "RUNTIME_SCHEMA_MIGRATION_INVALID",
            managed_activation_invariant_issues(missing_process_marker),
        )

        missing_root_marker = copy.deepcopy(migrated)
        missing_root_marker.pop("migrated_from_runtime_schema_version")
        with self.assertRaises(ValidationError):
            self.validator("managed-runtime-state-v2.schema.json").validate(
                missing_root_marker
            )
        self.assertIn(
            "RUNTIME_SCHEMA_MIGRATION_INVALID",
            managed_activation_invariant_issues(missing_root_marker),
        )

        mixed = copy.deepcopy(migrated)
        strict_process = process_fixture()
        strict_process.update(
            {
                "process_id": "process-2",
                "port_lease_id": "port-lease-2",
                "restart_policy": "never",
                "max_restart_attempts": 0,
            }
        )
        mixed["processes"].append(strict_process)
        self.validator("managed-runtime-state-v2.schema.json").validate(mixed)
        mixed["processes"][1]["command_redacted"] = ["bad\0command"]
        with self.assertRaises(ValidationError):
            self.validator("managed-runtime-state-v2.schema.json").validate(mixed)

        wrong_version = copy.deepcopy(migrated)
        wrong_version["schema_version"] = 1
        self.assertIn(
            "RUNTIME_SCHEMA_MIGRATION_INVALID",
            managed_activation_invariant_issues(wrong_version),
        )

        invalid_marker = copy.deepcopy(migrated)
        invalid_marker["migrated_from_runtime_schema_version"] = True
        with self.assertRaises(ValidationError):
            self.validator("managed-runtime-state-v2.schema.json").validate(
                invalid_marker
            )
        self.assertIn(
            "RUNTIME_SCHEMA_MIGRATION_INVALID",
            managed_activation_invariant_issues(invalid_marker),
        )

        escaped_process = process_fixture()
        escaped_process["migrated_from_runtime_schema_version"] = 1
        with self.assertRaises(ValidationError):
            self.def_validator(
                "managed-runtime-state-v2.schema.json", "process"
            ).validate(escaped_process)

    def test_every_public_api_schema_has_a_positive_instance(self) -> None:
        result_key = managed_result_key("assignment-1", "DONE", COMMIT)
        runtime_v2 = active_runtime_fixture()
        runtime_v2["schema_version"] = 2
        cases = {
            "managed-api-error-v1.schema.json": {
                "detail": {"error": "SPRINT_PREFLIGHT_FAILED", "correlation_id": "corr-1"}
            },
            "managed-assignment-result-v1.schema.json": {
                "assignment_id": "assignment-1",
                "status": "DONE",
                "result": "Implemented and tested",
                "from_commit": COMMIT,
                "git_commit": COMMIT,
                "git_branch": "agent/example",
            },
            "managed-assignment-result-response-v1.schema.json": {
                "assignment_id": "assignment-1",
                "outcome": "DONE",
                "result_commit": COMMIT,
                "result_key": result_key,
                "status": "REVIEWS_PENDING",
                "deduplicated": False,
            },
            "managed-project-control-v1.schema.json": project_control_fixture(),
            "managed-review-decision-v1.schema.json": {
                "assignment_id": "review-assignment-1",
                "status": "APPROVE",
            },
            "managed-review-decision-response-v1.schema.json": {
                "assignment_id": "review-assignment-1",
                "source_assignment_id": "assignment-1",
                "result_key": result_key,
                "decision": "APPROVE",
                "status": "REVIEW_ACCEPTED",
                "deduplicated": False,
            },
            "managed-runtime-config-v1.schema.json": runtime_config_fixture(),
            "managed-runtime-state-v1.schema.json": active_runtime_fixture(),
            "managed-runtime-state-v2.schema.json": runtime_v2,
            "managed-workspace-sprint-v1.schema.json": managed_manifest_fixture(),
            "repair-sprint-v1.schema.json": {
                "expected_revision": 1,
                "repair_source_commit": COMMIT,
                "idempotency_key": "repair-key",
                "patch": {"remove_future_node_ids": ["later"]},
            },
            "repair-sprint-response-v1.schema.json": {
                "sprint_id": SPRINT_ID,
                "from_revision": 1,
                "graph_revision": 2,
                "repair_source_commit": COMMIT,
                "deduplicated": False,
            },
            "sprint-dispatch-v1.schema.json": {"sprint_type": "legacy_v1", "legacy": True},
            "sprint-preflight-report-v1.schema.json": preflight_fixture(),
            "start-sprint-from-git-v1.schema.json": {
                "repository_id": "main",
                "ref": "refs/heads/sprint-definition",
                "manifest_path": "orchestration/sprint.json",
                "idempotency_key": "key",
            },
            "start-sprint-from-git-response-v1.schema.json": start_response_fixture(),
        }
        self.assertEqual(set(cases), EXPECTED_SCHEMA_FILES)
        for filename, instance in cases.items():
            with self.subTest(schema=filename):
                self.validator(filename).validate(instance)

    def test_review_result_and_response_conditionals_are_strict(self) -> None:
        review_validator = self.validator("managed-review-decision-v1.schema.json")
        review_validator.validate({"assignment_id": "review-1", "status": "APPROVE"})
        with self.assertRaises(ValidationError):
            review_validator.validate({"assignment_id": "review-1", "status": "REJECT"})

        result_response = {
            "assignment_id": "assignment-1",
            "outcome": "DONE",
            "result_commit": COMMIT,
            "result_key": managed_result_key("assignment-1", "DONE", COMMIT),
            "status": "ALREADY_ACCEPTED",
            "deduplicated": False,
        }
        self.assert_invalid("managed-assignment-result-response-v1.schema.json", result_response)

        sequential = start_response_fixture()
        sequential["initial_assignment_ids"].append("assignment-2")
        self.assert_invalid("start-sprint-from-git-response-v1.schema.json", sequential)

    def test_object_ids_and_date_times_are_strict(self) -> None:
        for length in (39, 41, 63, 65):
            response = start_response_fixture()
            response["identity"]["commit"] = "a" * length
            self.assert_invalid("start-sprint-from-git-response-v1.schema.json", response)
        for length in (40, 64):
            response = start_response_fixture()
            response["identity"]["commit"] = "a" * length
            response["workspace_source_commit"] = "a" * length
            self.validator("start-sprint-from-git-response-v1.schema.json").validate(response)
        preflight = preflight_fixture()
        preflight["checked_at"] = "not-a-timestamp"
        self.assert_invalid("sprint-preflight-report-v1.schema.json", preflight)

    def test_active_runtime_schema_and_relational_checker_agree(self) -> None:
        valid = active_runtime_fixture()
        self.validator("managed-runtime-state-v1.schema.json").validate(valid)
        self.assertEqual(managed_activation_invariant_issues(valid), ())
        for field, replacement in (
            ("workflow", None),
            ("import_attempts", []),
            ("assignments", []),
        ):
            with self.subTest(field=field):
                invalid = copy.deepcopy(valid)
                invalid[field] = replacement
                with self.assertRaises(ValidationError):
                    self.validator("managed-runtime-state-v1.schema.json").validate(invalid)
        no_active_assignment = copy.deepcopy(valid)
        no_active_assignment["active_assignment_ids"] = []
        no_active_assignment["allowed_outcomes_by_assignment"] = {}
        self.validator("managed-runtime-state-v1.schema.json").validate(
            no_active_assignment
        )
        self.assertIn(
            "ACTIVE_ASSIGNMENTS_INVALID",
            managed_activation_invariant_issues(no_active_assignment),
        )
        no_active_outcomes = copy.deepcopy(valid)
        no_active_outcomes["allowed_outcomes_by_assignment"] = {}
        self.validator("managed-runtime-state-v1.schema.json").validate(
            no_active_outcomes
        )
        self.assertIn(
            "ACTIVE_OUTCOMES_INVALID",
            managed_activation_invariant_issues(no_active_outcomes),
        )

    def test_occurrence_and_terminal_relations_detect_corruption(self) -> None:
        state = active_runtime_fixture()
        state["workflow"]["occurrences"][0]["occurrence_id"] = "occ-" + "0" * 64
        self.assertIn("OCCURRENCE_ID_MISMATCH", managed_activation_invariant_issues(state))

        terminal_with_live_work = active_runtime_fixture()
        terminal_with_live_work["status"] = "completed"
        issues = managed_activation_invariant_issues(terminal_with_live_work)
        self.assertIn("TERMINAL_STATE_HAS_LIVE_WORK", issues)
        self.assertIn("COMPLETED_STATE_TERMINAL_PROOF_MISSING", issues)

        digest_mismatch = active_runtime_fixture()
        digest_mismatch["graph_revisions"][0]["definition_sha256"] = "0" * 64
        self.assertIn(
            "GRAPH_REVISION_DIGEST_MISMATCH",
            managed_activation_invariant_issues(digest_mismatch),
        )

        stranded_terminal = completed_runtime_fixture()
        stranded_terminal["workflow"]["transition_tokens"][0].update(
            {"status": "available", "target_graph_revision": None}
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            stranded_terminal
        )
        self.assertIn(
            "TRANSITION_TOKEN_TARGET_INVALID",
            managed_activation_invariant_issues(stranded_terminal),
        )

    def test_completed_runtime_has_terminal_and_independent_review_proof(self) -> None:
        completed = completed_runtime_fixture()
        self.validator("managed-runtime-state-v1.schema.json").validate(completed)
        self.assertEqual(managed_activation_invariant_issues(completed), ())

        one_review = completed_runtime_fixture()
        one_review["review_assignments"].pop()
        one_review["reviews"].pop()
        self.assertIn("REVIEW_QUORUM_INVALID", managed_activation_invariant_issues(one_review))

    def test_graph_semantics_are_revalidated_for_every_snapshot(self) -> None:
        manifest = managed_manifest_fixture()
        self.assertEqual(managed_graph_semantic_issues(manifest), ())

        unknown_target = copy.deepcopy(manifest)
        unknown_target["nodes"][0]["transitions"]["DONE"] = "ghost"
        self.assertIn(
            "GRAPH_TRANSITION_TARGET_UNKNOWN",
            managed_graph_semantic_issues(unknown_target),
        )

        wrong_join = copy.deepcopy(manifest)
        wrong_join["nodes"][1].pop("activation_policy")
        self.assertIn(
            "GRAPH_JOIN_PARENT_ORDER_INVALID",
            managed_graph_semantic_issues(wrong_join),
        )

        spurious_order = copy.deepcopy(manifest)
        spurious_order["nodes"][0]["join_parent_order"] = [
            "continuity",
            "build",
        ]
        self.assertIn(
            "GRAPH_JOIN_PARENT_ORDER_INVALID",
            managed_graph_semantic_issues(spurious_order),
        )

        inherited_never_with_restarts = copy.deepcopy(manifest)
        inherited_never_with_restarts["nodes"][0]["workspace"]["process"].pop(
            "restart_policy"
        )
        inherited_never_with_restarts["nodes"][0]["workspace"]["process"][
            "max_restart_attempts"
        ] = 3
        self.assertIn(
            "PROCESS_POLICY_INVALID",
            managed_graph_semantic_issues(inherited_never_with_restarts),
        )

        inherited_cap_with_never_override = copy.deepcopy(manifest)
        inherited_cap_with_never_override["process_policy"] = {
            "restart_policy": "on_failure",
            "max_restart_attempts": 3,
        }
        self.assertIn(
            "PROCESS_POLICY_INVALID",
            managed_graph_semantic_issues(inherited_cap_with_never_override),
        )

        unsafe_cwd = copy.deepcopy(manifest)
        unsafe_cwd["nodes"][0]["workspace"]["process"]["cwd"] = "cache/CON"
        self.assertIn(
            "PATH_SEGMENT_UNSAFE",
            managed_graph_semantic_issues(unsafe_cwd),
        )

        colliding_file = copy.deepcopy(manifest)
        colliding_file["files"][1]["path"] = "ORCHESTRATION/readme.md"
        self.assertIn(
            "MANIFEST_CHECKSUM_PATH_DUPLICATE",
            managed_graph_semantic_issues(colliding_file),
        )

        colliding_branch = copy.deepcopy(manifest)
        colliding_branch["execution"]["reviewers"][0]["git_branch"] = (
            "AGENT/EXAMPLE"
        )
        self.assertIn(
            "BRANCH_CASEFOLD_COLLISION",
            managed_graph_semantic_issues(colliding_branch),
        )

        runtime = active_runtime_fixture()
        runtime["graph_revisions"][0]["definition"] = unknown_target
        runtime["graph_revisions"][0]["definition_sha256"] = canonical_json_sha256(
            unknown_target
        )
        self.assertIn(
            "GRAPH_TRANSITION_TARGET_UNKNOWN",
            managed_activation_invariant_issues(runtime),
        )

    def test_reverse_resource_and_outbox_ownership_is_mandatory(self) -> None:
        missing_outbox = active_runtime_fixture()
        missing_outbox["outbox"] = []
        self.validator("managed-runtime-state-v1.schema.json").validate(missing_outbox)
        self.assertIn(
            "ASSIGNMENT_OUTBOX_COVERAGE_INVALID",
            managed_activation_invariant_issues(missing_outbox),
        )

        orphan_workspace = active_runtime_fixture()
        workspace = copy.deepcopy(orphan_workspace["workspaces"][0])
        workspace.update(
            {"workspace_id": "workspace-ghost", "assignment_id": "assignment-ghost"}
        )
        orphan_workspace["workspaces"].append(workspace)
        self.validator("managed-runtime-state-v1.schema.json").validate(orphan_workspace)
        self.assertIn(
            "WORKSPACE_OWNER_INVALID",
            managed_activation_invariant_issues(orphan_workspace),
        )

        orphan_branch = active_runtime_fixture()
        lease = copy.deepcopy(orphan_branch["branch_leases"][0])
        lease.update(
            {
                "lease_id": "branch-lease-ghost",
                "assignment_id": "assignment-ghost",
                "branch": "agent/ghost",
            }
        )
        orphan_branch["branch_leases"].append(lease)
        self.validator("managed-runtime-state-v1.schema.json").validate(orphan_branch)
        self.assertIn(
            "BRANCH_LEASE_OWNER_INVALID",
            managed_activation_invariant_issues(orphan_branch),
        )

        orphan_port = active_runtime_fixture()
        orphan_port["port_leases"].append(
            {
                "lease_id": "port-lease-ghost",
                "instance_id": "umse-staging",
                "network_namespace_id": "host",
                "assignment_id": "assignment-ghost",
                "process_id": None,
                "host": "127.0.0.1",
                "port": 18100,
                "status": "reserved",
                "bind_verified": False,
                "acquired_at": TIMESTAMP,
                "released_at": None,
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(orphan_port)
        self.assertIn(
            "PORT_LEASE_OWNER_INVALID",
            managed_activation_invariant_issues(orphan_port),
        )

    def test_review_and_import_receipts_are_cryptographically_bound(self) -> None:
        review_corruption = completed_runtime_fixture()
        review_corruption["reviews"][0]["request_fingerprint"] = "0" * 64
        self.validator("managed-runtime-state-v1.schema.json").validate(
            review_corruption
        )
        self.assertIn(
            "REVIEW_DECISION_BINDING_INVALID",
            managed_activation_invariant_issues(review_corruption),
        )

        for mutate in (
            lambda state: state["import_attempts"][0]["preflight"].update(
                {"lease_snapshot_sha256": "0" * 64}
            ),
            lambda state: state["import_attempts"][0]["identity"].update(
                {"repository_id": "other"}
            ),
            lambda state: state["import_attempts"][0].update(
                {"prepared_artifact_ids": ["ghost-artifact"]}
            ),
            lambda state: state["import_attempts"][0]["activation_response"].update(
                {"initial_assignment_ids": ["assignment-ghost"]}
            ),
        ):
            with self.subTest(mutate=mutate):
                state = active_runtime_fixture()
                mutate(state)
                self.validator("managed-runtime-state-v1.schema.json").validate(state)
                issues = managed_activation_invariant_issues(state)
                self.assertTrue(
                    {"IMPORT_ATTEMPT_BINDING_INVALID", "PREFLIGHT_SNAPSHOT_DIGEST_MISMATCH"}
                    .intersection(issues)
                )

        duplicate_import_key = active_runtime_fixture()
        failed_attempt = copy.deepcopy(duplicate_import_key["import_attempts"][0])
        failed_attempt.update(
            {
                "attempt_id": "attempt-2",
                "phase": "PREPARE",
                "status": "failed",
                "prepared_artifact_ids": [],
                "activation_response": None,
            }
        )
        duplicate_import_key["import_attempts"].append(failed_attempt)
        self.validator("managed-runtime-state-v1.schema.json").validate(
            duplicate_import_key
        )
        self.assertIn(
            "IMPORT_IDEMPOTENCY_KEY_DUPLICATE",
            managed_activation_invariant_issues(duplicate_import_key),
        )

    def test_repair_history_is_materialized_and_future_only(self) -> None:
        valid = repaired_runtime_fixture()
        self.validator("managed-runtime-state-v1.schema.json").validate(valid)
        self.assertEqual(managed_activation_invariant_issues(valid), ())

        bad_fingerprint = repaired_runtime_fixture()
        bad_fingerprint["repairs"][0]["request_fingerprint"] = "0" * 64
        self.assertIn(
            "GRAPH_REPAIR_HISTORY_INVALID",
            managed_activation_invariant_issues(bad_fingerprint),
        )

        immutable = repaired_runtime_fixture(agent_id="builder")
        self.validator("managed-runtime-state-v1.schema.json").validate(immutable)
        self.assertIn(
            "REPAIR_IMMUTABLE_HISTORY",
            managed_activation_invariant_issues(immutable),
        )

        missing_record = repaired_runtime_fixture()
        missing_record["repairs"] = []
        self.validator("managed-runtime-state-v1.schema.json").validate(
            missing_record
        )
        self.assertIn(
            "GRAPH_REPAIR_HISTORY_INVALID",
            managed_activation_invariant_issues(missing_record),
        )

        task_to_terminal = completed_runtime_fixture()
        source_definition = task_to_terminal["graph_revisions"][0]["definition"]
        target_index = next(
            index
            for index, node in enumerate(source_definition["nodes"])
            if node["id"] == "completed"
        )
        source_definition["nodes"][target_index] = {
            "id": "completed",
            "agent": {"id": "finisher", "name": "Finisher", "phone": "2864"},
            "tasks": [
                {
                    "task_id": "FINISH-1",
                    "queue": "worker-all",
                    "message": "Finish",
                }
            ],
            "workspace": {"access": "read"},
            "activation_policy": "any_parent",
            "transitions": {"DONE": "final"},
        }
        source_definition["nodes"].append(
            {
                "id": "final",
                "type": "terminal",
                "status": "DONE",
                "message": "Final",
            }
        )
        task_to_terminal["graph_revisions"][0]["definition_sha256"] = (
            canonical_json_sha256(source_definition)
        )
        task_to_terminal["status"] = "active"
        token = task_to_terminal["workflow"]["transition_tokens"][0]
        token.update(
            {
                "status": "available",
                "target_graph_revision": None,
                "consumed_by_occurrence_id": None,
            }
        )
        self.assertEqual(managed_activation_invariant_issues(task_to_terminal), ())

        replacement = {
            "id": "completed",
            "type": "terminal",
            "status": "DONE",
            "message": "Completed",
        }
        terminal_patch = {
            "future_nodes": [replacement],
            "remove_future_node_ids": ["final"],
        }
        terminal_repair_id = "repair-terminal"
        terminal_repair_commit = "e" * 40
        repaired_definition = copy.deepcopy(source_definition)
        repaired_definition["nodes"] = [
            replacement if node["id"] == "completed" else node
            for node in repaired_definition["nodes"]
            if node["id"] != "final"
        ]
        task_to_terminal["repairs"] = [
            {
                "repair_id": terminal_repair_id,
                "from_revision": 1,
                "to_revision": 2,
                "repair_source_commit": terminal_repair_commit,
                "idempotency_key": "repair-terminal-key",
                "request_fingerprint": repair_request_fingerprint(
                    SPRINT_ID, 1, terminal_repair_commit, terminal_patch
                ),
                "patch": terminal_patch,
                "response": {
                    "sprint_id": SPRINT_ID,
                    "from_revision": 1,
                    "graph_revision": 2,
                    "repair_source_commit": terminal_repair_commit,
                    "deduplicated": False,
                },
                "created_at": TIMESTAMP,
            }
        ]
        task_to_terminal["graph_revisions"].append(
            {
                "revision": 2,
                "definition_sha256": canonical_json_sha256(repaired_definition),
                "definition": repaired_definition,
                "artifact_source_commit": terminal_repair_commit,
                "created_at": TIMESTAMP,
                "source": "repair",
                "repair_id": terminal_repair_id,
            }
        )
        task_to_terminal["graph_revision"] = 2
        task_to_terminal["workflow"]["graph_revision"] = 2
        self.validator("managed-runtime-state-v1.schema.json").validate(
            task_to_terminal
        )
        issues = managed_activation_invariant_issues(task_to_terminal)
        self.assertIn("TRANSITION_TOKEN_TARGET_INVALID", issues)
        self.assertIn("ACTIVE_ASSIGNMENTS_INVALID", issues)

        token.update({"status": "terminal", "target_graph_revision": 2})
        task_to_terminal["status"] = "completed"
        self.assertEqual(managed_activation_invariant_issues(task_to_terminal), ())

    def test_repair_preserves_live_assignment_and_available_token_routes(self) -> None:
        live_assignment = active_runtime_fixture()
        apply_moved_coordinator_repair(live_assignment)
        for revision in live_assignment["graph_revisions"]:
            self.assertEqual(
                managed_graph_semantic_issues(revision["definition"]), ()
            )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            live_assignment
        )
        self.assertIn(
            "REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE",
            managed_activation_invariant_issues(live_assignment),
        )

        prepared_assignment = active_runtime_fixture()
        prepared_assignment["assignments"][0]["status"] = "prepared"
        prepared_assignment["workflow"]["occurrences"][0]["state"] = "prepared"
        prepared_assignment["active_assignment_ids"] = []
        prepared_assignment["allowed_outcomes_by_assignment"] = {}
        apply_moved_coordinator_repair(prepared_assignment)
        self.validator("managed-runtime-state-v1.schema.json").validate(
            prepared_assignment
        )
        self.assertIn(
            "REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE",
            managed_activation_invariant_issues(prepared_assignment),
        )

        reviewing_done = active_runtime_fixture()
        reviewing_done["assignments"][0].update(
            {"status": "reviews_pending", "outcome": "DONE", "result_commit": COMMIT}
        )
        reviewing_done["workflow"]["occurrences"][0]["state"] = "reviews_pending"
        apply_moved_coordinator_repair(reviewing_done)
        self.validator("managed-runtime-state-v1.schema.json").validate(
            reviewing_done
        )
        self.assertNotIn(
            "REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE",
            managed_activation_invariant_issues(reviewing_done),
        )

        reviewing_stop = copy.deepcopy(reviewing_done)
        reviewing_stop["assignments"][0]["outcome"] = "STOP"
        self.assertIn(
            "REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE",
            managed_activation_invariant_issues(reviewing_stop),
        )

        state = pending_coordinator_runtime_fixture()
        apply_moved_coordinator_repair(state)
        self.validator("managed-runtime-state-v1.schema.json").validate(state)
        self.assertIn(
            "TRANSITION_TOKEN_TARGET_INELIGIBLE",
            managed_activation_invariant_issues(state),
        )

    def test_consumed_token_requires_durable_next_handoff(self) -> None:
        valid = successor_runtime_fixture()
        self.validator("managed-runtime-state-v1.schema.json").validate(valid)
        self.assertEqual(managed_activation_invariant_issues(valid), ())

        lost_handoff = successor_runtime_fixture()
        lost_handoff["transition_journal"][0].update(
            {
                "state": "TRANSITION_COMMITTED",
                "target_occurrence_ids": [],
                "outbox_event_ids": [],
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(lost_handoff)
        self.assertIn(
            "TRANSITION_JOURNAL_EFFECT_INVALID",
            managed_activation_invariant_issues(lost_handoff),
        )

    def test_committed_available_token_is_durable_scheduler_work(self) -> None:
        pending = pending_successor_runtime_fixture()
        self.validator("managed-runtime-state-v1.schema.json").validate(pending)
        self.assertEqual(managed_activation_invariant_issues(pending), ())

        pending_coordinator = pending_coordinator_runtime_fixture()
        self.validator("managed-runtime-state-v1.schema.json").validate(
            pending_coordinator
        )
        self.assertEqual(
            managed_activation_invariant_issues(pending_coordinator), ()
        )

        incomplete_stage = copy.deepcopy(pending)
        incomplete_stage["transition_journal"][0].update(
            {"state": "REVIEWS_ACCEPTED", "transition_token_ids": []}
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            incomplete_stage
        )
        issues = managed_activation_invariant_issues(incomplete_stage)
        self.assertIn("TRANSITION_JOURNAL_EFFECT_INVALID", issues)
        self.assertIn("ACTIVE_ASSIGNMENTS_INVALID", issues)

        live_source = successor_runtime_fixture()
        live_source["assignments"][0].update(
            {"status": "reviews_pending", "completed_at": None}
        )
        live_source["workflow"]["occurrences"][0].update(
            {"state": "reviews_pending", "completed_at": None}
        )
        live_source["branch_leases"][0].update(
            {"status": "active", "released_at": None}
        )
        live_source["active_assignment_ids"].append("assignment-1")
        live_source["allowed_outcomes_by_assignment"]["assignment-1"] = [
            "DONE",
            "STOP",
            "NEED_DECISION",
        ]
        self.validator("managed-runtime-state-v1.schema.json").validate(live_source)
        self.assertIn(
            "TRANSITION_SOURCE_NOT_SETTLED",
            managed_activation_invariant_issues(live_source),
        )

    def test_dangling_rework_and_duplicate_execution_are_rejected(self) -> None:
        dangling = active_runtime_fixture()
        dangling["reworks"].append(
            {
                "rework_id": "rework-ghost",
                "rejected_result_key": "result-" + "f" * 64,
                "reviewer_id": "reviewer-a",
                "feedback": "Fix it",
                "rework_cycle": 1,
                "new_assignment_id": "assignment-ghost",
                "committed_at": TIMESTAMP,
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(dangling)
        self.assertIn(
            "REWORK_BINDING_INVALID",
            managed_activation_invariant_issues(dangling),
        )

        duplicate = active_runtime_fixture()
        extra_assignment = copy.deepcopy(duplicate["assignments"][0])
        extra_assignment.update(
            {
                "assignment_id": "assignment-2",
                "workspace_id": "workspace-2",
                "branch_lease_id": "branch-lease-2",
                "status": "blocked",
                "completed_at": TIMESTAMP,
            }
        )
        duplicate["assignments"].append(extra_assignment)
        duplicate["workflow"]["occurrences"][0]["assignment_ids"].append(
            "assignment-2"
        )
        self.assertIn(
            "OCCURRENCE_ASSIGNMENT_LINEAGE_INVALID",
            managed_activation_invariant_issues(duplicate),
        )

    def test_runtime_nested_lifecycle_schemas(self) -> None:
        process_validator = self.def_validator("managed-runtime-state-v2.schema.json", "process")
        process = process_fixture()
        process_validator.validate(process)
        invalid_process = copy.deepcopy(process)
        invalid_process["max_restart_attempts"] = 0
        with self.assertRaises(ValidationError):
            process_validator.validate(invalid_process)

        contradictory_live = copy.deepcopy(process)
        contradictory_live["terminal_reason"] = "failure"
        with self.assertRaises(ValidationError):
            process_validator.validate(contradictory_live)

        failed = copy.deepcopy(process)
        failed.update(
            {
                "state": "FAILED",
                "failed_at": TIMESTAMP,
                "terminal_reason": "failure",
            }
        )
        process_validator.validate(failed)
        cancelled = {**failed, "terminal_reason": "operator_cancelled"}
        process_validator.validate(cancelled)
        with self.assertRaises(ValidationError):
            process_validator.validate(
                {**failed, "terminal_reason": "operator_stopped"}
            )

        review_validator = self.def_validator(
            "managed-runtime-state-v1.schema.json", "reviewAssignment"
        )
        review_assignment = {
            "assignment_id": "review-assignment-1",
            "source_assignment_id": "assignment-1",
            "result_key": "result-" + "f" * 64,
            "result_commit": COMMIT,
            "result_outcome": "DONE",
            "reviewer_id": "reviewer-a",
            "reviewer_phone": "2891",
            "reviewer_index": 1,
            "status": "prepared",
            "decision": None,
            "request_fingerprint": None,
            "response": None,
            "created_at": TIMESTAMP,
            "activated_at": None,
            "decided_at": None,
        }
        review_validator.validate(review_assignment)
        invalid_review = copy.deepcopy(review_assignment)
        invalid_review["status"] = "active"
        with self.assertRaises(ValidationError):
            review_validator.validate(invalid_review)

    def test_restart_budget_cross_field_invariant(self) -> None:
        state = active_runtime_fixture(include_process_definition=True)
        process = process_fixture()
        process["restart_attempt"] = 2
        process["restart_of_process_id"] = "process-old"
        process["max_restart_attempts"] = 1
        state["processes"].append(process)
        state["schema_version"] = 2
        state["port_leases"].append(
            {
                "lease_id": "port-lease-1",
                "instance_id": "umse-staging",
                "network_namespace_id": "host",
                "assignment_id": "assignment-1",
                "process_id": None,
                "host": "127.0.0.1",
                "port": 18100,
                "status": "reserved",
                "bind_verified": False,
                "acquired_at": TIMESTAMP,
                "released_at": None,
            }
        )
        self.validator("managed-runtime-state-v2.schema.json").validate(state)
        self.assertIn(
            "PROCESS_RESTART_BUDGET_INVALID",
            managed_activation_invariant_issues(state),
        )

    def test_process_configured_assignment_requires_one_attempt_chain(self) -> None:
        missing = active_runtime_fixture(include_process_definition=True)
        self.validator("managed-runtime-state-v1.schema.json").validate(missing)
        self.assertIn(
            "PROCESS_ASSIGNMENT_COVERAGE_INVALID",
            managed_activation_invariant_issues(missing),
        )

        unexpected = active_runtime_fixture()
        unexpected["schema_version"] = 2
        unexpected["processes"] = [process_fixture()]
        unexpected["port_leases"] = [
            {
                "lease_id": "port-lease-1",
                "instance_id": "umse-staging",
                "network_namespace_id": "host",
                "assignment_id": "assignment-1",
                "process_id": None,
                "host": "127.0.0.1",
                "port": 18100,
                "status": "reserved",
                "bind_verified": False,
                "acquired_at": TIMESTAMP,
                "released_at": None,
            }
        ]
        self.validator("managed-runtime-state-v2.schema.json").validate(unexpected)
        self.assertIn(
            "PROCESS_ASSIGNMENT_COVERAGE_INVALID",
            managed_activation_invariant_issues(unexpected),
        )

    def test_process_records_bind_policy_ownership_and_one_restart_chain(self) -> None:
        state = active_runtime_fixture(include_process_definition=True)
        state["schema_version"] = 2
        process = process_fixture()
        process.update(
            {
                "restart_policy": "never",
                "max_restart_attempts": 0,
            }
        )
        port = {
            "lease_id": "port-lease-1",
            "instance_id": "umse-staging",
            "network_namespace_id": "host",
            "assignment_id": "assignment-1",
            "process_id": None,
            "host": "127.0.0.1",
            "port": 18100,
            "status": "reserved",
            "bind_verified": False,
            "acquired_at": TIMESTAMP,
            "released_at": None,
        }
        state["processes"] = [process]
        state["port_leases"] = [port]
        self.validator("managed-runtime-state-v2.schema.json").validate(state)
        self.assertEqual(managed_activation_invariant_issues(state), ())

        wrong_command = copy.deepcopy(state)
        wrong_command["processes"][0]["command_redacted"] = ["evil.exe"]
        self.assertIn(
            "PROCESS_CONFIGURATION_MISMATCH",
            managed_activation_invariant_issues(wrong_command),
        )

        wrong_paths = copy.deepcopy(state)
        wrong_paths["processes"][0]["runtime_root"] = (
            "D:/nginx-qa-staging/runtime/processes/other-process"
        )
        self.assertIn(
            "PROCESS_PATH_CONFIGURATION_MISMATCH",
            managed_activation_invariant_issues(wrong_paths),
        )

        reused_port_lease = copy.deepcopy(state)
        port_reuser = copy.deepcopy(process)
        port_reuser.update(
            {
                "process_id": "process-2",
                "runtime_root": "D:/nginx-qa-staging/runtime/processes/process-2",
                "stdout_log": "D:/nginx-qa-staging/logs/process-2.stdout.log",
                "stderr_log": "D:/nginx-qa-staging/logs/process-2.stderr.log",
            }
        )
        reused_port_lease["processes"].append(port_reuser)
        self.validator("managed-runtime-state-v2.schema.json").validate(
            reused_port_lease
        )
        self.assertIn(
            "PROCESS_PORT_LEASE_REUSE_INVALID",
            managed_activation_invariant_issues(reused_port_lease),
        )

        duplicate_live = copy.deepcopy(state)
        second_process = copy.deepcopy(process)
        second_process.update(
            {
                "process_id": "process-2",
                "port_lease_id": "port-lease-2",
                "health_endpoint": {
                    "host": "127.0.0.1",
                    "port": 18101,
                    "path": "/health",
                },
            }
        )
        second_port = copy.deepcopy(port)
        second_port.update({"lease_id": "port-lease-2", "port": 18101})
        duplicate_live["processes"].append(second_process)
        duplicate_live["port_leases"].append(second_port)
        self.validator("managed-runtime-state-v2.schema.json").validate(
            duplicate_live
        )
        self.assertIn(
            "PROCESS_CHAIN_INVALID",
            managed_activation_invariant_issues(duplicate_live),
        )

        invalid_never_restart = process_fixture()
        invalid_never_restart.update(
            {
                "restart_policy": "never",
                "restart_attempt": 1,
                "restart_of_process_id": "process-old",
                "max_restart_attempts": 1,
            }
        )
        with self.assertRaises(ValidationError):
            self.def_validator(
                "managed-runtime-state-v2.schema.json", "process"
            ).validate(invalid_never_restart)

    def test_repository_provenance_is_reciprocal(self) -> None:
        referenced_credential = active_runtime_fixture()
        referenced_credential["repository"]["credential_reference"] = (
            "env:GITHUB_TOKEN"
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            referenced_credential
        )
        self.assertEqual(
            managed_activation_invariant_issues(referenced_credential), ()
        )

        raw_credential = active_runtime_fixture()
        raw_credential["repository"]["credential_reference"] = (
            "ghp_1234567890ABCDEFGHIJK"
        )
        with self.assertRaises(ValidationError):
            self.validator("managed-runtime-state-v1.schema.json").validate(
                raw_credential
            )

        disguised_credential = active_runtime_fixture()
        disguised_credential["repository"]["credential_reference"] = (
            "vault:ghp_1234567890ABCDEFGHIJK"
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            disguised_credential
        )
        self.assertIn(
            "REPOSITORY_CREDENTIAL_REFERENCE_INVALID",
            managed_activation_invariant_issues(disguised_credential),
        )

        wrong_remote = active_runtime_fixture()
        wrong_remote["workspaces"][0]["repository_remote"] = (
            "https://evil.invalid/repo.git"
        )
        self.assertIn(
            "ASSIGNMENT_WORKSPACE_BINDING_INVALID",
            managed_activation_invariant_issues(wrong_remote),
        )

        wrong_mirror = active_runtime_fixture()
        wrong_mirror["repository"]["mirror_storage_key"] = "mirror-" + "0" * 64
        wrong_mirror["branch_leases"][0]["mirror_storage_key"] = (
            "mirror-" + "0" * 64
        )
        self.assertIn(
            "REPOSITORY_PROVENANCE_INVALID",
            managed_activation_invariant_issues(wrong_mirror),
        )

        wrong_manifest_assertion = active_runtime_fixture()
        wrong_definition = wrong_manifest_assertion["graph_revisions"][0][
            "definition"
        ]
        wrong_definition["git_address"] = "https://different.invalid/repository.git"
        wrong_manifest_assertion["graph_revisions"][0][
            "definition_sha256"
        ] = canonical_json_sha256(wrong_definition)
        self.assertIn(
            "REPOSITORY_IDENTITY_MISMATCH",
            managed_activation_invariant_issues(wrong_manifest_assertion),
        )

        equivalent_manifest_assertion = active_runtime_fixture()
        equivalent_definition = equivalent_manifest_assertion["graph_revisions"][0][
            "definition"
        ]
        equivalent_definition["git_address"] = (
            "git@github.com:OWNER/Repository.git"
        )
        equivalent_manifest_assertion["graph_revisions"][0][
            "definition_sha256"
        ] = canonical_json_sha256(equivalent_definition)
        self.assertNotIn(
            "REPOSITORY_IDENTITY_MISMATCH",
            managed_activation_invariant_issues(equivalent_manifest_assertion),
        )

    def test_integration_workspace_is_deterministic_and_reciprocal(self) -> None:
        state, trigger_ids = prepared_integration_runtime_fixture()
        integration = state["integrations"][0]
        artifact = state["integration_workspaces"][0]
        self.validator("managed-runtime-state-v1.schema.json").validate(state)
        issues = managed_activation_invariant_issues(state)
        self.assertEqual(issues, ())
        self.assertNotIn("INTEGRATION_PROVENANCE_INVALID", issues)
        self.assertNotIn("INTEGRATION_WORKSPACE_BINDING_INVALID", issues)
        self.assertNotIn("INTEGRATION_COORDINATOR_CONTEXT_INVALID", issues)
        self.assertNotIn("JOIN_INTEGRATION_COVERAGE_INVALID", issues)

        join_only_active = copy.deepcopy(state)
        join_only_active["active_assignment_ids"] = []
        join_only_active["allowed_outcomes_by_assignment"] = {}
        self.validator("managed-runtime-state-v1.schema.json").validate(
            join_only_active
        )
        self.assertEqual(managed_activation_invariant_issues(join_only_active), ())
        self.assertNotIn(
            "ACTIVE_ASSIGNMENTS_INVALID",
            managed_activation_invariant_issues(join_only_active),
        )
        stranded_join = copy.deepcopy(join_only_active)
        stranded_join["integrations"] = []
        stranded_join["integration_workspaces"] = []
        stranded_issues = managed_activation_invariant_issues(stranded_join)
        self.assertIn("JOIN_INTEGRATION_COVERAGE_INVALID", stranded_issues)
        self.assertIn("ACTIVE_ASSIGNMENTS_INVALID", stranded_issues)

        any_parent = copy.deepcopy(state)
        any_parent_join = any_parent["graph_revisions"][0]["definition"]["nodes"][2]
        any_parent_join["activation_policy"] = "any_parent"
        any_parent_join.pop("join_parent_order")
        any_parent["graph_revisions"][0]["definition_sha256"] = canonical_json_sha256(
            any_parent["graph_revisions"][0]["definition"]
        )
        self.assertIn(
            "INTEGRATION_PROVENANCE_INVALID",
            managed_activation_invariant_issues(any_parent),
        )

        wrong_parent_order = copy.deepcopy(state)
        wrong_parent_order["integrations"][0]["parents"].reverse()
        self.assertIn(
            "INTEGRATION_PROVENANCE_INVALID",
            managed_activation_invariant_issues(wrong_parent_order),
        )

        same_commits = copy.deepcopy(state)
        same_commits["result_receipts"][0]["result_commit"] = "d" * 40
        same_commits["integrations"][0]["parents"][1]["commit"] = "d" * 40
        self.assertIn(
            "INTEGRATION_PROVENANCE_INVALID",
            managed_activation_invariant_issues(same_commits),
        )
        self.assertIn(
            "JOIN_READY_NOT_COMMITTED",
            managed_activation_invariant_issues(same_commits),
        )

        consumed_while_prepared = copy.deepcopy(state)
        consumed_while_prepared["workflow"]["transition_tokens"][0].update(
            {
                "status": "consumed",
                "target_graph_revision": 1,
                "consumed_by_occurrence_id": "occ-" + "0" * 64,
            }
        )
        self.assertIn(
            "INTEGRATION_PROVENANCE_INVALID",
            managed_activation_invariant_issues(consumed_while_prepared),
        )

        wrong_head = copy.deepcopy(state)
        wrong_head["integration_workspaces"][0]["head_commit"] = "e" * 40
        self.assertIn(
            "INTEGRATION_WORKSPACE_BINDING_INVALID",
            managed_activation_invariant_issues(wrong_head),
        )

        orphan = copy.deepcopy(state)
        orphan["integrations"] = []
        self.assertIn(
            "INTEGRATION_WORKSPACE_BINDING_INVALID",
            managed_activation_invariant_issues(orphan),
        )

        aliased_assignment_root = copy.deepcopy(state)
        windows_alias = WORKSPACE_1_ROOT.replace("/", "\\")
        aliased_assignment_root["integration_workspaces"][0].update(
            {
                "expected_root": windows_alias,
                "actual_git_toplevel": windows_alias,
                "actual_git_dir": windows_alias + "\\.git",
            }
        )
        self.assertIn(
            "WORKSPACE_ROOT_OWNERSHIP_CONFLICT",
            managed_activation_invariant_issues(aliased_assignment_root),
        )

        released = copy.deepcopy(artifact)
        released.update(
            {
                "artifact_status": "released",
                "working_tree_state": "unavailable",
                "released_at": TIMESTAMP,
            }
        )
        self.def_validator(
            "managed-runtime-state-v1.schema.json", "integrationWorkspaceArtifact"
        ).validate(released)
        invalid_release = copy.deepcopy(released)
        invalid_release["working_tree_state"] = "clean"
        with self.assertRaises(ValidationError):
            self.def_validator(
                "managed-runtime-state-v1.schema.json",
                "integrationWorkspaceArtifact",
            ).validate(invalid_release)

        conflict = copy.deepcopy(state)
        normalized_error = {"code": "MERGE_CONFLICT"}
        conflict["integrations"][0].update(
            {"status": "CONFLICT", "normalized_error": normalized_error}
        )
        conflict["integration_workspaces"][0].update(
            {"artifact_status": "preserved", "working_tree_state": "conflicted"}
        )
        self.assertIn(
            "INTEGRATION_COORDINATOR_CONTEXT_INVALID",
            managed_activation_invariant_issues(conflict),
        )
        conflict_artifact = conflict["integration_workspaces"][0]
        workspace_status = {
            field: conflict_artifact[field]
            for field in (
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
        context = {
            "context_id": "context-join",
            "graph_revision": 1,
            "failure_scope": "integration",
            "reason_code": "JOIN_MERGE_CONFLICT",
            "import_attempt_id": None,
            "failed_assignment_id": None,
            "join_target": {
                "target_graph_revision": 1,
                "target_node_id": "join",
                "trigger_token_ids": trigger_ids,
                "integration_id": conflict["integrations"][0]["integration_id"],
            },
            "assigned_branch": None,
            "source_commit": None,
            "result_commit": None,
            "diff_summary": {},
            "workspace_status": workspace_status,
            "process_records": [],
            "port_records": [],
            "test_evidence_summary": {},
            "normalized_error": normalized_error,
            "reviewer_feedback": [],
        }
        conflict["coordinator_contexts"] = [context]
        conflict["outbox"].append(
            {
                "event_id": "event-context-join",
                "dedupe_key": f"enqueue:coordinator:{SPRINT_ID}:context-join",
                "event_type": "COORDINATOR_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "context_id": "context-join",
                    "coordinator_phone": "2860",
                    "reason_code": "JOIN_MERGE_CONFLICT",
                },
                "status": "delivered",
                "created_at": TIMESTAMP,
                "delivered_at": TIMESTAMP,
                "queue_receipt_id": "queue-receipt-context-join",
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(conflict)
        conflict_issues = managed_activation_invariant_issues(conflict)
        self.assertNotIn("INTEGRATION_COORDINATOR_CONTEXT_INVALID", conflict_issues)
        self.assertNotIn("COORDINATOR_CONTEXT_BINDING_INVALID", conflict_issues)
        self.assertNotIn("COORDINATOR_OUTBOX_COVERAGE_INVALID", conflict_issues)

        wrong_reason = copy.deepcopy(conflict)
        wrong_reason["coordinator_contexts"][0]["reason_code"] = (
            "JOIN_INTEGRATION_FAILED"
        )
        wrong_reason["outbox"][-1]["payload"]["reason_code"] = (
            "JOIN_INTEGRATION_FAILED"
        )
        self.assertIn(
            "COORDINATOR_CONTEXT_BINDING_INVALID",
            managed_activation_invariant_issues(wrong_reason),
        )

        duplicate_context = copy.deepcopy(conflict)
        second_context = copy.deepcopy(context)
        second_context["context_id"] = "context-join-duplicate"
        duplicate_context["coordinator_contexts"].append(second_context)
        second_event = copy.deepcopy(conflict["outbox"][-1])
        second_event.update(
            {
                "event_id": "event-context-join-duplicate",
                "dedupe_key": (
                    f"enqueue:coordinator:{SPRINT_ID}:context-join-duplicate"
                ),
                "queue_receipt_id": "queue-receipt-context-join-duplicate",
            }
        )
        second_event["payload"]["context_id"] = "context-join-duplicate"
        duplicate_context["outbox"].append(second_event)
        duplicate_issues = managed_activation_invariant_issues(duplicate_context)
        self.assertIn("INTEGRATION_COORDINATOR_CONTEXT_INVALID", duplicate_issues)
        self.assertIn("JOIN_COORDINATOR_CONTEXT_INVALID", duplicate_issues)

        repaired_join = copy.deepcopy(conflict)
        repair_id = "repair-join"
        repair_commit = "f" * 40
        repair_patch = {"profiles": {"joiner": "recovered-join-profile"}}
        repair_request = {
            "expected_revision": 1,
            "repair_source_commit": repair_commit,
            "idempotency_key": "repair-join-key",
            "patch": repair_patch,
        }
        repair_response = {
            "sprint_id": SPRINT_ID,
            "from_revision": 1,
            "graph_revision": 2,
            "repair_source_commit": repair_commit,
            "deduplicated": False,
        }
        repaired_definition = copy.deepcopy(
            repaired_join["graph_revisions"][0]["definition"]
        )
        repaired_definition["nodes"][2]["agent"]["profile"] = (
            "recovered-join-profile"
        )
        repaired_join["repairs"] = [
            {
                "repair_id": repair_id,
                "from_revision": 1,
                "to_revision": 2,
                "repair_source_commit": repair_commit,
                "idempotency_key": "repair-join-key",
                "request_fingerprint": repair_request_fingerprint(
                    SPRINT_ID, 1, repair_commit, repair_patch
                ),
                "patch": repair_patch,
                "response": repair_response,
                "created_at": TIMESTAMP,
            }
        ]
        repaired_join["graph_revisions"].append(
            {
                "revision": 2,
                "definition_sha256": canonical_json_sha256(repaired_definition),
                "definition": repaired_definition,
                "artifact_source_commit": repair_commit,
                "created_at": TIMESTAMP,
                "source": "repair",
                "repair_id": repair_id,
            }
        )
        repaired_join["graph_revision"] = 2
        repaired_join["workflow"]["graph_revision"] = 2
        new_integration_id = managed_integration_id(
            SPRINT_ID, 2, "join", trigger_ids
        )
        new_workspace_artifact_id = managed_integration_workspace_id(
            SPRINT_ID, 2, "join", trigger_ids
        )
        new_integration = copy.deepcopy(state["integrations"][0])
        new_integration.update(
            {
                "integration_id": new_integration_id,
                "target_graph_revision": 2,
                "workspace_artifact_id": new_workspace_artifact_id,
            }
        )
        new_artifact = copy.deepcopy(state["integration_workspaces"][0])
        new_root = (
            "D:/nginx-qa-staging/managed/integration-workspaces/"
            + new_workspace_artifact_id
        )
        new_artifact.update(
            {
                "workspace_artifact_id": new_workspace_artifact_id,
                "integration_id": new_integration_id,
                "expected_root": new_root,
                "actual_git_toplevel": new_root,
                "actual_git_dir": new_root + "/.git",
            }
        )
        repaired_join["integrations"].append(new_integration)
        repaired_join["integration_workspaces"].append(new_artifact)
        produced_ids = [repair_id, new_integration_id, new_workspace_artifact_id]
        recovery_id = "recovery-join"
        recovery_parameters = {"request": repair_request}
        repaired_join["recovery_records"] = [
            {
                "recovery_id": recovery_id,
                "coordinator_id": "coordinator",
                "context_id": context["context_id"],
                "graph_revision": 1,
                "idempotency_key": "recovery-join-key",
                "request_fingerprint": recovery_request_fingerprint(
                    context["context_id"],
                    1,
                    "APPLY_REPAIR",
                    None,
                    None,
                    recovery_parameters,
                    context["join_target"],
                ),
                "action": "APPLY_REPAIR",
                "parameters": recovery_parameters,
                "import_attempt_id": None,
                "assignment_id": None,
                "join_target": context["join_target"],
                "produced_record_ids": produced_ids,
                "response": {
                    "recovery_id": recovery_id,
                    "action": "APPLY_REPAIR",
                    "status": "RECOVERY_COMPLETED",
                    "produced_record_ids": produced_ids,
                },
                "normalized_error": None,
                "evidence": {},
                "status": "completed",
                "created_at": TIMESTAMP,
                "completed_at": TIMESTAMP,
            }
        ]
        self.validator("managed-runtime-state-v1.schema.json").validate(
            repaired_join
        )
        self.assertEqual(managed_activation_invariant_issues(repaired_join), ())

        pending_shape_change = copy.deepcopy(repaired_join)
        changed_join = copy.deepcopy(
            conflict["graph_revisions"][0]["definition"]["nodes"][2]
        )
        changed_join["activation_policy"] = "any_parent"
        changed_join.pop("join_parent_order")
        incompatible_patch = {"future_nodes": [changed_join]}
        incompatible_request = {
            "expected_revision": 1,
            "repair_source_commit": repair_commit,
            "idempotency_key": "repair-join-key",
            "patch": incompatible_patch,
        }
        incompatible_definition = copy.deepcopy(
            conflict["graph_revisions"][0]["definition"]
        )
        incompatible_definition["nodes"][2] = changed_join
        pending_shape_change["repairs"][0].update(
            {
                "request_fingerprint": repair_request_fingerprint(
                    SPRINT_ID, 1, repair_commit, incompatible_patch
                ),
                "patch": incompatible_patch,
            }
        )
        pending_shape_change["graph_revisions"][1].update(
            {
                "definition_sha256": canonical_json_sha256(
                    incompatible_definition
                ),
                "definition": incompatible_definition,
            }
        )
        pending_shape_change["integrations"] = [
            pending_shape_change["integrations"][0]
        ]
        pending_shape_change["integration_workspaces"] = [
            pending_shape_change["integration_workspaces"][0]
        ]
        pending_recovery = pending_shape_change["recovery_records"][0]
        pending_recovery.update(
            {
                "request_fingerprint": recovery_request_fingerprint(
                    context["context_id"],
                    1,
                    "APPLY_REPAIR",
                    None,
                    None,
                    {"request": incompatible_request},
                    context["join_target"],
                ),
                "parameters": {"request": incompatible_request},
                "produced_record_ids": [],
                "response": None,
                "status": "pending",
                "completed_at": None,
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            pending_shape_change
        )
        self.assertIn(
            "REPAIR_JOIN_RECHECK_INVALID",
            managed_activation_invariant_issues(pending_shape_change),
        )

        pending_after_publish = copy.deepcopy(repaired_join)
        pending_recovery = pending_after_publish["recovery_records"][0]
        pending_recovery.update(
            {
                "produced_record_ids": [],
                "response": None,
                "status": "pending",
                "completed_at": None,
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            pending_after_publish
        )
        self.assertEqual(
            managed_activation_invariant_issues(pending_after_publish), ()
        )

        failed_after_publish = copy.deepcopy(repaired_join)
        failed_recovery = failed_after_publish["recovery_records"][0]
        failed_recovery.update(
            {
                "produced_record_ids": [],
                "response": None,
                "normalized_error": {"code": "RECOVERY_FAILED"},
                "status": "failed",
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            failed_after_publish
        )
        self.assertIn(
            "RECOVERY_EFFECT_BINDING_INVALID",
            managed_activation_invariant_issues(failed_after_publish),
        )

        committed_recheck = copy.deepcopy(repaired_join)
        integration_commit = "1" * 40
        target_occurrence_id = managed_occurrence_id(
            SPRINT_ID, 2, "join", 1, trigger_ids
        )
        committed_recheck["integrations"][1].update(
            {
                "status": "COMMITTED",
                "integration_commit": integration_commit,
                "target_occurrence_id": target_occurrence_id,
                "assignment_id": "join-assignment",
            }
        )
        committed_recheck["integration_workspaces"][1]["head_commit"] = (
            integration_commit
        )
        committed_recheck["workflow"]["occurrences"].append(
            {
                "occurrence_id": target_occurrence_id,
                "node_id": "join",
                "graph_revision": 2,
                "generation": 1,
                "activation_policy": "all_parents",
                "trigger_token_ids": trigger_ids,
                "state": "active",
                "assignment_ids": ["join-assignment"],
                "created_at": TIMESTAMP,
                "completed_at": None,
            }
        )
        for token in committed_recheck["workflow"]["transition_tokens"]:
            token.update(
                {
                    "status": "consumed",
                    "target_graph_revision": 2,
                    "consumed_by_occurrence_id": target_occurrence_id,
                }
            )
        join_workspace_root = (
            "D:/nginx-qa-staging/managed/projects/project-id/sprints/"
            f"{SPRINT_PATH_SEGMENT}/nodes/join/join-assignment"
        )
        committed_recheck["assignments"].append(
            {
                "assignment_id": "join-assignment",
                "occurrence_id": target_occurrence_id,
                "node_id": "join",
                "agent_id": "joiner",
                "agent_phone": "2863",
                "graph_revision": 2,
                "source_kind": "integration",
                "source_result_keys": [
                    parent["result_key"]
                    for parent in committed_recheck["integrations"][1]["parents"]
                ],
                "source_commit": integration_commit,
                "initial_head_commit": integration_commit,
                "integration_id": new_integration_id,
                "rework_cycle": 0,
                "result_commit": None,
                "outcome": None,
                "status": "active",
                "workspace_id": "workspace-join",
                "branch_lease_id": None,
                "allowed_outcomes": ["DONE", "STOP", "NEED_DECISION"],
                "created_at": TIMESTAMP,
                "completed_at": None,
            }
        )
        committed_recheck["workspaces"].append(
            {
                "workspace_id": "workspace-join",
                "project_id": "project-id",
                "sprint_id": SPRINT_ID,
                "node_id": "join",
                "assignment_id": "join-assignment",
                "expected_root": join_workspace_root,
                "actual_git_toplevel": join_workspace_root,
                "actual_git_dir": join_workspace_root + "/.git",
                "repository_id": "main",
                "repository_remote": REPOSITORY_KEY,
                "source_commit": integration_commit,
                "initial_head_commit": integration_commit,
                "assigned_branch": None,
                "working_tree_state": "clean",
            }
        )
        committed_recheck["active_assignment_ids"] = ["join-assignment"]
        committed_recheck["allowed_outcomes_by_assignment"] = {
            "join-assignment": ["DONE", "STOP", "NEED_DECISION"]
        }
        committed_recheck["outbox"].append(
            {
                "event_id": "event-join-assignment",
                "dedupe_key": f"enqueue:assignment:{SPRINT_ID}:join-assignment",
                "event_type": "ASSIGNMENT_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "graph_revision": 2,
                    "assignment_id": "join-assignment",
                    "node_id": "join",
                    "agent_phone": "2863",
                },
                "status": "delivered",
                "created_at": TIMESTAMP,
                "delivered_at": TIMESTAMP,
                "queue_receipt_id": "queue-receipt-join-assignment",
            }
        )
        for journal in committed_recheck["transition_journal"]:
            journal.update(
                {
                    "state": "NEXT_ASSIGNMENT_ENQUEUED",
                    "target_occurrence_ids": [target_occurrence_id],
                    "outbox_event_ids": ["event-join-assignment"],
                }
            )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            committed_recheck
        )
        self.assertEqual(
            managed_activation_invariant_issues(committed_recheck), ()
        )

        later_revision = copy.deepcopy(committed_recheck)
        revision_three_definition = copy.deepcopy(repaired_definition)
        revision_three_definition["nodes"][3]["agent"]["profile"] = (
            "later-coordinator-profile"
        )
        revision_three_patch = {
            "profiles": {"coordinator": "later-coordinator-profile"}
        }
        later_revision["repairs"].append(
            {
                "repair_id": "repair-later",
                "from_revision": 2,
                "to_revision": 3,
                "repair_source_commit": "2" * 40,
                "idempotency_key": "repair-later-key",
                "request_fingerprint": repair_request_fingerprint(
                    SPRINT_ID, 2, "2" * 40, revision_three_patch
                ),
                "patch": revision_three_patch,
                "response": {
                    "sprint_id": SPRINT_ID,
                    "from_revision": 2,
                    "graph_revision": 3,
                    "repair_source_commit": "2" * 40,
                    "deduplicated": False,
                },
                "created_at": TIMESTAMP,
            }
        )
        later_revision["graph_revisions"].append(
            {
                "revision": 3,
                "definition_sha256": canonical_json_sha256(
                    revision_three_definition
                ),
                "definition": revision_three_definition,
                "artifact_source_commit": "2" * 40,
                "created_at": TIMESTAMP,
                "source": "repair",
                "repair_id": "repair-later",
            }
        )
        later_revision["graph_revision"] = 3
        later_revision["workflow"]["graph_revision"] = 3
        self.assertEqual(managed_activation_invariant_issues(later_revision), ())

        hidden_recheck_effects = copy.deepcopy(repaired_join)
        hidden_recheck_effects["recovery_records"][0]["produced_record_ids"] = [
            repair_id
        ]
        hidden_recheck_effects["recovery_records"][0]["response"][
            "produced_record_ids"
        ] = [repair_id]
        self.assertIn(
            "RECOVERY_EFFECT_BINDING_INVALID",
            managed_activation_invariant_issues(hidden_recheck_effects),
        )

        repair_without_recheck = copy.deepcopy(hidden_recheck_effects)
        repair_without_recheck["integrations"] = [
            repair_without_recheck["integrations"][0]
        ]
        repair_without_recheck["integration_workspaces"] = [
            repair_without_recheck["integration_workspaces"][0]
        ]
        missing_recheck_issues = managed_activation_invariant_issues(
            repair_without_recheck
        )
        self.assertIn("JOIN_INTEGRATION_COVERAGE_INVALID", missing_recheck_issues)
        self.assertIn("ACTIVE_ASSIGNMENTS_INVALID", missing_recheck_issues)
        self.assertIn("RECOVERY_EFFECT_BINDING_INVALID", missing_recheck_issues)

        divergence = copy.deepcopy(state)
        divergence_join = divergence["graph_revisions"][0]["definition"]["nodes"][2]
        divergence_join["workspace"]["join_strategy"] = "require_same_commit"
        divergence["graph_revisions"][0]["definition_sha256"] = canonical_json_sha256(
            divergence["graph_revisions"][0]["definition"]
        )
        divergence["integrations"] = []
        divergence["integration_workspaces"] = []
        self.assertIn(
            "JOIN_COORDINATOR_CONTEXT_INVALID",
            managed_activation_invariant_issues(divergence),
        )
        divergence_context = copy.deepcopy(context)
        divergence_context.update(
            {
                "context_id": "context-divergence",
                "reason_code": "JOIN_SOURCE_DIVERGED",
                "workspace_status": {},
                "normalized_error": {"code": "JOIN_SOURCE_DIVERGED"},
            }
        )
        divergence_context["join_target"]["integration_id"] = None
        divergence["coordinator_contexts"] = [divergence_context]
        divergence_event = copy.deepcopy(conflict["outbox"][-1])
        divergence_event.update(
            {
                "event_id": "event-context-divergence",
                "dedupe_key": f"enqueue:coordinator:{SPRINT_ID}:context-divergence",
                "queue_receipt_id": "queue-receipt-context-divergence",
            }
        )
        divergence_event["payload"].update(
            {
                "context_id": "context-divergence",
                "reason_code": "JOIN_SOURCE_DIVERGED",
            }
        )
        divergence["outbox"].append(divergence_event)
        divergence_issues = managed_activation_invariant_issues(divergence)
        self.assertNotIn("JOIN_COORDINATOR_CONTEXT_INVALID", divergence_issues)
        self.assertNotIn("COORDINATOR_CONTEXT_BINDING_INVALID", divergence_issues)

    def test_preparing_and_terminal_failure_boundaries_are_explicit(self) -> None:
        preparing = active_runtime_fixture()
        preparing.update(
            {
                "status": "preparing",
                "workflow": None,
                "active_assignment_ids": [],
                "allowed_outcomes_by_assignment": {},
            }
        )
        for collection in (
            "assignments",
            "result_receipts",
            "review_assignments",
            "reviews",
            "reworks",
            "integrations",
            "integration_workspaces",
            "branch_leases",
            "workspaces",
            "port_leases",
            "processes",
            "transition_journal",
            "outbox",
            "repairs",
            "coordinator_contexts",
            "blocker_observations",
            "recovery_records",
        ):
            preparing[collection] = []
        preparing["import_attempts"][0].update(
            {
                "phase": "PREPARE",
                "status": "running",
                "prepared_artifact_ids": ["prepared-workspace-1"],
                "activation_response": None,
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(preparing)
        self.assertEqual(managed_activation_invariant_issues(preparing), ())

        preparing_with_live_work = active_runtime_fixture()
        preparing_with_live_work["status"] = "preparing"
        with self.assertRaises(ValidationError):
            self.validator("managed-runtime-state-v1.schema.json").validate(
                preparing_with_live_work
            )
        self.assertIn(
            "PREPARING_STATE_HAS_ACTIVATED_WORK",
            managed_activation_invariant_issues(preparing_with_live_work),
        )

        blocked_without_evidence = active_runtime_fixture()
        blocked_without_evidence["status"] = "blocked"
        blocked_without_evidence["active_assignment_ids"] = []
        blocked_without_evidence["allowed_outcomes_by_assignment"] = {}
        blocked_without_evidence["assignments"][0].update(
            {"status": "blocked", "completed_at": TIMESTAMP}
        )
        blocked_without_evidence["workflow"]["occurrences"][0].update(
            {"state": "blocked", "completed_at": TIMESTAMP}
        )
        blocked_without_evidence["branch_leases"][0].update(
            {"status": "released", "released_at": TIMESTAMP}
        )
        with self.assertRaises(ValidationError):
            self.validator("managed-runtime-state-v1.schema.json").validate(
                blocked_without_evidence
            )
        self.assertIn(
            "TERMINAL_FAILURE_EVIDENCE_MISSING",
            managed_activation_invariant_issues(blocked_without_evidence),
        )
        self.assertIn(
            "TERMINAL_TOKEN_PROOF_MISSING",
            managed_activation_invariant_issues(blocked_without_evidence),
        )
        self.assertIn(
            "TERMINAL_BRANCH_PROOF_MISSING",
            managed_activation_invariant_issues(blocked_without_evidence),
        )

        failed_with_succeeded_receipt = active_runtime_fixture()
        failed_with_succeeded_receipt.update(
            {
                "status": "failed",
                "workflow": None,
                "active_assignment_ids": [],
                "allowed_outcomes_by_assignment": {},
                "assignments": [],
                "branch_leases": [],
                "workspaces": [],
                "outbox": [],
                "coordinator_contexts": [
                    {
                        "context_id": "context-import",
                        "graph_revision": 1,
                        "failure_scope": "recovery",
                        "reason_code": "ACTIVATION_STATE_INCONSISTENT",
                        "import_attempt_id": "attempt-1",
                        "failed_assignment_id": None,
                        "join_target": None,
                        "assigned_branch": None,
                        "source_commit": None,
                        "result_commit": None,
                        "diff_summary": {},
                        "workspace_status": {},
                        "process_records": [],
                        "port_records": [],
                        "test_evidence_summary": {},
                        "normalized_error": {"code": "STATE_INCONSISTENT"},
                        "reviewer_feedback": [],
                    }
                ],
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            failed_with_succeeded_receipt
        )
        self.assertIn(
            "IMPORT_ATTEMPT_BINDING_INVALID",
            managed_activation_invariant_issues(failed_with_succeeded_receipt),
        )

        blocked_by_completed_recovery = completed_runtime_fixture()
        blocked_by_completed_recovery["status"] = "blocked"
        blocked_definition = blocked_by_completed_recovery["graph_revisions"][0][
            "definition"
        ]
        blocked_definition["nodes"][2]["status"] = "BLOCKED_EXTERNAL"
        blocked_by_completed_recovery["graph_revisions"][0][
            "definition_sha256"
        ] = canonical_json_sha256(blocked_definition)
        context = {
            "context_id": "context-blocked",
            "graph_revision": 1,
            "failure_scope": "recovery",
            "reason_code": "EXTERNAL_DEPENDENCY",
            "import_attempt_id": None,
            "failed_assignment_id": "assignment-1",
            "join_target": None,
            "assigned_branch": "agent/example",
            "source_commit": COMMIT,
            "result_commit": COMMIT,
            "diff_summary": {},
            "workspace_status": {},
            "process_records": [],
            "port_records": [],
            "test_evidence_summary": {},
            "normalized_error": {"code": "EXTERNAL_DEPENDENCY"},
            "reviewer_feedback": [],
        }
        observation = {
            "fingerprint": "f" * 64,
            "assignment_id": "assignment-1",
            "count": 1,
            "first_seen_at": TIMESTAMP,
            "last_seen_at": TIMESTAMP,
        }
        parameters = {
            "reason_code": "EXTERNAL_DEPENDENCY",
            "operator_action": "Restore the dependency and retry",
        }
        recovery_id = "recovery-blocked"
        recovery = {
            "recovery_id": recovery_id,
            "coordinator_id": "coordinator",
            "context_id": context["context_id"],
            "graph_revision": 1,
            "idempotency_key": "block-external-1",
            "request_fingerprint": recovery_request_fingerprint(
                context["context_id"],
                1,
                "BLOCK_EXTERNAL",
                None,
                "assignment-1",
                parameters,
            ),
            "action": "BLOCK_EXTERNAL",
            "parameters": parameters,
            "import_attempt_id": None,
            "assignment_id": "assignment-1",
            "join_target": None,
            "produced_record_ids": [observation["fingerprint"]],
            "response": {
                "recovery_id": recovery_id,
                "action": "BLOCK_EXTERNAL",
                "status": "RECOVERY_COMPLETED",
                "produced_record_ids": [observation["fingerprint"]],
            },
            "normalized_error": None,
            "evidence": {},
            "status": "completed",
            "created_at": TIMESTAMP,
            "completed_at": TIMESTAMP,
        }
        blocked_by_completed_recovery["coordinator_contexts"] = [context]
        blocked_by_completed_recovery["blocker_observations"] = [observation]
        blocked_by_completed_recovery["recovery_records"] = [recovery]
        blocked_by_completed_recovery["outbox"].append(
            {
                "event_id": "coordinator-event-blocked",
                "dedupe_key": (
                    f"enqueue:coordinator:{SPRINT_ID}:{context['context_id']}"
                ),
                "event_type": "COORDINATOR_ENQUEUE",
                "payload": {
                    "sprint_id": SPRINT_ID,
                    "context_id": context["context_id"],
                    "coordinator_phone": "2860",
                    "reason_code": context["reason_code"],
                },
                "status": "delivered",
                "created_at": TIMESTAMP,
                "delivered_at": TIMESTAMP,
                "queue_receipt_id": "coordinator-queue-receipt-blocked",
            }
        )
        self.validator("managed-runtime-state-v1.schema.json").validate(
            blocked_by_completed_recovery
        )
        self.assertEqual(
            managed_activation_invariant_issues(blocked_by_completed_recovery),
            (),
        )

        recovery_with_unrelated_effect = copy.deepcopy(
            blocked_by_completed_recovery
        )
        recovery_with_unrelated_effect["recovery_records"][0][
            "produced_record_ids"
        ].append("workspace-1")
        recovery_with_unrelated_effect["recovery_records"][0]["response"][
            "produced_record_ids"
        ].append("workspace-1")
        self.validator("managed-runtime-state-v1.schema.json").validate(
            recovery_with_unrelated_effect
        )
        self.assertIn(
            "RECOVERY_EFFECT_BINDING_INVALID",
            managed_activation_invariant_issues(recovery_with_unrelated_effect),
        )

    def test_project_control_schema_and_relational_checker(self) -> None:
        control = project_control_fixture()
        self.validator("managed-project-control-v1.schema.json").validate(control)
        self.assertEqual(
            managed_project_control_invariant_issues(control, {SPRINT_ID}),
            (),
        )

        duplicate = project_control_fixture()
        copied = copy.deepcopy(duplicate["start_idempotency_records"][0])
        copied["attempt_id"] = "attempt-2"
        copied["fencing_token"] = None
        duplicate["start_idempotency_records"].append(copied)
        self.assertIn(
            "START_IDEMPOTENCY_KEY_DUPLICATE",
            managed_project_control_invariant_issues(duplicate),
        )

        bad_recovery = project_control_fixture()
        child = copy.deepcopy(bad_recovery["start_idempotency_records"][0])
        child.update(
            {
                "idempotency_key": "recovery-key",
                "attempt_id": "attempt-2",
                "recovery_of_attempt_id": "attempt-1",
                "recovery_generation": 1,
                "fencing_token": None,
            }
        )
        bad_recovery["start_idempotency_records"].append(child)
        self.assertIn(
            "START_RECOVERY_LINEAGE_INVALID",
            managed_project_control_invariant_issues(bad_recovery),
        )

        sibling_recovery = copy.deepcopy(child)
        sibling_recovery.update(
            {
                "idempotency_key": "recovery-key-sibling",
                "attempt_id": "attempt-3",
            }
        )
        bad_recovery["start_idempotency_records"].append(sibling_recovery)
        self.assertIn(
            "START_RECOVERY_CHILD_DUPLICATE",
            managed_project_control_invariant_issues(bad_recovery),
        )

        missing_active_pointer = project_control_fixture()
        missing_active_pointer["active_sprint_id"] = None
        self.validator("managed-project-control-v1.schema.json").validate(
            missing_active_pointer
        )
        self.assertIn(
            "ACTIVE_SPRINT_INDEX_MISMATCH",
            managed_project_control_invariant_issues(
                missing_active_pointer,
                {SPRINT_ID},
            ),
        )

        unfenced = project_control_fixture()
        unfenced["active_sprint_id"] = None
        unfenced["activation_fencing_counter"] = 0
        unfenced_record = unfenced["start_idempotency_records"][0]
        unfenced_record.update(
            {
                "status": "PREPARING",
                "fencing_token": None,
                "response": None,
                "http_status": None,
                "error": None,
            }
        )
        with self.assertRaises(ValidationError):
            self.validator("managed-project-control-v1.schema.json").validate(
                unfenced
            )
        self.assertIn(
            "START_FENCING_TOKEN_INVALID",
            managed_project_control_invariant_issues(unfenced),
        )

        stale_pointer = project_control_fixture()
        newer = copy.deepcopy(stale_pointer["start_idempotency_records"][0])
        newer_identity = copy.deepcopy(newer["pinned_identity"])
        newer_identity.update({"commit": "c" * 40, "manifest_sha256": "d" * 64})
        newer_sprint_id = SprintProvenance(**newer_identity).stable_sprint_id()
        newer.update(
            {
                "idempotency_key": "start-key-2",
                "attempt_id": "attempt-2",
                "created_fencing_token": 2,
                "fencing_token": 2,
                "pinned_identity": newer_identity,
                "sprint_id": newer_sprint_id,
            }
        )
        newer["response"].update(
            {"sprint_id": newer_sprint_id, "identity": newer_identity}
        )
        stale_pointer["activation_fencing_counter"] = 2
        stale_pointer["start_idempotency_records"].append(newer)
        self.validator("managed-project-control-v1.schema.json").validate(
            stale_pointer
        )
        self.assertIn(
            "ACTIVE_SPRINT_INDEX_MISMATCH",
            managed_project_control_invariant_issues(
                stale_pointer, {SPRINT_ID, newer_sprint_id}
            ),
        )
        stale_pointer["active_sprint_id"] = newer_sprint_id
        self.assertNotIn(
            "ACTIVE_SPRINT_INDEX_MISMATCH",
            managed_project_control_invariant_issues(
                stale_pointer, {SPRINT_ID, newer_sprint_id}
            ),
        )

    def test_runtime_config_freezes_env_and_derived_fields(self) -> None:
        schema = self.schemas["managed-runtime-config-v1.schema.json"]
        config = runtime_config_fixture()
        self.validator("managed-runtime-config-v1.schema.json").validate(config)
        self.assertEqual(managed_runtime_config_invariant_issues(config), ())
        self.assertIsNotNone(windows_absolute_path_key(r"D:\nginx-qa"))
        self.assertEqual(windows_absolute_path_key("D:/"), "d:\\")
        self.assertTrue(windows_path_is_within(r"D:\safe", "D:/"))
        for aliased_path in (
            r"\\?\D:\nginx-qa",
            r"\\.\D:\nginx-qa",
            r"\\server\share\nginx-qa",
            r"D:\nginx-qa\CON",
            "D:\\safe\\COM¹.txt",
            "D:\\safe\\NUL .txt",
            r"C:\PROGRA~1\Common Files",
            "D:\\ nginx-qa",
            "D:\\nginx-qa ",
        ):
            self.assertIsNone(windows_absolute_path_key(aliased_path))

        aliased_root = runtime_config_fixture()
        aliased_root["managed_root"] = r"\\?\D:\nginx-qa"
        self.assertIn(
            "RUNTIME_CONFIG_PATH_INVALID",
            managed_runtime_config_invariant_issues(aliased_root),
        )
        short_name_root = runtime_config_fixture()
        short_name_root["service_root"] = r"C:\PROGRA~1\Common Files"
        short_name_root["protected_roots"] = [r"C:\Program Files"]
        self.assertIn(
            "RUNTIME_CONFIG_PATH_INVALID",
            managed_runtime_config_invariant_issues(short_name_root),
        )
        drive_root_protected = runtime_config_fixture()
        drive_root_protected["protected_roots"] = ["D:/"]
        self.assertIn(
            "RUNTIME_CONFIG_PROTECTED_ROOT_CONFLICT",
            managed_runtime_config_invariant_issues(drive_root_protected),
        )
        drive_root_service = runtime_config_fixture()
        drive_root_service["service_root"] = "D:/"
        self.assertIn(
            "RUNTIME_CONFIG_PATH_CONFLICT",
            managed_runtime_config_invariant_issues(drive_root_service),
        )
        env_names = {
            definition["x-env"]
            for definition in schema["properties"].values()
            if "x-env" in definition
        }
        self.assertEqual(
            env_names,
            {
                "NGINX_QA_HTTP_HOST",
                "NGINX_QA_HTTP_PORT",
                "NGINX_QA_SERVICE_ROOT",
                "NGINX_QA_PROTECTED_ROOTS",
                "NGINX_QA_RUNTIME_ROOT",
                "NGINX_QA_PROMPT_ROOT",
                "NGINX_QA_MANAGED_ROOT",
                "NGINX_QA_GIT_FETCH_TIMEOUT_SECONDS",
                "NGINX_QA_CHILD_PORT_RANGE",
                "NGINX_QA_INSTANCE_ID",
                "NGINX_QA_DISABLE_TELEGRAM",
                "NGINX_QA_DISABLE_TUNNEL",
            },
        )
        derived = {
            definition["x-derived-from"]
            for definition in schema["properties"].values()
            if "x-derived-from" in definition
        }
        self.assertEqual(
            derived,
            {
                "runtime_root/processes",
                "runtime_root/logs",
                "runtime_root/pids",
                "runtime_root/leases",
            },
        )

        redirected = runtime_config_fixture()
        redirected["process_runtime_root"] = "D:/nginx-qa/runtime/processes"
        self.assertIn(
            "RUNTIME_CONFIG_DERIVED_ROOT_MISMATCH",
            managed_runtime_config_invariant_issues(redirected),
        )
        self.assertIn(
            "RUNTIME_CONFIG_PROTECTED_ROOT_CONFLICT",
            managed_runtime_config_invariant_issues(redirected),
        )

        unsafe_ports = runtime_config_fixture()
        unsafe_ports.update({"child_port_start": 8025, "child_port_end": 8026})
        self.assertIn(
            "RUNTIME_CONFIG_PORT_RANGE_INVALID",
            managed_runtime_config_invariant_issues(unsafe_ports),
        )


if __name__ == "__main__":
    unittest.main()
