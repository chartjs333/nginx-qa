"""In-memory upgrade check against COPY inputs; no HTTP or input-file writes."""
from __future__ import annotations
import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from nginx_qa.legacy_scope_control import (
    acknowledgement_for_binding, acknowledgement_id, apply_semantic_scope_revision,
    assignment_binding, canonical_json_sha256, require_exact_scope_context,
    LegacyScopeControlError, store_acknowledgement, validate_versioned_scope_history,
)
from nginx_qa.scope_control_migrate import migrate_document
from nginx_qa.scope_control_prestart import _strict_object


def verify_copy(state_path: Path, agents_path: Path, project_id: str) -> dict:
    raw, agent_raw = state_path.read_bytes(), agents_path.read_bytes()
    input_hash = hashlib.sha256(raw).hexdigest()
    document, agent_document = _strict_object(state_path), json.loads(agent_raw)
    agents = agent_document.get("agents", []) if isinstance(agent_document, dict) else agent_document
    project_key = next(key for key, project in document["projects"].items()
        if str(key) == project_id or str(project.get("project_phone")) == project_id)
    original = deepcopy(document["projects"][project_key]["agent_assignment"])
    original_control = original["scope_control"]
    assignment_id = original["current_assignment_id"]
    original_binding = assignment_binding(original, assignment_id)
    if acknowledgement_for_binding(original_control, original_binding) is None:
        raise ValueError("Compatibility fixture requires an exact historical ACK")
    migrated, migration = migrate_document(document,
        migrated_at="isolated-compatibility-simulation", source_sha256=input_hash,
        required_project=project_id)
    state = migrated["projects"][project_key]["agent_assignment"]
    original_execution = {key: value for key, value in state.items() if key != "scope_control"}
    revision = int(state["scope_control"]["effective_revision"])
    active = state["scope_control"]["amendments"][-1]
    checks = {}
    for target_revision in (revision + 1, revision + 2):
        previous_binding = assignment_binding(state, assignment_id)
        state, _ = apply_semantic_scope_revision(state, agents,
            amendment_id=f"ISOLATED-COMPATIBILITY-{target_revision}",
            proposal={"instructions": f"ISOLATED COMPATIBILITY SIMULATION {target_revision}. Not live authorization.",
                "retained_restrictions": ["No live mutations", "No deployment", "Existing review gates retained"],
                "node_ids": list(active["node_overrides"]),
                "reviewer_ids": list(state["workflow"]["reviewer_agent_ids"])},
            source=active["source"], authorization={"kind": "isolated_compatibility_simulation"},
            expected_scope_revision=target_revision - 1, applied_at=f"isolated-revision-{target_revision}")
        binding = assignment_binding(state, assignment_id)
        try:
            require_exact_scope_context(previous_binding["scope_context"], binding)
        except LegacyScopeControlError as exc:
            checks[f"revision_{target_revision}_stale_context_rejected"] = exc.code == "SCOPE_CONTEXT_MISMATCH"
        else:
            raise AssertionError("Old context was incorrectly accepted")
        assert acknowledgement_for_binding(state["scope_control"], binding) is None
        pre_ack_execution = deepcopy({key: value for key, value in state.items() if key != "scope_control"})
        store_acknowledgement(state["scope_control"], {"ack_id": acknowledgement_id(binding),
            **{key: binding[key] for key in ("assignment_id", "agent_id", "agent_phone", "amendment_id", "effective_revision")},
            "scope_context": deepcopy(binding["scope_context"]), "acknowledged_at": "isolated-simulation"})
        assert pre_ack_execution == {key: value for key, value in state.items() if key != "scope_control"}
        validate_versioned_scope_history(state["scope_control"])
        checks[f"revision_{target_revision}_ack_no_execution_side_effects"] = True
    control = state["scope_control"]
    assert control["amendments"][:len(original_control["amendments"])] == original_control["amendments"]
    for aid, binding in original_control["assignment_bindings"].items():
        assert control["binding_history"][aid][0] == binding
    for ack in original_control["acknowledgements"].values():
        assert control["acknowledgement_history"][ack["ack_id"]] == ack
    assert {key: value for key, value in state.items() if key != "scope_control"} == original_execution
    assert raw == state_path.read_bytes() and agent_raw == agents_path.read_bytes()
    checks.update(original_amendments_unchanged=True, original_bindings_unchanged=True,
        original_acks_unchanged=True, assignments_reviews_topology_pointer_unchanged=True,
        input_files_unchanged=True)
    assignments = original.get("assignments") or []
    return {"schema_version": 1, "kind": "isolated_copy_compatibility_evidence",
        "state_source_sha256": input_hash, "agents_source_sha256": hashlib.sha256(agent_raw).hexdigest(),
        "project_id": project_id, "source_execution_revision": original.get("revision"),
        "source_assignment_id": assignment_id, "source_scope_revision": revision,
        "simulated_scope_revisions": [revision+1, revision+2],
        "assignment_count": len(assignments),
        "review_assignment_count": sum(item.get("kind") in {"review", "transition_review"} for item in assignments),
        "preserved_assignments_sha256": canonical_json_sha256(assignments),
        "preserved_original_scope_sha256": canonical_json_sha256(original_control),
        "migration_project_count": len(migration["migrated_projects"]),
        "checks": checks, "passed": all(checks.values()), "live_mutated": False, "http_requests": 0,
        "note": "Observation is compatibility input, not a coherent backup or current-state guarantee; synthetic revisions were applied only in memory."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-copy", type=Path, required=True)
    parser.add_argument("--agents-copy", type=Path, required=True)
    parser.add_argument("--project", required=True)
    args = parser.parse_args()
    print(json.dumps(verify_copy(args.state_copy, args.agents_copy, args.project), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
