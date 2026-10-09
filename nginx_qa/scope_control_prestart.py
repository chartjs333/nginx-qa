"""Offline compatibility gate for legacy scope-controlled runtime state.

Run this module against a stopped instance before starting nginx-qa.  It is
deliberately read-only and prints only structural counts and identifiers.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys
from typing import Any

from nginx_qa.legacy_scope_control import (
    EFFECTIVE_SCOPE_HASH_CONTRACT,
    LegacyScopeControlError,
    SCOPE_CONTROL_CAPABILITY,
    SCOPE_CONTROL_SCHEMA_VERSION,
    SCOPE_CONTROL_V2_CAPABILITY,
    SUPPORTED_SCOPE_CONTROL_VERSIONS,
    assignment_binding,
    canonical_json_bytes,
    canonical_json_sha256,
    effective_scope_core,
    validate_versioned_scope_history,
)


class CompatibilityError(ValueError):
    pass


def _strict_object(path: Path) -> dict[str, Any]:
    def reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CompatibilityError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> Any:
        raise CompatibilityError(f"non-finite JSON number: {value}")

    try:
        parsed = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicate,
            parse_constant=reject_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CompatibilityError("state file is not readable strict UTF-8 JSON") from exc
    if not isinstance(parsed, dict):
        raise CompatibilityError("state file root must be an object")
    return parsed


def validate_scope_control_compatibility(
    document: dict[str, Any],
    *,
    required_project: str = "",
    require_scope_control: str = "any",
) -> dict[str, Any]:
    projects = document.get("projects")
    if not isinstance(projects, dict):
        raise CompatibilityError("state has no projects object")
    matched_project = not required_project
    required_project_controlled = False
    controlled_projects = 0
    binding_count = 0
    acknowledgement_count = 0

    for project_key, project in projects.items():
        if not isinstance(project, dict):
            raise CompatibilityError("every project entry must be an object")
        project_phone = str(project.get("project_phone") or "")
        is_required_project = required_project in {str(project_key), project_phone}
        if is_required_project:
            matched_project = True
        state = project.get("agent_assignment")
        if not isinstance(state, dict):
            continue
        scope_control = state.get("scope_control")
        if scope_control is None:
            continue
        if not isinstance(scope_control, dict):
            raise CompatibilityError("scope_control must be an object")
        controlled_projects += 1
        if is_required_project:
            required_project_controlled = True
        try:
            schema_version = int(scope_control.get("schema_version") or 0)
        except (TypeError, ValueError) as exc:
            raise CompatibilityError("scope_control schema_version is invalid") from exc
        if schema_version not in SUPPORTED_SCOPE_CONTROL_VERSIONS:
            raise CompatibilityError("unsupported scope_control schema_version")
        expected_capability = SCOPE_CONTROL_V2_CAPABILITY if schema_version == 2 else SCOPE_CONTROL_CAPABILITY
        if scope_control.get("minimum_runtime_capability") != expected_capability:
            raise CompatibilityError("unsupported minimum_runtime_capability")
        amendments = scope_control.get("amendments")
        if (
            not isinstance(amendments, list)
            or not amendments
            or (schema_version == 1 and len(amendments) != 1)
            or not isinstance(amendments[0], dict)
        ):
            raise CompatibilityError("v1 requires exactly one applied amendment")
        active_id = str(scope_control.get("active_amendment_id") or "")
        if active_id != str(amendments[-1].get("amendment_id") or ""):
            raise CompatibilityError("active amendment does not match the v1 record")
        if schema_version == 2:
            try:
                validate_versioned_scope_history(scope_control)
            except (LegacyScopeControlError, TypeError, ValueError) as exc:
                raise CompatibilityError("versioned scope history is invalid") from exc
        bindings = scope_control.get("assignment_bindings")
        acknowledgements = scope_control.get("acknowledgements")
        if not isinstance(bindings, dict) or not isinstance(acknowledgements, dict):
            raise CompatibilityError("scope-control binding/ACK maps are invalid")
        binding_count += len(bindings)
        acknowledgement_count += len(acknowledgements)
        for assignment_id, binding in bindings.items():
            try:
                assignment_binding(state, assignment_id)
            except (LegacyScopeControlError, TypeError, ValueError) as exc:
                raise CompatibilityError("assignment binding integrity is invalid") from exc
            if not isinstance(binding, dict) or str(
                binding.get("assignment_id") or ""
            ) != str(assignment_id):
                raise CompatibilityError("assignment binding identity is invalid")
            effective_core = binding.get("effective_core")
            effective = binding.get("effective")
            if not isinstance(effective_core, dict) or not isinstance(effective, dict):
                raise CompatibilityError("assignment binding has no effective_core")
            if canonical_json_bytes(effective_core) != canonical_json_bytes(
                effective_scope_core(effective)
            ):
                raise CompatibilityError("assignment effective/core snapshots differ")
            if canonical_json_bytes(
                binding.get("effective_scope_hash_contract")
            ) != canonical_json_bytes(EFFECTIVE_SCOPE_HASH_CONTRACT):
                raise CompatibilityError("assignment scope hash contract is invalid")
            expected_hash = canonical_json_sha256(effective_core)
            if expected_hash != str(binding.get("effective_scope_sha256") or ""):
                raise CompatibilityError("assignment effective scope hash is invalid")
            context = binding.get("scope_context")
            if not isinstance(context, dict) or str(
                context.get("effective_scope_sha256") or ""
            ) != expected_hash:
                raise CompatibilityError("assignment scope_context is not hash-bound")
            if canonical_json_sha256(context) != str(
                binding.get("scope_context_sha256") or ""
            ):
                raise CompatibilityError("assignment scope_context hash is invalid")
            acknowledgement = acknowledgements.get(str(assignment_id))
            if acknowledgement is not None and schema_version == 1:
                if not isinstance(acknowledgement, dict) or canonical_json_bytes(
                    acknowledgement.get("scope_context")
                ) != canonical_json_bytes(context):
                    raise CompatibilityError("assignment ACK is not context-bound")

    if not matched_project:
        raise CompatibilityError("required project was not found")
    gated_controlled = (
        required_project_controlled if required_project else controlled_projects > 0
    )
    if require_scope_control == "present" and not gated_controlled:
        raise CompatibilityError("scope_control is required but absent")
    if require_scope_control == "absent" and gated_controlled:
        raise CompatibilityError("scope_control is present but must be absent")
    return {
        "compatible": True,
        "supported_capability": SCOPE_CONTROL_V2_CAPABILITY,
        "supported_schema_version": 2,
        "supported_schema_versions": list(SUPPORTED_SCOPE_CONTROL_VERSIONS),
        "project_count": len(projects),
        "controlled_project_count": controlled_projects,
        "assignment_binding_count": binding_count,
        "acknowledgement_count": acknowledgement_count,
        "required_project": required_project or None,
        "require_scope_control": require_scope_control,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only nginx-qa scope-control state compatibility gate"
    )
    parser.add_argument("--state-file", required=True, type=Path)
    parser.add_argument("--project", default="")
    parser.add_argument(
        "--require-scope-control",
        choices=("any", "present", "absent"),
        default="any",
    )
    args = parser.parse_args(argv)
    try:
        document = _strict_object(args.state_file)
        result = validate_scope_control_compatibility(
            document,
            required_project=args.project,
            require_scope_control=args.require_scope_control,
        )
    except CompatibilityError as exc:
        print(json.dumps({"compatible": False, "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(deepcopy(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
