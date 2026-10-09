"""Explicit offline, copy-out v1 -> v2 scope ledger migration.

Never rewrites the input or starts a server. The operator must freeze *all*
writers and verify the source hash first; output is created exclusively.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path

from nginx_qa.legacy_scope_control import (
    SCOPE_CONTROL_V2_CAPABILITY, canonical_json_sha256, validate_versioned_scope_history,
)
from nginx_qa.scope_control_prestart import _strict_object, validate_scope_control_compatibility


def migrate_document(document: dict, *, migrated_at: str, source_sha256: str, required_project: str = "") -> tuple[dict, dict]:
    """Convert the ledger envelope, preserving every original fact verbatim."""
    validate_scope_control_compatibility(document, required_project=required_project)
    result = deepcopy(document)
    changed = []
    for project_id, project in result["projects"].items():
        if required_project and required_project not in {str(project_id), str(project.get("project_phone") or "")}:
            continue
        state = project.get("agent_assignment") or {}
        control = state.get("scope_control")
        if not isinstance(control, dict) or control.get("schema_version") == 2:
            continue
        original_control = deepcopy(control)
        bindings = control.get("assignment_bindings") or {}
        acks = control.get("acknowledgements") or {}
        control.update(
            schema_version=2,
            minimum_runtime_capability=SCOPE_CONTROL_V2_CAPABILITY,
            binding_history={key: [deepcopy(value)] for key, value in bindings.items()},
            acknowledgement_history={value["ack_id"]: deepcopy(value) for value in acks.values()},
            migration={"kind": "offline-v1-to-v2", "migrated_at": migrated_at,
                "source_document_sha256": source_sha256,
                "original_scope_control_sha256": canonical_json_sha256(original_control)},
        )
        validate_versioned_scope_history(control)
        changed.append(str(project_id))
    validate_scope_control_compatibility(result)
    return result, {"schema_version": 1, "migrated_projects": changed,
        "source_sha256": source_sha256, "graph_advanced": False,
        "assignment_preserved": True, "ack_created": False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--project", required=True, help="Exact project key or logical phone; other projects remain unchanged")
    parser.add_argument("--writers-stopped", action="store_true", required=True)
    args = parser.parse_args(argv)
    from datetime import datetime, timezone
    if args.state_file.resolve() == args.output.resolve():
        parser.error("output must differ from input; in-place migration is forbidden")
    original = args.state_file.read_bytes()
    source_hash = hashlib.sha256(original).hexdigest()
    if source_hash != args.expected_sha256:
        parser.error("source SHA-256 mismatch; no output written")
    migrated, report = migrate_document(_strict_object(args.state_file),
        migrated_at=datetime.now(timezone.utc).isoformat(), source_sha256=source_hash,
        required_project=args.project)
    # Detect a writer racing validation even though the operator asserted a freeze.
    if args.state_file.read_bytes() != original:
        parser.error("source changed during migration; no output written")
    encoded = json.dumps(migrated, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    if args.output.read_bytes() != encoded:
        raise RuntimeError("migration output verification failed")
    report.update(output=str(args.output.resolve()), output_sha256=hashlib.sha256(encoded).hexdigest())
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
