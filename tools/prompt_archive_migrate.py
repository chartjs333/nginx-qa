"""Analyze or migrate explicit isolated legacy-inline response copies.

This offline CLI does not authorize or hydrate authoritative descriptor stubs.
Selected response files with a recognized descriptor kind, an unknown kind, or
duplicate top-level kind members fail closed before durable planning.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nginx_qa.prompt_archive_migration import (  # noqa: E402
    OFFLINE_OPERATOR_GATE,
    PromptArchiveMigration,
    PromptArchiveMigrationError,
    migration_error_evidence,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "INPUT CONTRACT: explicit isolated legacy-inline copies only. "
            "Authoritative descriptor stubs and unknown/duplicate top-level "
            "kind members are rejected; a local path is not authorization."
        ),
    )
    parser.add_argument(
        "command",
        choices=(
            "analyze",
            "migrate",
            "resume-operation",
            "reconstruct",
            "rollback-verify",
        ),
    )
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--source-root-handle-id", required=True)
    parser.add_argument(
        "--operator-gate",
        help=f"Required exact offline authorization gate: {OFFLINE_OPERATOR_GATE}",
    )
    parser.add_argument(
        "--operator-principal-id",
        help="Non-secret durable identity of the authorizing offline operator.",
    )
    parser.add_argument(
        "--private-evidence-file",
        type=Path,
        help=(
            "Explicit isolated create-or-verify file for the private full "
            "pre/post path inventory; mandatory for migrate/rollback-verify."
        ),
    )
    parser.add_argument(
        "--protected-root",
        action="append",
        type=Path,
        default=[],
        help="Additional configured/live root to reject (repeatable).",
    )
    parser.add_argument(
        "--reconstruction-root",
        type=Path,
        help="Required only for reconstruct; must be isolated from every other root.",
    )
    parser.add_argument("--response-id")
    parser.add_argument("--operation-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = args.command
    if (
        args.operator_gate != OFFLINE_OPERATOR_GATE
        or not args.operator_principal_id
        or (
            command in {"migrate", "reconstruct", "rollback-verify"}
            and args.private_evidence_file is None
        )
        or (
            command in {"analyze", "resume-operation"}
            and args.private_evidence_file is not None
        )
    ):
        exc = PromptArchiveMigrationError(
            "PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID"
        )
        print(
            json.dumps(
                migration_error_evidence(exc, command=command),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 2
    if (command == "reconstruct") != (args.reconstruction_root is not None):
        _parser().error(
            "--reconstruction-root is required only when command is reconstruct"
        )
    operation_identifiers_supplied = (
        args.response_id is not None or args.operation_id is not None
    )
    if (
        command == "resume-operation"
        and not (args.response_id and args.operation_id)
    ) or (command != "resume-operation" and operation_identifiers_supplied):
        _parser().error(
            "--response-id and --operation-id are required only for resume-operation"
        )
    try:
        migration = PromptArchiveMigration(
            input_root=args.input_root,
            archive_root=args.archive_root,
            project_id=args.project_id,
            source_root_handle_id=args.source_root_handle_id,
            operator_gate=args.operator_gate,
            operator_principal_id=args.operator_principal_id,
            private_evidence_file=args.private_evidence_file,
            protected_roots=args.protected_root,
        )
        if command == "analyze":
            evidence = migration.analyze()
        elif command == "migrate":
            evidence = migration.migrate()
        elif command == "reconstruct":
            evidence = migration.reconstruct(args.reconstruction_root)
        elif command == "resume-operation":
            evidence = migration.resume_operation(
                response_id=args.response_id,
                operation_id=args.operation_id,
            )
        else:
            evidence = migration.verify_rollback()
    except PromptArchiveMigrationError as exc:
        print(
            json.dumps(
                migration_error_evidence(exc, command=command),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 2
    print(
        json.dumps(
            evidence,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
