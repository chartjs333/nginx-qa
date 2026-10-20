import asyncio
import contextlib
import copy
import hashlib
import io
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

import main
from nginx_qa.prompt_archive_incremental import ArchiveOccurrence, response_id_v1
from nginx_qa.prompt_archive_latest import (
    ExternalLatestCoordinator,
    FAULT_AFTER_CURRENT_INTENT,
    FAULT_AFTER_OPERATION_PLAN,
)
from nginx_qa.prompt_archive_runtime import (
    PromptArchivePublication,
    PromptArchiveRuntimeConfig,
    PromptArchiveRuntimeRegistry,
)
from nginx_qa.prompt_archive_source_metadata import (
    freeze_source_metadata_screen,
    project_source_metadata,
)
from nginx_qa.prompt_archive_storage import PromptArchiveStorageError
from tests.test_managed_continuity import (
    ManagedContinuityTests,
    WORKSPACE_1_ROOT,
    asgi_request,
)
from nginx_qa.execution_observability import append_execution_checkpoint
from tests.test_prompt_archive_contract import (
    PromptArchiveContractTests,
    descriptor_fixture,
)


class PromptArchiveRuntimeIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.prompt_root = self.root / "prompt"
        self.archive_root = self.root / "archive"
        self.prompt_root.mkdir()
        self.original_runtime = main.prompt_archive_runtime
        self.original_receipt_path = main.prompt_archive_occurrence_receipt_path
        self.project_prompt_resolver = patch.object(
            main,
            "_prompt_archive_project_prompt_directory",
            side_effect=self.resolve_project_prompt_directory,
        )
        self.project_prompt_resolver.start()

    def tearDown(self) -> None:
        main.prompt_archive_runtime = self.original_runtime
        main.prompt_archive_occurrence_receipt_path = self.original_receipt_path
        self.project_prompt_resolver.stop()
        self.temporary.cleanup()

    def resolve_project_prompt_directory(
        self,
        project_id: str,
        *,
        create: bool = False,
    ) -> Path:
        selected = (
            self.prompt_root
            / "archive-fixture"
            / ".nginx-qa-projects"
            / project_id
        )
        if create:
            selected.mkdir(parents=True, exist_ok=True)
        return selected

    def environment(self, mode: str) -> dict[str, str]:
        environment = {
            "NGINX_QA_PROMPT_ARCHIVE_MODE": mode,
            "NGINX_QA_PROMPT_ARCHIVE_ROOT": str(self.archive_root),
            "NGINX_QA_PROMPT_ROOT": str(self.prompt_root),
            "NGINX_QA_PROTECTED_ROOTS": "[]",
        }
        if mode == "authoritative":
            environment["NGINX_QA_PROMPT_ARCHIVE_OPERATOR_GATE"] = (
                "prompt-archive-authoritative-v1"
            )
        return environment

    def source_reader(
        self,
        *,
        project_id: str,
        response_key: str,
        response_kind: str,
        maximum_bytes: int,
    ) -> bytes | None:
        if project_id != "9000":
            return None
        path = (
            self.prompt_root
            / "archive-fixture"
            / f"{response_kind}-{response_key}.json"
        )
        if not path.is_file():
            return None
        raw = path.read_bytes()
        return raw[: maximum_bytes + 1]

    def configure(self, mode: str):
        registry = main.configure_prompt_archive_runtime(
            self.environment(mode),
            occurrence_source_reader=self.source_reader,
            occurrence_receipt_root=self.root / "occurrence-receipts",
        )
        self.assertIs(registry.metrics, main.prompt_archive_runtime_metrics)
        return registry

    def response(self) -> dict:
        return {
            "answer": "Transition accepted",
            "project_id": "9000",
            "sprint_id": "sprint-runtime-one",
            "project": {
                "project_name": "Archive Fixture",
                "git_address": "https://github.com/acme/archive-fixture.git",
                "git_context_key": "github.com/acme/archive-fixture",
            },
            "agent": {"id": "agent-one", "name": "Agent", "phone": "4102"},
            "active_task": {
                "id": "task-one",
                "message": "Complete task body\nwithout archive hydration.",
                "metadata": {
                    "assignment_id": "assignment-one",
                    "sprint_id": "sprint-runtime-one",
                    "allowed_outcomes": ["DONE", "STOP"],
                    "result_endpoint": "/result",
                    "source_commit": "1" * 40,
                },
            },
            "transition_applied": {
                "transition_id": "transition-one",
                "source_assignment_id": "assignment-one",
                "outcome": "DONE",
                "target_node_id": "next",
                "status": "applied",
                "proposed_at": "2026-10-11T18:00:00+02:00",
                "resolved_at": "2026-10-11T18:01:00+02:00",
            },
            "project_state": {
                "recent_activity": [{"event": "one"}, {"event": "two"}],
                "history_with_patches": {
                    "text": "history\n",
                    "timeline": [{"event": "one"}, {"event": "two"}],
                    "patches": [{"sha": "1" * 40}],
                },
                "activity_with_patches": [
                    {"event": "one"},
                    {"event": "two"},
                ],
                "code_patches": [{"sha": "1" * 40}],
            },
        }

    def occurrence(self, response: dict) -> ArchiveOccurrence | None:
        transition = response.get("transition_applied")
        sprint_id = response.get("sprint_id")
        if not isinstance(transition, dict) or not isinstance(sprint_id, str):
            return None
        occurrence_id = transition["transition_id"]
        return ArchiveOccurrence(
            project_id=response["project_id"],
            sprint_id=sprint_id,
            response_id=response_id_v1(
                response["project_id"],
                sprint_id,
                "transition-response",
                "transition",
                occurrence_id,
            ),
            response_kind="transition-response",
            occurrence_kind="transition",
            occurrence_id=occurrence_id,
            transition_id=occurrence_id,
            source={
                "source_kind": "runtime_shadow",
                "assignment_id": "assignment-one",
                "source_commit": "1" * 40,
            },
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "legacy_local_principal",
                "principal_id": response["project_id"],
                "binding_id": None,
                "scope_source": "legacy_project_context",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": occurrence_id,
            },
            created_at=transition["resolved_at"],
            verified_at=transition["resolved_at"],
        )

    def materialize(
        self,
        response: dict | None = None,
        *,
        archive_occurrence: ArchiveOccurrence | None = None,
        request_agent_phone: str = "4102",
        archive_source_metadata: dict | None = None,
    ) -> dict:
        selected = response or self.response()
        return main.materialize_sequential_graph_response(
            response=selected,
            directory_template=str(self.prompt_root / "{repository}"),
            agent_latest_file_template=str(
                self.prompt_root / "agents" / "{repository}_{agent_phone}-latest.prompt"
            ),
            response_kind="transition-response",
            request_agent_phone=request_agent_phone,
            archive_occurrence=(
                self.occurrence(selected)
                if archive_occurrence is None
                else archive_occurrence
            ),
            archive_source_metadata=archive_source_metadata,
        )

    def test_shadow_uses_exact_committed_bytes_and_publishes_after_legacy(self) -> None:
        self.configure("shadow")

        compact = self.materialize()

        descriptor = compact["prompt_archive"]
        self.assertTrue(compact["response_archive_available"])
        self.assertIn("state_url", descriptor)
        response_path = Path(compact["response_file_path"])
        committed = response_path.read_bytes()
        self.assertNotIn("prompt_archive", json.loads(committed))
        runtime = main.prompt_archive_runtime.for_project("9000")
        selected = runtime.lookup_archive(
            sprint_id="sprint-runtime-one",
            archive_id=descriptor["archive_id"],
        )
        self.assertEqual(runtime.hydrate_content(selected), committed)
        self.assertEqual(
            json.loads(Path(compact["latest_agent_prompt_file_path"]).read_bytes()),
            compact,
        )
        self.assertEqual(
            compact["active_task"]["message"],
            "Complete task body\nwithout archive hydration.",
        )
        self.assertEqual(
            compact["active_task"]["metadata"]["allowed_outcomes"],
            ["DONE", "STOP"],
        )

    def test_authoritative_actor_reviewer_consumer_uses_authorized_dispatch(self) -> None:
        response = self.response()
        response["completed_assignment"] = {
            "assignment_id": "assignment-one",
            "kind": "graph_node",
            "node_id": "implementation",
            "agent_id": "agent-one",
            "agent_phone": "4102",
            "status": "completed",
        }
        response["agent"] = {
            "id": "reviewer-one",
            "name": "Reviewer One",
            "phone": "4103",
        }
        response["active_task"] = {
            "id": "review-one",
            "message": "Review the completed actor result through PAS1.",
            "metadata": {
                "assignment_id": "review-assignment-one",
                "source_assignment_id": "assignment-one",
                "sprint_id": "sprint-runtime-one",
                "allowed_outcomes": ["APPROVE", "REJECT"],
                "result_endpoint": "/review-result",
                "source_commit": "1" * 40,
            },
        }

        class ForbiddenCumulativeState(dict):
            def __deepcopy__(self, _memo):
                raise AssertionError("cumulative project_state copied")

        response["project_state"] = ForbiddenCumulativeState(
            {"history_with_patches": {"text": "must not serialize"}}
        )
        occurrence = replace(
            self.occurrence(response),
            source={
                "source_kind": "native_runtime",
                "assignment_id": "assignment-one",
                "source_commit": "1" * 40,
            },
        )
        with (
            patch.object(
                self,
                "source_reader",
                side_effect=AssertionError("native receipt read legacy source"),
            ) as source_reader,
            patch(
                "nginx_qa.prompt_archive_incremental."
                "PromptArchiveIncremental.hydrate_archive_bytes",
                side_effect=AssertionError("authoritative hot path hydrated"),
            ),
        ):
            self.configure("authoritative")
            compact = self.materialize(
                response,
                archive_occurrence=occurrence,
            )

        source_reader.assert_not_called()
        self.assertNotIn("project_state", compact)
        self.assertTrue(compact["response_archive_available"])
        self.assertIn("state_url", compact["prompt_archive"])
        self.assertEqual(compact["active_task"], response["active_task"])
        response_path = Path(compact["response_file_path"])
        latest_path = Path(compact["latest_response_file_path"])
        self.assertEqual(latest_path.parent.name, "9000")
        self.assertEqual(latest_path.parent.parent.name, ".nginx-qa-projects")
        self.assertEqual(response_path.read_bytes(), latest_path.read_bytes())
        stub = json.loads(response_path.read_bytes())
        self.assertEqual(stub["kind"], "prompt_archive_descriptor_stub")
        self.assertNotIn("project_state", stub)
        self.assertTrue(
            (latest_path.parent / "latest-response.json.nginx-qa-v1").is_dir()
        )

        runtime = main.prompt_archive_runtime.for_project("9000")
        authorization_calls: list[tuple[str, str, str]] = []

        def authorize_archive(
            project_id: str,
            sprint_id: str,
            archive_id: str,
        ) -> bool:
            authorization_calls.append((project_id, sprint_id, archive_id))
            return (
                project_id,
                sprint_id,
                archive_id,
            ) == (
                "9000",
                "sprint-runtime-one",
                compact["prompt_archive"]["archive_id"],
            )

        hydrated = runtime.read_inline_or_archive(
            response_path.read_bytes(),
            authorize_archive=authorize_archive,
        )
        self.assertEqual(
            authorization_calls,
            [
                (
                    "9000",
                    "sprint-runtime-one",
                    compact["prompt_archive"]["archive_id"],
                )
            ],
        )
        archived = json.loads(hydrated)
        self.assertEqual(
            archived["kind"],
            "prompt_archive_occurrence_document",
        )
        self.assertEqual(archived["occurrence"]["response_id"], occurrence.response_id)
        expected_execution = dict(compact)
        expected_execution.pop("prompt_archive")
        expected_execution.pop("response_archive_available", None)
        expected_execution.pop("completed_assignments", None)
        for evidence_key in (
            "result",
            "feedback",
            "review",
            "reviews",
            "review_context",
            "transition",
            "pending_transition",
            "last_transition",
            "transition_applied",
            "completed_assignment",
            "source_commit",
            "from_commit",
            "git_commit",
            "git_branch",
        ):
            expected_execution.pop(evidence_key, None)
        expected_execution["response_kind"] = occurrence.response_kind
        self.assertEqual(archived["execution_response"], expected_execution)
        self.assertEqual(
            archived["evidence_delta"]["transition_applied"],
            response["transition_applied"],
        )
        self.assertEqual(
            archived["execution_response"]["active_task"]["id"],
            "review-one",
        )
        self.assertEqual(
            archived["evidence_delta"]["completed_assignment"]["agent_id"],
            "agent-one",
        )
        self.assertNotIn("project_state", archived)
        self.assertNotIn("work_history", archived)
        prompt_path = Path(compact["latest_agent_prompt_file_path"])
        self.assertEqual(json.loads(prompt_path.read_bytes()), compact)

        PromptArchiveContractTests.setUpClass()
        contract = PromptArchiveContractTests(
            "test_durable_publication_plan_is_replayable_and_path_free"
        )
        archive_plan_path = next(self.archive_root.rglob("plan.json"))
        archive_journal_path = next(self.archive_root.rglob("journal.json"))
        contract.validator(
            "prompt-archive-publication-plan-v1.schema.json"
        ).validate(json.loads(archive_plan_path.read_text(encoding="utf-8")))
        contract.validator("prompt-archive-journal-v1.schema.json").validate(
            json.loads(archive_journal_path.read_text(encoding="utf-8"))
        )

        # Exact replay is idempotent at the immutable transition stub and the
        # shared external latest authority.
        replayed = self.materialize(response, archive_occurrence=occurrence)
        self.assertEqual(replayed, compact)

        # A subsequent read-only identity heartbeat has no new durable
        # occurrence.  It must not advertise invented response files, but it
        # retains the prior descriptor after reopening and verifying the
        # immutable archive for the same active assignment.
        heartbeat = main.materialize_sequential_graph_response(
            response=response,
            directory_template=str(self.prompt_root / "{repository}"),
            agent_latest_file_template=str(
                self.prompt_root
                / "agents"
                / "{repository}_{agent_phone}-latest.prompt"
            ),
            response_kind="transition-response",
            request_agent_phone="4102",
            archive_occurrence=None,
        )
        self.assertEqual(heartbeat["prompt_archive"], compact["prompt_archive"])
        self.assertTrue(heartbeat["response_archive_available"])
        self.assertNotIn("response_file_path", heartbeat)
        self.assertNotIn("latest_response_file_path", heartbeat)

    def test_authoritative_source_metadata_is_frozen_before_plan_and_reused(self) -> None:
        response = self.response()
        occurrence = replace(
            self.occurrence(response),
            source={
                "source_kind": "native_runtime",
                "assignment_id": "assignment-one",
                "source_commit": "1" * 40,
            },
        )
        raw_identifier = "conversation-source-canary"
        configured_secret = "§Ω"
        raw_metadata = {
            "source_type": "email",
            "conversation_id": raw_identifier,
            "sender_label": "Alice",
            "title": configured_secret,
            "observed_at": "2026-10-11T18:00:00+02:00",
            "adapter_command": "must-be-discarded",
        }
        screen = freeze_source_metadata_screen(
            configured_values=(configured_secret,),
        )

        self.configure("authoritative")
        with patch.object(
            main,
            "prompt_archive_source_metadata_screen_snapshot",
            return_value=screen,
        ) as snapshot:
            compact = self.materialize(
                response,
                archive_occurrence=occurrence,
                archive_source_metadata=raw_metadata,
            )
        snapshot.assert_called_once_with(project_id="9000")

        runtime = main.prompt_archive_runtime.for_project("9000")
        selected = runtime.lookup_archive(
            sprint_id="sprint-runtime-one",
            archive_id=compact["prompt_archive"]["archive_id"],
        )
        self.assertIn("source_metadata", selected.manifest["roots"])
        receipt = selected.manifest["source_metadata_receipt"]
        self.assertEqual(receipt["source_type"], "email")
        self.assertEqual(receipt["discarded_field_count"], 1)
        self.assertEqual(
            receipt["projected_fields"],
            ["conversation_id", "observed_at", "sender_label", "title"],
        )

        hydrated = json.loads(runtime.hydrate_content(selected))
        self.assertNotIn("source_metadata", hydrated)
        self.assertEqual(hydrated["kind"], "prompt_archive_occurrence_document")

        persisted = b"\n".join(
            path.read_bytes()
            for root in (self.archive_root, self.root / "occurrence-receipts")
            if root.exists()
            for path in root.rglob("*")
            if path.is_file()
        )
        self.assertNotIn(configured_secret.encode("utf-8"), persisted)
        self.assertNotIn(raw_identifier.encode("utf-8"), persisted)
        self.assertNotIn(b"must-be-discarded", persisted)
        self.assertIn(b"[REDACTED]", persisted)

        # A retry after credential rotation and with different raw input must
        # recover the immutable projection without invoking the new snapshot.
        self.configure("authoritative")
        with patch.object(
            main,
            "prompt_archive_source_metadata_screen_snapshot",
            side_effect=AssertionError("credential snapshot re-read on retry"),
        ) as rotated_snapshot:
            replayed = self.materialize(
                response,
                archive_occurrence=occurrence,
                archive_source_metadata={
                    "source_type": "folder",
                    "title": "different-raw-value",
                },
            )
        rotated_snapshot.assert_not_called()
        self.assertEqual(replayed, compact)

    def test_existing_plan_recovers_missing_projection_proof_without_rescreen(
        self,
    ) -> None:
        response = self.response()
        occurrence = replace(
            self.occurrence(response),
            source={"source_kind": "native_runtime"},
        )
        assert occurrence is not None
        metadata = {"source_type": "api", "title": "frozen-owner-title"}
        self.configure("authoritative")
        with patch.object(
            main,
            "prompt_archive_source_metadata_screen_snapshot",
            return_value=freeze_source_metadata_screen(),
        ):
            first = self.materialize(
                response,
                archive_occurrence=occurrence,
                archive_source_metadata=metadata,
            )
        proof_paths = []
        receipt_root = self.root / "occurrence-receipts"
        for path in receipt_root.rglob("*.json"):
            try:
                document = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if document.get("kind") == "prompt_archive_source_metadata_projection_proof":
                proof_paths.append(path)
        self.assertEqual(len(proof_paths), 1)
        proof_paths[0].unlink()

        self.configure("authoritative")
        with patch.object(
            main,
            "prompt_archive_source_metadata_screen_snapshot",
            side_effect=AssertionError("immutable plan projection was re-screened"),
        ) as snapshot:
            replayed = self.materialize(
                response,
                archive_occurrence=occurrence,
                archive_source_metadata={
                    "source_type": "folder",
                    "title": "rotated-owner-title",
                },
            )

        snapshot.assert_not_called()
        self.assertEqual(replayed, first)
        self.assertTrue(proof_paths[0].is_file())

    def test_shadow_source_metadata_is_bound_without_changing_legacy_bytes(self) -> None:
        self.configure("shadow")
        raw_metadata = {
            "source_type": "api",
            "conversation_id": "shadow-conversation-canary",
            "title": "shadow-safe-title",
            "observed_at": "2026-10-11T18:00:00+02:00",
        }
        with patch.object(
            main,
            "prompt_archive_source_metadata_screen_snapshot",
            return_value=freeze_source_metadata_screen(),
        ) as snapshot:
            compact = self.materialize(archive_source_metadata=raw_metadata)

        snapshot.assert_called_once_with(project_id="9000")
        committed = Path(compact["response_file_path"]).read_bytes()
        runtime = main.prompt_archive_runtime.for_project("9000")
        selected = runtime.lookup_archive(
            sprint_id="sprint-runtime-one",
            archive_id=compact["prompt_archive"]["archive_id"],
        )
        self.assertIn("source_metadata", selected.manifest["roots"])
        self.assertEqual(
            selected.manifest["source_metadata_receipt"]["source_type"],
            "api",
        )
        self.assertEqual(runtime.hydrate_content(selected), committed)
        self.assertNotIn("source_metadata", json.loads(committed))

    def test_production_attach_derives_non_rendered_source_metadata(self) -> None:
        self.configure("authoritative")
        response = self.response()
        occurrence = replace(
            self.occurrence(response),
            source={"source_kind": "native_runtime"},
        )
        assert occurrence is not None
        with (
            patch.object(
                main,
                "read_sequential_prompt_settings_file",
                return_value={
                    "directory_template": str(self.prompt_root / "{repository}"),
                    "agent_latest_file_template": str(
                        self.prompt_root
                        / "agents"
                        / "{repository}_{agent_phone}-latest.prompt"
                    ),
                },
            ),
            patch.object(
                main,
                "prompt_archive_source_metadata_screen_snapshot",
                return_value=freeze_source_metadata_screen(),
            ) as snapshot,
        ):
            compact = asyncio.run(
                main.attach_sequential_graph_response_storage(
                    response,
                    response_kind="transition-response",
                    request_agent_phone="4102",
                    archive_occurrence=occurrence,
                )
            )

        snapshot.assert_called_once_with(project_id="9000")
        runtime = main.prompt_archive_runtime.for_project("9000")
        selected = runtime.lookup_archive(
            sprint_id="sprint-runtime-one",
            archive_id=compact["prompt_archive"]["archive_id"],
        )
        receipt = selected.manifest["source_metadata_receipt"]
        self.assertIn("source_metadata", selected.manifest["roots"])
        self.assertEqual(receipt["source_type"], "api")
        self.assertEqual(
            receipt["projected_fields"],
            ["conversation_id", "message_id", "observed_at", "title"],
        )
        hydrated = json.loads(runtime.hydrate_content(selected))
        self.assertNotIn("source_metadata", hydrated)

    def test_shadow_source_screen_failure_preserves_legacy_response(self) -> None:
        self.configure("shadow")
        with patch.object(
            main,
            "prompt_archive_source_metadata_screen_snapshot",
            side_effect=PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE"),
        ) as snapshot:
            compact = self.materialize(
                archive_source_metadata={
                    "source_type": "api",
                    "title": "safe-title",
                }
            )

        snapshot.assert_called_once_with(project_id="9000")
        self.assertNotIn("prompt_archive", compact)
        self.assertFalse(compact.get("response_archive_available", False))
        self.assertTrue(Path(compact["response_file_path"]).is_file())
        self.assertEqual(
            json.loads(Path(compact["response_file_path"]).read_bytes())["answer"],
            "Transition accepted",
        )

    def test_oversized_metadata_never_blocks_legacy_or_shadow_commit(self) -> None:
        for mode in ("legacy", "shadow"):
            with self.subTest(mode=mode):
                self.configure(mode)
                response = self.response()
                response["transition_applied"] = {
                    **response["transition_applied"],
                    "transition_id": f"transition-oversized-{mode}",
                }
                occurrence = self.occurrence(response)
                assert occurrence is not None
                with patch.object(
                    main,
                    "read_sequential_prompt_settings_file",
                    return_value={
                        "directory_template": str(
                            self.prompt_root / "{repository}"
                        ),
                        "agent_latest_file_template": str(
                            self.prompt_root
                            / "agents"
                            / "{repository}_{agent_phone}-latest.prompt"
                        ),
                    },
                ):
                    compact = asyncio.run(
                        main.attach_sequential_graph_response_storage(
                            response,
                            response_kind="transition-response",
                            request_agent_phone="4102",
                            archive_occurrence=occurrence,
                            archive_source_metadata={"title": "x" * (20 * 1024)},
                        )
                    )

                self.assertNotIn("response_storage_error", compact)
                self.assertTrue(Path(compact["response_file_path"]).is_file())
                self.assertEqual(
                    json.loads(Path(compact["response_file_path"]).read_bytes())[
                        "answer"
                    ],
                    "Transition accepted",
                )
                self.assertNotIn("prompt_archive", compact)

    def _assert_exact_retry_adopts_plan_before_oversized_raw_metadata(
        self,
        mode: str,
    ) -> None:
        settings = {
            "directory_template": str(self.prompt_root / "{repository}"),
            "agent_latest_file_template": str(
                self.prompt_root
                / "agents"
                / "{repository}_{agent_phone}-latest.prompt"
            ),
        }
        self.configure(mode)
        response = self.response()
        response["transition_applied"] = {
            **response["transition_applied"],
            "transition_id": f"transition-retry-{mode}",
        }
        occurrence = self.occurrence(response)
        assert occurrence is not None
        if mode == "authoritative":
            occurrence = replace(
                occurrence,
                source={"source_kind": "native_runtime"},
            )
        with (
            patch.object(
                main,
                "read_sequential_prompt_settings_file",
                return_value=settings,
            ),
            patch.object(
                main,
                "prompt_archive_source_metadata_screen_snapshot",
                return_value=freeze_source_metadata_screen(),
            ),
        ):
            first = asyncio.run(
                main.attach_sequential_graph_response_storage(
                    response,
                    response_kind="transition-response",
                    request_agent_phone="4102",
                    archive_occurrence=occurrence,
                    archive_source_metadata={
                        "source_type": "api",
                        "title": "first-owner-title",
                    },
                )
            )

        with (
            patch.object(
                main,
                "read_sequential_prompt_settings_file",
                return_value=settings,
            ),
            patch.object(
                main,
                "prompt_archive_source_metadata_screen_snapshot",
                side_effect=AssertionError(
                    "exact retry re-screened oversized raw metadata"
                ),
            ) as snapshot,
        ):
            replayed = asyncio.run(
                main.attach_sequential_graph_response_storage(
                    response,
                    response_kind="transition-response",
                    request_agent_phone="4102",
                    archive_occurrence=occurrence,
                    archive_source_metadata={
                        "title": "x" * (20 * 1024),
                    },
                )
            )

        snapshot.assert_not_called()
        self.assertEqual(
            replayed["prompt_archive"]["archive_id"],
            first["prompt_archive"]["archive_id"],
        )
        self.assertTrue(replayed["response_archive_available"])

    def test_authoritative_exact_retry_adopts_plan_before_oversized_raw_metadata(
        self,
    ) -> None:
        self._assert_exact_retry_adopts_plan_before_oversized_raw_metadata(
            "authoritative"
        )

    def test_shadow_exact_retry_adopts_plan_before_oversized_raw_metadata(
        self,
    ) -> None:
        self._assert_exact_retry_adopts_plan_before_oversized_raw_metadata("shadow")

    def test_invalid_occurrence_is_rejected_before_projection_proof_write(self) -> None:
        self.configure("authoritative")
        occurrence = replace(
            self.occurrence(self.response()),
            source={"source_kind": "native_runtime"},
        )
        assert occurrence is not None
        runtime = main.prompt_archive_runtime.for_project("9000")
        invalid_occurrences = (
            replace(occurrence, created_at="not-a-timestamp"),
            replace(occurrence, response_kind="unbounded-response-kind"),
        )

        for invalid in invalid_occurrences:
            with self.subTest(invalid=invalid.response_kind), patch.object(
                main,
                "prompt_archive_source_metadata_screen_snapshot",
            ) as snapshot, self.assertRaises(PromptArchiveStorageError) as raised:
                runtime.bind_source_metadata_projection(
                    invalid,
                    {"source_type": "api", "title": "safe-title"},
                    screen_factory=main.prompt_archive_source_metadata_screen_snapshot,
                )
            self.assertEqual(
                raised.exception.code,
                "PROMPT_ARCHIVE_REFERENCE_INVALID",
            )
            snapshot.assert_not_called()

        persisted_files = [
            path
            for root in (self.archive_root, self.root / "occurrence-receipts")
            if root.exists()
            for path in root.rglob("*")
            if path.is_file()
        ]
        self.assertEqual(persisted_files, [])

    def test_supplied_source_projection_cannot_bypass_first_write_screen(self) -> None:
        self.configure("authoritative")
        response = self.response()
        occurrence = replace(
            self.occurrence(response),
            source={"source_kind": "native_runtime"},
        )
        assert occurrence is not None
        secret = "opaque-configured-value"
        unsafe = project_source_metadata(
            {"source_type": "api", "title": secret},
            screen=freeze_source_metadata_screen(),
        )
        supplied = replace(
            occurrence,
            source_metadata_projection=unsafe.document,
            source_metadata_receipt=unsafe.receipt,
        )
        runtime = main.prompt_archive_runtime.for_project("9000")

        with self.assertRaises(PromptArchiveStorageError) as raised:
            runtime.bind_source_metadata_projection(supplied)

        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_REQUEST_INVALID")
        self.assertFalse(any(self.archive_root.rglob("plan.json")))
        persisted = b"\n".join(
            path.read_bytes()
            for root in (self.archive_root, self.root / "occurrence-receipts")
            if root.exists()
            for path in root.rglob("*")
            if path.is_file()
        )
        self.assertNotIn(secret.encode("utf-8"), persisted)

    def test_supplied_source_projection_must_match_fresh_owner_projection(self) -> None:
        self.configure("authoritative")
        response = self.response()
        occurrence = replace(
            self.occurrence(response),
            source={"source_kind": "native_runtime"},
        )
        assert occurrence is not None
        supplied_projection = project_source_metadata(
            {"source_type": "api", "title": "stale-safe-title"},
            screen=freeze_source_metadata_screen(),
        )
        supplied = replace(
            occurrence,
            source_metadata_projection=supplied_projection.document,
            source_metadata_receipt=supplied_projection.receipt,
        )
        runtime = main.prompt_archive_runtime.for_project("9000")

        with self.assertRaises(PromptArchiveStorageError) as raised:
            runtime.bind_source_metadata_projection(
                supplied,
                {"source_type": "api", "title": "current-safe-title"},
                screen_factory=freeze_source_metadata_screen,
            )

        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
        )
        self.assertFalse(any(self.archive_root.rglob("plan.json")))

    def test_authoritative_read_only_identity_writes_only_compact_prompt(self) -> None:
        self.configure("authoritative")
        response = self.response()
        response["newly_assigned"] = False
        compact = main.materialize_sequential_graph_response(
            response=response,
            directory_template=str(self.prompt_root / "{repository}"),
            agent_latest_file_template=str(
                self.prompt_root
                / "agents"
                / "{repository}_{agent_phone}-latest.prompt"
            ),
            response_kind="transition-response",
            request_agent_phone="4102",
            archive_occurrence=None,
        )

        self.assertEqual(compact["active_task"], response["active_task"])
        self.assertNotIn("project_state", compact)
        self.assertTrue(Path(compact["latest_agent_prompt_file_path"]).is_file())
        self.assertFalse(compact["response_archive_available"])
        self.assertNotIn("response_file_path", compact)
        self.assertNotIn("latest_response_file_path", compact)
        self.assertEqual(list(self.archive_root.rglob("*.json")), [])

    def test_legacy_after_authoritative_reuses_project_latest_authority(self) -> None:
        self.configure("authoritative")
        response = self.response()
        occurrence = replace(
            self.occurrence(response),
            source={
                "source_kind": "native_runtime",
                "assignment_id": "assignment-one",
                "source_commit": "1" * 40,
            },
        )
        authoritative = self.materialize(
            response,
            archive_occurrence=occurrence,
        )
        authoritative_latest = Path(authoritative["latest_response_file_path"])
        self.assertEqual(authoritative_latest.parent.name, "9000")

        self.configure("legacy")
        legacy_response = self.response()
        legacy_response["answer"] = "Explicit legacy-mode response after disable"
        legacy_response["assignment"] = {"revision": occurrence.state_revision + 1}
        legacy = main.materialize_sequential_graph_response(
            response=legacy_response,
            directory_template=str(self.prompt_root / "{repository}"),
            agent_latest_file_template=str(
                self.prompt_root
                / "agents"
                / "{repository}_{agent_phone}-latest.prompt"
            ),
            response_kind="transition-response",
            request_agent_phone="4102",
            archive_occurrence=None,
        )

        rollback_latest = Path(legacy["latest_response_file_path"])
        self.assertEqual(rollback_latest, authoritative_latest)
        self.assertEqual(
            json.loads(rollback_latest.read_text(encoding="utf-8"))["answer"],
            legacy_response["answer"],
        )
        shared_base_latest = (
            self.prompt_root / "archive-fixture" / "latest-response.json"
        )
        self.assertFalse(shared_base_latest.exists())
        self.assertTrue(
            (rollback_latest.parent / "latest-response.json.nginx-qa-v1").is_dir()
        )

    def test_legacy_latest_response_kinds_are_explicit_and_closed(self) -> None:
        self.configure("legacy")
        coordinator = Mock()
        coordinator.enrolled.return_value = False
        with patch.object(main, "ExternalLatestCoordinator", return_value=coordinator):
            for response_kind, write_kind in (
                ("transition-response", "transition"),
                ("identity-response", "identity"),
                ("state-response", "state"),
            ):
                with self.subTest(response_kind=response_kind):
                    coordinator.publish.reset_mock()
                    main.materialize_sequential_graph_response(
                        response=self.response(),
                        directory_template=str(self.prompt_root / "{repository}"),
                        agent_latest_file_template=str(
                            self.prompt_root
                            / "agents"
                            / "{repository}_{agent_phone}-latest.prompt"
                        ),
                        response_kind=response_kind,
                        request_agent_phone="4102",
                        archive_occurrence=None,
                    )
                    self.assertEqual(
                        coordinator.publish.call_args.kwargs["write_kind"],
                        write_kind,
                    )

            coordinator.reset_mock()
            with self.assertRaises(PromptArchiveStorageError) as caught:
                main.materialize_sequential_graph_response(
                    response=self.response(),
                    directory_template=str(self.prompt_root / "{repository}"),
                    agent_latest_file_template=str(
                        self.prompt_root
                        / "agents"
                        / "{repository}_{agent_phone}-latest.prompt"
                    ),
                    response_kind="future-unreviewed-response",
                    request_agent_phone="4102",
                    archive_occurrence=None,
                )
        self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REQUEST_INVALID")
        coordinator.publish.assert_not_called()

    def test_rootless_legacy_second_materialize_updates_typed_latest(self) -> None:
        environment = self.environment("legacy")
        environment.pop("NGINX_QA_PROMPT_ARCHIVE_ROOT")
        configured = main.configure_prompt_archive_runtime(
            environment,
            occurrence_source_reader=self.source_reader,
            occurrence_receipt_root=self.root / "occurrence-receipts",
        )
        self.assertIsNone(configured.config.root)

        first_response = self.response()
        first_response["answer"] = "rootless first latest"
        first = self.materialize(first_response)
        self.assertNotIn(
            "project_latest",
            first.get("response_storage_errors", {}),
        )
        latest_path = Path(first["latest_response_file_path"])
        self.assertEqual(
            json.loads(latest_path.read_text(encoding="utf-8"))["answer"],
            first_response["answer"],
        )

        second_response = copy.deepcopy(first_response)
        second_response["answer"] = "rootless second latest"
        second_response["transition_applied"]["transition_id"] = (
            "rootless-transition-two"
        )
        second_response["transition_applied"]["resolved_at"] = (
            "2026-10-11T18:02:00+02:00"
        )
        second = self.materialize(second_response)

        self.assertNotIn(
            "project_latest",
            second.get("response_storage_errors", {}),
        )
        self.assertEqual(Path(second["latest_response_file_path"]), latest_path)
        self.assertEqual(
            json.loads(latest_path.read_text(encoding="utf-8"))["answer"],
            second_response["answer"],
        )
        sidecar = latest_path.parent / "latest-response.json.nginx-qa-v1"
        self.assertTrue(sidecar.is_dir())
        terminal_journals = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sidecar.rglob("journal.json")
        ]
        self.assertEqual(len(terminal_journals), 2)
        self.assertEqual(
            {journal["state"] for journal in terminal_journals},
            {"complete"},
        )
        self.assertFalse(self.archive_root.exists())

    def test_configured_legacy_private_bootstrap_exact_retry_recovers_without_revision(
        self,
    ) -> None:
        for stage in (FAULT_AFTER_OPERATION_PLAN, FAULT_AFTER_CURRENT_INTENT):
            with self.subTest(stage=stage):
                self.prompt_root = self.root / f"prompt-private-retry-{stage}"
                self.archive_root = self.root / f"archive-private-retry-{stage}"
                self.prompt_root.mkdir()
                self.archive_root.mkdir()
                self.configure("legacy")
                response = self.response()
                response["answer"] = f"exact private retry after {stage}"
                fired = False

                def inject(observed: str) -> None:
                    nonlocal fired
                    if observed == stage and not fired:
                        fired = True
                        raise PromptArchiveStorageError(
                            "PROMPT_ARCHIVE_UNAVAILABLE"
                        )

                real_coordinator = ExternalLatestCoordinator

                def coordinator_with_fault(*args, **kwargs):
                    options = {**kwargs, "fault_hook": inject}
                    return real_coordinator(*args, **options)

                def materialize():
                    return main.materialize_sequential_graph_response(
                        response=response,
                        directory_template=str(
                            self.prompt_root / "{repository}"
                        ),
                        agent_latest_file_template=str(
                            self.prompt_root
                            / "agents"
                            / "{repository}_{agent_phone}-latest.prompt"
                        ),
                        response_kind="identity-response",
                        request_agent_phone="4102",
                        archive_occurrence=None,
                    )

                with patch.object(
                    main,
                    "ExternalLatestCoordinator",
                    side_effect=coordinator_with_fault,
                ):
                    interrupted = materialize()
                    self.assertTrue(fired)
                    # Before enrollment the injected storage failure is
                    # reported directly.  After current-intent installation,
                    # the same request may first probe the common path and
                    # report its missing-revision admission error.  Both are
                    # safe warnings; the invariant under test is that the
                    # following exact no-revision retry adopts the private plan.
                    self.assertIn(
                        interrupted["response_storage_errors"]["project_latest"],
                        {
                            "PROMPT_ARCHIVE_UNAVAILABLE",
                            "PROMPT_ARCHIVE_REQUEST_INVALID",
                        },
                    )

                    recovered = materialize()

                self.assertNotIn(
                    "response_storage_errors",
                    recovered,
                )
                latest_path = Path(recovered["latest_response_file_path"])
                self.assertEqual(
                    json.loads(latest_path.read_text(encoding="utf-8"))["answer"],
                    response["answer"],
                )
                sidecar = latest_path.parent / "latest-response.json.nginx-qa-v1"
                plans = list(sidecar.glob("operations/v1/*/*/plan.json"))
                journals = list(sidecar.glob("operations/v1/*/*/journal.json"))
                self.assertEqual(len(plans), 1)
                self.assertEqual(len(journals), 1)
                self.assertEqual(
                    json.loads(journals[0].read_text(encoding="utf-8"))["state"],
                    "complete",
                )
                slot = json.loads(
                    (sidecar / "slot.json").read_text(encoding="utf-8")
                )
                self.assertEqual(slot["generation"], 1)
                self.assertEqual(list(self.archive_root.rglob("plan.json")), [])

    def test_configured_legacy_enrollment_race_switches_to_one_common_result(
        self,
    ) -> None:
        self.archive_root.mkdir()
        self.configure("legacy")
        response = self.response()
        response["answer"] = "requested writer wins through common recovery"
        response["assignment"] = {"revision": 7}
        original_publish = ExternalLatestCoordinator.publish
        injected = False

        def publish_with_racing_enrollment(coordinator, content, **kwargs):
            nonlocal injected
            if not injected and kwargs.get("pre_enrollment_verifier") is not None:
                injected = True
                race_occurrence_id = "racing-private-enrollment"
                race_operation_id = "parop1-" + hashlib.sha256(
                    race_occurrence_id.encode("ascii")
                ).hexdigest()[:32]
                race_response_id = response_id_v1(
                    coordinator.project_id,
                    "sprint-runtime-one",
                    "state-response",
                    "identity-state",
                    race_occurrence_id,
                )
                racing = ExternalLatestCoordinator(
                    coordinator.external_latest,
                    project_id=coordinator.project_id,
                )
                original_publish(
                    racing,
                    b'{"answer":"racing private enrollment"}',
                    write_kind="state",
                    occurrence_id=race_occurrence_id,
                    operation_id=race_operation_id,
                    published_at="2026-10-20T18:00:00Z",
                    source_response_id=race_response_id,
                    source_sprint_id="sprint-runtime-one",
                    source_revision=1,
                    content_kind="legacy_inline",
                )
            return original_publish(coordinator, content, **kwargs)

        with patch.object(
            ExternalLatestCoordinator,
            "publish",
            new=publish_with_racing_enrollment,
        ):
            compact = main.materialize_sequential_graph_response(
                response=response,
                directory_template=str(self.prompt_root / "{repository}"),
                agent_latest_file_template=str(
                    self.prompt_root
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
                response_kind="state-response",
                request_agent_phone="4102",
                archive_occurrence=None,
            )

        self.assertTrue(injected)
        self.assertNotIn("response_storage_errors", compact)
        latest_path = Path(compact["latest_response_file_path"])
        self.assertEqual(
            json.loads(latest_path.read_text(encoding="utf-8"))["answer"],
            response["answer"],
        )
        sidecar = latest_path.parent / "latest-response.json.nginx-qa-v1"
        private_plans = list(sidecar.glob("operations/v1/*/*/plan.json"))
        private_journals = list(sidecar.glob("operations/v1/*/*/journal.json"))
        self.assertEqual(len(private_plans), 2)
        self.assertEqual(len(private_journals), 2)
        self.assertEqual(
            {
                json.loads(path.read_text(encoding="utf-8"))["state"]
                for path in private_journals
            },
            {"complete"},
        )
        common_plans = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in self.archive_root.rglob("plan.json")
        ]
        self.assertEqual(len(common_plans), 1)
        self.assertEqual(
            common_plans[0]["legacy_latest"]["write_kind"],
            "state",
        )
        self.assertEqual(
            json.loads((sidecar / "slot.json").read_text(encoding="utf-8"))[
                "generation"
            ],
            2,
        )

    def test_shared_legacy_target_switches_to_common_plan_after_enrollment(
        self,
    ) -> None:
        self.archive_root.mkdir()
        self.configure("legacy")
        first_response = self.response()
        first_response["answer"] = "pre-enrollment latest"
        first = main.materialize_sequential_graph_response(
            response=first_response,
            directory_template=str(self.prompt_root / "{repository}"),
            agent_latest_file_template=str(
                self.prompt_root
                / "agents"
                / "{repository}_{agent_phone}-latest.prompt"
            ),
            response_kind="state-response",
            request_agent_phone="4102",
            archive_occurrence=None,
        )
        latest_path = Path(first["latest_response_file_path"])
        self.assertEqual(
            json.loads(latest_path.read_text(encoding="utf-8"))["answer"],
            first_response["answer"],
        )

        missing_revision = self.response()
        missing_revision["answer"] = "must not supersede without revision"
        rejected = main.materialize_sequential_graph_response(
            response=missing_revision,
            directory_template=str(self.prompt_root / "{repository}"),
            agent_latest_file_template=str(
                self.prompt_root
                / "agents"
                / "{repository}_{agent_phone}-latest.prompt"
            ),
            response_kind="state-response",
            request_agent_phone="4102",
            archive_occurrence=None,
        )
        self.assertEqual(
            rejected["response_storage_errors"]["project_latest"],
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )
        self.assertEqual(
            json.loads(latest_path.read_text(encoding="utf-8"))["answer"],
            first_response["answer"],
        )

        second_response = self.response()
        second_response["answer"] = "post-enrollment common latest"
        second_response["assignment"] = {"revision": 7}
        second = main.materialize_sequential_graph_response(
            response=second_response,
            directory_template=str(self.prompt_root / "{repository}"),
            agent_latest_file_template=str(
                self.prompt_root
                / "agents"
                / "{repository}_{agent_phone}-latest.prompt"
            ),
            response_kind="state-response",
            request_agent_phone="4102",
            archive_occurrence=None,
        )

        self.assertNotIn("project_latest", second.get("response_storage_errors", {}))
        self.assertEqual(
            json.loads(latest_path.read_text(encoding="utf-8"))["answer"],
            second_response["answer"],
        )
        common_plans = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in self.archive_root.rglob("plan.json")
        ]
        self.assertEqual(len(common_plans), 1)
        plan = common_plans[0]
        self.assertEqual(plan["operation_kind"], "legacy_latest_publish")
        self.assertEqual(plan["legacy_latest"]["write_kind"], "state")
        self.assertRegex(
            plan["scope"]["project_id"],
            r"^prompt-target-[0-9a-f]{32}$",
        )
        self.assertEqual(
            plan["compatibility_publication"]["target_authority_id"],
            ExternalLatestCoordinator(
                latest_path,
                project_id=plan["scope"]["project_id"],
            ).target_authority_id,
        )

    def test_legacy_latest_committed_occurrence_provenance_is_exact(self) -> None:
        response = self.response()
        committed = self.occurrence(response)
        assert committed is not None

        def project(candidate: ArchiveOccurrence, *, project_id="9000", sprint_id=None):
            selected_sprint = committed.sprint_id if sprint_id is None else sprint_id
            occurrence_id = "synthetic-before-projection"
            return main._legacy_latest_occurrence(
                project_id=project_id,
                sprint_id=selected_sprint,
                response_kind="transition-response",
                occurrence_id=occurrence_id,
                source_response_id=response_id_v1(
                    project_id,
                    selected_sprint,
                    "transition-response",
                    "transition",
                    occurrence_id,
                ),
                published_at="1970-01-01T00:00:00Z",
                response=response,
                committed_occurrence=candidate,
            )

        with self.assertRaises(PromptArchiveStorageError) as sprint_mismatch:
            project(committed, sprint_id="different-sprint")
        self.assertEqual(
            sprint_mismatch.exception.code,
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )

        with self.assertRaises(PromptArchiveStorageError) as project_mismatch:
            project(committed, project_id="9001")
        self.assertEqual(
            project_mismatch.exception.code,
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )

        forged = replace(committed, response_id="parresp1-" + "f" * 64)
        with self.assertRaises(PromptArchiveStorageError) as response_mismatch:
            project(forged)
        self.assertEqual(
            response_mismatch.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_operator_rollback_is_gated_verified_and_uses_ordered_protocol(
        self,
    ) -> None:
        self.archive_root.mkdir()
        environment = self.environment("legacy")
        environment["NGINX_QA_PROMPT_ARCHIVE_OPERATOR_GATE"] = (
            "prompt-archive-authoritative-v1"
        )
        main.configure_prompt_archive_runtime(
            environment,
            occurrence_source_reader=self.source_reader,
            occurrence_receipt_root=self.root / "occurrence-receipts",
        )
        project_directory = self.resolve_project_prompt_directory(
            "9000",
            create=True,
        )
        latest_path = project_directory / "latest-response.json"
        runtime = main.prompt_archive_runtime.for_project("9000")
        publisher = main.SequentialPromptArchiveCompatibilityPublisher(
            project_id="9000",
            transition_stub_path=project_directory / "state-selected.json",
            latest_path=latest_path,
            archive_owner_verifier=runtime.verify_authoritative_archive_owner,
        )

        def state_occurrence(name: str, revision: int, timestamp: str):
            return ArchiveOccurrence(
                project_id="9000",
                sprint_id="sprint-runtime-one",
                response_id=response_id_v1(
                    "9000",
                    "sprint-runtime-one",
                    "state-response",
                    "identity-state",
                    name,
                ),
                response_kind="state-response",
                occurrence_kind="identity-state",
                occurrence_id=name,
                transition_id=None,
                source={"source_kind": "runtime_shadow"},
                authorization={},
                created_at=timestamp,
                verified_at=timestamp,
                state_revision=revision,
            )

        selected = b'{"answer":"operator-selected legacy rollback"}'
        selected_occurrence = state_occurrence(
            "state-selected-for-rollback",
            1,
            "2026-10-20T16:00:00Z",
        )
        selected_operation_id = runtime._legacy_latest_operation_id(
            selected_occurrence,
            "state",
        )
        runtime.publish_legacy_latest(
            selected,
            selected_occurrence,
            write_kind="state",
            operation_id=selected_operation_id,
            compatibility_publisher=publisher,
        )
        initial = b'{"answer":"newer authoritative-compatible value"}'
        newer_occurrence = state_occurrence(
            "state-newer-before-rollback",
            2,
            "2026-10-20T16:00:30Z",
        )
        runtime.publish_legacy_latest(
            initial,
            newer_occurrence,
            write_kind="state",
            compatibility_publisher=publisher,
        )
        self.assertEqual(latest_path.read_bytes(), initial)

        selected_sha256 = hashlib.sha256(selected).hexdigest()
        rollback_response_id = response_id_v1(
            "9000",
            "sprint-runtime-one",
            "state-response",
            "identity-state",
            "rollback-selection-one",
        )
        common = {
            "project_id": "9000",
            "source_sprint_id": "sprint-runtime-one",
            "source_response_id": rollback_response_id,
            "occurrence_id": "rollback-selection-one",
            "operation_id": "parop1-" + "3" * 32,
            "selected_response_id": selected_occurrence.response_id,
            "selected_operation_id": selected_operation_id,
            "published_at": "2026-10-20T16:01:00Z",
            "source_revision": 3,
            "expected_content_sha256": selected_sha256,
            "expected_content_size": len(selected),
        }

        with self.assertRaises(PromptArchiveStorageError) as unauthorized:
            main.publish_prompt_archive_legacy_latest_rollback(
                operator_gate="wrong-gate",
                **common,
            )
        self.assertEqual(
            unauthorized.exception.code,
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )
        self.assertEqual(latest_path.read_bytes(), initial)

        with self.assertRaises(PromptArchiveStorageError) as mismatched:
            main.publish_prompt_archive_legacy_latest_rollback(
                operator_gate="prompt-archive-authoritative-v1",
                **{**common, "expected_content_sha256": "0" * 64},
            )
        self.assertEqual(
            mismatched.exception.code,
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )
        self.assertEqual(latest_path.read_bytes(), initial)

        receipt = main.publish_prompt_archive_legacy_latest_rollback(
            operator_gate="prompt-archive-authoritative-v1",
            **common,
        )
        self.assertEqual(receipt["outcome"], "published")
        self.assertEqual(latest_path.read_bytes(), selected)
        operation_root = (
            latest_path.parent
            / "latest-response.json.nginx-qa-v1"
            / "operations"
            / "v1"
            / rollback_response_id
            / common["operation_id"]
        )
        plan = json.loads((operation_root / "plan.json").read_text(encoding="utf-8"))
        journal = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        self.assertEqual(plan["operation_kind"], "legacy_latest_publish")
        self.assertEqual(plan["write_kind"], "rollback")
        self.assertEqual(journal["operation_kind"], "legacy_latest_publish")
        self.assertEqual(journal["write_kind"], "rollback")
        common_plan_path = next(
            self.archive_root.rglob(f"{common['operation_id']}/plan.json")
        )
        common_plan = json.loads(common_plan_path.read_text(encoding="utf-8"))
        self.assertEqual(common_plan["operation_kind"], "legacy_latest_publish")
        self.assertEqual(common_plan["legacy_latest"]["write_kind"], "rollback")
        self.assertEqual(
            common_plan["legacy_latest"]["content_sha256"],
            selected_sha256,
        )
        self.assertEqual(
            common_plan["legacy_latest"]["content_size"],
            len(selected),
        )
        self.assertIn("payload_root", common_plan["legacy_latest"])
        selection_path = next(
            self.archive_root.rglob(
                f"rollback-selections/v1/{common['operation_id']}.json"
            )
        )
        selection = json.loads(selection_path.read_text(encoding="utf-8"))
        self.assertEqual(
            selection["selected_response_id"],
            selected_occurrence.response_id,
        )
        self.assertEqual(
            selection["selected_operation_id"],
            selected_operation_id,
        )

    def test_storage_errors_never_expose_oserror_path_or_canary(self) -> None:
        self.configure("legacy")
        real_write = main.write_prompt_text_if_changed
        raw_canary = "RAW-STORAGE-CANARY-DO-NOT-RETURN"
        raw_path = str(self.root / "operator-secret" / "response.json")

        def fail_transition_write(target_path: Path, prompt: str) -> None:
            if target_path.name.startswith("transition-response-"):
                raise OSError(f"{raw_canary}: cannot write {raw_path}")
            real_write(target_path, prompt)

        with (
            patch.object(
                main,
                "write_prompt_text_if_changed",
                side_effect=fail_transition_write,
            ),
            patch.object(
                main.ExternalLatestCoordinator,
                "publish",
                side_effect=OSError(
                    f"{raw_canary}: cannot replace {raw_path}.latest"
                ),
            ),
        ):
            compact = self.materialize()

        self.assertEqual(
            set(compact["response_storage_errors"]),
            {"response_archive", "project_latest"},
        )
        self.assertEqual(
            compact["response_storage_errors"]["response_archive"],
            "SEQUENTIAL_RESPONSE_STORAGE_UNAVAILABLE",
        )
        for code in compact["response_storage_errors"].values():
            self.assertRegex(code, r"^[A-Z][A-Z0-9_]{1,95}$")
        projected_errors = json.dumps(
            compact["response_storage_errors"],
            ensure_ascii=False,
        )
        self.assertNotIn(raw_canary, projected_errors)
        self.assertNotIn(raw_path, projected_errors)
        agent_prompt = Path(compact["latest_agent_prompt_file_path"])
        self.assertTrue(agent_prompt.is_file())
        self.assertEqual(
            json.loads(agent_prompt.read_text(encoding="utf-8")),
            compact,
        )

    def test_enrolled_resolver_failure_keeps_legacy_compact_and_skips_fallback_latest(
        self,
    ) -> None:
        enrolled = (
            self.prompt_root
            / "archive-fixture"
            / ".nginx-qa-projects"
            / "9000"
        )
        enrolled.mkdir(parents=True)
        fallback_latest = (
            self.prompt_root / "archive-fixture" / "latest-response.json"
        )

        for mode in ("legacy", "shadow"):
            with self.subTest(mode=mode):
                self.configure(mode)
                response = self.response()
                response["answer"] = f"usable compact response in {mode}"
                with (
                    patch.object(
                        main,
                        "_prompt_archive_project_prompt_directory",
                        side_effect=PromptArchiveStorageError(
                            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"
                        ),
                    ),
                    patch.object(
                        main.ExternalLatestCoordinator,
                        "publish",
                    ) as latest_publish,
                ):
                    compact = main.materialize_sequential_graph_response(
                        response=response,
                        directory_template=str(self.prompt_root / "{repository}"),
                        agent_latest_file_template=str(
                            self.prompt_root
                            / "agents"
                            / "{repository}_{agent_phone}-latest.prompt"
                        ),
                        response_kind="transition-response",
                        request_agent_phone="4102",
                        archive_occurrence=None,
                    )

                latest_publish.assert_not_called()
                self.assertEqual(
                    compact["response_storage_errors"]["project_latest"],
                    "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
                )
                self.assertFalse(fallback_latest.exists())
                self.assertTrue(Path(compact["response_file_path"]).is_file())
                prompt_path = Path(compact["latest_agent_prompt_file_path"])
                self.assertTrue(prompt_path.is_file())
                self.assertEqual(
                    json.loads(prompt_path.read_text(encoding="utf-8")),
                    compact,
                )
                self.assertEqual(compact["active_task"], response["active_task"])

    def test_authoritative_resolver_failure_is_fail_closed_without_fallback(self) -> None:
        self.configure("authoritative")
        response = self.response()
        occurrence = replace(
            self.occurrence(response),
            source={
                "source_kind": "native_runtime",
                "assignment_id": "assignment-one",
                "source_commit": "1" * 40,
            },
        )
        fallback_latest = (
            self.prompt_root / "archive-fixture" / "latest-response.json"
        )
        agent_prompt = (
            self.prompt_root / "agents" / "archive-fixture_4199-latest.prompt"
        )

        with (
            patch.object(
                main,
                "_prompt_archive_project_prompt_directory",
                side_effect=PromptArchiveStorageError(
                    "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"
                ),
            ),
            patch.object(
                main.ExternalLatestCoordinator,
                "publish",
            ) as latest_publish,
        ):
            with self.assertRaises(PromptArchiveStorageError) as caught:
                main.materialize_sequential_graph_response(
                    response=response,
                    directory_template=str(self.prompt_root / "{repository}"),
                    agent_latest_file_template=str(
                        self.prompt_root
                        / "agents"
                        / "{repository}_{agent_phone}-latest.prompt"
                    ),
                    response_kind="transition-response",
                    request_agent_phone="4199",
                    archive_occurrence=occurrence,
                )

        self.assertEqual(
            caught.exception.code,
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )
        latest_publish.assert_not_called()
        self.assertFalse(fallback_latest.exists())
        self.assertFalse(agent_prompt.exists())

    def test_authoritative_cross_component_boundaries_recover_exactly_once(
        self,
    ) -> None:
        scenarios = (
            ("latest-current-intent", "after_current_intent_publish", "latest"),
            ("transition-stub", "after_transition_stub_publish", "latest"),
            ("external-latest", "after_external_content_publish", "latest"),
            ("latest-current-slot", "after_current_slot_publish", "latest"),
            ("latest-terminal", "after_terminal_journal_publish", "latest"),
            (
                "archive-compatibility-receipt",
                "after_compatibility_publication",
                "archive",
            ),
        )

        def file_snapshot(*roots: Path) -> dict[str, str]:
            return {
                f"{ordinal}:{path.relative_to(root).as_posix()}": hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for ordinal, root in enumerate(roots)
                for path in root.rglob("*")
                if path.is_file()
            }

        for label, fault_stage, fault_owner in scenarios:
            with self.subTest(boundary=label):
                scenario_root = self.root / f"cross-component-{label}"
                archive_root = scenario_root / "archive"
                prompt_root = scenario_root / "prompt"
                prompt_root.mkdir(parents=True)
                latest_path = prompt_root / "latest-response.json"
                config = PromptArchiveRuntimeConfig(
                    mode="authoritative",
                    root=archive_root,
                    max_hydrated_bytes=8 * 1024 * 1024,
                    operator_gate="prompt-archive-authoritative-v1",
                )
                occurrence = replace(
                    self.occurrence(self.response()),
                    source={
                        "source_kind": "native_runtime",
                        "assignment_id": "assignment-one",
                        "source_commit": "1" * 40,
                    },
                )
                transition_path = prompt_root / (
                    f"{occurrence.response_kind}-{occurrence.response_id}.json"
                )
                native_bytes = main.prompt_archive_canonical_json_bytes_v1(
                    {
                        "active_task": self.response()["active_task"],
                        "project_id": "9000",
                        "sprint_id": "sprint-runtime-one",
                    },
                    maximum=8 * 1024 * 1024,
                )
                fired = False

                def inject(observed: str) -> None:
                    nonlocal fired
                    if observed == fault_stage and not fired:
                        fired = True
                        if fault_owner == "archive":
                            raise PromptArchiveStorageError(
                                "PROMPT_ARCHIVE_UNAVAILABLE"
                            )
                        raise RuntimeError(f"injected {fault_stage}")

                def publisher(runtime, *, fault_hook=None):
                    selected = main.SequentialPromptArchiveCompatibilityPublisher(
                        project_id="9000",
                        transition_stub_path=transition_path,
                        latest_path=latest_path,
                        archive_owner_verifier=(
                            runtime.verify_authoritative_archive_owner
                        ),
                    )
                    if fault_hook is not None:
                        selected.coordinator = ExternalLatestCoordinator(
                            latest_path,
                            project_id="9000",
                            fault_hook=fault_hook,
                            archive_owner_verifier=(
                                runtime.verify_authoritative_archive_owner
                            ),
                        )
                    return selected

                failing_registry = PromptArchiveRuntimeRegistry(config)
                failing_runtime = failing_registry.for_project("9000")
                if fault_owner == "archive":
                    assert failing_runtime.storage is not None
                    failing_runtime.storage._fault_hook = inject
                    failing_publisher = publisher(failing_runtime)
                else:
                    failing_publisher = publisher(
                        failing_runtime, fault_hook=inject
                    )

                with self.assertRaises(PromptArchiveStorageError) as caught:
                    failing_runtime.publish_authoritative(
                        native_bytes,
                        occurrence,
                        compatibility_publisher=failing_publisher,
                    )
                self.assertEqual(
                    caught.exception.code,
                    "PROMPT_ARCHIVE_UNAVAILABLE",
                )
                self.assertTrue(fired)

                restarted_registry = PromptArchiveRuntimeRegistry(
                    config,
                    compatibility_resolver=lambda project_id, _plan: publisher(
                        restarted_registry.for_project(project_id)
                    ),
                )
                self.assertEqual(
                    restarted_registry.validate_and_recover(("9000",)),
                    1,
                )
                restarted = restarted_registry.for_project("9000")

                plans = list(archive_root.rglob("plan.json"))
                journals = list(archive_root.rglob("journal.json"))
                compatibility_results = list(
                    archive_root.rglob("compatibility-result.json")
                )
                self.assertEqual(len(plans), 1)
                self.assertEqual(len(journals), 1)
                self.assertEqual(len(compatibility_results), 1)
                plan = json.loads(plans[0].read_text(encoding="utf-8"))
                journal = json.loads(journals[0].read_text(encoding="utf-8"))
                self.assertEqual(journal["state"], "complete")
                self.assertEqual(journal["journal_generation"], 6)
                self.assertNotEqual(journal["compatibility_result"], "pending")

                descriptor_bytes = main.prompt_archive_canonical_json_bytes_v1(
                    plan["compatibility_publication"]["descriptor"]["document"],
                    maximum=64 * 1024,
                )
                self.assertEqual(transition_path.read_bytes(), descriptor_bytes)
                self.assertEqual(latest_path.read_bytes(), descriptor_bytes)

                sidecar = prompt_root / "latest-response.json.nginx-qa-v1"
                latest_journals = list(sidecar.rglob("journal.json"))
                self.assertEqual(len(latest_journals), 1)
                self.assertEqual(
                    json.loads(latest_journals[0].read_text(encoding="utf-8"))[
                        "state"
                    ],
                    "complete",
                )

                selected = restarted.lookup_archive(
                    sprint_id="sprint-runtime-one",
                    archive_id=plan["intended_manifest"]["archive_id"],
                )
                self.assertEqual(restarted.hydrate_content(selected), native_bytes)

                before_replay = file_snapshot(archive_root, prompt_root)
                replayed = restarted.publish_authoritative(
                    native_bytes,
                    occurrence,
                    compatibility_publisher=publisher(restarted),
                )
                self.assertEqual(
                    replayed.archive_id,
                    plan["intended_manifest"]["archive_id"],
                )
                self.assertEqual(
                    file_snapshot(archive_root, prompt_root),
                    before_replay,
                )
                self.assertEqual(restarted.recover_pending(), 0)

    def test_durable_receipt_aborts_missing_source_at_intent(self) -> None:
        registry = self.configure("shadow")
        runtime = registry.for_project("9000")
        fired = False

        def fail_after_registration(stage: str) -> None:
            nonlocal fired
            if stage == "after_pending_registration" and not fired:
                fired = True
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

        assert runtime.storage is not None
        runtime.storage._fault_hook = fail_after_registration
        compact = self.materialize()
        self.assertTrue(fired)
        self.assertNotIn("prompt_archive", compact)
        Path(compact["response_file_path"]).unlink()

        restarted = self.configure("shadow")
        self.assertEqual(restarted.validate_and_recover(("9000",)), 1)
        self.assertEqual(restarted.for_project("9000").recover_pending(), 0)
        self.assertEqual(
            list(self.archive_root.rglob("pointers/v1/*.json")),
            [],
        )

    def test_verified_graph_recovers_after_legacy_source_is_deleted(self) -> None:
        registry = self.configure("shadow")
        runtime = registry.for_project("9000")
        journal_writes = 0

        def fail_after_blobs_verified(stage: str) -> None:
            nonlocal journal_writes
            if stage == "after_journal_current":
                journal_writes += 1
                if journal_writes == 2:
                    raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

        assert runtime.storage is not None
        runtime.storage._fault_hook = fail_after_blobs_verified
        compact = self.materialize()
        self.assertEqual(journal_writes, 2)
        self.assertNotIn("prompt_archive", compact)
        Path(compact["response_file_path"]).unlink()

        restarted = self.configure("shadow")
        self.assertEqual(restarted.validate_and_recover(("9000",)), 1)
        pointers = list(self.archive_root.rglob("pointers/v1/*.json"))
        self.assertEqual(len(pointers), 1)
        self.assertEqual(restarted.for_project("9000").recover_pending(), 0)

    def test_prompt_settings_cannot_overlap_occurrence_receipt_root(self) -> None:
        self.configure("shadow")
        with self.assertRaises(PromptArchiveStorageError) as caught:
            main.validate_prompt_archive_template_roots(
                str(self.root / "occurrence-receipts" / "{repository}"),
                str(
                    self.prompt_root
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
            )
        self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REQUEST_INVALID")

    def test_legacy_mode_creates_no_occurrence_receipt_root(self) -> None:
        receipt_root = self.root / "occurrence-receipts"
        main.configure_prompt_archive_runtime(
            self.environment("legacy"),
            occurrence_source_reader=self.source_reader,
            occurrence_receipt_root=receipt_root,
        )
        self.assertFalse(receipt_root.exists())

    def test_existing_legacy_archive_root_is_recovered_at_startup(self) -> None:
        self.archive_root.mkdir()
        configured = self.configure("legacy")
        with patch.object(
            configured,
            "validate_and_recover",
            return_value=1,
        ) as recover:
            selected = asyncio.run(
                main.recover_prompt_archive_runtime_for_startup(
                    configured,
                    ("9000",),
                )
            )

        self.assertIs(selected, configured)
        recover.assert_called_once_with(("9000",))

    def test_legacy_startup_without_native_prover_preserves_pending_for_authoritative_restart(
        self,
    ) -> None:
        configured = self.configure("authoritative")
        runtime = configured.for_project("9000")
        occurrence = replace(
            self.occurrence(self.response()),
            source={
                "source_kind": "native_runtime",
                "assignment_id": "assignment-one",
                "source_commit": "1" * 40,
            },
        )
        native_bytes = main.prompt_archive_canonical_json_bytes_v1(
            {
                "active_task": self.response()["active_task"],
                "project_id": "9000",
                "sprint_id": "sprint-runtime-one",
            },
            maximum=8 * 1024 * 1024,
        )
        project_directory = self.resolve_project_prompt_directory(
            "9000",
            create=True,
        )
        publisher = main.SequentialPromptArchiveCompatibilityPublisher(
            project_id="9000",
            transition_stub_path=(
                project_directory
                / f"{occurrence.response_kind}-{occurrence.response_id}.json"
            ),
            latest_path=project_directory / "latest-response.json",
            archive_owner_verifier=runtime.verify_authoritative_archive_owner,
        )
        fired = False

        def fail_after_registration(stage: str) -> None:
            nonlocal fired
            if stage == "after_pending_registration" and not fired:
                fired = True
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

        assert runtime.storage is not None
        runtime.storage._fault_hook = fail_after_registration
        with self.assertRaises(PromptArchiveStorageError) as interrupted:
            runtime.publish_authoritative(
                native_bytes,
                occurrence,
                compatibility_publisher=publisher,
            )
        self.assertEqual(interrupted.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")
        self.assertTrue(fired)

        plans = list(self.archive_root.rglob("plan.json"))
        self.assertEqual(len(plans), 1)
        operation_root = plans[0].parent
        self.assertFalse((operation_root / "journal.json").exists())
        self.assertEqual(list(operation_root.rglob("journal-generations")), [])

        def recovery_state() -> dict[str, bytes]:
            return {
                path.relative_to(self.archive_root).as_posix(): path.read_bytes()
                for path in self.archive_root.rglob("*")
                if path.is_file() and not path.name.endswith(".lock")
            }

        before_legacy_startup = recovery_state()
        legacy = self.configure("legacy")
        with self.assertRaises(PromptArchiveStorageError) as denied:
            asyncio.run(
                main.recover_prompt_archive_runtime_for_startup(
                    legacy,
                    ("9000",),
                )
            )
        self.assertEqual(denied.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")
        self.assertEqual(recovery_state(), before_legacy_startup)
        self.assertFalse((operation_root / "journal.json").exists())

        authoritative = self.configure("authoritative")
        selected = asyncio.run(
            main.recover_prompt_archive_runtime_for_startup(
                authoritative,
                ("9000",),
            )
        )
        self.assertIs(selected, authoritative)
        terminal = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        self.assertEqual(terminal["state"], "complete")
        self.assertEqual(authoritative.for_project("9000").recover_pending(), 0)

    def test_legacy_configured_prompt_base_drift_cannot_rebind_shared_latest(
        self,
    ) -> None:
        first = self.response()
        first_occurrence = replace(
            self.occurrence(first),
            source={
                "source_kind": "native_runtime",
                "assignment_id": "assignment-one",
                "source_commit": "1" * 40,
            },
        )
        self.configure("authoritative")
        first_compact = self.materialize(
            first,
            archive_occurrence=first_occurrence,
        )
        self.assertNotIn("response_storage_errors", first_compact)

        first_latest = Path(first_compact["latest_response_file_path"])
        self.assertTrue(first_latest.is_file())
        first_latest_bytes = first_latest.read_bytes()
        first_runtime = main.prompt_archive_runtime.for_project("9000")
        self.assertIsNotNone(first_runtime.storage)
        binding = first_runtime.storage.read_immutable_json(
            first_runtime._legacy_latest_target_binding_path()
        )
        self.assertEqual(
            binding["target_authority_id"],
            ExternalLatestCoordinator(
                first_latest,
                project_id="9000",
            ).target_authority_id,
        )

        self.configure("legacy")

        def archive_snapshot() -> dict[str, bytes]:
            return {
                path.relative_to(self.archive_root).as_posix(): path.read_bytes()
                for path in self.archive_root.rglob("*")
                if path.is_file() and not path.name.endswith(".lock")
            }

        before_drift = archive_snapshot()
        second = copy.deepcopy(first)
        second["answer"] = "A different response must not rebind latest"
        second["transition_applied"]["transition_id"] = "transition-two"
        second["transition_applied"]["resolved_at"] = (
            "2026-10-11T18:02:00+02:00"
        )
        second_base = self.root / "configured-prompt-base-b"
        second_authority = (
            second_base
            / "archive-fixture"
            / ".nginx-qa-projects"
            / "9000"
        )
        second_authority.mkdir(parents=True)

        def resolve_second_authority(
            project_id: str,
            *,
            create: bool = False,
        ) -> Path:
            self.assertEqual(project_id, "9000")
            if create:
                second_authority.mkdir(parents=True, exist_ok=True)
            return second_authority

        with patch.object(
            main,
            "_prompt_archive_project_prompt_directory",
            side_effect=resolve_second_authority,
        ):
            second_compact = main.materialize_sequential_graph_response(
                response=second,
                directory_template=str(second_base / "{repository}"),
                agent_latest_file_template=str(
                    second_base
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
                response_kind="transition-response",
                request_agent_phone="4102",
                archive_occurrence=self.occurrence(second),
            )

        self.assertEqual(
            second_compact["response_storage_errors"]["project_latest"],
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )
        second_latest = second_base / "archive-fixture" / "latest-response.json"
        self.assertFalse(second_latest.exists())
        self.assertFalse(
            (
                second_base
                / "archive-fixture"
                / "latest-response.json.nginx-qa-v1"
            ).exists()
        )
        self.assertEqual(list(second_authority.iterdir()), [])
        self.assertEqual(first_latest.read_bytes(), first_latest_bytes)
        self.assertEqual(archive_snapshot(), before_drift)

    def test_shared_target_is_in_startup_set_and_resolves_same_latest_authority(
        self,
    ) -> None:
        context = {
            "git_context_key": "github.com/acme/archive-fixture",
            "git_address": "https://github.com/acme/archive-fixture.git",
            "project_name": "Archive Fixture",
        }
        target_id = main._prompt_archive_shared_target_project_id(**context)
        directory = self.prompt_root / "archive-fixture"
        directory.mkdir(exist_ok=True)
        occurrence_id = "shared-target-recovery"
        response_id = response_id_v1(
            target_id,
            "sprint-runtime-one",
            "state-response",
            "identity-state",
            occurrence_id,
        )
        plan = {
            "occurrence": {
                "response_kind": "state-response",
                "response_id": response_id,
            }
        }
        with (
            patch.object(
                main,
                "configured_git_contexts_from_config",
                return_value=[context],
            ),
            patch.object(main, "read_git_config_file", return_value={"configured": True}),
            patch.object(main, "git_config_file_lock", return_value=contextlib.nullcontext()),
            patch.object(
                main,
                "read_sequential_prompt_settings_file",
                return_value={
                    "directory_template": str(self.prompt_root / "{repository}"),
                    "agent_latest_file_template": str(
                        self.prompt_root
                        / "agents"
                        / "{repository}_{agent_phone}-latest.prompt"
                    ),
                },
            ),
        ):
            self.assertIn(target_id, main.prompt_archive_startup_project_ids({}))
            publisher = main.resolve_prompt_archive_compatibility_publisher(
                target_id,
                plan,
            )

        self.assertEqual(
            publisher.coordinator.external_latest,
            directory / "latest-response.json",
        )
        self.assertEqual(
            publisher.target_authority_id,
            ExternalLatestCoordinator(
                directory / "latest-response.json",
                project_id=target_id,
            ).target_authority_id,
        )

    def test_existing_legacy_archive_root_recovery_failure_is_fail_closed(
        self,
    ) -> None:
        self.archive_root.mkdir()
        configured = self.configure("legacy")
        with patch.object(
            configured,
            "validate_and_recover",
            side_effect=PromptArchiveStorageError(
                "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"
            ),
        ):
            with self.assertRaises(PromptArchiveStorageError) as caught:
                asyncio.run(
                    main.recover_prompt_archive_runtime_for_startup(
                        configured,
                        ("9000",),
                    )
                )

        self.assertEqual(
            caught.exception.code,
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )
        self.assertIs(main.prompt_archive_runtime, configured)

    def test_shadow_recovery_failure_disables_writes_without_blocking_service(self) -> None:
        failures = (
            (RuntimeError("opaque recovery failure"), 0),
            (
                PromptArchiveStorageError(
                    "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"
                ),
                1,
            ),
        )
        for failure, expected_corruption_delta in failures:
            with self.subTest(failure=type(failure).__name__):
                registry = self.configure("shadow")
                receipt_root = main.prompt_archive_occurrence_receipt_path
                self.assertIsNotNone(receipt_root)
                before = main.prompt_archive_runtime_metrics.snapshot()
                with patch.object(
                    registry,
                    "validate_and_recover",
                    side_effect=failure,
                ):
                    disabled = asyncio.run(
                        main.recover_prompt_archive_runtime_for_startup(
                            registry,
                            ("9000",),
                        )
                    )
                after = main.prompt_archive_runtime_metrics.snapshot()

                self.assertIs(disabled, main.prompt_archive_runtime)
                self.assertEqual(disabled.config.mode, "legacy")
                self.assertEqual(disabled.config.root, registry.config.root)
                self.assertIs(disabled.metrics, main.prompt_archive_runtime_metrics)
                self.assertEqual(
                    main.prompt_archive_occurrence_receipt_path,
                    receipt_root,
                )
                self.assertEqual(
                    after.recovery_failures,
                    before.recovery_failures + 1,
                )
                self.assertEqual(
                    after.corruption_failures,
                    before.corruption_failures + expected_corruption_delta,
                )
                self.assertIsNone(
                    disabled.for_project("9000").publish_shadow(
                        b"legacy remains authoritative",
                        self.occurrence(self.response()),
                    )
                )

    def test_authoritative_recovery_failure_does_not_degrade_to_legacy(self) -> None:
        configured = self.configure("authoritative")
        with patch.object(
            configured,
            "validate_and_recover",
            side_effect=PromptArchiveStorageError(
                "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"
            ),
        ):
            with self.assertRaises(PromptArchiveStorageError) as caught:
                asyncio.run(
                    main.recover_prompt_archive_runtime_for_startup(
                        configured,
                        ("9000",),
                    )
                )
        self.assertEqual(
            caught.exception.code,
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )
        self.assertIs(main.prompt_archive_runtime, configured)
        self.assertEqual(main.prompt_archive_runtime.config.mode, "authoritative")

    def test_recovery_disabled_legacy_registry_rejects_prompt_root_overlap(self) -> None:
        configured = self.configure("shadow")
        receipt_root = main.prompt_archive_occurrence_receipt_path
        self.assertIsNotNone(receipt_root)
        disabled = main.disable_prompt_archive_shadow_after_recovery_failure(
            configured
        )
        self.assertEqual(disabled.config.mode, "legacy")
        self.assertEqual(disabled.config.root, self.archive_root)
        self.assertEqual(main.prompt_archive_occurrence_receipt_path, receipt_root)

        with self.assertRaises(PromptArchiveStorageError) as directory_overlap:
            main.validate_prompt_archive_template_roots(
                str(self.archive_root / "prompts" / "{repository}"),
                str(
                    self.prompt_root
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
            )
        self.assertEqual(
            directory_overlap.exception.code,
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )

        with self.assertRaises(PromptArchiveStorageError) as agent_overlap:
            main.validate_prompt_archive_template_roots(
                str(self.prompt_root / "{repository}"),
                str(
                    self.archive_root
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
            )
        self.assertEqual(
            agent_overlap.exception.code,
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )

        assert receipt_root is not None
        with self.assertRaises(PromptArchiveStorageError) as receipt_directory:
            main.validate_prompt_archive_template_roots(
                str(receipt_root / "prompts" / "{repository}"),
                str(
                    self.prompt_root
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
            )
        self.assertEqual(
            receipt_directory.exception.code,
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )

        with self.assertRaises(PromptArchiveStorageError) as receipt_agent:
            main.validate_prompt_archive_template_roots(
                str(self.prompt_root / "{repository}"),
                str(
                    receipt_root
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
            )
        self.assertEqual(
            receipt_agent.exception.code,
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )

    def test_exact_source_reader_is_bounded_and_rejects_hardlinks(self) -> None:
        source_root = self.root / "exact-source"
        source_root.mkdir()
        source_path = source_root / "transition-response-deadbeef0123456789ab.json"
        source_path.write_bytes(b"12345")
        self.assertEqual(
            main._read_prompt_archive_source_file(
                source_path,
                trusted_root=source_root,
                maximum_bytes=5,
            ),
            b"12345",
        )

        source_path.write_bytes(b"123456")
        with self.assertRaises(PromptArchiveStorageError) as oversized:
            main._read_prompt_archive_source_file(
                source_path,
                trusted_root=source_root,
                maximum_bytes=5,
            )
        self.assertEqual(
            oversized.exception.code,
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
        )

        source_path.write_bytes(b"12345")
        hardlink = source_root / "second-link.json"
        os.link(source_path, hardlink)
        with self.assertRaises(PromptArchiveStorageError) as linked:
            main._read_prompt_archive_source_file(
                source_path,
                trusted_root=source_root,
                maximum_bytes=5,
            )
        self.assertEqual(
            linked.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_exact_source_reader_binds_open_handle_to_initial_leaf(self) -> None:
        source_root = self.root / "source-open-identity"
        source_root.mkdir()
        source_path = source_root / "transition-response-deadbeef0123456789ab.json"
        replacement_path = source_root / "replacement.json"
        source_path.write_bytes(b"original-A")
        replacement_path.write_bytes(b"swapped--B")
        real_open = main.os.open

        def open_replacement(path, flags, *args, **kwargs):
            selected = (
                replacement_path
                if Path(path) == source_path
                else path
            )
            return real_open(selected, flags, *args, **kwargs)

        # Model the critical A->B->A window deterministically: the pre/post
        # pathname snapshots still describe A, while open() yields B.  The
        # resolved-path adapter is held at the expected pathname just as it can
        # be after an ABA restoration; the fstat/snapshot binding must reject.
        with (
            patch.object(main.os, "open", side_effect=open_replacement),
            patch.object(
                main,
                "resolved_path_from_open_descriptor",
                return_value=str(source_path),
            ),
            self.assertRaises(PromptArchiveStorageError) as swapped,
        ):
            main._read_prompt_archive_source_file(
                source_path,
                trusted_root=source_root,
                maximum_bytes=64,
            )
        self.assertEqual(
            swapped.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )
        self.assertEqual(source_path.read_bytes(), b"original-A")

    def test_shadow_failure_preserves_all_legacy_and_prompt_bytes(self) -> None:
        self.configure("legacy")
        baseline = self.materialize()
        paths = [
            Path(baseline["response_file_path"]),
            Path(baseline["latest_response_file_path"]),
            Path(baseline["latest_agent_prompt_file_path"]),
        ]
        before = {path: path.read_bytes() for path in paths}

        self.configure("shadow")
        runtime = main.prompt_archive_runtime.for_project("9000")
        with patch.object(
            runtime,
            "publish_shadow",
            side_effect=PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE"),
        ) as publish:
            compact = self.materialize()

        publish.assert_called_once()
        self.assertNotIn("prompt_archive", compact)
        self.assertEqual({path: path.read_bytes() for path in paths}, before)

    def test_unexpected_shadow_runtime_failure_keeps_compact_prompt_usable(self) -> None:
        self.configure("shadow")
        runtime = main.prompt_archive_runtime.for_project("9000")
        with patch.object(
            runtime,
            "publish_shadow",
            side_effect=RuntimeError("opaque sidecar adapter failure"),
        ) as publish:
            compact = self.materialize()

        publish.assert_called_once()
        self.assertNotIn("prompt_archive", compact)
        self.assertEqual(compact["active_task"], self.response()["active_task"])
        self.assertTrue(Path(compact["response_file_path"]).is_file())
        self.assertEqual(
            json.loads(
                Path(compact["latest_agent_prompt_file_path"]).read_text(
                    encoding="utf-8"
                )
            ),
            compact,
        )

    def test_unexpected_predecessor_hint_failure_is_shadow_only(self) -> None:
        self.configure("shadow")
        runtime = main.prompt_archive_runtime.for_project("9000")
        with (
            patch.object(
                main,
                "verified_prompt_archive_descriptor",
                side_effect=RuntimeError("opaque hint adapter failure"),
            ),
            patch.object(runtime, "publish_shadow") as publish,
        ):
            compact = self.materialize()

        publish.assert_not_called()
        self.assertNotIn("prompt_archive", compact)
        self.assertTrue(Path(compact["response_file_path"]).is_file())
        self.assertTrue(Path(compact["latest_agent_prompt_file_path"]).is_file())

    def test_failed_same_occurrence_retry_retains_verified_descriptor(self) -> None:
        self.configure("shadow")
        first = self.materialize()
        prompt_path = Path(first["latest_agent_prompt_file_path"])
        before = prompt_path.read_bytes()
        runtime = main.prompt_archive_runtime.for_project("9000")

        with patch.object(
            runtime,
            "publish_shadow",
            side_effect=PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE"),
        ):
            retried = self.materialize()

        self.assertEqual(retried["prompt_archive"], first["prompt_archive"])
        self.assertEqual(prompt_path.read_bytes(), before)

    def test_descriptor_retention_read_failure_is_shadow_only(self) -> None:
        self.configure("shadow")
        response = self.response()

        for failure in (
            RuntimeError("opaque retention adapter failure"),
            PromptArchiveStorageError("PROMPT_ARCHIVE_LIMIT_EXCEEDED"),
        ):
            with self.subTest(failure=type(failure).__name__):
                # Re-establish a valid same-occurrence descriptor before each
                # injected failure.  The retry must still commit every legacy
                # destination even when retaining that optional hint fails.
                published = self.materialize(response)
                self.assertIn("prompt_archive", published)
                original_reader = main._read_prompt_archive_source_file

                def fail_committed_response(path, **kwargs):
                    if Path(path).name.startswith("transition-response-"):
                        raise failure
                    return original_reader(path, **kwargs)

                with patch.object(
                    main,
                    "_read_prompt_archive_source_file",
                    side_effect=fail_committed_response,
                ) as bounded_read:
                    compact = self.materialize(response)

                self.assertGreaterEqual(
                    sum(
                        Path(call.args[0]).name.startswith("transition-response-")
                        for call in bounded_read.call_args_list
                    ),
                    1,
                )
                self.assertNotIn("prompt_archive", compact)
                response_path = Path(compact["response_file_path"])
                latest_path = Path(compact["latest_response_file_path"])
                prompt_path = Path(compact["latest_agent_prompt_file_path"])
                self.assertTrue(response_path.is_file())
                self.assertEqual(response_path.read_bytes(), latest_path.read_bytes())
                self.assertEqual(
                    json.loads(prompt_path.read_text(encoding="utf-8")),
                    compact,
                )

    def test_restart_uses_verified_prompt_descriptor_as_predecessor(self) -> None:
        self.configure("shadow")
        first = self.materialize()
        first_archive_id = first["prompt_archive"]["archive_id"]
        first_runtime = main.prompt_archive_runtime.for_project("9000")
        first_read = first_runtime.lookup_archive(
            sprint_id="sprint-runtime-one",
            archive_id=first_archive_id,
        )

        # A new registry models a clean process restart: its in-memory
        # predecessor LRU is empty, while the compact prompt is durable.
        self.configure("shadow")
        second_response = self.response()
        second_response["transition_applied"] = {
            **second_response["transition_applied"],
            "transition_id": "transition-two",
            "resolved_at": "2026-10-11T18:02:00+02:00",
        }
        second = self.materialize(second_response)
        second_runtime = main.prompt_archive_runtime.for_project("9000")
        second_read = second_runtime.lookup_archive(
            sprint_id="sprint-runtime-one",
            archive_id=second["prompt_archive"]["archive_id"],
        )

        self.assertEqual(
            second_read.manifest["source"]["predecessor_archive_id"],
            first_archive_id,
        )
        self.assertEqual(
            second_read.manifest["roots"]["code_patches"],
            first_read.manifest["roots"]["code_patches"],
        )

    def test_restart_handoff_uses_prior_agents_verified_descriptor(self) -> None:
        self.configure("shadow")
        first = self.materialize()
        first_archive_id = first["prompt_archive"]["archive_id"]

        # Model a clean restart followed by a role hand-off. The new agent has
        # no .prompt yet, but the committed assignment roster names the prior
        # agent whose bounded descriptor can be reopened and verified.
        self.configure("shadow")
        second_response = self.response()
        second_response["agent"] = {
            "id": "agent-two",
            "name": "Agent Two",
            "phone": "4103",
        }
        second_response["assignment"] = {
            "assignments": [
                {"assignment_id": "assignment-one", "agent_phone": "4102"},
                {"assignment_id": "assignment-two", "agent_phone": "4103"},
            ]
        }
        second_response["transition_applied"] = {
            **second_response["transition_applied"],
            "transition_id": "transition-handoff",
            "resolved_at": "2026-10-11T18:03:00+02:00",
        }
        second = self.materialize(
            second_response,
            request_agent_phone="4103",
        )
        runtime = main.prompt_archive_runtime.for_project("9000")
        selected = runtime.lookup_archive(
            sprint_id="sprint-runtime-one",
            archive_id=second["prompt_archive"]["archive_id"],
        )
        self.assertEqual(
            selected.manifest["source"]["predecessor_archive_id"],
            first_archive_id,
        )

    def test_equal_time_predecessor_fork_fails_shadow_closed(self) -> None:
        self.configure("shadow")
        response = self.response()
        response["assignment"] = {
            "assignments": [
                {"assignment_id": "assignment-a", "agent_phone": "4102"},
                {"assignment_id": "assignment-b", "agent_phone": "4103"},
                {"assignment_id": "assignment-c", "agent_phone": "4104"},
            ]
        }
        descriptors = {
            "4102": {
                "archive_id": "par1-" + "a" * 64,
                "correlation_id": "parresp1-" + "1" * 64,
                "created_at": "2026-10-11T18:00:00+02:00",
            },
            "4103": {
                "archive_id": "par1-" + "b" * 64,
                "correlation_id": "parresp1-" + "2" * 64,
                "created_at": "2026-10-11T18:00:00+02:00",
            },
        }

        def descriptor_for(path, **_kwargs):
            return next(
                (
                    value
                    for phone, value in descriptors.items()
                    if f"_{phone}-latest.prompt" in path.name
                ),
                None,
            )

        runtime = main.prompt_archive_runtime.for_project("9000")
        with (
            patch.object(
                main,
                "verified_prompt_archive_descriptor",
                side_effect=descriptor_for,
            ),
            patch.object(runtime, "publish_shadow") as publish,
        ):
            compact = self.materialize(response, request_agent_phone="4104")

        publish.assert_not_called()
        self.assertNotIn("prompt_archive", compact)
        self.assertTrue(Path(compact["response_file_path"]).is_file())
        self.assertTrue(Path(compact["latest_agent_prompt_file_path"]).is_file())

    def test_legacy_runtime_uses_durable_current_sprint_identity(self) -> None:
        assignment = {"revision": 7, "status": "active"}
        audit: dict = {}
        append_execution_checkpoint(
            audit,
            {
                "project_id": "9000",
                "sprint_id": "sprint-runtime-one",
                "execution": assignment,
            },
            recorded_at="2026-10-11T18:04:00+02:00",
        )
        assignment["execution_audit"] = audit
        runtime = {
            "project_id": "9000",
            "project_phone": "9000",
            "context_key": "github.com/acme/archive-fixture",
            "identity_reused": False,
            "assignment": assignment,
            "task": {
                "id": "queue-occurrence-one",
                "queued_at": "2026-10-11T18:04:00+02:00",
                "metadata": {},
            },
        }
        enriched = asyncio.run(
            main.attach_prompt_archive_sprint_identity(runtime)
        )
        occurrence = main.prompt_archive_occurrence_from_sequential_runtime(
            enriched
        )
        self.assertIsNotNone(occurrence)
        assert occurrence is not None
        self.assertEqual(occurrence.sprint_id, "sprint-runtime-one")
        self.assertEqual(occurrence.state_revision, 7)
        self.assertNotIn("sprint_id", runtime)

    def test_legacy_is_default_and_sprintless_shadow_is_skipped(self) -> None:
        default = {
            "NGINX_QA_PROMPT_ROOT": str(self.prompt_root),
            "NGINX_QA_PROTECTED_ROOTS": "[]",
        }
        registry = main.configure_prompt_archive_runtime(default)
        self.assertEqual(registry.config.mode, "legacy")
        self.assertIsNone(registry.config.root)

        self.configure("shadow")
        response = self.response()
        response.pop("sprint_id")
        response["active_task"]["metadata"].pop("sprint_id")
        compact = self.materialize(response)
        self.assertNotIn("prompt_archive", compact)
        self.assertEqual(list(self.archive_root.rglob("*.json")), [])

    def test_occurrence_requires_durable_identity_facts(self) -> None:
        result = {
            "project_id": "9000",
            "newly_assigned": True,
            "assignment": {
                "sprint_id": "sprint-runtime-one",
                "current_assignment_id": "assignment-one",
                "assignments": [
                    {
                        "assignment_id": "assignment-one",
                        "started_at": "2026-10-11T18:02:00+02:00",
                    }
                ],
                "active_task": {
                    "metadata": {"source_commit": "1" * 40}
                },
                "pending_transition": {
                    "transition_id": "pending-must-not-be-used",
                    "proposed_at": "2026-10-11T18:01:00+02:00",
                },
                "last_transition": {
                    "transition_id": "stale-must-not-be-used",
                    "resolved_at": "2026-10-11T18:00:00+02:00",
                },
            },
        }
        occurrence = main.prompt_archive_occurrence_from_sequential_result(
            result,
            response_kind="identity-response",
        )
        self.assertIsNotNone(occurrence)
        assert occurrence is not None
        self.assertEqual(occurrence.occurrence_kind, "identity-state")
        self.assertEqual(occurrence.occurrence_id, "assignment-one")

        result["newly_assigned"] = False
        self.assertIsNone(
            main.prompt_archive_occurrence_from_sequential_result(
                result,
                response_kind="identity-response",
            )
        )

    def test_simple_terminal_completion_is_a_durable_shadow_occurrence(self) -> None:
        completed_at = "2026-10-11T18:03:00+02:00"
        result = {
            "project_id": "9000",
            "sprint_id": "sprint-runtime-one",
            "all_completed": True,
            "completed_assignment": {
                "assignment_id": "assignment-terminal",
                "status": "completed",
                "completed_at": completed_at,
            },
            "assignment": {
                "sprint_id": "sprint-runtime-one",
                "active_task": {
                    "metadata": {
                        "assignment_id": "assignment-terminal",
                        "source_commit": "1" * 40,
                    }
                },
            },
        }

        occurrence = main.prompt_archive_occurrence_from_sequential_result(
            result,
            response_kind="transition-response",
        )

        self.assertIsNotNone(occurrence)
        assert occurrence is not None
        self.assertEqual(occurrence.occurrence_kind, "identity-state")
        self.assertEqual(occurrence.occurrence_id, "assignment-terminal")
        self.assertIsNone(occurrence.transition_id)
        self.assertEqual(occurrence.created_at, completed_at)
        self.assertEqual(
            occurrence.source["assignment_id"], "assignment-terminal"
        )

    def test_invalid_shadow_occurrence_never_breaks_legacy_response(self) -> None:
        self.configure("shadow")
        response = self.response()
        invalid = self.occurrence(response)
        assert invalid is not None
        invalid = replace(
            invalid,
            created_at="not-a-time",
            verified_at="not-a-time",
        )

        compact = self.materialize(response, archive_occurrence=invalid)

        self.assertNotIn("prompt_archive", compact)
        self.assertTrue(Path(compact["response_file_path"]).is_file())

    def test_prompt_template_change_cannot_overlap_active_archive_root(self) -> None:
        self.configure("shadow")
        settings_path = self.root / "runtime" / "sequential-prompt-settings.json"
        settings_path.parent.mkdir()
        with patch.object(
            main,
            "sequential_prompt_settings_path",
            return_value=settings_path,
        ):
            with self.assertRaises(main.HTTPException) as caught:
                main.write_sequential_prompt_settings_file(
                    str(self.archive_root / "{repository}"),
                    str(
                        self.prompt_root
                        / "agents"
                        / "{repository}_{agent_phone}-latest.prompt"
                    ),
                )

        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(
            caught.exception.detail["error"],
            "PROMPT_ARCHIVE_REQUEST_INVALID",
        )
        self.assertFalse(settings_path.exists())

    def test_startup_preflight_protects_persisted_prompt_templates(self) -> None:
        settings_path = self.root / "runtime" / "sequential-prompt-settings.json"
        settings_path.parent.mkdir()
        settings_path.write_text(
            json.dumps(
                {
                    "directory_template": str(
                        self.archive_root / "{repository}"
                    ),
                    "agent_latest_file_template": str(
                        self.prompt_root
                        / "agents"
                        / "{repository}_{agent_phone}-latest.prompt"
                    ),
                }
            ),
            encoding="utf-8",
        )

        with patch.object(
            main,
            "sequential_prompt_settings_path",
            return_value=settings_path,
        ):
            with self.assertRaises(PromptArchiveStorageError) as caught:
                self.configure("shadow")

        self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REQUEST_INVALID")

    def test_compact_task_and_result_controls_do_not_hydrate(self) -> None:
        response = self.response()
        response["active_task"]["metadata"].update(
            {
                "agent": {"id": "future-agent-contract"},
                "tasks": [{"id": "future-task-contract"}],
                "workflow": {"result_endpoint": "/future/result"},
                "future_control": {"required": True},
            }
        )
        response.update(
            {
                "execution_authorized": True,
                "requires_additional_confirmation": False,
                "identity_request_required": False,
                "current_assignment_id": "assignment-one",
                "next_whoami_endpoint": "/whoami",
                "completed_assignment": {
                    "assignment_id": "assignment-one",
                    "status": "completed",
                    "outcome": "DONE",
                    "feedback": "accepted",
                },
            }
        )
        with patch(
            "nginx_qa.prompt_archive_incremental.PromptArchiveIncremental.hydrate_archive_bytes",
            side_effect=AssertionError("compact task path must not hydrate"),
        ):
            compact = main.compact_sequential_agent_prompt_response(response)
        for field in (
            "execution_authorized",
            "requires_additional_confirmation",
            "identity_request_required",
            "current_assignment_id",
            "next_whoami_endpoint",
            "completed_assignment",
        ):
            self.assertEqual(compact[field], response[field])
        self.assertEqual(compact["active_task"], response["active_task"])
        self.assertNotIn("project_state", compact)

    def test_psc_a11_identity_task_prompt_mode_matrix_is_hot_path_only(self) -> None:
        for mode in ("legacy", "shadow", "authoritative"):
            with self.subTest(mode=mode):
                self.prompt_root = self.root / f"prompt-{mode}"
                self.archive_root = self.root / f"archive-{mode}"
                self.prompt_root.mkdir()
                self.configure(mode)
                response = self.response()
                response["active_task"] = {
                    "id": f"task-{mode}",
                    "message": "Full task body remains executable.",
                    "metadata": {
                        "assignment_id": f"assignment-{mode}",
                        "sprint_id": "sprint-runtime-one",
                        "allowed_outcomes": ["DONE", "NEED_DECISION", "STOP"],
                        "result_endpoint": "/api/v1/projects/9000/agents/4102/whoami",
                        "future_nested_control": {
                            "version": 7,
                            "steps": [
                                {"kind": "verify", "required": True},
                                {"kind": "submit", "formats": ["json", "text"]},
                            ],
                        },
                    },
                    "future_task_field": {"must_survive": [1, {"deep": "value"}]},
                }
                response["project_state"] = {
                    "history_with_patches": {"text": "must stay off hot path"},
                    "activity_with_patches": [{"event": "heavy"}],
                    "code_patches": [{"patch": "heavy"}],
                }
                with (
                    patch.object(
                        main,
                        "project_state_json",
                        side_effect=AssertionError("full project state built"),
                    ) as state_builder,
                    patch.object(
                        main,
                        "project_history_with_patches_context",
                        side_effect=AssertionError("project history built"),
                    ) as project_history_builder,
                    patch.object(
                        main,
                        "history_with_patches_context",
                        side_effect=AssertionError("history with patches built"),
                    ) as history_builder,
                    patch.object(
                        main,
                        "read_agent_work_history_file",
                        side_effect=AssertionError("full agent history read"),
                    ) as history_reader,
                    patch(
                        "nginx_qa.prompt_archive_incremental."
                        "PromptArchiveIncremental.hydrate_archive_bytes",
                        side_effect=AssertionError("archive blob hydrated"),
                    ) as hydrator,
                    patch(
                        "nginx_qa.prompt_archive_runtime."
                        "PromptArchiveRuntime.hydrate_content",
                        side_effect=AssertionError("archive content hydrated"),
                    ) as content_hydrator,
                ):
                    compact = main.materialize_sequential_graph_response(
                        response=response,
                        directory_template=str(self.prompt_root / "{repository}"),
                        agent_latest_file_template=str(
                            self.prompt_root
                            / "agents"
                            / "{repository}_{agent_phone}-latest.prompt"
                        ),
                        response_kind="identity-response",
                        request_agent_phone="4102",
                        archive_occurrence=None,
                    )

                state_builder.assert_not_called()
                project_history_builder.assert_not_called()
                history_builder.assert_not_called()
                history_reader.assert_not_called()
                hydrator.assert_not_called()
                content_hydrator.assert_not_called()
                self.assertEqual(compact["active_task"], response["active_task"])
                self.assertNotIn("project_state", compact)
                self.assertNotIn("response_storage_errors", compact)
                prompt = json.loads(
                    Path(compact["latest_agent_prompt_file_path"]).read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(prompt["active_task"], response["active_task"])
                self.assertNotIn("project_state", prompt)

        # The explicitly gated native writer is a separate path from ordinary
        # read-only identity/task materialization.  Exercise it once with a
        # durable identity occurrence and keep the same no-builder/no-hydrator
        # invariant.
        self.prompt_root = self.root / "prompt-authoritative-writer"
        self.archive_root = self.root / "archive-authoritative-writer"
        self.prompt_root.mkdir()
        self.configure("authoritative")
        response = self.response()
        response["active_task"]["metadata"]["future_nested_control"] = {
            "version": 8,
            "steps": [{"kind": "submit", "required": True}],
        }
        occurrence_id = "assignment-authoritative-writer"
        identity_occurrence = replace(
            self.occurrence(response),
            response_id=response_id_v1(
                "9000",
                "sprint-runtime-one",
                "identity-response",
                "identity-state",
                occurrence_id,
            ),
            response_kind="identity-response",
            occurrence_kind="identity-state",
            occurrence_id=occurrence_id,
            transition_id=None,
            source={
                "source_kind": "native_runtime",
                "assignment_id": occurrence_id,
            },
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "legacy_local_principal",
                "principal_id": "9000",
                "binding_id": None,
                "scope_source": "legacy_project_context",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": occurrence_id,
            },
        )
        with (
            patch.object(
                main,
                "project_state_json",
                side_effect=AssertionError("full project state built"),
            ) as state_builder,
            patch.object(
                main,
                "project_history_with_patches_context",
                side_effect=AssertionError("project history built"),
            ) as project_history_builder,
            patch.object(
                main,
                "history_with_patches_context",
                side_effect=AssertionError("history with patches built"),
            ) as history_builder,
            patch.object(
                main,
                "read_agent_work_history_file",
                side_effect=AssertionError("full agent history read"),
            ) as history_reader,
            patch(
                "nginx_qa.prompt_archive_incremental."
                "PromptArchiveIncremental.hydrate_archive_bytes",
                side_effect=AssertionError("archive blob hydrated"),
            ) as hydrator,
            patch(
                "nginx_qa.prompt_archive_runtime."
                "PromptArchiveRuntime.hydrate_content",
                side_effect=AssertionError("archive content hydrated"),
            ) as content_hydrator,
        ):
            compact = main.materialize_sequential_graph_response(
                response=response,
                directory_template=str(self.prompt_root / "{repository}"),
                agent_latest_file_template=str(
                    self.prompt_root
                    / "agents"
                    / "{repository}_{agent_phone}-latest.prompt"
                ),
                response_kind="identity-response",
                request_agent_phone="4102",
                archive_occurrence=identity_occurrence,
            )
        state_builder.assert_not_called()
        project_history_builder.assert_not_called()
        history_builder.assert_not_called()
        history_reader.assert_not_called()
        hydrator.assert_not_called()
        content_hydrator.assert_not_called()
        self.assertEqual(compact["active_task"], response["active_task"])
        self.assertNotIn("project_state", compact)


class PromptArchiveManagedHotPathTests(unittest.IsolatedAsyncioTestCase):
    async def test_storage_owner_derives_source_metadata_from_durable_occurrence(
        self,
    ) -> None:
        occurrence = ArchiveOccurrence(
            project_id="9000",
            sprint_id="sprint-production-seam",
            response_id=response_id_v1(
                "9000",
                "sprint-production-seam",
                "transition-response",
                "transition",
                "transition-production-seam",
            ),
            response_kind="transition-response",
            occurrence_kind="transition",
            occurrence_id="transition-production-seam",
            transition_id="transition-production-seam",
            source={"source_kind": "native_runtime"},
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "legacy_local_principal",
                "principal_id": "9000",
                "binding_id": None,
                "scope_source": "legacy_project_context",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": "transition-production-seam",
            },
            created_at="2026-10-20T16:00:00+02:00",
            verified_at="2026-10-20T16:00:00+02:00",
        )
        registry = Mock()
        registry.config.mode = "authoritative"
        with (
            patch.object(main, "prompt_archive_runtime", registry),
            patch.object(
                main,
                "read_sequential_prompt_settings_file",
                return_value={
                    "directory_template": r"C:\prompt\{repository}",
                    "agent_latest_file_template": (
                        r"C:\prompt\agents\{repository}_{agent_phone}-latest.prompt"
                    ),
                },
            ),
            patch.object(
                main,
                "materialize_sequential_graph_response",
                return_value={"answer": "stored"},
            ) as materialize,
        ):
            stored = await main.attach_sequential_graph_response_storage(
                {"answer": "committed"},
                response_kind="transition-response",
                archive_occurrence=occurrence,
            )

        self.assertEqual(stored, {"answer": "stored"})
        supplied = materialize.call_args.kwargs["archive_source_metadata"]
        self.assertEqual(
            supplied,
            {
                "source_type": "api",
                "conversation_id": "sprint-production-seam",
                "message_id": "transition-production-seam",
                "title": "transition-response:transition",
                "observed_at": "2026-10-20T16:00:00+02:00",
            },
        )

    async def test_source_metadata_override_is_frozen_before_first_await(self) -> None:
        occurrence = ArchiveOccurrence(
            project_id="9000",
            sprint_id="sprint-freeze-seam",
            response_id=response_id_v1(
                "9000",
                "sprint-freeze-seam",
                "identity-response",
                "identity-state",
                "task-freeze-seam",
            ),
            response_kind="identity-response",
            occurrence_kind="identity-state",
            occurrence_id="task-freeze-seam",
            transition_id=None,
            source={"source_kind": "native_runtime"},
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "legacy_local_principal",
                "principal_id": "9000",
                "binding_id": None,
                "scope_source": "legacy_project_context",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": "task-freeze-seam",
            },
            created_at="2026-10-20T16:00:00+02:00",
            verified_at="2026-10-20T16:00:00+02:00",
        )
        raw_metadata = {"source_type": "api", "title": "before-await"}
        registry = Mock()
        registry.config.mode = "authoritative"
        await main.sequential_prompt_storage_lock.acquire()
        try:
            with (
                patch.object(main, "prompt_archive_runtime", registry),
                patch.object(
                    main,
                    "read_sequential_prompt_settings_file",
                    return_value={
                        "directory_template": r"C:\prompt\{repository}",
                        "agent_latest_file_template": (
                            r"C:\prompt\agents\{repository}_{agent_phone}-latest.prompt"
                        ),
                    },
                ),
                patch.object(
                    main,
                    "materialize_sequential_graph_response",
                    return_value={"answer": "stored"},
                ) as materialize,
            ):
                pending = asyncio.create_task(
                    main.attach_sequential_graph_response_storage(
                        {"answer": "committed"},
                        response_kind="identity-response",
                        archive_occurrence=occurrence,
                        archive_source_metadata=raw_metadata,
                    )
                )
                await asyncio.sleep(0)
                raw_metadata["title"] = "mutated-after-await"
                main.sequential_prompt_storage_lock.release()
                stored = await pending
        finally:
            if main.sequential_prompt_storage_lock.locked():
                main.sequential_prompt_storage_lock.release()

        self.assertEqual(stored, {"answer": "stored"})
        self.assertEqual(
            materialize.call_args.kwargs["archive_source_metadata"]["title"],
            "before-await",
        )

    def test_source_screen_uses_only_supported_and_referenced_credentials(
        self,
    ) -> None:
        fixed_secret = "fixed-gemini-secret"
        referenced_secret = "opaque-reference-secret"
        unrelated_secret = "opaquePgValue"
        pending_token = "pending-capability-secret"
        registered_token = "A" * 48
        environment = {
            "GEMINI_API_KEY": fixed_secret,
            "PGPASSWORD": unrelated_secret,
            "GIT_CREDENTIAL_BUNDLE": json.dumps(
                {"GIT_PASSWORD": referenced_secret}
            ),
        }
        screen = main.prompt_archive_source_metadata_screen_snapshot(
            environment,
            credential_references=("GIT_CREDENTIAL_BUNDLE",),
            pending_access_tokens=(pending_token,),
            producer_token_sha256=(
                hashlib.sha256(registered_token.encode("ascii")).hexdigest(),
            ),
        )

        projected = project_source_metadata(
            {
                "source_type": "api",
                "sender_label": fixed_secret,
                "title": referenced_secret,
                "conversation_id": pending_token,
                "thread_id": registered_token,
                "message_id": unrelated_secret,
            },
            screen=screen,
        ).document

        self.assertEqual(projected["sender_label"], "[REDACTED]")
        self.assertEqual(projected["title"], "[REDACTED]")
        self.assertEqual(projected["conversation_id"], "[REDACTED]")
        self.assertEqual(projected["thread_id"], "[REDACTED]")
        self.assertRegex(projected["message_id"], r"^sha256:[0-9a-f]{64}$")

        normalized = main.prompt_archive_source_metadata_screen_snapshot(
            {"GEMINI_API_KEY": "  CanaRy42  "},
            credential_references=(),
            pending_access_tokens=(),
            producer_token_sha256=(),
        )
        normalized_projection = project_source_metadata(
            {"source_type": "api", "title": "CanaRy42"},
            screen=normalized,
        ).document
        self.assertEqual(normalized_projection["title"], "[REDACTED]")

    def test_credential_reference_discovery_is_structural_and_fail_closed(
        self,
    ) -> None:
        project_references = main._prompt_archive_project_credential_references(
            {
                main.PROJECTS_KEY: {
                    "context-one": {
                        "credential_reference": "env:PROJECT_CREDENTIAL",
                        "task_payload": {"secret_ref": "env:UNTRUSTED_TASK_VALUE"},
                        "repositories": {
                            "origin": {
                                "credential_reference": "env:REPOSITORY_CREDENTIAL",
                                "nested": {"secret_ref": "env:UNTRUSTED_NESTED"},
                            }
                        },
                    }
                }
            }
        )
        managed_references = main._prompt_archive_managed_credential_references(
            {
                "repository": {"credential_reference": "env:MANAGED_REPOSITORY"},
                "task": {"secret_ref": "env:UNTRUSTED_MANAGED_TASK"},
                "processes": [
                    {
                        "environment_redacted": {
                            "TOKEN": {"secret_ref": "env:MANAGED_PROCESS_TOKEN"},
                            "NOT_EXACT": {
                                "secret_ref": "env:UNTRUSTED_PROCESS_VALUE",
                                "extra": True,
                            },
                        }
                    }
                ],
            }
        )

        self.assertEqual(
            project_references,
            ("PROJECT_CREDENTIAL", "REPOSITORY_CREDENTIAL"),
        )
        self.assertEqual(
            managed_references,
            ("MANAGED_PROCESS_TOKEN", "MANAGED_REPOSITORY"),
        )
        with self.assertRaises(PromptArchiveStorageError) as raised:
            main.prompt_archive_source_metadata_screen_snapshot(
                {},
                credential_references=project_references + managed_references,
                pending_access_tokens=(),
                producer_token_sha256=(),
            )
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")

    def test_configured_credential_inventory_is_current_project_scoped(self) -> None:
        config = {
            main.PROJECTS_KEY: {
                "context-current": {
                    "project_phone": "9000",
                    "credential_reference": "env:CURRENT_PROJECT_CREDENTIAL",
                },
                "context-other": {
                    "project_phone": "9001",
                    "credential_reference": "env:OTHER_PROJECT_CREDENTIAL",
                },
            }
        }
        runtime = SimpleNamespace(store=Mock())
        runtime.store.active_runtime_state.return_value = {
            "status": "active",
            "repository": {
                "credential_reference": "env:ACTIVE_MANAGED_CREDENTIAL"
            },
        }
        with (
            patch.object(
                main,
                "git_config_file_lock",
                return_value=contextlib.nullcontext(),
            ),
            patch.object(main, "read_git_config_file", return_value=config),
            patch.object(main, "managed_continuity_runtime", runtime),
        ):
            references = main._prompt_archive_configured_credential_references(
                "9000"
            )

        self.assertEqual(
            references,
            ("ACTIVE_MANAGED_CREDENTIAL", "CURRENT_PROJECT_CREDENTIAL"),
        )
        runtime.store.active_runtime_state.assert_called_once_with("9000")

    def test_managed_credential_inventory_does_not_depend_on_registry_entry(
        self,
    ) -> None:
        runtime = SimpleNamespace(store=Mock())
        runtime.store.active_runtime_state.return_value = {
            "status": "active",
            "repository": {
                "credential_reference": "env:ACTIVE_MANAGED_CREDENTIAL"
            },
        }
        with (
            patch.object(
                main,
                "git_config_file_lock",
                return_value=contextlib.nullcontext(),
            ),
            patch.object(
                main,
                "read_git_config_file",
                return_value={main.PROJECTS_KEY: {}},
            ),
            patch.object(main, "managed_continuity_runtime", runtime),
        ):
            references = main._prompt_archive_configured_credential_references(
                "9000"
            )

        self.assertEqual(references, ("ACTIVE_MANAGED_CREDENTIAL",))
        runtime.store.active_runtime_state.assert_called_once_with("9000")

    def test_default_screen_traverses_supported_secret_authorities(self) -> None:
        fixed = "fixed-secret-value"
        role = "role-secret-value"
        referenced = "referenced-secret-value"
        pending = "pending-secret-value"
        producer = "P" * 43
        environment = {
            "GEMINI_API_KEY": fixed,
            "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2974": role,
            "CURRENT_PROJECT_CREDENTIAL": referenced,
            main.INBOUND_PRODUCER_REGISTRY_ENV: r"C:\protected\producers.json",
        }
        with (
            patch.object(
                main,
                "_prompt_archive_configured_credential_references",
                return_value=("CURRENT_PROJECT_CREDENTIAL",),
            ) as references,
            patch.object(
                main,
                "_prompt_archive_pending_access_tokens",
                return_value=(pending,),
            ) as pending_tokens,
            patch.object(
                main,
                "load_producer_registry",
                return_value=(
                    SimpleNamespace(
                        token_sha256=hashlib.sha256(
                            producer.encode("ascii")
                        ).hexdigest()
                    ),
                ),
            ) as producer_registry,
        ):
            screen = main.prompt_archive_source_metadata_screen_snapshot(
                environment,
                project_id="9000",
            )

        references.assert_called_once_with("9000")
        pending_tokens.assert_called_once_with()
        producer_registry.assert_called_once_with(
            environment[main.INBOUND_PRODUCER_REGISTRY_ENV],
            require_protected_acl=True,
        )
        values = (fixed, role, referenced, pending, producer)
        for index, secret in enumerate(values):
            with self.subTest(authority=index):
                projected = project_source_metadata(
                    {"source_type": "api", "title": secret},
                    screen=screen,
                ).document
                self.assertEqual(projected["title"], "[REDACTED]")

    async def test_storage_error_fallback_never_exposes_oserror_path_or_canary(
        self,
    ) -> None:
        raw_canary = "RAW-MATERIALIZER-CANARY-DO-NOT-RETURN"
        raw_path = r"C:\operator-private\latest-response.json"
        registry = Mock()
        registry.config.mode = "legacy"
        with (
            patch.object(main, "prompt_archive_runtime", registry),
            patch.object(
                main,
                "read_sequential_prompt_settings_file",
                return_value={
                    "directory_template": r"C:\prompt\{repository}",
                    "agent_latest_file_template": (
                        r"C:\prompt\agents\{repository}_{agent_phone}-latest.prompt"
                    ),
                },
            ),
            patch.object(
                main,
                "materialize_sequential_graph_response",
                side_effect=OSError(f"{raw_canary}: {raw_path}"),
            ),
        ):
            compact = await main.attach_sequential_graph_response_storage(
                {
                    "answer": "usable fallback",
                    "active_task": {
                        "id": "task-storage-fallback",
                        "message": "complete task",
                    },
                },
                response_kind="identity-response",
                request_agent_phone="4102",
            )

        self.assertEqual(
            compact["response_storage_error"],
            "SEQUENTIAL_RESPONSE_STORAGE_UNAVAILABLE",
        )
        serialized = json.dumps(compact, ensure_ascii=False)
        self.assertNotIn(raw_canary, serialized)
        self.assertNotIn(raw_path, serialized)

    async def test_authoritative_publication_error_uses_stable_http_envelope(
        self,
    ) -> None:
        occurrence = ArchiveOccurrence(
            project_id="9000",
            sprint_id="sprint-http-error",
            response_id=response_id_v1(
                "9000",
                "sprint-http-error",
                "identity-response",
                "identity-state",
                "task-http-error",
            ),
            response_kind="identity-response",
            occurrence_kind="identity-state",
            occurrence_id="task-http-error",
            transition_id=None,
            source={"source_kind": "native_runtime"},
            authorization={},
            created_at="2026-10-20T16:00:00+02:00",
            verified_at="2026-10-20T16:00:00+02:00",
        )
        registry = Mock()
        registry.config.mode = "authoritative"
        with (
            patch.object(main, "prompt_archive_runtime", registry),
            patch.object(
                main,
                "read_sequential_prompt_settings_file",
                return_value={
                    "directory_template": r"C:\prompt\{repository}",
                    "agent_latest_file_template": (
                        r"C:\prompt\agents\{repository}_{agent_phone}-latest.prompt"
                    ),
                },
            ),
            patch.object(
                main,
                "materialize_sequential_graph_response",
                side_effect=PromptArchiveStorageError(
                    "PROMPT_ARCHIVE_UNAVAILABLE"
                ),
            ),
            self.assertRaises(main.HTTPException) as raised,
        ):
            await main.attach_sequential_graph_response_storage(
                {"answer": "committed"},
                response_kind="identity-response",
                archive_occurrence=occurrence,
            )

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail,
            {
                "schema_version": 1,
                "error": "PROMPT_ARCHIVE_UNAVAILABLE",
                "message": "Prompt archive is unavailable for this request.",
                "correlation_id": occurrence.response_id,
                "retryable": True,
            },
        )
        self.assertEqual(
            raised.exception.headers["X-Correlation-ID"],
            occurrence.response_id,
        )

    def test_publication_http_error_never_echoes_invalid_response_id(self) -> None:
        canary = "CANARY\r\nX-Injected: yes"
        occurrence = ArchiveOccurrence(
            project_id="9000",
            sprint_id="sprint-invalid-correlation",
            response_id=canary,
            response_kind="identity-response",
            occurrence_kind="identity-state",
            occurrence_id="task-invalid-correlation",
            transition_id=None,
            source={"source_kind": "native_runtime"},
            authorization={},
            created_at="2026-10-20T16:00:00+02:00",
            verified_at="2026-10-20T16:00:00+02:00",
        )

        mapped = main._prompt_archive_publication_http_exception(
            PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID"),
            occurrence,
        )

        correlation_id = mapped.detail["correlation_id"]
        self.assertEqual(mapped.status_code, 400)
        self.assertRegex(
            correlation_id,
            r"^prompt-archive-request-[0-9a-f]{32}$",
        )
        self.assertEqual(mapped.headers["X-Correlation-ID"], correlation_id)
        serialized = json.dumps(
            {"detail": mapped.detail, "headers": mapped.headers},
            ensure_ascii=False,
        )
        self.assertNotIn(canary, serialized)
        self.assertNotIn("X-Injected", serialized)

    def test_history_tail_reader_bounds_bytes_records_and_identity_call_sites(
        self,
    ) -> None:
        context_key = "github.com/acme/archive-fixture"
        agent = {"id": "agent-hot-path", "phone": "4102", "name": "Agent"}
        records = [
            {
                "id": f"history-{index}",
                "timestamp": f"2026-10-11T18:00:0{index}+00:00",
                "event": f"tail-{index}",
                "metadata": {
                    "git_context_key": context_key,
                    "to_agent_id": "agent-hot-path",
                },
            }
            for index in range(6)
        ]
        payload = (
            b"x" * 8192
            + b"\n"
            + b"\n".join(
                json.dumps(record, separators=(",", ":")).encode("utf-8")
                for record in records
            )
            + b"\n"
        )

        class RecordingBytesIO(io.BytesIO):
            def __init__(self, value: bytes) -> None:
                super().__init__(value)
                self.read_sizes: list[int] = []

            def read(self, size: int = -1) -> bytes:
                self.read_sizes.append(size)
                return super().read(size)

        class BoundedHistoryPath:
            def __init__(self, value: bytes) -> None:
                self.value = value
                self.handles: list[RecordingBytesIO] = []

            def exists(self) -> bool:
                return True

            def stat(self) -> SimpleNamespace:
                return SimpleNamespace(st_size=len(self.value))

            def open(self, mode: str) -> RecordingBytesIO:
                self.assert_binary_mode(mode)
                handle = RecordingBytesIO(self.value)
                self.handles.append(handle)
                return handle

            @staticmethod
            def assert_binary_mode(mode: str) -> None:
                if mode != "rb":
                    raise AssertionError(f"unbounded history mode used: {mode}")

        bounded_path = BoundedHistoryPath(payload)
        with patch.object(main, "history_path", bounded_path):
            selected = main.read_agent_work_history_tail_file(
                agent,
                context_key,
                max_records=3,
                max_scan_bytes=4096,
            )

        self.assertEqual(
            [record["event"] for record in selected],
            ["tail-3", "tail-4", "tail-5"],
        )
        self.assertEqual(len(bounded_path.handles), 1)
        self.assertEqual(bounded_path.handles[0].read_sizes, [4096])
        for identity_callable in (
            main.identify_sequential_project_agent,
            main.sequential_runtime_agent_message_reminder,
            main.identify_project_agent,
        ):
            with self.subTest(identity_callable=identity_callable.__name__):
                self.assertNotIn(
                    "read_agent_work_history_file",
                    identity_callable.__code__.co_names,
                )
                self.assertIn(
                    "read_agent_work_history_tail_file",
                    identity_callable.__code__.co_names,
                )

    async def test_authoritative_result_uses_bounded_review_context_without_git_io(
        self,
    ) -> None:
        submitted_result = "RESULT-BEGIN\n" + ("R" * 8192) + "\nRESULT-END"
        submitted_feedback = "FEEDBACK-BEGIN\n" + ("F" * 2048) + "\nFEEDBACK-END"
        from_commit = "1" * 40
        git_commit = "2" * 40
        git_branch = "agent/hot-path-review-context"
        assignment = {
            "status": "active",
            "pending_transition": {
                "transition_id": "transition-hot-path",
                "status": "reviewing",
            },
            "assignments": [],
        }
        committed = {
            "project_id": "9000",
            "project_phone": "9000",
            "sprint_id": "sprint-hot-path",
            "context_key": "github.com/acme/archive-fixture",
            "project": {
                "project_name": "Archive Fixture",
                "git_address": "https://github.com/acme/archive-fixture.git",
                "git_context_key": "github.com/acme/archive-fixture",
            },
            "queue_context": {
                "git_address": "https://github.com/acme/archive-fixture.git",
                "git_context_key": "github.com/acme/archive-fixture",
            },
            "completed_agent": {
                "id": "agent-hot-path",
                "name": "Hot Path Agent",
                "phone": "4102",
                "git_branch": git_branch,
            },
            "completed_assignment": {
                "assignment_id": "assignment-hot-path",
                "kind": "graph_node",
                "status": "completed",
            },
            "assignment": assignment,
            "agent": None,
            "blocked": False,
        }
        captured_context: dict[str, object] = {}

        async def transaction(function, *args):
            if function is main.sequential_agent_assignment_transaction:
                return committed
            if function is main.attach_transition_review_context_transaction:
                captured_context.update(args[2])
                return assignment
            raise AssertionError(f"unexpected transaction: {function}")

        request = main.Request(
            {
                "type": "http",
                "method": "POST",
                "scheme": "http",
                "path": "/api/v1/projects/9000/agents/4102/whoami",
                "root_path": "",
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 50000),
                "server": ("127.0.0.1", 18032),
            }
        )
        registry = Mock()
        registry.config.mode = "authoritative"
        with (
            patch.object(
                main,
                "run_group_write_transaction",
                side_effect=transaction,
            ),
            patch.object(
                main,
                "remove_project_actor_queue_items",
                AsyncMock(return_value=[]),
            ),
            patch.object(main, "append_history", AsyncMock()),
            patch.object(
                main,
                "attach_prompt_archive_sprint_identity",
                AsyncMock(side_effect=lambda value: value),
            ),
            patch.object(
                main,
                "prompt_archive_occurrence_from_sequential_result",
                return_value=None,
            ),
            patch.object(
                main,
                "project_history_with_patches_context",
                AsyncMock(side_effect=AssertionError("cumulative history built")),
            ) as history_builder,
            patch.object(
                main,
                "resolve_git_patch",
                side_effect=AssertionError("Git patch resolved"),
            ) as patch_resolver,
            patch.object(
                main,
                "read_agent_work_history_file",
                side_effect=AssertionError("full history reader used"),
            ),
            patch.object(main, "prompt_archive_runtime", registry),
        ):
            response = await main.identify_sequential_project_agent(
                "9000",
                "4102",
                "done",
                True,
                request,
                identity_payload={
                    "assignment_id": "assignment-hot-path",
                    "status": "DONE",
                    "result": submitted_result,
                    "feedback": submitted_feedback,
                    "from_commit": from_commit,
                    "git_commit": git_commit,
                    "git_branch": git_branch,
                },
            )

        history_builder.assert_not_awaited()
        patch_resolver.assert_not_called()
        self.assertTrue(response["all_completed"])
        self.assertEqual(captured_context["schema_version"], 2)
        self.assertEqual(captured_context["text"], submitted_result)
        self.assertEqual(captured_context["patches"], [])
        self.assertEqual(captured_context["patch_count"], 0)
        self.assertEqual(
            captured_context["submission"],
            {
                "assignment_id": "assignment-hot-path",
                "outcome": "DONE",
                "from_commit": from_commit,
                "git_commit": git_commit,
                "git_branch": git_branch,
            },
        )
        evidence = captured_context["timeline"][0]
        self.assertEqual(evidence["result"], submitted_result)
        self.assertEqual(evidence["feedback"], submitted_feedback)

    def test_phone_whoami_openapi_declares_and_validates_compact_response(
        self,
    ) -> None:
        response_schema = main.app.openapi()["paths"][
            "/api/v1/projects/{project_id}/agents/{agent_phone}/whoami"
        ]["post"]["responses"]["200"]["content"]["application/json"][
            "schema"
        ]
        compact_schema = next(
            option
            for option in response_schema["oneOf"]
            if option.get("$id", "").endswith(
                "/prompt-archive-compact-response-v1.schema.json"
            )
        )
        representative = main.compact_sequential_agent_prompt_response(
            {
                "answer": "continue",
                "active_task": {
                    "id": "task-compact-contract",
                    "message": "This complete task body must remain executable.",
                    "metadata": {
                        "assignment_id": "assignment-compact-contract",
                        "allowed_outcomes": ["DONE", "STOP"],
                        "result_endpoint": (
                            "/api/v1/projects/9000/agents/4102/whoami"
                        ),
                    },
                },
                "assignment": {
                    "status": "active",
                    "current_assignment_id": "assignment-compact-contract",
                    "revision": 9,
                },
                "project_state": {
                    "history_with_patches": {"text": "must be omitted"}
                },
                "history_with_patches": {"text": "legacy top-level history"},
                "activity_with_patches": [{"event": "legacy"}],
                "code_patches": [{"patch": "legacy"}],
                "recent_activity": [{"event": "legacy"}],
                "execution_audit": [{"event": "legacy"}],
                "response_archive_available": False,
            }
        )

        self.assertEqual(
            compact_schema["$id"],
            "https://nginx-qa.local/schemas/"
            "prompt-archive-compact-response-v1.schema.json",
        )
        self.assertEqual(
            representative["active_task"]["metadata"]["result_endpoint"],
            "/api/v1/projects/9000/agents/4102/whoami",
        )
        self.assertNotIn("project_state", representative)
        for cumulative_key in (
            "history_with_patches",
            "activity_with_patches",
            "code_patches",
            "recent_activity",
            "execution_audit",
        ):
            self.assertNotIn(cumulative_key, representative)
        Draft202012Validator(compact_schema).validate(representative)
        Draft202012Validator(response_schema).validate(representative)

        self.assertEqual(
            compact_schema["properties"]["prompt_archive"]["$ref"],
            "#/$defs/promptArchiveDescriptorV1",
        )
        embedded_descriptor = compact_schema["$defs"][
            "promptArchiveDescriptorV1"
        ]
        self.assertFalse(embedded_descriptor["additionalProperties"])
        self.assertTrue(
            {
                "descriptor_url",
                "manifest_url",
                "content_url",
                "state_url",
                "route_context",
                "scope",
                "integrity",
                "created_at",
            }.issubset(embedded_descriptor["required"])
        )

        def nested_refs(value):
            if isinstance(value, dict):
                for key, nested in value.items():
                    if key == "$ref":
                        yield nested
                    else:
                        yield from nested_refs(nested)
            elif isinstance(value, list):
                for nested in value:
                    yield from nested_refs(nested)

        self.assertTrue(
            all(
                isinstance(reference, str) and reference.startswith("#")
                for reference in nested_refs(compact_schema)
            )
        )
        PromptArchiveContractTests.setUpClass()
        contract = PromptArchiveContractTests(
            "test_compact_occurrence_and_state_view_schemas_close_response_contract"
        )
        # The served response union is a complete offline bundle.  Deliberately
        # validate it without the contract registry (and therefore without any
        # pre-registered network IDs) to prove local OpenAPI resolution.
        response_validator = Draft202012Validator(response_schema)
        descriptor_data = descriptor_fixture()
        materialized_descriptor = main.prompt_archive_local_descriptor(
            PromptArchivePublication(
                project_id="9000",
                sprint_id="sprint-example-1",
                archive_id=descriptor_data["archive_id"],
                archive_format=descriptor_data["archive_format"],
                scope=descriptor_data["scope"],
                integrity=descriptor_data["integrity"],
                created_at=descriptor_data["created_at"],
                pointer_id="parptr1-" + "b" * 64,
                manifest_ref={},
                locator={},
            ),
            correlation_id=descriptor_data["correlation_id"],
        )
        self.assertEqual(materialized_descriptor, descriptor_data)
        contract.validator(
            "prompt-archive-descriptor-v1.schema.json"
        ).validate(materialized_descriptor)

        archived = copy.deepcopy(representative)
        archived["response_archive_available"] = True
        archived["prompt_archive"] = materialized_descriptor
        contract.validator(
            "prompt-archive-compact-response-v1.schema.json"
        ).validate(archived)
        response_validator.validate(archived)

        incomplete_descriptor = copy.deepcopy(archived)
        incomplete_descriptor["prompt_archive"].pop("state_url")
        with self.assertRaises(ValidationError):
            response_validator.validate(incomplete_descriptor)

        cumulative = copy.deepcopy(representative)
        cumulative["project_state"] = {"history_with_patches": {}}
        with self.assertRaises(ValidationError):
            response_validator.validate(cumulative)

        # Existing non-managed submission receipts retain the third union
        # branch; tightening the compact branch cannot capture or reject them.
        response_validator.validate({"accepted": True, "result_key": "legacy"})

    async def test_default_sequential_identity_does_not_build_full_state(self) -> None:
        response = {
            "answer": "identity",
            "communication": {"instructions": "continue"},
            "active_task": {
                "id": "task-hot-path",
                "message": "keep the complete executable task",
                "metadata": {
                    "assignment_id": "assignment-hot-path",
                    "allowed_outcomes": ["DONE", "STOP"],
                    "future_result_control": {"required": True},
                },
            },
        }

        async def capture_storage(value, **_kwargs):
            self.assertTrue(_kwargs["archive_occurrence_failed"])
            self.assertNotIn("project_state", value)
            self.assertEqual(value["active_task"], response["active_task"])
            return main.compact_sequential_agent_prompt_response(value)

        registry = Mock()
        registry.config.mode = "shadow"
        with (
            patch.object(
                main,
                "project_phone_for_actor_import",
                AsyncMock(return_value="9000"),
            ),
            patch.object(
                main,
                "dequeue_sequential_runtime_task",
                AsyncMock(return_value={"task": {"id": "queue-item"}}),
            ),
            patch.object(
                main,
                "sequential_runtime_response",
                return_value=response,
            ),
            patch.object(
                main,
                "sequential_runtime_agent_message_reminder",
                AsyncMock(return_value={"required": False, "message": ""}),
            ),
            patch.object(
                main,
                "attach_prompt_archive_sprint_identity",
                AsyncMock(side_effect=RuntimeError("opaque occurrence failure")),
            ),
            patch.object(
                main,
                "attach_sequential_graph_response_storage",
                AsyncMock(side_effect=capture_storage),
            ),
            patch.object(
                main,
                "project_state_json",
                AsyncMock(side_effect=AssertionError("full state builder called")),
            ),
            patch.object(
                main,
                "history_with_patches_context",
                side_effect=AssertionError("history builder called"),
            ),
            patch.object(main, "prompt_archive_runtime", registry),
            patch(
                "nginx_qa.prompt_archive_incremental.PromptArchiveIncremental.hydrate_archive_bytes",
                side_effect=AssertionError("identity path hydrated an archive"),
            ),
            patch(
                "nginx_qa.prompt_archive_state_view."
                "hydrate_prompt_archive_state_view",
                side_effect=AssertionError("identity path hydrated full state"),
            ),
        ):
            status_code, payload, _raw = await asgi_request(
                "/api/v1/agents/whoami/repository",
                b'{"git_address":"https://github.com/acme/archive-fixture.git"}',
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["active_task"], response["active_task"])
        self.assertNotIn("project_state", payload)
        self.assertIn("project_state_url", payload)

    async def test_project_sequential_active_task_does_not_build_full_state(self) -> None:
        response = {
            "answer": "active assignment",
            "active_task": {
                "id": "task-project-hot-path",
                "message": "complete task body",
                "metadata": {
                    "assignment_id": "assignment-project-hot-path",
                    "result_endpoint": "/result",
                    "future_control": {"mode": "closed"},
                },
            },
        }
        snapshot = {
            "assignment_mode": "sequential",
            "assignment": {"workflow": {"enabled": True}},
            "project_phone": "9000",
        }

        async def capture_storage(value, **_kwargs):
            self.assertNotIn("project_state", value)
            self.assertEqual(value["active_task"], response["active_task"])
            return main.compact_sequential_agent_prompt_response(value)

        with (
            patch.dict(os.environ, {"NGINX_QA_MANAGED_ROOT": ""}, clear=False),
            patch.object(
                main,
                "run_group_write_transaction",
                AsyncMock(return_value=snapshot),
            ),
            patch.object(
                main,
                "identify_sequential_project_agent",
                AsyncMock(return_value=response),
            ),
            patch.object(
                main,
                "attach_sequential_graph_response_storage",
                AsyncMock(side_effect=capture_storage),
            ),
            patch.object(
                main,
                "project_state_json",
                AsyncMock(side_effect=AssertionError("full state builder called")),
            ),
            patch.object(
                main,
                "history_with_patches_context",
                side_effect=AssertionError("history builder called"),
            ),
            patch(
                "nginx_qa.prompt_archive_incremental.PromptArchiveIncremental.hydrate_archive_bytes",
                side_effect=AssertionError("active-task path hydrated an archive"),
            ),
        ):
            status_code, payload, _raw = await asgi_request(
                "/api/v1/projects/9000/agents/4102/whoami",
                b'{"message":"who am I?"}',
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload["active_task"], response["active_task"])
        self.assertNotIn("project_state", payload)
        self.assertIn("project_state_url", payload)

    async def test_managed_phone_identity_is_archive_mode_invariant(self) -> None:
        for mode in ("legacy", "shadow", "authoritative"):
            with self.subTest(mode=mode):
                harness = ManagedContinuityTests()
                harness.setUp()
                try:
                    sentinel_registry = Mock()
                    sentinel_registry.config.mode = mode
                    sentinel_registry.for_project.side_effect = AssertionError(
                        "managed identity must not access archive storage"
                    )
                    with (
                        patch.dict(
                            os.environ,
                            {
                                "NGINX_QA_MANAGED_ROOT": "C:/managed-test-root",
                                "NGINX_QA_PROMPT_ARCHIVE_MODE": mode,
                            },
                            clear=False,
                        ),
                        patch.object(
                            main,
                            "prompt_archive_runtime",
                            sentinel_registry,
                        ),
                        patch.object(
                            main,
                            "managed_continuity_runtime",
                            harness.runtime,
                        ),
                        patch.object(
                            main,
                            "read_git_config",
                            AsyncMock(return_value={}),
                        ),
                        patch.object(
                            main,
                            "managed_continuity_runtime_for_requests",
                            return_value=harness.runtime,
                        ),
                        patch.object(
                            main,
                            "project_state_json",
                            AsyncMock(
                                side_effect=AssertionError(
                                    "full state builder called"
                                )
                            ),
                        ),
                        patch.object(
                            main,
                            "history_with_patches_context",
                            side_effect=AssertionError(
                                "history builder called"
                            ),
                        ),
                        patch.object(
                            main,
                            "materialize_sequential_graph_response",
                            side_effect=AssertionError(
                                "legacy response writer called"
                            ),
                        ),
                        patch(
                            "nginx_qa.prompt_archive_incremental."
                            "PromptArchiveIncremental.hydrate_archive_bytes",
                            side_effect=AssertionError(
                                "managed identity hydrated archive bytes"
                            ),
                        ),
                        patch(
                            "nginx_qa.prompt_archive_state_view."
                            "hydrate_prompt_archive_state_view",
                            side_effect=AssertionError(
                                "managed identity hydrated full state"
                            ),
                        ),
                    ):
                        status_code, payload, _raw = await asgi_request(
                            "/api/v1/projects/project-id/agents/2861/whoami",
                            b'{"message":"who am I?"}',
                        )

                    self.assertEqual(status_code, 200)
                    self.assertTrue(payload["managed"])
                    self.assertEqual(
                        payload["workspace"]["actual_git_toplevel"],
                        WORKSPACE_1_ROOT,
                    )
                    self.assertNotIn("prompt_archive", payload)
                    self.assertNotIn("response_storage_errors", payload)
                    sentinel_registry.for_project.assert_not_called()
                finally:
                    harness.tearDown()


if __name__ == "__main__":
    unittest.main()
