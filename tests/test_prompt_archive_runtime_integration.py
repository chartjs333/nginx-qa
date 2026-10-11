import asyncio
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import main
from nginx_qa.prompt_archive_incremental import ArchiveOccurrence, response_id_v1
from nginx_qa.prompt_archive_storage import PromptArchiveStorageError
from tests.test_managed_continuity import (
    ManagedContinuityTests,
    WORKSPACE_1_ROOT,
    asgi_request,
)
from nginx_qa.execution_observability import append_execution_checkpoint


class PromptArchiveRuntimeIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.prompt_root = self.root / "prompt"
        self.archive_root = self.root / "archive"
        self.prompt_root.mkdir()
        self.original_runtime = main.prompt_archive_runtime
        self.original_receipt_path = main.prompt_archive_occurrence_receipt_path

    def tearDown(self) -> None:
        main.prompt_archive_runtime = self.original_runtime
        main.prompt_archive_occurrence_receipt_path = self.original_receipt_path
        self.temporary.cleanup()

    def environment(self, mode: str) -> dict[str, str]:
        return {
            "NGINX_QA_PROMPT_ARCHIVE_MODE": mode,
            "NGINX_QA_PROMPT_ARCHIVE_ROOT": str(self.archive_root),
            "NGINX_QA_PROMPT_ROOT": str(self.prompt_root),
            "NGINX_QA_PROTECTED_ROOTS": "[]",
        }

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
        )

    def test_shadow_uses_exact_committed_bytes_and_publishes_after_legacy(self) -> None:
        self.configure("shadow")

        compact = self.materialize()

        descriptor = compact["prompt_archive"]
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


class PromptArchiveManagedHotPathTests(unittest.IsolatedAsyncioTestCase):
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
        harness = ManagedContinuityTests()
        harness.setUp()
        self.addCleanup(harness.tearDown)
        sentinel_registry = Mock()
        sentinel_registry.for_project.side_effect = AssertionError(
            "managed identity must not access archive storage"
        )
        with (
            patch.dict(
                os.environ,
                {
                    "NGINX_QA_MANAGED_ROOT": "C:/managed-test-root",
                    "NGINX_QA_PROMPT_ARCHIVE_MODE": "shadow",
                },
                clear=False,
            ),
            patch.object(main, "prompt_archive_runtime", sentinel_registry),
            patch.object(main, "managed_continuity_runtime", harness.runtime),
            patch.object(main, "read_git_config", AsyncMock(return_value={})),
            patch.object(
                main,
                "managed_continuity_runtime_for_requests",
                return_value=harness.runtime,
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
            patch.object(
                main,
                "materialize_sequential_graph_response",
                side_effect=AssertionError("legacy response writer called"),
            ),
        ):
            status_code, payload, _raw = await asgi_request(
                "/api/v1/projects/project-id/agents/2861/whoami",
                b'{"message":"who am I?"}',
            )

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["managed"])
        self.assertEqual(payload["workspace"]["actual_git_toplevel"], WORKSPACE_1_ROOT)
        self.assertNotIn("prompt_archive", payload)
        sentinel_registry.for_project.assert_not_called()


if __name__ == "__main__":
    unittest.main()
