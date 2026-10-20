import copy
import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from nginx_qa.prompt_archive_incremental import ArchiveOccurrence, response_id_v1
from nginx_qa.prompt_archive_response import (
    build_authoritative_occurrence_document,
)
from nginx_qa.prompt_archive_runtime import (
    PromptArchiveRuntimeConfig,
    PromptArchiveRuntimeRegistry,
)
from nginx_qa.prompt_archive_state_view import (
    hydrate_prompt_archive_state_view,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)
from tests.test_prompt_archive_runtime import ClosedCompatibilityPublisher


PROJECT_ID = "9000"
SPRINT_ID = "sprint-runtime-tests"


def occurrence(ordinal: int, *, predecessor: str | None = None) -> ArchiveOccurrence:
    occurrence_id = f"state-view-{ordinal}"
    hour, minute = divmod(ordinal, 60)
    return ArchiveOccurrence(
        project_id=PROJECT_ID,
        sprint_id=SPRINT_ID,
        response_id=response_id_v1(
            PROJECT_ID,
            SPRINT_ID,
            "transition-response",
            "transition",
            occurrence_id,
        ),
        response_kind="transition-response",
        occurrence_kind="transition",
        occurrence_id=occurrence_id,
        transition_id=occurrence_id,
        source={
            "source_kind": "native_runtime",
            "assignment_id": f"assignment-{ordinal}",
            **(
                {"predecessor_archive_id": predecessor}
                if predecessor is not None
                else {}
            ),
        },
        authorization={
            "auth_model_version": "prompt-archive-authorization-v1",
            "principal_kind": "synthetic_test",
            "principal_id": "state-view-tests",
            "binding_id": None,
            "scope_source": "synthetic_test",
            "effective_scope_revision": None,
            "effective_scope_sha256": None,
            "decision_occurrence_id": f"state-view-decision-{ordinal}",
        },
        created_at=f"2026-10-12T{hour:02d}:{minute:02d}:00Z",
        verified_at=f"2026-10-12T{hour:02d}:{minute:02d}:01Z",
        state_revision=ordinal,
    )


def occurrence_bytes(value: ArchiveOccurrence) -> bytes:
    response = {
        "response_compact": True,
        "active_task_message_complete": True,
        "omitted_response_fields": [],
        "project_id": PROJECT_ID,
        "sprint_id": SPRINT_ID,
        "response_id": value.response_id,
        "response_kind": value.response_kind,
        "active_task": {
            "id": f"task-{value.state_revision}",
            "message": f"current task {value.state_revision}",
        },
        "result": {
            "status": "DONE",
            "ordinal": value.state_revision,
        },
    }
    document = build_authoritative_occurrence_document(response, value)
    return canonical_json_bytes_v1(document, maximum=1024 * 1024)


class PromptArchiveStateViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def runtime(self):
        return PromptArchiveRuntimeRegistry(
            PromptArchiveRuntimeConfig(
                mode="authoritative",
                root=self.base / "archive",
                max_hydrated_bytes=8 * 1024 * 1024,
                operator_gate="prompt-archive-authoritative-v1",
            )
        ).for_project(PROJECT_ID)

    def assert_code(self, code: str, operation) -> None:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def test_runtime_hydrates_complete_root_to_head_state_view(self) -> None:
        runtime = self.runtime()
        first_occurrence = occurrence(1)
        with mock.patch(
            "nginx_qa.prompt_archive_state_view."
            "hydrate_prompt_archive_state_view",
            side_effect=AssertionError("publication invoked cold state hydration"),
        ) as cold_reader:
            first = runtime.publish_authoritative(
                occurrence_bytes(first_occurrence),
                first_occurrence,
                compatibility_publisher=ClosedCompatibilityPublisher(
                    self.base / "prompt",
                    runtime,
                ),
            )
            second_occurrence = occurrence(2)
            second = runtime.publish_authoritative(
                occurrence_bytes(second_occurrence),
                second_occurrence,
                compatibility_publisher=ClosedCompatibilityPublisher(
                    self.base / "prompt",
                    runtime,
                ),
            )
        cold_reader.assert_not_called()

        with mock.patch.object(
            runtime,
            "lookup_archive_metadata",
            wraps=runtime.lookup_archive_metadata,
        ) as lookup, mock.patch.object(
            runtime,
            "hydrate_content",
            wraps=runtime.hydrate_content,
        ) as hydrate:
            raw = runtime.hydrate_state_view(
                sprint_id=SPRINT_ID,
                archive_id=second.archive_id,
            )

        view = json.loads(raw)
        self.assertEqual(view["kind"], "prompt_archive_state_view")
        self.assertEqual(view["scope"]["root_archive_id"], first.archive_id)
        self.assertEqual(view["scope"]["head_archive_id"], second.archive_id)
        self.assertEqual(view["scope"]["occurrence_count"], 2)
        self.assertEqual(
            [item["archive_id"] for item in view["occurrences"]],
            [first.archive_id, second.archive_id],
        )
        self.assertEqual(
            view["occurrences"][1]["predecessor_archive_id"],
            first.archive_id,
        )
        self.assertEqual(
            view["state"]["current"]["execution_response"]["active_task"][
                "message"
            ],
            "current task 2",
        )
        self.assertEqual(
            [item["evidence_keys"] for item in view["state"]["history"]],
            [["result"], ["result"]],
        )
        self.assertEqual(
            [
                item["document"]["evidence_delta"]["result"]["ordinal"]
                for item in view["occurrences"]
            ],
            [1, 2],
        )
        self.assertEqual(lookup.call_count, 2)
        self.assertEqual(hydrate.call_count, 2)
        self.assertEqual(
            view["integrity"]["aggregate_hydrated_bytes"],
            first.integrity["hydrated_size"] + second.integrity["hydrated_size"],
        )
        self.assertEqual(
            view["integrity"]["chain_sha256"],
            hashlib.sha256(
                canonical_json_bytes_v1(
                    view["occurrences"],
                    maximum=8 * 1024 * 1024,
                )
            ).hexdigest(),
        )

    def test_legacy_or_shadow_archive_is_unsupported_without_hydration(self) -> None:
        archive_id = "par1-" + "a" * 64
        fake = _FakeRuntime(
            {
                archive_id: _fake_selected(
                    archive_id,
                    occurrence(1),
                    source_kind="runtime_shadow",
                    publication_mode="shadow",
                )
            }
        )
        self.assert_code(
            "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=archive_id,
            ),
        )
        self.assertEqual(fake.hydrate_calls, [])

    def test_declared_aggregate_limit_fails_before_blob_hydration(self) -> None:
        archive_id = "par1-" + "b" * 64
        selected = _fake_selected(archive_id, occurrence(1))
        selected.manifest["integrity"]["hydrated_size"] = 1024
        fake = _FakeRuntime({archive_id: selected}, maximum=1024)
        self.assert_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=archive_id,
                max_output_bytes=512,
            ),
        )
        self.assertEqual(fake.hydrate_calls, [])

    def test_cycle_and_document_scope_mismatch_fail_closed(self) -> None:
        first_id = "par1-" + "c" * 64
        second_id = "par1-" + "d" * 64
        first_occurrence = occurrence(1, predecessor=second_id)
        second_occurrence = dataclasses.replace(
            occurrence(2, predecessor=first_id),
            state_revision=1,
        )
        fake = _FakeRuntime(
            {
                first_id: _fake_selected(
                    first_id,
                    first_occurrence,
                    predecessor=second_id,
                ),
                second_id: _fake_selected(
                    second_id,
                    second_occurrence,
                    predecessor=first_id,
                ),
            }
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=first_id,
            ),
        )

    def test_count_and_final_output_are_independently_bounded(self) -> None:
        identifiers = ["par1-" + f"{index + 1:064x}" for index in range(257)]
        selected = {}
        for index, identifier in enumerate(identifiers):
            selected[identifier] = _fake_selected(
                identifier,
                occurrence(
                    index + 1,
                    predecessor=(identifiers[index - 1] if index else None),
                ),
                predecessor=(identifiers[index - 1] if index else None),
            )
        fake = _FakeRuntime(selected)
        self.assert_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=identifiers[-1],
            ),
        )
        self.assertEqual(len(fake.lookup_calls), 256)
        self.assertEqual(len(fake.hydrate_calls), 256)
        self.assertNotIn(identifiers[0], fake.lookup_calls)

        one_id = "par1-" + "f" * 64
        one = _fake_selected(one_id, occurrence(1))
        fake = _FakeRuntime({one_id: one})
        # The occurrence itself fits, but the state envelope and indexes do
        # not.  Final canonical serialization therefore enforces a distinct
        # response bound after the one allowed hydration.
        self.assert_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=one_id,
                max_output_bytes=len(one.raw) + 16,
            ),
        )
        self.assertEqual(fake.hydrate_calls, [one_id])

    def test_source_and_revision_chain_mismatches_fail_closed(self) -> None:
        source_id = "par1-" + "7" * 64
        source_selected = _fake_selected(source_id, occurrence(1))
        cross_scope = _FakeRuntime({source_id: source_selected})
        self.assert_code(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: hydrate_prompt_archive_state_view(
                cross_scope,
                sprint_id="different-sprint",
                archive_id=source_id,
            ),
        )
        self.assertEqual(cross_scope.hydrate_calls, [])

        source_selected.manifest["source"]["assignment_id"] = "different-owner"
        fake = _FakeRuntime({source_id: source_selected})
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=source_id,
            ),
        )

        root_id = "par1-" + "8" * 64
        head_id = "par1-" + "9" * 64
        root_occurrence = occurrence(2)
        head_occurrence = occurrence(1, predecessor=root_id)
        fake = _FakeRuntime(
            {
                root_id: _fake_selected(root_id, root_occurrence),
                head_id: _fake_selected(
                    head_id,
                    head_occurrence,
                    predecessor=root_id,
                ),
            }
        )
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=head_id,
            ),
        )

        mismatch_id = "par1-" + "e" * 64
        mismatch_occurrence = occurrence(3)
        selected = _fake_selected(mismatch_id, mismatch_occurrence)
        document = json.loads(selected.raw)
        document["occurrence"]["sprint_id"] = "different-sprint"
        selected.raw = canonical_json_bytes_v1(document, maximum=1024 * 1024)
        selected.manifest["integrity"]["hydrated_size"] = len(selected.raw)
        fake = _FakeRuntime({mismatch_id: selected})
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=mismatch_id,
            ),
        )

    def test_cancellation_stops_before_lookup_and_between_chain_steps(self) -> None:
        archive_id = "par1-" + "4" * 64
        cancelled = PromptArchiveCancellationToken()
        cancelled.cancel()
        fake = _FakeRuntime({archive_id: _fake_selected(archive_id, occurrence(1))})
        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=archive_id,
                cancellation=cancelled,
            ),
        )
        self.assertEqual(fake.lookup_calls, [])
        self.assertEqual(fake.hydrate_calls, [])

        root_id = "par1-" + "5" * 64
        head_id = "par1-" + "6" * 64
        token = PromptArchiveCancellationToken()
        fake = _FakeRuntime(
            {
                root_id: _fake_selected(root_id, occurrence(1)),
                head_id: _fake_selected(
                    head_id,
                    occurrence(2, predecessor=root_id),
                    predecessor=root_id,
                ),
            },
            cancel_after_hydrations=1,
        )
        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: hydrate_prompt_archive_state_view(
                fake,
                sprint_id=SPRINT_ID,
                archive_id=head_id,
                cancellation=token,
            ),
        )
        self.assertEqual(fake.lookup_calls, [head_id])
        self.assertEqual(fake.hydrate_calls, [head_id])
        self.assertEqual(fake.cancellation_tokens, [token])


class _FakeRuntime:
    def __init__(
        self,
        selected,
        *,
        maximum: int = 8 * 1024 * 1024,
        cancel_after_hydrations: int | None = None,
    ) -> None:
        self.project_id = PROJECT_ID
        self.max_hydrated_bytes = maximum
        self.selected = selected
        self.hydrate_calls = []
        self.lookup_calls = []
        self.cancellation_tokens = []
        self.cancel_after_hydrations = cancel_after_hydrations

    def lookup_archive_metadata(
        self,
        *,
        sprint_id,
        archive_id,
        cancellation=None,
    ):
        self.lookup_calls.append(archive_id)
        if sprint_id != SPRINT_ID or archive_id not in self.selected:
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_NOT_FOUND")
        return self.selected[archive_id]

    def hydrate_content(self, selected, *, cancellation=None):
        self.hydrate_calls.append(selected.archive_id)
        self.cancellation_tokens.append(cancellation)
        if (
            self.cancel_after_hydrations is not None
            and len(self.hydrate_calls) >= self.cancel_after_hydrations
            and cancellation is not None
        ):
            cancellation.cancel()
        return selected.raw


def _fake_selected(
    archive_id: str,
    value: ArchiveOccurrence,
    *,
    predecessor: str | None = None,
    source_kind: str = "native_runtime",
    publication_mode: str = "authoritative",
):
    if predecessor is not None:
        value = dataclasses.replace(
            value,
            source={**dict(value.source), "predecessor_archive_id": predecessor},
        )
    raw = occurrence_bytes(value)
    source = copy.deepcopy(dict(value.source))
    source["source_kind"] = source_kind
    scope = {
        "project_id": PROJECT_ID,
        "sprint_id": SPRINT_ID,
        "response_id": value.response_id,
        "response_kind": value.response_kind,
        "occurrence_kind": value.occurrence_kind,
        "occurrence_id": value.occurrence_id,
        "transition_id": value.transition_id,
    }
    manifest_digest = (
        "1" if archive_id[-1] != "1" else "2"
    ) * 64
    manifest_ref = {"digest": manifest_digest}
    pointer_id = "parptr1-" + manifest_digest
    return SimpleNamespace(
        project_id=PROJECT_ID,
        sprint_id=SPRINT_ID,
        archive_id=archive_id,
        archive_format="incremental_transition_v1",
        raw=raw,
        manifest={
            "archive_id": archive_id,
            "archive_format": "incremental_transition_v1",
            "scope": scope,
            "source": source,
            "authorization": copy.deepcopy(dict(value.authorization)),
            "integrity": {
                "semantics": "native-byte-equality",
                "hydrated_media_type": "application/json",
                "hydrated_size": len(raw),
            },
            "publication": {
                "mode": publication_mode,
                "legacy_authoritative": publication_mode != "authoritative",
                "verified_at": value.verified_at,
            },
            "created_at": value.created_at,
        },
        pointer={
            "archive_id": archive_id,
            "response_id": value.response_id,
            "pointer_id": pointer_id,
            "manifest_ref": manifest_ref,
        },
        locator={
            "archive_id": archive_id,
            "response_id": value.response_id,
            "pointer_id": pointer_id,
            "manifest_ref": manifest_ref,
        },
    )


if __name__ == "__main__":
    unittest.main()
