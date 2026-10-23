import copy
import json
import unittest
from dataclasses import replace
from unittest.mock import Mock

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    pas1_id_v1,
    response_id_v1,
)
from nginx_qa.prompt_archive_response import (
    build_authoritative_occurrence_document,
    dispatch_prompt_archive_response,
)
from nginx_qa.prompt_archive_storage import PromptArchiveStorageError


DOCUMENT_KIND = "prompt_archive_occurrence_document"
DOCUMENT_MEDIA = (
    "application/vnd.nginx-qa.prompt-archive-occurrence-document-v1+json"
)
STUB_MEDIA = (
    "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
)


def occurrence() -> ArchiveOccurrence:
    project_id = "9000"
    sprint_id = "sprint-response-one"
    occurrence_id = "transition-response-one"
    return ArchiveOccurrence(
        project_id=project_id,
        sprint_id=sprint_id,
        response_id=response_id_v1(
            project_id,
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
            "source_kind": "native_runtime",
            "assignment_id": "assignment-response-one",
            "review_id": "review-response-one",
            "result_key": "result-response-one",
            "source_commit": "a" * 40,
        },
        authorization={
            "auth_model_version": "prompt-archive-authorization-v1",
            "principal_kind": "synthetic_test",
            "principal_id": "response-test-principal",
            "binding_id": None,
            "scope_source": "synthetic_test",
            "effective_scope_revision": None,
            "effective_scope_sha256": None,
            "decision_occurrence_id": "response-test-decision",
        },
        created_at="2026-10-12T10:00:00Z",
        verified_at="2026-10-12T10:00:01Z",
        state_revision=42,
    )


def committed_response(selected: ArchiveOccurrence | None = None) -> dict:
    frozen = selected or occurrence()
    return {
        "answer": "Transition accepted",
        "project_id": frozen.project_id,
        "sprint_id": frozen.sprint_id,
        "response_id": frozen.response_id,
        "response_kind": frozen.response_kind,
        "response_compact": True,
        "active_task": {
            "id": "task-response-one",
            "message": "Preserve this complete task and its controls.",
            "metadata": {
                "assignment_id": "assignment-response-one",
                "allowed_outcomes": ["DONE", "STOP"],
                "result_endpoint": "/managed/result",
                "source_commit": "a" * 40,
                "future_control": {"required": True},
            },
        },
        "assignment": {
            "current_assignment_id": "assignment-response-one",
            "current_node_id": "runtime-integration",
            "status": "active",
        },
        "result": {
            "summary": "Current result evidence",
            "tests": ["focused-response-tests"],
        },
        "feedback": "Current actionable feedback",
        "review": {
            "review_id": "review-response-one",
            "decision": "APPROVE",
        },
        "reviews": [
            {
                "review_id": "review-response-one",
                "decision": "APPROVE",
            }
        ],
        "transition_applied": {
            "transition_id": frozen.transition_id,
            "outcome": "DONE",
            "status": "applied",
        },
        "source_commit": "a" * 40,
        "from_commit": "a" * 40,
        "git_commit": "b" * 40,
        "git_branch": "agent/psc-response",
    }


def descriptor_stub(selected: ArchiveOccurrence | None = None) -> dict:
    frozen = selected or occurrence()
    scope = {
        "project_id": frozen.project_id,
        "sprint_id": frozen.sprint_id,
        "response_id": frozen.response_id,
        "response_kind": frozen.response_kind,
        "occurrence_kind": frozen.occurrence_kind,
        "occurrence_id": frozen.occurrence_id,
        "transition_id": frozen.transition_id,
    }
    archive_id = "par1-" + "b" * 64
    pointer_id = "parptr1-" + "c" * 64
    manifest_ref = {
        "schema_version": 1,
        "kind": "prompt_archive_blob_ref",
        "hash_algorithm": "sha256",
        "digest": "d" * 64,
        "logical_kind": "manifest",
        "media_type": (
            "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
        ),
        "content_encoding": "identity",
        "uncompressed_size": 128,
        "stored_size": 128,
        "stored_sha256": "d" * 64,
    }
    created_at = "2026-10-12T10:00:00Z"
    published_at = "2026-10-12T10:00:02Z"
    stub_id = "parstub1-" + pas1_id_v1(
        "descriptor-stub-id",
        (
            ("project_id", scope["project_id"]),
            ("sprint_id", scope["sprint_id"]),
            ("response_id", scope["response_id"]),
            ("archive_id", archive_id),
            ("pointer_id", pointer_id),
            ("manifest_sha256", manifest_ref["digest"]),
            ("created_at", created_at),
            ("published_at", published_at),
        ),
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_descriptor_stub",
        "media_type": STUB_MEDIA,
        "id_algorithm": "PAS1-ID-v1",
        "stub_id": stub_id,
        "archive_id": archive_id,
        "archive_format": "incremental_transition_v1",
        "scope": scope,
        "integrity": {
            "semantics": "native-byte-equality",
            "hydrated_media_type": "application/json",
            "hydrated_size": 4096,
            "hydrated_sha256": "e" * 64,
            "canonical": {
                "algorithm": "nginx-qa-canonical-json-v1",
                "digest": "f" * 64,
            },
        },
        "pointer_id": pointer_id,
        "manifest_ref": manifest_ref,
        "created_at": created_at,
        "published_at": published_at,
    }


def encoded(value: dict) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class PromptArchiveResponseTests(unittest.TestCase):
    def assert_error(self, code: str, operation) -> None:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def test_builder_preserves_current_controls_and_separates_evidence(self) -> None:
        selected = occurrence()
        response = committed_response(selected)
        original = copy.deepcopy(response)

        document = build_authoritative_occurrence_document(response, selected)

        self.assertEqual(
            set(document),
            {
                "schema_version",
                "kind",
                "media_type",
                "occurrence",
                "execution_response",
                "evidence_delta",
            },
        )
        self.assertEqual(document["schema_version"], 1)
        self.assertEqual(document["kind"], DOCUMENT_KIND)
        self.assertEqual(document["media_type"], DOCUMENT_MEDIA)
        self.assertEqual(
            document["execution_response"]["active_task"],
            original["active_task"],
        )
        self.assertEqual(
            document["execution_response"]["assignment"],
            original["assignment"],
        )
        for key in (
            "result",
            "feedback",
            "review",
            "reviews",
            "transition_applied",
            "source_commit",
            "from_commit",
            "git_commit",
            "git_branch",
        ):
            with self.subTest(key=key):
                self.assertEqual(document["evidence_delta"][key], original[key])
                self.assertNotIn(key, document["execution_response"])
        self.assertEqual(
            document["occurrence"]["source"]["source_commit"],
            "a" * 40,
        )
        for transport_only in (
            "source_response_key",
            "source_byte_size",
            "source_sha256",
        ):
            self.assertNotIn(transport_only, document["occurrence"])

        response["active_task"]["message"] = "mutated after build"
        response["result"]["summary"] = "mutated after build"
        self.assertEqual(
            document["execution_response"]["active_task"],
            original["active_task"],
        )
        self.assertEqual(document["evidence_delta"]["result"], original["result"])

    def test_builder_rejects_every_known_cumulative_shape_before_copying(self) -> None:
        class Poison:
            def __deepcopy__(self, _memo):
                raise AssertionError("cumulative value must not be copied")

        for key in (
            "project_state",
            "work_history",
            "assignments",
            "completed_assignments",
            "history_with_patches",
            "activity_with_patches",
            "code_patches",
            "recent_activity",
            "execution_audit",
        ):
            with self.subTest(key=key):
                response = committed_response()
                response[key] = Poison()
                self.assert_error(
                    "PROMPT_ARCHIVE_REQUEST_INVALID",
                    lambda response=response: build_authoritative_occurrence_document(
                        response,
                        occurrence(),
                    ),
                )

        for container_name in ("assignment", "execution"):
            for history_name in ("assignments", "execution_audit"):
                with self.subTest(
                    container=container_name,
                    history=history_name,
                ):
                    response = committed_response()
                    response[container_name] = {history_name: Poison()}
                    self.assert_error(
                        "PROMPT_ARCHIVE_REQUEST_INVALID",
                        lambda response=response: (
                            build_authoritative_occurrence_document(
                                response,
                                occurrence(),
                            )
                        ),
                    )

    def test_builder_rejects_non_native_transport_or_cross_binding(self) -> None:
        selected = occurrence()
        self.assert_error(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: build_authoritative_occurrence_document(
                committed_response(selected),
                replace(
                    selected,
                    source_response_key="0123456789abcdef0123",
                    source_byte_size=10,
                    source_sha256="1" * 64,
                ),
            ),
        )
        self.assert_error(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: build_authoritative_occurrence_document(
                committed_response(selected),
                replace(
                    selected,
                    source={**selected.source, "source_kind": "runtime_shadow"},
                ),
            ),
        )
        mismatched = committed_response(selected)
        mismatched["sprint_id"] = "sprint-other"
        self.assert_error(
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
            lambda: build_authoritative_occurrence_document(mismatched, selected),
        )

    def test_dispatcher_returns_validated_legacy_mapping_without_reader(self) -> None:
        reader = Mock(side_effect=AssertionError("legacy must not hydrate"))
        raw = (
            b' {\n  "answer": "legacy", "ratio": 1.5, '
            b'"active_task": {"message": "complete"}\n } '
        )

        result = dispatch_prompt_archive_response(raw, archive_reader=reader)

        self.assertEqual(
            result,
            {
                "answer": "legacy",
                "ratio": 1.5,
                "active_task": {"message": "complete"},
            },
        )
        reader.assert_not_called()

    def test_stub_invokes_authorized_reader_once_and_validates_document(self) -> None:
        selected = occurrence()
        document = build_authoritative_occurrence_document(
            committed_response(selected),
            selected,
        )
        stub = descriptor_stub(selected)
        reader = Mock(return_value=encoded(document))

        result = dispatch_prompt_archive_response(
            encoded(stub),
            archive_reader=reader,
            maximum_bytes=2 * 1024 * 1024,
        )

        self.assertEqual(result, document)
        reader.assert_called_once()
        self.assertEqual(
            reader.call_args.kwargs["descriptor_stub"],
            stub,
        )
        self.assertEqual(
            reader.call_args.kwargs["maximum_bytes"],
            2 * 1024 * 1024,
        )

    def test_stub_scope_is_cross_bound_to_hydrated_occurrence(self) -> None:
        selected = occurrence()
        document = build_authoritative_occurrence_document(
            committed_response(selected),
            selected,
        )
        wrong = copy.deepcopy(document)
        wrong["occurrence"]["project_id"] = "9001"
        wrong_occurrence_id = response_id_v1(
            "9001",
            selected.sprint_id,
            selected.response_kind,
            selected.occurrence_kind,
            selected.occurrence_id,
        )
        wrong["occurrence"]["response_id"] = wrong_occurrence_id
        wrong["execution_response"]["project_id"] = "9001"
        wrong["execution_response"]["response_id"] = wrong_occurrence_id
        reader = Mock(return_value=encoded(wrong))

        self.assert_error(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: dispatch_prompt_archive_response(
                encoded(descriptor_stub(selected)),
                archive_reader=reader,
            ),
        )
        reader.assert_called_once()

    def test_unknown_and_malformed_documents_fail_closed(self) -> None:
        reader = Mock()
        cases = (
            (
                b'{"kind":"future_response","schema_version":2}',
                "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
            ),
            (b'{"answer":', "PROMPT_ARCHIVE_REFERENCE_INVALID"),
            (b'["not-an-object"]', "PROMPT_ARCHIVE_REFERENCE_INVALID"),
            (b'{"answer":1,"answer":2}', "PROMPT_ARCHIVE_REFERENCE_INVALID"),
            (
                b'{"kind":"prompt_archive_descriptor_stub","kind":"legacy"}',
                "PROMPT_ARCHIVE_REFERENCE_INVALID",
            ),
            (b'{"kind":"legacy"}', "PROMPT_ARCHIVE_VERSION_UNSUPPORTED"),
            (b'{"kind":null}', "PROMPT_ARCHIVE_VERSION_UNSUPPORTED"),
        )
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assert_error(
                    expected,
                    lambda raw=raw: dispatch_prompt_archive_response(
                        raw,
                        archive_reader=reader,
                    ),
                )
        reader.assert_not_called()

        selected = occurrence()
        stub = descriptor_stub(selected)
        malformed_stub = copy.deepcopy(stub)
        del malformed_stub["manifest_ref"]
        self.assert_error(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: dispatch_prompt_archive_response(
                encoded(malformed_stub),
                archive_reader=reader,
            ),
        )
        reader.assert_not_called()

    def test_reader_output_kind_version_and_failures_are_stable(self) -> None:
        raw_stub = encoded(descriptor_stub())
        outputs = (
            (
                {"kind": "future_occurrence_document", "schema_version": 1},
                "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
            ),
            (
                {
                    "kind": DOCUMENT_KIND,
                    "schema_version": 2,
                    "media_type": DOCUMENT_MEDIA,
                },
                "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
            ),
            ({"kind": DOCUMENT_KIND}, "PROMPT_ARCHIVE_VERSION_UNSUPPORTED"),
        )
        for output, expected in outputs:
            with self.subTest(output=output):
                reader = Mock(return_value=encoded(output))
                self.assert_error(
                    expected,
                    lambda reader=reader: dispatch_prompt_archive_response(
                        raw_stub,
                        archive_reader=reader,
                    ),
                )
                reader.assert_called_once()

        stable = PromptArchiveStorageError("PROMPT_ARCHIVE_NOT_FOUND")
        self.assert_error(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: dispatch_prompt_archive_response(
                raw_stub,
                archive_reader=Mock(side_effect=stable),
            ),
        )
        self.assert_error(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: dispatch_prompt_archive_response(
                raw_stub,
                archive_reader=Mock(side_effect=RuntimeError("secret path")),
            ),
        )


if __name__ == "__main__":
    unittest.main()
