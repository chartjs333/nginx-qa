import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from nginx_qa.prompt_archive_incremental import ArchiveOccurrence, response_id_v1
from nginx_qa.prompt_archive_occurrence_store import (
    PromptArchiveOccurrenceReceiptStore,
    PromptArchiveOccurrenceSource,
    PromptArchiveSourceMetadataProjectionProof,
)
from nginx_qa.prompt_archive_source_metadata import (
    freeze_source_metadata_screen,
    project_source_metadata,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


PROJECT_ID = "9000"
SPRINT_ID = "sprint-source-metadata-projection"
OCCURRENCE_ID = "transition-source-metadata-projection-1"
PAYLOAD = b'{"committed":true}\n'


def locator():
    return {
        "locator_kind": "runtime_occurrence",
        "locator_version": "prompt-archive-runtime-occurrence-locator-v1",
        "runtime_instance_id": "psc-runtime-18032",
        "runtime_occurrence_id": OCCURRENCE_ID,
        "state_revision": 41,
    }


def projection_proof(*, title="Release evidence"):
    document = {
        "schema_version": 1,
        "kind": "prompt_archive_source_metadata",
        "projection_version": "prompt-archive-source-projection-v1",
        "redaction_version": "prompt-archive-source-redaction-v1",
        "source_type": "email",
        "conversation_id": "sha256:" + "a" * 64,
        "thread_id": "[REDACTED]",
        "sender_label": "Alice",
        "title": title,
        "observed_at": "2026-10-20T09:30:00Z",
        "redaction_count": 1,
        "redacted_fields": ["thread_id"],
        "discarded_field_count": 2,
    }
    canonical = canonical_json_bytes_v1(document, maximum=16 * 1024)
    digest = hashlib.sha256(canonical).hexdigest()
    receipt = {
        "schema_version": 1,
        "kind": "prompt_archive_source_metadata_projection_receipt",
        "projection_version": "prompt-archive-source-projection-v1",
        "redaction_version": "prompt-archive-source-redaction-v1",
        "projection_sha256": digest,
        "projection_byte_size": len(canonical),
        "source_type": "email",
        "discarded_field_count": 2,
        "projected_fields": [
            "conversation_id",
            "observed_at",
            "sender_label",
            "thread_id",
            "title",
        ],
    }
    return PromptArchiveSourceMetadataProjectionProof(
        document=document,
        canonical_bytes=canonical,
        sha256=digest,
        receipt=receipt,
    )


def legacy_occurrence():
    source_digest = hashlib.sha256(PAYLOAD).hexdigest()
    return ArchiveOccurrence(
        project_id=PROJECT_ID,
        sprint_id=SPRINT_ID,
        response_id=response_id_v1(
            PROJECT_ID,
            SPRINT_ID,
            "transition-response",
            "transition",
            OCCURRENCE_ID,
        ),
        response_kind="transition-response",
        occurrence_kind="transition",
        occurrence_id=OCCURRENCE_ID,
        transition_id=OCCURRENCE_ID,
        source={
            "source_kind": "runtime_shadow",
            "source_commit": "a" * 40,
            "assignment_id": "assignment-projection-store-1",
        },
        authorization={
            "auth_model_version": "prompt-archive-authorization-v1",
            "principal_kind": "legacy_local_principal",
            "principal_id": PROJECT_ID,
            "binding_id": None,
            "scope_source": "legacy_project_context",
            "effective_scope_revision": None,
            "effective_scope_sha256": None,
            "decision_occurrence_id": OCCURRENCE_ID,
        },
        created_at="2026-10-20T09:30:00Z",
        verified_at="2026-10-20T09:30:01Z",
        state_revision=41,
        source_response_key="0123456789abcdefabcd",
        source_byte_size=len(PAYLOAD),
        source_sha256=source_digest,
    )


class PromptArchiveSourceMetadataProjectionStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name) / "occurrence-receipts"
        self.reader_calls = 0

        def reader(**_kwargs):
            self.reader_calls += 1
            return PAYLOAD

        self.store = PromptArchiveOccurrenceReceiptStore(
            self.root,
            reader,
            max_source_bytes=1024 * 1024,
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def projection_path(self):
        locator_raw = canonical_json_bytes_v1(locator(), maximum=4 * 1024)
        locator_digest = hashlib.sha256(locator_raw).hexdigest()
        target, _lock = self.store._source_metadata_projection_paths(
            PROJECT_ID,
            locator_digest,
        )
        return target

    def test_load_or_register_projects_once_and_reuses_durable_proof(self):
        expected = projection_proof()
        calls = []

        first = self.store.load_or_register_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
            projector=lambda: calls.append("project") or expected,
        )

        restarted = PromptArchiveOccurrenceReceiptStore(
            self.root,
            lambda **_kwargs: None,
            max_source_bytes=1024 * 1024,
        )
        second = restarted.load_or_register_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
            projector=lambda: self.fail("recovery must load before projecting"),
        )

        self.assertEqual(calls, ["project"])
        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        self.assertEqual(second.canonical_bytes, expected.canonical_bytes)

    def test_accepts_the_pure_projector_result_without_an_adapter(self):
        projected = project_source_metadata(
            {
                "source_type": "telegram",
                "message_id": "raw-external-message-17",
                "sender_label": "Operator",
            },
            screen=freeze_source_metadata_screen(),
        )

        stored = self.store.load_or_register_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
            projector=lambda: projected,
        )

        self.assertEqual(stored.document, projected.document)
        self.assertEqual(stored.canonical_bytes, projected.canonical_bytes)
        self.assertEqual(stored.sha256, projected.sha256)
        self.assertEqual(stored.receipt, projected.receipt)

    def test_persisted_document_is_closed_and_contains_no_raw_or_screen(self):
        raw_canary = "configured-secret-canary-7f43"
        token_digest_canary = "b" * 64
        expected = projection_proof()

        self.store.load_or_register_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
            projector=lambda: expected,
        )

        raw = self.projection_path().read_bytes()
        stored = json.loads(raw)
        self.assertNotIn(raw_canary.encode("utf-8"), raw)
        self.assertNotIn(token_digest_canary.encode("ascii"), raw)
        self.assertEqual(
            set(stored),
            {
                "schema_version",
                "kind",
                "media_type",
                "hash_algorithm",
                "project_id",
                "locator_sha256",
                "locator",
                "projection",
                "projection_receipt",
                "proof_sha256",
            },
        )
        self.assertEqual(stored["projection"], expected.document)
        self.assertEqual(stored["projection_receipt"], expected.receipt)
        self.assertNotIn("raw_metadata", stored)
        self.assertNotIn("screen_snapshot", stored)
        self.assertEqual(canonical_json_bytes_v1(stored, maximum=64 * 1024), raw)

    def test_register_is_idempotent_but_rejects_different_projection(self):
        expected = projection_proof()
        self.store.register_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
            projection=expected,
        )
        same = self.store.register_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
            projection=expected,
        )
        self.assertEqual(same, expected)

        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.store.register_source_metadata_projection(
                project_id=PROJECT_ID,
                locator=locator(),
                projection=projection_proof(title="Different evidence"),
            )
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_GENERATION_CONFLICT")

    def test_cross_binding_mismatch_is_rejected_before_publication(self):
        expected = projection_proof()
        broken_receipt = dict(expected.receipt)
        broken_receipt["discarded_field_count"] = 3
        broken = dataclasses.replace(expected, receipt=broken_receipt)

        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.store.register_source_metadata_projection(
                project_id=PROJECT_ID,
                locator=locator(),
                projection=broken,
            )

        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
        self.assertFalse(self.projection_path().exists())

    def test_failed_projector_leaves_no_proof(self):
        def fail():
            raise RuntimeError("projector failed before persistence")

        with self.assertRaisesRegex(RuntimeError, "projector failed"):
            self.store.load_or_register_source_metadata_projection(
                project_id=PROJECT_ID,
                locator=locator(),
                projector=fail,
            )

        self.assertIsNone(
            self.store.load_source_metadata_projection(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        )

    def test_projection_proof_coexists_with_legacy_occurrence_receipt(self):
        expected_projection = projection_proof()
        self.store.register_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
            projection=expected_projection,
        )
        expected_occurrence = legacy_occurrence()
        expected_source = PromptArchiveOccurrenceSource(
            response_key="0123456789abcdefabcd",
            byte_size=len(PAYLOAD),
            sha256=hashlib.sha256(PAYLOAD).hexdigest(),
        )
        self.store.register_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
            occurrence=expected_occurrence,
            source=expected_source,
        )

        occurrence_proof = self.store.load_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
        )
        projection = self.store.load_source_metadata_projection(
            project_id=PROJECT_ID,
            locator=locator(),
        )

        self.assertIsNotNone(occurrence_proof)
        self.assertEqual(occurrence_proof.occurrence, expected_occurrence)
        self.assertEqual(projection, expected_projection)
        self.assertEqual(self.reader_calls, 1)

    def test_projection_and_receipt_round_trip_inside_new_occurrence(self):
        projected = projection_proof()
        expected_occurrence = dataclasses.replace(
            legacy_occurrence(),
            source_metadata_projection=projected.document,
            source_metadata_receipt=projected.receipt,
        )
        expected_source = PromptArchiveOccurrenceSource(
            response_key="0123456789abcdefabcd",
            byte_size=len(PAYLOAD),
            sha256=hashlib.sha256(PAYLOAD).hexdigest(),
        )

        self.store.register_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
            occurrence=expected_occurrence,
            source=expected_source,
        )
        recovered = self.store.load_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
        )

        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.occurrence, expected_occurrence)


if __name__ == "__main__":
    unittest.main()
