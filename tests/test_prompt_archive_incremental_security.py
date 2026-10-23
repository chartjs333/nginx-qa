import dataclasses
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from nginx_qa.prompt_archive_incremental import (
    PromptArchiveIncremental,
    SequenceLeaf,
    SequenceReceipt,
    _load_canonical_control,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


_INDEX_MEDIA_TYPE = "application/vnd.nginx-qa.prompt-archive-index-node-v1+json"


class _ExistsProbe:
    def exists(self) -> bool:
        raise AssertionError("incremental metrics bypassed the storage safety boundary")


class PromptArchiveIncrementalSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="prompt-archive-incremental-")
        self.root = Path(self.temporary.name).resolve()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def storage(
        self,
        project_id: str = "9000",
        *,
        limits: PromptArchiveLimits | None = None,
    ) -> PromptArchiveStorage:
        return PromptArchiveStorage(
            self.root,
            project_id,
            protected_roots=(),
            limits=limits,
        )

    def incremental(
        self,
        storage: PromptArchiveStorage,
        project_id: str = "9000",
    ) -> PromptArchiveIncremental:
        return PromptArchiveIncremental(storage, project_id, "sprint-security-test")

    def assert_archive_error(self, expected_code: str):
        return self.assertRaisesRegex(
            PromptArchiveStorageError,
            "^Prompt archive is unavailable for this request\\.$",
        )

    @staticmethod
    def assert_error_code(
        context: unittest.case._AssertRaisesContext,
        expected_code: str,
    ) -> None:
        if context.exception.code != expected_code:
            raise AssertionError(
                f"expected {expected_code}, observed {context.exception.code}"
            )

    def test_sequence_rejects_missing_leaf_before_publishing_parent(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        leaf_ref = storage.put_blob(
            b"leaf",
            logical_kind="raw_chunk",
            media_type="application/json",
        )
        storage.blob_path(leaf_ref).unlink()
        before = incremental.storage_inventory()

        with self.assert_archive_error("PROMPT_ARCHIVE_BLOB_MISSING") as raised:
            incremental.build_sequence(
                "raw_bytes",
                [SequenceLeaf(leaf_ref, len(b"leaf"))],
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_BLOB_MISSING")
        self.assertEqual(incremental.storage_inventory(), before)

    def test_sequence_rejects_leaf_logical_size_mismatch(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        leaf_ref = storage.put_blob(
            b"x",
            logical_kind="raw_chunk",
            media_type="application/json",
        )

        with self.assert_archive_error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") as raised:
            incremental.build_sequence(
                "raw_bytes",
                [SequenceLeaf(leaf_ref, 999)],
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def test_append_rejects_missing_fully_shaped_left_subtree(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        leaf_ref = storage.put_blob(
            b"x",
            logical_kind="raw_chunk",
            media_type="application/json",
        )
        left_children = [
            {
                "ordinal_start": ordinal,
                "item_count": 1,
                "logical_bytes": 1,
                "ref": leaf_ref,
            }
            for ordinal in range(64)
        ]
        _left_document, left_ref = incremental._index_document(
            "raw_bytes",
            0,
            left_children,
        )
        storage.blob_path(left_ref).unlink()
        _right_document, right_ref = incremental._index_document(
            "raw_bytes",
            0,
            [
                {
                    "ordinal_start": 64,
                    "item_count": 1,
                    "logical_bytes": 1,
                    "ref": leaf_ref,
                }
            ],
        )
        forged_root = {
            "schema_version": 1,
            "kind": "prompt_archive_index_node",
            "sequence_kind": "raw_bytes",
            "level": 1,
            "item_count": 65,
            "logical_bytes": 65,
            "children": [
                {
                    "ordinal_start": 0,
                    "item_count": 64,
                    "logical_bytes": 64,
                    "ref": left_ref,
                },
                {
                    "ordinal_start": 64,
                    "item_count": 1,
                    "logical_bytes": 1,
                    "ref": right_ref,
                },
            ],
        }
        forged_root_ref = storage.put_blob(
            canonical_json_bytes_v1(forged_root, maximum=8 * 1024 * 1024),
            logical_kind="index_node",
            media_type=_INDEX_MEDIA_TYPE,
        )
        previous = SequenceReceipt(forged_root_ref, "raw_bytes", 1, 65, 65)
        before = incremental.storage_inventory()

        with self.assert_archive_error("PROMPT_ARCHIVE_BLOB_MISSING") as raised:
            incremental.append_sequence(
                previous,
                SequenceLeaf(leaf_ref, 1),
                sequence_kind="raw_bytes",
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_BLOB_MISSING")
        self.assertEqual(incremental.storage_inventory(), before)

    def test_append_rechecks_cached_predecessor_after_left_subtree_corruption(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        payloads = [f"leaf-{ordinal}".encode("ascii") for ordinal in range(66)]
        refs = [
            storage.put_blob(
                payload,
                logical_kind="raw_chunk",
                media_type="application/json",
            )
            for payload in payloads
        ]
        previous = incremental.build_sequence(
            "raw_bytes",
            [
                SequenceLeaf(ref, len(payload))
                for ref, payload in zip(refs[:65], payloads[:65], strict=True)
            ],
        )
        self.assertIsNotNone(previous)
        storage.blob_path(refs[0]).unlink()

        with self.assert_archive_error("PROMPT_ARCHIVE_BLOB_MISSING") as raised:
            incremental.append_sequence(
                previous,
                SequenceLeaf(refs[65], len(payloads[65])),
                sequence_kind="raw_bytes",
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_BLOB_MISSING")

    def test_sequence_enforces_unique_reference_budget(self) -> None:
        storage = self.storage(
            limits=PromptArchiveLimits(max_referenced_objects=1),
        )
        incremental = self.incremental(storage)
        leaf_ref = storage.put_blob(
            b"x",
            logical_kind="raw_chunk",
            media_type="application/json",
        )

        with self.assert_archive_error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") as raised:
            incremental.build_sequence(
                "raw_bytes",
                [SequenceLeaf(leaf_ref, 1), SequenceLeaf(leaf_ref, 1)],
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_sequence_enforces_aggregate_hydrated_byte_budget(self) -> None:
        storage = self.storage(
            limits=PromptArchiveLimits(max_hydrated_bytes=9 * 1024 * 1024),
        )
        incremental = self.incremental(storage)
        first = b"a" * (8 * 1024 * 1024)
        second = b"b" * (2 * 1024 * 1024)
        first_ref = storage.put_blob(
            first,
            logical_kind="raw_chunk",
            media_type="application/json",
        )
        second_ref = storage.put_blob(
            second,
            logical_kind="raw_chunk",
            media_type="application/json",
        )

        with self.assert_archive_error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") as raised:
            incremental.build_sequence(
                "raw_bytes",
                [SequenceLeaf(first_ref, len(first)), SequenceLeaf(second_ref, len(second))],
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_deep_control_document_maps_recursion_to_stable_limit_error(self) -> None:
        depth = 2500
        raw = (b'{"nested":' * depth) + b"0" + (b"}" * depth)

        with self.assert_archive_error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") as raised:
            _load_canonical_control(raw)
        self.assert_error_code(raised, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_incremental_project_must_match_storage_project(self) -> None:
        storage = self.storage(project_id="9000")

        with self.assert_archive_error("PROMPT_ARCHIVE_REQUEST_INVALID") as raised:
            PromptArchiveIncremental(storage, "9001", "sprint-security-test")
        self.assert_error_code(raised, "PROMPT_ARCHIVE_REQUEST_INVALID")

    def test_concurrent_winner_is_not_counted_as_created_by_this_writer(self) -> None:
        storage = self.storage()
        competitor = self.storage()
        incremental = self.incremental(storage)
        original_put = storage.put_blob_with_outcome

        def race(data, *, logical_kind, media_type):
            competitor.put_blob(
                data,
                logical_kind=logical_kind,
                media_type=media_type,
            )
            return original_put(
                data,
                logical_kind=logical_kind,
                media_type=media_type,
            )

        before = incremental.work_metrics()
        with mock.patch.object(storage, "put_blob_with_outcome", side_effect=race):
            incremental._put_blob(
                b"competitor-wins",
                logical_kind="raw_chunk",
                media_type="application/json",
            )
        delta = incremental.work_metrics().delta(before)
        self.assertEqual(delta.new_object_count, 0)
        self.assertEqual(delta.new_stored_bytes, 0)
        self.assertEqual(delta.new_uncompressed_bytes, 0)

    def test_metrics_do_not_probe_manual_digest_path_before_cas(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)

        with mock.patch.object(
            incremental,
            "_digest_path",
            return_value=_ExistsProbe(),
            create=True,
        ):
            ref = incremental._put_blob(
                b"root-safety",
                logical_kind="raw_chunk",
                media_type="application/json",
            )
        storage.verify_blob(ref)

    def test_storage_inventory_is_project_scoped(self) -> None:
        storage = self.storage(project_id="9000")
        incremental = self.incremental(storage, project_id="9000")
        storage.put_blob(
            b"project-9000",
            logical_kind="raw_chunk",
            media_type="application/json",
        )
        before = incremental.storage_inventory()

        other = self.storage(project_id="9001")
        other.put_blob(
            b"project-9001",
            logical_kind="raw_chunk",
            media_type="application/json",
        )

        self.assertEqual(incremental.storage_inventory(), before)

    def test_recursive_json_enforces_complete_graph_reference_budget(self) -> None:
        storage = self.storage(
            limits=PromptArchiveLimits(max_referenced_objects=14),
        )
        incremental = self.incremental(storage)

        with self.assert_archive_error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") as raised:
            incremental.store_json_value(b'{"a":1,"b":2}')
        self.assert_error_code(raised, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_recursive_json_enforces_complete_serialized_reference_depth(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        receipt = incremental.store_json_value(b"[1]")

        with self.assert_archive_error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") as raised:
            incremental._hydrate_json_value(
                receipt.ref,
                _base_reference_depth=511,
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_recursive_json_validates_incoming_type_before_memo_reuse(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        receipt = incremental.store_json_value(b"[1,1]")
        root = incremental._read_control(receipt.ref, logical_kind="json_value")
        entries = incremental._receipt_from_root(root["entries_root"], "json_edges")
        leaves = incremental.sequence_leaves(entries)
        second_edge = incremental._read_control(
            leaves[1].ref,
            logical_kind="json_edge",
        )
        second_edge["value_ref"] = dict(second_edge["value_ref"])
        second_edge["value_ref"]["logical_kind"] = "json_edge"
        second_edge["value_ref"]["media_type"] = (
            "application/vnd.nginx-qa.prompt-archive-json-edge-v1+json"
        )
        forged_edge_ref = incremental._put_control(second_edge, "json_edge")
        forged_entries = incremental.build_sequence(
            "json_edges",
            [
                leaves[0],
                SequenceLeaf(forged_edge_ref, leaves[1].logical_bytes),
            ],
        )
        self.assertIsNotNone(forged_entries)
        root["entries_root"] = dict(forged_entries.root_ref)
        forged_root_ref = incremental._put_control(root, "json_value")

        with self.assert_archive_error("PROMPT_ARCHIVE_REFERENCE_INVALID") as raised:
            incremental.hydrate_json_value(forged_root_ref)
        self.assert_error_code(raised, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_recursive_json_validates_physical_ref_before_memo_reuse(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        receipt = incremental.store_json_value(b"[1,1]")
        root = incremental._read_control(receipt.ref, logical_kind="json_value")
        entries = incremental._receipt_from_root(root["entries_root"], "json_edges")
        leaves = incremental.sequence_leaves(entries)
        second_edge = incremental._read_control(
            leaves[1].ref,
            logical_kind="json_edge",
        )
        second_edge["value_ref"] = dict(second_edge["value_ref"])
        second_edge["value_ref"]["stored_size"] += 1
        second_edge["value_ref"]["uncompressed_size"] += 1
        forged_edge_ref = incremental._put_control(second_edge, "json_edge")
        forged_entries = incremental.build_sequence(
            "json_edges",
            [
                leaves[0],
                SequenceLeaf(forged_edge_ref, leaves[1].logical_bytes),
            ],
        )
        self.assertIsNotNone(forged_entries)
        root["entries_root"] = dict(forged_entries.root_ref)
        forged_root_ref = incremental._put_control(root, "json_value")

        with self.assert_archive_error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") as raised:
            incremental.hydrate_json_value(forged_root_ref)
        self.assert_error_code(raised, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def test_overlapping_public_calls_do_not_share_read_cache(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        receipt = incremental.store_json_value(b'{"value":1}')
        root_path = storage.blob_path(receipt.ref)
        first_hydrated = threading.Event()
        release_first = threading.Event()
        original_hydrate = incremental._hydrate_json_value
        call_lock = threading.Lock()
        call_count = 0

        def pausing_hydrate(*args, **kwargs):
            nonlocal call_count
            with call_lock:
                call_count += 1
                ordinal = call_count
            result = original_hydrate(*args, **kwargs)
            if ordinal == 1:
                first_hydrated.set()
                if not release_first.wait(timeout=5):
                    raise AssertionError("timed out waiting to release first hydration")
            return result

        outcomes: list[tuple[str, object]] = []

        def invoke(label: str) -> None:
            try:
                outcomes.append((label, incremental.hydrate_json_value(receipt)))
            except BaseException as error:  # captured for deterministic thread assertions
                outcomes.append((label, error))

        with mock.patch.object(
            incremental,
            "_hydrate_json_value",
            side_effect=pausing_hydrate,
        ):
            first = threading.Thread(target=invoke, args=("first",))
            first.start()
            self.assertTrue(first_hydrated.wait(timeout=5))
            root_path.unlink()
            second = threading.Thread(target=invoke, args=("second",))
            second.start()
            time.sleep(0.05)
            self.assertTrue(second.is_alive(), "second call bypassed operation lock")
            release_first.set()
            first.join(timeout=5)
            second.join(timeout=5)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        by_label = dict(outcomes)
        self.assertEqual(by_label["first"], b'{"value":1}')
        self.assertIsInstance(by_label["second"], PromptArchiveStorageError)
        self.assertEqual(by_label["second"].code, "PROMPT_ARCHIVE_BLOB_MISSING")

    def test_diagnostics_wait_for_active_publication_operation(self) -> None:
        writer_paused = threading.Event()
        release_writer = threading.Event()
        paused_once = False

        def fault(stage: str) -> None:
            nonlocal paused_once
            if stage == "after_temp_create" and not paused_once:
                paused_once = True
                writer_paused.set()
                if not release_writer.wait(timeout=5):
                    raise AssertionError("timed out waiting to release writer")

        storage = PromptArchiveStorage(
            self.root,
            "9000",
            protected_roots=(),
            fault_hook=fault,
        )
        incremental = self.incremental(storage)
        outcomes: dict[str, object] = {}

        def write() -> None:
            try:
                outcomes["write"] = incremental.store_byte_stream(
                    b"publication",
                    sequence_kind="raw_bytes",
                )
            except BaseException as error:
                outcomes["write"] = error

        def inventory() -> None:
            try:
                outcomes["inventory"] = incremental.storage_inventory()
            except BaseException as error:
                outcomes["inventory"] = error

        def metrics() -> None:
            try:
                outcomes["metrics"] = incremental.work_metrics()
            except BaseException as error:
                outcomes["metrics"] = error

        writer = threading.Thread(target=write)
        writer.start()
        self.assertTrue(writer_paused.wait(timeout=5))
        inventory_reader = threading.Thread(target=inventory)
        metrics_reader = threading.Thread(target=metrics)
        inventory_reader.start()
        metrics_reader.start()
        time.sleep(0.05)
        self.assertTrue(inventory_reader.is_alive())
        self.assertTrue(metrics_reader.is_alive())
        release_writer.set()
        for thread in (writer, inventory_reader, metrics_reader):
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())

        self.assertFalse(any(isinstance(value, BaseException) for value in outcomes.values()))
        inventory_result = outcomes["inventory"]
        metrics_result = outcomes["metrics"]
        self.assertGreaterEqual(inventory_result.file_count, 2)
        self.assertGreaterEqual(metrics_result.new_object_count, 2)

    def test_recursive_json_rejects_edge_kind_incompatible_with_parent(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        receipt = incremental.store_json_value(b"[1]")
        root = incremental._read_control(receipt.ref, logical_kind="json_value")
        entries = incremental._receipt_from_root(root["entries_root"], "json_edges")
        leaf = incremental.sequence_leaves(entries)[0]
        edge = incremental._read_control(leaf.ref, logical_kind="json_edge")
        edge["edge_kind"] = "object_member"
        edge["key_token"] = edge["separator_before"]
        edge["key_value_separator"] = edge["separator_before"]
        forged_edge_ref = incremental._put_control(edge, "json_edge")
        forged_entries = incremental.build_sequence(
            "json_edges",
            [SequenceLeaf(forged_edge_ref, leaf.logical_bytes)],
        )
        self.assertIsNotNone(forged_entries)
        root["entries_root"] = dict(forged_entries.root_ref)
        forged_root_ref = incremental._put_control(root, "json_value")

        with self.assert_archive_error("PROMPT_ARCHIVE_REFERENCE_INVALID") as raised:
            incremental.hydrate_json_value(forged_root_ref)
        self.assert_error_code(raised, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_recursive_json_rejects_alternate_exact_byte_span_partition(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        receipt = incremental.store_json_value(b'{"a":1}')
        root = incremental._read_control(receipt.ref, logical_kind="json_value")
        entries = incremental._receipt_from_root(root["entries_root"], "json_edges")
        leaf = incremental.sequence_leaves(entries)[0]
        edge = incremental._read_control(leaf.ref, logical_kind="json_edge")
        key_with_colon = incremental.store_byte_stream(
            b'"a":',
            sequence_kind="literal_chunks",
        )
        empty = incremental.store_byte_stream(
            b"",
            sequence_kind="literal_chunks",
        )
        edge["key_token"] = incremental._span_document(key_with_colon)
        edge["key_value_separator"] = incremental._span_document(empty)
        forged_edge_ref = incremental._put_control(edge, "json_edge")
        forged_entries = incremental.build_sequence(
            "json_edges",
            [SequenceLeaf(forged_edge_ref, leaf.logical_bytes)],
        )
        self.assertIsNotNone(forged_entries)
        root["entries_root"] = dict(forged_entries.root_ref)
        forged_root_ref = incremental._put_control(root, "json_value")

        with self.assert_archive_error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") as raised:
            incremental.hydrate_json_value(forged_root_ref)
        self.assert_error_code(raised, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def test_source_metadata_rejects_unredacted_fixed_secret_shape(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        source_metadata = canonical_json_bytes_v1(
            {
                "schema_version": 1,
                "kind": "prompt_archive_source_metadata",
                "projection_version": "prompt-archive-source-projection-v1",
                "redaction_version": "prompt-archive-source-redaction-v1",
                "source_type": "email",
                "title": "https://alice:hunter2@example.test/x",
                "redaction_count": 0,
                "redacted_fields": [],
                "discarded_field_count": 0,
            },
            maximum=16 * 1024,
        )

        with self.assert_archive_error("PROMPT_ARCHIVE_REFERENCE_INVALID") as raised:
            incremental.build_record(
                "source_metadata",
                source_metadata,
                ordinal=0,
                provenance={
                    "source_kind": "synthetic_test",
                    "origin_response_id": "parresp1-" + ("a" * 64),
                    "origin_transition_id": None,
                },
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_record_rejects_control_metadata_before_payload_publication(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        valid_provenance = {
            "source_kind": "synthetic_test",
            "origin_response_id": "parresp1-" + ("a" * 64),
            "origin_transition_id": None,
        }
        cases = (
            ("invalid provenance", {"source_kind": "synthetic_test"}, None),
            ("invalid occurred_at", valid_provenance, "2026-99-99T99:99:99Z"),
        )

        for label, provenance, occurred_at in cases:
            with self.subTest(label=label):
                before_inventory = incremental.storage_inventory()
                before_objects = incremental.work_metrics().new_object_count
                with self.assert_archive_error(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID"
                ) as raised:
                    incremental.build_record(
                        "event",
                        b'{"large":"' + (b"x" * 100_000) + b'"}',
                        ordinal=0,
                        provenance=provenance,
                        occurred_at=occurred_at,
                    )
                self.assert_error_code(raised, "PROMPT_ARCHIVE_REFERENCE_INVALID")
                self.assertEqual(incremental.storage_inventory(), before_inventory)
                self.assertEqual(
                    incremental.work_metrics().new_object_count,
                    before_objects,
                )

    def test_append_validates_previous_before_reading_new_leaf(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        leaf_ref = storage.put_blob(
            b"leaf",
            logical_kind="raw_chunk",
            media_type="application/json",
        )

        with mock.patch.object(
            storage,
            "read_blob",
            side_effect=AssertionError("new leaf was read before previous validation"),
        ):
            with self.assert_archive_error(
                "PROMPT_ARCHIVE_REFERENCE_INVALID"
            ) as raised:
                incremental.append_sequence(
                    {},
                    SequenceLeaf(leaf_ref, len(b"leaf")),
                    sequence_kind="raw_bytes",
                )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_source_metadata_accepts_nonregistered_long_safe_display(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        source_metadata = canonical_json_bytes_v1(
            {
                "schema_version": 1,
                "kind": "prompt_archive_source_metadata",
                "projection_version": "prompt-archive-source-projection-v1",
                "redaction_version": "prompt-archive-source-redaction-v1",
                "source_type": "manual",
                "title": "a" * 43,
                "redaction_count": 0,
                "redacted_fields": [],
                "discarded_field_count": 0,
            },
            maximum=16 * 1024,
        )
        record = incremental.build_record(
            "source_metadata",
            source_metadata,
            ordinal=0,
            provenance={
                "source_kind": "synthetic_test",
                "origin_response_id": "parresp1-" + ("a" * 64),
                "origin_transition_id": None,
            },
        )

        self.assertEqual(incremental.hydrate_record(record), source_metadata)

    def test_record_sequence_enforces_aggregate_graph_reference_budget(self) -> None:
        storage = self.storage(
            limits=PromptArchiveLimits(max_referenced_objects=17),
        )
        incremental = self.incremental(storage)
        provenance = {
            "source_kind": "synthetic_test",
            "origin_response_id": "parresp1-" + ("a" * 64),
            "origin_transition_id": None,
        }
        first = incremental.build_record(
            "event",
            b'{"x":1}',
            ordinal=0,
            provenance=provenance,
        )
        second = incremental.build_record(
            "event",
            b'{"y":2}',
            ordinal=1,
            provenance=provenance,
        )
        before = incremental.storage_inventory()

        with self.assert_archive_error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") as raised:
            incremental.build_record_sequence("events", [first, second])
        self.assert_error_code(raised, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        self.assertEqual(incremental.storage_inventory(), before)

    def test_record_sequence_composes_depth_with_record_payload_graph(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        record = incremental.build_record(
            "event",
            b"1",
            ordinal=0,
            provenance={
                "source_kind": "synthetic_test",
                "origin_response_id": "parresp1-" + ("a" * 64),
                "origin_transition_id": None,
            },
        )
        sequence = incremental.build_record_sequence("events", [record])
        self.assertIsNotNone(sequence)
        assert sequence is not None

        with self.assert_archive_error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") as raised:
            incremental._sequence_leaves(
                sequence,
                _base_reference_depth=510,
            )
        self.assert_error_code(raised, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_record_receipt_cannot_weaken_expected_type(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        record = incremental.build_record(
            "event",
            b"1",
            ordinal=0,
            provenance={
                "source_kind": "synthetic_test",
                "origin_response_id": "parresp1-" + ("a" * 64),
                "origin_transition_id": None,
            },
        )

        with self.assert_archive_error("PROMPT_ARCHIVE_REFERENCE_INVALID") as raised:
            incremental.hydrate_record(record, expected_record_type="patch")
        self.assert_error_code(raised, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_record_receipt_binds_sequence_and_payload_root_metadata(self) -> None:
        storage = self.storage()
        incremental = self.incremental(storage)
        record = incremental.build_record(
            "event",
            b"1",
            ordinal=0,
            provenance={
                "source_kind": "synthetic_test",
                "origin_response_id": "parresp1-" + ("a" * 64),
                "origin_transition_id": None,
            },
        )
        cases = (
            dataclasses.replace(record, sequence_kind="patches"),
            dataclasses.replace(record, payload_root_digest="b" * 64),
        )

        for forged in cases:
            with self.subTest(forged=forged):
                with self.assert_archive_error(
                    "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"
                ) as raised:
                    incremental.hydrate_record(forged)
                self.assert_error_code(raised, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def test_reference_depth_is_a_fixed_v1_structural_constant(self) -> None:
        with self.assertRaisesRegex(ValueError, "fixed at 512"):
            PromptArchiveLimits(max_reference_depth=511)


if __name__ == "__main__":
    unittest.main()
