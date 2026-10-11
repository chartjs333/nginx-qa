import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from nginx_qa.prompt_archive_incremental import (
    ByteStreamReceipt,
    PromptArchiveIncremental,
    SequenceLeaf,
    SequenceReceipt,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


PROJECT_ID = "9000"
SPRINT_ID = "sprint-sequence-tests"
INDEX_MEDIA_TYPE = "application/vnd.nginx-qa.prompt-archive-index-node-v1+json"
CHUNK_BYTES = 8 * 1024 * 1024
FANOUT = 64


def expected_tree_shape(item_count: int) -> tuple[int, int]:
    """Return (root level, total canonical index nodes) for fanout 64."""

    level = 0
    width = item_count
    nodes = 0
    while True:
        width = (width + FANOUT - 1) // FANOUT
        nodes += width
        if width == 1:
            return level, nodes
        level += 1


class PromptArchiveIncrementalSequenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def new_archive(
        self,
        label: str,
    ) -> tuple[PromptArchiveStorage, PromptArchiveIncremental]:
        storage = PromptArchiveStorage(
            self.base / label,
            PROJECT_ID,
            protected_roots=(),
        )
        return storage, PromptArchiveIncremental(storage, PROJECT_ID, SPRINT_ID)

    @staticmethod
    def put_chunk_leaf(storage: PromptArchiveStorage) -> dict:
        return storage.put_blob(
            b"x",
            logical_kind="raw_chunk",
            media_type="application/json",
        )

    @staticmethod
    def read_index(storage: PromptArchiveStorage, ref) -> dict:
        return json.loads(storage.read_blob(ref).decode("utf-8"))

    @staticmethod
    def put_index(
        storage: PromptArchiveStorage,
        document: dict,
        *,
        canonical: bool = True,
    ) -> dict:
        raw = (
            canonical_json_bytes_v1(document, maximum=1024 * 1024)
            if canonical
            else (json.dumps(document, indent=2) + "\n").encode("utf-8")
        )
        return storage.put_blob(
            raw,
            logical_kind="index_node",
            media_type=INDEX_MEDIA_TYPE,
        )

    def assert_error_code(self, expected: str, operation) -> None:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, expected)

    def collect_index_nodes(
        self,
        storage: PromptArchiveStorage,
        receipt: SequenceReceipt,
    ) -> dict[tuple[int, int, int], str]:
        """Map (level, ordinal start, item count) to digest for a valid tree."""

        result: dict[tuple[int, int, int], str] = {}

        def visit(ref, level: int) -> None:
            document = self.read_index(storage, ref)
            start = document["children"][0]["ordinal_start"]
            key = (level, start, document["item_count"])
            result[key] = ref["digest"]
            if level:
                for child in document["children"]:
                    visit(child["ref"], level - 1)

        visit(receipt.root_ref, receipt.level)
        return result

    def test_byte_stream_boundaries_reconstruct_exact_bytes_and_account_storage(self) -> None:
        fixtures = (
            ("empty", b"", 1, (0,)),
            ("one", b"x", 1, (1,)),
            ("exact-chunk", b"x" * CHUNK_BYTES, 1, (CHUNK_BYTES,)),
            (
                "chunk-plus-one",
                b"x" * (CHUNK_BYTES + 1),
                2,
                (CHUNK_BYTES, 1),
            ),
        )
        for label, payload, chunk_count, chunk_sizes in fixtures:
            with self.subTest(label=label):
                storage, archive = self.new_archive(f"bytes-{label}")
                before_inventory = archive.storage_inventory()
                before_work = archive.work_metrics()

                receipt = archive.store_byte_stream(
                    payload,
                    sequence_kind="raw_bytes",
                )
                after_store_inventory = archive.storage_inventory()
                store_work = archive.work_metrics().delta(before_work)

                self.assertEqual(receipt.byte_size, len(payload))
                self.assertEqual(receipt.sha256, hashlib.sha256(payload).hexdigest())
                self.assertEqual(receipt.chunk_count, chunk_count)
                self.assertEqual(receipt.sequence.item_count, chunk_count)
                self.assertEqual(receipt.sequence.logical_bytes, len(payload))
                self.assertEqual(receipt.sequence.level, 0)
                self.assertEqual(
                    tuple(
                        leaf.logical_bytes
                        for leaf in archive.sequence_leaves(receipt.sequence)
                    ),
                    chunk_sizes,
                )
                self.assertEqual(archive.hydrate_byte_stream(receipt), payload)

                inventory_delta = after_store_inventory.delta(before_inventory)
                self.assertEqual(inventory_delta.file_count, store_work.new_object_count)
                self.assertEqual(inventory_delta.content_bytes, store_work.new_stored_bytes)
                self.assertGreaterEqual(inventory_delta.directory_count, 1)
                self.assertGreaterEqual(inventory_delta.allocated_bytes, 0)
                # One unique chunk per distinct chunk value, plus one index node.
                self.assertEqual(
                    store_work.new_object_count,
                    len(set(chunk_sizes)) + 1,
                )

                retry_inventory = archive.storage_inventory()
                retry_work = archive.work_metrics()
                retried = archive.store_byte_stream(
                    payload,
                    sequence_kind="raw_bytes",
                )
                self.assertEqual(retried, receipt)
                self.assertEqual(
                    archive.storage_inventory().delta(retry_inventory).file_count,
                    0,
                )
                self.assertEqual(
                    archive.storage_inventory().delta(retry_inventory).content_bytes,
                    0,
                )
                retry_delta = archive.work_metrics().delta(retry_work)
                self.assertEqual(retry_delta.new_object_count, 0)
                self.assertEqual(retry_delta.new_stored_bytes, 0)
                self.assertEqual(retry_delta.new_uncompressed_bytes, 0)

    def test_canonical_fanout_tree_at_every_required_boundary(self) -> None:
        # The second fanout level is exercised once, incrementally, below.  Do
        # not rebuild the same 4K-leaf prefix three times: on Windows every
        # typed-leaf validation intentionally performs filesystem identity
        # checks, and redundant builds turn a focused boundary test into an
        # I/O benchmark.
        for item_count in (1, 63, 64, 65, 127, 128):
            with self.subTest(item_count=item_count):
                storage, archive = self.new_archive(f"fanout-{item_count}")
                leaf_ref = self.put_chunk_leaf(storage)
                leaf = SequenceLeaf(leaf_ref, 1)
                before_inventory = archive.storage_inventory()
                before_work = archive.work_metrics()

                receipt = archive.build_sequence(
                    "raw_bytes",
                    (leaf for _ in range(item_count)),
                )
                self.assertIsNotNone(receipt)
                assert receipt is not None

                expected_level, expected_nodes = expected_tree_shape(item_count)
                self.assertEqual(receipt.level, expected_level)
                self.assertEqual(receipt.item_count, item_count)
                self.assertEqual(receipt.logical_bytes, item_count)
                leaves = archive.sequence_leaves(receipt)
                self.assertEqual(len(leaves), item_count)
                self.assertTrue(all(value.ref == leaf_ref for value in leaves))
                self.assertTrue(all(value.logical_bytes == 1 for value in leaves))

                work = archive.work_metrics().delta(before_work)
                inventory = archive.storage_inventory().delta(before_inventory)
                self.assertEqual(work.index_nodes_serialized, expected_nodes)
                self.assertEqual(work.new_object_count, expected_nodes)
                self.assertEqual(inventory.file_count, expected_nodes)
                self.assertEqual(inventory.content_bytes, work.new_stored_bytes)

                nodes = self.collect_index_nodes(storage, receipt)
                self.assertEqual(len(nodes), expected_nodes)
                for (level, start, count), _digest in nodes.items():
                    self.assertEqual(start % (FANOUT ** (level + 1)), 0)
                    self.assertLessEqual(count, FANOUT ** (level + 1))
                    if start + count < item_count:
                        self.assertEqual(count, FANOUT ** (level + 1))

    def test_append_rewrites_only_right_spine_and_is_idempotent(self) -> None:
        for item_count in (1, 63, 64, 65, 127, 128):
            with self.subTest(item_count=item_count):
                storage, archive = self.new_archive(f"append-{item_count}")
                leaf_ref = self.put_chunk_leaf(storage)
                leaf = SequenceLeaf(leaf_ref, 1)
                previous = archive.build_sequence(
                    "raw_bytes",
                    (leaf for _ in range(item_count)),
                )
                assert previous is not None
                old_nodes = self.collect_index_nodes(storage, previous)
                before_inventory = archive.storage_inventory()
                before_work = archive.work_metrics()

                appended = archive.append_sequence(
                    previous,
                    leaf,
                    sequence_kind="raw_bytes",
                )

                delta = archive.work_metrics().delta(before_work)
                inventory_delta = archive.storage_inventory().delta(before_inventory)
                root_capacity = FANOUT ** (previous.level + 1)
                height_grew = item_count == root_capacity
                expected_serialized = previous.level + (2 if height_grew else 1)
                # Read-side work may scrub the entire untrusted predecessor
                # graph before publication.  The O(delta) contract is about
                # newly serialized/physical bytes, so only write-side metrics
                # are bounded to the copied right spine here.
                self.assertEqual(delta.index_nodes_serialized, expected_serialized)
                self.assertEqual(delta.new_object_count, expected_serialized)
                self.assertEqual(inventory_delta.file_count, expected_serialized)
                self.assertEqual(inventory_delta.content_bytes, delta.new_stored_bytes)
                self.assertEqual(appended.item_count, item_count + 1)
                self.assertEqual(appended.logical_bytes, item_count + 1)
                self.assertEqual(
                    appended.level,
                    previous.level + (1 if height_grew else 0),
                )
                self.assertEqual(len(archive.sequence_leaves(appended)), item_count + 1)

                new_nodes = self.collect_index_nodes(storage, appended)
                old_digests = set(old_nodes.values())
                new_digests = set(new_nodes.values())
                if height_grew:
                    # A full old root becomes the complete left child verbatim.
                    self.assertLessEqual(old_digests, new_digests)
                else:
                    right_spine = {
                        digest
                        for (level, start, count), digest in old_nodes.items()
                        if start + count == item_count
                    }
                    self.assertLessEqual(old_digests - right_spine, new_digests)

                # Replaying the same append from the same predecessor must yield
                # the same root and no additional physical CAS objects.
                retry_inventory = archive.storage_inventory()
                retry_work = archive.work_metrics()
                retried = archive.append_sequence(
                    previous,
                    leaf,
                    sequence_kind="raw_bytes",
                )
                self.assertEqual(retried, appended)
                retry_delta = archive.work_metrics().delta(retry_work)
                self.assertEqual(retry_delta.index_nodes_serialized, expected_serialized)
                self.assertEqual(retry_delta.new_object_count, 0)
                self.assertEqual(retry_delta.new_stored_bytes, 0)
                self.assertEqual(
                    archive.storage_inventory().delta(retry_inventory).file_count,
                    0,
                )

    def test_second_level_boundaries_4095_4096_4097_reuse_the_complete_prefix(
        self,
    ) -> None:
        storage, archive = self.new_archive("second-level-boundaries")
        leaf_ref = self.put_chunk_leaf(storage)
        leaf = SequenceLeaf(leaf_ref, 1)
        # This test exercises 4K fanout topology, persistent writes, and real
        # filesystem inventory. Cache only the one immutable synthetic record
        # body so the required full predecessor scrub can parse and validate it
        # at every ordinal without performing ~16K identical disk reads. Small
        # boundary and corruption tests above use the real CAS read path.
        leaf_bytes = storage.read_blob(leaf_ref)
        real_read_blob = storage.read_blob

        def read_with_fixture_cache(ref, *args, **kwargs):
            if isinstance(ref, dict) and ref.get("digest") == leaf_ref["digest"]:
                return leaf_bytes
            return real_read_blob(ref, *args, **kwargs)

        read_patch = mock.patch.object(
            storage,
            "read_blob",
            side_effect=read_with_fixture_cache,
        )
        read_patch.start()
        self.addCleanup(read_patch.stop)
        before_4095 = archive.work_metrics()
        at_4095 = archive.build_sequence(
            "raw_bytes",
            (leaf for _ in range(4095)),
        )
        assert at_4095 is not None
        build_4095 = archive.work_metrics().delta(before_4095)
        self.assertEqual(
            (at_4095.level, at_4095.item_count),
            (1, 4095),
        )
        self.assertEqual(
            len(self.collect_index_nodes(storage, at_4095)),
            expected_tree_shape(4095)[1],
        )

        before_4096 = archive.work_metrics()
        inventory_4095 = archive.storage_inventory()
        at_4096 = archive.append_sequence(
            at_4095,
            leaf,
            sequence_kind="raw_bytes",
        )
        delta_4096 = archive.work_metrics().delta(before_4096)
        files_4096 = archive.storage_inventory().delta(inventory_4095)
        _level, full_rebuild_nodes = expected_tree_shape(4096)

        self.assertEqual((at_4096.level, at_4096.item_count), (1, 4096))
        self.assertEqual(delta_4096.index_nodes_serialized, 2)
        self.assertEqual(delta_4096.new_object_count, 2)
        self.assertEqual(files_4096.file_count, 2)
        self.assertLess(delta_4096.index_nodes_serialized, full_rebuild_nodes)
        self.assertLess(
            delta_4096.control_bytes_serialized,
            build_4095.control_bytes_serialized,
        )
        self.assertEqual(
            len(self.collect_index_nodes(storage, at_4096)),
            full_rebuild_nodes,
        )

        nodes_4096 = set(self.collect_index_nodes(storage, at_4096).values())
        before_4097 = archive.work_metrics()
        inventory_4096 = archive.storage_inventory()
        at_4097 = archive.append_sequence(
            at_4096,
            leaf,
            sequence_kind="raw_bytes",
        )
        delta_4097 = archive.work_metrics().delta(before_4097)
        files_4097 = archive.storage_inventory().delta(inventory_4096)

        self.assertEqual((at_4097.level, at_4097.item_count), (2, 4097))
        self.assertEqual(delta_4097.index_nodes_serialized, 3)
        self.assertEqual(delta_4097.new_object_count, 3)
        self.assertEqual(files_4097.file_count, 3)
        self.assertLessEqual(
            nodes_4096,
            set(self.collect_index_nodes(storage, at_4097).values()),
        )
        self.assertEqual(
            len(self.collect_index_nodes(storage, at_4097)),
            expected_tree_shape(4097)[1],
        )

        retry_inventory = archive.storage_inventory()
        retry_work = archive.work_metrics()
        retried = archive.append_sequence(at_4096, leaf, sequence_kind="raw_bytes")
        self.assertEqual(retried, at_4097)
        self.assertEqual(
            archive.work_metrics().delta(retry_work).new_object_count,
            0,
        )
        self.assertEqual(
            archive.storage_inventory().delta(retry_inventory).file_count,
            0,
        )

    def test_sequence_reader_rejects_wrong_kind_media_ordinals_totals_and_layout(self) -> None:
        storage, archive = self.new_archive("invalid-indexes")
        leaf_ref = self.put_chunk_leaf(storage)
        leaf = SequenceLeaf(leaf_ref, 1)
        valid_one = archive.build_sequence("raw_bytes", [leaf])
        valid_sixty_three = archive.build_sequence("raw_bytes", [leaf] * 63)
        self.assertIsNotNone(valid_one)
        self.assertIsNotNone(valid_sixty_three)
        assert valid_one is not None
        assert valid_sixty_three is not None

        wrong_leaf = storage.put_blob(
            b"{}",
            logical_kind="event_record",
            media_type="application/vnd.nginx-qa.prompt-archive-record-v1+json",
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.build_sequence("raw_bytes", [SequenceLeaf(wrong_leaf, 1)]),
        )

        wrong_kind_receipt = SequenceReceipt(
            valid_one.root_ref,
            "patches",
            valid_one.level,
            valid_one.item_count,
            valid_one.logical_bytes,
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.sequence_leaves(wrong_kind_receipt),
        )

        wrong_media_ref = dict(valid_one.root_ref)
        wrong_media_ref["media_type"] = "application/json"
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.sequence_leaves(
                SequenceReceipt(wrong_media_ref, "raw_bytes", 0, 1, 1)
            ),
        )

        def leaf_child(ordinal: int, *, size: int = 1) -> dict:
            return {
                "ordinal_start": ordinal,
                "item_count": 1,
                "logical_bytes": size,
                "ref": leaf_ref,
            }

        malformed_documents = {
            "ordinal-gap": {
                "schema_version": 1,
                "kind": "prompt_archive_index_node",
                "sequence_kind": "raw_bytes",
                "level": 0,
                "item_count": 2,
                "logical_bytes": 2,
                "children": [leaf_child(0), leaf_child(2)],
            },
            "wrong-item-total": {
                "schema_version": 1,
                "kind": "prompt_archive_index_node",
                "sequence_kind": "raw_bytes",
                "level": 0,
                "item_count": 2,
                "logical_bytes": 1,
                "children": [leaf_child(0)],
            },
            "wrong-byte-total": {
                "schema_version": 1,
                "kind": "prompt_archive_index_node",
                "sequence_kind": "raw_bytes",
                "level": 0,
                "item_count": 1,
                "logical_bytes": 2,
                "children": [leaf_child(0)],
            },
            "noncanonical-partial-left-child": {
                "schema_version": 1,
                "kind": "prompt_archive_index_node",
                "sequence_kind": "raw_bytes",
                "level": 1,
                "item_count": 64,
                "logical_bytes": 64,
                "children": [
                    {
                        "ordinal_start": 0,
                        "item_count": 63,
                        "logical_bytes": 63,
                        "ref": valid_sixty_three.root_ref,
                    },
                    {
                        "ordinal_start": 63,
                        "item_count": 1,
                        "logical_bytes": 1,
                        "ref": valid_one.root_ref,
                    },
                ],
            },
            "noncanonical-single-child-root": {
                "schema_version": 1,
                "kind": "prompt_archive_index_node",
                "sequence_kind": "raw_bytes",
                "level": 1,
                "item_count": 1,
                "logical_bytes": 1,
                "children": [
                    {
                        "ordinal_start": 0,
                        "item_count": 1,
                        "logical_bytes": 1,
                        "ref": valid_one.root_ref,
                    }
                ],
            },
        }
        for label, document in malformed_documents.items():
            with self.subTest(label=label):
                ref = self.put_index(storage, document)
                receipt = SequenceReceipt(
                    ref,
                    "raw_bytes",
                    document["level"],
                    document["item_count"],
                    document["logical_bytes"],
                )
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda receipt=receipt: archive.sequence_leaves(receipt),
                )

        canonical_document = self.read_index(storage, valid_one.root_ref)
        noncanonical_ref = self.put_index(
            storage,
            canonical_document,
            canonical=False,
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.sequence_leaves(
                SequenceReceipt(noncanonical_ref, "raw_bytes", 0, 1, 1)
            ),
        )

        wrong_receipt_total = SequenceReceipt(
            valid_one.root_ref,
            "raw_bytes",
            0,
            2,
            1,
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.sequence_leaves(wrong_receipt_total),
        )

    def test_byte_stream_receipt_integrity_fields_are_authoritative(self) -> None:
        _storage, archive = self.new_archive("byte-integrity")
        receipt = archive.store_byte_stream(b"payload", sequence_kind="raw_bytes")

        bad_size = ByteStreamReceipt(
            receipt.sequence,
            receipt.byte_size + 1,
            receipt.sha256,
            receipt.chunk_count,
        )
        bad_digest = ByteStreamReceipt(
            receipt.sequence,
            receipt.byte_size,
            "0" * 64,
            receipt.chunk_count,
        )
        bad_count = ByteStreamReceipt(
            receipt.sequence,
            receipt.byte_size,
            receipt.sha256,
            receipt.chunk_count + 1,
        )
        for label, malformed, expected in (
            ("size", bad_size, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"),
            ("digest", bad_digest, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH"),
            ("count", bad_count, "PROMPT_ARCHIVE_REFERENCE_INVALID"),
        ):
            with self.subTest(label=label):
                self.assert_error_code(
                    expected,
                    lambda malformed=malformed: archive.hydrate_byte_stream(malformed),
                )


if __name__ == "__main__":
    unittest.main()
