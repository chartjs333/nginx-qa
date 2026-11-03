"""Bounded gzip finalization regressions using synthetic isolated blobs only."""

import copy
import dataclasses
import hashlib
import random
import struct
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from nginx_qa import prompt_archive_storage as storage_module
from nginx_qa.prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
)


class PromptArchiveGzipMemoryRegressionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="pas1-gzip-memory-")
        self.addCleanup(temporary.cleanup)
        self.store = PromptArchiveStorage(
            Path(temporary.name) / "archive", "9000", protected_roots=()
        )

    def put(self, raw):
        return self.store.put_blob(
            raw, logical_kind="payload_chunk", media_type="application/json"
        )

    def assert_error(self, code, callback):
        with self.assertRaises(PromptArchiveStorageError) as raised:
            callback()
        self.assertEqual(raised.exception.code, code)

    def test_tiny_blob_does_not_allocate_remaining_global_cap_for_flush(self):
        raw = b"x" * 1024
        ref = self.put(raw)
        self.assertEqual(ref["content_encoding"], "gzip")
        original_factory = storage_module.zlib.decompressobj
        flush_lengths = []

        class AllocationGuardInflater:
            def __init__(self, *args, **kwargs):
                self.delegate = original_factory(*args, **kwargs)

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def flush(self, length=16384):
                flush_lengths.append(length)
                if length > 64 * 1024:
                    raise AssertionError(
                        f"tiny blob requested {length} bytes for flush allocation"
                    )
                return self.delegate.flush(length)

        with mock.patch.object(
            storage_module.zlib, "decompressobj", AllocationGuardInflater
        ):
            self.assertEqual(self.store.read_blob(ref), raw)
        self.assertEqual(flush_lengths, [])

    def test_complete_multichunk_stream_reaches_eof_without_flush(self):
        raw = random.Random(20261027).randbytes(192 * 1024 + 17)
        stored = storage_module._gzip_bytes(raw)
        self.assertGreater(len(stored) - 18, 2 * 64 * 1024)
        self.assertEqual(
            PromptArchiveStorage._decode_exact_gzip(stored, len(raw)), raw
        )
        ref = self.put(raw)
        self.assertEqual(self.store.read_blob(ref, max_output_bytes=len(raw)), raw)

    def test_output_cap_exact_boundary_and_one_byte_over_remain_enforced(self):
        for raw in (b"", b"z" * 1024, b"z" * (128 * 1024)):
            stored = storage_module._gzip_bytes(raw)
            with self.subTest(size=len(raw), case="exact"):
                self.assertEqual(
                    PromptArchiveStorage._decode_exact_gzip(stored, len(raw)), raw
                )
            if raw:
                with self.subTest(size=len(raw), case="one-byte-over"):
                    self.assert_error(
                        "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
                        lambda: PromptArchiveStorage._decode_exact_gzip(
                            stored, len(raw) - 1
                        ),
                    )

    def test_missing_eof_trailing_bytes_and_multiple_members_fail_closed(self):
        raw = random.Random(87).randbytes(80 * 1024)
        stored = storage_module._gzip_bytes(raw)
        variants = {
            "missing-deflate-end": stored[:-9] + stored[-8:],
            "truncated-trailer": stored[:-1],
            "trailing-deflate-data": stored[:-8] + b"trailing" + stored[-8:],
            "trailing-file-data": stored + b"trailing",
            "second-member": stored + storage_module._gzip_bytes(b"second"),
        }
        for name, corrupt in variants.items():
            with self.subTest(name=name):
                self.assert_error(
                    "PROMPT_ARCHIVE_BLOB_CORRUPT",
                    lambda: PromptArchiveStorage._decode_exact_gzip(
                        corrupt, 2 * len(raw)
                    ),
                )

    def test_crc_isize_and_canonical_header_remain_mandatory(self):
        raw = b"canonical" * 1024
        stored = storage_module._gzip_bytes(raw)
        crc, size = struct.unpack("<II", stored[-8:])
        variants = {
            "crc": stored[:-8] + struct.pack("<II", crc ^ 1, size),
            "isize": stored[:-8] + struct.pack("<II", crc, size + 1),
            "header": stored[:4] + b"\x01" + stored[5:],
        }
        for name, corrupt in variants.items():
            with self.subTest(name=name):
                self.assert_error(
                    "PROMPT_ARCHIVE_BLOB_CORRUPT",
                    lambda: PromptArchiveStorage._decode_exact_gzip(
                        corrupt, len(raw) + 1
                    ),
                )

    def test_reference_sizes_and_digests_still_fail_closed(self):
        raw = b"verified" * 1024
        ref = self.put(raw)
        stored = self.store.blob_path(ref).read_bytes()
        for field, value in (
            ("stored_size", len(stored) + 1),
            ("stored_sha256", "0" * 64),
            ("uncompressed_size", len(raw) + 1),
            ("digest", "0" * 64),
        ):
            changed = {**ref, field: value}
            with self.subTest(field=field):
                self.assert_error(
                    "PROMPT_ARCHIVE_BLOB_CORRUPT",
                    lambda: self.store._decode_stored(
                        changed, stored, max_output_bytes=None
                    ),
                )

    def test_ratio_and_request_output_bounds_remain_enforced(self):
        raw = b"x" * (128 * 1024)
        ref = self.put(raw)
        stored = self.store.blob_path(ref).read_bytes()
        self.assert_error(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: self.store.read_blob(ref, max_output_bytes=len(raw) - 1),
        )
        self.store.limits = dataclasses.replace(
            PromptArchiveLimits(), max_compression_ratio=2
        )
        self.assert_error(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: self.store._decode_stored(ref, stored, max_output_bytes=None),
        )

    def test_alternate_deflate_encoding_is_not_accepted_as_canonical(self):
        raw = b"canonical encoding remains mandatory" * 2048
        ref = self.put(raw)
        compressor = storage_module.zlib.compressobj(
            1, storage_module.zlib.DEFLATED, -15
        )
        body = compressor.compress(raw) + compressor.flush()
        stored = storage_module._GZIP_HEADER + body + struct.pack(
            "<II", storage_module.zlib.crc32(raw) & 0xFFFFFFFF, len(raw)
        )
        self.assertNotEqual(stored, storage_module._gzip_bytes(raw))
        changed = copy.deepcopy(ref)
        changed["stored_size"] = len(stored)
        changed["stored_sha256"] = hashlib.sha256(stored).hexdigest()
        self.assert_error(
            "PROMPT_ARCHIVE_BLOB_CORRUPT",
            lambda: self.store._decode_stored(changed, stored, max_output_bytes=None),
        )

    def test_cancellation_during_inflate_still_stops_before_return(self):
        raw = random.Random(42).randbytes(128 * 1024)
        stored = storage_module._gzip_bytes(raw)
        token = PromptArchiveCancellationToken()
        original_factory = storage_module.zlib.decompressobj

        class CancellingInflater:
            def __init__(self, *args, **kwargs):
                self.delegate = original_factory(*args, **kwargs)

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def decompress(self, *args):
                output = self.delegate.decompress(*args)
                token.cancel()
                return output

        with mock.patch.object(
            storage_module.zlib, "decompressobj", CancellingInflater
        ):
            self.assert_error(
                "PROMPT_ARCHIVE_UNAVAILABLE",
                lambda: PromptArchiveStorage._decode_exact_gzip(
                    stored, len(raw), cancellation=token
                ),
            )


if __name__ == "__main__":
    unittest.main()
