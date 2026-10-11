import concurrent.futures
import copy
import dataclasses
import gzip
import hashlib
import json
import multiprocessing
import os
import subprocess
import tempfile
import threading
import time
import traceback
import unittest
from pathlib import Path
from unittest import mock

import nginx_qa.prompt_archive_storage as prompt_archive_storage_module
from nginx_qa.prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
)


PROJECT_ID = "9000"
PAYLOAD_KIND = "payload_chunk"
PAYLOAD_MEDIA_TYPE = "application/json"
ERROR_CANARY = "PROMPT_ARCHIVE_SECRET_ERROR_CANARY"
_REAL_BYTES = bytes
RESPONSE_ID = "parresp1-" + "1" * 64
SPRINT_STORAGE_KEY = "2" * 64
ARCHIVE_ID = "par1-" + "3" * 64
INVENTORY_ID = "parinv1-" + "4" * 64
REGISTRY_ID = "parpending1-" + "5" * 64
JOURNAL_SHA256 = "6" * 64


def operation_id_for(label: str) -> str:
    digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
    return f"parop1-{digest[:32]}"


def operation_plan_components(operation_id: str) -> tuple[str, ...]:
    return ("operations", "v1", RESPONSE_ID, operation_id, "plan.json")


def plan_components(label: str) -> tuple[str, ...]:
    return operation_plan_components(operation_id_for(label))


class InjectedPublicationFault(RuntimeError):
    """Test-only crash boundary marker."""


class AllocationGuardBytearray(bytearray):
    """Fails if storage copies an oversized mutable input before checking size."""

    def __bytes__(self) -> bytes:
        raise AssertionError("oversized bytearray was copied before limit check")


class SpoofedBytearray(bytearray):
    """Attempts to hide its buffer size and replace its frozen contents."""

    def __len__(self) -> int:
        return 1

    def __bytes__(self) -> bytes:
        raise AssertionError("overridden __bytes__ must never be called")


class BytesOverrideBytearray(bytearray):
    """Small buffer whose Python-level conversion tries to expand payload."""

    def __bytes__(self) -> bytes:
        raise AssertionError("overridden __bytes__ must never be called")


class GuardBytesMeta(type):
    def __instancecheck__(cls, instance) -> bool:
        return isinstance(instance, _REAL_BYTES)

    def __call__(cls, value=b"", *args, **kwargs) -> bytes:
        if isinstance(value, memoryview) and value.nbytes > 4:
            raise AssertionError("oversized memoryview was copied before limit check")
        return _REAL_BYTES(value, *args, **kwargs)


class GuardBytes(metaclass=GuardBytesMeta):
    """Drop-in builtins.bytes guard used only while testing allocation order."""


def process_put_blob(root: str, payload: bytes, result_queue) -> None:
    """Spawn-safe writer used to exercise the cross-process CAS race."""

    try:
        storage = PromptArchiveStorage(Path(root), PROJECT_ID, protected_roots=())
        result_queue.put(
            (
                "ok",
                storage.put_blob(
                    payload,
                    logical_kind=PAYLOAD_KIND,
                    media_type=PAYLOAD_MEDIA_TYPE,
                ),
            )
        )
    except Exception as exc:  # pragma: no cover - reported to the parent process
        result_queue.put(("error", type(exc).__name__, str(exc)))


def process_publish_immutable(
    root: str,
    operation_id: str,
    payload: bytes,
    result_queue,
) -> None:
    """Spawn-safe immutable publisher for cross-process race tests."""

    try:
        storage = PromptArchiveStorage(Path(root), PROJECT_ID, protected_roots=())
        receipt = storage.publish_immutable_bytes(
            operation_plan_components(operation_id),
            payload,
        )
        result_queue.put(
            ("ok", payload, receipt.created, receipt.sha256, receipt.byte_size)
        )
    except PromptArchiveStorageError as exc:  # pragma: no cover - parent asserts
        result_queue.put(("storage-error", payload, exc.code, exc.retryable))
    except Exception as exc:  # pragma: no cover - reported to the parent process
        result_queue.put(("error", payload, type(exc).__name__, str(exc)))


def process_crash_publish_immutable(
    root: str,
    operation_id: str,
    payload: bytes,
    crash_stage: str,
) -> None:
    """Terminate without unwinding at one durable publication boundary."""

    def terminate(stage: str) -> None:
        if stage == crash_stage:
            os._exit(77)

    storage = PromptArchiveStorage(
        Path(root), PROJECT_ID, protected_roots=(), fault_hook=terminate
    )
    storage.publish_immutable_bytes(
        operation_plan_components(operation_id),
        payload,
    )


def process_increment_mutable_control(root: str, iterations: int, result_queue) -> None:
    """Spawn-safe proof that every writer uses the same pinned lock leaf."""

    response_id = "parresp1-" + "7" * 64
    operation_id = "parop1-" + "8" * 32
    lock = ("operations", "v1", response_id, operation_id, "operation.lock")
    journal = ("operations", "v1", response_id, operation_id, "journal.json")
    try:
        storage = PromptArchiveStorage(Path(root), PROJECT_ID, protected_roots=())
        for _ in range(iterations):
            with storage.control_lock(lock):
                current = storage.read_current_json(journal) or {"value": 0}
                storage.replace_current_json(
                    journal,
                    {"value": current["value"] + 1},
                )
        result_queue.put(("ok", iterations))
    except PromptArchiveStorageError as exc:  # pragma: no cover - parent asserts
        result_queue.put(("storage-error", exc.code))
    except Exception as exc:  # pragma: no cover - parent asserts
        result_queue.put(("error", type(exc).__name__, str(exc)))


class PromptArchiveStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name) / "archive"
        self.store = PromptArchiveStorage(self.root, PROJECT_ID, protected_roots=())

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def put(self, payload: bytes, *, store: PromptArchiveStorage | None = None) -> dict:
        return (store or self.store).put_blob(
            payload,
            logical_kind=PAYLOAD_KIND,
            media_type=PAYLOAD_MEDIA_TYPE,
        )

    def assert_error_code(self, expected: str, operation) -> PromptArchiveStorageError:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, expected)
        self.assertEqual(
            raised.exception.retryable,
            expected
            in {
                "PROMPT_ARCHIVE_GENERATION_CONFLICT",
                "PROMPT_ARCHIVE_BUSY",
                "PROMPT_ARCHIVE_UNAVAILABLE",
            },
        )
        return raised.exception

    def run_processes(self, processes, result_queue):
        for process in processes:
            process.start()
        try:
            deadline = time.monotonic() + 30
            for process in processes:
                process.join(timeout=max(0, deadline - time.monotonic()))
            self.assertTrue(
                all(not process.is_alive() for process in processes),
                "storage writer process did not finish",
            )
            self.assertTrue(
                all(process.exitcode == 0 for process in processes),
                [process.exitcode for process in processes],
            )
            return [result_queue.get(timeout=5) for _ in processes]
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=5)
                process.close()
            result_queue.close()
            result_queue.join_thread()

    @staticmethod
    def prepare_identity_blob_parent(
        store: PromptArchiveStorage,
        payload: bytes,
    ) -> Path:
        digest = hashlib.sha256(payload).hexdigest()
        target = (
            store.root
            / "projects"
            / store.project_storage_key
            / "blobs"
            / "v1"
            / "sha256"
            / digest[:2]
            / digest[2:4]
            / f"{digest}.blob"
        )
        target.parent.mkdir(parents=True)
        return target

    def test_public_error_contract_matches_prompt_archive_error_v1(self) -> None:
        expected = {
            "PROMPT_ARCHIVE_REQUEST_INVALID": (400, False),
            "PROMPT_ARCHIVE_NOT_FOUND": (404, False),
            "PROMPT_ARCHIVE_VERSION_UNSUPPORTED": (409, False),
            "PROMPT_ARCHIVE_REFERENCE_INVALID": (409, False),
            "PROMPT_ARCHIVE_BLOB_MISSING": (409, False),
            "PROMPT_ARCHIVE_BLOB_CORRUPT": (409, False),
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH": (409, False),
            "PROMPT_ARCHIVE_GENERATION_CONFLICT": (409, True),
            "PROMPT_ARCHIVE_BUSY": (429, True),
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED": (413, False),
            "PROMPT_ARCHIVE_UNAVAILABLE": (503, True),
        }
        public_message = "Prompt archive is unavailable for this request."

        for code, (http_status, retryable) in expected.items():
            with self.subTest(code=code):
                error = PromptArchiveStorageError(code)
                self.assertEqual(error.code, code)
                self.assertEqual(error.http_status, http_status)
                self.assertEqual(error.retryable, retryable)
                self.assertEqual(error.message, public_message)
                self.assertEqual(str(error), public_message)

        unknown = PromptArchiveStorageError("NOT_A_PUBLIC_ERROR_CODE")
        self.assertEqual(unknown.code, "PROMPT_ARCHIVE_UNAVAILABLE")
        self.assertEqual(unknown.http_status, 503)
        self.assertTrue(unknown.retryable)
        self.assertEqual(str(unknown), public_message)

    def test_project_id_uses_the_safe_id_contract_before_storage_creation(self) -> None:
        invalid_root = self.root.parent / "invalid-project-storage"
        invalid_project_ids = (
            "",
            "../outside",
            "project/child",
            "project\\child",
            " project",
            "project ",
            "-project",
            "prøject",
            "a" * 161,
            None,
            9000,
        )

        for project_id in invalid_project_ids:
            with self.subTest(project_id=project_id):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REQUEST_INVALID",
                    lambda value=project_id: PromptArchiveStorage(
                        invalid_root,
                        value,
                        protected_roots=(),
                    ),
                )
                self.assertFalse(invalid_root.exists())

    def test_protected_roots_policy_is_an_explicit_constructor_requirement(self) -> None:
        with self.assertRaises(TypeError):
            PromptArchiveStorage(self.root / "implicit-policy", PROJECT_ID)

    def test_constructor_does_not_create_through_redirected_ancestor(self) -> None:
        outside = self.root.parent / "constructor-redirect-outside"
        outside.mkdir()
        redirected = self.root.parent / "constructor-redirect"
        try:
            redirected.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            if os.name != "nt":
                self.assertIsInstance(exc, (OSError, NotImplementedError))
                return
            junction = subprocess.run(
                [
                    "cmd.exe",
                    "/d",
                    "/c",
                    "mklink",
                    "/J",
                    str(redirected),
                    str(outside),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if junction.returncode != 0:
                self.assertNotEqual(junction.returncode, 0)
                return

        with self.assertRaises(PromptArchiveStorageError) as raised:
            PromptArchiveStorage(
                redirected / "new-archive-root",
                PROJECT_ID,
                protected_roots=(),
            )
        self.assertIn(
            raised.exception.code,
            {"PROMPT_ARCHIVE_REFERENCE_INVALID", "PROMPT_ARCHIVE_UNAVAILABLE"},
        )
        self.assertFalse((outside / "new-archive-root").exists())

    def test_constructor_ancestor_swap_is_blocked_or_detected(self) -> None:
        ancestor = self.root.parent / "constructor-swap-parent"
        ancestor.mkdir()
        displaced = self.root.parent / "constructor-swap-displaced"
        requested = ancestor / "new-archive-root"
        original = PromptArchiveStorage._validate_bootstrap_directory
        race = {"attempted": False, "performed": False, "blocked": False}

        def swap_after_pin(pinned, intended):
            identity = original(pinned, intended)
            if Path(intended) == ancestor and not race["attempted"]:
                race["attempted"] = True
                try:
                    ancestor.rename(displaced)
                except OSError:
                    race["blocked"] = True
                else:
                    race["performed"] = True
                    ancestor.mkdir()
            return identity

        with mock.patch.object(
            PromptArchiveStorage,
            "_validate_bootstrap_directory",
            side_effect=swap_after_pin,
        ):
            if os.name == "nt":
                store = PromptArchiveStorage(
                    requested,
                    PROJECT_ID,
                    protected_roots=(),
                )
                self.assertTrue(race["attempted"])
                self.assertTrue(race["blocked"])
                self.assertFalse(race["performed"])
                self.assertEqual(store.root, requested)
            else:
                with self.assertRaises(PromptArchiveStorageError):
                    PromptArchiveStorage(
                        requested,
                        PROJECT_ID,
                        protected_roots=(),
                    )
                self.assertTrue(race["attempted"])
                self.assertTrue(race["performed"])
                self.assertFalse((ancestor / "new-archive-root").exists())

    def test_constructor_rejects_windows_root_case_aliases_and_ads(self) -> None:
        if os.name != "nt":
            return
        exact = self.root.parent / "ConstructorExactCase"
        exact.mkdir()
        with self.assertRaises(PromptArchiveStorageError) as case_error:
            PromptArchiveStorage(
                self.root.parent / "constructorexactcase" / "archive",
                PROJECT_ID,
                protected_roots=(),
            )
        self.assertEqual(
            case_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )
        self.assertFalse((exact / "archive").exists())

        for component in ("archive:stream", "trailing.", "trailing ", "CON"):
            with self.subTest(component=component):
                with self.assertRaises(PromptArchiveStorageError) as invalid:
                    PromptArchiveStorage(
                        self.root.parent / component,
                        PROJECT_ID,
                        protected_roots=(),
                    )
                self.assertEqual(
                    invalid.exception.code,
                    "PROMPT_ARCHIVE_REQUEST_INVALID",
                )

    def test_identity_and_frozen_gzip_storage_are_deterministic_and_exact(self) -> None:
        empty = self.put(b"")
        self.assertEqual(empty["digest"], hashlib.sha256(b"").hexdigest())
        self.assertEqual(empty["content_encoding"], "identity")
        self.assertEqual(empty["uncompressed_size"], 0)
        self.assertEqual(empty["stored_size"], 0)
        self.assertEqual(self.store.read_blob(empty), b"")

        small = bytes(1023)
        first = self.put(small)
        second = self.put(small)
        self.assertEqual(first, second)
        self.assertEqual(first["digest"], hashlib.sha256(small).hexdigest())
        self.assertEqual(first["content_encoding"], "identity")
        self.assertEqual(
            set(first),
            {
                "schema_version",
                "kind",
                "hash_algorithm",
                "digest",
                "logical_kind",
                "media_type",
                "content_encoding",
                "uncompressed_size",
                "stored_size",
                "stored_sha256",
            },
        )
        self.assertEqual(first["schema_version"], 1)
        self.assertEqual(first["kind"], "prompt_archive_blob_ref")
        self.assertEqual(first["hash_algorithm"], "sha256")
        self.assertNotIn("encoding_parameters", first)
        self.assertEqual(self.store.blob_path(first).read_bytes(), small)
        self.assertEqual(self.store.read_blob(first), small)

        raw = bytes(1024)
        expected_stored = bytes.fromhex(
            "1f8b08000000000002ff63601805a360148c5400002eafb5ef00040000"
        )
        compressed = self.put(raw)
        repeated = self.put(raw)
        self.assertEqual(compressed, repeated)
        self.assertEqual(compressed["digest"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(compressed["content_encoding"], "gzip")
        self.assertEqual(set(compressed), set(first) | {"encoding_parameters"})
        self.assertEqual(
            compressed["encoding_parameters"],
            "gzip-rfc1952-zlib-1.2.13-level9-v1",
        )
        self.assertEqual(self.store.blob_path(compressed).read_bytes(), expected_stored)
        self.assertEqual(compressed["stored_size"], len(expected_stored))
        self.assertEqual(
            compressed["stored_sha256"],
            hashlib.sha256(expected_stored).hexdigest(),
        )
        self.assertEqual(self.store.read_blob(compressed), raw)
        self.store.verify_blob(first)
        self.store.verify_blob(compressed)
        self.assertEqual(gzip.decompress(expected_stored), raw)

    def test_describe_blob_is_exact_and_never_publishes(self) -> None:
        for payload in (b"described-only", bytes(1024)):
            with self.subTest(byte_size=len(payload)):
                before = {
                    path.relative_to(self.root)
                    for path in self.root.rglob("*")
                    if path.is_file()
                }
                described = self.store.describe_blob(
                    payload,
                    logical_kind=PAYLOAD_KIND,
                    media_type=PAYLOAD_MEDIA_TYPE,
                )
                after = {
                    path.relative_to(self.root)
                    for path in self.root.rglob("*")
                    if path.is_file()
                }
                self.assertEqual(after, before)
                self.assertEqual(self.put(payload), described)
                self.assertEqual(self.store.read_blob(described), payload)

    def test_cooperative_cancellation_stops_bounded_blob_read(self) -> None:
        payload = os.urandom(512 * 1024)
        ref = self.put(payload)
        token = PromptArchiveCancellationToken.with_timeout(5)
        read_started = threading.Event()
        original_read = prompt_archive_storage_module.os.read

        def deliberately_slow_read(descriptor, amount):
            chunk = original_read(descriptor, amount)
            read_started.set()
            token.event.wait(timeout=1)
            return chunk

        with mock.patch.object(
            prompt_archive_storage_module.os,
            "read",
            side_effect=deliberately_slow_read,
        ):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    self.store.read_blob,
                    ref,
                    cancellation=token,
                )
                self.assertTrue(read_started.wait(timeout=2))
                token.cancel()
                with self.assertRaises(PromptArchiveStorageError) as raised:
                    future.result(timeout=2)
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")

    def test_existing_blob_is_fully_verified_before_reuse_and_never_repaired(self) -> None:
        raw = b"x" * 2048
        ref = self.put(raw)
        path = self.store.blob_path(ref)
        corrupt_buffer = bytearray(path.read_bytes())
        corrupt_buffer[len(corrupt_buffer) // 2] ^= 1
        corrupt = bytes(corrupt_buffer)
        path.write_bytes(corrupt)

        self.assert_error_code(
            "PROMPT_ARCHIVE_BLOB_CORRUPT",
            lambda: self.put(raw),
        )
        self.assertEqual(path.read_bytes(), corrupt)
        self.assert_error_code(
            "PROMPT_ARCHIVE_BLOB_CORRUPT",
            lambda: self.store.read_blob(ref),
        )

    def test_blob_fault_boundaries_are_replay_safe_and_leave_no_temp_files(self) -> None:
        payload = b"fault-boundary" * 1000
        post_publish_stages = {"after_publish", "after_directory_sync"}
        stages = (
            "after_parent_pin",
            "after_temp_create",
            "after_temp_write",
            "after_temp_flush",
            "after_temp_fsync",
            "after_temp_verify",
            "before_publish",
            "after_parent_precheck",
            "after_publish",
            "after_directory_sync",
        )
        for stage in stages:
            with self.subTest(stage=stage):
                root = self.root / f"blob-fault-{stage.replace('_', '-')}"

                def inject(observed_stage: str, *, selected: str = stage) -> None:
                    if observed_stage == selected:
                        raise InjectedPublicationFault(selected)

                faulting = PromptArchiveStorage(
                    root, PROJECT_ID, protected_roots=(), fault_hook=inject
                )
                with self.assertRaises(InjectedPublicationFault):
                    self.put(payload, store=faulting)

                finals = list(root.rglob("*.blob"))
                self.assertEqual(
                    len(finals),
                    1 if stage in post_publish_stages else 0,
                )
                self.assertEqual(list(root.rglob("*.tmp")), [])

                recovered = PromptArchiveStorage(root, PROJECT_ID, protected_roots=())
                ref = self.put(payload, store=recovered)
                self.assertEqual(recovered.read_blob(ref), payload)
                self.assertEqual(len(list(root.rglob("*.blob"))), 1)
                self.assertEqual(list(root.rglob("*.tmp")), [])

    def test_no_replace_and_post_publish_verification_failures_recover(self) -> None:
        payload = b"publication-recovery" * 1000
        publish_failure_root = self.root / "publish-system-failure"
        faulting = PromptArchiveStorage(
            publish_failure_root, PROJECT_ID, protected_roots=()
        )
        with mock.patch(
            "nginx_qa.prompt_archive_storage._atomic_rename_pinned_no_replace",
            side_effect=OSError("injected pinned atomic rename failure"),
        ):
            failure = self.assert_error_code(
                "PROMPT_ARCHIVE_UNAVAILABLE",
                lambda: self.put(payload, store=faulting),
            )
        self.assertTrue(failure.retryable)
        self.assertNotIn("injected", str(failure))
        self.assertEqual(list(publish_failure_root.rglob("*.blob")), [])
        self.assertEqual(list(publish_failure_root.rglob("*.tmp")), [])

        recovered = PromptArchiveStorage(
            publish_failure_root, PROJECT_ID, protected_roots=()
        )
        ref = self.put(payload, store=recovered)
        self.assertEqual(recovered.read_blob(ref), payload)

        verification_failure_root = self.root / "post-publish-verification-failure"
        verification_faulting = PromptArchiveStorage(
            verification_failure_root,
            PROJECT_ID,
            protected_roots=(),
        )
        original_verify = verification_faulting._verify_blob_at_path

        def fail_final_verification(ref_value, path):
            if path.suffix == ".blob":
                raise InjectedPublicationFault("post-publish verification")
            return original_verify(ref_value, path)

        with mock.patch.object(
            verification_faulting,
            "_verify_blob_at_path",
            side_effect=fail_final_verification,
        ):
            with self.assertRaises(InjectedPublicationFault):
                self.put(payload, store=verification_faulting)

        self.assertEqual(len(list(verification_failure_root.rglob("*.blob"))), 1)
        self.assertEqual(list(verification_failure_root.rglob("*.tmp")), [])
        verifier = PromptArchiveStorage(
            verification_failure_root, PROJECT_ID, protected_roots=()
        )
        recovered_ref = self.put(payload, store=verifier)
        self.assertEqual(verifier.read_blob(recovered_ref), payload)

    def test_raw_filesystem_errors_are_mapped_to_safe_unavailable(self) -> None:
        payload = b"raw-filesystem-error"

        def assert_safe_unavailable(operation, root: Path) -> None:
            error = self.assert_error_code(
                "PROMPT_ARCHIVE_UNAVAILABLE",
                operation,
            )
            self.assertNotIn(ERROR_CANARY, str(error))
            self.assertNotIn(str(root), str(error))
            rendered = "".join(traceback.format_exception(error))
            self.assertNotIn(ERROR_CANARY, rendered)
            self.assertNotIn(str(root), rendered)
            self.assertIsNone(error.__cause__)
            self.assertTrue(error.__suppress_context__)

        open_root = self.root / "raw-open-error"
        open_store = PromptArchiveStorage(open_root, PROJECT_ID, protected_roots=())
        self.prepare_identity_blob_parent(open_store, payload)

        with self.subTest(stage="open"), mock.patch(
            "nginx_qa.prompt_archive_storage.PromptArchiveStorage._create_relative_temp",
            side_effect=OSError(f"{ERROR_CANARY}:{open_root}"),
        ):
            assert_safe_unavailable(
                lambda: self.put(payload, store=open_store),
                open_root,
            )

        write_root = self.root / "raw-write-error"
        write_store = PromptArchiveStorage(write_root, PROJECT_ID, protected_roots=())
        self.prepare_identity_blob_parent(write_store, payload)
        original_fdopen = prompt_archive_storage_module.os.fdopen

        class FailingWriteStream:
            def __init__(self, stream) -> None:
                self.stream = stream

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return self.stream.__exit__(exc_type, exc_value, traceback)

            def write(self, _data):
                raise OSError(f"{ERROR_CANARY}:{write_root}")

            def flush(self):
                return self.stream.flush()

            def fileno(self):
                return self.stream.fileno()

        def failing_fdopen(*args, **kwargs):
            return FailingWriteStream(original_fdopen(*args, **kwargs))

        with self.subTest(stage="write"), mock.patch(
            "nginx_qa.prompt_archive_storage.os.fdopen",
            side_effect=failing_fdopen,
        ):
            assert_safe_unavailable(
                lambda: self.put(payload, store=write_store),
                write_root,
            )

        fsync_root = self.root / "raw-fsync-error"
        fsync_store = PromptArchiveStorage(fsync_root, PROJECT_ID, protected_roots=())
        self.prepare_identity_blob_parent(fsync_store, payload)
        with self.subTest(stage="fsync"), mock.patch(
            "nginx_qa.prompt_archive_storage.os.fsync",
            side_effect=OSError(f"{ERROR_CANARY}:{fsync_root}"),
        ):
            assert_safe_unavailable(
                lambda: self.put(payload, store=fsync_store),
                fsync_root,
            )

        read_root = self.root / "raw-read-error"
        read_store = PromptArchiveStorage(read_root, PROJECT_ID, protected_roots=())
        read_ref = self.put(payload, store=read_store)
        with self.subTest(stage="read"), mock.patch(
            "nginx_qa.prompt_archive_storage.os.read",
            side_effect=OSError(f"{ERROR_CANARY}:{read_root}"),
        ):
            assert_safe_unavailable(
                lambda: read_store.read_blob(read_ref),
                read_root,
            )

        immutable_root = self.root / "raw-immutable-fsync-error"
        immutable_store = PromptArchiveStorage(
            immutable_root, PROJECT_ID, protected_roots=()
        )
        with self.subTest(stage="immutable-fsync"), mock.patch(
            "nginx_qa.prompt_archive_storage.os.fsync",
            side_effect=OSError(f"{ERROR_CANARY}:{immutable_root}"),
        ):
            assert_safe_unavailable(
                lambda: immutable_store.publish_immutable_bytes(
                    plan_components("raw-fsync"),
                    payload,
                ),
                immutable_root,
            )

        for root in (
            open_root,
            write_root,
            fsync_root,
            read_root,
            immutable_root,
        ):
            self.assertEqual(list(root.rglob("*.tmp")), [])

    def test_ref_digest_and_path_are_closed_and_caller_path_is_rejected(self) -> None:
        ref = self.put(b'{"safe":true}')
        path = self.store.blob_path(ref)
        self.assertTrue(path.is_relative_to(self.root.resolve()))
        self.assertEqual(path.name, f"{ref['digest']}.blob")
        self.assertEqual(path.parent.name, ref["digest"][2:4])
        self.assertEqual(path.parent.parent.name, ref["digest"][:2])
        self.assertEqual(
            path,
            self.root.resolve()
            / "projects"
            / self.store.project_storage_key
            / "blobs"
            / "v1"
            / "sha256"
            / ref["digest"][:2]
            / ref["digest"][2:4]
            / f"{ref['digest']}.blob",
        )

        invalid_values = (
            {**ref, "digest": "../" + "a" * 61},
            {**ref, "digest": ref["digest"].upper()},
            {**ref, "digest": "a" * 63},
            {**ref, "stored_sha256": "z" * 64},
            {**ref, "uncompressed_size": -1},
            {**ref, "uncompressed_size": True},
            {**ref, "schema_version": "1"},
            {key: value for key, value in ref.items() if key != "digest"},
            {**ref, "unexpected": "closed-shape"},
            {**ref, "kind": None},
            {**ref, "path": "..\\..\\outside.blob"},
            {**ref, "$cas_ref": {"path": "C:\\outside"}},
        )
        for invalid in invalid_values:
            with self.subTest(invalid=invalid):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda invalid=invalid: self.store.blob_path(invalid),
                )
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda invalid=invalid: self.store.read_blob(invalid),
                )

        unsupported_values = (
            {**ref, "schema_version": 2},
            {**ref, "kind": "prompt_archive_blob_ref_v2"},
            {**ref, "content_encoding": "deflate"},
        )
        for unsupported in unsupported_values:
            with self.subTest(unsupported=unsupported):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
                    lambda unsupported=unsupported: self.store.read_blob(unsupported),
                )

        absent = copy.deepcopy(ref)
        absent["digest"] = "f" * 64
        absent["stored_sha256"] = "f" * 64
        missing = self.assert_error_code(
            "PROMPT_ARCHIVE_BLOB_MISSING",
            lambda: self.store.read_blob(absent),
        )
        self.assertFalse(missing.retryable)
        self.assertNotIn(str(self.root), str(missing))

    def test_ref_semantics_bind_type_media_and_threshold_encoding(self) -> None:
        identity_ref = self.put(b"small")
        gzip_ref = self.put(bytes(1024))
        invalid_values = (
            {**identity_ref, "logical_kind": "not-a-declared-kind"},
            {**identity_ref, "media_type": "text/plain"},
            {
                **identity_ref,
                "logical_kind": "manifest",
                "media_type": "application/json",
            },
            {
                **identity_ref,
                "content_encoding": "gzip",
                "encoding_parameters": "gzip-rfc1952-zlib-1.2.13-level9-v1",
            },
            {
                key: value
                for key, value in {**gzip_ref, "content_encoding": "identity"}.items()
                if key != "encoding_parameters"
            },
            {**identity_ref, "hash_algorithm": "sha512"},
        )
        for invalid in invalid_values:
            with self.subTest(invalid=invalid):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda invalid=invalid: self.store.read_blob(invalid),
                )

        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.store.read_blob(
                {
                    **identity_ref,
                    "stored_size": identity_ref["stored_size"] + 1,
                }
            ),
        )

    def test_malformed_versions_and_unhashable_fields_use_stable_errors(self) -> None:
        ref = self.put(b"stable-error-shape")
        malformed_refs = (
            {**ref, "schema_version": True},
            {key: value for key, value in ref.items() if key != "schema_version"},
            {**ref, "logical_kind": []},
            {**ref, "logical_kind": {}},
            {**ref, "content_encoding": []},
            {**ref, "content_encoding": {}},
        )
        for malformed in malformed_refs:
            with self.subTest(malformed=malformed):
                error = self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda malformed=malformed: self.store.read_blob(malformed),
                )
                self.assertNotIn(str(self.root), str(error))

        invalid_puts = (
            {"logical_kind": [], "media_type": PAYLOAD_MEDIA_TYPE},
            {"logical_kind": {}, "media_type": PAYLOAD_MEDIA_TYPE},
            {"logical_kind": PAYLOAD_KIND, "media_type": []},
        )
        for kwargs in invalid_puts:
            with self.subTest(kwargs=kwargs):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda kwargs=kwargs: self.store.put_blob(b"data", **kwargs),
                )

    def test_project_storage_key_uses_the_frozen_pas1_golden_vector(self) -> None:
        self.assertEqual(
            self.store.project_storage_key,
            "d23f4ba66de9f6b13715a9dedafdbc65fe54c2fd121bbc788bf671745ab42531",
        )
        other = PromptArchiveStorage(self.root, "9001", protected_roots=())
        self.assertNotEqual(other.project_storage_key, self.store.project_storage_key)

    def test_same_bytes_can_have_distinct_typed_refs_but_one_physical_blob(self) -> None:
        payload = b'{"typed":"content"}'
        payload_ref = self.put(payload)
        raw_ref = self.store.put_blob(
            payload,
            logical_kind="raw_chunk",
            media_type="application/json",
        )

        self.assertEqual(payload_ref["digest"], raw_ref["digest"])
        self.assertEqual(payload_ref["stored_sha256"], raw_ref["stored_sha256"])
        self.assertEqual(payload_ref["logical_kind"], "payload_chunk")
        self.assertEqual(raw_ref["logical_kind"], "raw_chunk")
        self.assertEqual(self.store.blob_path(payload_ref), self.store.blob_path(raw_ref))
        self.assertEqual(self.store.read_blob(payload_ref), payload)
        self.assertEqual(self.store.read_blob(raw_ref), payload)
        self.assertEqual(len(list(self.root.rglob("*.blob"))), 1)

    def test_identical_bytes_are_physically_isolated_between_projects(self) -> None:
        payload = b"project-scoped-deduplication"
        project_9000_ref = self.put(payload)
        project_9001 = PromptArchiveStorage(self.root, "9001", protected_roots=())
        project_9001_ref = self.put(payload, store=project_9001)

        self.assertEqual(project_9000_ref, project_9001_ref)
        project_9000_path = self.store.blob_path(project_9000_ref)
        project_9001_path = project_9001.blob_path(project_9001_ref)
        self.assertNotEqual(project_9000_path, project_9001_path)
        self.assertEqual(project_9000_path.read_bytes(), payload)
        self.assertEqual(project_9001_path.read_bytes(), payload)
        self.assertEqual(len(list(self.root.rglob("*.blob"))), 2)

    def test_put_rejects_invalid_type_media_binding_without_publishing(self) -> None:
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.store.put_blob(
                b"invalid-binding",
                logical_kind="manifest",
                media_type="application/json",
            ),
        )
        self.assertEqual(list(self.root.rglob("*.blob")), [])

    def test_concurrent_same_and_different_content_converges_without_temp_collisions(self) -> None:
        same = b"same-content" * 1000
        distinct = [f"distinct-{index}".encode() * 1000 for index in range(8)]
        inputs = [same] * 24 + distinct
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            refs = list(executor.map(self.put, inputs))

        same_refs = refs[:24]
        self.assertTrue(all(ref == same_refs[0] for ref in same_refs))
        self.assertEqual(self.store.read_blob(same_refs[0]), same)
        for payload, ref in zip(distinct, refs[24:]):
            self.assertEqual(self.store.read_blob(ref), payload)

        blob_files = list(self.root.rglob("*.blob"))
        self.assertEqual(len(blob_files), 1 + len(distinct))
        self.assertEqual(
            [path for path in self.root.rglob("*") if ".tmp" in path.name],
            [],
        )

    def test_process_level_same_content_race_converges(self) -> None:
        context = multiprocessing.get_context("spawn")
        result_queue = context.Queue()
        payload = b"cross-process-content" * 1000
        processes = [
            context.Process(
                target=process_put_blob,
                args=(str(self.root), payload, result_queue),
            )
            for _ in range(4)
        ]
        results = self.run_processes(processes, result_queue)
        self.assertTrue(all(result[0] == "ok" for result in results), results)
        refs = [result[1] for result in results]
        self.assertTrue(all(ref == refs[0] for ref in refs))
        self.assertEqual(self.store.read_blob(refs[0]), payload)
        self.assertEqual(len(list(self.root.rglob("*.blob"))), 1)
        self.assertFalse(any(".tmp" in path.name for path in self.root.rglob("*")))

    def test_symlink_or_reparse_component_cannot_escape_the_archive_root(self) -> None:
        payload = b"symlink-boundary" * 100
        probe = self.put(payload)
        target = self.store.blob_path(probe)
        shard = target.parent.parent
        outside = self.root.parent / "outside"
        outside.mkdir(parents=True, exist_ok=True)
        target.unlink()
        target.parent.rmdir()
        shard.rmdir()
        try:
            shard.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            if os.name != "nt":
                self.skipTest(f"directory symlinks are unavailable: {exc}")
            junction = subprocess.run(
                ["cmd.exe", "/d", "/c", "mklink", "/J", str(shard), str(outside)],
                check=False,
                capture_output=True,
                text=True,
            )
            if junction.returncode != 0:
                self.skipTest("directory symlinks and junctions are unavailable")

        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.put(payload),
        )
        self.assertFalse(any(outside.iterdir()))

    def test_mocked_reparse_detection_fails_closed_without_platform_privileges(self) -> None:
        root = self.root / "mocked-reparse"
        store = PromptArchiveStorage(root, PROJECT_ID, protected_roots=())
        payload = b"mocked-reparse-boundary" * 100
        first_shard = hashlib.sha256(payload).hexdigest()[:2]
        original = prompt_archive_storage_module._path_entry_is_redirected

        def detect_selected_shard(path: Path) -> bool:
            if path.name == first_shard and path.parent.name == "sha256":
                return True
            return original(path)

        with mock.patch(
            "nginx_qa.prompt_archive_storage._path_entry_is_redirected",
            side_effect=detect_selected_shard,
        ):
            self.assert_error_code(
                "PROMPT_ARCHIVE_REFERENCE_INVALID",
                lambda: self.put(payload, store=store),
            )

    def test_parent_swap_between_check_and_rename_cannot_publish_outside_root(self) -> None:
        payload = b"pinned-parent-publication" * 100
        root = self.root / "pinned-parent-swap"
        outside = self.root.parent / "pinned-parent-outside"
        outside.mkdir(parents=True, exist_ok=True)
        holder: dict[str, Path] = {}

        def replace_parent(stage: str) -> None:
            if stage != "after_parent_precheck":
                return
            parent = holder["parent"]
            detached = holder["detached"]
            parent.rename(detached)
            holder["swapped"] = detached
            if os.name == "nt":
                junction = subprocess.run(
                    [
                        "cmd.exe",
                        "/d",
                        "/c",
                        "mklink",
                        "/J",
                        str(parent),
                        str(outside),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(junction.returncode, 0)
            else:
                parent.symlink_to(outside, target_is_directory=True)

        store = PromptArchiveStorage(
            root,
            PROJECT_ID,
            protected_roots=(),
            fault_hook=replace_parent,
        )
        target = self.prepare_identity_blob_parent(store, payload)
        holder["parent"] = target.parent
        holder["detached"] = target.parent.with_name(target.parent.name + "-detached")

        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.put(payload, store=store)
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID"
            if "swapped" in holder
            else "PROMPT_ARCHIVE_UNAVAILABLE",
        )
        self.assertEqual(list(outside.iterdir()), [])
        if "swapped" in holder:
            self.assertEqual(len(list(holder["detached"].glob("*.blob"))), 1)
        else:
            self.assertFalse(holder["detached"].exists())
        self.assertEqual(list(root.rglob("*.blob")), [])
        self.assertEqual(list(root.rglob("*.tmp")), [])

    def test_relocation_after_parent_pin_cannot_write_into_protected_root(self) -> None:
        protected = self.root.parent / "isolated-protected-relocation"
        protected.mkdir()

        for artifact in ("blob", "immutable"):
            with self.subTest(artifact=artifact):
                root = self.root / f"pin-relocation-{artifact}"
                holder: dict[str, Path | bool] = {}

                def relocate_parent(stage: str) -> None:
                    if stage != "after_parent_pin":
                        return
                    parent = holder["parent"]
                    assert isinstance(parent, Path)
                    relocated = protected / f"relocated-{artifact}"
                    parent.rename(relocated)
                    holder["relocated"] = relocated

                store = PromptArchiveStorage(
                    root,
                    PROJECT_ID,
                    protected_roots=(protected,),
                    fault_hook=relocate_parent,
                )
                payload = f"pinned-{artifact}-payload".encode("ascii")
                if artifact == "blob":
                    target = self.prepare_identity_blob_parent(store, payload)
                    operation = lambda: self.put(payload, store=store)
                else:
                    components = plan_components("pin-relocation-immutable")
                    operation_id = components[-2]
                    target = (
                        root
                        / "projects"
                        / store.project_storage_key
                        / "operations"
                        / "v1"
                        / RESPONSE_ID
                        / operation_id
                        / "plan.json"
                    )
                    target.parent.mkdir(parents=True)
                    operation = lambda: store.publish_immutable_bytes(
                        components,
                        payload,
                    )
                holder["parent"] = target.parent

                error = self.assert_error_code(
                    "PROMPT_ARCHIVE_UNAVAILABLE",
                    operation,
                )
                self.assertTrue(error.retryable)
                relocated = holder.get("relocated")
                if isinstance(relocated, Path):
                    self.assertEqual(list(relocated.iterdir()), [])
                    relocated.rename(target.parent)
                self.assertEqual(list(protected.iterdir()), [])
                self.assertEqual(list(root.rglob("*.tmp")), [])
                self.assertEqual(list(root.rglob("*.blob")), [])
                self.assertEqual(list(root.rglob("plan.json")), [])

    def test_existing_hard_link_winners_are_rejected_without_touching_source(self) -> None:
        protected = self.root.parent / "isolated-protected-hardlinks"
        protected.mkdir()
        root = self.root / "hard-link-winners"
        store = PromptArchiveStorage(
            root,
            PROJECT_ID,
            protected_roots=(protected,),
        )

        blob_payload = b"protected-hard-link-blob"
        ref = self.put(blob_payload)
        blob_target = store.blob_path(ref)
        blob_target.parent.mkdir(parents=True)
        blob_source = protected / "blob-source.bin"
        blob_source.write_bytes(blob_payload)
        os.link(blob_source, blob_target)

        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.put(blob_payload, store=store),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: store.read_blob(ref),
        )
        self.assertEqual(blob_source.read_bytes(), blob_payload)

        immutable_payload = b'{"protected-hard-link":true}\n'
        components = plan_components("protected-hard-link")
        immutable_target = (
            root
            / "projects"
            / store.project_storage_key
            / "operations"
            / "v1"
            / RESPONSE_ID
            / components[-2]
            / "plan.json"
        )
        immutable_target.parent.mkdir(parents=True)
        immutable_source = protected / "immutable-source.json"
        immutable_source.write_bytes(immutable_payload)
        os.link(immutable_source, immutable_target)

        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: store.publish_immutable_bytes(components, immutable_payload),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: store.verify_immutable_bytes(
                components,
                expected_sha256=hashlib.sha256(immutable_payload).hexdigest(),
                expected_size=len(immutable_payload),
            ),
        )
        self.assertEqual(immutable_source.read_bytes(), immutable_payload)

    def test_hard_link_created_during_read_is_detected(self) -> None:
        protected = self.root.parent / "isolated-protected-read-race"
        protected.mkdir()
        root = self.root / "hard-link-read-race"
        store = PromptArchiveStorage(
            root,
            PROJECT_ID,
            protected_roots=(protected,),
        )
        payload = b"hard-link-read-race" * 100
        ref = self.put(payload, store=store)
        target = store.blob_path(ref)
        alias = protected / "late-alias.blob"
        original_read = os.read
        linked = False

        def link_during_read(descriptor: int, count: int) -> bytes:
            nonlocal linked
            chunk = original_read(descriptor, count)
            if chunk and not linked:
                os.link(target, alias)
                linked = True
            return chunk

        try:
            with mock.patch(
                "nginx_qa.prompt_archive_storage.os.read",
                side_effect=link_during_read,
            ):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda: store.read_blob(ref),
                )
            self.assertTrue(linked)
            self.assertEqual(alias.read_bytes(), target.read_bytes())
        finally:
            if alias.exists():
                alias.unlink()

    def test_casefold_collision_in_storage_chain_fails_closed(self) -> None:
        root = self.root / "casefold-chain"
        store = PromptArchiveStorage(root, PROJECT_ID, protected_roots=())
        (root / "Projects").mkdir()

        error = self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.put(b"casefold-chain-payload", store=store),
        )
        self.assertNotIn(str(root), str(error))
        self.assertEqual(list(root.rglob("*.blob")), [])
        self.assertEqual(list(root.rglob("*.tmp")), [])

    def test_publication_never_uses_replace_semantics(self) -> None:
        root = self.root / "no-replace"
        store = PromptArchiveStorage(root, PROJECT_ID, protected_roots=())
        payload = b"no-replace-publication" * 100
        with mock.patch(
            "nginx_qa.prompt_archive_storage.os.replace",
            side_effect=AssertionError("immutable publication attempted replacement"),
        ):
            ref = self.put(payload, store=store)
            receipt = store.publish_immutable_bytes(
                plan_components("no-replace"),
                b'{"immutable":true}',
            )
        self.assertEqual(store.read_blob(ref), payload)
        self.assertEqual(receipt.path.read_bytes(), b'{"immutable":true}')

    def test_reference_shaped_user_payload_remains_opaque_bytes(self) -> None:
        payload = json.dumps(
            {
                "$cas_ref": {
                    "digest": "../outside",
                    "path": "C:\\secrets\\credential.dpapi",
                    "content_encoding": "magic",
                },
                "nested": [{"$cas_ref": {"sha256": "not-a-reference"}}],
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        ref = self.put(payload)
        self.assertEqual(self.store.read_blob(ref), payload)
        self.assertEqual(self.store.blob_path(ref).read_bytes(), payload)

    def test_bounded_decompression_rejects_output_and_ratio_before_returning_bytes(self) -> None:
        raw = bytes(1024 * 1024)
        writer = PromptArchiveStorage(
            self.root / "writer", PROJECT_ID, protected_roots=()
        )
        ref = self.put(raw, store=writer)

        output_bounded = PromptArchiveStorage(
            self.root / "writer",
            PROJECT_ID,
            protected_roots=(),
            limits=dataclasses.replace(
                PromptArchiveLimits(),
                max_hydrated_bytes=4096,
            ),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: output_bounded.read_blob(ref),
        )

        ratio_bounded = PromptArchiveStorage(
            self.root / "writer",
            PROJECT_ID,
            protected_roots=(),
            limits=dataclasses.replace(
                PromptArchiveLimits(),
                max_hydrated_bytes=len(raw),
                max_compression_ratio=2,
            ),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: ratio_bounded.read_blob(ref),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: writer.read_blob(ref, max_output_bytes=4096),
        )

        put_bounded = PromptArchiveStorage(
            self.root / "put-bounded",
            PROJECT_ID,
            protected_roots=(),
            limits=dataclasses.replace(
                PromptArchiveLimits(),
                max_blob_uncompressed_bytes=1024,
            ),
        )
        accepted = self.put(bytes(1024), store=put_bounded)
        self.assertEqual(put_bounded.read_blob(accepted), bytes(1024))
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: self.put(bytes(1025), store=put_bounded),
        )

        stored_bounded = PromptArchiveStorage(
            self.root / "stored-bounded",
            PROJECT_ID,
            protected_roots=(),
            limits=dataclasses.replace(
                PromptArchiveLimits(),
                max_blob_stored_bytes=20,
            ),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: self.put(bytes(range(256)) * 4, store=stored_bounded),
        )
        self.assertEqual(list((self.root / "stored-bounded").rglob("*.blob")), [])

    def test_oversized_mutable_inputs_are_rejected_before_copying(self) -> None:
        limits = dataclasses.replace(
            PromptArchiveLimits(),
            max_blob_uncompressed_bytes=4,
            max_hydrated_bytes=4,
            max_immutable_bytes=4,
        )
        store = PromptArchiveStorage(
            self.root / "allocation-bounds",
            PROJECT_ID,
            protected_roots=(),
            limits=limits,
        )

        oversized_bytearray = AllocationGuardBytearray(b"12345")
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: self.put(oversized_bytearray, store=store),
        )

        cast_view = memoryview(bytearray(8)).cast("I")
        self.assertEqual(len(cast_view), 2)
        self.assertEqual(cast_view.nbytes, 8)
        with mock.patch.object(
            prompt_archive_storage_module,
            "bytes",
            GuardBytes,
            create=True,
        ):
            self.assert_error_code(
                "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
                lambda: self.put(cast_view, store=store),
            )
        cast_view.release()

        immutable_components = plan_components("immutable-allocation-bound")
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: store.publish_immutable_bytes(
                immutable_components,
                AllocationGuardBytearray(b"12345"),
            ),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: store.publish_immutable_json(
                immutable_components,
                {"value": "too-large"},
            ),
        )
        self.assertEqual(list((self.root / "allocation-bounds").rglob("*.tmp")), [])
        self.assertEqual(
            list((self.root / "allocation-bounds").rglob("plan.json")),
            [],
        )

    def test_bytes_like_subclasses_cannot_spoof_caps_or_frozen_payload(self) -> None:
        limits = dataclasses.replace(
            PromptArchiveLimits(),
            max_blob_uncompressed_bytes=8,
            max_hydrated_bytes=8,
            max_immutable_bytes=8,
        )
        store = PromptArchiveStorage(
            self.root / "subclass-buffer-bounds",
            PROJECT_ID,
            protected_roots=(),
            limits=limits,
        )
        oversized = SpoofedBytearray(b"0123456789abcdef")
        self.assertEqual(len(oversized), 1)

        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: self.put(oversized, store=store),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: store.publish_immutable_bytes(
                plan_components("spoofed-buffer-limit"),
                oversized,
            ),
        )

        small = BytesOverrideBytearray(b"safe")
        ref = self.put(small, store=store)
        self.assertEqual(store.read_blob(ref), b"safe")
        receipt = store.publish_immutable_bytes(
            plan_components("spoofed-bytes-conversion"),
            small,
        )
        self.assertEqual(receipt.byte_size, 4)
        self.assertEqual(receipt.path.read_bytes(), b"safe")

    def test_gzip_reader_rejects_noncanonical_corrupt_and_unknown_codec(self) -> None:
        raw = b"compress-me" * 1000
        ref = self.put(raw)
        self.assertEqual(ref["content_encoding"], "gzip")
        path = self.store.blob_path(ref)
        original = path.read_bytes()

        bad_crc = original[:-8] + bytes([original[-8] ^ 1]) + original[-7:]
        variants = {
            "trailing": original + b"trailing-garbage",
            "second-member": original + gzip.compress(b"second-member", mtime=0),
            "truncated": original[:-1],
            "bad-crc": bad_crc,
            "alternate-valid-header": gzip.compress(raw, compresslevel=9, mtime=1),
        }
        for name, changed in variants.items():
            with self.subTest(name=name):
                changed_ref = copy.deepcopy(ref)
                changed_ref["stored_size"] = len(changed)
                changed_ref["stored_sha256"] = hashlib.sha256(changed).hexdigest()
                path.write_bytes(changed)
                self.assert_error_code(
                    "PROMPT_ARCHIVE_BLOB_CORRUPT",
                    lambda changed_ref=changed_ref: self.store.read_blob(changed_ref),
                )
                path.write_bytes(original)

        unknown_codec = copy.deepcopy(ref)
        unknown_codec["encoding_parameters"] = (
            "gzip-rfc1952-zlib-1.2.13-level9-v2"
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
            lambda: self.store.read_blob(unknown_codec),
        )

    def test_exact_typed_immutable_control_layouts_are_accepted(self) -> None:
        operation_id = operation_id_for("typed-layouts")
        layouts = (
            operation_plan_components(operation_id),
            (
                "operations",
                "v1",
                RESPONSE_ID,
                operation_id,
                "journal-generations",
                f"0-{JOURNAL_SHA256}.json",
            ),
            (
                "pending",
                "v1",
                "generations",
                "ab",
                f"1-{REGISTRY_ID}.json",
            ),
            (
                "sprints",
                SPRINT_STORAGE_KEY,
                "pointers",
                "v1",
                f"{RESPONSE_ID}.json",
            ),
            (
                "sprints",
                SPRINT_STORAGE_KEY,
                "archives",
                "v1",
                f"{ARCHIVE_ID}.json",
            ),
            ("inventories", "v1", f"{INVENTORY_ID}.json"),
        )
        for index, components in enumerate(layouts):
            payload = f'{{"layout":{index}}}'.encode("ascii")
            with self.subTest(components=components):
                receipt = self.store.publish_immutable_bytes(components, payload)
                self.assertTrue(receipt.created)
                self.assertEqual(receipt.path.read_bytes(), payload)
                self.assertEqual(
                    self.store.verify_immutable_bytes(
                        components,
                        expected_sha256=hashlib.sha256(payload).hexdigest(),
                        expected_size=len(payload),
                    ),
                    payload,
                )

    def test_typed_immutable_paths_reject_near_misses_and_mutable_controls(self) -> None:
        operation_id = operation_id_for("typed-rejections")

        class EqualToEveryString(str):
            def __eq__(self, _other) -> bool:
                return True

            __hash__ = str.__hash__

        invalid_layouts = (
            # Old generic/arbitrary layouts cannot poison a control namespace.
            ("operations", operation_id, "plan.json"),
            ("operations", "v2", RESPONSE_ID, operation_id, "plan.json"),
            ("operations", "v1", RESPONSE_ID.upper(), operation_id, "plan.json"),
            ("operations", "v1", RESPONSE_ID, operation_id.upper(), "plan.json"),
            ("operations", "v1", RESPONSE_ID, operation_id, "other.json"),
            (
                "operations",
                "v1",
                RESPONSE_ID,
                operation_id,
                "journal-generations",
                f"7-{JOURNAL_SHA256}.json",
            ),
            (
                "operations",
                "v1",
                RESPONSE_ID,
                operation_id,
                "journal-generations",
                f"00-{JOURNAL_SHA256}.json",
            ),
            (
                "pending",
                "v1",
                "generations",
                "AB",
                f"1-{REGISTRY_ID}.json",
            ),
            (
                "pending",
                "v1",
                "generations",
                "ab",
                f"01-{REGISTRY_ID}.json",
            ),
            (
                "pending",
                "v1",
                "generations",
                "ab",
                f"9223372036854775808-{REGISTRY_ID}.json",
            ),
            (
                "sprints",
                "sprint-raw-id",
                "pointers",
                "v1",
                f"{RESPONSE_ID}.json",
            ),
            (
                "sprints",
                SPRINT_STORAGE_KEY,
                "archives",
                "v1",
                f"pararc1-{'3' * 64}.json",
            ),
            ("inventories", "v1", f"{INVENTORY_ID.upper()}.json"),
            # These are stable PAS1 paths, but are mutable CAS/current files or
            # lock receipts and must not use immutable create-or-verify.
            ("operations", "v1", RESPONSE_ID, operation_id, "journal.json"),
            ("operations", "v1", RESPONSE_ID, operation_id, "operation.lock"),
            ("pending", "v1", "shards", "ab.json"),
            ("pending", "v1", "locks", "ab.lock"),
            ("locks", "v1", f"{RESPONSE_ID}.lock"),
            (
                EqualToEveryString("evil-top"),
                EqualToEveryString("evil-version"),
                RESPONSE_ID,
                operation_id,
                EqualToEveryString("evil.json"),
            ),
        )
        for components in invalid_layouts:
            with self.subTest(components=components):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda components=components: self.store.publish_immutable_bytes(
                        components,
                        b"must-not-publish",
                    ),
                )

        class OversizedComponents(tuple):
            def __len__(self) -> int:
                return 7

            def __iter__(self):
                raise AssertionError("oversized components were copied before rejection")

        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.store.publish_immutable_bytes(
                OversizedComponents(("operations", "v1", "unused")),
                b"must-not-publish",
            ),
        )
        self.assertEqual(
            [path for path in self.root.rglob("*") if path.is_file()],
            [],
        )

    def test_immutable_artifact_and_verification_caps_fail_before_allocation(self) -> None:
        locator = (
            "sprints",
            SPRINT_STORAGE_KEY,
            "archives",
            "v1",
            f"{ARCHIVE_ID}.json",
        )
        accepted = b"x" * (64 * 1024)
        receipt = self.store.publish_immutable_bytes(locator, accepted)
        self.assertEqual(receipt.byte_size, len(accepted))

        oversized_locator = (
            "sprints",
            SPRINT_STORAGE_KEY,
            "archives",
            "v1",
            f"par1-{'7' * 64}.json",
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: self.store.publish_immutable_bytes(
                oversized_locator,
                AllocationGuardBytearray(b"x" * (64 * 1024 + 1)),
            ),
        )

        plan = plan_components("verification-cap")
        payload = b"0123456789abcdef"
        self.store.publish_immutable_bytes(plan, payload)
        bounded = PromptArchiveStorage(
            self.root,
            PROJECT_ID,
            protected_roots=(),
            limits=dataclasses.replace(
                PromptArchiveLimits(),
                max_immutable_bytes=8,
            ),
        )
        with mock.patch(
            "nginx_qa.prompt_archive_storage.os.open",
            side_effect=AssertionError("oversized verification reached the filesystem"),
        ):
            self.assert_error_code(
                "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
                lambda: bounded.verify_immutable_bytes(
                    plan,
                    expected_sha256=hashlib.sha256(payload).hexdigest(),
                    expected_size=len(payload),
                ),
            )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: bounded.verify_immutable_bytes(
                plan,
                expected_sha256=hashlib.sha256(payload).hexdigest(),
            ),
        )

    def test_canonical_encoder_rejects_tiny_budget_before_large_token_render(self) -> None:
        bounded = PromptArchiveStorage(
            self.root / "tiny-canonical-budget",
            PROJECT_ID,
            protected_roots=(),
            limits=dataclasses.replace(
                PromptArchiveLimits(),
                max_immutable_bytes=1,
            ),
        )
        huge_integer = 10**1000
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: bounded.publish_immutable_json(
                plan_components("tiny-integer"),
                huge_integer,
            ),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: bounded.publish_immutable_json(
                plan_components("tiny-string"),
                "x" * 100_000,
            ),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: bounded.publish_immutable_json(
                plan_components("tiny-list"),
                [None] * 100_000,
            ),
        )
        with mock.patch(
            "builtins.sorted",
            side_effect=AssertionError("over-budget mapping reached key sorting"),
        ):
            self.assert_error_code(
                "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
                lambda: bounded.publish_immutable_json(
                    plan_components("tiny-mapping"),
                    {"large-key": None},
                ),
            )
        self.assertEqual(list(bounded.root.rglob("*.tmp")), [])
        self.assertEqual(list(bounded.root.rglob("plan.json")), [])

    def test_canonical_encoder_rejects_scalar_and_container_subclasses(self) -> None:
        class SpoofedInteger(int):
            def __str__(self) -> str:
                return "null"

        class ReorderedString(str):
            def __lt__(self, _other) -> bool:
                return True

        for label, document in (
            ("integer-subclass", SpoofedInteger(7)),
            ("string-subclass", ReorderedString("value")),
            ("list-subclass", type("ListSubclass", (list,), {})([1])),
            ("dict-subclass", type("DictSubclass", (dict,), {})({"a": 1})),
            ("key-subclass", {ReorderedString("a"): 1, "b": 2}),
        ):
            with self.subTest(label=label):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda document=document, label=label: self.store.publish_immutable_json(
                        plan_components(label),
                        document,
                    ),
                )
        self.assertEqual(list(self.root.rglob("plan.json")), [])

    def test_immutable_bytes_and_json_are_create_or_verify_and_recover_idempotently(self) -> None:
        components = plan_components("example-1")
        raw = b'{"plan":"exact"}\n'
        digest = hashlib.sha256(raw).hexdigest()
        created = self.store.publish_immutable_bytes(
            components,
            raw,
            expected_sha256=digest,
        )
        self.assertTrue(created.created)
        self.assertEqual(created.sha256, digest)
        self.assertEqual(created.byte_size, len(raw))
        self.assertEqual(created.path.read_bytes(), raw)

        recovered = self.store.publish_immutable_bytes(
            components,
            raw,
            expected_sha256=digest,
        )
        self.assertFalse(recovered.created)
        self.assertEqual(recovered.path, created.path)
        self.assertEqual(
            self.store.verify_immutable_bytes(
                components,
                expected_sha256=digest,
                expected_size=len(raw),
            ),
            raw,
        )

        conflict = self.assert_error_code(
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
            lambda: self.store.publish_immutable_bytes(
                components,
                b'{"plan":"different"}\n',
            ),
        )
        self.assertTrue(conflict.retryable)
        self.assertNotIn(str(created.path), str(conflict))
        self.assertNotIn("different", str(conflict))
        self.assertEqual(created.path.read_bytes(), raw)

        document = {"b": "Привет", "a": [True, None, -2, "\n"]}
        canonical = '{"a":[true,null,-2,"\\u000a"],"b":"Привет"}'.encode(
            "utf-8"
        )
        json_components = plan_components("example-json")
        json_receipt = self.store.publish_immutable_json(json_components, document)
        self.assertTrue(json_receipt.created)
        self.assertEqual(json_receipt.path.read_bytes(), canonical)
        json_replay = self.store.publish_immutable_json(json_components, document)
        self.assertFalse(json_replay.created)
        self.assertEqual(json_replay.sha256, hashlib.sha256(canonical).hexdigest())

        corrupt_components = plan_components("example-corrupt")
        corrupt_receipt = self.store.publish_immutable_bytes(
            corrupt_components,
            raw,
        )
        corrupt_bytes = b"!" + raw[1:]
        corrupt_receipt.path.write_bytes(corrupt_bytes)
        self.assert_error_code(
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
            lambda: self.store.publish_immutable_bytes(corrupt_components, raw),
        )
        self.assertEqual(corrupt_receipt.path.read_bytes(), corrupt_bytes)

    def test_immutable_json_rejects_noncanonical_values_before_publication(self) -> None:
        invalid_documents = (
            {"float": 1.25},
            {"nan": float("nan")},
            {1: "non-string-key"},
            {"surrogate": "\ud800"},
        )
        for index, document in enumerate(invalid_documents):
            components = plan_components(f"invalid-json-{index}")
            with self.subTest(document=document):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda components=components, document=document: (
                        self.store.publish_immutable_json(components, document)
                    ),
                )

        mismatch_components = plan_components("integrity-mismatch")
        self.assert_error_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: self.store.publish_immutable_bytes(
                mismatch_components,
                b"planned-bytes",
                expected_sha256="f" * 64,
            ),
        )
        self.assertFalse(
            (
                self.root
                / "projects"
                / self.store.project_storage_key
                / Path(*mismatch_components)
            ).exists()
        )

    def test_concurrent_immutable_publication_is_first_writer_wins(self) -> None:
        components = plan_components("race-1")
        raw = b'{"winner":"same"}\n'
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            receipts = list(
                executor.map(
                    lambda _index: self.store.publish_immutable_bytes(
                        components,
                        raw,
                    ),
                    range(32),
                )
            )

        self.assertEqual(sum(receipt.created for receipt in receipts), 1)
        self.assertTrue(all(receipt.path == receipts[0].path for receipt in receipts))
        self.assertEqual(receipts[0].path.read_bytes(), raw)
        self.assertEqual(
            self.store.verify_immutable_bytes(
                components,
                expected_sha256=hashlib.sha256(raw).hexdigest(),
                expected_size=len(raw),
            ),
            raw,
        )

        divergent_components = plan_components("divergent-race-1")
        candidates = [f'{{"candidate":{index}}}\n'.encode() for index in range(16)]

        def publish_candidate(candidate: bytes):
            try:
                return (
                    "ok",
                    candidate,
                    self.store.publish_immutable_bytes(
                        divergent_components,
                        candidate,
                    ),
                )
            except PromptArchiveStorageError as exc:
                return ("error", candidate, exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            outcomes = list(executor.map(publish_candidate, candidates))

        successes = [outcome for outcome in outcomes if outcome[0] == "ok"]
        conflicts = [outcome for outcome in outcomes if outcome[0] == "error"]
        self.assertEqual(len(successes), 1, outcomes)
        self.assertTrue(
            all(
                outcome[2].code == "PROMPT_ARCHIVE_GENERATION_CONFLICT"
                for outcome in conflicts
            ),
            outcomes,
        )
        winner = successes[0][1]
        self.assertEqual(successes[0][2].path.read_bytes(), winner)
        self.assertIn(winner, candidates)

    def test_immutable_fault_boundaries_are_replay_safe_and_never_replace(self) -> None:
        payload = b'{"fault":"immutable-boundary"}\n'
        post_publish_stages = {"after_publish", "after_directory_sync"}
        stages = (
            "after_parent_pin",
            "after_temp_create",
            "after_temp_write",
            "after_temp_flush",
            "after_temp_fsync",
            "after_temp_verify",
            "before_publish",
            "after_parent_precheck",
            "after_publish",
            "after_directory_sync",
        )
        for stage in stages:
            with self.subTest(stage=stage):
                root = self.root / f"immutable-fault-{stage.replace('_', '-')}"
                components = plan_components(f"immutable-{stage}")

                def inject(observed_stage: str, *, selected: str = stage) -> None:
                    if observed_stage == selected:
                        raise InjectedPublicationFault(selected)

                faulting = PromptArchiveStorage(
                    root, PROJECT_ID, protected_roots=(), fault_hook=inject
                )
                with self.assertRaises(InjectedPublicationFault):
                    faulting.publish_immutable_bytes(components, payload)

                finals = list(root.rglob("plan.json"))
                self.assertEqual(
                    len(finals),
                    1 if stage in post_publish_stages else 0,
                )
                self.assertEqual(list(root.rglob("*.tmp")), [])

                recovered = PromptArchiveStorage(root, PROJECT_ID, protected_roots=())
                receipt = recovered.publish_immutable_bytes(components, payload)
                self.assertEqual(receipt.created, stage not in post_publish_stages)
                self.assertEqual(receipt.path.read_bytes(), payload)
                self.assertEqual(list(root.rglob("*.tmp")), [])

    def test_process_crash_before_and_after_rename_recovers_idempotently(self) -> None:
        context = multiprocessing.get_context("spawn")
        payload = b'{"actual-process-crash":true}\n'
        for stage, final_exists in (
            ("after_temp_fsync", False),
            ("after_publish", True),
        ):
            with self.subTest(stage=stage):
                root = self.root / f"process-crash-{stage}"
                operation_id = operation_id_for(f"process-crash-{stage}")
                process = context.Process(
                    target=process_crash_publish_immutable,
                    args=(str(root), operation_id, payload, stage),
                )
                process.start()
                process.join(timeout=20)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                    self.fail("crash-injection process did not terminate")
                self.assertEqual(process.exitcode, 77)

                store = PromptArchiveStorage(root, PROJECT_ID, protected_roots=())
                target = (
                    root
                    / "projects"
                    / store.project_storage_key
                    / "operations"
                    / "v1"
                    / RESPONSE_ID
                    / operation_id
                    / "plan.json"
                )
                self.assertEqual(target.exists(), final_exists)
                receipt = store.publish_immutable_bytes(
                    operation_plan_components(operation_id),
                    payload,
                )
                self.assertEqual(receipt.created, not final_exists)
                self.assertEqual(receipt.path.read_bytes(), payload)

    def test_immutable_winner_is_verified_after_publish_before_success(self) -> None:
        root = self.root / "immutable-post-publish-verification"
        payload = b'{"planned":"immutable"}\n'
        corrupt = b"!" + payload[1:]
        components = plan_components("post-publish-verification")

        def corrupt_winner(stage: str) -> None:
            if stage == "after_publish":
                winners = list(root.rglob("plan.json"))
                self.assertEqual(len(winners), 1)
                winners[0].write_bytes(corrupt)

        store = PromptArchiveStorage(
            root, PROJECT_ID, protected_roots=(), fault_hook=corrupt_winner
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: store.publish_immutable_bytes(components, payload),
        )
        final_path = list(root.rglob("plan.json"))
        self.assertEqual(len(final_path), 1)
        self.assertEqual(final_path[0].read_bytes(), corrupt)
        self.assertEqual(list(root.rglob("*.tmp")), [])

        replay = PromptArchiveStorage(root, PROJECT_ID, protected_roots=())
        self.assert_error_code(
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
            lambda: replay.publish_immutable_bytes(components, payload),
        )
        self.assertEqual(final_path[0].read_bytes(), corrupt)

    def test_process_level_immutable_races_are_first_writer_wins(self) -> None:
        context = multiprocessing.get_context("spawn")
        same_root = self.root / "immutable-process-same"
        same_payload = b'{"same":true}\n'
        same_queue = context.Queue()
        same_processes = [
            context.Process(
                target=process_publish_immutable,
                args=(
                    str(same_root),
                    operation_id_for("process-same"),
                    same_payload,
                    same_queue,
                ),
            )
            for _ in range(4)
        ]
        same_results = self.run_processes(same_processes, same_queue)
        self.assertTrue(all(result[0] == "ok" for result in same_results), same_results)
        self.assertEqual(sum(result[2] for result in same_results), 1)
        self.assertEqual(
            {result[3] for result in same_results},
            {hashlib.sha256(same_payload).hexdigest()},
        )
        self.assertEqual({result[4] for result in same_results}, {len(same_payload)})

        same_store = PromptArchiveStorage(same_root, PROJECT_ID, protected_roots=())
        same_components = plan_components("process-same")
        self.assertEqual(
            same_store.verify_immutable_bytes(
                same_components,
                expected_sha256=hashlib.sha256(same_payload).hexdigest(),
                expected_size=len(same_payload),
            ),
            same_payload,
        )

        divergent_root = self.root / "immutable-process-divergent"
        candidates = [f'{{"candidate":{index}}}\n'.encode() for index in range(4)]
        divergent_queue = context.Queue()
        divergent_processes = [
            context.Process(
                target=process_publish_immutable,
                args=(
                    str(divergent_root),
                    operation_id_for("process-divergent"),
                    candidate,
                    divergent_queue,
                ),
            )
            for candidate in candidates
        ]
        divergent_results = self.run_processes(
            divergent_processes,
            divergent_queue,
        )
        winners = [result for result in divergent_results if result[0] == "ok"]
        losers = [
            result for result in divergent_results if result[0] == "storage-error"
        ]
        self.assertEqual(len(winners), 1, divergent_results)
        self.assertEqual(len(losers), len(candidates) - 1, divergent_results)
        self.assertTrue(
            all(
                result[2] == "PROMPT_ARCHIVE_GENERATION_CONFLICT"
                and result[3]
                for result in losers
            ),
            divergent_results,
        )
        winner = winners[0][1]
        final_path = (
            divergent_root
            / "projects"
            / PromptArchiveStorage(
                divergent_root, PROJECT_ID, protected_roots=()
            ).project_storage_key
            / "operations"
            / "v1"
            / RESPONSE_ID
            / operation_id_for("process-divergent")
            / "plan.json"
        )
        self.assertEqual(final_path.read_bytes(), winner)
        self.assertIn(winner, candidates)
        self.assertEqual(list(divergent_root.rglob("*.tmp")), [])

    def test_immutable_names_reject_traversal_absolute_ads_and_unknown_components(self) -> None:
        invalid_components = (
            ("..", "escape.json"),
            ("operations", "..", "escape.json"),
            ("operations/escape", "file.json"),
            ("operations\\escape", "file.json"),
            ("C:\\outside", "file.json"),
            ("operations", "file.json:stream"),
            ("", "file.json"),
            (".", "file.json"),
            ("CON", "file.json"),
            ("aux.txt", "file.json"),
            ("trailing.", "file.json"),
            ("trailing ", "file.json"),
            ("\\\\server\\share", "file.json"),
            ("\\\\?\\C:\\device", "file.json"),
        )
        for components in invalid_components:
            with self.subTest(components=components):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda components=components: self.store.publish_immutable_bytes(
                        components,
                        b"data",
                    ),
                )
        self.assertFalse((self.root.parent / "escape.json").exists())

    def test_immutable_helper_rejects_reserved_and_unknown_namespaces(self) -> None:
        forbidden = (
            ("blobs", "v1", "sha256", "aa", "bb", "payload.blob"),
            ("projects", "nested-project", "plan.json"),
            ("unknown-top-level", "plan.json"),
        )
        for components in forbidden:
            with self.subTest(components=components):
                self.assert_error_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda components=components: self.store.publish_immutable_bytes(
                        components,
                        b"must-not-publish",
                    ),
                )
        self.assertEqual(list(self.root.rglob("payload.blob")), [])
        self.assertEqual(list(self.root.rglob("plan.json")), [])

    def test_mutable_controls_round_trip_under_one_pinned_lock(self) -> None:
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        lock = ("operations", "v1", response_id, operation_id, "operation.lock")
        journal = ("operations", "v1", response_id, operation_id, "journal.json")

        with self.store.control_lock(lock):
            self.assertIsNone(self.store.read_current_json(journal))
            self.store.replace_current_json(journal, {"revision": 1})
            self.assertEqual(
                self.store.read_current_json(journal),
                {"revision": 1},
            )
            self.store.replace_current_json(journal, {"revision": 2})
            self.assertEqual(
                self.store.read_current_json(journal),
                {"revision": 2},
            )

    def test_mutable_lock_and_journal_hardlink_aliases_are_rejected(self) -> None:
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        lock = ("operations", "v1", response_id, operation_id, "operation.lock")
        journal = ("operations", "v1", response_id, operation_id, "journal.json")
        with self.store.control_lock(lock):
            self.store.replace_current_json(journal, {"revision": 1})

        lock_path = self.store._mutable_path_from_validated(lock)
        journal_path = self.store._mutable_path_from_validated(journal)
        lock_alias = lock_path.with_name("lock-hardlink-alias")
        journal_alias = journal_path.with_name("journal-hardlink-alias")
        try:
            os.link(lock_path, lock_alias)
            os.link(journal_path, journal_alias)
        except OSError as exc:
            self.assertIsInstance(exc, OSError)
            return

        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.store.read_current_json(journal),
        )
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.store.control_lock(lock).__enter__(),
        )

    def test_mutable_controls_reject_mocked_leaf_reparse_before_io(self) -> None:
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        journal = ("operations", "v1", response_id, operation_id, "journal.json")
        self.store.replace_current_json(journal, {"outside": False})
        target = self.store._mutable_path_from_validated(journal)
        original = prompt_archive_storage_module._path_entry_is_redirected

        def selected_reparse(path: Path) -> bool:
            if path == target:
                return True
            return original(path)

        with mock.patch(
            "nginx_qa.prompt_archive_storage._path_entry_is_redirected",
            side_effect=selected_reparse,
        ):
            self.assert_error_code(
                "PROMPT_ARCHIVE_REFERENCE_INVALID",
                lambda: self.store.read_current_json(journal),
            )

    def test_mutable_descendant_parent_redirect_cannot_read_or_write_outside(self) -> None:
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        journal = ("operations", "v1", response_id, operation_id, "journal.json")
        target = self.store._mutable_path_from_validated(journal)
        self.store._ensure_directory(target.parent)
        outside = self.root.parent / "mutable-outside"
        outside.mkdir()
        outside_target = outside / target.name
        outside_bytes = b'{"outside":true}'
        outside_target.write_bytes(outside_bytes)
        target.parent.rmdir()
        try:
            target.parent.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            if os.name != "nt":
                self.assertIsInstance(exc, (OSError, NotImplementedError))
                return
            junction = subprocess.run(
                [
                    "cmd.exe",
                    "/d",
                    "/c",
                    "mklink",
                    "/J",
                    str(target.parent),
                    str(outside),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if junction.returncode != 0:
                self.assertNotEqual(junction.returncode, 0)
                return

        for operation in (
            lambda: self.store.read_current_json(journal),
            lambda: self.store.replace_current_json(journal, {"outside": False}),
        ):
            self.assert_error_code(
                "PROMPT_ARCHIVE_REFERENCE_INVALID",
                operation,
            )
        self.assertEqual(outside_target.read_bytes(), outside_bytes)

    def test_mutable_parent_swap_cannot_publish_outside_pinned_directory(self) -> None:
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        journal = ("operations", "v1", response_id, operation_id, "journal.json")
        target = self.store._mutable_path_from_validated(journal)
        self.store._ensure_directory(target.parent)
        outside = self.root.parent / "mutable-swap-outside"
        outside.mkdir()
        detached = target.parent.with_name(target.parent.name + "-detached")
        original = self.store._replace_relative_control_file
        swapped = False

        def swap_then_replace(descriptor, parent, temporary_name, target_name):
            nonlocal swapped
            try:
                target.parent.rename(detached)
            except OSError:
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE") from None
            swapped = True
            if os.name == "nt":
                junction = subprocess.run(
                    [
                        "cmd.exe",
                        "/d",
                        "/c",
                        "mklink",
                        "/J",
                        str(target.parent),
                        str(outside),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                if junction.returncode != 0:
                    raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
            else:
                target.parent.symlink_to(outside, target_is_directory=True)
            return original(descriptor, parent, temporary_name, target_name)

        with mock.patch.object(
            self.store,
            "_replace_relative_control_file",
            side_effect=swap_then_replace,
        ):
            with self.assertRaises(PromptArchiveStorageError) as raised:
                self.store.replace_current_json(journal, {"revision": 1})
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID"
            if swapped
            else "PROMPT_ARCHIVE_UNAVAILABLE",
        )
        self.assertEqual(list(outside.iterdir()), [])

    def test_mutable_ancestor_swap_cannot_redirect_lock_creation(self) -> None:
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        lock = ("operations", "v1", response_id, operation_id, "operation.lock")
        target = self.store._mutable_path_from_validated(lock)
        self.store._ensure_directory(target.parent)
        ancestor = self.store._project_root / "operations"
        detached = ancestor.with_name("operations-detached")
        outside = self.root.parent / "mutable-ancestor-outside"
        outside.mkdir()
        original = self.store._open_relative_control_file
        swapped = False

        def swap_then_open(parent, selected_target, **kwargs):
            nonlocal swapped
            try:
                ancestor.rename(detached)
            except OSError:
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE") from None
            swapped = True
            if os.name == "nt":
                junction = subprocess.run(
                    [
                        "cmd.exe",
                        "/d",
                        "/c",
                        "mklink",
                        "/J",
                        str(ancestor),
                        str(outside),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                if junction.returncode != 0:
                    raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
            else:
                ancestor.symlink_to(outside, target_is_directory=True)
            return original(parent, selected_target, **kwargs)

        with mock.patch.object(
            self.store,
            "_open_relative_control_file",
            side_effect=swap_then_open,
        ):
            with self.assertRaises(PromptArchiveStorageError) as raised:
                with self.store.control_lock(lock):
                    self.fail("redirected lock path must not be acquired")
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID"
            if swapped
            else "PROMPT_ARCHIVE_UNAVAILABLE",
        )
        self.assertEqual(list(outside.iterdir()), [])

    def test_mutable_replace_never_uses_unpinned_absolute_replace(self) -> None:
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        journal = ("operations", "v1", response_id, operation_id, "journal.json")
        original = os.replace

        def require_dirfds(source, target, *args, **kwargs):
            if kwargs.get("src_dir_fd") is None or kwargs.get("dst_dir_fd") is None:
                raise AssertionError("mutable replace escaped its pinned parent")
            return original(source, target, *args, **kwargs)

        with mock.patch(
            "nginx_qa.prompt_archive_storage.os.replace",
            side_effect=require_dirfds,
        ):
            self.store.replace_current_json(journal, {"revision": 1})
            self.store.replace_current_json(journal, {"revision": 2})
        self.assertEqual(
            self.store.read_current_json(journal),
            {"revision": 2},
        )

    def test_mutable_control_lock_serializes_cross_process_writers(self) -> None:
        context = multiprocessing.get_context("spawn")
        result_queue = context.Queue()
        processes = [
            context.Process(
                target=process_increment_mutable_control,
                args=(str(self.root), 8, result_queue),
            )
            for _ in range(4)
        ]
        outcomes = self.run_processes(processes, result_queue)
        self.assertEqual(outcomes, [("ok", 8)] * 4)
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        journal = ("operations", "v1", response_id, operation_id, "journal.json")
        self.assertEqual(self.store.read_current_json(journal), {"value": 32})

    def test_mutable_control_rejects_wrong_case_leaf_alias(self) -> None:
        if os.name != "nt":
            return
        response_id = "parresp1-" + "7" * 64
        operation_id = "parop1-" + "8" * 32
        journal = ("operations", "v1", response_id, operation_id, "journal.json")
        target = self.store._mutable_path_from_validated(journal)
        self.store._ensure_directory(target.parent)
        target.with_name("Journal.json").write_text('{"revision":1}', encoding="utf-8")
        self.assert_error_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.store.read_current_json(journal),
        )

    def test_archive_root_must_not_overlap_optional_protected_roots(self) -> None:
        protected = self.root / "protected"
        protected.mkdir(parents=True)
        nested_archive = protected / "archive"
        self.assert_error_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: PromptArchiveStorage(
                nested_archive,
                PROJECT_ID,
                protected_roots=(protected,),
            ),
        )
        self.assertFalse(nested_archive.exists())

        archive_parent = self.root / "archive-parent"
        protected_child = archive_parent / "runtime"
        protected_child.mkdir(parents=True)
        self.assert_error_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: PromptArchiveStorage(
                archive_parent,
                PROJECT_ID,
                protected_roots=(protected_child,),
            ),
        )


if __name__ == "__main__":
    unittest.main()
