"""Crash-safe, path-free content-addressed storage for Prompt Archive V1.

This module deliberately has no dependency on the response builder or HTTP
runtime.  It implements the small storage seam frozen by PSC-001: immutable
typed blob references, deterministic storage bytes, bounded verification, and
create-or-verify publication for immutable control artifacts.

No caller-supplied filesystem path is accepted for a blob.  Its physical path
is derived solely from the validated project storage key and lowercase content
digest.  The generic immutable helpers accept only closed single path
components and are intended for higher-level PAS1 control-artifact code.
"""

from __future__ import annotations

import dataclasses
import ctypes
import errno
import hashlib
import json
import os
import re
import stat
import struct
import threading
import time
import zlib
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Final
from uuid import uuid4


_MIB: Final = 1024 * 1024
_GZIP_THRESHOLD: Final = 1024
_GZIP_CODEC: Final = "gzip-rfc1952-zlib-1.2.13-level9-v1"
_GZIP_HEADER: Final = bytes.fromhex("1f8b08000000000002ff")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$", re.ASCII)
_MEDIA_TYPE_RE: Final = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,63}/"
    r"[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,62}$",
    re.ASCII,
)
_COMPONENT_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", re.ASCII)
_RESPONSE_ID_RE: Final = re.compile(r"^parresp1-[0-9a-f]{64}$", re.ASCII)
_OPERATION_ID_RE: Final = re.compile(r"^parop1-[0-9a-f]{32}$", re.ASCII)
_SPRINT_STORAGE_KEY_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ARCHIVE_ID_RE: Final = re.compile(r"^par1-[0-9a-f]{64}$", re.ASCII)
_INVENTORY_ID_RE: Final = re.compile(r"^parinv1-[0-9a-f]{64}$", re.ASCII)
_MIGRATION_BATCH_ID_RE: Final = re.compile(r"^parmb1-[0-9a-f]{64}$", re.ASCII)
_PENDING_REGISTRY_ID_RE: Final = re.compile(
    r"^parpending1-[0-9a-f]{64}$",
    re.ASCII,
)
_JOURNAL_GENERATION_FILE_RE: Final = re.compile(
    r"^[0-6]-[0-9a-f]{64}\.json$",
    re.ASCII,
)
_MIGRATION_JOURNAL_GENERATION_FILE_RE: Final = re.compile(
    r"^(?:0|[1-9][0-9]{0,18})-[0-9a-f]{64}\.json$",
    re.ASCII,
)
_PENDING_GENERATION_FILE_RE: Final = re.compile(
    r"^([1-9][0-9]{0,18})-(parpending1-[0-9a-f]{64})\.json$",
    re.ASCII,
)
_SHARD_RE: Final = re.compile(r"^[0-9a-f]{2}$", re.ASCII)
_STORAGE_TEMP_FILE_RE: Final = re.compile(
    r"^\.(.+)\.([0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15})\.tmp$",
    re.ASCII,
)
_REPARSE_POINT: Final = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)

_REF_KEYS: Final = frozenset(
    {
        "schema_version",
        "kind",
        "hash_algorithm",
        "digest",
        "logical_kind",
        "media_type",
        "content_encoding",
        "encoding_parameters",
        "uncompressed_size",
        "stored_size",
        "stored_sha256",
    }
)
_REF_REQUIRED_KEYS: Final = _REF_KEYS - {"encoding_parameters"}

_RECORD_KINDS: Final = frozenset(
    {
        "event_record",
        "patch_record",
        "history_text_record",
        "result_record",
        "feedback_record",
        "review_context_record",
        "execution_audit_record",
        "source_metadata_record",
        "current_field_record",
    }
)
_FIXED_MEDIA_TYPES: Final = {
    "raw_chunk": "application/json",
    "render_plan_chunk": "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
    "literal_chunk": "application/vnd.nginx-qa.prompt-archive-json-fragment-v1",
    "json_value": "application/vnd.nginx-qa.prompt-archive-json-value-v1+json",
    "json_edge": "application/vnd.nginx-qa.prompt-archive-json-edge-v1+json",
    "index_node": "application/vnd.nginx-qa.prompt-archive-index-node-v1+json",
    "manifest": "application/vnd.nginx-qa.prompt-archive-manifest-v1+json",
    "publication_plan": "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json",
    "journal": "application/vnd.nginx-qa.prompt-archive-journal-v1+json",
    "pointer": "application/vnd.nginx-qa.prompt-archive-pointer-v1+json",
    "archive_locator": "application/vnd.nginx-qa.prompt-archive-locator-v1+json",
    "descriptor_stub": "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json",
    "latest_slot": "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json",
    "source_inventory": "application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json",
    "migration_batch_plan": (
        "application/vnd.nginx-qa.prompt-archive-migration-batch-plan-v1+json"
    ),
    "migration_batch_journal": (
        "application/vnd.nginx-qa.prompt-archive-migration-batch-journal-v1+json"
    ),
    "migration_evidence": (
        "application/vnd.nginx-qa.prompt-archive-migration-evidence-v1+json"
    ),
    "pending_operation_registry": (
        "application/vnd.nginx-qa.prompt-archive-pending-operation-registry-v1+json"
    ),
}
_RECORD_MEDIA_TYPE: Final = "application/vnd.nginx-qa.prompt-archive-record-v1+json"
_PAYLOAD_MEDIA_TYPES: Final = frozenset(
    {
        "application/json",
        "application/vnd.nginx-qa.prompt-archive-source-metadata-v1+json",
        "application/vnd.nginx-qa.prompt-archive-json-string-fragment-v1",
    }
)
_LOGICAL_KINDS: Final = frozenset(_FIXED_MEDIA_TYPES) | _RECORD_KINDS | {
    "payload_chunk"
}
_ARCHIVE_LOCATOR_MAX_BYTES: Final = 64 * 1024
_MAX_CONTROL_JSON_DEPTH: Final = 128
_MUTABLE_CONTROL_MAX_BYTES: Final = 8 * _MIB
_MAX_PENDING_GENERATION: Final = 9_223_372_036_854_775_807
_BYTES_LIKE_TYPES: Final = (bytes, bytearray, memoryview)

_ERROR_MESSAGE: Final = "Prompt archive is unavailable for this request."
_ERRORS: Final = {
    "PROMPT_ARCHIVE_REQUEST_INVALID": (400, False),
    "PROMPT_ARCHIVE_NOT_FOUND": (404, False),
    "PROMPT_ARCHIVE_VERSION_UNSUPPORTED": (
        409,
        False,
    ),
    "PROMPT_ARCHIVE_REFERENCE_INVALID": (
        409,
        False,
    ),
    "PROMPT_ARCHIVE_BLOB_MISSING": (409, False),
    "PROMPT_ARCHIVE_BLOB_CORRUPT": (409, False),
    "PROMPT_ARCHIVE_INTEGRITY_MISMATCH": (
        409,
        False,
    ),
    "PROMPT_ARCHIVE_GENERATION_CONFLICT": (
        409,
        True,
    ),
    "PROMPT_ARCHIVE_BUSY": (429, True),
    "PROMPT_ARCHIVE_LIMIT_EXCEEDED": (
        413,
        False,
    ),
    "PROMPT_ARCHIVE_UNAVAILABLE": (503, True),
}


class PromptArchiveStorageError(RuntimeError):
    """Payload-safe stable storage error.

    The exception text is fixed by code.  Paths, refs, payload fragments and
    operating-system exception text are intentionally never interpolated.
    """

    def __init__(self, code: str) -> None:
        if code not in _ERRORS:
            code = "PROMPT_ARCHIVE_UNAVAILABLE"
        http_status, retryable = _ERRORS[code]
        super().__init__(_ERROR_MESSAGE)
        self.code = code
        self.http_status = http_status
        self.retryable = retryable
        self.message = _ERROR_MESSAGE


class PromptArchiveCancellationToken:
    """Cooperative, thread-safe cancellation for bounded archive reads.

    HTTP hydration runs in a worker thread, so cancelling the asyncio future
    alone cannot stop filesystem reads or decompression.  The caller owns this
    token and may signal it from the event-loop thread; storage checks it at
    every bounded read/decompression chunk.  Deadlines use ``monotonic`` time
    so wall-clock adjustments cannot extend a request's work budget.
    """

    __slots__ = ("event", "deadline")

    def __init__(
        self,
        *,
        event: threading.Event | None = None,
        deadline: float | None = None,
    ) -> None:
        if event is not None and not isinstance(event, threading.Event):
            raise TypeError("Prompt archive cancellation event must be threading.Event")
        if deadline is not None and not isinstance(deadline, (int, float)):
            raise TypeError("Prompt archive cancellation deadline must be monotonic time")
        self.event = event if event is not None else threading.Event()
        self.deadline = float(deadline) if deadline is not None else None

    @classmethod
    def with_timeout(cls, seconds: float) -> "PromptArchiveCancellationToken":
        if not isinstance(seconds, (int, float)) or seconds <= 0:
            raise ValueError("Prompt archive cancellation timeout must be positive")
        return cls(deadline=time.monotonic() + float(seconds))

    def cancel(self) -> None:
        self.event.set()

    def check(self) -> None:
        if self.event.is_set() or (
            self.deadline is not None and time.monotonic() >= self.deadline
        ):
            self.event.set()
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")


class _DuplicateControlMember(ValueError):
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchiveLimits:
    max_blob_uncompressed_bytes: int = 8 * _MIB
    max_blob_stored_bytes: int = 8 * _MIB + 64 * 1024
    max_compression_ratio: int = 2048
    max_hydrated_bytes: int = 256 * _MIB
    max_reference_depth: int = 512
    max_referenced_objects: int = 250_000
    max_immutable_bytes: int = 8 * _MIB

    def __post_init__(self) -> None:
        ceilings = {
            "max_blob_uncompressed_bytes": 16 * _MIB,
            "max_blob_stored_bytes": 16 * _MIB + 64 * 1024,
            "max_compression_ratio": 4096,
            "max_hydrated_bytes": 512 * _MIB,
            "max_reference_depth": 512,
            "max_referenced_objects": 1_000_000,
            "max_immutable_bytes": 8 * _MIB,
        }
        for name, ceiling in ceilings.items():
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError("Prompt archive limits must be positive integers")
            if value > ceiling:
                raise ValueError("Prompt archive limit exceeds the V1 hard ceiling")
        if self.max_reference_depth != 512:
            raise ValueError("Prompt archive V1 reference depth is fixed at 512")


@dataclasses.dataclass(frozen=True, slots=True)
class ImmutablePublicationReceipt:
    path: Path
    sha256: str
    byte_size: int
    created: bool


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchiveCasWriteMetrics:
    """Accepted CAS outcomes observed inside one explicit operation scope."""

    put_attempts: int
    new_object_count: int
    new_stored_bytes: int
    dedup_hits: int


@dataclasses.dataclass(slots=True)
class _PromptArchiveCasWriteAccounting:
    put_attempts: int = 0
    new_object_count: int = 0
    new_stored_bytes: int = 0
    dedup_hits: int = 0

    def record(self, *, stored_size: int, created: bool) -> None:
        self.put_attempts += 1
        if created:
            self.new_object_count += 1
            self.new_stored_bytes += stored_size
        else:
            self.dedup_hits += 1

    def snapshot(self) -> PromptArchiveCasWriteMetrics:
        return PromptArchiveCasWriteMetrics(
            put_attempts=self.put_attempts,
            new_object_count=self.new_object_count,
            new_stored_bytes=self.new_stored_bytes,
            dedup_hits=self.dedup_hits,
        )


def _error(code: str) -> PromptArchiveStorageError:
    return PromptArchiveStorageError(code)


def _storage_temporary_name(target_name: str) -> str:
    """Return the unique sibling name used by storage publication."""

    return f".{target_name}.{uuid4().hex}.tmp"


def _storage_temporary_target_name(name: str) -> str | None:
    """Classify an exact storage-created UUIDv4 sibling temp name."""

    if type(name) is not str:
        return None
    matched = _STORAGE_TEMP_FILE_RE.fullmatch(name)
    return None if matched is None else matched.group(1)


def _frame(value: str) -> bytes:
    try:
        encoded = value.encode("utf-8", errors="strict")
    except (UnicodeError, AttributeError) as exc:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
    return b"\x00" + struct.pack(">Q", len(encoded)) + encoded


def _project_storage_key(project_id: str) -> str:
    if (
        type(project_id) is not str
        or not 1 <= len(project_id) <= 160
        or _SAFE_ID_RE.fullmatch(project_id) is None
    ):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    preimage = (
        b"nginx-qa-pas1-id-v1\x00"
        + _frame("project-storage-key")
        + _frame("project_id")
        + _frame(project_id)
    )
    return hashlib.sha256(preimage).hexdigest()


def _path_entry_is_redirected(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
    return stat.S_ISLNK(metadata.st_mode) or bool(
        _REPARSE_POINT and getattr(metadata, "st_file_attributes", 0) & _REPARSE_POINT
    )


def _require_regular(metadata: os.stat_result) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")


def _bounded_bytes(
    value: bytes | bytearray | memoryview,
    maximum: int,
) -> bytes:
    """Freeze a bytes-like value only after its size passes the bound."""

    if not isinstance(value, _BYTES_LIKE_TYPES):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    try:
        # Buffer metadata is supplied by the interpreter, unlike overridable
        # ``__len__``/``__bytes__`` methods on bytes-like subclasses.
        view = memoryview(value)
        try:
            size = view.nbytes
            if size > maximum:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            frozen = view.tobytes()
        finally:
            view.release()
    except PromptArchiveStorageError:
        raise
    except (MemoryError, BufferError, ValueError, TypeError):
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
    if len(frozen) != size or len(frozen) > maximum:
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
    return frozen


def _path_from_open_descriptor(descriptor: int) -> Path | None:
    """Return the kernel-resolved path for an open descriptor when supported."""

    if os.name == "nt":
        try:
            import msvcrt
            from ctypes import wintypes

            handle = msvcrt.get_osfhandle(descriptor)
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            get_final_path = kernel32.GetFinalPathNameByHandleW
            get_final_path.argtypes = (
                wintypes.HANDLE,
                wintypes.LPWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
            )
            get_final_path.restype = wintypes.DWORD
            required = get_final_path(handle, None, 0, 0)
            if not required:
                return None
            buffer = ctypes.create_unicode_buffer(required + 1)
            written = get_final_path(handle, buffer, len(buffer), 0)
            if not written or written >= len(buffer):
                return None
            raw = buffer.value
            if raw.startswith("\\\\?\\UNC\\"):
                raw = "\\\\" + raw[8:]
            elif raw.startswith("\\\\?\\"):
                raw = raw[4:]
            return Path(raw)
        except (ImportError, OSError, ValueError, AttributeError):
            return None
    proc_entry = Path(f"/proc/self/fd/{descriptor}")
    try:
        if proc_entry.exists():
            return proc_entry.resolve(strict=True)
    except OSError:
        return None
    return None


def resolved_path_from_open_descriptor(descriptor: int) -> Path | None:
    """Expose the kernel-resolved path check for trusted external readers."""

    if type(descriptor) is not int or descriptor < 0:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    return _path_from_open_descriptor(descriptor)


def _windows_lock_descriptor(descriptor: int, *, unlock: bool) -> None:
    """Block on (or release) byte zero without CRT's ten-second retry cap."""

    if os.name != "nt":
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    import msvcrt
    from ctypes import wintypes

    # OVERLAPPED's anonymous union is represented by its offset fields.  This
    # layout is identical on 32- and 64-bit Windows because ULONG_PTR/HANDLE
    # follow pointer width while the file offsets remain DWORD pairs.
    class Overlapped(ctypes.Structure):
        _fields_ = (
            ("Internal", ctypes.c_size_t),
            ("InternalHigh", ctypes.c_size_t),
            ("Offset", wintypes.DWORD),
            ("OffsetHigh", wintypes.DWORD),
            ("hEvent", wintypes.HANDLE),
        )

    handle = msvcrt.get_osfhandle(descriptor)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    overlapped = Overlapped()
    if unlock:
        operation = kernel32.UnlockFileEx
        operation.argtypes = (
            wintypes.HANDLE,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.POINTER(Overlapped),
        )
        operation.restype = wintypes.BOOL
        accepted = operation(handle, 0, 1, 0, ctypes.byref(overlapped))
    else:
        operation = kernel32.LockFileEx
        operation.argtypes = (
            wintypes.HANDLE,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.POINTER(Overlapped),
        )
        operation.restype = wintypes.BOOL
        # LOCKFILE_EXCLUSIVE_LOCK without FAIL_IMMEDIATELY is a true blocking
        # lock.  A process crash closes the handle and releases it in-kernel.
        accepted = operation(
            handle,
            0x00000002,
            0,
            1,
            0,
            ctypes.byref(overlapped),
        )
    if not accepted:
        code = ctypes.get_last_error()
        raise OSError(code, "Windows byte-range lock failed")


class _PinnedDirectory:
    """An opened directory used as the authority for relative publication."""

    def __init__(self, path: Path, descriptor: int) -> None:
        self.path = path
        self.descriptor = descriptor

    def close(self) -> None:
        if self.descriptor == -1:
            return
        descriptor, self.descriptor = self.descriptor, -1
        if os.name == "nt":
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            kernel32.CloseHandle.restype = wintypes.BOOL
            kernel32.CloseHandle(descriptor)
        else:
            os.close(descriptor)

    def __enter__(self) -> "_PinnedDirectory":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _windows_path_from_handle(handle: int) -> Path | None:
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_final_path = kernel32.GetFinalPathNameByHandleW
    get_final_path.argtypes = (
        wintypes.HANDLE,
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    get_final_path.restype = wintypes.DWORD
    required = get_final_path(handle, None, 0, 0)
    if not required:
        return None
    buffer = ctypes.create_unicode_buffer(required + 1)
    written = get_final_path(handle, buffer, len(buffer), 0)
    if not written or written >= len(buffer):
        return None
    raw = buffer.value
    if raw.startswith("\\\\?\\UNC\\"):
        raw = "\\\\" + raw[8:]
    elif raw.startswith("\\\\?\\"):
        raw = raw[4:]
    return Path(raw)


def _windows_open_pinned_directory(path: Path) -> int:
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    handle = create_file(
        str(path),
        0x00100000 | 0x00000080 | 0x00000020 | 0x00000001,
        0x00000001 | 0x00000002,
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    if handle == ctypes.c_void_p(-1).value:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    return int(handle)


def _windows_open_or_create_relative_directory(
    directory_handle: int,
    name: str,
    *,
    create: bool = True,
    writable: bool = True,
) -> tuple[int, bool]:
    from ctypes import wintypes

    class UnicodeString(ctypes.Structure):
        _fields_ = (
            ("Length", wintypes.USHORT),
            ("MaximumLength", wintypes.USHORT),
            ("Buffer", wintypes.LPWSTR),
        )

    class ObjectAttributes(ctypes.Structure):
        _fields_ = (
            ("Length", wintypes.ULONG),
            ("RootDirectory", wintypes.HANDLE),
            ("ObjectName", ctypes.POINTER(UnicodeString)),
            ("Attributes", wintypes.ULONG),
            ("SecurityDescriptor", ctypes.c_void_p),
            ("SecurityQualityOfService", ctypes.c_void_p),
        )

    class IoStatusValue(ctypes.Union):
        _fields_ = (("Status", wintypes.LONG), ("Pointer", ctypes.c_void_p))

    class IoStatusBlock(ctypes.Structure):
        _anonymous_ = ("value",)
        _fields_ = (("value", IoStatusValue), ("Information", ctypes.c_size_t))

    encoded_name = name.encode("utf-16-le", errors="strict")
    name_buffer = ctypes.create_unicode_buffer(name)
    unicode_name = UnicodeString(
        len(encoded_name),
        len(encoded_name),
        ctypes.cast(name_buffer, wintypes.LPWSTR),
    )
    attributes = ObjectAttributes(
        ctypes.sizeof(ObjectAttributes),
        directory_handle,
        ctypes.pointer(unicode_name),
        0x00000040,
        None,
        None,
    )
    status_block = IoStatusBlock()
    child_handle = wintypes.HANDLE()
    ntdll = ctypes.WinDLL("ntdll")
    create_file = ntdll.NtCreateFile
    create_file.argtypes = (
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.DWORD,
        ctypes.POINTER(ObjectAttributes),
        ctypes.POINTER(IoStatusBlock),
        ctypes.c_void_p,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
    )
    create_file.restype = wintypes.LONG
    desired_access = 0x00000001 | 0x00000020 | 0x00000080 | 0x00100000
    if writable:
        desired_access |= 0x00000002 | 0x00000004
    status = create_file(
        ctypes.byref(child_handle),
        desired_access,
        ctypes.byref(attributes),
        ctypes.byref(status_block),
        None,
        0x00000010,
        0x00000001 | 0x00000002,
        3 if create else 1,
        0x00000001 | 0x00000020 | 0x00200000,
        None,
        0,
    )
    if status != 0:
        unsigned_status = ctypes.c_ulong(status).value
        if unsigned_status in {0xC0000034, 0xC000003A}:
            raise FileNotFoundError(name)
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    return int(child_handle.value), status_block.Information == 2


def _windows_create_relative_file(directory_handle: int, name: str) -> int:
    """Create one exclusive regular file relative to a pinned NT directory."""

    import msvcrt
    from ctypes import wintypes

    class UnicodeString(ctypes.Structure):
        _fields_ = (
            ("Length", wintypes.USHORT),
            ("MaximumLength", wintypes.USHORT),
            ("Buffer", wintypes.LPWSTR),
        )

    class ObjectAttributes(ctypes.Structure):
        _fields_ = (
            ("Length", wintypes.ULONG),
            ("RootDirectory", wintypes.HANDLE),
            ("ObjectName", ctypes.POINTER(UnicodeString)),
            ("Attributes", wintypes.ULONG),
            ("SecurityDescriptor", ctypes.c_void_p),
            ("SecurityQualityOfService", ctypes.c_void_p),
        )

    class IoStatusValue(ctypes.Union):
        _fields_ = (("Status", wintypes.LONG), ("Pointer", ctypes.c_void_p))

    class IoStatusBlock(ctypes.Structure):
        _anonymous_ = ("value",)
        _fields_ = (("value", IoStatusValue), ("Information", ctypes.c_size_t))

    encoded_name = name.encode("utf-16-le", errors="strict")
    name_buffer = ctypes.create_unicode_buffer(name)
    unicode_name = UnicodeString(
        len(encoded_name),
        len(encoded_name),
        ctypes.cast(name_buffer, wintypes.LPWSTR),
    )
    attributes = ObjectAttributes(
        ctypes.sizeof(ObjectAttributes),
        directory_handle,
        ctypes.pointer(unicode_name),
        0x00000040,
        None,
        None,
    )
    status_block = IoStatusBlock()
    file_handle = wintypes.HANDLE()
    ntdll = ctypes.WinDLL("ntdll")
    create_file = ntdll.NtCreateFile
    create_file.argtypes = (
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.DWORD,
        ctypes.POINTER(ObjectAttributes),
        ctypes.POINTER(IoStatusBlock),
        ctypes.c_void_p,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
    )
    create_file.restype = wintypes.LONG
    status = create_file(
        ctypes.byref(file_handle),
        0x80000000 | 0x40000000 | 0x00010000 | 0x00100000,
        ctypes.byref(attributes),
        ctypes.byref(status_block),
        None,
        0x00000080,
        0x00000001 | 0x00000002 | 0x00000004,
        2,
        0x00000040 | 0x00000020 | 0x00200000,
        None,
        0,
    )
    if status != 0:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    try:
        return msvcrt.open_osfhandle(
            int(file_handle.value),
            os.O_RDWR | getattr(os, "O_BINARY", 0),
        )
    except (OSError, ValueError):
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle(file_handle)
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None


def _windows_open_relative_control_file(
    directory_handle: int,
    name: str,
    *,
    create: bool,
    writable: bool,
) -> int:
    """Open/open-if one control leaf relative to a pinned NT directory."""

    import msvcrt
    from ctypes import wintypes

    class UnicodeString(ctypes.Structure):
        _fields_ = (
            ("Length", wintypes.USHORT),
            ("MaximumLength", wintypes.USHORT),
            ("Buffer", wintypes.LPWSTR),
        )

    class ObjectAttributes(ctypes.Structure):
        _fields_ = (
            ("Length", wintypes.ULONG),
            ("RootDirectory", wintypes.HANDLE),
            ("ObjectName", ctypes.POINTER(UnicodeString)),
            ("Attributes", wintypes.ULONG),
            ("SecurityDescriptor", ctypes.c_void_p),
            ("SecurityQualityOfService", ctypes.c_void_p),
        )

    class IoStatusValue(ctypes.Union):
        _fields_ = (("Status", wintypes.LONG), ("Pointer", ctypes.c_void_p))

    class IoStatusBlock(ctypes.Structure):
        _anonymous_ = ("value",)
        _fields_ = (("value", IoStatusValue), ("Information", ctypes.c_size_t))

    encoded_name = name.encode("utf-16-le", errors="strict")
    name_buffer = ctypes.create_unicode_buffer(name)
    unicode_name = UnicodeString(
        len(encoded_name),
        len(encoded_name),
        ctypes.cast(name_buffer, wintypes.LPWSTR),
    )
    attributes = ObjectAttributes(
        ctypes.sizeof(ObjectAttributes),
        directory_handle,
        ctypes.pointer(unicode_name),
        0x00000040,
        None,
        None,
    )
    status_block = IoStatusBlock()
    file_handle = wintypes.HANDLE()
    ntdll = ctypes.WinDLL("ntdll")
    create_file = ntdll.NtCreateFile
    create_file.argtypes = (
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.DWORD,
        ctypes.POINTER(ObjectAttributes),
        ctypes.POINTER(IoStatusBlock),
        ctypes.c_void_p,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
    )
    create_file.restype = wintypes.LONG
    desired_access = 0x80000000 | 0x00000080 | 0x00100000
    if writable:
        desired_access |= 0x40000000
    status = create_file(
        ctypes.byref(file_handle),
        desired_access,
        ctypes.byref(attributes),
        ctypes.byref(status_block),
        None,
        0x00000080,
        0x00000001 | 0x00000002,  # no delete sharing for lock/current leaves
        3 if create else 1,  # FILE_OPEN_IF / FILE_OPEN
        0x00000040 | 0x00000020 | 0x00200000,
        None,
        0,
    )
    unsigned_status = ctypes.c_ulong(status).value
    if status != 0:
        if unsigned_status in {0xC0000034, 0xC000003A}:
            raise FileNotFoundError(name)
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    try:
        return msvcrt.open_osfhandle(
            int(file_handle.value),
            (os.O_RDWR if writable else os.O_RDONLY)
            | getattr(os, "O_BINARY", 0),
        )
    except (OSError, ValueError):
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle(file_handle)
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None


def _windows_rename_relative_no_replace(
    descriptor: int,
    directory_handle: int,
    target_name: str,
) -> bool:
    import msvcrt
    from ctypes import wintypes

    encoded_name = target_name.encode("utf-16-le", errors="strict")
    information = ctypes.create_string_buffer(20 + len(encoded_name))
    ctypes.c_ubyte.from_buffer(information, 0).value = 0
    ctypes.c_void_p.from_buffer(information, 8).value = directory_handle
    ctypes.c_ulong.from_buffer(information, 16).value = len(encoded_name)
    ctypes.memmove(ctypes.addressof(information) + 20, encoded_name, len(encoded_name))

    class IoStatusValue(ctypes.Union):
        _fields_ = (("Status", wintypes.LONG), ("Pointer", ctypes.c_void_p))

    class IoStatusBlock(ctypes.Structure):
        _anonymous_ = ("value",)
        _fields_ = (("value", IoStatusValue), ("Information", ctypes.c_size_t))

    status_block = IoStatusBlock()
    ntdll = ctypes.WinDLL("ntdll")
    set_information = ntdll.NtSetInformationFile
    set_information.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(IoStatusBlock),
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.c_int,
    )
    set_information.restype = wintypes.LONG
    status = set_information(
        msvcrt.get_osfhandle(descriptor),
        ctypes.byref(status_block),
        information,
        len(information),
        10,
    )
    unsigned_status = ctypes.c_ulong(status).value
    if status == 0:
        return True
    if unsigned_status in {0xC0000035, 0xC00000BA}:
        return False
    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")


def _windows_rename_relative_replace(
    descriptor: int,
    directory_handle: int,
    target_name: str,
) -> None:
    """Replace one exact leaf relative to a pinned Windows directory."""

    import msvcrt
    from ctypes import wintypes

    encoded_name = target_name.encode("utf-16-le", errors="strict")
    information = ctypes.create_string_buffer(20 + len(encoded_name))
    ctypes.c_ubyte.from_buffer(information, 0).value = 1
    ctypes.c_void_p.from_buffer(information, 8).value = directory_handle
    ctypes.c_ulong.from_buffer(information, 16).value = len(encoded_name)
    ctypes.memmove(ctypes.addressof(information) + 20, encoded_name, len(encoded_name))

    class IoStatusValue(ctypes.Union):
        _fields_ = (("Status", wintypes.LONG), ("Pointer", ctypes.c_void_p))

    class IoStatusBlock(ctypes.Structure):
        _anonymous_ = ("value",)
        _fields_ = (("value", IoStatusValue), ("Information", ctypes.c_size_t))

    status_block = IoStatusBlock()
    ntdll = ctypes.WinDLL("ntdll")
    set_information = ntdll.NtSetInformationFile
    set_information.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(IoStatusBlock),
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.c_int,
    )
    set_information.restype = wintypes.LONG
    status = set_information(
        msvcrt.get_osfhandle(descriptor),
        ctypes.byref(status_block),
        information,
        len(information),
        10,
    )
    if status != 0:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")


def _windows_mark_file_for_delete(descriptor: int) -> None:
    import msvcrt
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    set_information = kernel32.SetFileInformationByHandle
    set_information.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    set_information.restype = wintypes.BOOL
    delete = wintypes.BOOL(True)
    if not set_information(
        msvcrt.get_osfhandle(descriptor),
        4,
        ctypes.byref(delete),
        ctypes.sizeof(delete),
    ):
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")


def _atomic_rename_pinned_no_replace(
    descriptor: int,
    directory: _PinnedDirectory,
    source_name: str,
    target_name: str,
) -> bool:
    if os.name == "nt":
        return _windows_rename_relative_no_replace(
            descriptor,
            directory.descriptor,
            target_name,
        )
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        directory.descriptor,
        os.fsencode(source_name),
        directory.descriptor,
        os.fsencode(target_name),
        1,
    )
    if result == 0:
        return True
    if ctypes.get_errno() == errno.EEXIST:
        return False
    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")


def _canonical_json_bytes(value: Any, *, maximum: int) -> bytes:
    """Encode nginx-qa-canonical-json-v1 under a hard output-byte budget."""

    if type(maximum) is not int or maximum <= 0:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    output = bytearray()
    observed = 0

    def emit(text: str) -> None:
        nonlocal observed
        remaining = maximum - observed
        # Every Unicode scalar requires at least one UTF-8 byte.  Reject a
        # token that cannot fit before allocating its encoded representation.
        if len(text) > remaining:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        try:
            encoded = text.encode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if len(encoded) > remaining:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        output.extend(encoded)
        observed += len(encoded)

    def encode_string(text: str) -> None:
        emit('"')
        safe_run: list[str] = []

        def flush_safe_run() -> None:
            if safe_run:
                emit("".join(safe_run))
                safe_run.clear()

        for character in text:
            codepoint = ord(character)
            if 0xD800 <= codepoint <= 0xDFFF:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if character == '"':
                flush_safe_run()
                emit('\\"')
            elif character == "\\":
                flush_safe_run()
                emit("\\\\")
            elif codepoint <= 0x1F:
                flush_safe_run()
                emit(f"\\u{codepoint:04x}")
            else:
                if len(safe_run) >= maximum - observed:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                safe_run.append(character)
                if len(safe_run) == 4096:
                    flush_safe_run()
        flush_safe_run()
        emit('"')

    def encode(item: Any, depth: int) -> None:
        if depth > 128:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if item is None:
            emit("null")
            return
        if item is True:
            emit("true")
            return
        if item is False:
            emit("false")
            return
        if type(item) is int:
            remaining = maximum - observed
            bit_length = int.bit_length(item)
            minimum_digits = (
                1
                if bit_length == 0
                else ((bit_length - 1) * 301_029_995_663_981) // 10**15 + 1
            )
            if minimum_digits + (1 if item < 0 else 0) > remaining:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            emit(str(item))
            return
        if type(item) is str:
            encode_string(item)
            return
        if type(item) is list:
            # Even one-byte children need brackets and separators.  Bound the
            # number of iterations before walking a caller-owned container.
            minimum_length = 2 if not item else 2 * len(item) + 1
            if minimum_length > maximum - observed:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            emit("[")
            for index, child in enumerate(item):
                if index:
                    emit(",")
                encode(child, depth + 1)
            emit("]")
            return
        if type(item) is dict:
            minimum_length = 2
            for key in item:
                if type(key) is not str:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                # Two quotes, a colon, a one-byte minimum value, and (after
                # the first member) a comma accompany every key.
                minimum_length += len(key) + 4
                if minimum_length > maximum - observed:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            if item:
                minimum_length += len(item) - 1
            if minimum_length > maximum - observed:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            # Python string ordering is Unicode scalar ordering for the
            # surrogate-free strings accepted by encode_string().
            ordered = sorted(item)
            emit("{")
            for index, key in enumerate(ordered):
                if index:
                    emit(",")
                encode_string(key)
                emit(":")
                encode(item[key], depth + 1)
            emit("}")
            return
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    try:
        encode(value, 0)
        return bytes(output)
    except PromptArchiveStorageError:
        raise
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None


def canonical_json_bytes_v1(value: Any, *, maximum: int) -> bytes:
    """Public frozen encoder for PAS1 control documents.

    Higher archive layers must use this seam rather than duplicate the
    canonical grammar, otherwise equal control documents could fork their CAS
    identities.
    """

    return _canonical_json_bytes(value, maximum=maximum)


def _validate_control_nesting(raw: bytes) -> None:
    depth = 0
    in_string = False
    escaped = False
    for value in raw:
        if in_string:
            if escaped:
                escaped = False
            elif value == 0x5C:  # backslash
                escaped = True
            elif value == 0x22:  # quote
                in_string = False
            continue
        if value == 0x22:
            in_string = True
        elif value in (0x5B, 0x7B):  # [ {
            depth += 1
            if depth > _MAX_CONTROL_JSON_DEPTH:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        elif value in (0x5D, 0x7D):  # ] }
            depth -= 1
            if depth < 0:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")


def parse_canonical_json_v1(
    raw: bytes | bytearray | memoryview,
    *,
    maximum: int,
) -> dict[str, Any]:
    """Decode one bounded canonical PAS1 control object.

    Duplicate decoded member names, non-integer JSON numbers, excessive
    nesting, non-object roots, and non-canonical encodings are rejected.  This
    is the public counterpart to :func:`canonical_json_bytes_v1`; runtime
    readers use it rather than growing a second, subtly different grammar.
    """

    payload = _bounded_bytes(raw, maximum)

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in values:
            if type(key) is not str or key in output:
                raise _DuplicateControlMember
            output[key] = value
        return output

    def reject_number(_value: str) -> Any:
        raise _DuplicateControlMember

    _validate_control_nesting(payload)
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=pairs,
            parse_float=reject_number,
            parse_constant=reject_number,
        )
    except (UnicodeError, json.JSONDecodeError, _DuplicateControlMember, ValueError):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    except (RecursionError, MemoryError):
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") from None
    if type(value) is not dict:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if canonical_json_bytes_v1(value, maximum=maximum) != payload:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _gzip_bytes(
    data: bytes,
    *,
    cancellation: PromptArchiveCancellationToken | None = None,
) -> bytes:
    if zlib.ZLIB_VERSION != "1.2.13" or zlib.ZLIB_RUNTIME_VERSION != "1.2.13":
        raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
    compressor = zlib.compressobj(
        9,
        zlib.DEFLATED,
        -15,
        8,
        zlib.Z_DEFAULT_STRATEGY,
    )
    parts: list[bytes] = []
    for offset in range(0, len(data), 64 * 1024):
        if cancellation is not None:
            cancellation.check()
        parts.append(compressor.compress(data[offset : offset + 64 * 1024]))
    if cancellation is not None:
        cancellation.check()
    parts.append(compressor.flush(zlib.Z_FINISH))
    deflated = b"".join(parts)
    trailer = struct.pack("<II", zlib.crc32(data) & 0xFFFFFFFF, len(data) & 0xFFFFFFFF)
    return _GZIP_HEADER + deflated + trailer


class PromptArchiveStorage:
    """Project-scoped Prompt Archive V1 immutable storage."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        project_id: str,
        *,
        protected_roots: Sequence[str | os.PathLike[str]],
        limits: PromptArchiveLimits | Mapping[str, int] | None = None,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        self.limits = self._coerce_limits(limits)
        self.project_id = project_id
        self.project_storage_key = _project_storage_key(project_id)
        raw_root = Path(root)
        if not raw_root.is_absolute():
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        # Validate the caller's spelling before Win32 normalization can erase
        # trailing dots/spaces or reinterpret reserved device names.
        self._validate_configured_root(raw_root)
        raw_root = Path(os.path.normpath(os.path.abspath(raw_root)))
        self._validate_configured_root(raw_root)
        self._preflight_protected_roots(raw_root, protected_roots)
        # Establish the root authority without an absolute mkdir/resolve gap.
        # Every component is opened or created relative to a pinned parent,
        # every ancestor remains pinned until final revalidation, and the
        # kernel-resolved handle path must equal the configured lexical path.
        self._root_identity, root_directories_durable = self._bootstrap_root(
            raw_root
        )
        self.root = raw_root
        with self._open_pinned_directory(self.root):
            self._validate_protected_roots(protected_roots)
        self._fault_hook = fault_hook
        self._cas_write_accounting = threading.local()
        self.directory_sync_supported = root_directories_durable

    @staticmethod
    def _coerce_limits(
        value: PromptArchiveLimits | Mapping[str, int] | None,
    ) -> PromptArchiveLimits:
        if value is None:
            return PromptArchiveLimits()
        if isinstance(value, PromptArchiveLimits):
            return value
        if not isinstance(value, Mapping):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        names = {field.name for field in dataclasses.fields(PromptArchiveLimits)}
        if not set(value).issubset(names):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        try:
            return PromptArchiveLimits(**dict(value))
        except (TypeError, ValueError) as exc:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None

    def _fault(self, stage: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage)

    @staticmethod
    def _validate_configured_root(root: Path) -> None:
        """Reject ambiguous Windows root spellings before any mutation."""

        if not root.is_absolute():
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if os.name != "nt":
            return
        anchor_parts = len(Path(root.anchor).parts)
        reserved = re.compile(
            r"^(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?$",
            re.IGNORECASE | re.ASCII,
        )
        for component in root.parts[anchor_parts:]:
            if (
                component in {"", ".", ".."}
                or ":" in component
                or component.endswith((" ", "."))
                or reserved.fullmatch(component) is not None
            ):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

    @staticmethod
    def _validate_bootstrap_resolved_path(resolved: Path, intended: Path) -> None:
        """Bind a bootstrap handle to the exact configured lexical path."""

        try:
            resolved_absolute = Path(os.path.abspath(resolved))
            intended_absolute = Path(os.path.abspath(intended))
            if os.path.normcase(str(resolved_absolute)) != os.path.normcase(
                str(intended_absolute)
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if os.name == "nt":
                intended_tail = intended_absolute.relative_to(
                    Path(intended_absolute.anchor)
                ).parts
                if intended_tail:
                    observed_tail = resolved_absolute.parts[-len(intended_tail) :]
                    if observed_tail != intended_tail:
                        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        except PromptArchiveStorageError:
            raise
        except (OSError, ValueError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None

    @classmethod
    def _validate_bootstrap_directory(
        cls,
        pinned: _PinnedDirectory,
        intended: Path,
    ) -> tuple[int, int]:
        resolved = (
            _windows_path_from_handle(pinned.descriptor)
            if os.name == "nt"
            else _path_from_open_descriptor(pinned.descriptor)
        )
        if resolved is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        cls._validate_bootstrap_resolved_path(resolved, intended)
        try:
            metadata = intended.lstat()
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if not stat.S_ISDIR(metadata.st_mode) or _path_entry_is_redirected(intended):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        identity = metadata.st_dev, metadata.st_ino
        if os.name != "nt":
            try:
                opened = os.fstat(pinned.descriptor)
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            if (
                not stat.S_ISDIR(opened.st_mode)
                or (opened.st_dev, opened.st_ino) != identity
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return identity

    @classmethod
    def _bootstrap_root(cls, root: Path) -> tuple[tuple[int, int], bool]:
        """Create a root by relative no-follow traversal from a pinned anchor."""

        anchor = Path(root.anchor)
        try:
            components = root.relative_to(anchor).parts
        except ValueError:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None

        pins: list[_PinnedDirectory] = []
        durable = os.name != "nt"
        try:
            try:
                if os.name == "nt":
                    descriptor = _windows_open_pinned_directory(anchor)
                else:
                    nofollow = getattr(os, "O_NOFOLLOW", 0)
                    if not nofollow:
                        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                    descriptor = os.open(
                        anchor,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow,
                    )
            except PromptArchiveStorageError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None

            current_path = anchor
            current = _PinnedDirectory(anchor, descriptor)
            pins.append(current)
            cls._validate_bootstrap_directory(current, current_path)

            for component in components:
                if component in {"", ".", ".."} or any(
                    separator in component for separator in ("/", "\0")
                ):
                    raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
                intended = current_path / component
                created = False
                try:
                    if os.name == "nt":
                        try:
                            child_descriptor, _ = (
                                _windows_open_or_create_relative_directory(
                                    current.descriptor,
                                    component,
                                    create=False,
                                    writable=False,
                                )
                            )
                        except FileNotFoundError:
                            child_descriptor, created = (
                                _windows_open_or_create_relative_directory(
                                    current.descriptor,
                                    component,
                                    create=True,
                                    writable=True,
                                )
                            )
                    else:
                        nofollow = getattr(os, "O_NOFOLLOW", 0)
                        if not nofollow:
                            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                        try:
                            os.mkdir(component, mode=0o700, dir_fd=current.descriptor)
                            created = True
                        except FileExistsError:
                            pass
                        child_descriptor = os.open(
                            component,
                            os.O_RDONLY
                            | getattr(os, "O_DIRECTORY", 0)
                            | nofollow,
                            dir_fd=current.descriptor,
                        )
                except PromptArchiveStorageError:
                    raise
                except OSError:
                    raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None

                child = _PinnedDirectory(intended, child_descriptor)
                pins.append(child)
                cls._validate_bootstrap_directory(child, intended)
                cls._validate_bootstrap_directory(current, current_path)
                if created:
                    durable = cls._sync_pinned_directory(current) and durable
                current = child
                current_path = intended

            identities = [
                cls._validate_bootstrap_directory(pin, pin.path) for pin in pins
            ]
            return identities[-1], durable
        finally:
            for pin in reversed(pins):
                pin.close()

    def _validate_protected_roots(
        self,
        roots: Sequence[str | os.PathLike[str]],
    ) -> None:
        if isinstance(roots, (str, bytes)) or not isinstance(roots, Sequence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        root_key = os.path.normcase(str(self.root))
        for raw_protected in roots:
            protected = Path(raw_protected)
            if not protected.is_absolute():
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            try:
                resolved = protected.resolve(strict=True)
                metadata = resolved.lstat()
            except OSError as exc:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
            if _path_entry_is_redirected(resolved):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            protected_key = os.path.normcase(str(resolved))
            try:
                common = os.path.commonpath((root_key, protected_key))
            except ValueError:
                continue
            if common in {root_key, protected_key} or (
                metadata.st_dev,
                metadata.st_ino,
            ) == self._root_identity:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

    @staticmethod
    def _preflight_protected_roots(
        candidate: Path,
        roots: Sequence[str | os.PathLike[str]],
    ) -> None:
        if isinstance(roots, (str, bytes)) or not isinstance(roots, Sequence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        try:
            candidate_key = os.path.normcase(str(candidate.resolve(strict=False)))
        except OSError as exc:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
        for raw_protected in roots:
            protected = Path(raw_protected)
            if not protected.is_absolute():
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            try:
                protected_key = os.path.normcase(
                    str(protected.resolve(strict=True))
                )
                common = os.path.commonpath((candidate_key, protected_key))
            except ValueError:
                continue
            except OSError as exc:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
            if common in {candidate_key, protected_key}:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

    def _assert_root_identity(self) -> None:
        try:
            metadata = self.root.lstat()
        except OSError as exc:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if _path_entry_is_redirected(self.root) or not stat.S_ISDIR(metadata.st_mode):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (metadata.st_dev, metadata.st_ino) != self._root_identity:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _assert_within_root(self, path: Path) -> None:
        try:
            common = os.path.commonpath(
                (os.path.normcase(str(self.root)), os.path.normcase(str(path.absolute())))
            )
        except (OSError, ValueError) as exc:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if common != os.path.normcase(str(self.root)):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _ensure_directory(self, directory: Path) -> None:
        self._assert_root_identity()
        self._assert_within_root(directory)
        try:
            relative = directory.relative_to(self.root)
        except ValueError as exc:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        current_path = self.root
        current_handle = self._open_pinned_directory(self.root)
        try:
            for component in relative.parts:
                if not _COMPONENT_RE.fullmatch(component) or ":" in component:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                self._reject_case_collision(current_path, component)
                child_path = current_path / component
                child_handle, created = self._open_or_create_relative_directory(
                    current_handle,
                    component,
                    child_path,
                )
                if created:
                    self.directory_sync_supported = (
                        self._sync_pinned_directory(current_handle)
                        and self.directory_sync_supported
                    )
                current_handle.close()
                current_handle = child_handle
                current_path = child_path
        finally:
            current_handle.close()
        self._assert_root_identity()

    @staticmethod
    def _reject_case_collision(parent: Path, expected: str) -> None:
        try:
            collisions = [
                entry.name
                for entry in os.scandir(parent)
                if entry.name.casefold() == expected.casefold() and entry.name != expected
            ]
        except OSError as exc:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if collisions:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _assert_existing_chain_safe(self, path: Path) -> None:
        self._assert_root_identity()
        self._assert_within_root(path)
        try:
            relative = path.relative_to(self.root)
        except ValueError as exc:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        current = self.root
        for component in relative.parts:
            current = current / component
            if not current.exists() and not current.is_symlink():
                return
            self._reject_case_collision(current.parent, component)
            if _path_entry_is_redirected(current):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _directory_identity(self, directory: Path) -> tuple[int, int]:
        self._assert_existing_chain_safe(directory)
        try:
            metadata = directory.lstat()
        except OSError as exc:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if not stat.S_ISDIR(metadata.st_mode) or _path_entry_is_redirected(directory):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return metadata.st_dev, metadata.st_ino

    def _assert_directory_identity(
        self,
        directory: Path,
        expected: tuple[int, int],
    ) -> None:
        if self._directory_identity(directory) != expected:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _open_pinned_directory(self, directory: Path) -> _PinnedDirectory:
        """Open and validate the exact directory used for relative mutation."""

        expected = self._directory_identity(directory)
        try:
            if os.name == "nt":
                descriptor = _windows_open_pinned_directory(directory)
                resolved = _windows_path_from_handle(descriptor)
            else:
                descriptor = os.open(
                    directory,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                )
                resolved = _path_from_open_descriptor(descriptor)
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        pinned = _PinnedDirectory(directory, descriptor)
        try:
            self._validate_pinned_directory(pinned, directory, resolved=resolved)
            self._assert_directory_identity(directory, expected)
            return pinned
        except BaseException:
            pinned.close()
            raise

    def _validate_pinned_directory(
        self,
        pinned: _PinnedDirectory,
        intended_path: Path,
        *,
        resolved: Path | None = None,
    ) -> None:
        if resolved is None:
            resolved = (
                _windows_path_from_handle(pinned.descriptor)
                if os.name == "nt"
                else _path_from_open_descriptor(pinned.descriptor)
            )
        if resolved is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        try:
            resolved = resolved.resolve(strict=True)
            intended = intended_path.resolve(strict=True)
            metadata = intended_path.lstat()
            common = os.path.commonpath(
                (
                    os.path.normcase(str(self.root)),
                    os.path.normcase(str(resolved)),
                )
            )
        except (OSError, ValueError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if (
            common != os.path.normcase(str(self.root))
            or os.path.normcase(str(resolved)) != os.path.normcase(str(intended))
            or not stat.S_ISDIR(metadata.st_mode)
            or _path_entry_is_redirected(intended_path)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _open_or_create_relative_directory(
        self,
        parent: _PinnedDirectory,
        component: str,
        intended_path: Path,
    ) -> tuple[_PinnedDirectory, bool]:
        try:
            if os.name == "nt":
                descriptor, created = _windows_open_or_create_relative_directory(
                    parent.descriptor,
                    component,
                )
            else:
                created = False
                try:
                    os.mkdir(component, mode=0o700, dir_fd=parent.descriptor)
                    created = True
                except FileExistsError:
                    pass
                descriptor = os.open(
                    component,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent.descriptor,
                )
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        child = _PinnedDirectory(intended_path, descriptor)
        try:
            self._validate_pinned_directory(child, intended_path)
            return child, created
        except BaseException:
            child.close()
            raise

    @staticmethod
    def _create_relative_temp(directory: _PinnedDirectory, name: str) -> int:
        try:
            if os.name == "nt":
                descriptor = _windows_create_relative_file(directory.descriptor, name)
            else:
                descriptor = os.open(
                    name,
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_BINARY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=directory.descriptor,
                )
            metadata = os.fstat(descriptor)
            _require_regular(metadata)
            if metadata.st_nlink != 1:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            return descriptor
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None

    @staticmethod
    def _delete_relative_temp(
        directory: _PinnedDirectory,
        name: str,
        descriptor: int,
    ) -> None:
        try:
            if os.name == "nt":
                _windows_mark_file_for_delete(descriptor)
            else:
                os.unlink(name, dir_fd=directory.descriptor)
        except (FileNotFoundError, PromptArchiveStorageError, OSError):
            return

    @staticmethod
    def _read_open_descriptor(descriptor: int, *, maximum: int) -> bytes:
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            chunks: list[bytes] = []
            observed = 0
            while True:
                chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - observed))
                if not chunk:
                    break
                observed += len(chunk)
                if observed > maximum:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                chunks.append(chunk)
            return b"".join(chunks)
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None

    def _assert_open_handle(self, descriptor: int, expected_path: Path) -> None:
        """Verify the kernel-resolved open handle is the intended in-root entry."""

        resolved = _path_from_open_descriptor(descriptor)
        if resolved is None:
            if os.name == "nt":
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
            return
        try:
            resolved = resolved.resolve(strict=True)
            expected = expected_path.resolve(strict=True)
            common = os.path.commonpath(
                (
                    os.path.normcase(str(self.root)),
                    os.path.normcase(str(resolved)),
                )
            )
        except (OSError, ValueError) as exc:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if common != os.path.normcase(str(self.root)):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if os.path.normcase(str(resolved)) != os.path.normcase(str(expected)):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._assert_root_identity()

    @property
    def _project_root(self) -> Path:
        return self.root / "projects" / self.project_storage_key

    @property
    def _blob_root(self) -> Path:
        return self._project_root / "blobs" / "v1" / "sha256"

    @staticmethod
    def _validate_logical_type(logical_kind: Any, media_type: Any) -> None:
        if type(logical_kind) is not str or type(media_type) is not str:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if logical_kind not in _LOGICAL_KINDS:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if _MEDIA_TYPE_RE.fullmatch(media_type) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if logical_kind in _RECORD_KINDS and media_type != _RECORD_MEDIA_TYPE:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        expected = _FIXED_MEDIA_TYPES.get(logical_kind)
        if expected is not None and media_type != expected:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if logical_kind == "payload_chunk" and media_type not in _PAYLOAD_MEDIA_TYPES:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _validate_ref(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        ref = dict(value)
        if not _REF_REQUIRED_KEYS.issubset(ref) or not set(ref).issubset(_REF_KEYS):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        schema_version = ref.get("schema_version")
        if type(schema_version) is not int:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if schema_version != 1:
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        kind = ref.get("kind")
        if type(kind) is not str:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if kind != "prompt_archive_blob_ref":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        hash_algorithm = ref.get("hash_algorithm")
        if type(hash_algorithm) is not str or hash_algorithm != "sha256":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        digest = ref.get("digest")
        stored_digest = ref.get("stored_sha256")
        if type(digest) is not str or _SHA256_RE.fullmatch(digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if type(stored_digest) is not str or _SHA256_RE.fullmatch(stored_digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._validate_logical_type(ref.get("logical_kind"), ref.get("media_type"))
        for field in ("uncompressed_size", "stored_size"):
            number = ref.get(field)
            if type(number) is not int or number < 0:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if ref["uncompressed_size"] > 16 * _MIB or ref["stored_size"] > 16 * _MIB + 64 * 1024:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        encoding = ref.get("content_encoding")
        if type(encoding) is not str:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if encoding not in {"identity", "gzip"}:
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        if encoding == "identity":
            if "encoding_parameters" in ref:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if ref["uncompressed_size"] >= _GZIP_THRESHOLD:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if ref["stored_size"] != ref["uncompressed_size"]:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if ref["stored_sha256"] != ref["digest"]:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        else:
            parameters = ref.get("encoding_parameters")
            if parameters != _GZIP_CODEC:
                if type(parameters) is str:
                    raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if ref["uncompressed_size"] < _GZIP_THRESHOLD or ref["stored_size"] <= 0:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return ref

    def blob_path(self, ref: Mapping[str, Any]) -> Path:
        validated = self._validate_ref(ref)
        digest = validated["digest"]
        path = self._blob_root / digest[:2] / digest[2:4] / f"{digest}.blob"
        self._assert_within_root(path)
        self._assert_existing_chain_safe(path.parent)
        return path

    def put_blob(
        self,
        data: bytes | bytearray | memoryview,
        *,
        logical_kind: str,
        media_type: str,
    ) -> dict[str, Any]:
        ref, _created = self.put_blob_with_outcome(
            data,
            logical_kind=logical_kind,
            media_type=media_type,
        )
        return ref

    def put_blob_with_outcome(
        self,
        data: bytes | bytearray | memoryview,
        *,
        logical_kind: str,
        media_type: str,
    ) -> tuple[dict[str, Any], bool]:
        """Publish one immutable blob and report whether this call won creation.

        The outcome is established by the same pinned-directory/no-replace
        operation that publishes the object.  Callers therefore do not need an
        unsafe pathname pre-check merely to collect physical write metrics.
        """
        ref, stored = self._prepare_blob(
            data,
            logical_kind=logical_kind,
            media_type=media_type,
        )
        target = self.blob_path(ref)
        self._ensure_directory(target.parent)
        if target.exists() or target.is_symlink():
            self._verify_blob_at_path(ref, target)
            self._record_accepted_cas_write(ref["stored_size"], created=False)
            return ref, False
        created = self._publish_blob_no_replace(target, stored, ref)
        self._record_accepted_cas_write(ref["stored_size"], created=created)
        return ref, created

    def describe_blob(
        self,
        data: bytes | bytearray | memoryview,
        *,
        logical_kind: str,
        media_type: str,
    ) -> dict[str, Any]:
        """Return the deterministic typed CAS reference without publishing.

        Recovery uses this seam to verify that a retained control generation
        still has its earlier-published CAS object.  Unlike ``put_blob``, it
        cannot heal or otherwise mutate a missing object while validating
        durable state.
        """

        ref, _stored = self._prepare_blob(
            data,
            logical_kind=logical_kind,
            media_type=media_type,
        )
        return ref

    def _prepare_blob(
        self,
        data: bytes | bytearray | memoryview,
        *,
        logical_kind: str,
        media_type: str,
    ) -> tuple[dict[str, Any], bytes]:
        payload = _bounded_bytes(data, self.limits.max_blob_uncompressed_bytes)
        self._validate_logical_type(logical_kind, media_type)
        digest = hashlib.sha256(payload).hexdigest()
        if len(payload) < _GZIP_THRESHOLD:
            stored = payload
            encoding = "identity"
        else:
            stored = _gzip_bytes(payload)
            encoding = "gzip"
        if len(stored) > self.limits.max_blob_stored_bytes:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if encoding == "gzip" and len(payload) > len(stored) * self.limits.max_compression_ratio:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        ref: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_blob_ref",
            "hash_algorithm": "sha256",
            "digest": digest,
            "logical_kind": logical_kind,
            "media_type": media_type,
            "content_encoding": encoding,
            "uncompressed_size": len(payload),
            "stored_size": len(stored),
            "stored_sha256": hashlib.sha256(stored).hexdigest(),
        }
        if encoding == "gzip":
            ref["encoding_parameters"] = _GZIP_CODEC
        return self._validate_ref(ref), stored

    @contextmanager
    def account_cas_writes(self):
        """Account accepted CAS outcomes on this thread without tree scans.

        A put is visible to the scope only after the existing or newly-created
        object has passed the normal full verification.  Failed attempts are
        absent.  Nested scopes are supported so diagnostics can observe an
        outer operation without changing the runtime's own exact accounting.
        """

        accounting = _PromptArchiveCasWriteAccounting()
        stack = getattr(self._cas_write_accounting, "stack", ())
        self._cas_write_accounting.stack = (*stack, accounting)
        try:
            yield accounting
        finally:
            current = getattr(self._cas_write_accounting, "stack", ())
            if not current or current[-1] is not accounting:
                self._cas_write_accounting.stack = ()
                raise RuntimeError("Prompt archive CAS accounting scope mismatch")
            self._cas_write_accounting.stack = current[:-1]

    def _record_accepted_cas_write(self, stored_size: int, *, created: bool) -> None:
        for accounting in getattr(self._cas_write_accounting, "stack", ()):
            accounting.record(stored_size=stored_size, created=created)

    def _publish_blob_no_replace(
        self,
        target: Path,
        stored: bytes,
        ref: Mapping[str, Any],
    ) -> bool:
        temporary_name = _storage_temporary_name(target.name)
        created_final = False
        parent_identity = self._directory_identity(target.parent)
        with self._open_pinned_directory(target.parent) as pinned:
            descriptor = -1
            try:
                # The parent handle is the mutation authority.  Give fault/race
                # harnesses a boundary before the first write, then prove that
                # its pathname still names the directory that was pinned.  On
                # Windows the pinned handle also denies FILE_SHARE_DELETE, so
                # the directory cannot be renamed while publication is active.
                self._fault("after_parent_pin")
                self._assert_directory_identity(target.parent, parent_identity)
                descriptor = self._create_relative_temp(pinned, temporary_name)
                with os.fdopen(descriptor, "wb", closefd=False) as stream:
                    self._fault("after_temp_create")
                    stream.write(stored)
                    self._fault("after_temp_write")
                    stream.flush()
                    self._fault("after_temp_flush")
                    os.fsync(stream.fileno())
                    self._fault("after_temp_fsync")
                observed = self._read_open_descriptor(
                    descriptor,
                    maximum=self.limits.max_blob_stored_bytes,
                )
                self._decode_stored(ref, observed, max_output_bytes=None)
                self._fault("after_temp_verify")
                self._fault("before_publish")
                self._assert_directory_identity(target.parent, parent_identity)
                self._fault("after_parent_precheck")
                created_final = _atomic_rename_pinned_no_replace(
                    descriptor,
                    pinned,
                    temporary_name,
                    target.name,
                )
                if created_final:
                    os.close(descriptor)
                    descriptor = -1
                self._assert_directory_identity(target.parent, parent_identity)
                self._fault("after_publish")
                if created_final:
                    self.directory_sync_supported = (
                        self._sync_pinned_directory(pinned)
                        and self.directory_sync_supported
                    )
                    self._fault("after_directory_sync")
            except PromptArchiveStorageError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            finally:
                if descriptor != -1:
                    if not created_final:
                        self._delete_relative_temp(
                            pinned,
                            temporary_name,
                            descriptor,
                        )
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
        self._verify_blob_at_path(ref, target)
        return created_final

    @staticmethod
    def _sync_pinned_directory(directory: _PinnedDirectory) -> bool:
        if os.name == "nt":
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            flush = kernel32.FlushFileBuffers
            flush.argtypes = (wintypes.HANDLE,)
            flush.restype = wintypes.BOOL
            return bool(flush(directory.descriptor))
        try:
            os.fsync(directory.descriptor)
            return True
        except OSError:
            return False

    @staticmethod
    def _sync_directory(directory: Path) -> bool:
        if os.name == "nt":
            # Python has no portable directory FlushFileBuffers primitive.
            # The file itself is flushed before publication; callers can
            # observe this explicit capability bit rather than infer more.
            return False
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        try:
            descriptor = os.open(directory, flags)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return True
        except OSError:
            return False

    def _read_regular_file(
        self,
        path: Path,
        *,
        maximum: int,
        missing_code: str,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> bytes:
        if cancellation is not None:
            cancellation.check()
        self._assert_existing_chain_safe(path)
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise _error(missing_code) from None
        except OSError as exc:
            if _path_entry_is_redirected(path):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        chunks: list[bytes] = []
        observed = 0
        try:
            self._assert_open_handle(descriptor, path)
            metadata = os.fstat(descriptor)
            _require_regular(metadata)
            if metadata.st_nlink != 1:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            while True:
                if cancellation is not None:
                    cancellation.check()
                chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - observed))
                if not chunk:
                    break
                observed += len(chunk)
                if observed > maximum:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                chunks.append(chunk)
            metadata_after = os.fstat(descriptor)
            _require_regular(metadata_after)
            if (
                metadata_after.st_nlink != 1
                or (metadata_after.st_dev, metadata_after.st_ino)
                != (metadata.st_dev, metadata.st_ino)
                or metadata_after.st_size != metadata.st_size
                or metadata_after.st_size != observed
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            try:
                path_metadata = path.lstat()
            except OSError as exc:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            if _path_entry_is_redirected(path):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            _require_regular(path_metadata)
            if (
                path_metadata.st_nlink != 1
                or (metadata_after.st_dev, metadata_after.st_ino)
                != (path_metadata.st_dev, path_metadata.st_ino)
                or path_metadata.st_size != metadata_after.st_size
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            self._assert_open_handle(descriptor, path)
        except PromptArchiveStorageError:
            raise
        except OSError as exc:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass
        self._assert_root_identity()
        if cancellation is not None:
            cancellation.check()
        return b"".join(chunks)

    def _decode_stored(
        self,
        ref: Mapping[str, Any],
        stored: bytes,
        *,
        max_output_bytes: int | None,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> bytes:
        if cancellation is not None:
            cancellation.check()
        if len(stored) != ref["stored_size"]:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        if hashlib.sha256(stored).hexdigest() != ref["stored_sha256"]:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        output_cap = min(
            self.limits.max_blob_uncompressed_bytes,
            self.limits.max_hydrated_bytes,
        )
        if max_output_bytes is not None:
            if (
                type(max_output_bytes) is not int
                or max_output_bytes < 0
            ):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            output_cap = min(output_cap, max_output_bytes)
        if ref["uncompressed_size"] > output_cap:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if ref["content_encoding"] == "identity":
            output = stored
        else:
            output = self._decode_exact_gzip(
                stored,
                output_cap,
                cancellation=cancellation,
            )
            if len(output) > len(stored) * self.limits.max_compression_ratio:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            if _gzip_bytes(output, cancellation=cancellation) != stored:
                raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        if len(output) != ref["uncompressed_size"]:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        if hashlib.sha256(output).hexdigest() != ref["digest"]:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        if cancellation is not None:
            cancellation.check()
        return output

    @staticmethod
    def _decode_exact_gzip(
        stored: bytes,
        output_cap: int,
        *,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> bytes:
        if cancellation is not None:
            cancellation.check()
        if len(stored) < 18 or stored[:10] != _GZIP_HEADER:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        body = stored[10:-8]
        expected_crc, expected_size = struct.unpack("<II", stored[-8:])
        inflater = zlib.decompressobj(-15)
        output_parts: list[bytes] = []
        output_size = 0
        try:
            for offset in range(0, len(body), 64 * 1024):
                if cancellation is not None:
                    cancellation.check()
                pending = body[offset : offset + 64 * 1024]
                while pending:
                    if cancellation is not None:
                        cancellation.check()
                    chunk = inflater.decompress(pending, output_cap - output_size + 1)
                    output_size += len(chunk)
                    if output_size > output_cap:
                        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                    output_parts.append(chunk)
                    pending = inflater.unconsumed_tail
                    if not pending:
                        break
            if cancellation is not None:
                cancellation.check()
            tail = inflater.flush(output_cap - output_size + 1)
        except PromptArchiveStorageError:
            raise
        except zlib.error as exc:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT") from None
        output_size += len(tail)
        if output_size > output_cap:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        output_parts.append(tail)
        if not inflater.eof or inflater.unused_data or inflater.unconsumed_tail:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        output = b"".join(output_parts)
        if len(output) & 0xFFFFFFFF != expected_size:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        if zlib.crc32(output) & 0xFFFFFFFF != expected_crc:
            raise _error("PROMPT_ARCHIVE_BLOB_CORRUPT")
        return output

    def _verify_blob_at_path(self, ref: Mapping[str, Any], path: Path) -> bytes:
        maximum = self.limits.max_blob_stored_bytes
        if ref["uncompressed_size"] > min(
            self.limits.max_blob_uncompressed_bytes,
            self.limits.max_hydrated_bytes,
        ):
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if (
            ref["content_encoding"] == "gzip"
            and ref["uncompressed_size"]
            > ref["stored_size"] * self.limits.max_compression_ratio
        ):
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if ref["stored_size"] > self.limits.max_blob_stored_bytes:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        stored = self._read_regular_file(
            path,
            maximum=maximum,
            missing_code="PROMPT_ARCHIVE_BLOB_MISSING",
        )
        return self._decode_stored(ref, stored, max_output_bytes=None)

    def read_blob(
        self,
        ref: Mapping[str, Any],
        *,
        max_output_bytes: int | None = None,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> bytes:
        if cancellation is not None:
            cancellation.check()
        validated = self._validate_ref(ref)
        path = self.blob_path(validated)
        maximum = self.limits.max_blob_stored_bytes
        if validated["uncompressed_size"] > min(
            self.limits.max_blob_uncompressed_bytes,
            self.limits.max_hydrated_bytes,
        ):
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if (
            validated["content_encoding"] == "gzip"
            and validated["uncompressed_size"]
            > validated["stored_size"] * self.limits.max_compression_ratio
        ):
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if validated["stored_size"] > self.limits.max_blob_stored_bytes:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        stored = self._read_regular_file(
            path,
            maximum=maximum,
            missing_code="PROMPT_ARCHIVE_BLOB_MISSING",
            cancellation=cancellation,
        )
        return self._decode_stored(
            validated,
            stored,
            max_output_bytes=max_output_bytes,
            cancellation=cancellation,
        )

    def verify_blob(self, ref: Mapping[str, Any]) -> None:
        self.read_blob(ref)

    @staticmethod
    def _validate_relative_components(components: Sequence[str]) -> tuple[str, ...]:
        if isinstance(components, (str, bytes)) or not isinstance(components, Sequence):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            component_count = len(components)
        except (TypeError, ValueError, OverflowError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if component_count < 3 or component_count > 6:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            result = tuple(components[index] for index in range(component_count))
            stable_count = len(components)
        except (IndexError, KeyError, TypeError, ValueError, OverflowError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if stable_count != component_count:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        reserved = {
            "con",
            "prn",
            "aux",
            "nul",
            *(f"com{index}" for index in range(1, 10)),
            *(f"lpt{index}" for index in range(1, 10)),
        }
        for component in result:
            if (
                type(component) is not str
                or _COMPONENT_RE.fullmatch(component) is None
                or component in {".", ".."}
                or component[-1:] in {".", " "}
                or ":" in component
                or "/" in component
                or "\\" in component
                or component.split(".", 1)[0].casefold() in reserved
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if PromptArchiveStorage._immutable_artifact_kind(result) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return result

    @staticmethod
    def _immutable_artifact_kind(components: tuple[str, ...]) -> str | None:
        """Classify a closed, immutable PAS1 stable control-artifact path.

        The mutable current journal/shard files and lock receipts listed by
        the contract are intentionally excluded from this create-or-verify
        seam.  Their publication protocols require compare-and-swap or lock
        lifecycle semantics, not immutable first-writer-wins publication.
        """

        if (
            len(components) == 5
            and components[0:2] == ("operations", "v1")
            and _RESPONSE_ID_RE.fullmatch(components[2]) is not None
            and _OPERATION_ID_RE.fullmatch(components[3]) is not None
            and components[4] == "plan.json"
        ):
            return "publication_plan"
        if (
            len(components) == 5
            and components[0:2] == ("operations", "v1")
            and _RESPONSE_ID_RE.fullmatch(components[2]) is not None
            and _OPERATION_ID_RE.fullmatch(components[3]) is not None
            and components[4] == "compatibility-result.json"
        ):
            return "compatibility_result"
        if (
            len(components) == 6
            and components[0:2] == ("operations", "v1")
            and _RESPONSE_ID_RE.fullmatch(components[2]) is not None
            and _OPERATION_ID_RE.fullmatch(components[3]) is not None
            and components[4] == "journal-generations"
            and _JOURNAL_GENERATION_FILE_RE.fullmatch(components[5]) is not None
        ):
            return "journal_generation"
        if (
            len(components) == 5
            and components[0:3] == ("pending", "v1", "generations")
            and _SHARD_RE.fullmatch(components[3]) is not None
        ):
            matched = _PENDING_GENERATION_FILE_RE.fullmatch(components[4])
            if matched is not None:
                generation = int(matched.group(1))
                registry_id = matched.group(2)
                if (
                    generation <= _MAX_PENDING_GENERATION
                    and _PENDING_REGISTRY_ID_RE.fullmatch(registry_id) is not None
                ):
                    return "pending_registry_generation"
        if (
            len(components) == 5
            and components[0] == "sprints"
            and _SPRINT_STORAGE_KEY_RE.fullmatch(components[1]) is not None
            and components[2:4] == ("pointers", "v1")
            and components[4].endswith(".json")
            and _RESPONSE_ID_RE.fullmatch(components[4][:-5]) is not None
        ):
            return "pointer"
        if (
            len(components) == 5
            and components[0] == "sprints"
            and _SPRINT_STORAGE_KEY_RE.fullmatch(components[1]) is not None
            and components[2:4] == ("archives", "v1")
            and components[4].endswith(".json")
            and _ARCHIVE_ID_RE.fullmatch(components[4][:-5]) is not None
        ):
            return "archive_locator"
        if (
            len(components) == 3
            and components[0:2] == ("inventories", "v1")
            and components[2].endswith(".json")
            and _INVENTORY_ID_RE.fullmatch(components[2][:-5]) is not None
        ):
            return "source_inventory"
        if (
            len(components) == 4
            and components[0:2] == ("migrations", "v1")
            and _MIGRATION_BATCH_ID_RE.fullmatch(components[2]) is not None
            and components[3] == "plan.json"
        ):
            return "migration_batch_plan"
        if (
            len(components) == 5
            and components[0:2] == ("migrations", "v1")
            and _MIGRATION_BATCH_ID_RE.fullmatch(components[2]) is not None
            and components[3] == "journal-generations"
            and _MIGRATION_JOURNAL_GENERATION_FILE_RE.fullmatch(components[4])
            is not None
        ):
            return "migration_batch_journal"
        if (
            len(components) == 4
            and components[0:2] == ("migrations", "v1")
            and _MIGRATION_BATCH_ID_RE.fullmatch(components[2]) is not None
            and components[3] == "evidence.json"
        ):
            return "migration_evidence"
        return None

    def _immutable_artifact_limit(self, components: tuple[str, ...]) -> int:
        kind = self._immutable_artifact_kind(components)
        if kind is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if kind == "archive_locator":
            return min(self.limits.max_immutable_bytes, _ARCHIVE_LOCATOR_MAX_BYTES)
        return self.limits.max_immutable_bytes

    def _immutable_path_from_validated(self, validated: tuple[str, ...]) -> Path:
        path = self._project_root.joinpath(*validated)
        self._assert_within_root(path)
        self._assert_existing_chain_safe(path.parent)
        return path

    def _immutable_path(self, components: Sequence[str]) -> Path:
        validated = self._validate_relative_components(components)
        return self._immutable_path_from_validated(validated)

    @staticmethod
    def _validate_mutable_components(
        components: Sequence[str],
        *,
        lock: bool,
    ) -> tuple[str, ...]:
        if isinstance(components, (str, bytes)) or not isinstance(components, Sequence):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            result = tuple(components)
        except (TypeError, ValueError, OverflowError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if any(
            type(component) is not str
            or _COMPONENT_RE.fullmatch(component) is None
            or component in {".", ".."}
            or ":" in component
            or "/" in component
            or "\\" in component
            for component in result
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        operation = (
            len(result) == 5
            and result[0:2] == ("operations", "v1")
            and _RESPONSE_ID_RE.fullmatch(result[2]) is not None
            and _OPERATION_ID_RE.fullmatch(result[3]) is not None
            and result[4] == ("operation.lock" if lock else "journal.json")
        )
        shard = (
            len(result) == 4
            and result[0:3]
            == (("pending", "v1", "locks") if lock else ("pending", "v1", "shards"))
            and result[3].endswith(".lock" if lock else ".json")
            and _SHARD_RE.fullmatch(result[3][:-5]) is not None
        )
        migration = (
            len(result) == 4
            and result[0:2] == ("migrations", "v1")
            and _MIGRATION_BATCH_ID_RE.fullmatch(result[2]) is not None
            and result[3] == ("operation.lock" if lock else "journal.json")
        )
        if not (operation or shard or migration):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return result

    def _mutable_path_from_validated(self, components: tuple[str, ...]) -> Path:
        path = self._project_root.joinpath(*components)
        self._assert_within_root(path)
        self._assert_existing_chain_safe(path.parent)
        return path

    def _assert_relative_control_handle(
        self,
        descriptor: int,
        *,
        parent: _PinnedDirectory,
        target: Path,
        compare_size: bool = True,
    ) -> os.stat_result:
        """Bind an opened mutable leaf to its exact pinned in-root parent."""

        self._validate_pinned_directory(parent, target.parent)
        self._assert_open_handle(descriptor, target)
        metadata = os.fstat(descriptor)
        _require_regular(metadata)
        if metadata.st_nlink != 1:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        resolved = resolved_path_from_open_descriptor(descriptor)
        if os.name == "nt":
            if resolved is None:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
            if resolved.name != target.name:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            if os.name == "nt":
                path_metadata = target.lstat()
            else:
                path_metadata = os.stat(
                    target.name,
                    dir_fd=parent.descriptor,
                    follow_symlinks=False,
                )
        except FileNotFoundError:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        _require_regular(path_metadata)
        if (
            path_metadata.st_nlink != 1
            or (path_metadata.st_dev, path_metadata.st_ino)
            != (metadata.st_dev, metadata.st_ino)
            or (compare_size and path_metadata.st_size != metadata.st_size)
            or _path_entry_is_redirected(target)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return metadata

    def _open_relative_control_file(
        self,
        parent: _PinnedDirectory,
        target: Path,
        *,
        create: bool,
        writable: bool,
        compare_size: bool = True,
    ) -> int | None:
        """Open one closed mutable filename under an already pinned parent."""

        flags = os.O_RDWR if writable else os.O_RDONLY
        flags |= getattr(os, "O_BINARY", 0)
        if create:
            flags |= os.O_CREAT
        if os.name != "nt":
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            if not nofollow:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
            flags |= nofollow
        try:
            if os.name == "nt":
                descriptor = _windows_open_relative_control_file(
                    parent.descriptor,
                    target.name,
                    create=create,
                    writable=writable,
                )
            else:
                descriptor = os.open(
                    target.name,
                    flags,
                    0o600,
                    dir_fd=parent.descriptor,
                )
        except FileNotFoundError:
            if not create:
                return None
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        except OSError:
            if _path_entry_is_redirected(target):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        try:
            self._assert_relative_control_handle(
                descriptor,
                parent=parent,
                target=target,
                compare_size=compare_size,
            )
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _read_relative_control_file(
        self,
        parent: _PinnedDirectory,
        target: Path,
    ) -> bytes | None:
        descriptor = self._open_relative_control_file(
            parent,
            target,
            create=False,
            writable=False,
        )
        if descriptor is None:
            return None
        try:
            before = self._assert_relative_control_handle(
                descriptor,
                parent=parent,
                target=target,
            )
            if before.st_size > _MUTABLE_CONTROL_MAX_BYTES:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            payload = self._read_open_descriptor(
                descriptor,
                maximum=_MUTABLE_CONTROL_MAX_BYTES,
            )
            after = self._assert_relative_control_handle(
                descriptor,
                parent=parent,
                target=target,
            )
            if (
                (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
                or after.st_size != before.st_size
                or after.st_size != len(payload)
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            return payload
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass

    @staticmethod
    def _replace_relative_control_file(
        descriptor: int,
        parent: _PinnedDirectory,
        temporary_name: str,
        target_name: str,
    ) -> None:
        if os.name == "nt":
            _windows_rename_relative_replace(
                descriptor,
                parent.descriptor,
                target_name,
            )
            return
        try:
            os.replace(
                temporary_name,
                target_name,
                src_dir_fd=parent.descriptor,
                dst_dir_fd=parent.descriptor,
            )
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None

    @contextmanager
    def control_lock(self, relative_components: Sequence[str]):
        """Take one closed cross-process operation or pending-shard lock."""

        validated = self._validate_mutable_components(relative_components, lock=True)
        target = self._mutable_path_from_validated(validated)
        self._ensure_directory(target.parent)
        descriptor = -1
        with self._open_pinned_directory(target.parent) as parent:
            try:
                descriptor = self._open_relative_control_file(
                    parent,
                    target,
                    create=True,
                    writable=True,
                    compare_size=False,
                )
                assert descriptor is not None
                metadata = os.fstat(descriptor)
                if metadata.st_size not in {0, 1}:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                if metadata.st_size == 0:
                    os.write(descriptor, b"\0")
                    os.fsync(descriptor)
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    _windows_lock_descriptor(descriptor, unlock=False)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                try:
                    self._assert_relative_control_handle(
                        descriptor,
                        parent=parent,
                        target=target,
                    )
                    yield
                    self._assert_relative_control_handle(
                        descriptor,
                        parent=parent,
                        target=target,
                    )
                finally:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    if os.name == "nt":
                        _windows_lock_descriptor(descriptor, unlock=True)
                    else:
                        import fcntl

                        fcntl.flock(descriptor, fcntl.LOCK_UN)
            except PromptArchiveStorageError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            finally:
                if descriptor != -1:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass

    def read_current_json(
        self,
        relative_components: Sequence[str],
    ) -> dict[str, Any] | None:
        """Read one fixed mutable current control file; absence is explicit."""

        validated = self._validate_mutable_components(relative_components, lock=False)
        path = self._mutable_path_from_validated(validated)
        self._ensure_directory(path.parent)
        with self._open_pinned_directory(path.parent) as parent:
            payload = self._read_relative_control_file(parent, path)
        if payload is None:
            return None
        return parse_canonical_json_v1(payload, maximum=_MUTABLE_CONTROL_MAX_BYTES)

    def replace_current_json(
        self,
        relative_components: Sequence[str],
        document: Any,
    ) -> ImmutablePublicationReceipt:
        """Atomically replace one fixed current file while its caller holds its lock."""

        validated = self._validate_mutable_components(relative_components, lock=False)
        payload = canonical_json_bytes_v1(document, maximum=_MUTABLE_CONTROL_MAX_BYTES)
        digest = hashlib.sha256(payload).hexdigest()
        target = self._mutable_path_from_validated(validated)
        self._ensure_directory(target.parent)
        temporary_name = _storage_temporary_name(target.name)
        descriptor = -1
        replaced = False
        with self._open_pinned_directory(target.parent) as parent:
            try:
                existing = self._open_relative_control_file(
                    parent,
                    target,
                    create=False,
                    writable=False,
                )
                if existing is not None:
                    os.close(existing)
                descriptor = self._create_relative_temp(parent, temporary_name)
                with os.fdopen(descriptor, "wb", closefd=False) as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                observed = self._read_open_descriptor(
                    descriptor,
                    maximum=_MUTABLE_CONTROL_MAX_BYTES,
                )
                if observed != payload:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                self._validate_pinned_directory(parent, target.parent)
                self._replace_relative_control_file(
                    descriptor,
                    parent,
                    temporary_name,
                    target.name,
                )
                replaced = True
                self._assert_relative_control_handle(
                    descriptor,
                    parent=parent,
                    target=target,
                )
                self.directory_sync_supported = (
                    self._sync_pinned_directory(parent)
                    and self.directory_sync_supported
                )
                os.close(descriptor)
                descriptor = -1
                verified = self._read_relative_control_file(parent, target)
                if verified != payload:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                return ImmutablePublicationReceipt(
                    target,
                    digest,
                    len(payload),
                    True,
                )
            except PromptArchiveStorageError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            finally:
                if descriptor != -1:
                    if not replaced:
                        self._delete_relative_temp(
                            parent,
                            temporary_name,
                            descriptor,
                        )
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass

    def publish_immutable_bytes(
        self,
        relative_components: Sequence[str],
        data: bytes | bytearray | memoryview,
        *,
        expected_sha256: str | None = None,
    ) -> ImmutablePublicationReceipt:
        validated = self._validate_relative_components(relative_components)
        maximum = self._immutable_artifact_limit(validated)
        payload = _bounded_bytes(data, maximum)
        digest = hashlib.sha256(payload).hexdigest()
        if expected_sha256 is not None:
            if type(expected_sha256) is not str or _SHA256_RE.fullmatch(expected_sha256) is None:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if digest != expected_sha256:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        target = self._immutable_path_from_validated(validated)
        self._ensure_directory(target.parent)
        if target.exists() or target.is_symlink():
            existing = self._read_immutable_for_replay(target, len(payload))
            if existing != payload:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            return ImmutablePublicationReceipt(target, digest, len(payload), False)
        created = self._publish_immutable_no_replace(target, payload)
        if not created:
            existing = self._read_immutable_for_replay(target, len(payload))
            if existing != payload:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        else:
            published = self._read_immutable_for_replay(target, len(payload))
            if published != payload:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            self._fault("after_final_verify")
        return ImmutablePublicationReceipt(target, digest, len(payload), created)

    def _publish_immutable_no_replace(self, target: Path, payload: bytes) -> bool:
        temporary_name = _storage_temporary_name(target.name)
        created_final = False
        parent_identity = self._directory_identity(target.parent)
        with self._open_pinned_directory(target.parent) as pinned:
            descriptor = -1
            try:
                self._fault("after_parent_pin")
                self._assert_directory_identity(target.parent, parent_identity)
                descriptor = self._create_relative_temp(pinned, temporary_name)
                with os.fdopen(descriptor, "wb", closefd=False) as stream:
                    self._fault("after_temp_create")
                    stream.write(payload)
                    self._fault("after_temp_write")
                    stream.flush()
                    self._fault("after_temp_flush")
                    os.fsync(stream.fileno())
                    self._fault("after_temp_fsync")
                observed = self._read_open_descriptor(
                    descriptor,
                    maximum=len(payload),
                )
                if observed != payload:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                self._fault("after_temp_verify")
                self._fault("before_publish")
                self._assert_directory_identity(target.parent, parent_identity)
                self._fault("after_parent_precheck")
                created_final = _atomic_rename_pinned_no_replace(
                    descriptor,
                    pinned,
                    temporary_name,
                    target.name,
                )
                if created_final:
                    os.close(descriptor)
                    descriptor = -1
                self._assert_directory_identity(target.parent, parent_identity)
                self._fault("after_publish")
                if created_final:
                    self.directory_sync_supported = (
                        self._sync_pinned_directory(pinned)
                        and self.directory_sync_supported
                    )
                    self._fault("after_directory_sync")
            except PromptArchiveStorageError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            finally:
                if descriptor != -1:
                    if not created_final:
                        self._delete_relative_temp(
                            pinned,
                            temporary_name,
                            descriptor,
                        )
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
        return created_final

    def _read_immutable_for_replay(self, path: Path, expected_size: int) -> bytes:
        try:
            return self._read_regular_file(
                path,
                maximum=expected_size,
                missing_code="PROMPT_ARCHIVE_NOT_FOUND",
            )
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_LIMIT_EXCEEDED":
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT") from None
            raise

    def publish_immutable_json(
        self,
        relative_components: Sequence[str],
        document: Any,
        *,
        expected_sha256: str | None = None,
    ) -> ImmutablePublicationReceipt:
        validated = self._validate_relative_components(relative_components)
        payload = _canonical_json_bytes(
            document,
            maximum=self._immutable_artifact_limit(validated),
        )
        return self.publish_immutable_bytes(
            validated,
            payload,
            expected_sha256=expected_sha256,
        )

    def verify_immutable_bytes(
        self,
        relative_components: Sequence[str],
        *,
        expected_sha256: str,
        expected_size: int | None = None,
    ) -> bytes:
        if type(expected_sha256) is not str or _SHA256_RE.fullmatch(expected_sha256) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if expected_size is not None and (
            type(expected_size) is not int
            or expected_size < 0
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        validated = self._validate_relative_components(relative_components)
        maximum = self._immutable_artifact_limit(validated)
        if expected_size is not None and expected_size > maximum:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        path = self._immutable_path_from_validated(validated)
        payload = self._read_regular_file(
            path,
            maximum=maximum,
            missing_code="PROMPT_ARCHIVE_NOT_FOUND",
        )
        if expected_size is not None and len(payload) != expected_size:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return payload

    def read_immutable_json(
        self,
        relative_components: Sequence[str],
        *,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> dict[str, Any]:
        """Read one closed stable control artifact under its contract bound.

        Callers select only a grammar-recognized tuple of path components;
        arbitrary paths never enter this seam.  The artifact-specific byte
        ceiling is enforced before canonical JSON is decoded.
        """

        validated = self._validate_relative_components(relative_components)
        maximum = self._immutable_artifact_limit(validated)
        path = self._immutable_path_from_validated(validated)
        payload = self._read_regular_file(
            path,
            maximum=maximum,
            missing_code="PROMPT_ARCHIVE_NOT_FOUND",
            cancellation=cancellation,
        )
        return parse_canonical_json_v1(payload, maximum=maximum)


__all__ = [
    "canonical_json_bytes_v1",
    "ImmutablePublicationReceipt",
    "parse_canonical_json_v1",
    "PromptArchiveCasWriteMetrics",
    "PromptArchiveCancellationToken",
    "PromptArchiveLimits",
    "PromptArchiveStorage",
    "PromptArchiveStorageError",
    "resolved_path_from_open_descriptor",
]
