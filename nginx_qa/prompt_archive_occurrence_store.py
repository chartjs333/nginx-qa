"""Durable, path-free source proofs for Prompt Archive runtime occurrences.

The archive runtime needs to resume an interrupted publication without trusting
an in-memory occurrence or a mutable ``latest`` alias.  This module stores one
small immutable receipt under the hash of a closed runtime locator.  The
receipt contains occurrence metadata and a digest/size binding for the exact
legacy response, but never contains its bytes or a filesystem path.

Lookup is direct.  No directory is enumerated and no receipt deletion API is
provided.  The configured source reader receives only a safe logical response
key and must derive the trusted source location itself.
"""

from __future__ import annotations

import dataclasses
import ctypes
import errno
import hashlib
import hmac
import os
import re
import stat
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Literal, Protocol
from uuid import uuid4

from nginx_qa.prompt_archive_incremental import ArchiveOccurrence, response_id_v1
from nginx_qa.prompt_archive_storage import (
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
    parse_canonical_json_v1,
    resolved_path_from_open_descriptor,
)


_MAX_INT64: Final = 9_223_372_036_854_775_807
_LOCATOR_MAX_BYTES: Final = 4 * 1024
_RECEIPT_MAX_BYTES: Final = 64 * 1024
_SOURCE_DOCUMENT_MAX_BYTES: Final = 16 * 1024
_DEFAULT_MAX_SOURCE_BYTES: Final = 512 * 1024 * 1024
_REPARSE_POINT: Final = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)

_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$", re.ASCII)
_RESPONSE_ID_RE: Final = re.compile(r"^parresp1-[0-9a-f]{64}$", re.ASCII)
_RESPONSE_KEY_RE: Final = re.compile(r"^[0-9a-f]{20}$", re.ASCII)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_COMMIT_RE: Final = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$", re.ASCII)
_ARCHIVE_ID_RE: Final = re.compile(r"^par1-[0-9a-f]{64}$", re.ASCII)
_RFC3339_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$",
    re.ASCII,
)
_SECRET_PREFIX_RE: Final = re.compile(
    r"^(?:sk-(?:proj-)?|ghp_|github_pat_|glpat-|xox[A-Za-z0-9]{1,8}-)",
    re.IGNORECASE | re.ASCII,
)

_RESPONSE_KINDS: Final = frozenset(
    {
        "identity-response",
        "transition-response",
        "state-response",
        "migration-snapshot",
    }
)
_OCCURRENCE_KINDS: Final = frozenset(
    {"transition", "identity-state", "migration"}
)
_SOURCE_KINDS: Final = frozenset(
    {"runtime_shadow", "native_runtime", "offline_migration"}
)
_SOURCE_KEYS: Final = frozenset(
    {
        "source_kind",
        "source_commit",
        "assignment_id",
        "review_id",
        "result_key",
        "predecessor_archive_id",
    }
)
_AUTHORIZATION_KEYS: Final = frozenset(
    {
        "auth_model_version",
        "principal_kind",
        "principal_id",
        "binding_id",
        "scope_source",
        "effective_scope_revision",
        "effective_scope_sha256",
        "decision_occurrence_id",
    }
)
_LOCATOR_KEYS: Final = frozenset(
    {
        "locator_kind",
        "locator_version",
        "runtime_instance_id",
        "runtime_occurrence_id",
        "state_revision",
    }
)
_RECEIPT_KEYS: Final = frozenset(
    {
        "schema_version",
        "kind",
        "media_type",
        "hash_algorithm",
        "project_id",
        "locator_sha256",
        "locator",
        "occurrence",
        "source",
        "receipt_sha256",
    }
)
_OCCURRENCE_KEYS: Final = frozenset(
    {
        "project_id",
        "sprint_id",
        "response_id",
        "response_kind",
        "occurrence_kind",
        "occurrence_id",
        "transition_id",
        "source",
        "authorization",
        "created_at",
        "verified_at",
        "state_revision",
    }
)


def _error(code: str) -> PromptArchiveStorageError:
    return PromptArchiveStorageError(code)


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchiveOccurrenceSource:
    """Path-free identity of exact committed legacy response bytes."""

    response_key: str
    byte_size: int
    sha256: str


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchiveOccurrenceProof:
    """A stored occurrence plus the current state of its trusted source."""

    status: Literal["verified", "source_unavailable", "source_changed"]
    occurrence: ArchiveOccurrence
    source: PromptArchiveOccurrenceSource


class PromptArchiveOccurrenceSourceReader(Protocol):
    """Resolve a logical response key inside a caller-owned trusted root."""

    def __call__(
        self,
        *,
        project_id: str,
        response_key: str,
        response_kind: str,
        maximum_bytes: int,
    ) -> bytes | bytearray | memoryview | None: ...


class _PinnedDirectory:
    """An opened directory handle used as the authority for relative I/O."""

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
    """Return the kernel-resolved path for a native Windows handle."""

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
    """Open a directory itself, without traversing a terminal reparse point."""

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
        # SYNCHRONIZE | FILE_READ_ATTRIBUTES | FILE_TRAVERSE | FILE_LIST_DIRECTORY
        0x00100000 | 0x00000080 | 0x00000020 | 0x00000001,
        # Deliberately exclude FILE_SHARE_DELETE: a pinned parent cannot be
        # renamed out from under a receipt/lock operation on Windows.
        0x00000001 | 0x00000002,
        None,
        3,  # OPEN_EXISTING
        0x02000000 | 0x00200000,  # BACKUP_SEMANTICS | OPEN_REPARSE_POINT
        None,
    )
    if handle == ctypes.c_void_p(-1).value:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    return int(handle)


def _windows_nt_open_relative(
    directory_handle: int,
    name: str,
    *,
    disposition: int,
    directory: bool,
    writable: bool,
    share_delete: bool = False,
    delete_access: bool = False,
) -> tuple[int, bool]:
    """Open/create one exact child relative to a pinned NT directory.

    The returned integer is a native handle for directories and a CRT file
    descriptor for files.  ``FILE_OPEN_REPARSE_POINT`` is always used, and the
    share mask intentionally excludes delete so the opened lock/receipt cannot
    be swapped while it is being checked or used.
    """

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
        0x00000040,  # OBJ_CASE_INSENSITIVE; exact spelling is checked later.
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
    if directory:
        desired_access = 0x00000001 | 0x00000020 | 0x00000080 | 0x00100000
        if writable:
            desired_access |= 0x00000002 | 0x00000004
    else:
        desired_access = 0x80000000 | 0x00000080 | 0x00100000
        if writable:
            desired_access |= 0x40000000
        if delete_access:
            desired_access |= 0x00010000
    create_options = (
        (0x00000001 if directory else 0x00000040)
        | 0x00000020
        | 0x00200000
    )
    file_attributes = 0x00000010 if directory else 0x00000080
    status = create_file(
        ctypes.byref(child_handle),
        desired_access,
        ctypes.byref(attributes),
        ctypes.byref(status_block),
        None,
        file_attributes,
        0x00000001 | 0x00000002 | (0x00000004 if share_delete else 0),
        disposition,
        create_options,
        None,
        0,
    )
    unsigned_status = ctypes.c_ulong(status).value
    if status != 0:
        if unsigned_status in {0xC0000034, 0xC000003A}:
            raise FileNotFoundError(name)
        if unsigned_status in {0xC0000035, 0xC00000BA}:
            raise FileExistsError(name)
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    created = status_block.Information == 2
    if directory:
        return int(child_handle.value), created
    try:
        descriptor = msvcrt.open_osfhandle(
            int(child_handle.value),
            (os.O_RDWR if writable else os.O_RDONLY)
            | getattr(os, "O_BINARY", 0),
        )
    except (OSError, ValueError):
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle(child_handle)
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
    return descriptor, created


def _windows_rename_relative_no_replace(
    descriptor: int,
    directory_handle: int,
    target_name: str,
) -> bool:
    """Rename an opened temp into one pinned parent without replacement."""

    import msvcrt
    from ctypes import wintypes

    encoded_name = target_name.encode("utf-16-le", errors="strict")
    information = ctypes.create_string_buffer(20 + len(encoded_name))
    ctypes.c_ubyte.from_buffer(information, 0).value = 0  # ReplaceIfExists = false
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
        10,  # FileRenameInformation
    )
    unsigned_status = ctypes.c_ulong(status).value
    if status == 0:
        return True
    if unsigned_status in {0xC0000035, 0xC00000BA}:
        return False
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
        4,  # FileDispositionInfo
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
    """Publish one name atomically and never replace an existing leaf."""

    if os.name == "nt":
        return _windows_rename_relative_no_replace(
            descriptor,
            directory.descriptor,
            target_name,
        )
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        # A link/unlink fallback has a crash window with link-count two and is
        # not an atomic rename.  Fail closed on platforms without RENAME_NOREPLACE.
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
        1,  # RENAME_NOREPLACE
    )
    if result == 0:
        return True
    if ctypes.get_errno() == errno.EEXIST:
        return False
    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")


def _is_redirected(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
    return stat.S_ISLNK(metadata.st_mode) or bool(
        _REPARSE_POINT
        and getattr(metadata, "st_file_attributes", 0) & _REPARSE_POINT
    )


def _require_regular(metadata: os.stat_result) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or bool(
            _REPARSE_POINT
            and getattr(metadata, "st_file_attributes", 0) & _REPARSE_POINT
        )
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")


def _validate_rfc3339(value: Any) -> str:
    if type(value) is not str or _RFC3339_RE.fullmatch(value) is None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _validate_safe_id(value: Any) -> str:
    if type(value) is not str or _SAFE_ID_RE.fullmatch(value) is None:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    if _SECRET_PREFIX_RE.match(value):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _canonical_mapping(value: Any, *, maximum: int) -> dict[str, Any]:
    if type(value) is not dict:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    raw = canonical_json_bytes_v1(value, maximum=maximum)
    return parse_canonical_json_v1(raw, maximum=maximum)


def _validate_source_metadata(value: Any) -> dict[str, Any]:
    source = _canonical_mapping(value, maximum=_SOURCE_DOCUMENT_MAX_BYTES)
    if not set(source).issubset(_SOURCE_KEYS) or source.get("source_kind") not in _SOURCE_KINDS:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    for name in ("assignment_id", "review_id", "result_key"):
        if name in source:
            _validate_safe_id(source[name])
    if "source_commit" in source and (
        type(source["source_commit"]) is not str
        or _COMMIT_RE.fullmatch(source["source_commit"]) is None
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    predecessor = source.get("predecessor_archive_id")
    if predecessor is not None and (
        type(predecessor) is not str
        or _ARCHIVE_ID_RE.fullmatch(predecessor) is None
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return source


def _validate_authorization(value: Any) -> dict[str, Any]:
    authorization = _canonical_mapping(
        value,
        maximum=_SOURCE_DOCUMENT_MAX_BYTES,
    )
    if (
        set(authorization) != _AUTHORIZATION_KEYS
        or authorization.get("auth_model_version")
        != "prompt-archive-authorization-v1"
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    for name in ("principal_id", "decision_occurrence_id"):
        _validate_safe_id(authorization.get(name))
    combination = {
        "managed_agent": ("managed_effective_scope", True),
        "legacy_local_principal": ("legacy_project_context", False),
        "offline_operator": ("offline_operator_gate", False),
        "synthetic_test": ("synthetic_test", False),
    }.get(authorization.get("principal_kind"))
    if combination is None or authorization.get("scope_source") != combination[0]:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if combination[1]:
        _validate_safe_id(authorization.get("binding_id"))
        revision = authorization.get("effective_scope_revision")
        digest = authorization.get("effective_scope_sha256")
        if (
            type(revision) is not int
            or not 0 <= revision <= _MAX_INT64
            or type(digest) is not str
            or _SHA256_RE.fullmatch(digest) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    elif any(
        authorization.get(name) is not None
        for name in (
            "binding_id",
            "effective_scope_revision",
            "effective_scope_sha256",
        )
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return authorization


def _validate_occurrence(value: Any, *, project_id: str) -> ArchiveOccurrence:
    if isinstance(value, ArchiveOccurrence):
        # The exact committed-response binding is normalized into the
        # receipt's separate closed ``source`` object.  Do not duplicate those
        # transport-only dataclass fields in the occurrence document.
        document: Any = {
            "project_id": value.project_id,
            "sprint_id": value.sprint_id,
            "response_id": value.response_id,
            "response_kind": value.response_kind,
            "occurrence_kind": value.occurrence_kind,
            "occurrence_id": value.occurrence_id,
            "transition_id": value.transition_id,
            "source": dict(value.source),
            "authorization": dict(value.authorization),
            "created_at": value.created_at,
            "verified_at": value.verified_at,
            "state_revision": value.state_revision,
        }
    else:
        document = value
    occurrence = _canonical_mapping(document, maximum=_RECEIPT_MAX_BYTES)
    if set(occurrence) != _OCCURRENCE_KEYS:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if occurrence.get("project_id") != project_id:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    _validate_safe_id(project_id)
    sprint_id = occurrence.get("sprint_id")
    if sprint_id is not None:
        _validate_safe_id(sprint_id)
    response_id = occurrence.get("response_id")
    occurrence_id = occurrence.get("occurrence_id")
    response_kind = occurrence.get("response_kind")
    occurrence_kind = occurrence.get("occurrence_kind")
    if (
        type(response_id) is not str
        or _RESPONSE_ID_RE.fullmatch(response_id) is None
        or response_kind not in _RESPONSE_KINDS
        or occurrence_kind not in _OCCURRENCE_KINDS
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    _validate_safe_id(occurrence_id)
    if response_id != response_id_v1(
        project_id,
        sprint_id,
        response_kind,
        occurrence_kind,
        occurrence_id,
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    transition_id = occurrence.get("transition_id")
    if occurrence_kind == "transition":
        _validate_safe_id(transition_id)
    elif transition_id is not None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if occurrence_kind == "migration":
        if response_kind != "migration-snapshot" or sprint_id is not None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    elif response_kind == "migration-snapshot" or sprint_id is None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    source = _validate_source_metadata(occurrence.get("source"))
    authorization = _validate_authorization(occurrence.get("authorization"))
    _validate_rfc3339(occurrence.get("created_at"))
    _validate_rfc3339(occurrence.get("verified_at"))
    state_revision = occurrence.get("state_revision")
    if type(state_revision) is not int or not 0 <= state_revision <= _MAX_INT64:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return ArchiveOccurrence(
        project_id=project_id,
        sprint_id=sprint_id,
        response_id=response_id,
        response_kind=response_kind,
        occurrence_kind=occurrence_kind,
        occurrence_id=occurrence_id,
        transition_id=transition_id,
        source=source,
        authorization=authorization,
        created_at=occurrence["created_at"],
        verified_at=occurrence["verified_at"],
        state_revision=state_revision,
    )


def _validate_locator(value: Any) -> tuple[dict[str, Any], bytes, str]:
    locator = _canonical_mapping(value, maximum=_LOCATOR_MAX_BYTES)
    if (
        set(locator) != _LOCATOR_KEYS
        or locator.get("locator_kind") != "runtime_occurrence"
        or locator.get("locator_version")
        != "prompt-archive-runtime-occurrence-locator-v1"
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    _validate_safe_id(locator.get("runtime_instance_id"))
    _validate_safe_id(locator.get("runtime_occurrence_id"))
    revision = locator.get("state_revision")
    if type(revision) is not int or not 0 <= revision <= _MAX_INT64:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    raw = canonical_json_bytes_v1(locator, maximum=_LOCATOR_MAX_BYTES)
    return locator, raw, hashlib.sha256(raw).hexdigest()


def _validate_occurrence_source(
    value: Any,
    *,
    maximum_source_bytes: int,
) -> PromptArchiveOccurrenceSource:
    if isinstance(value, PromptArchiveOccurrenceSource):
        source = dataclasses.asdict(value)
    else:
        source = _canonical_mapping(value, maximum=_SOURCE_DOCUMENT_MAX_BYTES)
    if set(source) != {"response_key", "byte_size", "sha256"}:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    response_key = source.get("response_key")
    byte_size = source.get("byte_size")
    digest = source.get("sha256")
    if (
        type(response_key) is not str
        or _RESPONSE_KEY_RE.fullmatch(response_key) is None
        or _SECRET_PREFIX_RE.match(response_key)
        or type(byte_size) is not int
        or not 0 <= byte_size <= maximum_source_bytes
        or type(digest) is not str
        or _SHA256_RE.fullmatch(digest) is None
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return PromptArchiveOccurrenceSource(response_key, byte_size, digest)


class PromptArchiveOccurrenceReceiptStore:
    """Immutable direct-addressed occurrence receipt store.

    ``receipt_root`` is an explicit absolute control root.  It is never
    persisted in a receipt.  ``source_reader`` is the only authority allowed
    to turn a logical response key into bytes; this store neither accepts nor
    returns a response path.
    """

    def __init__(
        self,
        receipt_root: str | os.PathLike[str],
        source_reader: PromptArchiveOccurrenceSourceReader,
        *,
        max_source_bytes: int = _DEFAULT_MAX_SOURCE_BYTES,
    ) -> None:
        if not isinstance(receipt_root, (str, os.PathLike)):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        root = Path(os.fspath(receipt_root))
        if not root.is_absolute() or not callable(source_reader):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if (
            type(max_source_bytes) is not int
            or not 1 <= max_source_bytes <= _DEFAULT_MAX_SOURCE_BYTES
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        self._root = Path(os.path.normpath(os.path.abspath(root)))
        self._validate_configured_root(self._root)
        self._source_reader = source_reader
        self._max_source_bytes = max_source_bytes
        # Create/traverse the configured root one component at a time from a
        # pinned filesystem anchor.  In particular, never ``mkdir`` an
        # absolute pathname and then accept ``Path.resolve()`` as the root:
        # an ancestor junction/symlink swap in that gap could silently move
        # the receipt authority outside the configured lexical root.
        self._root_identity = self._bootstrap_root(self._root)
        with self._open_pinned_directory(self._root):
            pass

    @property
    def max_source_bytes(self) -> int:
        return self._max_source_bytes

    def _assert_root(self) -> None:
        if self._directory_identity(self._root) != self._root_identity:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    @staticmethod
    def _validate_configured_root(path: Path) -> None:
        """Reject Windows aliases and ADS spellings before root creation."""

        if not path.is_absolute():
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if os.name != "nt":
            return
        anchor_parts = len(Path(path.anchor).parts)
        reserved = re.compile(
            r"^(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?$",
            re.IGNORECASE | re.ASCII,
        )
        for component in path.parts[anchor_parts:]:
            if (
                component in {"", ".", ".."}
                or ":" in component
                or component.endswith((" ", "."))
                or reserved.fullmatch(component) is not None
            ):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

    @staticmethod
    def _validate_relative_component(component: str) -> str:
        if (
            type(component) is not str
            or not component
            or len(component) > 200
            or component in {".", ".."}
            or any(character in component for character in (":", "/", "\\", "\0"))
            or component.endswith((" ", "."))
            or re.fullmatch(
                r"(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?",
                component,
                re.IGNORECASE | re.ASCII,
            )
            is not None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return component

    def _assert_within_root(self, path: Path) -> None:
        try:
            root_key = os.path.normcase(str(self._root.absolute()))
            path_key = os.path.normcase(str(path.absolute()))
            common = os.path.commonpath((root_key, path_key))
        except (OSError, ValueError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if common != root_key:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _validate_resolved_path(self, resolved: Path, intended: Path) -> None:
        """Bind an opened handle to the exact intended in-root path."""

        try:
            resolved_absolute = Path(os.path.abspath(resolved))
            intended_absolute = Path(os.path.abspath(intended))
            root_absolute = self._root.absolute()
            root_key = os.path.normcase(str(root_absolute))
            resolved_key = os.path.normcase(str(resolved_absolute))
            intended_key = os.path.normcase(str(intended_absolute))
            common = os.path.commonpath((root_key, resolved_key))
            intended_relative = intended_absolute.relative_to(root_absolute)
        except (OSError, ValueError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if common != root_key or resolved_key != intended_key:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if os.name == "nt" and intended_relative.parts:
            observed_tail = resolved_absolute.parts[-len(intended_relative.parts) :]
            if observed_tail != intended_relative.parts:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    @staticmethod
    def _directory_identity(path: Path) -> tuple[int, int]:
        try:
            metadata = path.lstat()
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if not stat.S_ISDIR(metadata.st_mode) or _is_redirected(path):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return metadata.st_dev, metadata.st_ino

    def _assert_directory_identity(
        self,
        path: Path,
        expected: tuple[int, int],
    ) -> None:
        if self._directory_identity(path) != expected:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    @staticmethod
    def _validate_bootstrap_resolved_path(resolved: Path, intended: Path) -> None:
        """Require a bootstrap handle to name the configured lexical path."""

        try:
            resolved_absolute = Path(os.path.abspath(resolved))
            intended_absolute = Path(os.path.abspath(intended))
            if os.path.normcase(str(resolved_absolute)) != os.path.normcase(
                str(intended_absolute)
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if os.name == "nt":
                anchor = Path(intended_absolute.anchor)
                intended_tail = intended_absolute.relative_to(anchor).parts
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
        """Validate a pinned root component without trusting later state."""

        resolved = (
            _windows_path_from_handle(pinned.descriptor)
            if os.name == "nt"
            else resolved_path_from_open_descriptor(pinned.descriptor)
        )
        # Bootstrap is an authority-establishing operation.  If the platform
        # cannot prove which lexical path the handle resolved to, fail closed.
        if resolved is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        cls._validate_bootstrap_resolved_path(resolved, intended)
        try:
            metadata = intended.lstat()
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if not stat.S_ISDIR(metadata.st_mode) or _is_redirected(intended):
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
    def _bootstrap_root(cls, root: Path) -> tuple[int, int]:
        """Create ``root`` by relative no-follow traversal from its anchor."""

        anchor = Path(root.anchor)
        try:
            components = root.relative_to(anchor).parts
        except ValueError:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None

        pins: list[_PinnedDirectory] = []
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
                # Windows aliases/ADS were rejected for the whole configured
                # root above.  These basic checks also keep POSIX traversal a
                # closed one-component operation without banning valid POSIX
                # names such as ``a:b``.
                if component in {"", ".", ".."} or any(
                    separator in component for separator in ("/", "\\", "\0")
                ):
                    raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
                intended = current_path / component
                created = False
                try:
                    if os.name == "nt":
                        try:
                            child_descriptor, _ = _windows_nt_open_relative(
                                current.descriptor,
                                component,
                                disposition=1,  # FILE_OPEN
                                directory=True,
                                writable=False,
                            )
                        except FileNotFoundError:
                            child_descriptor, created = _windows_nt_open_relative(
                                current.descriptor,
                                component,
                                disposition=3,  # FILE_OPEN_IF
                                directory=True,
                                writable=True,
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
                if created and os.name != "nt":
                    try:
                        os.fsync(current.descriptor)
                    except OSError:
                        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
                current = child
                current_path = intended

            # Keep every ancestor pinned until the complete chain has been
            # revalidated.  A POSIX rename does not invalidate an open fd, so
            # the resolved-path checks are repeated only after the final leaf
            # exists; Windows handles additionally deny delete/rename sharing.
            identities = [
                cls._validate_bootstrap_directory(pin, pin.path) for pin in pins
            ]
            return identities[-1]
        finally:
            for pin in reversed(pins):
                pin.close()

    def _validate_pinned_directory(
        self,
        pinned: _PinnedDirectory,
        intended: Path,
    ) -> None:
        resolved = (
            _windows_path_from_handle(pinned.descriptor)
            if os.name == "nt"
            else resolved_path_from_open_descriptor(pinned.descriptor)
        )
        if resolved is None:
            if os.name == "nt":
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        else:
            self._validate_resolved_path(resolved, intended)
        try:
            metadata = intended.lstat()
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if not stat.S_ISDIR(metadata.st_mode) or _is_redirected(intended):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._assert_within_root(intended)
        self._assert_root()

    def _open_pinned_directory(self, directory: Path) -> _PinnedDirectory:
        expected = self._directory_identity(directory)
        try:
            if os.name == "nt":
                descriptor = _windows_open_pinned_directory(directory)
            else:
                nofollow = getattr(os, "O_NOFOLLOW", 0)
                if not nofollow:
                    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                descriptor = os.open(
                    directory,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow,
                )
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        pinned = _PinnedDirectory(directory, descriptor)
        try:
            self._validate_pinned_directory(pinned, directory)
            self._assert_directory_identity(directory, expected)
            return pinned
        except BaseException:
            pinned.close()
            raise

    def _open_or_create_relative_directory(
        self,
        parent: _PinnedDirectory,
        component: str,
        intended: Path,
    ) -> _PinnedDirectory:
        self._validate_relative_component(component)
        try:
            if os.name == "nt":
                descriptor, _created = _windows_nt_open_relative(
                    parent.descriptor,
                    component,
                    disposition=3,
                    directory=True,
                    writable=True,
                )
            else:
                nofollow = getattr(os, "O_NOFOLLOW", 0)
                if not nofollow:
                    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                try:
                    os.mkdir(component, mode=0o700, dir_fd=parent.descriptor)
                except FileExistsError:
                    pass
                descriptor = os.open(
                    component,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow,
                    dir_fd=parent.descriptor,
                )
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        child = _PinnedDirectory(intended, descriptor)
        try:
            self._validate_pinned_directory(child, intended)
            self._validate_pinned_directory(parent, parent.path)
            return child
        except BaseException:
            child.close()
            raise

    def _ensure_relative_directory(
        self,
        components: tuple[str, ...],
    ) -> _PinnedDirectory:
        self._assert_root()
        current_path = self._root
        current = self._open_pinned_directory(self._root)
        try:
            for component in components:
                selected = self._validate_relative_component(component)
                child_path = current_path / selected
                child = self._open_or_create_relative_directory(
                    current,
                    selected,
                    child_path,
                )
                current.close()
                current = child
                current_path = child_path
            return current
        except BaseException:
            current.close()
            raise

    def _relative_components_for_parent(self, parent: Path) -> tuple[str, ...]:
        self._assert_within_root(parent)
        try:
            relative = parent.relative_to(self._root)
        except ValueError:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        return tuple(
            self._validate_relative_component(item) for item in relative.parts
        )

    @staticmethod
    def _sync_pinned_directory(directory: _PinnedDirectory) -> None:
        if os.name == "nt":
            return
        try:
            os.fsync(directory.descriptor)
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None

    def _derived_paths(
        self,
        project_id: str,
        locator_sha256: str,
    ) -> tuple[Path, Path]:
        project_digest = hashlib.sha256(project_id.encode("utf-8")).hexdigest()
        receipt_parent = self._root.joinpath(
            "receipts",
            "v1",
            project_digest[:2],
            project_digest,
            locator_sha256[:2],
        )
        lock_parent = self._root.joinpath(
            "locks",
            "v1",
            project_digest[:2],
            project_digest,
            locator_sha256[:2],
        )
        receipt_components = self._relative_components_for_parent(receipt_parent)
        lock_components = self._relative_components_for_parent(lock_parent)
        with self._ensure_relative_directory(receipt_components):
            pass
        with self._ensure_relative_directory(lock_components):
            pass
        self._assert_root()
        return (
            receipt_parent / f"{locator_sha256}.json",
            lock_parent / f"{locator_sha256}.lock",
        )

    @contextmanager
    def _locked(self, lock_path: Path):
        self._validate_relative_component(lock_path.name)
        parent_components = self._relative_components_for_parent(lock_path.parent)
        descriptor = -1
        with self._ensure_relative_directory(parent_components) as parent:
            try:
                if os.name == "nt":
                    descriptor, _created = _windows_nt_open_relative(
                        parent.descriptor,
                        lock_path.name,
                        disposition=3,
                        directory=False,
                        writable=True,
                    )
                else:
                    nofollow = getattr(os, "O_NOFOLLOW", 0)
                    if not nofollow:
                        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                    descriptor = os.open(
                        lock_path.name,
                        os.O_RDWR
                        | os.O_CREAT
                        | getattr(os, "O_BINARY", 0)
                        | nofollow,
                        0o600,
                        dir_fd=parent.descriptor,
                    )
                self._assert_open_file(
                    descriptor,
                    parent=parent,
                    name=lock_path.name,
                    intended=lock_path,
                    compare_size=False,
                )
                metadata = os.fstat(descriptor)
                if metadata.st_size not in {0, 1}:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                if metadata.st_size == 0:
                    os.write(descriptor, b"\0")
                    os.fsync(descriptor)
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_LOCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                try:
                    self._assert_open_file(
                        descriptor,
                        parent=parent,
                        name=lock_path.name,
                        intended=lock_path,
                    )
                    yield
                    self._assert_open_file(
                        descriptor,
                        parent=parent,
                        name=lock_path.name,
                        intended=lock_path,
                    )
                finally:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
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

    def _assert_open_file(
        self,
        descriptor: int,
        *,
        parent: _PinnedDirectory,
        name: str,
        intended: Path,
        compare_size: bool = True,
    ) -> os.stat_result:
        metadata = os.fstat(descriptor)
        _require_regular(metadata)
        self._validate_pinned_directory(parent, intended.parent)
        resolved = resolved_path_from_open_descriptor(descriptor)
        if resolved is None:
            if os.name == "nt":
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        else:
            self._validate_resolved_path(resolved, intended)
        try:
            if os.name == "nt":
                path_metadata = intended.lstat()
            else:
                path_metadata = os.stat(
                    name,
                    dir_fd=parent.descriptor,
                    follow_symlinks=False,
                )
        except FileNotFoundError:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        _require_regular(path_metadata)
        if (
            (path_metadata.st_dev, path_metadata.st_ino)
            != (metadata.st_dev, metadata.st_ino)
            or (compare_size and path_metadata.st_size != metadata.st_size)
            or _is_redirected(intended)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return metadata

    def _open_relative_file(
        self,
        parent: _PinnedDirectory,
        *,
        name: str,
        intended: Path,
    ) -> int | None:
        self._validate_relative_component(name)
        try:
            if os.name == "nt":
                descriptor, _created = _windows_nt_open_relative(
                    parent.descriptor,
                    name,
                    disposition=1,
                    directory=False,
                    writable=False,
                )
            else:
                nofollow = getattr(os, "O_NOFOLLOW", 0)
                if not nofollow:
                    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                descriptor = os.open(
                    name,
                    os.O_RDONLY | getattr(os, "O_BINARY", 0) | nofollow,
                    dir_fd=parent.descriptor,
                )
        except FileNotFoundError:
            return None
        except PromptArchiveStorageError:
            raise
        except OSError:
            if _is_redirected(intended):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        try:
            self._assert_open_file(
                descriptor,
                parent=parent,
                name=name,
                intended=intended,
            )
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _read_file(self, path: Path) -> bytes | None:
        parent_components = self._relative_components_for_parent(path.parent)
        with self._ensure_relative_directory(parent_components) as parent:
            descriptor = self._open_relative_file(
                parent,
                name=path.name,
                intended=path,
            )
            if descriptor is None:
                return None
            try:
                metadata = self._assert_open_file(
                    descriptor,
                    parent=parent,
                    name=path.name,
                    intended=path,
                )
                if metadata.st_size > _RECEIPT_MAX_BYTES:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                output = bytearray()
                while len(output) <= _RECEIPT_MAX_BYTES:
                    chunk = os.read(
                        descriptor,
                        min(64 * 1024, _RECEIPT_MAX_BYTES + 1 - len(output)),
                    )
                    if not chunk:
                        break
                    output.extend(chunk)
                if len(output) > _RECEIPT_MAX_BYTES:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                after = self._assert_open_file(
                    descriptor,
                    parent=parent,
                    name=path.name,
                    intended=path,
                )
                if (
                    (after.st_dev, after.st_ino) != (metadata.st_dev, metadata.st_ino)
                    or after.st_size != metadata.st_size
                    or after.st_size != len(output)
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                return bytes(output)
            except PromptArchiveStorageError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            finally:
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def _create_relative_temp(
        self,
        parent: _PinnedDirectory,
        name: str,
        intended: Path,
    ) -> int:
        self._validate_relative_component(name)
        descriptor = -1
        try:
            if os.name == "nt":
                descriptor, _created = _windows_nt_open_relative(
                    parent.descriptor,
                    name,
                    disposition=2,
                    directory=False,
                    writable=True,
                    # Native rename-by-handle requires delete sharing on the
                    # source temp. The immutable target and lock never use it.
                    share_delete=True,
                    delete_access=True,
                )
            else:
                nofollow = getattr(os, "O_NOFOLLOW", 0)
                if not nofollow:
                    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                descriptor = os.open(
                    name,
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_BINARY", 0)
                    | nofollow,
                    0o600,
                    dir_fd=parent.descriptor,
                )
            self._assert_open_file(
                descriptor,
                parent=parent,
                name=name,
                intended=intended,
            )
            return descriptor
        except PromptArchiveStorageError:
            if descriptor != -1:
                os.close(descriptor)
            raise
        except OSError:
            if descriptor != -1:
                os.close(descriptor)
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None

    @staticmethod
    def _delete_relative_temp(
        parent: _PinnedDirectory,
        name: str,
        descriptor: int,
    ) -> None:
        try:
            if os.name == "nt":
                _windows_mark_file_for_delete(descriptor)
            else:
                os.unlink(name, dir_fd=parent.descriptor)
        except (FileNotFoundError, PromptArchiveStorageError, OSError):
            pass

    def _atomic_publish_no_replace(
        self,
        descriptor: int,
        parent: _PinnedDirectory,
        temporary_name: str,
        target_name: str,
    ) -> bool:
        return _atomic_rename_pinned_no_replace(
            descriptor,
            parent,
            temporary_name,
            target_name,
        )

    def _publish(self, target: Path, raw: bytes) -> None:
        parent_components = self._relative_components_for_parent(target.parent)
        temporary_name = f".{target.stem}.{uuid4().hex}.tmp"
        temporary_path = target.parent / temporary_name
        descriptor = -1
        published = False
        with self._ensure_relative_directory(parent_components) as parent:
            try:
                descriptor = self._create_relative_temp(
                    parent,
                    temporary_name,
                    temporary_path,
                )
                with os.fdopen(descriptor, "wb", closefd=False) as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.lseek(descriptor, 0, os.SEEK_SET)
                observed = os.read(descriptor, len(raw) + 1)
                if observed != raw:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                self._assert_open_file(
                    descriptor,
                    parent=parent,
                    name=temporary_name,
                    intended=temporary_path,
                )
                created = self._atomic_publish_no_replace(
                    descriptor,
                    parent,
                    temporary_name,
                    target.name,
                )
                if not created:
                    raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                published = True
                self._assert_open_file(
                    descriptor,
                    parent=parent,
                    name=target.name,
                    intended=target,
                )
                self._sync_pinned_directory(parent)
                os.close(descriptor)
                descriptor = -1
                if self._read_file(target) != raw:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            except PromptArchiveStorageError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            finally:
                if descriptor != -1:
                    if not published:
                        self._delete_relative_temp(
                            parent,
                            temporary_name,
                            descriptor,
                        )
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass

    @staticmethod
    def _occurrence_document(occurrence: ArchiveOccurrence) -> dict[str, Any]:
        return {
            "project_id": occurrence.project_id,
            "sprint_id": occurrence.sprint_id,
            "response_id": occurrence.response_id,
            "response_kind": occurrence.response_kind,
            "occurrence_kind": occurrence.occurrence_kind,
            "occurrence_id": occurrence.occurrence_id,
            "transition_id": occurrence.transition_id,
            "source": dict(occurrence.source),
            "authorization": dict(occurrence.authorization),
            "created_at": occurrence.created_at,
            "verified_at": occurrence.verified_at,
            "state_revision": occurrence.state_revision,
        }

    @staticmethod
    def _source_document(
        source: PromptArchiveOccurrenceSource,
    ) -> dict[str, Any]:
        return {
            "response_key": source.response_key,
            "byte_size": source.byte_size,
            "sha256": source.sha256,
        }

    def _receipt_document(
        self,
        *,
        project_id: str,
        locator: dict[str, Any],
        locator_sha256: str,
        occurrence: ArchiveOccurrence,
        source: PromptArchiveOccurrenceSource,
    ) -> dict[str, Any]:
        core = {
            "schema_version": 1,
            "kind": "prompt_archive_runtime_occurrence_receipt",
            "media_type": (
                "application/vnd.nginx-qa.prompt-archive-runtime-"
                "occurrence-receipt-v1+json"
            ),
            "hash_algorithm": "sha256-canonical-json-v1",
            "project_id": project_id,
            "locator_sha256": locator_sha256,
            "locator": locator,
            "occurrence": self._occurrence_document(occurrence),
            "source": self._source_document(source),
        }
        core_raw = canonical_json_bytes_v1(core, maximum=_RECEIPT_MAX_BYTES)
        return {
            **core,
            "receipt_sha256": hashlib.sha256(core_raw).hexdigest(),
        }

    def _decode_receipt(
        self,
        raw: bytes,
        *,
        project_id: str,
        locator: dict[str, Any],
        locator_sha256: str,
    ) -> tuple[ArchiveOccurrence, PromptArchiveOccurrenceSource]:
        try:
            receipt = parse_canonical_json_v1(raw, maximum=_RECEIPT_MAX_BYTES)
            if (
                set(receipt) != _RECEIPT_KEYS
                or receipt.get("schema_version") != 1
                or receipt.get("kind")
                != "prompt_archive_runtime_occurrence_receipt"
                or receipt.get("media_type")
                != (
                    "application/vnd.nginx-qa.prompt-archive-runtime-"
                    "occurrence-receipt-v1+json"
                )
                or receipt.get("hash_algorithm") != "sha256-canonical-json-v1"
                or receipt.get("project_id") != project_id
                or receipt.get("locator_sha256") != locator_sha256
                or receipt.get("locator") != locator
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            core = dict(receipt)
            observed_receipt_sha256 = core.pop("receipt_sha256", None)
            expected_receipt_sha256 = hashlib.sha256(
                canonical_json_bytes_v1(core, maximum=_RECEIPT_MAX_BYTES)
            ).hexdigest()
            if (
                type(observed_receipt_sha256) is not str
                or not hmac.compare_digest(
                    observed_receipt_sha256,
                    expected_receipt_sha256,
                )
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            occurrence = _validate_occurrence(
                receipt.get("occurrence"),
                project_id=project_id,
            )
            source = _validate_occurrence_source(
                receipt.get("source"),
                maximum_source_bytes=self._max_source_bytes,
            )
            if (
                locator["runtime_occurrence_id"] != occurrence.occurrence_id
                or locator["state_revision"] != occurrence.state_revision
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            return (
                dataclasses.replace(
                    occurrence,
                    source_response_key=source.response_key,
                    source_byte_size=source.byte_size,
                    source_sha256=source.sha256,
                ),
                source,
            )
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_UNAVAILABLE":
                raise
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") from None
        except (KeyError, TypeError, ValueError, OverflowError):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") from None

    def register_occurrence(
        self,
        *,
        project_id: str,
        locator: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        source: PromptArchiveOccurrenceSource,
    ) -> None:
        """Durably create or byte-verify one immutable occurrence receipt."""

        selected_project = _validate_safe_id(project_id)
        frozen_locator, _locator_raw, locator_digest = _validate_locator(locator)
        frozen_occurrence = _validate_occurrence(
            occurrence,
            project_id=selected_project,
        )
        frozen_source = _validate_occurrence_source(
            source,
            maximum_source_bytes=self._max_source_bytes,
        )
        if (
            occurrence.source_response_key,
            occurrence.source_byte_size,
            occurrence.source_sha256,
        ) != (
            frozen_source.response_key,
            frozen_source.byte_size,
            frozen_source.sha256,
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        frozen_occurrence = dataclasses.replace(
            frozen_occurrence,
            source_response_key=frozen_source.response_key,
            source_byte_size=frozen_source.byte_size,
            source_sha256=frozen_source.sha256,
        )
        if (
            frozen_locator["runtime_occurrence_id"]
            != frozen_occurrence.occurrence_id
            or frozen_locator["state_revision"]
            != frozen_occurrence.state_revision
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        receipt = self._receipt_document(
            project_id=selected_project,
            locator=frozen_locator,
            locator_sha256=locator_digest,
            occurrence=frozen_occurrence,
            source=frozen_source,
        )
        expected = canonical_json_bytes_v1(receipt, maximum=_RECEIPT_MAX_BYTES)
        target, lock_path = self._derived_paths(selected_project, locator_digest)
        with self._locked(lock_path):
            self._assert_root()
            observed = self._read_file(target)
            if observed is None:
                self._publish(target, expected)
                return
            self._decode_receipt(
                observed,
                project_id=selected_project,
                locator=frozen_locator,
                locator_sha256=locator_digest,
            )
            if observed != expected:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")

    def load_occurrence(
        self,
        *,
        project_id: str,
        locator: Mapping[str, Any],
    ) -> PromptArchiveOccurrenceProof | None:
        """Load one exact receipt and verify its committed legacy source."""

        selected_project = _validate_safe_id(project_id)
        frozen_locator, _locator_raw, locator_digest = _validate_locator(locator)
        target, _lock_path = self._derived_paths(selected_project, locator_digest)
        observed = self._read_file(target)
        if observed is None:
            return None
        occurrence, source = self._decode_receipt(
            observed,
            project_id=selected_project,
            locator=frozen_locator,
            locator_sha256=locator_digest,
        )
        source_changed = False
        try:
            source_bytes = self._source_reader(
                project_id=selected_project,
                response_key=source.response_key,
                response_kind=occurrence.response_kind,
                maximum_bytes=self._max_source_bytes,
            )
        except FileNotFoundError:
            source_bytes = None
        except PromptArchiveStorageError as exc:
            source_bytes = None
            if exc.code in {
                "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
                "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
                "PROMPT_ARCHIVE_REFERENCE_INVALID",
            }:
                source_changed = True
            elif exc.code not in {
                "PROMPT_ARCHIVE_NOT_FOUND",
                "PROMPT_ARCHIVE_UNAVAILABLE",
            }:
                raise
        except (OSError, UnicodeError):
            source_bytes = None
        if source_changed:
            status: Literal["verified", "source_unavailable", "source_changed"] = (
                "source_changed"
            )
        elif source_bytes is None:
            status: Literal["verified", "source_unavailable", "source_changed"] = (
                "source_unavailable"
            )
        elif not isinstance(source_bytes, (bytes, bytearray, memoryview)):
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        else:
            try:
                view = memoryview(source_bytes)
                try:
                    matches = (
                        view.nbytes == source.byte_size
                        and view.nbytes <= self._max_source_bytes
                        and hashlib.sha256(view).hexdigest() == source.sha256
                    )
                finally:
                    view.release()
            except (BufferError, TypeError, ValueError):
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            status = "verified" if matches else "source_changed"
        return PromptArchiveOccurrenceProof(status, occurrence, source)


__all__ = [
    "PromptArchiveOccurrenceProof",
    "PromptArchiveOccurrenceReceiptStore",
    "PromptArchiveOccurrenceSource",
    "PromptArchiveOccurrenceSourceReader",
]
