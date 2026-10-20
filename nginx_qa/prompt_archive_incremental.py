"""Incremental, exact-byte Prompt Archive V1 graph primitives.

This module is deliberately independent from the runtime response writer.  It
builds and verifies the immutable record, recursive JSON, byte-stream, and
persistent-index layer accepted by PSC-001 on top of ``PromptArchiveStorage``.
Runtime publication, mutable latest pointers, migration, and garbage
collection belong to later layers.
"""

from __future__ import annotations

import ctypes
import base64
import dataclasses
import hmac
import hashlib
import os
import re
import stat
import struct
import threading
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from .prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
    parse_canonical_json_v1,
    _error,
)
from .prompt_archive_json_spans import (
    ByteSpan,
    JsonEdge,
    JsonNode,
    JsonSpanParseError,
    parse_json_bytes,
)
from .prompt_archive_source_metadata import (
    PromptArchiveSourceMetadataError,
    validate_source_metadata_projection,
)


_MIB: Final = 1024 * 1024
_CHUNK_BYTES: Final = 8 * _MIB
_FANOUT: Final = 64
_MAX_INT64: Final = 9_223_372_036_854_775_807
_MAX_MANIFEST_BYTES: Final = 256 * 1024
_MAX_REFERENCED_STORED_BYTES: Final = 384 * _MIB
_MAX_REFERENCED_UNCOMPRESSED_BYTES: Final = 384 * _MIB
_ROOT_ORDER: Final = (
    "raw_bytes",
    "current_fields",
    "recent_activity",
    "history_text",
    "history_timeline",
    "activity_with_patches",
    "history_patches",
    "code_patches",
    "work_history",
    "review_context",
    "results",
    "feedback",
    "execution_audit",
    "source_metadata",
)
_CONTROL_MEDIA: Final = {
    "index_node": "application/vnd.nginx-qa.prompt-archive-index-node-v1+json",
    "json_value": "application/vnd.nginx-qa.prompt-archive-json-value-v1+json",
    "json_edge": "application/vnd.nginx-qa.prompt-archive-json-edge-v1+json",
    "manifest": "application/vnd.nginx-qa.prompt-archive-manifest-v1+json",
}
_BYTE_SEQUENCE_TYPES: Final = {
    "raw_bytes": ("raw_chunk", "application/json"),
    "payload_chunks": ("payload_chunk", "application/json"),
    "render_plan_chunks": (
        "render_plan_chunk",
        "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
    ),
    "literal_chunks": (
        "literal_chunk",
        "application/vnd.nginx-qa.prompt-archive-json-fragment-v1",
    ),
}
_PAYLOAD_MEDIA_TYPES: Final = frozenset(
    {
        "application/json",
        "application/vnd.nginx-qa.prompt-archive-source-metadata-v1+json",
        "application/vnd.nginx-qa.prompt-archive-json-string-fragment-v1",
    }
)
_RECORD_SEQUENCE_TYPES: Final = {
    "events": "event_record",
    "patches": "patch_record",
    "history_text": "history_text_record",
    "results": "result_record",
    "feedback": "feedback_record",
    "review_contexts": "review_context_record",
    "execution_audit": "execution_audit_record",
    "source_metadata": "source_metadata_record",
    "current_fields": "current_field_record",
}
_SEQUENCE_KINDS: Final = frozenset(_BYTE_SEQUENCE_TYPES) | frozenset(
    {"json_edges"}
) | frozenset(_RECORD_SEQUENCE_TYPES)
_RECORD_TYPES: Final = {
    "event": ("events", "event_record", "recursive_json_merkle_v1", "application/json"),
    "patch": ("patches", "patch_record", "recursive_json_merkle_v1", "application/json"),
    "history_text_fragment": (
        "history_text",
        "history_text_record",
        "opaque_chunks_v1",
        "application/vnd.nginx-qa.prompt-archive-json-string-fragment-v1",
    ),
    "result": ("results", "result_record", "recursive_json_merkle_v1", "application/json"),
    "feedback": ("feedback", "feedback_record", "recursive_json_merkle_v1", "application/json"),
    "review_context": (
        "review_contexts",
        "review_context_record",
        "recursive_json_merkle_v1",
        "application/json",
    ),
    "execution_audit": (
        "execution_audit",
        "execution_audit_record",
        "recursive_json_merkle_v1",
        "application/json",
    ),
    "source_metadata": (
        "source_metadata",
        "source_metadata_record",
        "recursive_json_merkle_v1",
        "application/vnd.nginx-qa.prompt-archive-source-metadata-v1+json",
    ),
    "current_field": (
        "current_fields",
        "current_field_record",
        "recursive_json_merkle_v1",
        "application/json",
    ),
}
_PROMOTED_SLOTS: Final = {
    ("project_state", "recent_activity"): ("recent_activity", "events", "event"),
    ("project_state", "history_with_patches", "text"): (
        "history_text",
        "history_text",
        "history_text_fragment",
    ),
    ("project_state", "history_with_patches", "timeline"): (
        "history_timeline",
        "events",
        "event",
    ),
    ("project_state", "activity_with_patches"): (
        "activity_with_patches",
        "events",
        "event",
    ),
    ("project_state", "history_with_patches", "patches"): (
        "history_patches",
        "patches",
        "patch",
    ),
    ("project_state", "code_patches"): ("code_patches", "patches", "patch"),
    ("work_history",): ("work_history", "events", "event"),
    ("assignment", "execution_audit", "checkpoints"): (
        "execution_audit",
        "execution_audit",
        "execution_audit",
    ),
}
_SLOT_IDS_BY_ROOT: Final = {
    "recent_activity": "project_state.recent_activity",
    "history_text": "project_state.history_with_patches.text",
    "history_timeline": "project_state.history_with_patches.timeline",
    "activity_with_patches": "project_state.activity_with_patches",
    "history_patches": "project_state.history_with_patches.patches",
    "code_patches": "project_state.code_patches",
    "work_history": "work_history",
    "execution_audit": "assignment.execution_audit.checkpoints",
}
_ROOT_SEQUENCE_KINDS: Final = {
    "current_fields": "current_fields",
    "recent_activity": "events",
    "history_text": "history_text",
    "history_timeline": "events",
    "activity_with_patches": "events",
    "history_patches": "patches",
    "code_patches": "patches",
    "work_history": "events",
    "review_context": "review_contexts",
    "results": "results",
    "feedback": "feedback",
    "execution_audit": "execution_audit",
    "source_metadata": "source_metadata",
}
_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$", re.ASCII)
_RESPONSE_ID_RE: Final = re.compile(r"^parresp1-[0-9a-f]{64}$", re.ASCII)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_RFC3339_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$",
    re.ASCII,
)
_SOURCE_METADATA_OPTIONAL_FIELDS: Final = frozenset(
    {
        "conversation_id",
        "thread_id",
        "message_id",
        "sender_label",
        "title",
        "observed_at",
    }
)
_SOURCE_METADATA_RECEIPT_KEYS: Final = frozenset(
    {
        "schema_version",
        "kind",
        "projection_version",
        "redaction_version",
        "projection_sha256",
        "projection_byte_size",
        "source_type",
        "discarded_field_count",
        "projected_fields",
    }
)


@dataclasses.dataclass(frozen=True, slots=True)
class SequenceLeaf:
    ref: Mapping[str, Any]
    logical_bytes: int


@dataclasses.dataclass(frozen=True, slots=True)
class SequenceReceipt:
    root_ref: Mapping[str, Any]
    sequence_kind: str
    level: int
    item_count: int
    logical_bytes: int


@dataclasses.dataclass(frozen=True, slots=True)
class ByteStreamReceipt:
    sequence: SequenceReceipt
    byte_size: int
    sha256: str
    chunk_count: int


@dataclasses.dataclass(frozen=True, slots=True)
class JsonValueReceipt:
    ref: Mapping[str, Any]
    byte_size: int
    sha256: str
    value_nodes: int
    edges: int
    literal_chunks: int
    index_nodes: int


@dataclasses.dataclass(frozen=True, slots=True)
class RecordReceipt:
    ref: Mapping[str, Any]
    record_id: str
    record_type: str
    sequence_kind: str
    ordinal: int
    payload_size: int
    payload_sha256: str
    payload_root_digest: str


@dataclasses.dataclass(frozen=True, slots=True)
class ArchiveOccurrence:
    project_id: str
    sprint_id: str | None
    response_id: str
    response_kind: str
    occurrence_kind: str
    occurrence_id: str
    transition_id: str | None
    source: Mapping[str, Any]
    authorization: Mapping[str, Any]
    created_at: str
    verified_at: str
    # Durable legacy assignment revision used only by the closed recovery
    # locator. It is deliberately not part of the manifest transport shape.
    state_revision: int = 0
    # Transport-only identity of the exact committed legacy response.  These
    # fields are registered with the closed runtime-occurrence prover and are
    # deliberately absent from the immutable archive manifest.
    source_response_key: str | None = None
    source_byte_size: int | None = None
    source_sha256: str | None = None
    # A runtime occurrence may carry one already-screened, canonical source
    # projection and its closed receipt.  Both are immutable pre-plan input;
    # raw source metadata and the ephemeral secret-screen snapshot never enter
    # this type or the CAS graph.
    source_metadata_projection: Mapping[str, Any] | None = None
    source_metadata_receipt: Mapping[str, Any] | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class BuiltPromptArchive:
    archive_id: str
    archive_format: str
    manifest: Mapping[str, Any]
    manifest_ref: Mapping[str, Any]
    roots: Mapping[str, Mapping[str, Any]]
    hydrated_size: int
    hydrated_sha256: str


@dataclasses.dataclass(frozen=True, slots=True)
class StorageInventory:
    file_count: int
    directory_count: int
    content_bytes: int
    allocated_bytes: int

    def delta(self, previous: "StorageInventory") -> "StorageInventory":
        return StorageInventory(
            self.file_count - previous.file_count,
            self.directory_count - previous.directory_count,
            self.content_bytes - previous.content_bytes,
            self.allocated_bytes - previous.allocated_bytes,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class WorkMetrics:
    cas_put_attempts: int
    cas_read_attempts: int
    control_bytes_serialized: int
    index_nodes_read: int
    index_nodes_serialized: int
    new_object_count: int
    new_stored_bytes: int
    new_uncompressed_bytes: int

    def delta(self, previous: "WorkMetrics") -> "WorkMetrics":
        return WorkMetrics(
            self.cas_put_attempts - previous.cas_put_attempts,
            self.cas_read_attempts - previous.cas_read_attempts,
            self.control_bytes_serialized - previous.control_bytes_serialized,
            self.index_nodes_read - previous.index_nodes_read,
            self.index_nodes_serialized - previous.index_nodes_serialized,
            self.new_object_count - previous.new_object_count,
            self.new_stored_bytes - previous.new_stored_bytes,
            self.new_uncompressed_bytes - previous.new_uncompressed_bytes,
        )


def _require_exact_int(value: Any, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum or value > _MAX_INT64:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _require_exact_string(value: Any) -> str:
    if type(value) is not str:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _require_rfc3339(value: Any) -> str:
    text = _require_exact_string(value)
    if _RFC3339_RE.fullmatch(text) is None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return text


def _copy_ref(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    copied = dict(value)
    if any(type(key) is not str for key in copied):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return copied


def _register_digest_role(
    digest_roles: dict[str, tuple[str, str]],
    ref: Mapping[str, Any],
) -> tuple[str, str, str]:
    """Bind one physical digest to exactly one role in an archive closure.

    The storage layer intentionally permits identical bytes to be addressed by
    multiple typed references.  A single archive graph does not: manifest
    counts are physical-digest counts, so admitting two logical roles for one
    digest would make those counts disagree with the typed publication
    closure.  Keep this invariant at graph traversal boundaries rather than
    weakening the storage-wide CAS contract.
    """

    copied = _copy_ref(ref)
    digest = _require_exact_string(copied.get("digest"))
    logical_kind = _require_exact_string(copied.get("logical_kind"))
    media_type = _require_exact_string(copied.get("media_type"))
    if _SHA256_RE.fullmatch(digest) is None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    role = (logical_kind, media_type)
    previous = digest_roles.get(digest)
    if previous is not None and previous != role:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    digest_roles[digest] = role
    return digest, logical_kind, media_type


def _pas1_frame(value: str | None) -> bytes:
    if value is None:
        return b"\xff"
    if type(value) is not str:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    return b"\x00" + struct.pack(">Q", len(encoded)) + encoded


def pas1_id_v1(domain: str, fields: Sequence[tuple[str, str | None]]) -> str:
    """Return a lowercase PAS1-ID-v1 digest for one frozen field sequence."""

    if type(domain) is not str or not domain or len(domain) > 160:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if isinstance(fields, (str, bytes)) or not isinstance(fields, Sequence):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if len(fields) > 64:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    preimage = b"nginx-qa-pas1-id-v1\x00" + _pas1_frame(domain)
    for field in fields:
        if (
            not isinstance(field, tuple)
            or len(field) != 2
            or type(field[0]) is not str
            or not field[0]
            or len(field[0]) > 160
            or (field[1] is not None and type(field[1]) is not str)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        name, value = field
        preimage += _pas1_frame(name) + _pas1_frame(value)
    return hashlib.sha256(preimage).hexdigest()


def sprint_storage_key_v1(project_id: str, sprint_id: str | None) -> str:
    """Return the path-safe PAS1 sprint bucket key.

    A null sprint is retained for the already-frozen offline migration bucket;
    runtime publication requires a non-null sprint separately.
    """

    if type(project_id) is not str or _SAFE_ID_RE.fullmatch(project_id) is None:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    if sprint_id is not None and (
        type(sprint_id) is not str or _SAFE_ID_RE.fullmatch(sprint_id) is None
    ):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    return pas1_id_v1(
        "sprint-storage-key",
        (("project_id", project_id), ("sprint_id", sprint_id)),
    )


def response_id_v1(
    project_id: str,
    sprint_id: str | None,
    response_kind: str,
    occurrence_kind: str,
    occurrence_id: str,
) -> str:
    """Return the deterministic PAS1 response identity for one occurrence."""

    return "parresp1-" + pas1_id_v1(
        "response-id",
        (
            ("project_id", project_id),
            ("sprint_id", sprint_id),
            ("response_kind", response_kind),
            ("occurrence_kind", occurrence_kind),
            ("occurrence_id", occurrence_id),
        ),
    )


def _load_canonical_control(raw: bytes) -> dict[str, Any]:
    return parse_canonical_json_v1(raw, maximum=max(1, len(raw)))


def _allocated_size(path: Path, metadata: os.stat_result) -> int:
    blocks = getattr(metadata, "st_blocks", None)
    if type(blocks) is int:
        return blocks * 512
    if os.name != "nt":
        return metadata.st_size
    from ctypes import wintypes

    high = wintypes.DWORD()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_size = kernel32.GetCompressedFileSizeW
    get_size.argtypes = (wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD))
    get_size.restype = wintypes.DWORD
    low = get_size(str(path), ctypes.byref(high))
    if low == 0xFFFFFFFF and ctypes.get_last_error() != 0:
        return metadata.st_size
    return (int(high.value) << 32) | int(low)


class PromptArchiveIncremental:
    """Build and verify immutable PAS1 incremental graphs."""

    def __init__(
        self,
        storage: PromptArchiveStorage,
        project_id: str,
        sprint_id: str | None,
        *,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> None:
        if not isinstance(storage, PromptArchiveStorage):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if type(project_id) is not str or _SAFE_ID_RE.fullmatch(project_id) is None:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if sprint_id is not None and (
            type(sprint_id) is not str or _SAFE_ID_RE.fullmatch(sprint_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if getattr(storage, "project_id", None) != project_id:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if cancellation is not None and not isinstance(
            cancellation, PromptArchiveCancellationToken
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        self.storage = storage
        self.project_id = project_id
        self.sprint_id = sprint_id
        self._cancellation = cancellation
        self._cas_put_attempts = 0
        self._cas_read_attempts = 0
        self._control_bytes_serialized = 0
        self._index_nodes_read = 0
        self._index_nodes_serialized = 0
        self._new_digests: set[str] = set()
        self._new_stored_bytes = 0
        self._new_uncompressed_bytes = 0
        # Public operations on one archive instance are synchronous and
        # serialized.  This prevents overlapping callers from observing the
        # mutable traversal budget/cache owned by another operation while
        # still allowing nested helper calls in the same thread.
        self._operation_lock = threading.RLock()
        self._active_reference_digests: set[str] | None = None
        self._active_read_cache: dict[
            str, tuple[dict[str, Any], bytes]
        ] | None = None

    def _check_cancelled(self) -> None:
        if self._cancellation is not None:
            self._cancellation.check()

    @contextmanager
    def _reference_budget(self):
        with self._operation_lock:
            self._check_cancelled()
            owner = self._active_reference_digests is None
            if owner:
                self._active_reference_digests = set()
                self._active_read_cache = {}
            try:
                yield
            finally:
                if owner:
                    self._active_reference_digests = None
                    self._active_read_cache = None

    def _track_reference(self, ref: Mapping[str, Any]) -> None:
        self._check_cancelled()
        if self._active_reference_digests is None:
            return
        digest = _require_exact_string(_copy_ref(ref).get("digest"))
        self._active_reference_digests.add(digest)
        if len(self._active_reference_digests) > self.storage.limits.max_referenced_objects:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def _start_fresh_read_epoch(self) -> None:
        """Discard traversal-local reads before a final integrity pass.

        A cache epoch may span the planning reads within one public operation,
        but it must never let those reads stand in for the final complete graph
        verification performed immediately before publication or return.
        """
        self._check_cancelled()
        if self._active_read_cache is not None:
            self._active_read_cache = {}

    def work_metrics(self) -> WorkMetrics:
        with self._operation_lock:
            return WorkMetrics(
                self._cas_put_attempts,
                self._cas_read_attempts,
                self._control_bytes_serialized,
                self._index_nodes_read,
                self._index_nodes_serialized,
                len(self._new_digests),
                self._new_stored_bytes,
                self._new_uncompressed_bytes,
            )

    def storage_inventory(self) -> StorageInventory:
        with self._operation_lock:
            return self._storage_inventory()

    def _storage_inventory(self) -> StorageInventory:
        files = directories = content = allocated = 0
        project_root = self.storage._project_root
        try:
            self.storage._assert_root_identity()
            if not project_root.exists():
                return StorageInventory(0, 0, 0, 0)
            self.storage._assert_existing_chain_safe(project_root)
            for path in project_root.rglob("*"):
                self.storage._assert_within_root(path)
                self.storage._assert_existing_chain_safe(path)
                metadata = path.lstat()
                if stat.S_ISLNK(metadata.st_mode):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                if stat.S_ISDIR(metadata.st_mode):
                    directories += 1
                elif stat.S_ISREG(metadata.st_mode):
                    files += 1
                    content += metadata.st_size
                    allocated += _allocated_size(path, metadata)
            self.storage._assert_root_identity()
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        return StorageInventory(files, directories, content, allocated)

    def _put_blob(
        self,
        raw: bytes,
        *,
        logical_kind: str,
        media_type: str,
    ) -> dict[str, Any]:
        self._cas_put_attempts += 1
        ref, created = self.storage.put_blob_with_outcome(
            raw,
            logical_kind=logical_kind,
            media_type=media_type,
        )
        self._track_reference(ref)
        digest = ref["digest"]
        if created and digest not in self._new_digests:
            self._new_digests.add(digest)
            self._new_stored_bytes += ref["stored_size"]
            self._new_uncompressed_bytes += ref["uncompressed_size"]
        return ref

    def _put_control(self, document: Mapping[str, Any], logical_kind: str) -> dict[str, Any]:
        maximum = self.storage.limits.max_blob_uncompressed_bytes
        raw = canonical_json_bytes_v1(dict(document), maximum=maximum)
        self._control_bytes_serialized += len(raw)
        if logical_kind == "index_node":
            self._index_nodes_serialized += 1
        return self._put_blob(
            raw,
            logical_kind=logical_kind,
            media_type=_CONTROL_MEDIA[logical_kind],
        )

    def _read_blob(
        self,
        ref: Mapping[str, Any],
        *,
        logical_kind: str,
        media_type: str,
    ) -> bytes:
        self._check_cancelled()
        validated = _copy_ref(ref)
        if (
            validated.get("logical_kind") != logical_kind
            or validated.get("media_type") != media_type
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._track_reference(validated)
        digest = _require_exact_string(validated.get("digest"))
        physical = {
            key: value
            for key, value in validated.items()
            if key not in {"logical_kind", "media_type"}
        }
        if self._active_read_cache is not None and digest in self._active_read_cache:
            cached_physical, cached_raw = self._active_read_cache[digest]
            if cached_physical != physical:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            return cached_raw
        self._cas_read_attempts += 1
        if self._cancellation is None:
            raw = self.storage.read_blob(validated)
        else:
            raw = self.storage.read_blob(
                validated,
                cancellation=self._cancellation,
            )
        if self._active_read_cache is not None:
            self._active_read_cache[digest] = (physical, raw)
        return raw

    def _read_control(
        self,
        ref: Mapping[str, Any],
        *,
        logical_kind: str,
    ) -> dict[str, Any]:
        raw = self._read_blob(
            ref,
            logical_kind=logical_kind,
            media_type=_CONTROL_MEDIA[logical_kind],
        )
        if logical_kind == "index_node":
            self._index_nodes_read += 1
        return _load_canonical_control(raw)

    @staticmethod
    def _leaf_kind(sequence_kind: str) -> tuple[str, str]:
        if sequence_kind in _BYTE_SEQUENCE_TYPES:
            return _BYTE_SEQUENCE_TYPES[sequence_kind]
        if sequence_kind == "json_edges":
            return "json_edge", _CONTROL_MEDIA["json_edge"]
        if sequence_kind in _RECORD_SEQUENCE_TYPES:
            return (
                _RECORD_SEQUENCE_TYPES[sequence_kind],
                "application/vnd.nginx-qa.prompt-archive-record-v1+json",
            )
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    @staticmethod
    def _index_object_count(item_count: int) -> int:
        width = item_count
        result = 0
        while width:
            width = (width + _FANOUT - 1) // _FANOUT
            result += width
            if width == 1:
                return result
        return 0

    def _ensure_sequence_budget(self, item_count: int, logical_bytes: int) -> None:
        count = _require_exact_int(item_count, minimum=1)
        size = _require_exact_int(logical_bytes)
        if size > self.storage.limits.max_hydrated_bytes:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        references = count + self._index_object_count(count)
        if references > self.storage.limits.max_referenced_objects:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        level = 0
        width = count
        while width > _FANOUT:
            width = (width + _FANOUT - 1) // _FANOUT
            level += 1
        if level + 1 > self.storage.limits.max_reference_depth:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    @staticmethod
    def _sequence_level(item_count: int) -> int:
        level = 0
        width = item_count
        while width > _FANOUT:
            width = (width + _FANOUT - 1) // _FANOUT
            level += 1
        return level

    @staticmethod
    def _record_type_for_sequence(sequence_kind: str) -> str | None:
        return next(
            (
                record_type
                for record_type, definition in _RECORD_TYPES.items()
                if definition[0] == sequence_kind
            ),
            None,
        )

    def _validate_leaf_ref(
        self,
        sequence_kind: str,
        ref: Mapping[str, Any],
        *,
        logical_bytes: int | None = None,
        ordinal: int | None = None,
    ) -> dict[str, Any]:
        expected_kind, expected_media = self._leaf_kind(sequence_kind)
        value = _copy_ref(ref)
        if (
            value.get("logical_kind") != expected_kind
            or (
                sequence_kind == "payload_chunks"
                and value.get("media_type") not in _PAYLOAD_MEDIA_TYPES
            )
            or (
                sequence_kind != "payload_chunks"
                and value.get("media_type") != expected_media
            )
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if sequence_kind == "payload_chunks":
            expected_media = value["media_type"]
        raw = self._read_blob(
            value,
            logical_kind=expected_kind,
            media_type=expected_media,
        )
        if sequence_kind in _BYTE_SEQUENCE_TYPES:
            actual_size = len(raw)
        else:
            document = _load_canonical_control(raw)
            if sequence_kind == "json_edges":
                if (
                    document.get("schema_version") != 1
                    or document.get("kind") != "prompt_archive_json_edge"
                    or document.get("media_type") != _CONTROL_MEDIA["json_edge"]
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                actual_size = _require_exact_int(document.get("byte_size"), minimum=1)
            else:
                expected_record_type = next(
                    (
                        record_type
                        for record_type, definition in _RECORD_TYPES.items()
                        if definition[0] == sequence_kind
                    ),
                    None,
                )
                document = self._validate_record_document(
                    document,
                    expected_record_type=expected_record_type,
                )
                content = self.hydrate_record(
                    value,
                    expected_record_type=expected_record_type,
                )
                actual_size = len(content)
                if ordinal is not None and document.get("ordinal") != ordinal:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if logical_bytes is not None and actual_size != logical_bytes:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return value

    def _index_document(
        self,
        sequence_kind: str,
        level: int,
        children: Sequence[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if sequence_kind not in _SEQUENCE_KINDS:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if type(level) is not int or not 0 <= level <= 32:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if not 1 <= len(children) <= _FANOUT:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        frozen_children = [dict(child) for child in children]
        first = _require_exact_int(frozen_children[0].get("ordinal_start"))
        next_ordinal = first
        item_count = logical_bytes = 0
        capacity = _FANOUT**level
        for index, child in enumerate(frozen_children):
            if set(child) != {"ordinal_start", "item_count", "logical_bytes", "ref"}:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            ordinal = _require_exact_int(child["ordinal_start"])
            count = _require_exact_int(child["item_count"], minimum=1)
            size = _require_exact_int(child["logical_bytes"])
            if ordinal != next_ordinal:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if level == 0:
                if count != 1:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                child["ref"] = self._validate_leaf_ref(
                    sequence_kind,
                    child["ref"],
                    logical_bytes=size,
                    ordinal=ordinal,
                )
            else:
                ref = _copy_ref(child["ref"])
                if (
                    ref.get("logical_kind") != "index_node"
                    or ref.get("media_type") != _CONTROL_MEDIA["index_node"]
                    or count > capacity
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                self.storage.blob_path(ref)
                if index < len(frozen_children) - 1 and count != capacity:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                child["ref"] = ref
            next_ordinal += count
            item_count += count
            logical_bytes += size
            if next_ordinal > _MAX_INT64 or logical_bytes > _MAX_INT64:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        document = {
            "schema_version": 1,
            "kind": "prompt_archive_index_node",
            "sequence_kind": sequence_kind,
            "level": level,
            "item_count": item_count,
            "logical_bytes": logical_bytes,
            "children": frozen_children,
        }
        return document, self._put_control(document, "index_node")

    def build_sequence(
        self,
        sequence_kind: str,
        leaves: Iterable[SequenceLeaf],
    ) -> SequenceReceipt | None:
        with self._reference_budget():
            return self._build_sequence(sequence_kind, leaves)

    def _build_sequence(
        self,
        sequence_kind: str,
        leaves: Iterable[SequenceLeaf],
    ) -> SequenceReceipt | None:
        if sequence_kind not in _SEQUENCE_KINDS:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        frozen: list[dict[str, Any]] = []
        next_ordinal = 0
        logical_total = 0
        for leaf in leaves:
            if not isinstance(leaf, SequenceLeaf):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            size = _require_exact_int(leaf.logical_bytes)
            logical_total += size
            if logical_total > self.storage.limits.max_hydrated_bytes:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            if next_ordinal + 1 + self._index_object_count(next_ordinal + 1) > (
                self.storage.limits.max_referenced_objects
            ):
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            frozen.append(
                {
                    "ordinal_start": next_ordinal,
                    "item_count": 1,
                    "logical_bytes": size,
                    "ref": self._validate_leaf_ref(
                        sequence_kind,
                        leaf.ref,
                        logical_bytes=size,
                        ordinal=next_ordinal,
                    ),
                }
            )
            next_ordinal += 1
            if next_ordinal > _MAX_INT64:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if not frozen:
            return None
        self._ensure_sequence_budget(len(frozen), logical_total)
        record_type = self._record_type_for_sequence(sequence_kind)
        if record_type is not None:
            # Native sequence roots are manifest refs at depth one.  A leaf
            # record is one edge below each index level, hence level + 2.
            leaf_depth = self._sequence_level(len(frozen)) + 2
            for child in frozen:
                self._hydrate_record(
                    child["ref"],
                    expected_record_type=record_type,
                    _base_reference_depth=leaf_depth,
                )
        level = 0
        current = frozen
        while True:
            parents: list[dict[str, Any]] = []
            for offset in range(0, len(current), _FANOUT):
                document, ref = self._index_document(
                    sequence_kind,
                    level,
                    current[offset : offset + _FANOUT],
                )
                parents.append(
                    {
                        "ordinal_start": document["children"][0]["ordinal_start"],
                        "item_count": document["item_count"],
                        "logical_bytes": document["logical_bytes"],
                        "ref": ref,
                    }
                )
            if len(parents) == 1:
                root = parents[0]
                receipt = SequenceReceipt(
                    root["ref"],
                    sequence_kind,
                    level,
                    root["item_count"],
                    root["logical_bytes"],
                )
                return receipt
            current = parents
            level += 1
            if level > 32:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def _load_index_document(
        self,
        ref: Mapping[str, Any],
        *,
        sequence_kind: str,
        expected_level: int | None = None,
    ) -> dict[str, Any]:
        document = self._read_control(ref, logical_kind="index_node")
        required = {
            "schema_version",
            "kind",
            "sequence_kind",
            "level",
            "item_count",
            "logical_bytes",
            "children",
        }
        if set(document) != required:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            document.get("schema_version") != 1
            or document.get("kind") != "prompt_archive_index_node"
            or document.get("sequence_kind") != sequence_kind
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        level = _require_exact_int(document.get("level"))
        if level > 32 or (expected_level is not None and level != expected_level):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        children = document.get("children")
        if type(children) is not list or not 1 <= len(children) <= _FANOUT:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        # Rebuild semantic totals and typed incoming edges without publishing.
        first = None
        next_ordinal = item_count = logical_bytes = 0
        capacity = _FANOUT**level
        for index, child in enumerate(children):
            if type(child) is not dict or set(child) != {
                "ordinal_start",
                "item_count",
                "logical_bytes",
                "ref",
            }:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            ordinal = _require_exact_int(child["ordinal_start"])
            count = _require_exact_int(child["item_count"], minimum=1)
            size = _require_exact_int(child["logical_bytes"])
            if first is None:
                first = ordinal
                next_ordinal = ordinal
            if ordinal != next_ordinal:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if level == 0:
                if count != 1:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                self._validate_leaf_ref(
                    sequence_kind,
                    child["ref"],
                    logical_bytes=size,
                    ordinal=ordinal,
                )
            else:
                child_ref = _copy_ref(child["ref"])
                if (
                    child_ref.get("logical_kind") != "index_node"
                    or child_ref.get("media_type") != _CONTROL_MEDIA["index_node"]
                    or count > capacity
                    or (index < len(children) - 1 and count != capacity)
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                self.storage.blob_path(child_ref)
            item_count += count
            logical_bytes += size
            next_ordinal += count
            if next_ordinal > _MAX_INT64 or logical_bytes > _MAX_INT64:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if (
            document.get("item_count") != item_count
            or document.get("logical_bytes") != logical_bytes
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return document

    def _new_index_path(
        self,
        sequence_kind: str,
        level: int,
        leaf: Mapping[str, Any],
    ) -> dict[str, Any]:
        child = dict(leaf)
        for child_level in range(level + 1):
            document, ref = self._index_document(sequence_kind, child_level, [child])
            child = {
                "ordinal_start": document["children"][0]["ordinal_start"],
                "item_count": document["item_count"],
                "logical_bytes": document["logical_bytes"],
                "ref": ref,
            }
        return child

    def _append_index_node(
        self,
        ref: Mapping[str, Any],
        *,
        sequence_kind: str,
        level: int,
        leaf: Mapping[str, Any],
    ) -> dict[str, Any]:
        document = self._load_index_document(
            ref,
            sequence_kind=sequence_kind,
            expected_level=level,
        )
        children = [dict(child) for child in document["children"]]
        if level == 0:
            if len(children) >= _FANOUT:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            children.append(dict(leaf))
        else:
            right = children[-1]
            child_capacity = _FANOUT**level
            if right["item_count"] < child_capacity:
                children[-1] = self._append_index_node(
                    right["ref"],
                    sequence_kind=sequence_kind,
                    level=level - 1,
                    leaf=leaf,
                )
            elif len(children) < _FANOUT:
                children.append(
                    self._new_index_path(sequence_kind, level - 1, leaf)
                )
            else:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        rebuilt, rebuilt_ref = self._index_document(sequence_kind, level, children)
        return {
            "ordinal_start": rebuilt["children"][0]["ordinal_start"],
            "item_count": rebuilt["item_count"],
            "logical_bytes": rebuilt["logical_bytes"],
            "ref": rebuilt_ref,
        }

    def append_sequence(
        self,
        previous: SequenceReceipt | None,
        leaf: SequenceLeaf,
        *,
        sequence_kind: str,
    ) -> SequenceReceipt:
        with self._reference_budget():
            return self._append_sequence(
                previous,
                leaf,
                sequence_kind=sequence_kind,
            )

    def _append_sequence(
        self,
        previous: SequenceReceipt | None,
        leaf: SequenceLeaf,
        *,
        sequence_kind: str,
    ) -> SequenceReceipt:
        if not isinstance(leaf, SequenceLeaf):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if previous is not None and (
            not isinstance(previous, SequenceReceipt)
            or previous.sequence_kind != sequence_kind
            or type(previous.level) is not int
            or type(previous.item_count) is not int
            or type(previous.logical_bytes) is not int
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        size = _require_exact_int(leaf.logical_bytes)
        leaf_ref = self._validate_leaf_ref(
            sequence_kind,
            leaf.ref,
            logical_bytes=size,
            ordinal=0 if previous is None else previous.item_count,
        )
        if previous is None:
            receipt = self.build_sequence(sequence_kind, [SequenceLeaf(leaf_ref, size)])
            assert receipt is not None
            return receipt
        assert isinstance(previous, SequenceReceipt)
        # Re-verify the durable predecessor before it becomes reachable from a
        # new parent.  A prior in-process verification is not an integrity
        # lease: an external disk fault or unauthorized mutation can happen
        # between appends.  This read-side scrub does not change the frozen
        # O(delta) *write amplification*: only the right spine is serialized.
        self.sequence_leaves(previous)
        root = self._load_index_document(
            previous.root_ref,
            sequence_kind=sequence_kind,
            expected_level=previous.level,
        )
        if (
            root["children"][0]["ordinal_start"] != 0
            or root["item_count"] != previous.item_count
            or root["logical_bytes"] != previous.logical_bytes
            or (previous.level > 0 and len(root["children"]) == 1)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if previous.item_count >= _MAX_INT64 or previous.logical_bytes + size > _MAX_INT64:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        self._ensure_sequence_budget(
            previous.item_count + 1,
            previous.logical_bytes + size,
        )
        new_level = self._sequence_level(previous.item_count + 1)
        record_type = self._record_type_for_sequence(sequence_kind)
        if record_type is not None:
            self._hydrate_record(
                leaf_ref,
                expected_record_type=record_type,
                _base_reference_depth=new_level + 2,
            )
            if new_level > previous.level:
                self._sequence_leaves(
                    previous,
                    _base_reference_depth=1,
                )
        new_leaf = {
            "ordinal_start": previous.item_count,
            "item_count": 1,
            "logical_bytes": size,
            "ref": leaf_ref,
        }
        root_capacity = _FANOUT ** (previous.level + 1)
        if previous.item_count == root_capacity:
            right = self._new_index_path(sequence_kind, previous.level, new_leaf)
            left = {
                "ordinal_start": 0,
                "item_count": previous.item_count,
                "logical_bytes": previous.logical_bytes,
                "ref": _copy_ref(previous.root_ref),
            }
            document, ref = self._index_document(
                sequence_kind,
                previous.level + 1,
                [left, right],
            )
            receipt = SequenceReceipt(
                ref,
                sequence_kind,
                previous.level + 1,
                document["item_count"],
                document["logical_bytes"],
            )
            return receipt
        rebuilt = self._append_index_node(
            previous.root_ref,
            sequence_kind=sequence_kind,
            level=previous.level,
            leaf=new_leaf,
        )
        receipt = SequenceReceipt(
            rebuilt["ref"],
            sequence_kind,
            previous.level,
            rebuilt["item_count"],
            rebuilt["logical_bytes"],
        )
        return receipt

    def _replace_sequence_leaves(
        self,
        previous: SequenceReceipt,
        replacements: Mapping[int, SequenceLeaf],
        *,
        sequence_kind: str,
    ) -> SequenceReceipt:
        """Copy-on-write replacement of stable ordinals in a verified sequence.

        Unchanged subtrees remain reachable by their predecessor refs.  A
        replacement serializes only the index nodes on the path from its leaf
        to the root (shared paths are rebuilt once), which keeps physical
        writes proportional to the changed ordinals rather than the sequence
        cardinality.
        """

        if (
            not isinstance(previous, SequenceReceipt)
            or previous.sequence_kind != sequence_kind
            or sequence_kind not in _SEQUENCE_KINDS
            or not isinstance(replacements, Mapping)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        # Re-verify the complete durable predecessor before preserving any of
        # its refs in the new root.  The operation-local read cache makes the
        # subsequent path reads cheap without weakening this integrity pass.
        predecessor_leaves = self.sequence_leaves(previous)
        if not replacements:
            return previous

        frozen: dict[int, SequenceLeaf] = {}
        for raw_ordinal, leaf in replacements.items():
            ordinal = _require_exact_int(raw_ordinal)
            if ordinal >= previous.item_count or not isinstance(leaf, SequenceLeaf):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            logical_bytes = _require_exact_int(leaf.logical_bytes)
            leaf_ref = self._validate_leaf_ref(
                sequence_kind,
                leaf.ref,
                logical_bytes=logical_bytes,
                ordinal=ordinal,
            )
            frozen[ordinal] = SequenceLeaf(leaf_ref, logical_bytes)

        effective = {
            ordinal: leaf
            for ordinal, leaf in frozen.items()
            if (
                _copy_ref(leaf.ref)
                != _copy_ref(predecessor_leaves[ordinal].ref)
                or leaf.logical_bytes != predecessor_leaves[ordinal].logical_bytes
            )
        }
        if not effective:
            return previous

        def rebuild(
            ref: Mapping[str, Any],
            level: int,
        ) -> tuple[dict[str, Any], bool]:
            document = self._load_index_document(
                ref,
                sequence_kind=sequence_kind,
                expected_level=level,
            )
            children = [dict(child) for child in document["children"]]
            changed = False
            for index, child in enumerate(children):
                start = child["ordinal_start"]
                end = start + child["item_count"]
                if not any(start <= ordinal < end for ordinal in effective):
                    continue
                if level == 0:
                    replacement = effective.get(start)
                    if replacement is None:
                        continue
                    children[index] = {
                        "ordinal_start": start,
                        "item_count": 1,
                        "logical_bytes": replacement.logical_bytes,
                        "ref": _copy_ref(replacement.ref),
                    }
                    changed = True
                else:
                    rebuilt_child, child_changed = rebuild(
                        child["ref"],
                        level - 1,
                    )
                    if child_changed:
                        children[index] = rebuilt_child
                        changed = True
            if not changed:
                return (
                    {
                        "ordinal_start": document["children"][0]["ordinal_start"],
                        "item_count": document["item_count"],
                        "logical_bytes": document["logical_bytes"],
                        "ref": _copy_ref(ref),
                    },
                    False,
                )
            rebuilt, rebuilt_ref = self._index_document(
                sequence_kind,
                level,
                children,
            )
            return (
                {
                    "ordinal_start": rebuilt["children"][0]["ordinal_start"],
                    "item_count": rebuilt["item_count"],
                    "logical_bytes": rebuilt["logical_bytes"],
                    "ref": rebuilt_ref,
                },
                True,
            )

        rebuilt, changed = rebuild(previous.root_ref, previous.level)
        if (
            not changed
            or rebuilt["ordinal_start"] != 0
            or rebuilt["item_count"] != previous.item_count
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        self._ensure_sequence_budget(
            rebuilt["item_count"],
            rebuilt["logical_bytes"],
        )
        return SequenceReceipt(
            rebuilt["ref"],
            sequence_kind,
            previous.level,
            rebuilt["item_count"],
            rebuilt["logical_bytes"],
        )

    def _truncate_sequence(
        self,
        previous: SequenceReceipt,
        item_count: int,
        *,
        sequence_kind: str,
    ) -> SequenceReceipt:
        """Return a canonical persistent prefix without rebuilding siblings."""

        if (
            not isinstance(previous, SequenceReceipt)
            or previous.sequence_kind != sequence_kind
            or sequence_kind not in _SEQUENCE_KINDS
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        count = _require_exact_int(item_count, minimum=1)
        if count > previous.item_count:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self.sequence_leaves(previous)
        if count == previous.item_count:
            return previous

        target_level = self._sequence_level(count)
        root_ref = _copy_ref(previous.root_ref)
        level = previous.level
        while level > target_level:
            document = self._load_index_document(
                root_ref,
                sequence_kind=sequence_kind,
                expected_level=level,
            )
            first = document["children"][0]
            if first["ordinal_start"] != 0 or count > first["item_count"]:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            root_ref = _copy_ref(first["ref"])
            level -= 1

        def truncate(
            ref: Mapping[str, Any],
            node_level: int,
            keep: int,
        ) -> dict[str, Any]:
            document = self._load_index_document(
                ref,
                sequence_kind=sequence_kind,
                expected_level=node_level,
            )
            if keep == document["item_count"]:
                return {
                    "ordinal_start": document["children"][0]["ordinal_start"],
                    "item_count": document["item_count"],
                    "logical_bytes": document["logical_bytes"],
                    "ref": _copy_ref(ref),
                }
            if not 1 <= keep < document["item_count"]:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            retained: list[dict[str, Any]] = []
            remaining = keep
            for child in document["children"]:
                if remaining == 0:
                    break
                child_count = child["item_count"]
                if child_count <= remaining:
                    retained.append(dict(child))
                    remaining -= child_count
                    continue
                if node_level == 0:
                    raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                retained.append(
                    truncate(child["ref"], node_level - 1, remaining)
                )
                remaining = 0
            if remaining != 0:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            rebuilt, rebuilt_ref = self._index_document(
                sequence_kind,
                node_level,
                retained,
            )
            return {
                "ordinal_start": rebuilt["children"][0]["ordinal_start"],
                "item_count": rebuilt["item_count"],
                "logical_bytes": rebuilt["logical_bytes"],
                "ref": rebuilt_ref,
            }

        rebuilt = truncate(root_ref, target_level, count)
        if rebuilt["ordinal_start"] != 0 or rebuilt["item_count"] != count:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        self._ensure_sequence_budget(
            rebuilt["item_count"],
            rebuilt["logical_bytes"],
        )
        return SequenceReceipt(
            rebuilt["ref"],
            sequence_kind,
            target_level,
            rebuilt["item_count"],
            rebuilt["logical_bytes"],
        )

    def sequence_leaves(
        self,
        receipt: SequenceReceipt,
    ) -> list[SequenceLeaf]:
        with self._reference_budget():
            return self._sequence_leaves(receipt)

    def _sequence_leaves(
        self,
        receipt: SequenceReceipt,
        *,
        _base_reference_depth: int = 0,
    ) -> list[SequenceLeaf]:
        if not isinstance(receipt, SequenceReceipt):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if (
            type(receipt.level) is not int
            or not 0 <= receipt.level <= 32
            or type(receipt.item_count) is not int
            or type(receipt.logical_bytes) is not int
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._ensure_sequence_budget(receipt.item_count, receipt.logical_bytes)
        if _base_reference_depth + receipt.level + 1 > (
            self.storage.limits.max_reference_depth
        ):
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        active: set[str] = set()
        seen: set[str] = set()
        leaves: list[SequenceLeaf] = []
        referenced_stored = 0
        referenced_uncompressed = 0

        def track(ref: Mapping[str, Any]) -> None:
            nonlocal referenced_stored, referenced_uncompressed
            copied = _copy_ref(ref)
            digest = _require_exact_string(copied.get("digest"))
            if digest in seen:
                return
            seen.add(digest)
            if len(seen) > self.storage.limits.max_referenced_objects:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            referenced_stored += _require_exact_int(copied.get("stored_size"))
            referenced_uncompressed += _require_exact_int(
                copied.get("uncompressed_size")
            )
            if (
                referenced_stored > _MAX_REFERENCED_STORED_BYTES
                or referenced_uncompressed > _MAX_REFERENCED_UNCOMPRESSED_BYTES
            ):
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

        def visit(
            ref: Mapping[str, Any],
            level: int,
            *,
            root: bool,
            reference_depth: int,
        ) -> tuple[int, int, int]:
            self._check_cancelled()
            copied = _copy_ref(ref)
            digest = _require_exact_string(copied.get("digest"))
            if digest in active:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            active.add(digest)
            try:
                track(copied)
                node = self._load_index_document(
                    copied,
                    sequence_kind=receipt.sequence_kind,
                    expected_level=level,
                )
                if root and (
                    node["children"][0]["ordinal_start"] != 0
                    or (level > 0 and len(node["children"]) == 1)
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                first = node["children"][0]["ordinal_start"]
                count = size = 0
                for child in node["children"]:
                    self._check_cancelled()
                    if level == 0:
                        track(child["ref"])
                        record_type = self._record_type_for_sequence(
                            receipt.sequence_kind
                        )
                        if record_type is not None:
                            self._hydrate_record(
                                child["ref"],
                                expected_record_type=record_type,
                                _base_reference_depth=reference_depth + 1,
                            )
                        leaves.append(SequenceLeaf(_copy_ref(child["ref"]), child["logical_bytes"]))
                        child_first, child_count, child_size = (
                            child["ordinal_start"],
                            1,
                            child["logical_bytes"],
                        )
                    else:
                        child_first, child_count, child_size = visit(
                            child["ref"],
                            level - 1,
                            root=False,
                            reference_depth=reference_depth + 1,
                        )
                        if (
                            child_first != child["ordinal_start"]
                            or child_count != child["item_count"]
                            or child_size != child["logical_bytes"]
                        ):
                            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                    count += child_count
                    size += child_size
                if count != node["item_count"] or size != node["logical_bytes"]:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                return first, count, size
            finally:
                active.remove(digest)

        first, count, size = visit(
            receipt.root_ref,
            receipt.level,
            root=True,
            reference_depth=_base_reference_depth,
        )
        if (
            first != 0
            or count != receipt.item_count
            or size != receipt.logical_bytes
            or len(leaves) != receipt.item_count
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return leaves

    def store_byte_stream(
        self,
        data: bytes | bytearray | memoryview,
        *,
        sequence_kind: str,
        media_type: str | None = None,
    ) -> ByteStreamReceipt:
        with self._reference_budget():
            return self._store_byte_stream(
                data,
                sequence_kind=sequence_kind,
                media_type=media_type,
            )

    def _store_byte_stream(
        self,
        data: bytes | bytearray | memoryview,
        *,
        sequence_kind: str,
        media_type: str | None = None,
    ) -> ByteStreamReceipt:
        if sequence_kind not in _BYTE_SEQUENCE_TYPES:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        try:
            view = memoryview(data)
            try:
                if view.nbytes > self.storage.limits.max_hydrated_bytes:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                payload = view.tobytes()
            finally:
                view.release()
        except PromptArchiveStorageError:
            raise
        except (TypeError, ValueError, BufferError, MemoryError):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
        logical_kind, expected_media = _BYTE_SEQUENCE_TYPES[sequence_kind]
        if media_type is not None:
            if sequence_kind != "payload_chunks" or type(media_type) is not str:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            expected_media = media_type
        chunks = (
            [payload[offset : offset + _CHUNK_BYTES] for offset in range(0, len(payload), _CHUNK_BYTES)]
            if payload
            else [b""]
        )
        leaves = [
            SequenceLeaf(
                self._put_blob(
                    chunk,
                    logical_kind=logical_kind,
                    media_type=expected_media,
                ),
                len(chunk),
            )
            for chunk in chunks
        ]
        sequence = self.build_sequence(sequence_kind, leaves)
        assert sequence is not None
        return ByteStreamReceipt(
            sequence,
            len(payload),
            hashlib.sha256(payload).hexdigest(),
            len(chunks),
        )

    def hydrate_byte_stream(
        self,
        receipt: ByteStreamReceipt,
        *,
        media_type: str | None = None,
    ) -> bytes:
        with self._reference_budget():
            return self._hydrate_byte_stream(receipt, media_type=media_type)

    def _hydrate_byte_stream(
        self,
        receipt: ByteStreamReceipt,
        *,
        media_type: str | None = None,
    ) -> bytes:
        if not isinstance(receipt, ByteStreamReceipt):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        sequence_kind = receipt.sequence.sequence_kind
        if sequence_kind not in _BYTE_SEQUENCE_TYPES:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        logical_kind, expected_media = _BYTE_SEQUENCE_TYPES[sequence_kind]
        if media_type is not None:
            if sequence_kind != "payload_chunks" or type(media_type) is not str:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            expected_media = media_type
        chunks: list[bytes] = []
        total = 0
        leaves = self.sequence_leaves(receipt.sequence)
        if len(leaves) != receipt.chunk_count:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        for index, leaf in enumerate(leaves):
            self._check_cancelled()
            raw = self._read_blob(
                leaf.ref,
                logical_kind=logical_kind,
                media_type=expected_media,
            )
            if len(raw) != leaf.logical_bytes:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            if len(leaves) == 1 and receipt.byte_size == 0:
                if raw != b"":
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            elif index < len(leaves) - 1:
                if len(raw) != _CHUNK_BYTES:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            elif not 1 <= len(raw) <= _CHUNK_BYTES:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            total += len(raw)
            if total > self.storage.limits.max_hydrated_bytes:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            chunks.append(raw)
        payload = b"".join(chunks)
        if (
            total != receipt.byte_size
            or receipt.sequence.logical_bytes != total
            or hashlib.sha256(payload).hexdigest() != receipt.sha256
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return payload

    @staticmethod
    def _span_document(receipt: ByteStreamReceipt) -> dict[str, Any]:
        return {
            "chunks_root": _copy_ref(receipt.sequence.root_ref),
            "byte_size": receipt.byte_size,
            "sha256": receipt.sha256,
        }

    def _store_literal_span(self, source: bytes, span: ByteSpan) -> ByteStreamReceipt:
        return self.store_byte_stream(
            span.bytes_from(source),
            sequence_kind="literal_chunks",
        )

    def store_json_value(
        self,
        data: bytes | bytearray | memoryview,
    ) -> JsonValueReceipt:
        """Store one strict complete JSON value as an exact-byte Merkle DAG."""

        with self._reference_budget():
            return self._store_json_value(data)

    def _store_json_value(
        self,
        data: bytes | bytearray | memoryview,
    ) -> JsonValueReceipt:

        try:
            view = memoryview(data)
            try:
                if view.nbytes > self.storage.limits.max_hydrated_bytes:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                source = view.tobytes()
            finally:
                view.release()
        except PromptArchiveStorageError:
            raise
        except (TypeError, ValueError, BufferError, MemoryError):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
        try:
            root = parse_json_bytes(
                source,
                max_depth=128,
                max_nodes=min(self.storage.limits.max_referenced_objects, 1_000_000),
                max_edges=min(self.storage.limits.max_referenced_objects, 1_000_000),
            )
        except JsonSpanParseError:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None

        def build(node: JsonNode) -> JsonValueReceipt:
            value_bytes = node.bytes_from(source)
            value_digest = hashlib.sha256(value_bytes).hexdigest()
            value_nodes = 1
            edge_count = literal_chunks = index_nodes = 0
            if node.node_kind == "scalar":
                assert node.token is not None and node.scalar_kind is not None
                token = self._store_literal_span(source, node.token)
                literal_chunks += token.chunk_count
                index_nodes += self._index_object_count(token.chunk_count)
                document: dict[str, Any] = {
                    "schema_version": 1,
                    "kind": "prompt_archive_json_value",
                    "media_type": _CONTROL_MEDIA["json_value"],
                    "builder_version": "pas1-recursive-json-merkle-v1",
                    "node_kind": "scalar",
                    "scalar_kind": node.scalar_kind,
                    "token": self._span_document(token),
                    "byte_size": len(value_bytes),
                    "sha256": value_digest,
                }
            else:
                assert node.prefix is not None and node.suffix is not None
                prefix = self._store_literal_span(source, node.prefix)
                suffix = self._store_literal_span(source, node.suffix)
                literal_chunks += prefix.chunk_count + suffix.chunk_count
                index_nodes += self._index_object_count(
                    prefix.chunk_count
                ) + self._index_object_count(suffix.chunk_count)
                edge_leaves: list[SequenceLeaf] = []
                for edge in node.edges:
                    child = build(edge.value)
                    value_nodes += child.value_nodes
                    edge_count += child.edges + 1
                    literal_chunks += child.literal_chunks
                    index_nodes += child.index_nodes
                    separator = self._store_literal_span(source, edge.separator_before)
                    literal_chunks += separator.chunk_count
                    index_nodes += self._index_object_count(separator.chunk_count)
                    edge_document: dict[str, Any] = {
                        "schema_version": 1,
                        "kind": "prompt_archive_json_edge",
                        "media_type": _CONTROL_MEDIA["json_edge"],
                        "builder_version": "pas1-recursive-json-merkle-v1",
                        "edge_kind": edge.edge_kind,
                        "separator_before": self._span_document(separator),
                        "value_ref": _copy_ref(child.ref),
                        "byte_size": edge.end - edge.start,
                        "sha256": hashlib.sha256(edge.bytes_from(source)).hexdigest(),
                    }
                    if edge.edge_kind == "object_member":
                        assert edge.key_token is not None
                        assert edge.key_value_separator is not None
                        key_token = self._store_literal_span(source, edge.key_token)
                        key_separator = self._store_literal_span(
                            source,
                            edge.key_value_separator,
                        )
                        literal_chunks += key_token.chunk_count + key_separator.chunk_count
                        index_nodes += self._index_object_count(
                            key_token.chunk_count
                        ) + self._index_object_count(key_separator.chunk_count)
                        edge_document["key_token"] = self._span_document(key_token)
                        edge_document["key_value_separator"] = self._span_document(
                            key_separator
                        )
                    edge_ref = self._put_control(edge_document, "json_edge")
                    edge_leaves.append(
                        SequenceLeaf(edge_ref, edge_document["byte_size"])
                    )
                entries = self.build_sequence("json_edges", edge_leaves)
                if entries is not None:
                    index_nodes += self._index_object_count(entries.item_count)
                document = {
                    "schema_version": 1,
                    "kind": "prompt_archive_json_value",
                    "media_type": _CONTROL_MEDIA["json_value"],
                    "builder_version": "pas1-recursive-json-merkle-v1",
                    "node_kind": node.node_kind,
                    "prefix": self._span_document(prefix),
                    "entries_root": (
                        None if entries is None else _copy_ref(entries.root_ref)
                    ),
                    "entry_count": len(edge_leaves),
                    "suffix": self._span_document(suffix),
                    "byte_size": len(value_bytes),
                    "sha256": value_digest,
                }
            value_ref = self._put_control(document, "json_value")
            return JsonValueReceipt(
                value_ref,
                len(value_bytes),
                value_digest,
                value_nodes,
                edge_count,
                literal_chunks,
                index_nodes,
            )

        receipt = build(root)
        if self.hydrate_json_value(receipt) != source:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return receipt

    def _sequence_within_reference_depth(
        self,
        root_ref: Mapping[str, Any],
        sequence_kind: str,
        *,
        parent_reference_depth: int,
    ) -> SequenceReceipt:
        sequence = self._receipt_from_root(root_ref, sequence_kind)
        deepest_leaf = parent_reference_depth + 2 + sequence.level
        if deepest_leaf > self.storage.limits.max_reference_depth:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        return sequence

    def _hydrate_literal_span(
        self,
        value: Any,
        *,
        parent_reference_depth: int,
    ) -> bytes:
        if type(value) is not dict or set(value) != {
            "chunks_root",
            "byte_size",
            "sha256",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        byte_size = _require_exact_int(value.get("byte_size"))
        digest = _require_exact_string(value.get("sha256"))
        if _SHA256_RE.fullmatch(digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        sequence = self._sequence_within_reference_depth(
            value["chunks_root"],
            "literal_chunks",
            parent_reference_depth=parent_reference_depth,
        )
        leaves = self.sequence_leaves(sequence)
        stream = ByteStreamReceipt(sequence, byte_size, digest, len(leaves))
        return self.hydrate_byte_stream(stream)

    def hydrate_json_value(
        self,
        value: JsonValueReceipt | Mapping[str, Any],
        *,
        max_output_bytes: int | None = None,
    ) -> bytes:
        """Verify and reconstruct an exact recursive JSON value."""

        with self._reference_budget():
            return self._hydrate_json_value(
                value,
                max_output_bytes=max_output_bytes,
            )

    def _hydrate_json_value(
        self,
        value: JsonValueReceipt | Mapping[str, Any],
        *,
        max_output_bytes: int | None = None,
        _base_reference_depth: int = 0,
    ) -> bytes:

        if max_output_bytes is None:
            maximum = self.storage.limits.max_hydrated_bytes
        elif type(max_output_bytes) is not int or max_output_bytes < 0:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        else:
            maximum = min(max_output_bytes, self.storage.limits.max_hydrated_bytes)
        expected_size: int | None = None
        expected_digest: str | None = None
        if isinstance(value, JsonValueReceipt):
            root_ref = value.ref
            expected_size = value.byte_size
            expected_digest = value.sha256
        elif isinstance(value, Mapping):
            root_ref = value
        else:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        active: set[str] = set()
        memo: dict[str, bytes] = {}
        referenced: set[str] = set()
        edge_visits = 0

        def count_ref(ref: Mapping[str, Any]) -> str:
            self._check_cancelled()
            copied = _copy_ref(ref)
            digest = _require_exact_string(copied.get("digest"))
            if digest not in referenced:
                referenced.add(digest)
                if len(referenced) > self.storage.limits.max_referenced_objects:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            return digest

        def hydrate_span(span: Any, *, parent_reference_depth: int) -> bytes:
            if type(span) is not dict or set(span) != {
                "chunks_root",
                "byte_size",
                "sha256",
            }:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            count_ref(span["chunks_root"])
            return self._hydrate_literal_span(
                span,
                parent_reference_depth=parent_reference_depth,
            )

        def hydrate_value(
            ref: Mapping[str, Any],
            syntax_depth: int,
            reference_depth: int,
        ) -> bytes:
            nonlocal edge_visits
            self._check_cancelled()
            if syntax_depth > 128 or reference_depth > self.storage.limits.max_reference_depth:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            incoming = _copy_ref(ref)
            if (
                incoming.get("logical_kind") != "json_value"
                or incoming.get("media_type") != _CONTROL_MEDIA["json_value"]
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            self.storage.blob_path(incoming)
            digest = count_ref(incoming)
            if digest in active:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            # Validate every incoming typed/physical reference even when the
            # decoded value is memoized.  The operation read cache makes the
            # common identical-reference case cheap while rejecting a forged
            # descriptor that reuses a digest with divergent metadata.
            raw_document = self._read_blob(
                incoming,
                logical_kind="json_value",
                media_type=_CONTROL_MEDIA["json_value"],
            )
            cached = memo.get(digest)
            if cached is not None:
                return cached
            active.add(digest)
            try:
                document = _load_canonical_control(raw_document)
                common = {
                    "schema_version",
                    "kind",
                    "media_type",
                    "builder_version",
                    "node_kind",
                    "byte_size",
                    "sha256",
                }
                if (
                    document.get("schema_version") != 1
                    or document.get("kind") != "prompt_archive_json_value"
                    or document.get("media_type") != _CONTROL_MEDIA["json_value"]
                    or document.get("builder_version")
                    != "pas1-recursive-json-merkle-v1"
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                node_kind = document.get("node_kind")
                declared_size = _require_exact_int(document.get("byte_size"), minimum=1)
                declared_digest = _require_exact_string(document.get("sha256"))
                if _SHA256_RE.fullmatch(declared_digest) is None or declared_size > maximum:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                if node_kind == "scalar":
                    if set(document) != common | {"scalar_kind", "token"} or document.get(
                        "scalar_kind"
                    ) not in {"null", "boolean", "number", "string"}:
                        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                    output = hydrate_span(
                        document["token"],
                        parent_reference_depth=reference_depth,
                    )
                elif node_kind in {"array", "object"}:
                    if set(document) != common | {
                        "prefix",
                        "entries_root",
                        "entry_count",
                        "suffix",
                    }:
                        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                    entry_count = _require_exact_int(document.get("entry_count"))
                    prefix = hydrate_span(
                        document["prefix"],
                        parent_reference_depth=reference_depth,
                    )
                    suffix = hydrate_span(
                        document["suffix"],
                        parent_reference_depth=reference_depth,
                    )
                    edge_parts: list[bytes] = []
                    edge_components: list[
                        tuple[str, bytes, bytes | None, bytes | None, bytes]
                    ] = []
                    if entry_count == 0:
                        if document.get("entries_root") is not None:
                            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                    else:
                        entries = self._sequence_within_reference_depth(
                            document.get("entries_root"),
                            "json_edges",
                            parent_reference_depth=reference_depth,
                        )
                        if entries.item_count != entry_count:
                            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                        for leaf in self.sequence_leaves(entries):
                            edge_visits += 1
                            if edge_visits > min(
                                self.storage.limits.max_referenced_objects,
                                1_000_000,
                            ):
                                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                            edge_ref = _copy_ref(leaf.ref)
                            count_ref(edge_ref)
                            edge = self._read_control(edge_ref, logical_kind="json_edge")
                            edge_common = {
                                "schema_version",
                                "kind",
                                "media_type",
                                "builder_version",
                                "edge_kind",
                                "separator_before",
                                "value_ref",
                                "byte_size",
                                "sha256",
                            }
                            if (
                                edge.get("schema_version") != 1
                                or edge.get("kind") != "prompt_archive_json_edge"
                                or edge.get("media_type") != _CONTROL_MEDIA["json_edge"]
                                or edge.get("builder_version")
                                != "pas1-recursive-json-merkle-v1"
                            ):
                                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                            edge_kind = edge.get("edge_kind")
                            expected_edge_kind = (
                                "array_item" if node_kind == "array" else "object_member"
                            )
                            if edge_kind != expected_edge_kind:
                                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                            edge_reference_depth = reference_depth + 2 + entries.level
                            separator_bytes = hydrate_span(
                                edge.get("separator_before"),
                                parent_reference_depth=edge_reference_depth,
                            )
                            part = separator_bytes
                            key_token_bytes: bytes | None = None
                            key_separator_bytes: bytes | None = None
                            if edge_kind == "array_item":
                                if set(edge) != edge_common:
                                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                            elif edge_kind == "object_member":
                                if set(edge) != edge_common | {
                                    "key_token",
                                    "key_value_separator",
                                }:
                                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                                key_token_bytes = hydrate_span(
                                    edge["key_token"],
                                    parent_reference_depth=edge_reference_depth,
                                )
                                key_separator_bytes = hydrate_span(
                                    edge["key_value_separator"],
                                    parent_reference_depth=edge_reference_depth,
                                )
                                part += key_token_bytes
                                part += key_separator_bytes
                            else:
                                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                            child_bytes = hydrate_value(
                                edge.get("value_ref"),
                                syntax_depth + 1,
                                edge_reference_depth + 1,
                            )
                            part += child_bytes
                            edge_size = _require_exact_int(edge.get("byte_size"), minimum=1)
                            edge_digest = _require_exact_string(edge.get("sha256"))
                            if (
                                edge_size != len(part)
                                or _SHA256_RE.fullmatch(edge_digest) is None
                                or hashlib.sha256(part).hexdigest() != edge_digest
                                or leaf.logical_bytes != len(part)
                            ):
                                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                            edge_parts.append(part)
                            edge_components.append(
                                (
                                    edge_kind,
                                    separator_bytes,
                                    key_token_bytes,
                                    key_separator_bytes,
                                    child_bytes,
                                )
                            )
                    output = prefix + b"".join(edge_parts) + suffix
                else:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                if len(output) > maximum:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                if (
                    len(output) != declared_size
                    or hashlib.sha256(output).hexdigest() != declared_digest
                ):
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                try:
                    parsed = parse_json_bytes(
                        output,
                        max_depth=128,
                        max_nodes=min(
                            self.storage.limits.max_referenced_objects,
                            1_000_000,
                        ),
                        max_edges=min(
                            self.storage.limits.max_referenced_objects,
                            1_000_000,
                        ),
                    )
                except JsonSpanParseError:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") from None
                if parsed.node_kind != node_kind or (
                    node_kind == "scalar"
                    and parsed.scalar_kind != document.get("scalar_kind")
                ) or (
                    node_kind in {"array", "object"}
                    and parsed.entry_count != document.get("entry_count")
                ):
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                if node_kind in {"array", "object"}:
                    assert parsed.prefix is not None and parsed.suffix is not None
                    if (
                        parsed.prefix.bytes_from(output) != prefix
                        or parsed.suffix.bytes_from(output) != suffix
                        or len(parsed.edges) != len(edge_components)
                    ):
                        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                    for parsed_edge, component in zip(parsed.edges, edge_components):
                        (
                            edge_kind,
                            separator_bytes,
                            key_token_bytes,
                            key_separator_bytes,
                            child_bytes,
                        ) = component
                        if (
                            parsed_edge.edge_kind != edge_kind
                            or parsed_edge.separator_before.bytes_from(output)
                            != separator_bytes
                            or parsed_edge.value.bytes_from(output) != child_bytes
                        ):
                            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                        if edge_kind == "object_member":
                            assert parsed_edge.key_token is not None
                            assert parsed_edge.key_value_separator is not None
                            if (
                                parsed_edge.key_token.bytes_from(output)
                                != key_token_bytes
                                or parsed_edge.key_value_separator.bytes_from(output)
                                != key_separator_bytes
                            ):
                                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                memo[digest] = output
                return output
            finally:
                active.remove(digest)

        output = hydrate_value(root_ref, 1, _base_reference_depth)
        if expected_size is not None and expected_size != len(output):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if expected_digest is not None and expected_digest != hashlib.sha256(output).hexdigest():
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if len(output) > maximum:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        try:
            parse_json_bytes(
                output,
                max_depth=128,
                max_nodes=min(self.storage.limits.max_referenced_objects, 1_000_000),
                max_edges=min(self.storage.limits.max_referenced_objects, 1_000_000),
            )
        except JsonSpanParseError:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") from None
        return output

    @staticmethod
    def _validate_source_metadata_projection(
        raw: bytes,
    ) -> tuple[dict[str, Any], bytes, str]:
        document = _load_canonical_control(raw)
        try:
            projection = validate_source_metadata_projection(document)
        except PromptArchiveSourceMetadataError as exc:
            raise _error(exc.code) from None
        canonical = projection.canonical_bytes
        if canonical != raw:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return projection.document, canonical, projection.sha256

    @staticmethod
    def _validate_source_metadata_receipt(value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            raw = canonical_json_bytes_v1(dict(value), maximum=16 * 1024)
        except PromptArchiveStorageError:
            raise
        except (TypeError, ValueError, MemoryError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        receipt = _load_canonical_control(raw)
        fields = receipt.get("projected_fields")
        if (
            set(receipt) != _SOURCE_METADATA_RECEIPT_KEYS
            or receipt.get("schema_version") != 1
            or receipt.get("kind")
            != "prompt_archive_source_metadata_projection_receipt"
            or receipt.get("projection_version")
            != "prompt-archive-source-projection-v1"
            or receipt.get("redaction_version")
            != "prompt-archive-source-redaction-v1"
            or type(receipt.get("projection_sha256")) is not str
            or _SHA256_RE.fullmatch(receipt["projection_sha256"]) is None
            or type(receipt.get("projection_byte_size")) is not int
            or not 1 <= receipt["projection_byte_size"] <= 16 * 1024
            or receipt.get("source_type")
            not in {"api", "email", "folder", "manual", "telegram", "unknown"}
            or type(receipt.get("discarded_field_count")) is not int
            or not 0 <= receipt["discarded_field_count"] <= 1024
            or type(fields) is not list
            or any(type(field) is not str for field in fields)
            or fields != sorted(set(fields))
            or not set(fields).issubset(_SOURCE_METADATA_OPTIONAL_FIELDS)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return receipt

    def _validate_occurrence_source_metadata(
        self,
        occurrence: ArchiveOccurrence,
    ) -> tuple[dict[str, Any] | None, bytes | None, dict[str, Any] | None]:
        projection_value = occurrence.source_metadata_projection
        receipt_value = occurrence.source_metadata_receipt
        if (projection_value is None) != (receipt_value is None):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if projection_value is None:
            return None, None, None
        if occurrence.occurrence_kind == "migration" or not isinstance(
            projection_value,
            Mapping,
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            projection_raw = canonical_json_bytes_v1(
                dict(projection_value),
                maximum=16 * 1024,
            )
        except PromptArchiveStorageError:
            raise
        except (TypeError, ValueError, MemoryError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        projection, projection_raw, projection_sha256 = (
            self._validate_source_metadata_projection(projection_raw)
        )
        receipt = self._validate_source_metadata_receipt(receipt_value)
        self._validate_source_metadata_receipt_binding(
            projection,
            projection_raw,
            projection_sha256,
            receipt,
        )
        return projection, projection_raw, receipt

    @staticmethod
    def _validate_source_metadata_receipt_binding(
        projection: Mapping[str, Any],
        projection_raw: bytes,
        projection_sha256: str,
        receipt: Mapping[str, Any],
    ) -> None:
        projected_fields = sorted(
            field for field in _SOURCE_METADATA_OPTIONAL_FIELDS if field in projection
        )
        if (
            not hmac.compare_digest(
                receipt["projection_sha256"],
                projection_sha256,
            )
            or receipt["projection_byte_size"] != len(projection_raw)
            or receipt["source_type"] != projection["source_type"]
            or receipt["discarded_field_count"]
            != projection["discarded_field_count"]
            or receipt["projected_fields"] != projected_fields
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _source_metadata_from_root(
        self,
        root_ref: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bytes, str]:
        sequence = self._receipt_from_root(root_ref, "source_metadata")
        leaves = self.sequence_leaves(sequence)
        if sequence.item_count != 1 or len(leaves) != 1:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        projection_raw = self._hydrate_record(
            leaves[0].ref,
            expected_record_type="source_metadata",
        )
        if sequence.logical_bytes != len(projection_raw):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return self._validate_source_metadata_projection(projection_raw)

    def _validate_source_metadata_root_binding(
        self,
        root_ref: Mapping[str, Any],
        receipt_value: Mapping[str, Any],
    ) -> None:
        receipt = self._validate_source_metadata_receipt(receipt_value)
        projection, projection_raw, projection_sha256 = (
            self._source_metadata_from_root(root_ref)
        )
        self._validate_source_metadata_receipt_binding(
            projection,
            projection_raw,
            projection_sha256,
            receipt,
        )

    def _validate_record_document(
        self,
        document: Mapping[str, Any],
        *,
        expected_record_type: str | None = None,
    ) -> dict[str, Any]:
        value = dict(document)
        required = {
            "schema_version",
            "kind",
            "id_algorithm",
            "record_id",
            "record_type",
            "ordinal",
            "scope",
            "payload",
            "provenance",
        }
        if not required.issubset(value) or not set(value).issubset(required | {"occurred_at"}):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        record_type = value.get("record_type")
        if (
            value.get("schema_version") != 1
            or value.get("kind") != "prompt_archive_record"
            or value.get("id_algorithm") != "PAS1-ID-v1"
            or record_type not in _RECORD_TYPES
            or (expected_record_type is not None and record_type != expected_record_type)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        ordinal = _require_exact_int(value.get("ordinal"))
        if "occurred_at" in value and value["occurred_at"] is not None:
            _require_rfc3339(value["occurred_at"])
        scope = value.get("scope")
        if type(scope) is not dict or set(scope) != {"project_id", "sprint_id"} or (
            scope.get("project_id") != self.project_id
            or scope.get("sprint_id") != self.sprint_id
        ):
            raise _error("PROMPT_ARCHIVE_NOT_FOUND")
        provenance = value.get("provenance")
        frozen_provenance = self._freeze_record_provenance(provenance)
        if frozen_provenance != provenance:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        origin_transition = provenance.get("origin_transition_id")
        payload = value.get("payload")
        if type(payload) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _sequence_kind, _leaf_kind, representation, media_type = _RECORD_TYPES[record_type]
        common_payload = {"representation", "media_type", "byte_size", "sha256"}
        root_field = "chunks_root" if representation == "opaque_chunks_v1" else "value_root"
        if (
            set(payload) != common_payload | {root_field}
            or payload.get("representation") != representation
            or payload.get("media_type") != media_type
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        payload_size = _require_exact_int(
            payload.get("byte_size"),
            minimum=0 if representation == "opaque_chunks_v1" else 1,
        )
        payload_sha = _require_exact_string(payload.get("sha256"))
        if _SHA256_RE.fullmatch(payload_sha) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        root_ref = _copy_ref(payload[root_field])
        expected_root_kind = "index_node" if root_field == "chunks_root" else "json_value"
        expected_root_media = (
            _CONTROL_MEDIA["index_node"]
            if root_field == "chunks_root"
            else _CONTROL_MEDIA["json_value"]
        )
        if (
            root_ref.get("logical_kind") != expected_root_kind
            or root_ref.get("media_type") != expected_root_media
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        record_id = "parrec1-" + pas1_id_v1(
            "record-id",
            (
                ("project_id", self.project_id),
                ("sprint_id", self.sprint_id),
                ("record_type", record_type),
                ("origin_response_id", provenance["origin_response_id"]),
                ("origin_transition_id", origin_transition),
                ("ordinal", str(ordinal)),
                ("payload_size", str(payload_size)),
                ("payload_sha256", payload_sha),
                ("payload_root_digest", _require_exact_string(root_ref.get("digest"))),
            ),
        )
        if value.get("record_id") != record_id:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return value

    def _freeze_record_provenance(self, provenance: Any) -> dict[str, Any]:
        required_provenance = {
            "source_kind",
            "origin_response_id",
            "origin_transition_id",
        }
        optional_provenance = {
            "source_id",
            "source_commit",
            "assignment_id",
            "review_id",
            "result_key",
        }
        if not isinstance(provenance, Mapping):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        frozen = _load_canonical_control(
            canonical_json_bytes_v1(dict(provenance), maximum=16 * 1024)
        )
        provenance = frozen
        if (
            not required_provenance.issubset(provenance)
            or not set(provenance).issubset(required_provenance | optional_provenance)
            or provenance.get("source_kind")
            not in {"runtime_transition", "runtime_identity", "legacy_migration", "synthetic_test"}
            or type(provenance.get("origin_response_id")) is not str
            or _RESPONSE_ID_RE.fullmatch(provenance["origin_response_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        origin_transition = provenance.get("origin_transition_id")
        if origin_transition is not None and (
            type(origin_transition) is not str
            or _SAFE_ID_RE.fullmatch(origin_transition) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        for field in ("source_id", "assignment_id", "review_id", "result_key"):
            if field in provenance and (
                type(provenance[field]) is not str
                or _SAFE_ID_RE.fullmatch(provenance[field]) is None
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if "source_commit" in provenance and (
            type(provenance["source_commit"]) is not str
            or re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", provenance["source_commit"], re.ASCII)
            is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return frozen

    def build_record(
        self,
        record_type: str,
        payload_bytes: bytes | bytearray | memoryview,
        *,
        ordinal: int,
        provenance: Mapping[str, Any],
        occurred_at: str | None = None,
    ) -> RecordReceipt:
        with self._reference_budget():
            return self._build_record(
                record_type,
                payload_bytes,
                ordinal=ordinal,
                provenance=provenance,
                occurred_at=occurred_at,
            )

    def _build_record(
        self,
        record_type: str,
        payload_bytes: bytes | bytearray | memoryview,
        *,
        ordinal: int,
        provenance: Mapping[str, Any],
        occurred_at: str | None = None,
    ) -> RecordReceipt:
        if record_type not in _RECORD_TYPES or not isinstance(provenance, Mapping):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        ordinal = _require_exact_int(ordinal)
        frozen_provenance = self._freeze_record_provenance(provenance)
        frozen_occurred_at = (
            None if occurred_at is None else _require_rfc3339(occurred_at)
        )
        try:
            view = memoryview(payload_bytes)
            try:
                if view.nbytes > self.storage.limits.max_hydrated_bytes:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                payload = view.tobytes()
            finally:
                view.release()
        except PromptArchiveStorageError:
            raise
        except (TypeError, ValueError, BufferError, MemoryError):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
        sequence_kind, logical_kind, representation, media_type = _RECORD_TYPES[record_type]
        if representation == "opaque_chunks_v1":
            stream = self.store_byte_stream(
                payload,
                sequence_kind="payload_chunks",
                media_type=media_type,
            )
            payload_root = stream.sequence.root_ref
            payload_document = {
                "representation": representation,
                "chunks_root": _copy_ref(payload_root),
                "media_type": media_type,
                "byte_size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        else:
            if record_type == "source_metadata":
                self._validate_source_metadata_projection(payload)
            value_receipt = self.store_json_value(payload)
            payload_root = value_receipt.ref
            payload_document = {
                "representation": representation,
                "value_root": _copy_ref(payload_root),
                "media_type": media_type,
                "byte_size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        origin_response_id = frozen_provenance.get("origin_response_id")
        origin_transition_id = frozen_provenance.get("origin_transition_id")
        record_id = "parrec1-" + pas1_id_v1(
            "record-id",
            (
                ("project_id", self.project_id),
                ("sprint_id", self.sprint_id),
                ("record_type", record_type),
                ("origin_response_id", origin_response_id),
                ("origin_transition_id", origin_transition_id),
                ("ordinal", str(ordinal)),
                ("payload_size", str(len(payload))),
                ("payload_sha256", hashlib.sha256(payload).hexdigest()),
                ("payload_root_digest", _require_exact_string(payload_root.get("digest"))),
            ),
        )
        document: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_record",
            "id_algorithm": "PAS1-ID-v1",
            "record_id": record_id,
            "record_type": record_type,
            "ordinal": ordinal,
            "scope": {"project_id": self.project_id, "sprint_id": self.sprint_id},
            "payload": payload_document,
            "provenance": frozen_provenance,
        }
        if frozen_occurred_at is not None:
            document["occurred_at"] = frozen_occurred_at
        self._validate_record_document(document, expected_record_type=record_type)
        raw = canonical_json_bytes_v1(
            document,
            maximum=self.storage.limits.max_blob_uncompressed_bytes,
        )
        ref = self._put_blob(
            raw,
            logical_kind=logical_kind,
            media_type="application/vnd.nginx-qa.prompt-archive-record-v1+json",
        )
        receipt = RecordReceipt(
            ref,
            record_id,
            record_type,
            sequence_kind,
            ordinal,
            len(payload),
            hashlib.sha256(payload).hexdigest(),
            _require_exact_string(payload_root.get("digest")),
        )
        if self.hydrate_record(receipt) != payload:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return receipt

    def hydrate_record(
        self,
        record: RecordReceipt | Mapping[str, Any],
        *,
        expected_record_type: str | None = None,
    ) -> bytes:
        with self._reference_budget():
            return self._hydrate_record(
                record,
                expected_record_type=expected_record_type,
            )

    def _hydrate_record(
        self,
        record: RecordReceipt | Mapping[str, Any],
        *,
        expected_record_type: str | None = None,
        _base_reference_depth: int = 0,
    ) -> bytes:
        if _base_reference_depth > self.storage.limits.max_reference_depth:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if isinstance(record, RecordReceipt):
            if (
                expected_record_type is not None
                and expected_record_type != record.record_type
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            ref = record.ref
            expected_record_type = record.record_type
        elif isinstance(record, Mapping):
            ref = record
        else:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        copied = _copy_ref(ref)
        logical_kind = copied.get("logical_kind")
        candidates = [
            record_type
            for record_type, definition in _RECORD_TYPES.items()
            if definition[1] == logical_kind
        ]
        if len(candidates) != 1:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        record_type = candidates[0]
        if expected_record_type is not None and expected_record_type != record_type:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        raw = self._read_blob(
            copied,
            logical_kind=logical_kind,
            media_type="application/vnd.nginx-qa.prompt-archive-record-v1+json",
        )
        document = self._validate_record_document(
            _load_canonical_control(raw),
            expected_record_type=record_type,
        )
        payload = document["payload"]
        if payload["representation"] == "opaque_chunks_v1":
            sequence = self._sequence_within_reference_depth(
                payload["chunks_root"],
                "payload_chunks",
                parent_reference_depth=_base_reference_depth,
            )
            leaves = self.sequence_leaves(sequence)
            content = self.hydrate_byte_stream(
                ByteStreamReceipt(
                    sequence,
                    payload["byte_size"],
                    payload["sha256"],
                    len(leaves),
                ),
                media_type=payload["media_type"],
            )
        else:
            content = self._hydrate_json_value(
                payload["value_root"],
                max_output_bytes=payload["byte_size"],
                _base_reference_depth=_base_reference_depth + 1,
            )
        if (
            len(content) != payload["byte_size"]
            or hashlib.sha256(content).hexdigest() != payload["sha256"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if record_type == "source_metadata":
            self._validate_source_metadata_projection(content)
        if isinstance(record, RecordReceipt) and (
            record.record_id != document["record_id"]
            or record.record_type != document["record_type"]
            or record.sequence_kind != _RECORD_TYPES[record_type][0]
            or record.ordinal != document["ordinal"]
            or record.payload_size != len(content)
            or record.payload_sha256 != payload["sha256"]
            or record.payload_root_digest
            != _require_exact_string(
                payload[
                    "chunks_root"
                    if payload["representation"] == "opaque_chunks_v1"
                    else "value_root"
                ].get("digest")
            )
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return content

    def build_record_sequence(
        self,
        sequence_kind: str,
        records: Iterable[RecordReceipt],
    ) -> SequenceReceipt | None:
        with self._reference_budget():
            return self._build_record_sequence(sequence_kind, records)

    def _build_record_sequence(
        self,
        sequence_kind: str,
        records: Iterable[RecordReceipt],
    ) -> SequenceReceipt | None:
        if sequence_kind not in _RECORD_SEQUENCE_TYPES:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        leaves: list[SequenceLeaf] = []
        for expected_ordinal, record in enumerate(records):
            if (
                not isinstance(record, RecordReceipt)
                or record.sequence_kind != sequence_kind
                or record.ordinal != expected_ordinal
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            leaves.append(SequenceLeaf(record.ref, record.payload_size))
        return self.build_sequence(sequence_kind, leaves)

    def append_record(
        self,
        previous: SequenceReceipt | None,
        record: RecordReceipt,
    ) -> SequenceReceipt:
        with self._reference_budget():
            return self._append_record(previous, record)

    def _append_record(
        self,
        previous: SequenceReceipt | None,
        record: RecordReceipt,
    ) -> SequenceReceipt:
        if not isinstance(record, RecordReceipt) or record.ordinal != (
            0 if previous is None else previous.item_count
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return self.append_sequence(
            previous,
            SequenceLeaf(record.ref, record.payload_size),
            sequence_kind=record.sequence_kind,
        )

    def _receipt_from_root(
        self,
        root_ref: Mapping[str, Any],
        sequence_kind: str,
    ) -> SequenceReceipt:
        node = self._load_index_document(root_ref, sequence_kind=sequence_kind)
        if node["children"][0]["ordinal_start"] != 0 or (
            node["level"] > 0 and len(node["children"]) == 1
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return SequenceReceipt(
            _copy_ref(root_ref),
            sequence_kind,
            node["level"],
            node["item_count"],
            node["logical_bytes"],
        )

    @staticmethod
    def _limits_document(storage: PromptArchiveStorage) -> dict[str, Any]:
        return {
            "profile": "prompt-archive-limits-v1",
            "max_blob_uncompressed_bytes": storage.limits.max_blob_uncompressed_bytes,
            "max_blob_stored_bytes": storage.limits.max_blob_stored_bytes,
            "max_manifest_bytes": _MAX_MANIFEST_BYTES,
            "max_hydrated_bytes": storage.limits.max_hydrated_bytes,
            "max_render_literal_bytes": 8 * _MIB,
            "max_referenced_stored_bytes": 384 * _MIB,
            "max_referenced_uncompressed_bytes": 384 * _MIB,
            "max_reference_depth": 512,
            "max_reference_count": storage.limits.max_referenced_objects,
            "max_index_fanout": _FANOUT,
            "max_control_json_depth": 128,
            "max_json_value_depth": 128,
            "max_json_value_nodes": min(
                storage.limits.max_referenced_objects,
                1_000_000,
            ),
            "max_json_edges": min(
                storage.limits.max_referenced_objects,
                1_000_000,
            ),
            "max_compression_ratio": storage.limits.max_compression_ratio,
        }

    @staticmethod
    def _validate_limits_document(value: Any) -> dict[str, Any]:
        if type(value) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        expected = {
            "profile",
            "max_blob_uncompressed_bytes",
            "max_blob_stored_bytes",
            "max_manifest_bytes",
            "max_hydrated_bytes",
            "max_render_literal_bytes",
            "max_referenced_stored_bytes",
            "max_referenced_uncompressed_bytes",
            "max_reference_depth",
            "max_reference_count",
            "max_index_fanout",
            "max_control_json_depth",
            "max_json_value_depth",
            "max_json_value_nodes",
            "max_json_edges",
            "max_compression_ratio",
        }
        if set(value) != expected or value.get("profile") != "prompt-archive-limits-v1":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        ranged = {
            "max_blob_uncompressed_bytes": (1, 16 * _MIB),
            "max_blob_stored_bytes": (1, 16 * _MIB + 64 * 1024),
            "max_manifest_bytes": (1, _MIB),
            "max_hydrated_bytes": (1, 512 * _MIB),
            "max_referenced_stored_bytes": (1, 768 * _MIB),
            "max_referenced_uncompressed_bytes": (1, 768 * _MIB),
            "max_reference_count": (1, 1_000_000),
            "max_control_json_depth": (1, 128),
            "max_json_value_depth": (1, 128),
            "max_json_value_nodes": (1, 1_000_000),
            "max_json_edges": (1, 1_000_000),
            "max_compression_ratio": (1, 4096),
        }
        for name, (minimum, maximum) in ranged.items():
            candidate = value.get(name)
            if type(candidate) is not int or not minimum <= candidate <= maximum:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value.get("max_render_literal_bytes") != 8 * _MIB
            or value.get("max_reference_depth") != 512
            or value.get("max_index_fanout") != _FANOUT
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return dict(value)

    def validate_runtime_occurrence(
        self,
        occurrence: ArchiveOccurrence,
        *,
        publication_mode: str,
    ) -> None:
        """Validate a runtime occurrence before any durable sidecar write.

        Source-metadata projection proofs are persisted before publication
        plans.  This preflight therefore applies the same closed occurrence,
        source, authorization, timestamp, and optional projection contract as
        the archive builder before a proof store can observe the occurrence.
        """

        source_kind = {
            "shadow": "runtime_shadow",
            "authoritative": "native_runtime",
        }.get(publication_mode)
        if source_kind is None:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        self._validate_occurrence(occurrence, source_kind=source_kind)
        self._validate_occurrence_source_metadata(occurrence)
        if self.sprint_id is None or occurrence.occurrence_kind == "migration":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _validate_occurrence(
        self,
        occurrence: ArchiveOccurrence,
        *,
        source_kind: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        if not isinstance(occurrence, ArchiveOccurrence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if occurrence.project_id != self.project_id or occurrence.sprint_id != self.sprint_id:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            type(occurrence.response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(occurrence.response_id) is None
            or occurrence.response_kind
            not in {
                "identity-response",
                "transition-response",
                "state-response",
                "migration-snapshot",
            }
            or occurrence.occurrence_kind
            not in {"transition", "identity-state", "migration"}
            or type(occurrence.occurrence_id) is not str
            or _SAFE_ID_RE.fullmatch(occurrence.occurrence_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if occurrence.response_id != response_id_v1(
            occurrence.project_id,
            occurrence.sprint_id,
            occurrence.response_kind,
            occurrence.occurrence_kind,
            occurrence.occurrence_id,
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if occurrence.occurrence_kind == "transition":
            if (
                type(occurrence.transition_id) is not str
                or _SAFE_ID_RE.fullmatch(occurrence.transition_id) is None
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        elif occurrence.transition_id is not None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if occurrence.occurrence_kind == "migration" and (
            occurrence.response_kind != "migration-snapshot" or occurrence.sprint_id is not None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if occurrence.occurrence_kind != "migration" and occurrence.response_kind == "migration-snapshot":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_rfc3339(occurrence.created_at)
        _require_rfc3339(occurrence.verified_at)
        if (
            type(occurrence.state_revision) is not int
            or not 0 <= occurrence.state_revision <= _MAX_INT64
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        source_fact = (
            occurrence.source_response_key,
            occurrence.source_byte_size,
            occurrence.source_sha256,
        )
        if any(value is not None for value in source_fact):
            if (
                type(source_fact[0]) is not str
                or _SAFE_ID_RE.fullmatch(source_fact[0]) is None
                or type(source_fact[1]) is not int
                or not 0 <= source_fact[1] <= self.storage.limits.max_hydrated_bytes
                or type(source_fact[2]) is not str
                or _SHA256_RE.fullmatch(source_fact[2]) is None
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        source_raw = canonical_json_bytes_v1(
            occurrence.source,
            maximum=16 * 1024,
        )
        source = _load_canonical_control(source_raw)
        allowed_source = {
            "source_kind",
            "source_commit",
            "assignment_id",
            "review_id",
            "result_key",
            "predecessor_archive_id",
        }
        if not set(source).issubset(allowed_source) or source.get("source_kind") != source_kind:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        for field in ("assignment_id", "review_id", "result_key"):
            if field in source and (
                type(source[field]) is not str or _SAFE_ID_RE.fullmatch(source[field]) is None
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if "source_commit" in source and (
            type(source["source_commit"]) is not str
            or re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", source["source_commit"], re.ASCII)
            is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        predecessor = source.get("predecessor_archive_id")
        if predecessor is not None and (
            type(predecessor) is not str
            or re.fullmatch(r"par1-[0-9a-f]{64}", predecessor, re.ASCII) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        authorization_raw = canonical_json_bytes_v1(
            occurrence.authorization,
            maximum=16 * 1024,
        )
        authorization = _load_canonical_control(authorization_raw)
        required_authorization = {
            "auth_model_version",
            "principal_kind",
            "principal_id",
            "binding_id",
            "scope_source",
            "effective_scope_revision",
            "effective_scope_sha256",
            "decision_occurrence_id",
        }
        if set(authorization) != required_authorization or authorization.get(
            "auth_model_version"
        ) != "prompt-archive-authorization-v1":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        for field in ("principal_id", "decision_occurrence_id"):
            if (
                type(authorization[field]) is not str
                or _SAFE_ID_RE.fullmatch(authorization[field]) is None
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        principal = authorization.get("principal_kind")
        combination = {
            "managed_agent": ("managed_effective_scope", True),
            "legacy_local_principal": ("legacy_project_context", False),
            "offline_operator": ("offline_operator_gate", False),
            "synthetic_test": ("synthetic_test", False),
        }.get(principal)
        if combination is None or authorization.get("scope_source") != combination[0]:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if combination[1]:
            if (
                type(authorization.get("binding_id")) is not str
                or _SAFE_ID_RE.fullmatch(authorization["binding_id"]) is None
                or type(authorization.get("effective_scope_revision")) is not int
                or not 0 <= authorization["effective_scope_revision"] <= _MAX_INT64
                or type(authorization.get("effective_scope_sha256")) is not str
                or _SHA256_RE.fullmatch(authorization["effective_scope_sha256"]) is None
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        elif any(
            authorization.get(field) is not None
            for field in ("binding_id", "effective_scope_revision", "effective_scope_sha256")
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        scope = {
            "project_id": self.project_id,
            "sprint_id": self.sprint_id,
            "response_id": occurrence.response_id,
            "response_kind": occurrence.response_kind,
            "occurrence_kind": occurrence.occurrence_kind,
            "occurrence_id": occurrence.occurrence_id,
            "transition_id": occurrence.transition_id,
        }
        # Freeze and cross-bind the optional sidecar before any caller can
        # proceed to render-plan or CAS construction.  The values are returned
        # separately by the build path; validation here makes the occurrence
        # contract itself fail closed as well.
        self._validate_occurrence_source_metadata(occurrence)
        return scope, source, authorization

    @staticmethod
    def _archive_id(
        *,
        archive_format: str,
        scope: Mapping[str, Any],
        roots: Mapping[str, Mapping[str, Any]],
        hydrated_size: int,
        hydrated_sha256: str,
        envelope_sha256: str | None,
    ) -> str:
        fields: list[tuple[str, str | None]] = [
            ("archive_format", archive_format),
            ("project_id", scope["project_id"]),
            ("sprint_id", scope["sprint_id"]),
            ("response_id", scope["response_id"]),
            ("envelope_sha256", envelope_sha256),
        ]
        for root_name in _ROOT_ORDER:
            root_ref = roots.get(root_name)
            fields.append(
                (
                    f"{root_name}_root_digest",
                    None if root_ref is None else _require_exact_string(root_ref.get("digest")),
                )
            )
        fields.extend(
            [
                ("hydrated_size", str(hydrated_size)),
                ("hydrated_sha256", hydrated_sha256),
            ]
        )
        return "par1-" + pas1_id_v1("archive-id", fields)

    def _raw_graph_counts(self, receipt: SequenceReceipt) -> dict[str, int]:
        seen: set[str] = set()
        digest_roles: dict[str, tuple[str, str]] = {}
        completed_indexes: set[str] = set()
        active_indexes: set[str] = set()
        raw_chunk_digests: set[str] = set()
        records = raw_chunks = index_nodes = graph_edges = 0
        stored_bytes = uncompressed_bytes = 0

        def add_ref(ref: Mapping[str, Any]) -> None:
            nonlocal stored_bytes, uncompressed_bytes
            value = _copy_ref(ref)
            digest, _logical_kind, _media_type = _register_digest_role(
                digest_roles,
                value,
            )
            if digest not in seen:
                seen.add(digest)
                stored_bytes += _require_exact_int(value.get("stored_size"))
                uncompressed_bytes += _require_exact_int(value.get("uncompressed_size"))

        def visit(ref: Mapping[str, Any], level: int) -> None:
            nonlocal raw_chunks, index_nodes, graph_edges
            add_ref(ref)
            digest = _require_exact_string(ref.get("digest"))
            if digest in active_indexes:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if digest in completed_indexes:
                return
            active_indexes.add(digest)
            index_nodes += 1
            try:
                node = self._load_index_document(
                    ref,
                    sequence_kind="raw_bytes",
                    expected_level=level,
                )
                graph_edges += len(node["children"])
                for child in node["children"]:
                    if level == 0:
                        add_ref(child["ref"])
                        chunk = self._read_blob(
                            child["ref"],
                            logical_kind="raw_chunk",
                            media_type="application/json",
                        )
                        declared_bytes = _require_exact_int(child["logical_bytes"])
                        if len(chunk) != declared_bytes:
                            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                        ordinal = _require_exact_int(child["ordinal_start"])
                        if receipt.item_count == 1 and receipt.logical_bytes == 0:
                            if chunk != b"":
                                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                        elif ordinal < receipt.item_count - 1:
                            if len(chunk) != _CHUNK_BYTES:
                                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                        elif not 1 <= len(chunk) <= _CHUNK_BYTES:
                            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                        chunk_digest = _require_exact_string(child["ref"].get("digest"))
                        raw_chunk_digests.add(chunk_digest)
                    else:
                        visit(child["ref"], level - 1)
            finally:
                active_indexes.remove(digest)
            completed_indexes.add(digest)

        visit(receipt.root_ref, receipt.level)
        raw_chunks = len(raw_chunk_digests)
        return {
            "records": records,
            "raw_chunks": raw_chunks,
            "payload_chunks": 0,
            "literal_chunks": 0,
            "render_plan_chunks": 0,
            "json_value_nodes": 0,
            "json_edges": 0,
            "index_nodes": index_nodes,
            "unique_referenced_objects": len(seen),
            "graph_edges": graph_edges,
            "logical_bytes": receipt.logical_bytes,
            "referenced_stored_bytes": stored_bytes,
            "referenced_uncompressed_bytes": uncompressed_bytes,
        }

    @staticmethod
    def _native_source_bytes(
        raw_bytes: bytes | bytearray | memoryview,
        *,
        maximum: int,
    ) -> bytes:
        try:
            view = memoryview(raw_bytes)
            try:
                if view.nbytes > maximum:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                return view.tobytes()
            finally:
                view.release()
        except PromptArchiveStorageError:
            raise
        except (TypeError, ValueError, BufferError, MemoryError):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None

    @staticmethod
    def _canonical_b64(raw: bytes) -> str:
        if len(raw) > 4096:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        return base64.b64encode(raw).decode("ascii")

    @staticmethod
    def _decode_canonical_b64(value: Any) -> bytes:
        if type(value) is not str or len(value) > 5464:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            encoded = value.encode("ascii", errors="strict")
            raw = base64.b64decode(encoded, validate=True)
        except (UnicodeError, ValueError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if len(raw) > 4096 or base64.b64encode(raw) != encoded:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return raw

    @staticmethod
    def _record_provenance(occurrence: ArchiveOccurrence) -> dict[str, Any]:
        provenance: dict[str, Any] = {
            "source_kind": (
                "runtime_transition"
                if occurrence.occurrence_kind == "transition"
                else "runtime_identity"
            ),
            "origin_response_id": occurrence.response_id,
            "origin_transition_id": occurrence.transition_id,
            "source_id": occurrence.occurrence_id,
        }
        for field in ("source_commit", "assignment_id", "review_id", "result_key"):
            if field in occurrence.source:
                provenance[field] = occurrence.source[field]
        return provenance

    @staticmethod
    def _candidate_value_spans(root: JsonNode) -> list[tuple[str, tuple[str, ...], JsonNode]]:
        candidates: list[tuple[str, tuple[str, ...], JsonNode]] = []
        targets = tuple(_PROMOTED_SLOTS)

        def walk(node: JsonNode, path: tuple[str, ...]) -> None:
            if path in _PROMOTED_SLOTS:
                candidates.append(("sequence", path, node))
                return
            has_target_below = any(
                len(target) > len(path) and target[: len(path)] == path
                for target in targets
            )
            if has_target_below and node.node_kind == "object":
                for edge in node.edges:
                    assert edge.member_name is not None
                    walk(edge.value, path + (edge.member_name,))
                return
            candidates.append(("current_field", path, node))

        walk(root, ())
        candidates.sort(key=lambda candidate: candidate[2].start)
        previous_end = 0
        for _kind, _path, node in candidates:
            if node.start < previous_end or node.end <= node.start:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            previous_end = node.end
        return candidates

    @staticmethod
    def _empty_promoted_candidate(path: tuple[str, ...], node: JsonNode) -> bool:
        sequence_kind = _PROMOTED_SLOTS[path][1]
        if sequence_kind == "history_text":
            return (
                node.node_kind == "scalar"
                and node.scalar_kind == "string"
                and node.end - node.start == 2
            )
        return node.node_kind == "array" and not node.edges

    def _previous_sequence(
        self,
        previous_roots: Mapping[str, Mapping[str, Any]],
        root_name: str,
        sequence_kind: str,
    ) -> SequenceReceipt | None:
        ref = previous_roots.get(root_name)
        if ref is None:
            return None
        receipt = self._receipt_from_root(ref, sequence_kind)
        self.sequence_leaves(receipt)
        return receipt

    def _promoted_sequence(
        self,
        source: bytes,
        path: tuple[str, ...],
        node: JsonNode,
        occurrence: ArchiveOccurrence,
        previous_roots: Mapping[str, Mapping[str, Any]],
    ) -> tuple[SequenceReceipt, dict[str, Any]] | None:
        root_name, sequence_kind, record_type = _PROMOTED_SLOTS[path]
        payloads: list[bytes]
        if sequence_kind == "history_text":
            if node.node_kind != "scalar" or node.scalar_kind != "string":
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            token = node.bytes_from(source)
            if len(token) < 2 or token[:1] != b'"' or token[-1:] != b'"':
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            interior = token[1:-1]
            if not interior:
                return None
            payloads = [interior]
            prefix = b'"'
            separator = b""
            suffix = b'"'
        else:
            if node.node_kind != "array":
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            if not node.edges:
                return None
            payloads = [edge.value.bytes_from(source) for edge in node.edges]
            prefix = source[node.start : node.edges[0].value.start]
            separators = [
                source[previous.value.end : current.value.start]
                for previous, current in zip(node.edges, node.edges[1:])
            ]
            separator = separators[0] if separators else b""
            if any(item != separator for item in separators):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            suffix = source[node.edges[-1].value.end : node.end]
        for framing in (prefix, separator, suffix):
            if len(framing) > 4096:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

        previous = self._previous_sequence(previous_roots, root_name, sequence_kind)
        previous_payloads: list[bytes] = []
        if previous is not None:
            previous_payloads = [
                self._hydrate_record(leaf.ref, expected_record_type=record_type)
                for leaf in self.sequence_leaves(previous)
            ]

        provenance = self._record_provenance(occurrence)
        if sequence_kind == "history_text":
            old = b"".join(previous_payloads)
            new = payloads[0]
            if previous is not None and new.startswith(old):
                sequence = previous
                if len(new) > len(old):
                    record = self._build_record(
                        record_type,
                        new[len(old) :],
                        ordinal=previous.item_count,
                        provenance=provenance,
                        occurred_at=occurrence.created_at,
                    )
                    sequence = self.append_record(previous, record)
                assert sequence is not None
            else:
                record = self._build_record(
                    record_type,
                    new,
                    ordinal=0,
                    provenance=provenance,
                    occurred_at=occurrence.created_at,
                )
                sequence = self.build_record_sequence(sequence_kind, [record])
                assert sequence is not None
        elif (
            previous is not None
            and len(previous_payloads) <= len(payloads)
            and previous_payloads == payloads[: len(previous_payloads)]
        ):
            sequence = previous
            for ordinal, payload in enumerate(
                payloads[len(previous_payloads) :],
                start=len(previous_payloads),
            ):
                record = self._build_record(
                    record_type,
                    payload,
                    ordinal=ordinal,
                    provenance=provenance,
                    occurred_at=occurrence.created_at,
                )
                sequence = self.append_record(sequence, record)
        else:
            records = [
                self._build_record(
                    record_type,
                    payload,
                    ordinal=ordinal,
                    provenance=provenance,
                    occurred_at=occurrence.created_at,
                )
                for ordinal, payload in enumerate(payloads)
            ]
            sequence = self.build_record_sequence(sequence_kind, records)
            assert sequence is not None

        step = {
            "step_kind": "sequence",
            "source_start": node.start,
            "source_end": node.end,
            "slot_id": _SLOT_IDS_BY_ROOT[root_name],
            "sequence_kind": sequence_kind,
            "index_root": _copy_ref(sequence.root_ref),
            "prefix_b64": self._canonical_b64(prefix),
            "separator_b64": self._canonical_b64(separator),
            "suffix_b64": self._canonical_b64(suffix),
        }
        return sequence, step

    def _build_native_plan(
        self,
        source: bytes,
        occurrence: ArchiveOccurrence,
        previous_roots: Mapping[str, Mapping[str, Any]] | None,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any], ByteStreamReceipt]:
        if previous_roots is None:
            frozen_previous: dict[str, Mapping[str, Any]] = {}
        elif isinstance(previous_roots, Mapping):
            frozen_previous = dict(previous_roots)
            if (
                any(type(name) is not str for name in frozen_previous)
                or not set(frozen_previous).issubset(_ROOT_SEQUENCE_KINDS)
                or any(not isinstance(ref, Mapping) for ref in frozen_previous.values())
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        else:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        try:
            parsed = parse_json_bytes(
                source,
                max_depth=128,
                max_nodes=min(self.storage.limits.max_referenced_objects, 1_000_000),
                max_edges=min(self.storage.limits.max_referenced_objects, 1_000_000),
            )
        except JsonSpanParseError:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None

        roots: dict[str, dict[str, Any]] = {}
        value_steps: list[dict[str, Any]] = []
        current_leaves: list[SequenceLeaf] = []
        provenance = self._record_provenance(occurrence)
        previous_current = self._previous_sequence(
            frozen_previous,
            "current_fields",
            "current_fields",
        )
        previous_current_leaves = (
            [] if previous_current is None else self.sequence_leaves(previous_current)
        )

        def current_leaf(payload: bytes, ordinal: int) -> SequenceLeaf:
            if ordinal < len(previous_current_leaves):
                predecessor = previous_current_leaves[ordinal]
                if (
                    predecessor.logical_bytes == len(payload)
                    and self._hydrate_record(
                        predecessor.ref,
                        expected_record_type="current_field",
                    )
                    == payload
                ):
                    # Preserve the exact record ref, including the provenance
                    # of the transition that last changed this stable ordinal.
                    return SequenceLeaf(
                        _copy_ref(predecessor.ref),
                        predecessor.logical_bytes,
                    )
            record = self._build_record(
                "current_field",
                payload,
                ordinal=ordinal,
                provenance=provenance,
                occurred_at=occurrence.created_at,
            )
            return SequenceLeaf(_copy_ref(record.ref), record.payload_size)

        candidates = self._candidate_value_spans(parsed)
        for kind, path, node in candidates:
            if kind != "sequence":
                continue
            sequence_kind = _PROMOTED_SLOTS[path][1]
            if (
                sequence_kind == "history_text"
                and (node.node_kind != "scalar" or node.scalar_kind != "string")
            ) or (sequence_kind != "history_text" and node.node_kind != "array"):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        for candidate_kind, path, node in candidates:
            if candidate_kind == "sequence":
                promoted = self._promoted_sequence(
                    source,
                    path,
                    node,
                    occurrence,
                    frozen_previous,
                )
                if promoted is not None:
                    receipt, step = promoted
                    root_name = _PROMOTED_SLOTS[path][0]
                    roots[root_name] = _copy_ref(receipt.root_ref)
                    value_steps.append(step)
                    continue
            ordinal = len(current_leaves)
            leaf = current_leaf(node.bytes_from(source), ordinal)
            current_leaves.append(leaf)
            value_steps.append(
                {
                    "step_kind": "current_field",
                    "source_start": node.start,
                    "source_end": node.end,
                    "value_ordinal": ordinal,
                    "record_ref": _copy_ref(leaf.ref),
                }
            )

        if current_leaves:
            if previous_current is None:
                current_sequence = self.build_sequence(
                    "current_fields",
                    current_leaves,
                )
                assert current_sequence is not None
            else:
                overlap = min(previous_current.item_count, len(current_leaves))
                replacements = {
                    ordinal: current_leaves[ordinal]
                    for ordinal in range(overlap)
                    if (
                        _copy_ref(current_leaves[ordinal].ref)
                        != _copy_ref(previous_current_leaves[ordinal].ref)
                        or current_leaves[ordinal].logical_bytes
                        != previous_current_leaves[ordinal].logical_bytes
                    )
                }
                current_sequence = self._replace_sequence_leaves(
                    previous_current,
                    replacements,
                    sequence_kind="current_fields",
                )
                if len(current_leaves) < current_sequence.item_count:
                    current_sequence = self._truncate_sequence(
                        current_sequence,
                        len(current_leaves),
                        sequence_kind="current_fields",
                    )
                else:
                    for leaf in current_leaves[current_sequence.item_count :]:
                        current_sequence = self._append_sequence(
                            current_sequence,
                            leaf,
                            sequence_kind="current_fields",
                        )
            roots["current_fields"] = _copy_ref(current_sequence.root_ref)

        steps: list[dict[str, Any]] = []
        cursor = 0
        literal_total = 0

        def add_literals(start: int, end: int) -> None:
            nonlocal literal_total
            offset = start
            while offset < end:
                next_offset = min(end, offset + 65_536)
                span = ByteSpan(offset, next_offset)
                receipt = self._store_literal_span(source, span)
                literal_total += span.byte_size
                if literal_total > 8 * _MIB:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                steps.append(
                    {
                        "step_kind": "literal",
                        "source_start": offset,
                        "source_end": next_offset,
                        "chunks_root": _copy_ref(receipt.sequence.root_ref),
                        "byte_size": span.byte_size,
                        "sha256": receipt.sha256,
                    }
                )
                offset = next_offset

        for step in sorted(value_steps, key=lambda item: item["source_start"]):
            start = _require_exact_int(step.get("source_start"))
            end = _require_exact_int(step.get("source_end"), minimum=1)
            if start < cursor or end <= start or end > len(source):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            add_literals(cursor, start)
            steps.append(step)
            cursor = end
        add_literals(cursor, len(source))
        if not 1 <= len(steps) <= 250_000:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

        envelope = {
            "schema_version": 1,
            "kind": "prompt_archive_response_envelope",
            "media_type": "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
            "builder_version": "pas1-render-plan-builder-v1",
            "renderer_version": "pas1-concat-renderer-v1",
            "json_value_model": "pas1-recursive-json-merkle-v1",
            "response_id": occurrence.response_id,
            "response_kind": occurrence.response_kind,
            "source_byte_size": len(source),
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "steps": steps,
        }
        envelope_bytes = canonical_json_bytes_v1(
            envelope,
            maximum=self.storage.limits.max_hydrated_bytes,
        )
        envelope_stream = self.store_byte_stream(
            envelope_bytes,
            sequence_kind="render_plan_chunks",
        )
        rendered = self._render_native_envelope(envelope, roots)
        if rendered != source:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return roots, envelope, envelope_stream

    def _render_native_envelope(
        self,
        envelope: Mapping[str, Any],
        roots: Mapping[str, Mapping[str, Any]],
    ) -> bytes:
        if type(envelope) is not dict or set(envelope) != {
            "schema_version",
            "kind",
            "media_type",
            "builder_version",
            "renderer_version",
            "json_value_model",
            "response_id",
            "response_kind",
            "source_byte_size",
            "source_sha256",
            "steps",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            envelope.get("schema_version") != 1
            or envelope.get("kind") != "prompt_archive_response_envelope"
            or envelope.get("media_type")
            != "application/vnd.nginx-qa.prompt-response-envelope-v1+json"
            or envelope.get("builder_version") != "pas1-render-plan-builder-v1"
            or envelope.get("renderer_version") != "pas1-concat-renderer-v1"
            or envelope.get("json_value_model") != "pas1-recursive-json-merkle-v1"
            or type(envelope.get("response_id")) is not str
            or _RESPONSE_ID_RE.fullmatch(envelope["response_id"]) is None
            or envelope.get("response_kind")
            not in {"identity-response", "transition-response", "state-response"}
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        source_size = _require_exact_int(envelope.get("source_byte_size"))
        source_digest = _require_exact_string(envelope.get("source_sha256"))
        if _SHA256_RE.fullmatch(source_digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if source_size > self.storage.limits.max_hydrated_bytes:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        steps = envelope.get("steps")
        if type(steps) is not list or not 1 <= len(steps) <= 250_000:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if type(roots) is not dict or not roots or (
            not set(roots).issubset(_ROOT_SEQUENCE_KINDS)
            or "raw_bytes" in roots
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        receipts = {
            name: self._receipt_from_root(ref, _ROOT_SEQUENCE_KINDS[name])
            for name, ref in roots.items()
        }
        # source_metadata is an occurrence-scoped, non-rendered sidecar.  It is
        # still part of the manifest reachability graph and archive identity,
        # but must never change executable response bytes or become an adapter
        # instruction.  Validate the closed one-record root before rendering.
        rendered_root_names = set(roots)
        if "source_metadata" in roots:
            self._source_metadata_from_root(roots["source_metadata"])
            rendered_root_names.remove("source_metadata")
        current_leaves = (
            self.sequence_leaves(receipts["current_fields"])
            if "current_fields" in receipts
            else []
        )
        current_ordinal = 0
        used_roots = {"current_fields"} if "current_fields" in receipts else set()
        used_slots: set[str] = set()
        sequence_observations: dict[
            str, tuple[bytes, bytes, bytes, list[bytes]]
        ] = {}
        literal_total = 0
        cursor = 0
        output_parts: list[bytes] = []
        observed_non_literals: list[tuple[str, int, int, str | None]] = []

        for step in steps:
            self._check_cancelled()
            if type(step) is not dict:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            start = _require_exact_int(step.get("source_start"))
            end = _require_exact_int(step.get("source_end"), minimum=1)
            if start != cursor or end <= start or end > source_size:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            kind = step.get("step_kind")
            if kind == "literal":
                if set(step) != {
                    "step_kind",
                    "source_start",
                    "source_end",
                    "chunks_root",
                    "byte_size",
                    "sha256",
                }:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                byte_size = _require_exact_int(step.get("byte_size"), minimum=1)
                digest = _require_exact_string(step.get("sha256"))
                if byte_size != end - start or byte_size > 65_536 or (
                    _SHA256_RE.fullmatch(digest) is None
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                literal_total += byte_size
                if literal_total > 8 * _MIB:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                receipt = self._receipt_from_root(step["chunks_root"], "literal_chunks")
                leaves = self.sequence_leaves(receipt)
                raw = self.hydrate_byte_stream(
                    ByteStreamReceipt(receipt, byte_size, digest, len(leaves))
                )
            elif kind == "current_field":
                if set(step) != {
                    "step_kind",
                    "source_start",
                    "source_end",
                    "value_ordinal",
                    "record_ref",
                }:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                ordinal = _require_exact_int(step.get("value_ordinal"))
                if ordinal != current_ordinal or ordinal >= len(current_leaves) or (
                    _copy_ref(step["record_ref"]) != _copy_ref(current_leaves[ordinal].ref)
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                raw = self._hydrate_record(
                    step["record_ref"],
                    expected_record_type="current_field",
                )
                current_ordinal += 1
                observed_non_literals.append((kind, start, end, None))
            elif kind == "sequence":
                if set(step) != {
                    "step_kind",
                    "source_start",
                    "source_end",
                    "slot_id",
                    "sequence_kind",
                    "index_root",
                    "prefix_b64",
                    "separator_b64",
                    "suffix_b64",
                }:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                slot_id = step.get("slot_id")
                matching = [
                    name for name, known_slot in _SLOT_IDS_BY_ROOT.items() if known_slot == slot_id
                ]
                if len(matching) != 1 or slot_id in used_slots:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                root_name = matching[0]
                expected_kind = _ROOT_SEQUENCE_KINDS[root_name]
                if (
                    root_name not in receipts
                    or step.get("sequence_kind") != expected_kind
                    or _copy_ref(step["index_root"]) != _copy_ref(roots[root_name])
                ):
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                used_slots.add(slot_id)
                used_roots.add(root_name)
                prefix = self._decode_canonical_b64(step.get("prefix_b64"))
                separator = self._decode_canonical_b64(step.get("separator_b64"))
                suffix = self._decode_canonical_b64(step.get("suffix_b64"))
                record_type = self._record_type_for_sequence(expected_kind)
                assert record_type is not None
                payloads = [
                    self._hydrate_record(leaf.ref, expected_record_type=record_type)
                    for leaf in self.sequence_leaves(receipts[root_name])
                ]
                raw = prefix + separator.join(payloads) + suffix
                sequence_observations[slot_id] = (
                    prefix,
                    separator,
                    suffix,
                    payloads,
                )
                observed_non_literals.append((kind, start, end, slot_id))
            else:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if len(raw) != end - start:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            output_parts.append(raw)
            cursor = end

        if (
            cursor != source_size
            or current_ordinal != len(current_leaves)
            or used_roots != rendered_root_names
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        output = b"".join(output_parts)
        if len(output) != source_size or hashlib.sha256(output).hexdigest() != source_digest:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

        # Re-derive the unique V1 partition from the reconstructed source so a
        # caller cannot substitute a byte-equivalent alternate render plan.
        try:
            parsed = parse_json_bytes(
                output,
                max_depth=128,
                max_nodes=min(self.storage.limits.max_referenced_objects, 1_000_000),
                max_edges=min(self.storage.limits.max_referenced_objects, 1_000_000),
            )
        except JsonSpanParseError:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") from None
        expected_non_literals: list[tuple[str, int, int, str | None]] = []
        candidates = self._candidate_value_spans(parsed)
        for candidate_kind, path, node in candidates:
            self._check_cancelled()
            if candidate_kind != "sequence":
                continue
            sequence_kind = _PROMOTED_SLOTS[path][1]
            if (
                sequence_kind == "history_text"
                and (node.node_kind != "scalar" or node.scalar_kind != "string")
            ) or (sequence_kind != "history_text" and node.node_kind != "array"):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        for candidate_kind, path, node in candidates:
            self._check_cancelled()
            if candidate_kind == "sequence":
                _root_name, sequence_kind, _record_type = _PROMOTED_SLOTS[path]
                if sequence_kind == "history_text":
                    if node.node_kind != "scalar" or node.scalar_kind != "string":
                        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                elif node.node_kind != "array":
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                is_empty = self._empty_promoted_candidate(path, node)
                if not is_empty:
                    slot_id = _SLOT_IDS_BY_ROOT[_root_name]
                    observation = sequence_observations.get(slot_id)
                    if observation is None:
                        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                    prefix, separator, suffix, payloads = observation
                    if sequence_kind == "history_text":
                        token = node.bytes_from(output)
                        if (
                            prefix != b'"'
                            or separator != b""
                            or suffix != b'"'
                            or b"".join(payloads) != token[1:-1]
                        ):
                            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                    else:
                        expected_payloads = [
                            edge.value.bytes_from(output) for edge in node.edges
                        ]
                        expected_prefix = output[
                            node.start : node.edges[0].value.start
                        ]
                        expected_separators = [
                            output[previous.value.end : current.value.start]
                            for previous, current in zip(node.edges, node.edges[1:])
                        ]
                        expected_separator = (
                            expected_separators[0] if expected_separators else b""
                        )
                        if (
                            any(item != expected_separator for item in expected_separators)
                            or prefix != expected_prefix
                            or separator != expected_separator
                            or suffix
                            != output[node.edges[-1].value.end : node.end]
                            or payloads != expected_payloads
                        ):
                            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                    expected_non_literals.append(
                        (
                            "sequence",
                            node.start,
                            node.end,
                            slot_id,
                        )
                    )
                    continue
            expected_non_literals.append(("current_field", node.start, node.end, None))
        if observed_non_literals != expected_non_literals:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        expected_step_ranges: list[tuple[str, int, int]] = []
        gap_cursor = 0
        for kind, start, end, _slot in expected_non_literals:
            self._check_cancelled()
            offset = gap_cursor
            while offset < start:
                self._check_cancelled()
                next_offset = min(start, offset + 65_536)
                expected_step_ranges.append(("literal", offset, next_offset))
                offset = next_offset
            expected_step_ranges.append((kind, start, end))
            gap_cursor = end
        while gap_cursor < source_size:
            self._check_cancelled()
            next_offset = min(source_size, gap_cursor + 65_536)
            expected_step_ranges.append(("literal", gap_cursor, next_offset))
            gap_cursor = next_offset
        observed_step_ranges = [
            (step["step_kind"], step["source_start"], step["source_end"])
            for step in steps
        ]
        if observed_step_ranges != expected_step_ranges:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return output

    def _native_graph_counts(
        self,
        roots: Mapping[str, Mapping[str, Any]],
        envelope_stream: ByteStreamReceipt,
        *,
        logical_bytes: int,
    ) -> dict[str, int]:
        seen: dict[str, dict[str, Any]] = {}
        digest_roles: dict[str, tuple[str, str]] = {}
        seen_typed: set[tuple[str, str, str]] = set()
        control_children: dict[
            tuple[str, str, str], list[Mapping[str, Any]]
        ] = {}
        deepest_expansion: dict[tuple[str, str, str], int] = {}
        active: set[tuple[str, str, str]] = set()
        role_digests: dict[str, set[str]] = {
            "records": set(),
            "raw_chunks": set(),
            "payload_chunks": set(),
            "literal_chunks": set(),
            "render_plan_chunks": set(),
            "json_value_nodes": set(),
            "json_edges": set(),
            "index_nodes": set(),
        }
        graph_edges = 0
        totals = {
            "records": 0,
            "raw_chunks": 0,
            "payload_chunks": 0,
            "literal_chunks": 0,
            "render_plan_chunks": 0,
            "json_value_nodes": 0,
            "json_edges": 0,
            "index_nodes": 0,
            "unique_referenced_objects": 0,
            "graph_edges": 0,
            "logical_bytes": logical_bytes,
            "referenced_stored_bytes": 0,
            "referenced_uncompressed_bytes": 0,
        }
        control_kinds = {
            "index_node",
            "json_value",
            "json_edge",
            *set(_RECORD_SEQUENCE_TYPES.values()),
        }

        def nested_refs(value: Any) -> Iterable[Mapping[str, Any]]:
            if isinstance(value, Mapping):
                if value.get("kind") == "prompt_archive_blob_ref":
                    yield value
                    return
                for child in value.values():
                    yield from nested_refs(child)
            elif type(value) is list:
                for child in value:
                    yield from nested_refs(child)

        def visit(ref: Mapping[str, Any], reference_depth: int) -> None:
            nonlocal graph_edges
            self._check_cancelled()
            if reference_depth > self.storage.limits.max_reference_depth:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            copied = _copy_ref(ref)
            digest, logical_kind, media_type = _register_digest_role(
                digest_roles,
                copied,
            )
            typed_key = (digest, logical_kind, media_type)
            physical = {
                key: item
                for key, item in copied.items()
                if key not in {"logical_kind", "media_type"}
            }
            if digest in seen and seen[digest] != physical:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            if digest not in seen:
                stored_size = _require_exact_int(copied.get("stored_size"))
                uncompressed_size = _require_exact_int(copied.get("uncompressed_size"))
                totals["referenced_stored_bytes"] += stored_size
                totals["referenced_uncompressed_bytes"] += uncompressed_size
                if (
                    totals["referenced_stored_bytes"] > _MAX_REFERENCED_STORED_BYTES
                    or totals["referenced_uncompressed_bytes"]
                    > _MAX_REFERENCED_UNCOMPRESSED_BYTES
                ):
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                seen[digest] = physical
                if len(seen) > self.storage.limits.max_referenced_objects:
                    raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            if typed_key not in seen_typed:
                if logical_kind == "index_node":
                    role_digests["index_nodes"].add(digest)
                elif logical_kind == "json_value":
                    role_digests["json_value_nodes"].add(digest)
                elif logical_kind == "json_edge":
                    role_digests["json_edges"].add(digest)
                elif logical_kind.endswith("_record"):
                    role_digests["records"].add(digest)
                elif logical_kind in {
                    "raw_chunk",
                    "payload_chunk",
                    "literal_chunk",
                    "render_plan_chunk",
                }:
                    role_digests[logical_kind + "s"].add(digest)
                else:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
                seen_typed.add(typed_key)
            # Validate every incoming edge before memo reuse.  A shared node
            # reached through a deeper path must also revalidate its subtree,
            # because the same relative descendants may then cross depth 512.
            raw = self._read_blob(
                copied,
                logical_kind=logical_kind,
                media_type=media_type,
            )
            if logical_kind not in control_kinds:
                return
            if typed_key in active:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            if deepest_expansion.get(typed_key, -1) >= reference_depth:
                return
            active.add(typed_key)
            try:
                children = control_children.get(typed_key)
                if children is None:
                    document = _load_canonical_control(raw)
                    children = list(nested_refs(document))
                    graph_edges += len(children)
                    control_children[typed_key] = children
                for child in children:
                    visit(child, reference_depth + 1)
                deepest_expansion[typed_key] = reference_depth
            finally:
                active.remove(typed_key)

        for name in _ROOT_ORDER:
            if name in roots:
                visit(roots[name], 1)
        visit(envelope_stream.sequence.root_ref, 1)
        envelope_raw = self.hydrate_byte_stream(envelope_stream)
        envelope_document = _load_canonical_control(envelope_raw)
        envelope_children = list(nested_refs(envelope_document))
        graph_edges += len(envelope_children)
        for child in envelope_children:
            # The hydrated envelope is a manifest-bound control document; its
            # serialized refs are first-level graph edges just like roots.
            visit(child, 1)
        for name, digests in role_digests.items():
            totals[name] = len(digests)
        totals["unique_referenced_objects"] = len(seen)
        totals["graph_edges"] = graph_edges
        return totals

    def _validate_native_manifest(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(manifest)
        required = {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "archive_id",
            "archive_format",
            "scope",
            "source",
            "authorization",
            "renderer_version",
            "json_value_model",
            "envelope",
            "roots",
            "integrity",
            "counts",
            "limits",
            "publication",
            "created_at",
        }
        allowed = required | {"legacy_comparison", "source_metadata_receipt"}
        if not required.issubset(value) or not set(value).issubset(allowed):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value.get("schema_version") != 1
            or value.get("kind") != "prompt_archive_manifest"
            or value.get("media_type") != _CONTROL_MEDIA["manifest"]
            or value.get("id_algorithm") != "PAS1-ID-v1"
            or value.get("archive_format") != "incremental_transition_v1"
            or value.get("renderer_version") != "pas1-concat-renderer-v1"
            or value.get("json_value_model") != "pas1-recursive-json-merkle-v1"
            or type(value.get("archive_id")) is not str
            or re.fullmatch(r"par1-[0-9a-f]{64}", value["archive_id"], re.ASCII) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        scope = value.get("scope")
        if type(scope) is not dict or set(scope) != {
            "project_id",
            "sprint_id",
            "response_id",
            "response_kind",
            "occurrence_kind",
            "occurrence_id",
            "transition_id",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        publication = value.get("publication")
        if type(publication) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        mode = publication.get("mode")
        if mode == "shadow":
            source_kind = "runtime_shadow"
            if (
                set(publication)
                != {"mode", "legacy_authoritative", "writer_version", "verified_at"}
                or publication.get("legacy_authoritative") is not True
                or "legacy_comparison" not in value
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        elif mode == "authoritative":
            source_kind = "native_runtime"
            if (
                set(publication)
                != {
                    "mode",
                    "legacy_authoritative",
                    "operator_gate",
                    "writer_version",
                    "verified_at",
                }
                or publication.get("legacy_authoritative") is not False
                or publication.get("operator_gate")
                != "prompt-archive-authoritative-v1"
                or "legacy_comparison" in value
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        else:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            type(publication.get("writer_version")) is not str
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}",
                publication["writer_version"],
                re.ASCII,
            )
            is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        occurrence = ArchiveOccurrence(
            project_id=scope.get("project_id"),
            sprint_id=scope.get("sprint_id"),
            response_id=scope.get("response_id"),
            response_kind=scope.get("response_kind"),
            occurrence_kind=scope.get("occurrence_kind"),
            occurrence_id=scope.get("occurrence_id"),
            transition_id=scope.get("transition_id"),
            source=value.get("source"),
            authorization=value.get("authorization"),
            created_at=value.get("created_at"),
            verified_at=publication.get("verified_at"),
        )
        frozen_scope, frozen_source, frozen_authorization = self._validate_occurrence(
            occurrence,
            source_kind=source_kind,
        )
        if (
            self.sprint_id is None
            or frozen_scope != scope
            or frozen_source != value.get("source")
            or frozen_authorization != value.get("authorization")
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        roots = value.get("roots")
        if (
            type(roots) is not dict
            or not roots
            or not set(roots).issubset(_ROOT_SEQUENCE_KINDS)
            or "raw_bytes" in roots
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        for name, ref in roots.items():
            copied = _copy_ref(ref)
            if (
                copied.get("logical_kind") != "index_node"
                or copied.get("media_type") != _CONTROL_MEDIA["index_node"]
                or name not in _ROOT_SEQUENCE_KINDS
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        source_metadata_receipt = value.get("source_metadata_receipt")
        if ("source_metadata" in roots) != (source_metadata_receipt is not None):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if source_metadata_receipt is not None:
            self._validate_source_metadata_receipt(source_metadata_receipt)

        envelope = value.get("envelope")
        if type(envelope) is not dict or set(envelope) != {
            "media_type",
            "byte_size",
            "sha256",
            "chunks_root",
        } or envelope.get("media_type") != (
            "application/vnd.nginx-qa.prompt-response-envelope-v1+json"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_exact_int(envelope.get("byte_size"))
        envelope_digest = _require_exact_string(envelope.get("sha256"))
        envelope_root = _copy_ref(envelope.get("chunks_root"))
        if (
            _SHA256_RE.fullmatch(envelope_digest) is None
            or envelope_root.get("logical_kind") != "index_node"
            or envelope_root.get("media_type") != _CONTROL_MEDIA["index_node"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        integrity = value.get("integrity")
        if type(integrity) is not dict or set(integrity) != {
            "semantics",
            "hydrated_media_type",
            "hydrated_size",
            "hydrated_sha256",
        } or (
            integrity.get("semantics") != "native-byte-equality"
            or integrity.get("hydrated_media_type") != "application/json"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_exact_int(integrity.get("hydrated_size"))
        integrity_digest = _require_exact_string(integrity.get("hydrated_sha256"))
        if _SHA256_RE.fullmatch(integrity_digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        if mode == "shadow":
            comparison = value.get("legacy_comparison")
            if type(comparison) is not dict or set(comparison) != {
                "byte_size",
                "sha256",
                "renderer_version",
                "matched",
            } or (
                comparison.get("byte_size") != integrity["hydrated_size"]
                or comparison.get("sha256") != integrity["hydrated_sha256"]
                or comparison.get("renderer_version") != value["renderer_version"]
                or comparison.get("matched") is not True
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

        self._validate_limits_document(value.get("limits"))
        counts = value.get("counts")
        count_fields = {
            "records",
            "raw_chunks",
            "payload_chunks",
            "literal_chunks",
            "render_plan_chunks",
            "json_value_nodes",
            "json_edges",
            "index_nodes",
            "unique_referenced_objects",
            "graph_edges",
            "logical_bytes",
            "referenced_stored_bytes",
            "referenced_uncompressed_bytes",
        }
        if type(counts) is not dict or set(counts) != count_fields or any(
            type(counts[name]) is not int or counts[name] < 0 for name in count_fields
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return value

    def _verified_previous_roots(
        self,
        occurrence: ArchiveOccurrence,
        *,
        previous_roots: Mapping[str, Mapping[str, Any]] | None,
        previous_manifest_ref: Mapping[str, Any] | None,
    ) -> Mapping[str, Mapping[str, Any]] | None:
        predecessor_id = occurrence.source.get("predecessor_archive_id")
        if predecessor_id is None:
            if previous_roots is not None or previous_manifest_ref is not None:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            return None
        if previous_manifest_ref is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        previous_manifest = self._read_control(
            previous_manifest_ref,
            logical_kind="manifest",
        )
        if previous_manifest.get("archive_format") != "incremental_transition_v1":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        previous_manifest = self._validate_native_manifest(previous_manifest)
        if (
            previous_manifest.get("archive_id") != predecessor_id
            or previous_manifest["scope"].get("project_id") != self.project_id
            or previous_manifest["scope"].get("sprint_id") != self.sprint_id
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        # A predecessor proof is content, not merely a caller-supplied ID.
        # Rehydrate its complete graph before any descendant object is written.
        self._hydrate_incremental_archive(previous_manifest, max_output_bytes=None)
        manifest_roots = {
            name: _copy_ref(ref)
            for name, ref in previous_manifest["roots"].items()
        }
        if previous_roots is not None:
            if not isinstance(previous_roots, Mapping):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            supplied = {
                name: _copy_ref(ref) for name, ref in previous_roots.items()
            }
            if supplied != manifest_roots:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return manifest_roots

    def build_incremental_archive(
        self,
        raw_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
        *,
        previous_roots: Mapping[str, Mapping[str, Any]] | None = None,
        previous_manifest_ref: Mapping[str, Any] | None = None,
        publication_mode: str = "shadow",
        operator_gate: str | None = None,
        legacy_committed_bytes: bytes | bytearray | memoryview | None = None,
    ) -> BuiltPromptArchive:
        with self._reference_budget():
            return self._build_incremental_archive(
                raw_bytes,
                occurrence,
                previous_roots=previous_roots,
                previous_manifest_ref=previous_manifest_ref,
                publication_mode=publication_mode,
                operator_gate=operator_gate,
                legacy_committed_bytes=legacy_committed_bytes,
            )

    def _build_incremental_archive(
        self,
        raw_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
        *,
        previous_roots: Mapping[str, Mapping[str, Any]] | None,
        previous_manifest_ref: Mapping[str, Any] | None,
        publication_mode: str,
        operator_gate: str | None,
        legacy_committed_bytes: bytes | bytearray | memoryview | None,
    ) -> BuiltPromptArchive:
        if publication_mode == "shadow":
            source_kind = "runtime_shadow"
            if operator_gate is not None or legacy_committed_bytes is None:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        elif publication_mode == "authoritative":
            source_kind = "native_runtime"
            if (
                operator_gate != "prompt-archive-authoritative-v1"
                or legacy_committed_bytes is not None
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        else:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        scope, source, authorization = self._validate_occurrence(
            occurrence,
            source_kind=source_kind,
        )
        (
            source_metadata_projection,
            source_metadata_projection_raw,
            source_metadata_receipt,
        ) = self._validate_occurrence_source_metadata(occurrence)
        if self.sprint_id is None or occurrence.occurrence_kind == "migration":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        source_bytes = self._native_source_bytes(
            raw_bytes,
            maximum=self.storage.limits.max_hydrated_bytes,
        )
        if publication_mode == "shadow":
            committed = self._native_source_bytes(
                legacy_committed_bytes,
                maximum=self.storage.limits.max_hydrated_bytes,
            )
            if committed != source_bytes:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        frozen_occurrence = dataclasses.replace(
            occurrence,
            source=source,
            authorization=authorization,
            source_metadata_projection=source_metadata_projection,
            source_metadata_receipt=source_metadata_receipt,
        )
        verified_previous_roots = self._verified_previous_roots(
            frozen_occurrence,
            previous_roots=previous_roots,
            previous_manifest_ref=previous_manifest_ref,
        )
        roots, envelope, envelope_stream = self._build_native_plan(
            source_bytes,
            frozen_occurrence,
            verified_previous_roots,
        )
        if source_metadata_projection_raw is not None:
            source_metadata_record = self._build_record(
                "source_metadata",
                source_metadata_projection_raw,
                ordinal=0,
                provenance=self._record_provenance(frozen_occurrence),
                occurred_at=frozen_occurrence.created_at,
            )
            source_metadata_sequence = self.build_record_sequence(
                "source_metadata",
                [source_metadata_record],
            )
            assert source_metadata_sequence is not None
            roots = dict(roots)
            roots["source_metadata"] = _copy_ref(
                source_metadata_sequence.root_ref
            )
            assert source_metadata_receipt is not None
            self._validate_source_metadata_root_binding(
                roots["source_metadata"],
                source_metadata_receipt,
            )
        envelope_bytes = canonical_json_bytes_v1(
            envelope,
            maximum=self.storage.limits.max_hydrated_bytes,
        )
        envelope_descriptor = {
            "media_type": "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
            "byte_size": len(envelope_bytes),
            "sha256": hashlib.sha256(envelope_bytes).hexdigest(),
            "chunks_root": _copy_ref(envelope_stream.sequence.root_ref),
        }
        source_sha256 = hashlib.sha256(source_bytes).hexdigest()
        archive_id = self._archive_id(
            archive_format="incremental_transition_v1",
            scope=scope,
            roots=roots,
            hydrated_size=len(source_bytes),
            hydrated_sha256=source_sha256,
            envelope_sha256=envelope_descriptor["sha256"],
        )
        # Planning and predecessor verification intentionally share an epoch.
        # The publication proof must, however, re-read the complete graph from
        # storage so a missing or corrupted object cannot be hidden by cache.
        self._start_fresh_read_epoch()
        counts = self._native_graph_counts(
            roots,
            envelope_stream,
            logical_bytes=len(source_bytes),
        )
        publication: dict[str, Any] = {
            "mode": publication_mode,
            "legacy_authoritative": publication_mode == "shadow",
            "writer_version": "prompt-archive-writer-v1",
            "verified_at": occurrence.verified_at,
        }
        if publication_mode == "authoritative":
            publication["operator_gate"] = operator_gate
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_manifest",
            "media_type": _CONTROL_MEDIA["manifest"],
            "id_algorithm": "PAS1-ID-v1",
            "archive_id": archive_id,
            "archive_format": "incremental_transition_v1",
            "scope": scope,
            "source": source,
            "authorization": authorization,
            "renderer_version": "pas1-concat-renderer-v1",
            "json_value_model": "pas1-recursive-json-merkle-v1",
            "envelope": envelope_descriptor,
            "roots": roots,
            "integrity": {
                "semantics": "native-byte-equality",
                "hydrated_media_type": "application/json",
                "hydrated_size": len(source_bytes),
                "hydrated_sha256": source_sha256,
            },
            "counts": counts,
            "limits": self._limits_document(self.storage),
            "publication": publication,
            "created_at": occurrence.created_at,
        }
        if source_metadata_receipt is not None:
            manifest["source_metadata_receipt"] = dict(source_metadata_receipt)
        if publication_mode == "shadow":
            manifest["legacy_comparison"] = {
                "byte_size": len(source_bytes),
                "sha256": source_sha256,
                "renderer_version": "pas1-concat-renderer-v1",
                "matched": True,
            }
        self._validate_native_manifest(manifest)
        manifest_bytes = canonical_json_bytes_v1(manifest, maximum=_MAX_MANIFEST_BYTES)
        self._control_bytes_serialized += len(manifest_bytes)
        manifest_ref = self._put_blob(
            manifest_bytes,
            logical_kind="manifest",
            media_type=_CONTROL_MEDIA["manifest"],
        )
        if self._read_control(manifest_ref, logical_kind="manifest") != manifest:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return BuiltPromptArchive(
            archive_id,
            "incremental_transition_v1",
            manifest,
            manifest_ref,
            roots,
            len(source_bytes),
            source_sha256,
        )

    def _validate_legacy_manifest(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(manifest)
        expected_keys = {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "archive_id",
            "archive_format",
            "scope",
            "source",
            "authorization",
            "roots",
            "integrity",
            "counts",
            "limits",
            "publication",
            "created_at",
        }
        if set(value) != expected_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value.get("schema_version") != 1
            or value.get("kind") != "prompt_archive_manifest"
            or value.get("media_type") != _CONTROL_MEDIA["manifest"]
            or value.get("id_algorithm") != "PAS1-ID-v1"
            or value.get("archive_format") != "legacy_raw_chunks_v1"
            or type(value.get("archive_id")) is not str
            or re.fullmatch(r"par1-[0-9a-f]{64}", value["archive_id"], re.ASCII) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        scope = value.get("scope")
        if type(scope) is not dict or set(scope) != {
            "project_id",
            "sprint_id",
            "response_id",
            "response_kind",
            "occurrence_kind",
            "occurrence_id",
            "transition_id",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        publication = value.get("publication")
        if (
            type(publication) is not dict
            or set(publication)
            != {"mode", "legacy_authoritative", "writer_version", "verified_at"}
            or publication.get("mode") != "migration"
            or publication.get("legacy_authoritative") is not True
            or type(publication.get("writer_version")) is not str
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}",
                publication["writer_version"],
                re.ASCII,
            )
            is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        occurrence = ArchiveOccurrence(
            project_id=scope.get("project_id"),
            sprint_id=scope.get("sprint_id"),
            response_id=scope.get("response_id"),
            response_kind=scope.get("response_kind"),
            occurrence_kind=scope.get("occurrence_kind"),
            occurrence_id=scope.get("occurrence_id"),
            transition_id=scope.get("transition_id"),
            source=value.get("source"),
            authorization=value.get("authorization"),
            created_at=value.get("created_at"),
            verified_at=publication.get("verified_at"),
        )
        validated_scope, validated_source, validated_authorization = self._validate_occurrence(
            occurrence,
            source_kind="offline_migration",
        )
        if (
            validated_scope != scope
            or validated_source != value.get("source")
            or validated_authorization != value.get("authorization")
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        roots = value.get("roots")
        if type(roots) is not dict or set(roots) != {"raw_bytes"}:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        root_ref = _copy_ref(roots["raw_bytes"])
        if (
            root_ref.get("logical_kind") != "index_node"
            or root_ref.get("media_type") != _CONTROL_MEDIA["index_node"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        integrity = value.get("integrity")
        if type(integrity) is not dict or set(integrity) != {
            "semantics",
            "hydrated_media_type",
            "hydrated_size",
            "hydrated_sha256",
        } or (
            integrity.get("semantics") != "raw-byte-equality"
            or integrity.get("hydrated_media_type") != "application/json"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_exact_int(integrity.get("hydrated_size"))
        digest = _require_exact_string(integrity.get("hydrated_sha256"))
        if _SHA256_RE.fullmatch(digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._validate_limits_document(value.get("limits"))
        counts = value.get("counts")
        required_counts = {
            "records",
            "raw_chunks",
            "payload_chunks",
            "literal_chunks",
            "render_plan_chunks",
            "json_value_nodes",
            "json_edges",
            "index_nodes",
            "unique_referenced_objects",
            "graph_edges",
            "logical_bytes",
            "referenced_stored_bytes",
            "referenced_uncompressed_bytes",
        }
        if type(counts) is not dict or set(counts) != required_counts or any(
            type(counts[name]) is not int or counts[name] < 0 for name in required_counts
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return value

    def build_legacy_raw_archive(
        self,
        raw_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
    ) -> BuiltPromptArchive:
        with self._reference_budget():
            return self._build_legacy_raw_archive(raw_bytes, occurrence)

    def _build_legacy_raw_archive(
        self,
        raw_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
    ) -> BuiltPromptArchive:
        scope, source, authorization = self._validate_occurrence(
            occurrence,
            source_kind="offline_migration",
        )
        if self.sprint_id is not None or occurrence.occurrence_kind != "migration":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        # Freeze the caller's exact original before any CAS write.  The raw
        # chunk graph is permitted to leave unreachable objects on failure,
        # but a manifest must never attest to renderer output unless that
        # output independently matches the original byte length and digest.
        frozen = self._native_source_bytes(
            raw_bytes,
            maximum=self.storage.limits.max_hydrated_bytes,
        )
        source_size = len(frozen)
        source_sha256 = hashlib.sha256(frozen).hexdigest()
        stream = self.store_byte_stream(frozen, sequence_kind="raw_bytes")
        # Re-render before the reachability artifact is admitted.
        rendered = self.hydrate_byte_stream(stream)
        if (
            len(rendered) != source_size
            or hashlib.sha256(rendered).hexdigest() != source_sha256
            or rendered != frozen
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        roots = {"raw_bytes": _copy_ref(stream.sequence.root_ref)}
        archive_id = self._archive_id(
            archive_format="legacy_raw_chunks_v1",
            scope=scope,
            roots=roots,
            hydrated_size=source_size,
            hydrated_sha256=source_sha256,
            envelope_sha256=None,
        )
        # A migration manifest must not rely on bytes cached by the initial
        # render.  Scrub the complete index and every raw leaf again before
        # publishing the reachability artifact.
        self._start_fresh_read_epoch()
        counts = self._raw_graph_counts(stream.sequence)
        manifest = {
            "schema_version": 1,
            "kind": "prompt_archive_manifest",
            "media_type": _CONTROL_MEDIA["manifest"],
            "id_algorithm": "PAS1-ID-v1",
            "archive_id": archive_id,
            "archive_format": "legacy_raw_chunks_v1",
            "scope": scope,
            "source": source,
            "authorization": authorization,
            "roots": roots,
            "integrity": {
                "semantics": "raw-byte-equality",
                "hydrated_media_type": "application/json",
                "hydrated_size": source_size,
                "hydrated_sha256": source_sha256,
            },
            "counts": counts,
            "limits": self._limits_document(self.storage),
            "publication": {
                "mode": "migration",
                "legacy_authoritative": True,
                "writer_version": "prompt-archive-writer-v1",
                "verified_at": occurrence.verified_at,
            },
            "created_at": occurrence.created_at,
        }
        manifest_bytes = canonical_json_bytes_v1(
            manifest,
            maximum=_MAX_MANIFEST_BYTES,
        )
        self._control_bytes_serialized += len(manifest_bytes)
        manifest_ref = self._put_blob(
            manifest_bytes,
            logical_kind="manifest",
            media_type=_CONTROL_MEDIA["manifest"],
        )
        verified = self._read_control(manifest_ref, logical_kind="manifest")
        if verified != manifest:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return BuiltPromptArchive(
            archive_id,
            "legacy_raw_chunks_v1",
            manifest,
            manifest_ref,
            roots,
            source_size,
            source_sha256,
        )

    def hydrate_archive_bytes(
        self,
        manifest_ref: Mapping[str, Any],
        *,
        max_output_bytes: int | None = None,
    ) -> bytes:
        with self._reference_budget():
            return self._hydrate_archive_bytes(
                manifest_ref,
                max_output_bytes=max_output_bytes,
            )

    def read_manifest_metadata(
        self,
        manifest_ref: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Read one bounded manifest without traversing its referenced graph.

        This is the metadata-only seam used by descriptor and manifest HTTP
        resources.  The manifest CAS object, closed schema, project/sprint
        scope, and archive self-ID are verified here.  Referenced index,
        record, payload, and chunk blobs are deliberately not opened; complete
        reachability, count, and hydrated-byte verification belongs only to
        :meth:`hydrate_archive_bytes` under hydration admission.
        """

        with self._reference_budget():
            manifest = self._read_control(manifest_ref, logical_kind="manifest")
            if manifest.get("schema_version") != 1:
                raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
            archive_format = manifest.get("archive_format")
            if archive_format == "incremental_transition_v1":
                value = self._validate_native_manifest(manifest)
            elif archive_format == "legacy_raw_chunks_v1":
                value = self._validate_legacy_manifest(manifest)
            else:
                raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
            # Metadata reads stay shallow, but all references present directly
            # in the bounded manifest still participate in the one-role-per-
            # digest closure invariant.  Descendant roles are deliberately
            # deferred to full reachability/hydration traversal.
            digest_roles: dict[str, tuple[str, str]] = {}
            _register_digest_role(digest_roles, manifest_ref)
            for ref in value["roots"].values():
                _register_digest_role(digest_roles, ref)
            if archive_format == "incremental_transition_v1":
                _register_digest_role(
                    digest_roles,
                    value["envelope"]["chunks_root"],
                )
            scope = value.get("scope")
            if type(scope) is not dict or (
                scope.get("project_id") != self.project_id
                or scope.get("sprint_id") != self.sprint_id
            ):
                raise _error("PROMPT_ARCHIVE_NOT_FOUND")
            integrity = value["integrity"]
            envelope_sha256 = (
                value["envelope"]["sha256"]
                if archive_format == "incremental_transition_v1"
                else None
            )
            expected_archive_id = self._archive_id(
                archive_format=archive_format,
                scope=scope,
                roots=value["roots"],
                hydrated_size=_require_exact_int(integrity["hydrated_size"]),
                hydrated_sha256=_require_exact_string(
                    integrity["hydrated_sha256"]
                ),
                envelope_sha256=envelope_sha256,
            )
            if value["archive_id"] != expected_archive_id:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            return value

    def read_manifest(
        self,
        manifest_ref: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Backward-compatible alias for the explicit metadata-only reader."""

        return self.read_manifest_metadata(manifest_ref)

    def reachable_refs(
        self,
        manifest_ref: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        """Return and verify the manifest's unique typed CAS closure.

        The publication plan binds this exact set.  Traversal is deliberately
        rooted in the supplied manifest reference and follows only closed
        ``prompt_archive_blob_ref`` values found in PAS1 control objects; it
        never enumerates the blob directory.
        """

        control_kinds = {
            "manifest",
            "index_node",
            "json_value",
            "json_edge",
            *set(_RECORD_SEQUENCE_TYPES.values()),
        }
        observed: dict[tuple[str, str, str], dict[str, Any]] = {}
        digest_roles: dict[str, tuple[str, str]] = {}
        active: set[tuple[str, str, str]] = set()

        def nested_refs(value: Any) -> Iterable[Mapping[str, Any]]:
            if isinstance(value, Mapping):
                if value.get("kind") == "prompt_archive_blob_ref":
                    yield value
                    return
                for child in value.values():
                    yield from nested_refs(child)
            elif type(value) is list:
                for child in value:
                    yield from nested_refs(child)

        def visit(ref: Mapping[str, Any], depth: int) -> None:
            if depth > self.storage.limits.max_reference_depth:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            copied = _copy_ref(ref)
            typed_key = _register_digest_role(
                digest_roles,
                copied,
            )
            if typed_key in active:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            previous = observed.get(typed_key)
            if previous is not None:
                if previous != copied:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                return
            if len(observed) >= self.storage.limits.max_referenced_objects:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            raw = self.storage.read_blob(copied)
            observed[typed_key] = copied
            logical_kind = typed_key[1]
            if logical_kind not in control_kinds:
                return
            active.add(typed_key)
            try:
                document = _load_canonical_control(raw)
                for child in nested_refs(document):
                    visit(child, depth + 1)
            finally:
                active.remove(typed_key)

        with self._reference_budget():
            visit(manifest_ref, 1)
        return tuple(
            observed[key]
            for key in sorted(
                observed,
                key=lambda item: tuple(part.encode("utf-8") for part in item),
            )
        )

    def _hydrate_archive_bytes(
        self,
        manifest_ref: Mapping[str, Any],
        *,
        max_output_bytes: int | None = None,
    ) -> bytes:
        manifest = self._read_control(manifest_ref, logical_kind="manifest")
        if manifest.get("schema_version") != 1:
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        archive_format = manifest.get("archive_format")
        if archive_format == "incremental_transition_v1":
            return self._hydrate_incremental_archive(
                manifest,
                max_output_bytes=max_output_bytes,
            )
        if archive_format != "legacy_raw_chunks_v1":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        manifest = self._validate_legacy_manifest(manifest)
        scope = manifest.get("scope")
        if type(scope) is not dict or (
            scope.get("project_id") != self.project_id
            or scope.get("sprint_id") != self.sprint_id
        ):
            raise _error("PROMPT_ARCHIVE_NOT_FOUND")
        integrity = manifest.get("integrity")
        if type(integrity) is not dict or integrity.get("semantics") != "raw-byte-equality":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        byte_size = _require_exact_int(integrity.get("hydrated_size"))
        digest = _require_exact_string(integrity.get("hydrated_sha256"))
        if _SHA256_RE.fullmatch(digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if max_output_bytes is not None:
            if type(max_output_bytes) is not int or max_output_bytes < 0:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            if byte_size > max_output_bytes:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        root = self._receipt_from_root(manifest["roots"]["raw_bytes"], "raw_bytes")
        leaves = self.sequence_leaves(root)
        stream = ByteStreamReceipt(root, byte_size, digest, len(leaves))
        expected_archive_id = self._archive_id(
            archive_format="legacy_raw_chunks_v1",
            scope=scope,
            roots=manifest["roots"],
            hydrated_size=byte_size,
            hydrated_sha256=digest,
            envelope_sha256=None,
        )
        if manifest.get("archive_id") != expected_archive_id:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        counts = self._raw_graph_counts(root)
        if manifest.get("counts") != counts:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        # Return bytes only after an independent final storage traversal; the
        # reachability/count pass must not satisfy this read from its cache.
        self._start_fresh_read_epoch()
        payload = self.hydrate_byte_stream(stream)
        if len(payload) != byte_size or hashlib.sha256(payload).hexdigest() != digest:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return payload

    def _hydrate_incremental_archive(
        self,
        manifest: Mapping[str, Any],
        *,
        max_output_bytes: int | None,
    ) -> bytes:
        value = self._validate_native_manifest(manifest)
        scope = value["scope"]
        if (
            scope.get("project_id") != self.project_id
            or scope.get("sprint_id") != self.sprint_id
        ):
            raise _error("PROMPT_ARCHIVE_NOT_FOUND")
        source_metadata_receipt = value.get("source_metadata_receipt")
        if source_metadata_receipt is not None:
            self._validate_source_metadata_root_binding(
                value["roots"]["source_metadata"],
                source_metadata_receipt,
            )
        integrity = value["integrity"]
        byte_size = _require_exact_int(integrity.get("hydrated_size"))
        if max_output_bytes is not None:
            if type(max_output_bytes) is not int or max_output_bytes < 0:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            if byte_size > max_output_bytes:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        if byte_size > self.storage.limits.max_hydrated_bytes:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

        descriptor = value["envelope"]
        envelope_root = self._receipt_from_root(
            descriptor["chunks_root"],
            "render_plan_chunks",
        )
        envelope_leaves = self.sequence_leaves(envelope_root)
        envelope_bytes = self.hydrate_byte_stream(
            ByteStreamReceipt(
                envelope_root,
                _require_exact_int(descriptor["byte_size"]),
                _require_exact_string(descriptor["sha256"]),
                len(envelope_leaves),
            )
        )
        envelope = _load_canonical_control(envelope_bytes)
        if (
            envelope.get("response_id") != scope["response_id"]
            or envelope.get("response_kind") != scope["response_kind"]
            or envelope.get("source_byte_size") != byte_size
            or envelope.get("source_sha256") != integrity["hydrated_sha256"]
            or envelope.get("renderer_version") != value["renderer_version"]
            or envelope.get("json_value_model") != value["json_value_model"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        payload = self._render_native_envelope(envelope, value["roots"])
        payload_digest = hashlib.sha256(payload).hexdigest()
        if len(payload) != byte_size or payload_digest != integrity["hydrated_sha256"]:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        expected_archive_id = self._archive_id(
            archive_format="incremental_transition_v1",
            scope=scope,
            roots=value["roots"],
            hydrated_size=byte_size,
            hydrated_sha256=payload_digest,
            envelope_sha256=descriptor["sha256"],
        )
        if value["archive_id"] != expected_archive_id:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        # Revalidate the graph independently of bytes cached by rendering.
        self._start_fresh_read_epoch()
        counts = self._native_graph_counts(
            value["roots"],
            ByteStreamReceipt(
                envelope_root,
                descriptor["byte_size"],
                descriptor["sha256"],
                len(envelope_leaves),
            ),
            logical_bytes=byte_size,
        )
        if value["counts"] != counts:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return payload


__all__ = [
    "ArchiveOccurrence",
    "ByteStreamReceipt",
    "BuiltPromptArchive",
    "JsonValueReceipt",
    "PromptArchiveIncremental",
    "RecordReceipt",
    "SequenceLeaf",
    "SequenceReceipt",
    "StorageInventory",
    "WorkMetrics",
    "pas1_id_v1",
    "response_id_v1",
    "sprint_storage_key_v1",
]
