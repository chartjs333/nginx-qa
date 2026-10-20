"""Bounded authoritative response documents and top-level dispatch.

This module deliberately owns no filesystem, archive runtime, or hydration
adapter.  It freezes one already-committed compact response together with its
durable :class:`ArchiveOccurrence`, and it dispatches stored response bytes by
their top-level kind.  Descriptor resolution is possible only through an
injected authorized, bounded reader.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any, Final

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    pas1_id_v1,
    response_id_v1,
)
from nginx_qa.prompt_archive_occurrence_store import (
    _validate_occurrence as _validate_archive_occurrence,
)
from nginx_qa.prompt_archive_storage import PromptArchiveStorageError


_MIB: Final = 1024 * 1024
_DEFAULT_MAX_BYTES: Final = 16 * _MIB
_MAX_BYTES: Final = 512 * _MIB
_MAX_JSON_DEPTH: Final = 128
_MAX_JSON_NODES: Final = 250_000
_STUB_MAX_BYTES: Final = 64 * 1024

_DOCUMENT_KIND: Final = "prompt_archive_occurrence_document"
_DOCUMENT_MEDIA_TYPE: Final = (
    "application/vnd.nginx-qa.prompt-archive-occurrence-document-v1+json"
)
_DOCUMENT_KEYS: Final = frozenset(
    {
        "schema_version",
        "kind",
        "media_type",
        "occurrence",
        "execution_response",
        "evidence_delta",
    }
)

_STUB_KIND: Final = "prompt_archive_descriptor_stub"
_STUB_MEDIA_TYPE: Final = (
    "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
)
_STUB_KEYS: Final = frozenset(
    {
        "schema_version",
        "kind",
        "media_type",
        "id_algorithm",
        "stub_id",
        "archive_id",
        "archive_format",
        "scope",
        "integrity",
        "pointer_id",
        "manifest_ref",
        "created_at",
        "published_at",
    }
)
_SCOPE_KEYS: Final = frozenset(
    {
        "project_id",
        "sprint_id",
        "response_id",
        "response_kind",
        "occurrence_kind",
        "occurrence_id",
        "transition_id",
    }
)
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
_INTEGRITY_KEYS: Final = frozenset(
    {
        "semantics",
        "hydrated_media_type",
        "hydrated_size",
        "hydrated_sha256",
        "canonical",
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
_EVIDENCE_KEYS: Final = frozenset(
    {
        "result",
        "feedback",
        "review",
        "reviews",
        "review_context",
        "transition",
        "pending_transition",
        "last_transition",
        "transition_applied",
        "completed_assignment",
        "source_commit",
        "from_commit",
        "git_commit",
        "git_branch",
    }
)
_FORBIDDEN_TOP_LEVEL: Final = frozenset(
    {
        "project_state",
        "work_history",
        "assignments",
        "completed_assignments",
        "history_with_patches",
        "activity_with_patches",
        "code_patches",
        "recent_activity",
        "execution_audit",
    }
)

_SAFE_ID_RE: Final = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$",
    re.ASCII,
)
_RESPONSE_ID_RE: Final = re.compile(r"^parresp1-[0-9a-f]{64}$", re.ASCII)
_ARCHIVE_ID_RE: Final = re.compile(r"^par1-[0-9a-f]{64}$", re.ASCII)
_STUB_ID_RE: Final = re.compile(r"^parstub1-[0-9a-f]{64}$", re.ASCII)
_POINTER_ID_RE: Final = re.compile(r"^parptr1-[0-9a-f]{64}$", re.ASCII)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)


class _InvalidJson(Exception):
    pass


def _error(code: str) -> PromptArchiveStorageError:
    return PromptArchiveStorageError(code)


def _validate_maximum(maximum_bytes: Any) -> int:
    if (
        type(maximum_bytes) is not int
        or not 1 <= maximum_bytes <= _MAX_BYTES
    ):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    return maximum_bytes


def _bounded_bytes(
    value: Any,
    *,
    maximum: int,
    invalid_code: str,
) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise _error(invalid_code)
    try:
        frozen = bytes(value)
    except (TypeError, ValueError, BufferError):
        raise _error(invalid_code) from None
    if len(frozen) > maximum:
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
    return frozen


def _validate_nesting(raw: bytes) -> None:
    depth = 0
    in_string = False
    escaped = False
    for value in raw:
        if in_string:
            if escaped:
                escaped = False
            elif value == 0x5C:
                escaped = True
            elif value == 0x22:
                in_string = False
            continue
        if value == 0x22:
            in_string = True
        elif value in (0x5B, 0x7B):
            depth += 1
            if depth > _MAX_JSON_DEPTH:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        elif value in (0x5D, 0x7D):
            depth -= 1
            if depth < 0:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")


def _validate_json_tree(
    value: Any,
    *,
    invalid_code: str,
    depth: int = 0,
    observed: list[int] | None = None,
) -> None:
    if observed is None:
        observed = [0]
    observed[0] += 1
    if observed[0] > _MAX_JSON_NODES or depth > _MAX_JSON_DEPTH:
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
    if value is None or type(value) in {bool, int}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise _error(invalid_code)
        return
    if type(value) is str:
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeError:
            raise _error(invalid_code) from None
        return
    if type(value) is list:
        for child in value:
            _validate_json_tree(
                child,
                invalid_code=invalid_code,
                depth=depth + 1,
                observed=observed,
            )
        return
    if type(value) is dict:
        for key, child in value.items():
            if type(key) is not str:
                raise _error(invalid_code)
            _validate_json_tree(
                child,
                invalid_code=invalid_code,
                depth=depth + 1,
                observed=observed,
            )
        return
    raise _error(invalid_code)


def _parse_json_mapping(raw: bytes, *, maximum: int) -> dict[str, Any]:
    if len(raw) > maximum:
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
    _validate_nesting(raw)

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in values:
            if type(key) is not str or key in output:
                raise _InvalidJson
            output[key] = value
        return output

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise _InvalidJson
        return parsed

    def reject_constant(_value: str) -> Any:
        raise _InvalidJson

    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=pairs,
            parse_float=finite_float,
            parse_constant=reject_constant,
        )
    except (
        UnicodeError,
        json.JSONDecodeError,
        _InvalidJson,
        OverflowError,
        ValueError,
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    except (MemoryError, RecursionError):
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") from None
    if type(value) is not dict:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    _validate_json_tree(value, invalid_code="PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _freeze_mapping(
    value: Any,
    *,
    maximum: int,
    invalid_code: str,
) -> dict[str, Any]:
    if type(value) is not dict:
        raise _error(invalid_code)
    _validate_json_tree(value, invalid_code=invalid_code)
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")
    except (UnicodeError, TypeError, ValueError, OverflowError):
        raise _error(invalid_code) from None
    if len(raw) > maximum:
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
    return _parse_json_mapping(raw, maximum=maximum)


def _occurrence_document(occurrence: ArchiveOccurrence) -> dict[str, Any]:
    if not isinstance(occurrence, ArchiveOccurrence):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    if any(
        value is not None
        for value in (
            occurrence.source_response_key,
            occurrence.source_byte_size,
            occurrence.source_sha256,
        )
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    try:
        frozen = _validate_archive_occurrence(
            occurrence,
            project_id=occurrence.project_id,
        )
    except PromptArchiveStorageError:
        raise
    except Exception:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    if frozen.sprint_id is None or frozen.source.get("source_kind") != "native_runtime":
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return {
        "project_id": frozen.project_id,
        "sprint_id": frozen.sprint_id,
        "response_id": frozen.response_id,
        "response_kind": frozen.response_kind,
        "occurrence_kind": frozen.occurrence_kind,
        "occurrence_id": frozen.occurrence_id,
        "transition_id": frozen.transition_id,
        "source": dict(frozen.source),
        "authorization": dict(frozen.authorization),
        "created_at": frozen.created_at,
        "verified_at": frozen.verified_at,
        "state_revision": frozen.state_revision,
    }


def _validated_occurrence_mapping(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _OCCURRENCE_KEYS:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    project_id = value.get("project_id")
    if type(project_id) is not str:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    try:
        occurrence = _validate_archive_occurrence(value, project_id=project_id)
    except PromptArchiveStorageError as exc:
        if exc.code == "PROMPT_ARCHIVE_REQUEST_INVALID":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        raise
    except Exception:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    if (
        occurrence.sprint_id is None
        or occurrence.source.get("source_kind") != "native_runtime"
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return _occurrence_document(occurrence)


def _reject_cumulative(response: Mapping[str, Any]) -> None:
    if any(key in response for key in _FORBIDDEN_TOP_LEVEL):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    for container_name in ("assignment", "execution"):
        container = response.get(container_name)
        if type(container) is dict and any(
            key in container for key in ("assignments", "execution_audit")
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")


def _cross_bind_response(
    response: Mapping[str, Any],
    occurrence: Mapping[str, Any],
) -> None:
    for key in ("project_id", "sprint_id", "response_id", "response_kind"):
        if key in response and response[key] != occurrence[key]:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")


def _validate_occurrence_document(value: Any, *, maximum: int) -> dict[str, Any]:
    document = _freeze_mapping(
        value,
        maximum=maximum,
        invalid_code="PROMPT_ARCHIVE_REFERENCE_INVALID",
    )
    if document.get("kind") != _DOCUMENT_KIND:
        if "kind" in document:
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if (
        document.get("schema_version") != 1
        or document.get("media_type") != _DOCUMENT_MEDIA_TYPE
    ):
        raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
    if set(document) != _DOCUMENT_KEYS:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    occurrence = _validated_occurrence_mapping(document.get("occurrence"))
    execution = document.get("execution_response")
    evidence = document.get("evidence_delta")
    if type(execution) is not dict or type(evidence) is not dict:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if set(execution).intersection(_EVIDENCE_KEYS):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if not set(evidence).issubset(_EVIDENCE_KEYS):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    combined = {**execution, **evidence}
    try:
        _reject_cumulative(combined)
    except PromptArchiveStorageError as exc:
        if exc.code == "PROMPT_ARCHIVE_REQUEST_INVALID":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        raise
    _cross_bind_response(combined, occurrence)
    document["occurrence"] = occurrence
    return document


def build_authoritative_occurrence_document(
    committed_response: Mapping[str, Any],
    occurrence: ArchiveOccurrence,
    *,
    maximum_bytes: int = _DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    """Build one closed non-cumulative authoritative occurrence document.

    ``committed_response`` is a transaction result already accepted by the
    caller.  This function performs no state lookup and never invokes a full
    response builder.  Response-scoped result/review/transition evidence is
    moved into ``evidence_delta``; the remaining current contract, including
    the complete ``active_task``, stays in ``execution_response``.
    """

    maximum = _validate_maximum(maximum_bytes)
    if type(committed_response) is not dict:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    # Reject known cumulative roots before reading/copying their values.  This
    # also keeps a poison/full-state adapter from running on this hot path.
    _reject_cumulative(committed_response)
    frozen_response = _freeze_mapping(
        committed_response,
        maximum=maximum,
        invalid_code="PROMPT_ARCHIVE_REQUEST_INVALID",
    )
    frozen_occurrence = _occurrence_document(occurrence)
    _cross_bind_response(frozen_response, frozen_occurrence)
    evidence = {
        key: value
        for key, value in frozen_response.items()
        if key in _EVIDENCE_KEYS
    }
    execution = {
        key: value
        for key, value in frozen_response.items()
        if key not in _EVIDENCE_KEYS
    }
    document = {
        "schema_version": 1,
        "kind": _DOCUMENT_KIND,
        "media_type": _DOCUMENT_MEDIA_TYPE,
        "occurrence": frozen_occurrence,
        "execution_response": execution,
        "evidence_delta": evidence,
    }
    return _validate_occurrence_document(document, maximum=maximum)


def validate_authoritative_occurrence_document(
    value: Mapping[str, Any],
    *,
    maximum_bytes: int = _DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    """Validate and defensively copy one hydrated occurrence document.

    Cold state reconstruction uses this public seam after the archive runtime
    has verified and hydrated immutable content.  It accepts no descriptor or
    legacy-inline fallback and therefore cannot accidentally reinterpret an
    arbitrary response as a native authoritative occurrence.
    """

    maximum = _validate_maximum(maximum_bytes)
    return _validate_occurrence_document(value, maximum=maximum)


def _require_safe_id(value: Any) -> str:
    if type(value) is not str or _SAFE_ID_RE.fullmatch(value) is None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _require_timestamp(value: Any) -> str:
    if type(value) is not str:
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


def _validate_scope(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _SCOPE_KEYS:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    project_id = _require_safe_id(value.get("project_id"))
    sprint_id = _require_safe_id(value.get("sprint_id"))
    occurrence_id = _require_safe_id(value.get("occurrence_id"))
    response_id = value.get("response_id")
    response_kind = value.get("response_kind")
    occurrence_kind = value.get("occurrence_kind")
    transition_id = value.get("transition_id")
    if (
        type(response_id) is not str
        or _RESPONSE_ID_RE.fullmatch(response_id) is None
        or response_kind
        not in {"identity-response", "transition-response", "state-response"}
        or occurrence_kind not in {"transition", "identity-state"}
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if occurrence_kind == "transition":
        _require_safe_id(transition_id)
    elif transition_id is not None:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if response_id != response_id_v1(
        project_id,
        sprint_id,
        response_kind,
        occurrence_kind,
        occurrence_id,
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _validate_manifest_ref(value: Any) -> dict[str, Any]:
    if (
        type(value) is not dict
        or not _REF_REQUIRED_KEYS.issubset(value)
        or not set(value).issubset(_REF_KEYS)
        or value.get("schema_version") != 1
        or value.get("kind") != "prompt_archive_blob_ref"
        or value.get("hash_algorithm") != "sha256"
        or value.get("logical_kind") != "manifest"
        or value.get("media_type")
        != "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
        or type(value.get("digest")) is not str
        or _SHA256_RE.fullmatch(value["digest"]) is None
        or type(value.get("stored_sha256")) is not str
        or _SHA256_RE.fullmatch(value["stored_sha256"]) is None
        or type(value.get("uncompressed_size")) is not int
        or not 0 <= value["uncompressed_size"] <= 16 * _MIB
        or type(value.get("stored_size")) is not int
        or not 0 <= value["stored_size"] <= 16 * _MIB + 64 * 1024
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    encoding = value.get("content_encoding")
    parameters = value.get("encoding_parameters")
    if encoding == "identity":
        if parameters is not None or value["stored_size"] != value["uncompressed_size"]:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    elif encoding == "gzip":
        if parameters != "gzip-rfc1952-zlib-1.2.13-level9-v1":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    else:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _validate_integrity(value: Any) -> dict[str, Any]:
    if (
        type(value) is not dict
        or not {"semantics", "hydrated_media_type", "hydrated_size", "hydrated_sha256"}
        .issubset(value)
        or not set(value).issubset(_INTEGRITY_KEYS)
        or value.get("semantics")
        not in {"raw-byte-equality", "native-byte-equality"}
        or value.get("hydrated_media_type") != "application/json"
        or type(value.get("hydrated_size")) is not int
        or not 0 <= value["hydrated_size"] <= _MAX_BYTES
        or type(value.get("hydrated_sha256")) is not str
        or _SHA256_RE.fullmatch(value["hydrated_sha256"]) is None
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    canonical = value.get("canonical")
    if canonical is not None and (
        type(canonical) is not dict
        or set(canonical) != {"algorithm", "digest"}
        or canonical.get("algorithm") != "nginx-qa-canonical-json-v1"
        or type(canonical.get("digest")) is not str
        or _SHA256_RE.fullmatch(canonical["digest"]) is None
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _validate_stub(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if (
        value.get("schema_version") != 1
        or value.get("media_type") != _STUB_MEDIA_TYPE
    ):
        raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
    if (
        set(value) != _STUB_KEYS
        or value.get("kind") != _STUB_KIND
        or value.get("id_algorithm") != "PAS1-ID-v1"
        or type(value.get("stub_id")) is not str
        or _STUB_ID_RE.fullmatch(value["stub_id"]) is None
        or type(value.get("archive_id")) is not str
        or _ARCHIVE_ID_RE.fullmatch(value["archive_id"]) is None
        or value.get("archive_format")
        not in {"legacy_raw_chunks_v1", "incremental_transition_v1"}
        or type(value.get("pointer_id")) is not str
        or _POINTER_ID_RE.fullmatch(value["pointer_id"]) is None
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    scope = _validate_scope(value.get("scope"))
    manifest_ref = _validate_manifest_ref(value.get("manifest_ref"))
    _validate_integrity(value.get("integrity"))
    created_at = _require_timestamp(value.get("created_at"))
    published_at = _require_timestamp(value.get("published_at"))
    expected_id = "parstub1-" + pas1_id_v1(
        "descriptor-stub-id",
        (
            ("project_id", scope["project_id"]),
            ("sprint_id", scope["sprint_id"]),
            ("response_id", scope["response_id"]),
            ("archive_id", value["archive_id"]),
            ("pointer_id", value["pointer_id"]),
            ("manifest_sha256", manifest_ref["digest"]),
            ("created_at", created_at),
            ("published_at", published_at),
        ),
    )
    if value["stub_id"] != expected_id:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _cross_bind_stub(
    stub: Mapping[str, Any],
    document: Mapping[str, Any],
) -> None:
    scope = stub["scope"]
    occurrence = document["occurrence"]
    if any(occurrence[key] != scope[key] for key in _SCOPE_KEYS):
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")


def dispatch_prompt_archive_response(
    raw_bytes: bytes | bytearray | memoryview,
    *,
    archive_reader: Callable[..., bytes | bytearray | memoryview] | None = None,
    maximum_bytes: int = _DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    """Dispatch legacy inline JSON or one authorized descriptor stub.

    A legacy object has no top-level ``kind`` and is returned as a validated
    mapping.  The descriptor branch invokes ``archive_reader`` exactly once as
    ``archive_reader(descriptor_stub=..., maximum_bytes=...)``.  That injected
    seam owns authorization and bounded archive hydration; its returned bytes
    must be a valid authoritative occurrence document cross-bound to the stub.
    """

    maximum = _validate_maximum(maximum_bytes)
    raw = _bounded_bytes(
        raw_bytes,
        maximum=maximum,
        invalid_code="PROMPT_ARCHIVE_REQUEST_INVALID",
    )
    top_level = _parse_json_mapping(raw, maximum=maximum)
    if "kind" not in top_level:
        return top_level
    if top_level.get("kind") != _STUB_KIND:
        raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
    if len(raw) > _STUB_MAX_BYTES:
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
    stub = _validate_stub(top_level)
    if not callable(archive_reader):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    try:
        hydrated = archive_reader(
            descriptor_stub=_freeze_mapping(
                stub,
                maximum=_STUB_MAX_BYTES,
                invalid_code="PROMPT_ARCHIVE_REFERENCE_INVALID",
            ),
            maximum_bytes=maximum,
        )
    except PromptArchiveStorageError:
        raise
    except Exception:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
    hydrated_raw = _bounded_bytes(
        hydrated,
        maximum=maximum,
        invalid_code="PROMPT_ARCHIVE_REFERENCE_INVALID",
    )
    occurrence_value = _parse_json_mapping(hydrated_raw, maximum=maximum)
    document = _validate_occurrence_document(
        occurrence_value,
        maximum=maximum,
    )
    _cross_bind_stub(stub, document)
    return document


__all__ = [
    "build_authoritative_occurrence_document",
    "dispatch_prompt_archive_response",
    "validate_authoritative_occurrence_document",
]
