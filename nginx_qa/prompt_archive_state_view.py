"""Bounded on-demand reconstruction of one native PAS1 sprint state.

Authoritative PAS1 occurrences intentionally contain only the current compact
execution response and the evidence produced by that occurrence.  This cold
reader follows the immutable manifest predecessor chain, verifies and hydrates
each occurrence, and returns a single root-to-head state view.  It is never
called by publication, identity, active-task, or ``.prompt`` paths.

Authorization and hydration admission belong to the HTTP adapter.  The public
runtime hook in :mod:`nginx_qa.prompt_archive_runtime` is deliberately usable
only after that adapter has authenticated the exact project/sprint/archive
request; archive IDs and manifest provenance are not capabilities.
"""

from __future__ import annotations

import copy
import hashlib
import re
from collections.abc import Mapping
from typing import Any, Final, Protocol

from nginx_qa.prompt_archive_response import (
    validate_authoritative_occurrence_document,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
    parse_canonical_json_v1,
)


_ARCHIVE_ID_RE: Final = re.compile(r"^par1-[0-9a-f]{64}$", re.ASCII)
_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$", re.ASCII)
_MAX_OCCURRENCES: Final = 256
_MEDIA_TYPE: Final = (
    "application/vnd.nginx-qa.prompt-archive-state-view-v1+json"
)


class _ArchiveRead(Protocol):
    project_id: str
    sprint_id: str | None
    archive_id: str
    locator: Mapping[str, Any]
    pointer: Mapping[str, Any]
    manifest: Mapping[str, Any]

    @property
    def archive_format(self) -> str: ...


class PromptArchiveStateViewRuntime(Protocol):
    """Minimal verified runtime surface required by the cold state reader."""

    project_id: str
    max_hydrated_bytes: int

    def lookup_archive_metadata(
        self,
        *,
        sprint_id: str,
        archive_id: str,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> _ArchiveRead: ...

    def hydrate_content(
        self,
        selected: _ArchiveRead,
        *,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> bytes: ...


def _error(code: str) -> PromptArchiveStorageError:
    return PromptArchiveStorageError(code)


def _safe_id(value: Any) -> str:
    if type(value) is not str or _SAFE_ID_RE.fullmatch(value) is None:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    return value


def _archive_id(value: Any) -> str:
    if type(value) is not str or _ARCHIVE_ID_RE.fullmatch(value) is None:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    return value


def _maximum(runtime: PromptArchiveStateViewRuntime, value: Any) -> int:
    configured = getattr(runtime, "max_hydrated_bytes", None)
    if type(configured) is not int or configured < 1:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
    if value is None:
        return configured
    if type(value) is not int or not 1 <= value <= configured:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    return value


def _copy_mapping(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    try:
        return copy.deepcopy(value)
    except (MemoryError, RecursionError):
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") from None


def _native_manifest(
    selected: _ArchiveRead,
    *,
    project_id: str,
    sprint_id: str,
    archive_id: str,
) -> tuple[dict[str, Any], dict[str, Any], str | None, int]:
    """Apply state-view-specific constraints after runtime metadata proof."""

    manifest = _copy_mapping(selected.manifest)
    pointer = _copy_mapping(selected.pointer)
    locator = _copy_mapping(selected.locator)
    scope = manifest.get("scope")
    source = manifest.get("source")
    integrity = manifest.get("integrity")
    publication = manifest.get("publication")
    if (
        selected.project_id != project_id
        or selected.sprint_id != sprint_id
        or selected.archive_id != archive_id
        or selected.archive_format != "incremental_transition_v1"
        or manifest.get("archive_id") != archive_id
        or type(scope) is not dict
        or scope.get("project_id") != project_id
        or scope.get("sprint_id") != sprint_id
        or type(source) is not dict
        or source.get("source_kind") != "native_runtime"
        or type(integrity) is not dict
        or integrity.get("semantics") != "native-byte-equality"
        or integrity.get("hydrated_media_type") != "application/json"
        or type(publication) is not dict
        or publication.get("mode") != "authoritative"
        or publication.get("legacy_authoritative") is not False
        or pointer.get("archive_id") != archive_id
        or locator.get("archive_id") != archive_id
        or pointer.get("response_id") != scope.get("response_id")
        or locator.get("response_id") != scope.get("response_id")
        or pointer.get("pointer_id") != locator.get("pointer_id")
        or pointer.get("manifest_ref") != locator.get("manifest_ref")
    ):
        # A state view has deliberately different semantics from the raw
        # legacy content endpoint.  Legacy/shadow/migration artifacts are not
        # reinterpreted as occurrence deltas.
        if (
            selected.archive_format != "incremental_transition_v1"
            or (isinstance(source, Mapping) and source.get("source_kind") != "native_runtime")
        ):
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    hydrated_size = integrity.get("hydrated_size")
    manifest_ref = pointer.get("manifest_ref")
    if (
        type(hydrated_size) is not int
        or hydrated_size < 1
        or type(manifest_ref) is not dict
        or type(manifest_ref.get("digest")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", manifest_ref["digest"], re.ASCII) is None
        or type(pointer.get("pointer_id")) is not str
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    predecessor = source.get("predecessor_archive_id")
    if predecessor is not None and (
        type(predecessor) is not str
        or _ARCHIVE_ID_RE.fullmatch(predecessor) is None
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return manifest, pointer, predecessor, hydrated_size


def _validated_document(
    raw: bytes,
    *,
    maximum: int,
    manifest: Mapping[str, Any],
    predecessor: str | None,
) -> dict[str, Any]:
    value = parse_canonical_json_v1(raw, maximum=maximum)
    document = validate_authoritative_occurrence_document(
        value,
        maximum_bytes=maximum,
    )
    occurrence = document["occurrence"]
    scope = manifest["scope"]
    scope_keys = (
        "project_id",
        "sprint_id",
        "response_id",
        "response_kind",
        "occurrence_kind",
        "occurrence_id",
        "transition_id",
    )
    if any(occurrence.get(key) != scope.get(key) for key in scope_keys):
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    manifest_source = dict(manifest["source"])
    document_source = dict(occurrence["source"])
    # The runtime may select and persist the predecessor after the hot-path
    # occurrence document has been serialized.  Every other source fact must
    # still be identical, and a document that does carry the predecessor must
    # carry the exact manifest value.
    document_predecessor = document_source.pop("predecessor_archive_id", None)
    manifest_source.pop("predecessor_archive_id", None)
    if document_source != manifest_source or document_predecessor not in {
        None,
        predecessor,
    }:
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    if (
        occurrence.get("authorization") != manifest.get("authorization")
        or occurrence.get("created_at") != manifest.get("created_at")
        or occurrence.get("verified_at")
        != manifest.get("publication", {}).get("verified_at")
    ):
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    return document


def hydrate_prompt_archive_state_view(
    runtime: PromptArchiveStateViewRuntime,
    *,
    sprint_id: str,
    archive_id: str,
    max_output_bytes: int | None = None,
    cancellation: PromptArchiveCancellationToken | None = None,
) -> bytes:
    """Return one verified root-to-head native PAS1 state view.

    ``archive_id`` is the head selected by an already-authorized caller.  The
    reader performs direct immutable locator lookups only; it never enumerates
    archives or consults a mutable latest alias.  The aggregate declared and
    observed hydrated bytes, occurrence count, and final canonical output all
    have hard bounds.  A cycle, broken cross-binding, non-native predecessor,
    or incomplete chain fails closed.
    """

    project_id = _safe_id(getattr(runtime, "project_id", None))
    sprint_id = _safe_id(sprint_id)
    head_archive_id = _archive_id(archive_id)
    maximum = _maximum(runtime, max_output_bytes)
    if cancellation is not None and not isinstance(
        cancellation,
        PromptArchiveCancellationToken,
    ):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    if cancellation is not None:
        cancellation.check()

    seen: set[str] = set()
    reverse_occurrences: list[dict[str, Any]] = []
    cursor: str | None = head_archive_id
    aggregate_hydrated_bytes = 0
    last_revision: int | None = None
    while cursor is not None:
        if cancellation is not None:
            cancellation.check()
        if cursor in seen:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if len(reverse_occurrences) >= _MAX_OCCURRENCES:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        seen.add(cursor)

        selected = runtime.lookup_archive_metadata(
            sprint_id=sprint_id,
            archive_id=cursor,
            cancellation=cancellation,
        )
        if cancellation is not None:
            cancellation.check()
        manifest, pointer, predecessor, declared_size = _native_manifest(
            selected,
            project_id=project_id,
            sprint_id=sprint_id,
            archive_id=cursor,
        )
        aggregate_hydrated_bytes += declared_size
        if aggregate_hydrated_bytes > maximum:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        raw = runtime.hydrate_content(
            selected,
            cancellation=cancellation,
        )
        if cancellation is not None:
            cancellation.check()
        if not isinstance(raw, bytes) or len(raw) != declared_size:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        document = _validated_document(
            raw,
            maximum=min(maximum, declared_size),
            manifest=manifest,
            predecessor=predecessor,
        )
        revision = document["occurrence"]["state_revision"]
        if last_revision is not None and revision > last_revision:
            # Traversal moves from head to root; revisions may repeat but must
            # never increase toward an older immutable predecessor.
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        last_revision = revision
        reverse_occurrences.append(
            {
                "archive_id": cursor,
                "pointer_id": pointer["pointer_id"],
                "manifest_sha256": pointer["manifest_ref"]["digest"],
                "predecessor_archive_id": predecessor,
                "document": document,
            }
        )
        cursor = predecessor

    if not reverse_occurrences:
        raise _error("PROMPT_ARCHIVE_NOT_FOUND")
    occurrences = list(reversed(reverse_occurrences))
    for ordinal, entry in enumerate(occurrences):
        if cancellation is not None:
            cancellation.check()
        entry["ordinal"] = ordinal
        expected_predecessor = None if ordinal == 0 else occurrences[ordinal - 1]["archive_id"]
        if entry["predecessor_archive_id"] != expected_predecessor:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    history: list[dict[str, Any]] = []
    for entry in occurrences:
        if cancellation is not None:
            cancellation.check()
        occurrence = entry["document"]["occurrence"]
        evidence = entry["document"]["evidence_delta"]
        history.append(
            {
                "occurrence_ordinal": entry["ordinal"],
                "archive_id": entry["archive_id"],
                "response_id": occurrence["response_id"],
                "response_kind": occurrence["response_kind"],
                "occurrence_kind": occurrence["occurrence_kind"],
                "occurrence_id": occurrence["occurrence_id"],
                "transition_id": occurrence["transition_id"],
                "state_revision": occurrence["state_revision"],
                "created_at": occurrence["created_at"],
                "evidence_keys": sorted(evidence),
            }
        )

    head = occurrences[-1]
    head_document = head["document"]
    head_occurrence = head_document["occurrence"]
    if cancellation is not None:
        cancellation.check()
    chain_bytes = canonical_json_bytes_v1(occurrences, maximum=maximum)
    view = {
        "schema_version": 1,
        "kind": "prompt_archive_state_view",
        "media_type": _MEDIA_TYPE,
        "scope": {
            "project_id": project_id,
            "sprint_id": sprint_id,
            "root_archive_id": occurrences[0]["archive_id"],
            "head_archive_id": head_archive_id,
            "head_response_id": head_occurrence["response_id"],
            "occurrence_count": len(occurrences),
        },
        "integrity": {
            "algorithm": "sha256",
            "chain_sha256": hashlib.sha256(chain_bytes).hexdigest(),
            "aggregate_hydrated_bytes": aggregate_hydrated_bytes,
        },
        "occurrences": occurrences,
        "state": {
            "current": {
                "occurrence_ordinal": head["ordinal"],
                "archive_id": head_archive_id,
                "response_id": head_occurrence["response_id"],
                "state_revision": head_occurrence["state_revision"],
                "execution_response": copy.deepcopy(
                    head_document["execution_response"]
                ),
                "evidence_delta": copy.deepcopy(head_document["evidence_delta"]),
            },
            "history": history,
        },
    }
    output = canonical_json_bytes_v1(view, maximum=maximum)
    if cancellation is not None:
        cancellation.check()
    return output


__all__ = [
    "PromptArchiveStateViewRuntime",
    "hydrate_prompt_archive_state_view",
]
