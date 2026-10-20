"""Pure native source-metadata projection for Prompt Archive V1.

The owner supplies an already-frozen credential screen.  This module never
discovers credentials and never reads environment, filesystem, network, or
process state.  It turns one caller-owned mapping into the closed
``prompt-archive-source-metadata-v1`` document and a secret-free receipt that
can be frozen in a durable occurrence before archive planning.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import hmac
import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, Final

from .prompt_archive_storage import canonical_json_bytes_v1


_PROJECTION_VERSION: Final = "prompt-archive-source-projection-v1"
_REDACTION_VERSION: Final = "prompt-archive-source-redaction-v1"
_REDACTED: Final = "[REDACTED]"
_MAX_DISCARDED_FIELDS: Final = 1024
_MAX_CANONICAL_BYTES: Final = 16 * 1024

_SOURCE_TYPES: Final = frozenset(
    {"api", "email", "folder", "manual", "telegram", "unknown"}
)
_IDENTIFIER_FIELDS: Final = (
    "conversation_id",
    "thread_id",
    "message_id",
)
_DISPLAY_FIELDS: Final = ("sender_label", "title")
_OPTIONAL_FIELDS: Final = frozenset(
    {*_IDENTIFIER_FIELDS, *_DISPLAY_FIELDS, "observed_at"}
)
_INPUT_FIELDS: Final = frozenset({"source_type", *_OPTIONAL_FIELDS})
_REQUIRED_PROJECTION_FIELDS: Final = frozenset(
    {
        "schema_version",
        "kind",
        "projection_version",
        "redaction_version",
        "source_type",
        "redaction_count",
        "redacted_fields",
        "discarded_field_count",
    }
)
_RECEIPT_FIELDS: Final = frozenset(
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

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_PROJECTED_IDENTIFIER_RE: Final = re.compile(
    r"^(?:sha256:[0-9a-f]{64}|\[REDACTED\])$", re.ASCII
)
_RFC3339_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$",
    re.ASCII,
)
_REGISTERED_TOKEN_CANDIDATE_RE: Final = re.compile(
    r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{43,}(?![A-Za-z0-9_-])",
    re.ASCII,
)
_SOURCE_SECRET_PATTERNS: Final = tuple(
    re.compile(pattern, re.IGNORECASE | re.ASCII)
    for pattern in (
        r"[\x00-\x1f\x7f]",
        r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"']+",
        r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----",
        r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])",
        r"\b(?:Bearer|Basic)[ \t]+\S+",
        r"\b(?:sk-(?:proj-)?|ghp_|github_pat_|glpat-|xox[A-Za-z0-9]{1,8}-)[A-Za-z0-9_-]{8,}",
        r"(?:^|[-_.\s])(?:authorization|cookie|access[-_]?token|api[-_]?key|apikey|bearer[-_]?token|credentials?|password|passwd|private[-_]?key|secrets?|token|pending[-_]?token|producer[-_]?token|dpapi|webhook)(?:$|[-_.\s:=])",
        r"(?:^|[\s'\"(])(?:[A-Za-z]:[\\/]|\\\\[^\\/\s]+[\\/]|//[^/\s]+/|/(?:[^/\s]+(?:/[^/\s]*)*))",
    )
)


class PromptArchiveSourceMetadataError(ValueError):
    """Stable, payload-free error raised by the pure projection boundary."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__("Prompt archive source metadata is invalid.")


def _error(code: str) -> PromptArchiveSourceMetadataError:
    return PromptArchiveSourceMetadataError(code)


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class SourceMetadataScreenSnapshot:
    """Ephemeral immutable credential/token-digest screen.

    The values are intentionally private and the representation exposes only
    counts so an exception or debug representation cannot persist a secret.
    """

    _short_configured_values: tuple[str, ...]
    _long_configured_values: tuple[str, ...]
    _registered_token_sha256: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            type(self._short_configured_values) is not tuple
            or type(self._long_configured_values) is not tuple
            or type(self._registered_token_sha256) is not tuple
            or any(
                type(value) is not str or not 1 <= len(value) <= 3
                for value in self._short_configured_values
            )
            or any(
                type(value) is not str or len(value) < 4
                for value in self._long_configured_values
            )
            or any(
                type(digest) is not str or _SHA256_RE.fullmatch(digest) is None
                for digest in self._registered_token_sha256
            )
            or len(set(self._short_configured_values))
            != len(self._short_configured_values)
            or len(set(self._long_configured_values))
            != len(self._long_configured_values)
            or len(set(self._registered_token_sha256))
            != len(self._registered_token_sha256)
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

    def __repr__(self) -> str:
        return (
            "SourceMetadataScreenSnapshot("
            f"short_configured_count={len(self._short_configured_values)}, "
            f"long_configured_count={len(self._long_configured_values)}, "
            f"registered_token_digest_count={len(self._registered_token_sha256)})"
        )


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class SourceMetadataProjection:
    """Frozen canonical projection bytes plus their closed durable receipt."""

    _document: dict[str, Any]
    canonical_bytes: bytes
    sha256: str
    _receipt: dict[str, Any]

    @property
    def document(self) -> dict[str, Any]:
        return copy.deepcopy(self._document)

    @property
    def receipt(self) -> dict[str, Any]:
        return copy.deepcopy(self._receipt)

    def __repr__(self) -> str:
        return (
            "SourceMetadataProjection("
            f"sha256={self.sha256!r}, byte_size={len(self.canonical_bytes)}, "
            f"source_type={self._document['source_type']!r})"
        )


def _freeze_iterable(values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray, memoryview)):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    try:
        iterator = iter(values)
        frozen: list[str] = []
        observed: set[str] = set()
        for value in iterator:
            if type(value) is not str:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            if value not in observed:
                observed.add(value)
                frozen.append(value)
        return tuple(frozen)
    except PromptArchiveSourceMetadataError:
        raise
    except Exception:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None


def freeze_source_metadata_screen(
    *,
    configured_values: Iterable[str] = (),
    registered_token_sha256: Iterable[str] = (),
) -> SourceMetadataScreenSnapshot:
    """Freeze caller-supplied credentials without discovering or persisting them."""

    configured = _freeze_iterable(configured_values)
    digests = _freeze_iterable(registered_token_sha256)
    if any(_SHA256_RE.fullmatch(digest) is None for digest in digests):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    short_values = tuple(value for value in configured if 1 <= len(value) <= 3)
    long_values = tuple(value for value in configured if len(value) >= 4)
    return SourceMetadataScreenSnapshot(short_values, long_values, digests)


def _valid_rfc3339(value: Any) -> bool:
    if type(value) is not str or _RFC3339_RE.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _fixed_screen_rejects(value: str) -> bool:
    return (
        value == _REDACTED
        or len(value) > 256
        or any(pattern.search(value) is not None for pattern in _SOURCE_SECRET_PATTERNS)
    )


def _registered_token_matches(
    value: str,
    snapshot: SourceMetadataScreenSnapshot,
) -> bool:
    matched = False
    for candidate in _REGISTERED_TOKEN_CANDIDATE_RE.finditer(value):
        token_run = candidate.group(0)
        digest = hashlib.sha256(token_run.encode("ascii")).hexdigest()
        for registered_digest in snapshot._registered_token_sha256:
            # Put compare_digest first so every frozen digest is compared even
            # after a match; ``or matched`` must not short-circuit the call.
            matched = hmac.compare_digest(digest, registered_digest) or matched
        # The protected registry intentionally persists only token digests, not
        # plaintext lengths.  A maximal token-character run longer than the
        # canonical minimum can contain a registered token with adjacent
        # token characters (for example ``x<TOKEN>y``), while its whole-run
        # digest differs.  Fail closed for that ambiguous bounded case instead
        # of allowing a digest-only inventory to be bypassed by concatenation.
        if snapshot._registered_token_sha256 and len(token_run) > 43:
            matched = True
    return matched


def _screen_rejects(value: Any, snapshot: SourceMetadataScreenSnapshot) -> bool:
    if type(value) is not str or not value or _fixed_screen_rejects(value):
        return True
    if value in snapshot._short_configured_values:
        return True
    if any(secret in value for secret in snapshot._long_configured_values):
        return True
    return _registered_token_matches(value, snapshot)


def _project_identifier(
    value: Any,
    snapshot: SourceMetadataScreenSnapshot,
) -> tuple[str, bool]:
    if _screen_rejects(value, snapshot):
        return _REDACTED, True
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError:
        return _REDACTED, True
    return "sha256:" + hashlib.sha256(encoded).hexdigest(), False


def _project_display(
    value: Any,
    snapshot: SourceMetadataScreenSnapshot,
) -> tuple[str, bool]:
    if _screen_rejects(value, snapshot):
        return _REDACTED, True
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        return _REDACTED, True
    return value, False


def _copy_mapping(value: Any, *, code: str) -> dict[Any, Any]:
    if not isinstance(value, Mapping):
        raise _error(code)
    try:
        return dict(value.items())
    except Exception:
        raise _error(code) from None


def _expected_receipt(
    document: dict[str, Any], canonical_bytes: bytes, digest: str
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "prompt_archive_source_metadata_projection_receipt",
        "projection_version": _PROJECTION_VERSION,
        "redaction_version": _REDACTION_VERSION,
        "projection_sha256": digest,
        "projection_byte_size": len(canonical_bytes),
        "source_type": document["source_type"],
        "discarded_field_count": document["discarded_field_count"],
        "projected_fields": sorted(
            field for field in _OPTIONAL_FIELDS if field in document
        ),
    }


def _validate_receipt(receipt: Any, expected: dict[str, Any]) -> None:
    value = _copy_mapping(receipt, code="PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    digest = value.get("projection_sha256")
    if (
        set(value) != _RECEIPT_FIELDS
        or type(digest) is not str
        or _SHA256_RE.fullmatch(digest) is None
        or not hmac.compare_digest(digest, expected["projection_sha256"])
    ):
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    value_without_digest = dict(value)
    expected_without_digest = dict(expected)
    value_without_digest.pop("projection_sha256")
    expected_without_digest.pop("projection_sha256")
    if value_without_digest != expected_without_digest:
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")


def validate_source_metadata_projection(
    document: Mapping[str, Any],
    receipt: Mapping[str, Any] | None = None,
) -> SourceMetadataProjection:
    """Validate/canonicalize a projected document and optionally its receipt."""

    value = _copy_mapping(document, code="PROMPT_ARCHIVE_REFERENCE_INVALID")
    if (
        not _REQUIRED_PROJECTION_FIELDS.issubset(value)
        or not set(value).issubset(_REQUIRED_PROJECTION_FIELDS | _OPTIONAL_FIELDS)
        or value.get("schema_version") != 1
        or value.get("kind") != "prompt_archive_source_metadata"
        or value.get("projection_version") != _PROJECTION_VERSION
        or value.get("redaction_version") != _REDACTION_VERSION
        or type(value.get("source_type")) is not str
        or value.get("source_type") not in _SOURCE_TYPES
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    redacted_fields = value.get("redacted_fields")
    if (
        type(redacted_fields) is not list
        or len(redacted_fields) > len(_IDENTIFIER_FIELDS) + len(_DISPLAY_FIELDS)
        or any(
            type(field) is not str
            or field not in {*_IDENTIFIER_FIELDS, *_DISPLAY_FIELDS}
            for field in redacted_fields
        )
        or redacted_fields != sorted(set(redacted_fields))
        or type(value.get("redaction_count")) is not int
        or value["redaction_count"] != len(redacted_fields)
        or type(value.get("discarded_field_count")) is not int
        or not 0 <= value["discarded_field_count"] <= _MAX_DISCARDED_FIELDS
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    observed_redacted = sorted(
        field
        for field in (*_IDENTIFIER_FIELDS, *_DISPLAY_FIELDS)
        if value.get(field) == _REDACTED
    )
    if observed_redacted != redacted_fields:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    for field in _IDENTIFIER_FIELDS:
        if field in value and (
            type(value[field]) is not str
            or _PROJECTED_IDENTIFIER_RE.fullmatch(value[field]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    for field in _DISPLAY_FIELDS:
        if field not in value:
            continue
        display = value[field]
        if (
            type(display) is not str
            or not display
            or len(display) > 256
            or (display != _REDACTED and _fixed_screen_rejects(display))
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            display.encode("utf-8", errors="strict")
        except UnicodeError:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    if "observed_at" in value and not _valid_rfc3339(value["observed_at"]):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    normalized = copy.deepcopy(value)
    try:
        canonical = canonical_json_bytes_v1(
            normalized, maximum=_MAX_CANONICAL_BYTES
        )
    except Exception as exc:
        code = getattr(exc, "code", "PROMPT_ARCHIVE_REFERENCE_INVALID")
        if code == "PROMPT_ARCHIVE_LIMIT_EXCEEDED":
            raise _error(code) from None
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    digest = hashlib.sha256(canonical).hexdigest()
    expected_receipt = _expected_receipt(normalized, canonical, digest)
    if receipt is not None:
        _validate_receipt(receipt, expected_receipt)
    return SourceMetadataProjection(
        normalized,
        canonical,
        digest,
        expected_receipt,
    )


def project_source_metadata(
    raw_metadata: Mapping[str, Any],
    *,
    screen: SourceMetadataScreenSnapshot,
) -> SourceMetadataProjection:
    """Project raw owner metadata using one caller-frozen screen snapshot."""

    if type(screen) is not SourceMetadataScreenSnapshot:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    raw = _copy_mapping(raw_metadata, code="PROMPT_ARCHIVE_REQUEST_INVALID")
    discarded = sum(
        1 for field in raw if type(field) is not str or field not in _INPUT_FIELDS
    )
    if discarded > _MAX_DISCARDED_FIELDS:
        raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    allowed_raw = {
        field: value
        for field, value in raw.items()
        if type(field) is str and field in _INPUT_FIELDS
    }
    source_type = allowed_raw.get("source_type")
    safe_source_type = (
        source_type
        if (
            type(source_type) is str
            and source_type in _SOURCE_TYPES
            and not _screen_rejects(source_type, screen)
        )
        else "unknown"
    )
    projection: dict[str, Any] = {
        "schema_version": 1,
        "kind": "prompt_archive_source_metadata",
        "projection_version": _PROJECTION_VERSION,
        "redaction_version": _REDACTION_VERSION,
        "source_type": safe_source_type,
        "redaction_count": 0,
        "redacted_fields": [],
        "discarded_field_count": discarded,
    }
    redacted: list[str] = []
    for field in _IDENTIFIER_FIELDS:
        if field not in allowed_raw:
            continue
        projected, was_redacted = _project_identifier(allowed_raw[field], screen)
        projection[field] = projected
        if was_redacted:
            redacted.append(field)
    for field in _DISPLAY_FIELDS:
        if field not in allowed_raw:
            continue
        projected, was_redacted = _project_display(allowed_raw[field], screen)
        projection[field] = projected
        if was_redacted:
            redacted.append(field)
    if (
        "observed_at" in allowed_raw
        and _valid_rfc3339(allowed_raw["observed_at"])
        and not _screen_rejects(allowed_raw["observed_at"], screen)
    ):
        # A timestamp remains optional.  Omit it on credential collision rather
        # than persisting the credential verbatim or inventing a non-RFC3339
        # redaction sentinel that would weaken the closed contract.
        projection["observed_at"] = allowed_raw["observed_at"]
    projection["redacted_fields"] = sorted(redacted)
    projection["redaction_count"] = len(redacted)
    return validate_source_metadata_projection(projection)


__all__ = [
    "PromptArchiveSourceMetadataError",
    "SourceMetadataProjection",
    "SourceMetadataScreenSnapshot",
    "freeze_source_metadata_screen",
    "project_source_metadata",
    "validate_source_metadata_projection",
]
