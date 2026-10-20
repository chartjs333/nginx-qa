"""Closed runtime publication and lookup facade for Prompt Archive V1.

The legacy response writer remains authoritative in ``shadow`` mode.  Its
already-committed exact bytes enter :meth:`PromptArchiveRuntime.publish_shadow`
only after the legacy files have succeeded.  The runtime builds and rehydrates
the PAS1 graph before publishing a generation-one response pointer and, last,
the immutable archive locator that makes the archive discoverable.

Publication is driven by the accepted PAS1 durable protocol: an immutable
publication plan, a bounded 256-shard pending-operation registry, and a
hash-chained retained/current journal.  Startup recovery enumerates only those
fixed shard targets and resumes each registered operation idempotently.  The
legacy response remains authoritative in shadow mode even if archive work
fails; archive discovery appears only after the journaled locator boundary.

The separately gated ``authoritative`` mode accepts only a bounded native
response document.  It publishes the same immutable PAS1 graph without a
legacy comparison and never falls back to the cumulative legacy writer.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
import os
import re
import threading
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Protocol

from .prompt_archive_incremental import (
    ArchiveOccurrence,
    PromptArchiveIncremental,
    pas1_id_v1,
    response_id_v1,
    sprint_storage_key_v1,
)
from .prompt_archive_occurrence_store import (
    PromptArchiveNativeOccurrenceProof,
    PromptArchiveNativeOccurrenceSource,
    PromptArchiveOccurrenceProof,
    PromptArchiveOccurrenceSource,
    PromptArchiveSourceMetadataProjectionProof,
)
from .prompt_archive_source_metadata import (
    PromptArchiveSourceMetadataError,
    SourceMetadataProjection,
    SourceMetadataScreenSnapshot,
    project_source_metadata,
    validate_source_metadata_projection,
)
from .prompt_archive_response import dispatch_prompt_archive_response
from .prompt_archive_metrics import PromptArchiveMetrics
from .prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
    parse_canonical_json_v1,
    _error,
)


_MIB: Final = 1024 * 1024
_DEFAULT_MAX_HYDRATED_BYTES: Final = 256 * _MIB
_MAX_MAX_HYDRATED_BYTES: Final = 512 * _MIB
_CONTROL_MAX_BYTES: Final = 64 * 1024
_MANIFEST_MAX_BYTES: Final = 256 * 1024
_PLAN_MAX_BYTES: Final = 8 * _MIB
_JOURNAL_MAX_BYTES: Final = 1024 * 1024
_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$", re.ASCII)
_RESPONSE_ID_RE: Final = re.compile(r"^parresp1-[0-9a-f]{64}$", re.ASCII)
_ARCHIVE_ID_RE: Final = re.compile(r"^par1-[0-9a-f]{64}$", re.ASCII)
_POINTER_ID_RE: Final = re.compile(r"^parptr1-[0-9a-f]{64}$", re.ASCII)
_LOCATOR_ID_RE: Final = re.compile(r"^pararc1-[0-9a-f]{64}$", re.ASCII)
_INVENTORY_ID_RE: Final = re.compile(r"^parinv1-[0-9a-f]{64}$", re.ASCII)
_MIGRATION_BATCH_ID_RE: Final = re.compile(r"^parmb1-[0-9a-f]{64}$", re.ASCII)
_MIGRATION_ENTRY_ID_RE: Final = re.compile(r"^parme1-[0-9a-f]{64}$", re.ASCII)
_STUB_ID_RE: Final = re.compile(r"^parstub1-[0-9a-f]{64}$", re.ASCII)
_LATEST_INTENT_ID_RE: Final = re.compile(
    r"^parlatestintent1-[0-9a-f]{64}$", re.ASCII
)
_LATEST_SLOT_ID_RE: Final = re.compile(r"^parlatest1-[0-9a-f]{64}$", re.ASCII)
_TARGET_AUTHORITY_ID_RE: Final = re.compile(
    r"^parlatestauthority1-[0-9a-f]{64}$", re.ASCII
)
_OPERATION_ID_RE: Final = re.compile(r"^parop1-[0-9a-f]{32}$", re.ASCII)
_RECOVERY_KEY_RE: Final = re.compile(r"^parloc1-[0-9a-f]{64}$", re.ASCII)
_JOURNAL_ID_RE: Final = re.compile(r"^parjournal1-[0-9a-f]{64}$", re.ASCII)
_PENDING_ID_RE: Final = re.compile(r"^parpending1-[0-9a-f]{64}$", re.ASCII)
_SHARD_RE: Final = re.compile(r"^[0-9a-f]{2}$", re.ASCII)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_LEGACY_RESPONSE_KEY_RE: Final = re.compile(r"^[0-9a-f]{20}$", re.ASCII)
_RFC3339_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$",
    re.ASCII,
)

_MANIFEST_MEDIA: Final = "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
_POINTER_MEDIA: Final = "application/vnd.nginx-qa.prompt-archive-pointer-v1+json"
_LOCATOR_MEDIA: Final = "application/vnd.nginx-qa.prompt-archive-locator-v1+json"
_STUB_MEDIA: Final = "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
_LATEST_SLOT_MEDIA: Final = "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json"
_COMPATIBILITY_RESULT_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-compatibility-result-v1+json"
)
_PLAN_MEDIA: Final = "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
_JOURNAL_MEDIA: Final = "application/vnd.nginx-qa.prompt-archive-journal-v1+json"
_PENDING_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-pending-operation-registry-v1+json"
)
_INVENTORY_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json"
)
_OFFLINE_OPERATOR_GATE: Final = "prompt-archive-offline-migration-v1"
_OFFLINE_MIGRATION_CONTRACT: Final = "prompt-archive-offline-migration-contract-v1"
_OFFLINE_MIGRATION_TOOL: Final = "prompt-archive-migration-v1"
_MIGRATION_BATCH_PLAN_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-migration-batch-plan-v1+json"
)
_JOURNAL_STATES: Final = (
    "intent",
    "blobs_verified",
    "manifest_verified",
    "pointer_published",
    "locator_published",
    "compatibility_published",
    "complete",
)
# Runtime source proof is an intent-only gate.  Once ``blobs_verified`` is
# durable, the closed plan/manifest graph is the recovery source of truth.
_PRE_POINTER_STATES: Final = frozenset({"intent"})
_ABORT_REASON_CODES: Final = frozenset(
    {
        "source_unavailable",
        "source_changed",
        "limit_exceeded",
        "blob_verification_failed",
        "manifest_verification_failed",
        "generation_conflict",
        "operator_cancelled",
    }
)
_JOURNAL_TIMESTAMP_KEYS: Final = (
    "intent_at",
    "blobs_verified_at",
    "manifest_verified_at",
    "pointer_published_at",
    "locator_published_at",
    "compatibility_published_at",
    "completed_at",
)


class _JsonObjectPairs(list[tuple[str, Any]]):
    """Preserve object members while inspecting only the top-level control key."""


def _require_safe_id(value: Any) -> str:
    if type(value) is not str or _SAFE_ID_RE.fullmatch(value) is None:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    return value


def _require_rfc3339(value: Any) -> str:
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


def _require_ref_kind(value: Any, *, logical_kind: str, media_type: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    digest = value.get("digest")
    if (
        type(digest) is not str
        or _SHA256_RE.fullmatch(digest) is None
        or value.get("logical_kind") != logical_kind
        or value.get("media_type") != media_type
    ):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return value


def _copy_document(value: Mapping[str, Any], *, maximum: int) -> dict[str, Any]:
    raw = canonical_json_bytes_v1(dict(value), maximum=maximum)
    return parse_canonical_json_v1(raw, maximum=maximum)


def _freeze_bytes(
    value: bytes | bytearray | memoryview,
    *,
    maximum: int,
) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    try:
        view = memoryview(value)
        try:
            if view.nbytes > maximum:
                raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
            return view.tobytes()
        finally:
            view.release()
    except PromptArchiveStorageError:
        raise
    except (TypeError, ValueError, BufferError):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None


def _reachable_set_document(
    refs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    lines: set[bytes] = set()
    digest_roles: dict[str, tuple[str, str]] = {}
    includes_manifest = False
    for ref in refs:
        if not isinstance(ref, Mapping):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        values = (
            ref.get("logical_kind"),
            ref.get("media_type"),
            ref.get("digest"),
            ref.get("stored_sha256"),
            ref.get("uncompressed_size"),
            ref.get("stored_size"),
        )
        if (
            type(values[0]) is not str
            or type(values[1]) is not str
            or type(values[2]) is not str
            or _SHA256_RE.fullmatch(values[2]) is None
            or type(values[3]) is not str
            or _SHA256_RE.fullmatch(values[3]) is None
            or type(values[4]) is not int
            or type(values[5]) is not int
            or values[4] < 0
            or values[5] < 0
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        role = (values[0], values[1])
        previous_role = digest_roles.get(values[2])
        if previous_role is not None and previous_role != role:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        digest_roles[values[2]] = role
        includes_manifest = includes_manifest or values[0] == "manifest"
        lines.add(
            values[0].encode("utf-8")
            + b"\0"
            + values[2].encode("ascii")
            + b"\0"
            + values[3].encode("ascii")
            + b"\0"
            + str(values[4]).encode("ascii")
            + b"\0"
            + str(values[5]).encode("ascii")
            + b"\n"
        )
    if not lines or not includes_manifest:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    digest = hashlib.sha256(
        b"prompt-archive-reachable-set-digest-v1\n" + b"".join(sorted(lines))
    ).hexdigest()
    return {
        "digest_version": "prompt-archive-reachable-set-digest-v1",
        "hash_algorithm": "sha256",
        "digest": digest,
        "unique_ref_count": len(lines),
        "includes_manifest": True,
    }


def _closed_ref(
    value: Any,
    *,
    logical_kind: str,
    media_type: str,
) -> dict[str, Any]:
    return copy.deepcopy(
        _require_ref_kind(value, logical_kind=logical_kind, media_type=media_type)
    )


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchiveRuntimeConfig:
    mode: str = "legacy"
    root: Path | None = None
    max_hydrated_bytes: int = _DEFAULT_MAX_HYDRATED_BYTES
    operator_gate: str | None = None
    protected_roots: tuple[Path, ...] = ()
    runtime_instance_id: str = "nginx-qa-runtime"

    def __post_init__(self) -> None:
        if self.mode not in {"legacy", "shadow", "authoritative"}:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if self.root is not None and (
            not isinstance(self.root, Path) or not self.root.is_absolute()
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if self.mode == "shadow" and self.root is None:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if (
            type(self.max_hydrated_bytes) is not int
            or not 1 <= self.max_hydrated_bytes <= _MAX_MAX_HYDRATED_BYTES
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if not isinstance(self.protected_roots, tuple) or any(
            not isinstance(root, Path) or not root.is_absolute()
            for root in self.protected_roots
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if self.operator_gate is not None and type(self.operator_gate) is not str:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if (
            type(self.runtime_instance_id) is not str
            or _SAFE_ID_RE.fullmatch(self.runtime_instance_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if self.mode == "authoritative":
            if self.operator_gate != "prompt-archive-authoritative-v1":
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            if self.root is None:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        protected_roots: Sequence[str | os.PathLike[str]] = (),
    ) -> "PromptArchiveRuntimeConfig":
        source = os.environ if environ is None else environ
        if not isinstance(source, Mapping):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        mode = source.get("NGINX_QA_PROMPT_ARCHIVE_MODE", "legacy")
        if type(mode) is not str or mode not in {"legacy", "shadow", "authoritative"}:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        raw_root = source.get("NGINX_QA_PROMPT_ARCHIVE_ROOT")
        if raw_root in {None, ""}:
            root = None
        elif type(raw_root) is str:
            root = Path(raw_root)
            if not root.is_absolute():
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        else:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        raw_maximum = source.get("NGINX_QA_PROMPT_ARCHIVE_MAX_HYDRATED_BYTES")
        if raw_maximum in {None, ""}:
            maximum = _DEFAULT_MAX_HYDRATED_BYTES
        elif type(raw_maximum) is str and re.fullmatch(r"[1-9][0-9]*", raw_maximum):
            maximum = int(raw_maximum)
        else:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        gate = source.get("NGINX_QA_PROMPT_ARCHIVE_OPERATOR_GATE")
        instance_id = source.get("NGINX_QA_INSTANCE_ID") or "nginx-qa-runtime"
        try:
            roots = tuple(Path(value) for value in protected_roots)
        except (TypeError, ValueError):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
        return cls(
            mode=mode,
            root=root,
            max_hydrated_bytes=maximum,
            operator_gate=gate,
            protected_roots=roots,
            runtime_instance_id=instance_id,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchiveRead:
    project_id: str
    sprint_id: str | None
    archive_id: str
    locator: Mapping[str, Any]
    pointer: Mapping[str, Any]
    manifest: Mapping[str, Any]

    @property
    def archive_format(self) -> str:
        return self.manifest["archive_format"]

    @property
    def scope(self) -> Mapping[str, Any]:
        return self.manifest["scope"]

    @property
    def integrity(self) -> Mapping[str, Any]:
        return self.manifest["integrity"]

    @property
    def created_at(self) -> str:
        return self.manifest["created_at"]


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchivePublication:
    project_id: str
    sprint_id: str | None
    archive_id: str
    archive_format: str
    scope: Mapping[str, Any]
    integrity: Mapping[str, Any]
    created_at: str
    pointer_id: str
    manifest_ref: Mapping[str, Any]
    locator: Mapping[str, Any]


class PromptArchiveOccurrenceProver(Protocol):
    """Closed durable adapter for runtime-occurrence recovery proofs.

    ``register_occurrence`` is an immutable create-or-verify operation.  A
    durable implementation must flush the fact before returning and must never
    overwrite a different occurrence under the same closed locator.
    ``load_occurrence`` resolves only that exact locator, returns ``None`` only
    when no receipt exists, and reports current source loss/change through the
    closed proof status.  Implementations must not discover facts by scanning
    directories or by consulting a mutable "latest" alias.
    """

    def register_occurrence(
        self,
        *,
        project_id: str,
        locator: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        source: PromptArchiveOccurrenceSource,
    ) -> None: ...

    def register_native_occurrence(
        self,
        *,
        project_id: str,
        locator: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        source: PromptArchiveNativeOccurrenceSource,
    ) -> None: ...

    def load_occurrence(
        self,
        *,
        project_id: str,
        locator: Mapping[str, Any],
    ) -> PromptArchiveOccurrenceProof | PromptArchiveNativeOccurrenceProof | None: ...

    def load_source_metadata_projection(
        self,
        *,
        project_id: str,
        locator: Mapping[str, Any],
    ) -> PromptArchiveSourceMetadataProjectionProof | None: ...

    def load_or_register_source_metadata_projection(
        self,
        *,
        project_id: str,
        locator: Mapping[str, Any],
        projector: Callable[[], Any],
    ) -> PromptArchiveSourceMetadataProjectionProof: ...


class PromptArchiveOfflineInventoryVerifier(Protocol):
    """Re-prove one explicitly supplied isolated source at journal intent.

    The verifier is deliberately process-local and path-free from the
    runtime's perspective.  Offline recovery has no authority to discover or
    scan an input root: the operator/tool must supply the exact root handle
    represented by the immutable inventory locator on every resume.
    """

    def __call__(self, locator: Mapping[str, Any]) -> str: ...


class PromptArchiveCompatibilityPublisher(Protocol):
    """Trusted adapter for the path-free authoritative compatibility plan."""

    @property
    def target_authority_id(self) -> str: ...

    def prepare_authoritative(
        self,
        *,
        descriptor_bytes: bytes,
        descriptor_ref: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        operation_id: str,
        published_at: str,
    ) -> Mapping[str, Any]: ...

    def publish_authoritative(
        self,
        compatibility_publication: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    def verify_authoritative(
        self,
        compatibility_publication: Mapping[str, Any],
        publication: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class PromptArchiveRuntime:
    """One project-bound Prompt Archive runtime."""

    def __init__(
        self,
        config: PromptArchiveRuntimeConfig,
        project_id: str,
        storage: PromptArchiveStorage | None,
        occurrence_prover: PromptArchiveOccurrenceProver | None = None,
        compatibility_resolver: Callable[
            [str, Mapping[str, Any]], PromptArchiveCompatibilityPublisher | None
        ]
        | None = None,
        metrics: PromptArchiveMetrics | None = None,
    ) -> None:
        if not isinstance(config, PromptArchiveRuntimeConfig):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        self.config = config
        self.project_id = _require_safe_id(project_id)
        if storage is not None and storage.project_id != self.project_id:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if occurrence_prover is not None and (
            not callable(getattr(occurrence_prover, "register_occurrence", None))
            or not callable(getattr(occurrence_prover, "load_occurrence", None))
            or (
                config.mode == "authoritative"
                and not callable(
                    getattr(occurrence_prover, "register_native_occurrence", None)
                )
            )
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if metrics is not None and not isinstance(metrics, PromptArchiveMetrics):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if compatibility_resolver is not None and not callable(
            compatibility_resolver
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        self.storage = storage
        self._occurrence_prover = occurrence_prover
        self._compatibility_resolver = compatibility_resolver
        self.metrics = metrics if metrics is not None else PromptArchiveMetrics()
        self._live_occurrence_proofs: dict[
            bytes,
            PromptArchiveOccurrenceProof | PromptArchiveNativeOccurrenceProof,
        ] = {}
        self._live_source_metadata_proofs: dict[
            bytes,
            PromptArchiveSourceMetadataProjectionProof,
        ] = {}
        self._source_metadata_lock = threading.Lock()
        # Projection selection and publication share one re-entrant stream
        # serialization boundary.  This prevents concurrent absent/present
        # metadata callers from freezing different identities for the same
        # occurrence while still allowing publish_* to call the public binder.
        self._stream_lock = threading.RLock()
        self._predecessor_archives: OrderedDict[str, str] = OrderedDict()

    @property
    def max_hydrated_bytes(self) -> int:
        return self.config.max_hydrated_bytes

    def _require_storage(self) -> PromptArchiveStorage:
        if self.storage is None:
            raise _error("PROMPT_ARCHIVE_NOT_FOUND")
        return self.storage

    @staticmethod
    def _require_compatibility_publisher(
        value: PromptArchiveCompatibilityPublisher | None,
        *,
        expected_target_authority_id: str | None = None,
    ) -> PromptArchiveCompatibilityPublisher:
        if value is None or not callable(
            getattr(value, "prepare_authoritative", None)
        ) or not callable(
            getattr(value, "publish_authoritative", None)
        ) or not callable(getattr(value, "verify_authoritative", None)):
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        target_authority_id = getattr(value, "target_authority_id", None)
        if (
            type(target_authority_id) is not str
            or _TARGET_AUTHORITY_ID_RE.fullmatch(target_authority_id) is None
            or (
                expected_target_authority_id is not None
                and target_authority_id != expected_target_authority_id
            )
        ):
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        return value

    def _resolve_compatibility_publisher(
        self,
        plan: Mapping[str, Any],
        supplied: PromptArchiveCompatibilityPublisher | None,
    ) -> PromptArchiveCompatibilityPublisher:
        compatibility = plan.get("compatibility_publication")
        expected_target = (
            compatibility.get("target_authority_id")
            if isinstance(compatibility, Mapping)
            else None
        )
        if supplied is not None:
            return self._require_compatibility_publisher(
                supplied,
                expected_target_authority_id=expected_target,
            )
        if self._compatibility_resolver is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        try:
            resolved = self._compatibility_resolver(
                self.project_id,
                copy.deepcopy(dict(plan)),
            )
        except PromptArchiveStorageError:
            raise
        except Exception:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        return self._require_compatibility_publisher(
            resolved,
            expected_target_authority_id=expected_target,
        )

    @staticmethod
    def _occurrence_locator_key(locator: Mapping[str, Any]) -> bytes:
        return canonical_json_bytes_v1(dict(locator), maximum=_CONTROL_MAX_BYTES)

    @staticmethod
    def _freeze_occurrence(occurrence: ArchiveOccurrence) -> ArchiveOccurrence:
        if not isinstance(occurrence, ArchiveOccurrence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        projection = occurrence.source_metadata_projection
        receipt = occurrence.source_metadata_receipt
        if (projection is None) != (receipt is None):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return dataclasses.replace(
            occurrence,
            source=_copy_document(occurrence.source, maximum=16 * 1024),
            authorization=_copy_document(
                occurrence.authorization,
                maximum=16 * 1024,
            ),
            source_metadata_projection=(
                None
                if projection is None
                else _copy_document(projection, maximum=16 * 1024)
            ),
            source_metadata_receipt=(
                None
                if receipt is None
                else _copy_document(receipt, maximum=16 * 1024)
            ),
        )

    def _runtime_occurrence_locator(
        self,
        occurrence: ArchiveOccurrence,
    ) -> dict[str, Any]:
        if (
            not isinstance(occurrence, ArchiveOccurrence)
            or occurrence.project_id != self.project_id
            or occurrence.sprint_id is None
            or type(occurrence.state_revision) is not int
            or not 0 <= occurrence.state_revision <= 9_223_372_036_854_775_807
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_safe_id(occurrence.occurrence_id)
        return {
            "locator_kind": "runtime_occurrence",
            "locator_version": "prompt-archive-runtime-occurrence-locator-v1",
            "runtime_instance_id": self.config.runtime_instance_id,
            "runtime_occurrence_id": occurrence.occurrence_id,
            "state_revision": occurrence.state_revision,
        }

    @staticmethod
    def _validated_source_metadata_proof(
        proof: Any,
    ) -> SourceMetadataProjection:
        if not isinstance(proof, PromptArchiveSourceMetadataProjectionProof):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            validated = validate_source_metadata_projection(
                proof.document,
                proof.receipt,
            )
        except PromptArchiveSourceMetadataError as exc:
            raise _error(exc.code) from None
        if (
            validated.canonical_bytes != proof.canonical_bytes
            or validated.sha256 != proof.sha256
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return validated

    @staticmethod
    def _source_metadata_proof_from_projection(
        projection: SourceMetadataProjection,
    ) -> PromptArchiveSourceMetadataProjectionProof:
        return PromptArchiveSourceMetadataProjectionProof(
            document=projection.document,
            canonical_bytes=projection.canonical_bytes,
            sha256=projection.sha256,
            receipt=projection.receipt,
        )

    def _load_existing_occurrence_plan(
        self,
        occurrence: ArchiveOccurrence,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """Load and fully validate the immutable plan for one retry, if any."""

        storage = self._require_storage()
        operation_id = self._operation_id(occurrence)
        path = (
            "operations",
            "v1",
            occurrence.response_id,
            operation_id,
            "plan.json",
        )
        try:
            plan = storage.read_immutable_json(path)
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_NOT_FOUND":
                return None
            raise
        raw = canonical_json_bytes_v1(plan, maximum=_PLAN_MAX_BYTES)
        plan_ref = storage.put_blob(
            raw,
            logical_kind="publication_plan",
            media_type=_PLAN_MEDIA,
        )
        return self._validate_plan(plan, plan_ref), plan_ref

    def _restore_plan_source_metadata_proof(
        self,
        occurrence: ArchiveOccurrence,
        projection: SourceMetadataProjection,
    ) -> None:
        """Recover a missing proof only from a verified immutable plan graph."""

        locator = self._runtime_occurrence_locator(occurrence)
        locator_key = self._occurrence_locator_key(locator)
        expected = self._source_metadata_proof_from_projection(projection)

        def require_match(proof: Any) -> None:
            validated = self._validated_source_metadata_proof(proof)
            if (
                validated.canonical_bytes != projection.canonical_bytes
                or validated.receipt != projection.receipt
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

        prover = self._occurrence_prover
        if prover is None:
            with self._source_metadata_lock:
                existing = self._live_source_metadata_proofs.get(locator_key)
                if existing is None:
                    self._live_source_metadata_proofs[locator_key] = expected
                else:
                    require_match(existing)
            return

        load_projection = getattr(
            prover,
            "load_source_metadata_projection",
            None,
        )
        load_or_register = getattr(
            prover,
            "load_or_register_source_metadata_projection",
            None,
        )
        if not callable(load_projection) or not callable(load_or_register):
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        existing = load_projection(
            project_id=self.project_id,
            locator=copy.deepcopy(locator),
        )
        if existing is None:
            existing = load_or_register(
                project_id=self.project_id,
                locator=copy.deepcopy(locator),
                projector=lambda: projection,
            )
        require_match(existing)

    def _occurrence_from_existing_plan(
        self,
        occurrence: ArchiveOccurrence,
        plan: Mapping[str, Any],
        supplied_projection: SourceMetadataProjection | None,
    ) -> ArchiveOccurrence:
        """Freeze source-metadata identity from a pre-existing plan graph."""

        manifest = plan["intended_manifest"]["document"]
        roots = manifest.get("roots")
        receipt = manifest.get("source_metadata_receipt")
        if type(roots) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        root_ref = roots.get("source_metadata")
        plan_projection: SourceMetadataProjection | None = None
        if receipt is None and root_ref is None:
            if supplied_projection is not None:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            planned = dataclasses.replace(
                occurrence,
                source_metadata_projection=None,
                source_metadata_receipt=None,
            )
        elif receipt is not None and isinstance(root_ref, Mapping):
            incremental = PromptArchiveIncremental(
                self._require_storage(),
                self.project_id,
                occurrence.sprint_id,
            )
            document, _raw, _sha256 = incremental._source_metadata_from_root(
                root_ref
            )
            try:
                projection = validate_source_metadata_projection(
                    document,
                    receipt,
                )
            except PromptArchiveSourceMetadataError as exc:
                raise _error(exc.code) from None
            if supplied_projection is not None and (
                supplied_projection.canonical_bytes != projection.canonical_bytes
                or supplied_projection.receipt != projection.receipt
            ):
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            planned = dataclasses.replace(
                occurrence,
                source_metadata_projection=projection.document,
                source_metadata_receipt=projection.receipt,
            )
            plan_projection = projection
        else:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if not self._occurrence_matches_plan(plan, planned):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        if plan_projection is not None:
            self._restore_plan_source_metadata_proof(planned, plan_projection)
        return planned

    def source_metadata_occurrence_from_existing_plan(
        self,
        occurrence: ArchiveOccurrence,
    ) -> ArchiveOccurrence | None:
        """Return an immutable plan's source-metadata identity, if it exists.

        This read-before-raw seam lets the HTTP owner preserve exact retries
        when a caller supplies changed, oversized, or otherwise unusable raw
        metadata.  A miss authorizes no metadata and performs no projection
        write; first publication must still pass through the normal owner
        screen in :meth:`bind_source_metadata_projection`.
        """

        with self._stream_lock:
            frozen = self._freeze_occurrence(occurrence)
            storage = self._require_storage()
            PromptArchiveIncremental(
                storage,
                self.project_id,
                frozen.sprint_id,
            ).validate_runtime_occurrence(
                frozen,
                publication_mode=self.config.mode,
            )
            supplied_projection: SourceMetadataProjection | None = None
            if frozen.source_metadata_projection is not None:
                try:
                    supplied_projection = validate_source_metadata_projection(
                        frozen.source_metadata_projection,
                        frozen.source_metadata_receipt,
                    )
                except PromptArchiveSourceMetadataError as exc:
                    raise _error(exc.code) from None
            existing_plan = self._load_existing_occurrence_plan(frozen)
            if existing_plan is None:
                return None
            return self._occurrence_from_existing_plan(
                frozen,
                existing_plan[0],
                supplied_projection,
            )

    def bind_source_metadata_projection(
        self,
        occurrence: ArchiveOccurrence,
        raw_metadata: Mapping[str, Any] | None = None,
        *,
        screen_factory: Callable[[], SourceMetadataScreenSnapshot] | None = None,
    ) -> ArchiveOccurrence:
        """Freeze or recover one occurrence-scoped source projection.

        The durable prover is consulted before ``screen_factory`` or raw input.
        A retry therefore reuses the first canonical projection even after
        credential rotation.  Only the screened document and its closed receipt
        are attached to the returned occurrence; raw metadata and the ephemeral
        screen never enter a plan, journal, descriptor, or CAS object.
        """

        with self._stream_lock:
            return self._bind_source_metadata_projection_serialized(
                occurrence,
                raw_metadata,
                screen_factory=screen_factory,
            )

    def _bind_source_metadata_projection_serialized(
        self,
        occurrence: ArchiveOccurrence,
        raw_metadata: Mapping[str, Any] | None = None,
        *,
        screen_factory: Callable[[], SourceMetadataScreenSnapshot] | None = None,
    ) -> ArchiveOccurrence:
        frozen = self._freeze_occurrence(occurrence)
        storage = self._require_storage()
        PromptArchiveIncremental(
            storage,
            self.project_id,
            frozen.sprint_id,
        ).validate_runtime_occurrence(
            frozen,
            publication_mode=self.config.mode,
        )
        locator = self._runtime_occurrence_locator(frozen)
        locator_key = self._occurrence_locator_key(locator)
        supplied_projection: SourceMetadataProjection | None = None
        if frozen.source_metadata_projection is not None:
            try:
                supplied_projection = validate_source_metadata_projection(
                    frozen.source_metadata_projection,
                    frozen.source_metadata_receipt,
                )
            except PromptArchiveSourceMetadataError as exc:
                raise _error(exc.code) from None

        # A verified immutable plan is the source of truth across rolling
        # upgrades.  Legacy plans intentionally preserve metadata absence;
        # new-format plans recover their exact projection from CAS without
        # re-reading rotated credentials.  This check must precede proof
        # creation or an exact retry could fork the occurrence identity.
        existing_plan = self._load_existing_occurrence_plan(frozen)
        if existing_plan is not None:
            return self._occurrence_from_existing_plan(
                frozen,
                existing_plan[0],
                supplied_projection,
            )

        def attach(proof: Any) -> ArchiveOccurrence:
            validated = self._validated_source_metadata_proof(proof)
            if supplied_projection is not None and (
                supplied_projection.canonical_bytes != validated.canonical_bytes
                or supplied_projection.receipt != validated.receipt
            ):
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            return dataclasses.replace(
                frozen,
                source_metadata_projection=validated.document,
                source_metadata_receipt=validated.receipt,
            )

        def create_projection() -> SourceMetadataProjection:
            if raw_metadata is None or not callable(screen_factory):
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            try:
                screen = screen_factory()
                projected = project_source_metadata(raw_metadata, screen=screen)
            except PromptArchiveSourceMetadataError as exc:
                raise _error(exc.code) from None
            except PromptArchiveStorageError:
                raise
            except Exception:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            # An occurrence-carried projection is only an equality witness on
            # the first write, never authority to bypass the owner's current
            # credential/token screen.  Only a proof already recovered from
            # the durable occurrence store may skip raw re-screening.
            if supplied_projection is not None and (
                supplied_projection.canonical_bytes != projected.canonical_bytes
                or supplied_projection.receipt != projected.receipt
            ):
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            return projected

        prover = self._occurrence_prover
        if prover is None:
            with self._source_metadata_lock:
                existing = self._live_source_metadata_proofs.get(locator_key)
                if existing is not None:
                    return attach(existing)
                if supplied_projection is None and raw_metadata is None:
                    return frozen
                created = create_projection()
                proof = self._source_metadata_proof_from_projection(created)
                self._live_source_metadata_proofs[locator_key] = proof
                return attach(proof)

        load_projection = getattr(
            prover,
            "load_source_metadata_projection",
            None,
        )
        load_or_register = getattr(
            prover,
            "load_or_register_source_metadata_projection",
            None,
        )
        if not callable(load_projection) or not callable(load_or_register):
            if supplied_projection is None and raw_metadata is None:
                return frozen
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        try:
            existing = load_projection(
                project_id=self.project_id,
                locator=copy.deepcopy(locator),
            )
            if existing is not None:
                return attach(existing)
            if supplied_projection is None and raw_metadata is None:
                return frozen
            registered = load_or_register(
                project_id=self.project_id,
                locator=copy.deepcopy(locator),
                projector=create_projection,
            )
        except PromptArchiveSourceMetadataError as exc:
            raise _error(exc.code) from None
        return attach(registered)

    def _occurrence_source(
        self,
        occurrence: ArchiveOccurrence,
        *,
        committed_bytes: bytes | None = None,
    ) -> PromptArchiveOccurrenceSource:
        """Freeze the path-free identity of one exact legacy response.

        The logical key is the legacy materializer's closed 20-hex response
        id, not a filename or a mutable ``latest`` alias.  When bytes are
        supplied, their size and digest must exactly match the occurrence
        transport fact before any durable publication work is admitted.
        """

        response_key = occurrence.source_response_key
        byte_size = occurrence.source_byte_size
        digest = occurrence.source_sha256
        if (
            type(response_key) is not str
            or _LEGACY_RESPONSE_KEY_RE.fullmatch(response_key) is None
            or type(byte_size) is not int
            or not 0 <= byte_size <= self.max_hydrated_bytes
            or type(digest) is not str
            or _SHA256_RE.fullmatch(digest) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        source = PromptArchiveOccurrenceSource(response_key, byte_size, digest)
        if committed_bytes is not None and (
            byte_size != len(committed_bytes)
            or digest != hashlib.sha256(committed_bytes).hexdigest()
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return source

    def _source_matches_plan(
        self,
        plan: Mapping[str, Any],
        source: Any,
    ) -> bool:
        if not isinstance(source, PromptArchiveOccurrenceSource):
            return False
        try:
            if (
                _LEGACY_RESPONSE_KEY_RE.fullmatch(source.response_key) is None
                or type(source.byte_size) is not int
                or not 0 <= source.byte_size <= self.max_hydrated_bytes
                or type(source.sha256) is not str
                or _SHA256_RE.fullmatch(source.sha256) is None
            ):
                return False
        except TypeError:
            return False
        intended = plan.get("intended_manifest")
        if type(intended) is not dict:
            return False
        manifest = intended.get("document")
        if type(manifest) is not dict:
            return False
        integrity = manifest.get("integrity")
        comparison = manifest.get("legacy_comparison")
        return bool(
            type(integrity) is dict
            and type(comparison) is dict
            and comparison.get("matched") is True
            and integrity.get("hydrated_size") == source.byte_size
            and integrity.get("hydrated_sha256") == source.sha256
            and comparison.get("byte_size") == source.byte_size
            and comparison.get("sha256") == source.sha256
        )

    def _native_source_for_plan(
        self,
        plan: Mapping[str, Any],
    ) -> PromptArchiveNativeOccurrenceSource:
        intended = plan.get("intended_manifest")
        if type(intended) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        manifest = intended.get("document")
        if type(manifest) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        integrity = manifest.get("integrity")
        if (
            plan.get("operation_kind") != "native_publish"
            or type(integrity) is not dict
            or type(integrity.get("hydrated_size")) is not int
            or not 0 <= integrity["hydrated_size"] <= self.max_hydrated_bytes
            or type(integrity.get("hydrated_sha256")) is not str
            or _SHA256_RE.fullmatch(integrity["hydrated_sha256"]) is None
            or type(intended.get("archive_id")) is not str
            or _ARCHIVE_ID_RE.fullmatch(intended["archive_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        manifest_ref = _closed_ref(
            intended.get("manifest_ref"),
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        return PromptArchiveNativeOccurrenceSource(
            byte_size=integrity["hydrated_size"],
            sha256=integrity["hydrated_sha256"],
            archive_id=intended["archive_id"],
            manifest_ref=manifest_ref,
        )

    def _native_source_matches_plan(
        self,
        plan: Mapping[str, Any],
        source: Any,
    ) -> bool:
        if not isinstance(source, PromptArchiveNativeOccurrenceSource):
            return False
        try:
            return source == self._native_source_for_plan(plan)
        except PromptArchiveStorageError:
            return False

    def _proof_matches_plan(
        self,
        plan: Mapping[str, Any],
        proof: Any,
    ) -> bool:
        if isinstance(proof, PromptArchiveNativeOccurrenceProof):
            return bool(
                proof.status == "verified"
                and self._occurrence_matches_plan(plan, proof.occurrence)
                and self._native_source_matches_plan(plan, proof.source)
            )
        if not isinstance(proof, PromptArchiveOccurrenceProof):
            return False
        if proof.status not in {
            "verified",
            "source_unavailable",
            "source_changed",
        }:
            return False
        if not self._occurrence_matches_plan(plan, proof.occurrence):
            return False
        try:
            occurrence_source = self._occurrence_source(proof.occurrence)
        except PromptArchiveStorageError:
            return False
        return occurrence_source == proof.source and self._source_matches_plan(
            plan,
            proof.source,
        )

    def _occurrence_matches_plan(
        self,
        plan: Mapping[str, Any],
        occurrence: Any,
    ) -> bool:
        if not isinstance(occurrence, ArchiveOccurrence):
            return False
        try:
            frozen = self._freeze_occurrence(occurrence)
        except PromptArchiveStorageError:
            return False
        planned_occurrence = plan.get("occurrence")
        intended_manifest = plan.get("intended_manifest")
        if type(planned_occurrence) is not dict or type(intended_manifest) is not dict:
            return False
        manifest = intended_manifest.get("document")
        if type(manifest) is not dict:
            return False
        publication = manifest.get("publication")
        if type(publication) is not dict:
            return False
        expected_occurrence = {
            "response_id": frozen.response_id,
            "response_kind": frozen.response_kind,
            "occurrence_kind": frozen.occurrence_kind,
            "occurrence_id": frozen.occurrence_id,
            "transition_id": frozen.transition_id,
        }
        if (
            frozen.project_id != self.project_id
            or frozen.sprint_id != plan.get("scope", {}).get("sprint_id")
            or type(frozen.state_revision) is not int
            or not 0 <= frozen.state_revision <= 9_223_372_036_854_775_807
            or frozen.state_revision
            != plan.get("source_locator", {}).get("state_revision")
            or planned_occurrence != expected_occurrence
            or frozen.authorization != manifest.get("authorization")
            or frozen.created_at != manifest.get("created_at")
            or frozen.verified_at != publication.get("verified_at")
        ):
            return False

        persisted_source = manifest.get("source")
        source_matches = frozen.source == persisted_source
        if (
            not source_matches
            and type(persisted_source) is dict
            and "predecessor_archive_id" not in frozen.source
            and "predecessor_archive_id" in persisted_source
        ):
            # The runtime may select and durably freeze the verified stream
            # head after the caller supplied its occurrence.
            source_without_runtime_predecessor = copy.deepcopy(persisted_source)
            del source_without_runtime_predecessor["predecessor_archive_id"]
            source_matches = frozen.source == source_without_runtime_predecessor
        if not source_matches:
            return False

        projection = frozen.source_metadata_projection
        receipt = frozen.source_metadata_receipt
        manifest_receipt = manifest.get("source_metadata_receipt")
        if (projection is None) != (receipt is None) or (
            projection is None
        ) != (manifest_receipt is None):
            return False
        if projection is not None:
            try:
                validated_projection = validate_source_metadata_projection(
                    projection,
                    receipt,
                )
            except PromptArchiveSourceMetadataError:
                return False
            if validated_projection.receipt != manifest_receipt:
                return False

        if plan.get("operation_kind") == "native_publish":
            return bool(
                type(persisted_source) is dict
                and persisted_source.get("source_kind") == "native_runtime"
                and manifest.get("publication", {}).get("mode") == "authoritative"
                and manifest.get("publication", {}).get("legacy_authoritative") is False
            )

        try:
            source = self._occurrence_source(frozen)
        except PromptArchiveStorageError:
            return False
        return self._source_matches_plan(plan, source)

    def _register_runtime_occurrence(
        self,
        plan: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        source: PromptArchiveOccurrenceSource,
    ) -> None:
        frozen = self._freeze_occurrence(occurrence)
        if (
            not self._occurrence_matches_plan(plan, frozen)
            or self._occurrence_source(frozen) != source
            or not self._source_matches_plan(plan, source)
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        locator = _copy_document(
            plan["source_locator"],
            maximum=_CONTROL_MAX_BYTES,
        )
        key = self._occurrence_locator_key(locator)
        prover = self._occurrence_prover
        if prover is None:
            existing = self._live_occurrence_proofs.get(key)
            if existing is not None and not self._proof_matches_plan(plan, existing):
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            proof = PromptArchiveOccurrenceProof("verified", frozen, source)
            if existing is not None and existing != proof:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            self._live_occurrence_proofs[key] = proof
            return

        existing = prover.load_occurrence(
            project_id=self.project_id,
            locator=copy.deepcopy(locator),
        )
        if existing is not None and not self._proof_matches_plan(plan, existing):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        prover.register_occurrence(
            project_id=self.project_id,
            locator=copy.deepcopy(locator),
            occurrence=frozen,
            source=source,
        )
        registered = prover.load_occurrence(
            project_id=self.project_id,
            locator=copy.deepcopy(locator),
        )
        if registered is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        if not self._proof_matches_plan(plan, registered):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")

    def _register_native_runtime_occurrence(
        self,
        plan: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        source: PromptArchiveNativeOccurrenceSource,
    ) -> None:
        frozen = self._freeze_occurrence(occurrence)
        if (
            not self._occurrence_matches_plan(plan, frozen)
            or not self._native_source_matches_plan(plan, source)
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        locator = _copy_document(
            plan["source_locator"],
            maximum=_CONTROL_MAX_BYTES,
        )
        key = self._occurrence_locator_key(locator)
        prover = self._occurrence_prover
        if prover is None:
            existing = self._live_occurrence_proofs.get(key)
            if existing is not None and not self._proof_matches_plan(plan, existing):
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            proof = PromptArchiveNativeOccurrenceProof("verified", frozen, source)
            if existing is not None and existing != proof:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            self._live_occurrence_proofs[key] = proof
            return

        existing = prover.load_occurrence(
            project_id=self.project_id,
            locator=copy.deepcopy(locator),
        )
        if existing is not None and not self._proof_matches_plan(plan, existing):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        prover.register_native_occurrence(
            project_id=self.project_id,
            locator=copy.deepcopy(locator),
            occurrence=frozen,
            source=source,
        )
        registered = prover.load_occurrence(
            project_id=self.project_id,
            locator=copy.deepcopy(locator),
        )
        if registered is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        if not self._proof_matches_plan(plan, registered):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")

    def _runtime_occurrence_abort_reason(
        self,
        plan: Mapping[str, Any],
    ) -> str | None:
        locator = _copy_document(
            plan["source_locator"],
            maximum=_CONTROL_MAX_BYTES,
        )
        prover = self._occurrence_prover
        if prover is None:
            observed = self._live_occurrence_proofs.get(
                self._occurrence_locator_key(locator)
            )
        else:
            observed = prover.load_occurrence(
                project_id=self.project_id,
                locator=copy.deepcopy(locator),
            )
        if observed is None:
            return "source_unavailable"
        if not self._proof_matches_plan(plan, observed):
            return "source_changed"
        if isinstance(observed, PromptArchiveNativeOccurrenceProof):
            return None
        if observed.status == "source_unavailable":
            return "source_unavailable"
        if observed.status == "source_changed":
            return "source_changed"
        return None

    def _validate_inventory_descriptor(
        self,
        descriptor: Any,
    ) -> dict[str, Any]:
        """Validate the closed, path-free source inventory descriptor."""

        if type(descriptor) is not dict or set(descriptor) != {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "inventory_id",
            "inventory_format",
            "project_id",
            "source_root_handle_id",
            "source_root_identity_sha256",
            "inventory_sha256",
            "entry_count",
            "created_at",
        } or (
            descriptor.get("schema_version") != 1
            or descriptor.get("kind") != "prompt_archive_source_inventory"
            or descriptor.get("media_type") != _INVENTORY_MEDIA
            or descriptor.get("id_algorithm") != "PAS1-ID-v1"
            or type(descriptor.get("inventory_id")) is not str
            or _INVENTORY_ID_RE.fullmatch(descriptor["inventory_id"]) is None
            or descriptor.get("inventory_format")
            != "prompt-archive-isolated-inventory-v1"
            or descriptor.get("project_id") != self.project_id
            or type(descriptor.get("source_root_handle_id")) is not str
            or _SAFE_ID_RE.fullmatch(descriptor["source_root_handle_id"]) is None
            or type(descriptor.get("source_root_identity_sha256")) is not str
            or _SHA256_RE.fullmatch(descriptor["source_root_identity_sha256"])
            is None
            or type(descriptor.get("inventory_sha256")) is not str
            or _SHA256_RE.fullmatch(descriptor["inventory_sha256"]) is None
            or type(descriptor.get("entry_count")) is not int
            or not 1 <= descriptor["entry_count"] <= 1_000_000
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        created_at = _require_rfc3339(descriptor.get("created_at"))
        expected_id = "parinv1-" + pas1_id_v1(
            "source-inventory-id",
            (
                ("project_id", self.project_id),
                ("source_root_handle_id", descriptor["source_root_handle_id"]),
                (
                    "source_root_identity_sha256",
                    descriptor["source_root_identity_sha256"],
                ),
                ("inventory_sha256", descriptor["inventory_sha256"]),
                ("entry_count", str(descriptor["entry_count"])),
                ("created_at", created_at),
            ),
        )
        if descriptor["inventory_id"] != expected_id:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return copy.deepcopy(dict(descriptor))

    def _load_offline_batch_plan(
        self,
        descriptor: Mapping[str, Any],
        inventory_locator: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, dict[str, dict[str, Any]]]:
        """Verify the durable operator gate and every reserved source entry.

        The accepted per-entry PAS1 plan deliberately carries a compact
        inventory locator.  Its authorization and exact source-entry binding
        close through the immutable outer migration plan whose stable key is
        derived from that inventory's project and source-root handle.  A
        caller therefore cannot publish arbitrary null-sprint bytes merely by
        manufacturing an ``ArchiveOccurrence`` or an aggregate descriptor.
        """

        source_handle = descriptor.get("source_root_handle_id")
        if type(source_handle) is not str or _SAFE_ID_RE.fullmatch(source_handle) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        batch_id = "parmb1-" + pas1_id_v1(
            "offline-migration-batch-id",
            (
                ("project_id", self.project_id),
                ("source_root_handle_id", source_handle),
            ),
        )
        if _MIGRATION_BATCH_ID_RE.fullmatch(batch_id) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        batch = self._require_storage().read_immutable_json(
            ("migrations", "v1", batch_id, "plan.json")
        )
        required = {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "batch_id",
            "operation_kind",
            "contract_version",
            "tool_version",
            "tool_source_sha256",
            "project_id",
            "sprint_id",
            "source_root_handle_id",
            "operator_authorization",
            "source_inventory",
            "source_safety",
            "reservations",
            "created_at",
        }
        if type(batch) is not dict or set(batch) != required or (
            batch.get("schema_version") != 1
            or batch.get("kind") != "prompt_archive_migration_batch_plan"
            or batch.get("media_type") != _MIGRATION_BATCH_PLAN_MEDIA
            or batch.get("id_algorithm") != "PAS1-ID-v1"
            or batch.get("batch_id") != batch_id
            or batch.get("operation_kind") != "offline_migration_batch"
            or batch.get("contract_version") != _OFFLINE_MIGRATION_CONTRACT
            or batch.get("tool_version") != _OFFLINE_MIGRATION_TOOL
            or type(batch.get("tool_source_sha256")) is not str
            or _SHA256_RE.fullmatch(batch["tool_source_sha256"]) is None
            or batch.get("project_id") != self.project_id
            or batch.get("sprint_id") is not None
            or batch.get("source_root_handle_id") != source_handle
            or _require_rfc3339(batch.get("created_at"))
            != descriptor.get("created_at")
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        authorization = batch.get("operator_authorization")
        if type(authorization) is not dict or set(authorization) != {
            "gate",
            "principal_kind",
            "principal_id",
            "scope_source",
        } or (
            authorization.get("gate") != _OFFLINE_OPERATOR_GATE
            or authorization.get("principal_kind") != "offline_operator"
            or type(authorization.get("principal_id")) is not str
            or _SAFE_ID_RE.fullmatch(authorization["principal_id"]) is None
            or authorization.get("scope_source") != "offline_operator_gate"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        source_inventory = batch.get("source_inventory")
        if source_inventory != inventory_locator:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        source_safety = batch.get("source_safety")
        if type(source_safety) is not dict or set(source_safety) != {
            "source_root_identity_sha256",
            "safety_snapshot_sha256",
            "total_bytes",
        } or (
            source_safety.get("source_root_identity_sha256")
            != descriptor.get("source_root_identity_sha256")
            or type(source_safety.get("safety_snapshot_sha256")) is not str
            or _SHA256_RE.fullmatch(source_safety["safety_snapshot_sha256"])
            is None
            or type(source_safety.get("total_bytes")) is not int
            or not 0 <= source_safety["total_bytes"] <= 9_223_372_036_854_775_807
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        reservations = batch.get("reservations")
        if (
            type(reservations) is not list
            or not reservations
            or len(reservations) > descriptor["entry_count"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        by_operation: dict[str, dict[str, Any]] = {}
        seen_ordinals: set[int] = set()
        seen_responses: set[str] = set()
        reserved_bytes = 0
        for raw_reservation in reservations:
            if type(raw_reservation) is not dict or set(raw_reservation) != {
                "ordinal",
                "entry_id",
                "path_sha256",
                "source_byte_size",
                "source_sha256",
                "operation_id",
                "occurrence_id",
                "response_id",
                "decision_occurrence_id",
            }:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            reservation = copy.deepcopy(raw_reservation)
            ordinal = reservation.get("ordinal")
            byte_size = reservation.get("source_byte_size")
            path_sha256 = reservation.get("path_sha256")
            source_sha256 = reservation.get("source_sha256")
            operation_id = reservation.get("operation_id")
            if (
                type(ordinal) is not int
                or not 0 <= ordinal < descriptor["entry_count"]
                or ordinal in seen_ordinals
                or type(byte_size) is not int
                or not 0 <= byte_size <= self.max_hydrated_bytes
                or type(path_sha256) is not str
                or _SHA256_RE.fullmatch(path_sha256) is None
                or type(source_sha256) is not str
                or _SHA256_RE.fullmatch(source_sha256) is None
                or type(operation_id) is not str
                or _OPERATION_ID_RE.fullmatch(operation_id) is None
                or operation_id in by_operation
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            entry_id = "parme1-" + pas1_id_v1(
                "offline-migration-entry-id",
                (
                    ("inventory_id", descriptor["inventory_id"]),
                    ("ordinal", str(ordinal)),
                    ("path_sha256", path_sha256),
                    ("byte_size", str(byte_size)),
                    ("sha256", source_sha256),
                ),
            )
            occurrence_id = "migration-" + pas1_id_v1(
                "offline-migration-occurrence",
                (("batch_id", batch_id), ("entry_id", entry_id)),
            )
            expected_response_id = response_id_v1(
                self.project_id,
                None,
                "migration-snapshot",
                "migration",
                occurrence_id,
            )
            if (
                reservation.get("entry_id") != entry_id
                or _MIGRATION_ENTRY_ID_RE.fullmatch(entry_id) is None
                or reservation.get("occurrence_id") != occurrence_id
                or reservation.get("response_id") != expected_response_id
                or expected_response_id in seen_responses
                or reservation.get("decision_occurrence_id")
                != "decision-" + entry_id[-64:]
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            seen_ordinals.add(ordinal)
            seen_responses.add(expected_response_id)
            reserved_bytes += byte_size
            if reserved_bytes > source_safety["total_bytes"]:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            by_operation[operation_id] = reservation
        batch_raw = canonical_json_bytes_v1(batch, maximum=_PLAN_MAX_BYTES)
        return copy.deepcopy(batch), hashlib.sha256(batch_raw).hexdigest(), by_operation

    def _validate_offline_reservation(
        self,
        *,
        descriptor: Mapping[str, Any],
        inventory_locator: Mapping[str, Any],
        operation_id: str,
        occurrence: ArchiveOccurrence,
        byte_size: int,
        sha256: str,
    ) -> dict[str, Any]:
        batch, batch_plan_sha256, reservations = self._load_offline_batch_plan(
            descriptor,
            inventory_locator,
        )
        reservation = reservations.get(operation_id)
        authorization = occurrence.authorization
        if reservation is None or (
            occurrence.project_id != self.project_id
            or occurrence.sprint_id is not None
            or occurrence.response_id != reservation["response_id"]
            or occurrence.response_kind != "migration-snapshot"
            or occurrence.occurrence_kind != "migration"
            or occurrence.occurrence_id != reservation["occurrence_id"]
            or occurrence.transition_id is not None
            or occurrence.source != {"source_kind": "offline_migration"}
            or occurrence.created_at != batch["created_at"]
            or occurrence.verified_at != batch["created_at"]
            or byte_size != reservation["source_byte_size"]
            or sha256 != reservation["source_sha256"]
            or type(authorization) is not dict
            or authorization
            != {
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "offline_operator",
                "principal_id": batch["operator_authorization"]["principal_id"],
                "binding_id": None,
                "scope_source": "offline_operator_gate",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": reservation["decision_occurrence_id"],
            }
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        return {
            "batch_id": batch["batch_id"],
            "batch_plan_sha256": batch_plan_sha256,
            "reservation": copy.deepcopy(reservation),
        }

    def _validate_inventory_locator(
        self,
        locator: Any,
        *,
        require_migration_binding: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Validate one path-free isolated inventory and its stable binding."""

        base_keys = {
            "locator_kind",
            "locator_version",
            "inventory_id",
            "inventory_format",
            "inventory_sha256",
            "entry_count",
            "inventory_ref",
        }
        if type(locator) is not dict or frozenset(locator) not in {
            frozenset(base_keys),
            frozenset(base_keys | {"migration_binding"}),
        } or (require_migration_binding and "migration_binding" not in locator):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        base_locator = {name: copy.deepcopy(locator[name]) for name in base_keys}
        inventory_id = locator.get("inventory_id")
        inventory_digest = locator.get("inventory_sha256")
        entry_count = locator.get("entry_count")
        if (
            locator.get("locator_kind") != "isolated_inventory"
            or locator.get("locator_version")
            != "prompt-archive-isolated-inventory-locator-v1"
            or type(inventory_id) is not str
            or _INVENTORY_ID_RE.fullmatch(inventory_id) is None
            or locator.get("inventory_format")
            != "prompt-archive-isolated-inventory-v1"
            or type(inventory_digest) is not str
            or _SHA256_RE.fullmatch(inventory_digest) is None
            or type(entry_count) is not int
            or not 1 <= entry_count <= 1_000_000
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        inventory_ref = _closed_ref(
            locator.get("inventory_ref"),
            logical_kind="source_inventory",
            media_type=_INVENTORY_MEDIA,
        )
        raw = self._require_storage().read_blob(
            inventory_ref,
            max_output_bytes=_CONTROL_MAX_BYTES,
        )
        descriptor = self._validate_inventory_descriptor(
            parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES)
        )
        if (
            descriptor["inventory_id"] != inventory_id
            or descriptor["inventory_format"] != locator["inventory_format"]
            or descriptor["inventory_sha256"] != inventory_digest
            or descriptor["entry_count"] != entry_count
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        stable = self._require_storage().read_immutable_json(
            ("inventories", "v1", f"{inventory_id}.json")
        )
        if stable != descriptor:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        # The inventory is not an authority by itself.  Close it through the
        # immutable, operator-gated batch plan before accepting any locator.
        batch, batch_plan_sha256, reservations = self._load_offline_batch_plan(
            descriptor,
            base_locator,
        )
        binding = locator.get("migration_binding")
        if binding is not None:
            if type(binding) is not dict or set(binding) != {
                "batch_id",
                "batch_plan_sha256",
                "reservation",
            }:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            reservation = binding.get("reservation")
            if (
                binding.get("batch_id") != batch["batch_id"]
                or binding.get("batch_plan_sha256") != batch_plan_sha256
                or type(reservation) is not dict
                or reservations.get(reservation.get("operation_id")) != reservation
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return copy.deepcopy(locator), descriptor

    def publish_offline_inventory_descriptor(
        self,
        descriptor: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Create-or-verify one accepted path-free inventory binding.

        This method never discovers an input root.  The caller owns the
        explicit isolated-root inventory and must durably reserve its random
        operation identity before invoking any per-entry publication.
        """

        validated = self._validate_inventory_descriptor(descriptor)
        raw = canonical_json_bytes_v1(validated, maximum=_CONTROL_MAX_BYTES)
        storage = self._require_storage()
        described_ref = storage.describe_blob(
            raw,
            logical_kind="source_inventory",
            media_type=_INVENTORY_MEDIA,
        )
        prospective_locator = {
            "locator_kind": "isolated_inventory",
            "locator_version": "prompt-archive-isolated-inventory-locator-v1",
            "inventory_id": validated["inventory_id"],
            "inventory_format": validated["inventory_format"],
            "inventory_sha256": validated["inventory_sha256"],
            "entry_count": validated["entry_count"],
            "inventory_ref": copy.deepcopy(described_ref),
        }
        # Authorization precedes the first inventory CAS mutation.
        self._load_offline_batch_plan(validated, prospective_locator)
        inventory_ref = storage.put_blob(
            raw,
            logical_kind="source_inventory",
            media_type=_INVENTORY_MEDIA,
        )
        storage.publish_immutable_bytes(
            ("inventories", "v1", f"{validated['inventory_id']}.json"),
            raw,
            expected_sha256=hashlib.sha256(raw).hexdigest(),
        )
        locator = copy.deepcopy(prospective_locator)
        if locator["inventory_ref"] != inventory_ref:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        verified, _ = self._validate_inventory_locator(locator)
        return verified

    def _offline_inventory_abort_reason(
        self,
        plan: Mapping[str, Any],
        verifier: PromptArchiveOfflineInventoryVerifier | None,
    ) -> str | None:
        if verifier is None:
            return "source_unavailable"
        locator, _descriptor = self._validate_inventory_locator(
            plan.get("source_locator"),
            require_migration_binding=True,
        )
        locator = {
            name: copy.deepcopy(value)
            for name, value in locator.items()
            if name != "migration_binding"
        }
        try:
            status = verifier(copy.deepcopy(locator))
        except PromptArchiveStorageError:
            return "source_unavailable"
        if status == "verified":
            return None
        if status in {"source_unavailable", "source_changed"}:
            return status
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    @staticmethod
    def _pointer_id(pointer: Mapping[str, Any]) -> str:
        return "parptr1-" + pas1_id_v1(
            "pointer-id",
            (
                ("project_id", pointer["project_id"]),
                ("sprint_id", pointer["sprint_id"]),
                ("response_id", pointer["response_id"]),
                ("generation", str(pointer["generation"])),
                ("archive_id", pointer["archive_id"]),
                ("manifest_sha256", pointer["manifest_ref"]["digest"]),
                ("previous_pointer_id", pointer["previous_pointer_id"]),
                ("published_at", pointer["published_at"]),
            ),
        )

    @staticmethod
    def _locator_id(locator: Mapping[str, Any]) -> str:
        return "pararc1-" + pas1_id_v1(
            "archive-locator-id",
            (
                ("project_id", locator["project_id"]),
                ("sprint_id", locator["sprint_id"]),
                ("response_id", locator["response_id"]),
                ("archive_id", locator["archive_id"]),
                ("pointer_id", locator["pointer_id"]),
                ("manifest_sha256", locator["manifest_ref"]["digest"]),
                ("published_at", locator["published_at"]),
            ),
        )

    def publish_shadow(
        self,
        committed_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
        *,
        raw_source_metadata: Mapping[str, Any] | None = None,
        source_metadata_screen_factory: (
            Callable[[], SourceMetadataScreenSnapshot] | None
        ) = None,
    ) -> PromptArchivePublication | None:
        """Publish one shadow occurrence while serializing its sprint stream."""

        if self.config.mode == "legacy":
            return None
        if self.config.mode != "shadow":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        if not isinstance(occurrence, ArchiveOccurrence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if occurrence.project_id != self.project_id or occurrence.sprint_id is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        storage = self._require_storage()
        with self._stream_lock:
            with storage.account_cas_writes() as cas_writes:
                occurrence = self.bind_source_metadata_projection(
                    occurrence,
                    raw_source_metadata,
                    screen_factory=source_metadata_screen_factory,
                )
                publication = self._publish_runtime_serialized(
                    committed_bytes,
                    occurrence,
                    publication_mode="shadow",
                )
            if publication is not None:
                outcomes = cas_writes.snapshot()
                self.metrics.record_publication(
                    logical_bytes=publication.integrity["hydrated_size"],
                    new_physical_bytes=outcomes.new_stored_bytes,
                    dedup_hits=outcomes.dedup_hits,
                )
            return publication

    def publish_authoritative(
        self,
        native_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
        compatibility_publisher: PromptArchiveCompatibilityPublisher | None = None,
        *,
        raw_source_metadata: Mapping[str, Any] | None = None,
        source_metadata_screen_factory: (
            Callable[[], SourceMetadataScreenSnapshot] | None
        ) = None,
    ) -> PromptArchivePublication:
        """Publish one operator-gated native PAS1 occurrence.

        ``native_bytes`` is the bounded compact response assembled from the
        committed transaction facts.  It is not a legacy cumulative snapshot.
        Unlike shadow mode, failures are propagated and there is deliberately
        no legacy fallback.
        """

        if self.config.mode != "authoritative":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        if self.config.operator_gate != "prompt-archive-authoritative-v1":
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if not isinstance(occurrence, ArchiveOccurrence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if occurrence.project_id != self.project_id or occurrence.sprint_id is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        storage = self._require_storage()
        with self._stream_lock:
            with storage.account_cas_writes() as cas_writes:
                # Projection selection and plan publication are one serialized
                # and accounted decision.  A retry first adopts an immutable
                # plan's metadata identity; a first publication freezes
                # owner-screened raw input.
                occurrence = self.bind_source_metadata_projection(
                    occurrence,
                    raw_source_metadata,
                    screen_factory=source_metadata_screen_factory,
                )
                publication = self._publish_runtime_serialized(
                    native_bytes,
                    occurrence,
                    publication_mode="authoritative",
                    compatibility_publisher=compatibility_publisher,
                )
            outcomes = cas_writes.snapshot()
            self.metrics.record_publication(
                logical_bytes=publication.integrity["hydrated_size"],
                new_physical_bytes=outcomes.new_stored_bytes,
                dedup_hits=outcomes.dedup_hits,
            )
            return publication

    def descriptor_stub(
        self,
        publication: PromptArchivePublication,
    ) -> dict[str, Any]:
        """Return the canonical route-neutral stub for one publication."""

        if (
            not isinstance(publication, PromptArchivePublication)
            or publication.project_id != self.project_id
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        scope = _copy_document(publication.scope, maximum=_CONTROL_MAX_BYTES)
        manifest_ref = _closed_ref(
            publication.manifest_ref,
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        published_at = _require_rfc3339(
            publication.locator.get("published_at")
        )
        stub: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_descriptor_stub",
            "media_type": _STUB_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "stub_id": "",
            "archive_id": publication.archive_id,
            "archive_format": publication.archive_format,
            "scope": scope,
            "integrity": _copy_document(
                publication.integrity,
                maximum=_CONTROL_MAX_BYTES,
            ),
            "pointer_id": publication.pointer_id,
            "manifest_ref": copy.deepcopy(manifest_ref),
            "created_at": _require_rfc3339(publication.created_at),
            "published_at": published_at,
        }
        stub["stub_id"] = "parstub1-" + pas1_id_v1(
            "descriptor-stub-id",
            (
                ("project_id", self.project_id),
                ("sprint_id", publication.sprint_id),
                ("response_id", scope.get("response_id")),
                ("archive_id", publication.archive_id),
                ("pointer_id", publication.pointer_id),
                ("manifest_sha256", manifest_ref["digest"]),
                ("created_at", publication.created_at),
                ("published_at", published_at),
            ),
        )
        return self._validate_stub(stub)

    def descriptor_stub_artifact(
        self,
        publication: PromptArchivePublication,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Persist and return the route-neutral stub plus its typed CAS ref."""

        stub = self.descriptor_stub(publication)
        raw = canonical_json_bytes_v1(stub, maximum=_CONTROL_MAX_BYTES)
        reference = self._require_storage().put_blob(
            raw,
            logical_kind="descriptor_stub",
            media_type=_STUB_MEDIA,
        )
        return stub, reference

    @staticmethod
    def publication_operation_id(occurrence: ArchiveOccurrence) -> str:
        """Expose the deterministic retry key shared with latest publication."""

        if not isinstance(occurrence, ArchiveOccurrence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        return PromptArchiveRuntime._operation_id(occurrence)

    def verify_authoritative_archive_owner(
        self,
        archive_owner: Mapping[str, Any],
        latest_intent: Mapping[str, Any],
    ) -> bool:
        """Read-only proof that an external latest intent owns this archive.

        This callback is invoked while the external-target latest lock is held.
        It consequently takes only the exact archive operation lock and never
        the stream or pending-registry locks.  It also disables retained
        successor adoption so verification has no recovery side effect.
        """

        owner_keys = {
            "protocol",
            "project_id",
            "sprint_id",
            "response_id",
            "response_kind",
            "operation_id",
            "recovery_key",
            "outer_plan_identity_sha256",
            "archive_id",
            "pointer_id",
            "locator_id",
            "target_authority_id",
            "transition_stub_name",
            "descriptor_stub_id",
        }
        if type(archive_owner) is not dict or set(archive_owner) != owner_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        owner = _copy_document(archive_owner, maximum=_CONTROL_MAX_BYTES)
        if (
            owner.get("protocol") != "prompt-archive-latest-archive-owner-v1"
            or owner.get("project_id") != self.project_id
            or type(owner.get("sprint_id")) is not str
            or _SAFE_ID_RE.fullmatch(owner["sprint_id"]) is None
            or type(owner.get("response_id")) is not str
            or _RESPONSE_ID_RE.fullmatch(owner["response_id"]) is None
            or owner.get("response_kind")
            not in {"identity-response", "transition-response", "state-response"}
            or type(owner.get("operation_id")) is not str
            or _OPERATION_ID_RE.fullmatch(owner["operation_id"]) is None
            or type(owner.get("recovery_key")) is not str
            or _RECOVERY_KEY_RE.fullmatch(owner["recovery_key"]) is None
            or type(owner.get("archive_id")) is not str
            or _ARCHIVE_ID_RE.fullmatch(owner["archive_id"]) is None
            or type(owner.get("pointer_id")) is not str
            or _POINTER_ID_RE.fullmatch(owner["pointer_id"]) is None
            or type(owner.get("locator_id")) is not str
            or _LOCATOR_ID_RE.fullmatch(owner["locator_id"]) is None
            or type(owner.get("target_authority_id")) is not str
            or _TARGET_AUTHORITY_ID_RE.fullmatch(owner["target_authority_id"])
            is None
            or type(owner.get("descriptor_stub_id")) is not str
            or _STUB_ID_RE.fullmatch(owner["descriptor_stub_id"]) is None
            or owner.get("transition_stub_name")
            != f"{owner['response_kind']}-{owner['response_id']}.json"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        projection = {
            key: owner[key]
            for key in (
                "project_id",
                "sprint_id",
                "response_id",
                "response_kind",
                "operation_id",
                "recovery_key",
                "archive_id",
                "pointer_id",
                "locator_id",
                "target_authority_id",
                "transition_stub_name",
                "descriptor_stub_id",
            )
        }
        projection_digest = hashlib.sha256(
            canonical_json_bytes_v1(projection, maximum=_CONTROL_MAX_BYTES)
        ).hexdigest()
        if owner.get("outer_plan_identity_sha256") != projection_digest:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        intent = _copy_document(latest_intent, maximum=_CONTROL_MAX_BYTES)

        storage = self._require_storage()
        plan_path = (
            "operations",
            "v1",
            owner["response_id"],
            owner["operation_id"],
            "plan.json",
        )
        plan_value = storage.read_immutable_json(plan_path)
        plan_raw = canonical_json_bytes_v1(plan_value, maximum=_PLAN_MAX_BYTES)
        plan_ref = storage.describe_blob(
            plan_raw,
            logical_kind="publication_plan",
            media_type=_PLAN_MEDIA,
        )
        if storage.read_blob(plan_ref, max_output_bytes=_PLAN_MAX_BYTES) != plan_raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        plan = self._validate_plan(plan_value, plan_ref)
        compatibility = plan["compatibility_publication"]
        if (
            plan["operation_kind"] != "native_publish"
            or compatibility.get("mode") != "authoritative_descriptor_stubs"
            or compatibility.get("archive_owner") != owner
            or compatibility.get("target_authority_id")
            != owner["target_authority_id"]
            or compatibility.get("latest_intent") != intent
            or compatibility.get("descriptor", {}).get("document", {}).get(
                "stub_id"
            )
            != owner["descriptor_stub_id"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

        with storage.control_lock(self._operation_lock_path(plan)):
            current = self._load_journal_chain_locked(
                plan,
                plan_ref,
                adopt_retained_successor=False,
            )
        if current is None or current[0].get("journal_generation", -1) < 4:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        sprint_key = sprint_storage_key_v1(self.project_id, owner["sprint_id"])
        pointer = storage.read_immutable_json(
            (
                "sprints",
                sprint_key,
                "pointers",
                "v1",
                f"{owner['response_id']}.json",
            )
        )
        if pointer != plan["intended_pointer"]["document"]:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        locator = storage.read_immutable_json(
            (
                "sprints",
                sprint_key,
                "archives",
                "v1",
                f"{owner['archive_id']}.json",
            )
        )
        if locator != plan["intended_archive_locator"]["document"]:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return True

    def _publish_shadow_serialized(
        self,
        committed_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
    ) -> PromptArchivePublication | None:
        """Publish a discoverable shadow archive of exact committed bytes.

        ``legacy`` performs no write.  The caller deliberately owns failure
        isolation: invoke this only after legacy outputs commit, catch the safe
        stable exception, and leave those authoritative outputs unchanged.
        """

        if self.config.mode == "legacy":
            return None
        return self._publish_runtime_serialized(
            committed_bytes,
            occurrence,
            publication_mode="shadow",
        )

    def _publish_runtime_serialized(
        self,
        committed_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
        *,
        publication_mode: str,
        compatibility_publisher: PromptArchiveCompatibilityPublisher | None = None,
    ) -> PromptArchivePublication:
        if publication_mode not in {"shadow", "authoritative"}:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if self.config.mode != publication_mode:
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        if not isinstance(occurrence, ArchiveOccurrence):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if occurrence.project_id != self.project_id or occurrence.sprint_id is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        storage = self._require_storage()
        frozen = _freeze_bytes(committed_bytes, maximum=self.max_hydrated_bytes)
        occurrence_source = (
            self._occurrence_source(occurrence, committed_bytes=frozen)
            if publication_mode == "shadow"
            else None
        )
        resumed = self._resume_existing_occurrence(
            frozen,
            occurrence,
            occurrence_source,
            compatibility_publisher=compatibility_publisher,
        )
        if resumed is not None:
            return resumed
        incremental = PromptArchiveIncremental(
            storage,
            self.project_id,
            occurrence.sprint_id,
        )
        effective_occurrence, previous_roots, previous_manifest_ref = (
            self._resolve_predecessor(occurrence)
        )
        built = incremental.build_incremental_archive(
            frozen,
            effective_occurrence,
            previous_roots=previous_roots,
            previous_manifest_ref=previous_manifest_ref,
            publication_mode=publication_mode,
            operator_gate=(
                self.config.operator_gate
                if publication_mode == "authoritative"
                else None
            ),
            legacy_committed_bytes=(
                frozen if publication_mode == "shadow" else None
            ),
        )
        # Verify the complete closed graph before creating any discoverable
        # pointer.  Unreachable CAS created during planning is harmless.
        if publication_mode == "shadow":
            hydrated = incremental.hydrate_archive_bytes(
                built.manifest_ref,
                max_output_bytes=self.max_hydrated_bytes,
            )
            if hydrated != frozen:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

        plan = self._build_shadow_plan(
            incremental,
            built,
            effective_occurrence,
            publication_mode=publication_mode,
            compatibility_publisher=compatibility_publisher,
        )
        native_source: PromptArchiveNativeOccurrenceSource | None = None
        if publication_mode == "authoritative":
            native_source = self._native_source_for_plan(plan)
            if (
                native_source.byte_size != len(frozen)
                or native_source.sha256 != hashlib.sha256(frozen).hexdigest()
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            # The compact native occurrence is durably receipted before its
            # publication plan becomes a recovery root.  A crash can therefore
            # leave harmless unreachable CAS/receipt data, but never a planned
            # operation whose native source proof is missing.
            self._register_native_runtime_occurrence(
                plan,
                effective_occurrence,
                native_source,
            )
        plan_raw = canonical_json_bytes_v1(plan, maximum=_PLAN_MAX_BYTES)
        plan_ref = storage.put_blob(
            plan_raw,
            logical_kind="publication_plan",
            media_type=_PLAN_MEDIA,
        )
        storage.publish_immutable_bytes(
            self._plan_path(plan),
            plan_raw,
            expected_sha256=hashlib.sha256(plan_raw).hexdigest(),
        )
        storage._fault("after_publication_plan")
        if occurrence_source is not None:
            self._register_runtime_occurrence(
                plan,
                effective_occurrence,
                occurrence_source,
            )
        with storage.control_lock(self._operation_lock_path(plan)):
            existing_journal = self._load_journal_chain_locked(plan, plan_ref)
        terminal_state: str
        if existing_journal is not None and existing_journal[0]["state"] in {
            "complete",
            "aborted",
        }:
            # A crash may have persisted the terminal journal before removing
            # its pending entry.  A normal same-occurrence retry completes that
            # final mutation instead of merely observing the locator.
            self._apply_pending_mutation(*existing_journal)
            terminal_state = existing_journal[0]["state"]
        else:
            self._register_planned(plan, plan_ref)
            storage._fault("after_pending_registration")
            terminal_state = self._drive_operation(
                plan,
                plan_ref,
                compatibility_publisher=compatibility_publisher,
            )
        if terminal_state == "aborted":
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")

        verified = self.lookup_archive(
            sprint_id=effective_occurrence.sprint_id,
            archive_id=built.archive_id,
        )
        self._remember_predecessor(
            effective_occurrence.sprint_id,
            built.archive_id,
            effective_occurrence.source.get("predecessor_archive_id"),
        )
        return PromptArchivePublication(
            project_id=self.project_id,
            sprint_id=occurrence.sprint_id,
            archive_id=verified.archive_id,
            archive_format=verified.archive_format,
            scope=_copy_document(verified.scope, maximum=_CONTROL_MAX_BYTES),
            integrity=_copy_document(verified.integrity, maximum=_CONTROL_MAX_BYTES),
            created_at=verified.created_at,
            pointer_id=verified.pointer["pointer_id"],
            manifest_ref=copy.deepcopy(verified.pointer["manifest_ref"]),
            locator=copy.deepcopy(verified.locator),
        )

    def _resume_existing_occurrence(
        self,
        committed_bytes: bytes,
        occurrence: ArchiveOccurrence,
        occurrence_source: PromptArchiveOccurrenceSource | None,
        *,
        compatibility_publisher: PromptArchiveCompatibilityPublisher | None = None,
    ) -> PromptArchivePublication | None:
        storage = self._require_storage()
        operation_id = self._operation_id(occurrence)
        path = (
            "operations",
            "v1",
            occurrence.response_id,
            operation_id,
            "plan.json",
        )
        try:
            plan = storage.read_immutable_json(path)
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_NOT_FOUND":
                return None
            raise
        raw = canonical_json_bytes_v1(plan, maximum=_PLAN_MAX_BYTES)
        plan_ref = storage.put_blob(
            raw,
            logical_kind="publication_plan",
            media_type=_PLAN_MEDIA,
        )
        plan = self._validate_plan(plan, plan_ref)
        manifest = plan["intended_manifest"]["document"]
        integrity = manifest.get("integrity")
        if (
            plan["operation_id"] != operation_id
            or plan["scope"]
            != {"project_id": self.project_id, "sprint_id": occurrence.sprint_id}
            or not self._occurrence_matches_plan(plan, occurrence)
            or type(integrity) is not dict
            or integrity.get("hydrated_size") != len(committed_bytes)
            or integrity.get("hydrated_sha256")
            != hashlib.sha256(committed_bytes).hexdigest()
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        self._verify_plan_graph(plan)
        if plan["operation_kind"] == "shadow_publish":
            if occurrence_source is None:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            self._register_runtime_occurrence(
                plan,
                occurrence,
                occurrence_source,
            )
        elif plan["operation_kind"] == "native_publish":
            if occurrence_source is not None:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            persisted_source = manifest.get("source")
            if type(persisted_source) is not dict:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            effective_occurrence = dataclasses.replace(
                occurrence,
                source=_copy_document(
                    persisted_source,
                    maximum=16 * 1024,
                ),
            )
            self._register_native_runtime_occurrence(
                plan,
                effective_occurrence,
                self._native_source_for_plan(plan),
            )
        elif occurrence_source is not None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        with storage.control_lock(self._operation_lock_path(plan)):
            existing_journal = self._load_journal_chain_locked(plan, plan_ref)
        terminal_state: str
        if existing_journal is not None and existing_journal[0]["state"] in {
            "complete",
            "aborted",
        }:
            self._apply_pending_mutation(*existing_journal)
            terminal_state = existing_journal[0]["state"]
        else:
            self._register_planned(plan, plan_ref)
            terminal_state = self._drive_operation(
                plan,
                plan_ref,
                compatibility_publisher=compatibility_publisher,
            )
        if terminal_state == "aborted":
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        archive_id = plan["intended_manifest"]["archive_id"]
        verified = self.lookup_archive(
            sprint_id=occurrence.sprint_id,
            archive_id=archive_id,
        )
        self._remember_predecessor(
            occurrence.sprint_id,
            archive_id,
            plan["intended_manifest"]["document"].get("source", {}).get(
                "predecessor_archive_id"
            ),
        )
        return PromptArchivePublication(
            project_id=self.project_id,
            sprint_id=occurrence.sprint_id,
            archive_id=verified.archive_id,
            archive_format=verified.archive_format,
            scope=_copy_document(verified.scope, maximum=_CONTROL_MAX_BYTES),
            integrity=_copy_document(verified.integrity, maximum=_CONTROL_MAX_BYTES),
            created_at=verified.created_at,
            pointer_id=verified.pointer["pointer_id"],
            manifest_ref=copy.deepcopy(verified.pointer["manifest_ref"]),
            locator=copy.deepcopy(verified.locator),
        )

    def _load_offline_plan_identity(
        self,
        *,
        response_id: str,
        operation_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if (
            type(response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(response_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        storage = self._require_storage()
        plan = storage.read_immutable_json(
            ("operations", "v1", response_id, operation_id, "plan.json")
        )
        raw = canonical_json_bytes_v1(plan, maximum=_PLAN_MAX_BYTES)
        plan_ref = storage.put_blob(
            raw,
            logical_kind="publication_plan",
            media_type=_PLAN_MEDIA,
        )
        return self._validate_offline_plan_identity(
            plan,
            plan_ref,
            response_id=response_id,
            operation_id=operation_id,
        )

    def _read_offline_plan_identity(
        self,
        *,
        response_id: str,
        operation_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Read one retained offline plan without healing its CAS identity."""

        if (
            type(response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(response_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        storage = self._require_storage()
        plan = storage.read_immutable_json(
            ("operations", "v1", response_id, operation_id, "plan.json")
        )
        raw = canonical_json_bytes_v1(plan, maximum=_PLAN_MAX_BYTES)
        plan_ref = storage.describe_blob(
            raw,
            logical_kind="publication_plan",
            media_type=_PLAN_MEDIA,
        )
        if storage.read_blob(plan_ref, max_output_bytes=_PLAN_MAX_BYTES) != raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return self._validate_offline_plan_identity(
            plan,
            plan_ref,
            response_id=response_id,
            operation_id=operation_id,
        )

    def _validate_offline_plan_identity(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        *,
        response_id: str,
        operation_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        validated = self._validate_plan(plan, plan_ref)
        if (
            validated.get("operation_kind") != "offline_migration"
            or validated.get("operation_id") != operation_id
            or validated.get("occurrence", {}).get("response_id") != response_id
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        return validated, plan_ref

    @staticmethod
    def _offline_occurrence_matches_plan(
        plan: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        frozen: bytes,
        inventory_locator: Mapping[str, Any],
    ) -> bool:
        manifest = plan.get("intended_manifest", {}).get("document")
        if type(manifest) is not dict:
            return False
        integrity = manifest.get("integrity")
        publication = manifest.get("publication")
        roots = manifest.get("roots")
        expected_occurrence = {
            "response_id": occurrence.response_id,
            "response_kind": occurrence.response_kind,
            "occurrence_kind": occurrence.occurrence_kind,
            "occurrence_id": occurrence.occurrence_id,
            "transition_id": occurrence.transition_id,
        }
        return bool(
            plan.get("operation_kind") == "offline_migration"
            and occurrence.source_metadata_projection is None
            and occurrence.source_metadata_receipt is None
            and plan.get("scope")
            == {"project_id": occurrence.project_id, "sprint_id": None}
            and plan.get("occurrence") == expected_occurrence
            and plan.get("source_locator") == inventory_locator
            and manifest.get("source") == occurrence.source
            and manifest.get("authorization") == occurrence.authorization
            and manifest.get("created_at") == occurrence.created_at
            and type(roots) is dict
            and set(roots) == {"raw_bytes"}
            and "source_metadata_receipt" not in manifest
            and type(publication) is dict
            and publication.get("verified_at") == occurrence.verified_at
            and type(integrity) is dict
            and integrity.get("hydrated_size") == len(frozen)
            and integrity.get("hydrated_sha256")
            == hashlib.sha256(frozen).hexdigest()
        )

    def _finish_offline_plan(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        *,
        inventory_verifier: PromptArchiveOfflineInventoryVerifier | None,
    ) -> PromptArchivePublication:
        storage = self._require_storage()
        lock = (
            storage.existing_control_lock
            if inventory_verifier is None
            else storage.control_lock
        )
        with lock(self._operation_lock_path(plan)):
            existing_journal = self._load_journal_chain_locked(plan, plan_ref)
        if existing_journal is not None and existing_journal[0]["state"] in {
            "complete",
            "aborted",
        }:
            self._apply_pending_mutation(*existing_journal)
            terminal_state = existing_journal[0]["state"]
        else:
            self._register_planned(plan, plan_ref)
            storage._fault("after_pending_registration")
            terminal_state = self._drive_operation(
                plan,
                plan_ref,
                offline_inventory_verifier=inventory_verifier,
            )
        if terminal_state == "aborted":
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        verified = self.lookup_offline_archive_metadata(
            archive_id=plan["intended_manifest"]["archive_id"]
        )
        return PromptArchivePublication(
            project_id=self.project_id,
            sprint_id=None,
            archive_id=verified.archive_id,
            archive_format=verified.archive_format,
            scope=_copy_document(verified.scope, maximum=_CONTROL_MAX_BYTES),
            integrity=_copy_document(verified.integrity, maximum=_CONTROL_MAX_BYTES),
            created_at=verified.created_at,
            pointer_id=verified.pointer["pointer_id"],
            manifest_ref=copy.deepcopy(verified.pointer["manifest_ref"]),
            locator=copy.deepcopy(verified.locator),
        )

    def publish_offline_migration(
        self,
        raw_bytes: bytes | bytearray | memoryview,
        occurrence: ArchiveOccurrence,
        *,
        operation_id: str,
        inventory_descriptor: Mapping[str, Any],
        inventory_ref: Mapping[str, Any],
        inventory_verifier: PromptArchiveOfflineInventoryVerifier,
    ) -> PromptArchivePublication:
        """Publish or resume one explicitly authorized offline migration.

        ``operation_id`` is supplied by the migration controller because that
        controller must reserve 128 cryptographically random bits durably
        before this method may create per-entry graph objects.  This runtime
        never derives an offline operation identity from source metadata.
        """

        if self.config.mode != "shadow":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        if (
            not isinstance(occurrence, ArchiveOccurrence)
            or occurrence.project_id != self.project_id
            or occurrence.sprint_id is not None
            or occurrence.response_kind != "migration-snapshot"
            or occurrence.occurrence_kind != "migration"
            or occurrence.transition_id is not None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
            or not callable(inventory_verifier)
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if (
            occurrence.source_metadata_projection is not None
            or occurrence.source_metadata_receipt is not None
        ):
            # Offline migration is an opaque legacy-byte path.  Treat native
            # source-metadata authorities as an invalid reference, including
            # retained-plan retries, rather than silently accepting or
            # ignoring them as generic request decoration.
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        storage = self._require_storage()
        frozen = _freeze_bytes(raw_bytes, maximum=self.max_hydrated_bytes)
        descriptor = self._validate_inventory_descriptor(inventory_descriptor)
        expected_ref = _closed_ref(
            inventory_ref,
            logical_kind="source_inventory",
            media_type=_INVENTORY_MEDIA,
        )
        descriptor_raw = canonical_json_bytes_v1(
            descriptor,
            maximum=_CONTROL_MAX_BYTES,
        )
        if storage.read_blob(
            expected_ref,
            max_output_bytes=_CONTROL_MAX_BYTES,
        ) != descriptor_raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        inventory_locator = {
            "locator_kind": "isolated_inventory",
            "locator_version": "prompt-archive-isolated-inventory-locator-v1",
            "inventory_id": descriptor["inventory_id"],
            "inventory_format": descriptor["inventory_format"],
            "inventory_sha256": descriptor["inventory_sha256"],
            "entry_count": descriptor["entry_count"],
            "inventory_ref": copy.deepcopy(expected_ref),
        }
        inventory_locator, _ = self._validate_inventory_locator(inventory_locator)
        migration_binding = self._validate_offline_reservation(
            descriptor=descriptor,
            inventory_locator=inventory_locator,
            operation_id=operation_id,
            occurrence=occurrence,
            byte_size=len(frozen),
            sha256=hashlib.sha256(frozen).hexdigest(),
        )
        bound_inventory_locator = copy.deepcopy(inventory_locator)
        bound_inventory_locator["migration_binding"] = migration_binding

        plan_path = ("operations", "v1", occurrence.response_id, operation_id, "plan.json")
        with self._stream_lock:
            with storage.account_cas_writes() as cas_writes:
                try:
                    existing = storage.read_immutable_json(plan_path)
                except PromptArchiveStorageError as exc:
                    if exc.code != "PROMPT_ARCHIVE_NOT_FOUND":
                        raise
                    existing = None
                if existing is not None:
                    plan_raw = canonical_json_bytes_v1(
                        existing,
                        maximum=_PLAN_MAX_BYTES,
                    )
                    plan_ref = storage.put_blob(
                        plan_raw,
                        logical_kind="publication_plan",
                        media_type=_PLAN_MEDIA,
                    )
                    plan = self._validate_plan(existing, plan_ref)
                    if (
                        plan.get("operation_id") != operation_id
                        or not self._offline_occurrence_matches_plan(
                            plan,
                            occurrence,
                            frozen,
                            bound_inventory_locator,
                        )
                    ):
                        raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                else:
                    try:
                        inventory_status = inventory_verifier(
                            copy.deepcopy(inventory_locator)
                        )
                    except PromptArchiveStorageError:
                        inventory_status = "source_unavailable"
                    if inventory_status != "verified":
                        if inventory_status not in {
                            "source_unavailable",
                            "source_changed",
                        }:
                            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
                        raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
                    incremental = PromptArchiveIncremental(
                        storage,
                        self.project_id,
                        None,
                    )
                    built = incremental.build_legacy_raw_archive(frozen, occurrence)
                    hydrated = incremental.hydrate_archive_bytes(
                        built.manifest_ref,
                        max_output_bytes=self.max_hydrated_bytes,
                    )
                    if hydrated != frozen:
                        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                    plan = self._build_offline_plan(
                        incremental,
                        built,
                        occurrence,
                        operation_id=operation_id,
                        inventory_locator=bound_inventory_locator,
                    )
                    plan_raw = canonical_json_bytes_v1(
                        plan,
                        maximum=_PLAN_MAX_BYTES,
                    )
                    plan_ref = storage.put_blob(
                        plan_raw,
                        logical_kind="publication_plan",
                        media_type=_PLAN_MEDIA,
                    )
                    storage.publish_immutable_bytes(
                        self._plan_path(plan),
                        plan_raw,
                        expected_sha256=hashlib.sha256(plan_raw).hexdigest(),
                    )
                    storage._fault("after_publication_plan")
                    plan = self._validate_plan(plan, plan_ref)
                publication = self._finish_offline_plan(
                    plan,
                    plan_ref,
                    inventory_verifier=inventory_verifier,
                )
            outcomes = cas_writes.snapshot()
            self.metrics.record_publication(
                logical_bytes=publication.integrity["hydrated_size"],
                new_physical_bytes=outcomes.new_stored_bytes,
                dedup_hits=outcomes.dedup_hits,
            )
            return publication

    def resume_offline_migration(
        self,
        *,
        response_id: str,
        operation_id: str,
        inventory_verifier: PromptArchiveOfflineInventoryVerifier | None,
    ) -> PromptArchivePublication:
        """Resume one exact offline plan without directory discovery.

        An absent verifier is accepted only after the retained journal crossed
        ``blobs_verified``; the intent state deterministically aborts instead.
        """

        if self.config.mode != "shadow":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        if inventory_verifier is not None and not callable(inventory_verifier):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        storage = self._require_storage()
        with self._stream_lock:
            with storage.account_cas_writes() as cas_writes:
                plan_reader = (
                    self._read_offline_plan_identity
                    if inventory_verifier is None
                    else self._load_offline_plan_identity
                )
                plan, plan_ref = plan_reader(
                    response_id=response_id,
                    operation_id=operation_id,
                )
                publication = self._finish_offline_plan(
                    plan,
                    plan_ref,
                    inventory_verifier=inventory_verifier,
                )
            outcomes = cas_writes.snapshot()
            self.metrics.record_publication(
                logical_bytes=publication.integrity["hydrated_size"],
                new_physical_bytes=outcomes.new_stored_bytes,
                dedup_hits=outcomes.dedup_hits,
            )
            return publication

    def _resolve_predecessor(
        self,
        occurrence: ArchiveOccurrence,
    ) -> tuple[
        ArchiveOccurrence,
        Mapping[str, Mapping[str, Any]] | None,
        Mapping[str, Any] | None,
    ]:
        sprint_id = occurrence.sprint_id
        if sprint_id is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        declared = occurrence.source.get("predecessor_archive_id")
        cached = self._predecessor_archives.get(sprint_id)
        if declared is not None and cached is not None and declared != cached:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        predecessor_id = declared if declared is not None else cached
        if predecessor_id is None:
            return occurrence, None, None
        if type(predecessor_id) is not str or _ARCHIVE_ID_RE.fullmatch(predecessor_id) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        selected = self.lookup_archive(
            sprint_id=sprint_id,
            archive_id=predecessor_id,
        )
        roots = selected.manifest.get("roots")
        if selected.archive_format != "incremental_transition_v1" or type(roots) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        source = copy.deepcopy(dict(occurrence.source))
        if declared is None:
            source["predecessor_archive_id"] = predecessor_id
        effective = dataclasses.replace(occurrence, source=source)
        return (
            effective,
            copy.deepcopy(roots),
            copy.deepcopy(selected.pointer["manifest_ref"]),
        )

    def _remember_predecessor(
        self,
        sprint_id: str,
        archive_id: str,
        predecessor_archive_id: Any,
    ) -> None:
        _require_safe_id(sprint_id)
        if type(archive_id) is not str or _ARCHIVE_ID_RE.fullmatch(archive_id) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if predecessor_archive_id is not None and (
            type(predecessor_archive_id) is not str
            or _ARCHIVE_ID_RE.fullmatch(predecessor_archive_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        current = self._predecessor_archives.get(sprint_id)
        # Replaying an older completed occurrence must not move the in-memory
        # stream head backwards.  A new head may advance only from the exact
        # predecessor that was verified while its graph was built.
        if current not in {None, archive_id, predecessor_archive_id}:
            return
        self._predecessor_archives[sprint_id] = archive_id
        self._predecessor_archives.move_to_end(sprint_id)
        while len(self._predecessor_archives) > 256:
            self._predecessor_archives.popitem(last=False)

    @staticmethod
    def _operation_id(occurrence: ArchiveOccurrence) -> str:
        # A durable occurrence has one publication operation.  Determinism
        # makes a caller retry converge on the same stable plan and turns a
        # changed source for that occurrence into a generation conflict.
        digest = pas1_id_v1(
            "shadow-publication-operation",
            (
                ("project_id", occurrence.project_id),
                ("sprint_id", occurrence.sprint_id),
                ("response_id", occurrence.response_id),
                ("occurrence_id", occurrence.occurrence_id),
            ),
        )
        return "parop1-" + digest[:32]

    def _authoritative_archive_owner(
        self,
        *,
        occurrence: ArchiveOccurrence,
        operation_id: str,
        recovery_key: str,
        archive_id: str,
        pointer_id: str,
        locator_id: str,
        target_authority_id: str,
        transition_stub_name: str,
        descriptor_stub_id: str,
    ) -> dict[str, Any]:
        """Freeze the path-free, non-circular identity of the outer plan."""

        projection = {
            "project_id": self.project_id,
            "sprint_id": occurrence.sprint_id,
            "response_id": occurrence.response_id,
            "response_kind": occurrence.response_kind,
            "operation_id": operation_id,
            "recovery_key": recovery_key,
            "archive_id": archive_id,
            "pointer_id": pointer_id,
            "locator_id": locator_id,
            "target_authority_id": target_authority_id,
            "transition_stub_name": transition_stub_name,
            "descriptor_stub_id": descriptor_stub_id,
        }
        raw = canonical_json_bytes_v1(projection, maximum=_CONTROL_MAX_BYTES)
        return {
            "protocol": "prompt-archive-latest-archive-owner-v1",
            **projection,
            "outer_plan_identity_sha256": hashlib.sha256(raw).hexdigest(),
        }

    def _build_shadow_plan(
        self,
        incremental: PromptArchiveIncremental,
        built: Any,
        occurrence: ArchiveOccurrence,
        *,
        publication_mode: str = "shadow",
        compatibility_publisher: PromptArchiveCompatibilityPublisher | None = None,
    ) -> dict[str, Any]:
        if publication_mode not in {"shadow", "authoritative"}:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        operation_id = self._operation_id(occurrence)
        planned_at = _require_rfc3339(occurrence.verified_at)
        source_observed_at = _require_rfc3339(occurrence.created_at)
        timestamps = {
            "source_observed_at": source_observed_at,
            "planned_at": planned_at,
            "intent_at": planned_at,
            "blobs_verified_at": planned_at,
            "manifest_verified_at": planned_at,
            "pointer_published_at": planned_at,
            "locator_published_at": planned_at,
            "compatibility_published_at": planned_at,
            "completed_at": planned_at,
            "aborted_at": planned_at,
        }
        manifest = _copy_document(built.manifest, maximum=_MANIFEST_MAX_BYTES)
        manifest_ref = _closed_ref(
            built.manifest_ref,
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        pointer: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_pointer",
            "id_algorithm": "PAS1-ID-v1",
            "pointer_id": "",
            "project_id": self.project_id,
            "sprint_id": occurrence.sprint_id,
            "response_id": occurrence.response_id,
            "archive_id": built.archive_id,
            "generation": 1,
            "previous_pointer_id": None,
            "manifest_ref": copy.deepcopy(manifest_ref),
            "published_at": timestamps["pointer_published_at"],
        }
        pointer["pointer_id"] = self._pointer_id(pointer)
        pointer_raw = canonical_json_bytes_v1(pointer, maximum=_CONTROL_MAX_BYTES)
        pointer_ref = self._require_storage().put_blob(
            pointer_raw,
            logical_kind="pointer",
            media_type=_POINTER_MEDIA,
        )
        locator: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_locator",
            "media_type": _LOCATOR_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "locator_id": "",
            "scope_kind": "sprint",
            "project_id": self.project_id,
            "sprint_id": occurrence.sprint_id,
            "response_id": occurrence.response_id,
            "archive_id": built.archive_id,
            "pointer_id": pointer["pointer_id"],
            "pointer_ref": copy.deepcopy(pointer_ref),
            "manifest_ref": copy.deepcopy(manifest_ref),
            "published_at": timestamps["locator_published_at"],
        }
        locator["locator_id"] = self._locator_id(locator)
        locator_raw = canonical_json_bytes_v1(locator, maximum=_CONTROL_MAX_BYTES)
        locator_ref = self._require_storage().put_blob(
            locator_raw,
            logical_kind="archive_locator",
            media_type=_LOCATOR_MEDIA,
        )
        recovery_key = "parloc1-" + pas1_id_v1(
            "publication-locator",
            (
                ("project_id", self.project_id),
                ("sprint_id", occurrence.sprint_id),
                ("response_id", occurrence.response_id),
                ("occurrence_id", occurrence.occurrence_id),
                ("operation_id", operation_id),
            ),
        )
        shard_id = hashlib.sha256(recovery_key.encode("utf-8")).hexdigest()[:2]
        recovery_locator = {
            "locator_version": "prompt-archive-publication-locator-v1",
            "recovery_key": recovery_key,
            "project_id": self.project_id,
            "sprint_id": occurrence.sprint_id,
            "response_id": occurrence.response_id,
            "occurrence_id": occurrence.occurrence_id,
            "operation_id": operation_id,
        }
        source_locator = self._runtime_occurrence_locator(occurrence)
        compatibility_publication: dict[str, Any] = {"mode": "none"}
        if publication_mode == "authoritative":
            publisher = self._require_compatibility_publisher(
                compatibility_publisher
            )
            target_authority_id = publisher.target_authority_id
            provisional = PromptArchivePublication(
                project_id=self.project_id,
                sprint_id=occurrence.sprint_id,
                archive_id=built.archive_id,
                archive_format=manifest["archive_format"],
                scope=copy.deepcopy(manifest["scope"]),
                integrity=copy.deepcopy(manifest["integrity"]),
                created_at=manifest["created_at"],
                pointer_id=pointer["pointer_id"],
                manifest_ref=copy.deepcopy(manifest_ref),
                locator=copy.deepcopy(locator),
            )
            descriptor = self.descriptor_stub(provisional)
            descriptor_bytes = canonical_json_bytes_v1(
                descriptor,
                maximum=_CONTROL_MAX_BYTES,
            )
            descriptor_ref = self._require_storage().put_blob(
                descriptor_bytes,
                logical_kind="descriptor_stub",
                media_type=_STUB_MEDIA,
            )
            transition_stub = {
                "publication_mode": "immutable_create_or_verify",
                "stub_id": descriptor["stub_id"],
                "response_id": occurrence.response_id,
                "descriptor_ref": copy.deepcopy(descriptor_ref),
                "published_at": descriptor["published_at"],
            }
            archive_owner = self._authoritative_archive_owner(
                occurrence=occurrence,
                operation_id=operation_id,
                recovery_key=recovery_key,
                archive_id=built.archive_id,
                pointer_id=pointer["pointer_id"],
                locator_id=locator["locator_id"],
                target_authority_id=target_authority_id,
                transition_stub_name=(
                    f"{occurrence.response_kind}-{occurrence.response_id}.json"
                ),
                descriptor_stub_id=descriptor["stub_id"],
            )
            try:
                latest_intent = publisher.prepare_authoritative(
                    descriptor_bytes=descriptor_bytes,
                    descriptor_ref=copy.deepcopy(descriptor_ref),
                    occurrence=occurrence,
                    operation_id=operation_id,
                    published_at=timestamps["compatibility_published_at"],
                )
            except PromptArchiveStorageError:
                raise
            except Exception:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            compatibility_publication = {
                "mode": "authoritative_descriptor_stubs",
                "target_authority_id": target_authority_id,
                "archive_owner": archive_owner,
                "descriptor": {
                    "identity_version": (
                        "prompt-archive-descriptor-stub-identity-v1"
                    ),
                    "descriptor_ref": copy.deepcopy(descriptor_ref),
                    "document": descriptor,
                },
                "transition_stub": transition_stub,
                "latest_intent": _copy_document(
                    latest_intent,
                    maximum=_CONTROL_MAX_BYTES,
                ),
            }
        return {
            "schema_version": 1,
            "kind": "prompt_archive_publication_plan",
            "media_type": _PLAN_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "operation_id": operation_id,
            "operation_kind": (
                "shadow_publish"
                if publication_mode == "shadow"
                else "native_publish"
            ),
            "scope": {
                "project_id": self.project_id,
                "sprint_id": occurrence.sprint_id,
            },
            "occurrence": {
                "response_id": occurrence.response_id,
                "response_kind": occurrence.response_kind,
                "occurrence_kind": occurrence.occurrence_kind,
                "occurrence_id": occurrence.occurrence_id,
                "transition_id": occurrence.transition_id,
            },
            "recovery_locator": recovery_locator,
            "pending_registry": {
                "protocol": "prompt-archive-pending-registry-cas-v1",
                "shard": {
                    "project_id": self.project_id,
                    "shard_algorithm": "sha256-recovery-key-prefix-2-v1",
                    "shard_id": shard_id,
                    "max_entries": 1024,
                },
                "entry_precondition": {"kind": "absent"},
                "intended_mutation": {
                    "mutation": "upsert_planned",
                    "operation_id": operation_id,
                    "recovery_key": recovery_key,
                    "plan_ref_binding": "canonical_current_plan_ref",
                    "state": "planned",
                    "journal_ref": None,
                    "created_at": planned_at,
                    "updated_at": planned_at,
                    "published_at": planned_at,
                },
            },
            "source_locator": source_locator,
            "intended_manifest": {
                "identity_version": "prompt-archive-intended-manifest-identity-v1",
                "archive_id": built.archive_id,
                "manifest_ref": manifest_ref,
                "document": manifest,
            },
            "intended_pointer": {
                "identity_version": "prompt-archive-intended-pointer-identity-v1",
                "pointer_id": pointer["pointer_id"],
                "pointer_ref": pointer_ref,
                "document": pointer,
            },
            "intended_archive_locator": {
                "identity_version": "prompt-archive-intended-locator-identity-v1",
                "locator_id": locator["locator_id"],
                "locator_ref": locator_ref,
                "document": locator,
            },
            "planned_reachable_set": _reachable_set_document(
                incremental.reachable_refs(manifest_ref)
            ),
            "pointer_precondition": {
                "expected_generation": 0,
                "previous_pointer_id": None,
            },
            "compatibility_publication": compatibility_publication,
            "frozen_timestamps": timestamps,
        }

    def _build_offline_plan(
        self,
        incremental: PromptArchiveIncremental,
        built: Any,
        occurrence: ArchiveOccurrence,
        *,
        operation_id: str,
        inventory_locator: Mapping[str, Any],
    ) -> dict[str, Any]:
        if (
            type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
            or occurrence.sprint_id is not None
            or occurrence.response_kind != "migration-snapshot"
            or occurrence.occurrence_kind != "migration"
            or occurrence.transition_id is not None
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        planned_at = _require_rfc3339(occurrence.verified_at)
        source_observed_at = _require_rfc3339(occurrence.created_at)
        timestamps = {
            "source_observed_at": source_observed_at,
            "planned_at": planned_at,
            "intent_at": planned_at,
            "blobs_verified_at": planned_at,
            "manifest_verified_at": planned_at,
            "pointer_published_at": planned_at,
            "locator_published_at": planned_at,
            "compatibility_published_at": planned_at,
            "completed_at": planned_at,
            "aborted_at": planned_at,
        }
        manifest = _copy_document(built.manifest, maximum=_MANIFEST_MAX_BYTES)
        manifest_ref = _closed_ref(
            built.manifest_ref,
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        pointer: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_pointer",
            "id_algorithm": "PAS1-ID-v1",
            "pointer_id": "",
            "project_id": self.project_id,
            "sprint_id": None,
            "response_id": occurrence.response_id,
            "archive_id": built.archive_id,
            "generation": 1,
            "previous_pointer_id": None,
            "manifest_ref": copy.deepcopy(manifest_ref),
            "published_at": timestamps["pointer_published_at"],
        }
        pointer["pointer_id"] = self._pointer_id(pointer)
        pointer_raw = canonical_json_bytes_v1(pointer, maximum=_CONTROL_MAX_BYTES)
        pointer_ref = self._require_storage().put_blob(
            pointer_raw,
            logical_kind="pointer",
            media_type=_POINTER_MEDIA,
        )
        locator: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_locator",
            "media_type": _LOCATOR_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "locator_id": "",
            "scope_kind": "project",
            "project_id": self.project_id,
            "sprint_id": None,
            "response_id": occurrence.response_id,
            "archive_id": built.archive_id,
            "pointer_id": pointer["pointer_id"],
            "pointer_ref": copy.deepcopy(pointer_ref),
            "manifest_ref": copy.deepcopy(manifest_ref),
            "published_at": timestamps["locator_published_at"],
        }
        locator["locator_id"] = self._locator_id(locator)
        locator_raw = canonical_json_bytes_v1(locator, maximum=_CONTROL_MAX_BYTES)
        locator_ref = self._require_storage().put_blob(
            locator_raw,
            logical_kind="archive_locator",
            media_type=_LOCATOR_MEDIA,
        )
        recovery_key = "parloc1-" + pas1_id_v1(
            "publication-locator",
            (
                ("project_id", self.project_id),
                ("sprint_id", None),
                ("response_id", occurrence.response_id),
                ("occurrence_id", occurrence.occurrence_id),
                ("operation_id", operation_id),
            ),
        )
        shard_id = hashlib.sha256(recovery_key.encode("utf-8")).hexdigest()[:2]
        recovery_locator = {
            "locator_version": "prompt-archive-publication-locator-v1",
            "recovery_key": recovery_key,
            "project_id": self.project_id,
            "sprint_id": None,
            "response_id": occurrence.response_id,
            "occurrence_id": occurrence.occurrence_id,
            "operation_id": operation_id,
        }
        return {
            "schema_version": 1,
            "kind": "prompt_archive_publication_plan",
            "media_type": _PLAN_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "operation_id": operation_id,
            "operation_kind": "offline_migration",
            "scope": {"project_id": self.project_id, "sprint_id": None},
            "occurrence": {
                "response_id": occurrence.response_id,
                "response_kind": occurrence.response_kind,
                "occurrence_kind": occurrence.occurrence_kind,
                "occurrence_id": occurrence.occurrence_id,
                "transition_id": None,
            },
            "recovery_locator": recovery_locator,
            "pending_registry": {
                "protocol": "prompt-archive-pending-registry-cas-v1",
                "shard": {
                    "project_id": self.project_id,
                    "shard_algorithm": "sha256-recovery-key-prefix-2-v1",
                    "shard_id": shard_id,
                    "max_entries": 1024,
                },
                "entry_precondition": {"kind": "absent"},
                "intended_mutation": {
                    "mutation": "upsert_planned",
                    "operation_id": operation_id,
                    "recovery_key": recovery_key,
                    "plan_ref_binding": "canonical_current_plan_ref",
                    "state": "planned",
                    "journal_ref": None,
                    "created_at": planned_at,
                    "updated_at": planned_at,
                    "published_at": planned_at,
                },
            },
            "source_locator": copy.deepcopy(dict(inventory_locator)),
            "intended_manifest": {
                "identity_version": "prompt-archive-intended-manifest-identity-v1",
                "archive_id": built.archive_id,
                "manifest_ref": manifest_ref,
                "document": manifest,
            },
            "intended_pointer": {
                "identity_version": "prompt-archive-intended-pointer-identity-v1",
                "pointer_id": pointer["pointer_id"],
                "pointer_ref": pointer_ref,
                "document": pointer,
            },
            "intended_archive_locator": {
                "identity_version": "prompt-archive-intended-locator-identity-v1",
                "locator_id": locator["locator_id"],
                "locator_ref": locator_ref,
                "document": locator,
            },
            "planned_reachable_set": _reachable_set_document(
                incremental.reachable_refs(manifest_ref)
            ),
            "pointer_precondition": {
                "expected_generation": 0,
                "previous_pointer_id": None,
            },
            "compatibility_publication": {"mode": "none"},
            "frozen_timestamps": timestamps,
        }

    @staticmethod
    def _plan_path(plan: Mapping[str, Any]) -> tuple[str, ...]:
        return (
            "operations",
            "v1",
            plan["occurrence"]["response_id"],
            plan["operation_id"],
            "plan.json",
        )

    @staticmethod
    def _operation_lock_path(plan: Mapping[str, Any]) -> tuple[str, ...]:
        return (
            "operations",
            "v1",
            plan["occurrence"]["response_id"],
            plan["operation_id"],
            "operation.lock",
        )

    @staticmethod
    def _journal_current_path(plan: Mapping[str, Any]) -> tuple[str, ...]:
        return (
            "operations",
            "v1",
            plan["occurrence"]["response_id"],
            plan["operation_id"],
            "journal.json",
        )

    @staticmethod
    def _compatibility_result_path(plan: Mapping[str, Any]) -> tuple[str, ...]:
        return (
            "operations",
            "v1",
            plan["occurrence"]["response_id"],
            plan["operation_id"],
            "compatibility-result.json",
        )

    def _validate_compatibility_result(
        self,
        plan: Mapping[str, Any],
        value: Mapping[str, Any],
    ) -> dict[str, Any]:
        result = dict(value)
        intent = plan["compatibility_publication"]["latest_intent"]
        if set(result) != {
            "schema_version",
            "kind",
            "media_type",
            "operation_id",
            "intent_id",
            "result_latest_slot_id",
            "result_latest_slot_ref",
            "result_latest_slot_generation",
            "latest_slot_outcome",
            "content_sha256",
            "content_size",
            "source_revision",
            "published_at",
        } or (
            result.get("schema_version") != 1
            or result.get("kind") != "prompt_archive_compatibility_result"
            or result.get("media_type") != _COMPATIBILITY_RESULT_MEDIA
            or result.get("operation_id") != plan["operation_id"]
            or result.get("intent_id") != intent["intent_id"]
            or type(result.get("result_latest_slot_id")) is not str
            or _LATEST_SLOT_ID_RE.fullmatch(result["result_latest_slot_id"])
            is None
            or type(result.get("result_latest_slot_generation")) is not int
            or not 1
            <= result["result_latest_slot_generation"]
            <= 9_223_372_036_854_775_807
            or result.get("latest_slot_outcome")
            not in {"published", "superseded"}
            or result.get("content_sha256") != intent["content_sha256"]
            or result.get("content_size") != intent["content_size"]
            or result.get("source_revision")
            != intent["ordering"]["source_revision"]
            or result.get("published_at")
            != plan["frozen_timestamps"]["compatibility_published_at"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        result["result_latest_slot_ref"] = _closed_ref(
            result["result_latest_slot_ref"],
            logical_kind="latest_slot",
            media_type=_LATEST_SLOT_MEDIA,
        )
        return result

    @staticmethod
    def _compatibility_receipt_as_publication(
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "outcome": receipt["latest_slot_outcome"],
            "intent_id": receipt["intent_id"],
            "latest_slot_id": receipt["result_latest_slot_id"],
            "latest_slot_ref": copy.deepcopy(receipt["result_latest_slot_ref"]),
            "source_revision": receipt["source_revision"],
            "generation": receipt["result_latest_slot_generation"],
            "content_sha256": receipt["content_sha256"],
            "content_size": receipt["content_size"],
            "recovered": True,
        }

    def _load_compatibility_result(
        self,
        plan: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        try:
            value = self._require_storage().read_immutable_json(
                self._compatibility_result_path(plan)
            )
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_NOT_FOUND":
                return None
            raise
        return self._validate_compatibility_result(plan, value)

    def _ensure_authoritative_compatibility(
        self,
        plan: Mapping[str, Any],
        publisher: PromptArchiveCompatibilityPublisher,
    ) -> dict[str, Any]:
        existing = self._load_compatibility_result(plan)
        if existing is not None:
            try:
                reverified = publisher.verify_authoritative(
                    copy.deepcopy(plan["compatibility_publication"]),
                    self._compatibility_receipt_as_publication(existing),
                )
            except PromptArchiveStorageError:
                raise
            except Exception:
                raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
            if not isinstance(reverified, Mapping):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            normalized = self._compatibility_receipt_as_publication(existing)
            normalized["recovered"] = reverified.get("recovered")
            if any(
                reverified.get(key) != value
                for key, value in normalized.items()
                if key != "outcome"
            ) or reverified.get("outcome") not in {
                existing["latest_slot_outcome"],
                "idempotent"
                if existing["latest_slot_outcome"] == "published"
                else existing["latest_slot_outcome"],
            }:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            return existing
        try:
            published = publisher.publish_authoritative(
                copy.deepcopy(plan["compatibility_publication"])
            )
        except PromptArchiveStorageError:
            raise
        except Exception:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if not isinstance(published, Mapping):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            verified = publisher.verify_authoritative(
                copy.deepcopy(plan["compatibility_publication"]),
                copy.deepcopy(dict(published)),
            )
        except PromptArchiveStorageError:
            raise
        except Exception:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if not isinstance(verified, Mapping):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        intent = plan["compatibility_publication"]["latest_intent"]
        outcome = verified.get("outcome")
        normalized_outcome = "published" if outcome == "idempotent" else outcome
        receipt = self._validate_compatibility_result(
            plan,
            {
                "schema_version": 1,
                "kind": "prompt_archive_compatibility_result",
                "media_type": _COMPATIBILITY_RESULT_MEDIA,
                "operation_id": plan["operation_id"],
                "intent_id": verified.get("intent_id"),
                "result_latest_slot_id": verified.get("latest_slot_id"),
                "result_latest_slot_ref": verified.get("latest_slot_ref"),
                "result_latest_slot_generation": verified.get("generation"),
                "latest_slot_outcome": normalized_outcome,
                "content_sha256": verified.get("content_sha256"),
                "content_size": verified.get("content_size"),
                "source_revision": verified.get("source_revision"),
                "published_at": intent["published_at"],
            },
        )
        raw = canonical_json_bytes_v1(receipt, maximum=_CONTROL_MAX_BYTES)
        self._require_storage().publish_immutable_bytes(
            self._compatibility_result_path(plan),
            raw,
            expected_sha256=hashlib.sha256(raw).hexdigest(),
        )
        self._require_storage()._fault("after_compatibility_publication")
        observed = self._load_compatibility_result(plan)
        if observed != receipt:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return receipt

    @staticmethod
    def _journal_generation_path(
        plan: Mapping[str, Any], generation: int, digest: str
    ) -> tuple[str, ...]:
        return (
            "operations",
            "v1",
            plan["occurrence"]["response_id"],
            plan["operation_id"],
            "journal-generations",
            f"{generation}-{digest}.json",
        )

    @staticmethod
    def _registry_current_path(shard_id: str) -> tuple[str, ...]:
        return ("pending", "v1", "shards", f"{shard_id}.json")

    @staticmethod
    def _registry_lock_path(shard_id: str) -> tuple[str, ...]:
        return ("pending", "v1", "locks", f"{shard_id}.lock")

    @staticmethod
    def _registry_generation_path(
        shard_id: str, generation: int, registry_id: str
    ) -> tuple[str, ...]:
        return (
            "pending",
            "v1",
            "generations",
            shard_id,
            f"{generation}-{registry_id}.json",
        )

    @staticmethod
    def _planned_entry(
        plan: Mapping[str, Any], plan_ref: Mapping[str, Any]
    ) -> dict[str, Any]:
        planned_at = plan["frozen_timestamps"]["planned_at"]
        return {
            "operation_id": plan["operation_id"],
            "recovery_key": plan["recovery_locator"]["recovery_key"],
            "plan_ref": copy.deepcopy(dict(plan_ref)),
            "state": "planned",
            "journal_ref": None,
            "created_at": planned_at,
            "updated_at": planned_at,
        }

    def _validate_registry(
        self,
        value: Mapping[str, Any],
        *,
        shard_id: str,
    ) -> dict[str, Any]:
        registry = dict(value)
        if set(registry) != {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "registry_id",
            "project_id",
            "shard_algorithm",
            "shard_id",
            "generation",
            "previous_registry_id",
            "entries",
            "published_at",
        } or (
            registry.get("schema_version") != 1
            or registry.get("kind") != "prompt_archive_pending_operation_registry"
            or registry.get("media_type") != _PENDING_MEDIA
            or registry.get("id_algorithm") != "PAS1-ID-v1"
            or registry.get("project_id") != self.project_id
            or registry.get("shard_algorithm")
            != "sha256-recovery-key-prefix-2-v1"
            or registry.get("shard_id") != shard_id
            or _SHARD_RE.fullmatch(shard_id) is None
            or type(registry.get("generation")) is not int
            or not 1 <= registry["generation"] <= 9_223_372_036_854_775_807
            or type(registry.get("entries")) is not list
            or len(registry["entries"]) > 1024
            or type(registry.get("registry_id")) is not str
            or _PENDING_ID_RE.fullmatch(registry["registry_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        previous_id = registry.get("previous_registry_id")
        if (registry["generation"] == 1 and previous_id is not None) or (
            registry["generation"] > 1
            and (
                type(previous_id) is not str
                or _PENDING_ID_RE.fullmatch(previous_id) is None
            )
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_rfc3339(registry.get("published_at"))
        entries: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for raw_entry in registry["entries"]:
            if type(raw_entry) is not dict or set(raw_entry) != {
                "operation_id",
                "recovery_key",
                "plan_ref",
                "state",
                "journal_ref",
                "created_at",
                "updated_at",
            }:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            entry = copy.deepcopy(raw_entry)
            if (
                type(entry["operation_id"]) is not str
                or _OPERATION_ID_RE.fullmatch(entry["operation_id"]) is None
                or type(entry["recovery_key"]) is not str
                or _RECOVERY_KEY_RE.fullmatch(entry["recovery_key"]) is None
                or entry["recovery_key"] in seen_keys
                or hashlib.sha256(entry["recovery_key"].encode("utf-8")).hexdigest()[:2]
                != shard_id
                or entry["state"] not in {"planned", "journaled"}
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            _closed_ref(
                entry["plan_ref"],
                logical_kind="publication_plan",
                media_type=_PLAN_MEDIA,
            )
            if entry["state"] == "planned":
                if entry["journal_ref"] is not None:
                    raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            else:
                _closed_ref(
                    entry["journal_ref"],
                    logical_kind="journal",
                    media_type=_JOURNAL_MEDIA,
                )
            _require_rfc3339(entry["created_at"])
            _require_rfc3339(entry["updated_at"])
            seen_keys.add(entry["recovery_key"])
            entries.append(entry)
        if entries != sorted(
            entries, key=lambda item: (item["recovery_key"], item["operation_id"])
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        entries_raw = canonical_json_bytes_v1(entries, maximum=_PLAN_MAX_BYTES)
        expected_id = "parpending1-" + pas1_id_v1(
            "pending-operation-registry-id",
            (
                ("project_id", self.project_id),
                ("shard_id", shard_id),
                ("generation", str(registry["generation"])),
                ("previous_registry_id", previous_id),
                ("entries_sha256", hashlib.sha256(entries_raw).hexdigest()),
                ("published_at", registry["published_at"]),
            ),
        )
        if registry["registry_id"] != expected_id:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return registry

    def _read_registry_locked(self, shard_id: str) -> dict[str, Any] | None:
        storage = self._require_storage()
        current = storage.read_current_json(self._registry_current_path(shard_id))
        if current is None:
            return None
        registry = self._validate_registry(current, shard_id=shard_id)
        retained = storage.read_immutable_json(
            self._registry_generation_path(
                shard_id, registry["generation"], registry["registry_id"]
            )
        )
        if retained != registry:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if registry["generation"] > 1:
            predecessor = storage.read_immutable_json(
                self._registry_generation_path(
                    shard_id,
                    registry["generation"] - 1,
                    registry["previous_registry_id"],
                )
            )
            predecessor = self._validate_registry(
                predecessor, shard_id=shard_id
            )
            if (
                predecessor["generation"] != registry["generation"] - 1
                or predecessor["registry_id"] != registry["previous_registry_id"]
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return registry

    def _write_registry_locked(
        self,
        *,
        shard_id: str,
        previous: Mapping[str, Any] | None,
        entries: Sequence[Mapping[str, Any]],
        published_at: str,
    ) -> dict[str, Any]:
        storage = self._require_storage()
        _require_rfc3339(published_at)
        frozen_entries = sorted(
            (copy.deepcopy(dict(entry)) for entry in entries),
            key=lambda item: (item["recovery_key"], item["operation_id"]),
        )
        if len(frozen_entries) > 1024:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        generation = 1 if previous is None else previous["generation"] + 1
        if generation > 9_223_372_036_854_775_807:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        previous_id = None if previous is None else previous["registry_id"]
        entries_raw = canonical_json_bytes_v1(frozen_entries, maximum=_PLAN_MAX_BYTES)
        registry_id = "parpending1-" + pas1_id_v1(
            "pending-operation-registry-id",
            (
                ("project_id", self.project_id),
                ("shard_id", shard_id),
                ("generation", str(generation)),
                ("previous_registry_id", previous_id),
                ("entries_sha256", hashlib.sha256(entries_raw).hexdigest()),
                ("published_at", published_at),
            ),
        )
        registry = {
            "schema_version": 1,
            "kind": "prompt_archive_pending_operation_registry",
            "media_type": _PENDING_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "registry_id": registry_id,
            "project_id": self.project_id,
            "shard_algorithm": "sha256-recovery-key-prefix-2-v1",
            "shard_id": shard_id,
            "generation": generation,
            "previous_registry_id": previous_id,
            "entries": frozen_entries,
            "published_at": published_at,
        }
        registry = self._validate_registry(registry, shard_id=shard_id)
        raw = canonical_json_bytes_v1(registry, maximum=_PLAN_MAX_BYTES)
        storage.publish_immutable_bytes(
            self._registry_generation_path(shard_id, generation, registry_id),
            raw,
            expected_sha256=hashlib.sha256(raw).hexdigest(),
        )
        storage._fault("after_pending_generation")
        storage.replace_current_json(self._registry_current_path(shard_id), registry)
        storage._fault("after_pending_current")
        return registry

    def _register_planned(
        self, plan: Mapping[str, Any], plan_ref: Mapping[str, Any]
    ) -> None:
        shard_id = plan["pending_registry"]["shard"]["shard_id"]
        planned = self._planned_entry(plan, plan_ref)
        storage = self._require_storage()
        with storage.control_lock(self._registry_lock_path(shard_id)):
            registry = self._read_registry_locked(shard_id)
            entries = [] if registry is None else copy.deepcopy(registry["entries"])
            existing = next(
                (
                    entry
                    for entry in entries
                    if entry["recovery_key"] == planned["recovery_key"]
                ),
                None,
            )
            if existing is not None:
                if existing == planned:
                    return
                # A retry may meet an operation whose journal is already
                # registered.  It is the same operation only when its plan
                # and immutable identity match exactly; never downgrade it.
                if (
                    existing["operation_id"] == planned["operation_id"]
                    and existing["plan_ref"] == planned["plan_ref"]
                    and existing["state"] == "journaled"
                ):
                    return
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            entries.append(planned)
            self._write_registry_locked(
                shard_id=shard_id,
                previous=registry,
                entries=entries,
                published_at=plan["frozen_timestamps"]["planned_at"],
            )

    def _journal_document(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        generation: int,
        previous: Mapping[str, Any] | None,
        previous_ref: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if type(generation) is not int or not 0 <= generation < len(_JOURNAL_STATES):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if generation == 0 and (previous is not None or previous_ref is not None):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if generation > 0 and (previous is None or previous_ref is None):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if generation > 0 and (
            previous.get("journal_generation") != generation - 1
            or previous.get("state") != _JOURNAL_STATES[generation - 1]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        state = _JOURNAL_STATES[generation]
        updated_at = plan["frozen_timestamps"][_JOURNAL_TIMESTAMP_KEYS[generation]]
        if previous is None:
            precondition_entry = self._planned_entry(plan, plan_ref)
        else:
            precondition_entry = {
                "operation_id": plan["operation_id"],
                "recovery_key": plan["recovery_locator"]["recovery_key"],
                "plan_ref": copy.deepcopy(dict(plan_ref)),
                "state": "journaled",
                "journal_ref": copy.deepcopy(dict(previous_ref)),
                "created_at": plan["frozen_timestamps"]["planned_at"],
                "updated_at": previous["updated_at"],
            }
        terminal = state == "complete"
        pending_mutation = (
            {
                "mutation": "remove_terminal_entry",
                "terminal_state": "complete",
                "published_at": updated_at,
            }
            if terminal
            else {
                "mutation": "bind_current_journal",
                "journal_ref_binding": "canonical_current_journal_ref",
                "updated_at": updated_at,
                "published_at": updated_at,
            }
        )
        pointer = plan["intended_pointer"]
        locator = plan["intended_archive_locator"]
        events = [
            {
                "state": item_state,
                "ordinal": ordinal,
                "occurred_at": plan["frozen_timestamps"][
                    _JOURNAL_TIMESTAMP_KEYS[ordinal]
                ],
            }
            for ordinal, item_state in enumerate(_JOURNAL_STATES[: generation + 1])
        ]
        compatibility = plan["compatibility_publication"]
        compatibility_mode = compatibility["mode"]
        compatibility_receipt = (
            self._load_compatibility_result(plan)
            if generation >= 5
            and compatibility_mode == "authoritative_descriptor_stubs"
            else None
        )
        if (
            generation >= 5
            and compatibility_mode == "authoritative_descriptor_stubs"
            and compatibility_receipt is None
        ):
            raise _error("PROMPT_ARCHIVE_NOT_FOUND")
        return {
            "schema_version": 1,
            "kind": "prompt_archive_journal",
            "media_type": _JOURNAL_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "journal_id": "parjournal1-"
            + pas1_id_v1(
                "publication-journal",
                (
                    ("project_id", self.project_id),
                    ("sprint_id", plan["scope"]["sprint_id"]),
                    ("response_id", plan["occurrence"]["response_id"]),
                    ("operation_id", plan["operation_id"]),
                ),
            ),
            "operation_id": plan["operation_id"],
            "operation_kind": plan["operation_kind"],
            "scope": copy.deepcopy(plan["scope"]),
            "occurrence": {
                "response_id": plan["occurrence"]["response_id"],
                "occurrence_id": plan["occurrence"]["occurrence_id"],
            },
            "recovery_locator": copy.deepcopy(plan["recovery_locator"]),
            "pending_registry": {
                "protocol": "prompt-archive-pending-registry-cas-v1",
                "shard": copy.deepcopy(plan["pending_registry"]["shard"]),
                "precondition_entry": precondition_entry,
                "intended_mutation": pending_mutation,
            },
            "archive_id": plan["intended_manifest"]["archive_id"],
            "plan_ref": copy.deepcopy(dict(plan_ref)),
            "manifest_ref": copy.deepcopy(plan["intended_manifest"]["manifest_ref"]),
            "pointer_ref": copy.deepcopy(pointer["pointer_ref"]),
            "archive_locator_ref": copy.deepcopy(locator["locator_ref"]),
            "compatibility_mode": compatibility_mode,
            "compatibility_descriptor_ref": (
                None
                if compatibility_mode == "none"
                else copy.deepcopy(compatibility["descriptor"]["descriptor_ref"])
            ),
            "latest_intent": (
                None
                if compatibility_mode == "none"
                else copy.deepcopy(compatibility["latest_intent"])
            ),
            "expected_pointer_generation": 0,
            "previous_pointer_id": None,
            "journal_generation": generation,
            "previous_journal_generation": None if previous is None else generation - 1,
            "previous_journal_sha256": (
                None
                if previous is None
                else hashlib.sha256(
                    canonical_json_bytes_v1(previous, maximum=_JOURNAL_MAX_BYTES)
                ).hexdigest()
            ),
            "result_pointer_id": pointer["pointer_id"] if generation >= 3 else None,
            "result_archive_locator_id": (
                locator["locator_id"] if generation >= 4 else None
            ),
            "result_latest_slot_id": (
                None
                if compatibility_receipt is None
                else compatibility_receipt["result_latest_slot_id"]
            ),
            "result_latest_slot_ref": (
                None
                if compatibility_receipt is None
                else copy.deepcopy(
                    compatibility_receipt["result_latest_slot_ref"]
                )
            ),
            "latest_slot_outcome": (
                "pending"
                if generation < 5
                else (
                    "not_applicable"
                    if compatibility_mode == "none"
                    else compatibility_receipt["latest_slot_outcome"]
                )
            ),
            "compatibility_result": (
                "pending"
                if generation < 5
                else (
                    "none"
                    if compatibility_mode == "none"
                    else compatibility_receipt["latest_slot_outcome"]
                )
            ),
            "frozen_timestamps": copy.deepcopy(plan["frozen_timestamps"]),
            "state": state,
            "events": events,
            "updated_at": updated_at,
        }

    def _aborted_journal_document(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        previous: Mapping[str, Any],
        previous_ref: Mapping[str, Any],
        *,
        reason_code: str,
    ) -> dict[str, Any]:
        from_state = previous.get("state")
        generation = previous.get("journal_generation", -1) + 1
        if (
            from_state not in _PRE_POINTER_STATES
            or reason_code not in _ABORT_REASON_CODES
            or generation not in {1, 2, 3}
            or _JOURNAL_STATES[generation - 1] != from_state
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        journal = self._journal_document(
            plan,
            plan_ref,
            generation,
            previous,
            previous_ref,
        )
        aborted_at = plan["frozen_timestamps"]["aborted_at"]
        journal.update(
            {
                "state": "aborted",
                "abort_from_state": from_state,
                "abort_reason_code": reason_code,
                "result_pointer_id": None,
                "result_archive_locator_id": None,
                "latest_slot_outcome": "pending",
                "compatibility_result": "pending",
                "updated_at": aborted_at,
            }
        )
        journal["events"] = [
            *copy.deepcopy(previous["events"]),
            {
                "state": "aborted",
                "ordinal": generation,
                "occurred_at": aborted_at,
                "from_state": from_state,
            },
        ]
        journal["pending_registry"]["intended_mutation"] = {
            "mutation": "remove_terminal_entry",
            "terminal_state": "aborted",
            "published_at": aborted_at,
        }
        return journal

    @staticmethod
    def _latest_intent_id(intent: Mapping[str, Any]) -> str:
        ordering = intent["ordering"]
        authority = ordering["revision_authority"]
        bootstrap = intent["bootstrap_precondition"]
        descriptor_ref = intent["descriptor_ref"]
        return "parlatestintent1-" + pas1_id_v1(
            "latest-content-intent-id",
            (
                ("project_id", intent["project_id"]),
                ("slot_id", intent["slot_id"]),
                ("revision_protocol", authority["protocol"]),
                ("revision_epoch", str(authority["epoch"])),
                ("revision_reset_policy", authority["reset_policy"]),
                ("source_revision", str(ordering["source_revision"])),
                ("occurrence_id", ordering["occurrence_id"]),
                ("operation_id", ordering["operation_id"]),
                ("bootstrap_kind", bootstrap["kind"]),
                ("bootstrap_sha256", bootstrap.get("sha256")),
                (
                    "bootstrap_byte_size",
                    None
                    if bootstrap.get("byte_size") is None
                    else str(bootstrap["byte_size"]),
                ),
                ("content_kind", intent["content_kind"]),
                ("content_sha256", intent["content_sha256"]),
                ("content_size", str(intent["content_size"])),
                ("descriptor_digest", descriptor_ref["digest"]),
                ("source_sprint_id", intent["source_sprint_id"]),
                ("source_response_id", intent["source_response_id"]),
                ("published_at", intent["published_at"]),
            ),
        )

    def _validate_authoritative_compatibility(
        self,
        plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        compatibility = plan.get("compatibility_publication")
        if type(compatibility) is not dict or set(compatibility) != {
            "mode",
            "target_authority_id",
            "archive_owner",
            "descriptor",
            "transition_stub",
            "latest_intent",
        } or compatibility.get("mode") != "authoritative_descriptor_stubs":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        target_authority_id = compatibility["target_authority_id"]
        if (
            type(target_authority_id) is not str
            or _TARGET_AUTHORITY_ID_RE.fullmatch(target_authority_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        descriptor_artifact = compatibility["descriptor"]
        if type(descriptor_artifact) is not dict or set(descriptor_artifact) != {
            "identity_version",
            "descriptor_ref",
            "document",
        } or descriptor_artifact.get("identity_version") != (
            "prompt-archive-descriptor-stub-identity-v1"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        descriptor = self._validate_stub(descriptor_artifact["document"])
        descriptor_raw = canonical_json_bytes_v1(
            descriptor,
            maximum=_CONTROL_MAX_BYTES,
        )
        descriptor_ref = _closed_ref(
            descriptor_artifact["descriptor_ref"],
            logical_kind="descriptor_stub",
            media_type=_STUB_MEDIA,
        )
        if self._require_storage().read_blob(
            descriptor_ref,
            max_output_bytes=_CONTROL_MAX_BYTES,
        ) != descriptor_raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        transition = compatibility["transition_stub"]
        if type(transition) is not dict or set(transition) != {
            "publication_mode",
            "stub_id",
            "response_id",
            "descriptor_ref",
            "published_at",
        } or (
            transition.get("publication_mode") != "immutable_create_or_verify"
            or transition.get("stub_id") != descriptor["stub_id"]
            or transition.get("response_id") != plan["occurrence"]["response_id"]
            or transition.get("descriptor_ref") != descriptor_ref
            or transition.get("published_at") != descriptor["published_at"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

        intent = compatibility["latest_intent"]
        required_intent_keys = {
            "identity_version",
            "intent_id",
            "project_id",
            "slot_scope",
            "slot_id",
            "publication_protocol",
            "ordering",
            "bootstrap_precondition",
            "content_kind",
            "content_sha256",
            "content_size",
            "descriptor_ref",
            "source_sprint_id",
            "source_response_id",
            "published_at",
        }
        if type(intent) is not dict or set(intent) != required_intent_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        authority = {
            "protocol": "prompt-archive-durable-project-latest-revision-v1",
            "epoch": 1,
            "reset_policy": "restore_high_water_mark_or_fail_closed",
        }
        ordering = intent.get("ordering")
        bootstrap = intent.get("bootstrap_precondition")
        bootstrap_base = {
            "protocol": "prompt-archive-project-global-bootstrap-enrollment-v1",
            "lock_scope": "project",
            "slot_id": "latest-response",
            "registry_shard_count": 256,
            "writer_lock_policy": "all-latest-writers-pre-and-post-enrollment-v1",
            "lock_location_policy": "external-latest-colocated-stable-lock-v1",
            "registration_order": "observe-mark-unlock-register-relock-reconcile-v1",
            "bootstrap_completion_barrier": (
                "durable-marker-before-unlock-content-slot-before-final-unlock-v1"
            ),
        }
        if type(ordering) is not dict or set(ordering) != {
            "revision_authority",
            "source_revision",
            "occurrence_id",
            "operation_id",
        } or (
            ordering.get("revision_authority") != authority
            or type(ordering.get("source_revision")) is not int
            or not 0 <= ordering["source_revision"] <= 9_223_372_036_854_775_807
            or ordering.get("occurrence_id")
            != plan["occurrence"]["occurrence_id"]
            or ordering.get("operation_id") != plan["operation_id"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if type(bootstrap) is not dict or bootstrap.get("kind") not in {
            "enrolled",
            "absent",
            "legacy_bytes",
        } or any(bootstrap.get(key) != value for key, value in bootstrap_base.items()):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        bootstrap_keys = set(bootstrap_base) | {"kind"}
        if bootstrap["kind"] == "legacy_bytes":
            bootstrap_keys |= {"sha256", "byte_size"}
            if (
                type(bootstrap.get("sha256")) is not str
                or _SHA256_RE.fullmatch(bootstrap["sha256"]) is None
                or type(bootstrap.get("byte_size")) is not int
                or not 0 <= bootstrap["byte_size"] <= 512 * _MIB
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if set(bootstrap) != bootstrap_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            intent.get("identity_version")
            != "prompt-archive-latest-content-intent-v1"
            or intent.get("project_id") != self.project_id
            or intent.get("slot_scope") != "project"
            or intent.get("slot_id") != "latest-response"
            or intent.get("publication_protocol")
            != "prompt-archive-project-global-ordered-latest-intent-v1"
            or intent.get("content_kind") != "descriptor_stub"
            or intent.get("content_sha256")
            != hashlib.sha256(descriptor_raw).hexdigest()
            or intent.get("content_size") != len(descriptor_raw)
            or intent.get("descriptor_ref") != descriptor_ref
            or intent.get("source_sprint_id") != plan["scope"]["sprint_id"]
            or intent.get("source_response_id")
            != plan["occurrence"]["response_id"]
            or intent.get("published_at")
            != plan["frozen_timestamps"]["compatibility_published_at"]
            or type(intent.get("intent_id")) is not str
            or _LATEST_INTENT_ID_RE.fullmatch(intent["intent_id"]) is None
            or intent["intent_id"] != self._latest_intent_id(intent)
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        owner_projection = {
            "project_id": self.project_id,
            "sprint_id": plan["scope"]["sprint_id"],
            "response_id": plan["occurrence"]["response_id"],
            "response_kind": plan["occurrence"]["response_kind"],
            "operation_id": plan["operation_id"],
            "recovery_key": plan["recovery_locator"]["recovery_key"],
            "archive_id": plan["intended_manifest"]["archive_id"],
            "pointer_id": plan["intended_pointer"]["pointer_id"],
            "locator_id": plan["intended_archive_locator"]["locator_id"],
            "target_authority_id": target_authority_id,
            "transition_stub_name": (
                f"{plan['occurrence']['response_kind']}-"
                f"{plan['occurrence']['response_id']}.json"
            ),
            "descriptor_stub_id": descriptor["stub_id"],
        }
        owner_raw = canonical_json_bytes_v1(
            owner_projection,
            maximum=_CONTROL_MAX_BYTES,
        )
        expected_owner = {
            "protocol": "prompt-archive-latest-archive-owner-v1",
            **owner_projection,
            "outer_plan_identity_sha256": hashlib.sha256(owner_raw).hexdigest(),
        }
        if compatibility["archive_owner"] != expected_owner:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return compatibility

    def _validate_plan(
        self,
        value: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
    ) -> dict[str, Any]:
        plan = dict(value)
        operation_kind = plan.get("operation_kind")
        if set(plan) != {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "operation_id",
            "operation_kind",
            "scope",
            "occurrence",
            "recovery_locator",
            "pending_registry",
            "source_locator",
            "intended_manifest",
            "intended_pointer",
            "intended_archive_locator",
            "planned_reachable_set",
            "pointer_precondition",
            "compatibility_publication",
            "frozen_timestamps",
        } or (
            plan.get("schema_version") != 1
            or plan.get("kind") != "prompt_archive_publication_plan"
            or plan.get("media_type") != _PLAN_MEDIA
            or plan.get("id_algorithm") != "PAS1-ID-v1"
            or operation_kind
            not in {"shadow_publish", "native_publish", "offline_migration"}
            or type(plan.get("operation_id")) is not str
            or _OPERATION_ID_RE.fullmatch(plan["operation_id"]) is None
            or plan.get("pointer_precondition")
            != {"expected_generation": 0, "previous_pointer_id": None}
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        scope = plan.get("scope")
        occurrence = plan.get("occurrence")
        locator = plan.get("recovery_locator")
        source = plan.get("source_locator")
        if (
            type(scope) is not dict
            or set(scope) != {"project_id", "sprint_id"}
            or scope.get("project_id") != self.project_id
            or (
                operation_kind in {"shadow_publish", "native_publish"}
                and (
                    type(scope.get("sprint_id")) is not str
                    or _SAFE_ID_RE.fullmatch(scope["sprint_id"]) is None
                )
            )
            or (
                operation_kind == "offline_migration"
                and scope.get("sprint_id") is not None
            )
            or type(occurrence) is not dict
            or set(occurrence)
            != {
                "response_id",
                "response_kind",
                "occurrence_kind",
                "occurrence_id",
                "transition_id",
            }
            or type(occurrence.get("response_id")) is not str
            or _RESPONSE_ID_RE.fullmatch(occurrence["response_id"]) is None
            or type(occurrence.get("occurrence_id")) is not str
            or _SAFE_ID_RE.fullmatch(occurrence["occurrence_id"]) is None
            or occurrence.get("response_kind")
            not in {
                "identity-response",
                "transition-response",
                "state-response",
                "migration-snapshot",
            }
            or occurrence.get("occurrence_kind")
            not in {"transition", "identity-state", "migration"}
            or (
                occurrence["occurrence_kind"] == "transition"
                and (
                    type(occurrence.get("transition_id")) is not str
                    or _SAFE_ID_RE.fullmatch(occurrence["transition_id"]) is None
                )
            )
            or (
                occurrence["occurrence_kind"] != "transition"
                and occurrence.get("transition_id") is not None
            )
            or (
                operation_kind == "offline_migration"
                and (
                    occurrence.get("response_kind") != "migration-snapshot"
                    or occurrence.get("occurrence_kind") != "migration"
                )
            )
            or (
                operation_kind in {"shadow_publish", "native_publish"}
                and occurrence.get("occurrence_kind") == "migration"
            )
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if type(locator) is not dict or set(locator) != {
            "locator_version",
            "recovery_key",
            "project_id",
            "sprint_id",
            "response_id",
            "occurrence_id",
            "operation_id",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        expected_locator = {
            "locator_version": "prompt-archive-publication-locator-v1",
            "recovery_key": "parloc1-"
            + pas1_id_v1(
                "publication-locator",
                (
                    ("project_id", self.project_id),
                    ("sprint_id", scope["sprint_id"]),
                    ("response_id", occurrence["response_id"]),
                    ("occurrence_id", occurrence["occurrence_id"]),
                    ("operation_id", plan["operation_id"]),
                ),
            ),
            "project_id": self.project_id,
            "sprint_id": scope["sprint_id"],
            "response_id": occurrence["response_id"],
            "occurrence_id": occurrence["occurrence_id"],
            "operation_id": plan["operation_id"],
        }
        if locator != expected_locator:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        offline_descriptor: dict[str, Any] | None = None
        offline_base_locator: dict[str, Any] | None = None
        offline_binding: dict[str, Any] | None = None
        if operation_kind in {"shadow_publish", "native_publish"}:
            if (
                type(source) is not dict
                or set(source)
                != {
                    "locator_kind",
                    "locator_version",
                    "runtime_instance_id",
                    "runtime_occurrence_id",
                    "state_revision",
                }
                or source.get("locator_kind") != "runtime_occurrence"
                or source.get("locator_version")
                != "prompt-archive-runtime-occurrence-locator-v1"
                or type(source.get("runtime_instance_id")) is not str
                or _SAFE_ID_RE.fullmatch(source["runtime_instance_id"]) is None
                or source.get("runtime_instance_id")
                != self.config.runtime_instance_id
                or source.get("runtime_occurrence_id")
                != occurrence["occurrence_id"]
                or type(source.get("state_revision")) is not int
                or not 0
                <= source["state_revision"]
                <= 9_223_372_036_854_775_807
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        else:
            validated_source, offline_descriptor = self._validate_inventory_locator(
                source,
                require_migration_binding=True,
            )
            offline_binding = copy.deepcopy(validated_source["migration_binding"])
            offline_base_locator = {
                name: copy.deepcopy(value)
                for name, value in validated_source.items()
                if name != "migration_binding"
            }
        timestamps = plan.get("frozen_timestamps")
        expected_timestamp_keys = {
            "source_observed_at",
            "planned_at",
            "intent_at",
            "blobs_verified_at",
            "manifest_verified_at",
            "pointer_published_at",
            "locator_published_at",
            "compatibility_published_at",
            "completed_at",
            "aborted_at",
        }
        if type(timestamps) is not dict or set(timestamps) != expected_timestamp_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        for timestamp in timestamps.values():
            _require_rfc3339(timestamp)
        pending = plan.get("pending_registry")
        shard_id = hashlib.sha256(locator["recovery_key"].encode("utf-8")).hexdigest()[:2]
        planned = self._planned_entry(plan, plan_ref)
        expected_pending = {
            "protocol": "prompt-archive-pending-registry-cas-v1",
            "shard": {
                "project_id": self.project_id,
                "shard_algorithm": "sha256-recovery-key-prefix-2-v1",
                "shard_id": shard_id,
                "max_entries": 1024,
            },
            "entry_precondition": {"kind": "absent"},
            "intended_mutation": {
                "mutation": "upsert_planned",
                "operation_id": plan["operation_id"],
                "recovery_key": locator["recovery_key"],
                "plan_ref_binding": "canonical_current_plan_ref",
                "state": "planned",
                "journal_ref": None,
                "created_at": planned["created_at"],
                "updated_at": planned["updated_at"],
                "published_at": planned["updated_at"],
            },
        }
        if pending != expected_pending:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        intended_manifest = plan.get("intended_manifest")
        intended_pointer = plan.get("intended_pointer")
        intended_locator = plan.get("intended_archive_locator")
        if (
            type(intended_manifest) is not dict
            or set(intended_manifest)
            != {"identity_version", "archive_id", "manifest_ref", "document"}
            or intended_manifest.get("identity_version")
            != "prompt-archive-intended-manifest-identity-v1"
            or type(intended_pointer) is not dict
            or set(intended_pointer)
            != {"identity_version", "pointer_id", "pointer_ref", "document"}
            or intended_pointer.get("identity_version")
            != "prompt-archive-intended-pointer-identity-v1"
            or type(intended_locator) is not dict
            or set(intended_locator)
            != {"identity_version", "locator_id", "locator_ref", "document"}
            or intended_locator.get("identity_version")
            != "prompt-archive-intended-locator-identity-v1"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        manifest_ref = _closed_ref(
            intended_manifest["manifest_ref"],
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        manifest_raw = self._require_storage().read_blob(
            manifest_ref, max_output_bytes=_MANIFEST_MAX_BYTES
        )
        manifest = parse_canonical_json_v1(manifest_raw, maximum=_MANIFEST_MAX_BYTES)
        if manifest != intended_manifest["document"] or (
            manifest.get("archive_id") != intended_manifest.get("archive_id")
            or manifest.get("scope", {}).get("project_id") != self.project_id
            or manifest.get("scope", {}).get("sprint_id") != scope["sprint_id"]
            or manifest.get("scope", {}).get("response_id") != occurrence["response_id"]
            or manifest.get("scope", {}).get("occurrence_id")
            != occurrence["occurrence_id"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        for field in ("response_kind", "occurrence_kind", "transition_id"):
            if manifest.get("scope", {}).get(field) != occurrence[field]:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        publication = manifest.get("publication")
        manifest_source = manifest.get("source")
        if type(publication) is not dict or type(manifest_source) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if operation_kind == "offline_migration":
            if (
                manifest.get("archive_format") != "legacy_raw_chunks_v1"
                or manifest_source.get("source_kind") != "offline_migration"
                or publication.get("mode") != "migration"
                or publication.get("legacy_authoritative") is not True
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            integrity = manifest.get("integrity")
            if (
                offline_descriptor is None
                or offline_base_locator is None
                or offline_binding is None
                or type(integrity) is not dict
                or type(integrity.get("hydrated_size")) is not int
                or type(integrity.get("hydrated_sha256")) is not str
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            proven_binding = self._validate_offline_reservation(
                descriptor=offline_descriptor,
                inventory_locator=offline_base_locator,
                operation_id=plan["operation_id"],
                occurrence=ArchiveOccurrence(
                    project_id=self.project_id,
                    sprint_id=None,
                    response_id=occurrence["response_id"],
                    response_kind=occurrence["response_kind"],
                    occurrence_kind=occurrence["occurrence_kind"],
                    occurrence_id=occurrence["occurrence_id"],
                    transition_id=occurrence["transition_id"],
                    source=manifest_source,
                    authorization=manifest.get("authorization"),
                    created_at=manifest.get("created_at"),
                    verified_at=publication.get("verified_at"),
                ),
                byte_size=integrity["hydrated_size"],
                sha256=integrity["hydrated_sha256"],
            )
            if proven_binding != offline_binding:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        else:
            expected_mode = (
                "shadow" if operation_kind == "shadow_publish" else "authoritative"
            )
            expected_source_kind = (
                "runtime_shadow"
                if operation_kind == "shadow_publish"
                else "native_runtime"
            )
            if (
                manifest.get("archive_format") != "incremental_transition_v1"
                or manifest_source.get("source_kind") != expected_source_kind
                or publication.get("mode") != expected_mode
                or publication.get("legacy_authoritative")
                is not (operation_kind == "shadow_publish")
                or (
                    operation_kind == "native_publish"
                    and publication.get("operator_gate")
                    != "prompt-archive-authoritative-v1"
                )
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        pointer = self._validate_pointer(
            intended_pointer["document"],
            project_id=self.project_id,
            sprint_id=scope["sprint_id"],
            response_id=occurrence["response_id"],
            archive_id=manifest["archive_id"],
        )
        pointer_ref = _closed_ref(
            intended_pointer["pointer_ref"],
            logical_kind="pointer",
            media_type=_POINTER_MEDIA,
        )
        if (
            intended_pointer["pointer_id"] != pointer["pointer_id"]
            or pointer["manifest_ref"] != manifest_ref
            or pointer["published_at"] != timestamps["pointer_published_at"]
            or parse_canonical_json_v1(
                self._require_storage().read_blob(
                    pointer_ref, max_output_bytes=_CONTROL_MAX_BYTES
                ),
                maximum=_CONTROL_MAX_BYTES,
            )
            != pointer
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        archive_locator = self._validate_locator(
            intended_locator["document"],
            project_id=self.project_id,
            sprint_id=scope["sprint_id"],
            archive_id=manifest["archive_id"],
        )
        locator_ref = _closed_ref(
            intended_locator["locator_ref"],
            logical_kind="archive_locator",
            media_type=_LOCATOR_MEDIA,
        )
        if (
            intended_locator["locator_id"] != archive_locator["locator_id"]
            or archive_locator["pointer_ref"] != pointer_ref
            or archive_locator["manifest_ref"] != manifest_ref
            or archive_locator["published_at"] != timestamps["locator_published_at"]
            or parse_canonical_json_v1(
                self._require_storage().read_blob(
                    locator_ref, max_output_bytes=_CONTROL_MAX_BYTES
                ),
                maximum=_CONTROL_MAX_BYTES,
            )
            != archive_locator
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        incremental = PromptArchiveIncremental(
            self._require_storage(), self.project_id, scope["sprint_id"]
        )
        if incremental.read_manifest(manifest_ref) != manifest:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if _reachable_set_document(incremental.reachable_refs(manifest_ref)) != plan.get(
            "planned_reachable_set"
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if operation_kind == "native_publish":
            compatibility = self._validate_authoritative_compatibility(plan)
            descriptor = compatibility["descriptor"]["document"]
            provisional = PromptArchivePublication(
                project_id=self.project_id,
                sprint_id=scope["sprint_id"],
                archive_id=manifest["archive_id"],
                archive_format=manifest["archive_format"],
                scope=copy.deepcopy(manifest["scope"]),
                integrity=copy.deepcopy(manifest["integrity"]),
                created_at=manifest["created_at"],
                pointer_id=pointer["pointer_id"],
                manifest_ref=copy.deepcopy(manifest_ref),
                locator=copy.deepcopy(archive_locator),
            )
            if descriptor != self.descriptor_stub(provisional):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        elif plan.get("compatibility_publication") != {"mode": "none"}:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return plan

    def _load_plan(self, entry: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        plan_ref = _closed_ref(
            entry.get("plan_ref"),
            logical_kind="publication_plan",
            media_type=_PLAN_MEDIA,
        )
        raw = self._require_storage().read_blob(
            plan_ref, max_output_bytes=_PLAN_MAX_BYTES
        )
        plan = parse_canonical_json_v1(raw, maximum=_PLAN_MAX_BYTES)
        occurrence = plan.get("occurrence")
        if (
            type(plan.get("operation_id")) is not str
            or _OPERATION_ID_RE.fullmatch(plan["operation_id"]) is None
            or type(occurrence) is not dict
            or type(occurrence.get("response_id")) is not str
            or _RESPONSE_ID_RE.fullmatch(occurrence["response_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        stable = self._require_storage().read_immutable_json(self._plan_path(plan))
        if stable != plan:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        validated = self._validate_plan(plan, plan_ref)
        if (
            entry.get("operation_id") != validated["operation_id"]
            or entry.get("recovery_key")
            != validated["recovery_locator"]["recovery_key"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return validated, plan_ref

    def _load_journal_chain_locked(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        *,
        adopt_retained_successor: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        storage = self._require_storage()
        current = storage.read_current_json(self._journal_current_path(plan))
        if current is None:
            return None
        generation = current.get("journal_generation")
        if type(generation) is not int or not 0 <= generation < len(_JOURNAL_STATES):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        reverse_documents: list[dict[str, Any]] = []
        document = current
        expected_digest: str | None = None
        for expected_generation in range(generation, -1, -1):
            raw = canonical_json_bytes_v1(document, maximum=_JOURNAL_MAX_BYTES)
            digest = hashlib.sha256(raw).hexdigest()
            if expected_digest is not None and digest != expected_digest:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            if document.get("journal_generation") != expected_generation:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            retained = storage.read_immutable_json(
                self._journal_generation_path(plan, expected_generation, digest)
            )
            if retained != document:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            reverse_documents.append(document)
            if expected_generation == 0:
                if document.get("previous_journal_sha256") is not None:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                break
            expected_digest = document.get("previous_journal_sha256")
            if type(expected_digest) is not str or _SHA256_RE.fullmatch(expected_digest) is None:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            document = storage.read_immutable_json(
                self._journal_generation_path(
                    plan, expected_generation - 1, expected_digest
                )
            )
        documents = list(reversed(reverse_documents))
        previous: dict[str, Any] | None = None
        previous_ref: dict[str, Any] | None = None
        for ordinal, observed in enumerate(documents):
            if previous is not None:
                candidate_ref = observed.get("pending_registry", {}).get(
                    "precondition_entry", {}
                ).get("journal_ref")
                previous_ref = _closed_ref(
                    candidate_ref,
                    logical_kind="journal",
                    media_type=_JOURNAL_MEDIA,
                )
                previous_raw = canonical_json_bytes_v1(
                    previous, maximum=_JOURNAL_MAX_BYTES
                )
                if storage.read_blob(
                    previous_ref, max_output_bytes=_JOURNAL_MAX_BYTES
                ) != previous_raw:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            if observed.get("state") == "aborted":
                if previous is None or previous_ref is None or ordinal != generation:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                expected = self._aborted_journal_document(
                    plan,
                    plan_ref,
                    previous,
                    previous_ref,
                    reason_code=observed.get("abort_reason_code"),
                )
            else:
                expected = self._journal_document(
                    plan, plan_ref, ordinal, previous, previous_ref
                )
            if observed != expected:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            previous = observed
        current_raw = canonical_json_bytes_v1(current, maximum=_JOURNAL_MAX_BYTES)
        current_ref = storage.describe_blob(
            current_raw,
            logical_kind="journal",
            media_type=_JOURNAL_MEDIA,
        )
        if storage.read_blob(
            current_ref,
            max_output_bytes=_JOURNAL_MAX_BYTES,
        ) != current_raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if adopt_retained_successor:
            retained_successor = self._retained_journal_successor_locked(
                plan,
                plan_ref,
                current,
                current_ref,
            )
            if retained_successor is not None:
                return retained_successor
        return current, current_ref

    def _retained_journal_successor_locked(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        current: Mapping[str, Any],
        current_ref: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """Adopt the sole deterministic successor retained before a crash.

        Journal publication writes the immutable generation before replacing
        ``journal.json``.  A process failure at that boundary must not cause a
        retry to re-evaluate source proof and publish a different successor at
        the same generation.  Probe only the closed set of documents that this
        state machine could have produced, validate their CAS objects, reject
        forks, and finish the interrupted current-pointer replacement.

        The caller holds the per-operation lock.
        """

        state = current.get("state")
        if state in {"complete", "aborted"}:
            return None
        generation = current.get("journal_generation")
        if (
            type(generation) is not int
            or not 0 <= generation < len(_JOURNAL_STATES) - 1
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        # Generation five is defined by the immutable compatibility receipt.
        # Before that receipt exists there cannot be a valid retained
        # successor to adopt, and constructing one would incorrectly turn a
        # recoverable cross-component pause into a missing-object failure.
        if (
            state == "locator_published"
            and plan["compatibility_publication"]["mode"]
            == "authoritative_descriptor_stubs"
            and self._load_compatibility_result(plan) is None
        ):
            return None

        candidates = [
            self._journal_document(
                plan,
                plan_ref,
                generation + 1,
                current,
                current_ref,
            )
        ]
        if state in _PRE_POINTER_STATES:
            candidates.extend(
                self._aborted_journal_document(
                    plan,
                    plan_ref,
                    current,
                    current_ref,
                    reason_code=reason_code,
                )
                for reason_code in sorted(_ABORT_REASON_CODES)
            )

        storage = self._require_storage()
        retained: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for expected in candidates:
            raw = canonical_json_bytes_v1(expected, maximum=_JOURNAL_MAX_BYTES)
            digest = hashlib.sha256(raw).hexdigest()
            try:
                observed = storage.read_immutable_json(
                    self._journal_generation_path(
                        plan,
                        generation + 1,
                        digest,
                    )
                )
            except PromptArchiveStorageError as exc:
                if exc.code == "PROMPT_ARCHIVE_NOT_FOUND":
                    continue
                raise
            if observed != expected:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            successor_ref = storage.describe_blob(
                raw,
                logical_kind="journal",
                media_type=_JOURNAL_MEDIA,
            )
            if storage.read_blob(
                successor_ref,
                max_output_bytes=_JOURNAL_MAX_BYTES,
            ) != raw:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            retained.append((expected, successor_ref))

        if len(retained) > 1:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        if not retained:
            return None
        successor, successor_ref = retained[0]
        storage.replace_current_json(self._journal_current_path(plan), successor)
        storage._fault("after_journal_current")
        return successor, successor_ref

    def _publish_journal_generation_locked(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        *,
        generation: int,
        previous: Mapping[str, Any] | None,
        previous_ref: Mapping[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        storage = self._require_storage()
        journal = self._journal_document(
            plan, plan_ref, generation, previous, previous_ref
        )
        raw = canonical_json_bytes_v1(journal, maximum=_JOURNAL_MAX_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        journal_ref = storage.put_blob(
            raw,
            logical_kind="journal",
            media_type=_JOURNAL_MEDIA,
        )
        storage.publish_immutable_bytes(
            self._journal_generation_path(plan, generation, digest),
            raw,
            expected_sha256=digest,
        )
        storage._fault("after_journal_generation")
        storage.replace_current_json(self._journal_current_path(plan), journal)
        storage._fault("after_journal_current")
        return journal, journal_ref

    def _publish_abort_generation_locked(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        *,
        previous: Mapping[str, Any],
        previous_ref: Mapping[str, Any],
        reason_code: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        storage = self._require_storage()
        journal = self._aborted_journal_document(
            plan,
            plan_ref,
            previous,
            previous_ref,
            reason_code=reason_code,
        )
        raw = canonical_json_bytes_v1(journal, maximum=_JOURNAL_MAX_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        journal_ref = storage.put_blob(
            raw,
            logical_kind="journal",
            media_type=_JOURNAL_MEDIA,
        )
        storage.publish_immutable_bytes(
            self._journal_generation_path(
                plan,
                journal["journal_generation"],
                digest,
            ),
            raw,
            expected_sha256=digest,
        )
        storage._fault("after_journal_generation")
        storage.replace_current_json(self._journal_current_path(plan), journal)
        storage._fault("after_journal_current")
        return journal, journal_ref

    def _apply_pending_mutation(
        self,
        journal: Mapping[str, Any],
        journal_ref: Mapping[str, Any],
    ) -> None:
        storage = self._require_storage()
        pending = journal["pending_registry"]
        shard_id = pending["shard"]["shard_id"]
        precondition = pending["precondition_entry"]
        mutation = pending["intended_mutation"]
        recovery_key = journal["recovery_locator"]["recovery_key"]
        with storage.control_lock(self._registry_lock_path(shard_id)):
            registry = self._read_registry_locked(shard_id)
            if registry is None:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            entries = copy.deepcopy(registry["entries"])
            index = next(
                (
                    ordinal
                    for ordinal, entry in enumerate(entries)
                    if entry["recovery_key"] == recovery_key
                ),
                None,
            )
            if mutation["mutation"] == "remove_terminal_entry":
                if index is None:
                    return
                if entries[index] != precondition:
                    raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                del entries[index]
                self._write_registry_locked(
                    shard_id=shard_id,
                    previous=registry,
                    entries=entries,
                    published_at=mutation["published_at"],
                )
                storage._fault("after_terminal_registry_removal")
                return
            if mutation.get("mutation") != "bind_current_journal":
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            desired = {
                "operation_id": journal["operation_id"],
                "recovery_key": recovery_key,
                "plan_ref": copy.deepcopy(journal["plan_ref"]),
                "state": "journaled",
                "journal_ref": copy.deepcopy(dict(journal_ref)),
                "created_at": precondition["created_at"],
                "updated_at": mutation["updated_at"],
            }
            if index is None:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            if entries[index] == desired:
                return
            if entries[index] != precondition:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            entries[index] = desired
            self._write_registry_locked(
                shard_id=shard_id,
                previous=registry,
                entries=entries,
                published_at=mutation["published_at"],
            )
            storage._fault("after_journal_registry_bind")

    def _verify_plan_graph(self, plan: Mapping[str, Any]) -> None:
        manifest = plan["intended_manifest"]["document"]
        manifest_ref = plan["intended_manifest"]["manifest_ref"]
        incremental = PromptArchiveIncremental(
            self._require_storage(), self.project_id, plan["scope"]["sprint_id"]
        )
        refs = incremental.reachable_refs(manifest_ref)
        if _reachable_set_document(refs) != plan["planned_reachable_set"]:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if plan.get("operation_kind") == "native_publish":
            # The native builder verified the bounded source against its
            # render plan before the immutable manifest/plan were admitted.
            # Recovery verifies the complete reachable graph above, but must
            # not hydrate the archived response on the authoritative hot path.
            integrity = manifest.get("integrity")
            if (
                type(integrity) is not dict
                or integrity.get("semantics") != "native-byte-equality"
                or type(integrity.get("hydrated_size")) is not int
                or type(integrity.get("hydrated_sha256")) is not str
                or _SHA256_RE.fullmatch(integrity["hydrated_sha256"]) is None
                or manifest.get("legacy_comparison") is not None
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            return
        hydrated = incremental.hydrate_archive_bytes(
            manifest_ref, max_output_bytes=self.max_hydrated_bytes
        )
        integrity = manifest.get("integrity")
        comparison = manifest.get("legacy_comparison")
        if (
            type(integrity) is not dict
            or integrity.get("hydrated_size") != len(hydrated)
            or integrity.get("hydrated_sha256")
            != hashlib.sha256(hydrated).hexdigest()
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if plan.get("operation_kind") == "shadow_publish" and (
            type(comparison) is not dict
            or comparison.get("matched") is not True
            or comparison.get("byte_size") != len(hydrated)
            or comparison.get("sha256") != hashlib.sha256(hydrated).hexdigest()
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if plan.get("operation_kind") == "offline_migration" and comparison is not None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _intended_pointer_is_published(
        self,
        plan: Mapping[str, Any],
    ) -> bool:
        """Reconcile a crash after the irreversible pointer filesystem write."""

        sprint_key = sprint_storage_key_v1(
            self.project_id,
            plan["scope"]["sprint_id"],
        )
        path = (
            "sprints",
            sprint_key,
            "pointers",
            "v1",
            f"{plan['occurrence']['response_id']}.json",
        )
        try:
            observed = self._require_storage().read_immutable_json(path)
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_NOT_FOUND":
                return False
            raise
        if observed != plan["intended_pointer"]["document"]:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return True

    def _perform_state_action(
        self,
        plan: Mapping[str, Any],
        journal: Mapping[str, Any],
        *,
        offline_inventory_verifier: PromptArchiveOfflineInventoryVerifier
        | None = None,
        compatibility_publisher: PromptArchiveCompatibilityPublisher | None = None,
    ) -> str | None:
        storage = self._require_storage()
        state = journal["state"]
        # The exact external source is required at intent.  After the graph is
        # verified and ``blobs_verified`` is journaled, recovery deliberately
        # depends only on the immutable plan/manifest graph: reconstructing or
        # retaining the legacy source is neither necessary nor permitted by
        # the PAS1 recovery contract.  A source loss before this boundary takes
        # the deterministic abort path; a later loss cannot relabel verified
        # durable work as an abort.
        if state == "intent" and not self._intended_pointer_is_published(plan):
            if plan.get("operation_kind") == "offline_migration":
                abort_reason = self._offline_inventory_abort_reason(
                    plan,
                    offline_inventory_verifier,
                )
            else:
                abort_reason = self._runtime_occurrence_abort_reason(plan)
            if abort_reason is not None:
                return abort_reason
        if state == "intent":
            self._verify_plan_graph(plan)
            return None
        if state == "blobs_verified":
            incremental = PromptArchiveIncremental(
                storage, self.project_id, plan["scope"]["sprint_id"]
            )
            if incremental.read_manifest(
                plan["intended_manifest"]["manifest_ref"]
            ) != plan["intended_manifest"]["document"]:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            return None
        sprint_key = sprint_storage_key_v1(
            self.project_id, plan["scope"]["sprint_id"]
        )
        if state == "manifest_verified":
            pointer = plan["intended_pointer"]["document"]
            raw = canonical_json_bytes_v1(pointer, maximum=_CONTROL_MAX_BYTES)
            storage.publish_immutable_bytes(
                (
                    "sprints",
                    sprint_key,
                    "pointers",
                    "v1",
                    f"{plan['occurrence']['response_id']}.json",
                ),
                raw,
                expected_sha256=hashlib.sha256(raw).hexdigest(),
            )
            storage._fault("after_pointer_publication")
            return None
        if state == "pointer_published":
            locator = plan["intended_archive_locator"]["document"]
            raw = canonical_json_bytes_v1(locator, maximum=_CONTROL_MAX_BYTES)
            storage.publish_immutable_bytes(
                (
                    "sprints",
                    sprint_key,
                    "archives",
                    "v1",
                    f"{plan['intended_manifest']['archive_id']}.json",
                ),
                raw,
                expected_sha256=hashlib.sha256(raw).hexdigest(),
            )
            storage._fault("after_locator_publication")
            return None
        if state == "locator_published":
            compatibility = plan["compatibility_publication"]
            if compatibility["mode"] == "authoritative_descriptor_stubs":
                publisher = self._resolve_compatibility_publisher(
                    plan,
                    compatibility_publisher,
                )
                self._ensure_authoritative_compatibility(plan, publisher)
            elif compatibility != {"mode": "none"}:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            return None
        if state == "compatibility_published":
            return None
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

    def _drive_operation(
        self,
        plan: Mapping[str, Any],
        plan_ref: Mapping[str, Any],
        *,
        offline_inventory_verifier: PromptArchiveOfflineInventoryVerifier
        | None = None,
        compatibility_publisher: PromptArchiveCompatibilityPublisher | None = None,
    ) -> str:
        storage = self._require_storage()
        while True:
            with storage.control_lock(self._operation_lock_path(plan)):
                loaded = self._load_journal_chain_locked(plan, plan_ref)
                if loaded is None:
                    journal, journal_ref = self._publish_journal_generation_locked(
                        plan,
                        plan_ref,
                        generation=0,
                        previous=None,
                        previous_ref=None,
                    )
                else:
                    journal, journal_ref = loaded
            self._apply_pending_mutation(journal, journal_ref)
            if journal["state"] in {"complete", "aborted"}:
                return journal["state"]
            abort_reason = self._perform_state_action(
                plan,
                journal,
                offline_inventory_verifier=offline_inventory_verifier,
                compatibility_publisher=compatibility_publisher,
            )
            with storage.control_lock(self._operation_lock_path(plan)):
                latest = self._load_journal_chain_locked(plan, plan_ref)
                if latest is None:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                latest_journal, latest_ref = latest
                if latest_journal != journal:
                    continue
                if abort_reason is None:
                    successor, successor_ref = self._publish_journal_generation_locked(
                        plan,
                        plan_ref,
                        generation=journal["journal_generation"] + 1,
                        previous=journal,
                        previous_ref=journal_ref,
                    )
                else:
                    successor, successor_ref = self._publish_abort_generation_locked(
                        plan,
                        plan_ref,
                        previous=journal,
                        previous_ref=journal_ref,
                        reason_code=abort_reason,
                    )
            self._apply_pending_mutation(successor, successor_ref)
            if successor["state"] == "aborted":
                return "aborted"

    def _validate_recovery_lower_bound(
        self,
        entry: Mapping[str, Any],
        current: tuple[dict[str, Any], dict[str, Any]] | None,
    ) -> None:
        if entry["state"] == "planned":
            if current is not None and current[0]["journal_generation"] != 0:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            return
        if current is None:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        bound_ref = _closed_ref(
            entry["journal_ref"], logical_kind="journal", media_type=_JOURNAL_MEDIA
        )
        bound_raw = self._require_storage().read_blob(
            bound_ref, max_output_bytes=_JOURNAL_MAX_BYTES
        )
        bound = parse_canonical_json_v1(bound_raw, maximum=_JOURNAL_MAX_BYTES)
        current_journal = current[0]
        delta = current_journal["journal_generation"] - bound.get(
            "journal_generation", -100
        )
        if delta == 0:
            if current_journal != bound:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        elif delta == 1:
            if current_journal.get("previous_journal_sha256") != hashlib.sha256(
                bound_raw
            ).hexdigest():
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        else:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")

    def recover_pending(self) -> int:
        """Replay pending operations found through exactly 256 fixed shards."""

        if self.config.mode not in {"shadow", "authoritative"}:
            return 0
        storage = self._require_storage()
        pending: list[dict[str, Any]] = []
        for ordinal in range(256):
            shard_id = f"{ordinal:02x}"
            with storage.control_lock(self._registry_lock_path(shard_id)):
                registry = self._read_registry_locked(shard_id)
                if registry is not None:
                    pending.extend(copy.deepcopy(registry["entries"]))
        recovered = 0
        for entry in pending:
            with storage.account_cas_writes() as cas_writes:
                plan, plan_ref = self._load_plan(entry)
                with storage.control_lock(self._operation_lock_path(plan)):
                    current = self._load_journal_chain_locked(plan, plan_ref)
                    self._validate_recovery_lower_bound(entry, current)
                # Offline operations require the operator to re-supply the
                # exact isolated root handle.  Runtime startup has no such
                # authority and must leave their registry entries pending.
                # The retained plan, journal chain and registry lower bound
                # are nevertheless verified above so startup never masks a
                # fork or corrupt pending receipt.
                if plan["operation_kind"] == "offline_migration":
                    if current is None or current[0]["state"] not in {
                        "complete",
                        "aborted",
                    }:
                        continue
                    # A terminal journal needs no source authority.  Finish
                    # the exact retained registry mutation so a crash after
                    # terminal publication cannot strand a pending entry.
                    self._apply_pending_mutation(*current)
                    terminal_state = current[0]["state"]
                else:
                    terminal_state = self._drive_operation(plan, plan_ref)
            if terminal_state == "complete":
                outcomes = cas_writes.snapshot()
                self.metrics.record_publication(
                    logical_bytes=plan["intended_manifest"]["document"][
                        "integrity"
                    ]["hydrated_size"],
                    new_physical_bytes=outcomes.new_stored_bytes,
                    dedup_hits=outcomes.dedup_hits,
                )
            recovered += 1
        return recovered

    @staticmethod
    def _validate_pointer(
        value: Mapping[str, Any],
        *,
        project_id: str,
        sprint_id: str | None,
        response_id: str,
        archive_id: str,
    ) -> dict[str, Any]:
        pointer = dict(value)
        if set(pointer) != {
            "schema_version",
            "kind",
            "id_algorithm",
            "pointer_id",
            "project_id",
            "sprint_id",
            "response_id",
            "archive_id",
            "generation",
            "previous_pointer_id",
            "manifest_ref",
            "published_at",
        } or (
            pointer.get("schema_version") != 1
            or pointer.get("kind") != "prompt_archive_pointer"
            or pointer.get("id_algorithm") != "PAS1-ID-v1"
            or pointer.get("project_id") != project_id
            or pointer.get("sprint_id") != sprint_id
            or pointer.get("response_id") != response_id
            or pointer.get("archive_id") != archive_id
            or pointer.get("generation") != 1
            or pointer.get("previous_pointer_id") is not None
            or type(pointer.get("manifest_ref")) is not dict
            or type(pointer.get("pointer_id")) is not str
            or _POINTER_ID_RE.fullmatch(pointer["pointer_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_rfc3339(pointer.get("published_at"))
        _require_ref_kind(
            pointer["manifest_ref"],
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        if pointer["pointer_id"] != PromptArchiveRuntime._pointer_id(pointer):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return pointer

    @staticmethod
    def _validate_locator(
        value: Mapping[str, Any],
        *,
        project_id: str,
        sprint_id: str | None,
        archive_id: str,
    ) -> dict[str, Any]:
        locator = dict(value)
        if set(locator) != {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "locator_id",
            "scope_kind",
            "project_id",
            "sprint_id",
            "response_id",
            "archive_id",
            "pointer_id",
            "pointer_ref",
            "manifest_ref",
            "published_at",
        } or (
            locator.get("schema_version") != 1
            or locator.get("kind") != "prompt_archive_locator"
            or locator.get("media_type") != _LOCATOR_MEDIA
            or locator.get("id_algorithm") != "PAS1-ID-v1"
            or locator.get("scope_kind")
            != ("sprint" if sprint_id is not None else "project")
            or locator.get("project_id") != project_id
            or locator.get("sprint_id") != sprint_id
            or locator.get("archive_id") != archive_id
            or type(locator.get("response_id")) is not str
            or _RESPONSE_ID_RE.fullmatch(locator["response_id"]) is None
            or type(locator.get("pointer_id")) is not str
            or _POINTER_ID_RE.fullmatch(locator["pointer_id"]) is None
            or type(locator.get("locator_id")) is not str
            or _LOCATOR_ID_RE.fullmatch(locator["locator_id"]) is None
            or type(locator.get("pointer_ref")) is not dict
            or type(locator.get("manifest_ref")) is not dict
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        _require_rfc3339(locator.get("published_at"))
        _require_ref_kind(
            locator["pointer_ref"],
            logical_kind="pointer",
            media_type=_POINTER_MEDIA,
        )
        _require_ref_kind(
            locator["manifest_ref"],
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        if locator["locator_id"] != PromptArchiveRuntime._locator_id(locator):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return locator

    def _lookup_archive_metadata_scoped(
        self,
        *,
        sprint_id: str | None,
        archive_id: str,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> PromptArchiveRead:
        """Verify one exact sprint or offline project archive binding."""

        if cancellation is not None:
            cancellation.check()
        if sprint_id is not None:
            sprint_id = _require_safe_id(sprint_id)
        if type(archive_id) is not str or _ARCHIVE_ID_RE.fullmatch(archive_id) is None:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        storage = self._require_storage()
        sprint_key = sprint_storage_key_v1(self.project_id, sprint_id)

        def read_immutable(components: Sequence[str]) -> dict[str, Any]:
            if cancellation is None:
                return storage.read_immutable_json(components)
            return storage.read_immutable_json(
                components,
                cancellation=cancellation,
            )

        locator = self._validate_locator(
            read_immutable(
                ("sprints", sprint_key, "archives", "v1", f"{archive_id}.json")
            ),
            project_id=self.project_id,
            sprint_id=sprint_id,
            archive_id=archive_id,
        )
        if cancellation is None:
            pointer_raw = storage.read_blob(
                locator["pointer_ref"],
                max_output_bytes=_CONTROL_MAX_BYTES,
            )
        else:
            pointer_raw = storage.read_blob(
                locator["pointer_ref"],
                max_output_bytes=_CONTROL_MAX_BYTES,
                cancellation=cancellation,
            )
        pointer_from_cas = parse_canonical_json_v1(
            pointer_raw,
            maximum=_CONTROL_MAX_BYTES,
        )
        pointer = self._validate_pointer(
            read_immutable(
                (
                    "sprints",
                    sprint_key,
                    "pointers",
                    "v1",
                    f"{locator['response_id']}.json",
                )
            ),
            project_id=self.project_id,
            sprint_id=sprint_id,
            response_id=locator["response_id"],
            archive_id=archive_id,
        )
        if pointer_from_cas != pointer or (
            locator["pointer_id"] != pointer["pointer_id"]
            or locator["manifest_ref"] != pointer["manifest_ref"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

        if cancellation is None:
            incremental = PromptArchiveIncremental(
                storage,
                self.project_id,
                sprint_id,
            )
        else:
            incremental = PromptArchiveIncremental(
                storage,
                self.project_id,
                sprint_id,
                cancellation=cancellation,
            )
        manifest = incremental.read_manifest_metadata(pointer["manifest_ref"])
        scope = manifest.get("scope")
        if (
            manifest.get("archive_id") != archive_id
            or type(scope) is not dict
            or scope.get("project_id") != self.project_id
            or scope.get("sprint_id") != sprint_id
            or scope.get("response_id") != locator["response_id"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if sprint_id is None:
            source = manifest.get("source")
            publication = manifest.get("publication")
            if (
                manifest.get("archive_format") != "legacy_raw_chunks_v1"
                or type(source) is not dict
                or source.get("source_kind") != "offline_migration"
                or scope.get("response_kind") != "migration-snapshot"
                or scope.get("occurrence_kind") != "migration"
                or type(publication) is not dict
                or publication.get("mode") != "migration"
                or publication.get("legacy_authoritative") is not True
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return PromptArchiveRead(
            project_id=self.project_id,
            sprint_id=sprint_id,
            archive_id=archive_id,
            locator=_copy_document(locator, maximum=_CONTROL_MAX_BYTES),
            pointer=_copy_document(pointer, maximum=_CONTROL_MAX_BYTES),
            manifest=_copy_document(manifest, maximum=_MANIFEST_MAX_BYTES),
        )

    def lookup_archive_metadata(
        self,
        *,
        sprint_id: str,
        archive_id: str,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> PromptArchiveRead:
        """Verify sprint-scoped metadata without opening referenced leaves."""

        sprint_id = _require_safe_id(sprint_id)
        return self._lookup_archive_metadata_scoped(
            sprint_id=sprint_id,
            archive_id=archive_id,
            cancellation=cancellation,
        )

    def lookup_offline_archive_metadata(
        self,
        *,
        archive_id: str,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> PromptArchiveRead:
        """Internal migration lookup through locator -> pointer -> manifest."""

        return self._lookup_archive_metadata_scoped(
            sprint_id=None,
            archive_id=archive_id,
            cancellation=cancellation,
        )

    def lookup_archive(
        self,
        *,
        sprint_id: str,
        archive_id: str,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> PromptArchiveRead:
        """Backward-compatible alias for bounded metadata lookup."""

        return self.lookup_archive_metadata(
            sprint_id=sprint_id,
            archive_id=archive_id,
            cancellation=cancellation,
        )

    def _validated_read(
        self,
        selected: PromptArchiveRead,
        *,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> PromptArchiveRead:
        if not isinstance(selected, PromptArchiveRead) or selected.project_id != self.project_id:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        return self._lookup_archive_metadata_scoped(
            sprint_id=selected.sprint_id,
            archive_id=selected.archive_id,
            cancellation=cancellation,
        )

    def read_manifest(self, selected: PromptArchiveRead) -> dict[str, Any]:
        verified = self._validated_read(selected)
        return _copy_document(verified.manifest, maximum=_MANIFEST_MAX_BYTES)

    def hydrate_content(
        self,
        selected: PromptArchiveRead,
        *,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> bytes:
        if cancellation is not None:
            cancellation.check()
        verified = self._validated_read(selected, cancellation=cancellation)
        if cancellation is not None:
            cancellation.check()
        storage = self._require_storage()
        return PromptArchiveIncremental(
            storage,
            self.project_id,
            verified.sprint_id,
            cancellation=cancellation,
        ).hydrate_archive_bytes(
            verified.pointer["manifest_ref"],
            max_output_bytes=self.max_hydrated_bytes,
        )

    def hydrate_state_view(
        self,
        *,
        sprint_id: str,
        archive_id: str,
        max_output_bytes: int | None = None,
        cancellation: PromptArchiveCancellationToken | None = None,
    ) -> bytes:
        """Hydrate a bounded root-to-head authoritative PAS1 state view.

        This is an explicit cold-path seam.  HTTP adapters must complete their
        operator or phone-specific managed authorization and admission before
        invoking it; project/sprint/archive identifiers are not capabilities.
        Keeping the implementation in a lazily imported module also prevents
        identity, active-task, publication, and ``.prompt`` paths from loading
        or accidentally invoking the full-chain reader.
        """

        from nginx_qa.prompt_archive_state_view import (
            hydrate_prompt_archive_state_view,
        )

        return hydrate_prompt_archive_state_view(
            self,
            sprint_id=sprint_id,
            archive_id=archive_id,
            max_output_bytes=max_output_bytes,
            cancellation=cancellation,
        )

    def read_inline_or_archive(
        self,
        raw_bytes: bytes | bytearray | memoryview,
        *,
        authorize_archive: Callable[[str, str, str], bool],
    ) -> bytes:
        """Read legacy inline bytes or an authorized descriptor stub.

        The authorization callback is evaluated before locator/filesystem
        lookup.  Unknown top-level kinds fail closed.
        """

        raw = _freeze_bytes(raw_bytes, maximum=self.max_hydrated_bytes)
        try:
            top_level = json.loads(
                raw.decode("utf-8", errors="strict"),
                object_pairs_hook=_JsonObjectPairs,
            )
        except (
            UnicodeError,
            json.JSONDecodeError,
            ValueError,
            RecursionError,
            MemoryError,
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if not isinstance(top_level, _JsonObjectPairs):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        kind_members = [value for key, value in top_level if key == "kind"]
        if len(kind_members) > 1:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if not kind_members:
            return raw
        kind = kind_members[0]
        if kind != "prompt_archive_descriptor_stub":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        # A descriptor stub is a PAS1 control object, unlike an arbitrary
        # legacy response.  Apply the full duplicate/canonical grammar only
        # after the unique top-level discriminator selected that branch.
        value = parse_canonical_json_v1(
            raw,
            maximum=min(self.max_hydrated_bytes, _CONTROL_MAX_BYTES),
        )
        stub = self._validate_stub(value)
        scope = stub["scope"]
        if not callable(authorize_archive):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        try:
            allowed = authorize_archive(
                scope["project_id"],
                scope["sprint_id"],
                stub["archive_id"],
            )
        except PromptArchiveStorageError:
            raise
        except Exception:
            raise _error("PROMPT_ARCHIVE_NOT_FOUND") from None
        if allowed is not True:
            raise _error("PROMPT_ARCHIVE_NOT_FOUND")
        selected = self.lookup_archive(
            sprint_id=scope["sprint_id"],
            archive_id=stub["archive_id"],
        )
        if (
            stub["pointer_id"] != selected.pointer["pointer_id"]
            or stub["manifest_ref"] != selected.pointer["manifest_ref"]
            or stub["archive_format"] != selected.archive_format
            or stub["scope"] != selected.scope
            or stub["integrity"] != selected.integrity
            or stub["created_at"] != selected.created_at
            or stub["published_at"] != selected.pointer["published_at"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        hydrated = self.hydrate_content(selected)
        try:
            hydrated_top_level = json.loads(
                hydrated.decode("utf-8", errors="strict"),
                object_pairs_hook=_JsonObjectPairs,
            )
        except (
            UnicodeError,
            json.JSONDecodeError,
            ValueError,
            RecursionError,
            MemoryError,
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if not isinstance(hydrated_top_level, _JsonObjectPairs):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        hydrated_kinds = [
            member for key, member in hydrated_top_level if key == "kind"
        ]
        if len(hydrated_kinds) > 1:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if not hydrated_kinds:
            # Compatibility for PAS1 artifacts created before the bounded
            # occurrence-document wrapper was introduced.  The enclosing stub,
            # pointer, locator, and manifest were already fully cross-checked.
            return hydrated
        if hydrated_kinds[0] != "prompt_archive_occurrence_document":
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        dispatch_prompt_archive_response(
            raw,
            archive_reader=(
                lambda *, descriptor_stub, maximum_bytes: hydrated
            ),
            maximum_bytes=self.max_hydrated_bytes,
        )
        return hydrated

    def _validate_stub(self, value: Mapping[str, Any]) -> dict[str, Any]:
        stub = dict(value)
        if set(stub) != {
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
        } or (
            stub.get("schema_version") != 1
            or stub.get("kind") != "prompt_archive_descriptor_stub"
            or stub.get("media_type") != _STUB_MEDIA
            or stub.get("id_algorithm") != "PAS1-ID-v1"
            or type(stub.get("stub_id")) is not str
            or _STUB_ID_RE.fullmatch(stub["stub_id"]) is None
            or type(stub.get("archive_id")) is not str
            or _ARCHIVE_ID_RE.fullmatch(stub["archive_id"]) is None
            or type(stub.get("pointer_id")) is not str
            or _POINTER_ID_RE.fullmatch(stub["pointer_id"]) is None
            or type(stub.get("scope")) is not dict
            or type(stub.get("integrity")) is not dict
            or type(stub.get("manifest_ref")) is not dict
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        scope = stub["scope"]
        if (
            scope.get("project_id") != self.project_id
            or type(scope.get("sprint_id")) is not str
            or _SAFE_ID_RE.fullmatch(scope["sprint_id"]) is None
            or type(scope.get("response_id")) is not str
            or _RESPONSE_ID_RE.fullmatch(scope["response_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_NOT_FOUND")
        created_at = _require_rfc3339(stub.get("created_at"))
        published_at = _require_rfc3339(stub.get("published_at"))
        _require_ref_kind(
            stub["manifest_ref"],
            logical_kind="manifest",
            media_type=_MANIFEST_MEDIA,
        )
        manifest_digest = stub["manifest_ref"].get("digest")
        if type(manifest_digest) is not str or _SHA256_RE.fullmatch(manifest_digest) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        expected = "parstub1-" + pas1_id_v1(
            "descriptor-stub-id",
            (
                ("project_id", self.project_id),
                ("sprint_id", scope["sprint_id"]),
                ("response_id", scope["response_id"]),
                ("archive_id", stub["archive_id"]),
                ("pointer_id", stub["pointer_id"]),
                ("manifest_sha256", manifest_digest),
                ("created_at", created_at),
                ("published_at", published_at),
            ),
        )
        if stub["stub_id"] != expected:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return stub


class PromptArchiveRuntimeRegistry:
    """Thread-safe multi-project factory suitable for ``host`` registration."""

    def __init__(
        self,
        config: PromptArchiveRuntimeConfig,
        *,
        occurrence_prover: PromptArchiveOccurrenceProver | None = None,
        compatibility_resolver: Callable[
            [str, Mapping[str, Any]], PromptArchiveCompatibilityPublisher | None
        ]
        | None = None,
        metrics: PromptArchiveMetrics | None = None,
    ) -> None:
        if not isinstance(config, PromptArchiveRuntimeConfig):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if occurrence_prover is not None and (
            not callable(getattr(occurrence_prover, "register_occurrence", None))
            or not callable(getattr(occurrence_prover, "load_occurrence", None))
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if metrics is not None and not isinstance(metrics, PromptArchiveMetrics):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if compatibility_resolver is not None and not callable(
            compatibility_resolver
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        self.config = config
        self._occurrence_prover = occurrence_prover
        self._compatibility_resolver = compatibility_resolver
        self.metrics = metrics if metrics is not None else PromptArchiveMetrics()
        self._lock = threading.Lock()
        self._projects: dict[str, PromptArchiveRuntime] = {}

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        protected_roots: Sequence[str | os.PathLike[str]] = (),
        occurrence_prover: PromptArchiveOccurrenceProver | None = None,
        compatibility_resolver: Callable[
            [str, Mapping[str, Any]], PromptArchiveCompatibilityPublisher | None
        ]
        | None = None,
        metrics: PromptArchiveMetrics | None = None,
    ) -> "PromptArchiveRuntimeRegistry":
        return cls(
            PromptArchiveRuntimeConfig.from_environment(
                environ,
                protected_roots=protected_roots,
            ),
            occurrence_prover=occurrence_prover,
            compatibility_resolver=compatibility_resolver,
            metrics=metrics,
        )

    def for_project(self, project_id: str) -> PromptArchiveRuntime:
        project_id = _require_safe_id(project_id)
        with self._lock:
            existing = self._projects.get(project_id)
            if existing is not None:
                return existing
            if self.config.root is None or (
                self.config.mode == "legacy" and not self.config.root.exists()
            ):
                storage = None
            else:
                storage = PromptArchiveStorage(
                    self.config.root,
                    project_id,
                    protected_roots=self.config.protected_roots,
                    limits=PromptArchiveLimits(
                        max_hydrated_bytes=self.config.max_hydrated_bytes,
                    ),
                )
            runtime = PromptArchiveRuntime(
                self.config,
                project_id,
                storage,
                occurrence_prover=self._occurrence_prover,
                compatibility_resolver=self._compatibility_resolver,
                metrics=self.metrics,
            )
            self._projects[project_id] = runtime
            return runtime

    def validate_and_recover(self, project_ids: Sequence[str] = ()) -> int:
        """Validate the configured root and replay fixed-shard pending work.

        The caller supplies durable project ids from its registry.  Recovery
        never discovers work by walking operation directories; each project
        runtime enumerates exactly the 256 specified shard-current targets.
        This method is intended for process lifespan startup before legacy
        response writes are admitted.
        """

        if isinstance(project_ids, (str, bytes)) or not isinstance(
            project_ids, Sequence
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        frozen_ids = tuple(_require_safe_id(project_id) for project_id in project_ids)
        if len(set(frozen_ids)) != len(frozen_ids):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if self.config.mode == "legacy":
            return 0
        if self.config.mode not in {"shadow", "authoritative"} or self.config.root is None:
            raise _error("PROMPT_ARCHIVE_VERSION_UNSUPPORTED")
        # Even an empty project registry must preflight/create the configured
        # root and prove protected-root separation before the server starts.
        if not frozen_ids:
            PromptArchiveStorage(
                self.config.root,
                "prompt-archive-startup-validation",
                protected_roots=self.config.protected_roots,
                limits=PromptArchiveLimits(
                    max_hydrated_bytes=self.config.max_hydrated_bytes,
                ),
            )
            return 0
        recovered = 0
        for project_id in frozen_ids:
            recovered += self.for_project(project_id).recover_pending()
        return recovered


__all__ = [
    "PromptArchivePublication",
    "PromptArchiveRead",
    "PromptArchiveRuntime",
    "PromptArchiveRuntimeConfig",
    "PromptArchiveOccurrenceProver",
    "PromptArchiveCompatibilityPublisher",
    "PromptArchiveRuntimeRegistry",
]
