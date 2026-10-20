"""Crash-safe authority for the external ``latest-response.json`` file.

The archive root is intentionally *not* an input to this module.  Every
process which can write the compatibility file derives the same sibling
sidecar from the canonical external target and serializes through its one
``latest.lock`` file.

Only closed, content-derived names are used below the sidecar.  ``intent.json``
and ``slot.json`` are fixed recovery roots; recovery never enumerates intent,
content, slot, or operation directories.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Literal

from .prompt_archive_incremental import pas1_id_v1
from .prompt_archive_storage import (
    ImmutablePublicationReceipt,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    _bounded_bytes,
    _path_entry_is_redirected,
    _storage_temporary_name,
    _windows_lock_descriptor,
    canonical_json_bytes_v1,
    parse_canonical_json_v1,
    resolved_path_from_open_descriptor,
)


_MIB: Final = 1024 * 1024
_CONTROL_MAX_BYTES: Final = 8 * _MIB
_CONTENT_MAX_BYTES: Final = 512 * _MIB
_MAX_REVISION: Final = 9_223_372_036_854_775_807
_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$", re.ASCII)
_OPERATION_ID_RE: Final = re.compile(r"^parop1-[0-9a-f]{32}$", re.ASCII)
_RESPONSE_ID_RE: Final = re.compile(r"^parresp1-[0-9a-f]{64}$", re.ASCII)
_INTENT_ID_RE: Final = re.compile(r"^parlatestintent1-[0-9a-f]{64}$", re.ASCII)
_PLAN_ID_RE: Final = re.compile(r"^parlatestplan1-[0-9a-f]{64}$", re.ASCII)
_SLOT_ID_RE: Final = re.compile(r"^parlatest1-[0-9a-f]{64}$", re.ASCII)
_TARGET_AUTHORITY_ID_RE: Final = re.compile(
    r"^parlatestauthority1-[0-9a-f]{64}$",
    re.ASCII,
)
_ARCHIVE_ID_RE: Final = re.compile(r"^par1-[0-9a-f]{64}$", re.ASCII)
_POINTER_ID_RE: Final = re.compile(r"^parptr1-[0-9a-f]{64}$", re.ASCII)
_LOCATOR_ID_RE: Final = re.compile(r"^pararc1-[0-9a-f]{64}$", re.ASCII)
_RECOVERY_KEY_RE: Final = re.compile(r"^parloc1-[0-9a-f]{64}$", re.ASCII)
_DESCRIPTOR_STUB_ID_RE: Final = re.compile(r"^parstub1-[0-9a-f]{64}$", re.ASCII)
_PENDING_ID_RE: Final = re.compile(r"^parpending1-[0-9a-f]{64}$", re.ASCII)
_SHARD_ID_RE: Final = re.compile(r"^[0-9a-f]{2}$", re.ASCII)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_RFC3339_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$",
    re.ASCII,
)
_REPARSE_POINT: Final = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)

_REVISION_AUTHORITY: Final[dict[str, Any]] = {
    "protocol": "prompt-archive-durable-project-latest-revision-v1",
    "epoch": 1,
    "reset_policy": "restore_high_water_mark_or_fail_closed",
}
_BOOTSTRAP_BASE: Final[dict[str, Any]] = {
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
_ARCHIVE_OWNER_PROTOCOL: Final = "prompt-archive-latest-archive-owner-v1"
_PLAN_MEDIA_TYPE: Final = (
    "application/vnd.nginx-qa.prompt-archive-external-latest-plan-v1+json"
)
_JOURNAL_MEDIA_TYPE: Final = (
    "application/vnd.nginx-qa.prompt-archive-external-latest-journal-v1+json"
)
_PENDING_MEDIA_TYPE: Final = (
    "application/vnd.nginx-qa.prompt-archive-pending-operation-registry-v1+json"
)
_PENDING_SHARD_COUNT: Final = 256
_PENDING_SHARD_MAX_ENTRIES: Final = 1024
_RESPONSE_KINDS: Final = frozenset(
    {
        "identity-response",
        "transition-response",
        "state-response",
        "migration-snapshot",
    }
)
_LEGACY_LATEST_OPERATION_KIND: Final = "legacy_latest_publish"
_LEGACY_LATEST_WRITE_KINDS: Final = frozenset(
    {"transition", "identity", "state", "rollback"}
)

FAULT_AFTER_CONTENT: Final = "after_content_publish"
FAULT_AFTER_OPERATION_PLAN: Final = "after_operation_plan_publish"
FAULT_AFTER_PENDING_BIND: Final = "after_pending_registry_bind"
FAULT_AFTER_CURRENT_INTENT: Final = "after_current_intent_publish"
FAULT_AFTER_CURRENT_INSTALL: Final = "after_current_plan_install"
FAULT_AFTER_TRANSITION_STUB: Final = "after_transition_stub_publish"
FAULT_AFTER_EXTERNAL_CONTENT: Final = "after_external_content_publish"
FAULT_AFTER_RETAINED_SLOT: Final = "after_retained_slot_publish"
FAULT_AFTER_CURRENT_SLOT: Final = "after_current_slot_publish"
FAULT_AFTER_TERMINAL: Final = "after_terminal_journal_publish"
FAULT_AFTER_TERMINAL_BEFORE_PENDING_REMOVE: Final = (
    "after_terminal_before_pending_remove"
)
FAULT_STAGES: Final[tuple[str, ...]] = (
    FAULT_AFTER_CONTENT,
    FAULT_AFTER_OPERATION_PLAN,
    FAULT_AFTER_PENDING_BIND,
    FAULT_AFTER_CURRENT_INTENT,
    FAULT_AFTER_CURRENT_INSTALL,
    FAULT_AFTER_TRANSITION_STUB,
    FAULT_AFTER_EXTERNAL_CONTENT,
    FAULT_AFTER_RETAINED_SLOT,
    FAULT_AFTER_CURRENT_SLOT,
    FAULT_AFTER_TERMINAL,
    FAULT_AFTER_TERMINAL_BEFORE_PENDING_REMOVE,
)


def _error(code: str) -> PromptArchiveStorageError:
    return PromptArchiveStorageError(code)


class _AuthorityStorage(PromptArchiveStorage):
    """Use the hardened storage primitives with an authority root directly."""

    def __init__(self, root: Path) -> None:
        super().__init__(
            root,
            "external-latest-authority",
            protected_roots=(),
            fault_hook=None,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class ExternalLatestResult:
    """Result of one ordered latest publication or exact replay."""

    outcome: Literal["published", "idempotent", "superseded"]
    intent_id: str
    latest_slot_id: str
    latest_slot_ref: Mapping[str, Any]
    source_revision: int
    generation: int
    content_sha256: str
    content_size: int
    recovered: bool = False


def _safe_leaf_name(name: str) -> None:
    if (
        type(name) is not str
        or not name
        or len(name) > 255
        or name in {".", ".."}
        or name[-1:] in {".", " "}
        or ":" in name
        or "/" in name
        or "\\" in name
        or "\x00" in name
    ):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")


def _assert_non_redirected_ancestors(directory: Path) -> None:
    """Reject a lexical parent chain containing symlinks or reparse points."""

    try:
        current = Path(directory.anchor)
        parts = directory.parts[1:] if directory.anchor else directory.parts
        for component in parts:
            current = current / component
            metadata = current.lstat()
            if (
                stat.S_ISLNK(metadata.st_mode)
                or bool(
                    _REPARSE_POINT
                    and getattr(metadata, "st_file_attributes", 0) & _REPARSE_POINT
                )
                or not stat.S_ISDIR(metadata.st_mode)
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    except PromptArchiveStorageError:
        raise
    except (OSError, ValueError):
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None


def _resolved_external_leaf(
    value: str | os.PathLike[str],
    *,
    absent_basename: str | None,
) -> tuple[Path, str]:
    """Return a canonical parent and unique actual directory-entry spelling."""

    try:
        raw = Path(value)
    except (TypeError, ValueError):
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
    if not raw.is_absolute():
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    raw_text = os.fspath(raw)
    if "\x00" in raw_text or os.path.normpath(raw_text) != raw_text:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    _safe_leaf_name(raw.name)
    _assert_non_redirected_ancestors(raw.parent)
    try:
        parent = raw.parent.resolve(strict=True)
    except OSError:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
    _assert_non_redirected_ancestors(parent)

    # ``exists`` is false for a dangling link, which is still an unsafe entry.
    try:
        redirected = _path_entry_is_redirected(raw)
        exists = raw.exists()
    except OSError:
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
    if redirected:
        raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if not exists:
        if absent_basename is not None and raw.name != absent_basename:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return parent, raw.name

    descriptor = -1
    try:
        descriptor = os.open(
            raw,
            os.O_RDONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        resolved = resolved_path_from_open_descriptor(descriptor)
        if resolved is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        resolved = resolved.resolve(strict=True)
        if os.path.normcase(str(resolved.parent)) != os.path.normcase(str(parent)):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        actual_name = resolved.name
        _safe_leaf_name(actual_name)
        actual = parent / actual_name
        actual_metadata = actual.lstat()
        if (
            _path_entry_is_redirected(actual)
            or not stat.S_ISREG(actual_metadata.st_mode)
            or actual_metadata.st_nlink != 1
            or (actual_metadata.st_dev, actual_metadata.st_ino)
            != (metadata.st_dev, metadata.st_ino)
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return parent, actual_name
    except PromptArchiveStorageError:
        raise
    except (OSError, ValueError):
        raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
    finally:
        if descriptor != -1:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _read_optional(
    storage: _AuthorityStorage,
    path: Path,
    *,
    maximum: int,
) -> bytes | None:
    try:
        return storage._read_regular_file(
            path,
            maximum=maximum,
            missing_code="PROMPT_ARCHIVE_NOT_FOUND",
        )
    except PromptArchiveStorageError as exc:
        if exc.code == "PROMPT_ARCHIVE_NOT_FOUND":
            return None
        raise


def _publish_immutable(
    storage: _AuthorityStorage,
    path: Path,
    payload: bytes,
) -> bool:
    """Exclusive durable create or exact-byte replay."""

    storage._ensure_directory(path.parent)
    try:
        existing = _read_optional(storage, path, maximum=len(payload))
    except PromptArchiveStorageError as exc:
        if exc.code == "PROMPT_ARCHIVE_LIMIT_EXCEEDED":
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT") from None
        raise
    if existing is not None:
        if existing != payload:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        return False
    created = storage._publish_immutable_no_replace(path, payload)
    if not created:
        try:
            existing = _read_optional(storage, path, maximum=len(payload))
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_LIMIT_EXCEEDED":
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT") from None
            raise
        if existing != payload:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
    verified = _read_optional(storage, path, maximum=len(payload))
    if verified != payload:
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    return created


def _replace_exact(
    storage: _AuthorityStorage,
    path: Path,
    payload: bytes,
    *,
    existing_maximum: int = _CONTROL_MAX_BYTES,
) -> bool:
    """Atomically replace one fixed leaf, flush it, and verify exact bytes."""

    storage._ensure_directory(path.parent)
    existing = _read_optional(
        storage,
        path,
        maximum=max(len(payload), existing_maximum),
    )
    if existing == payload:
        return False
    temporary_name = _storage_temporary_name(path.name)
    descriptor = -1
    replaced = False
    with storage._open_pinned_directory(path.parent) as parent:
        try:
            # Opening the old entry is a security check even though replacement
            # itself does not modify the old inode.
            old = storage._open_relative_control_file(
                parent,
                path,
                create=False,
                writable=False,
            )
            if old is not None:
                os.close(old)
            descriptor = storage._create_relative_temp(parent, temporary_name)
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            observed = storage._read_open_descriptor(descriptor, maximum=len(payload))
            if observed != payload:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            storage._validate_pinned_directory(parent, path.parent)
            storage._replace_relative_control_file(
                descriptor,
                parent,
                temporary_name,
                path.name,
            )
            replaced = True
            storage._assert_relative_control_handle(
                descriptor,
                parent=parent,
                target=path,
            )
            storage.directory_sync_supported = (
                storage._sync_pinned_directory(parent)
                and storage.directory_sync_supported
            )
            os.close(descriptor)
            descriptor = -1
        except PromptArchiveStorageError:
            raise
        except OSError:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        finally:
            if descriptor != -1:
                if not replaced:
                    storage._delete_relative_temp(parent, temporary_name, descriptor)
                try:
                    os.close(descriptor)
                except OSError:
                    pass
    verified = _read_optional(storage, path, maximum=len(payload))
    if verified != payload:
        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    return True


def publish_authoritative_transition_stub(
    path: str | os.PathLike[str],
    content: bytes | bytearray | memoryview,
    *,
    maximum_bytes: int = _CONTROL_MAX_BYTES,
) -> ImmutablePublicationReceipt:
    """Durably create an authoritative transition stub or verify exact replay.

    The helper never overwrites an existing entry.  A byte-identical entry is
    idempotent; a divergent entry, link, reparse point, or non-regular file
    fails with a payload-safe storage error.
    """

    if type(maximum_bytes) is not int or not 1 <= maximum_bytes <= _CONTROL_MAX_BYTES:
        raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
    payload = _bounded_bytes(content, maximum_bytes)
    parent, name = _resolved_external_leaf(path, absent_basename=None)
    storage = _AuthorityStorage(parent)
    target = parent / name
    created = _publish_immutable(storage, target, payload)
    return ImmutablePublicationReceipt(
        path=target,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_size=len(payload),
        created=created,
    )


class ExternalLatestCoordinator:
    """Serialize, publish, and recover one external latest-response authority."""

    def __init__(
        self,
        external_latest: str | os.PathLike[str],
        *,
        project_id: str,
        fault_hook: Callable[[str], None] | None = None,
        archive_owner_verifier: Callable[
            [Mapping[str, Any], Mapping[str, Any]], bool
        ]
        | None = None,
        max_content_bytes: int = _CONTENT_MAX_BYTES,
    ) -> None:
        if (
            type(project_id) is not str
            or _SAFE_ID_RE.fullmatch(project_id) is None
            or (fault_hook is not None and not callable(fault_hook))
            or (
                archive_owner_verifier is not None
                and not callable(archive_owner_verifier)
            )
            or type(max_content_bytes) is not int
            or not 1 <= max_content_bytes <= _CONTENT_MAX_BYTES
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        parent, name = _resolved_external_leaf(
            external_latest,
            absent_basename="latest-response.json",
        )
        if name.casefold() != "latest-response.json":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self.project_id = project_id
        self.external_latest = parent / name
        self._target_authority_id = self._target_authority_id_from_parent(parent)
        self.sidecar = parent / f"{name}.nginx-qa-v1"
        self._fault_hook = fault_hook
        self._archive_owner_verifier = archive_owner_verifier
        self.max_content_bytes = max_content_bytes
        self._parent_storage = _AuthorityStorage(parent)
        self._sidecar_storage = _AuthorityStorage(self.sidecar)
        self.latest_lock_path = self.sidecar / "latest.lock"

    @staticmethod
    def _target_authority_id_from_parent(parent: Path) -> str:
        return "parlatestauthority1-" + pas1_id_v1(
            "external-latest-target-authority-id",
            (
                ("canonical_parent", os.path.normcase(str(parent))),
                ("normative_leaf", "latest-response.json"),
            ),
        )

    @classmethod
    def target_authority_id_for_path(
        cls,
        external_latest: str | os.PathLike[str],
    ) -> str:
        """Return the canonical target identity without creating a sidecar."""

        parent, name = _resolved_external_leaf(
            external_latest,
            absent_basename="latest-response.json",
        )
        if name.casefold() != "latest-response.json":
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return cls._target_authority_id_from_parent(parent)

    @property
    def target_authority_id(self) -> str:
        """Path-free identity of this canonical external-latest authority."""

        return self._target_authority_id

    def _fault(self, stage: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage)

    @contextmanager
    def _control_lock(self, lock_path: Path):
        """Hold one stable sidecar control lock without following redirects."""

        storage = self._sidecar_storage
        descriptor = -1
        storage._ensure_directory(lock_path.parent)
        with storage._open_pinned_directory(lock_path.parent) as parent:
            try:
                descriptor = storage._open_relative_control_file(
                    parent,
                    lock_path,
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
                    storage._sync_pinned_directory(parent)
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    _windows_lock_descriptor(descriptor, unlock=False)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                try:
                    storage._assert_relative_control_handle(
                        descriptor,
                        parent=parent,
                        target=lock_path,
                        compare_size=False,
                    )
                    yield
                    storage._assert_relative_control_handle(
                        descriptor,
                        parent=parent,
                        target=lock_path,
                        compare_size=False,
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

    @contextmanager
    def _latest_lock(self):
        with self._control_lock(self.latest_lock_path):
            yield

    @contextmanager
    def _operation_lock(self, plan: Mapping[str, Any]):
        with self._operation_lock_ids(
            plan["recovery_response_id"],
            plan["operation_id"],
        ):
            yield

    @contextmanager
    def _operation_lock_ids(self, response_id: str, operation_id: str):
        if (
            type(response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(response_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        path = (
            self.sidecar
            / "operations"
            / "v1"
            / response_id
            / operation_id
            / "operation.lock"
        )
        with self._control_lock(path):
            yield

    @contextmanager
    def _pending_shard_lock(self, shard_id: str):
        if type(shard_id) is not str or _SHARD_ID_RE.fullmatch(shard_id) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        path = self.sidecar / "pending" / "v1" / "locks" / f"{shard_id}.lock"
        with self._control_lock(path):
            yield

    @staticmethod
    def _validate_timestamp(value: Any) -> str:
        if type(value) is not str or _RFC3339_RE.fullmatch(value) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        try:
            parsed = datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )
        except (ValueError, OverflowError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return value

    def _validate_authoritative_drain_barrier(
        self,
        value: Any,
    ) -> dict[str, Any]:
        keys = {
            "protocol",
            "lock_scope",
            "slot_id",
            "registry_shard_count",
            "observed_incomplete_operation_ids",
            "observed_at",
        }
        if type(value) is not dict or set(value) != keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value["protocol"]
            != "reconcile-authoritative-before-legacy-latest-write-v1"
            or value["lock_scope"] != "project"
            or value["slot_id"] != "latest-response"
            or value["registry_shard_count"] != _PENDING_SHARD_COUNT
            or value["observed_incomplete_operation_ids"] != []
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._validate_timestamp(value["observed_at"])
        return value

    def _authoritative_drain_barrier(self, observed_at: str) -> dict[str, Any]:
        return {
            "protocol": "reconcile-authoritative-before-legacy-latest-write-v1",
            "lock_scope": "project",
            "slot_id": "latest-response",
            "registry_shard_count": _PENDING_SHARD_COUNT,
            "observed_incomplete_operation_ids": [],
            "observed_at": self._validate_timestamp(observed_at),
        }

    @staticmethod
    def _validate_json_content(payload: bytes) -> None:
        class DuplicateMember(ValueError):
            pass

        def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in values:
                if key in result:
                    raise DuplicateMember
                result[key] = value
            return result

        def reject_constant(_value: str) -> Any:
            raise DuplicateMember

        try:
            value = json.loads(
                payload.decode("utf-8", errors="strict"),
                object_pairs_hook=pairs,
                parse_constant=reject_constant,
            )
        except (UnicodeError, json.JSONDecodeError, DuplicateMember, RecursionError):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID") from None
        except MemoryError:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED") from None
        if type(value) is not dict:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

    def _freeze_descriptor_ref(
        self,
        descriptor_ref: Mapping[str, Any] | None,
        *,
        content_kind: str,
        content_sha256: str,
        content_size: int,
    ) -> dict[str, Any] | None:
        if content_kind == "legacy_inline":
            if descriptor_ref is not None:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            return None
        if content_kind != "descriptor_stub" or not isinstance(descriptor_ref, Mapping):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        validated = self._sidecar_storage._validate_ref(descriptor_ref)
        if (
            validated["logical_kind"] != "descriptor_stub"
            or validated["media_type"]
            != "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
            or validated["digest"] != content_sha256
            or validated["uncompressed_size"] != content_size
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        raw = canonical_json_bytes_v1(validated, maximum=_CONTROL_MAX_BYTES)
        return parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES)

    @staticmethod
    def _bootstrap(
        kind: str,
        *,
        digest: str | None = None,
        size: int | None = None,
    ) -> dict[str, Any]:
        result = dict(_BOOTSTRAP_BASE)
        result["kind"] = kind
        if kind == "legacy_bytes":
            assert digest is not None and size is not None
            result["sha256"] = digest
            result["byte_size"] = size
        return result

    def _observe_external(self) -> tuple[dict[str, Any], bytes | None]:
        observed = _read_optional(
            self._parent_storage,
            self.external_latest,
            maximum=self.max_content_bytes,
        )
        if observed is None:
            return self._bootstrap("absent"), None
        return (
            self._bootstrap(
                "legacy_bytes",
                digest=hashlib.sha256(observed).hexdigest(),
                size=len(observed),
            ),
            observed,
        )

    def _transition_stub_leaf(
        self,
        value: str | os.PathLike[str],
    ) -> str:
        """Resolve one transition target to a safe colocated leaf.

        Plans deliberately retain only the leaf.  The canonical parent is the
        coordinator's already-pinned external-latest authority, so a replay
        cannot redirect publication by smuggling an absolute path into durable
        intent state.
        """

        parent, name = _resolved_external_leaf(value, absent_basename=None)
        if (
            os.path.normcase(str(parent))
            != os.path.normcase(str(self.external_latest.parent))
            or name.casefold() == self.external_latest.name.casefold()
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return name

    def _validate_archive_owner(
        self,
        value: Any,
        *,
        intent: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Validate the closed path-free binding to the owning archive plan."""

        if value is None:
            if intent["content_kind"] == "legacy_inline":
                return None
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        keys = {
            "protocol",
            "project_id",
            "sprint_id",
            "response_id",
            "operation_id",
            "recovery_key",
            "outer_plan_identity_sha256",
            "archive_id",
            "pointer_id",
            "locator_id",
            "target_authority_id",
            "response_kind",
            "transition_stub_name",
            "descriptor_stub_id",
        }
        if type(value) is not dict or set(value) != keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        owner = parse_canonical_json_v1(
            canonical_json_bytes_v1(value, maximum=_CONTROL_MAX_BYTES),
            maximum=_CONTROL_MAX_BYTES,
        )
        ordering = intent["ordering"]
        try:
            _safe_leaf_name(owner["transition_stub_name"])
        except (KeyError, PromptArchiveStorageError):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID") from None
        if (
            intent["content_kind"] != "descriptor_stub"
            or owner["protocol"] != _ARCHIVE_OWNER_PROTOCOL
            or owner["project_id"] != self.project_id
            or owner["project_id"] != intent["project_id"]
            or type(owner["sprint_id"]) is not str
            or _SAFE_ID_RE.fullmatch(owner["sprint_id"]) is None
            or owner["sprint_id"] != intent["source_sprint_id"]
            or type(owner["response_id"]) is not str
            or _RESPONSE_ID_RE.fullmatch(owner["response_id"]) is None
            or owner["response_id"] != intent["source_response_id"]
            or type(owner["operation_id"]) is not str
            or _OPERATION_ID_RE.fullmatch(owner["operation_id"]) is None
            or owner["operation_id"] != ordering["operation_id"]
            or type(owner["recovery_key"]) is not str
            or _RECOVERY_KEY_RE.fullmatch(owner["recovery_key"]) is None
            or type(owner["outer_plan_identity_sha256"]) is not str
            or _SHA256_RE.fullmatch(owner["outer_plan_identity_sha256"]) is None
            or type(owner["archive_id"]) is not str
            or _ARCHIVE_ID_RE.fullmatch(owner["archive_id"]) is None
            or type(owner["pointer_id"]) is not str
            or _POINTER_ID_RE.fullmatch(owner["pointer_id"]) is None
            or type(owner["locator_id"]) is not str
            or _LOCATOR_ID_RE.fullmatch(owner["locator_id"]) is None
            or type(owner["target_authority_id"]) is not str
            or _TARGET_AUTHORITY_ID_RE.fullmatch(owner["target_authority_id"])
            is None
            or owner["target_authority_id"] != self.target_authority_id
            or type(owner["response_kind"]) is not str
            or owner["response_kind"] not in _RESPONSE_KINDS
            or owner["transition_stub_name"].casefold()
            == self.external_latest.name.casefold()
            or type(owner["descriptor_stub_id"]) is not str
            or _DESCRIPTOR_STUB_ID_RE.fullmatch(owner["descriptor_stub_id"])
            is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        identity_projection = {
            key: owner[key]
            for key in (
                "project_id",
                "sprint_id",
                "response_id",
                "operation_id",
                "recovery_key",
                "archive_id",
                "pointer_id",
                "locator_id",
                "target_authority_id",
                "response_kind",
                "transition_stub_name",
                "descriptor_stub_id",
            )
        }
        expected_identity = hashlib.sha256(
            canonical_json_bytes_v1(
                identity_projection,
                maximum=_CONTROL_MAX_BYTES,
            )
        ).hexdigest()
        if owner["outer_plan_identity_sha256"] != expected_identity:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return owner

    def _verify_archive_owner(self, plan: Mapping[str, Any]) -> None:
        """Re-prove a descriptor plan's durable owner before any mutation."""

        intent = plan["latest_intent"]
        if intent["content_kind"] == "legacy_inline":
            return
        owner = plan["archive_owner"]
        # The plan was structurally validated already.  Re-run the closed
        # relation check here before delegating to the durable archive owner.
        validated = self._validate_archive_owner(owner, intent=intent)
        assert validated is not None
        verifier = self._archive_owner_verifier
        if verifier is None:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        try:
            accepted = verifier(
                parse_canonical_json_v1(
                    canonical_json_bytes_v1(validated, maximum=_CONTROL_MAX_BYTES),
                    maximum=_CONTROL_MAX_BYTES,
                ),
                parse_canonical_json_v1(
                    canonical_json_bytes_v1(intent, maximum=_CONTROL_MAX_BYTES),
                    maximum=_CONTROL_MAX_BYTES,
                ),
            )
        except PromptArchiveStorageError:
            raise
        except Exception:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if accepted is not True:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")

    @staticmethod
    def _intent_fields(intent: Mapping[str, Any]) -> tuple[tuple[str, str | None], ...]:
        ordering = intent["ordering"]
        authority = ordering["revision_authority"]
        bootstrap = intent["bootstrap_precondition"]
        descriptor = intent["descriptor_ref"]
        fields = (
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
                None if bootstrap.get("byte_size") is None else str(bootstrap["byte_size"]),
            ),
            ("content_kind", intent["content_kind"]),
            ("content_sha256", intent["content_sha256"]),
            ("content_size", str(intent["content_size"])),
            (
                "descriptor_digest",
                None if descriptor is None else descriptor["digest"],
            ),
            ("source_sprint_id", intent["source_sprint_id"]),
            ("source_response_id", intent["source_response_id"]),
            ("published_at", intent["published_at"]),
        )
        return fields

    @classmethod
    def _intent_id(cls, intent: Mapping[str, Any]) -> str:
        return "parlatestintent1-" + pas1_id_v1(
            "latest-content-intent-id",
            cls._intent_fields(intent),
        )

    @staticmethod
    def _slot_fields(slot: Mapping[str, Any]) -> tuple[tuple[str, str | None], ...]:
        ordering = slot["ordering"]
        authority = ordering["revision_authority"]
        descriptor = slot["descriptor_ref"]
        fields = (
            ("project_id", slot["project_id"]),
            ("slot_id", slot["slot_id"]),
            ("intent_id", slot["intent_id"]),
            ("revision_protocol", authority["protocol"]),
            ("revision_epoch", str(authority["epoch"])),
            ("revision_reset_policy", authority["reset_policy"]),
            ("source_revision", str(ordering["source_revision"])),
            ("occurrence_id", ordering["occurrence_id"]),
            ("operation_id", ordering["operation_id"]),
            ("generation", str(slot["generation"])),
            ("previous_slot_id", slot["previous_slot_id"]),
            ("content_kind", slot["content_kind"]),
            ("content_sha256", slot["content_sha256"]),
            ("content_size", str(slot["content_size"])),
            (
                "descriptor_digest",
                None if descriptor is None else descriptor["digest"],
            ),
            ("source_sprint_id", slot["source_sprint_id"]),
            ("source_response_id", slot["source_response_id"]),
            ("published_at", slot["published_at"]),
        )
        return fields

    @classmethod
    def _slot_id(cls, slot: Mapping[str, Any]) -> str:
        return "parlatest1-" + pas1_id_v1("latest-slot-id", cls._slot_fields(slot))

    @staticmethod
    def _plan_fields(plan: Mapping[str, Any]) -> tuple[tuple[str, str | None], ...]:
        owner = plan["archive_owner"]
        owner_sha256 = (
            None
            if owner is None
            else hashlib.sha256(
                canonical_json_bytes_v1(owner, maximum=_CONTROL_MAX_BYTES)
            ).hexdigest()
        )
        fields = (
            ("project_id", plan["project_id"]),
            ("recovery_response_id", plan["recovery_response_id"]),
            ("operation_id", plan["operation_id"]),
            ("recovery_key", plan["recovery_key"]),
            ("target_authority_id", plan["target_authority_id"]),
            ("transition_stub_name", plan["transition_stub_name"]),
            ("archive_owner_sha256", owner_sha256),
            ("intent_id", plan["latest_intent"]["intent_id"]),
        )
        if "operation_kind" in plan:
            barrier_sha256 = hashlib.sha256(
                canonical_json_bytes_v1(
                    plan["authoritative_drain_barrier"],
                    maximum=_CONTROL_MAX_BYTES,
                )
            ).hexdigest()
            fields += (
                ("operation_kind", plan["operation_kind"]),
                ("write_kind", plan["write_kind"]),
                ("authoritative_drain_barrier_sha256", barrier_sha256),
            )
        return fields

    @classmethod
    def _plan_id(cls, plan: Mapping[str, Any]) -> str:
        return "parlatestplan1-" + pas1_id_v1(
            "external-latest-plan-id",
            cls._plan_fields(plan),
        )

    def _legacy_recovery_key(
        self,
        intent: Mapping[str, Any],
        write_kind: str | None = None,
    ) -> str:
        ordering = intent["ordering"]
        fields: tuple[tuple[str, str | None], ...] = (
            ("project_id", self.project_id),
            ("target_authority_id", self.target_authority_id),
            ("response_id", intent["source_response_id"]),
            ("occurrence_id", ordering["occurrence_id"]),
            ("operation_id", ordering["operation_id"]),
        )
        if write_kind is not None:
            fields += (("write_kind", write_kind),)
        return "parloc1-" + pas1_id_v1(
            "external-latest-recovery-key",
            fields,
        )

    def _new_intent(
        self,
        *,
        content_sha256: str,
        content_size: int,
        content_kind: str,
        descriptor_ref: dict[str, Any] | None,
        source_sprint_id: str | None,
        source_response_id: str,
        source_revision: int,
        occurrence_id: str,
        operation_id: str,
        published_at: str,
        bootstrap: dict[str, Any],
    ) -> dict[str, Any]:
        intent: dict[str, Any] = {
            "identity_version": "prompt-archive-latest-content-intent-v1",
            "intent_id": "",
            "project_id": self.project_id,
            "slot_scope": "project",
            "slot_id": "latest-response",
            "publication_protocol": (
                "prompt-archive-project-global-ordered-latest-intent-v1"
            ),
            "ordering": {
                "revision_authority": dict(_REVISION_AUTHORITY),
                "source_revision": source_revision,
                "occurrence_id": occurrence_id,
                "operation_id": operation_id,
            },
            "bootstrap_precondition": bootstrap,
            "content_kind": content_kind,
            "content_sha256": content_sha256,
            "content_size": content_size,
            "descriptor_ref": descriptor_ref,
            "source_sprint_id": source_sprint_id,
            "source_response_id": source_response_id,
            "published_at": published_at,
        }
        intent["intent_id"] = self._intent_id(intent)
        return intent

    def _new_plan(
        self,
        *,
        content_sha256: str,
        content_size: int,
        content_kind: str,
        descriptor_ref: dict[str, Any] | None,
        source_sprint_id: str | None,
        source_response_id: str,
        source_revision: int,
        occurrence_id: str,
        operation_id: str,
        published_at: str,
        bootstrap: dict[str, Any],
        write_kind: str | None,
        authoritative_drain_barrier: dict[str, Any] | None = None,
        transition_stub_name: str | None = None,
        archive_owner: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        intent = self._new_intent(
            content_sha256=content_sha256,
            content_size=content_size,
            content_kind=content_kind,
            descriptor_ref=descriptor_ref,
            source_sprint_id=source_sprint_id,
            source_response_id=source_response_id,
            source_revision=source_revision,
            occurrence_id=occurrence_id,
            operation_id=operation_id,
            published_at=published_at,
            bootstrap=bootstrap,
        )
        return self._plan_for_intent(
            intent,
            transition_stub_name=transition_stub_name,
            archive_owner=archive_owner,
            write_kind=write_kind,
            authoritative_drain_barrier=authoritative_drain_barrier,
        )

    def _plan_for_intent(
        self,
        intent: dict[str, Any],
        *,
        transition_stub_name: str | None,
        archive_owner: dict[str, Any] | None,
        write_kind: str | None = None,
        authoritative_drain_barrier: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Construct the private crash-recovery plan for one exact intent."""

        intent = self._validate_intent(intent)
        if transition_stub_name is not None:
            _safe_leaf_name(transition_stub_name)
            if transition_stub_name.casefold() == self.external_latest.name.casefold():
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        frozen_owner = self._validate_archive_owner(archive_owner, intent=intent)
        if intent["content_kind"] == "descriptor_stub" and transition_stub_name is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            intent["content_kind"] == "descriptor_stub"
            and frozen_owner is not None
            and transition_stub_name != frozen_owner["transition_stub_name"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if intent["content_kind"] == "legacy_inline" and transition_stub_name is not None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        recovery_key = (
            self._legacy_recovery_key(intent, write_kind)
            if frozen_owner is None
            else frozen_owner["recovery_key"]
        )
        plan = {
            "schema_version": 1,
            "kind": "prompt_archive_external_latest_plan",
            "media_type": (
                "application/vnd.nginx-qa.prompt-archive-external-latest-plan-v1+json"
            ),
            "plan_id": "",
            "project_id": self.project_id,
            "target_authority_id": self.target_authority_id,
            "recovery_response_id": intent["source_response_id"],
            "operation_id": intent["ordering"]["operation_id"],
            "recovery_key": recovery_key,
            "transition_stub_name": transition_stub_name,
            "archive_owner": frozen_owner,
            "latest_intent": intent,
        }
        if intent["content_kind"] == "legacy_inline":
            if write_kind not in _LEGACY_LATEST_WRITE_KINDS:
                raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
            barrier = self._validate_authoritative_drain_barrier(
                authoritative_drain_barrier
            )
            plan["operation_kind"] = _LEGACY_LATEST_OPERATION_KIND
            plan["write_kind"] = write_kind
            plan["authoritative_drain_barrier"] = barrier
        elif write_kind is not None or authoritative_drain_barrier is not None:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        plan["plan_id"] = self._plan_id(plan)
        return plan

    def _validate_bootstrap(self, value: Any) -> dict[str, Any]:
        if type(value) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        kind = value.get("kind")
        expected_keys = set(_BOOTSTRAP_BASE) | {"kind"}
        if kind == "legacy_bytes":
            expected_keys |= {"sha256", "byte_size"}
        if set(value) != expected_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if any(value.get(key) != expected for key, expected in _BOOTSTRAP_BASE.items()):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if kind not in {"enrolled", "absent", "legacy_bytes"}:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if kind == "legacy_bytes" and (
            type(value.get("sha256")) is not str
            or _SHA256_RE.fullmatch(value["sha256"]) is None
            or type(value.get("byte_size")) is not int
            or not 0 <= value["byte_size"] <= self.max_content_bytes
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return value

    def _validate_intent(self, value: Any) -> dict[str, Any]:
        base_keys = {
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
        if type(value) is not dict or set(value) != base_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value["identity_version"] != "prompt-archive-latest-content-intent-v1"
            or value["project_id"] != self.project_id
            or value["slot_scope"] != "project"
            or value["slot_id"] != "latest-response"
            or value["publication_protocol"]
            != "prompt-archive-project-global-ordered-latest-intent-v1"
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        ordering = value["ordering"]
        if type(ordering) is not dict or set(ordering) != {
            "revision_authority",
            "source_revision",
            "occurrence_id",
            "operation_id",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if ordering["revision_authority"] != _REVISION_AUTHORITY:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            type(ordering["source_revision"]) is not int
            or not 0 <= ordering["source_revision"] <= _MAX_REVISION
            or type(ordering["occurrence_id"]) is not str
            or _SAFE_ID_RE.fullmatch(ordering["occurrence_id"]) is None
            or type(ordering["operation_id"]) is not str
            or _OPERATION_ID_RE.fullmatch(ordering["operation_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._validate_bootstrap(value["bootstrap_precondition"])
        if (
            value["content_kind"] not in {"legacy_inline", "descriptor_stub"}
            or type(value["content_sha256"]) is not str
            or _SHA256_RE.fullmatch(value["content_sha256"]) is None
            or type(value["content_size"]) is not int
            or not 0 <= value["content_size"] <= self.max_content_bytes
            or type(value["source_response_id"]) is not str
            or _RESPONSE_ID_RE.fullmatch(value["source_response_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        sprint_id = value["source_sprint_id"]
        if sprint_id is not None and (
            type(sprint_id) is not str or _SAFE_ID_RE.fullmatch(sprint_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._validate_timestamp(value["published_at"])
        descriptor = self._freeze_descriptor_ref(
            value["descriptor_ref"],
            content_kind=value["content_kind"],
            content_sha256=value["content_sha256"],
            content_size=value["content_size"],
        )
        if descriptor != value["descriptor_ref"]:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if value["content_kind"] == "descriptor_stub" and sprint_id is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            type(value["intent_id"]) is not str
            or _INTENT_ID_RE.fullmatch(value["intent_id"]) is None
            or value["intent_id"] != self._intent_id(value)
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return value

    def _validate_plan(self, value: Any) -> dict[str, Any]:
        base_keys = {
            "schema_version",
            "kind",
            "media_type",
            "plan_id",
            "project_id",
            "target_authority_id",
            "recovery_response_id",
            "operation_id",
            "recovery_key",
            "transition_stub_name",
            "archive_owner",
            "latest_intent",
        }
        if type(value) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        keys = set(value)
        if keys != base_keys and keys != base_keys | {
            "operation_kind",
            "write_kind",
            "authoritative_drain_barrier",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value["schema_version"] != 1
            or value["kind"] != "prompt_archive_external_latest_plan"
            or value["media_type"]
            != "application/vnd.nginx-qa.prompt-archive-external-latest-plan-v1+json"
            or type(value["plan_id"]) is not str
            or _PLAN_ID_RE.fullmatch(value["plan_id"]) is None
            or value["project_id"] != self.project_id
            or value["target_authority_id"] != self.target_authority_id
            or type(value["recovery_response_id"]) is not str
            or _RESPONSE_ID_RE.fullmatch(value["recovery_response_id"]) is None
            or type(value["operation_id"]) is not str
            or _OPERATION_ID_RE.fullmatch(value["operation_id"]) is None
            or type(value["recovery_key"]) is not str
            or _RECOVERY_KEY_RE.fullmatch(value["recovery_key"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        transition_stub_name = value["transition_stub_name"]
        if transition_stub_name is not None:
            _safe_leaf_name(transition_stub_name)
            if transition_stub_name.casefold() == "latest-response.json":
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        intent = self._validate_intent(value["latest_intent"])
        typed_legacy = "operation_kind" in value
        if typed_legacy:
            if (
                value["operation_kind"] != _LEGACY_LATEST_OPERATION_KIND
                or value["write_kind"] not in _LEGACY_LATEST_WRITE_KINDS
                or intent["content_kind"] != "legacy_inline"
            ):
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
            self._validate_authoritative_drain_barrier(
                value["authoritative_drain_barrier"]
            )
        owner = self._validate_archive_owner(value["archive_owner"], intent=intent)
        if owner != value["archive_owner"]:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if intent["content_kind"] == "descriptor_stub" and transition_stub_name is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            intent["content_kind"] == "descriptor_stub"
            and owner is not None
            and transition_stub_name != owner["transition_stub_name"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if intent["content_kind"] == "legacy_inline" and transition_stub_name is not None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value["recovery_response_id"] != intent["source_response_id"]
            or value["operation_id"] != intent["ordering"]["operation_id"]
            or value["recovery_key"]
            != (
                self._legacy_recovery_key(
                    intent,
                    value["write_kind"] if typed_legacy else None,
                )
                if owner is None
                else owner["recovery_key"]
            )
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if value["plan_id"] != self._plan_id(value):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return value

    @staticmethod
    def _order(document: Mapping[str, Any]) -> tuple[int, str, str]:
        ordering = document["ordering"]
        return (
            ordering["source_revision"],
            ordering["occurrence_id"],
            ordering["operation_id"],
        )

    def _content_path(self, digest: str) -> Path:
        return self.sidecar / "content" / "v1" / "sha256" / digest[:2] / f"{digest}.json"

    def _plan_paths(self, plan: Mapping[str, Any], raw: bytes) -> tuple[Path, Path]:
        intent = plan["latest_intent"]
        plan_sha = hashlib.sha256(raw).hexdigest()
        retained = self.sidecar / "intents" / "v1" / f"{intent['intent_id']}-{plan_sha}.json"
        operation = (
            self.sidecar
            / "operations"
            / "v1"
            / plan["recovery_response_id"]
            / plan["operation_id"]
            / "plan.json"
        )
        return retained, operation

    def _operation_root(self, plan: Mapping[str, Any]) -> Path:
        return (
            self.sidecar
            / "operations"
            / "v1"
            / plan["recovery_response_id"]
            / plan["operation_id"]
        )

    def _parse_plan_bytes(self, raw: bytes) -> dict[str, Any]:
        plan = parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES)
        return self._validate_plan(plan)

    def _verify_plan_artifacts(self, plan: dict[str, Any], raw: bytes) -> None:
        retained, operation = self._plan_paths(plan, raw)
        if _read_optional(self._sidecar_storage, retained, maximum=len(raw)) != raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if _read_optional(self._sidecar_storage, operation, maximum=len(raw)) != raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def _load_current_plan(self) -> tuple[dict[str, Any], bytes] | None:
        path = self.sidecar / "intent.json"
        raw = _read_optional(self._sidecar_storage, path, maximum=_CONTROL_MAX_BYTES)
        if raw is None:
            return None
        plan = self._parse_plan_bytes(raw)
        self._verify_plan_artifacts(plan, raw)
        return plan, raw

    def _load_operation_plan(
        self,
        response_id: str,
        operation_id: str,
    ) -> tuple[dict[str, Any], bytes] | None:
        path = (
            self.sidecar
            / "operations"
            / "v1"
            / response_id
            / operation_id
            / "plan.json"
        )
        raw = _read_optional(self._sidecar_storage, path, maximum=_CONTROL_MAX_BYTES)
        if raw is None:
            return None
        plan = self._parse_plan_bytes(raw)
        if plan["recovery_response_id"] != response_id or plan["operation_id"] != operation_id:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        retained, _operation = self._plan_paths(plan, raw)
        if _read_optional(self._sidecar_storage, retained, maximum=len(raw)) != raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return plan, raw

    @staticmethod
    def _plan_binding(plan: Mapping[str, Any], raw: bytes) -> dict[str, Any]:
        return {
            "kind": "prompt_archive_external_latest_plan_binding",
            "media_type": _PLAN_MEDIA_TYPE,
            "plan_id": plan["plan_id"],
            "sha256": hashlib.sha256(raw).hexdigest(),
            "byte_size": len(raw),
        }

    @staticmethod
    def _journal_lower_bound(raw: bytes) -> dict[str, Any]:
        return {
            "kind": "prompt_archive_external_latest_journal_lower_bound",
            "media_type": _JOURNAL_MEDIA_TYPE,
            "journal_generation": 1,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "byte_size": len(raw),
        }

    @staticmethod
    def _pending_shard_id(recovery_key: str) -> str:
        if type(recovery_key) is not str or _RECOVERY_KEY_RE.fullmatch(recovery_key) is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return hashlib.sha256(recovery_key.encode("utf-8")).hexdigest()[:2]

    def _pending_current_path(self, shard_id: str) -> Path:
        return self.sidecar / "pending" / "v1" / "shards" / f"{shard_id}.json"

    def _pending_generation_path(
        self,
        shard_id: str,
        generation: int,
        registry_id: str,
    ) -> Path:
        return (
            self.sidecar
            / "pending"
            / "v1"
            / "generations"
            / shard_id
            / f"{generation}-{registry_id}.json"
        )

    def _validate_plan_binding(self, value: Any) -> dict[str, Any]:
        keys = {"kind", "media_type", "plan_id", "sha256", "byte_size"}
        if type(value) is not dict or set(value) != keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value["kind"] != "prompt_archive_external_latest_plan_binding"
            or value["media_type"] != _PLAN_MEDIA_TYPE
            or type(value["plan_id"]) is not str
            or _PLAN_ID_RE.fullmatch(value["plan_id"]) is None
            or type(value["sha256"]) is not str
            or _SHA256_RE.fullmatch(value["sha256"]) is None
            or type(value["byte_size"]) is not int
            or not 1 <= value["byte_size"] <= _CONTROL_MAX_BYTES
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return value

    def _validate_journal_lower_bound(self, value: Any) -> dict[str, Any]:
        keys = {
            "kind",
            "media_type",
            "journal_generation",
            "sha256",
            "byte_size",
        }
        if type(value) is not dict or set(value) != keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value["kind"]
            != "prompt_archive_external_latest_journal_lower_bound"
            or value["media_type"] != _JOURNAL_MEDIA_TYPE
            or value["journal_generation"] != 1
            or type(value["sha256"]) is not str
            or _SHA256_RE.fullmatch(value["sha256"]) is None
            or type(value["byte_size"]) is not int
            or not 1 <= value["byte_size"] <= _CONTROL_MAX_BYTES
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return value

    def _pending_entry(
        self,
        plan: Mapping[str, Any],
        plan_raw: bytes,
        *,
        journal_raw: bytes | None = None,
    ) -> dict[str, Any]:
        published_at = plan["latest_intent"]["published_at"]
        return {
            "recovery_key": plan["recovery_key"],
            "recovery_response_id": plan["recovery_response_id"],
            "operation_id": plan["operation_id"],
            "plan_binding": self._plan_binding(plan, plan_raw),
            "state": "planned" if journal_raw is None else "journaled",
            "journal_lower_bound": (
                None if journal_raw is None else self._journal_lower_bound(journal_raw)
            ),
            "created_at": published_at,
            "updated_at": published_at,
        }

    def _validate_pending_entry(
        self,
        value: Any,
        *,
        shard_id: str,
    ) -> dict[str, Any]:
        keys = {
            "recovery_key",
            "recovery_response_id",
            "operation_id",
            "plan_binding",
            "state",
            "journal_lower_bound",
            "created_at",
            "updated_at",
        }
        if type(value) is not dict or set(value) != keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            type(value["recovery_key"]) is not str
            or _RECOVERY_KEY_RE.fullmatch(value["recovery_key"]) is None
            or self._pending_shard_id(value["recovery_key"]) != shard_id
            or type(value["recovery_response_id"]) is not str
            or _RESPONSE_ID_RE.fullmatch(value["recovery_response_id"]) is None
            or type(value["operation_id"]) is not str
            or _OPERATION_ID_RE.fullmatch(value["operation_id"]) is None
            or value["state"] not in {"planned", "journaled"}
            or self._validate_timestamp(value["created_at"]) != value["created_at"]
            or self._validate_timestamp(value["updated_at"]) != value["updated_at"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._validate_plan_binding(value["plan_binding"])
        if value["state"] == "planned":
            if value["journal_lower_bound"] is not None:
                raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        else:
            self._validate_journal_lower_bound(value["journal_lower_bound"])
        return value

    def _pending_registry_id(self, registry: Mapping[str, Any]) -> str:
        entries_raw = canonical_json_bytes_v1(
            registry["entries"],
            maximum=_CONTROL_MAX_BYTES,
        )
        return "parpending1-" + pas1_id_v1(
            "pending-operation-registry-id",
            (
                ("project_id", self.project_id),
                ("shard_id", registry["shard_id"]),
                ("generation", str(registry["generation"])),
                ("previous_registry_id", registry["previous_registry_id"]),
                ("entries_sha256", hashlib.sha256(entries_raw).hexdigest()),
                ("published_at", registry["published_at"]),
            ),
        )

    def _validate_pending_registry(
        self,
        value: Any,
        *,
        shard_id: str,
    ) -> dict[str, Any]:
        keys = {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "registry_id",
            "project_id",
            "target_authority_id",
            "shard_algorithm",
            "shard_id",
            "generation",
            "previous_registry_id",
            "entries",
            "published_at",
        }
        if type(value) is not dict or set(value) != keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        previous = value["previous_registry_id"]
        if (
            value["schema_version"] != 1
            or value["kind"] != "prompt_archive_pending_operation_registry"
            or value["media_type"] != _PENDING_MEDIA_TYPE
            or value["id_algorithm"] != "PAS1-ID-v1"
            or type(value["registry_id"]) is not str
            or _PENDING_ID_RE.fullmatch(value["registry_id"]) is None
            or value["project_id"] != self.project_id
            or value["target_authority_id"] != self.target_authority_id
            or value["shard_algorithm"] != "sha256-recovery-key-prefix-2-v1"
            or value["shard_id"] != shard_id
            or _SHARD_ID_RE.fullmatch(shard_id) is None
            or type(value["generation"]) is not int
            or not 1 <= value["generation"] <= _MAX_REVISION
            or type(value["entries"]) is not list
            or len(value["entries"]) > _PENDING_SHARD_MAX_ENTRIES
            or self._validate_timestamp(value["published_at"])
            != value["published_at"]
            or (value["generation"] == 1 and previous is not None)
            or (
                value["generation"] > 1
                and (
                    type(previous) is not str
                    or _PENDING_ID_RE.fullmatch(previous) is None
                )
            )
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        entries = [
            self._validate_pending_entry(entry, shard_id=shard_id)
            for entry in value["entries"]
        ]
        if entries != sorted(
            entries,
            key=lambda entry: (entry["recovery_key"], entry["operation_id"]),
        ) or len({entry["recovery_key"] for entry in entries}) != len(entries):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if value["registry_id"] != self._pending_registry_id(value):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return value

    def _read_pending_registry_locked(self, shard_id: str) -> dict[str, Any] | None:
        path = self._pending_current_path(shard_id)
        raw = _read_optional(self._sidecar_storage, path, maximum=_CONTROL_MAX_BYTES)
        if raw is None:
            return None
        registry = self._validate_pending_registry(
            parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES),
            shard_id=shard_id,
        )
        retained = self._pending_generation_path(
            shard_id,
            registry["generation"],
            registry["registry_id"],
        )
        if _read_optional(self._sidecar_storage, retained, maximum=len(raw)) != raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if registry["generation"] > 1:
            predecessor_path = self._pending_generation_path(
                shard_id,
                registry["generation"] - 1,
                registry["previous_registry_id"],
            )
            predecessor_raw = _read_optional(
                self._sidecar_storage,
                predecessor_path,
                maximum=_CONTROL_MAX_BYTES,
            )
            if predecessor_raw is None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            predecessor = self._validate_pending_registry(
                parse_canonical_json_v1(
                    predecessor_raw,
                    maximum=_CONTROL_MAX_BYTES,
                ),
                shard_id=shard_id,
            )
            if (
                predecessor["generation"] != registry["generation"] - 1
                or predecessor["registry_id"]
                != registry["previous_registry_id"]
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return registry

    def _write_pending_registry_locked(
        self,
        *,
        shard_id: str,
        previous: Mapping[str, Any] | None,
        entries: list[Mapping[str, Any]],
        published_at: str,
    ) -> dict[str, Any]:
        generation = 1 if previous is None else previous["generation"] + 1
        if generation > _MAX_REVISION:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        frozen_entries = sorted(
            (
                parse_canonical_json_v1(
                    canonical_json_bytes_v1(entry, maximum=_CONTROL_MAX_BYTES),
                    maximum=_CONTROL_MAX_BYTES,
                )
                for entry in entries
            ),
            key=lambda entry: (entry["recovery_key"], entry["operation_id"]),
        )
        if len(frozen_entries) > _PENDING_SHARD_MAX_ENTRIES:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        registry = {
            "schema_version": 1,
            "kind": "prompt_archive_pending_operation_registry",
            "media_type": _PENDING_MEDIA_TYPE,
            "id_algorithm": "PAS1-ID-v1",
            "registry_id": "",
            "project_id": self.project_id,
            "target_authority_id": self.target_authority_id,
            "shard_algorithm": "sha256-recovery-key-prefix-2-v1",
            "shard_id": shard_id,
            "generation": generation,
            "previous_registry_id": (
                None if previous is None else previous["registry_id"]
            ),
            "entries": frozen_entries,
            "published_at": self._validate_timestamp(published_at),
        }
        registry["registry_id"] = self._pending_registry_id(registry)
        registry = self._validate_pending_registry(registry, shard_id=shard_id)
        raw = canonical_json_bytes_v1(registry, maximum=_CONTROL_MAX_BYTES)
        retained = self._pending_generation_path(
            shard_id,
            generation,
            registry["registry_id"],
        )
        _publish_immutable(self._sidecar_storage, retained, raw)
        _replace_exact(
            self._sidecar_storage,
            self._pending_current_path(shard_id),
            raw,
        )
        return registry

    def _register_pending(self, plan: Mapping[str, Any], plan_raw: bytes) -> None:
        desired = self._pending_entry(plan, plan_raw)
        shard_id = self._pending_shard_id(plan["recovery_key"])
        with self._pending_shard_lock(shard_id):
            registry = self._read_pending_registry_locked(shard_id)
            entries = [] if registry is None else list(registry["entries"])
            existing = next(
                (
                    entry
                    for entry in entries
                    if entry["recovery_key"] == plan["recovery_key"]
                ),
                None,
            )
            if existing is not None:
                if existing == desired:
                    return
                terminal = self._load_journal(plan, plan_raw)
                if terminal is None:
                    raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                expected_journaled = self._pending_entry(
                    plan,
                    plan_raw,
                    journal_raw=terminal[1],
                )
                if existing != expected_journaled:
                    raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                return
            entries.append(desired)
            self._write_pending_registry_locked(
                shard_id=shard_id,
                previous=registry,
                entries=entries,
                published_at=plan["latest_intent"]["published_at"],
            )
            self._fault(FAULT_AFTER_PENDING_BIND)

    def _load_pending_plan(
        self,
        entry: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bytes]:
        loaded = self._load_operation_plan(
            entry["recovery_response_id"],
            entry["operation_id"],
        )
        if loaded is None:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        plan, raw = loaded
        if (
            plan["recovery_key"] != entry["recovery_key"]
            or self._plan_binding(plan, raw) != entry["plan_binding"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        self._read_content(plan["latest_intent"])
        return plan, raw

    def _validate_pending_lower_bound(
        self,
        entry: Mapping[str, Any],
        plan: Mapping[str, Any],
        plan_raw: bytes,
    ) -> tuple[dict[str, Any], bytes] | None:
        terminal = self._load_journal(plan, plan_raw)
        if entry["state"] == "planned":
            return terminal
        if terminal is None:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        journal, raw = terminal
        if self._journal_lower_bound(raw) != entry["journal_lower_bound"]:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return journal, raw

    def _finalize_pending(self, plan: Mapping[str, Any], plan_raw: bytes) -> None:
        terminal = self._load_journal(plan, plan_raw)
        if terminal is None:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        _journal, journal_raw = terminal
        planned = self._pending_entry(plan, plan_raw)
        journaled = self._pending_entry(plan, plan_raw, journal_raw=journal_raw)
        shard_id = self._pending_shard_id(plan["recovery_key"])

        with self._pending_shard_lock(shard_id):
            registry = self._read_pending_registry_locked(shard_id)
            if registry is None:
                return
            entries = list(registry["entries"])
            index = next(
                (
                    ordinal
                    for ordinal, entry in enumerate(entries)
                    if entry["recovery_key"] == plan["recovery_key"]
                ),
                None,
            )
            if index is None:
                return
            if entries[index] == planned:
                entries[index] = journaled
                registry = self._write_pending_registry_locked(
                    shard_id=shard_id,
                    previous=registry,
                    entries=entries,
                    published_at=plan["latest_intent"]["published_at"],
                )
            elif entries[index] != journaled:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")

        self._fault(FAULT_AFTER_TERMINAL_BEFORE_PENDING_REMOVE)

        with self._pending_shard_lock(shard_id):
            registry = self._read_pending_registry_locked(shard_id)
            if registry is None:
                return
            entries = list(registry["entries"])
            index = next(
                (
                    ordinal
                    for ordinal, entry in enumerate(entries)
                    if entry["recovery_key"] == plan["recovery_key"]
                ),
                None,
            )
            if index is None:
                return
            if entries[index] != journaled:
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            del entries[index]
            self._write_pending_registry_locked(
                shard_id=shard_id,
                previous=registry,
                entries=entries,
                published_at=plan["latest_intent"]["published_at"],
            )

    def _read_content(self, intent: Mapping[str, Any]) -> bytes:
        try:
            content = _read_optional(
                self._sidecar_storage,
                self._content_path(intent["content_sha256"]),
                maximum=intent["content_size"],
            )
        except PromptArchiveStorageError as exc:
            if exc.code == "PROMPT_ARCHIVE_LIMIT_EXCEEDED":
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") from None
            raise
        if (
            content is None
            or len(content) != intent["content_size"]
            or hashlib.sha256(content).hexdigest() != intent["content_sha256"]
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return content

    def _validate_slot(self, value: Any) -> dict[str, Any]:
        base_keys = {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "latest_slot_id",
            "intent_id",
            "project_id",
            "slot_scope",
            "slot_id",
            "publication_protocol",
            "ordering",
            "generation",
            "previous_slot_id",
            "content_kind",
            "content_sha256",
            "content_size",
            "descriptor_ref",
            "source_sprint_id",
            "source_response_id",
            "published_at",
        }
        if type(value) is not dict or set(value) != base_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        projection = {
            key: value[key]
            for key in (
                "intent_id",
                "project_id",
                "slot_scope",
                "slot_id",
                "publication_protocol",
                "ordering",
                "content_kind",
                "content_sha256",
                "content_size",
                "descriptor_ref",
                "source_sprint_id",
                "source_response_id",
                "published_at",
            )
        }
        projection["identity_version"] = "prompt-archive-latest-content-intent-v1"
        projection["bootstrap_precondition"] = self._bootstrap("enrolled")
        # Validate slot fields directly; bootstrap is intentionally absent from
        # the slot ID and need not equal the originating first-slot observation.
        if (
            value["schema_version"] != 1
            or value["kind"] != "prompt_archive_latest_slot"
            or value["media_type"]
            != "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json"
            or value["id_algorithm"] != "PAS1-ID-v1"
            or value["project_id"] != self.project_id
            or value["slot_scope"] != "project"
            or value["slot_id"] != "latest-response"
            or value["publication_protocol"]
            != "prompt-archive-project-global-ordered-latest-intent-v1"
            or type(value["generation"]) is not int
            or not 1 <= value["generation"] <= _MAX_REVISION
            or (value["generation"] == 1 and value["previous_slot_id"] is not None)
            or (
                value["generation"] > 1
                and (
                    type(value["previous_slot_id"]) is not str
                    or _SLOT_ID_RE.fullmatch(value["previous_slot_id"]) is None
                )
            )
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        ordering = value["ordering"]
        if type(ordering) is not dict or set(ordering) != {
            "revision_authority",
            "source_revision",
            "occurrence_id",
            "operation_id",
        }:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if ordering["revision_authority"] != _REVISION_AUTHORITY:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            type(ordering["source_revision"]) is not int
            or not 0 <= ordering["source_revision"] <= _MAX_REVISION
            or type(ordering["occurrence_id"]) is not str
            or _SAFE_ID_RE.fullmatch(ordering["occurrence_id"]) is None
            or type(ordering["operation_id"]) is not str
            or _OPERATION_ID_RE.fullmatch(ordering["operation_id"]) is None
            or type(value["intent_id"]) is not str
            or _INTENT_ID_RE.fullmatch(value["intent_id"]) is None
            or value["content_kind"] not in {"legacy_inline", "descriptor_stub"}
            or type(value["content_sha256"]) is not str
            or _SHA256_RE.fullmatch(value["content_sha256"]) is None
            or type(value["content_size"]) is not int
            or not 0 <= value["content_size"] <= self.max_content_bytes
            or type(value["source_response_id"]) is not str
            or _RESPONSE_ID_RE.fullmatch(value["source_response_id"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        sprint_id = value["source_sprint_id"]
        if sprint_id is not None and (
            type(sprint_id) is not str or _SAFE_ID_RE.fullmatch(sprint_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        self._validate_timestamp(value["published_at"])
        descriptor = self._freeze_descriptor_ref(
            value["descriptor_ref"],
            content_kind=value["content_kind"],
            content_sha256=value["content_sha256"],
            content_size=value["content_size"],
        )
        if descriptor != value["descriptor_ref"]:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if value["content_kind"] == "descriptor_stub" and sprint_id is None:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            type(value["latest_slot_id"]) is not str
            or _SLOT_ID_RE.fullmatch(value["latest_slot_id"]) is None
            or value["latest_slot_id"] != self._slot_id(value)
        ):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return value

    def _slot_path(self, slot: Mapping[str, Any]) -> Path:
        return (
            self.sidecar
            / "slots"
            / "v1"
            / f"{slot['generation']}-{slot['latest_slot_id']}.json"
        )

    def _load_retained_slot(self, generation: int, slot_id: str) -> tuple[dict[str, Any], bytes]:
        path = self.sidecar / "slots" / "v1" / f"{generation}-{slot_id}.json"
        raw = _read_optional(self._sidecar_storage, path, maximum=_CONTROL_MAX_BYTES)
        if raw is None:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        slot = self._validate_slot(parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES))
        if slot["generation"] != generation or slot["latest_slot_id"] != slot_id:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        self._read_content(slot)
        return slot, raw

    def _load_current_slot(self) -> tuple[dict[str, Any], bytes] | None:
        path = self.sidecar / "slot.json"
        raw = _read_optional(self._sidecar_storage, path, maximum=_CONTROL_MAX_BYTES)
        if raw is None:
            return None
        slot = self._validate_slot(parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES))
        retained, retained_raw = self._load_retained_slot(
            slot["generation"], slot["latest_slot_id"]
        )
        if retained != slot or retained_raw != raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return slot, raw

    def _slot_for_intent(
        self,
        intent: Mapping[str, Any],
        previous: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        generation = 1 if previous is None else previous["generation"] + 1
        if generation > _MAX_REVISION:
            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
        slot: dict[str, Any] = {
            "schema_version": 1,
            "kind": "prompt_archive_latest_slot",
            "media_type": "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json",
            "id_algorithm": "PAS1-ID-v1",
            "latest_slot_id": "",
            "intent_id": intent["intent_id"],
            "project_id": intent["project_id"],
            "slot_scope": intent["slot_scope"],
            "slot_id": intent["slot_id"],
            "publication_protocol": intent["publication_protocol"],
            "ordering": intent["ordering"],
            "generation": generation,
            "previous_slot_id": None if previous is None else previous["latest_slot_id"],
            "content_kind": intent["content_kind"],
            "content_sha256": intent["content_sha256"],
            "content_size": intent["content_size"],
            "descriptor_ref": intent["descriptor_ref"],
            "source_sprint_id": intent["source_sprint_id"],
            "source_response_id": intent["source_response_id"],
            "published_at": intent["published_at"],
        }
        slot["latest_slot_id"] = self._slot_id(slot)
        return slot

    @staticmethod
    def _slot_matches_intent(
        slot: Mapping[str, Any],
        intent: Mapping[str, Any],
    ) -> bool:
        """Return whether every intent-derived slot field is exact."""

        return all(
            slot[key] == intent[key]
            for key in (
                "intent_id",
                "project_id",
                "slot_scope",
                "slot_id",
                "publication_protocol",
                "ordering",
                "content_kind",
                "content_sha256",
                "content_size",
                "descriptor_ref",
                "source_sprint_id",
                "source_response_id",
                "published_at",
            )
        )

    def _journal_path(self, plan: Mapping[str, Any]) -> Path:
        return self._operation_root(plan) / "journal.json"

    def _validate_journal(
        self,
        value: Any,
        *,
        plan: Mapping[str, Any],
        plan_raw: bytes,
    ) -> dict[str, Any]:
        base_keys = {
            "schema_version",
            "kind",
            "media_type",
            "project_id",
            "recovery_response_id",
            "operation_id",
            "plan_sha256",
            "journal_generation",
            "state",
            "outcome",
            "result_latest_slot_id",
            "result_generation",
            "completed_at",
        }
        if type(value) is not dict:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        keys = set(value)
        typed_plan = "operation_kind" in plan
        expected_keys = (
            base_keys
            | {
                "operation_kind",
                "write_kind",
                "authoritative_drain_barrier",
            }
            if typed_plan
            else base_keys
        )
        if keys != expected_keys:
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if (
            value["schema_version"] != 1
            or value["kind"] != "prompt_archive_external_latest_journal"
            or value["media_type"]
            != "application/vnd.nginx-qa.prompt-archive-external-latest-journal-v1+json"
            or value["project_id"] != self.project_id
            or value["recovery_response_id"] != plan["recovery_response_id"]
            or value["operation_id"] != plan["operation_id"]
            or value["plan_sha256"] != hashlib.sha256(plan_raw).hexdigest()
            or value["journal_generation"] != 1
            or value["state"] != "complete"
            or value["outcome"] not in {"published", "superseded"}
            or type(value["result_latest_slot_id"]) is not str
            or _SLOT_ID_RE.fullmatch(value["result_latest_slot_id"]) is None
            or type(value["result_generation"]) is not int
            or not 1 <= value["result_generation"] <= _MAX_REVISION
            or value["completed_at"] != plan["latest_intent"]["published_at"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if typed_plan and (
            value["operation_kind"] != plan["operation_kind"]
            or value["write_kind"] != plan["write_kind"]
            or value["authoritative_drain_barrier"]
            != plan["authoritative_drain_barrier"]
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        slot, _raw = self._load_retained_slot(
            value["result_generation"], value["result_latest_slot_id"]
        )
        plan_order = self._order(plan["latest_intent"])
        slot_order = self._order(slot)
        if value["outcome"] == "published":
            if not self._slot_matches_intent(slot, plan["latest_intent"]):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        elif slot_order <= plan_order:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return value

    def _load_journal(
        self,
        plan: Mapping[str, Any],
        plan_raw: bytes,
    ) -> tuple[dict[str, Any], bytes] | None:
        raw = _read_optional(
            self._sidecar_storage,
            self._journal_path(plan),
            maximum=_CONTROL_MAX_BYTES,
        )
        if raw is None:
            return None
        journal = self._validate_journal(
            parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES),
            plan=plan,
            plan_raw=plan_raw,
        )
        digest = hashlib.sha256(raw).hexdigest()
        retained = (
            self._operation_root(plan)
            / "journal-generations"
            / f"1-{digest}.json"
        )
        if _read_optional(self._sidecar_storage, retained, maximum=len(raw)) != raw:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        return journal, raw

    def _ensure_terminal(
        self,
        plan: Mapping[str, Any],
        plan_raw: bytes,
        *,
        outcome: Literal["published", "superseded"],
        result_slot: Mapping[str, Any],
    ) -> dict[str, Any]:
        existing = self._load_journal(plan, plan_raw)
        if existing is not None:
            journal, _raw = existing
            # A formerly published operation remains a valid terminal replay
            # after a later slot supersedes it.
            if journal["outcome"] != outcome and journal["outcome"] != "published":
                raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
            return journal
        journal = {
            "schema_version": 1,
            "kind": "prompt_archive_external_latest_journal",
            "media_type": (
                "application/vnd.nginx-qa.prompt-archive-external-latest-journal-v1+json"
            ),
            "project_id": self.project_id,
            "recovery_response_id": plan["recovery_response_id"],
            "operation_id": plan["operation_id"],
            "plan_sha256": hashlib.sha256(plan_raw).hexdigest(),
            "journal_generation": 1,
            "state": "complete",
            "outcome": outcome,
            "result_latest_slot_id": result_slot["latest_slot_id"],
            "result_generation": result_slot["generation"],
            "completed_at": plan["latest_intent"]["published_at"],
        }
        if "operation_kind" in plan:
            journal["operation_kind"] = plan["operation_kind"]
            journal["write_kind"] = plan["write_kind"]
            journal["authoritative_drain_barrier"] = plan[
                "authoritative_drain_barrier"
            ]
        raw = canonical_json_bytes_v1(journal, maximum=_CONTROL_MAX_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        retained = (
            self._operation_root(plan)
            / "journal-generations"
            / f"1-{digest}.json"
        )
        _publish_immutable(self._sidecar_storage, retained, raw)
        _replace_exact(self._sidecar_storage, self._journal_path(plan), raw)
        self._fault(FAULT_AFTER_TERMINAL)
        return journal

    def _result(
        self,
        plan: Mapping[str, Any],
        slot: Mapping[str, Any],
        *,
        outcome: Literal["published", "idempotent", "superseded"],
        recovered: bool,
    ) -> ExternalLatestResult:
        intent = plan["latest_intent"]
        slot_raw = canonical_json_bytes_v1(slot, maximum=_CONTROL_MAX_BYTES)
        latest_slot_ref = self._sidecar_storage.describe_blob(
            slot_raw,
            logical_kind="latest_slot",
            media_type="application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json",
        )
        return ExternalLatestResult(
            outcome=outcome,
            intent_id=intent["intent_id"],
            latest_slot_id=slot["latest_slot_id"],
            latest_slot_ref=latest_slot_ref,
            source_revision=intent["ordering"]["source_revision"],
            generation=slot["generation"],
            content_sha256=intent["content_sha256"],
            content_size=intent["content_size"],
            recovered=recovered,
        )

    def _external_matches(self, expected_sha256: str, expected_size: int) -> bool:
        observed = _read_optional(
            self._parent_storage,
            self.external_latest,
            maximum=self.max_content_bytes,
        )
        return (
            observed is not None
            and len(observed) == expected_size
            and hashlib.sha256(observed).hexdigest() == expected_sha256
        )

    def _external_precondition_holds(
        self,
        intent: Mapping[str, Any],
        previous: Mapping[str, Any] | None,
    ) -> bool:
        observed = _read_optional(
            self._parent_storage,
            self.external_latest,
            maximum=self.max_content_bytes,
        )
        if observed is None:
            return True
        if (
            len(observed) == intent["content_size"]
            and hashlib.sha256(observed).hexdigest() == intent["content_sha256"]
        ):
            return True
        if previous is not None:
            return (
                len(observed) == previous["content_size"]
                and hashlib.sha256(observed).hexdigest() == previous["content_sha256"]
            )
        bootstrap = intent["bootstrap_precondition"]
        if bootstrap["kind"] == "absent":
            return False
        if bootstrap["kind"] == "legacy_bytes":
            return (
                len(observed) == bootstrap["byte_size"]
                and hashlib.sha256(observed).hexdigest() == bootstrap["sha256"]
            )
        return False

    def _publish_transition_stub(
        self,
        plan: Mapping[str, Any],
        intended_content: bytes,
    ) -> None:
        transition_stub_name = plan["transition_stub_name"]
        if transition_stub_name is None:
            return
        _safe_leaf_name(transition_stub_name)
        if transition_stub_name.casefold() == self.external_latest.name.casefold():
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        transition_target = self.external_latest.parent / transition_stub_name
        created = _publish_immutable(
            self._parent_storage,
            transition_target,
            intended_content,
        )
        if created:
            self._fault(FAULT_AFTER_TRANSITION_STUB)

    def _revalidate_authoritative_drain(
        self,
        plan: Mapping[str, Any],
        plan_raw: bytes,
        verifier: Callable[[Mapping[str, Any]], bool] | None,
    ) -> None:
        if plan.get("operation_kind") != _LEGACY_LATEST_OPERATION_KIND:
            return
        barrier = self._validate_authoritative_drain_barrier(
            plan.get("authoritative_drain_barrier")
        )
        if verifier is None:
            # A terminal operation may be inspected/replayed by generic
            # startup code without mutating external bytes.  An incomplete
            # typed legacy operation requires its outer common-runtime drain
            # authority before it can advance.
            if self._load_journal(plan, plan_raw) is not None:
                return
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        self._invoke_authoritative_drain_verifier(barrier, verifier)

    def _invoke_authoritative_drain_verifier(
        self,
        barrier: Mapping[str, Any],
        verifier: Callable[[Mapping[str, Any]], bool],
    ) -> None:
        if not callable(verifier):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        validated = self._validate_authoritative_drain_barrier(dict(barrier))
        frozen = parse_canonical_json_v1(
            canonical_json_bytes_v1(validated, maximum=_CONTROL_MAX_BYTES),
            maximum=_CONTROL_MAX_BYTES,
        )
        try:
            accepted = verifier(frozen)
        except PromptArchiveStorageError:
            raise
        except Exception:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if accepted is not True:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
        if self._validate_authoritative_drain_barrier(
            dict(barrier)
        ) != validated:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    @staticmethod
    def _invoke_pre_enrollment_verifier(
        verifier: Callable[[], bool],
    ) -> None:
        """Require one read-only caller precondition under ``latest.lock``."""

        if not callable(verifier):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        try:
            accepted = verifier()
        except PromptArchiveStorageError:
            raise
        except Exception:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
        if accepted is not True:
            raise _error("PROMPT_ARCHIVE_UNAVAILABLE")

    def _reconcile_plan(
        self,
        plan: dict[str, Any],
        plan_raw: bytes,
        *,
        recovered: bool,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> ExternalLatestResult:
        self._verify_archive_owner(plan)
        self._revalidate_authoritative_drain(
            plan,
            plan_raw,
            authoritative_drain_verifier,
        )
        intent = plan["latest_intent"]
        intended_content = self._read_content(intent)
        self._publish_transition_stub(plan, intended_content)
        current = self._load_current_slot()
        previous = None if current is None else current[0]
        if previous is not None:
            current_order = self._order(previous)
            intent_order = self._order(intent)
            if current_order > intent_order:
                journal = self._ensure_terminal(
                    plan,
                    plan_raw,
                    outcome="superseded",
                    result_slot=previous,
                )
                semantic_outcome: Literal["idempotent", "superseded"] = "superseded"
                if journal["outcome"] == "published":
                    semantic_outcome = "idempotent"
                    previous, _raw = self._load_retained_slot(
                        journal["result_generation"],
                        journal["result_latest_slot_id"],
                    )
                return self._result(
                    plan,
                    previous,
                    outcome=semantic_outcome,
                    recovered=recovered,
                )
            if current_order == intent_order:
                if previous["intent_id"] != intent["intent_id"]:
                    raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                if not self._slot_matches_intent(previous, intent):
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                if not self._external_matches(intent["content_sha256"], intent["content_size"]):
                    observed = _read_optional(
                        self._parent_storage,
                        self.external_latest,
                        maximum=self.max_content_bytes,
                    )
                    if observed is not None:
                        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                    _replace_exact(
                        self._parent_storage,
                        self.external_latest,
                        intended_content,
                        existing_maximum=self.max_content_bytes,
                    )
                    self._fault(FAULT_AFTER_EXTERNAL_CONTENT)
                self._ensure_terminal(
                    plan,
                    plan_raw,
                    outcome="published",
                    result_slot=previous,
                )
                return self._result(
                    plan,
                    previous,
                    outcome="idempotent",
                    recovered=recovered,
                )

        if not self._external_precondition_holds(intent, previous):
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        if not self._external_matches(intent["content_sha256"], intent["content_size"]):
            _replace_exact(
                self._parent_storage,
                self.external_latest,
                intended_content,
                existing_maximum=self.max_content_bytes,
            )
            self._fault(FAULT_AFTER_EXTERNAL_CONTENT)

        slot = self._slot_for_intent(intent, previous)
        slot_raw = canonical_json_bytes_v1(slot, maximum=_CONTROL_MAX_BYTES)
        created = _publish_immutable(self._sidecar_storage, self._slot_path(slot), slot_raw)
        if created:
            self._fault(FAULT_AFTER_RETAINED_SLOT)
        changed = _replace_exact(self._sidecar_storage, self.sidecar / "slot.json", slot_raw)
        if changed:
            self._fault(FAULT_AFTER_CURRENT_SLOT)
        self._ensure_terminal(
            plan,
            plan_raw,
            outcome="published",
            result_slot=slot,
        )
        return self._result(
            plan,
            slot,
            outcome="published",
            recovered=recovered,
        )

    def _publish_operation_artifacts(
        self,
        plan: dict[str, Any],
        plan_raw: bytes,
        content: bytes,
    ) -> None:
        self._verify_archive_owner(plan)
        intent = plan["latest_intent"]
        content_path = self._content_path(intent["content_sha256"])
        _publish_immutable(self._sidecar_storage, content_path, content)
        verified = self._read_content(intent)
        if verified != content:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        self._fault(FAULT_AFTER_CONTENT)
        retained, operation = self._plan_paths(plan, plan_raw)
        _publish_immutable(self._sidecar_storage, retained, plan_raw)
        _publish_immutable(self._sidecar_storage, operation, plan_raw)
        self._verify_plan_artifacts(plan, plan_raw)
        self._fault(FAULT_AFTER_OPERATION_PLAN)

    def _install_current_plan(
        self,
        plan: dict[str, Any],
        plan_raw: bytes,
        content: bytes,
    ) -> None:
        self._verify_archive_owner(plan)
        self._verify_plan_artifacts(plan, plan_raw)
        if self._read_content(plan["latest_intent"]) != content:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        changed = _replace_exact(
            self._sidecar_storage,
            self.sidecar / "intent.json",
            plan_raw,
        )
        if changed:
            self._fault(FAULT_AFTER_CURRENT_INTENT)
            self._fault(FAULT_AFTER_CURRENT_INSTALL)

    def _record_superseded(
        self,
        plan: dict[str, Any],
        plan_raw: bytes,
        content: bytes,
        current_slot: dict[str, Any],
        *,
        recovered: bool = False,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> ExternalLatestResult:
        self._verify_archive_owner(plan)
        self._revalidate_authoritative_drain(
            plan,
            plan_raw,
            authoritative_drain_verifier,
        )
        intent = plan["latest_intent"]
        self._verify_plan_artifacts(plan, plan_raw)
        if self._read_content(intent) != content:
            raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        # A superseded ordered intent must not change the project-global latest,
        # but its transition-specific authoritative response is still required.
        # The operation plan and content are durable before this publication,
        # so an exact operation replay can finish after a boundary fault.
        self._publish_transition_stub(plan, content)
        journal = self._ensure_terminal(
            plan,
            plan_raw,
            outcome="superseded",
            result_slot=current_slot,
        )
        result_slot = current_slot
        semantic: Literal["idempotent", "superseded"] = "superseded"
        if journal["outcome"] == "published":
            semantic = "idempotent"
            result_slot, _raw = self._load_retained_slot(
                journal["result_generation"], journal["result_latest_slot_id"]
            )
        return self._result(
            plan,
            result_slot,
            outcome=semantic,
            recovered=recovered,
        )

    def _assert_request_matches_plan(
        self,
        plan: Mapping[str, Any],
        *,
        content_sha256: str,
        content_size: int,
        content_kind: str,
        descriptor_ref: dict[str, Any] | None,
        source_sprint_id: str | None,
        source_response_id: str,
        source_revision: int | None,
        occurrence_id: str,
        operation_id: str,
        published_at: str,
        transition_stub_name: str | None,
        write_kind: str,
        authoritative_drain_barrier: Mapping[str, Any],
    ) -> None:
        intent = plan["latest_intent"]
        expected = {
            "content_sha256": content_sha256,
            "content_size": content_size,
            "content_kind": content_kind,
            "descriptor_ref": descriptor_ref,
            "source_sprint_id": source_sprint_id,
            "source_response_id": source_response_id,
            "published_at": published_at,
        }
        if any(intent[key] != value for key, value in expected.items()):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        if plan["transition_stub_name"] != transition_stub_name:
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        if (
            plan.get("operation_kind") != _LEGACY_LATEST_OPERATION_KIND
            or plan.get("write_kind") != write_kind
            or plan.get("authoritative_drain_barrier")
            != authoritative_drain_barrier
        ):
            # Do not infer a semantic kind for an old untyped operation, and
            # never let an exact operation key be replayed under a new kind.
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        ordering = intent["ordering"]
        if (
            ordering["occurrence_id"] != occurrence_id
            or ordering["operation_id"] != operation_id
            or (source_revision is not None and ordering["source_revision"] != source_revision)
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")

    def _reconcile_fixed_current(
        self,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> ExternalLatestResult | None:
        """Reconcile the one fixed current plan under latest then its op lock."""

        with self._latest_lock():
            current = self._load_current_plan()
            slot = self._load_current_slot()
            if current is None:
                if slot is not None:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                return None
            plan, raw = current
            with self._operation_lock(plan):
                observed = self._load_current_plan()
                if observed is None or observed[1] != raw or observed[0] != plan:
                    raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                return self._reconcile_plan(
                    plan,
                    raw,
                    recovered=True,
                    authoritative_drain_verifier=authoritative_drain_verifier,
                )

    def _drive_registered_plan(
        self,
        plan: dict[str, Any],
        plan_raw: bytes,
        *,
        recovered: bool,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> ExternalLatestResult:
        """Drive one registered plan while holding latest before operation."""

        with self._latest_lock():
            current = self._load_current_plan()
            current_slot_loaded = self._load_current_slot()
            if current is None and current_slot_loaded is not None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

            if current is not None and current[1] != plan_raw:
                current_plan, current_raw = current
                with self._operation_lock(current_plan):
                    observed = self._load_current_plan()
                    if (
                        observed is None
                        or observed[1] != current_raw
                        or observed[0] != current_plan
                    ):
                        raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                    self._reconcile_plan(
                        current_plan,
                        current_raw,
                        recovered=True,
                        authoritative_drain_verifier=(
                            authoritative_drain_verifier
                        ),
                    )

            with self._operation_lock(plan):
                loaded = self._load_operation_plan(
                    plan["recovery_response_id"],
                    plan["operation_id"],
                )
                if loaded is None or loaded[0] != plan or loaded[1] != plan_raw:
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                content = self._read_content(plan["latest_intent"])
                current_slot_loaded = self._load_current_slot()
                current_slot = (
                    None if current_slot_loaded is None else current_slot_loaded[0]
                )
                intent = plan["latest_intent"]
                if current_slot is not None:
                    new_order = self._order(intent)
                    old_order = self._order(current_slot)
                    if new_order < old_order:
                        return self._record_superseded(
                            plan,
                            plan_raw,
                            content,
                            current_slot,
                            recovered=recovered,
                            authoritative_drain_verifier=(
                                authoritative_drain_verifier
                            ),
                        )
                    if (
                        new_order == old_order
                        and current_slot["intent_id"] != intent["intent_id"]
                    ):
                        raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")

                current_again = self._load_current_plan()
                if current_again is None or current_again[1] != plan_raw:
                    self._install_current_plan(plan, plan_raw, content)
                result = self._reconcile_plan(
                    plan,
                    plan_raw,
                    recovered=recovered,
                    authoritative_drain_verifier=authoritative_drain_verifier,
                )
                return result

    def _complete_registered_plan(
        self,
        plan: dict[str, Any],
        plan_raw: bytes,
        *,
        recovered: bool,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> ExternalLatestResult:
        result = self._drive_registered_plan(
            plan,
            plan_raw,
            recovered=recovered,
            authoritative_drain_verifier=authoritative_drain_verifier,
        )
        self._finalize_pending(plan, plan_raw)
        return result

    def _recover_pending_results(
        self,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> list[ExternalLatestResult]:
        """Replay entries from exactly 256 fixed pending-shard current files."""

        entries: list[dict[str, Any]] = []
        for ordinal in range(_PENDING_SHARD_COUNT):
            shard_id = f"{ordinal:02x}"
            # Current-shard replacement is atomic and its immutable retained
            # generation is published first.  Startup therefore needs no
            # shard lock merely to take this bounded snapshot.
            registry = self._read_pending_registry_locked(shard_id)
            if registry is not None:
                entries.extend(
                    parse_canonical_json_v1(
                        canonical_json_bytes_v1(
                            entry,
                            maximum=_CONTROL_MAX_BYTES,
                        ),
                        maximum=_CONTROL_MAX_BYTES,
                    )
                    for entry in registry["entries"]
                )

        pending: list[tuple[dict[str, Any], dict[str, Any], bytes]] = []
        for entry in entries:
            plan, raw = self._load_pending_plan(entry)
            self._validate_pending_lower_bound(entry, plan, raw)
            pending.append((entry, plan, raw))
        pending.sort(
            key=lambda item: (
                *self._order(item[1]["latest_intent"]),
                item[0]["recovery_key"],
                item[0]["recovery_response_id"],
            )
        )
        results = []
        for _entry, plan, raw in pending:
            results.append(
                self._complete_registered_plan(
                    plan,
                    raw,
                    recovered=True,
                    authoritative_drain_verifier=authoritative_drain_verifier,
                )
            )
        return results

    def recover_pending(
        self,
        *,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> int:
        """Recover the bounded pending vector and return its operation count."""

        return len(self._recover_pending_results(authoritative_drain_verifier))

    def enrolled(self) -> bool:
        """Return whether a valid intent marker or slot enrolled the target."""

        with self._latest_lock():
            current = self._load_current_plan()
            slot = self._load_current_slot()
            if current is None and slot is not None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            if current is not None:
                self._read_content(current[0]["latest_intent"])
            return current is not None or slot is not None

    def operation_admitted(self, response_id: str, operation_id: str) -> bool:
        """Return whether this exact immutable operation plan is admitted.

        This read-only probe lets the outer runtime distinguish an enrolled
        target owned by another operation from a faulted bootstrap request
        which must be retried through its own durable private plan.  It never
        creates the per-operation lock or any filesystem artifact.
        """

        if (
            type(response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(response_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        return self._load_operation_plan(response_id, operation_id) is not None

    def recover(
        self,
        *,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
    ) -> ExternalLatestResult | None:
        """Reconcile the exact fixed current intent without directory scans."""

        pending_results = self._recover_pending_results(
            authoritative_drain_verifier
        )
        current = self._reconcile_fixed_current(authoritative_drain_verifier)
        if current is not None:
            for result in reversed(pending_results):
                if (
                    result.outcome == "published"
                    and result.latest_slot_id == current.latest_slot_id
                ):
                    return result
        return current

    def verify_prepared_publication(
        self,
        *,
        prepared_intent: Mapping[str, Any],
        publication: Mapping[str, Any] | ExternalLatestResult,
        archive_owner: Mapping[str, Any],
    ) -> ExternalLatestResult:
        """Verify a descriptor publication against durable authority records.

        This method is read-only.  It verifies the exact operation plan and
        archive owner, immutable transition bytes, typed retained slot and slot
        reference, terminal journal semantics, and the consistency of the
        current external latest bytes.  The returned result is normalized from
        durable evidence rather than trusting caller-supplied receipt fields.
        """

        if type(prepared_intent) is not dict or not isinstance(
            archive_owner, Mapping
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        intent = self._validate_intent(
            parse_canonical_json_v1(
                canonical_json_bytes_v1(
                    prepared_intent,
                    maximum=_CONTROL_MAX_BYTES,
                ),
                maximum=_CONTROL_MAX_BYTES,
            )
        )
        if intent["content_kind"] != "descriptor_stub":
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        frozen_owner = self._validate_archive_owner(
            dict(archive_owner),
            intent=intent,
        )
        assert frozen_owner is not None
        if isinstance(publication, ExternalLatestResult):
            supplied = dataclasses.asdict(publication)
        elif isinstance(publication, Mapping):
            supplied = dict(publication)
        else:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        supplied = parse_canonical_json_v1(
            canonical_json_bytes_v1(supplied, maximum=_CONTROL_MAX_BYTES),
            maximum=_CONTROL_MAX_BYTES,
        )
        result_keys = {
            "outcome",
            "intent_id",
            "latest_slot_id",
            "latest_slot_ref",
            "source_revision",
            "generation",
            "content_sha256",
            "content_size",
            "recovered",
        }
        if (
            set(supplied) != result_keys
            or type(supplied["outcome"]) is not str
            or type(supplied["recovered"]) is not bool
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")

        with self._latest_lock():
            loaded = self._load_operation_plan(
                intent["source_response_id"],
                intent["ordering"]["operation_id"],
            )
            if loaded is None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            plan, plan_raw = loaded
            if (
                plan["latest_intent"] != intent
                or plan["archive_owner"] != frozen_owner
                or plan["target_authority_id"] != self.target_authority_id
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            self._verify_archive_owner(plan)
            intended_content = self._read_content(intent)
            transition_name = plan["transition_stub_name"]
            if transition_name is None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            try:
                transition_content = _read_optional(
                    self._parent_storage,
                    self.external_latest.parent / transition_name,
                    maximum=intent["content_size"],
                )
            except PromptArchiveStorageError as exc:
                if exc.code == "PROMPT_ARCHIVE_LIMIT_EXCEEDED":
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH") from None
                raise
            if transition_content != intended_content:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

            terminal = self._load_journal(plan, plan_raw)
            if terminal is None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            journal, _journal_raw = terminal
            result_slot, result_slot_raw = self._load_retained_slot(
                journal["result_generation"],
                journal["result_latest_slot_id"],
            )
            expected_slot_ref = self._sidecar_storage.describe_blob(
                result_slot_raw,
                logical_kind="latest_slot",
                media_type=(
                    "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json"
                ),
            )
            durable_outcome: Literal["published", "superseded"] = journal["outcome"]
            allowed_supplied_outcomes = (
                {"published", "idempotent"}
                if durable_outcome == "published"
                else {"superseded"}
            )
            expected_fields = {
                "intent_id": intent["intent_id"],
                "latest_slot_id": result_slot["latest_slot_id"],
                "latest_slot_ref": expected_slot_ref,
                "source_revision": intent["ordering"]["source_revision"],
                "generation": result_slot["generation"],
                "content_sha256": intent["content_sha256"],
                "content_size": intent["content_size"],
            }
            if supplied["outcome"] not in allowed_supplied_outcomes or any(
                supplied[key] != expected
                for key, expected in expected_fields.items()
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

            current = self._load_current_slot()
            if current is None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            current_slot, _current_raw = current
            self._read_content(current_slot)
            if not self._external_matches(
                current_slot["content_sha256"],
                current_slot["content_size"],
            ):
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            return self._result(
                plan,
                result_slot,
                outcome=durable_outcome,
                recovered=False,
            )

    def prepare_legacy_intent(
        self,
        content: bytes | bytearray | memoryview,
        *,
        write_kind: Literal["transition", "identity", "state", "rollback"],
        occurrence_id: str,
        operation_id: str,
        published_at: str,
        source_response_id: str,
        source_sprint_id: str | None,
        source_revision: int,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool],
    ) -> dict[str, Any]:
        """Freeze one schema-valid legacy latest intent without publication.

        The semantic ``write_kind`` is deliberately validated here but is not
        embedded in the committed latest-intent schema.  The common outer plan
        and the target-colocated private plan/journal bind that value.  A
        durable source revision is mandatory because this read-only method
        does not reserve or mutate the project-global high-water mark.
        """

        payload = _bounded_bytes(content, self.max_content_bytes)
        self._validate_json_content(payload)
        digest = hashlib.sha256(payload).hexdigest()
        if (
            type(write_kind) is not str
            or write_kind not in _LEGACY_LATEST_WRITE_KINDS
            or type(occurrence_id) is not str
            or _SAFE_ID_RE.fullmatch(occurrence_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
            or type(source_response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(source_response_id) is None
            or (
                source_sprint_id is not None
                and (
                    type(source_sprint_id) is not str
                    or _SAFE_ID_RE.fullmatch(source_sprint_id) is None
                )
            )
            or type(source_revision) is not int
            or not 0 <= source_revision <= _MAX_REVISION
            or not callable(authoritative_drain_verifier)
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        published_at = self._validate_timestamp(published_at)

        with self._latest_lock():
            current = self._load_current_plan()
            current_slot = self._load_current_slot()
            if current is None and current_slot is not None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            bootstrap, _observed = (
                (self._bootstrap("enrolled"), None)
                if current is not None or current_slot is not None
                else self._observe_external()
            )
            intent = self._new_intent(
                content_sha256=digest,
                content_size=len(payload),
                content_kind="legacy_inline",
                descriptor_ref=None,
                source_sprint_id=source_sprint_id,
                source_response_id=source_response_id,
                source_revision=source_revision,
                occurrence_id=occurrence_id,
                operation_id=operation_id,
                published_at=published_at,
                bootstrap=bootstrap,
            )
            raw = canonical_json_bytes_v1(intent, maximum=_CONTROL_MAX_BYTES)
            intent = self._validate_intent(
                parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES)
            )
            barrier = self._authoritative_drain_barrier(published_at)
            self._invoke_authoritative_drain_verifier(
                barrier,
                authoritative_drain_verifier,
            )
            return {
                "latest_intent": intent,
                "authoritative_drain_barrier": barrier,
            }

    def prepare_descriptor_intent(
        self,
        content: bytes | bytearray | memoryview,
        *,
        transition_stub_path: str | os.PathLike[str],
        occurrence_id: str,
        operation_id: str,
        published_at: str,
        source_response_id: str,
        source_sprint_id: str,
        source_revision: int,
        descriptor_ref: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Prepare an exact authoritative latest intent without publishing it.

        The returned JSON-compatible intent is accepted later by
        :meth:`publish_prepared_descriptor`.  Preparation observes
        the enrolled/bootstrap state under the shared lock but writes no
        content, intent, slot, transition stub, or external latest bytes.  An
        explicit durable source revision is required so the plan does not
        depend on a later high-water allocation.
        """

        payload = _bounded_bytes(content, self.max_content_bytes)
        self._validate_json_content(payload)
        digest = hashlib.sha256(payload).hexdigest()
        if (
            type(occurrence_id) is not str
            or _SAFE_ID_RE.fullmatch(occurrence_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
            or type(source_response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(source_response_id) is None
            or type(source_sprint_id) is not str
            or _SAFE_ID_RE.fullmatch(source_sprint_id) is None
            or type(source_revision) is not int
            or not 0 <= source_revision <= _MAX_REVISION
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        published_at = self._validate_timestamp(published_at)
        frozen_ref = self._freeze_descriptor_ref(
            descriptor_ref,
            content_kind="descriptor_stub",
            content_sha256=digest,
            content_size=len(payload),
        )
        assert frozen_ref is not None
        transition_stub_name = self._transition_stub_leaf(transition_stub_path)

        with self._latest_lock():
            # Preparation must not reconcile an interrupted writer: that would
            # be a publication mutation.  It does, however, validate every
            # durable recovery root it observes before deriving bootstrap.
            current = self._load_current_plan()
            current_slot = self._load_current_slot()
            if current is None and current_slot is not None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            bootstrap, _observed = (
                (self._bootstrap("enrolled"), None)
                if current is not None or current_slot is not None
                else self._observe_external()
            )
            intent = self._new_intent(
                content_sha256=digest,
                content_size=len(payload),
                content_kind="descriptor_stub",
                descriptor_ref=frozen_ref,
                source_sprint_id=source_sprint_id,
                source_response_id=source_response_id,
                source_revision=source_revision,
                occurrence_id=occurrence_id,
                operation_id=operation_id,
                published_at=published_at,
                bootstrap=bootstrap,
            )
            # Round-trip through the canonical parser now.  The caller can
            # persist this exact value and publication will reject any change.
            raw = canonical_json_bytes_v1(intent, maximum=_CONTROL_MAX_BYTES)
            return self._validate_intent(
                parse_canonical_json_v1(raw, maximum=_CONTROL_MAX_BYTES)
            )

    def publish_prepared_descriptor(
        self,
        content: bytes | bytearray | memoryview,
        *,
        prepared_intent: Mapping[str, Any],
        transition_stub_path: str | os.PathLike[str],
        archive_owner: Mapping[str, Any],
        expected_target_authority_id: str,
    ) -> ExternalLatestResult:
        """Publish one exact intent returned by ``prepare_descriptor_intent``.

        The fixed current intent is made durable before the immutable
        transition stub.  The stub is created or byte-verified before mutable
        ``latest-response.json`` and is therefore reproducible by ``recover``
        after every injected boundary fault.
        """

        payload = _bounded_bytes(content, self.max_content_bytes)
        self._validate_json_content(payload)
        if (
            type(expected_target_authority_id) is not str
            or _TARGET_AUTHORITY_ID_RE.fullmatch(expected_target_authority_id) is None
            or expected_target_authority_id != self.target_authority_id
        ):
            raise _error("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if type(prepared_intent) is not dict:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        try:
            intent_raw = canonical_json_bytes_v1(
                prepared_intent,
                maximum=_CONTROL_MAX_BYTES,
            )
        except PromptArchiveStorageError:
            raise
        intent = self._validate_intent(
            parse_canonical_json_v1(intent_raw, maximum=_CONTROL_MAX_BYTES)
        )
        transition_stub_name = self._transition_stub_leaf(transition_stub_path)
        if not isinstance(archive_owner, Mapping):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        frozen_owner = self._validate_archive_owner(
            dict(archive_owner),
            intent=intent,
        )
        assert frozen_owner is not None
        plan = self._plan_for_intent(
            intent,
            transition_stub_name=transition_stub_name,
            archive_owner=frozen_owner,
        )
        plan_raw = canonical_json_bytes_v1(plan, maximum=_CONTROL_MAX_BYTES)
        plan = self._parse_plan_bytes(plan_raw)
        if (
            intent["content_kind"] != "descriptor_stub"
            or len(payload) != intent["content_size"]
            or hashlib.sha256(payload).hexdigest() != intent["content_sha256"]
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        self.recover_pending()
        recovered = False
        with self._latest_lock():
            with self._operation_lock(plan):
                self._verify_archive_owner(plan)
                existing = self._load_operation_plan(
                    plan["recovery_response_id"],
                    plan["operation_id"],
                )
                if existing is not None:
                    existing_plan, existing_raw = existing
                    if existing_raw != plan_raw or existing_plan != plan:
                        raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                    if self._read_content(intent) != payload:
                        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                    recovered = True
                else:
                    self._publish_operation_artifacts(plan, plan_raw, payload)

        self._register_pending(plan, plan_raw)
        return self._complete_registered_plan(
            plan,
            plan_raw,
            recovered=recovered,
        )

    def publish_prepared_legacy(
        self,
        content: bytes | bytearray | memoryview,
        *,
        prepared: Mapping[str, Any],
        write_kind: Literal["transition", "identity", "state", "rollback"],
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool],
    ) -> ExternalLatestResult:
        """Publish an outer-plan-owned typed legacy latest operation.

        The common runtime persists the exact intent, payload proof, semantic
        write kind, and drain barrier before calling this method.  This method
        cross-binds those immutable values into the colocated private recovery
        plan and rechecks the drain proof under ``latest.lock`` immediately
        before any incomplete operation can advance.
        """

        payload = _bounded_bytes(content, self.max_content_bytes)
        self._validate_json_content(payload)
        if (
            type(write_kind) is not str
            or write_kind not in _LEGACY_LATEST_WRITE_KINDS
            or type(prepared) is not dict
            or set(prepared)
            != {"latest_intent", "authoritative_drain_barrier"}
            or not callable(authoritative_drain_verifier)
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        intent = self._validate_intent(
            parse_canonical_json_v1(
                canonical_json_bytes_v1(
                    prepared["latest_intent"],
                    maximum=_CONTROL_MAX_BYTES,
                ),
                maximum=_CONTROL_MAX_BYTES,
            )
        )
        if (
            intent["content_kind"] != "legacy_inline"
            or intent["descriptor_ref"] is not None
            or len(payload) != intent["content_size"]
            or hashlib.sha256(payload).hexdigest() != intent["content_sha256"]
        ):
            raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        barrier = self._validate_authoritative_drain_barrier(
            parse_canonical_json_v1(
                canonical_json_bytes_v1(
                    prepared["authoritative_drain_barrier"],
                    maximum=_CONTROL_MAX_BYTES,
                ),
                maximum=_CONTROL_MAX_BYTES,
            )
        )
        plan = self._plan_for_intent(
            intent,
            transition_stub_name=None,
            archive_owner=None,
            write_kind=write_kind,
            authoritative_drain_barrier=barrier,
        )
        plan_raw = canonical_json_bytes_v1(plan, maximum=_CONTROL_MAX_BYTES)
        plan = self._parse_plan_bytes(plan_raw)

        self.recover_pending(
            authoritative_drain_verifier=authoritative_drain_verifier
        )
        self._reconcile_fixed_current(authoritative_drain_verifier)
        recovered = False
        with self._latest_lock():
            with self._operation_lock(plan):
                existing = self._load_operation_plan(
                    plan["recovery_response_id"],
                    plan["operation_id"],
                )
                if existing is not None:
                    existing_plan, existing_raw = existing
                    if existing_raw != plan_raw or existing_plan != plan:
                        raise _error("PROMPT_ARCHIVE_GENERATION_CONFLICT")
                    if self._read_content(intent) != payload:
                        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                    recovered = True
                else:
                    self._publish_operation_artifacts(plan, plan_raw, payload)

        self._register_pending(plan, plan_raw)
        return self._complete_registered_plan(
            plan,
            plan_raw,
            recovered=recovered,
            authoritative_drain_verifier=authoritative_drain_verifier,
        )

    def publish(
        self,
        content: bytes | bytearray | memoryview,
        *,
        write_kind: Literal["transition", "identity", "state", "rollback"],
        occurrence_id: str,
        operation_id: str,
        published_at: str,
        source_response_id: str,
        source_sprint_id: str | None = None,
        source_revision: int | None = None,
        content_kind: Literal["legacy_inline", "descriptor_stub"] = "legacy_inline",
        descriptor_ref: Mapping[str, Any] | None = None,
        authoritative_drain_verifier: Callable[[Mapping[str, Any]], bool]
        | None = None,
        pre_enrollment_verifier: Callable[[], bool] | None = None,
    ) -> ExternalLatestResult:
        """Publish one typed predecessor-independent legacy latest intent.

        ``source_revision=None`` allocates ``high_water + 1`` while holding the
        physical latest lock.  A caller-supplied revision is accepted only as
        part of the caller's already-durable occurrence and participates in the
        frozen lexicographic order.  ``write_kind`` is part of every durable
        identity and replay comparison; an operation cannot be relabelled on
        retry.  Operator rollback deliberately uses this same protocol with
        ``write_kind="rollback"`` rather than an out-of-band overwrite.

        ``pre_enrollment_verifier`` is the atomic first-write seam used by a
        caller which selected an un-enrolled path before entering this method.
        When supplied, recovery and the final precondition both invoke it
        while ``latest.lock`` is held.  Enrollment rejects a new operation,
        while an exact operation plan already admitted by this same bootstrap
        attempt may finish/replay after a crash.  A verifier result other than
        the literal ``True`` rejects before new artifacts.  Omitting the
        verifier preserves the ordinary rootless typed coordinator protocol,
        including subsequent ordered writes after enrollment.
        """

        payload = _bounded_bytes(content, self.max_content_bytes)
        self._validate_json_content(payload)
        digest = hashlib.sha256(payload).hexdigest()
        if (
            type(write_kind) is not str
            or write_kind not in _LEGACY_LATEST_WRITE_KINDS
            or type(occurrence_id) is not str
            or _SAFE_ID_RE.fullmatch(occurrence_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
            or type(source_response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(source_response_id) is None
            or (
                source_sprint_id is not None
                and (
                    type(source_sprint_id) is not str
                    or _SAFE_ID_RE.fullmatch(source_sprint_id) is None
                )
            )
            or (
                source_revision is not None
                and (
                    type(source_revision) is not int
                    or not 0 <= source_revision <= _MAX_REVISION
                )
            )
            or (
                pre_enrollment_verifier is not None
                and not callable(pre_enrollment_verifier)
            )
        ):
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        published_at = self._validate_timestamp(published_at)
        frozen_ref = self._freeze_descriptor_ref(
            descriptor_ref,
            content_kind=content_kind,
            content_sha256=digest,
            content_size=len(payload),
        )
        if content_kind == "descriptor_stub" and source_sprint_id is None:
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")
        if content_kind == "descriptor_stub":
            # Authoritative descriptor publication requires the archive-owner
            # cross-binding accepted only by ``publish_prepared_descriptor``.
            raise _error("PROMPT_ARCHIVE_REQUEST_INVALID")

        barrier = self._authoritative_drain_barrier(published_at)
        drain_verifier = authoritative_drain_verifier
        if drain_verifier is None:
            # Compatibility for callers which have not yet adopted the common
            # outer operation.  The production common-runtime path uses
            # ``prepare_legacy_intent``/``publish_prepared_legacy`` and always
            # supplies the authoritative registry verifier.
            drain_verifier = lambda observed: (
                self._validate_authoritative_drain_barrier(dict(observed))
                == dict(observed)
            )

        if pre_enrollment_verifier is not None:
            selected_drain_verifier = drain_verifier

            def guarded_drain_verifier(observed: Mapping[str, Any]) -> bool:
                self._invoke_pre_enrollment_verifier(
                    pre_enrollment_verifier
                )
                try:
                    accepted = selected_drain_verifier(observed)
                except PromptArchiveStorageError:
                    raise
                except Exception:
                    raise _error("PROMPT_ARCHIVE_UNAVAILABLE") from None
                return accepted is True

            drain_verifier = guarded_drain_verifier

        self.recover_pending(authoritative_drain_verifier=drain_verifier)
        self._reconcile_fixed_current(drain_verifier)
        recovered = False
        with self._latest_lock():
            current = self._load_current_plan()
            current_slot_loaded = self._load_current_slot()
            if current is None and current_slot_loaded is not None:
                raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            if pre_enrollment_verifier is not None:
                self._invoke_pre_enrollment_verifier(
                    pre_enrollment_verifier
                )
            current_slot = (
                None if current_slot_loaded is None else current_slot_loaded[0]
            )
            preexisting_operation: tuple[dict[str, Any], bytes] | None = None
            if (
                pre_enrollment_verifier is not None
                and (current is not None or current_slot_loaded is not None)
            ):
                # Do this read before taking the per-operation lock.  Creating
                # that lock also creates the operation directory, which would
                # leave an artifact for a new request rejected solely because
                # the target is already enrolled.  An exact crash-replay has
                # an immutable operation plan and may proceed.
                preexisting_operation = self._load_operation_plan(
                    source_response_id,
                    operation_id,
                )
                if preexisting_operation is None:
                    raise _error("PROMPT_ARCHIVE_UNAVAILABLE")
            with self._operation_lock_ids(source_response_id, operation_id):
                existing = self._load_operation_plan(
                    source_response_id,
                    operation_id,
                )
                if preexisting_operation is not None and (
                    existing is None or existing[1] != preexisting_operation[1]
                ):
                    raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                if existing is not None:
                    plan, raw = existing
                    self._assert_request_matches_plan(
                        plan,
                        content_sha256=digest,
                        content_size=len(payload),
                        content_kind=content_kind,
                        descriptor_ref=frozen_ref,
                        source_sprint_id=source_sprint_id,
                        source_response_id=source_response_id,
                        source_revision=source_revision,
                        occurrence_id=occurrence_id,
                        operation_id=operation_id,
                        published_at=published_at,
                        transition_stub_name=None,
                        write_kind=write_kind,
                        authoritative_drain_barrier=barrier,
                    )
                    if self._read_content(plan["latest_intent"]) != payload:
                        raise _error("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
                    recovered = True
                else:
                    high_water = 0
                    if current is not None:
                        high_water = current[0]["latest_intent"]["ordering"][
                            "source_revision"
                        ]
                    if current_slot is not None:
                        high_water = max(
                            high_water,
                            current_slot["ordering"]["source_revision"],
                        )
                    revision = source_revision
                    if revision is None:
                        if high_water >= _MAX_REVISION:
                            raise _error("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
                        revision = high_water + 1
                    bootstrap, _observed = (
                        (self._bootstrap("enrolled"), None)
                        if current is not None or current_slot is not None
                        else self._observe_external()
                    )
                    plan = self._new_plan(
                        content_sha256=digest,
                        content_size=len(payload),
                        content_kind=content_kind,
                        descriptor_ref=frozen_ref,
                        source_sprint_id=source_sprint_id,
                        source_response_id=source_response_id,
                        source_revision=revision,
                        occurrence_id=occurrence_id,
                        operation_id=operation_id,
                        published_at=published_at,
                        bootstrap=bootstrap,
                        write_kind=write_kind,
                        authoritative_drain_barrier=barrier,
                    )
                    raw = canonical_json_bytes_v1(
                        plan,
                        maximum=_CONTROL_MAX_BYTES,
                    )
                    self._publish_operation_artifacts(plan, raw, payload)

        self._register_pending(plan, raw)
        return self._complete_registered_plan(
            plan,
            raw,
            recovered=recovered,
            authoritative_drain_verifier=drain_verifier,
        )


__all__ = [
    "ExternalLatestCoordinator",
    "ExternalLatestResult",
    "FAULT_AFTER_CURRENT_INSTALL",
    "FAULT_AFTER_OPERATION_PLAN",
    "FAULT_AFTER_PENDING_BIND",
    "FAULT_AFTER_TERMINAL_BEFORE_PENDING_REMOVE",
    "FAULT_STAGES",
    "publish_authoritative_transition_stub",
]
