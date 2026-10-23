"""Offline, fail-closed migration tooling for Prompt Archive V1.

The runtime deliberately does not auto-discover legacy prompt trees.  This
module accepts an explicit isolated source root and an explicit archive root,
binds the source to a path-free durable handle and full-file inventory, and
publishes byte-exact legacy archives through :mod:`prompt_archive_incremental`.

Source files are never written or deleted.  All durable control documents are
path-free; an operator must re-supply the same root and handle label to resume.
"""

from __future__ import annotations

import copy
import ctypes
import dataclasses
import hashlib
import json
import os
import re
import secrets
import stat
import struct
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

from .prompt_archive_incremental import (
    ArchiveOccurrence,
    pas1_id_v1,
    response_id_v1,
)
from .prompt_archive_runtime import PromptArchiveRuntime, PromptArchiveRuntimeConfig
from .prompt_archive_storage import (
    _PinnedDirectory,
    _atomic_rename_pinned_no_replace,
    _storage_temporary_target_name,
    _windows_mark_file_for_delete,
    _windows_open_or_create_relative_directory,
    _windows_open_pinned_directory,
    _windows_path_from_handle,
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
    parse_canonical_json_v1,
    resolved_path_from_open_descriptor,
)


_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$", re.ASCII)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_BATCH_ID_RE: Final = re.compile(r"^parmb1-[0-9a-f]{64}$", re.ASCII)
_INVENTORY_ID_RE: Final = re.compile(r"^parinv1-[0-9a-f]{64}$", re.ASCII)
_OPERATION_ID_RE: Final = re.compile(r"^parop1-[0-9a-f]{32}$", re.ASCII)
_ENTRY_ID_RE: Final = re.compile(r"^parme1-[0-9a-f]{64}$", re.ASCII)
_RESPONSE_ID_RE: Final = re.compile(r"^parresp1-[0-9a-f]{64}$", re.ASCII)
_ARCHIVE_ID_RE: Final = re.compile(r"^par1-[0-9a-f]{64}$", re.ASCII)
_POINTER_ID_RE: Final = re.compile(r"^parptr1-[0-9a-f]{64}$", re.ASCII)
_MIGRATION_OCCURRENCE_ID_RE: Final = re.compile(
    r"^migration-[0-9a-f]{64}$", re.ASCII
)
_DECISION_OCCURRENCE_ID_RE: Final = re.compile(
    r"^decision-[0-9a-f]{64}$", re.ASCII
)
_UTC_TIMESTAMP_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    re.ASCII,
)
_REPARSE_POINT: Final = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
_INVENTORY_PREFIX: Final = b"prompt-archive-isolated-inventory-v1\0"
_INVENTORY_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json"
)
_EVIDENCE_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-migration-evidence-v1+json"
)
_BATCH_PLAN_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-migration-batch-plan-v1+json"
)
_BATCH_JOURNAL_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-migration-batch-journal-v1+json"
)
_PRIVATE_EVIDENCE_MEDIA: Final = (
    "application/vnd.nginx-qa.prompt-archive-migration-private-evidence-v1+json"
)
_MAX_CONTROL_BYTES: Final = 8 * 1024 * 1024
_MAX_PRIVATE_EVIDENCE_BYTES: Final = 512 * 1024 * 1024
_MAX_FILES: Final = 1_000_000
_MAX_DIRECTORIES: Final = 100_000
_MAX_DIRECTORY_DEPTH: Final = 256
_MAX_FILE_BYTES: Final = 512 * 1024 * 1024
_MAX_TOTAL_BYTES: Final = 64 * 1024 * 1024 * 1024
# One final and one abandoned publication temp per possible journal generation.
_MAX_JOURNAL_GENERATION_CHILDREN: Final = 2 * (_MAX_FILES + 2)
OFFLINE_OPERATOR_GATE: Final = "prompt-archive-offline-migration-v1"
TOOL_VERSION: Final = "prompt-archive-migration-v1"
CONTRACT_VERSION: Final = "prompt-archive-offline-migration-contract-v1"
_PROTECTED_ENV_NAMES: Final = (
    "NGINX_QA_SERVICE_ROOT",
    "NGINX_QA_RUNTIME_ROOT",
    "NGINX_QA_MANAGED_ROOT",
    "NGINX_QA_PROMPT_ROOT",
    "NGINX_QA_PROMPT_ARCHIVE_ROOT",
)
_PERSISTED_PROMPT_SETTINGS_MAX_BYTES: Final = 64 * 1024
_PROMPT_TEMPLATE_PLACEHOLDERS: Final = {
    "repository",
    "project",
    "git_context_key",
    "agent_phone",
}


class _JsonObjectPairs(list[tuple[str, Any]]):
    """Retain object members while inspecting only the top-level kind."""


def _reject_nonstandard_json_constant(_value: str) -> Any:
    raise ValueError


def _assert_legacy_input_bytes(raw: bytes, *, maximum: int) -> None:
    """Fail closed when a selected legacy input carries a top-level kind.

    Offline migration preserves legacy source bytes opaquely.  Invalid JSON is
    therefore still valid legacy evidence, as it was before this discriminator
    existed.  A syntactically valid JSON object with a top-level ``kind`` is
    not opaque, however: it is either an authoritative descriptor/control
    document or a future/unknown format.  Neither may be migrated as if its
    local pathname authorized hydration of the referenced archive.

    ``object_pairs_hook`` is intentional.  It preserves duplicate top-level
    ``kind`` members while leaving duplicate non-control members untouched in
    the original bytes.  The caller has already bounded the file read; this
    helper repeats the bound so it is safe to use independently in tests.
    """

    if type(maximum) is not int or maximum <= 0 or len(raw) > maximum:
        raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
    try:
        top_level = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_JsonObjectPairs,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (
        UnicodeError,
        json.JSONDecodeError,
        ValueError,
    ):
        # Raw-byte migration has always retained malformed/opaque legacy
        # evidence.  Do not turn this dispatch preflight into canonicalization.
        return
    except (RecursionError, MemoryError):
        # Resource exhaustion is not proof that the input is opaque legacy
        # evidence.  It must not turn an authoritative/unknown kind into the
        # no-kind branch.
        raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED") from None
    if not isinstance(top_level, _JsonObjectPairs):
        # Non-object JSON has no top-level object discriminator.  It remains
        # opaque legacy evidence under the existing preservation contract.
        return
    kind_members = [value for key, value in top_level if key == "kind"]
    if kind_members:
        # This includes the canonical descriptor kind, every unknown/non-string
        # kind, and duplicate kind members.  The offline tool never follows a
        # stub and never treats possession of its source path as authorization.
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")


class PromptArchiveMigrationError(RuntimeError):
    """Stable, payload-free migration failure."""

    _CODES = {
        "PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID",
        "PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP",
        "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE",
        "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED",
        "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT",
        "PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH",
        "PROMPT_ARCHIVE_MIGRATION_NOT_COMPLETE",
        "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED",
        "PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE",
    }

    def __init__(self, code: str) -> None:
        if code not in self._CODES:
            code = "PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE"
        super().__init__("Prompt archive migration failed safely.")
        self.code = code


def _error(code: str) -> PromptArchiveMigrationError:
    return PromptArchiveMigrationError(code)


def _canonical_bytes(value: Any, *, maximum: int) -> bytes:
    """Map canonical-control bounds and shape failures to migration errors."""

    try:
        return canonical_json_bytes_v1(value, maximum=maximum)
    except PromptArchiveStorageError as exc:
        raise _error_from_storage(exc) from None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _is_redirected(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    return path.is_symlink() or bool(
        _REPARSE_POINT and getattr(metadata, "st_file_attributes", 0) & _REPARSE_POINT
    )


def _validate_component(component: str) -> None:
    if component in {"", ".", ".."} or any(
        marker in component for marker in ("/", "\\", "\0")
    ):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    try:
        component.encode("utf-8", errors="strict")
    except UnicodeError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE") from None
    if ":" in component or component.endswith((" ", ".")):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    stem = component.split(".", 1)[0].casefold()
    if stem in {
        "con",
        "prn",
        "aux",
        "nul",
        *(f"com{number}" for number in range(1, 10)),
        *(f"lpt{number}" for number in range(1, 10)),
    }:
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")


def _claim_casefolded_path(seen: set[str], relative_path: str) -> None:
    folded = relative_path.casefold()
    if folded in seen:
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    seen.add(folded)


def _valid_root_identity(value: Any) -> bool:
    if type(value) is not dict:
        return False
    platform = value.get("platform")
    if platform == "posix-dev-inode-v1":
        return bool(
            set(value)
            == {"identity_version", "platform", "st_dev", "st_ino"}
            and value.get("identity_version")
            == "prompt-archive-root-identity-v1"
            and type(value.get("st_dev")) is str
            and value["st_dev"].isdigit()
            and type(value.get("st_ino")) is str
            and value["st_ino"].isdigit()
        )
    if platform == "windows-file-id-v1":
        return bool(
            set(value)
            == {
                "identity_version",
                "platform",
                "volume_serial_hex",
                "file_id_hex",
            }
            and value.get("identity_version")
            == "prompt-archive-root-identity-v1"
            and type(value.get("volume_serial_hex")) is str
            and re.fullmatch(r"[0-9a-f]{16}", value["volume_serial_hex"])
            is not None
            and type(value.get("file_id_hex")) is str
            and re.fullmatch(r"[0-9a-f]{32}", value["file_id_hex"])
            is not None
        )
    return False


def _valid_file_identity(value: Any) -> bool:
    if type(value) is not dict:
        return False
    platform = value.get("platform")
    if platform == "posix-dev-inode-v1":
        return bool(
            set(value)
            == {"identity_version", "platform", "st_dev", "st_ino"}
            and value.get("identity_version")
            == "prompt-archive-file-identity-v1"
            and type(value.get("st_dev")) is str
            and value["st_dev"].isdigit()
            and type(value.get("st_ino")) is str
            and value["st_ino"].isdigit()
        )
    if platform == "windows-stat-file-id-v1":
        return bool(
            set(value)
            == {
                "identity_version",
                "platform",
                "st_dev_hex",
                "st_ino_hex",
            }
            and value.get("identity_version")
            == "prompt-archive-file-identity-v1"
            and type(value.get("st_dev_hex")) is str
            and re.fullmatch(r"[0-9a-f]{16}", value["st_dev_hex"])
            is not None
            and type(value.get("st_ino_hex")) is str
            and re.fullmatch(r"[0-9a-f]{32}", value["st_ino_hex"])
            is not None
        )
    return False


def _windows_drive_device(drive: str) -> str:
    if os.name != "nt" or re.fullmatch(r"[A-Za-z]:", drive) is None:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    query = kernel32.QueryDosDeviceW
    query.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    query.restype = wintypes.DWORD
    buffer = ctypes.create_unicode_buffer(32768)
    written = query(drive, buffer, len(buffer))
    if not written:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    targets = [value for value in buffer[:written].split("\0") if value]
    if len(targets) != 1:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    return targets[0]


def _windows_reject_aliased_namespace(raw: str, path: Path) -> None:
    if os.name != "nt":
        return
    folded = raw.replace("/", "\\").casefold()
    if folded.startswith(("\\\\?\\", "\\\\.\\", "\\??\\", "\\\\")):
        # Extended/device/UNC spellings can alias a protected local tree.
        # This offline tool accepts only ordinary local drive-letter roots.
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    device = _windows_drive_device(path.drive).casefold()
    if not device.startswith("\\device\\harddiskvolume"):
        # SUBST and other DOS-device aliases are denied without opening their
        # target.  Normal fixed volumes resolve to \Device\HarddiskVolume*.
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")


def _absolute_root(value: str | os.PathLike[str]) -> Path:
    raw = os.fspath(value)
    if not isinstance(raw, str):
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    path = Path(value)
    if not path.is_absolute():
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    _windows_reject_aliased_namespace(raw, path)
    for component in path.parts[len(Path(path.anchor).parts) :]:
        _validate_component(component)
        if os.name == "nt" and "~" in component:
            # An 8.3 alias could name a protected tree.  Reject it from the
            # lexical request before any existence or canonicalization probe.
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    return Path(os.path.normpath(os.path.abspath(path)))


def _path_key(path: Path) -> str:
    return os.path.normcase(str(path))


def _isolation_path_key(path: Path) -> str:
    if os.name != "nt":
        return _path_key(path)
    try:
        tail = path.relative_to(Path(path.anchor))
    except ValueError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID") from None
    device = _windows_drive_device(path.drive).rstrip("\\")
    return os.path.normcase(str(Path(device).joinpath(*tail.parts)))


def _overlap(left: Path, right: Path) -> bool:
    left_key = _isolation_path_key(left)
    right_key = _isolation_path_key(right)
    try:
        common = os.path.commonpath((left_key, right_key))
    except ValueError:
        return False
    return common in {left_key, right_key}


def _prompt_template_protected_base(
    value: Any,
    *,
    file_template: bool,
) -> Path | None:
    """Return the owning root for one valid persisted prompt template.

    Invalid persisted fields fall back to the built-in ``D:\\Prompt`` in the
    runtime reader, which is already unconditionally protected below.  A valid
    custom field must additionally protect its non-variable owning prefix.
    """

    if type(value) is not str or not value.strip() or len(value) > 1000:
        return None
    template = value.strip()
    if "\0" in template:
        return None
    placeholders = set(re.findall(r"\{([^{}]+)\}", template))
    allowed = (
        _PROMPT_TEMPLATE_PLACEHOLDERS
        if file_template
        else _PROMPT_TEMPLATE_PLACEHOLDERS - {"agent_phone"}
    )
    if placeholders - allowed:
        return None
    remainder = template
    for placeholder in _PROMPT_TEMPLATE_PLACEHOLDERS:
        remainder = remainder.replace("{" + placeholder + "}", "")
    if "{" in remainder or "}" in remainder:
        return None
    expanded = os.path.expandvars(os.path.expanduser(template))
    if any(part == ".." for part in re.split(r"[\\/]", expanded)):
        return None
    if file_template:
        filename = re.split(r"[\\/]", expanded)[-1]
        if (
            "{agent_phone}" not in filename
            or not filename.casefold().endswith(".prompt")
        ):
            return None
    marker = "__nginx_qa_prompt_template_value__"
    concrete = expanded
    for placeholder in _PROMPT_TEMPLATE_PLACEHOLDERS:
        concrete = concrete.replace("{" + placeholder + "}", marker)
    path = Path(concrete)
    if not path.is_absolute():
        return None
    for index, component in enumerate(path.parts):
        if marker in component:
            if index == 0:
                return None
            return _absolute_root(Path(*path.parts[:index]))
    return _absolute_root(path.parent if file_template else path)


def _persisted_prompt_settings_path() -> Path:
    return (
        Path(__file__).absolute().parents[1]
        / "runtime_state"
        / "sequential-prompt-settings.json"
    )


def _persisted_prompt_protected_roots() -> tuple[Path, ...]:
    """Read only the local persisted prompt-template configuration snapshot."""

    settings_path = _persisted_prompt_settings_path()
    try:
        metadata = settings_path.lstat()
    except FileNotFoundError:
        return ()
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID") from None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or _is_redirected(settings_path)
        or metadata.st_size > _PERSISTED_PROMPT_SETTINGS_MAX_BYTES
    ):
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    try:
        raw, _observed = _read_regular_file(
            settings_path,
            maximum=_PERSISTED_PROMPT_SETTINGS_MAX_BYTES,
        )
        document = json.loads(raw.decode("utf-8", errors="strict"))
    except PromptArchiveMigrationError:
        raise
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
        # The runtime falls back to D:\Prompt for malformed settings.  That
        # default is already protected, so no custom base is trusted here.
        return ()
    if type(document) is not dict:
        return ()
    roots = []
    for name, file_template in (
        ("directory_template", False),
        ("agent_latest_file_template", True),
    ):
        root = _prompt_template_protected_base(
            document.get(name),
            file_template=file_template,
        )
        if root is not None:
            roots.append(root)
    return tuple(roots)


def _assert_existing_chain_safe(path: Path) -> None:
    absolute = _absolute_root(path)
    anchor = Path(absolute.anchor)
    current = anchor
    for component in absolute.relative_to(anchor).parts:
        current = current / component
        try:
            current.lstat()
        except FileNotFoundError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        except OSError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
        if _is_redirected(current):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")


def _configured_protected_roots(
    explicit: Sequence[str | os.PathLike[str]],
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    if isinstance(explicit, (str, bytes)) or not isinstance(explicit, Sequence):
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    raw_values: list[tuple[str | os.PathLike[str], bool]] = [
        (value, True) for value in explicit
    ]
    for name in _PROTECTED_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            raw_values.append((value, True))
    combined = os.environ.get("NGINX_QA_PROTECTED_ROOTS")
    if combined:
        try:
            values = json.loads(combined)
        except (TypeError, ValueError, json.JSONDecodeError):
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID") from None
        if not isinstance(values, list) or any(
            not isinstance(value, str) or not value.strip() for value in values
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        raw_values.extend((value.strip(), True) for value in values)
    # The known legacy live root is denied lexically; it is never opened merely
    # to decide whether an isolated request is allowed.
    if os.name == "nt":
        raw_values.append((r"D:\Prompt", False))
    raw_values.extend(
        (value, True) for value in _persisted_prompt_protected_roots()
    )
    raw_values.append((Path(__file__).absolute().parents[1], True))
    roots: list[Path] = []
    alias_sensitive: list[Path] = []
    seen: dict[str, int] = {}
    for raw, inspect_alias in raw_values:
        root = _absolute_root(raw)
        if os.name == "nt" and any("~" in component for component in root.parts):
            # A deny root supplied only through a legacy alias cannot be
            # canonicalized without probing the protected tree.  Fail closed.
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        key = _path_key(root)
        if key not in seen:
            seen[key] = len(roots)
            roots.append(root)
        if inspect_alias and all(_path_key(value) != key for value in alias_sensitive):
            alias_sensitive.append(root)
    return tuple(roots), tuple(alias_sensitive)


def _assert_roots_isolated(
    source: Path,
    archive: Path,
    protected: Sequence[Path],
    *,
    reconstruction: Path | None = None,
) -> None:
    candidates = [source, archive]
    if reconstruction is not None:
        candidates.append(reconstruction)
    for index, left in enumerate(candidates):
        for right in candidates[index + 1 :]:
            if _overlap(left, right):
                raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
        # Configured/live roots are a lexical deny list on every platform.
        # Windows performs an additional handle-canonical comparison below,
        # but a non-Windows caller must not silently bypass this boundary.
        for denied in protected:
            if _overlap(left, denied):
                raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")


def _canonical_candidate_path(path: Path) -> Path:
    """Resolve an allowed candidate through a pinned nearest-existing handle."""

    if os.name != "nt":
        return path
    cursor = path
    suffix: list[str] = []
    while True:
        try:
            cursor.lstat()
        except FileNotFoundError:
            pass
        except OSError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
        else:
            if _is_redirected(cursor):
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
            break
        if cursor == cursor.parent:
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        suffix.append(cursor.name)
        cursor = cursor.parent
    _assert_existing_chain_safe(cursor)
    try:
        cursor_metadata = cursor.lstat()
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    if not stat.S_ISDIR(cursor_metadata.st_mode):
        if suffix:
            # A missing candidate below an existing regular/special leaf can
            # never be created safely.  Reject during access preflight rather
            # than letting storage publication occur before the impossible
            # output shape is discovered.
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        # A private-evidence leaf is itself an output authority.  Reject its
        # ADS ambiguity during canonical access preflight, before archive
        # storage can be bootstrapped or a plan/inventory can be published.
        _windows_reject_named_streams(cursor)
        suffix.append(cursor.name)
        cursor = cursor.parent
        _assert_existing_chain_safe(cursor)
    _windows_reject_named_streams(cursor, directory=True)
    with _RootPin(cursor) as pinned:
        if pinned.resolved_path is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
        resolved = pinned.resolved_path.joinpath(*reversed(suffix))
        pinned.assert_stable()
    return Path(os.path.normpath(os.path.abspath(resolved)))


def _assert_canonical_candidates_isolated(
    candidates: Sequence[Path],
    protected: Sequence[Path],
    alias_sensitive_protected: Sequence[Path] = (),
) -> None:
    if os.name != "nt":
        return
    # Preserve the no-touch lexical deny path: a candidate visibly nested in a
    # protected root is rejected before any protected-root metadata probe.
    for left in candidates:
        for denied in protected:
            if _overlap(left, denied):
                raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
    canonical = tuple(_canonical_candidate_path(path) for path in candidates)
    # Canonicalize through the nearest existing parent even when the deny leaf
    # does not yet exist.  Otherwise ``junction\future-live`` could be skipped
    # and a candidate using the junction target spelling could create that
    # supposedly protected subtree.  _canonical_candidate_path never resolves
    # through a reparse point; it fails closed on that ancestor.
    canonical_denied = [
        _canonical_candidate_path(denied)
        for denied in alias_sensitive_protected
    ]
    for index, left in enumerate(canonical):
        for right in canonical[index + 1 :]:
            if _overlap(left, right):
                raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
        for denied in protected:
            if _overlap(left, denied):
                raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
        for denied in canonical_denied:
            if _overlap(left, denied):
                raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")


@dataclasses.dataclass(frozen=True, slots=True)
class InventoryEntry:
    relative_path: str
    path_sha256: str
    byte_size: int
    sha256: str
    mtime_ns: int
    file_identity: Mapping[str, Any]
    identity_sha256: str
    link_count: int

    def document(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "path_sha256": self.path_sha256,
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "mtime_ns": self.mtime_ns,
            "file_identity": copy.deepcopy(dict(self.file_identity)),
            "identity_sha256": self.identity_sha256,
            "link_count": self.link_count,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class SourceInventory:
    source_root_identity: Mapping[str, Any]
    source_root_identity_sha256: str
    inventory_sha256: str
    safety_snapshot_sha256: str
    total_bytes: int
    entries: tuple[InventoryEntry, ...]


class _RootPin:
    def __init__(self, path: Path, *, descriptor: int | None = None) -> None:
        self.path = path
        self.descriptor = -1 if descriptor is None else descriptor
        self._windows_handle: int | None = None
        self.identity: dict[str, Any] | None = None
        self.resolved_path: Path | None = None

    def __enter__(self) -> "_RootPin":
        if os.name == "nt":
            if self.descriptor != -1:
                self.close()
                raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
            self._open_windows()
        else:
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            if not nofollow:
                raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
            try:
                if self.descriptor == -1:
                    self.descriptor = os.open(
                        self.path,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow,
                    )
                metadata = os.fstat(self.descriptor)
            except OSError:
                self.close()
                raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
            if not stat.S_ISDIR(metadata.st_mode):
                self.close()
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
            self.identity = {
                "identity_version": "prompt-archive-root-identity-v1",
                "platform": "posix-dev-inode-v1",
                "st_dev": str(metadata.st_dev),
                "st_ino": str(metadata.st_ino),
            }
            self.resolved_path = resolved_path_from_open_descriptor(self.descriptor)
            if self.resolved_path is None:
                self.close()
                raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
            if not _same_path(self.resolved_path, self.path):
                self.close()
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        return self

    def _open_windows(self) -> None:
        from ctypes import wintypes

        class FileIdInfo(ctypes.Structure):
            _fields_ = (
                ("VolumeSerialNumber", ctypes.c_ulonglong),
                ("FileId", ctypes.c_ubyte * 16),
            )

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
            str(self.path),
            0x00000080 | 0x00100000,
            # Deliberately omit FILE_SHARE_DELETE: while the handle is held,
            # a directory cannot be renamed/replaced beneath traversal or a
            # create-or-verify publication.
            0x00000001 | 0x00000002,
            None,
            3,
            0x02000000 | 0x00200000,
            None,
        )
        invalid = ctypes.c_void_p(-1).value
        if handle == invalid:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
        self._windows_handle = int(handle)
        get_info = kernel32.GetFileInformationByHandleEx
        get_info.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        )
        get_info.restype = wintypes.BOOL
        info = FileIdInfo()
        if not get_info(handle, 18, ctypes.byref(info), ctypes.sizeof(info)):
            self.close()
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
        self.identity = {
            "identity_version": "prompt-archive-root-identity-v1",
            "platform": "windows-file-id-v1",
            "volume_serial_hex": f"{int(info.VolumeSerialNumber):016x}",
            "file_id_hex": bytes(info.FileId).hex(),
        }
        get_path = kernel32.GetFinalPathNameByHandleW
        get_path.argtypes = (
            wintypes.HANDLE,
            wintypes.LPWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
        )
        get_path.restype = wintypes.DWORD
        required = get_path(handle, None, 0, 0)
        if not required:
            self.close()
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
        buffer = ctypes.create_unicode_buffer(required + 1)
        written = get_path(handle, buffer, len(buffer), 0)
        if not written or written >= len(buffer):
            self.close()
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
        raw = buffer.value
        if raw.startswith("\\\\?\\UNC\\"):
            raw = "\\\\" + raw[8:]
        elif raw.startswith("\\\\?\\"):
            raw = raw[4:]
        self.resolved_path = Path(raw)

    def assert_stable(self) -> None:
        if self.identity is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
        if os.name == "nt":
            with _RootPin(self.path) as reopened:
                if (
                    reopened.identity != self.identity
                    or reopened.resolved_path is None
                    or self.resolved_path is None
                    or not _same_path(reopened.resolved_path, self.resolved_path)
                ):
                    raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
            return
        try:
            metadata = os.fstat(self.descriptor)
            path_metadata = self.path.lstat()
        except OSError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
        expected = (int(self.identity["st_dev"]), int(self.identity["st_ino"]))
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or not stat.S_ISDIR(path_metadata.st_mode)
            or (metadata.st_dev, metadata.st_ino) != expected
            or (path_metadata.st_dev, path_metadata.st_ino) != expected
            or _is_redirected(self.path)
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")

    def close(self) -> None:
        if self._windows_handle is not None:
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            kernel32.CloseHandle.restype = wintypes.BOOL
            kernel32.CloseHandle(self._windows_handle)
            self._windows_handle = None
        if self.descriptor != -1:
            os.close(self.descriptor)
            self.descriptor = -1

    def __exit__(self, *_args: object) -> None:
        self.close()


def _same_path(observed: Path, expected: Path) -> bool:
    return _path_key(Path(os.path.abspath(observed))) == _path_key(
        Path(os.path.abspath(expected))
    )


def _file_snapshot(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _file_identity(metadata: os.stat_result) -> dict[str, Any]:
    if os.name == "nt":
        return {
            "identity_version": "prompt-archive-file-identity-v1",
            "platform": "windows-stat-file-id-v1",
            "st_dev_hex": f"{metadata.st_dev:016x}",
            "st_ino_hex": f"{metadata.st_ino:032x}",
        }
    return {
        "identity_version": "prompt-archive-file-identity-v1",
        "platform": "posix-dev-inode-v1",
        "st_dev": str(metadata.st_dev),
        "st_ino": str(metadata.st_ino),
    }


def _windows_reject_named_streams(
    path: Path,
    *,
    directory: bool = False,
) -> None:
    """Reject NTFS alternate streams on one file or directory.

    ``FindFirstStreamW`` reports ``ERROR_HANDLE_EOF`` for a directory with no
    streams on supported Windows filesystems.  That is the sole safe empty
    result for a directory.  A regular file must expose exactly its unnamed
    data stream; every additional/other stream is rejected.
    """

    if os.name != "nt":
        return
    from ctypes import wintypes

    class FindStreamData(ctypes.Structure):
        _fields_ = (
            ("StreamSize", ctypes.c_longlong),
            ("cStreamName", wintypes.WCHAR * 296),
        )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    first = kernel32.FindFirstStreamW
    first.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD)
    first.restype = wintypes.HANDLE
    next_stream = kernel32.FindNextStreamW
    next_stream.argtypes = (wintypes.HANDLE, ctypes.c_void_p)
    next_stream.restype = wintypes.BOOL
    close = kernel32.FindClose
    close.argtypes = (wintypes.HANDLE,)
    close.restype = wintypes.BOOL
    data = FindStreamData()
    handle = first(str(path), 0, ctypes.byref(data), 0)
    if handle == ctypes.c_void_p(-1).value:
        if directory and ctypes.get_last_error() == 38:  # ERROR_HANDLE_EOF
            return
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
    streams: list[str] = []
    try:
        streams.append(data.cStreamName)
        while next_stream(handle, ctypes.byref(data)):
            streams.append(data.cStreamName)
        last_error = ctypes.get_last_error()
        if last_error not in {0, 38}:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
    finally:
        close(handle)
    if (directory and streams) or (not directory and streams != ["::$DATA"]):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")


def _windows_reject_named_streams_descriptor(descriptor: int) -> None:
    """Verify streams on the exact Windows file handle being mutated.

    The caller holds this handle without write/delete sharing.  Querying by
    handle avoids a pathname ABA window between the safety check and
    truncate/rename.
    """

    if os.name != "nt":
        return
    import msvcrt
    from ctypes import wintypes

    maximum = 64 * 1024
    information = ctypes.create_string_buffer(maximum)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_information = kernel32.GetFileInformationByHandleEx
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    get_information.restype = wintypes.BOOL
    if not get_information(
        msvcrt.get_osfhandle(descriptor),
        7,  # FileStreamInfo
        information,
        maximum,
    ):
        if ctypes.get_last_error() in {122, 234}:
            # A clean regular leaf has one tiny unnamed stream.  Overflowing a
            # bounded stream-information buffer therefore proves it unsafe.
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")

    streams: list[str] = []
    offset = 0
    while True:
        if offset < 0 or offset + 24 > maximum:
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        next_offset = ctypes.c_ulong.from_buffer(information, offset).value
        name_size = ctypes.c_ulong.from_buffer(information, offset + 4).value
        name_end = offset + 24 + name_size
        if name_size % 2 or name_end > maximum:
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        try:
            name = ctypes.string_at(
                ctypes.addressof(information) + offset + 24,
                name_size,
            ).decode("utf-16-le", errors="strict")
        except UnicodeError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE") from None
        streams.append(name)
        if next_offset == 0:
            break
        if next_offset < 24 + name_size or offset + next_offset <= offset:
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        offset += next_offset
    if streams != ["::$DATA"]:
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")


def _inspect_regular_file(
    path: Path,
    *,
    maximum: int,
    collect: bool,
    parent_descriptor: int | None = None,
    relative_name: str | None = None,
) -> tuple[bytes | None, int, str, os.stat_result]:
    relative_open = parent_descriptor is not None or relative_name is not None
    if relative_open and (
        os.name == "nt"
        or parent_descriptor is None
        or type(relative_name) is not str
    ):
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    if not relative_open and _is_redirected(path):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    _windows_reject_named_streams(path)
    try:
        before = (
            os.stat(relative_name, dir_fd=parent_descriptor, follow_symlinks=False)
            if relative_open
            else path.lstat()
        )
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or stat.S_ISLNK(before.st_mode)
        or bool(
            _REPARSE_POINT
            and getattr(before, "st_file_attributes", 0) & _REPARSE_POINT
        )
    ):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(
            relative_name if relative_open else path,
            flags,
            **({"dir_fd": parent_descriptor} if relative_open else {}),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or _file_snapshot(opened) != _file_snapshot(before)
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        resolved = resolved_path_from_open_descriptor(descriptor)
        if resolved is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
        if not _same_path(resolved, path):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        parts: list[bytes] | None = [] if collect else None
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > maximum:
                raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
            digest.update(chunk)
            if parts is not None:
                parts.append(chunk)
        after_open = os.fstat(descriptor)
    except PromptArchiveMigrationError:
        raise
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    finally:
        if descriptor != -1:
            try:
                os.close(descriptor)
            except OSError:
                pass
    try:
        after_path = (
            os.stat(relative_name, dir_fd=parent_descriptor, follow_symlinks=False)
            if relative_open
            else path.lstat()
        )
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED") from None
    if (
        _file_snapshot(after_open) != _file_snapshot(before)
        or _file_snapshot(after_path) != _file_snapshot(before)
        or stat.S_ISLNK(after_path.st_mode)
        or bool(
            _REPARSE_POINT
            and getattr(after_path, "st_file_attributes", 0) & _REPARSE_POINT
        )
        or (not relative_open and _is_redirected(path))
    ):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
    _windows_reject_named_streams(path)
    return (
        None if parts is None else b"".join(parts),
        total,
        digest.hexdigest(),
        before,
    )


def _read_regular_file(
    path: Path,
    *,
    maximum: int,
) -> tuple[bytes, os.stat_result]:
    raw, _size, _digest, metadata = _inspect_regular_file(
        path, maximum=maximum, collect=True
    )
    assert raw is not None
    return raw, metadata


def _validate_source_tree(root: Path) -> None:
    try:
        metadata = root.lstat()
    except FileNotFoundError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    if not stat.S_ISDIR(metadata.st_mode) or _is_redirected(root):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    _windows_reject_named_streams(root, directory=True)
    _assert_existing_chain_safe(root)


def _inventory_digest(entries: Sequence[InventoryEntry]) -> str:
    preimage = bytearray(_INVENTORY_PREFIX)
    preimage.extend(struct.pack(">Q", len(entries)))
    for entry in entries:
        encoded = entry.relative_path.encode("utf-8", errors="strict")
        preimage.extend(struct.pack(">Q", len(encoded)))
        preimage.extend(encoded)
        preimage.extend(struct.pack(">Q", entry.byte_size))
        preimage.extend(bytes.fromhex(entry.sha256))
    return hashlib.sha256(preimage).hexdigest()


def _safety_snapshot_digest(entries: Sequence[InventoryEntry]) -> str:
    digest = hashlib.sha256()
    digest.update(b"prompt-archive-safety-snapshot-v1\0")
    digest.update(struct.pack(">Q", len(entries)))
    for entry in entries:
        digest.update(bytes.fromhex(entry.path_sha256))
        digest.update(struct.pack(">Q", entry.byte_size))
        digest.update(bytes.fromhex(entry.sha256))
        digest.update(struct.pack(">q", entry.mtime_ns))
        digest.update(bytes.fromhex(entry.identity_sha256))
        digest.update(struct.pack(">Q", entry.link_count))
    return digest.hexdigest()


def inventory_isolated_root(root: str | os.PathLike[str]) -> SourceInventory:
    """Return the frozen full-file inventory of one explicit isolated root."""

    source = _absolute_root(root)
    _validate_source_tree(source)
    entries: list[InventoryEntry] = []
    total_bytes = 0
    directory_count = 0
    casefolded: set[str] = set()
    directory_snapshots: list[tuple[Path, tuple[int, int, int]]] = []
    with _RootPin(source) as pinned:
        assert pinned.identity is not None

        def walk(
            directory: Path,
            components: tuple[str, ...],
            *,
            inherited_descriptor: int | None = None,
        ) -> None:
            nonlocal directory_count, total_bytes
            directory_count += 1
            if (
                directory_count > _MAX_DIRECTORIES
                or len(components) > _MAX_DIRECTORY_DEPTH
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
            if _is_redirected(directory):
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
            _windows_reject_named_streams(directory, directory=True)
            try:
                with _RootPin(
                    directory,
                    descriptor=inherited_descriptor,
                ) as directory_pin:
                    if (
                        directory_pin.resolved_path is None
                        or not _same_path(directory_pin.resolved_path, directory)
                    ):
                        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
                    before = (
                        directory.lstat()
                        if os.name == "nt"
                        else os.fstat(directory_pin.descriptor)
                    )
                    children = list(
                        os.scandir(
                            directory
                            if os.name == "nt"
                            else directory_pin.descriptor
                        )
                    )
                    if not stat.S_ISDIR(before.st_mode):
                        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
                    directory_snapshots.append(
                        (
                            directory,
                            (before.st_dev, before.st_ino, before.st_mtime_ns),
                        )
                    )
                    for child in children:
                        _validate_component(child.name)
                    children.sort(
                        key=lambda item: item.name.encode(
                            "utf-8", errors="strict"
                        )
                    )
                    # Retain the exact directory handle until every listed
                    # child has been inspected.  On Windows the pin denies
                    # rename/delete sharing; on POSIX every leaf open is also
                    # checked against its kernel-resolved absolute path.
                    for child in children:
                        relative_components = components + (child.name,)
                        relative = "/".join(relative_components)
                        _claim_casefolded_path(casefolded, relative)
                        child_path = directory / child.name
                        try:
                            metadata = (
                                child_path.lstat()
                                if os.name == "nt"
                                else os.stat(
                                    child.name,
                                    dir_fd=directory_pin.descriptor,
                                    follow_symlinks=False,
                                )
                            )
                        except OSError:
                            raise _error(
                                "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED"
                            ) from None
                        if (
                            stat.S_ISLNK(metadata.st_mode)
                            or bool(
                                _REPARSE_POINT
                                and getattr(metadata, "st_file_attributes", 0)
                                & _REPARSE_POINT
                            )
                            or (os.name == "nt" and _is_redirected(child_path))
                        ):
                            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
                        if stat.S_ISDIR(metadata.st_mode):
                            directory_pin.assert_stable()
                            child_descriptor: int | None = None
                            if os.name != "nt":
                                try:
                                    child_descriptor = os.open(
                                        child.name,
                                        os.O_RDONLY
                                        | getattr(os, "O_DIRECTORY", 0)
                                        | getattr(os, "O_NOFOLLOW", 0),
                                        dir_fd=directory_pin.descriptor,
                                    )
                                    opened = os.fstat(child_descriptor)
                                except OSError:
                                    if child_descriptor is not None:
                                        os.close(child_descriptor)
                                    raise _error(
                                        "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED"
                                    ) from None
                                if (
                                    not stat.S_ISDIR(opened.st_mode)
                                    or (opened.st_dev, opened.st_ino)
                                    != (metadata.st_dev, metadata.st_ino)
                                ):
                                    os.close(child_descriptor)
                                    raise _error(
                                        "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED"
                                    )
                            walk(
                                child_path,
                                relative_components,
                                inherited_descriptor=child_descriptor,
                            )
                            continue
                        if (
                            not stat.S_ISREG(metadata.st_mode)
                            or metadata.st_nlink != 1
                        ):
                            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
                        _raw, byte_size, digest, observed = _inspect_regular_file(
                            child_path,
                            maximum=_MAX_FILE_BYTES,
                            collect=False,
                            parent_descriptor=(
                                None
                                if os.name == "nt"
                                else directory_pin.descriptor
                            ),
                            relative_name=(
                                None if os.name == "nt" else child.name
                            ),
                        )
                        total_bytes += byte_size
                        if total_bytes > _MAX_TOTAL_BYTES:
                            raise _error(
                                "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED"
                            )
                        path_bytes = relative.encode("utf-8", errors="strict")
                        file_identity = _file_identity(observed)
                        identity_raw = _canonical_bytes(
                            file_identity, maximum=4096
                        )
                        entries.append(
                            InventoryEntry(
                                relative_path=relative,
                                path_sha256=hashlib.sha256(path_bytes).hexdigest(),
                                byte_size=byte_size,
                                sha256=digest,
                                mtime_ns=observed.st_mtime_ns,
                                file_identity=file_identity,
                                identity_sha256=hashlib.sha256(
                                    identity_raw
                                ).hexdigest(),
                                link_count=observed.st_nlink,
                            )
                        )
                        if len(entries) > _MAX_FILES:
                            raise _error(
                                "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED"
                            )
                    try:
                        after = (
                            directory.lstat()
                            if os.name == "nt"
                            else os.fstat(directory_pin.descriptor)
                        )
                    except OSError:
                        raise _error(
                            "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED"
                        ) from None
                    if (after.st_dev, after.st_ino, after.st_mtime_ns) != (
                        before.st_dev,
                        before.st_ino,
                        before.st_mtime_ns,
                    ):
                        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
                    _windows_reject_named_streams(directory, directory=True)
                    directory_pin.assert_stable()
            except OSError:
                raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None

        walk(source, ())
        pinned.assert_stable()
        for directory, expected in directory_snapshots:
            try:
                observed = directory.lstat()
            except OSError:
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED") from None
            if (observed.st_dev, observed.st_ino, observed.st_mtime_ns) != expected:
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        identity = copy.deepcopy(pinned.identity)
    if not entries:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    entries.sort(key=lambda item: item.relative_path.encode("utf-8"))
    identity_raw = _canonical_bytes(identity, maximum=4096)
    return SourceInventory(
        source_root_identity=identity,
        source_root_identity_sha256=hashlib.sha256(identity_raw).hexdigest(),
        inventory_sha256=_inventory_digest(entries),
        safety_snapshot_sha256=_safety_snapshot_digest(entries),
        total_bytes=total_bytes,
        entries=tuple(entries),
    )


def _document_sha256(value: Any) -> str:
    return hashlib.sha256(
        _canonical_bytes(value, maximum=_MAX_CONTROL_BYTES)
    ).hexdigest()


def _tool_source_sha256() -> str:
    try:
        return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None


def _is_legacy_archive_candidate(relative_path: str) -> bool:
    name = relative_path.rsplit("/", 1)[-1]
    return name == "latest-response.json" or bool(
        name.startswith("transition-response-")
        and name.endswith(".json")
        and len(name) > len("transition-response-.json")
    )


def _entry_id(
    inventory_id: str,
    ordinal: int,
    entry: InventoryEntry,
) -> str:
    return "parme1-" + pas1_id_v1(
        "offline-migration-entry-id",
        (
            ("inventory_id", inventory_id),
            ("ordinal", str(ordinal)),
            ("path_sha256", entry.path_sha256),
            ("byte_size", str(entry.byte_size)),
            ("sha256", entry.sha256),
        ),
    )


class PromptArchiveMigration:
    """One explicit, resumable offline migration operation."""

    def __init__(
        self,
        *,
        input_root: str | os.PathLike[str],
        archive_root: str | os.PathLike[str],
        project_id: str,
        source_root_handle_id: str,
        operator_gate: str,
        operator_principal_id: str,
        private_evidence_file: str | os.PathLike[str] | None = None,
        protected_roots: Sequence[str | os.PathLike[str]] = (),
        clock: Callable[[], str] = _utc_now,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        # Authorization is checked before a candidate or archive path is
        # opened.  The handle label identifies the source; it is deliberately
        # not accepted as the operator principal.
        if (
            operator_gate != OFFLINE_OPERATOR_GATE
            or type(operator_principal_id) is not str
            or _SAFE_ID_RE.fullmatch(operator_principal_id) is None
            or type(project_id) is not str
            or _SAFE_ID_RE.fullmatch(project_id) is None
            or type(source_root_handle_id) is not str
            or _SAFE_ID_RE.fullmatch(source_root_handle_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        self.input_root = _absolute_root(input_root)
        self.archive_root = _absolute_root(archive_root)
        self.project_id = project_id
        self.source_root_handle_id = source_root_handle_id
        self.operator_gate = operator_gate
        self.operator_principal_id = operator_principal_id
        self.private_evidence_file = (
            None
            if private_evidence_file is None
            else _absolute_root(private_evidence_file)
        )
        if isinstance(protected_roots, (str, bytes)) or not isinstance(
            protected_roots, Sequence
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        explicit_protected = tuple(
            _absolute_root(value) for value in protected_roots
        )
        # Honor a visible explicit deny before reading even the local persisted
        # settings snapshot.  In particular, an input nested in a protected
        # root must be rejected without probing that tree.
        _assert_roots_isolated(
            self.input_root,
            self.archive_root,
            explicit_protected,
        )
        if self.private_evidence_file is not None:
            for root in (
                self.input_root,
                self.archive_root,
                *explicit_protected,
            ):
                if _overlap(self.private_evidence_file, root):
                    raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
        (
            self.protected_roots,
            self._alias_sensitive_protected_roots,
        ) = _configured_protected_roots(protected_roots)
        _assert_roots_isolated(
            self.input_root, self.archive_root, self.protected_roots
        )
        if self.private_evidence_file is not None:
            for root in (
                self.input_root,
                self.archive_root,
                *self.protected_roots,
            ):
                if _overlap(self.private_evidence_file, root):
                    raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
        self._clock = clock
        self._fault_hook = fault_hook
        # Lazy construction is essential: analyze is a genuinely read-only
        # command and must not even bootstrap an absent archive root.
        self._storage: PromptArchiveStorage | None = None
        self._runtime: PromptArchiveRuntime | None = None
        batch_digest = pas1_id_v1(
            "offline-migration-batch-id",
            (
                ("project_id", project_id),
                ("source_root_handle_id", source_root_handle_id),
            ),
        )
        self.batch_id = "parmb1-" + batch_digest

    def _assert_source_read_ready(self) -> None:
        _assert_canonical_candidates_isolated(
            (self.input_root,),
            self.protected_roots,
            self._alias_sensitive_protected_roots,
        )

    def _assert_migration_access_ready(self) -> None:
        candidates = [self.input_root, self.archive_root]
        if self.private_evidence_file is not None:
            candidates.append(self.private_evidence_file)
        _assert_canonical_candidates_isolated(
            candidates,
            self.protected_roots,
            self._alias_sensitive_protected_roots,
        )
        if self.private_evidence_file is not None:
            _preflight_private_evidence_leaf_identity(
                self.private_evidence_file
            )

    def _assert_resume_access_ready(self) -> None:
        # Source-free recovery intentionally does not inspect input_root, but
        # the retained archive control plane must still be canonical and
        # isolated from every configured/live protected root.
        _assert_canonical_candidates_isolated(
            (self.archive_root,),
            self.protected_roots,
            self._alias_sensitive_protected_roots,
        )

    @property
    def storage(self) -> PromptArchiveStorage:
        return self._storage_instance(require_existing_root=False)

    def _storage_instance(
        self,
        *,
        require_existing_root: bool,
    ) -> PromptArchiveStorage:
        if self._storage is None:
            try:
                self._storage = PromptArchiveStorage(
                    self.archive_root,
                    self.project_id,
                    # Candidate/protected overlap and Windows aliases were
                    # rejected before bootstrap.  Do not probe a possibly
                    # unavailable source merely to resume retained PAS1 state.
                    protected_roots=(),
                    fault_hook=self._fault,
                    require_existing_root=require_existing_root,
                )
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
        return self._storage

    def _runtime_instance(self) -> PromptArchiveRuntime:
        if self._runtime is None:
            config = PromptArchiveRuntimeConfig(
                mode="shadow",
                root=self.archive_root,
                max_hydrated_bytes=self.storage.limits.max_hydrated_bytes,
                protected_roots=(),
                runtime_instance_id="offline-migration-" + self.batch_id[-32:],
            )
            self._runtime = PromptArchiveRuntime(
                config,
                self.project_id,
                self.storage,
            )
        return self._runtime

    def _fault(self, stage: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage)

    @contextmanager
    def _batch_control_lock(
        self,
        *,
        existing_only: bool = False,
    ) -> Iterable[None]:
        try:
            lock = (
                self.storage.existing_control_lock
                if existing_only
                else self.storage.control_lock
            )
            with lock(self._lock_path()):
                yield
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None

    def _plan_path(self) -> tuple[str, ...]:
        return ("migrations", "v1", self.batch_id, "plan.json")

    def _journal_path(self) -> tuple[str, ...]:
        return ("migrations", "v1", self.batch_id, "journal.json")

    def _journal_generation_path(
        self, generation: int, digest: str
    ) -> tuple[str, ...]:
        return (
            "migrations",
            "v1",
            self.batch_id,
            "journal-generations",
            f"{generation}-{digest}.json",
        )

    def _lock_path(self) -> tuple[str, ...]:
        return ("migrations", "v1", self.batch_id, "operation.lock")

    def _evidence_path(self) -> tuple[str, ...]:
        return ("migrations", "v1", self.batch_id, "evidence.json")

    def _load_plan_optional(self) -> dict[str, Any] | None:
        try:
            return self.storage.read_immutable_json(self._plan_path())
        except PromptArchiveStorageError as exc:
            if exc.code in {"PROMPT_ARCHIVE_NOT_FOUND", "PROMPT_ARCHIVE_UNAVAILABLE"}:
                return None
            raise _error_from_storage(exc) from None

    def _inventory_descriptor(
        self,
        inventory: SourceInventory,
        *,
        created_at: str,
    ) -> dict[str, Any]:
        inventory_id = "parinv1-" + pas1_id_v1(
            "source-inventory-id",
            (
                ("project_id", self.project_id),
                ("source_root_handle_id", self.source_root_handle_id),
                (
                    "source_root_identity_sha256",
                    inventory.source_root_identity_sha256,
                ),
                ("inventory_sha256", inventory.inventory_sha256),
                ("entry_count", str(len(inventory.entries))),
                ("created_at", created_at),
            ),
        )
        return {
            "schema_version": 1,
            "kind": "prompt_archive_source_inventory",
            "media_type": _INVENTORY_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "inventory_id": inventory_id,
            "inventory_format": "prompt-archive-isolated-inventory-v1",
            "project_id": self.project_id,
            "source_root_handle_id": self.source_root_handle_id,
            "source_root_identity_sha256": inventory.source_root_identity_sha256,
            "inventory_sha256": inventory.inventory_sha256,
            "entry_count": len(inventory.entries),
            "created_at": created_at,
        }

    def _validate_plan_envelope(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        """Validate every source-independent field before indexed access."""

        if type(plan) is not dict:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        value = copy.deepcopy(dict(plan))
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
        if set(value) != required or (
            value.get("schema_version") != 1
            or value.get("kind") != "prompt_archive_migration_batch_plan"
            or value.get("media_type") != _BATCH_PLAN_MEDIA
            or value.get("id_algorithm") != "PAS1-ID-v1"
            or value.get("batch_id") != self.batch_id
            or value.get("operation_kind") != "offline_migration_batch"
            or value.get("contract_version") != CONTRACT_VERSION
            or value.get("tool_version") != TOOL_VERSION
            # The source digest is immutable provenance for the writer, not a
            # recovery compatibility gate.  Compatible code-only fixes keep
            # the frozen contract/tool version and must be able to finish a
            # retained v1 journal.  An incompatible change must instead bump
            # one of those explicit versions.
            or type(value.get("tool_source_sha256")) is not str
            or _SHA256_RE.fullmatch(value["tool_source_sha256"]) is None
            or value.get("project_id") != self.project_id
            or value.get("sprint_id") is not None
            or value.get("source_root_handle_id") != self.source_root_handle_id
            or type(value.get("created_at")) is not str
            or _UTC_TIMESTAMP_RE.fullmatch(value["created_at"]) is None
            or value.get("operator_authorization")
            != {
                "gate": OFFLINE_OPERATOR_GATE,
                "principal_kind": "offline_operator",
                "principal_id": self.operator_principal_id,
                "scope_source": "offline_operator_gate",
            }
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")

        source_safety = value.get("source_safety")
        if type(source_safety) is not dict or set(source_safety) != {
            "source_root_identity_sha256",
            "safety_snapshot_sha256",
            "total_bytes",
        }:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        total_bytes = source_safety.get("total_bytes")
        if (
            type(source_safety.get("source_root_identity_sha256")) is not str
            or _SHA256_RE.fullmatch(source_safety["source_root_identity_sha256"])
            is None
            or type(source_safety.get("safety_snapshot_sha256")) is not str
            or _SHA256_RE.fullmatch(source_safety["safety_snapshot_sha256"]) is None
            or type(total_bytes) is not int
            or not 0 <= total_bytes <= _MAX_TOTAL_BYTES
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")

        locator = value.get("source_inventory")
        if type(locator) is not dict or set(locator) != {
            "locator_kind",
            "locator_version",
            "inventory_id",
            "inventory_format",
            "inventory_sha256",
            "entry_count",
            "inventory_ref",
        }:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        entry_count = locator.get("entry_count")
        if (
            locator.get("locator_kind") != "isolated_inventory"
            or locator.get("locator_version")
            != "prompt-archive-isolated-inventory-locator-v1"
            or type(locator.get("inventory_id")) is not str
            or _INVENTORY_ID_RE.fullmatch(locator["inventory_id"]) is None
            or locator.get("inventory_format")
            != "prompt-archive-isolated-inventory-v1"
            or type(locator.get("inventory_sha256")) is not str
            or _SHA256_RE.fullmatch(locator["inventory_sha256"]) is None
            or type(entry_count) is not int
            or not 1 <= entry_count <= _MAX_FILES
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")

        reference = locator.get("inventory_ref")
        reference_required = {
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
        }
        if type(reference) is not dict:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        encoding = reference.get("content_encoding")
        expected_reference_keys = set(reference_required)
        if encoding == "gzip":
            expected_reference_keys.add("encoding_parameters")
        if set(reference) != expected_reference_keys or (
            reference.get("schema_version") != 1
            or reference.get("kind") != "prompt_archive_blob_ref"
            or reference.get("hash_algorithm") != "sha256"
            or type(reference.get("digest")) is not str
            or _SHA256_RE.fullmatch(reference["digest"]) is None
            or reference.get("logical_kind") != "source_inventory"
            or reference.get("media_type") != _INVENTORY_MEDIA
            or encoding not in {"identity", "gzip"}
            or (
                encoding == "gzip"
                and reference.get("encoding_parameters")
                != "gzip-rfc1952-zlib-1.2.13-level9-v1"
            )
            or type(reference.get("uncompressed_size")) is not int
            or not 0 <= reference["uncompressed_size"] <= 16 * 1024 * 1024
            or type(reference.get("stored_size")) is not int
            or not 0 <= reference["stored_size"] <= 16 * 1024 * 1024 + 64 * 1024
            or type(reference.get("stored_sha256")) is not str
            or _SHA256_RE.fullmatch(reference["stored_sha256"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")

        reservations = value.get("reservations")
        if (
            type(reservations) is not list
            or not reservations
            or len(reservations) > entry_count
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        seen_ordinals: set[int] = set()
        seen_entries: set[str] = set()
        seen_operations: set[str] = set()
        seen_occurrences: set[str] = set()
        seen_responses: set[str] = set()
        for reservation in reservations:
            if type(reservation) is not dict or set(reservation) != {
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
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            ordinal = reservation.get("ordinal")
            entry_id = reservation.get("entry_id")
            operation_id = reservation.get("operation_id")
            occurrence_id = reservation.get("occurrence_id")
            response_id = reservation.get("response_id")
            source_byte_size = reservation.get("source_byte_size")
            if (
                type(ordinal) is not int
                or not 0 <= ordinal < entry_count
                or ordinal in seen_ordinals
                or type(entry_id) is not str
                or _ENTRY_ID_RE.fullmatch(entry_id) is None
                or entry_id in seen_entries
                or type(reservation.get("path_sha256")) is not str
                or _SHA256_RE.fullmatch(reservation["path_sha256"]) is None
                or type(source_byte_size) is not int
                or not 0 <= source_byte_size <= _MAX_FILE_BYTES
                or type(reservation.get("source_sha256")) is not str
                or _SHA256_RE.fullmatch(reservation["source_sha256"]) is None
                or type(operation_id) is not str
                or _OPERATION_ID_RE.fullmatch(operation_id) is None
                or operation_id in seen_operations
                or type(occurrence_id) is not str
                or _MIGRATION_OCCURRENCE_ID_RE.fullmatch(occurrence_id) is None
                or occurrence_id in seen_occurrences
                or type(response_id) is not str
                or _RESPONSE_ID_RE.fullmatch(response_id) is None
                or response_id in seen_responses
                or type(reservation.get("decision_occurrence_id")) is not str
                or _DECISION_OCCURRENCE_ID_RE.fullmatch(
                    reservation["decision_occurrence_id"]
                )
                is None
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            seen_ordinals.add(ordinal)
            seen_entries.add(entry_id)
            seen_operations.add(operation_id)
            seen_occurrences.add(occurrence_id)
            seen_responses.add(response_id)
        if [reservation["ordinal"] for reservation in reservations] != sorted(
            seen_ordinals
        ) or sum(reservation["source_byte_size"] for reservation in reservations) > total_bytes:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        return value

    def _validate_plan(
        self,
        plan: Mapping[str, Any],
        inventory: SourceInventory,
    ) -> dict[str, Any]:
        value = self._validate_plan_envelope(plan)
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
        if set(value) != required or (
            value.get("schema_version") != 1
            or value.get("kind") != "prompt_archive_migration_batch_plan"
            or value.get("media_type") != _BATCH_PLAN_MEDIA
            or value.get("id_algorithm") != "PAS1-ID-v1"
            or value.get("batch_id") != self.batch_id
            or value.get("operation_kind") != "offline_migration_batch"
            or value.get("contract_version") != CONTRACT_VERSION
            or value.get("tool_version") != TOOL_VERSION
            # See _validate_plan_envelope: the retained source digest records
            # which writer made the plan, while the explicit versions govern
            # whether this reader may resume it.
            or type(value.get("tool_source_sha256")) is not str
            or _SHA256_RE.fullmatch(value["tool_source_sha256"]) is None
            or value.get("project_id") != self.project_id
            or value.get("sprint_id") is not None
            or value.get("source_root_handle_id") != self.source_root_handle_id
            or type(value.get("created_at")) is not str
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        authorization = value.get("operator_authorization")
        if authorization != {
            "gate": OFFLINE_OPERATOR_GATE,
            "principal_kind": "offline_operator",
            "principal_id": self.operator_principal_id,
            "scope_source": "offline_operator_gate",
        }:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        if value.get("source_safety") != {
            "source_root_identity_sha256": inventory.source_root_identity_sha256,
            "safety_snapshot_sha256": inventory.safety_snapshot_sha256,
            "total_bytes": inventory.total_bytes,
        }:
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        locator = value.get("source_inventory")
        if type(locator) is not dict or set(locator) != {
            "locator_kind",
            "locator_version",
            "inventory_id",
            "inventory_format",
            "inventory_sha256",
            "entry_count",
            "inventory_ref",
        }:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        descriptor = self._inventory_descriptor(
            inventory, created_at=value.get("created_at")
        )
        descriptor_raw = _canonical_bytes(
            descriptor, maximum=_MAX_CONTROL_BYTES
        )
        try:
            expected_ref = self.storage.describe_blob(
                descriptor_raw,
                logical_kind="source_inventory",
                media_type=_INVENTORY_MEDIA,
            )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        if (
            locator.get("locator_kind") != "isolated_inventory"
            or locator.get("locator_version")
            != "prompt-archive-isolated-inventory-locator-v1"
            or locator.get("inventory_id") != descriptor["inventory_id"]
            or locator.get("inventory_format") != descriptor["inventory_format"]
            or locator.get("inventory_sha256") != descriptor["inventory_sha256"]
            or locator.get("entry_count") != descriptor["entry_count"]
            or locator.get("inventory_ref") != expected_ref
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        candidates = [
            (ordinal, entry)
            for ordinal, entry in enumerate(inventory.entries)
            if _is_legacy_archive_candidate(entry.relative_path)
        ]
        reservations = value.get("reservations")
        if type(reservations) is not list or len(reservations) != len(candidates):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        seen_operations: set[str] = set()
        seen_responses: set[str] = set()
        for reservation, (ordinal, entry) in zip(reservations, candidates):
            entry_identifier = _entry_id(descriptor["inventory_id"], ordinal, entry)
            occurrence_id = "migration-" + pas1_id_v1(
                "offline-migration-occurrence",
                (("batch_id", self.batch_id), ("entry_id", entry_identifier)),
            )
            response_id = response_id_v1(
                self.project_id,
                None,
                "migration-snapshot",
                "migration",
                occurrence_id,
            )
            if type(reservation) is not dict or set(reservation) != {
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
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            operation_id = reservation.get("operation_id")
            expected = {
                "ordinal": ordinal,
                "entry_id": entry_identifier,
                "path_sha256": entry.path_sha256,
                "source_byte_size": entry.byte_size,
                "source_sha256": entry.sha256,
                "occurrence_id": occurrence_id,
                "response_id": response_id,
                "decision_occurrence_id": "decision-" + entry_identifier[-64:],
            }
            if (
                any(reservation.get(name) != expected_value for name, expected_value in expected.items())
                or type(operation_id) is not str
                or _OPERATION_ID_RE.fullmatch(operation_id) is None
                or operation_id in seen_operations
                or response_id in seen_responses
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            seen_operations.add(operation_id)
            seen_responses.add(response_id)
        return value

    def _build_plan(
        self,
        inventory: SourceInventory,
        *,
        created_at: str,
    ) -> dict[str, Any]:
        descriptor = self._inventory_descriptor(inventory, created_at=created_at)
        descriptor_raw = _canonical_bytes(
            descriptor, maximum=_MAX_CONTROL_BYTES
        )
        try:
            inventory_ref = self.storage.describe_blob(
                descriptor_raw,
                logical_kind="source_inventory",
                media_type=_INVENTORY_MEDIA,
            )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        reservations: list[dict[str, Any]] = []
        for ordinal, entry in enumerate(inventory.entries):
            if not _is_legacy_archive_candidate(entry.relative_path):
                continue
            entry_identifier = _entry_id(descriptor["inventory_id"], ordinal, entry)
            occurrence_id = "migration-" + pas1_id_v1(
                "offline-migration-occurrence",
                (("batch_id", self.batch_id), ("entry_id", entry_identifier)),
            )
            reservations.append(
                {
                    "ordinal": ordinal,
                    "entry_id": entry_identifier,
                    "path_sha256": entry.path_sha256,
                    "source_byte_size": entry.byte_size,
                    "source_sha256": entry.sha256,
                    # Random 128-bit IDs are reserved durably by this plan
                    # before a source-inventory or archive CAS write occurs.
                    "operation_id": "parop1-" + secrets.token_hex(16),
                    "occurrence_id": occurrence_id,
                    "response_id": response_id_v1(
                        self.project_id,
                        None,
                        "migration-snapshot",
                        "migration",
                        occurrence_id,
                    ),
                    "decision_occurrence_id": "decision-" + entry_identifier[-64:],
                }
            )
        if not reservations:
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        return {
            "schema_version": 1,
            "kind": "prompt_archive_migration_batch_plan",
            "media_type": _BATCH_PLAN_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "batch_id": self.batch_id,
            "operation_kind": "offline_migration_batch",
            "contract_version": CONTRACT_VERSION,
            "tool_version": TOOL_VERSION,
            "tool_source_sha256": _tool_source_sha256(),
            "project_id": self.project_id,
            "sprint_id": None,
            "source_root_handle_id": self.source_root_handle_id,
            "operator_authorization": {
                "gate": OFFLINE_OPERATOR_GATE,
                "principal_kind": "offline_operator",
                "principal_id": self.operator_principal_id,
                "scope_source": "offline_operator_gate",
            },
            "source_inventory": {
                "locator_kind": "isolated_inventory",
                "locator_version": "prompt-archive-isolated-inventory-locator-v1",
                "inventory_id": descriptor["inventory_id"],
                "inventory_format": descriptor["inventory_format"],
                "inventory_sha256": descriptor["inventory_sha256"],
                "entry_count": descriptor["entry_count"],
                "inventory_ref": inventory_ref,
            },
            "source_safety": {
                "source_root_identity_sha256": inventory.source_root_identity_sha256,
                "safety_snapshot_sha256": inventory.safety_snapshot_sha256,
                "total_bytes": inventory.total_bytes,
            },
            "reservations": reservations,
            "created_at": created_at,
        }

    def _prepare(self, inventory: SourceInventory) -> dict[str, Any]:
        existing = self._load_plan_optional()
        if existing is not None:
            plan = self._validate_plan(existing, inventory)
        else:
            plan = self._build_plan(inventory, created_at=self._clock())
            raw = _canonical_bytes(plan, maximum=_MAX_CONTROL_BYTES)
            try:
                self.storage.publish_immutable_bytes(
                    self._plan_path(),
                    raw,
                    expected_sha256=hashlib.sha256(raw).hexdigest(),
                )
            except PromptArchiveStorageError as exc:
                if exc.code != "PROMPT_ARCHIVE_GENERATION_CONFLICT":
                    raise _error_from_storage(exc) from None
                winner = self._load_plan_optional()
                if winner is None:
                    raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT") from None
                plan = winner
            self._fault("after_batch_plan_publication")
            plan = self._validate_plan(plan, inventory)

        descriptor = self._inventory_descriptor(
            inventory, created_at=plan["created_at"]
        )
        try:
            locator = self._runtime_instance().publish_offline_inventory_descriptor(
                descriptor
            )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        if locator != plan["source_inventory"]:
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        descriptor_raw = _canonical_bytes(descriptor, maximum=_MAX_CONTROL_BYTES)
        try:
            if self.storage.read_blob(
                locator["inventory_ref"], max_output_bytes=_MAX_CONTROL_BYTES
            ) != descriptor_raw:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            retained = self.storage.read_immutable_json(
                ("inventories", "v1", f"{descriptor['inventory_id']}.json")
            )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        if retained != descriptor:
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        self._fault("after_inventory_publication")
        return plan

    def _initial_journal(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "kind": "prompt_archive_migration_batch_journal",
            "media_type": _BATCH_JOURNAL_MEDIA,
            "id_algorithm": "PAS1-ID-v1",
            "batch_id": self.batch_id,
            "plan_sha256": _document_sha256(plan),
            "journal_generation": 0,
            "previous_journal_generation": None,
            "previous_journal_sha256": None,
            "state": "planned",
            "completed_entries": [],
            "pre_inventory": {
                "inventory_sha256": plan["source_inventory"]["inventory_sha256"],
                "entry_count": plan["source_inventory"]["entry_count"],
                "source_root_identity_sha256": plan["source_safety"]["source_root_identity_sha256"],
                "safety_snapshot_sha256": plan["source_safety"]["safety_snapshot_sha256"],
                "total_bytes": plan["source_safety"]["total_bytes"],
            },
            "post_inventory": None,
            "originals_preserved": True,
            "created_at": plan["created_at"],
            "updated_at": plan["created_at"],
        }

    def _validate_journal(
        self, value: Mapping[str, Any], plan: Mapping[str, Any]
    ) -> dict[str, Any]:
        if type(value) is not dict or type(plan) is not dict:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        journal = copy.deepcopy(dict(value))
        required = {
            "schema_version",
            "kind",
            "media_type",
            "id_algorithm",
            "batch_id",
            "plan_sha256",
            "journal_generation",
            "previous_journal_generation",
            "previous_journal_sha256",
            "state",
            "completed_entries",
            "pre_inventory",
            "post_inventory",
            "originals_preserved",
            "created_at",
            "updated_at",
        }
        expected_plan_sha = _document_sha256(plan)
        generation = journal.get("journal_generation")
        if set(journal) != required or (
            journal.get("schema_version") != 1
            or journal.get("kind") != "prompt_archive_migration_batch_journal"
            or journal.get("media_type") != _BATCH_JOURNAL_MEDIA
            or journal.get("id_algorithm") != "PAS1-ID-v1"
            or journal.get("batch_id") != self.batch_id
            or journal.get("plan_sha256") != expected_plan_sha
            or type(generation) is not int
            or not 0 <= generation <= 9_223_372_036_854_775_807
            or journal.get("state") not in {"planned", "running", "complete"}
            or type(journal.get("completed_entries")) is not list
            or journal.get("pre_inventory")
            != {
                "inventory_sha256": plan["source_inventory"]["inventory_sha256"],
                "entry_count": plan["source_inventory"]["entry_count"],
                "source_root_identity_sha256": plan["source_safety"]["source_root_identity_sha256"],
                "safety_snapshot_sha256": plan["source_safety"]["safety_snapshot_sha256"],
                "total_bytes": plan["source_safety"]["total_bytes"],
            }
            or journal.get("originals_preserved") is not True
            or journal.get("created_at") != plan["created_at"]
            or journal.get("updated_at") != plan["created_at"]
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        if generation == 0:
            if (
                journal.get("previous_journal_generation") is not None
                or journal.get("previous_journal_sha256") is not None
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        elif (
            journal.get("previous_journal_generation") != generation - 1
            or type(journal.get("previous_journal_sha256")) is not str
            or _SHA256_RE.fullmatch(journal["previous_journal_sha256"]) is None
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        reservations = plan["reservations"]
        completed = journal["completed_entries"]
        if len(completed) > len(reservations):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        for item, reservation in zip(completed, reservations):
            if type(item) is not dict or set(item) != {
                "entry_id",
                "ordinal",
                "operation_id",
                "response_id",
                "archive_id",
                "pointer_id",
                "source_byte_size",
                "source_sha256",
                "matched",
            }:
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            if (
                item.get("entry_id") != reservation["entry_id"]
                or item.get("ordinal") != reservation["ordinal"]
                or item.get("operation_id") != reservation["operation_id"]
                or item.get("response_id") != reservation["response_id"]
                or type(item.get("archive_id")) is not str
                or _ARCHIVE_ID_RE.fullmatch(item["archive_id"]) is None
                or type(item.get("pointer_id")) is not str
                or _POINTER_ID_RE.fullmatch(item["pointer_id"]) is None
                or item.get("source_byte_size") != reservation["source_byte_size"]
                or item.get("source_sha256") != reservation["source_sha256"]
                or item.get("matched") is not True
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        if journal["state"] == "complete":
            if (
                len(completed) != len(reservations)
                or generation != len(completed) + 1
                or journal.get("post_inventory") != journal["pre_inventory"]
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        elif journal.get("post_inventory") is not None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        elif journal["state"] == "planned":
            if generation != 0 or completed:
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        elif generation != len(completed) or not completed:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        return journal

    def _journal_generation_index(self) -> tuple[tuple[int, str], ...]:
        directory = self.storage._immutable_path(
            self._journal_generation_path(0, "0" * 64)
        ).parent
        try:
            metadata = directory.lstat()
        except FileNotFoundError:
            return ()
        except OSError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
        if not stat.S_ISDIR(metadata.st_mode) or _is_redirected(directory):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")

        def snapshot(
            parent: _PinnedDirectory,
        ) -> tuple[
            tuple[tuple[int, str], ...],
            tuple[tuple[str, int, int, int, int], ...],
        ]:
            try:
                children = []
                with os.scandir(
                        directory if os.name == "nt" else parent.descriptor
                ) as entries:
                    for child in entries:
                        if len(children) >= _MAX_JOURNAL_GENERATION_CHILDREN:
                            raise _error(
                                "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT"
                            )
                        children.append(child)
            except PromptArchiveMigrationError:
                raise
            except OSError:
                raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
            result: list[tuple[int, str]] = []
            temporary: list[tuple[str, int, int, int, int]] = []
            for child in children:
                match = re.fullmatch(r"(0|[1-9][0-9]*)-([0-9a-f]{64})\.json", child.name)
                temporary_target = _storage_temporary_target_name(child.name)
                temporary_match = (
                    None
                    if temporary_target is None
                    else re.fullmatch(
                        r"(0|[1-9][0-9]*)-([0-9a-f]{64})\.json",
                        temporary_target,
                    )
                )
                if match is None and temporary_match is None:
                    raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
                try:
                    child_metadata = (
                        (directory / child.name).lstat()
                        if os.name == "nt"
                        else child.stat(follow_symlinks=False)
                    )
                except OSError:
                    raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED") from None
                if (
                    child.is_symlink()
                    or not stat.S_ISREG(child_metadata.st_mode)
                    or child_metadata.st_nlink != 1
                    or bool(
                        _REPARSE_POINT
                        and getattr(child_metadata, "st_file_attributes", 0)
                        & _REPARSE_POINT
                    )
                ):
                    raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
                try:
                    _windows_reject_named_streams(directory / child.name)
                except PromptArchiveMigrationError as exc:
                    if exc.code == "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE":
                        raise _error(
                            "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT"
                        ) from None
                    raise
                classified = match if match is not None else temporary_match
                assert classified is not None
                generation = int(classified.group(1))
                if generation > 9_223_372_036_854_775_807:
                    raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
                if match is None:
                    if child_metadata.st_size > _MAX_CONTROL_BYTES:
                        raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
                    temporary.append(
                        (child.name, *_file_snapshot(child_metadata))
                    )
                    continue
                result.append((generation, match.group(2)))
            result.sort()
            temporary.sort()
            return tuple(result), tuple(temporary)

        with _open_pinned_mutation_directory(directory) as parent:
            before = snapshot(parent)
            after = snapshot(parent)
            _validate_pinned_mutation_directory(parent, directory)
        retained = before[0]
        if before != after or len(retained) != len(set(retained)):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        return retained

    def _read_journal(self, plan: Mapping[str, Any]) -> dict[str, Any] | None:
        try:
            current = self.storage.read_current_json(self._journal_path())
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        retained_generations = self._journal_generation_index()
        if current is None:
            if not retained_generations:
                return None
            initial = self._initial_journal(plan)
            initial_digest = _document_sha256(initial)
            if retained_generations != ((0, initial_digest),):
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            try:
                retained_initial = self.storage.read_immutable_json(
                    self._journal_generation_path(0, initial_digest)
                )
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            if retained_initial != initial:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            return None
        journal = self._validate_journal(current, plan)
        cursor = journal
        chain: set[tuple[int, str]] = set()
        while True:
            raw = _canonical_bytes(cursor, maximum=_MAX_CONTROL_BYTES)
            digest = hashlib.sha256(raw).hexdigest()
            try:
                retained = self.storage.read_immutable_json(
                    self._journal_generation_path(
                        cursor["journal_generation"], digest
                    )
                )
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            if retained != cursor:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            chain.add((cursor["journal_generation"], digest))
            if cursor["journal_generation"] == 0:
                break
            previous_digest = cursor["previous_journal_sha256"]
            try:
                previous = self.storage.read_immutable_json(
                    self._journal_generation_path(
                        cursor["journal_generation"] - 1, previous_digest
                    )
                )
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            previous = self._validate_journal(previous, plan)
            if _document_sha256(previous) != previous_digest:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            cursor = previous
        extras = set(retained_generations) - chain
        if extras:
            current_generation = journal["journal_generation"]
            current_digest = _document_sha256(journal)
            if len(extras) != 1:
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            child_generation, child_digest = next(iter(extras))
            if child_generation != current_generation + 1:
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            try:
                child = self.storage.read_immutable_json(
                    self._journal_generation_path(child_generation, child_digest)
                )
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            child = self._validate_journal(child, plan)
            if (
                _document_sha256(child) != child_digest
                or child.get("previous_journal_generation") != current_generation
                or child.get("previous_journal_sha256") != current_digest
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        return journal

    def _write_journal(self, journal: Mapping[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(dict(journal))
        raw = _canonical_bytes(value, maximum=_MAX_CONTROL_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        same_generation = {
            retained_digest
            for generation, retained_digest in self._journal_generation_index()
            if generation == value["journal_generation"]
        }
        if same_generation and same_generation != {digest}:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        try:
            self.storage.publish_immutable_bytes(
                self._journal_generation_path(value["journal_generation"], digest),
                raw,
                expected_sha256=digest,
            )
            self._fault("after_batch_journal_generation")
            self.storage.replace_current_json(self._journal_path(), value)
            self._fault("after_batch_journal_current")
            observed = self.storage.read_current_json(self._journal_path())
            retained = self.storage.read_immutable_json(
                self._journal_generation_path(value["journal_generation"], digest)
            )
            if observed != value or retained != value:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        return value

    def _advance_journal(
        self,
        journal: Mapping[str, Any],
        *,
        state: str,
        completed_entries: Sequence[Mapping[str, Any]],
        post_inventory: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = copy.deepcopy(dict(journal))
        previous_raw = _canonical_bytes(value, maximum=_MAX_CONTROL_BYTES)
        value["journal_generation"] += 1
        value["previous_journal_generation"] = journal["journal_generation"]
        value["previous_journal_sha256"] = hashlib.sha256(previous_raw).hexdigest()
        value["state"] = state
        value["completed_entries"] = [
            copy.deepcopy(dict(item)) for item in completed_entries
        ]
        value["post_inventory"] = (
            None if post_inventory is None else copy.deepcopy(dict(post_inventory))
        )
        return self._write_journal(value)

    def _entry_occurrence(
        self,
        reservation: Mapping[str, Any],
        plan: Mapping[str, Any],
    ) -> ArchiveOccurrence:
        return ArchiveOccurrence(
            project_id=self.project_id,
            sprint_id=None,
            response_id=reservation["response_id"],
            response_kind="migration-snapshot",
            occurrence_kind="migration",
            occurrence_id=reservation["occurrence_id"],
            transition_id=None,
            source={"source_kind": "offline_migration"},
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "offline_operator",
                "principal_id": self.operator_principal_id,
                "binding_id": None,
                "scope_source": "offline_operator_gate",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": reservation["decision_occurrence_id"],
            },
            created_at=plan["created_at"],
            verified_at=plan["created_at"],
        )

    def _source_bytes(self, entry: InventoryEntry) -> bytes:
        path = self.input_root.joinpath(*entry.relative_path.split("/"))
        raw, _ = _read_regular_file(
            path, maximum=self.storage.limits.max_hydrated_bytes
        )
        if len(raw) != entry.byte_size or hashlib.sha256(raw).hexdigest() != entry.sha256:
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        return raw

    def _preflight_hydration_maximum(self) -> int:
        """Return the effective bound without bootstrapping archive storage."""

        if self._storage is not None:
            maximum = self._storage.limits.max_hydrated_bytes
        else:
            maximum = PromptArchiveLimits().max_hydrated_bytes
        if type(maximum) is not int or maximum <= 0:
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        return maximum

    def _preflight_selected_legacy_inputs(
        self, inventory: SourceInventory
    ) -> None:
        """Bound and discriminate selected files before any durable output."""

        maximum = self._preflight_hydration_maximum()
        for entry in inventory.entries:
            if not _is_legacy_archive_candidate(entry.relative_path):
                continue
            if entry.byte_size > maximum:
                # Reject before the immutable batch reservation or journal is
                # published.  Otherwise a source accepted by the 512 MiB
                # inventory ceiling could become permanently unresumable under
                # a lower configured hydration ceiling.
                raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
            path = self.input_root.joinpath(*entry.relative_path.split("/"))
            raw, _ = _read_regular_file(path, maximum=maximum)
            if (
                len(raw) != entry.byte_size
                or hashlib.sha256(raw).hexdigest() != entry.sha256
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
            _assert_legacy_input_bytes(raw, maximum=maximum)

    def _verified_entry_plan_binding(
        self,
        plan: Mapping[str, Any],
        reservation: Mapping[str, Any],
        runtime: PromptArchiveRuntime,
        *,
        require_completed: bool,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Cross-bind one retained entry plan to its outer batch authority.

        Exact hydrated bytes are necessary but not sufficient recovery proof.
        The per-entry plan and journal must still belong to this batch,
        inventory, reservation, and offline operator.  Native source-metadata
        sidecars are deliberately excluded from the opaque legacy path.
        """

        response_id = reservation.get("response_id")
        operation_id = reservation.get("operation_id")
        try:
            # Verification and source-free resume both require the retained
            # plan CAS and operation-lock receipt to exist.  Only journal
            # successor adoption is an authorized recovery mutation.
            entry_plan, entry_plan_ref = runtime._read_offline_plan_identity(
                response_id=response_id,
                operation_id=operation_id,
            )
            with self.storage.existing_control_lock(
                runtime._operation_lock_path(entry_plan)
            ):
                entry_journal_pair = runtime._load_journal_chain_locked(
                    entry_plan,
                    entry_plan_ref,
                    adopt_retained_successor=not require_completed,
                )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None

        manifest = entry_plan.get("intended_manifest", {}).get("document")
        integrity = manifest.get("integrity") if type(manifest) is dict else None
        roots = manifest.get("roots") if type(manifest) is dict else None
        authorization = (
            manifest.get("authorization") if type(manifest) is dict else None
        )
        source = manifest.get("source") if type(manifest) is dict else None
        occurrence = entry_plan.get("occurrence")
        source_locator = entry_plan.get("source_locator")
        migration_binding = (
            source_locator.get("migration_binding")
            if type(source_locator) is dict
            else None
        )
        base_source_locator = (
            {
                name: copy.deepcopy(value)
                for name, value in source_locator.items()
                if name != "migration_binding"
            }
            if type(source_locator) is dict
            else None
        )
        journal_state = (
            entry_journal_pair[0].get("state")
            if entry_journal_pair is not None
            else None
        )
        expected_authorization = {
            "auth_model_version": "prompt-archive-authorization-v1",
            "principal_kind": "offline_operator",
            "principal_id": self.operator_principal_id,
            "binding_id": None,
            "scope_source": "offline_operator_gate",
            "effective_scope_revision": None,
            "effective_scope_sha256": None,
            "decision_occurrence_id": reservation.get("decision_occurrence_id"),
        }
        if (
            base_source_locator != plan.get("source_inventory")
            or type(migration_binding) is not dict
            or set(migration_binding)
            != {"batch_id", "batch_plan_sha256", "reservation"}
            or migration_binding.get("batch_id") != self.batch_id
            or migration_binding.get("batch_plan_sha256")
            != _document_sha256(plan)
            or migration_binding.get("reservation") != reservation
            or entry_plan.get("operation_id") != operation_id
            or type(occurrence) is not dict
            or occurrence
            != {
                "response_id": response_id,
                "response_kind": "migration-snapshot",
                "occurrence_kind": "migration",
                "occurrence_id": reservation.get("occurrence_id"),
                "transition_id": None,
            }
            or type(manifest) is not dict
            or manifest.get("archive_format") != "legacy_raw_chunks_v1"
            or source != {"source_kind": "offline_migration"}
            or authorization != expected_authorization
            or type(roots) is not dict
            or set(roots) != {"raw_bytes"}
            or "source_metadata_receipt" in manifest
            or type(integrity) is not dict
            or integrity.get("hydrated_size")
            != reservation.get("source_byte_size")
            or integrity.get("hydrated_sha256")
            != reservation.get("source_sha256")
            or entry_journal_pair is None
            or (
                require_completed
                and journal_state != "complete"
            )
            or (
                not require_completed
                and journal_state in {None, "intent", "aborted"}
            )
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        return entry_plan, manifest

    def _verify_completed_entry(
        self,
        plan: Mapping[str, Any],
        result: Mapping[str, Any],
        reservation: Mapping[str, Any],
        expected: InventoryEntry,
        runtime: PromptArchiveRuntime,
    ) -> bytes:
        if (
            result.get("entry_id") != reservation["entry_id"]
            or result.get("ordinal") != reservation["ordinal"]
            or result.get("operation_id") != reservation["operation_id"]
            or result.get("response_id") != reservation["response_id"]
            or result.get("source_byte_size") != expected.byte_size
            or result.get("source_sha256") != expected.sha256
            or result.get("matched") is not True
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        entry_plan, manifest = self._verified_entry_plan_binding(
            plan,
            reservation,
            runtime,
            require_completed=True,
        )
        intended_manifest = entry_plan["intended_manifest"]
        intended_pointer = entry_plan["intended_pointer"]
        if (
            result.get("archive_id") != intended_manifest.get("archive_id")
            or result.get("pointer_id") != intended_pointer.get("pointer_id")
            or manifest.get("archive_id") != result.get("archive_id")
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        try:
            selected = runtime.lookup_offline_archive_metadata(
                archive_id=result["archive_id"]
            )
            hydrated = runtime.hydrate_content(selected)
        except (PromptArchiveStorageError, KeyError, TypeError) as exc:
            if isinstance(exc, PromptArchiveStorageError):
                raise _error_from_storage(exc) from None
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT") from None
        if (
            selected.archive_id != result.get("archive_id")
            or selected.pointer.get("pointer_id") != result.get("pointer_id")
            or selected.manifest.get("scope", {}).get("response_id")
            != reservation["response_id"]
            or selected.manifest.get("roots", {}).keys() != {"raw_bytes"}
            or "source_metadata_receipt" in selected.manifest
            or len(hydrated) != expected.byte_size
            or hashlib.sha256(hydrated).hexdigest() != expected.sha256
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        return hydrated

    @staticmethod
    def _entry_for_reservation(
        inventory: SourceInventory,
        reservation: Mapping[str, Any],
    ) -> InventoryEntry:
        ordinal = reservation.get("ordinal")
        if type(ordinal) is not int or not 0 <= ordinal < len(inventory.entries):
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        entry = inventory.entries[ordinal]
        if (
            entry.path_sha256 != reservation.get("path_sha256")
            or entry.byte_size != reservation.get("source_byte_size")
            or entry.sha256 != reservation.get("source_sha256")
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        return entry

    def _inventory_verifier(
        self,
        plan: Mapping[str, Any],
    ) -> Callable[[Mapping[str, Any]], str]:
        expected = copy.deepcopy(plan["source_inventory"])

        def verify(locator: Mapping[str, Any]) -> str:
            if dict(locator) != expected:
                return "source_changed"
            try:
                observed = inventory_isolated_root(self.input_root)
            except PromptArchiveMigrationError as exc:
                if exc.code in {
                    "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED",
                    "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE",
                }:
                    return "source_changed"
                return "source_unavailable"
            descriptor = self._inventory_descriptor(
                observed, created_at=plan["created_at"]
            )
            try:
                retained_descriptor = self.storage.read_immutable_json(
                    (
                        "inventories",
                        "v1",
                        f"{expected['inventory_id']}.json",
                    )
                )
            except PromptArchiveStorageError:
                return "source_unavailable"
            return (
                "verified"
                if (
                    descriptor["inventory_id"] == expected["inventory_id"]
                    and descriptor["inventory_sha256"]
                    == expected["inventory_sha256"]
                    and descriptor["entry_count"] == expected["entry_count"]
                    and retained_descriptor == descriptor
                )
                else "source_changed"
            )

        return verify

    def _evidence(
        self,
        *,
        plan: Mapping[str, Any],
        journal: Mapping[str, Any],
        status: str,
        private_evidence_sha256: str,
        private_evidence_byte_size: int,
    ) -> dict[str, Any]:
        entries = copy.deepcopy(journal["completed_entries"])
        try:
            retained_descriptor = self.storage.read_immutable_json(
                (
                    "inventories",
                    "v1",
                    f"{plan['source_inventory']['inventory_id']}.json",
                )
            )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        if (
            type(retained_descriptor) is not dict
            or type(retained_descriptor.get("source_root_identity_sha256"))
            is not str
            or _SHA256_RE.fullmatch(
                retained_descriptor["source_root_identity_sha256"]
            )
            is None
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        return {
            "schema_version": 1,
            "kind": "prompt_archive_migration_evidence",
            "media_type": _EVIDENCE_MEDIA,
            "command": "migrate",
            "status": status,
            "batch_id": self.batch_id,
            "project_id": self.project_id,
            "sprint_id": None,
            "contract_version": CONTRACT_VERSION,
            "tool_version": plan["tool_version"],
            "tool_source_sha256": plan["tool_source_sha256"],
            "plan_sha256": _document_sha256(plan),
            "source_inventory": {
                "inventory_id": plan["source_inventory"]["inventory_id"],
                "source_root_identity_sha256": retained_descriptor[
                    "source_root_identity_sha256"
                ],
                "inventory_sha256": plan["source_inventory"]["inventory_sha256"],
                "entry_count": plan["source_inventory"]["entry_count"],
                "selected_archive_count": len(plan["reservations"]),
            },
            "journal": {
                "generation": journal["journal_generation"],
                "sha256": _document_sha256(journal),
                "state": journal["state"],
                "pre_inventory": copy.deepcopy(journal["pre_inventory"]),
                "post_inventory": copy.deepcopy(journal["post_inventory"]),
            },
            "archives": entries,
            "private_evidence": {
                "sha256": private_evidence_sha256,
                "byte_size": private_evidence_byte_size,
            },
            "safety": {
                "explicit_isolated_roots": True,
                "public_paths_persisted": False,
                "operator_gate_verified": True,
                "originals_modified": False,
                "originals_deleted": False,
                "archive_gc_performed": False,
                "live_rewrite_performed": False,
            },
            "created_at": plan["created_at"],
            "completed_at": plan["created_at"] if journal["state"] == "complete" else None,
        }

    def analyze(self) -> dict[str, Any]:
        """Inventory an explicit source without creating or opening archive output."""

        self._assert_source_read_ready()
        inventory = inventory_isolated_root(self.input_root)
        self._preflight_selected_legacy_inputs(inventory)
        selected = [
            {
                "ordinal": ordinal,
                "path_sha256": entry.path_sha256,
                "source_byte_size": entry.byte_size,
                "source_sha256": entry.sha256,
            }
            for ordinal, entry in enumerate(inventory.entries)
            if _is_legacy_archive_candidate(entry.relative_path)
        ]
        return {
            "schema_version": 1,
            "kind": "prompt_archive_migration_analysis",
            "media_type": _EVIDENCE_MEDIA,
            "command": "analyze",
            "status": "ready" if selected else "no_candidates",
            "batch_id": self.batch_id,
            "project_id": self.project_id,
            "contract_version": CONTRACT_VERSION,
            "tool_version": TOOL_VERSION,
            "tool_source_sha256": _tool_source_sha256(),
            "inventory": {
                "inventory_sha256": inventory.inventory_sha256,
                "safety_snapshot_sha256": inventory.safety_snapshot_sha256,
                "entry_count": len(inventory.entries),
                "total_bytes": inventory.total_bytes,
                "selected_archive_count": len(selected),
            },
            "selected_archives": selected,
            "safety": {
                "read_only": True,
                "archive_root_opened": False,
                "public_paths_persisted": False,
                "originals_modified": False,
            },
        }

    def _private_evidence_bytes(
        self,
        plan: Mapping[str, Any],
        pre: SourceInventory,
        post: SourceInventory,
    ) -> bytes:
        if pre.entries != post.entries:
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
        document = {
            "schema_version": 1,
            "kind": "prompt_archive_migration_private_evidence",
            "media_type": _PRIVATE_EVIDENCE_MEDIA,
            "batch_id": self.batch_id,
            "project_id": self.project_id,
            "source_root_handle_id": self.source_root_handle_id,
            "inventory_id": plan["source_inventory"]["inventory_id"],
            "pre": {
                "source_root_identity": copy.deepcopy(dict(pre.source_root_identity)),
                "source_root_identity_sha256": pre.source_root_identity_sha256,
                "inventory_sha256": pre.inventory_sha256,
                "safety_snapshot_sha256": pre.safety_snapshot_sha256,
                "total_bytes": pre.total_bytes,
                "entries": [entry.document() for entry in pre.entries],
            },
            "post": {
                "source_root_identity": copy.deepcopy(dict(post.source_root_identity)),
                "source_root_identity_sha256": post.source_root_identity_sha256,
                "inventory_sha256": post.inventory_sha256,
                "safety_snapshot_sha256": post.safety_snapshot_sha256,
                "total_bytes": post.total_bytes,
                "entries": [entry.document() for entry in post.entries],
            },
            "comparison": {
                "entry_count": len(pre.entries),
                "paths_sizes_mtimes_hashes_identities_equal": True,
                "originals_modified": False,
                "originals_deleted": False,
            },
            "observed_at": plan["created_at"],
        }
        return _canonical_bytes(
            document,
            maximum=_MAX_PRIVATE_EVIDENCE_BYTES,
        )

    def migrate(self) -> dict[str, Any]:
        """Migrate selected opaque legacy archives through accepted PAS1 state."""

        if self.private_evidence_file is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        self._assert_migration_access_ready()
        pre = inventory_isolated_root(self.input_root)
        self._fault("after_full_pre_inventory")
        self._preflight_selected_legacy_inputs(pre)
        if not any(
            _is_legacy_archive_candidate(entry.relative_path)
            for entry in pre.entries
        ):
            # Do not bootstrap the archive root merely to discover that this
            # explicit source has no migratable legacy response.
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        # The immutable reservation is deliberately published outside the
        # batch lock: first-writer-wins resolves two fresh processes without
        # writing inventory/archive CAS under unpersisted random operation IDs.
        plan = self._prepare(pre)
        runtime = self._runtime_instance()
        descriptor = self._inventory_descriptor(pre, created_at=plan["created_at"])
        with self._batch_control_lock():
            # A winner may have been published by a concurrent process before
            # this process acquired the lock.  Re-read and bind all work to it.
            retained_plan = self._load_plan_optional()
            if retained_plan is None:
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            plan = self._validate_plan(retained_plan, pre)
            # Prove that the complete, lossless private witness fits its
            # normative bound before creating any per-entry PAS1 graph.  The
            # already-retained batch reservation is harmless and resumable;
            # a size failure cannot leave partially published archives.
            preflight_private_raw = self._private_evidence_bytes(plan, pre, pre)
            _preflight_private_evidence_file(
                self.private_evidence_file,
                preflight_private_raw,
            )
            self._fault("after_private_evidence_preflight")
            journal = self._read_journal(plan)
            if journal is None:
                journal = self._write_journal(self._initial_journal(plan))
            completed = [copy.deepcopy(item) for item in journal["completed_entries"]]
            for index, result in enumerate(completed):
                reservation = plan["reservations"][index]
                entry = self._entry_for_reservation(pre, reservation)
                self._verify_completed_entry(
                    plan,
                    result,
                    reservation,
                    entry,
                    runtime,
                )

            if journal["state"] != "complete":
                for reservation in plan["reservations"][len(completed) :]:
                    entry = self._entry_for_reservation(pre, reservation)
                    self._fault("before_source_read")
                    first = self._source_bytes(entry)
                    self._fault("after_source_read")
                    # Raw files are intentionally opaque.  No UTF-8 or JSON
                    # parser is allowed on this preservation path.
                    raw = self._source_bytes(entry)
                    if raw != first:
                        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
                    occurrence = self._entry_occurrence(reservation, plan)
                    try:
                        publication = runtime.publish_offline_migration(
                            raw,
                            occurrence,
                            operation_id=reservation["operation_id"],
                            inventory_descriptor=descriptor,
                            inventory_ref=plan["source_inventory"]["inventory_ref"],
                            inventory_verifier=self._inventory_verifier(plan),
                        )
                    except PromptArchiveStorageError as exc:
                        raise _error_from_storage(exc) from None
                    self._fault("after_entry_publication")
                    result = {
                        "entry_id": reservation["entry_id"],
                        "ordinal": reservation["ordinal"],
                        "operation_id": reservation["operation_id"],
                        "response_id": reservation["response_id"],
                        "archive_id": publication.archive_id,
                        "pointer_id": publication.pointer_id,
                        "source_byte_size": entry.byte_size,
                        "source_sha256": entry.sha256,
                        "matched": True,
                    }
                    hydrated = self._verify_completed_entry(
                        plan,
                        result,
                        reservation,
                        entry,
                        runtime,
                    )
                    if hydrated != raw or self._source_bytes(entry) != raw:
                        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
                    completed.append(result)
                    journal = self._advance_journal(
                        journal,
                        state="running",
                        completed_entries=completed,
                    )
                    self._fault("after_entry_checkpoint")

                self._fault("before_full_post_inventory")
                post = inventory_isolated_root(self.input_root)
                if (
                    post.inventory_sha256 != pre.inventory_sha256
                    or post.source_root_identity_sha256
                    != pre.source_root_identity_sha256
                    or post.safety_snapshot_sha256 != pre.safety_snapshot_sha256
                    or post.entries != pre.entries
                ):
                    raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
                self._fault("after_full_post_inventory")
                private_raw = preflight_private_raw
                private_sha, private_size = _publish_private_evidence_file(
                    self.private_evidence_file,
                    private_raw,
                    ownership_id=plan["reservations"][0]["operation_id"],
                )
                self._fault("after_private_evidence_publication")
                journal = self._advance_journal(
                    journal,
                    state="complete",
                    completed_entries=completed,
                    post_inventory={
                        "inventory_sha256": post.inventory_sha256,
                        "entry_count": len(post.entries),
                        "source_root_identity_sha256": post.source_root_identity_sha256,
                        "safety_snapshot_sha256": post.safety_snapshot_sha256,
                        "total_bytes": post.total_bytes,
                    },
                )
            else:
                post = inventory_isolated_root(self.input_root)
                if post.entries != pre.entries:
                    raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
                private_raw = preflight_private_raw
                private_sha, private_size = _publish_private_evidence_file(
                    self.private_evidence_file,
                    private_raw,
                    ownership_id=plan["reservations"][0]["operation_id"],
                )

            evidence = self._evidence(
                plan=plan,
                journal=journal,
                status="complete",
                private_evidence_sha256=private_sha,
                private_evidence_byte_size=private_size,
            )
            evidence_raw = _canonical_bytes(
                evidence, maximum=_MAX_CONTROL_BYTES
            )
            try:
                self.storage.publish_immutable_bytes(
                    self._evidence_path(),
                    evidence_raw,
                    expected_sha256=hashlib.sha256(evidence_raw).hexdigest(),
                )
                retained_evidence = self.storage.read_immutable_json(
                    self._evidence_path()
                )
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            if retained_evidence != evidence:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            self._fault("after_complete_checkpoint")
            return evidence

    def _require_existing_plan(
        self,
        inventory: SourceInventory,
    ) -> dict[str, Any]:
        plan = self._load_plan_optional()
        if plan is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_NOT_COMPLETE")
        plan = self._validate_plan(plan, inventory)
        descriptor = self._inventory_descriptor(
            inventory, created_at=plan["created_at"]
        )
        raw = _canonical_bytes(descriptor, maximum=_MAX_CONTROL_BYTES)
        try:
            if (
                self.storage.read_immutable_json(
                    ("inventories", "v1", f"{descriptor['inventory_id']}.json")
                )
                != descriptor
                or self.storage.read_blob(
                    plan["source_inventory"]["inventory_ref"],
                    max_output_bytes=_MAX_CONTROL_BYTES,
                )
                != raw
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        return plan

    def resume_operation(
        self,
        *,
        response_id: str,
        operation_id: str,
    ) -> dict[str, Any]:
        """Resume one reserved PAS1 entry without opening the source tree.

        This is intentionally narrower than batch completion.  It lets an
        entry that already crossed ``blobs_verified`` converge through its
        retained plan/journal when the isolated source is temporarily absent;
        the batch still requires a later full post-inventory before complete.
        """

        if (
            type(response_id) is not str
            or _RESPONSE_ID_RE.fullmatch(response_id) is None
            or type(operation_id) is not str
            or _OPERATION_ID_RE.fullmatch(operation_id) is None
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        self._assert_resume_access_ready()
        self._storage_instance(require_existing_root=True)
        try:
            plan = self.storage.read_immutable_json(self._plan_path())
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        plan = self._validate_plan_envelope(plan)
        # The outer current/generation chain is the retained integrity binding
        # for the immutable batch plan.  Validate the entire chain (including
        # plan_sha256) before trusting any reservation from that plan.
        outer_journal = self._read_journal(plan)
        if outer_journal is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_NOT_COMPLETE")
        matches = [
            reservation
            for reservation in plan["reservations"]
            if type(reservation) is dict
            and reservation.get("response_id") == response_id
            and reservation.get("operation_id") == operation_id
        ]
        if len(matches) != 1:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        reservation = matches[0]
        runtime = self._runtime_instance()
        try:
            # Validate the accepted per-entry PAS1 plan and its full retained
            # journal chain before the source-free continuation.  This binds
            # the outer reservation to the inventory locator, immutable
            # manifest graph, operation/occurrence IDs, and content digest.
            self._verified_entry_plan_binding(
                plan,
                reservation,
                runtime,
                require_completed=False,
            )
            publication = runtime.resume_offline_migration(
                response_id=response_id,
                operation_id=operation_id,
                inventory_verifier=None,
            )
            selected = runtime.lookup_offline_archive_metadata(
                archive_id=publication.archive_id
            )
            hydrated = runtime.hydrate_content(selected)
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        if (
            len(hydrated) != reservation.get("source_byte_size")
            or hashlib.sha256(hydrated).hexdigest()
            != reservation.get("source_sha256")
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        return {
            "schema_version": 1,
            "kind": "prompt_archive_migration_operation_resume_evidence",
            "media_type": _EVIDENCE_MEDIA,
            "command": "resume-operation",
            "status": "completed",
            "batch_id": self.batch_id,
            "entry_id": reservation["entry_id"],
            "operation_id": operation_id,
            "response_id": response_id,
            "archive_id": publication.archive_id,
            "pointer_id": publication.pointer_id,
            "source_byte_size": len(hydrated),
            "source_sha256": hashlib.sha256(hydrated).hexdigest(),
            "batch_completion_deferred_until_post_inventory": True,
            "outer_journal_generation": outer_journal["journal_generation"],
            "source_tree_opened": False,
        }

    def verify_rollback(self) -> dict[str, Any]:
        """Verify originals and archived raw-byte readability without mutation."""

        if self.private_evidence_file is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        self._assert_migration_access_ready()
        self._storage_instance(require_existing_root=True)
        inventory = inventory_isolated_root(self.input_root)
        with self._batch_control_lock(existing_only=True):
            plan = self._require_existing_plan(inventory)
            journal = self._read_journal(plan)
            if journal is None or journal["state"] != "complete":
                raise _error("PROMPT_ARCHIVE_MIGRATION_NOT_COMPLETE")
            runtime = self._runtime_instance()
            for reservation, result in zip(
                plan["reservations"], journal["completed_entries"]
            ):
                expected = self._entry_for_reservation(inventory, reservation)
                self._verify_completed_entry(
                    plan,
                    result,
                    reservation,
                    expected,
                    runtime,
                )
            private_raw = self._private_evidence_bytes(plan, inventory, inventory)
            observed_private = _read_private_evidence_file(
                self.private_evidence_file,
                maximum=_MAX_PRIVATE_EVIDENCE_BYTES,
            )
            if observed_private != private_raw:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            private_sha = hashlib.sha256(private_raw).hexdigest()
            expected_evidence = self._evidence(
                plan=plan,
                journal=journal,
                status="verified",
                private_evidence_sha256=private_sha,
                private_evidence_byte_size=len(private_raw),
            )
            # Stored migration evidence always records completion; construct
            # that exact stable document before comparing its identity.
            expected_evidence["status"] = "complete"
            try:
                retained = self.storage.read_immutable_json(self._evidence_path())
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            if retained != expected_evidence:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            return {
                "schema_version": 1,
                "kind": "prompt_archive_migration_verification",
                "media_type": _EVIDENCE_MEDIA,
                "command": "rollback-verify",
                "status": "verified",
                "batch_id": self.batch_id,
                "migration_evidence_sha256": _document_sha256(retained),
                "private_evidence_sha256": private_sha,
                "entry_count": len(journal["completed_entries"]),
                "originals_equal": True,
                "archives_readable": True,
            }

    def _inventory_from_private_evidence(self) -> tuple[SourceInventory, bytes]:
        if self.private_evidence_file is None:
            raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
        raw = _read_private_evidence_file(
            self.private_evidence_file,
            maximum=_MAX_PRIVATE_EVIDENCE_BYTES,
        )
        try:
            document = parse_canonical_json_v1(
                raw,
                maximum=_MAX_PRIVATE_EVIDENCE_BYTES,
            )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        if type(document) is not dict or set(document) != {
            "schema_version",
            "kind",
            "media_type",
            "batch_id",
            "project_id",
            "source_root_handle_id",
            "inventory_id",
            "pre",
            "post",
            "comparison",
            "observed_at",
        } or (
            document.get("schema_version") != 1
            or document.get("kind")
            != "prompt_archive_migration_private_evidence"
            or document.get("media_type") != _PRIVATE_EVIDENCE_MEDIA
            or document.get("batch_id") != self.batch_id
            or document.get("project_id") != self.project_id
            or document.get("source_root_handle_id")
            != self.source_root_handle_id
            or type(document.get("inventory_id")) is not str
            or _INVENTORY_ID_RE.fullmatch(document["inventory_id"]) is None
            or type(document.get("observed_at")) is not str
            or _UTC_TIMESTAMP_RE.fullmatch(document["observed_at"]) is None
            or document.get("pre") != document.get("post")
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        comparison = document.get("comparison")
        if type(comparison) is not dict or set(comparison) != {
            "entry_count",
            "paths_sizes_mtimes_hashes_identities_equal",
            "originals_modified",
            "originals_deleted",
        } or (
            comparison.get("paths_sizes_mtimes_hashes_identities_equal")
            is not True
            or comparison.get("originals_modified") is not False
            or comparison.get("originals_deleted") is not False
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        pre = document["pre"]
        if type(pre) is not dict or set(pre) != {
            "source_root_identity",
            "source_root_identity_sha256",
            "inventory_sha256",
            "safety_snapshot_sha256",
            "total_bytes",
            "entries",
        } or (
            type(pre.get("entries")) is not list
            or not 1 <= len(pre["entries"]) <= _MAX_FILES
            or not _valid_root_identity(pre.get("source_root_identity"))
            or type(pre.get("source_root_identity_sha256")) is not str
            or _SHA256_RE.fullmatch(pre["source_root_identity_sha256"])
            is None
            or type(pre.get("inventory_sha256")) is not str
            or _SHA256_RE.fullmatch(pre["inventory_sha256"]) is None
            or type(pre.get("safety_snapshot_sha256")) is not str
            or _SHA256_RE.fullmatch(pre["safety_snapshot_sha256"]) is None
            or type(pre.get("total_bytes")) is not int
            or not 0 <= pre["total_bytes"] <= _MAX_TOTAL_BYTES
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        entries: list[InventoryEntry] = []
        folded: set[str] = set()
        for item in pre["entries"]:
            if type(item) is not dict or set(item) != {
                "relative_path",
                "path_sha256",
                "byte_size",
                "sha256",
                "mtime_ns",
                "file_identity",
                "identity_sha256",
                "link_count",
            }:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            relative = item.get("relative_path")
            if type(relative) is not str or not relative:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            components = relative.split("/")
            for component in components:
                _validate_component(component)
            _claim_casefolded_path(folded, relative)
            identity = item.get("file_identity")
            if not _valid_file_identity(identity):
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            try:
                identity_raw = _canonical_bytes(identity, maximum=4096)
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            if (
                item.get("path_sha256")
                != hashlib.sha256(relative.encode("utf-8")).hexdigest()
                or type(item.get("byte_size")) is not int
                or not 0 <= item["byte_size"] <= _MAX_FILE_BYTES
                or type(item.get("sha256")) is not str
                or _SHA256_RE.fullmatch(item["sha256"]) is None
                or type(item.get("mtime_ns")) is not int
                or not -(2**63) <= item["mtime_ns"] < 2**63
                or item.get("identity_sha256")
                != hashlib.sha256(identity_raw).hexdigest()
                or item.get("link_count") != 1
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            entries.append(
                InventoryEntry(
                    relative_path=relative,
                    path_sha256=item["path_sha256"],
                    byte_size=item["byte_size"],
                    sha256=item["sha256"],
                    mtime_ns=item["mtime_ns"],
                    file_identity=copy.deepcopy(identity),
                    identity_sha256=item["identity_sha256"],
                    link_count=1,
                )
            )
        entries.sort(key=lambda entry: entry.relative_path.encode("utf-8"))
        if comparison.get("entry_count") != len(entries):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        root_identity = pre.get("source_root_identity")
        try:
            root_identity_raw = _canonical_bytes(
                root_identity, maximum=4096
            )
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        inventory = SourceInventory(
            source_root_identity=copy.deepcopy(root_identity),
            source_root_identity_sha256=hashlib.sha256(
                root_identity_raw
            ).hexdigest(),
            inventory_sha256=_inventory_digest(entries),
            safety_snapshot_sha256=_safety_snapshot_digest(entries),
            total_bytes=sum(entry.byte_size for entry in entries),
            entries=tuple(entries),
        )
        if (
            inventory.source_root_identity_sha256
            != pre.get("source_root_identity_sha256")
            or inventory.inventory_sha256 != pre.get("inventory_sha256")
            or inventory.safety_snapshot_sha256
            != pre.get("safety_snapshot_sha256")
            or inventory.total_bytes != pre.get("total_bytes")
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        return inventory, raw

    def reconstruct(
        self,
        reconstruction_root: str | os.PathLike[str],
    ) -> dict[str, Any]:
        """Reconstruct exact raw files into a separate explicit isolated root."""

        target = _absolute_root(reconstruction_root)
        _assert_roots_isolated(
            self.input_root,
            self.archive_root,
            self.protected_roots,
            reconstruction=target,
        )
        canonical_candidates = [self.archive_root, target]
        if self.private_evidence_file is not None:
            canonical_candidates.append(self.private_evidence_file)
        _assert_canonical_candidates_isolated(
            canonical_candidates,
            self.protected_roots,
            self._alias_sensitive_protected_roots,
        )
        if self.private_evidence_file is not None and _overlap(
            target, self.private_evidence_file
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
        self._storage_instance(require_existing_root=True)
        inventory, private_raw = self._inventory_from_private_evidence()
        with self._batch_control_lock(existing_only=True):
            plan = self._require_existing_plan(inventory)
            journal = self._read_journal(plan)
            if journal is None or journal["state"] != "complete":
                raise _error("PROMPT_ARCHIVE_MIGRATION_NOT_COMPLETE")
            try:
                public_evidence = self.storage.read_immutable_json(
                    self._evidence_path()
                )
            except PromptArchiveStorageError as exc:
                raise _error_from_storage(exc) from None
            private_sha = hashlib.sha256(private_raw).hexdigest()
            expected_public_evidence = self._evidence(
                plan=plan,
                journal=journal,
                status="complete",
                private_evidence_sha256=private_sha,
                private_evidence_byte_size=len(private_raw),
            )
            if (
                type(public_evidence) is not dict
                or public_evidence != expected_public_evidence
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            runtime = self._runtime_instance()
            _ensure_reconstruction_root(target)
            with _RootPin(target) as target_pin:
                if target_pin.identity == inventory.source_root_identity:
                    # The private witness identifies the original root by
                    # filesystem object, so bind-mount/hard alias spellings
                    # cannot turn reconstruction into an in-place rewrite.
                    raise _error("PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP")
                target_pin.assert_stable()
            reconstruction_specs = [
                (
                    self._entry_for_reservation(inventory, reservation),
                    reservation["operation_id"],
                )
                for reservation in plan["reservations"]
            ]
            _preflight_reconstruction_root(target, reconstruction_specs)
            reconstructed_entries: list[dict[str, Any]] = []
            expected_entries: list[InventoryEntry] = []
            for reservation, result in zip(
                plan["reservations"], journal["completed_entries"]
            ):
                entry = self._entry_for_reservation(inventory, reservation)
                raw = self._verify_completed_entry(
                    plan,
                    result,
                    reservation,
                    entry,
                    runtime,
                )
                _publish_reconstructed_file(
                    target,
                    entry,
                    raw,
                    ownership_id=reservation["operation_id"],
                )
                expected_entries.append(entry)
                reconstructed_entries.append(
                    {
                        "entry_id": reservation["entry_id"],
                        "ordinal": reservation["ordinal"],
                        "response_id": reservation["response_id"],
                        "archive_id": result["archive_id"],
                        "source_byte_size": entry.byte_size,
                        "source_sha256": entry.sha256,
                        "reconstructed_byte_size": len(raw),
                        "reconstructed_sha256": hashlib.sha256(raw).hexdigest(),
                        "matched": True,
                        "journal_state": journal["state"],
                    }
                )
            reconstructed = inventory_isolated_root(target)
            observed_shape = tuple(
                (entry.relative_path, entry.byte_size, entry.sha256)
                for entry in reconstructed.entries
            )
            expected_shape = tuple(
                (entry.relative_path, entry.byte_size, entry.sha256)
                for entry in expected_entries
            )
            if observed_shape != expected_shape:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            return {
                "schema_version": 1,
                "kind": "prompt_archive_migration_reconstruction_evidence",
                "media_type": _EVIDENCE_MEDIA,
                "command": "reconstruct",
                "status": "verified",
                "batch_id": self.batch_id,
                "source_inventory_id": plan["source_inventory"]["inventory_id"],
                "entry_count": len(reconstructed_entries),
                "entries": reconstructed_entries,
                "raw_files_equal": True,
                "public_paths_persisted": False,
                "source_tree_opened": False,
            }


def _validate_pinned_mutation_directory(
    pinned: _PinnedDirectory,
    intended: Path,
) -> None:
    resolved = (
        _windows_path_from_handle(pinned.descriptor)
        if os.name == "nt"
        else resolved_path_from_open_descriptor(pinned.descriptor)
    )
    if resolved is None or not _same_path(resolved, intended):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
    try:
        metadata = intended.lstat()
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED") from None
    if not stat.S_ISDIR(metadata.st_mode) or _is_redirected(intended):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    _windows_reject_named_streams(intended, directory=True)
    if os.name != "nt":
        try:
            opened = os.fstat(pinned.descriptor)
        except OSError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
        if (
            not stat.S_ISDIR(opened.st_mode)
            or (opened.st_dev, opened.st_ino)
            != (metadata.st_dev, metadata.st_ino)
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")


def _open_pinned_mutation_directory(path: Path) -> _PinnedDirectory:
    _assert_existing_chain_safe(path)
    try:
        descriptor = (
            _windows_open_pinned_directory(path)
            if os.name == "nt"
            else os.open(
                path,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
        )
    except PromptArchiveStorageError as exc:
        raise _error_from_storage(exc) from None
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    pinned = _PinnedDirectory(path, descriptor)
    try:
        _validate_pinned_mutation_directory(pinned, path)
    except BaseException:
        pinned.close()
        raise
    return pinned


def _open_or_create_pinned_child(
    parent: _PinnedDirectory,
    component: str,
) -> tuple[_PinnedDirectory, bool]:
    _validate_component(component)
    intended = parent.path / component
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
    except PromptArchiveStorageError as exc:
        raise _error_from_storage(exc) from None
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    child = _PinnedDirectory(intended, descriptor)
    try:
        _validate_pinned_mutation_directory(parent, parent.path)
        _validate_pinned_mutation_directory(child, intended)
    except BaseException:
        child.close()
        raise
    if created:
        PromptArchiveStorage._sync_pinned_directory(parent)
    return child, created


@contextmanager
def _pinned_relative_parent(
    root: Path,
    relative: str,
) -> Iterable[tuple[_PinnedDirectory, str, Path]]:
    components = relative.split("/")
    if not components:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    for component in components:
        _validate_component(component)
    pins: list[_PinnedDirectory] = []
    try:
        current = _open_pinned_mutation_directory(root)
        pins.append(current)
        for component in components[:-1]:
            current, _created = _open_or_create_pinned_child(
                current,
                component,
            )
            pins.append(current)
        yield current, components[-1], current.path / components[-1]
        for pin in pins:
            _validate_pinned_mutation_directory(pin, pin.path)
    finally:
        for pin in reversed(pins):
            pin.close()


def _read_pinned_leaf(
    parent: _PinnedDirectory,
    name: str,
    path: Path,
    *,
    maximum: int,
) -> tuple[bytes, os.stat_result]:
    raw, _size, _digest, metadata = _inspect_regular_file(
        path,
        maximum=maximum,
        collect=True,
        parent_descriptor=None if os.name == "nt" else parent.descriptor,
        relative_name=None if os.name == "nt" else name,
    )
    assert raw is not None
    return raw, metadata


def _read_private_evidence_file(path: Path, *, maximum: int) -> bytes:
    try:
        with _open_pinned_mutation_directory(path.parent) as parent:
            raw, metadata = _read_pinned_leaf(
                parent,
                path.name,
                path,
                maximum=maximum,
            )
            _validate_pinned_mutation_directory(parent, path.parent)
    except PromptArchiveMigrationError as exc:
        if exc.code == "PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE":
            raise _error("PROMPT_ARCHIVE_MIGRATION_NOT_COMPLETE") from None
        raise
    if metadata.st_nlink != 1:
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    return raw


def _preflight_private_evidence_leaf_identity(path: Path) -> None:
    """Require an existing witness target to be one unaliased regular leaf.

    This role-only check intentionally runs before archive storage bootstrap.
    Exact witness bytes are compared later once the durable plan is known.
    """

    _validate_component(path.name)
    # The writer deliberately never creates the operator-selected parent.  Pin
    # and validate it now so a missing/unsafe parent cannot be discovered only
    # after the archive reservation has been published.
    try:
        parent_metadata = path.parent.lstat()
    except FileNotFoundError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID") from None
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    if not stat.S_ISDIR(parent_metadata.st_mode) or _is_redirected(path.parent):
        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
    with _open_pinned_mutation_directory(path.parent) as parent:
        try:
            metadata = (
                path.lstat()
                if os.name == "nt"
                else os.stat(
                    path.name,
                    dir_fd=parent.descriptor,
                    follow_symlinks=False,
                )
            )
        except FileNotFoundError:
            _validate_pinned_mutation_directory(parent, path.parent)
            return
        except OSError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_ISLNK(metadata.st_mode)
            or bool(
                _REPARSE_POINT
                and getattr(metadata, "st_file_attributes", 0) & _REPARSE_POINT
            )
            or (os.name == "nt" and _is_redirected(path))
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        _windows_reject_named_streams(path)
        _validate_pinned_mutation_directory(parent, path.parent)


def _windows_open_relative_file(
    directory_handle: int,
    name: str,
    *,
    create: bool,
) -> int:
    """Open a non-reparse regular leaf relative to a pinned directory."""

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
        0x00000100,
        # Hold the publication leaf without write/delete sharing.  A second
        # handle may inspect it, but it cannot add/modify an alternate stream,
        # replace the pathname, or delete the leaf between safety checks and
        # the handle-relative rename below.
        0x00000001,
        2 if create else 1,
        0x00000040 | 0x00000020 | 0x00200000,
        None,
        0,
    )
    if status != 0:
        # STATUS_OBJECT_NAME_COLLISION is the expected exclusive-create race.
        if create and int(status) & 0xFFFFFFFF == 0xC0000035:
            raise FileExistsError(name)
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")
    try:
        return msvcrt.open_osfhandle(
            int(file_handle.value),
            os.O_RDWR | getattr(os, "O_BINARY", 0),
        )
    except (OSError, ValueError):
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle(file_handle)
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None


def _owned_temp_name(target_name: str, label: str, ownership_id: str) -> str:
    if _OPERATION_ID_RE.fullmatch(ownership_id) is None:
        raise _error("PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID")
    _validate_component(target_name)
    _validate_component(label)
    target_tag = hashlib.sha256(target_name.encode("utf-8")).hexdigest()[:16]
    name = f".{label}-{ownership_id}-{target_tag}.tmp"
    _validate_component(name)
    return name


def _descriptor_matches_relative_leaf(
    parent: _PinnedDirectory,
    name: str,
    descriptor: int,
    path: Path,
) -> bool:
    try:
        opened = os.fstat(descriptor)
        if os.name == "nt":
            observed = path.lstat()
        else:
            observed = os.stat(name, dir_fd=parent.descriptor, follow_symlinks=False)
    except OSError:
        return False
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or opened.st_nlink != 1
        or observed.st_nlink != 1
        or stat.S_ISLNK(observed.st_mode)
        or bool(
            _REPARSE_POINT
            and getattr(observed, "st_file_attributes", 0) & _REPARSE_POINT
        )
        or (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino)
    ):
        return False
    resolved = resolved_path_from_open_descriptor(descriptor)
    return resolved is not None and _same_path(resolved, path)


def _open_owned_relative_temp(
    parent: _PinnedDirectory,
    target_name: str,
    *,
    label: str,
    ownership_id: str,
) -> tuple[str, Path, int]:
    """Create or resume the sole temp durably owned by one operation."""

    name = _owned_temp_name(target_name, label, ownership_id)
    path = parent.path / name
    created = False
    try:
        if os.name == "nt":
            try:
                descriptor = _windows_open_relative_file(
                    parent.descriptor,
                    name,
                    create=True,
                )
                created = True
            except FileExistsError:
                descriptor = _windows_open_relative_file(
                    parent.descriptor,
                    name,
                    create=False,
                )
        else:
            flags = (
                os.O_RDWR
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                descriptor = os.open(name, flags, 0o600, dir_fd=parent.descriptor)
                created = True
            except FileExistsError:
                descriptor = os.open(
                    name,
                    os.O_RDWR
                    | getattr(os, "O_BINARY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent.descriptor,
                )
    except PromptArchiveMigrationError:
        raise
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    try:
        if not _descriptor_matches_relative_leaf(parent, name, descriptor, path):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        # This check is deliberately before ftruncate/write.  A deterministic
        # name proves which operation may resume a temp, but it does not prove
        # that an operator-created leaf with the same name is stream-safe.
        _windows_reject_named_streams_descriptor(descriptor)
        if not created:
            # The deterministic name is an immutable reservation in the batch
            # plan.  Only that exact operation may resume and rewrite it.
            os.ftruncate(descriptor, 0)
            os.lseek(descriptor, 0, os.SEEK_SET)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    return name, path, descriptor


def _write_and_verify_descriptor(descriptor: int, raw: bytes) -> None:
    try:
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset : offset + 1024 * 1024])
            if written <= 0:
                raise OSError("short write")
            offset += written
        os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, len(raw) + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > len(raw):
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            chunks.append(chunk)
    except PromptArchiveMigrationError:
        raise
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    if b"".join(chunks) != raw:
        raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")


def _discard_owned_relative_temp(
    parent: _PinnedDirectory,
    name: str,
    descriptor: int,
) -> None:
    try:
        path = parent.path / name
        if descriptor < 0 or not _descriptor_matches_relative_leaf(
            parent, name, descriptor, path
        ):
            return
        if os.name == "nt":
            # Never delete an unsafe pre-existing/raced leaf while cleaning up
            # a failed publication.  The held handle denies write/delete share
            # and the second identity proof binds this pathname to that handle.
            _windows_reject_named_streams_descriptor(descriptor)
            if not _descriptor_matches_relative_leaf(
                parent, name, descriptor, path
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
            _windows_mark_file_for_delete(descriptor)
        else:
            os.unlink(name, dir_fd=parent.descriptor)
    except (
        FileNotFoundError,
        PromptArchiveMigrationError,
        PromptArchiveStorageError,
        OSError,
    ):
        # Cleanup is best-effort and must not mask the publication failure.
        # Every error above occurs before a potentially unsafe temp is deleted.
        pass


def _publish_pinned_no_replace(
    parent: _PinnedDirectory,
    target_name: str,
    target_path: Path,
    raw: bytes,
    *,
    label: str,
    ownership_id: str,
    maximum: int,
) -> bool:
    temporary_name, temporary_path, descriptor = _open_owned_relative_temp(
        parent,
        target_name,
        label=label,
        ownership_id=ownership_id,
    )
    published = False
    try:
        _write_and_verify_descriptor(descriptor, raw)
        try:
            if not _descriptor_matches_relative_leaf(
                parent,
                temporary_name,
                descriptor,
                temporary_path,
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
            # Recheck the complete stream set after writing and immediately
            # before publication.  The Windows handle remains open without
            # write/delete sharing throughout this check and rename.
            _windows_reject_named_streams_descriptor(descriptor)
            if not _descriptor_matches_relative_leaf(
                parent,
                temporary_name,
                descriptor,
                temporary_path,
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
            published = _atomic_rename_pinned_no_replace(
                descriptor,
                parent,
                temporary_name,
                target_name,
            )
            if published and os.name == "nt":
                # A CRT reader cannot share the DELETE access held by the NT
                # rename handle on every supported Windows build.  The named
                # file is already durable and is not delete-on-close, so close
                # before reopening it for the byte-exact verification below.
                os.close(descriptor)
                descriptor = -1
        except PromptArchiveStorageError as exc:
            raise _error_from_storage(exc) from None
        if published:
            observed, metadata = _read_pinned_leaf(
                parent,
                target_name,
                target_path,
                maximum=maximum,
            )
            if observed != raw or metadata.st_nlink != 1:
                raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
            PromptArchiveStorage._sync_pinned_directory(parent)
            return True
        observed, metadata = _read_pinned_leaf(
            parent,
            target_name,
            target_path,
            maximum=maximum,
        )
        if observed != raw or metadata.st_nlink != 1:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        return False
    finally:
        try:
            if not published:
                _discard_owned_relative_temp(parent, temporary_name, descriptor)
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _preflight_private_evidence_file(path: Path, raw: bytes) -> None:
    """Reject a conflicting witness before any per-entry publication."""

    if len(raw) > _MAX_PRIVATE_EVIDENCE_BYTES:
        raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
    with _open_pinned_mutation_directory(path.parent) as parent:
        _validate_component(path.name)
        try:
            metadata = (
                path.lstat()
                if os.name == "nt"
                else os.stat(
                    path.name,
                    dir_fd=parent.descriptor,
                    follow_symlinks=False,
                )
            )
        except FileNotFoundError:
            return
        except OSError:
            raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_ISLNK(metadata.st_mode)
            or bool(
                _REPARSE_POINT
                and getattr(metadata, "st_file_attributes", 0) & _REPARSE_POINT
            )
        ):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        observed, opened = _read_pinned_leaf(
            parent,
            path.name,
            path,
            maximum=len(raw),
        )
        if observed != raw or opened.st_nlink != 1:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        _validate_pinned_mutation_directory(parent, path.parent)


def _publish_private_evidence_file(
    path: Path,
    raw: bytes,
    *,
    ownership_id: str,
) -> tuple[str, int]:
    """Durably create-or-verify one explicitly selected private witness."""

    if len(raw) > _MAX_PRIVATE_EVIDENCE_BYTES:
        raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
    digest = hashlib.sha256(raw).hexdigest()
    with _open_pinned_mutation_directory(path.parent) as parent:
        _validate_component(path.name)
        try:
            observed, metadata = _read_pinned_leaf(
                parent,
                path.name,
                path,
                maximum=len(raw),
            )
        except PromptArchiveMigrationError as exc:
            if exc.code != "PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE":
                raise
        else:
            if observed != raw or metadata.st_nlink != 1:
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            return digest, len(raw)
        _publish_pinned_no_replace(
            parent,
            path.name,
            path,
            raw,
            label="par-private",
            ownership_id=ownership_id,
            maximum=len(raw),
        )
        observed, metadata = _read_pinned_leaf(
            parent,
            path.name,
            path,
            maximum=len(raw),
        )
        if observed != raw or metadata.st_nlink != 1:
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
        _validate_pinned_mutation_directory(parent, path.parent)
    return digest, len(raw)


def _ensure_reconstruction_root(root: Path) -> None:
    try:
        metadata = root.lstat()
    except FileNotFoundError:
        metadata = None
    except OSError:
        raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
    if metadata is not None:
        if not stat.S_ISDIR(metadata.st_mode) or _is_redirected(root):
            raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
        _assert_existing_chain_safe(root)
        return
    with _open_pinned_mutation_directory(root.parent) as parent:
        child, _created = _open_or_create_pinned_child(parent, root.name)
        try:
            _validate_pinned_mutation_directory(child, root)
        finally:
            child.close()


def _preflight_reconstruction_root(
    root: Path,
    specs: Sequence[tuple[InventoryEntry, str]],
) -> None:
    """Prove an existing target contains only exact or owned resume state.

    This scan completes before the first reconstructed file is published.  It
    accepts an empty target, byte-exact already-published outputs, and only the
    deterministic temporary leaf reserved by the corresponding durable
    operation.  Any unrelated directory/file therefore fails without leaving
    a partial reconstruction beside operator-owned content.
    """

    expected_files: dict[str, InventoryEntry] = {}
    expected_temps: dict[str, InventoryEntry] = {}
    expected_directories: set[str] = set()
    for entry, ownership_id in specs:
        if entry.relative_path in expected_files:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        expected_files[entry.relative_path] = entry
        components = entry.relative_path.split("/")
        for index in range(1, len(components)):
            expected_directories.add("/".join(components[:index]))
        temp_name = _owned_temp_name(
            components[-1],
            "par-reconstruct",
            ownership_id,
        )
        temp_relative = "/".join((*components[:-1], temp_name))
        if temp_relative in expected_temps or temp_relative in expected_files:
            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
        expected_temps[temp_relative] = entry

    seen_casefolded: set[str] = set()
    seen_files: set[str] = set()
    seen_temps: set[str] = set()
    directory_count = 0
    with _RootPin(root) as root_pin:
        def walk(
            directory: Path,
            components: tuple[str, ...],
            *,
            inherited_descriptor: int | None = None,
        ) -> None:
            nonlocal directory_count
            directory_count += 1
            if (
                directory_count > _MAX_DIRECTORIES
                or len(components) > _MAX_DIRECTORY_DEPTH
            ):
                raise _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
            if _is_redirected(directory):
                raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
            _windows_reject_named_streams(directory, directory=True)
            with _RootPin(
                directory,
                descriptor=inherited_descriptor,
            ) as directory_pin:
                if (
                    directory_pin.resolved_path is None
                    or not _same_path(directory_pin.resolved_path, directory)
                ):
                    raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
                try:
                    before = (
                        directory.lstat()
                        if os.name == "nt"
                        else os.fstat(directory_pin.descriptor)
                    )
                    children = list(
                        os.scandir(
                            directory
                            if os.name == "nt"
                            else directory_pin.descriptor
                        )
                    )
                except OSError:
                    raise _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE") from None
                for child in children:
                    _validate_component(child.name)
                children.sort(key=lambda item: item.name.encode("utf-8"))
                for child in children:
                    relative_components = components + (child.name,)
                    relative = "/".join(relative_components)
                    _claim_casefolded_path(seen_casefolded, relative)
                    child_path = directory / child.name
                    try:
                        metadata = (
                            child_path.lstat()
                            if os.name == "nt"
                            else os.stat(
                                child.name,
                                dir_fd=directory_pin.descriptor,
                                follow_symlinks=False,
                            )
                        )
                    except OSError:
                        raise _error(
                            "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED"
                        ) from None
                    if (
                        stat.S_ISLNK(metadata.st_mode)
                        or bool(
                            _REPARSE_POINT
                            and getattr(metadata, "st_file_attributes", 0)
                            & _REPARSE_POINT
                        )
                        or (os.name == "nt" and _is_redirected(child_path))
                    ):
                        raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE")
                    if stat.S_ISDIR(metadata.st_mode):
                        if relative not in expected_directories:
                            raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
                        child_descriptor: int | None = None
                        if os.name != "nt":
                            try:
                                child_descriptor = os.open(
                                    child.name,
                                    os.O_RDONLY
                                    | getattr(os, "O_DIRECTORY", 0)
                                    | getattr(os, "O_NOFOLLOW", 0),
                                    dir_fd=directory_pin.descriptor,
                                )
                            except OSError:
                                raise _error(
                                    "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED"
                                ) from None
                        walk(
                            child_path,
                            relative_components,
                            inherited_descriptor=child_descriptor,
                        )
                        continue
                    entry = expected_files.get(relative)
                    temporary = False
                    if entry is None:
                        entry = expected_temps.get(relative)
                        temporary = entry is not None
                    if entry is None:
                        raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
                    _raw, size, digest, _observed = _inspect_regular_file(
                        child_path,
                        maximum=entry.byte_size,
                        collect=False,
                        parent_descriptor=(
                            None if os.name == "nt" else directory_pin.descriptor
                        ),
                        relative_name=None if os.name == "nt" else child.name,
                    )
                    if temporary:
                        seen_temps.add(entry.relative_path)
                    else:
                        if size != entry.byte_size or digest != entry.sha256:
                            raise _error(
                                "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT"
                            )
                        seen_files.add(entry.relative_path)
                try:
                    after = (
                        directory.lstat()
                        if os.name == "nt"
                        else os.fstat(directory_pin.descriptor)
                    )
                except OSError:
                    raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED") from None
                if (after.st_dev, after.st_ino, after.st_mtime_ns) != (
                    before.st_dev,
                    before.st_ino,
                    before.st_mtime_ns,
                ):
                    raise _error("PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED")
                _windows_reject_named_streams(directory, directory=True)
                directory_pin.assert_stable()

        walk(root, ())
        root_pin.assert_stable()
    if seen_files & seen_temps:
        raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")


def _publish_reconstructed_file(
    root: Path,
    entry: InventoryEntry,
    raw: bytes,
    *,
    ownership_id: str,
) -> None:
    if len(raw) != entry.byte_size or hashlib.sha256(raw).hexdigest() != entry.sha256:
        raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
    with _pinned_relative_parent(root, entry.relative_path) as (
        parent,
        target_name,
        target,
    ):
        try:
            observed, metadata = _read_pinned_leaf(
                parent,
                target_name,
                target,
                maximum=entry.byte_size,
            )
        except PromptArchiveMigrationError as exc:
            if exc.code != "PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE":
                raise
        else:
            if observed != raw or metadata.st_nlink != 1:
                raise _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
            return
        _publish_pinned_no_replace(
            parent,
            target_name,
            target,
            raw,
            label="par-reconstruct",
            ownership_id=ownership_id,
            maximum=entry.byte_size,
        )
        observed, metadata = _read_pinned_leaf(
            parent,
            target_name,
            target,
            maximum=entry.byte_size,
        )
        if observed != raw or metadata.st_nlink != 1:
            raise _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")


def _error_from_storage(exc: PromptArchiveStorageError) -> PromptArchiveMigrationError:
    if exc.code in {
        "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        "PROMPT_ARCHIVE_BLOB_CORRUPT",
        "PROMPT_ARCHIVE_BLOB_MISSING",
    }:
        return _error("PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH")
    if exc.code in {
        "PROMPT_ARCHIVE_GENERATION_CONFLICT",
        "PROMPT_ARCHIVE_REFERENCE_INVALID",
        "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
    }:
        return _error("PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT")
    if exc.code == "PROMPT_ARCHIVE_LIMIT_EXCEEDED":
        return _error("PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED")
    return _error("PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE")


def migration_error_evidence(
    exc: PromptArchiveMigrationError,
    *,
    command: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "prompt_archive_migration_error",
        "media_type": _EVIDENCE_MEDIA,
        "command": command,
        "status": "failed",
        "error": exc.code,
        "message": str(exc),
        "fail_closed": True,
    }


__all__ = [
    "InventoryEntry",
    "OFFLINE_OPERATOR_GATE",
    "PromptArchiveMigration",
    "PromptArchiveMigrationError",
    "SourceInventory",
    "inventory_isolated_root",
    "migration_error_evidence",
]
